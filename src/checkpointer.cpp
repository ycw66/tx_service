#include "checkpointer.h"

#include "proto/cc_request.pb.h"
#include "range_slice.h"
#include "remote/cc_stream_sender.h"
#include "sharder.h"
#include "statistics.h"
#include "tx_service.h"

namespace txservice
{
Checkpointer::Checkpointer(LocalCcShards &shards,
                           store::DataStoreHandler *write_hd,
                           const uint32_t &checkpoint_interval,
                           TxLog *log_agent,
                           uint32_t ckpt_delay_seconds)
    : local_shards_(shards),
      ckpt_mux_(),
      ckpt_cv_(),
      request_ckpt_(false),
      store_hd_(write_hd),
      ckpt_thd_status_(Status::Active),
      checkpoint_interval_(checkpoint_interval),
      ckpt_delay_time_(ckpt_delay_seconds * 1000000),
      log_agent_(log_agent),
      flush_mux_(),
      flush_cv_(),
      worker_thd_status_(Status::Active),
      slice_update_mux_(),
      slice_update_cv_(),
      slice_thd_status_(Status::Active)
{
    tx_service_ = shards.tx_service_;
    for (std::unique_ptr<CcShard> &ccs : shards.cc_shards_)
    {
        ccs->ckpter_ = this;
    }

    thd_ = std::thread([this] { Run(); });

    DLOG(INFO) << "checkpointer init, checkpoint_interval_: "
               << checkpoint_interval_
               << " ,ckpt_delay_seconds: " << ckpt_delay_seconds;
}

Checkpointer::~Checkpointer()
{
    /*std::unique_lock<std::mutex> lk(ckpt_mux_);
    if (status_ == Status::Active)
    {
        lk.unlock();
        Exit();
        thd_.join();
        status_ = Status::Terminated;
    }
    else
    {
        lk.unlock();
        thd_.join();
        status_ = Status::Terminated;
    }*/
}

void Checkpointer::Ckpt(bool is_last_ckpt)
{
    if (local_shards_.Count() == 0 || store_hd_ == nullptr)
    {
        return;
    }
    std::vector<uint32_t> node_groups = Sharder::Instance().LocalNodeGroups();
    for (uint32_t node_group : node_groups)
    {
        // check whether this node is group leader, pin its data if it is
        int64_t leader_term =
            Sharder::Instance().TryPinNodeGroupData(node_group);
        if (leader_term < 0)
        {
            continue;
        }
        size_t shard_cnt = local_shards_.Count();
        CkptTsCc ckpt_req(shard_cnt, node_group);

        // Find minimum ckpt_ts from all the ccshards in parallel. ckpt_ts is
        // the minimum timestamp minus 1 among all the active transactions, thus
        // it's safe to flush all the entries smaller than or equal to ckpt_ts.
        for (auto &ccs : local_shards_.cc_shards_)
        {
            ccs->Enqueue(&ckpt_req);
        }
        ckpt_req.Wait();

        uint64_t ckpt_ts = UINT64_MAX;
        ckpt_ts = ckpt_req.GetCkptTs();

        if (local_shards_.EnableMvcc() && !is_last_ckpt)
        {
            uint64_t min_si_tx_ts =
                TxStartTsCollector::Instance().GlobalMinSiTxStartTs();
            uint64_t delayed_ckpt_ts = ckpt_req.GetCkptTs() - ckpt_delay_time_;
            if (min_si_tx_ts < delayed_ckpt_ts)
            {
                ckpt_ts = delayed_ckpt_ts;
            }
            else if (min_si_tx_ts < ckpt_req.GetCkptTs())
            {
                ckpt_ts = min_si_tx_ts;
            }
        }
        uint64_t last_ckpt_ts =
            Sharder::Instance().GetNodeGroupCkptTs(node_group);
        if (ckpt_ts <= last_ckpt_ts)
        {
            // skip checkpoint for this node group
            Sharder::Instance().UnpinNodeGroupData(node_group);
            continue;
        }
        LOG(INFO) << "Begin checkpoint with timestamp: " << ckpt_ts
                  << ". The memory usage of node is: " << ckpt_req.GetMemUsage()
                  << " KB.";

        // Reset result bool
        bool fail = true;
        flush_worker_failed_.compare_exchange_strong(fail, false);
        // Get table names in this node group, checkpointer should be TableName
        // string owner.
        std::vector<TableName> tables =
            local_shards_.GetCatalogTableNamesForCkpt(node_group);
        auto total_table = tables.size();

        std::mutex work_sender_mux;
        std::condition_variable work_sender_cv;
        uint16_t finish_work_cnt = 0;
        uint16_t work_started = 0;
#ifdef RANGE_PARTITION_ENABLED
        std::vector<std::thread> range_split_workers;
#endif

        // Iterate all the tables and execute CkptScanCc requests on this node
        // group's ccmaps on each ccshard. The result of CkptScanCc is stored in
        // ckpt_vec.
        for (uint16_t idx = 0; idx < tables.size(); idx++)
        {
            if (Sharder::Instance().LeaderTerm(node_group) != leader_term)
            {
                break;
            }

            TableName &table_name = tables.at(idx);
            if (table_name.Type() == TableType::Catalog ||
                table_name.Type() == TableType::RangePartition)
            {
                continue;
            }
            if (idx >= total_table)
            {
                // Have scanned over original tables. Now retrying the failed
                // tables. Wait for 1 second before retrying to avoid busy loop.
                std::this_thread::sleep_for(std::chrono::seconds(1));
                total_table = tables.size();
            }
            // Issue read catalog tx_request to acquire read lock on catalog
            // cc_entry using base table name, and acquire read lock in one
            // shard is good enough to block schema change.
            const TableName base_table_name{table_name.GetBaseTableNameSV(),
                                            TableType::Primary};

            TransactionExecution *ckpt_txm = tx_service_->NewTx();
            InitTxRequest init_req;
            // Set isolation level to RepeatableRead to ensure the readlock
            // will be set during the execution of the following
            // ReadTxRequest.
            init_req.iso_level_ = IsolationLevel::RepeatableRead;
            init_req.protocol_ = CcProtocol::Locking;
            init_req.Reset();
            ckpt_txm->Execute(&init_req);
            init_req.Wait();

            if (init_req.IsError())
            {
                LOG(INFO) << "checkpointer init checkpoint transaction failed.";
                tables.push_back(std::move(table_name));
                continue;
            }

            // If table_name has been dropped at this point, read lock would
            // not be acquired.
            CatalogKey table_key(base_table_name);
            CatalogRecord catalog_rec;

            ReadTxRequest read_req;
            read_req.Reset();
            read_req.Set(&catalog_ccm_name,
                         &table_key,
                         &catalog_rec,
                         false,
                         false,
                         true,
                         0UL);
            ckpt_txm->Execute(&read_req);
            read_req.Wait();

            if (read_req.IsError() || read_req.Result() != RecordStatus::Normal)
            {
                // Use AbortTxRequest to release read lock.
                AbortTxRequest abort_req;
                abort_req.Reset();
                ckpt_txm->Execute(&abort_req);
                abort_req.Wait();
                assert(abort_req.Result() == false);
                LOG(INFO) << "checkpointer add read lock on table failed, "
                             "table name: "
                          << table_key.Name().StringView();

                if (read_req.IsError())
                {
                    // If read lock acquire failed, retry next time. If
                    // table is delted, skip the table.
                    tables.push_back(std::move(table_name));
                }
                continue;
            }

            std::vector<std::vector<FlushRecord>> ckpt_vecs;
            std::vector<std::vector<FlushRecord>> archive_vecs;
            std::vector<std::vector<const TxKey *>> mv_base_vecs;

            std::vector<std::pair<TxKey::Uptr, bool>> resume_pos;
            bool scan_succ = true;

            for (size_t i = 0; i < local_shards_.cc_shards_.size(); i++)
            {
                ckpt_vecs.emplace_back();
                archive_vecs.emplace_back();
                mv_base_vecs.emplace_back();
                resume_pos.emplace_back(nullptr, false);
            }

            bool scan_data_drained = false;
            CkptScanCc scan_cc(table_name,
                               ckpt_ts,
                               node_group,
                               local_shards_.cc_shards_.size(),
                               std::move(resume_pos),
                               CKPT_SCAN_BATCH_SIZE);
            while (!scan_data_drained)
            {
                for (size_t i = 0; i < local_shards_.cc_shards_.size(); i++)
                {
                    local_shards_.EnqueueToCcShard(i, &scan_cc);
                }
                scan_cc.Wait();

                if (scan_cc.IsError())
                {
                    LOG(INFO) << "ckpt scan failed on table "
                              << table_name.StringView();
                    AbortTxRequest abort_req;
                    abort_req.Reset();
                    ckpt_txm->Execute(&abort_req);
                    abort_req.Wait();
                    tables.push_back(std::move(table_name));
                    scan_succ = false;
                    break;
                }
                else
                {
                    auto &res = scan_cc.Result();
                    scan_data_drained = true;

                    for (size_t i = 0; i < local_shards_.cc_shards_.size(); i++)
                    {
                        // if the data is drained
                        scan_data_drained =
                            res.at(i).second && scan_data_drained;
                        // move the bucket into the tank
                        std::move(scan_cc.CkptVec(i).begin(),
                                  scan_cc.CkptVec(i).end(),
                                  std::back_inserter(ckpt_vecs.at(i)));

                        std::move(scan_cc.ArchiveVec(i).begin(),
                                  scan_cc.ArchiveVec(i).end(),
                                  std::back_inserter(archive_vecs.at(i)));

                        std::move(scan_cc.MoveBaseVec(i).begin(),
                                  scan_cc.MoveBaseVec(i).end(),
                                  std::back_inserter(mv_base_vecs.at(i)));
                    }
                    scan_cc.Reset(std::move(res));
                }
            }

            if (!scan_succ)
            {
                continue;
            }
            std::unique_ptr<std::vector<FlushRecord>> ckpt_vec =
                std::make_unique<std::vector<FlushRecord>>();

            std::unique_ptr<std::vector<FlushRecord>> archive_vec =
                std::make_unique<std::vector<FlushRecord>>();

            std::unique_ptr<std::vector<const TxKey *>> mv_base_vec =
                std::make_unique<std::vector<const TxKey *>>();
            // Sort output vectors in key sorting order.
            auto key_greater = [](const TxKey *r1, const TxKey *r2) -> bool
            { return *r2 < *r1; };
            MergeSortedVectors(
                std::move(mv_base_vecs), *mv_base_vec, key_greater, true);
            auto rec_greater = [](const FlushRecord &r1,
                                  const FlushRecord &r2) -> bool
            { return *r2.Key() < *r1.Key(); };
            // To avoid repeatedly set the ckpt_ts_ of a cc entry, which might
            // cause the ccentry become invalid in between, remove duplicate
            // flush record from ckpt_vec.
            MergeSortedVectors(
                std::move(ckpt_vecs), *ckpt_vec, rec_greater, true);
            // For archive vec we don't need to worry about duplicate causing
            // issue since we're not visiting their cc entry. Also we cannot
            // rely on key compare to dedup archive vec since a key could have
            // multiple version of archive versions.
            MergeSortedVectors(
                std::move(archive_vecs), *archive_vec, rec_greater, false);

#ifdef RANGE_PARTITION_ENABLED
            std::vector<std::pair<const TxKey *, const TxKey *>> split_ranges;
            size_t batch_idx = 0;

            while (batch_idx < ckpt_vec->size())
            {
                std::pair<const StoreRange *, std::vector<const TxKey *>>
                    split_pair;
                split_pair.first = nullptr;

                bool ret = UpdateSliceAndCalculateRangeUpdate(
                    table_name,
                    catalog_rec.Schema()->GetKVCatalogInfo(),
                    catalog_rec.SchemaTs(),
                    node_group,
                    *ckpt_vec,
                    last_ckpt_ts,
                    ckpt_ts,
                    batch_idx,
                    split_pair);

                if (!ret)
                {
                    LOG(INFO) << "Pre-checkpoint slice update failed on table "
                              << table_name.StringView();

                    for (auto &split_thread : range_split_workers)
                    {
                        split_thread.join();
                    }
                    range_split_workers.clear();

                    AbortTxRequest abort_req;
                    ckpt_txm->Execute(&abort_req);
                    abort_req.Wait();
                    tables.push_back(std::move(table_name));
                    continue;
                }

                if (!is_last_ckpt && split_pair.first != nullptr)
                {
                    const StoreRange *split_range = split_pair.first;
                    split_ranges.emplace_back(split_range->RangeStartKey(),
                                              split_range->RangeEndKey());

                    range_split_workers.emplace_back(std::thread(
                        [this,
                         &table_name,
                         split_info = std::move(split_pair),
                         node_group] {
                            SplitFlushRange(table_name, node_group, split_info);
                        }));
                }
            }

            if (!is_last_ckpt && !split_ranges.empty())
            {
                // Remove the records that are in the splitting ranges.
                // They will  be handled by the splitting  worker.

                // Remove mv base vec first since it contains raw pointers
                // to the records in ckpt_vec and archive_vec
                auto key_lower_bound_cmp =
                    [](const TxKey *key1, const TxKey &key2)
                { return *key1 < key2; };
                std::unique_ptr<std::vector<const TxKey *>> flush_mv_base_vec =
                    std::make_unique<std::vector<const TxKey *>>();
                MoveNonSplittingRecords(*mv_base_vec,
                                        *flush_mv_base_vec,
                                        split_ranges,
                                        key_lower_bound_cmp);
                mv_base_vec = std::move(flush_mv_base_vec);

                auto lower_bound_cmp =
                    [](const FlushRecord &rec, const TxKey &key)
                { return *rec.Key() < key; };
                std::unique_ptr<std::vector<FlushRecord>> flush_ckpt_vec =
                    std::make_unique<std::vector<FlushRecord>>();
                MoveNonSplittingRecords(
                    *ckpt_vec, *flush_ckpt_vec, split_ranges, lower_bound_cmp);
                ckpt_vec = std::move(flush_ckpt_vec);

                std::unique_ptr<std::vector<FlushRecord>> flush_archive_vec =
                    std::make_unique<std::vector<FlushRecord>>();
                MoveNonSplittingRecords(*archive_vec,
                                        *flush_archive_vec,
                                        split_ranges,
                                        lower_bound_cmp);
                archive_vec = std::move(flush_archive_vec);
            }
#endif

            if (ckpt_vec->size() != 0 || archive_vec->size() != 0 ||
                mv_base_vec->size() != 0)
            {
                std::unique_lock<std::mutex> worker_lk(flush_mux_);
                work_started++;
                pending_flush_work_.emplace_back(node_group,
                                                 leader_term,
                                                 ckpt_ts,
                                                 table_name,
                                                 catalog_rec.Schema(),
                                                 std::move(ckpt_vec),
                                                 std::move(archive_vec),
                                                 std::move(mv_base_vec),
                                                 ckpt_txm,
                                                 &work_sender_mux,
                                                 &work_sender_cv,
                                                 &finish_work_cnt,
                                                 &flush_worker_failed_);
                flush_cv_.notify_one();
            }
            else
            {
                bool ok =
                    catalog_rec.Schema()->StatisticsObject()->PostCheckpoint(
                        store_hd_, table_name, node_group, ckpt_ts, true);
                if (!ok)
                {
                    AbortTxRequest abort_req;
                    ckpt_txm->Execute(&abort_req);
                    abort_req.Wait();
                }

                CommitTxRequest commit_req;

                commit_req.Reset();
                ckpt_txm->Execute(&commit_req);
                commit_req.Wait();
            }
        }
        if (Sharder::Instance().LeaderTerm(node_group) != leader_term)
        {
            // Skip the node groups that are no longer on this node.
            Sharder::Instance().UnpinNodeGroupData(node_group);
            continue;
        }

        {
            std::unique_lock<std::mutex> work_sender_lk(work_sender_mux);
            work_sender_cv.wait(work_sender_lk,
                                [&finish_work_cnt, &work_started]
                                { return finish_work_cnt == work_started; });
        }

#ifdef RANGE_PARTITION_ENABLED
        for (auto &split_thread : range_split_workers)
        {
            split_thread.join();
        }
#endif
        // finish checkpoint on this node group, unpin its data and clear its
        // ccmaps and catalogs if it is no longer leader
        Sharder::Instance().UnpinNodeGroupData(node_group);

        if (!flush_worker_failed_.load(std::memory_order_relaxed) &&
            Sharder::Instance().LeaderTerm(node_group) == leader_term)
        {
            LOG(INFO) << "Checkpoint of node group #" << node_group
                      << " succeeded with timestamp: " << ckpt_ts;
            Sharder::Instance().UpdateNodeGroupCkptTs(node_group, ckpt_ts);
            NotifyLogOfCkptTs(node_group, leader_term, ckpt_ts);
        }
    }
    // notify ccshard ckpt has finished and can re-check freeable ccentries.
    local_shards_.SetWaitingCkpt(false);
}

void Checkpointer::FlushDataWorker()
{
    std::unique_lock<std::mutex> worker_lk(flush_mux_);
    while (worker_thd_status_ == Status::Active)
    {
        flush_cv_.wait(worker_lk,
                       [this]
                       {
                           return !pending_flush_work_.empty() ||
                                  worker_thd_status_ == Status::Terminated;
                       });

        if (pending_flush_work_.empty())
        {
            continue;
        }

        // Retrieve first pending work and pop it.
        FlushDataWork &cur_work = pending_flush_work_.back();
        uint64_t ckpt_ts = cur_work.ckpt_ts_;
        uint32_t node_group = cur_work.node_group_;
        int64_t leader_term = cur_work.term_;
        TableName table_name = cur_work.table_name_;
        const TableSchema *schema = cur_work.schema_;
        std::unique_ptr<vector<FlushRecord>> ckpt_vec_owner, archive_vec_owner;
        std::vector<FlushRecord> *ckpt_vec, *archive_vec;
        std::unique_ptr<std::vector<const TxKey *>> mv_base_owner;
        std::vector<const TxKey *> *mv_base_vec;
        if (cur_work.vec_owner_)
        {
            ckpt_vec_owner = std::move(cur_work.ckpt_vec_);
            ckpt_vec = ckpt_vec_owner.get();
            archive_vec_owner = std::move(cur_work.archive_vec_);
            archive_vec = archive_vec_owner.get();
            mv_base_owner = std::move(cur_work.mv_base_vec_);
            mv_base_vec = mv_base_owner.get();
        }
        else
        {
            ckpt_vec = cur_work.ckpt_vec_ptr_;
            archive_vec = cur_work.archive_vec_ptr_;
            mv_base_vec = cur_work.mv_base_vec_ptr_;
        }
        TransactionExecution *txm = cur_work.txm_;
        std::mutex *sender_mux = cur_work.sender_mux_;
        std::condition_variable *sender_cv = cur_work.sender_cv_;
        uint16_t *finish_work_cnt = cur_work.finish_work_cnt_;
        std::atomic_bool *fail = cur_work.fail_;
        CcHandlerResult<Void> *hand_res = cur_work.hand_res_;

        pending_flush_work_.pop_back();
        worker_lk.unlock();

        bool succ = true;

        // flush to data store if this node group leader term does not
        // change
        if (!(ckpt_vec->empty() && archive_vec->empty() &&
              mv_base_vec->empty()) &&
            Sharder::Instance().LeaderTerm(node_group) == leader_term)
        {
            // Flushes to the data store
            bool ckpt_ret = true;

            if (local_shards_.EnableMvcc() && mv_base_vec->size() > 0)
            {
                ckpt_ret = store_hd_->CopyBaseToArchive(
                    *mv_base_vec, node_group, table_name, schema);
                if (!ckpt_ret)
                {
                    LOG(INFO) << "checkpointer CopyBaseToArchive flush to kv "
                                 "storage failed";
                }
            }

            if (ckpt_ret && !ckpt_vec->empty())
            {
                ckpt_ret = store_hd_->PutAll(
                    *ckpt_vec, table_name, schema, node_group);
                if (!ckpt_ret)
                {
                    LOG(INFO) << "checkpointer PutAll flush to kv "
                                 "storage failed";
                }
            }

            if (ckpt_ret && local_shards_.EnableMvcc())
            {
                ckpt_ret = store_hd_->PutArchivesAll(node_group,
                                                     table_name,
                                                     schema->GetKVCatalogInfo(),
                                                     *archive_vec);

                if (!ckpt_ret)
                {
                    // If ckpt succeeds and flushing undo fails, it is safe
                    // to update the local checkpoint timestamp, but not
                    // safe to truncate the redo log.
                    succ = false;
                    LOG(INFO) << "checkpointer PutArchivesAll flush to "
                                 "kv storage failed";
                }
            }

            // If flush to data store succeeds, update the ckpt_ts for each
            // entry in ccmap to latest checkpoint version's commit_ts.
            if (ckpt_ret)
            {
                for (auto &ref : *ckpt_vec)
                {
                    // todo: remove cce_
                    ref.cce_->data_store_size_.fetch_add(ref.delta_size_);
                    ref.cce_->ckpt_ts_.store(ref.commit_ts_,
                                             std::memory_order_release);
                }
                ResetCleanStartPageCc reset_cc(local_shards_.Count());
                for (auto &ccs : local_shards_.cc_shards_)
                {
                    ccs->Enqueue(&reset_cc);
                }
                reset_cc.Wait();
#ifdef RANGE_PARTITION_ENABLED
                // Update the slice size in data store.
                if (ckpt_vec->size())
                {
                    UpdateStoreSlice(table_name,
                                     schema->GetKVCatalogInfo(),
                                     schema->Version(),
                                     node_group,
                                     *ckpt_vec,
                                     ckpt_ts,
                                     true);
                }
#endif
            }
            else
            {
#ifdef RANGE_PARTITION_ENABLED
                // Reset the post ckpt size if flush failed
                UpdateStoreSlice(table_name,
                                 schema->GetKVCatalogInfo(),
                                 schema->Version(),
                                 node_group,
                                 *ckpt_vec,
                                 ckpt_ts,
                                 false);
#endif
                succ = false;
            }

            if (succ)
            {
                succ = schema->StatisticsObject()->PostCheckpoint(
                    store_hd_, table_name, node_group, ckpt_ts, false);
            }
        }

        if (txm != nullptr)
        {
            CommitTxRequest commit_req;

            commit_req.Reset();
            txm->Execute(&commit_req);
            commit_req.Wait();
        }

        // Update the work count if the work's sender is waiting.
        if (sender_mux != nullptr)
        {
            std::unique_lock<std::mutex> lk(*sender_mux);
            uint16_t &cnt = *finish_work_cnt;
            ++cnt;
            sender_cv->notify_one();
        }

        if (fail != nullptr && !succ)
        {
            bool false_ref = false;
            fail->compare_exchange_strong(false_ref, true);
        }

        if (hand_res)
        {
            if (!succ)
            {
                hand_res->SetError(CcErrorCode::DATA_STORE_ERR);
            }
            else
            {
                hand_res->SetFinished();
            }
        }

        worker_lk.lock();
    }
}

void Checkpointer::UpdateSliceSpecWorker()
{
    std::unique_lock<std::mutex> worker_lk(slice_update_mux_);
    while (worker_thd_status_ == Status::Active)
    {
        slice_update_cv_.wait(worker_lk,
                              [this]
                              {
                                  return !pending_slice_work_.empty() ||
                                         slice_thd_status_ ==
                                             Status::Terminated;
                              });

        if (pending_slice_work_.empty())
        {
            continue;
        }

        UpdateSliceSpecWork &cur_work = pending_slice_work_.back();

        uint64_t ckpt_ts = cur_work.ckpt_ts_;
        uint32_t node_group = cur_work.node_group_;
        TableName table_name = cur_work.table_name_;
        StoreRange *range = cur_work.range_;
        StoreSlice *slice = cur_work.slice_;
        size_t start_idx = cur_work.start_idx_;
        size_t end_idx = cur_work.end_idx_;
        const std::vector<FlushRecord> &flush_vec = cur_work.flush_vec_;
        std::mutex &sender_mux = cur_work.sender_mux_;
        std::condition_variable &sender_cv = cur_work.sender_cv_;
        size_t &finish_work_cnt = cur_work.finish_work_cnt_;
        bool &fail = cur_work.fail_;

        pending_slice_work_.pop_back();
        worker_lk.unlock();

        bool res = range->UpdateSliceSpec(slice,
                                          table_name,
                                          node_group,
                                          ckpt_ts,
                                          flush_vec,
                                          start_idx,
                                          end_idx,
                                          false);
        {
            std::unique_lock<std::mutex> lk(sender_mux);
            finish_work_cnt++;
            if (!res)
            {
                fail = true;
            }
            sender_cv.notify_one();
        }
        worker_lk.lock();
    }
}

void Checkpointer::SplitFlushRange(
    const TableName &table_name,
    NodeGroupId node_group,
    std::pair<const StoreRange *, std::vector<const TxKey *>> split_info)
{
    bool fail = false;
    std::string log_output(
        "Splitting table " + table_name.String() + " range " +
        std::to_string(split_info.first->PartitionId()) + " into " +
        std::to_string(split_info.second.size() + 1) +
        " ranges. New range ids ");
    // Request for new range ids from data store. The new range ids returned by
    // data store are always unique.
    std::vector<std::pair<TxKey::Uptr, int32_t>> new_range_ids;
    for (auto &new_key : split_info.second)
    {
        int32_t new_part_id;
        if (!store_hd_->GetNextRangePartitionId(table_name, &new_part_id))
        {
            LOG(INFO)
                << "Split range failed due to unable to get next partition id.";
            flush_worker_failed_.compare_exchange_strong(fail, true);
            return;
        }
        log_output.append(std::to_string(new_part_id) + ",");
        new_range_ids.emplace_back(std::move(new_key->Clone()), new_part_id);
    }
    log_output.back() = '.';
    LOG(INFO) << log_output;
    // Issue read catalog tx_request to acquire read lock on catalog
    // cc_entry using base table name, and acquire read lock in one
    // shard is good enough to block schema change.
    const TableName base_table_name{table_name.GetBaseTableNameSV(),
                                    TableType::Primary};

    TransactionExecution *split_txm = tx_service_->NewTx();
    InitTxRequest init_req;
    // Set isolation level to RepeatableRead to ensure the readlock will
    // be set during the execution of the following ReadTxRequest.
    init_req.iso_level_ = IsolationLevel::RepeatableRead;
    init_req.protocol_ = CcProtocol::Locking;
    init_req.Reset();
    split_txm->Execute(&init_req);
    init_req.Wait();

    if (init_req.IsError())
    {
        flush_worker_failed_.compare_exchange_strong(fail, true);
        return;
    }

    // If table_name has been dropped at this point, read lock would not
    // be acquired.
    CatalogKey table_key(base_table_name);
    CatalogRecord catalog_rec;

    ReadTxRequest read_req;
    read_req.Reset();
    read_req.Set(
        &catalog_ccm_name, &table_key, &catalog_rec, false, false, true, 0UL);
    split_txm->Execute(&read_req);
    read_req.Wait();

    if (read_req.IsError() || read_req.Result() != RecordStatus::Normal)
    {
        // Use AbortTxRequest to release read lock.
        AbortTxRequest abort_req;
        abort_req.Reset();
        split_txm->Execute(&abort_req);
        abort_req.Wait();
        assert(abort_req.Result() == false);
        LOG(INFO) << "checkpointer add read lock on table failed, "
                     "table name: "
                  << table_key.Name().StringView();
        flush_worker_failed_.compare_exchange_strong(fail, true);
        return;
    }

    catalog_rec.Schema()->StatisticsObject()->PriorSplitRange(table_name,
                                                              node_group);

    // Start the SplitFlush tx. This would split the range, flush the data and
    // update slice metadata.
    TableName range_table_name(table_name.StringView(),
                               TableType::RangePartition);
    const TableRangeEntry *entry = local_shards_.GetTableRangeEntry(
        range_table_name, node_group, split_info.first->RangeStartKey());
    assert(entry != nullptr);
    const TxKey *old_start_key = split_info.first->RangeStartKey();
    if (old_start_key == nullptr)
    {
        old_start_key = local_shards_.catalog_factory_->NegativeInfKey();
    }
    const TxKey *old_end_key = split_info.first->RangeEndKey();
    if (old_end_key == nullptr)
    {
        old_end_key = local_shards_.catalog_factory_->PositiveInfKey();
    }
    SplitFlushTxRequest split_req(table_name,
                                  catalog_rec.Schema(),
                                  node_group,
                                  old_start_key,
                                  old_end_key,
                                  entry->GetRangeInfo(),
                                  std::move(new_range_ids));
    split_txm->Execute(&split_req);
    split_req.Wait();
    if (split_req.IsError() || !split_req.Result())
    {
        LOG(INFO) << "Split range on table " << table_name.StringView()
                  << " partition " << entry->GetRangeInfo()->PartitionId()
                  << " failed.";
        AbortTxRequest abort_req;
        abort_req.Reset();
        split_txm->Execute(&abort_req);
        abort_req.Wait();
        assert(abort_req.Result() == false);
        flush_worker_failed_.compare_exchange_strong(fail, true);
        return;
    }
    CommitTxRequest commit_req;

    commit_req.Reset();
    split_txm->Execute(&commit_req);
    commit_req.Wait();
    LOG(INFO) << "Split range on table " << table_name.StringView()
              << " partition " << split_info.first->PartitionId()
              << " succeeded.";
}

void Checkpointer::Run()
{
    // Starts checkpointer worker threads
    for (int id = 0; id < checkpointer_worker_num_; id++)
    {
        flush_worker_thds_.push_back(
            std::thread([this] { FlushDataWorker(); }));
        update_slice_spec_thds_.push_back(
            std::thread([this] { UpdateSliceSpecWorker(); }));
    }

    std::unique_lock<std::mutex> lk(ckpt_mux_);
    while (ckpt_thd_status_ == Status::Active)
    {
        ckpt_cv_.wait_for(
            lk,
            std::chrono::seconds(checkpoint_interval_),
            [this]
            { return ckpt_thd_status_ != Status::Active || request_ckpt_; });

        CODE_FAULT_INJECTOR("checkpointer_skip_ckpt", { continue; });

        if (ckpt_thd_status_ == Status::Active)
        {
            lk.unlock();
            Ckpt();
            lk.lock();

            request_ckpt_ = false;
        }
    }

    // ensure normal shutdown execute checkpoint since we could receive
    // terminating request during the last checkpoint.
    lk.unlock();
    Ckpt(true);

    {
        std::unique_lock<std::mutex> worker_lk(flush_mux_);
        worker_thd_status_ = Status::Terminated;
    }
    flush_cv_.notify_all();

    // Collect worker threads. They should quit after the Ckpt call.
    for (int id = 0; id < checkpointer_worker_num_; id++)
    {
        flush_worker_thds_.at(id).join();
    }
    {
        std::unique_lock<std::mutex> worker_lk(slice_update_mux_);
        slice_thd_status_ = Status::Terminated;
    }

    slice_update_cv_.notify_all();
    for (int id = 0; id < checkpointer_worker_num_; id++)
    {
        update_slice_spec_thds_.at(id).join();
    }

    lk.lock();
    ckpt_thd_status_ = Status::Terminated;
    ckpt_cv_.notify_all();
}

/**
 * @brief Called by TxProcessor thread to notify checkpointer thread
 * to do checkpoint if there is no freeable entries to be kicked out
 * from ccmap.
 */
void Checkpointer::Notify()
{
    std::unique_lock<std::mutex> lk(ckpt_mux_);
    request_ckpt_ = true;
    ckpt_cv_.notify_one();
}

bool Checkpointer::IsTerminated()
{
    std::scoped_lock<std::mutex> lk(ckpt_mux_);
    return ckpt_thd_status_ == Status::Terminated;
}

void Checkpointer::Terminate()
{
    {
        std::unique_lock<std::mutex> lk(ckpt_mux_);
        assert(ckpt_thd_status_ == Status::Active);
        ckpt_thd_status_ = Status::Terminating;
    }
    ckpt_cv_.notify_one();

    // The checkpoint worker is terminated, when the tx service is
    // going to be shut down. The checkpoint worker flushes one more
    // time unflushed records to the data store, before exiting. The
    // caller of this method, i.e., the destructor of the tx
    // service, is blocked until last flushing finishes.
    std::unique_lock<std::mutex> lk(ckpt_mux_);
    ckpt_cv_.wait(lk,
                  [this] { return ckpt_thd_status_ == Status::Terminated; });
}

void Checkpointer::NotifyLogOfCkptTs(uint32_t node_group,
                                     int64_t term,
                                     uint64_t ckpt_ts)
{
    log_agent_->UpdateCheckpointTs(node_group, term, ckpt_ts);
}

bool Checkpointer::CkptEntryForTest(LruEntry *entry,
                                    std::vector<FlushRecord> &ckpt_vec)
{
    bool ckpt_ret = false;
    CcMap *ccm = entry->parent_map_;
    TableName table_name{ccm->table_name_.StringView(),
                         ccm->table_name_.Type()};
    uint32_t ng = Sharder::Instance().NodeId();
    ckpt_ret = store_hd_->PutAll(
        ckpt_vec, ccm->table_name_, ccm->GetTableSchema(), ng);

    return ckpt_ret;
}

bool Checkpointer::FlushArchiveForTest(LruEntry *entry,
                                       std::vector<FlushRecord> &archives)
{
    bool ckpt_ret = false;
    CcMap *ccm = entry->parent_map_;
    uint32_t ng = Sharder::Instance().NodeId();
    ckpt_ret =
        store_hd_->PutArchivesAll(ng,
                                  ccm->table_name_,
                                  ccm->GetTableSchema()->GetKVCatalogInfo(),
                                  archives);
    return ckpt_ret;
}

bool Checkpointer::UpdateStoreSlice(const TableName &table_name,
                                    const KVCatalogInfo *kv_info,
                                    uint64_t schema_ts,
                                    NodeGroupId node_group_id,
                                    std::vector<FlushRecord> &ckpt_vec,
                                    uint64_t ckpt_ts,
                                    bool flush_res)
{
    bool success = true;
    bool range_updated = false;
    StoreRange *curr_range = nullptr;
    StoreSlice *curr_slice = nullptr;
    bool new_slice = true;

    for (size_t idx = 0; idx < ckpt_vec.size(); ++idx)
    {
        if (new_slice)
        {
            const TxKey *ckpt_key = ckpt_vec[idx].Key();

            if (curr_range == nullptr ||
                curr_range->RangeEndKey() != nullptr &&
                    (*curr_range->RangeEndKey() < *ckpt_key ||
                     *curr_range->RangeEndKey() == *ckpt_key))
            {
                if (curr_range != nullptr && flush_res && range_updated)
                {
                    bool ret = curr_range->UpdateRangeSlicesInStore(
                        table_name, schema_ts, true, store_hd_);
                    success = ret && success;
                }

                // The current ckpt key falls into a new range. Finds the range.
                TableRangeEntry *range_entry = const_cast<TableRangeEntry *>(
                    local_shards_.GetTableRangeEntry(
                        table_name, node_group_id, ckpt_key));
                while (range_entry->GetRangeInfo()->PartitionId() %
                           Sharder::Instance().NodeGroupCount() !=
                       node_group_id)
                {
                    if (idx == ckpt_vec.size() - 1)
                    {
                        return success;
                    }

                    ckpt_key = ckpt_vec[++idx].Key();
                }
                curr_range = range_entry->RangeSlices();
                if (curr_range == nullptr)
                {
                    LOG(ERROR) << "Fail to find the range for the "
                                  "checkpoint key, "
                               << table_name.StringView();
                    return false;
                }
                range_updated = false;
            }

            curr_slice = curr_range->FindSlice(*ckpt_key);
        }

        // Have iterated all flushed data items falling into the
        // current slice. Re-calculates the slice's size.
        if (idx == ckpt_vec.size() - 1 ||
            curr_slice->EndKey() != nullptr &&
                !(*ckpt_vec[idx + 1].Key() < *curr_slice->EndKey()))
        {
            if (flush_res)
            {
                range_updated |= curr_slice->UpdateSize();
            }
            else
            {
                curr_slice->SetPostCkptSize(-1);
            }

            // The next entry falls into a new slice.
            new_slice = true;
        }
    }

    if (flush_res && range_updated)
    {
        bool ret = curr_range->UpdateRangeSlicesInStore(
            table_name, schema_ts, true, store_hd_);
        success = success && ret;
    }
    return success;
}

bool Checkpointer::UpdateSliceAndCalculateRangeUpdate(
    const TableName &tbl_name,
    const KVCatalogInfo *kv_info,
    uint64_t schema_ts,
    NodeGroupId node_group_id,
    std::vector<FlushRecord> &flush_batch,
    uint64_t last_ckpt_ts,
    uint64_t ckpt_ts,
    size_t &batch_idx,
    std::pair<const StoreRange *, std::vector<const TxKey *>> &splitting_info)
{
    std::mutex work_sender_mux;
    std::condition_variable work_sender_cv;
    size_t slice_update_done = 0;
    size_t slice_load_cnt = 0;
    bool fail = false;

    auto lower_bound_cmp = [](const FlushRecord &rec, const TxKey &key)
    { return *rec.Key() < key; };

    while (batch_idx < flush_batch.size())
    {
        const TxKey &range_start_key = *flush_batch[batch_idx].Key();

        // Finds the range.
        TableRangeEntry *range_entry =
            const_cast<TableRangeEntry *>(local_shards_.GetTableRangeEntry(
                tbl_name, node_group_id, &range_start_key));
        if (range_entry == nullptr)
        {
            // Range table not initialized yet. Issue a read request
            // to range table to create it and initialize its range
            // info in local cc shards.
            // We need to release the read lock on range immediately
            // after fetching the range info from data store so that it
            // does not block potential split-flush tx.
            TransactionExecution *txm = tx_service_->NewTx();
            InitTxRequest init_req;
            // Set isolation level to RepeatableRead to ensure the
            // readlock will be set during the execution of the
            // following ReadTxRequest.
            init_req.iso_level_ = IsolationLevel::RepeatableRead;
            init_req.protocol_ = CcProtocol::Locking;
            init_req.Reset();
            txm->Execute(&init_req);
            init_req.Wait();

            if (init_req.IsError())
            {
                AbortTxRequest abort_req;
                abort_req.Reset();
                txm->Execute(&abort_req);
                abort_req.Wait();

                LOG(ERROR) << "Fail to find the range for the checkpoint key, "
                           << tbl_name.StringView();
                return false;
            }
            TableName range_table_name(tbl_name.StringView(),
                                       TableType::RangePartition);
            RangeRecord rec;
            ReadTxRequest read_range_req(
                &range_table_name, &range_start_key, &rec, false, false, true);
            txm->Execute(&read_range_req);
            read_range_req.Wait();
            if (read_range_req.IsError())
            {
                LOG(ERROR) << "Fail to find the range for the checkpoint key, "
                           << tbl_name.StringView();
                return false;
            }

            CommitTxRequest commit_req;
            txm->Execute(&commit_req);
            commit_req.Wait();
            range_entry =
                const_cast<TableRangeEntry *>(local_shards_.GetTableRangeEntry(
                    tbl_name, node_group_id, &range_start_key));
        }

        assert(range_entry != nullptr);
        StoreRange *curr_range = range_entry->RangeSlices();
        if (curr_range == nullptr)
        {
            // Range does not belong to this ng, skips flushing records
            // belonging to this range.
            // TODO: Jumps to the record greater than or equal to the end of the
            // range.
            ++batch_idx;
            continue;
        }

        auto batch_it = flush_batch.begin() + batch_idx;
        auto range_start_it = batch_it;
        auto range_end_it = curr_range->RangeEndKey() == nullptr
                                ? flush_batch.end()
                                : std::lower_bound(batch_it,
                                                   flush_batch.end(),
                                                   *curr_range->RangeEndKey(),
                                                   lower_bound_cmp);

        while (batch_it != range_end_it)
        {
            const TxKey &slice_start_key = *batch_it->Key();
            StoreSlice *curr_slice = curr_range->FindSlice(slice_start_key);

            auto slice_end_it =
                curr_slice->EndKey() == curr_range->RangeEndKey()
                    ? range_end_it
                    : std::lower_bound(batch_it,
                                       range_end_it,
                                       *curr_slice->EndKey(),
                                       lower_bound_cmp);

            int32_t slice_delta_size = 0;
            uint32_t slice_size = 0;

            for (; batch_it != slice_end_it; ++batch_it)
            {
                slice_delta_size += batch_it->delta_size_;
            }

            int32_t sum = curr_slice->Size() + slice_delta_size;
            slice_size = sum >= 0 ? sum : 0;
            curr_slice->SetPostCkptSize(slice_size);
            batch_it = slice_end_it;
        }

        batch_idx = std::distance(flush_batch.begin(), range_end_it);

        size_t post_ckpt_size = curr_range->PostCkptSize();
        if (post_ckpt_size > StoreRange::range_max_size)
        {
            std::vector<const TxKey *> new_range_keys =
                curr_range->CalculateRangeSplitKeys(tbl_name,
                                                    node_group_id,
                                                    ckpt_ts,
                                                    post_ckpt_size,
                                                    range_start_it,
                                                    range_end_it,
                                                    flush_batch);

            // If the range is to be split, passes it to the caller via
            // splitting_info and stops the iteration.
            if (!new_range_keys.empty())
            {
                splitting_info.first = curr_range;
                splitting_info.second = std::move(new_range_keys);
                return true;
            }
        }
        else
        {
            // Range does not need to be splitted, to through the slices and
            // update their specs if necessary.
            auto range_batch_it = range_start_it;
            size_t slice_start_idx =
                std::distance(flush_batch.begin(), range_start_it);
            while (range_batch_it != range_end_it)
            {
                const TxKey &slice_start_key = *range_batch_it->Key();
                StoreSlice *curr_slice = curr_range->FindSlice(slice_start_key);

                auto slice_end_it =
                    curr_slice->EndKey() == curr_range->RangeEndKey()
                        ? range_end_it
                        : std::lower_bound(range_batch_it,
                                           range_end_it,
                                           *curr_slice->EndKey(),
                                           lower_bound_cmp);

                size_t slice_end_idx =
                    std::distance(flush_batch.begin(), slice_end_it);
                int32_t slice_delta_size = 0;
                uint32_t slice_size = 0;

                for (; range_batch_it != slice_end_it; ++range_batch_it)
                {
                    slice_delta_size += range_batch_it->delta_size_;
                }

                int32_t sum = curr_slice->Size() + slice_delta_size;
                slice_size = sum >= 0 ? sum : 0;
                curr_slice->SetPostCkptSize(slice_size);
                if (slice_size > StoreSlice::slice_upper_bound)
                {
                    // Since update slice specs might need loading from
                    // data store, hand it off to the worker and move on
                    // to the next slice.
                    slice_load_cnt++;
                    {
                        std::unique_lock<std::mutex> worker_lk(
                            slice_update_mux_);
                        pending_slice_work_.emplace_back(node_group_id,
                                                         ckpt_ts,
                                                         tbl_name,
                                                         flush_batch,
                                                         curr_range,
                                                         curr_slice,
                                                         slice_start_idx,
                                                         slice_end_idx,
                                                         work_sender_mux,
                                                         work_sender_cv,
                                                         slice_update_done,
                                                         fail);
                        slice_update_cv_.notify_one();
                    }
                }
                range_batch_it = slice_end_it;
                slice_start_idx = slice_end_idx;
            }
        }

        {
            // Wait for all slice specs in this range are updated before moving
            // on to the next range.
            std::unique_lock<std::mutex> work_sender_lk(work_sender_mux);
            work_sender_cv.wait(work_sender_lk,
                                [&slice_update_done, &slice_load_cnt] {
                                    return slice_load_cnt == slice_update_done;
                                });
            if (fail)
            {
                return false;
            }
        }

        slice_load_cnt = 0;
        slice_update_done = 0;
    }

    return true;
}

template <typename T, class Compare>
void Checkpointer::MoveNonSplittingRecords(
    std::vector<T> &flush_vec,
    std::vector<T> &non_split_vec,
    const std::vector<std::pair<const TxKey *, const TxKey *>> &split_ranges,
    Compare lower_bound_cmp)
{
    auto flush_vec_it = flush_vec.begin();

    for (const auto &[start_key, end_key] : split_ranges)
    {
        // The inclusive start of the splitting range is the
        // exclusive end of the gap preceding of the splitting
        // range.
        auto range_start_it = std::lower_bound(
            flush_vec_it, flush_vec.end(), *start_key, lower_bound_cmp);

        size_t copy_size = std::distance(flush_vec_it, range_start_it);
        non_split_vec.reserve(non_split_vec.size() + copy_size);

        // Moves the records in the gap preceding the splitting
        // range.
        std::move(
            flush_vec_it, range_start_it, std::back_inserter(non_split_vec));

        // Jumps to the exclusive end of the splitting range, which
        // is the inclusive start of the gap succeeding the
        // splitting range.
        flush_vec_it = end_key == nullptr ? flush_vec.end()
                                          : std::lower_bound(range_start_it,
                                                             flush_vec.end(),
                                                             *end_key,
                                                             lower_bound_cmp);
    }
    // Moves the records in the gap succeeding the last splitting
    // range.
    size_t copy_size = std::distance(flush_vec_it, flush_vec.end());
    non_split_vec.reserve(non_split_vec.size() + copy_size);
    std::move(flush_vec_it, flush_vec.end(), std::back_inserter(non_split_vec));
}

void Checkpointer::FlushData(const TableName &table_name,
                             const TableSchema *schema,
                             uint64_t node_group,
                             int64_t term,
                             uint64_t ckpt_ts,
                             std::vector<FlushRecord> *ckpt_vec,
                             std::vector<FlushRecord> *archive_vec,
                             std::vector<const TxKey *> *mv_vec,
                             CcHandlerResult<Void> *res)
{
    std::unique_lock<std::mutex> worker_lk(flush_mux_);
    pending_flush_work_.emplace_back(node_group,
                                     term,
                                     ckpt_ts,
                                     table_name,
                                     schema,
                                     ckpt_vec,
                                     archive_vec,
                                     mv_vec,
                                     res);
    flush_cv_.notify_one();
}
}  // namespace txservice
