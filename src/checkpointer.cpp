#include "checkpointer.h"

#include "proto/cc_request.pb.h"
#include "proto/statistics.pb.h"
#include "range_slice.h"
#include "remote/cc_stream_sender.h"
#include "sharder.h"
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
      worker_mux_(),
      worker_cv_(),
      worker_thd_status_(Status::Active)
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
                  << " KB"
                  << ". The log usage of node is: " << ckpt_req.GetLogUsage()
                  << "KB.";

        // Reset result bool
        bool fail = true;
        worker_failed_.compare_exchange_strong(fail, false);
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
            std::unique_ptr<std::vector<FlushRecord>> ckpt_vec =
                std::make_unique<std::vector<FlushRecord>>();
            std::unique_ptr<std::vector<FlushRecord>> archive_vec =
                std::make_unique<std::vector<FlushRecord>>();
            std::unique_ptr<std::vector<const TxKey *>> mv_base_vec =
                std::make_unique<std::vector<const TxKey *>>();

            CkptScanTxRequest scan_req(table_name,
                                       ckpt_ts,
                                       node_group,
                                       *ckpt_vec,
                                       *archive_vec,
                                       *mv_base_vec);
            ckpt_txm->Execute(&scan_req);
            scan_req.Wait();

            if (scan_req.IsError())
            {
                LOG(INFO) << "ckpt scan failed on table "
                          << table_name.StringView();
                AbortTxRequest abort_req;
                abort_req.Reset();
                ckpt_txm->Execute(&abort_req);
                abort_req.Wait();
                tables.push_back(std::move(table_name));
                continue;
            }

#ifdef RANGE_PARTITION_ENABLED
            std::vector<
                std::pair<const StoreRange *, std::vector<const TxKey *>>>
                splitting_info;
            if (!UpdateSliceAndCalculateRangeUpdate(
                    table_name,
                    catalog_rec.Schema()->GetKVCatalogInfo(),
                    catalog_rec.SchemaTs(),
                    node_group,
                    *ckpt_vec,
                    last_ckpt_ts,
                    ckpt_ts,
                    splitting_info))
            {
                LOG(INFO) << "Pre-checkpoint slice update failed on table "
                          << table_name.StringView();
                AbortTxRequest abort_req;
                abort_req.Reset();
                ckpt_txm->Execute(&abort_req);
                abort_req.Wait();
                tables.push_back(std::move(table_name));
                continue;
            }

            if (!is_last_ckpt && !splitting_info.empty())
            {
                // Remove splitting ranges from ckpt_vec and archive_vec.
                // Records in the splitting ranges will be flushed by the
                // SplitFlush transaction. We build a new vector here to
                // avoid vecotr.erase() which might take O(n) time.
                std::unique_ptr<std::vector<FlushRecord>> flush_ckpt_vec =
                    std::make_unique<std::vector<FlushRecord>>();
                auto ckpt_it = ckpt_vec->begin();
                for (auto range_it = splitting_info.begin();
                     range_it != splitting_info.end() &&
                     ckpt_it != ckpt_vec->end();
                     range_it++)
                {
                    const StoreRange *range = range_it->first;
                    // Move flush record to new ckpt vector if it is not
                    // in the splitting range.
                    while (ckpt_it != ckpt_vec->end() &&
                           *ckpt_it->Key() < *range->RangeStartKey())
                    {
                        flush_ckpt_vec->push_back(std::move(*ckpt_it));
                        ckpt_it++;
                    }

                    // Leave the flush record in the old ckpt vec if it is
                    // going to be splitted.
                    while (ckpt_it != ckpt_vec->end() &&
                           (range->RangeEndKey() == nullptr ||
                            *ckpt_it->Key() < *range->RangeEndKey()))
                    {
                        ckpt_it++;
                    }
                }
                // Overwrite the old ckpt vec
                ckpt_vec = std::move(flush_ckpt_vec);

                std::unique_ptr<std::vector<FlushRecord>> flush_archive_vec =
                    std::make_unique<std::vector<FlushRecord>>();
                auto archive_it = archive_vec->begin();
                for (auto range_it = splitting_info.begin();
                     range_it != splitting_info.end() &&
                     archive_it != archive_vec->end();
                     range_it++)
                {
                    const StoreRange *range = range_it->first;
                    while (archive_it != archive_vec->end() &&
                           *archive_it->Key() < *range->RangeStartKey())
                    {
                        flush_archive_vec->push_back(std::move(*archive_it));
                        archive_it++;
                    }

                    while (archive_it != archive_vec->end() &&
                           (range->RangeEndKey() == nullptr ||
                            *archive_it->Key() < *range->RangeEndKey()))
                    {
                        archive_it++;
                    }
                }
                archive_vec = std::move(flush_archive_vec);

                for (auto &info : splitting_info)
                {
                    // Init range split tx.
                    range_split_workers.push_back(std::thread(
                        [this, &table_name, info, &node_group]
                        { SplitFlushRange(table_name, node_group, info); }));
                }
            }
#endif

            if (ckpt_vec->size() != 0 || archive_vec->size() != 0 ||
                mv_base_vec->size() != 0)
            {
                std::unique_lock<std::mutex> worker_lk(worker_mux_);
                work_started++;
                pending_work_.emplace_back(node_group,
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
                                           &worker_failed_);
                worker_cv_.notify_one();
            }
            else
            {
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

        if (!worker_failed_.load(std::memory_order_relaxed) &&
            Sharder::Instance().LeaderTerm(node_group) == leader_term)
        {
            LOG(INFO) << "Checkpoint of node group #" << node_group
                      << " succeeded with timestamp: " << ckpt_ts;
            Sharder::Instance().UpdateNodeGroupCkptTs(node_group, ckpt_ts);
            NotifyLogOfCkptTs(node_group, leader_term, ckpt_ts);
        }

        LOG(INFO) << "End checkpoint node group #" << node_group
                  << " with timestamp: " << ckpt_ts;
    }
    // notify ccshard ckpt has finished and can re-check freeable ccentries.
    local_shards_.SetWaitingCkpt(false);
}

void Checkpointer::FlushDataWorker()
{
    std::unique_lock<std::mutex> worker_lk(worker_mux_);
    while (worker_thd_status_ == Status::Active)
    {
        worker_cv_.wait(worker_lk,
                        [this] {
                            return !pending_work_.empty() ||
                                   worker_thd_status_ == Status::Terminated;
                        });

        if (pending_work_.empty())
        {
            continue;
        }

        // Retrieve first pending work and pop it.
        FlushDataWork &cur_work = pending_work_.back();
#ifdef RANGE_PARTITION_ENABLED
        uint64_t ckpt_ts = cur_work.ckpt_ts_;
#endif
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

        pending_work_.pop_back();
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
                    ref.cce_->ckpt_ts_.store(ref.commit_ts_,
                                             std::memory_order_release);
                    ref.cce_->data_store_size_.fetch_add(ref.delta_size_);
                }
#ifdef RANGE_PARTITION_ENABLED
                // Update the slice size in data store.
                if (ckpt_vec->size())
                {
                    UpdateStoreSlice(table_name,
                                     schema->GetKVCatalogInfo(),
                                     schema->Version(),
                                     node_group,
                                     *ckpt_vec,
                                     ckpt_ts);
                }
#ifdef STATISTICS_ENABLED
                // Flush statistics based on primary table.
                if (table_name.Type() == TableType::Primary)
                {
                    uint32_t shard_code = Sharder::Instance().ShardCode(
                        std::hash<TableName>{}(table_name));
                    uint32_t shard_id = shard_code >> 10;
                    if (shard_id == node_group)
                    {
                        SyncStatistics(this,
                                       table_name,
                                       shard_code,
                                       schema,
                                       schema->Version());
                    }
                }
#endif
#endif
            }
            else
            {
                succ = false;
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
            worker_failed_.compare_exchange_strong(fail, true);
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
        worker_failed_.compare_exchange_strong(fail, true);
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
        worker_failed_.compare_exchange_strong(fail, true);
        return;
    }

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
                  << " partition " << entry->GetRangeInfo()->partition_id_
                  << " failed.";
        AbortTxRequest abort_req;
        abort_req.Reset();
        split_txm->Execute(&abort_req);
        abort_req.Wait();
        assert(abort_req.Result() == false);
        worker_failed_.compare_exchange_strong(fail, true);
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
        worker_thds_.push_back(std::thread([this] { FlushDataWorker(); }));
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
        std::unique_lock<std::mutex> worker_lk(worker_mux_);
        worker_thd_status_ = Status::Terminated;
    }
    worker_cv_.notify_all();

    // Collect worker threads. They should quit after the Ckpt call.
    for (int id = 0; id < checkpointer_worker_num_; id++)
    {
        worker_thds_.at(id).join();
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
                                    uint64_t ckpt_ts)
{
    bool success = true;
    bool range_updated = false;
    StoreRange *curr_range = nullptr;
    StoreSlice *curr_slice = nullptr;
    size_t slice_first_idx = 0;

    for (size_t idx = 0; idx < ckpt_vec.size(); ++idx)
    {
        if (slice_first_idx == idx)
        {
            const TxKey &ckpt_key = *ckpt_vec[idx].Key();

            if (curr_range == nullptr ||
                curr_range->RangeEndKey() != nullptr &&
                    (*curr_range->RangeEndKey() < ckpt_key ||
                     *curr_range->RangeEndKey() == ckpt_key))
            {
                if (curr_range != nullptr && range_updated)
                {
                    bool ret = curr_range->UpdateRangeSlicesInStore(
                        table_name, schema_ts, true, store_hd_);
                    success = ret && success;
                }

                // The current ckpt key falls into a new range. Finds the range.
                curr_range = local_shards_.FindRange(
                    table_name, node_group_id, ckpt_key);
                if (curr_range == nullptr)
                {
                    LOG(ERROR) << "Fail to find the range for the "
                                  "checkpoint key, "
                               << table_name.StringView();
                    return false;
                }
                range_updated = false;
            }

            curr_slice = curr_range->FindSlice(ckpt_key);
        }

        // Have iterated all flushed data items falling into the
        // current slice. Re-calculates the slice's size.
        if (idx == ckpt_vec.size() - 1 ||
            curr_slice->EndKey() != nullptr &&
                !(*ckpt_vec[idx + 1].Key() < *curr_slice->EndKey()))
        {
            int32_t slice_delta_size = 0;

            for (size_t pos = slice_first_idx; pos <= idx; ++pos)
            {
                slice_delta_size += ckpt_vec.at(pos).delta_size_;
            }

            if (slice_delta_size)
            {
                curr_slice->UpdateSize(curr_slice->Size() + slice_delta_size);
                range_updated = true;
            }

            // The next entry falls into a new slice.
            slice_first_idx = idx + 1;
        }
    }

    if (range_updated)
    {
        bool ret = curr_range->UpdateRangeSlicesInStore(
            table_name, schema_ts, true, store_hd_);
        success = success && ret;
    }
    return success;
}

bool Checkpointer::UpdateSliceAndCalculateRangeUpdate(
    const TableName &table_name,
    const KVCatalogInfo *kv_info,
    uint64_t schema_ts,
    NodeGroupId node_group_id,
    std::vector<FlushRecord> &ckpt_vec,
    uint64_t last_ckpt_ts,
    uint64_t ckpt_ts,
    std::vector<std::pair<const StoreRange *, std::vector<const TxKey *>>>
        &splitting_info)
{
    bool success = true;
    StoreRange *curr_range = nullptr;
    StoreSlice *curr_slice = nullptr;
    uint64_t curr_range_size = 0;
    std::vector<uint32_t> post_ckpt_slice_sizes;
    size_t slice_first_idx = 0;

    for (size_t idx = 0; idx < ckpt_vec.size(); ++idx)
    {
        if (slice_first_idx == idx)
        {
            const TxKey &ckpt_key = *ckpt_vec[idx].Key();

            if (curr_range == nullptr ||
                curr_range->RangeEndKey() != nullptr &&
                    (*curr_range->RangeEndKey() < ckpt_key ||
                     *curr_range->RangeEndKey() == ckpt_key))
            {
                // The current ckpt key falls into a new range. Finds
                // the range.
                curr_range = local_shards_.FindRange(
                    table_name, node_group_id, ckpt_key);
                curr_range_size = 0;
                post_ckpt_slice_sizes.clear();
                if (curr_range == nullptr)
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

                        LOG(ERROR) << "Fail to find the range for the "
                                      "checkpoint key, "
                                   << table_name.StringView();
                        return false;
                    }
                    TableName range_table_name(table_name.StringView(),
                                               TableType::RangePartition);
                    RangeRecord rec;
                    ReadTxRequest read_range_req(
                        &range_table_name, &ckpt_key, &rec, false, false, true);
                    txm->Execute(&read_range_req);
                    read_range_req.Wait();
                    if (read_range_req.IsError())
                    {
                        LOG(ERROR) << "Fail to find the range for the "
                                      "checkpoint key, "
                                   << table_name.StringView();
                        return false;
                    }
                    CommitTxRequest commit_req;

                    commit_req.Reset();
                    txm->Execute(&commit_req);
                    commit_req.Wait();
                    curr_range = local_shards_.FindRange(
                        table_name, node_group_id, ckpt_key);
                }
            }
            curr_slice = curr_range->FindSlice(ckpt_key);
        }

        // Have iterated all flushed data items falling into the
        // current slice. Re-calculates the slice's size.
        if (idx == ckpt_vec.size() - 1 ||
            curr_slice->EndKey() != nullptr &&
                !(*ckpt_vec[idx + 1].Key() < *curr_slice->EndKey()))
        {
            int32_t slice_delta_size = 0;
            uint32_t slice_size = 0;

            for (size_t pos = slice_first_idx; pos <= idx; ++pos)
            {
                slice_delta_size += ckpt_vec[pos].delta_size_;
            }

            int32_t sum = curr_slice->Size() + slice_delta_size;
            slice_size = sum >= 0 ? sum : 0;
            curr_range_size += slice_size;

            // If the slice needs to be split, calculate splitting keys and
            // sub-slices' sizes.
            if (slice_size > StoreSlice::slice_upper_bound)
            {
                GetPostCkptSlice post_ckpt_slice(table_name,
                                                 node_group_id,
                                                 curr_slice,
                                                 curr_range,
                                                 ckpt_vec,
                                                 slice_first_idx,
                                                 idx + 1,
                                                 last_ckpt_ts,
                                                 ckpt_ts);
                local_shards_.EnqueueCcRequest(0, &post_ckpt_slice);
                post_ckpt_slice.Wait();

                if (post_ckpt_slice.IsError())
                {
                    // There is a data store error when loading the slice.
                }

                const auto &item_vec = post_ckpt_slice.SliceRecordCollection();
                // Split the slice based on post checkpoint item size, but do
                // not update the slice size with the post checkpoint size yet
                // since the data is still not flushed into data store yet.
                uint32_t subslice_cnt =
                    slice_size / StoreSlice::slice_upper_bound + 1;
                uint32_t avg_subslice_size = slice_size / subslice_cnt;
                std::vector<std::pair<std::unique_ptr<TxKey>, uint32_t>>
                    splitting_keys;
                splitting_keys.reserve(subslice_cnt);

                uint32_t post_ckpt_subslice_size = 0;
                uint32_t curr_subslice_size = 0;
                uint32_t subslice_start = 0;
                for (size_t pos = 0; pos < item_vec.size(); ++pos)
                {
                    post_ckpt_subslice_size += std::get<2>(item_vec[pos]);
                    curr_subslice_size += std::get<1>(item_vec[pos]);

                    if (post_ckpt_subslice_size >= avg_subslice_size ||
                        pos == item_vec.size() - 1)
                    {
                        if (splitting_keys.empty())
                        {
                            // The first sub-slice's start key re-uses
                            // the old slice's start key, so there is no
                            // need to allocate a new key.
                            splitting_keys.emplace_back(nullptr,
                                                        curr_subslice_size);
                        }
                        else
                        {
                            splitting_keys.emplace_back(
                                std::get<0>(item_vec[subslice_start])->Clone(),
                                curr_subslice_size);
                        }
                        post_ckpt_slice_sizes.push_back(
                            post_ckpt_subslice_size);
                        post_ckpt_subslice_size = 0;
                        curr_subslice_size = 0;
                        subslice_start = pos + 1;
                    }
                }
                // Split StoreSlice in memory. Slice info in KV store
                // will be updated after checkpoint.
                if (splitting_keys.size() > 1)
                {
                    curr_range->UpdateSlice(curr_slice, splitting_keys);
                }
            }
            else
            {
                post_ckpt_slice_sizes.push_back(slice_size);
            }

            // At the end of current range
            if (idx == ckpt_vec.size() - 1 ||
                curr_range->RangeEndKey() != nullptr &&
                    (*curr_range->RangeEndKey() < *ckpt_vec[idx + 1].Key() ||
                     *curr_range->RangeEndKey() == *ckpt_vec[idx + 1].Key()))
            {
                if (curr_range->NeedSplit(curr_range_size))
                {
                    auto &slices = curr_range->Slices();
                    std::vector<const TxKey *> new_range_keys;
                    uint32_t slice_idx = 0;
                    bool first_subrange = true;
                    while (slice_idx < slices.size())
                    {
                        for (uint32_t curr_subrange_size = 0;
                             curr_subrange_size < StoreRange::range_max_size &&
                             slice_idx < slices.size();
                             slice_idx++)
                        {
                            curr_subrange_size +=
                                post_ckpt_slice_sizes[slice_idx];
                        }
                        // Skip the first subrange since it will reuse the
                        // current range entry
                        if (first_subrange)
                        {
                            first_subrange = false;
                        }
                        else if (slice_idx < slices.size())
                        {
                            new_range_keys.emplace_back(
                                slices[slice_idx]->StartKey());
                        }
                    }

                    // Pass todo splitting ranges to caller through
                    // splitting_info
                    if (new_range_keys.size())
                    {
                        splitting_info.emplace_back(curr_range,
                                                    std::move(new_range_keys));
                    }
                }
            }

            // The next entry falls into a new slice.
            slice_first_idx = idx + 1;
        }
    }

    return success;
}

void Checkpointer::SyncStatistics(Checkpointer *ckptr,
                                  const TableName &table_name,
                                  uint32_t table_shard_code,
                                  const TableSchema *table_schema,
                                  uint64_t table_schema_ts)
{
    store::Statistics store_statistics;

    const Statistics *statistics = table_schema->StatisticsObject();

    CkptStatisticsCc ckpt_stat_cc(statistics, &store_statistics);
    ckptr->local_shards_.EnqueueCcRequest(table_shard_code, &ckpt_stat_cc);
    ckpt_stat_cc.Wait();

    std::string statistics_binary = store_statistics.SerializeAsString();
    bool ok = ckptr->store_hd_->UpsertTableStatistics(
        table_name, statistics_binary, table_schema_ts);
    if (ok)
    {
        remote::CcStreamSender *stream_sender =
            Sharder::Instance().GetCcStreamSender();

        uint32_t src_node_id = Sharder::Instance().NodeId();
        uint32_t ng_cnt = Sharder::Instance().NodeGroupCount();
        for (uint32_t ng_id = 0; ng_id < ng_cnt; ng_id++)
        {
            if (ng_id != src_node_id)
            {
                remote::CcMessage send_msg;
                send_msg.set_type(
                    remote::CcMessage::MessageType::
                        CcMessage_MessageType_BroadcastStatisticsRequest);

                remote::BroadcastStatisticsRequest *broadcast_stat_req =
                    send_msg.mutable_broadcast_statistics_req();
                broadcast_stat_req->set_src_node_id(src_node_id);
                broadcast_stat_req->set_node_group_id(ng_id);
                broadcast_stat_req->set_table_name_str(table_name.String());
                broadcast_stat_req->set_statistics_binary(statistics_binary);

                stream_sender->SendMessageToNg(ng_id, send_msg);
            }
        }
    }
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
    std::unique_lock<std::mutex> worker_lk(worker_mux_);
    pending_work_.emplace_back(node_group,
                               term,
                               ckpt_ts,
                               table_name,
                               schema,
                               ckpt_vec,
                               archive_vec,
                               mv_vec,
                               res);
    worker_cv_.notify_one();
}
}  // namespace txservice
