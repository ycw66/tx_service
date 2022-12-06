#include "checkpointer.h"

#include "range_slice.h"
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
      status_(Status::Active),
      checkpoint_interval_(checkpoint_interval),
      ckpt_delay_time_(ckpt_delay_seconds * 1000000),
      log_agent_(log_agent),
      worker_mux_(),
      worker_cv_()
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

void Checkpointer::Ckpt()
{
    if (local_shards_.Count() == 0 || store_hd_ == nullptr)
    {
        return;
    }

    std::vector<FlushRecord> ckpt_vec;
    ckpt_vec.reserve(10000);
    std::vector<FlushRecord> archive_vec;
    // Cache the entries that exist in "archive_vec_" but not in "ckpt_vec_"
    std::vector<LruEntry *> extra_vec;
    // Cache the entries to move record from "base" table to "archive" table
    std::vector<LruEntry *> mv_base_vec;

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

        // Find minimum ckpt_ts from all the ccshards in parallel. ckpt_ts is
        // the minimum timestamp minus 1 among all the active transactions, thus
        // it's safe to flush all the entries smaller than or equal to ckpt_ts.
        size_t shard_cnt = local_shards_.Count();
        CkptTsCc ckpt_req(shard_cnt, node_group);
        for (auto &ccs : local_shards_.cc_shards_)
        {
            ccs->Enqueue(&ckpt_req);
        }
        ckpt_req.Wait();

        uint64_t ckpt_ts = UINT64_MAX;
        ckpt_ts = ckpt_req.GetCkptTs();

        if (local_shards_.EnableMvcc())
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

        LOG(INFO) << "Begin checkpoint node group #" << node_group
                  << " with timestamp: " << ckpt_ts
                  << ". The ccshard memory usage is: " << ckpt_req.GetMemUsage()
                  << "KB.";

        bool flushed = false;
        worker_flushed_.compare_exchange_strong(flushed, true);

        // Get table names in this node group, checkpointer should be TableName
        // string owner.
        std::unordered_set<TableName> tables =
            local_shards_.GetCatalogTableNamesForCkpt(node_group);

        std::unique_lock<std::mutex> worker_lk(worker_mux_);
        // Iterate all the tables and execute CkptScanCc requests on this node
        // group's ccmaps on each ccshard. The result of CkptScanCc is stored in
        // ckpt_vec.
        for (const auto &table_name : tables)
        {
            if (Sharder::Instance().LeaderTerm(node_group) != leader_term)
            {
                pending_work_.clear();
                break;
            }

            if (table_name.Type() == TableType::Catalog ||
                table_name.Type() == TableType::RangePartition)
            {
                continue;
            }
            pending_work_.push_back(CkptrWorkData{
                node_group, leader_term, ckpt_ts, last_ckpt_ts, table_name});
        }

        if (pending_work_.size() != 0 || active_workers_ != 0)
        {
            worker_cv_.notify_all();
            worker_cv_.wait(
                worker_lk,
                [this]
                { return pending_work_.size() == 0 && active_workers_ == 0; });
        }
        worker_lk.unlock();

        // finish checkpoint on this node group, unpin its data and clear its
        // ccmaps and catalogs if it is no longer leader
        Sharder::Instance().UnpinNodeGroupData(node_group);

        if (worker_flushed_.load(std::memory_order_acquire) &&
            Sharder::Instance().LeaderTerm(node_group) == leader_term)
        {
            Sharder::Instance().UpdateNodeGroupCkptTs(node_group, ckpt_ts);
            NotifyLogOfCkptTs(node_group, leader_term, ckpt_ts);
        }

        LOG(INFO) << "End checkpoint node group #" << node_group
                  << " with timestamp: " << ckpt_ts;
    }
    // notify ccshard ckpt has finished and can re-check freeable ccentries.
    local_shards_.SetWaitingCkpt(false);
}

void Checkpointer::CkptWorker(Checkpointer *ckptr)
{
    std::vector<FlushRecord> ckpt_vec;
    ckpt_vec.reserve(10000);
    std::vector<FlushRecord> archive_vec;
    // Cache the entries to move record from "base" table to "archive" table
    std::vector<LruEntry *> mv_base_vec;

    std::unique_lock<std::mutex> worker_lk(ckptr->worker_mux_);
    ckptr->active_workers_++;
    while (true)
    {
        std::unique_lock<std::mutex> status_lk(ckptr->ckpt_mux_);
        if (ckptr->pending_work_.size() == 0 &&
            ckptr->status_ != Status::Terminated)
        {
            status_lk.unlock();
            ckptr->active_workers_--;
            ckptr->worker_cv_.notify_all();
            ckptr->worker_cv_.wait(
                worker_lk,
                [ckptr]
                {
                    std::unique_lock<std::mutex> status_lk(ckptr->ckpt_mux_);
                    return ckptr->pending_work_.size() != 0 ||
                           ckptr->status_ == Status::Terminated;
                });
            ckptr->active_workers_++;
        }
        else
        {
            status_lk.unlock();
        }

        if (ckptr->pending_work_.size() == 0)
        {
            // Checkpointer is terminating
            ckptr->active_workers_--;
            return;
        }
        // Retrieve first pending work and pop it.
        CkptrWorkData &cur_work = ckptr->pending_work_.front();
        uint32_t node_group = cur_work.node_group_;
        int64_t leader_term = cur_work.term_;
        uint64_t ckpt_ts = cur_work.ckpt_ts_;
        // uint64_t last_ckpt_ts = cur_work.last_ckpt_ts_;
        TableName table_name = cur_work.table_name_;
        ckptr->pending_work_.erase(ckptr->pending_work_.begin());
        worker_lk.unlock();

        bool flushed = true;
        // Issue read catalog tx_request to acquire read lock on catalog
        // cc_entry using base table name, and acquire read lock in one
        // shard is good enough to block schema change.
        const TableName base_table_name_{table_name.GetBaseTableNameSV(),
                                         TableType::Primary};

        TransactionExecution *ckpt_txm = ckptr->tx_service_->NewTx();
        InitTxRequest init_req;
        // Set isolation level to RepeatableRead to ensure the readlock will
        // be set during the execution of the following ReadTxRequest.
        init_req.iso_level_ = IsolationLevel::RepeatableRead;
        init_req.protocol_ = CcProtocol::Locking;
        init_req.Reset();
        ckpt_txm->Execute(&init_req);
        init_req.Wait();

        if (init_req.IsError())
        {
            LOG(INFO) << "init tx failed";
            ckptr->worker_flushed_.compare_exchange_strong(flushed, false);
            worker_lk.lock();
            continue;
        }

        // If table_name has been dropped at this point, read lock would not
        // be acquired.
        CatalogKey table_key(base_table_name_);
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

            ckptr->worker_flushed_.compare_exchange_strong(flushed, false);
            worker_lk.lock();
            continue;
        }

        // Clear the container.
        ckpt_vec.clear();
        archive_vec.clear();
        mv_base_vec.clear();
        CkptScanCc ckpt_scan_cc(
            table_name, ckpt_ts, ckpt_vec, archive_vec, mv_base_vec);

        for (auto &ccs : ckptr->local_shards_.cc_shards_)
        {
            ckpt_scan_cc.Reset(node_group);
            ccs->Enqueue(&ckpt_scan_cc);
            ckpt_scan_cc.Wait();
        }

        // flush to data store if this node group leader term does not
        // change
        if (!(ckpt_vec.empty() && archive_vec.empty() && mv_base_vec.empty()) &&
            Sharder::Instance().LeaderTerm(node_group) == leader_term)
        {
            // Flushes to the data store
            bool ckpt_ret = true;

            if (ckptr->local_shards_.EnableMvcc() && mv_base_vec.size() > 0)
            {
                ckpt_ret = ckptr->store_hd_->CopyBaseToArchive(
                    mv_base_vec, node_group, table_name, catalog_rec.Schema());
                if (!ckpt_ret)
                {
                    LOG(INFO) << "checkpointer CopyBaseToArchive flush to kv "
                                 "storage failed";
                }
            }

            if (ckpt_ret && !ckpt_vec.empty())
            {
                ckpt_ret = ckptr->store_hd_->PutAll(
                    ckpt_vec, table_name, catalog_rec.Schema(), node_group);
                if (!ckpt_ret)
                {
                    LOG(INFO) << "checkpointer PutAll flush to kv "
                                 "storage failed";
                }
            }

            // If flush to data store succeeds, update the ckpt_ts for each
            // entry in ccmap to latest checkpoint version's commit_ts.
            if (ckpt_ret)
            {
                for (auto &ref : ckpt_vec)
                {
                    ref.cce_->ckpt_ts_.store(ref.commit_ts_,
                                             std::memory_order_release);
                }
            }
            else
            {
                ckptr->worker_flushed_.compare_exchange_strong(flushed, false);
            }

            bool flush_undo_ret = true;
            if (ckpt_ret && ckptr->local_shards_.EnableMvcc())
            {
                flush_undo_ret = ckptr->store_hd_->PutArchivesAll(
                    node_group,
                    table_name,
                    catalog_rec.Schema()->GetKVCatalogInfo(),
                    archive_vec);

                if (!flush_undo_ret)
                {
                    // If ckpt succeeds and flushing undo fails, it is safe
                    // to update the local checkpoint timestamp, but not
                    // safe to truncate the redo log.
                    ckptr->worker_flushed_.compare_exchange_strong(flushed,
                                                                   false);
                    LOG(INFO) << "checkpointer PutArchivesAll flush to "
                                 "kv storage failed";
                }
            }

#ifdef RANGE_PARTITIONED
            ckptr->UpdateStoreSlice(table_name,
                                    catalog_rec.Schema()->GetKVCatalogInfo(),
                                    catalog_rec.SchemaTs(),
                                    node_group,
                                    ckpt_vec,
                                    cur_work.last_ckpt_ts_,
                                    ckpt_ts);
#endif
        }

        // Use CommitTxRequest to release read lock.
        CommitTxRequest commit_req;

        commit_req.Reset();
        ckpt_txm->Execute(&commit_req);
        commit_req.Wait();
        worker_lk.lock();
    }
}

void Checkpointer::Run()
{
    using namespace std::chrono_literals;

    // Starts checkpointer worker threads
    for (int id = 0; id < checkpointer_worker_num_; id++)
    {
        worker_thds_.push_back(std::thread(CkptWorker, this));
    }

    std::unique_lock<std::mutex> lk(ckpt_mux_);
    while (status_ == Status::Active)
    {
        if (!request_ckpt_ && status_ == Status::Active)
        {
            ckpt_cv_.wait_for(
                lk,
                std::chrono::seconds(checkpoint_interval_),
                [this] { return status_ != Status::Active || request_ckpt_; });
        }

        CODE_FAULT_INJECTOR("checkpointer_skip_ckpt", {
            LOG(INFO) << "FaultInject  checkpointer_skip_ckpt";
            continue;
        });

        lk.unlock();
        Ckpt();
        lk.lock();

        request_ckpt_ = false;
    }

    // ensure normal shutdown execute checkpoint since we could receive
    // terminating request during the last checkpoint.
    lk.unlock();
    Ckpt();
    lk.lock();
    status_ = Status::Terminated;
    lk.unlock();
    worker_cv_.notify_all();
    // Collect worker threads. They should quit after the Ckpt call.
    for (int id = 0; id < checkpointer_worker_num_; id++)
    {
        worker_thds_.at(id).join();
    }
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
    return status_ == Status::Terminated;
}

void Checkpointer::Terminate()
{
    {
        std::scoped_lock<std::mutex> lk(ckpt_mux_);
        assert(status_ == Status::Active);
        status_ = Status::Terminating;
    }
    ckpt_cv_.notify_one();

    // The checkpoint worker is terminated, when the tx service is
    // going to be shut down. The checkpoint worker flushes one more
    // time unflushed records to the data store, before exiting. The
    // caller of this method, i.e., the destructor of the tx
    // service, is blocked until last flushing finishes.
    std::unique_lock<std::mutex> lk(ckpt_mux_);
    ckpt_cv_.wait(lk, [this] { return status_ == Status::Terminated; });
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
                                    uint64_t last_ckpt_ts,
                                    uint64_t ckpt_ts)
{
    bool success = true;
    bool slice_change = false;
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
                if (curr_range != nullptr)
                {
                    bool ret = store_hd_->UpdateRangeSlices(
                        table_name,
                        kv_info,
                        schema_ts,
                        curr_range->RangeStartKey(),
                        curr_range->Slices(),
                        slice_change);
                    success = ret && success;
                    slice_change = false;
                }

                // The current ckpt key falls into a new range. Finds the range.
                curr_range = local_shards_.FindRange(table_name, ckpt_key);
                if (curr_range == nullptr)
                {
                    LOG(ERROR)
                        << "Fail to find the range for the checkpoint key, "
                        << table_name.StringView();
                    return false;
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
            GetPostCkptSlice post_ckpt_slice(table_name,
                                             node_group_id,
                                             curr_slice,
                                             curr_range,
                                             last_ckpt_ts,
                                             ckpt_ts);

            int32_t slice_delta_size = 0;
            uint32_t slice_size = 0;

            for (size_t pos = slice_first_idx; pos <= idx; ++pos)
            {
                int32_t delta = ckpt_vec[pos].cce_->delta_size_.load(
                    std::memory_order_relaxed);
                if (delta == INT32_MAX)
                {
                    slice_delta_size = INT32_MAX;
                    break;
                }
                slice_delta_size += delta;
            }

            if (slice_delta_size == INT32_MAX)
            {
                local_shards_.EnqueueCcRequest(0, &post_ckpt_slice);
                post_ckpt_slice.Wait();

                if (post_ckpt_slice.IsError())
                {
                    // There is a data store error when loading the slice.
                }

                const auto &item_vec = post_ckpt_slice.SliceRecordCollection();
                for (const auto &item : item_vec)
                {
                    slice_size += item.second;
                }
            }
            else
            {
                int32_t sum = curr_slice->Size();
                sum += slice_delta_size;
                slice_size = sum >= 0 ? sum : 0;
            }

            // If the slice needs to be split, loads the slice from the
            // data store to calculate splitting keys and sub-slices'
            // sizes.
            const auto &item_vec = post_ckpt_slice.SliceRecordCollection();
            if (slice_size > StoreSlice::slice_upper_bound &&
                item_vec.size() > 1)
            {
                if (!post_ckpt_slice.IsFinish())
                {
                    local_shards_.EnqueueCcRequest(0, &post_ckpt_slice);
                    post_ckpt_slice.Wait();
                }

                if (post_ckpt_slice.IsError())
                {
                }

                uint32_t subslice_cnt =
                    slice_size / StoreSlice::slice_upper_bound + 1;
                uint32_t avg_subslice_size = slice_size / subslice_cnt;
                std::vector<std::pair<std::unique_ptr<TxKey>, uint32_t>>
                    splitting_keys;
                splitting_keys.reserve(subslice_cnt);

                uint32_t subslice_size = 0;
                uint32_t subslice_start = 0;
                for (size_t idx = 0; idx < item_vec.size(); ++idx)
                {
                    subslice_size += item_vec[idx].second;

                    if (subslice_size >= avg_subslice_size ||
                        idx == item_vec.size() - 1)
                    {
                        if (splitting_keys.empty())
                        {
                            // The first sub-slice's start key re-uses
                            // the old slice's start key, so there is no
                            // need to allocate a new key.
                            splitting_keys.emplace_back(nullptr, subslice_size);
                        }
                        else
                        {
                            splitting_keys.emplace_back(
                                item_vec[subslice_start].first->Clone(),
                                subslice_size);
                        }
                        subslice_size = 0;
                        subslice_start = idx + 1;
                    }
                }

                curr_range->UpdateSlice(curr_slice, splitting_keys);
                slice_change = true;
            }
            else
            {
                curr_slice->UpdateSize(slice_size);
            }

            // The next entry falls into a new slice.
            slice_first_idx = idx + 1;
        }
    }

    bool ret = store_hd_->UpdateRangeSlices(table_name,
                                            kv_info,
                                            schema_ts,
                                            curr_range->RangeStartKey(),
                                            curr_range->Slices(),
                                            slice_change);
    success = success && ret;

    return success;
}

}  // namespace txservice
