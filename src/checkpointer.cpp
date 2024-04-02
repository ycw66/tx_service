#include "checkpointer.h"

#include <cstdint>

#include "catalog_key_record.h"
#include "cc_request.h"
#include "range_slice.h"
#include "sharder.h"
#include "statistics.h"
#include "tx_start_ts_collector.h"

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
      log_agent_(log_agent)
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

std::pair<uint64_t, uint64_t> Checkpointer::GetNewCheckpointTs(
    uint32_t node_group_id, bool is_last_ckpt)
{
    size_t core_cnt = local_shards_.Count();
    CkptTsCc ckpt_req(core_cnt, node_group_id);

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

    return {ckpt_ts, ckpt_req.GetMemUsage()};
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

        auto [ckpt_ts, mem_usage] =
            GetNewCheckpointTs(node_group, is_last_ckpt);
        uint64_t last_ckpt_ts =
            Sharder::Instance().GetNodeGroupCkptTs(node_group);

        if (ckpt_ts <= last_ckpt_ts)
        {
            // skip checkpoint for this node group
            Sharder::Instance().UnpinNodeGroupData(node_group);
            continue;
        }

        LOG(INFO) << "Begin checkpoint with timestamp: " << ckpt_ts
                  << ". The memory usage of node is: " << mem_usage << " KB.";

        // Get table names in this node group, checkpointer should be TableName
        // string owner.
        std::unordered_map<TableName, bool> tables =
            local_shards_.GetCatalogTableNameSnapshot(node_group, ckpt_ts);

        std::shared_ptr<DataSyncStatus> status =
            std::make_shared<DataSyncStatus>(true);

        uint64_t last_succ_ckpt_ts = UINT64_MAX;
        bool can_be_skipped = (is_last_ckpt == false);

        // Iterate all the tables and execute CkptScanCc requests on this node
        // group's ccmaps on each ccshard. The result of CkptScanCc is stored in
        // ckpt_vec.
        for (auto it = tables.begin(); it != tables.end(); ++it)
        {
            if (Sharder::Instance().LeaderTerm(node_group) != leader_term)
            {
                break;
            }

            const TableName &table_name = it->first;
            bool is_dirty = it->second;
            // This should correspond to CcShard::ActiveTxMinTs.
            if (!table_name.IsMeta())
            {
                if (!is_dirty)
                {
                    // Skip the table if it's not updated since last sync ts.
                    GetTableLastCommitTsCc get_commit_ts_cc(
                        table_name, node_group, local_shards_.Count());
                    for (size_t core = 0; core < local_shards_.Count(); core++)
                    {
                        local_shards_.EnqueueCcRequest(core, &get_commit_ts_cc);
                    }
                    get_commit_ts_cc.Wait();

                    if (get_commit_ts_cc.LastCommitTs() < last_ckpt_ts)
                    {
                        continue;
                    }
                }

                uint64_t table_last_synced_ts = 0;
                local_shards_.EnqueueDataSyncTaskForTable(table_name,
                                                          node_group,
                                                          leader_term,
                                                          ckpt_ts,
                                                          table_last_synced_ts,
                                                          is_dirty,
                                                          can_be_skipped,
                                                          status);

                // Maybe we couldn't truncate log in this round of checkpoint.
                // Since some of the data sync tasks might be skipped due to
                // another task in queue. So we have no way of knowing if
                // the table or range was successfully flushed into storage in
                // this round of checkpoint. Check the smallest valid synced ts
                // of all tables and use it to truncate log.
                if (table_last_synced_ts > 0)
                {
                    last_succ_ckpt_ts =
                        std::min(last_succ_ckpt_ts, table_last_synced_ts);
                }
            }
        }

        if (Sharder::Instance().LeaderTerm(node_group) != leader_term)
        {
            // Skip the node groups that are no longer on this node.
            Sharder::Instance().UnpinNodeGroupData(node_group);
            continue;
        }

        if (last_succ_ckpt_ts != UINT64_MAX && last_succ_ckpt_ts > last_ckpt_ts)
        {
            assert(last_succ_ckpt_ts != 0);
            LOG(INFO) << "Checkpoint of node group #" << node_group
                      << " succeeded with timestamp: " << last_succ_ckpt_ts;
            Sharder::Instance().UpdateNodeGroupCkptTs(node_group,
                                                      last_succ_ckpt_ts);
            NotifyLogOfCkptTs(node_group, leader_term, last_succ_ckpt_ts);
        }

        {
            std::unique_lock<std::mutex> task_sender_lk(status->mux_);
            status->all_task_started_ = true;
            if (is_last_ckpt)
            {
                // Wait for all tasks to be done if this is last checkpoint
                // before graceful shutdown.
                status->cv_.wait(task_sender_lk,
                                 [&status]
                                 { return status->unfinished_tasks_ == 0; });
            }
            if (status->need_truncate_log_ && status->unfinished_tasks_ == 0 &&
                status->err_code_ == CcErrorCode::NO_ERROR)
            {
                // Truncate redo log
                LOG(INFO) << "Checkpoint of node group #" << node_group
                          << " succeeded with timestamp: "
                          << (status->truncate_log_ts_ == 0
                                  ? ckpt_ts
                                  : status->truncate_log_ts_);

                // Note: `status->truncate_log_ts_ may larger than `ckpt_ts`. So
                // we use `status->truncate_log_ts_` to truncate log.
                if (status->truncate_log_ts_ > last_ckpt_ts)
                {
                    assert(status->truncate_log_ts_ >= ckpt_ts);
                    Sharder::Instance().UpdateNodeGroupCkptTs(
                        node_group, status->truncate_log_ts_);
                    NotifyLogOfCkptTs(
                        node_group, leader_term, status->truncate_log_ts_);
                }
            }
        }

        // finish checkpoint on this node group, unpin its data and clear its
        // ccmaps and catalogs if it is no longer leader
        Sharder::Instance().UnpinNodeGroupData(node_group);
    }
}

void Checkpointer::Run()
{
    std::unique_lock<std::mutex> lk(ckpt_mux_);
    last_checkpoint_ts_ = std::chrono::high_resolution_clock::now();
    while (ckpt_thd_status_ == Status::Active)
    {
        while (!ckpt_cv_.wait_for(
            lk,
            std::chrono::seconds(checkpoint_interval_),
            [this]
            {
                if (ckpt_thd_status_ != Status::Active)
                {
                    return true;
                }

                // Either cc shards are full and have requested a checkpoint, or
                // we've sleeped for at least checkpoint_interval_ seconds.
                // Only enqueue new checkpoint task if there's idle worker.
                return (request_ckpt_ ||
                        std::chrono::high_resolution_clock::now() >=
                            last_checkpoint_ts_ +
                                std::chrono::seconds(checkpoint_interval_));
            }))
        {
            // go back to sleep if there's no idle worker.
        }

        CODE_FAULT_INJECTOR("checkpointer_skip_ckpt", {
            request_ckpt_ = false;
            last_checkpoint_ts_ = std::chrono::high_resolution_clock::now();
            continue;
        });
        if (ckpt_thd_status_ != Status::Active)
        {
            break;
        }

        last_checkpoint_ts_ = std::chrono::high_resolution_clock::now();
        lk.unlock();
        Ckpt();
        lk.lock();
        request_ckpt_ = false;
    }

    // ensure normal shutdown execute checkpoint since we could receive
    // terminating request during the last checkpoint.
    lk.unlock();
    Ckpt(true);

    lk.lock();
    ckpt_thd_status_ = Status::Terminated;
}

/**
 * @brief Called by TxProcessor thread to notify checkpointer thread
 * to do checkpoint if there is no freeable entries to be kicked out
 * from ccmap.
 */
void Checkpointer::Notify(bool request_ckpt)
{
    if (request_ckpt)
    {
        std::unique_lock<std::mutex> lk(ckpt_mux_);
        request_ckpt_ = true;
    }
    ckpt_cv_.notify_one();
}

bool Checkpointer::IsTerminated()
{
    std::scoped_lock<std::mutex> lk(ckpt_mux_);
    return ckpt_thd_status_ == Status::Terminated;
}

void Checkpointer::Terminate()
{
    std::unique_lock<std::mutex> lk(ckpt_mux_);
    assert(ckpt_thd_status_ == Status::Active);
    ckpt_thd_status_ = Status::Terminating;
    ckpt_cv_.notify_one();
}

void Checkpointer::Join()
{
    // The checkpoint worker is terminated, when the tx service is
    // going to be shut down. The checkpoint worker flushes one more
    // time unflushed records to the data store, before exiting. The
    // caller of this method, i.e., the destructor of the tx
    // service, is blocked until last flushing finishes.
    thd_.join();
}

void Checkpointer::NotifyLogOfCkptTs(uint32_t node_group,
                                     int64_t term,
                                     uint64_t ckpt_ts)
{
    log_agent_->UpdateCheckpointTs(node_group, term, ckpt_ts);
}

bool Checkpointer::CkptEntryForTest(const TableName &tbl_name,
                                    const TableSchema *tbl_schema,
                                    std::vector<FlushRecord> &ckpt_vec)
{
    bool ckpt_ret = false;
    uint32_t ng = Sharder::Instance().NodeId();
    ckpt_ret = store_hd_->PutAll(ckpt_vec, tbl_name, tbl_schema, ng);

    return ckpt_ret;
}

bool Checkpointer::FlushArchiveForTest(const TableName &tbl_name,
                                       const TableSchema *tbl_schema,
                                       std::vector<FlushRecord> &archives)
{
    bool ckpt_ret = false;
    uint32_t ng = Sharder::Instance().NodeId();
    ckpt_ret = store_hd_->PutArchivesAll(
        ng, tbl_name, tbl_schema->GetKVCatalogInfo(), archives);
    return ckpt_ret;
}
}  // namespace txservice
