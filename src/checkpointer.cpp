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

        // Get table names in this node group, checkpointer should be TableName
        // string owner.
        std::vector<TableName> tables =
            local_shards_.GetCatalogTableNamesForCkpt(node_group);

        std::mutex task_sender_mux;
        std::condition_variable task_sender_cv;
        uint16_t finished_task_cnt = 0;
        uint16_t task_started = 0;

        // Reset result bool
        std::atomic_bool tasks_failed{false};

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
                table_name.Type() == TableType::RangePartition ||
                table_name.Type() == TableType::RangeBucket)
            {
                continue;
            }

            local_shards_.EnqueueDataSyncTask(table_name,
                                              node_group,
                                              leader_term,
                                              ckpt_ts,
                                              &task_sender_mux,
                                              &task_sender_cv,
                                              &finished_task_cnt,
                                              &tasks_failed);
            ++task_started;
        }
        if (Sharder::Instance().LeaderTerm(node_group) != leader_term)
        {
            // Skip the node groups that are no longer on this node.
            Sharder::Instance().UnpinNodeGroupData(node_group);
            continue;
        }

        {
            std::unique_lock<std::mutex> task_sender_lk(task_sender_mux);
            task_sender_cv.wait(task_sender_lk,
                                [&finished_task_cnt, &task_started]
                                { return finished_task_cnt == task_started; });
        }

        // finish checkpoint on this node group, unpin its data and clear its
        // ccmaps and catalogs if it is no longer leader
        Sharder::Instance().UnpinNodeGroupData(node_group);

        if (!tasks_failed.load(std::memory_order_relaxed) &&
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

void Checkpointer::Run()
{
    std::unique_lock<std::mutex> lk(ckpt_mux_);
    while (ckpt_thd_status_ == Status::Active)
    {
        ckpt_cv_.wait_for(
            lk,
            std::chrono::seconds(checkpoint_interval_),
            [this]
            { return ckpt_thd_status_ != Status::Active || request_ckpt_; });

        CODE_FAULT_INJECTOR("checkpointer_skip_ckpt", {
            request_ckpt_ = false;
            continue;
        });

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
    std::unordered_set<uint32_t> skipped_record;
    ckpt_ret = store_hd_->PutAll(
        ckpt_vec, ccm->table_name_, ccm->GetTableSchema(), ng, skipped_record);

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
}  // namespace txservice