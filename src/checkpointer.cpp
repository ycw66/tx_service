#include "checkpointer.h"

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
      last_ckpt_ts_(0),
      mux_(),
      cv_(),
      request_ckpt_(false),
      store_hd_(write_hd),
      status_(Status::Active),
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
}

Checkpointer::~Checkpointer()
{
    /*std::unique_lock<std::mutex> lk(mux_);
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

    size_t shard_cnt = local_shards_.Count();
    CkptTsCc ckpt_req(shard_cnt);

    // Find minimum ckpt_ts from all the ccshards in parallel. ckpt_ts is the
    // minimum timestamp minus 1 among all the active transactions, thus it's
    // safe to flush all the entries smaller than or equal to ckpt_ts.
    for (auto &ccs : local_shards_.cc_shards_)
    {
        ccs->Enqueue(&ckpt_req);
    }
    ckpt_req.Wait();

    uint64_t ckpt_ts = UINT64_MAX;
    ckpt_ts = ckpt_req.GetCkptTs();

    if (local_shards_.EnableMvcc())
    {
        archive_vec.reserve(10000);
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

    LOG(INFO) << "Begin checkpoint with timestamp: " << ckpt_ts
              << ". The memory usage of node is: " << ckpt_req.GetMemUsage()
              << " KB"
              << ". The log usage of node is: " << ckpt_req.GetLogUsage()
              << "KB.";

    const CcShard &shard = *local_shards_.cc_shards_[0];

    std::vector<uint32_t> node_groups = Sharder::Instance().LocalNodeGroups();
    for (uint32_t node_group : node_groups)
    {
        assert(ckpt_ts >= last_ckpt_ts_[node_group]);
        uint64_t last_ckpt_ts = 0;
        auto ite = last_ckpt_ts_.find(node_group);
        if (ite != last_ckpt_ts_.end())
        {
            last_ckpt_ts = ite->second;
        }
        if (ckpt_ts == last_ckpt_ts)
        {
            continue;
        }
        // check whether this node is group leader, pin its data if it is
        int64_t leader_term =
            Sharder::Instance().TryPinNodeGroupData(node_group);
        if (leader_term < 0)
        {
            continue;
        }
        bool flushed = true;

        // Get table names in this node group, checkpointer should be TableName
        // string owner.
        std::unordered_set<TableName> tables =
            local_shards_.GetCatalogTableNamesForCkpt(node_group);

        // Iterate all the tables and execute CkptScanCc requests on this node
        // group's ccmaps on each ccshard. The result of CkptScanCc is stored in
        // ckpt_vec.
        for (const auto &table_name : tables)
        {
            if (Sharder::Instance().LeaderTerm(node_group) != leader_term)
            {
                flushed = false;
                break;
            }

            if (table_name.Type() == TableType::Catalog ||
                table_name.Type() == TableType::RangePartition)
            {
                continue;
            }

            // Issue read catalog tx_request to acquire read lock on catalog
            // cc_entry using base table name, and acquire read lock in one
            // shard is good enough to block schema change.
            const TableName base_table_name_{table_name.GetBaseTableNameSV(),
                                             TableType::Primary};

            TransactionExecution *ckpt_txm = tx_service_->NewTx();
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
                flushed = false;
                break;
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

                flushed = false;
                continue;
            }

            // Clear the container.
            ckpt_vec.clear();
            archive_vec.clear();
            extra_vec.clear();
            mv_base_vec.clear();
            CkptScanCc ckpt_scan_cc(table_name,
                                    ckpt_ts,
                                    ckpt_vec,
                                    archive_vec,
                                    extra_vec,
                                    mv_base_vec);

            for (auto &ccs : local_shards_.cc_shards_)
            {
                ckpt_scan_cc.Reset(node_group);
                ccs->Enqueue(&ckpt_scan_cc);
                ckpt_scan_cc.Wait();
            }

            // flush to data store if this node group leader term does not
            // change
            if (!(ckpt_vec.empty() && archive_vec.empty() &&
                  mv_base_vec.empty()) &&
                Sharder::Instance().LeaderTerm(node_group) == leader_term)
            {
                // Flushes to the data store
                bool ckpt_ret = true;

                CcMap *ccm;
                auto iter = shard.native_ccms_.find(table_name);
                if (iter == shard.native_ccms_.end())
                {
                    // ccm.table_schema_ and ccm.schema_ts_ should be the same
                    // if table_name exists in native_ccms_ as well as
                    // failover_ccms_. Or refactor this part to use
                    // LeaderTerm(node_group) to differentiate findings in
                    // different places.
                    auto it = shard.failover_ccms_.find(table_name);
                    assert(it != shard.failover_ccms_.end());
                    ccm = it->second.begin()->second.get();
                }
                else
                {
                    ccm = iter->second.get();
                }

                bool mv_base_ret = true;
                if (local_shards_.EnableMvcc() && mv_base_vec.size() > 0)
                {
                    mv_base_ret =
                        store_hd_->CopyBaseToArchive(mv_base_vec,
                                                     node_group,
                                                     table_name,
                                                     ccm->GetTableSchema());
                    if (!mv_base_ret)
                    {
                        LOG(INFO)
                            << "checkpointer CopyBaseToArchive flush to kv "
                               "storage failed";
                    }
                }

                if (mv_base_ret && !ckpt_vec.empty())
                {
                    ckpt_ret = store_hd_->PutAll(ckpt_vec,
                                                 table_name,
                                                 ccm->GetTableSchema(),
                                                 node_group);
                    if (!ckpt_ret)
                    {
                        LOG(INFO) << "checkpointer PutAll flush to kv "
                                     "storage failed";
                    }
                }

                if (ckpt_ret && local_shards_.EnableMvcc())
                {
                    bool flush_undo_ret = store_hd_->PutArchivesAll(
                        node_group,
                        table_name,
                        ccm->GetTableSchema()->GetKVCatalogInfo(),
                        archive_vec);

                    if (flush_undo_ret)
                    {
                        for (auto &ref : extra_vec)
                        {
                            ref->ckpt_ts_.store(ckpt_ts,
                                                std::memory_order_release);
                        }
                    }
                    else
                    {
                        // If ckpt succeeds and flushing undo fails, it is safe
                        // to update the local checkpoint timestamp, but not
                        // safe to truncate the redo log.
                        flushed = false;
                        LOG(INFO) << "checkpointer PutArchivesAll flush to "
                                     "kv storage failed";
                    }
                }

                // If flush to data store succeeds, update the ckpt_ts for each
                // entry in ccmap.
                if (ckpt_ret)
                {
                    for (auto &ref : ckpt_vec)
                    {
                        ref.cce_->ckpt_ts_.store(ckpt_ts,
                                                 std::memory_order_release);
                    }
                }
                else
                {
                    flushed = false;
                }
            }

            // Use CommitTxRequest to release read lock.
            CommitTxRequest commit_req;

            commit_req.Reset();
            ckpt_txm->Execute(&commit_req);
            commit_req.Wait();
        }

        // finish checkpoint on this node group, unpin its data and clear its
        // ccmaps and catalogs if it is no longer leader
        Sharder::Instance().UnpinNodeGroupData(node_group);

        if (flushed &&
            Sharder::Instance().LeaderTerm(node_group) == leader_term)
        {
            last_ckpt_ts_.insert_or_assign(node_group, ckpt_ts);
            NotifyLogOfCkptTs(node_group, leader_term, ckpt_ts);
        }
    }
    // notify ccshard ckpt has finished and can re-check freeable ccentries.
    local_shards_.SetWaitingCkpt(false);
    LOG(INFO) << "End checkpoint with timestamp: " << ckpt_ts;
}

void Checkpointer::Run()
{
    using namespace std::chrono_literals;

    std::unique_lock<std::mutex> lk(mux_);
    while (status_ == Status::Active)
    {
        if (!request_ckpt_ && status_ == Status::Active)
        {
            cv_.wait_for(
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
    cv_.notify_all();
}

/**
 * @brief Called by TxProcessor thread to notify checkpointer thread
 * to do checkpoint if there is no freeable entries to be kicked out
 * from ccmap.
 */
void Checkpointer::Notify()
{
    std::unique_lock<std::mutex> lk(mux_);
    request_ckpt_ = true;
    cv_.notify_one();
}

bool Checkpointer::IsTerminated()
{
    std::scoped_lock<std::mutex> lk(mux_);
    return status_ == Status::Terminated;
}

void Checkpointer::Terminate()
{
    {
        std::scoped_lock<std::mutex> lk(mux_);
        assert(status_ == Status::Active);
        status_ = Status::Terminating;
    }
    cv_.notify_one();

    // The checkpoint worker is terminated, when the tx service is
    // going to be shut down. The checkpoint worker flushes one more
    // time unflushed records to the data store, before exiting. The
    // caller of this method, i.e., the destructor of the tx
    // service, is blocked until last flushing finishes.
    std::unique_lock<std::mutex> lk(mux_);
    cv_.wait(lk, [this] { return status_ == Status::Terminated; });
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

}  // namespace txservice
