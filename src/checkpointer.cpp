#include "checkpointer.h"

#include "tx_service.h"

namespace txservice
{
Checkpointer::Checkpointer(LocalCcShards &shards,
                           store::DataStoreHandler *write_hd,
                           const uint32_t &checkpoint_interval,
                           TxLog *log_agent)
    : local_shards_(shards),
      last_ckpt_ts_(0),
      mux_(),
      cv_(),
      request_ckpt_(false),
      store_hd_(write_hd),
      status_(Status::Active),
      checkpoint_interval_(checkpoint_interval),
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

    std::vector<LruEntry *> ckpt_vec;
    ckpt_vec.reserve(10000);

    size_t shard_cnt = local_shards_.Count();
    CkptTsCc ckpt_req(shard_cnt);

    // Find minimum ckpt_ts from all the ccshard in parallel. ckpt_ts is the
    // minimum timestamp minus 1 among all the active transactions,
    // thus it's safe to flush all the entries smaller or equal to
    // this timestamp.
    for (auto &ccs : local_shards_.cc_shards_)
    {
        ccs->Enqueue(&ckpt_req);
    }
    ckpt_req.Wait();

    uint64_t ckpt_ts = UINT64_MAX;
    ckpt_ts = ckpt_req.GetCkptTs();

    const CcShard &shard = *local_shards_.cc_shards_[0];

    vector<uint32_t> node_groups = Sharder::Instance().LocalNodeGroups();
    for (uint32_t node_group : node_groups)
    {
        // check whether this node is group leader, set checkpoint flag if it is
        int64_t leader_term =
            Sharder::Instance().TryStartCheckpoint(node_group);
        assert(ckpt_ts >= last_ckpt_ts_[node_group]);
        uint64_t last_ckpt_ts = 0;
        auto ite = last_ckpt_ts_.find(node_group);
        if (ite != last_ckpt_ts_.end())
        {
            last_ckpt_ts = ite->second;
        }
        if (leader_term < 0 || ckpt_ts == last_ckpt_ts)
        {
            continue;
        }
        bool flushed = true;

        // get table names this node group contains
        std::unordered_set<TableName> tables =
            local_shards_.CatalogTableNames(node_group);

        // Iterate all the tables and execute CkptScanCc requests on this node
        // group's ccmaps on each ccshard. The result of CkptScanCc is stored in
        // ckpt_vec.
        for (const auto &table_name : tables)
        {
            if (table_name == catalog_ccm_name)
            {
                continue;
            }

            // Only issue tx_request using base table name
            const TableName *base_table_name_;
            TableName sk_base_table_name_;
            std::string::size_type pos = table_name.find(INDEX_NAME_PREFIX);
            if (pos != std::string::npos)
            {
                sk_base_table_name_ = table_name;
                sk_base_table_name_ = sk_base_table_name_.substr(0, pos);
                base_table_name_ = &sk_base_table_name_;
            }
            else
            {
                base_table_name_ = &table_name;
            }

            // Init a tx_request to acquire read lock on catalog cc_entry in one
            // shard, which is good enough to block schema change.
            TransactionExecution *ckpt_txm = tx_service_->NewTx();

            InitTxRequest init_req;
            // Set isolation level to RepeatableRead to ensure the readlock will
            // be set during the execution of the following ReadTxRequest.
            init_req.iso_level_ = IsolationLevel::RepeatableRead;
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
            CatalogKey table_key(*base_table_name_);
            CatalogRecord catalog_rec;

            ReadTxRequest read_req;
            read_req.Reset();
            read_req.Set(&catalog_ccm_name,
                         &table_key,
                         &catalog_rec,
                         LockType::ReadLock,
                         true);
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
                LOG(INFO) << "checkpointer add table lock failed";
                flushed = false;
                continue;
            }

            ckpt_vec.clear();
            CkptScanCc ckpt_scan_cc(table_name, ckpt_ts, ckpt_vec);

            for (auto &ccs : local_shards_.cc_shards_)
            {
                ckpt_scan_cc.Reset(node_group);
                ccs->Enqueue(&ckpt_scan_cc);
                ckpt_scan_cc.Wait();
            }

            // flush to data store if this node group leader term does not
            // change
            if (!ckpt_vec.empty() &&
                Sharder::Instance().LeaderTerm(node_group) == leader_term)
            {
                // Flushes to the data store
                bool ckpt_ret = false;

                CcMap *ccm;
                auto iter = shard.native_ccms_.find(table_name);
                if (iter == shard.native_ccms_.end())
                {
                    auto it = shard.failover_ccms_.find(table_name);
                    assert(it != shard.failover_ccms_.end());
                    ccm = it->second.begin()->second.get();
                }
                else
                {
                    ccm = iter->second.get();
                }

                if (ccm->Type() == TableType::Primary)
                {
                    const Schema *key_schema = ccm->KeySchema();
                    const Schema *rec_schema = ccm->RecordSchema();
                    // todo: stop flush process if this node is no longer node
                    //  group leader
                    ckpt_ret = store_hd_->PutAll(table_name,
                                                 ckpt_vec,
                                                 key_schema,
                                                 rec_schema,
                                                 ccm->SchemaTs());
                }
                else
                {
                    const SecondaryKeySchema *sk_schema =
                        static_cast<const SecondaryKeySchema *>(
                            ccm->KeySchema());
                    // todo: stop flush process if this node is no longer node
                    //  group leader
                    ckpt_ret = store_hd_->PutSkAll(
                        table_name, ckpt_vec, sk_schema, ccm->SchemaTs());
                }

                // If flush to data store succeeds, update the ckpt_ts for each
                // entry in ccmap.
                if (ckpt_ret)
                {
                    for (LruEntry *&entry : ckpt_vec)
                    {
                        entry->ckpt_ts_.store(ckpt_ts,
                                              std::memory_order_release);
                    }
                }
                else
                {
                    LOG(INFO) << "checkpointer flush to cassandra failed";
                    flushed = false;
                }
            }

            // Use CommitTxRequest to release read lock.
            CommitTxRequest commit_req;

            commit_req.Reset();
            ckpt_txm->Execute(&commit_req);
            commit_req.Wait();
            assert(commit_req.Result() == true);
        }

        // finish checkpoint on this node group, clear its ccmaps and catalogs
        // if it is no longer leader
        Sharder::Instance().FinishCheckpoint(node_group);

        if (flushed)
        {
            last_ckpt_ts_.insert_or_assign(node_group, ckpt_ts);
            NotifyLogOfCkptTs(node_group, leader_term, ckpt_ts);
        }
    }
}

bool Checkpointer::CkptEntry(LruEntry *entry)
{
    bool ckpt_ret = false;
    std::vector<LruEntry *> ckpt_vec;
    ckpt_vec.push_back(entry);
    CcMap *ccm = entry->parent_map_;
    TableName table_name = ccm->table_name_;
    if (ccm->Type() == TableType::Primary)
    {
        const Schema *key_schema = ccm->KeySchema();
        const Schema *rec_schema = ccm->RecordSchema();
        ckpt_ret = store_hd_->PutAll(
            table_name, ckpt_vec, key_schema, rec_schema, ccm->SchemaTs());
    }
    else
    {
        const SecondaryKeySchema *sk_schema =
            static_cast<const SecondaryKeySchema *>(ccm->KeySchema());
        ckpt_ret = store_hd_->PutSkAll(
            table_name, ckpt_vec, sk_schema, ccm->SchemaTs());
    }
    return ckpt_ret;
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
                chrono::seconds(checkpoint_interval_),
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
}  // namespace txservice
