#include "checkpointer.h"

#include "tx_service.h"

namespace txservice
{
Checkpointer::Checkpointer(LocalCcShards &shards,
                           store::DataStoreWriteHandler *write_hd)
    : local_shards_(shards),
      last_ckpt_ts_(0),
      mux_(),
      cv_(),
      request_ckpt_(false),
      store_hd_(write_hd),
      status_(Status::Active)
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
    thd_.join();

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

    assert(ckpt_ts >= last_ckpt_ts_);

    if (ckpt_ts == last_ckpt_ts_)
    {
        return;
    }

    const CcShard &shard = *local_shards_.cc_shards_[0];
    bool flushed = true;

    // Copy a list of TableName of native_ccms_
    std::unordered_set<TableName> tables;
    {
        // Acquire lock on the first shard to block DDL.
        std::lock_guard<std::mutex> lk(local_shards_.ShardMutex(0));
        for (const auto &ccm_pair : shard.native_ccms_)
        {
            tables.emplace(ccm_pair.first);
        }
    }

    // Iteratate all the tables and execute CkptScanCc requests on each
    // ccshard on all the ccmaps. The result of CkptScanCc is stored in
    // ckpt_vec.
    for (const auto &table_name : tables)
    {
        if (table_name == catalog_ccm_name)
        {
            continue;
        }

        // Init a tx_request to acquire read lock on catalog cc_entry in one
        // shard, which is good enough to block schema change.
        TransactionExecution *ckpt_txm = tx_service_->NewTx();

        InitTxRequest init_req;
        // Set isolation level to RepeatableRead to ensure the readlock will be
        // set during the execution of the following ReadTxRequest.
        init_req.iso_level_ = IsolationLevel::RepeatableRead;
        init_req.Reset();
        ckpt_txm->Execute(&init_req);
        init_req.Wait();

        // If table_name has been dropped at this point, read lock would not be
        // acquired.
        CatalogKey table_key(table_name);
        CatalogRecord catalog_rec;

        ReadTxRequest read_req;
        read_req.Reset();
        read_req.Set(&catalog_ccm_name,
                     &table_key,
                     &catalog_rec,
                     ReadType::Inside,
                     LockType::ReadLock,
                     true);
        ckpt_txm->Execute(&read_req);
        read_req.Wait();

        if (read_req.IsError() || read_req.Result() != RecordStatus::Normal)
        {
            // Use CommitTxRequest to release read lock.
            CommitTxRequest commit_req;
            commit_req.Reset();
            ckpt_txm->Execute(&commit_req);
            commit_req.Wait();
            assert(commit_req.Result() == true);
            continue;
        }

        ckpt_vec.clear();
        CkptScanCc ckpt_scan_cc(table_name, ckpt_ts, ckpt_vec);

        for (auto &ccs : local_shards_.cc_shards_)
        {
            ckpt_scan_cc.Reset(ccs->node_id_);
            ccs->Enqueue(&ckpt_scan_cc);
            ckpt_scan_cc.Wait();

            auto table_it = ccs->failover_ccms_.find(table_name);
            if (table_it != ccs->failover_ccms_.end())
            {
                for (auto &ng_pair : table_it->second)
                {
                    ckpt_scan_cc.Reset(ng_pair.first);
                    ccs->Enqueue(&ckpt_scan_cc);
                    ckpt_scan_cc.Wait();
                }
            }
        }

        if (!ckpt_vec.empty())
        {
            // Flushes to the data store
            bool ckpt_ret = false;

            CcMap *ccm = shard.native_ccms_.at(table_name).get();
            if (ccm->Type() == TableType::Primary)
            {
                const Schema *key_schema = ccm->KeySchema();
                const Schema *rec_schema = ccm->RecordSchema();
                ckpt_ret = store_hd_->PutAll(
                    table_name, ckpt_vec, key_schema, rec_schema);

                // fault injection to prolong the process of ckpt flush
                ACTION_FAULT_INJECTOR("after_ckpt_flush");
            }
            else
            {
                const SkSchema *sk_schema =
                    static_cast<const SkSchema *>(ccm->KeySchema());
                ckpt_ret = store_hd_->PutSkAll(table_name, ckpt_vec, sk_schema);
            }

            // If flush to data store succeeds, update the ckpt_ts for
            // each entries in ccmap.
            if (ckpt_ret)
            {
                for (LruEntry *&entry : ckpt_vec)
                {
                    entry->ckpt_ts_.store(ckpt_ts, std::memory_order_release);
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
        assert(commit_req.Result() == true);
    }

    if (flushed)
    {
        last_ckpt_ts_ = ckpt_ts;
    }
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
                10s,
                [this] { return status_ != Status::Active || request_ckpt_; });
        }

        lk.unlock();
        Ckpt();
        lk.lock();

        request_ckpt_ = false;
    }

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

}  // namespace txservice