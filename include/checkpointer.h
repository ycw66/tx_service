#pragma once

#include <chrono>
#include <iostream>
#include <thread>
#include <vector>

#include "cc/cc_entry.h"
#include "cc/cc_request.h"
#include "cc/local_cc_shards.h"
#include "store/data_store_handler.h"

using namespace std::chrono;

namespace txservice
{
class Checkpointer
{
public:
    Checkpointer(LocalCcShards &shards, store::DataStoreWriteHandler *write_hd)
        : local_shards_(shards),
          last_ckpt_ts_(0),
          mux_(),
          cv_(),
          request_ckpt_(false),
          store_hd_(write_hd),
          status_(Status::Active)
    {
        for (std::unique_ptr<CcShard> &ccs : shards.cc_shards_)
        {
            ccs->ckpter_ = this;
        }

        thd_ = std::thread([this] { Run(); });
    }

    ~Checkpointer()
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

    /*
     * Get Cassandra the table name.
     *
     * Input table name format: ./dbname/tablename
     * Cassandra table name format: dbname___tablename
     */
    std::string GetCassTablename(const std::string &name)
    {
        std::vector<std::string> tokens;
        std::string token;
        // full table name's format is "./dbname/tablename"
        // token[1] is dbname, token[2] is table name given '/' as splitter
        std::istringstream tokenStream(name);
        while (std::getline(tokenStream, token, '/'))
        {
            tokens.push_back(token);
        }

        std::string cass_name;
        cass_name.append(tokens[1]);
        // use three underscore as delimiter of mariadb database name and table
        // name in cassandra table name.
        cass_name.append("___");
        cass_name.append(tokens[2]);

        return cass_name;
    }

    void Ckpt()
    {
        if (local_shards_.Count() == 0 || store_hd_ == nullptr)
        {
            return;
        }

        std::vector<LruEntry *> cce_buf;
        cce_buf.reserve(10000);

        size_t shard_cnt = local_shards_.cc_shards_.size();
        CkptTsCc ckpt_req(shard_cnt);

        // find minimum ckpt_ts from all the ccshard in parallel. ckpt_ts is the
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
        bool flushed = false;

        // iteratate all the tables and execute CkptScanCc requests on each
        // ccshard on all the ccmaps. The result of CkptScanCc is stored in
        // cce_buf.
        for (const auto &[table_name, ccm] : shard.native_ccms_)
        {
            if (table_name == catalog_ccm_name)
            {
                continue;
            }

            cce_buf.clear();

            CkptScanCc ckpt_scan_cc(table_name, ckpt_ts, cce_buf);

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

            if (!cce_buf.empty())
            {
                // Flushes to the data store
                bool ckpt_ret = false;

                if (ccm->Type() == TableType::Primary)
                {
                    const Schema *key_schema = ccm->KeySchema();
                    const Schema *rec_schema = ccm->RecordSchema();
                    ckpt_ret = store_hd_->PutAll(GetCassTablename(table_name),
                                                 cce_buf,
                                                 key_schema,
                                                 rec_schema);
                }
                else
                {
                    const SkSchema *sk_schema =
                        static_cast<const SkSchema *>(ccm->KeySchema());
                    ckpt_ret = store_hd_->PutSkAll(
                        GetCassTablename(table_name), cce_buf, sk_schema);
                }

                // if flush to data store succeeds, update the ckpt_ts for
                // each entries in ccmap.
                if (ckpt_ret)
                {
                    for (LruEntry *&entry : cce_buf)
                    {
                        entry->ckpt_ts_.store(ckpt_ts,
                                              std::memory_order_release);
                    }
                    flushed = true;

                    std::cout << "finish checkpoint, cce_buf.size(): "
                              << cce_buf.size() << std::endl;
                }
                else
                {
                    std::cout << "ckpt fails" << std::endl;
                }
            }
        }

        if (flushed)
        {
            last_ckpt_ts_ = ckpt_ts;
        }
    }

    void Run()
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
                    [this]
                    { return status_ != Status::Active || request_ckpt_; });
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
    void Notify()
    {
        std::unique_lock<std::mutex> lk(mux_);
        request_ckpt_ = true;
        cv_.notify_one();
    }

    bool IsTerminated()
    {
        std::scoped_lock<std::mutex> lk(mux_);
        return status_ == Status::Terminated;
    }

    void Terminate()
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

private:
    enum struct Status
    {
        Active,
        Terminating,
        Terminated
    };

    LocalCcShards &local_shards_;
    uint64_t last_ckpt_ts_;
    std::mutex mux_;
    std::condition_variable cv_;
    bool request_ckpt_;
    store::DataStoreWriteHandler *store_hd_;
    std::thread thd_;
    Status status_;
};
}  // namespace txservice
