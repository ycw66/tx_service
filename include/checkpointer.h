#pragma once

#include <thread>
#include <vector>

#include "cc/cc_entry.h"
#include "cc/cc_request.h"
#include "cc/local_cc_shards.h"
#include "store/data_store_handler.h"

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
     * Normalize the table name which is consistent with storage engine, ie.
     * Cassandra.
     * Input table name format: ./dbname/tablename
     * Normalized table name format: dbname_tablename
     */
    std::string NormalizeTablename(const std::string &name)
    {
        std::string norm_name(name);
        size_t slash_pos = norm_name.find_first_of('/');
        norm_name = norm_name.substr(slash_pos + 1);
        slash_pos = norm_name.find_first_of('/');
        norm_name.at(slash_pos) = '_';

        return norm_name;
    }

    void Ckpt(int id)
    {
        if (local_shards_.Count() == 0 || store_hd_ == nullptr)
        {
            return;
        }

        std::vector<LruEntry *> cce_buf;
        cce_buf.reserve(1000000);

        uint64_t ckpt_ts = UINT64_MAX;
        CkptTsCc ckpt_ts_cc(id);

        for (const auto &ccs : local_shards_.cc_shards_)
        {
            ckpt_ts_cc.Reset();
            ccs->Enqueue(&ckpt_ts_cc);
            ckpt_ts_cc.Wait();
            ckpt_ts = std::min(ckpt_ts, ckpt_ts_cc.GetCkptTs());
        }

        assert(ckpt_ts >= last_ckpt_ts_);

        if (ckpt_ts == last_ckpt_ts_)
        {
            return;
        }

        const CcShard &shard = *local_shards_.cc_shards_.at(0);

        for (const auto &ccm_pair : shard.native_ccms_)
        {
            const TableName &tabname = ccm_pair.first;
            TableType type = ccm_pair.second->Type();
            cce_buf.clear();

            CkptScanCc ckpt_scan_cc(tabname, ckpt_ts, cce_buf);

            for (auto &ccs : local_shards_.cc_shards_)
            {
                ckpt_scan_cc.Reset(ccs->node_id_);
                ccs->Enqueue(&ckpt_scan_cc);
                ckpt_scan_cc.Wait();

                auto table_it = ccs->failover_ccms_.find(tabname);
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

            if (cce_buf.size() > 0)
            {
                // Flushes to the data store
                bool ckpt_ret = false;

                if (type == TableType::Primary)
                {
                    const Schema *key_schema = ccm_pair.second->KeySchema();
                    const Schema *rec_schema = ccm_pair.second->RecordSchema();
                    ckpt_ret = store_hd_->PutAll(NormalizeTablename(tabname),
                                                 cce_buf,
                                                 key_schema,
                                                 rec_schema);
                }
                else
                {
                    const SkSchema *sk_schema = static_cast<const SkSchema *>(
                        ccm_pair.second->KeySchema());
                    ckpt_ret = store_hd_->PutSkAll(
                        NormalizeTablename(tabname), cce_buf, sk_schema);
                }

                if (ckpt_ret)
                {
                    for (LruEntry *&entry : cce_buf)
                    {
                        entry->ckpt_ts_.store(ckpt_ts,
                                              std::memory_order_release);
                    }
                }
            }
        }

        last_ckpt_ts_ = ckpt_ts;
    }

    void Run()
    {
        using namespace std::chrono_literals;
        int id = 0;

        std::unique_lock<std::mutex> lk(mux_);
        while (status_ == Status::Active)
        {
            if (!request_ckpt_ && status_ == Status::Active)
            {
                cv_.wait_for(
                    lk,
                    5s,
                    [this]
                    { return status_ != Status::Active || request_ckpt_; });
            }

            lk.unlock();
            Ckpt(id);
            lk.lock();

            request_ckpt_ = false;
            ++id;
        }

        status_ = Status::Terminated;
        cv_.notify_all();
    }

    void Notify()
    {
        std::unique_lock<std::mutex> lk(mux_);
        request_ckpt_ = true;
        cv_.notify_one();
    }

    void Exit()
    {
        std::unique_lock<std::mutex> lk(mux_);
        status_ = Status::Terminating;
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

        // The checkpoint worker is terminated, when the tx service is going to
        // be shut down. The checkpoint worker flushes one more time unflushed
        // records to the data store, before exiting. The caller of this method,
        // i.e., the destructor of the tx service, is blocked until last
        // flushing finishes.
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
