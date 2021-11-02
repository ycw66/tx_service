#pragma once

#include <chrono>
#include <list>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

#include "catalog.h"
#include "checkpointer.h"
#include "local_cc_handler.h"
#include "local_cc_shards.h"
#include "moodycamelqueue.h"
#include "remote/remote_cc_handler.h"
#include "tx_execution.h"
#include "tx_request.h"
#include "txlog.h"

#ifdef _MSC_VER
#include "remote/remote_cc_handler_win.h"
#else
#include "remote/remote_cc_handler_brpc.h"
#endif

namespace txservice
{
class TxProcessor
{
public:
    TxProcessor(size_t thd_id,
                LocalCcShards &shards,
                std::unique_ptr<TxLog> txlog_hd)
        : thd_id_(thd_id),
          tx_cnt_(0),
          terminate_(false),
          local_cc_shards_(shards),
          cc_handler_(local_cc_shards_.GetCcHandler(thd_id)),
          tx_ws_(),
          ws_mutex_(),
          free_tx_(),
          waiting_mux_(shards.ShardMutex(thd_id)),
          waiting_cv_(shards.ShardCv(thd_id)),
          txlog_hd_(std::move(txlog_hd))
    {
        batch_.reserve(20);
    }

    TransactionExecution *NewTx()
    {
        TransactionExecution::uptr tx = nullptr;
        bool ret = free_tx_.try_dequeue(tx);

        if (!ret)
        {
            tx = std::make_unique<TransactionExecution>(
                cc_handler_, txlog_hd_ == nullptr ? nullptr : txlog_hd_.get());
        }

        TransactionExecution *tx_ptr = tx.get();
        tx->Reset();

        {
            const std::lock_guard<std::mutex> lock(ws_mutex_);
            tx_ws_.emplace_back(std::move(tx));
        }

        uint32_t prev_tx_cnt = tx_cnt_.fetch_add(1);
        if (prev_tx_cnt == 0)
        {
            waiting_cv_.notify_one();
        }

        return tx_ptr;
    }

    void RunOneRound(size_t &active_cnt, size_t &req_cnt)
    {
        active_cnt = 0;
        req_cnt = 0;
        size_t sweep_batch = 20;
        bool first_batch = true;
        std::list<TransactionExecution::uptr>::iterator ws_it;

        do
        {
            batch_.clear();
            {
                const std::lock_guard<std::mutex> lock(ws_mutex_);

                if (first_batch)
                {
                    ws_it = tx_ws_.begin();
                    first_batch = false;
                }

                size_t cnt = 0;
                while (ws_it != tx_ws_.end() && cnt < sweep_batch)
                {
                    if (ws_it->get()->finish_)
                    {
                        TransactionExecution::uptr tx_p = std::move(*ws_it);
                        ws_it = tx_ws_.erase(ws_it);
                        free_tx_.enqueue(std::move(tx_p));
                        tx_cnt_.fetch_sub(1);
                    }
                    else
                    {
                        batch_.emplace_back(ws_it->get());
                        ++cnt;
                        ++ws_it;
                    }
                }
            }

            for (auto iter = batch_.begin(); iter != batch_.end(); ++iter)
            {
                TransactionExecution *txm = *iter;
                txm->Forward();

                if (txm->Idle())
                {
                    TxRequest *req = txm->next_req_.exchange(nullptr);
                    if (req != nullptr)
                    {
                        req->Process(txm);
                        ++active_cnt;
                    }
                }
                else
                {
                    ++active_cnt;
                }
            }
        } while (batch_.size() == sweep_batch);

        req_cnt = local_cc_shards_.ProcessRequests(thd_id_);
    }

    void Run()
    {
        using namespace std::chrono_literals;

        auto t200ms = std::chrono::milliseconds(1000);
        auto t5ms = std::chrono::milliseconds(5);
        auto tstart = std::chrono::steady_clock::now();

        size_t idle_rnd = 0;

        while (!terminate_.load(std::memory_order_acquire))
        {
            size_t tx_cnt, req_cnt;
            RunOneRound(tx_cnt, req_cnt);
            if (tx_cnt > 0 || req_cnt > 0)
            {
                idle_rnd = 0;
                continue;
            }

            if (idle_rnd == 0)
            {
                // Records the time when busy wait starts.
                tstart = std::chrono::steady_clock::now();
            }

            ++idle_rnd;
            if ((idle_rnd & 0x3FF) == 0)
            {
                // For every 1024 busy wait cycles, checks if the busy wait
                // window exceeds 200ms.
                auto tnow = std::chrono::steady_clock::now();
                if (tnow - tstart >= t200ms)
                {
                    idle_rnd = 0;

                    std::unique_lock<std::mutex> lk(waiting_mux_);
                    while (tx_cnt_.load(std::memory_order_acquire) == 0 &&
                           local_cc_shards_.IsIdle(thd_id_) &&
                           !terminate_.load(std::memory_order_acquire))
                    {
                        waiting_cv_.wait_for(lk, t5ms);
                    }
                }
            }
        }
    }

    size_t thd_id_;
    std::atomic<uint32_t> tx_cnt_;
    std::atomic<bool> terminate_;

    // TxProcessor process requests on one cc_shard of the local_cc_shards_
    // which is identified by thd_id.
    LocalCcShards &local_cc_shards_;
    CcHandler *cc_handler_;

    // std::map<uint64_t, TransactionExecution::uptr> tx_ws_;
    std::list<TransactionExecution::uptr> tx_ws_;
    std::mutex ws_mutex_;
    moodycamel::ConcurrentQueue<TransactionExecution::uptr> free_tx_;
    std::vector<TransactionExecution *> batch_;

    std::mutex &waiting_mux_;
    std::condition_variable &waiting_cv_;

    std::unique_ptr<TxLog> txlog_hd_;

    friend class TxService;
};

class TxService
{
public:
    TxService(Catalog *catalog,
              uint32_t node_id = 0,
              uint16_t core_cnt = 1,
              int listen_port = 8000,
              store::DataStoreWriteHandler *store_hd = nullptr,
              std::unique_ptr<TxLog> log_hd = nullptr)
        : local_cc_shards_(node_id, core_cnt, catalog),
          ckpt_(local_cc_shards_, store_hd),
          log_hd_(std::move(log_hd))
    {
        pool_.reserve(core_cnt);
        thd_pool_.reserve(core_cnt);

        for (uint16_t thd_idx = 0; thd_idx < core_cnt; ++thd_idx)
        {
            pool_.emplace_back(std::make_unique<TxProcessor>(
                thd_idx,
                local_cc_shards_,
                log_hd_ == nullptr ? nullptr : log_hd_->Clone()));
        }

#ifdef _MSC_VER
        remote_hd_ =
            std::make_unique<remote::RemoteCcHandler_Win>(&local_cc_shards_);
#else
        remote_hd_ = std::make_unique<remote::RemoteCcHandler_Brpc>(
            &local_cc_shards_, listen_port);
#endif
    }

    void Start()
    {
        for (size_t thd_idx = 0; thd_idx < pool_.size(); ++thd_idx)
        {
            TxProcessor *tp = pool_[thd_idx].get();
            thd_pool_.emplace_back(std::thread([tp] { tp->Run(); }));
        }
    }

    ~TxService()
    {
        ckpt_.Terminate();

        for (size_t thd_idx = 0; thd_idx < thd_pool_.size(); ++thd_idx)
        {
            pool_[thd_idx]->terminate_.store(true);
            thd_pool_[thd_idx].join();
        }
    }

    TransactionExecution *NewTx()
    {
        thread_local uint16_t run_cnt = 0;
        size_t sid = run_cnt % pool_.size();
        ++run_cnt;
        return pool_[sid]->NewTx();
    }

    LocalCcShards &CcShards()
    {
        return local_cc_shards_;
    }

    template <typename KeyT, typename ValueT>
    void CreateCcTable(const TableName &tabname,
                       const Schema *key_schema = nullptr,
                       const Schema *rec_schema = nullptr,
                       uint32_t core_id = 0,
                       bool is_all = true)
    {
        local_cc_shards_.CreateCcTable<KeyT, ValueT>(
            tabname, key_schema, rec_schema, core_id, is_all);
    }

    void DropCcTable(const TableName &tabname, uint32_t core_id)
    {
        local_cc_shards_.DropCcTable(tabname, core_id);
    }

    template <typename SkT, typename PkT>
    void CreateIndexCcTable(const TableName &index_name,
                            const Schema *sk_schema = nullptr,
                            const Schema *pk_schema = nullptr,
                            uint32_t core_id = 0,
                            bool is_all = true)
    {
        local_cc_shards_.CreateSkCcTable<SkT, PkT>(
            index_name, sk_schema, pk_schema, core_id, is_all);
    }

    void FillTableCatalog(const TableName &tabname,
                          std::string &catalog_content,
                          std::string table_version,
                          uint32_t core_id = 0,
                          bool is_all = true)
    {
        local_cc_shards_.FillTableCatalog(
            tabname, catalog_content, table_version, core_id, is_all);
    }

    void RemoveTableCatalog(const TableName &tabname, uint32_t core_id)
    {
        local_cc_shards_.RemoveTableCatalog(tabname, core_id);
    }

    std::vector<std::unique_ptr<TxProcessor>> pool_;
    std::vector<std::thread> thd_pool_;
    LocalCcShards local_cc_shards_;
    std::unique_ptr<remote::RemoteCcHandler> remote_hd_;
    Checkpointer ckpt_;
    std::unique_ptr<TxLog> log_hd_;
};
}  // namespace txservice
