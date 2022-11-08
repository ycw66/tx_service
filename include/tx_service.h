#pragma once

#include <chrono>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "catalog.h"
#include "catalog_factory.h"
#include "checkpointer.h"
#include "local_cc_handler.h"
#include "local_cc_shards.h"
#include "metrics/metrics.h"
#include "metrics/tx_meter.h"
#include "moodycamelqueue.h"
#include "tx_execution.h"
#include "tx_request.h"
#include "tx_start_ts_collector.h"
#include "txlog.h"

using namespace std::chrono_literals;

namespace txservice
{
/**
 * @brief TxProcessor is a worker processing concurrency control (cc) requests
 * on one cc shard (identified by the thread/core ID), advances tx state
 * machines allocated for this shard and dispatches cc requests from the shard's
 * tx's to other cc shards, either in the same node or remote nodes .
 *
 */
class TxProcessor
{
public:
    enum struct TxProcessorStatus
    {
        Terminated = 0,
        Busy,
        Sleep
    };

    static const int64_t t1sec = 1000000L;
    static const int64_t t2sec = 4000000L;

    TxProcessor(metrics::MetricsRegistry *metrics_registry,
                size_t thd_id,
                LocalCcShards &shards,
                TxLog *txlog_hd)
        : thd_id_(thd_id),
          active_tx_cnt_(0),
          in_sleep_(false),
          terminated_(false),
          local_cc_shards_(shards),
          free_tx_list_(),
          active_tx_list_(),
          active_tx_mutex_(),
          waiting_mux_(shards.ShardMutex(thd_id)),
          waiting_cv_(shards.ShardCv(thd_id)),
          txlog_hd_(txlog_hd)
#ifdef METRICS_COLLECTOR_ENABLE
          ,
          tx_meter_ptr_(metrics_registry == nullptr
                            ? nullptr
                            : std::make_unique<metrics::TxMeter>(
                                  metrics_registry, thd_id, shards.NodeId()))
#endif
    {
    }

    TxProcessor(size_t thd_id, LocalCcShards &shards, TxLog *txlog_hd)
        : TxProcessor(nullptr, thd_id, shards, txlog_hd)
    {
    }

    TransactionExecution *NewTx()
    {
        TransactionExecution::uptr tx = nullptr;
        bool ret = free_tx_list_.try_dequeue(tx);

        if (!ret)
        {
            tx = std::make_unique<TransactionExecution>(
                cc_hd_.get(), txlog_hd_, this);
        }
        else
        {
            tx->Restart();
        }

        TransactionExecution *tx_ptr = tx.get();

        // add new transaction into active_tx_list_.
        {
            const std::lock_guard<std::mutex> lock(active_tx_mutex_);
            active_tx_list_.emplace_back(std::move(tx));
        }

        // wake up TxProcessor worker thread if neccessary.
        uint32_t prev_tx_cnt = active_tx_cnt_.fetch_add(1);
        if (prev_tx_cnt == 0 && in_sleep_.load(std::memory_order_acquire))
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
        std::list<TransactionExecution::uptr>::iterator active_tx_it;

        do
        {
            batch_.clear();
            {
                const std::lock_guard<std::mutex> lock(active_tx_mutex_);

                if (first_batch)
                {
                    active_tx_it = active_tx_list_.begin();
                    first_batch = false;
                }

                size_t cnt = 0;
                while (active_tx_it != active_tx_list_.end() &&
                       cnt < sweep_batch)
                {
                    if (active_tx_it->get()->tx_status_.load(
                            std::memory_order_relaxed) == TxnStatus::Finished)
                    {
                        // clean transaction and put it to free list.
                        TransactionExecution::uptr tx_p =
                            std::move(*active_tx_it);
                        active_tx_it = active_tx_list_.erase(active_tx_it);
                        free_tx_list_.enqueue(std::move(tx_p));
                        active_tx_cnt_.fetch_sub(1);
                    }
                    else
                    {
                        batch_.emplace_back(active_tx_it->get());
                        ++cnt;
                        ++active_tx_it;
                    }
                }
            }

            for (auto iter = batch_.begin(); iter != batch_.end(); ++iter)
            {
                TransactionExecution *txm = *iter;
                // Forward transaction state machine.
                txm->Forward();

                if (txm->Idle())
                {
                    TxRequest *req = txm->next_req_.exchange(nullptr);
                    if (req != nullptr)
                    {
                        // Process TxRequests.
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

        // Process CcRequests.
        req_cnt = local_cc_shards_.ProcessRequests(thd_id_);
    }

    void Run()
    {
        using namespace std::chrono_literals;

        auto t1000ms = std::chrono::milliseconds(1000);
        auto t100ms = std::chrono::milliseconds(100);
        auto tstart = std::chrono::steady_clock::now();

        size_t idle_rnd = 0;

        while (!terminated_.load(std::memory_order_acquire))
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
                // window exceeds 1000ms.
                auto tnow = std::chrono::steady_clock::now();
                if (tnow - tstart >= t1000ms)
                {
                    idle_rnd = 0;

                    std::unique_lock<std::mutex> lk(waiting_mux_);
                    while (active_tx_cnt_.load(std::memory_order_acquire) ==
                               0 &&
                           local_cc_shards_.IsIdle(thd_id_) &&
                           !terminated_.load(std::memory_order_acquire))
                    {
                        // SleepNotify() notifies the cc shard that its
                        // processor is going to enter into the sleep mode. When
                        // the cc shard receives a cc request and detects that
                        // the sleep flag is set, the cc shard wakes up the
                        // processor. Since the sleep flag is not sync'ed via
                        // the mutex, it is possible that the processor notifies
                        // the cc shard and the cc shard sends the wakeup signal
                        // via the condition variable BEFORE the processor
                        // enters wait_for(), causing the processor to miss the
                        // wakeup signal. Such a situation is rare given that
                        // the processor only enters the sleep mode after a
                        // period of busy wait. In the worse case scenario, the
                        // processor waits for 100ms to restart to process the
                        // cc request.
                        local_cc_shards_.SleepNotify(thd_id_);
                        in_sleep_.store(true, std::memory_order_release);
                        waiting_cv_.wait_for(lk, t100ms);
                    }

                    local_cc_shards_.WorkNotify(thd_id_);
                    in_sleep_.store(false, std::memory_order_release);
                }
            }
        }
    }

    void InitializeLocalHandler()
    {
        if (cc_hd_ == nullptr)
        {
            cc_hd_ =
                std::make_unique<LocalCcHandler>(thd_id_, local_cc_shards_);
        }
    }

#ifdef METRICS_COLLECTOR_ENABLE
    void MetricCollect(const metrics::Value metric_value,
                       metrics::MetricsNaming &naming_,
                       std::optional<metrics::MetricsLabels> label_)
    {
        if (!tx_meter_ptr_)
        {
            return;
        }
        auto meter =
            tx_meter_ptr_->GetMeter(std::move(naming_), std::move(label_));
        (*meter)(metric_value);
    }
#endif
    size_t thd_id_;
    std::atomic<bool> terminated_;
    std::atomic<bool> in_sleep_;

    LocalCcShards &local_cc_shards_;
    std::unique_ptr<LocalCcHandler> cc_hd_;

    std::atomic<uint32_t> active_tx_cnt_;
    std::list<TransactionExecution::uptr> active_tx_list_;
    std::mutex active_tx_mutex_;
    moodycamel::ConcurrentQueue<TransactionExecution::uptr> free_tx_list_;
    std::vector<TransactionExecution *> batch_;

    std::mutex &waiting_mux_;
    std::condition_variable &waiting_cv_;

    std::mutex sleep_mux_;
    std::condition_variable sleep_cv_;

    TxLog *txlog_hd_;

    friend class TxService;

private:
#ifdef METRICS_COLLECTOR_ENABLE
    std::unique_ptr<metrics::TxMeter> tx_meter_ptr_;
#endif
};

class TxService
{
public:
    TxService(const std::string &local_path,
              CatalogFactory *catalog_factory,
              const std::map<std::string, uint32_t> &conf,
              uint32_t node_id = 0,
              std::vector<std::string> *ips = nullptr,
              std::vector<uint16_t> *ports = nullptr,
              store::DataStoreHandler *store_hd = nullptr,
              metrics::MetricsRegistry *metrics_registry = nullptr,
              std::unique_ptr<TxLog> log_hd = nullptr,
              bool enable_mvcc = true)
        : local_cc_shards_(node_id,
                           conf.find("core_num")->second,
                           conf.find("node_memory_limit_mb")->second,
                           conf.find("node_log_limit_mb")->second,
                           catalog_factory,
                           store_hd,
                           metrics_registry,
                           this,
                           enable_mvcc),
          ckpt_(local_cc_shards_,
                store_hd,
                conf.find("checkpointer_interval")->second,
                log_hd.get(),
                conf.find("checkpointer_delay_seconds")->second)
    {
        uint32_t core_cnt = conf.find("core_num")->second;
        pool_.reserve(core_cnt);
        thd_pool_.reserve(core_cnt);

        Sharder::Instance(
            node_id, ips, ports, &local_cc_shards_, std::move(log_hd));
        for (uint16_t thd_idx = 0; thd_idx < core_cnt; ++thd_idx)
        {
#if defined(METRICS_COLLECTOR_ENABLE)
            pool_.emplace_back(std::make_unique<TxProcessor>(
                metrics_registry,
                thd_idx,
                local_cc_shards_,
                Sharder::Instance().GetLogAgent()));
#else
            pool_.emplace_back(std::make_unique<TxProcessor>(
                thd_idx, local_cc_shards_, Sharder::Instance().GetLogAgent()));
#endif
        }

        Sharder::Instance().Init(local_path);
    }

    TxService(const std::string &local_path,
              CatalogFactory *catalog_factory,
              const std::map<std::string, uint32_t> &conf,
              uint32_t node_id = 0,
              std::vector<std::string> *ips = nullptr,
              std::vector<uint16_t> *ports = nullptr,
              store::DataStoreHandler *store_hd = nullptr,
              std::unique_ptr<TxLog> log_hd = nullptr,
              bool enable_mvcc = true)
        : TxService(local_path,
                    catalog_factory,
                    conf,
                    node_id,
                    ips,
                    ports,
                    store_hd,
                    nullptr,
                    std::move(log_hd),
                    enable_mvcc)
    {
    }

    void Start()
    {
        for (size_t thd_idx = 0; thd_idx < pool_.size(); ++thd_idx)
        {
            TxProcessor *tp = pool_[thd_idx].get();

            tp->InitializeLocalHandler();
            thd_pool_.emplace_back(std::thread([tp] { tp->Run(); }));
        }

        if (local_cc_shards_.EnableMvcc())
        {
            TxStartTsCollector::Instance(&local_cc_shards_).Start();
        }
    }

    void WaitClusterReady()
    {
        Sharder::Instance().WaitClusterReady();
    }

    ~TxService()
    {
        ckpt_.Terminate();
        ckpt_.Join();
        if (local_cc_shards_.EnableMvcc())
        {
            TxStartTsCollector::Instance().Shutdown();
        }

        Sharder::Instance().Shutdown();

        for (size_t thd_idx = 0; thd_idx < thd_pool_.size(); ++thd_idx)
        {
            pool_[thd_idx]->terminated_.store(true, std::memory_order_relaxed);
        }
        for (auto &thd_idx : thd_pool_)
        {
            thd_idx.join();
        }
    }

    TransactionExecution *NewTx()
    {
        uint32_t run_cnt = tx_runs_.fetch_add(1, std::memory_order_relaxed);
        size_t sid = run_cnt % pool_.size();
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

    std::vector<std::unique_ptr<TxProcessor>> pool_;
    std::vector<std::thread> thd_pool_;
    LocalCcShards local_cc_shards_;
    Checkpointer ckpt_;
    // tx runs shared by all the clients of tx_service. It is used to balance
    // workloads between TxProcessors.
    std::atomic<uint32_t> tx_runs_{0};
};

}  // namespace txservice
