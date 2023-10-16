#pragma once

#include <pthread.h>

#include <algorithm>  // std::min
#include <array>
#include <chrono>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog.h"
#include "catalog_factory.h"
#include "checkpointer.h"
#include "circular_queue.h"
#include "dead_lock_check.h"
#include "local_cc_handler.h"
#include "local_cc_shards.h"
#include "meter.h"
#include "metrics.h"
#include "moodycamelqueue.h"
#include "tx_execution.h"
#include "tx_request.h"
#include "tx_start_ts_collector.h"
#include "txlog.h"

using namespace std::chrono_literals;

namespace txservice
{

// whether skip write redo log to log_service.
extern bool txservice_skip_redo_log;

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

    TxProcessor(size_t thd_id,
                LocalCcShards &shards,
                TxLog *txlog_hd,
                metrics::MetricsRegistry *metrics_registry = nullptr,
                metrics::CommonLabels common_labels = {})
        : thd_id_(thd_id),
          terminated_(false),
          in_sleep_(false),
          local_cc_shards_(shards),
          active_tx_cnt_(0),
          new_tx_cnt_(0),
          new_txs_(),
          new_tx_token_(new_txs_),
          free_prod_token_(free_txs),
          free_consumer_token_(free_txs),
          txlog_hd_(txlog_hd),
          meter_(
              std::make_unique<metrics::Meter>(metrics_registry, common_labels))
    {
        if (metrics::enable_busy_round_metrics)
        {
            meter_->Register(BUSY_ROUND_DURATION_NAME_,
                             metrics::Type::Histogram);
            meter_->Register(BUSY_ROUND_ACTIVE_TX_COUNT_NAME_,
                             metrics::Type::Gauge);
            meter_->Register(BUSY_ROUND_PROCESSED_CC_REQUEST_COUNT_NAME_,
                             metrics::Type::Gauge);
            meter_->Register(EMPTY_ROUND_RATIO_NAME_, metrics::Type::Gauge);
        }

        if (metrics::enable_transactions)
        {
            meter_->Register(TX_DURATION_NAME_, metrics::Type::Histogram);
            meter_->Register(TX_PROCESSED_TOTAL_NAME_, metrics::Type::Counter);
            meter_->Register(REMOTE_REQUEST_DURATION_NAME_,
                             metrics::Type::Histogram,
                             {{"type",
                               {"read",
                                "acquire_write",
                                "validate",
                                "post_process",
                                "scan_next",
                                "write_log"}}});
            meter_->Register(REMOTE_REQUEST_ON_FLY_COUNT_NAME_,
                             metrics::Type::Gauge,
                             {{"type",
                               {"read",
                                "acquire_write",
                                "validate",
                                "post_process",
                                "scan_next",
                                "write_log"}}});
        }
#ifdef EXT_TX_PROC_ENABLED
        external_processor_func_ = [this]() { RunOneRound(); };
#endif
    }

    TransactionExecution *NewTx()
    {
        TransactionExecution::uptr tx = nullptr;
        bool success =
            TxProcessor::free_txs.try_dequeue(free_consumer_token_, tx);
        if (success)
        {
            assert(tx != nullptr);
            tx->Restart(cc_hd_.get(), txlog_hd_, this);
        }
        else
        {
            tx = std::make_unique<TransactionExecution>(
                cc_hd_.get(), txlog_hd_, this);
        }

        TransactionExecution *tx_ptr = tx.get();

        uint32_t prev_tx_cnt =
            active_tx_cnt_.fetch_add(1, std::memory_order_relaxed);

        new_tx_cnt_.fetch_add(1, std::memory_order_relaxed);
        // Add the new transaction into the new tx set.
        new_txs_.enqueue(std::move(tx));

        // Wakes up the tx processor thread if it is in sleep.
        if (prev_tx_cnt == 0 && in_sleep_.load(std::memory_order_relaxed))
        {
            std::unique_lock<std::mutex> lk(sleep_mux_);
            sleep_cv_.notify_one();
        }

        return tx_ptr;
    }

    TransactionExecution::uptr NewTxm()
    {
        TransactionExecution::uptr txm = nullptr;
        bool success =
            TxProcessor::free_txs.try_dequeue(free_consumer_token_, txm);
        if (success)
        {
            assert(txm != nullptr);
            txm->Restart(cc_hd_.get(), txlog_hd_, this);
        }
        else
        {
            txm = std::make_unique<TransactionExecution>(
                cc_hd_.get(), txlog_hd_, this);
        }

        active_tx_cnt_.fetch_add(1, std::memory_order_relaxed);

        return txm;
    }

    void RecycleTxm(TransactionExecution::uptr txm)
    {
        TxProcessor::free_txs.enqueue(free_prod_token_, std::move(txm));
        active_tx_cnt_.fetch_sub(1, std::memory_order_relaxed);
    }

#ifdef EXT_TX_PROC_ENABLED
    void ExternalForward(TransactionExecution *txm)
    {
        external_process_counter_.fetch_add(1);

        bool expected = false;
        bool success = process_latch_.compare_exchange_strong(
            expected, true, std::memory_order_acq_rel);

        if (!success)
        {
            return;
        }

        for (size_t loop = 0; loop < 3; ++loop)
        {
            TxmStatus txm_status = txm->Forward();
            local_cc_shards_.ProcessRequests(thd_id_);

            if (txm_status != TxmStatus::Busy)
            {
                break;
            }
        }

        assert(process_latch_.load(std::memory_order_acquire));
        process_latch_.store(false, std::memory_order_release);
    }

    void RunOneRound()
    {
        std::array<TransactionExecution::uptr, 20> active_txs;

        for (size_t loop = 0; loop < 4; ++loop)
        {
            bool expected = false;
            bool success = process_latch_.compare_exchange_strong(
                expected, true, std::memory_order_acq_rel);
            if (!success)
            {
                return;
            }

            size_t active_tx_cnt = 0;
            {
                std::unique_lock<std::mutex> lk(active_tx_mutex_);
                while (active_txs_.Size() > 0 &&
                       active_tx_cnt < active_txs.size())
                {
                    active_txs[active_tx_cnt] = std::move(active_txs_.Peek());
                    active_txs_.Dequeue();
                    ++active_tx_cnt;
                }
            }

            for (size_t idx = 0; idx < active_tx_cnt; ++idx)
            {
                TxmStatus txm_status = active_txs[idx]->Forward();

                switch (txm_status)
                {
                case TxmStatus::Finished:
                {
                    TxProcessor::free_txs.enqueue(std::move(active_txs[idx]));
                }
                break;
                case TxmStatus::Idle:
                    active_txs_.Enqueue(std::move(active_txs[idx]));
                    break;
                case TxmStatus::Busy:
                    active_txs_.Enqueue(std::move(active_txs[idx]));
                    break;
                default:
                    break;
                }
            }

            local_cc_shards_.ProcessRequests(thd_id_);

            assert(process_latch_.load(std::memory_order_acquire));
            process_latch_.store(false, std::memory_order_release);

            if (active_tx_cnt == 0)
            {
                break;
            }
        }
    }
#endif

    void RunOneRound(size_t &active_cnt, size_t &req_cnt, bool &yield)
    {
#ifdef EXT_TX_PROC_ENABLED
        size_t native_txm_cnt = 0;
        {
            std::unique_lock<std::mutex> lk(active_tx_mutex_);
            native_txm_cnt = active_txs_.Size() + fly_tx_queue_.Size();
        }

        uint16_t ext_num =
            external_processor_num_.load(std::memory_order_relaxed);
        if (ext_num > 0 && native_txm_cnt == 0)
        {
            // When there is one or more external tx processor threads, the
            // native processor thread sleeps a period (e.g., 10us) before
            // every run. If the external processor thread(s) are active and
            // have advanced the the counter since last check, the native
            // processor thread yields.
            size_t ext_counter =
                external_process_counter_.load(std::memory_order_relaxed);
            if (internal_counter_ < ext_counter)
            {
                internal_counter_ = ext_counter;
                yield = true;
                return;
            }
        }
#endif

        yield = false;
        active_cnt = 0;
        req_cnt = 0;

        size_t new_tx_cnt = new_tx_cnt_.load(std::memory_order_relaxed);
        if (new_tx_cnt > 0)
        {
            std::array<TransactionExecution::uptr, 100> txs;
            size_t dq_cnt = std::min(txs.size(), new_tx_cnt);

            new_tx_cnt = new_txs_.try_dequeue_bulk(
                new_tx_token_, std::make_move_iterator(txs.begin()), dq_cnt);

            for (size_t idx = 0; idx < new_tx_cnt; ++idx)
            {
                idle_txs_.Enqueue(std::move(txs[idx]));
            }

            new_tx_cnt_.fetch_sub(new_tx_cnt, std::memory_order_relaxed);
        }

        size_t idle_size = idle_txs_.Size();
        for (size_t idx = 0; idx < idle_size; ++idx)
        {
            TransactionExecution::uptr tx = std::move(idle_txs_.Peek());
            idle_txs_.Dequeue();

            TxmStatus txm_status = tx->Forward();

            switch (txm_status)
            {
            case TxmStatus::Finished:
                TxProcessor::free_txs.enqueue(free_prod_token_, std::move(tx));
                active_tx_cnt_.fetch_sub(1, std::memory_order_relaxed);
                break;
            case TxmStatus::Idle:
                idle_txs_.Enqueue(std::move(tx));
                break;
            case TxmStatus::Busy:
                on_fly_txs_.Enqueue(std::move(tx));
                break;
            default:
                break;
            }
        }

        if (is_busy_round_ && metrics::enable_busy_round_metrics)
        {
            meter_->CollectDuration(BUSY_ROUND_DURATION_NAME_,
                                    busy_round_start_);
            meter_->Collect(BUSY_ROUND_ACTIVE_TX_COUNT_NAME_,
                            busy_round_active_tx_count_);
            meter_->Collect(BUSY_ROUND_PROCESSED_CC_REQUEST_COUNT_NAME_,
                            busy_round_processed_cc_req_count_);
            is_busy_round_ = false;
        }

        for (size_t loop = 0; loop < 5; ++loop)
        {
#ifdef EXT_TX_PROC_ENABLED
            bool expected = false;
            bool success = process_latch_.compare_exchange_strong(
                expected, true, std::memory_order_acq_rel);
            if (!success)
            {
                return;
            }
#endif
            size_t fly_size = on_fly_txs_.Size();
            for (size_t idx = 0; idx < fly_size; ++idx)
            {
                TransactionExecution::uptr tx = std::move(on_fly_txs_.Peek());
                on_fly_txs_.Dequeue();

                TxmStatus txm_status = tx->Forward();

                switch (txm_status)
                {
                case TxmStatus::Finished:
                    TxProcessor::free_txs.enqueue(free_prod_token_,
                                                  std::move(tx));
                    active_tx_cnt_.fetch_sub(1, std::memory_order_relaxed);
                    break;
                case TxmStatus::Idle:
                    idle_txs_.Enqueue(std::move(tx));
                    break;
                case TxmStatus::Busy:
                    on_fly_txs_.Enqueue(std::move(tx));
                    break;
                default:
                    break;
                }
            }

            if (loop == 0 && metrics::enable_busy_round_metrics &&
                local_cc_shards_.QueueSize(thd_id_) >= busy_round_threshold_)
            {
                is_busy_round_ = true;
                busy_round_start_ = metrics::Clock::now();
            }

            // Process CcRequests.
            req_cnt += local_cc_shards_.ProcessRequests(thd_id_);

#ifdef EXT_TX_PROC_ENABLED
            assert(process_latch_.load(std::memory_order_acquire));
            process_latch_.store(false, std::memory_order_release);
#endif
        }

        active_cnt =
            on_fly_txs_.Size() + new_tx_cnt_.load(std::memory_order_relaxed);

        if (metrics::enable_collect_metrics)
        {
            empty_round_count_ += req_cnt == 0 ? 1 : 0;
            if (++total_round_count_ == empty_round_threshold_)
            {
                meter_->Collect(EMPTY_ROUND_RATIO_NAME_,
                                static_cast<double>(empty_round_count_) /
                                    total_round_count_);
                empty_round_count_ = 0;
                total_round_count_ = 0;
            }

            if (is_busy_round_)
            {
                busy_round_active_tx_count_ = active_cnt;
                busy_round_processed_cc_req_count_ = req_cnt;
            }
        }
    }

    void Run()
    {
        using namespace std::chrono_literals;

        auto tstart = std::chrono::steady_clock::now();

        size_t idle_rnd = 0;
        local_cc_shards_.ProcessorSleepFlag(
            thd_id_, &in_sleep_, &sleep_mux_, &sleep_cv_);

        while (!terminated_.load(std::memory_order_acquire))
        {
            size_t tx_cnt = 0, req_cnt = 0;
            bool yield = false;
            RunOneRound(tx_cnt, req_cnt, yield);

#ifdef EXT_TX_PROC_ENABLED
            if (external_processor_num_.load(std::memory_order_relaxed) > 0)
            {
                if (yield)
                {
                    std::this_thread::sleep_for(1ms);
                }
                else if (tx_cnt + req_cnt > 0)
                {
                    std::this_thread::sleep_for(10us);
                }
                else
                {
                    std::this_thread::sleep_for(100us);
                }
            }
            else
            {
                std::this_thread::yield();
            }
#endif

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
                if (tnow - tstart >= 1000ms && IsIdle())
                {
                    idle_rnd = 0;

                    in_sleep_.store(true, std::memory_order_relaxed);

                    std::unique_lock<std::mutex> lk(sleep_mux_);
                    sleep_cv_.wait(lk, [this]() { return !IsIdle(); });

                    in_sleep_.store(false, std::memory_order_relaxed);
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

    void Terminate()
    {
        // decrease use_count of share pointer to TableSchema
        cc_hd_ = nullptr;

        terminated_.store(true, std::memory_order_relaxed);
        std::unique_lock<std::mutex> lk(sleep_mux_);
        sleep_cv_.notify_one();
    }

#ifdef EXT_TX_PROC_ENABLED
    std::function<void()> *ExtProcessorFunctor()
    {
        return &external_processor_func_;
    }
#endif

#ifdef EXT_TX_PROC_ENABLED
    std::function<void()> *ExtProcessorFunctor()
    {
        return &external_processor_func_;
    }

    std::atomic<bool> process_latch_{false};
    std::atomic<size_t> external_process_counter_{0};
    size_t internal_counter_{0};
    std::atomic<uint16_t> external_processor_num_{0};
    std::function<void()> external_processor_func_;
#endif

private:
    bool IsIdle()
    {
        return active_tx_cnt_.load(std::memory_order_relaxed) == 0 &&
               local_cc_shards_.IsIdle(thd_id_) &&
               !terminated_.load(std::memory_order_relaxed);
    }
    size_t thd_id_;
    std::atomic<bool> terminated_;
    std::atomic<bool> in_sleep_{false};

    LocalCcShards &local_cc_shards_;
    std::unique_ptr<LocalCcHandler> cc_hd_;

    std::atomic<uint16_t> active_tx_cnt_;
    std::atomic<uint16_t> new_tx_cnt_;
    moodycamel::ConcurrentQueue<TransactionExecution::uptr> new_txs_;
    moodycamel::ConsumerToken new_tx_token_;

    CircularQueue<TransactionExecution::uptr> idle_txs_{100};
    CircularQueue<TransactionExecution::uptr> on_fly_txs_{100};

    static moodycamel::ConcurrentQueue<TransactionExecution::uptr> free_txs;
    moodycamel::ProducerToken free_prod_token_;
    moodycamel::ConsumerToken free_consumer_token_;

    std::mutex sleep_mux_;
    std::condition_variable sleep_cv_;

    TxLog *txlog_hd_;

    metrics::TimePoint busy_round_start_;
    bool is_busy_round_{false};
    size_t busy_round_processed_cc_req_count_{0};
    size_t busy_round_active_tx_count_{0};
    size_t busy_round_threshold_{10};
    size_t empty_round_count_{0};
    size_t total_round_count_{0};
    size_t empty_round_threshold_{1000};

    const metrics::Name BUSY_ROUND_DURATION_NAME_{"busy_round_duration"};
    const metrics::Name BUSY_ROUND_ACTIVE_TX_COUNT_NAME_{
        "busy_round_active_tx_count"};
    const metrics::Name BUSY_ROUND_PROCESSED_CC_REQUEST_COUNT_NAME_{
        "busy_round_processed_cc_request_count"};
    const metrics::Name EMPTY_ROUND_RATIO_NAME_{"empty_round_ratio"};

public:
    std::unique_ptr<metrics::Meter> meter_;
    const metrics::Name TX_DURATION_NAME_{"tx_duration"};
    const metrics::Name TX_PROCESSED_TOTAL_NAME_{"tx_processed_total"};
    const metrics::Name REMOTE_REQUEST_DURATION_NAME_{
        "remote_request_duration"};
    const metrics::Name REMOTE_REQUEST_ON_FLY_COUNT_NAME_{
        "remote_request_on_fly_count"};

    friend class TxService;
    friend struct txservice::SplitFlushRangeOp;
};

class TxService
{
public:
    TxService(
        const std::string &local_path,
        CatalogFactory *catalog_factory,
        const std::map<std::string, uint32_t> &conf,
        uint32_t node_id,                                         // = 0,
        std::map<uint32_t, std::vector<NodeConfig>> *ng_configs,  // = nullptr,
        int32_t range_bucket_seed,                                // = -1,
        uint64_t cluster_config_version,                          // = 0,
        std::vector<std::string> *txlog_ips,                      // = nullptr,
        std::vector<uint16_t> *txlog_ports,                       // = nullptr,
        store::DataStoreHandler *store_hd,                        // = nullptr,
        std::unique_ptr<TxLog> log_hd,                            // = nullptr,
        bool enable_mvcc = true,
        bool skip_redo_log = false,
        metrics::MetricsRegistry *metrics_registry = nullptr,
        metrics::CommonLabels common_labels = {})
        : local_cc_shards_(node_id,
                           conf.find("core_num")->second,
                           conf.find("node_memory_limit_mb")->second,
                           conf.find("node_log_limit_mb")->second,
                           conf.find("realtime_sampling")->second,
                           catalog_factory,
                           ng_configs,
                           range_bucket_seed,
                           cluster_config_version,
                           store_hd,
                           this,
                           enable_mvcc,
                           metrics_registry,
                           common_labels),
          ckpt_(local_cc_shards_,
                store_hd,
                conf.find("checkpointer_interval")->second,
                log_hd.get(),
                conf.find("checkpointer_delay_seconds")->second)
    {
        uint32_t core_cnt = conf.find("core_num")->second;
        pool_.reserve(core_cnt);
        thd_pool_.reserve(core_cnt);

        for (uint16_t thd_idx = 0; thd_idx < core_cnt; ++thd_idx)
        {
            if (metrics::enable_collect_metrics)
            {
                common_labels["core_id"] = std::to_string(thd_idx);
                pool_.emplace_back(
                    std::make_unique<TxProcessor>(thd_idx,
                                                  local_cc_shards_,
                                                  log_hd.get(),
                                                  metrics_registry,
                                                  common_labels));
            }
            else
            {
                pool_.emplace_back(std::make_unique<TxProcessor>(
                    thd_idx, local_cc_shards_, log_hd.get()));
            }
        }

        Sharder::Instance().Init(node_id,
                                 ng_configs,
                                 cluster_config_version,
                                 txlog_ips,
                                 txlog_ports,
                                 &local_cc_shards_,
                                 std::move(log_hd),
                                 local_path);
        TxStartTsCollector::Instance().Init(
            &local_cc_shards_,
            conf.find("collect_active_tx_ts_interval_seconds")->second);
        DeadLockCheck::Init(local_cc_shards_);
        txservice_skip_redo_log = skip_redo_log;
    }

    void Start()
    {
        for (size_t thd_idx = 0; thd_idx < pool_.size(); ++thd_idx)
        {
            TxProcessor *tp = pool_[thd_idx].get();

            tp->InitializeLocalHandler();
            thd_pool_.emplace_back(std::thread([tp] { tp->Run(); }));
        }

        // Start cc stream receiver server.
        Sharder::Instance().StartCcStreamReceiver();

        if (local_cc_shards_.EnableMvcc())
        {
            TxStartTsCollector::Instance().Start();
        }
    }

    void WaitClusterReady()
    {
        Sharder::Instance().WaitClusterReady();
    }

    void Shutdown()
    {
        DeadLockCheck::SetStop();
        ckpt_.Terminate();
        ckpt_.Join();
        // Terminate the DataSync thds.
        local_cc_shards_.Terminate();
        if (local_cc_shards_.EnableMvcc())
        {
            TxStartTsCollector::Instance().Shutdown();
        }

        Sharder::Instance().Shutdown();

        for (size_t thd_idx = 0; thd_idx < thd_pool_.size(); ++thd_idx)
        {
            pool_[thd_idx]->Terminate();
        }
        for (auto &thd_idx : thd_pool_)
        {
            thd_idx.join();
        }

        // Maybe there has remote request in cache, so here close stream sender
        // after TxProcessor terminated.
        Sharder::Instance().CloseStreamSender();
        DeadLockCheck::Free();
    }

    TransactionExecution *NewTx()
    {
        uint32_t run_cnt = tx_runs_.fetch_add(1, std::memory_order_relaxed);
        size_t sid = run_cnt % pool_.size();
        return pool_[sid]->NewTx();
    }

    TransactionExecution::uptr NewTx(size_t shard_id)
    {
        size_t sid =
            shard_id < pool_.size() ? shard_id : (shard_id % pool_.size());
        return pool_[sid]->NewTxm();
    }

    void Recycle(size_t shard_id, TransactionExecution::uptr txm)
    {
        assert(shard_id < pool_.size());
        pool_[shard_id]->RecycleTxm(std::move(txm));
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
    // tx runs shared by all the clients of tx_service. It is used to
    // balance workloads between TxProcessors.
    std::atomic<uint32_t> tx_runs_{0};
    friend class txservice::fault::ReplayService;
};

}  // namespace txservice
