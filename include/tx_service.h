#pragma once

#include <bthread/bthread.h>
#include <bthread/task_group.h>
#include <butil/macros.h>
#include <mimalloc-2.1/mimalloc.h>
#include <pthread.h>

#include <algorithm>  // std::min
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
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
#include "concurrent_queue_wsize.h"
#include "dead_lock_check.h"
#include "local_cc_handler.h"
#include "local_cc_shards.h"
#include "spinlock.h"
#include "tx_execution.h"
#include "tx_request.h"
#include "tx_service_common.h"
#include "tx_service_metrics.h"
#include "tx_start_ts_collector.h"
#include "txlog.h"

using namespace std::chrono_literals;
namespace bthread
{
extern BAIDU_THREAD_LOCAL TaskGroup *tls_task_group;
};
namespace txservice
{

// whether skip write redo log to log_service.
inline bool txservice_skip_wal = false;
// whether skip accessing KV when cc map cache misses.
inline bool txservice_skip_kv = false;

// the OFFSET_TABLE contains only prime numbers
inline const size_t OFFSET_TABLE[] = {
#include "offset_inl.list"
};

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
    static const int64_t t1sec = 1000000L;
    static const int64_t t2sec = 4000000L;

    TxProcessor(size_t thd_id,
                LocalCcShards &shards,
                TxLog *txlog_hd,
                metrics::MetricsRegistry *metrics_registry = nullptr,
                metrics::CommonLabels common_labels = {})
        : thd_id_(thd_id),
          terminated_(false),
          tx_proc_status_(TxProcessorStatus::Busy),
          local_cc_shards_(shards),
          active_tx_cnt_(0),
          new_tx_cnt_(0),
          new_txs_(),
          new_tx_token_(new_txs_),
          free_txs_(),
          txlog_hd_(txlog_hd)
    {
        if (metrics::enable_busy_round_metrics)
        {
            auto meter = GetMeter();
            meter->Register(metrics::NAME_BUSY_ROUND_DURATION,
                            metrics::Type::Histogram);
            meter->Register(metrics::NAME_BUSY_ROUND_ACTIVE_TX_COUNT,
                            metrics::Type::Gauge);
            meter->Register(metrics::NAME_BUSY_ROUND_PROCESSED_CC_REQUEST_COUNT,
                            metrics::Type::Gauge);
            meter->Register(metrics::NAME_EMPTY_ROUND_RATIO,
                            metrics::Type::Gauge);
        }

        if (metrics::enable_tx_metrics)
        {
            auto meter = GetMeter();
            meter->Register(metrics::NAME_TX_DURATION,
                            metrics::Type::Histogram);
            meter->Register(metrics::NAME_TX_PROCESSED_TOTAL,
                            metrics::Type::Counter);
            meter->Register(metrics::NAME_REMOTE_REQUEST_DURATION,
                            metrics::Type::Histogram,
                            {{"type",
                              {"read",
                               "acquire_write",
                               "validate",
                               "post_process",
                               "scan_next",
                               "write_log"}}});
            meter->Register(metrics::NAME_IN_FLIGHT_REMOTE_REQUEST_COUNT,
                            metrics::Type::Gauge,
                            {{"type",
                              {"read",
                               "acquire_write",
                               "validate",
                               "post_process",
                               "scan_next",
                               "write_log"}}});
        }

        coordi_ = std::make_shared<TxProcCoordinator>();
    }

    ~TxProcessor() = default;

    metrics::Meter *GetMeter()
    {
        return local_cc_shards_.GetCcShard(thd_id_)->GetMeter();
    };

    TransactionExecution *NewTx()
    {
        TransactionExecution::uptr tx = nullptr;
        bool success = free_txs_.try_dequeue(tx);
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

        // The memory order of new_txs_.enqueue() ensures that the update of
        // active_tx_cnt_ happens before the new tx appearing in the queue.
        uint32_t prev_tx_cnt =
            active_tx_cnt_.fetch_add(1, std::memory_order_relaxed);

        new_tx_cnt_.fetch_add(1, std::memory_order_relaxed);
        // Add the new transaction into the new tx set.
        new_txs_.enqueue(std::move(tx));

        // Wakes up the tx processor thread if it is asleep.
        if (prev_tx_cnt == 0)
        {
            TxProcessorStatus native_proc_status =
                tx_proc_status_.load(std::memory_order_relaxed);
#ifdef EXT_TX_PROC_ENABLED
            if (native_proc_status == TxProcessorStatus::Sleep ||
                (coordi_->ext_processor_cnt_.load(std::memory_order_relaxed) ==
                     0 &&
                 native_proc_status == TxProcessorStatus::Standby))
            {
                Notify(coordi_->sleep_mux_, coordi_->sleep_cv_);
            }
#else
            if (native_proc_status == TxProcessorStatus::Sleep)
            {
                Notify(coordi_->sleep_mux_, coordi_->sleep_cv_);
            }
#endif
        }

        return tx_ptr;
    }

#ifdef EXT_TX_PROC_ENABLED
    TransactionExecution *NewExternalTx()
    {
        TransactionExecution::uptr tx = nullptr;
        bool success = free_txs_.try_dequeue(tx);
        if (success)
        {
            assert(tx != nullptr);
            tx->Restart(cc_hd_.get(), txlog_hd_, this, true);
        }
        else
        {
            tx = std::make_unique<TransactionExecution>(
                cc_hd_.get(), txlog_hd_, this, true);
        }

        TransactionExecution *tx_ptr = tx.get();
        active_tx_lock_.Lock();
        active_tx_map_.try_emplace(tx.get(), std::move(tx));
        active_tx_lock_.Unlock();
        return tx_ptr;
    }
#endif

    void RunOneRound(size_t &active_cnt,
                     size_t &req_cnt,
                     bool &yield
#ifdef EXT_TX_PROC_ENABLED
                     ,
                     std::atomic<TxShardStatus> &shard_status,
                     bool is_ext_proc
#endif
    )
    {
#ifdef EXT_TX_PROC_ENABLED
        TxShardStatus expected = TxShardStatus::Free;
        bool success = shard_status.compare_exchange_strong(
            expected, TxShardStatus::Occupied, std::memory_order_acq_rel);
        if (!success)
        {
            active_cnt = 0;
            req_cnt = 0;
            yield = true;
            return;
        }
        CcShard *shard = local_cc_shards_.GetCcShard(thd_id_);
        CcShardHeap *shard_heap = shard->GetShardHeap();
        if (shard_heap == nullptr)
        {
            assert(is_ext_proc);
            shard_status.store(TxShardStatus::Free, std::memory_order_release);
            return;
        }
        mi_heap_t *prev_heap = shard_heap->SetAsDefaultHeap();
        if (is_ext_proc)
        {
            // Override thread id as well if current thread id is not heap owner
            // thread id.
            shard->OverrideHeapThread();
            coordi_->ext_tx_proc_heap_ = prev_heap;
        }
        one_round_cnt_.fetch_add(1, std::memory_order_relaxed);
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
                free_txs_.enqueue(std::move(tx));
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
            auto meter = GetMeter();
            meter->CollectDuration(metrics::NAME_BUSY_ROUND_DURATION,
                                   busy_round_start_);
            meter->Collect(metrics::NAME_BUSY_ROUND_ACTIVE_TX_COUNT,
                           busy_round_active_tx_count_);
            meter->Collect(metrics::NAME_BUSY_ROUND_PROCESSED_CC_REQUEST_COUNT,
                           busy_round_processed_cc_req_count_);
            is_busy_round_ = false;
        }

#ifdef EXT_TX_PROC_ENABLED
        size_t loop_cnt = 3;
        CheckWaitingTxs();
#else
        size_t loop_cnt = 5;
#endif

#ifdef ON_KEY_OBJECT
        loop_cnt = 1;
#endif

        for (size_t loop = 0; loop < loop_cnt; ++loop)
        {
#ifdef EXT_TX_PROC_ENABLED
            if (is_ext_proc)
            {
                size_t resume_cnt = resume_tx_queue_.SizeApprox();
                while (resume_cnt > 0)
                {
                    std::array<TransactionExecution *, 100> tx_bulk;
                    size_t deque_cap = std::min(resume_cnt, tx_bulk.size());
                    size_t deque_size = resume_tx_queue_.TryDequeueBulk(
                        tx_bulk.begin(), deque_cap);

                    for (size_t idx = 0; idx < deque_size; ++idx)
                    {
                        TransactionExecution *tx_ptr = tx_bulk[idx];
                        if (tx_ptr->TxStatus() == TxnStatus::Finished)
                        {
                            continue;
                        }

                        TxmStatus txm_status = tx_ptr->Forward();
                        if (txm_status == TxmStatus::Finished)
                        {
                            active_tx_lock_.Lock();
                            auto it = active_tx_map_.find(tx_ptr);
                            if (it == active_tx_map_.end())
                            {
                                active_tx_lock_.Unlock();
                                continue;
                            }

                            TransactionExecution::uptr tx_uptr =
                                std::move(it->second);
                            active_tx_map_.erase(it);
                            active_tx_lock_.Unlock();
                            tx_progress_.erase(tx_uptr.get());

                            free_txs_.enqueue(std::move(tx_uptr));
                        }
                    }

                    resume_cnt = resume_tx_queue_.SizeApprox();
                }
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
                    free_txs_.enqueue(std::move(tx));
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
                local_cc_shards_.QueueSize(thd_id_) >=
                    metrics::busy_round_threshold)
            {
                is_busy_round_ = true;
                busy_round_start_ = metrics::Clock::now();
            }

            // Process CcRequests.
            req_cnt += local_cc_shards_.ProcessRequests(thd_id_);
        }

        active_cnt =
            on_fly_txs_.Size() + new_tx_cnt_.load(std::memory_order_relaxed);

#ifdef EXT_TX_PROC_ENABLED

        mi_heap_set_default(prev_heap);
        if (is_ext_proc)
        {
            assert(coordi_->ext_tx_proc_heap_ != nullptr);
            mi_restore_default_thread_id();
            coordi_->ext_tx_proc_heap_ = nullptr;
        }
        shard_status.store(TxShardStatus::Free, std::memory_order_release);
#endif

        if (metrics::enable_busy_round_metrics)
        {
            empty_round_count_ += req_cnt == 0 ? 1 : 0;
            if (++total_round_count_ == empty_round_threshold_)
            {
                auto meter = GetMeter();
                meter->Collect(metrics::NAME_EMPTY_ROUND_RATIO,
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
        local_cc_shards_.GetCcShard(thd_id_)->InitializeShardHeap();
        local_cc_shards_.SetTxProcNotifier(
            thd_id_, &tx_proc_status_, coordi_.get());

#ifdef EXT_TX_PROC_ENABLED
        size_t local_round_cnt = one_round_cnt_.load(std::memory_order_relaxed);
#endif

        while (!terminated_.load(std::memory_order_relaxed))
        {
            size_t tx_cnt = 0, req_cnt = 0;
            bool yield = false;

#ifdef EXT_TX_PROC_ENABLED
            RunOneRound(tx_cnt, req_cnt, yield, coordi_->shard_status_, false);
            ++local_round_cnt;

            size_t round_cnt = one_round_cnt_.load(std::memory_order_relaxed);
            bool has_ext_proc =
                coordi_->ext_processor_cnt_.load(std::memory_order_relaxed) > 0;
            bool is_ext_proc_active = local_round_cnt != round_cnt;

            if (yield ||
                (has_ext_proc && (is_ext_proc_active ||
                                  (req_cnt + tx_cnt == 0 && idle_rnd > 1000))))
            {
                tx_cnt = 0;
                req_cnt = 0;
                idle_rnd = 0;

                tx_proc_status_.store(TxProcessorStatus::Standby,
                                      std::memory_order_relaxed);

                std::unique_lock<std::mutex> lk(coordi_->sleep_mux_);
                do
                {
                    local_round_cnt = round_cnt;
                    // LOG(INFO) << "native thd yield sleeps, core #" << thd_id_
                    //           << ", round cnt: " << local_round_cnt;
                    bool no_ext_proc = coordi_->sleep_cv_.wait_for(
                        lk,
                        2s,
                        [this]()
                        {
                            return coordi_->ext_processor_cnt_.load(
                                       std::memory_order_relaxed) == 0 ||
                                   terminated_.load(std::memory_order_relaxed);
                        });

                    round_cnt = one_round_cnt_.load(std::memory_order_relaxed);
                    // LOG(INFO) << "native thd yield wakes up, core #" <<
                    // thd_id_
                    //           << ", no ext proc: " << (int) no_ext_proc
                    //           << ", round cnt: " << round_cnt;

                    // If the round counter is not incremented since last sleep,
                    // it means that there is no external processor, or the
                    // external processor has not visited the shard for a while.
                    // Steps out of the standby mode to forward tx's and process
                    // cc requests.
                    if (no_ext_proc || round_cnt == local_round_cnt)
                    {
                        local_round_cnt = round_cnt;
                        break;
                    }
                } while (!terminated_.load(std::memory_order_relaxed));

                tx_proc_status_.store(TxProcessorStatus::Busy,
                                      std::memory_order_relaxed);
            }
#else
            RunOneRound(tx_cnt, req_cnt, yield);
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

                    tx_proc_status_.store(TxProcessorStatus::Sleep,
                                          std::memory_order_relaxed);

                    // LOG(INFO) << "native thd long sleeps, core #" << thd_id_
                    //           << ", round cnt: " << local_round_cnt;

                    std::unique_lock<std::mutex> lk(coordi_->sleep_mux_);
                    coordi_->sleep_cv_.wait(lk, [this]() { return !IsIdle(); });

#ifdef EXT_TX_PROC_ENABLED
                    local_round_cnt =
                        one_round_cnt_.load(std::memory_order_relaxed);
                    // LOG(INFO) << "native thd long wakes up, core #" <<
                    // thd_id_
                    //           << ", round cnt: " << local_round_cnt;
#endif
                    tx_proc_status_.store(TxProcessorStatus::Busy,
                                          std::memory_order_relaxed);
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
        TxShardStatus expected = TxShardStatus::Free;
        while (!coordi_->shard_status_.compare_exchange_weak(
            expected, TxShardStatus::Deconstructed, std::memory_order_acq_rel))
        {
            expected = TxShardStatus::Free;
        }

        // decrease use_count of share pointer to TableSchema
        cc_hd_ = nullptr;

        {
            std::unique_lock<std::mutex> lk(coordi_->sleep_mux_);
            terminated_.store(true, std::memory_order_relaxed);
            coordi_->sleep_cv_.notify_one();
        }
    }

#ifdef EXT_TX_PROC_ENABLED
    std::function<void()> TxProcessorFunctor()
    {
        return [this, coordi = coordi_]()
        {
            size_t active_cnt = 0, req_cnt = 0;
            bool yield = false;
            RunOneRound(
                active_cnt, req_cnt, yield, coordi->shard_status_, true);
        };
    }

    std::function<void(int16_t)> UpdateExtProcFunctor()
    {
        return [this, coordi = coordi_](int16_t thd_delta)
        {
            int16_t ext_thd_cnt = coordi->ext_processor_cnt_.fetch_add(
                thd_delta, std::memory_order_relaxed);

            ext_thd_cnt += thd_delta;
            assert(ext_thd_cnt >= 0);

            // There is no external thread anymore. Wakes up the native tx
            // processor.
            if (ext_thd_cnt == 0)
            {
                Notify(coordi->sleep_mux_, coordi->sleep_cv_);
            }
        };
    }

    std::function<bool(bool)> OverrideShardHeapFunctor()
    {
        return [this](bool yield)
        {
            if (yield)
            {
                // Since only brpc worker thread will read and modify
                // coordi_->ext_tx_proc_heap_, it is safe to directly
                // access without lock.
                if (coordi_->ext_tx_proc_heap_)
                {
                    // tx proc is occupied by ext tx processor.
                    mi_heap_set_default(coordi_->ext_tx_proc_heap_);
                    mi_restore_default_thread_id();
                    coordi_->ext_tx_proc_heap_ = nullptr;
                    return true;
                }
            }
            else
            {
                CcShard *shard = local_cc_shards_.GetCcShard(thd_id_);
                CcShardHeap *shard_heap = shard->GetShardHeap();
                assert(shard_heap);
                shard->OverrideHeapThread();
                coordi_->ext_tx_proc_heap_ = shard_heap->SetAsDefaultHeap();
            }
            return false;
        };
    }

    void EnlistTx(TransactionExecution *txm)
    {
        resume_tx_queue_.Enqueue(txm);
    }

    /**
     * @brief Lets the external processor to forward the tx state machine.
     * Forwarding needs to hold the latch of the tx shard, because it accesses
     * thread-unsafe resources (such as cc handler) belonging to the tx shard.
     *
     * @param txm
     * @return true, if the external processor acquires the latch successfully
     * and forwards the tx state machine.
     * @return false, if someone else is holding the latch. The tx will be
     * enlisted into the resume queue for later execution.
     */
    bool ForwardTx(TransactionExecution *txm)
    {
#ifdef ON_KEY_OBJECT
        assert(bthread::tls_task_group->group_id_ >= 0);
        if (bthread::tls_task_group->group_id_ != (int32_t) thd_id_)
        {
            // For redis a tx life cycle can spread across multiple cmds, which
            // might be put into different bthread task group. If the task group
            // id does not match the tx processor id, it is not safe to forward
            // txm.
            return false;
        }
#endif
        TxShardStatus expected = TxShardStatus::Free;
        bool success = coordi_->shard_status_.compare_exchange_strong(
            expected, TxShardStatus::Occupied, std::memory_order_acquire);
        if (!success)
        {
            return false;
        }
        // Override default heap since we're accessing txm in cc shard.
        CcShard *shard = local_cc_shards_.GetCcShard(thd_id_);
        CcShardHeap *shard_heap = shard->GetShardHeap();
        shard->OverrideHeapThread();
        coordi_->ext_tx_proc_heap_ = shard_heap->SetAsDefaultHeap();

        TxmStatus txm_status = txm->Forward();
        if (txm_status == TxmStatus::Finished)
        {
            active_tx_lock_.Lock();
            auto it = active_tx_map_.find(txm);
            if (it != active_tx_map_.end())
            {
                TransactionExecution::uptr tx_uptr = std::move(it->second);
                active_tx_map_.erase(it);
                active_tx_lock_.Unlock();
                tx_progress_.erase(tx_uptr.get());

                free_txs_.enqueue(std::move(tx_uptr));
            }
            else
            {
                active_tx_lock_.Unlock();
            }
        }
        mi_heap_set_default(coordi_->ext_tx_proc_heap_);
        mi_restore_default_thread_id();
        coordi_->ext_tx_proc_heap_ = nullptr;
        assert(coordi_->shard_status_.load(std::memory_order_relaxed) ==
               TxShardStatus::Occupied);
        coordi_->shard_status_.store(TxShardStatus::Free,
                                     std::memory_order_release);
        return true;
    }

    void CheckWaitingTxs()
    {
        static const uint64_t check_progress_period = 2000000;
        uint64_t now_ts = LocalCcShards::ClockTs();
        if (now_ts - progress_check_ts_ <= check_progress_period)
        {
            return;
        }

        for (auto &[tx, progress] : tx_progress_)
        {
            // If the tx has been stuck on the same command for a while, enlists
            // the tx for execution.
            uint16_t cmd_id = tx->CommandId();
            if (now_ts - progress.wait_clock_ts_ > check_progress_period &&
                cmd_id == progress.cmd_id_)
            {
                EnlistTx(tx);
            }
            else if (cmd_id > progress.cmd_id_)
            {
                progress.cmd_id_ = cmd_id;
            }
        }

        progress_check_ts_ = now_ts;
    }

    void EnlistWaitingTx(TransactionExecution *txm)
    {
        uint16_t cmd_id = txm->CommandId();
        uint64_t clock_ts = LocalCcShards::ClockTs();
        auto tx_it = tx_progress_.try_emplace(txm, cmd_id, clock_ts);
        if (!tx_it.second)
        {
            tx_it.first->second.cmd_id_ = cmd_id;
            tx_it.first->second.wait_clock_ts_ = clock_ts;
        }
    }
#endif

private:
    bool IsIdle()
    {
        return active_tx_cnt_.load(std::memory_order_relaxed) == 0 &&
               local_cc_shards_.IsIdle(thd_id_) &&
               !terminated_.load(std::memory_order_relaxed);
    }

    /**
     * @brief Notifies the tx processor that a tx or a cc request waits to be
     * processed and wakes up the processor if it is asleep and there is no
     * exteranl thread to process. Even though in general std::mutex is not need
     * for cv.notify(), the method intentionally places std::mutex before
     * notify(). This is because tx's or cc requests are managed via lock-free
     * data structures. The std::mutex in this method creates a barrier, forcing
     * the lock-free mutations to precede cv.notify(), so that whoever woken up
     * will see the effects of the mutations. Moreover, it creates a critical
     * section such that the to-sleep tx processor either precedes cv.notify(),
     * thereby being woken up by the notify signal, or succeeds cv.notify(),
     * thereby detecting the tx or cc request mutations and thus not entering
     * the sleep mode.
     *
     */
    void Notify(std::mutex &sleep_mux, std::condition_variable &sleep_cv)
    {
        std::unique_lock<std::mutex> lk(sleep_mux);
        sleep_cv.notify_one();
    }

    /**
     * @brief This method is only utilized for sampling the tx_duration metric.
     */
    inline size_t CheckAndUpdateTxCurrentRound()
    {
        return (tx_current_round_++ % metrics::collect_tx_duration_round) == 0;
    };

    size_t thd_id_;
    std::atomic<bool> terminated_;
    std::atomic<TxProcessorStatus> tx_proc_status_{TxProcessorStatus::Busy};

    LocalCcShards &local_cc_shards_;
    std::unique_ptr<LocalCcHandler> cc_hd_;

    std::atomic<uint16_t> active_tx_cnt_;
    std::atomic<uint16_t> new_tx_cnt_;
    moodycamel::ConcurrentQueue<TransactionExecution::uptr> new_txs_;
    moodycamel::ConsumerToken new_tx_token_;

    CircularQueue<TransactionExecution::uptr> idle_txs_{100};
    CircularQueue<TransactionExecution::uptr> on_fly_txs_{100};

    moodycamel::ConcurrentQueue<TransactionExecution::uptr> free_txs_;

    TxLog *txlog_hd_;

    std::shared_ptr<TxProcCoordinator> coordi_;

#ifdef EXT_TX_PROC_ENABLED
    /**
     * @brief The number of rounds this shard has been processed. We use the
     * number to track if external threads have visited the shard for a certain
     * amount of time, and if not (because of external threads being occupied),
     * wake up the native tx processor to process the shard's binding active
     * tx's and cc requests.
     *
     */
    std::atomic<size_t> one_round_cnt_{0};

    std::unordered_map<TransactionExecution *, TransactionExecution::uptr>
        active_tx_map_;
    SimpleSpinlock active_tx_lock_;
    ConcurrentQueueWSize<TransactionExecution *> resume_tx_queue_;

    struct TxProgress
    {
        TxProgress() = delete;
        TxProgress(uint16_t cmd_id, uint64_t clock_ts)
            : cmd_id_(cmd_id), wait_clock_ts_(clock_ts)
        {
        }

        uint16_t cmd_id_;
        uint64_t wait_clock_ts_;
    };

    std::unordered_map<TransactionExecution *, TxProgress> tx_progress_;
    uint64_t progress_check_ts_{0};
#endif

    metrics::TimePoint busy_round_start_;
    bool is_busy_round_{false};
    size_t busy_round_processed_cc_req_count_{0};
    size_t busy_round_active_tx_count_{0};
    size_t empty_round_count_{0};
    size_t total_round_count_{0};
    size_t empty_round_threshold_{1000};

    // tx_current_round_ is only utilized for sampling the tx_duration and
    // remote request metric.
    size_t tx_current_round_{1};

public:
    friend class TxService;
    friend struct txservice::SplitFlushRangeOp;
    friend class TransactionExecution;
};

class TxService
{
public:
    TxService(
        CatalogFactory *catalog_factory,
        SystemHandler *system_handler,
        const std::map<std::string, uint32_t> &conf,
        uint32_t node_id,  // = 0,
        std::unordered_map<uint32_t, std::vector<NodeConfig>>
            *ng_configs,                    // = nullptr,
        int32_t range_bucket_seed,          // = -1,
        uint64_t cluster_config_version,    // = 0,
        store::DataStoreHandler *store_hd,  // = nullptr,
        TxLog *log_hd,                      // = nullptr,
        bool enable_mvcc = true,
        bool skip_wal = false,
        bool skip_kv = false,  // only used in mono_redis
        metrics::MetricsRegistry *metrics_registry = nullptr,
        metrics::CommonLabels common_labels = {},
        std::unordered_map<TableName, std::string> *prebuilt_tables = nullptr,
        std::function<void(std::string_view, std::string_view)> publish_func =
            nullptr)
        : local_cc_shards_(node_id,
                           conf.at("core_num"),
                           conf.at("range_split_worker_num"),
                           conf.at("node_memory_limit_mb"),
                           conf.at("node_log_limit_mb"),
                           conf.at("realtime_sampling"),
                           catalog_factory,
                           system_handler,
                           ng_configs,
                           range_bucket_seed,
                           cluster_config_version,
                           store_hd,
                           this,
                           enable_mvcc,
                           metrics_registry,
                           common_labels,
                           prebuilt_tables,
                           publish_func),
          ckpt_(local_cc_shards_,
                store_hd,
                conf.at("checkpointer_interval"),
                log_hd,
                conf.at("checkpointer_delay_seconds"))
    {
        assert(store_hd != nullptr || skip_kv);
        uint32_t core_cnt = conf.at("core_num");
        pool_.reserve(core_cnt);
        thd_pool_.reserve(core_cnt);

        for (uint16_t thd_idx = 0; thd_idx < core_cnt; ++thd_idx)
        {
            if (metrics::enable_metrics)
            {
                common_labels["core_id"] = std::to_string(thd_idx);
                pool_.emplace_back(
                    std::make_unique<TxProcessor>(thd_idx,
                                                  local_cc_shards_,
                                                  log_hd,
                                                  metrics_registry,
                                                  common_labels));
            }
            else
            {
                pool_.emplace_back(std::make_unique<TxProcessor>(
                    thd_idx, local_cc_shards_, log_hd));
            }
        }

        txservice_skip_wal = skip_wal;
        txservice_skip_kv = skip_kv;
    }

    int Start(uint32_t node_id,
              const std::unordered_map<NodeGroupId, std::vector<NodeConfig>>
                  *ng_configs,
              uint64_t cluster_config_version,
              const std::vector<std::string> *txlog_ips,
              const std::vector<uint16_t> *txlog_ports,
              const std::string *hm_ip,
              const uint16_t *hm_port,
              const std::string *hm_bin_path,
              const std::map<std::string, uint32_t> &conf,
              std::unique_ptr<TxLog> log_agent,
              const std::string &local_path)
    {
        uint16_t ng_rep_cnt = (uint16_t) conf.at("rep_group_cnt");
        if (Sharder::Instance().Init(node_id,
                                     ng_configs,
                                     cluster_config_version,
                                     txlog_ips,
                                     txlog_ports,
                                     hm_ip,
                                     hm_port,
                                     hm_bin_path,
                                     &local_cc_shards_,
                                     std::move(log_agent),
                                     local_path,
                                     ng_rep_cnt) < 0)

        {
            return -1;
        }
        TxStartTsCollector::Instance().Init(
            &local_cc_shards_,
            conf.at("collect_active_tx_ts_interval_seconds"));
        DeadLockCheck::Init(local_cc_shards_);
        for (size_t thd_idx = 0; thd_idx < pool_.size(); ++thd_idx)
        {
            TxProcessor *tp = pool_[thd_idx].get();

            tp->InitializeLocalHandler();
            thd_pool_.emplace_back(std::thread([tp] { tp->Run(); }));
        }
#if defined(EXT_TX_PROC_ENABLED) && defined(ON_KEY_OBJECT)
        // set ext_tx_prc_func to brpc
        bthread_set_ext_tx_prc_func(GetTxProcFunctors());
#endif

        // Start cc stream receiver server.
        Sharder::Instance().StartCcStreamReceiver();

        if (local_cc_shards_.EnableMvcc())
        {
            TxStartTsCollector::Instance().Start();
        }
        local_cc_shards_.StartBackgroudWorkers();
        return 0;
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
        // The rand seed will be initialized automatically.
        static thread_local uint32_t tx_runs = butil::fast_rand();
        // Based on the OFFSET_TABLE, each thread has its own tx_run pattern,
        // and the workloads are balanced between TxProcessors.
        static thread_local uint32_t tx_run_offset =
            OFFSET_TABLE[tx_runs % ARRAY_SIZE(OFFSET_TABLE)];
        uint32_t run_cnt = tx_runs;
        tx_runs += tx_run_offset;
        size_t sid = run_cnt % pool_.size();
        return pool_[sid]->NewTx();
    }

#ifdef EXT_TX_PROC_ENABLED
    TransactionExecution *NewTx(size_t shard_id)
    {
        size_t sid =
            shard_id < pool_.size() ? shard_id : (shard_id % pool_.size());
        return pool_[sid]->NewExternalTx();
    }

#ifdef ON_KEY_OBJECT
    std::function<std::tuple<std::function<void()>,
                             std::function<void(int16_t)>,
                             std::function<bool(bool)>>(int16_t)>
    GetTxProcFunctors()
    {
        return [this](int16_t group_id)
        {
            assert(group_id >= 0);
            int16_t sid = group_id % pool_.size();
            return std::make_tuple(pool_[sid]->TxProcessorFunctor(),
                                   pool_[sid]->UpdateExtProcFunctor(),
                                   pool_[sid]->OverrideShardHeapFunctor());
        };
    }
#else
    std::function<
        std::pair<std::function<void()>, std::function<void(int16_t)>>(int16_t)>
    GetTxProcFunctors()
    {
        return [this](int16_t group_id)
        {
            assert(group_id >= 0);
            int16_t sid = group_id % pool_.size();
            return std::make_pair(pool_[sid]->TxProcessorFunctor(),
                                  pool_[sid]->UpdateExtProcFunctor());
        };
    }
#endif
#endif

    LocalCcShards &CcShards()
    {
        return local_cc_shards_;
    }

    LocalCcShards local_cc_shards_;
    std::vector<std::unique_ptr<TxProcessor>> pool_;
    std::vector<std::thread> thd_pool_;
    Checkpointer ckpt_;

    friend class txservice::fault::ReplayService;
};

}  // namespace txservice
