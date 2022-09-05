#pragma once

#include <chrono>
#include <list>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

#include "catalog.h"
#include "catalog_factory.h"
#include "checkpointer.h"
#include "local_cc_handler.h"
#include "local_cc_shards.h"
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

    TxProcessor(size_t thd_id, LocalCcShards &shards, TxLog *txlog_hd)
        : thd_id_(thd_id),
          terminated_(false),
          local_cc_shards_(shards),
          free_tx_list_(),
          txlog_hd_(txlog_hd)
    {
    }

    TransactionExecution *NewTx()
    {
        TransactionExecution *tx_ptr = nullptr;
        bool found = free_tx_list_.try_dequeue(tx_ptr);

        if (found)
        {
            tx_ptr->Restart();
        }
        else
        {
            std::unique_ptr<TransactionExecution> new_tx =
                std::make_unique<TransactionExecution>(
                    cc_hd_.get(), txlog_hd_, this);

            tx_ptr = new_tx.get();
            txs_.enqueue(std::move(new_tx));
        }

        return tx_ptr;
    }

    void RunOneRound(size_t &tx_cnt, size_t &req_cnt)
    {
        TransactionExecution *txs[100];

        size_t tx_batch = to_exec_txs_.try_dequeue_bulk(txs, 100);

        tx_cnt = tx_batch;
        while (tx_batch > 0)
        {
            for (size_t tx_idx = 0; tx_idx < tx_batch; ++tx_idx)
            {
                txs[tx_idx]->Forward();
                if (txs[tx_idx]->TxStatus() == TxnStatus::Finished)
                {
                    txs[tx_idx]->Recycle();
                    free_tx_list_.enqueue(txs[tx_idx]);
                }
            }
            tx_batch = to_exec_txs_.try_dequeue_bulk(txs, 100);
            tx_cnt += tx_batch;
        }

        req_cnt = local_cc_shards_.ProcessRequests(thd_id_);
    }

    void Run()
    {
        size_t idle_rounds = 0;
        size_t busy_rounds = 0;
        auto idle_start = std::chrono::system_clock::now();

        while (!terminated_.load(std::memory_order_relaxed))
        {
            size_t tx_cnt = 0, req_cnt = 0;
            RunOneRound(tx_cnt, req_cnt);

            if (tx_cnt > 0 || req_cnt > 0)
            {
                idle_rounds = 0;
                ++busy_rounds;

                // For every 65536 rounds, checks tx's in the waiting queue.
                if ((busy_rounds & 0xFFFF) == 0)
                {
                    CheckWaitingTx();
                }

                continue;
            }

            busy_rounds = 0;
            if (idle_rounds == 0)
            {
                idle_start = std::chrono::system_clock::now();
            }

            ++idle_rounds;

            if ((idle_rounds & 0xFFFF) != 0)
            {
                continue;
            }

            // For every 65536 rounds, checks if the tx processor has been idle
            // for 1 second. If so, the tx processor enters into the sleep mode.
            auto now_time = std::chrono::system_clock::now();
            while (now_time - idle_start > 2s)
            {
                auto [enlist_tx_cnt, waiting_queue_size] = CheckWaitingTx();

                if (enlist_tx_cnt > 0)
                {
                    idle_rounds = 0;
                    break;
                }

                bool woke_up = false;
                // When the waiting queue is not empty, wakes up periodically to
                // check if the waiting tx's time out.
                if (waiting_queue_size > 0)
                {
                    std::unique_lock<std::mutex> lk(sleep_mux_);
                    woke_up = sleep_cv_.wait_for(
                        lk,
                        1s,
                        [this]
                        {
                            int64_t now_ts =
                                std::chrono::duration_cast<
                                    std::chrono::microseconds>(
                                    std::chrono::system_clock::now()
                                        .time_since_epoch())
                                    .count();
                            int64_t last_wakeup_ts =
                                last_wakeup_ts_.load(std::memory_order_relaxed);

                            // The last wakeup timestamp is updated by
                            // tx's who get timestamps from the local clock
                            // at LocalCcShards. Since the clock is sync'ed
                            // with real time in roughly every 2 seconds,
                            // there could be up to a 2-second delay. So,
                            // wakes the tx processor as long as the last
                            // wakeup request falls into the 2-second
                            // window.
                            return terminated_.load(
                                       std::memory_order_relaxed) ||
                                   now_ts - last_wakeup_ts <= t2sec;
                        });
                }
                else
                {
                    std::unique_lock<std::mutex> lk(sleep_mux_);

                    sleep_cv_.wait(
                        lk,
                        [this]
                        {
                            int64_t now_ts =
                                std::chrono::duration_cast<
                                    std::chrono::microseconds>(
                                    std::chrono::system_clock::now()
                                        .time_since_epoch())
                                    .count();
                            int64_t last_wakeup_ts =
                                last_wakeup_ts_.load(std::memory_order_relaxed);

                            // The last wakeup timestamp is updated by
                            // tx's who get timestamps from the local clock
                            // at LocalCcShards. Since the clock is sync'ed
                            // with real time in roughly every 2 seconds,
                            // there could be up to a 2-second delay. So,
                            // wakes the tx processor as long as the last
                            // wakeup request falls into the 2-second
                            // window.
                            return terminated_.load(
                                       std::memory_order_relaxed) ||
                                   now_ts - last_wakeup_ts <= t2sec;
                        });

                    woke_up = true;
                }

                if (woke_up)
                {
                    // The tx processor is woken up by a signal. Exits the sleep
                    // mode.
                    idle_rounds = 0;
                    break;
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

    std::pair<uint32_t, uint32_t> CheckWaitingTx()
    {
        uint32_t enlist_cnt = 0;
        int64_t now_ts = NowTs();

        std::lock_guard<std::mutex> lk(waiting_queue_mux_);

        for (auto tx_it = waiting_txs_.begin(); tx_it != waiting_txs_.end();
             ++tx_it)
        {
            int64_t wait_start_ts = tx_it->second.wait_start_ts_;
            if (now_ts - wait_start_ts >= t1sec)
            {
                to_exec_txs_.enqueue(tx_it->first);
                ++enlist_cnt;
            }
        }

        return {enlist_cnt, waiting_txs_.size()};
    }

    /**
     * @brief Enlists the input tx into the waiting queue. Tx's in the queue are
     * periodically visited to check timeout. Timeout happens when a tx sends
     * one or more remote requests but does not receive all responses due to
     * various failures.
     *
     * @param txm The tx state machine to enlist for waiting
     */
    void EnlistWaitingTx(TransactionExecution *txm)
    {
        std::lock_guard<std::mutex> lk(waiting_queue_mux_);
        auto tx_it = waiting_txs_.try_emplace(txm, NowTs());

        if (!tx_it.second)
        {
            ++tx_it.first->second.remote_cnt_;
        }
    }

    /**
     * @brief Enlists the input tx for execution. Enlisting is either because
     * the input tx is previously blocked on a cc request and receives the
     * response, or because the tx receives a tx request from the external user.
     *
     * @param txm The tx state machine to enlist for execution
     * @param remote_response Whether or not enlisting is triggered by a remote
     * response.
     */
    void EnlistExecutingTx(TransactionExecution *txm,
                           bool remote_response,
                           bool skip_remote_cnt)
    {
        if (remote_response)
        {
            std::lock_guard<std::mutex> lk(waiting_queue_mux_);
            if (skip_remote_cnt)
            {
                waiting_txs_.erase(txm);
            }
            else
            {
                auto waiting_tx_it = waiting_txs_.find(txm);
                if (waiting_tx_it != waiting_txs_.end())
                {
                    if (waiting_tx_it->second.remote_cnt_ > 0)
                    {
                        --waiting_tx_it->second.remote_cnt_;
                    }

                    if (waiting_tx_it->second.remote_cnt_ > 0)
                    {
                        return;
                    }
                    else
                    {
                        waiting_txs_.erase(waiting_tx_it);
                    }
                }
            }
        }

        to_exec_txs_.enqueue(txm);
        WakesUp();
    }

    /**
     * @brief Removes the tx from the waiting queue. This is called when the tx
     * has timed out and is about to move forward to retry or abort.
     *
     * @param txm
     */
    void RemoveWaitingTx(TransactionExecution *txm)
    {
        std::lock_guard<std::mutex> lk(waiting_queue_mux_);
        waiting_txs_.erase(txm);
    }

    /**
     * @brief Wakes up the tx processor in case it is in the sleep mode. The tx
     * processor is woken up, when (1) one of its binding tx's is enlisted for
     * execution, or (2) a cc request is dispatched to its cc request queue for
     * processing.
     *
     * @param wakeup_ts
     */
    void WakesUp()
    {
        uint64_t wakeup_ts = NowTs();
        uint64_t last_wakeup_ts =
            last_wakeup_ts_.load(std::memory_order_relaxed);
        uint64_t current_wakeup_ts = last_wakeup_ts;

        while (current_wakeup_ts < wakeup_ts)
        {
            if (last_wakeup_ts_.compare_exchange_weak(
                    current_wakeup_ts,
                    std::max(current_wakeup_ts, wakeup_ts),
                    std::memory_order_acq_rel))
            {
                break;
            }
        }

        // If the caller's wakeup timestamp is 1 second greater than last
        // time when someone tries to wake up the processor, it's possible
        // that the processor is in the sleep mode. Sends a wakeup signal
        // via the conditional variable.
        if (wakeup_ts < last_wakeup_ts || wakeup_ts - last_wakeup_ts >= t1sec)
        {
            // Locks the mutex before waking up the tx processor via the
            // conditional variable. This ensures that the notification
            // signal either precedes or follows the critical section in
            // which the tx processor enters into the sleep mode. When the
            // notification precedes, the tx processor sees the updated
            // wakeup timestamp and only enters into the sleep mode after
            // re-checking the condition. When the notification follows, the
            // notification signal wakes up the sleeping tx processor.
            std::lock_guard<std::mutex> lk(sleep_mux_);
            sleep_cv_.notify_one();
        }
    }

    uint64_t NowTs() const
    {
        return local_cc_shards_.ShardClockTs(thd_id_);
    }

    size_t thd_id_;
    std::atomic<bool> terminated_;

    LocalCcShards &local_cc_shards_;
    std::unique_ptr<LocalCcHandler> cc_hd_;

    moodycamel::ConcurrentQueue<TransactionExecution::uptr> txs_;
    moodycamel::ConcurrentQueue<TransactionExecution *> free_tx_list_;

    /**
     * @brief A collection of tx's who are ready to be executed. A tx is ready
     * to be executed, if it (a) receives the response from a local/remote cc
     * request, (b) receives a new tx request from the tx user, or (c) times out
     * on the prior cc request.
     *
     */
    moodycamel::ConcurrentQueue<TransactionExecution *> to_exec_txs_;
    std::mutex sleep_mux_;
    std::condition_variable sleep_cv_;

    struct WaitStatus
    {
        WaitStatus() = delete;
        WaitStatus(uint64_t wait_start_ts)
            : wait_start_ts_(wait_start_ts), remote_cnt_(1)
        {
        }

        uint64_t wait_start_ts_;
        uint32_t remote_cnt_;
    };

    /**
     * @brief A collection of tx's being blocked on remote cc requests and the
     * timestamp when they were blocked.
     *
     */
    std::unordered_map<TransactionExecution *, WaitStatus> waiting_txs_;
    std::mutex waiting_queue_mux_;
    /**
     * @brief The timestamp when last caller tries to wake up this tx processor.
     *
     */
    std::atomic<uint64_t> last_wakeup_ts_{0};

    TxLog *txlog_hd_;

    friend class TxService;
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
              std::unique_ptr<TxLog> log_hd = nullptr,
              bool enable_mvcc = true)
        : local_cc_shards_(node_id,
                           conf.find("core_num")->second,
                           conf.find("node_memory_limit_mb")->second,
                           conf.find("node_log_limit_mb")->second,
                           catalog_factory,
                           store_hd,
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
            pool_.emplace_back(std::make_unique<TxProcessor>(
                thd_idx, local_cc_shards_, Sharder::Instance().GetLogAgent()));
        }

        Sharder::Instance().Init(local_path);
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

    void WakeUpTxProcessor(uint16_t thd_id)
    {
        assert(thd_id < pool_.size());
        pool_[thd_id]->WakesUp();
    }

    std::vector<std::unique_ptr<TxProcessor>> pool_;
    std::vector<std::thread> thd_pool_;
    LocalCcShards local_cc_shards_;
    Checkpointer ckpt_;
};
}  // namespace txservice
