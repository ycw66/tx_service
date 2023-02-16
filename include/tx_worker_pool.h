#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <functional>
#include <iostream>
#include <mutex>
#include <thread>
#include <vector>

static size_t MAX_WORKERS_NUM = 3;

namespace txservice
{
class TxWorkerPool
{
public:
    TxWorkerPool(size_t max_workers_num = MAX_WORKERS_NUM);
    ~TxWorkerPool() = default;

    void SubmitWork(std::function<void()> work);
    size_t WorkQueueSize();
    void Shutdown();
    size_t WorkerPoolSize()
    {
        return max_workers_num_;
    }

private:
    size_t max_workers_num_;
    std::vector<std::thread> workers_;
    std::deque<std::function<void()>> work_queue_;
    std::mutex work_queue_mutex_;
    std::condition_variable work_queue_cv_;
    std::atomic<bool> shutdown_indicator_{false};
};
}  // namespace txservice
