#include "tx_worker_pool.h"

namespace txservice
{
TxWorkerPool::TxWorkerPool(size_t max_workers_num)
    : max_workers_num_(max_workers_num)
{
    for (size_t i = 0; i < max_workers_num_; i++)
    {
        std::thread worker = std::thread(
            [this]
            {
                while (!shutdown_indicator_.load(std::memory_order_acquire))
                {
                    // Acquire work queue mutex
                    std::unique_lock<std::mutex> lk(work_queue_mutex_);
                    // Wait for new work come in or shutdown happen
                    work_queue_cv_.wait(
                        lk,
                        [this]
                        {
                            return !work_queue_.empty() ||
                                   shutdown_indicator_.load(
                                       std::memory_order_acquire);
                        });
                    // Quit loop if shutdown
                    if (shutdown_indicator_.load(std::memory_order_acquire))
                    {
                        lk.unlock();
                        break;
                    }
                    // Take work if work queue is not empty
                    if (!work_queue_.empty())
                    {
                        std::function<void()> work = work_queue_.front();
                        work_queue_.pop_front();
                        lk.unlock();
                        // Do work
                        work();
                    }
                }
            });
        workers_.push_back(std::move(worker));
    }
}

size_t TxWorkerPool::WorkQueueSize()
{
    std::unique_lock lk(work_queue_mutex_);
    return work_queue_.size();
}

void TxWorkerPool::SubmitWork(std::function<void()> work)
{
    std::unique_lock lk(work_queue_mutex_);
    work_queue_.push_back(work);
    work_queue_cv_.notify_one();
}

void TxWorkerPool::Shutdown()
{
    {
        std::unique_lock lk(work_queue_mutex_);
        shutdown_indicator_.store(true, std::memory_order_release);
        work_queue_cv_.notify_all();
    }

    for (std::thread &worker : workers_)
    {
        worker.join();
    }
}
}  // namespace txservice
