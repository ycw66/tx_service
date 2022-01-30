#pragma once

#include <atomic>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <thread>

namespace txservice
{
enum struct TxResultStatus
{
    Unknown,
    Finished,
    Error
};

/**
 * @brief The result of a transaction request, i.e., begin, read, write,
 * scan_begin, scan_next, scan_end, commit and abort. The sender of the request
 * is blocked, when the result has not returned. If the sender needs to yield by
 * context switching, the sender should block on a condition variable and be
 * woke up later when Finish() is invoked by the tx service.
 *
 * @tparam T The type of the returned result of the tx request.
 */
template <typename T>
class TxResult
{
public:
    TxResult() : value_(), status_(TxResultStatus::Unknown), mutex_(), cv_()
    {
    }

    TxResultStatus Status()
    {
        std::lock_guard<std::mutex> lk(mutex_);
        return status_;
    }

    bool IsError() const
    {
        return status_ == TxResultStatus::Error;
    }

    T &Value()
    {
        return value_;
    }

    const T &Value() const
    {
        return value_;
    }

    void Finish(const T &val)
    {
        std::lock_guard<std::mutex> lk(mutex_);
        value_ = val;
        status_ = TxResultStatus::Finished;
        cv_.notify_one();
    }

    void Finish(T &&val)
    {
        std::lock_guard<std::mutex> lk(mutex_);
        value_ = std::move(val);
        status_ = TxResultStatus::Finished;
        cv_.notify_one();
    }

    void FinishError()
    {
        std::lock_guard<std::mutex> lk(mutex_);
        status_ = TxResultStatus::Error;
        cv_.notify_one();
    }

    void Reset()
    {
        std::lock_guard<std::mutex> lk(mutex_);
        status_ = TxResultStatus::Unknown;
    }

    int Wait()
    {
        using namespace std::chrono_literals;

        std::unique_lock<std::mutex> lk(mutex_);
        cv_.wait(lk, [this] { return status_ != TxResultStatus::Unknown; });

        /*while (status_ == TxResultStatus::Unknown)
        {
            cv_.wait_for(lk, 50us, [this] {
                return status_ != TxResultStatus::Unknown;
            });
        }*/

        /*for (size_t idx = 0; idx < 200; ++idx)
        {
            if (returned_.load(std::memory_order_acquire))
            {
                return 0;
            }
#if __GNUC__
            __asm volatile("pause" :::);
#endif
        }

        auto start = std::chrono::steady_clock::now();
        auto now = start;
        while (now - start <= std::chrono::microseconds(100))
        {
            std::this_thread::yield();
            if (returned_.load(std::memory_order_acquire))
            {
                return 0;
            }
            now = std::chrono::steady_clock::now();
        }

        using namespace std::chrono_literals;
        while (!returned_.load(std::memory_order_acquire))
        {
            std::unique_lock<std::mutex> lk(mutex_);
            cv_.wait_for(lk, 10ms, [this] {
                return returned_.load(std::memory_order_acquire);
            });
        }*/

        return 0;
    }

private:
    T value_;
    TxResultStatus status_;
    std::mutex mutex_;
    std::condition_variable cv_;
};
}  // namespace txservice
