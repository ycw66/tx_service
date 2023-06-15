#pragma once

#include <atomic>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <thread>

#include "error_messages.h"

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
    TxResult(const std::function<void()> *yield_fp,
             const std::function<void()> *resume_fp)
        : value_(),
          status_(TxResultStatus::Unknown),
          error_code_(TxErrorCode::NO_ERROR),
          mutex_(),
          cv_(),
          waiting_(false),
          yield_func_(yield_fp),
          resume_func_(resume_fp)
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

    TxErrorCode ErrorCode() const
    {
        return error_code_;
    }

    void Finish(const T &val)
    {
        std::unique_lock<std::mutex> lk(mutex_);
        value_ = val;
        status_ = TxResultStatus::Finished;

        if (waiting_)
        {
            if (resume_func_ != nullptr)
            {
                lk.unlock();
                // The resume functor schedules the coroutine waiting for the
                // result to re-run/resume from the point it yields, i.e.,
                // inside Wait().
                (*resume_func_)();
            }
            else if (yield_func_ == nullptr)
            {
                // cv notification needs to be in the lock scope. This is
                // because the tx request is owned by the sender and the sending
                // thread may wake up spuriously before notify_one() is called.
                // If so, the sending thread moves forward and de-allocate the
                // tx request, before notify_one() is called, causing invalid
                // memory access.
                cv_.notify_one();
            }
        }
    }

    void Finish(T &&val)
    {
        std::unique_lock<std::mutex> lk(mutex_);
        value_ = std::move(val);
        status_ = TxResultStatus::Finished;

        if (waiting_)
        {
            if (resume_func_ != nullptr)
            {
                lk.unlock();
                // The resume functor schedules the waiting coroutine to
                // re-run/resume from the point it is blocking for the result,
                // i.e., inside Wait().
                (*resume_func_)();
            }
            else if (yield_func_ == nullptr)
            {
                cv_.notify_one();
            }
        }
    }

    void FinishError(TxErrorCode err_code = TxErrorCode::UNDEFINED_ERR)
    {
        std::unique_lock<std::mutex> lk(mutex_);
        status_ = TxResultStatus::Error;
        error_code_ = err_code;

        if (waiting_)
        {
            if (resume_func_ != nullptr)
            {
                lk.unlock();
                (*resume_func_)();
            }
            else if (yield_func_ == nullptr)
            {
                cv_.notify_one();
            }
        }
    }

    /**
     * @brief Set the Error Code for tx request.
     * For example, set the reason for transaction abort.
     *
     * @param err_code
     */
    void SetErrorCode(TxErrorCode err_code = TxErrorCode::UNDEFINED_ERR)
    {
        error_code_ = err_code;
    }

    void Reset(const std::function<void()> *yield_fptr = nullptr,
               const std::function<void()> *resume_fptr = nullptr)
    {
        std::lock_guard<std::mutex> lk(mutex_);
        status_ = TxResultStatus::Unknown;
        error_code_ = TxErrorCode::NO_ERROR;
        waiting_ = false;
        yield_func_ = yield_fptr;
        resume_func_ = resume_fptr;
    }

    int Wait()
    {
        if (yield_func_ != nullptr)
        {
            std::unique_lock<std::mutex> lk(mutex_);
            if (status_ == TxResultStatus::Unknown)
            {
                waiting_ = true;
                lk.unlock();

                // The yield functor invokes the coroutine's resume() and
                // returns the control to the caller of the coroutine that sends
                // the tx request, i.e., the runtime thread executing the query.
                // The runtime thread skips the blocking coroutine and moves on
                // to process the next command.
                (*yield_func_)();
            }
        }
        else
        {
            std::unique_lock<std::mutex> lk(mutex_);
            waiting_ = true;
            cv_.wait(lk, [this] { return status_ != TxResultStatus::Unknown; });
        }

        return 0;
    }

private:
    T value_;
    TxResultStatus status_;
    TxErrorCode error_code_;
    std::mutex mutex_;
    std::condition_variable cv_;

    bool waiting_{false};
    const std::function<void()> *yield_func_;
    const std::function<void()> *resume_func_;

    friend struct TxRequest;
};
}  // namespace txservice
