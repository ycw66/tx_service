#pragma once

#include <atomic>
#include <cassert>
#include <chrono>
#include <functional>
#include <system_error>
#include <variant>

namespace txservice
{
class TransactionExecution;

template <typename T>
class CcHandlerResult
{
public:
    CcHandlerResult(const TransactionExecution *txm) : result_(), txm_(txm)
    {
    }

    CcHandlerResult(const CcHandlerResult &rhs) = delete;

    CcHandlerResult(CcHandlerResult &&rhs) noexcept
        : result_(std::move(rhs.result_)),
          is_finished_(rhs.is_finished_.load(std::memory_order_acquire)),
          error_code_(rhs.error_code_),
          txm_(rhs.txm_),
          post_lambda_(rhs.post_lambda_)
    {
    }

    CcHandlerResult &operator=(const CcHandlerResult &rhs)
    {
        result_ = rhs.result_;
        is_finished_ = rhs.is_finished_.load(std::memory_order_acquire);
        error_code_ = rhs.error_code_;
        return *this;
    }

    bool IsFinished() const
    {
        return is_finished_.load(std::memory_order_acquire);
    }

    bool IsError() const
    {
        return error_code_ != 0;
    }

    int8_t ErrorCode() const
    {
        return error_code_;
    }

    void SetRefCnt(uint32_t cnt)
    {
        ref_cnted_ = true;
        ref_cnt_.store(cnt, std::memory_order_release);
    }

    void ClearRefCnt()
    {
        ref_cnted_ = false;
    }

    void SetFinished()
    {
        if (ref_cnted_)
        {
            auto r = ref_cnt_.fetch_sub(1, std::memory_order_acq_rel);
            if (r == 1)
            {
                bool expect = false;
                is_finished_.compare_exchange_strong(
                    expect, true, std::memory_order_acq_rel);

                if (post_lambda_)
                {
                    post_lambda_(this);
                }
            }
        }
        else
        {
            bool expect = false;
            is_finished_.compare_exchange_strong(
                expect, true, std::memory_order_acq_rel);

            if (post_lambda_)
            {
                post_lambda_(this);
            }
        }
    }

    void SetError(int8_t err_code)
    {
        error_code_ = err_code;
        SetFinished();
    }

    void SetValue(const T &val)
    {
        result_ = val;
    }

    void SetValue(T &&val)
    {
        result_ = std::move(val);
    }

    const T &Value() const
    {
        return result_;
    }

    T &Value()
    {
        return result_;
    }

    void Reset()
    {
        is_finished_.store(false, std::memory_order_release);
        error_code_ = 0;
        ClearRefCnt();
    }

    const TransactionExecution *Txm() const
    {
        return txm_;
    }

private:
    T result_;
    std::atomic<bool> is_finished_{false};
    int8_t error_code_{0};
    bool ref_cnted_{false};
    std::atomic<uint32_t> ref_cnt_;
    // The parent tx state machine who sends a cc request and waits on this
    // handler result. The handler result is bound to a fixed tx machine. The tx
    // machine, however, may be re-used repeatedly for different user-level
    // tx's.
    const TransactionExecution *const txm_;

public:
    std::function<void(CcHandlerResult<T> *)> post_lambda_;
};
}  // namespace txservice
