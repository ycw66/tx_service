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

/**
 * @brief CcHandlerResultBase is the base class of CcHandlerResult of different
 * operators which provides SetError and SetFinished API.
 *
 */
class CcHandlerResultBase
{
public:
    virtual ~CcHandlerResultBase() = default;
    virtual void SetError(int8_t err_code) = 0;
    virtual void SetFinished() = 0;
    virtual bool IsFinished() const = 0;
    virtual bool IsError() const = 0;
};

template <typename T>
class CcHandlerResult : public CcHandlerResultBase
{
public:
    CcHandlerResult(const TransactionExecution *txm) : result_(), txm_(txm)
    {
    }

    CcHandlerResult(const CcHandlerResult &rhs) = delete;

    CcHandlerResult(CcHandlerResult &&rhs) noexcept
        : result_(std::move(rhs.result_)),
          is_finished_(rhs.is_finished_.load(std::memory_order_acquire)),
          error_code_(rhs.error_code_.load(std::memory_order_acquire)),
          txm_(rhs.txm_),
          post_lambda_(rhs.post_lambda_)
    {
    }

    CcHandlerResult &operator=(const CcHandlerResult &rhs)
    {
        result_ = rhs.result_;
        is_finished_ = rhs.is_finished_.load(std::memory_order_acquire);
        error_code_.store(rhs.error_code_.load(std::memory_order_acquire),
                          std::memory_order_release);
        ref_cnted_ = rhs.ref_cnt_;
        ref_cnt_.store(rhs.ref_cnt_.load(std::memory_order_acquire));
        post_lambda_ = rhs.post_lambda_;

        return *this;
    }

    bool IsFinished() const override
    {
        return is_finished_.load(std::memory_order_acquire);
    }

    bool IsError() const override
    {
        return error_code_.load(std::memory_order_acquire) != 0;
    }

    int8_t ErrorCode() const
    {
        return error_code_.load(std::memory_order_acquire);
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

    void SetFinished() override
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

    void SetError(int8_t err_code) override
    {
        int8_t no_error = 0;
        error_code_.compare_exchange_strong(
            no_error, err_code, std::memory_order_acq_rel);
        SetFinished();
    }

    /**
     * @brief Forces the handler result to an error state.
     *
     * The method is used exclusively to force a tx to stop waiting for a
     * remote cc request's response and to set the request's result to an error
     * state. In case the remote response returns at the same time (which is
     * unlikely), ForceError() either precedes or follows the response's two
     * consecutive invocations of (a) SetValue() or setting the error code and
     * (b) SetFinished(). If ForceError() follows, the prior SetFinish() will
     * prevent it from setting the error code. The request finishes normally. If
     * ForceError() prcedes, it will prevent the invocation of SetFinish(), but
     * cannot prevent SetError() or SetValue(). This means that when the tx
     * moves on to cancel the current request, the request's result is
     * guaranteed to have an error code, but the result value may be set by the
     * remote response, or the error code is overwritten by the code returned by
     * the remote reseponse. At any rate, this is still correct in that the
     * request finishes with an error and the tx moves on as expected.
     *
     * @return true, if the result is forced to be errored; false, if the result
     * has already been set by the remote request's resposne.
     */
    bool ForceError()
    {
        bool expect = false;
        bool success = is_finished_.compare_exchange_strong(
            expect, true, std::memory_order_acq_rel);

        if (success)
        {
            int8_t no_error = 0;
            error_code_.compare_exchange_strong(
                no_error, -2, std::memory_order_acq_rel);

            if (post_lambda_)
            {
                post_lambda_(this);
            }
        }

        return success;
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
        error_code_.store(0, std::memory_order_release);
        ClearRefCnt();
    }

    const TransactionExecution *Txm() const
    {
        return txm_;
    }

private:
    T result_;
    std::atomic<bool> is_finished_{false};
    std::atomic<int8_t> error_code_{0};
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
