#pragma once

#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <string>
#include <system_error>
#include <utility>
#include <variant>

#include "error_messages.h"  // CcErrorCode

namespace txservice
{
class TransactionExecution;

enum class HandlerResultErrorType
{
    // Ccnode is not the raft leader
    NotLeader = -1,
    // Other errors
    Error = 1,
    // Unknown
    Unknown = 3
};

/**
 * @brief CcHandlerResultBase is the base class of CcHandlerResult of different
 * operators which provides SetError and SetFinished API.
 *
 */
class CcHandlerResultBase
{
public:
    virtual ~CcHandlerResultBase() = default;
    virtual void SetError(CcErrorCode err_code) = 0;
    virtual void SetFinished() = 0;
    virtual bool IsFinished() const = 0;
    virtual bool ForceError() = 0;
    virtual bool IsError() const = 0;
};

template <typename T>
class CcHandlerResult : public CcHandlerResultBase
{
public:
    CcHandlerResult(TransactionExecution *txm) : result_(), txm_(txm)
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

    CcHandlerResult &operator=(const CcHandlerResult &rhs) = delete;

    bool IsFinished() const override
    {
        return is_finished_.load(std::memory_order_acquire);
    }

    bool IsError() const override
    {
        return error_code_.load(std::memory_order_acquire) !=
               CcErrorCode::NO_ERROR;
    }

    CcErrorCode ErrorCode() const
    {
        return error_code_.load(std::memory_order_acquire);
    }

    const std::string ErrorMsg() const
    {
        auto it = cc_error_messages.find(ErrorCode());
        if (it != cc_error_messages.end())
        {
            return it->second;
        }
        return "CcErrorCode:" + std::to_string(static_cast<int>(ErrorCode()));
    }

    void SetRefCnt(uint32_t cnt)
    {
        if (cnt <= 0)
        {
            ClearRefCnt();
            return;
        }
        ref_cnted_ = true;
        ref_cnt_.store(cnt, std::memory_order_relaxed);
        remote_ref_cnt_.store(0, std::memory_order_relaxed);
    }

    void ClearRefCnt()
    {
        ref_cnted_ = false;
        ref_cnt_.store(0, std::memory_order_relaxed);
        remote_ref_cnt_.store(0, std::memory_order_relaxed);
    }

    uint32_t RefCnt() const
    {
        return ref_cnt_.load(std::memory_order_relaxed);
    }

    void IncrementRemoteRef()
    {
        remote_ref_cnt_.fetch_add(1, std::memory_order_relaxed);
    }

    uint32_t RemoteRefCnt()
    {
        return remote_ref_cnt_.load(std::memory_order_relaxed);
    }

    uint32_t LocalRefCnt()
    {
        uint32_t total = ref_cnt_.load(std::memory_order_relaxed);
        uint32_t remote = remote_ref_cnt_.load(std::memory_order_relaxed);
        return total - remote > 0 ? total - remote : 0;
    }

    void SetValue(const T &val) = delete;

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

    void SetFinished() override;

    void SetRemoteFinished()
    {
        remote_ref_cnt_.fetch_sub(1, std::memory_order_relaxed);
        SetFinished();
    }

    void SetError(CcErrorCode err_code) override;

    void SetRemoteError(CcErrorCode err_code)
    {
        remote_ref_cnt_.fetch_sub(1, std::memory_order_relaxed);
        SetError(err_code);
    }

    /**
     * @brief Forces the handler result to an error state.
     *
     * The method is used exclusively to force a tx to stop waiting for a remote
     * cc request's response and to set the request's result to an error state.
     * In case the remote response returns at the same time (which is unlikely),
     * ForceError() either precedes or follows the response's two consecutive
     * invocations of (a) SetValue()/SetError() and (b) SetFinished(). If
     * ForceError() follows, the prior SetFinish() will prevent it from setting
     * the error code. The request finishes normally. If ForceError() prcedes,
     * it will prevent the invocation of SetFinish(), but cannot prevent
     * SetError() or SetValue(). This means that when the tx moves on to cancel
     * the current request, the request's result is guaranteed to have an error
     * code, but the result value may be set by the remote response, or the
     * error code is the code returned by the remote reseponse. At any rate,
     * this is still correct in that the request finishes with an error and the
     * tx moves on as expected.
     *
     * @return true, if the result is forced to be errored; false, if the result
     * has already been set by the remote request's resposne.
     */
    bool ForceError() override;

    void Reset()
    {
        is_finished_.store(false, std::memory_order_release);
        error_code_.store(CcErrorCode::NO_ERROR, std::memory_order_release);
        ClearRefCnt();
    }

    void ResetTxm(TransactionExecution *txm)
    {
        assert(txm != nullptr);
        txm_ = txm;
    }

    TransactionExecution *Txm()
    {
        return txm_;
    }

private:
    T result_;
    std::atomic<bool> is_finished_{false};
    // std::atomic<int8_t> error_code_{0};
    std::atomic<CcErrorCode> error_code_{CcErrorCode::NO_ERROR};
    bool ref_cnted_{false};
    std::atomic<uint32_t> ref_cnt_;
    std::atomic<uint32_t> remote_ref_cnt_{0};
    // The parent tx state machine who sends a cc request and waits on this
    // handler result. The handler result is bound to a fixed tx machine. The tx
    // machine, however, may be re-used repeatedly for different user-level
    // tx's.
    TransactionExecution *txm_;

public:
    std::function<void(CcHandlerResult<T> *)> post_lambda_;
};
}  // namespace txservice
