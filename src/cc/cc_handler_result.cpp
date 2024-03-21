#include "cc/cc_handler_result.h"

#include <atomic>
#include <cassert>
#include <chrono>
#include <functional>
#include <string>
#include <system_error>
#include <utility>
#include <variant>
#include <vector>

#include "tx_execution.h"
#include "tx_trace.h"

namespace txservice
{
template <typename T>
void CcHandlerResult<T>::SetFinished()
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        static_cast<T *>(&result_),
        [this]() -> std::string
        {
            if (this->txm_ == nullptr)
            {
                return std::string("\"txm_\":\"nullptr\"");
            }
            else
            {
                return std::string("\"tx_number\":")
                    .append(std::to_string(this->txm_->TxNumber()))
                    .append(",\"tx_term\":")
                    .append(std::to_string(this->txm_->TxTerm()))
                    .append(",\"ref_cnt_\":")
                    .append(std::to_string(
                        this->ref_cnt_.load(std::memory_order_acquire)));
            }
        });
    TX_TRACE_DUMP(static_cast<T *>(&result_));

    if (ref_cnted_)
    {
        auto r = ref_cnt_.fetch_sub(1, std::memory_order_relaxed);
        if (r == 1)
        {
            bool expect = false;
            if (is_finished_.compare_exchange_strong(
                    expect, true, std::memory_order_acq_rel))
            {
                if (post_lambda_)
                {
                    post_lambda_(this);
                }
#ifdef EXT_TX_PROC_ENABLED
                if (txm_ != nullptr && is_blocking_)
                {
                    txm_->Enlist();
                }
#endif
            }
        }
    }
    else
    {
        bool expect = false;
        if (is_finished_.compare_exchange_strong(
                expect, true, std::memory_order_acq_rel))
        {
            if (post_lambda_)
            {
                post_lambda_(this);
            }

#ifdef EXT_TX_PROC_ENABLED
            if (txm_ != nullptr && is_blocking_)
            {
                txm_->Enlist();
            }
#endif
        }
    }
};

template <typename T>
void CcHandlerResult<T>::SetError(CcErrorCode err_code)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        (int8_t) err_code,
        [this]() -> std::string
        {
            if (this->txm_ == nullptr)
            {
                return std::string("\"txm_\":\"nullptr\"");
            }
            else
            {
                return std::string("\"tx_number\":")
                    .append(std::to_string(this->txm_->TxNumber()))
                    .append("\"tx_term\":")
                    .append(std::to_string(this->txm_->TxTerm()));
            }
        });
    CcErrorCode no_error = CcErrorCode::NO_ERROR;
    error_code_.compare_exchange_strong(
        no_error, err_code, std::memory_order_relaxed);
    SetFinished();
};

template <typename T>
bool CcHandlerResult<T>::ForceError()
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        (int8_t) 0,
        [this]() -> std::string
        {
            if (this->txm_ == nullptr)
            {
                return std::string("\"txm_\":\"nullptr\"");
            }
            else
            {
                return std::string("\"tx_number\":")
                    .append(std::to_string(this->txm_->TxNumber()))
                    .append("\"tx_term\":")
                    .append(std::to_string(this->txm_->TxTerm()));
            }
        });
    bool expect = false;
    bool success = is_finished_.compare_exchange_strong(
        expect, true, std::memory_order_acq_rel);

    if (success)
    {
        CcErrorCode no_error = CcErrorCode::NO_ERROR;
        error_code_.compare_exchange_strong(
            no_error, CcErrorCode::FORCE_FAIL, std::memory_order_acq_rel);

        if (post_lambda_)
        {
            post_lambda_(this);
        }

#ifdef EXT_TX_PROC_ENABLED
        if (txm_ != nullptr && is_blocking_)
        {
            txm_->Enlist();
        }
#endif
    }

    return success;
};

template class CcHandlerResult<InitTxResult>;
template class CcHandlerResult<ReadKeyResult>;
template class CcHandlerResult<ScanNextResult>;
template class CcHandlerResult<ScanOpenResult>;
template class CcHandlerResult<RangeScanSliceResult>;
template class CcHandlerResult<AcquireAllResult>;
template class CcHandlerResult<std::vector<AcquireKeyResult>>;
template class CcHandlerResult<Void>;
template class CcHandlerResult<TxId>;
template class CcHandlerResult<PostProcessResult>;
template class CcHandlerResult<bool>;
template class CcHandlerResult<uint64_t>;
template class CcHandlerResult<int8_t>;
template class CcHandlerResult<std::string>;
template class CcHandlerResult<ObjectCommandResult>;
template class CcHandlerResult<std::vector<int64_t>>;
template class CcHandlerResult<UploadBatchResult>;
}  // namespace txservice
