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
        auto r = ref_cnt_.fetch_sub(1, std::memory_order_acq_rel);
        if (r == 1)
        {
            if (post_lambda_)
            {
                post_lambda_(this);
            }

            bool expect = false;
            is_finished_.compare_exchange_strong(
                expect, true, std::memory_order_acq_rel);
        }
    }
    else
    {
        if (post_lambda_)
        {
            post_lambda_(this);
        }

        bool expect = false;
        is_finished_.compare_exchange_strong(
            expect, true, std::memory_order_acq_rel);
    }
};

template <typename T>
void CcHandlerResult<T>::SetError(int8_t err_code)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        err_code,
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
    int8_t no_error = 0;
    error_code_.compare_exchange_strong(
        no_error, err_code, std::memory_order_acq_rel);
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
        int8_t no_error = 0;
        error_code_.compare_exchange_strong(
            no_error, -2, std::memory_order_acq_rel);

        if (post_lambda_)
        {
            post_lambda_(this);
        }
    }

    return success;
};

template class CcHandlerResult<InitTxResult>;
template class CcHandlerResult<ReadKeyResult>;
template class CcHandlerResult<ScanNextResult>;
template class CcHandlerResult<ScanOpenResult>;
template class CcHandlerResult<AcquireAllResult>;
template class CcHandlerResult<AcquireKeyResult>;
template class CcHandlerResult<RangeMedianKeyResult>;
template class CcHandlerResult<Void>;
template class CcHandlerResult<TxId>;
template class CcHandlerResult<std::vector<txservice::TxId>>;
template class CcHandlerResult<bool>;
template class CcHandlerResult<uint64_t>;
template class CcHandlerResult<int8_t>;
}  // namespace txservice
