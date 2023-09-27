#pragma once

#include "cc_protocol.h"
#include "tx_execution.h"
#include "tx_request.h"
#include "tx_service.h"
#include "util.h"

namespace txservice
{
static inline void AbortTx(txservice::TransactionExecution *tx,
                           const std::function<void()> *yield_fptr,
                           const std::function<void()> *resume_fptr)
{
    if (tx == nullptr)
        return;
    txservice::AbortTxRequest abort_req(yield_fptr, resume_fptr);
    int err = tx->Execute(&abort_req);
    if (err == 0)
    {
        abort_req.Wait();
    }
}

static inline TransactionExecution *NewTxInit(
    txservice::TxService *tx_service,
    txservice::IsolationLevel level = txservice::IsolationLevel::ReadCommitted,
    txservice::CcProtocol proto = txservice::CcProtocol::Locking,
    NodeGroupId tx_owner = UINT32_MAX,
    const std::function<void()> *yield_fptr = nullptr,
    const std::function<void()> *resume_fptr = nullptr,
    int retry_count = 8)
{
    assert(tx_service != nullptr);
    txservice::TransactionExecution *txm = nullptr;
    while (retry_count > 0)
    {
        txm = tx_service->NewTx();
        bool init_tx_success = false;
        txservice::InitTxRequest init_tx_req(
            level, proto, yield_fptr, resume_fptr, nullptr, tx_owner);

        txm->Execute(&init_tx_req);
        init_tx_req.Wait();
        if (init_tx_req.IsError())
        {
            init_tx_success = false;
        }
        else
        {
            init_tx_success = true;
        }

        if (!init_tx_success)
        {
            txm = nullptr;
            retry_count--;
            if (retry_count > 0)
            {
                std::this_thread::sleep_for(std::chrono::seconds(4));
            }
        }
        else
        {
            break;
        }
    }
    return txm;
}

static inline TxErrorCode TxReadCatalog(TransactionExecution *txm,
                                        ReadTxRequest &read_tx_req,
                                        bool &exists)
{
    assert(txm != nullptr);

    txm->Execute(&read_tx_req);
    read_tx_req.Wait();
    if (read_tx_req.IsError())
    {
        return read_tx_req.ErrorCode();
    }

    const RecordStatus &rec_status = read_tx_req.Result().first;
    if (rec_status == RecordStatus::Deleted)
    {
        exists = false;
        return TxErrorCode::NO_ERROR;
    }
    else
    {
        assert(rec_status == RecordStatus::Normal);

        CatalogRecord *catalog_rec =
            static_cast<CatalogRecord *>(read_tx_req.rec_);
        if (catalog_rec->Schema())
        {
            exists = true;
            return TxErrorCode::NO_ERROR;
        }
        else
        {
            return TxErrorCode::UNDEFINED_ERR;
        }
    }
}
}  // namespace txservice
