#pragma once

#include "cc_protocol.h"
#include "error_messages.h"
#include "tx_execution.h"
#include "tx_request.h"
#include "tx_service.h"
#include "util.h"

namespace txservice
{
static inline void AbortTx(txservice::TransactionExecution *txm,
                           const std::function<void()> *yield_func = nullptr,
                           const std::function<void()> *resume_func = nullptr)
{
    if (txm == nullptr)
    {
        return;
    }

    // Abort tx request.
    CommitTxRequest req(false, yield_func, resume_func, txm);
    txm->CommitTx(req);
}

static inline std::pair<bool, TxErrorCode> CommitTx(
    txservice::TransactionExecution *txm,
    const std::function<void()> *yield_func = nullptr,
    const std::function<void()> *resume_func = nullptr)
{
    if (txm == nullptr)
    {
        return {true, TxErrorCode::NO_ERROR};
    }

    CommitTxRequest commit_req(true, yield_func, resume_func, txm);
    bool success = txm->CommitTx(commit_req);
    return {success, commit_req.ErrorCode()};
}

static inline TransactionExecution *NewTxInit(
    txservice::TxService *tx_service,
    txservice::IsolationLevel level = txservice::IsolationLevel::ReadCommitted,
    txservice::CcProtocol proto = txservice::CcProtocol::Locking,
    NodeGroupId tx_owner = UINT32_MAX,
    int16_t group_id = -1,
    bool start_now = false)
{
    assert(tx_service != nullptr);
    txservice::TransactionExecution *txm = nullptr;
#ifdef EXT_TX_PROC_ENABLED
    txm = group_id >= 0 ? tx_service->NewTx(group_id) : tx_service->NewTx();
#else
    txm = tx_service->NewTx();
#endif
    txm->InitTx(level, proto, tx_owner, start_now);

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
