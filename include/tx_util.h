#pragma once

#include "cc_protocol.h"
#include "tx_execution.h"
#include "tx_request.h"
#include "tx_service.h"
#include "util.h"

namespace txservice
{
static inline void AbortTx(txservice::TransactionExecution *tx)
{
    if (tx == nullptr)
        return;
    txservice::AbortTxRequest abort_req;
    abort_req.Reset();
    tx->Execute(&abort_req);
    abort_req.Wait();
}

static inline bool InitTx(txservice::TransactionExecution *txm,
                          txservice::InitTxRequest *init_txn_ptr,
                          txservice::IsolationLevel level,
                          txservice::CcProtocol proto)
{
    init_txn_ptr->Reset();
    init_txn_ptr->iso_level_ = level;
    init_txn_ptr->protocol_ = proto;
    txm->Execute(init_txn_ptr);
    init_txn_ptr->Wait();
    if (init_txn_ptr->IsError())
    {
        return false;
    }
    return true;
}

static inline txservice::TransactionExecution *NewTxInit(
    txservice::TxService *tx_service,
    txservice::IsolationLevel level = txservice::IsolationLevel::ReadCommitted,
    txservice::CcProtocol proto = txservice::CcProtocol::Locking,
    txservice::InitTxRequest *init_tx_ptr = nullptr,
    int retry_count = 8)
{
    assert(tx_service != nullptr);
    txservice::TransactionExecution *txm = nullptr;
    while (retry_count > 0)
    {
        txm = tx_service->NewTx();
        bool init_tx_success = false;
        if (init_tx_ptr == nullptr)
        {
            txservice::InitTxRequest init_tx_req;
            init_tx_success = InitTx(txm, &init_tx_req, level, proto);
        }
        else
        {
            init_tx_success = InitTx(txm, init_tx_ptr, level, proto);
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
}  // namespace txservice
