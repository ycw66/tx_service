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

static inline TransactionExecution *NewTxInit(
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

static inline bool TxReadCatalog(TransactionExecution *txm,
                                 const store::DataStoreHandler *storage_hd,
                                 ReadTxRequest &read_tx_req,
                                 bool &exists)
{
    assert(storage_hd != nullptr);
    assert(txm != nullptr);

    bool ok = true;

    const CatalogKey *catalog_key =
        static_cast<const CatalogKey *>(read_tx_req.key_);
    CatalogRecord *catalog_rec = static_cast<CatalogRecord *>(read_tx_req.rec_);

    txm->Execute(&read_tx_req);
    read_tx_req.Wait();
    assert(!read_tx_req.IsError());

    const RecordStatus &rec_status = read_tx_req.Result();
    if (rec_status == RecordStatus::Deleted)
    {
        exists = false;
    }
    else if (rec_status == RecordStatus::Unknown)
    {
        std::string schema_image;
        uint64_t schema_ts = 0;
        ok = storage_hd->FetchTable(
            catalog_key->Name(), schema_image, exists, schema_ts);
        if (ok)
        {
            catalog_rec->SetSchemaImage(std::move(schema_image));
            ReadOutsideTxRequest read_outside(*catalog_rec, !exists, schema_ts);
            txm->Execute(&read_outside);
            read_outside.Wait();
        }
    }
    else
    {
        assert(rec_status == RecordStatus::Normal);
        exists = true;
        catalog_rec->SetSchemaImage(catalog_rec->Schema()->SchemaImage());
    }

    return ok;
}
}  // namespace txservice
