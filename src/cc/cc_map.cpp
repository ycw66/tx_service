
#include "cc/cc_map.h"

#include <type_traits>  // std::is_same_v
#include <utility>      // std::pair

#include "cc/local_cc_shards.h"
#include "cc_entry.h"
#include "tx_trace.h"

namespace txservice
{
void CcMap::MoveRequest(CcRequestBase *cc_req, uint32_t target_core_id)
{
    shard_->local_shards_.EnqueueCcRequest(
        shard_->core_id_, target_core_id, cc_req);
}

std::pair<LockType, CcErrorCode> CcMap::AcquireCceKeyLock(
    LruEntry *cce,
    RecordStatus cce_payload_status,
    CcRequestBase *req,
    uint32_t ng_id,
    int64_t ng_term,
    int64_t tx_term,
    CcOperation cc_op,
    IsolationLevel iso_level,
    CcProtocol protocol,
    uint64_t read_ts,
    bool is_covering_keys)
{
    if (iso_level == IsolationLevel::Snapshot)
    {
        if (cc_op == CcOperation::ReadForWrite && read_ts < cce->commit_ts_)
        {
            LOG(WARNING) << "SI ReadForWrite, latest version not fits the read "
                            "timestamp. tx:"
                         << req->Txn();
            // For ReadForWrite under Snapshot Isolation,  we will return the
            // latest version, only if the latest version fits the read's
            // timestamp. Otherwise, we will return an error to abort the tx.
            // Because, snapshot isolation is a guarantee that all reads made in
            // a transaction will see a consistent snapshot of the database, and
            // the transaction itself will successfully commit only if no
            // updates it has made conflict with any concurrent updates made
            // since that snapshot.
            return std::pair<LockType, CcErrorCode>(
                LockType::NoLock, CcErrorCode::MVCC_READ_FOR_WRITE_CONFLICT);
        }
        else if (cc_op == CcOperation::Read ||
                 cc_op == CcOperation::ReadSkIndex)
        {
            if (cce->key_lock_ptr_ != nullptr &&
                cce->key_lock_ptr_->HasWriteLock() &&
                cce->key_lock_ptr_->WLockTs() < read_ts)
            {
                // Having write lock means the ccentry will be updated soon.
                // If wlock_ts_ < ts, the future 'commit_ts' is may also less
                // than the 'read timestamp', then should return the future
                // version.
                // There are two choice: (1)wait until the future version is
                // committed; (2) abort read transcation.
                return std::pair<LockType, CcErrorCode>(
                    LockType::NoLock, CcErrorCode::MVCC_READ_MUST_WAIT_WRITE);
            }
        }
    }

    // deduce the lock type to acquire
    LockType lock_type = LockTypeUtil::DeduceLockType(
        cc_op, iso_level, protocol, is_covering_keys);

    TxNumber tx_number = req->Txn();
    LockOpStatus lock_op_status = LockOpStatus::Successful;
    CcErrorCode err_code = CcErrorCode::NO_ERROR;

    if (lock_type != LockType::NoLock)
    {
        if (lock_type == LockType::WriteLock ||
            lock_type == LockType::WriteIntent ||
            cce_payload_status != RecordStatus::Deleted)
        {
            lock_op_status =
                cce->GetKeyLock().AcquireLock(req, protocol, lock_type);
        }
        else
        {
            lock_type = LockType::NoLock;
        }
    }

    if (lock_op_status == LockOpStatus::Successful)
    {
        if (lock_type != LockType::NoLock)
        {
            shard_->UpsertLockHoldingTx(tx_number,
                                        tx_term,
                                        cce,
                                        lock_type == LockType::WriteLock,
                                        ng_id,
                                        table_name_.Type());
        }

        if (cce->key_lock_ptr_ != nullptr &&
            cce->key_lock_ptr_->HasWriteLock() &&
            cce->key_lock_ptr_->WriteLockTx() != tx_number)
        {
            shard_->CheckRecoverTx(
                cce->key_lock_ptr_->WriteLockTx(), ng_id, ng_term);
        }
        TX_TRACE_ACTION_WITH_CONTEXT(
            req,
            "AcquireCcEntryKeyLock.Successful",
            cce,
            (
                [&req, &cce, &lock_type, &cce_payload_status]() -> std::string
                {
                    return std::string(",\"tx_number\":")
                        .append(std::to_string(req->Txn()))
                        .append(",\"CcEntry\":")
                        .append(FMT_POINTER_TO_UINT64T(cce))
                        .append(",\"LockType\":")
                        .append(std::to_string((uint8_t) lock_type))
                        .append(",\"cce_payload_status\":")
                        .append(std::to_string((uint8_t) cce_payload_status))
                        .append(",\"CcEntry.key_lock_\":")
                        .append(cce->GetKeyLock().DebugInfo());
                }));
    }
    else if (lock_op_status == LockOpStatus::Failed)
    {
        // check and recover conflicted transactions.
        RecoverTxForLockConfilct(cce->GetKeyLock(), lock_type, ng_id, ng_term);
        if (lock_type == LockType::WriteLock &&
            !cce->GetKeyLock().HasWriteLock() &&
            !cce->GetKeyLock().HasWriteIntent())
        {
            err_code = CcErrorCode::ACQUIRE_KEY_LOCK_FAILED_FOR_RW_CONFLICT;
        }
        else
        {
            err_code = CcErrorCode::ACQUIRE_KEY_LOCK_FAILED_FOR_WW_CONFLICT;
        }

        TX_TRACE_ACTION_WITH_CONTEXT(
            req,
            "AcquireCcEntryKeyLock.Failed",
            cce,
            (
                [&req, &cce, &lock_type, &cce_payload_status]() -> std::string
                {
                    return std::string(",\"tx_number\":")
                        .append(std::to_string(req->Txn()))
                        .append(",\"CcEntry\":")
                        .append(FMT_POINTER_TO_UINT64T(cce))
                        .append(",\"LockType\":")
                        .append(std::to_string((uint8_t) lock_type))
                        .append(",\"cce_payload_status\":")
                        .append(std::to_string((uint8_t) cce_payload_status))
                        .append(",\"CcEntry.key_lock_\":")
                        .append(cce->GetKeyLock().DebugInfo());
                }));
    }
    else
    {
        TX_TRACE_ACTION_WITH_CONTEXT(
            req,
            "AcquireCcEntryKeyLock.Blocked",
            cce,
            (
                [&req, &cce, &lock_type, &cce_payload_status]() -> std::string
                {
                    return std::string(",\"tx_number\":")
                        .append(std::to_string(req->Txn()))
                        .append(",\"CcEntry\":")
                        .append(FMT_POINTER_TO_UINT64T(cce))
                        .append(",\"LockType\":")
                        .append(std::to_string((uint8_t) lock_type))
                        .append(",\"cce_payload_status\":")
                        .append(std::to_string((uint8_t) cce_payload_status))
                        .append(",\"CcEntry.key_lock_\":")
                        .append(cce->GetKeyLock().DebugInfo());
                }));

        // check and recover conflicted transactions.
        RecoverTxForLockConfilct(
            *(cce->key_lock_ptr_), lock_type, ng_id, ng_term);
        err_code = CcErrorCode::ACQUIRE_LOCK_BLOCKED;
    }

    return std::pair<LockType, CcErrorCode>(lock_type, err_code);
}

std::pair<LockType, CcErrorCode> CcMap::LockHandleForResumedRequest(
    LruEntry *cce,
    RecordStatus cce_payload_status,
    CcRequestBase *req,
    uint32_t ng_id,
    int64_t ng_term,
    int64_t tx_term,
    CcOperation cc_op,
    IsolationLevel iso_level,
    CcProtocol protocol,
    uint64_t read_ts,
    bool is_covering_keys)
{
    TxNumber tx_number = req->Txn();
    LockType acquired_lock = LockTypeUtil::DeduceLockType(
        cc_op, iso_level, protocol, is_covering_keys);
    CcErrorCode err_code = CcErrorCode::NO_ERROR;

    bool should_release_lock = (cce_payload_status == RecordStatus::Deleted &&
                                acquired_lock != LockType::WriteLock &&
                                acquired_lock != LockType::WriteIntent);

    if (iso_level == IsolationLevel::Snapshot &&
        cc_op == CcOperation::ReadForWrite && read_ts < cce->commit_ts_)
    {
        LOG(WARNING) << "SI ReadForWrite, latest version not fits the read "
                        "timestamp. tx:"
                     << req->Txn();
        // For ReadForWrite under Snapshot Isolation,  we will return the
        // latest version, only if the latest version fits the read's timestamp.
        // Otherwise, we will return an error to abort the tx.
        // Because, snapshot isolation is a guarantee that all reads made in a
        // transaction will see a consistent snapshot of the database, and the
        // transaction itself will successfully commit only if no updates it has
        // made conflict with any concurrent updates made since that snapshot.
        should_release_lock = true;
        err_code = CcErrorCode::MVCC_READ_FOR_WRITE_CONFLICT;
    }

    if (should_release_lock)
    {
        cce->key_lock_ptr_->ReleaseLock(tx_number, shard_, acquired_lock);
        cce->RecycleKeyLock();
        acquired_lock = LockType::NoLock;
        // Here "DeleteLockHoldingTx" is required. For, this may be a retried
        // request and the prior blocked request may has upsert tx's lock info.
        shard_->DeleteLockHoldingTx(tx_number, cce, ng_id);
    }
    else
    {
        assert(acquired_lock != LockType::NoLock);
        shard_->UpsertLockHoldingTx(tx_number,
                                    tx_term,
                                    cce,
                                    acquired_lock == LockType::WriteLock,
                                    ng_id,
                                    table_name_.Type());
    }

    return std::pair<LockType, CcErrorCode>(acquired_lock, err_code);
}

void CcMap::RecoverTxForLockConfilct(NonBlockingLock &lock,
                                     LockType lock_type,
                                     uint32_t ng_id,
                                     int64_t ng_term)
{
    // check and recover conflicted transactions.
    switch (lock_type)
    {
    case LockType::WriteLock:
    {
        if (lock.HasWriteLock())
        {
            TX_TRACE_ACTION_WITH_CONTEXT(
                this,
                "RecoverTxForLockConfilct",
                &lock,
                [&lock]() -> std::string
                {
                    return std::string("\"Lock\":")
                        .append(FMT_POINTER_TO_UINT64T(&lock))
                        .append(",\"associate\":\"key_lock_.write_lock\"");
                });

            shard_->CheckRecoverTx(lock.WriteLockTx(), ng_id, ng_term);
        }
        else if (lock.HasWriteIntent())
        {
            shard_->CheckRecoverTx(lock.WriteIntentTx(), ng_id, ng_term);
        }
        else
        {
            const std::unordered_set<TxNumber> &read_locks = lock.ReadLocks();
            // If the request fails to acquire the write lock
            // because of read locks, checks each read lock and
            // recovers if needed.
            for (const auto &read_tx : read_locks)
            {
                shard_->CheckRecoverTx(read_tx, ng_id, ng_term);
            }
        }
        break;
    }
    case LockType::WriteIntent:
    {
        if (lock.HasWriteLock())
        {
            shard_->CheckRecoverTx(lock.WriteLockTx(), ng_id, ng_term);
        }
        else if (lock.HasWriteIntent())
        {
            shard_->CheckRecoverTx(lock.WriteIntentTx(), ng_id, ng_term);
        }
        break;
    }
    case LockType::ReadLock:
    {
        if (lock.HasWriteLock())
        {
            shard_->CheckRecoverTx(lock.WriteLockTx(), ng_id, ng_term);
        }
        break;
    }
    default:
        break;
    }  // switch
}

void CcMap::DowngradeCceKeyWriteLock(LruEntry *cce, TxNumber tx_number)
{
    cce->key_lock_ptr_->DowngradeWriteLock(tx_number, shard_);
}

void CcMap::ReleaseCceKeyLock(LruEntry *cce, TxNumber tx_number, uint32_t ng_id)
{
    if (cce != nullptr && cce->key_lock_ptr_ != nullptr)
    {
        bool is_write_lock = (cce->key_lock_ptr_->HasWriteLock() &&
                              cce->key_lock_ptr_->WriteLockTx() == tx_number);
        cce->key_lock_ptr_->ClearTx(tx_number, shard_);
        shard_->DeleteLockHoldingTx(tx_number, cce, ng_id);
        if (is_write_lock)
        {
            cce->key_lock_ptr_->SetWLockTs(0);
        }
        cce->RecycleKeyLock();
    }
}

void CcMap::ReleaseCceGapLock(LruEntry *cce, TxNumber tx_number, uint32_t ng_id)
{
    if (cce != nullptr && cce->gap_lock_ptr_ != nullptr)
    {
        bool is_write_lock = (cce->gap_lock_ptr_->HasWriteLock() &&
                              cce->gap_lock_ptr_->WriteLockTx() == tx_number);
        cce->gap_lock_ptr_->ClearTx(tx_number, shard_);
        shard_->DeleteLockHoldingTx(tx_number, cce, ng_id);
        if (is_write_lock)
        {
            cce->gap_lock_ptr_->SetWLockTs(0);
        }
        cce->RecycleGapLock();
    }
}

}  // namespace txservice
