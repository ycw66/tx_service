
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

std::pair<LockType, LockOpStatus> CcMap::AcquireCceKeyLock(
    LruEntry *cce,
    RecordStatus cce_payload_status,
    CcRequestBase *req,
    uint32_t ng_id,
    int64_t ng_term,
    int64_t tx_term,
    CcOperation cc_op,
    IsolationLevel iso_level,
    CcProtocol protocol)
{
    // deduce the lock type to acquire
    LockType lock_type =
        LockTypeUtil::DeduceLockType(cc_op, iso_level, protocol);

    TxNumber tx_number = req->Txn();
    bool is_already_held = false;
    LockOpStatus lock_op_status = LockOpStatus::Successful;

    if (lock_type != LockType::NoLock)
    {
        if (lock_type == LockType::WriteLock ||
            cce_payload_status != RecordStatus::Deleted)
        {
            LockType held_lock = cce->GetKeyLock().LockTypeHeldByTx(tx_number);
            if (held_lock < lock_type)
            {
                lock_op_status =
                    cce->GetKeyLock().AcquireLock(req, protocol, lock_type);
            }
            else
            {
                is_already_held = true;
            }
        }
        else
        {
            lock_type = LockType::NoLock;
        }
    }

    if (lock_op_status == LockOpStatus::Successful)
    {
        TX_TRACE_ACTION_WITH_CONTEXT(
            req,
            "AcquireCcEntryKeyLock.Successful",
            cce,
            (
                [&req, &cce]() -> std::string
                {
                    return std::string(",\"tx_number\":")
                        .append(std::to_string(req->Txn()))
                        .append(",\"CcEntry\":")
                        .append(FMT_POINTER_TO_UINT64T(cce))
                        .append(",\"CcEntry.key_lock_\":")
                        .append(FMT_POINTER_TO_UINT64T(&(cce->GetKeyLock())));
                }));

        if (lock_type != LockType::NoLock && !is_already_held)
        {
            shard_->UpsertLockHoldingTx(
                tx_number, tx_term, cce, lock_type == LockType::WriteLock);
        }

        if (cce->key_lock_ptr_ != nullptr && cce->GetKeyLock().HasWriteLock() &&
            cce->GetKeyLock().WriteLockTx() != tx_number)
        {
            shard_->CheckRecoverTx(
                cce->GetKeyLock().WriteLockTx(), ng_id, ng_term);
        }
    }
    else if (lock_op_status == LockOpStatus::Failed)
    {
        TX_TRACE_ACTION_WITH_CONTEXT(
            req,
            "AcquireCcEntryKeyLock.Failed",
            cce,
            (
                [&req, &cce]() -> std::string
                {
                    return std::string(",\"tx_number\":")
                        .append(std::to_string(req->Txn()))
                        .append(",\"CcEntry\":")
                        .append(FMT_POINTER_TO_UINT64T(cce))
                        .append(",\"CcEntry.key_lock_\":")
                        .append(FMT_POINTER_TO_UINT64T(&(cce->GetKeyLock())));
                }));

        // check and recover conflicted transactions.
        RecoverTxForLockConfilct(cce->GetKeyLock(), lock_type, ng_id, ng_term);
    }
    else
    {
        TX_TRACE_ACTION_WITH_CONTEXT(
            req,
            "AcquireCcEntryKeyLock.Blocked",
            cce,
            (
                [&req, &cce]() -> std::string
                {
                    return std::string(",\"tx_number\":")
                        .append(std::to_string(req->Txn()))
                        .append(",\"CcEntry\":")
                        .append(FMT_POINTER_TO_UINT64T(cce))
                        .append(",\"CcEntry.key_lock_\":")
                        .append(FMT_POINTER_TO_UINT64T(&(cce->GetKeyLock())));
                }));

        // check and recover conflicted transactions.
        RecoverTxForLockConfilct(cce->GetKeyLock(), lock_type, ng_id, ng_term);
    }

    return std::pair<LockType, LockOpStatus>(lock_type, lock_op_status);
}

LockType CcMap::LockHandleForResumedRequest(CcRequestBase *req,
                                            int64_t tx_term,
                                            LruEntry *cce,
                                            RecordStatus cce_payload_status)
{
    TxNumber tx_number = req->Txn();
    LockType acquired_lock = cce->GetKeyLock().LockTypeHeldByTx(tx_number);
    if (cce_payload_status == RecordStatus::Deleted &&
        acquired_lock != LockType::WriteLock)
    {
        cce->GetKeyLock().ReleaseLock(tx_number, shard_, acquired_lock);
        cce->RecycleKeyLock();
        acquired_lock = LockType::NoLock;
    }
    else
    {
        shard_->UpsertLockHoldingTx(
            tx_number, tx_term, cce, acquired_lock == LockType::WriteLock);
    }

    return acquired_lock;
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
    cce->GetKeyLock().DowngradeWriteLock(tx_number, shard_);
    shard_->DecTxHeldWriteLockCount(tx_number);
}

LockType CcMap::CceKeyLockTypeHeldByTx(LruEntry *cce, TxNumber tx_number)
{
    return cce->GetKeyLock().LockTypeHeldByTx(tx_number);
}

void CcMap::ReleaseCceKeyLock(LruEntry *cce, TxNumber tx_number)
{
    if (cce != nullptr)
    {
        bool is_write_lock = (cce->GetKeyLock().HasWriteLock() &&
                              cce->GetKeyLock().WriteLockTx() == tx_number);
        cce->GetKeyLock().ClearTx(tx_number, shard_);
        shard_->DeleteLockHoldingTx(tx_number, cce, is_write_lock);
        if (is_write_lock)
        {
            cce->wlock_ts_ = 0;
        }
        cce->RecycleKeyLock();
    }
}

void CcMap::ReleaseCceGapLock(LruEntry *cce, TxNumber tx_number)
{
    if (cce != nullptr)
    {
        bool is_write_lock = (cce->GetGapLock().HasWriteLock() &&
                              cce->GetGapLock().WriteLockTx() == tx_number);
        cce->GetGapLock().ClearTx(tx_number, shard_);
        shard_->DeleteLockHoldingTx(tx_number, cce, is_write_lock);
        if (is_write_lock)
        {
            cce->wlock_ts_ = 0;
        }
        cce->RecycleGapLock();
    }
}

}  // namespace txservice
