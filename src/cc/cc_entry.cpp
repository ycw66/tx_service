#include "cc/cc_entry.h"

#include "cc/cc_shard.h"

namespace txservice
{
LruEntry::~LruEntry()
{
    if (key_lock_ptr_ != nullptr && parent_map_)
    {
        CcShard *ccshard = parent_map_->shard_;
        // Deletes key write lock.
        if (key_lock_ptr_->HasWriteLock())
        {
            ccshard->DeleteLockHoldingTx(key_lock_ptr_->WriteLockTx(), this);
        }

        // Deletes key write intent.
        if (key_lock_ptr_->HasWriteIntent())
        {
            ccshard->DeleteLockHoldingTx(key_lock_ptr_->WriteIntentTx(), this);
        }

        // Deletes key read locks.
        const std::unordered_set<TxNumber> &key_read_locks =
            key_lock_ptr_->ReadLocks();
        for (const TxNumber &txn : key_read_locks)
        {
            ccshard->DeleteLockHoldingTx(txn, this);
        }

        for (const TxNumber &txn : key_lock_ptr_->ReadIntents())
        {
            ccshard->DeleteLockHoldingTx(txn, this);
        }

        // reset lock entry in ccshard lock array to make it reusable.
        key_lock_ptr_->SetUsedStatus(false);
        key_lock_ptr_ = nullptr;
        ccshard->DecreaseLockCount();
    }

    if (gap_lock_ptr_ != nullptr && parent_map_)
    {
        CcShard *ccshard = parent_map_->shard_;
        // Deletes gap write lock.
        if (gap_lock_ptr_->HasWriteLock())
        {
            ccshard->DeleteLockHoldingTx(gap_lock_ptr_->WriteLockTx(), this);
        }

        // Deletes gap write intent.
        if (gap_lock_ptr_->HasWriteIntent())
        {
            ccshard->DeleteLockHoldingTx(gap_lock_ptr_->WriteIntentTx(), this);
        }

        // Deletes gap read locks.
        const std::unordered_set<TxNumber> &gap_read_locks =
            gap_lock_ptr_->ReadLocks();
        for (const TxNumber &txn : gap_read_locks)
        {
            ccshard->DeleteLockHoldingTx(txn, this);
        }

        for (const TxNumber &txn : gap_lock_ptr_->ReadIntents())
        {
            ccshard->DeleteLockHoldingTx(txn, this);
        }

        // reset lock entry in ccshard lock array to make it reusable.
        gap_lock_ptr_->SetUsedStatus(false);
        gap_lock_ptr_ = nullptr;
        ccshard->DecreaseLockCount();
    }
}

LruEntry::LruEntry(CcMap *parent) : parent_map_(parent)
{
    // ccshard head_cce and tail_cce's parent ccmaps are null.
    if (parent != nullptr)
    {
        uint64_t now_ts = parent->shard_->Now();
        last_read_ts_ = now_ts;
        gap_last_read_ts_ = now_ts;
    }
    else
    {
        last_read_ts_ = 1;
        gap_last_read_ts_ = 1;
    }
}

bool LruEntry::IsFree()
{
    return (key_lock_ptr_ == nullptr || key_lock_ptr_->IsEmpty()) &&
           (gap_lock_ptr_ == nullptr || gap_lock_ptr_->IsEmpty()) &&
           commit_ts_ <= ckpt_ts_.load(std::memory_order_acquire);
}

NonBlockingLock &LruEntry::GetKeyLock()
{
    // key lock
    if (key_lock_ptr_ == nullptr)
    {
        key_lock_ptr_ = parent_map_->shard_->NewLock();
    }
    return *key_lock_ptr_;
}

NonBlockingLock &LruEntry::GetGapLock()
{
    // gap lock
    if (gap_lock_ptr_ == nullptr)
    {
        gap_lock_ptr_ = parent_map_->shard_->NewLock();
    }
    return *gap_lock_ptr_;
}

void LruEntry::RecycleKeyLock()
{
    // key lock
    if (key_lock_ptr_ != nullptr && key_lock_ptr_->IsEmpty())
    {
        // recycle key lock if all the locks in lock entry are released.
        key_lock_ptr_->SetUsedStatus(false);
        parent_map_->shard_->DecreaseLockCount();
        key_lock_ptr_ = nullptr;
    }
    return;
}

void LruEntry::RecycleGapLock()
{
    // key lock
    if (gap_lock_ptr_ != nullptr && gap_lock_ptr_->IsEmpty())
    {
        // recycle key lock if all the locks in lock entry are released.
        gap_lock_ptr_->SetUsedStatus(false);
        parent_map_->shard_->DecreaseLockCount();
        gap_lock_ptr_ = nullptr;
    }
    return;
}

const TxKey *FlushRecord::Key() const
{
    return cce_->Key();
}
}  // namespace txservice