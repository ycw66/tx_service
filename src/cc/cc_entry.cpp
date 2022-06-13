#include "cc/cc_entry.h"

#include "cc/cc_shard.h"

namespace txservice
{
LruEntry::~LruEntry()
{
    // Deletes key write lock.
    if (key_lock_.HasWriteLock())
    {
        parent_map_->shard_->DeleteLockHolidngTx(
            key_lock_.WriteLockTx(), this, true);
    }
    // Deletes gap write lock.
    if (gap_lock_.HasWriteLock())
    {
        parent_map_->shard_->DeleteLockHolidngTx(
            gap_lock_.WriteLockTx(), this, false);
    }

    // Deletes key write intent.
    if (key_lock_.HasWriteIntent())
    {
        parent_map_->shard_->DeleteLockHolidngTx(
            key_lock_.WriteIntentTx(), this, false);
    }
    // Deletes gap write intent.
    if (gap_lock_.HasWriteIntent())
    {
        parent_map_->shard_->DeleteLockHolidngTx(
            gap_lock_.WriteIntentTx(), this, false);
    }

    // Deletes key read locks.
    const std::unordered_set<TxNumber> &key_read_locks = key_lock_.ReadLocks();
    for (const TxNumber &txn : key_read_locks)
    {
        parent_map_->shard_->DeleteLockHolidngTx(txn, this, false);
    }

    // Deletes gap read locks.
    const std::unordered_set<TxNumber> &gap_read_locks = gap_lock_.ReadLocks();
    for (const TxNumber &txn : gap_read_locks)
    {
        parent_map_->shard_->DeleteLockHolidngTx(txn, this, false);
    }

    for (const TxNumber &txn : key_lock_.ReadIntents())
    {
        parent_map_->shard_->DeleteLockHolidngTx(txn, this, false);
    }

    for (const TxNumber &txn : gap_lock_.ReadIntents())
    {
        parent_map_->shard_->DeleteLockHolidngTx(txn, this, false);
    }
}

LruEntry::LruEntry(CcMap *parent) : parent_map_(parent)
{
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

bool LruEntry::IsFree() const
{
    return key_lock_.IsEmpty() && gap_lock_.IsEmpty() &&
           commit_ts_ <= ckpt_ts_.load(std::memory_order_acquire);
}
}  // namespace txservice