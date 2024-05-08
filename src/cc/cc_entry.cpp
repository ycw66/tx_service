#include "cc/cc_entry.h"

#include "cc/cc_shard.h"

namespace txservice
{
LruEntry::LruEntry()
{
    uint8_t unknown_status = (uint8_t) RecordStatus::Unknown;
    uint64_t init_ts = 0;
    commit_ts_and_status_.store((init_ts << 8) | unknown_status);
}

uint64_t LruEntry::CommitTs() const
{
    return commit_ts_and_status_.load(std::memory_order_relaxed) >> 8;
}

void LruEntry::SetCkptTs(uint64_t ts)
{
#ifndef ON_KEY_OBJECT
    uint64_t curr_ckpt_ts = ckpt_ts_.load(std::memory_order_relaxed);
    while (curr_ckpt_ts < ts &&
           !ckpt_ts_.compare_exchange_weak(
               curr_ckpt_ts, ts, std::memory_order_acq_rel))
        ;
#else
    uint64_t curr_val = commit_ts_and_status_.load(std::memory_order_relaxed);
    uint64_t curr_commit_ts = curr_val >> 8;
    while (curr_commit_ts <= ts &&
           !commit_ts_and_status_.compare_exchange_weak(
               curr_val, curr_val | 0x10, std::memory_order_acq_rel))
    {
        curr_commit_ts = curr_val >> 8;
    }
#endif
}

bool LruEntry::IsPersistent() const
{
#ifndef ON_KEY_OBJECT
    return CommitTs() <= ckpt_ts_;
#else
    uint64_t curr_val = commit_ts_and_status_.load(std::memory_order_relaxed);
    // The fifth bit represents if the latest version has been flushed.
    return curr_val & 0x10;
#endif
}

RecordStatus LruEntry::PayloadStatus() const
{
    uint64_t curr_val = commit_ts_and_status_.load(std::memory_order_relaxed);
    // The lowest 4 bits encode the record status.
    RecordStatus status = static_cast<RecordStatus>(curr_val & 0x0F);
    return status;
}

void LruEntry::SetCommitTsPayloadStatus(uint64_t ts, RecordStatus status)
{
    uint8_t stat = static_cast<uint8_t>(status);
    uint64_t curr_val = commit_ts_and_status_.load(std::memory_order_relaxed);
    uint64_t curr_ts = curr_val >> 8;

    // Updates the commit timestamp and the record status, if the current commit
    // timestmap is smaller than the input one. The commit timestamp and record
    // status can only be updated by a single tx processor thread. But since we
    // use the 5th bit to represent if the record has been flushed when MVCC is
    // not needed, the checkpoint thread may concurrently update
    // commit_ts_and_status_ to flip the bit. The atomic update ensures that if
    // the input timestamp is larger, (1) the input commit timestamp is not
    // overwritten by the checkpoint thread, and (2) the flush bit is 0 after
    // the update.
    while (curr_ts < ts &&
           !commit_ts_and_status_.compare_exchange_weak(
               curr_val, (ts << 8) | stat, std::memory_order_acq_rel))
    {
        curr_ts = curr_val >> 8;
    }
}

bool LruEntry::IsFree() const
{
    // As long as all locks are released, the lock associated with this cc entry
    // should be recycled.
    assert(cc_lock_and_extra_ == nullptr ||
           !cc_lock_and_extra_->KeyLock()->IsEmpty());

    return cc_lock_and_extra_ == nullptr && IsPersistent();
}

NonBlockingLock &LruEntry::GetOrCreateKeyLock(CcShard *ccs,
                                              CcMap *ccm,
                                              LruPage *page)
{
    if (cc_lock_and_extra_ == nullptr)
    {
        cc_lock_and_extra_ = ccs->NewLock(ccm, page);
    }

    assert(cc_lock_and_extra_->GetCcMap() == ccm);
    // For cc entries of the bucket cc map, the input page may be null.
    assert(page == nullptr || cc_lock_and_extra_->GetCcPage() == nullptr ||
           cc_lock_and_extra_->GetCcPage() == page);
    return *cc_lock_and_extra_->KeyLock();
}

NonBlockingLock *LruEntry::GetKeyLock() const
{
    return cc_lock_and_extra_ == nullptr ? nullptr
                                         : cc_lock_and_extra_->KeyLock();
}

NonBlockingLock *LruEntry::GetGapLock() const
{
    assert("Gap lock unsupported.");
    return nullptr;
}

bool LruEntry::RecycleKeyLock(CcShard &ccs)
{
    if (cc_lock_and_extra_ != nullptr && cc_lock_and_extra_->IsEmpty())
    {
        // recycle key lock if all the locks in lock entry are released.
        cc_lock_and_extra_->SetUsedStatus(false);
        ccs.DecreaseLockCount();
        cc_lock_and_extra_ = nullptr;
        return true;
    }
    return false;
}

void LruEntry::ClearLocks(CcShard &ccs,
                          NodeGroupId ng_id,
                          bool invalidate_owner_term)
{
    if (cc_lock_and_extra_ == nullptr)
    {
        return;
    }

    NonBlockingLock *key_lock = cc_lock_and_extra_->KeyLock();

    // Deletes the write lock/intent.
    auto [w_tx, w_type] = key_lock->WriteTx();
    if (w_type != NonBlockingLock::WriteLockType::NoWritelock)
    {
        ccs.DeleteLockHoldingTx(w_tx, this, ng_id, invalidate_owner_term);
    }

    // Deletes key read locks.
    const std::unordered_set<TxNumber> &key_read_locks = key_lock->ReadLocks();
    for (const TxNumber &txn : key_read_locks)
    {
        ccs.DeleteLockHoldingTx(txn, this, ng_id, invalidate_owner_term);
    }

    for (const TxNumber &txn : key_lock->ReadIntents())
    {
        ccs.DeleteLockHoldingTx(txn, this, ng_id, invalidate_owner_term);
    }

    // reset lock entry in ccshard lock array to make it reusable.
    cc_lock_and_extra_->SetUsedStatus(false);
    cc_lock_and_extra_ = nullptr;
    ccs.DecreaseLockCount();
}

CcMap *LruEntry::GetCcMap() const
{
    return cc_lock_and_extra_ != nullptr ? cc_lock_and_extra_->GetCcMap()
                                         : nullptr;
}

LruPage *LruEntry::GetCcPage() const
{
    return cc_lock_and_extra_ != nullptr ? cc_lock_and_extra_->GetCcPage()
                                         : nullptr;
}

void LruEntry::UpdateCcPage(LruPage *page)
{
    if (cc_lock_and_extra_ != nullptr)
    {
        cc_lock_and_extra_->UpdateCcPage(page);
    }
}

TxKey FlushRecord::Key() const
{
    if (key_type_ == FlushKeyType::TxKey)
    {
        return tx_key_.GetShallowCopy();
    }
    else
    {
        assert(
            "The flush key is of type KeyIndex and cannot return the key "
            "pointer.");
        return TxKey();
    }
}
}  // namespace txservice
