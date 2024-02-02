#include "cc/cc_entry.h"

#include "cc/cc_shard.h"

namespace txservice
{
bool LruEntry::IsFree()
{
    // As long as all locks are released, the lock associated with this cc entry
    // should be recycled.
    assert(cc_lock_ == nullptr || !cc_lock_->KeyLock()->IsEmpty());

    return cc_lock_ == nullptr &&
           commit_ts_ <= ckpt_ts_.load(std::memory_order_acquire);
}

NonBlockingLock &LruEntry::GetOrCreateKeyLock(CcShard *ccs,
                                              CcMap *ccm,
                                              LruPage *page)
{
    if (cc_lock_ == nullptr)
    {
        cc_lock_ = ccs->NewLock(ccm, page);
    }

    assert(cc_lock_->GetCcMap() == ccm);
    // For cc entries of the bucket cc map, the input page may be null.
    assert(page == nullptr || cc_lock_->GetCcPage() == nullptr ||
           cc_lock_->GetCcPage() == page);
    return *cc_lock_->KeyLock();
}

NonBlockingLock *LruEntry::GetKeyLock() const
{
    return cc_lock_ == nullptr ? nullptr : cc_lock_->KeyLock();
}

NonBlockingLock *LruEntry::GetGapLock() const
{
    assert("Gap lock unsupported.");
    return nullptr;
}

void LruEntry::RecycleKeyLock(CcShard &ccs)
{
    if (cc_lock_ != nullptr && cc_lock_->KeyLock()->IsEmpty())
    {
        // recycle key lock if all the locks in lock entry are released.
        cc_lock_->SetUsedStatus(false);
        ccs.DecreaseLockCount();
        cc_lock_ = nullptr;
    }
}

void LruEntry::ClearLocks(CcShard &ccs, NodeGroupId ng_id)
{
    if (cc_lock_ == nullptr)
    {
        return;
    }

    NonBlockingLock *key_lock = cc_lock_->KeyLock();

    // Deletes the write lock/intent.
    auto [w_tx, w_type] = key_lock->WriteTx();
    if (w_type != NonBlockingLock::WriteLockType::NoWritelock)
    {
        ccs.DeleteLockHoldingTx(w_tx, this, ng_id);
    }

    // Deletes key read locks.
    const std::unordered_set<TxNumber> &key_read_locks = key_lock->ReadLocks();
    for (const TxNumber &txn : key_read_locks)
    {
        ccs.DeleteLockHoldingTx(txn, this, ng_id);
    }

    for (const TxNumber &txn : key_lock->ReadIntents())
    {
        ccs.DeleteLockHoldingTx(txn, this, ng_id);
    }

    // reset lock entry in ccshard lock array to make it reusable.
    cc_lock_->SetUsedStatus(false);
    cc_lock_ = nullptr;
    ccs.DecreaseLockCount();
}

CcMap *LruEntry::GetCcMap() const
{
    return cc_lock_ != nullptr ? cc_lock_->GetCcMap() : nullptr;
}

LruPage *LruEntry::GetCcPage() const
{
    return cc_lock_ != nullptr ? cc_lock_->GetCcPage() : nullptr;
}

void LruEntry::UpdateCcPage(LruPage *page)
{
    if (cc_lock_ != nullptr)
    {
        cc_lock_->UpdateCcPage(page);
    }
}

const TxKey *FlushRecord::Key() const
{
    if (is_key_owner_)
    {
        return key_.uptr_.get();
    }
    return key_.ptr_;
}
}  // namespace txservice