#include "cc/cc_shard.h"

#include "cc/cc_request.h"
#include "cc/ccm_scanner.h"
#include "checkpointer.h"

txservice::TEntry &txservice::CcShard::NewTx(uint64_t start_ts)
{
    start_ts = ts_base_.load(std::memory_order_relaxed);

    // Cicurlar iteration
    size_t cnt = 0;
    while (cnt < tx_vec_.size())
    {
        TEntry &te = tx_vec_[tx_head_];
        if ((te.status_ == TxnStatus::Committed ||
             te.status_ == TxnStatus::Aborted) &&
            start_ts - te.commit_ts_ > 1000)  // in macro sec
        {
            break;
        }

        ++tx_head_;
        if (tx_head_ >= tx_vec_.size())
        {
            tx_head_ = 0;
        }

        ++cnt;
    }

    if (cnt == tx_vec_.size())
    {
        // Increases the capacity of the tx vector.
        tx_head_ = (uint32_t) tx_vec_.size();
        uint32_t new_size = (uint32_t) (tx_vec_.size() * 1.5);
        tx_vec_.reserve(new_size);

        for (uint32_t idx = tx_head_; idx < new_size; ++idx)
        {
            tx_vec_.emplace_back(idx);
        }

        tx_vec_[tx_head_].status_ = TxnStatus::Ongoing;
    }
    else
    {
        tx_vec_[tx_head_].status_ = TxnStatus::Ongoing;
    }

    TEntry &tentry = tx_vec_.at(tx_head_);
    tentry.Reset(start_ts, tx_cnt_);
    ++tx_cnt_;
    ++tx_head_;
    tx_head_ = tx_head_ == tx_vec_.size() ? 0 : tx_head_;

    tentry.commit_ts_ = 0;
    tentry.lower_bound_ = start_ts;
    return tentry;
}

txservice::TEntry *txservice::CcShard::LocateTx(const TxId &tx_id)
{
    if (tx_id.Empty())
    {
        return nullptr;
    }

    TEntry &tentry = tx_vec_.at(tx_id.vec_idx_);
    return tentry.ident_ == tx_id.ident_ ? &tentry : nullptr;
}

inline void txservice::CcShard::DetachLru(LruEntry *entry)
{
    LruEntry *prev = entry->lru_prev_;
    LruEntry *post = entry->lru_next_;
    prev->lru_next_ = post;
    post->lru_prev_ = prev;
    entry->lru_prev_ = nullptr;
    entry->lru_next_ = nullptr;
}

void txservice::CcShard::UpdateLruList(LruEntry *entry)
{
    // Removes the entry from the list, if it's already in the list. A
    // entry's prev and post are both not-null when the entry is in the
    // list. This is because we have a reserved head and tail for the list.
    if (entry->lru_prev_ != nullptr)
    {
        DetachLru(entry);
    }
    else
    {
        ++size_;
    }

    // Inserts the entry as the second-to-last (the tail is reserved).
    LruEntry *second_last = tail_cce_.lru_prev_;
    second_last->lru_next_ = entry;
    entry->lru_prev_ = second_last;
    entry->lru_next_ = &tail_cce_;
    tail_cce_.lru_prev_ = entry;
}

size_t txservice::CcShard::Clean()
{
    LruEntry *cce = head_cce_.lru_next_;
    size_t free_cnt = 0;
    while (free_cnt < CcShard::freeBatchSize && cce != &tail_cce_)
    {
        LruEntry *next_cce = cce->lru_next_;
        if (cce->IsFree())
        {
            CcShard::DetachLru(cce);

            if (cce->ckpt_next_ != nullptr)
            {
                // If the cc entry is in the checkpoint list, removes it from
                // the checkpoint list.
                CcShard::DetachCkpt(cce);
            }

            cce->parent_map_->Clean(cce);
            --size_;
            ++free_cnt;
        }

        cce = next_cce;
    }

    if (free_cnt == 0 && ckpter_ != nullptr)
    {
        ckpter_->Notify();
    }

    return free_cnt;
}