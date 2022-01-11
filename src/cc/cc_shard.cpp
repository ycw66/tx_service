#include "cc/cc_shard.h"

#include "cc/cc_request.h"
#include "cc/ccm_scanner.h"
#include "checkpointer.h"

namespace txservice
{
CcMap *CcShard::GetCcm(const TableName &table_name,
                       uint32_t node_group,
                       int8_t &error_code)
{
    if (node_group == node_id_)
    {
        auto table_it = native_ccms_.find(table_name);
        if (table_it == native_ccms_.end())
        {
            error_code = 1;
            return nullptr;
        }
        else
        {
            error_code = 0;
            return table_it->second.get();
        }
    }
    else
    {
        auto native_table_it = native_ccms_.find(table_name);
        if (native_table_it == native_ccms_.end())
        {
            error_code = 1;
            return nullptr;
        }

        auto table_it = failover_ccms_.try_emplace(table_name);
        std::unordered_map<uint32_t, CcMap::uptr> &ng_ccm =
            table_it.first->second;

        auto ccm_it = ng_ccm.find(node_group);
        if (ccm_it != ng_ccm.end())
        {
            return ccm_it->second.get();
        }
        else
        {
            auto new_ccm_it = ng_ccm.try_emplace(
                node_group, native_table_it->second->Clone());
            return new_ccm_it.first->second.get();
        }
    }
}

void CcShard::Enqueue(uint32_t thd_id, CcRequestBase *req)
{
    assert(thd_id < thd_token_.size());
    bool ret = cc_queue_.enqueue(thd_token_.at(thd_id), req);
    assert(ret == true);

    // Wakes up the thread dedicated to this shard, when it is in the sleep
    // mode.
    if (processor_sleep_.load(std::memory_order_acquire))
    {
        shard_cv_.notify_one();
    }
}

void CcShard::Enqueue(CcRequestBase *req)
{
    bool ret = cc_queue_.enqueue(req);
    assert(ret == true);

    // Wakes up the thread dedicated to this shard, when it is in the sleep
    // mode.
    if (processor_sleep_.load(std::memory_order_acquire))
    {
        shard_cv_.notify_one();
    }
}

TEntry &CcShard::NewTx(uint64_t start_ts)
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

TEntry *CcShard::LocateTx(const TxId &tx_id)
{
    if (tx_id.Empty())
    {
        return nullptr;
    }

    TEntry &tentry = tx_vec_.at(tx_id.vec_idx_);
    return tentry.ident_ == tx_id.ident_ ? &tentry : nullptr;
}

TEntry *CcShard::LocateTx(TxNumber tx_number)
{
    // The lower 4 bytes represent the identity on a core, while the higher 4
    // bytes represent the global core ID.
    uint32_t identity = tx_number & 0xFFFFFFFF;

    for (TEntry &tx_entry : tx_vec_)
    {
        if (tx_entry.ident_ == identity)
        {
            return &tx_entry;
        }
    }

    return nullptr;
}

inline void CcShard::DetachLru(LruEntry *entry)
{
    LruEntry *prev = entry->lru_prev_;
    LruEntry *post = entry->lru_next_;
    prev->lru_next_ = post;
    post->lru_prev_ = prev;
    entry->lru_prev_ = nullptr;
    entry->lru_next_ = nullptr;
}

void CcShard::UpdateLruList(LruEntry *entry)
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

TxLockInfo *CcShard::UpsertLockHoldingTx(TxNumber txn,
                                         int64_t tx_term,
                                         LruEntry *cce_ptr)
{
    auto em_it = lock_holding_txs_.try_emplace(txn, tx_term, Now());
    em_it.first->second.cce_list_.emplace(cce_ptr);
    return &em_it.first->second;
}

void CcShard::DeleteLockHolidngTx(TxNumber txn, LruEntry *cce_ptr)
{
    auto tx_it = lock_holding_txs_.find(txn);
    if (tx_it == lock_holding_txs_.end())
    {
        return;
    }

    TxLockInfo &lk_info = tx_it->second;
    lk_info.cce_list_.erase(cce_ptr);
}

void CcShard::CheckRecoverTx(TxNumber txn,
                             uint32_t cc_ng_id,
                             int64_t cc_ng_term)
{
    auto tx_it = lock_holding_txs_.find(txn);
    if (tx_it == lock_holding_txs_.end())
    {
        return;
    }
    TxLockInfo &lk_info = tx_it->second;

    using namespace std::chrono_literals;
    constexpr uint64_t ts_gap =
        std::chrono::duration_cast<std::chrono::seconds>(5s).count();
    uint64_t now_ts = Now();

    // If the tx has been holding a lock/intention for an extended period of
    // time (more than 5 seconds), inquires the tx's status. If the tx has
    // failed or committed, recovers the orphan lock/intention. Or, does nothing
    // and waits for the tx to make further actions.
    if (now_ts - lk_info.ts_ >= ts_gap &&
        now_ts - lk_info.last_recover_ts_ >= ts_gap)
    {
        Sharder::Instance().RecoverTx(txn, lk_info.term_, cc_ng_id, cc_ng_term);

        // Updates the last_recover_ts field, so that following
        // conflicting tx's will not try recovery immediately,
        // avoiding a flood of recovery requests.
        lk_info.last_recover_ts_ = now_ts;
    }
}

void CcShard::ClearTx(TxNumber txn)
{
    auto tx_it = lock_holding_txs_.find(txn);
    if (tx_it == lock_holding_txs_.end())
    {
        return;
    }

    TxLockInfo &lk_info = tx_it->second;
    for (auto &lru_ptr : lk_info.cce_list_)
    {
        lru_ptr->key_lock_.ClearTx(txn, this);
    }

    lock_holding_txs_.erase(tx_it);
}

size_t CcShard::Clean()
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
}  // namespace txservice