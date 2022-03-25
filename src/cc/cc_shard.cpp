#include "cc/cc_shard.h"

#include "cc/catalog_cc_map.h"
#include "cc/cc_request.h"
#include "cc/ccm_scanner.h"
#include "checkpointer.h"

namespace txservice
{
CcShard::CcShard(uint16_t core_id,
                 uint32_t core_cnt,
                 uint64_t base_ts,
                 uint32_t node_id,
                 LocalCcShards &local_shards,
                 CatalogFactory *catalog_factory)
    : node_id_(node_id),
      core_id_(core_id),
      core_cnt_(core_cnt),
      local_shards_(local_shards),
      native_ccms_(),
      failover_ccms_(),
      cc_queue_(256),
      req_buf_(),
      tx_vec_(),
      next_tx_idx_(0),
      next_tx_ident_(0),
      ts_base_(base_ts),
      head_cce_(nullptr),
      tail_cce_(nullptr),
      size_(0),
      ckpter_(nullptr),
      processor_sleep_(false),
      catalog_factory_(catalog_factory)
{
    tx_vec_.reserve(128);
    for (int idx = 0; idx < 128; ++idx)
    {
        tx_vec_.emplace_back(idx);
    }

    head_cce_.lru_prev_ = nullptr;
    head_cce_.lru_next_ = &tail_cce_;
    tail_cce_.lru_prev_ = &head_cce_;
    tail_cce_.lru_next_ = nullptr;

    thd_token_.reserve((size_t) core_cnt + 1);
    for (size_t idx = 0; idx < core_cnt; ++idx)
    {
        thd_token_.emplace_back(moodycamel::ProducerToken(cc_queue_));
    }

    native_ccms_.try_emplace(catalog_ccm_name,
                             std::make_unique<CatalogCcMap>(this));
}

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
            auto native_table_it = native_ccms_.find(table_name);
            if (native_table_it == native_ccms_.end())
            {
                error_code = 1;
                return nullptr;
            }

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

TEntry &CcShard::NewTx()
{
    // allocate start timestamp.
    uint64_t start_ts = ts_base_.load(std::memory_order_relaxed);

    // Cicurlar iteration to find an available transaction entry.
    size_t cnt = 0;
    while (cnt < tx_vec_.size())
    {
        TEntry &te = tx_vec_[next_tx_idx_];
        if (te.status_ == TxnStatus::Finished)
        {
            break;
        }

        ++next_tx_idx_;
        if (next_tx_idx_ >= tx_vec_.size())
        {
            next_tx_idx_ = 0;
        }

        ++cnt;
    }

    if (cnt == tx_vec_.size())
    {
        uint32_t old_size = (uint32_t) tx_vec_.size();
        // Increases the capacity of the tx vector.
        uint32_t new_size = (uint32_t) (tx_vec_.size() * 1.5);
        tx_vec_.reserve(new_size);

        for (uint32_t idx = old_size; idx < new_size; ++idx)
        {
            tx_vec_.emplace_back(idx);
        }

        // position old_size must be an available slot.
        next_tx_idx_ = old_size;
    }

    TEntry &tentry = tx_vec_.at(next_tx_idx_);
    // Reset() set lower_bound ts and commit ts.
    tentry.Reset(start_ts, next_tx_ident_);
    ++next_tx_ident_;
    ++next_tx_idx_;
    next_tx_idx_ = next_tx_idx_ == tx_vec_.size() ? 0 : next_tx_idx_;
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

void CcShard::DetachLru(LruEntry *entry)
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
    // Removes the entry from the list, if it's already in the list. This is
    // used to keep the updated entry at the end(tail) of the LRU list. A
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

void CcShard::DetachCkpt(LruEntry *entry)
{
    LruEntry *prev = entry->ckpt_prev_;
    LruEntry *post = entry->ckpt_next_;
    prev->ckpt_next_ = post;
    post->ckpt_prev_ = prev;
    entry->ckpt_prev_ = nullptr;
    entry->ckpt_next_ = nullptr;

    estimate_ccshard_log_size_ -= entry->estimate_ccentry_log_size_;
    entry->estimate_ccentry_log_size_ = 0;
}

void CcShard::UpdateEstimateLogSize(LruEntry *entry,
                                    size_t key_size,
                                    size_t payload_size)
{
    entry->estimate_ccentry_log_size_ += key_size + payload_size;
    estimate_ccshard_log_size_ += key_size + payload_size;

    if (estimate_ccshard_log_size_ >= log_size_limit)
    {
        NotifyCkpt();
    }
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
    if (lk_info.cce_list_.empty())
    {
        lock_holding_txs_.erase(tx_it);
    }
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
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::seconds(5))
            .count();
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

/**
 * @brief Kick out freeable entries from ccmap.
 *
 * @return the number of freed entries in ccmap.
 */
size_t CcShard::Clean()
{
    LruEntry *cce = head_cce_.lru_next_;
    size_t free_cnt = 0;
    while (free_cnt < CcShard::freeBatchSize && cce != &tail_cce_)
    {
        LruEntry *next_cce = cce->lru_next_;
        if (cce->IsFree())
        {
            cce->parent_map_->Clean(cce);
            --size_;
            ++free_cnt;
        }

        cce = next_cce;
    }

    // notify the checkpointer thread to do checkpoint if there is not freeable
    // entries to be kicked out from ccmap.
    if (free_cnt == 0)
    {
        NotifyCkpt();
    }

    return free_cnt;
}

void CcShard::NotifyCkpt()
{
    if (ckpter_ != nullptr)
    {
        ckpter_->Notify();
    }
}

const TableSchemaView *CcShard::CreateCatalog(const TableName &table_name,
                                              const std::string &catalog_image,
                                              uint64_t commit_ts)
{
    return local_shards_.CreateCatalog(table_name, catalog_image, commit_ts);
}

const TableSchemaView *CcShard::CreateDirtyCatalog(
    const std::string &table_name,
    const std::string &catalog_image,
    uint64_t commit_ts)
{
    return local_shards_.CreateDirtyCatalog(
        table_name, catalog_image, commit_ts);
}

const TableSchemaView *CcShard::CommitDirtyCatalog(const TableName &table_name)
{
    return local_shards_.CommitDirtyCatalog(table_name);
}

const TableSchemaView *CcShard::GetCatalog(const std::string &table_name)
{
    return local_shards_.GetCatalog(table_name);
}

void CcShard::FetchCatalog(const TableName &table_name,
                           CcRequestBase *requester)
{
    auto tab_it =
        fetch_catalog_reqs_.try_emplace(table_name, table_name, *this);
    FetchCatalogCc &fetch_req = tab_it.first->second;
    fetch_req.AddRequester(requester);

    if (fetch_req.RequesterCount() == 1)
    {
        local_shards_.store_hd_->FetchTableCatalog(table_name, &fetch_req);
    }
}

void CcShard::RemoveFetchRequest(const TableName &table_name)
{
    fetch_catalog_reqs_.erase(table_name);
}

CcMap *CcShard::CreatePkCcMap(const TableName &table_name,
                              const TableSchema *table_schema,
                              NodeGroupId ng_id)
{
    if (ng_id == node_id_)
    {
        auto ccm_it = native_ccms_.try_emplace(
            table_name, catalog_factory_->CreatePkCcMap(table_schema, this));
        return ccm_it.first->second.get();
    }
    else
    {
        auto fail_ccm_it = failover_ccms_.try_emplace(table_name).first;
        std::unordered_map<NodeGroupId, CcMap::uptr> &ccms =
            fail_ccm_it->second;
        auto ccm_it = ccms.try_emplace(
            ng_id, catalog_factory_->CreatePkCcMap(table_schema, this));
        return ccm_it.first->second.get();
    }
}

CcMap *CcShard::CreateSkCcMap(const TableName &index_name,
                              const TableSchema *table_schema,
                              NodeGroupId ng_id)
{
    if (ng_id == node_id_)
    {
        auto ccm_it = native_ccms_.try_emplace(
            index_name,
            catalog_factory_->CreateSkCcMap(index_name, table_schema, this));
        return ccm_it.first->second.get();
    }
    else
    {
        auto fail_ccm_it = failover_ccms_.try_emplace(index_name).first;
        std::unordered_map<NodeGroupId, CcMap::uptr> &ccms =
            fail_ccm_it->second;
        auto ccm_it = ccms.try_emplace(
            ng_id,
            catalog_factory_->CreateSkCcMap(index_name, table_schema, this));
        return ccm_it.first->second.get();
    }
}

void CcShard::DropCcm(const TableName &table_name, NodeGroupId ng_id)
{
    if (ng_id == node_id_)
    {
        native_ccms_.erase(table_name);
    }
    else
    {
        auto fail_ccm_it = failover_ccms_.find(table_name);
        if (fail_ccm_it != failover_ccms_.end())
        {
            std::unordered_map<NodeGroupId, CcMap::uptr> &ccms =
                fail_ccm_it->second;
            ccms.erase(ng_id);
            if (ccms.empty())
            {
                failover_ccms_.erase(fail_ccm_it);
            }
        }
    }
}

}  // namespace txservice
