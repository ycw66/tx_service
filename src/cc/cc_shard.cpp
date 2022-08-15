#include "cc/cc_shard.h"

#include "archives_flusher.h"
#include "cc/catalog_cc_map.h"
#include "cc/cc_request.h"
#include "cc/ccm_scanner.h"
#include "checkpointer.h"
#include "sharder.h"  // Sharder
#include "tx_start_ts_collector.h"

namespace txservice
{
CcShard::CcShard(uint16_t core_id,
                 uint32_t core_cnt,
                 uint32_t node_memory_limit_mb,
                 uint32_t node_log_limit_mb,
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
    // memory_limit_ and log_limit_ are calculated at shard level.
    memory_limit_ = (uint64_t) MB(node_memory_limit_mb);
    memory_limit_ /= core_cnt_;
    log_limit_ = (uint64_t) MB(node_log_limit_mb);
    log_limit_ /= core_cnt_;

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

    native_ccms_.try_emplace(
        catalog_ccm_name,
        std::make_unique<CatalogCcMap>(this, catalog_ccm_name));
}

CcMap *CcShard::GetCcm(const TableName &table_name, uint32_t node_group)
{
    if (node_group == node_id_)
    {
        auto table_it = native_ccms_.find(table_name);
        if (table_it == native_ccms_.end())
        {
            return nullptr;
        }
        else
        {
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
        else if (table_name == catalog_ccm_name)
        {
            // The catalog cc map "___catalog" is initialized as one of native
            // cc maps when the cc shard is initialized. The cc map in failed
            // over cc node is initialized lazily, when the cc node becomes the
            // leader.
            auto catalog_it = ng_ccm.try_emplace(
                node_group,
                std::make_unique<CatalogCcMap>(this, catalog_ccm_name));

            return catalog_it.first->second.get();
        }
        else
        {
            return nullptr;
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
    int64_t term = Sharder::Instance().LeaderTerm(node_id_);

    // Cicurlar iteration to find an available transaction entry.
    size_t cnt = 0;
    while (cnt < tx_vec_.size())
    {
        TEntry &te = tx_vec_[next_tx_idx_];
        if (te.status_ == TxnStatus::Finished ||
            te.status_ == TxnStatus::Committed ||
            te.status_ == TxnStatus::Aborted)
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
    tentry.Reset(start_ts, next_tx_ident_, term);
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
    // The lower 4 bytes represent the identity on a core, while the higher
    // 4 bytes represent the global core ID.
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
}

void CcShard::UpdateEstimateLogSize(LruEntry *entry,
                                    size_t key_size,
                                    size_t payload_size)
{
    entry->estimate_ccentry_log_size_ += key_size + payload_size;
    estimate_ccshard_log_size_ += key_size + payload_size;

    if (estimate_ccshard_log_size_ >= log_limit_)
    {
        NotifyCkpt();
    }
}

TxLockInfo *CcShard::UpsertLockHoldingTx(TxNumber txn,
                                         int64_t tx_term,
                                         LruEntry *cce_ptr,
                                         bool is_key_write_lock)
{
    auto em_it = lock_holding_txs_.try_emplace(txn, tx_term, Now());
    em_it.first->second.cce_list_.emplace(cce_ptr);
    if (is_key_write_lock)
    {
        // write lock should update ts if the txn exists, or the CkptTsCc
        // request may get an older ckpt_ts.
        if (!em_it.second)
        {
            em_it.first->second.ts_ = Now();
        }
        em_it.first->second.key_write_lock_count_++;
    }
    return &em_it.first->second;
}

void CcShard::DeleteLockHoldingTx(TxNumber txn,
                                  LruEntry *cce_ptr,
                                  bool is_key_write_lock)
{
    auto tx_it = lock_holding_txs_.find(txn);
    if (tx_it == lock_holding_txs_.end())
    {
        return;
    }

    TxLockInfo &lk_info = tx_it->second;
    lk_info.cce_list_.erase(cce_ptr);
    if (is_key_write_lock)
    {
        lk_info.key_write_lock_count_--;
    }

    if (lk_info.cce_list_.empty())
    {
        lock_holding_txs_.erase(tx_it);
    }
}

void CcShard::CheckRecoverTx(TxNumber lock_holding_txn,
                             uint32_t cc_ng_id,
                             int64_t cc_ng_term)
{
    auto tx_it = lock_holding_txs_.find(lock_holding_txn);
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
    // failed or committed, recovers the orphan lock/intention. Or, does
    // nothing and waits for the tx to make further actions.
    if (now_ts - lk_info.ts_ >= ts_gap &&
        now_ts - lk_info.last_recover_ts_ >= ts_gap)
    {
        uint32_t txn_node_group = lock_holding_txn >> 42L;
        if (txn_node_group == Sharder::Instance().NodeId())
        {
            LOG(WARNING)
                << "orphan lock detected, lock holding txn: "
                << lock_holding_txn
                << ", txn is initiated by this machine, no need to recover";
            // no need to check and recover local txn, it must be ongoing
            return;
        }
        LOG(WARNING) << "orphan lock detected, lock holding txn: "
                     << lock_holding_txn << ", try to recover";
        Sharder::Instance().RecoverTx(lock_holding_txn,
                                      lk_info.tx_coord_term_,
                                      cc_ng_id,
                                      cc_ng_term,
                                      lk_info.key_write_lock_count_);

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

uint64_t CcShard::StatMinTxStartTs()
{
    const int64_t term = Sharder::Instance().LeaderTerm(node_id_);
    if (tx_vec_.size() == 0)
    {
        min_tx_start_ts_term_.store(term);
        min_tx_start_ts_ = ts_base_.load(std::memory_order_acquire);
    }
    else
    {
        uint64_t min_ts = ts_base_.load(std::memory_order_acquire);
        for (const auto &tx : tx_vec_)
        {
            if (tx.term_ != term || tx.status_ == TxnStatus::Finished ||
                tx.status_ == TxnStatus::Aborted ||
                tx.status_ == TxnStatus::Committed)
            {
                continue;
            }
            min_ts = std::min(min_ts, tx.lower_bound_);
        }

        min_tx_start_ts_term_.store(term);
        min_tx_start_ts_.store(min_ts);
    }
    return min_tx_start_ts_;
}

uint64_t CcShard::GlobalMinTxStartTs()
{
    return TxStartTsCollector::Instance().GlobalMinTxStartTs();
}

/**
 * @brief Kick out freeable entries from ccmap.
 *
 * @return the number of freed entries in ccmap.
 */
size_t CcShard::Clean()
{
    LruEntry *cce = head_cce_.lru_next_;

    // 1- Clean the lru entry which is free and has no historical versions.
    size_t free_cnt = 0;
    size_t mem_size = 0;
    size_t scan_cnt = 0;
    size_t scan_batch_size = CcShard::freeBatchSize * 2;
    uint64_t recyle_ts = GlobalMinTxStartTs();
    while (scan_cnt < scan_batch_size && cce != &tail_cce_)
    {
        LruEntry *next_cce = cce->lru_next_;
        mem_size += cce->KickOutArchiveRecords(recyle_ts);
        if (cce->IsFree() && cce->ArchiveRecordsCount() == 0)
        {
            cce->parent_map_->ccm_has_full_entries_ = false;
            cce->parent_map_->Clean(cce);
            --size_;
            ++free_cnt;
        }

        cce = next_cce;
        scan_cnt++;
    }

    if (mem_size > 0 || free_cnt > 0)
    {
        DecrementMemory(mem_size);
        return 1;
    }

    // 2- Flush historical versions and clean entry.
    scan_cnt = 0;
    cce = head_cce_.lru_next_;
    while (scan_cnt < CcShard::freeBatchSize && cce != &tail_cce_)
    {
        LruEntry *next_cce = cce->lru_next_;
        if (cce->IsFree())
        {
            // Save archives to data store.
            ArchivesFlusher::Instance().AddTask(cce);
            ++scan_cnt;
        }

        cce = next_cce;
    }

    // 3- Notify the checkpointer thread to do checkpoint if there is not
    // freeable entries to be kicked out from ccmap.
    if (free_cnt == 0)
    {
        NotifyCkpt();
    }

    return free_cnt;
}

/**
 * @brief Flush Entry to KvStore. Now, only used for test.
 *
 */
bool CcShard::FlushEntry(LruEntry *entry, bool only_archives)
{
    // TODO(lzx): Now, only flush archives synchronously for test.
    if (only_archives)
    {
        return ArchivesFlusher::Instance().Flush(entry);
    }
    else
    {
        return (ckpter_->CkptEntry(entry)) &&
               (ArchivesFlusher::Instance().Flush(entry));
    }
}

void CcShard::NotifyCkpt()
{
    if (ckpter_ != nullptr)
    {
        ckpter_->Notify();
    }
}

const CatalogEntry *CcShard::CreateCatalog(const TableName &table_name,
                                           NodeGroupId cc_ng_id,
                                           const std::string &catalog_image,
                                           uint64_t commit_ts)
{
    return local_shards_.CreateCatalog(
        table_name, cc_ng_id, catalog_image, commit_ts);
}

const CatalogEntry *CcShard::CreateDirtyCatalog(
    const std::string &table_name,
    NodeGroupId cc_ng_id,
    const std::string &catalog_image,
    uint64_t commit_ts)
{
    return local_shards_.CreateDirtyCatalog(
        table_name, cc_ng_id, catalog_image, commit_ts);
}

void CcShard::CommitDirtyCatalog(const TableName &table_name,
                                 NodeGroupId cc_ng_id)
{
    local_shards_.CommitDirtyCatalog(table_name, cc_ng_id);
}

const CatalogEntry *CcShard::GetCatalog(const std::string &table_name,
                                        NodeGroupId cc_ng_id)
{
    return local_shards_.GetCatalog(table_name, cc_ng_id);
}

void CcShard::InitTableRanges(const TableName &range_table_name,
                              std::vector<InitRangeEntry> &init_ranges)
{
    local_shards_.InitTableRanges(range_table_name, init_ranges);
}

const std::map<uint32_t, TableRangeEntry> *CcShard::GetTableRanges(
    const TableName &range_table_name)
{
    return local_shards_.GetTableRanges(range_table_name);
}

void CcShard::FetchCatalog(const TableName &table_name,
                           NodeGroupId cc_ng_id,
                           CcRequestBase *requester)
{
    auto tab_it = fetch_reqs_.try_emplace(
        table_name,
        std::make_unique<FetchCatalogCc>(table_name, *this, cc_ng_id));
    FetchCatalogCc *fetch_req =
        static_cast<FetchCatalogCc *>(tab_it.first->second.get());

    fetch_req->AddRequester(requester);
    if (fetch_req->RequesterCount() == 1)
    {
        local_shards_.store_hd_->FetchTableCatalog(table_name, fetch_req);
    }
}

void CcShard::FetchTableRanges(const TableName &range_table_name,
                               const Schema *key_schema,
                               CcRequestBase *requester)
{
    auto table_it =
        fetch_reqs_.try_emplace(range_table_name,
                                std::make_unique<FetchTableRangesCc>(
                                    range_table_name, key_schema, *this));
    FetchTableRangesCc *fetch_req =
        static_cast<FetchTableRangesCc *>(table_it.first->second.get());

    fetch_req->AddRequester(requester);
    if (fetch_req->RequesterCount() == 1)
    {
        local_shards_.store_hd_->FetchTableRanges(range_table_name, fetch_req);
    }
}

void CcShard::RemoveFetchRequest(const TableName &table_name)
{
    fetch_reqs_.erase(table_name);
}

CcMap *CcShard::CreatePkCcMap(const TableName &table_name,
                              const TableSchema *table_schema,
                              NodeGroupId ng_id,
                              uint64_t schema_ts,
                              bool ccm_has_full_entries)
{
    if (ng_id == node_id_)
    {
        auto ccm_it = native_ccms_.try_emplace(
            table_name,
            catalog_factory_->CreatePkCcMap(table_name,
                                            table_schema,
                                            schema_ts,
                                            ccm_has_full_entries,
                                            this));
        return ccm_it.first->second.get();
    }
    else
    {
        auto fail_ccm_it = failover_ccms_.try_emplace(table_name).first;
        std::unordered_map<NodeGroupId, CcMap::uptr> &ccms =
            fail_ccm_it->second;
        auto ccm_it = ccms.try_emplace(
            ng_id,
            catalog_factory_->CreatePkCcMap(table_name,
                                            table_schema,
                                            schema_ts,
                                            ccm_has_full_entries,
                                            this));
        return ccm_it.first->second.get();
    }
}

CcMap *CcShard::CreateSkCcMap(const TableName &index_name,
                              const TableSchema *table_schema,
                              NodeGroupId ng_id,
                              uint64_t schema_ts)
{
    if (ng_id == node_id_)
    {
        auto ccm_it = native_ccms_.try_emplace(
            index_name,
            catalog_factory_->CreateSkCcMap(
                index_name, table_schema, schema_ts, this));
        return ccm_it.first->second.get();
    }
    else
    {
        auto fail_ccm_it = failover_ccms_.try_emplace(index_name).first;
        std::unordered_map<NodeGroupId, CcMap::uptr> &ccms =
            fail_ccm_it->second;
        auto ccm_it =
            ccms.try_emplace(ng_id,
                             catalog_factory_->CreateSkCcMap(
                                 index_name, table_schema, schema_ts, this));
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

/**
 * @brief Clean ccentries from ccm given a table name.
 *
 * @param table_name
 */
void CcShard::CleanCcm(const TableName &table_name)
{
    auto native_it = native_ccms_.find(table_name);
    if (native_it != native_ccms_.end())
    {
        native_it->second->Clean();
    }

    auto fail_ccm_it = failover_ccms_.find(table_name);
    if (fail_ccm_it != failover_ccms_.end())
    {
        std::unordered_map<NodeGroupId, CcMap::uptr> &ccms =
            fail_ccm_it->second;
        std::unordered_map<NodeGroupId, CcMap::uptr>::iterator it =
            ccms.begin();
        while (it != ccms.end())
        {
            it->second->Clean();
            it++;
        }
    }
}

void CcShard::DropCcms(NodeGroupId ng_id)
{
    if (node_id_ == ng_id)
    {
        for (auto ccm_it = native_ccms_.begin(); ccm_it != native_ccms_.end();)
        {
            if (ccm_it->first == catalog_ccm_name)
            {
                ccm_it->second->Clean();
                ++ccm_it;
                continue;
            }

            ccm_it = native_ccms_.erase(ccm_it);
        }
    }
    else
    {
        for (auto table_it = failover_ccms_.begin();
             table_it != failover_ccms_.end();)
        {
            std::unordered_map<NodeGroupId, CcMap::uptr> &ng_ccm =
                table_it->second;
            ng_ccm.erase(ng_id);
            if (ng_ccm.empty())
            {
                table_it = failover_ccms_.erase(table_it);
            }
            else
            {
                ++table_it;
            }
        }
    }
}

void CcShard::CreateRangeCcMap(const TableName &range_table_name,
                               NodeGroupId ng_id)
{
    if (ng_id == node_id_)
    {
        native_ccms_.try_emplace(
            range_table_name,
            catalog_factory_->CreatePkRangeMap(range_table_name, this));
    }
    else
    {
        auto fail_range_it = failover_ccms_.try_emplace(range_table_name).first;
        std::unordered_map<NodeGroupId, CcMap::uptr> &range_maps =
            fail_range_it->second;
        range_maps.try_emplace(
            ng_id, catalog_factory_->CreatePkRangeMap(range_table_name, this));
    }
}

void CcShard::DecrementMemory(size_t mem_size)
{
    if (mem_usage_ >= mem_size)
    {
        mem_usage_ -= mem_size;
    }
    else
    {
        mem_usage_ = 0;
    }
}

}  // namespace txservice
