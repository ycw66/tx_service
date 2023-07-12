#include "cc/cc_shard.h"

#include <chrono>  // std::chrono

#include "cc/catalog_cc_map.h"
#include "cc/cc_request.h"
#include "cc/ccm_scanner.h"
#include "cc/non_blocking_lock.h"  // lock_vec_
#include "checkpointer.h"
#include "sharder.h"  // Sharder
#include "tx_start_ts_collector.h"

namespace txservice
{
CcShard::CcShard(uint16_t core_id,
                 uint32_t core_cnt,
                 uint32_t node_memory_limit_mb,
                 uint32_t node_log_limit_mb,
                 bool realtime_sampling,
                 uint32_t node_id,
                 LocalCcShards &local_shards,
                 CatalogFactory *catalog_factory)
    : node_id_(node_id),
      core_id_(core_id),
      core_cnt_(core_cnt),
      local_shards_(local_shards),
      realtime_sampling_(realtime_sampling),
      native_ccms_(),
      failover_ccms_(),
      cc_queue_(256),
      req_buf_(),
      tx_vec_(),
      next_tx_idx_(0),
      lock_vec_(),
      next_lock_idx_(0),
      used_lock_count_(0),
      next_tx_ident_(0),
      head_ccp_(nullptr),
      tail_ccp_(nullptr),
      clean_start_ccp_(nullptr),
      size_(0),
      ckpter_(nullptr),
      processor_sleep_(nullptr),
      processor_mux_(nullptr),
      catalog_factory_(catalog_factory),
      active_si_txs_(),
      meter_(std::make_unique<metrics::Meter>(local_shards.metrics_registry_,
                                              std::to_string(core_id)))

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

    lock_vec_.reserve(LOCK_ARRAY_INIT_SIZE);
    for (uint32_t idx = 0; idx < LOCK_ARRAY_INIT_SIZE; ++idx)
    {
        lock_vec_.emplace_back(std::make_unique<NonBlockingLock>());
    }

    head_ccp_.lru_prev_ = nullptr;
    head_ccp_.lru_next_ = &tail_ccp_;
    tail_ccp_.lru_prev_ = &head_ccp_;
    tail_ccp_.lru_next_ = nullptr;

    thd_token_.reserve((size_t) core_cnt + 1);
    for (size_t idx = 0; idx < core_cnt; ++idx)
    {
        thd_token_.emplace_back(moodycamel::ProducerToken(cc_queue_));
    }

    native_ccms_.try_emplace(
        catalog_ccm_name,
        std::make_unique<CatalogCcMap>(this, node_id_, catalog_ccm_name));

    if (metrics::enable_collect_metrics)
    {
        meter_->Register(MEMORY_LIMIT_NAME_, metrics::Type::Gauge);
    }

    if (metrics::enable_cache_hit_rate)
    {
        meter_->Register(CACHE_HIT_OR_MISS_TOTAL_NAME_,
                         metrics::Type::Counter,
                         {{"type", {"hits", "miss"}}});
    }

    if (metrics::enable_memory_usage)
    {
        meter_->Register(MEMORY_USAGE_NAME_, metrics::Type::Gauge);
    }

    // collect metrics: memory limit
    if (metrics::enable_collect_metrics)
    {
        meter_->Collect(MEMORY_LIMIT_NAME_, memory_limit_);
    }
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
            auto catalog_it = ng_ccm.find(node_group);
            if (catalog_it != ng_ccm.end())
            {
                return catalog_it->second.get();
            }
            else
            {
                auto insert_it =
                    ng_ccm.emplace(node_group,
                                   std::make_unique<CatalogCcMap>(
                                       this, node_group, catalog_ccm_name));
                return insert_it.first->second.get();
            }
        }
        else
        {
            return nullptr;
        }
    }
}

void CcShard::Enqueue(uint32_t thd_id, CcRequestBase *req)
{
    // The memory order in enqueue() of the concurrent queue ensures that the
    // queue size update is visible first.
    cc_queue_size_.fetch_add(1, std::memory_order_relaxed);

    assert(thd_id < thd_token_.size());
    bool ret = cc_queue_.enqueue(thd_token_.at(thd_id), req);
    assert(ret == true);

    // Wakes up the thread dedicated to this shard, when it is in the sleep
    // mode.
    if (processor_sleep_->load(std::memory_order_relaxed))
    {
        // Condition variable's notify() does not rely on std::mutex. We use it
        // here for a special purpose. C++ standard on std::atomic does not
        // enforce the StoreLoad fence. As a result, processor_sleep_->load()
        // may precede enqueue() and queue size update. And the tx processor may
        // miss the queue size change and the notify() signal. With std::mutex,
        // the memory order (std::memory_order_release) in std::mutex ensures
        // that after entering the critical section, the prior queue size update
        // is visible to the tx processor thread. The tx processor either
        // captures the notify() signal, or enters the critical section later,
        // sees the queue size update and continues without sleeping.
        std::unique_lock<std::mutex> lk(*processor_mux_);
        processor_cv_->notify_one();
    }
}

void CcShard::Enqueue(CcRequestBase *req)
{
    cc_queue_size_.fetch_add(1, std::memory_order_relaxed);

    bool ret = cc_queue_.enqueue(req);
    assert(ret == true);

    // Wakes up the thread dedicated to this shard, when it is in the sleep
    // mode.
    if (processor_sleep_->load(std::memory_order_relaxed))
    {
        std::unique_lock<std::mutex> lk(*processor_mux_);
        processor_cv_->notify_one();
    }
}

TEntry &CcShard::NewTx()
{
    // allocate start timestamp.
    uint64_t start_ts = Now();
    int64_t term = Sharder::Instance().LeaderTerm(node_id_);

    // Cicurlar iteration to find an available transaction entry.
    size_t cnt = 0;
    while (cnt < tx_vec_.size())
    {
        TEntry &te = tx_vec_[next_tx_idx_];
        if (te.status_ == TxnStatus::Finished ||
            te.status_ == TxnStatus::Committed ||
            te.status_ == TxnStatus::Aborted ||
            te.status_ == TxnStatus::Unknown)
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

NonBlockingLock *CcShard::NewLock()
{
    // Cicurlar iteration to find an available lock.
    size_t cnt = 0;
    while (cnt < lock_vec_.size())
    {
        NonBlockingLock *lentry = lock_vec_[next_lock_idx_].get();

        if (lentry->GetUsedStatus() == false)
        {
            break;
        }
        ++next_lock_idx_;
        if (next_lock_idx_ >= lock_vec_.size())
        {
            next_lock_idx_ = 0;
        }

        ++cnt;
    }

    if (cnt == lock_vec_.size())
    {
        uint32_t old_size = (uint32_t) lock_vec_.size();
        // Increases the capacity of the lock vector.
        uint32_t new_size = (uint32_t) (lock_vec_.size() * 2);
        lock_vec_.reserve(new_size);

        DLOG(INFO) << "the size of lock array increased to: " << new_size;

        for (uint32_t idx = old_size; idx < new_size; ++idx)
        {
            lock_vec_.emplace_back(std::make_unique<NonBlockingLock>());
        }

        // position old_size must be an available slot.
        next_lock_idx_ = old_size;
    }

    NonBlockingLock *lentry = lock_vec_.at(next_lock_idx_).get();
    lentry->Reset();
    lentry->SetUsedStatus(true);
    used_lock_count_++;
    ++next_lock_idx_;
    next_lock_idx_ = next_lock_idx_ == lock_vec_.size() ? 0 : next_lock_idx_;
    return lentry;
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

void CcShard::DetachLru(LruPage *page)
{
    LruPage *prev = page->lru_prev_;
    LruPage *next = page->lru_next_;
    // If page is the head to start looking for cc entry to kickout, move
    // the clean head to the next page
    if (clean_start_ccp_ == page)
    {
        clean_start_ccp_ = page->lru_next_;
    }
    assert(prev != nullptr && next != nullptr);
    prev->lru_next_ = next;
    next->lru_prev_ = prev;
    page->lru_prev_ = nullptr;
    page->lru_next_ = nullptr;
}

void CcShard::UpdateLruList(LruPage *page)
{
    // page already at the tail, do nothing
    if (page->lru_next_ == &tail_ccp_ && tail_ccp_.lru_prev_ == page)
    {
        return;
    }
    // Removes the page from the list, if it's already in the list. This is
    // used to keep the updated page at the end(tail) of the LRU list. A
    // page's prev and post are both not-null when the page is in the
    // list. This is because we have a reserved head and tail for the list.
    if (page->lru_next_ != nullptr)
    {
        DetachLru(page);
    }
    LruPage *second_tail = tail_ccp_.lru_prev_;
    second_tail->lru_next_ = page;
    tail_ccp_.lru_prev_ = page;
    page->lru_next_ = &tail_ccp_;
    page->lru_prev_ = second_tail;
}

TxLockInfo *CcShard::UpsertLockHoldingTx(TxNumber txn,
                                         int64_t tx_term,
                                         LruEntry *cce_ptr,
                                         bool is_key_write_lock,
                                         NodeGroupId cc_ng_id)
{
    auto ng_em_it = lock_holding_txs_.try_emplace(cc_ng_id);
    auto tx_em_it = ng_em_it.first->second.try_emplace(txn, tx_term);
    tx_em_it.first->second.cce_list_.emplace(cce_ptr);
    tx_em_it.first->second.last_recover_ts_ = Now();

    if (is_key_write_lock)
    {
        // write lock should update ts if the txn exists, or the CkptTsCc
        // request may get an older ckpt_ts.
        tx_em_it.first->second.wlock_ts_ = Now();
    }
    return &tx_em_it.first->second;
}

void CcShard::DeleteLockHoldingTx(TxNumber txn,
                                  LruEntry *cce_ptr,
                                  NodeGroupId cc_ng_id)
{
    auto ng_it = lock_holding_txs_.find(cc_ng_id);
    if (ng_it == lock_holding_txs_.end())
    {
        return;
    }

    auto tx_it = ng_it->second.find(txn);
    if (tx_it == ng_it->second.end())
    {
        return;
    }
    TxLockInfo &lk_info = tx_it->second;
    lk_info.cce_list_.erase(cce_ptr);

    if (lk_info.cce_list_.empty())
    {
        ng_it->second.erase(tx_it);
    }
}

void CcShard::CheckRecoverTx(TxNumber lock_holding_txn,
                             uint32_t cc_ng_id,
                             int64_t cc_ng_term)
{
    auto ng_it = lock_holding_txs_.find(cc_ng_id);
    if (ng_it == lock_holding_txs_.end())
    {
        return;
    }
    auto tx_it = ng_it->second.find(lock_holding_txn);
    if (tx_it == ng_it->second.end())
    {
        return;
    }

    CheckRecoverTx(lock_holding_txn, tx_it->second, cc_ng_id, cc_ng_term);
}

void CcShard::CheckRecoverTx(TxNumber lock_holding_txn,
                             TxLockInfo &lk_info,
                             uint32_t cc_ng_id,
                             int64_t cc_ng_term)
{
    constexpr uint64_t ts_gap =
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::seconds(5))
            .count();

    uint64_t now_ts = Now();

    // If the tx has been holding a lock/intention for an extended period of
    // time (more than 5 seconds), inquires the tx's status. If the tx has
    // failed or committed, recovers the orphan lock/intention. Or, does
    // nothing and waits for the tx to make further actions.
    if (now_ts - lk_info.last_recover_ts_ >= ts_gap)
    {
        uint32_t txn_node_group = lock_holding_txn >> 42L;

        CODE_FAULT_INJECTOR("recover_local_txn", { txn_node_group = 12345; })

        if (txn_node_group == Sharder::Instance().NodeId())
        {
            LOG(WARNING)
                << "orphan lock detected, lock holding txn: "
                << lock_holding_txn
                << ", txn is initiated by this machine, no need to recover.";

            for (const auto &lru : lk_info.cce_list_)
            {
                LOG(INFO) << "table: " << lru->parent_map_->table_name_.Trace();
            }
            // no need to check and recover local txn, it must be ongoing
            return;
        }
        LOG(WARNING) << "orphan lock detected, lock holding txn: "
                     << lock_holding_txn << ", try to recover";
        Sharder::Instance().RecoverTx(
            lock_holding_txn, lk_info.tx_coord_term_, cc_ng_id, cc_ng_term);

        // Updates the last_recover_ts field, so that following
        // conflicting tx's will not try recovery immediately,
        // avoiding a flood of recovery requests.
        lk_info.last_recover_ts_ = now_ts;
    }
}

void CcShard::ClearTx(TxNumber txn)
{
    for (auto &ng_pair : lock_holding_txs_)
    {
        auto tx_it = ng_pair.second.find(txn);
        if (tx_it == ng_pair.second.end())
        {
            continue;
        }

        TxLockInfo &lk_info = tx_it->second;
        for (auto &lru_ptr : lk_info.cce_list_)
        {
            if (lru_ptr->key_lock_ptr_ != nullptr)
            {
                lru_ptr->key_lock_ptr_->ClearTx(txn, this);
                lru_ptr->RecycleKeyLock();
            }
        }
        ng_pair.second.erase(tx_it);
    }
}

void CcShard::VerifyLruList()
{
    LruPage *pre = &head_ccp_;
    for (LruPage *cur = head_ccp_.lru_next_; cur != nullptr;
         cur = cur->lru_next_)
    {
        assert(pre->lru_next_ == cur && cur->lru_prev_ == pre);
        pre = cur;
    }
    assert(pre == &tail_ccp_);
}

/**
 * @brief Kick out freeable entries from ccmap.
 *
 * @return the number of freed entries in ccmap.
 */
size_t CcShard::Clean()
{
    LruPage *ccp = clean_start_ccp_ ? clean_start_ccp_ : head_ccp_.lru_next_;
    size_t free_cnt = 0;

    while ((Full() || free_cnt < CcShard::freeBatchSize) && ccp != &tail_ccp_)
    {
        // merge and removal might happen during Clean so ccp and ccp->lru_next_
        // might change
        auto [freed, next] = ccp->parent_map_->CleanPageAndReBalance(ccp);
        free_cnt += freed;
        ccp = next;
    }
    clean_start_ccp_ = ccp;

    // notify the checkpointer thread to do checkpoint if there is not freeable
    // entries to be kicked out from ccmap.
    if (free_cnt == 0 && !local_shards_.IsWaitingCkpt())
    {
        local_shards_.SetWaitingCkpt(true);
        NotifyCkpt();
    }

    return free_cnt;
}

/**
 * @brief Flush Entry to KvStore. Now, only used for test.
 *
 */
bool CcShard::FlushEntryForTest(LruEntry *entry,
                                std::vector<FlushRecord> &ckpt_vec,
                                std::vector<FlushRecord> &archives,
                                bool only_archives)
{
    // TODO(lzx): Now, only flush archives synchronously for test.
    if (only_archives)
    {
        return ckpter_->FlushArchiveForTest(entry, archives);
    }
    else
    {
        return (ckpter_->CkptEntryForTest(entry, ckpt_vec)) &&
               (ckpter_->FlushArchiveForTest(entry, archives));
    }
}

void CcShard::NotifyCkpt()
{
    if (ckpter_ != nullptr)
    {
        ckpter_->Notify();
    }
}

std::pair<bool, const CatalogEntry *> CcShard::CreateCatalog(
    const TableName &table_name,
    NodeGroupId cc_ng_id,
    const std::string &catalog_image,
    uint64_t commit_ts)
{
    return local_shards_.CreateCatalog(
        table_name, cc_ng_id, catalog_image, commit_ts);
}

std::pair<bool, const CatalogEntry *> CcShard::CreateReplayCatalog(
    const TableName &table_name,
    NodeGroupId cc_ng_id,
    const std::string &old_schema_image,
    const std::string &new_schema_image,
    uint64_t old_schema_ts,
    uint64_t dirty_schema_ts)
{
    return local_shards_.CreateReplayCatalog(table_name,
                                             cc_ng_id,
                                             old_schema_image,
                                             new_schema_image,
                                             old_schema_ts,
                                             dirty_schema_ts);
}

CatalogEntry *CcShard::CreateDirtyCatalog(const TableName &table_name,
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

CatalogEntry *CcShard::GetCatalog(const TableName &table_name,
                                  NodeGroupId cc_ng_id)
{
    return local_shards_.GetCatalog(table_name, cc_ng_id);
}

void CcShard::InitTableRanges(const TableName &range_table_name,
                              std::vector<InitRangeEntry> &init_ranges,
                              NodeGroupId ng_id,
                              bool fully_cached)
{
    local_shards_.InitTableRanges(
        range_table_name, init_ranges, ng_id, fully_cached);
}

std::map<const TxKey *, TableRangeEntry, PtrLessThan<TxKey>>
    *CcShard::GetTableRangesForATable(const TableName &range_table_name,
                                      const NodeGroupId ng_id)
{
    return local_shards_.GetTableRangesForATable(range_table_name, ng_id);
}

void CcShard::FetchCatalog(const TableName &table_name,
                           NodeGroupId cc_ng_id,
                           CcRequestBase *requester)
{
    FetchCatalogCc *fetch_req = nullptr;
    auto tab_it = fetch_reqs_.find(table_name);
    if (tab_it != fetch_reqs_.end())
    {
        fetch_req = static_cast<FetchCatalogCc *>(tab_it->second.get());
    }
    else
    {
        std::unique_ptr<FetchCatalogCc> fetch_catalog_cc =
            std::make_unique<FetchCatalogCc>(table_name, *this, cc_ng_id);
        fetch_req = fetch_catalog_cc.get();
        fetch_reqs_.emplace(table_name, std::move(fetch_catalog_cc));
    }

    fetch_req->AddRequester(requester);
    if (fetch_req->RequesterCount() == 1)
    {
        local_shards_.store_hd_->FetchTableCatalog(table_name, fetch_req);
    }
}

void CcShard::FetchTableStatistics(const TableName &table_name,
                                   NodeGroupId cc_ng_id,
                                   CcRequestBase *requester)
{
    FetchTableStatisticsCc *fetch_req = nullptr;
    auto tab_it = fetch_reqs_.find(table_name);
    if (tab_it != fetch_reqs_.end())
    {
        fetch_req = static_cast<FetchTableStatisticsCc *>(tab_it->second.get());
    }
    else
    {
        std::unique_ptr<FetchTableStatisticsCc> fetch_statistics_cc =
            std::make_unique<FetchTableStatisticsCc>(
                table_name, *this, cc_ng_id);
        fetch_req = fetch_statistics_cc.get();
        fetch_reqs_.emplace(table_name, std::move(fetch_statistics_cc));
    }

    fetch_req->AddRequester(requester);
    if (fetch_req->RequesterCount() == 1)
    {
        local_shards_.store_hd_->FetchCurrentTableStatistics(table_name,
                                                             fetch_req);
    }
}

void CcShard::FetchTableRanges(const TableName &table_name,
                               const KVCatalogInfo *kv_info,
                               CcRequestBase *requester,
                               NodeGroupId ng_id)
{
    FetchTableRangesCc *fetch_req = nullptr;
    auto table_it = fetch_reqs_.find(table_name);
    if (table_it != fetch_reqs_.end())
    {
        fetch_req = static_cast<FetchTableRangesCc *>(table_it->second.get());
    }
    else
    {
        auto insert_it = fetch_reqs_.try_emplace(table_name, nullptr);

        std::unique_ptr<FetchTableRangesCc> fetch_range_cc =
            std::make_unique<FetchTableRangesCc>(
                insert_it.first->first, *this, ng_id);
        fetch_req = fetch_range_cc.get();

        insert_it.first->second = std::move(fetch_range_cc);
    }

    fetch_req->AddRequester(requester);
    if (fetch_req->RequesterCount() == 1)
    {
        local_shards_.store_hd_->FetchTableRanges(kv_info, fetch_req);
    }
}

const TableRangeEntry *CcShard::UploadNewRangeInfo(
    const TableName &table_name,
    const NodeGroupId ng_id,
    const TxKey *key,
    const std::vector<std::unique_ptr<TxKey>> &new_key,
    const std::vector<int32_t> &new_partition_id,
    uint64_t commit_ts)
{
    return local_shards_.UploadNewRangeInfo(
        table_name, ng_id, key, new_key, new_partition_id, commit_ts);
}

const TableRangeEntry *CcShard::CreateTableRange(
    const TableName &table_name,
    const NodeGroupId ng_id,
    int32_t partition_id,
    TxKey::Uptr start_key,
    const TxKey *end_key,
    uint64_t version,
    std::vector<std::tuple<TxKey::Uptr, uint32_t, SliceStatus>> *slice_keys)
{
    return local_shards_.CreateTableRange(table_name,
                                          ng_id,
                                          partition_id,
                                          std::move(start_key),
                                          end_key,
                                          version,
                                          slice_keys);
}

void CcShard::CleanTableRange(const TableName &table_name,
                              const NodeGroupId ng_id)
{
    local_shards_.CleanTableRange(table_name, ng_id);
}

TableRangeEntry *CcShard::GetTableRangeEntry(const TableName &table_name,
                                             const NodeGroupId ng_id,
                                             const TxKey *key)
{
    return local_shards_.GetTableRangeEntry(table_name, ng_id, key);
}

const TableRangeEntry *CcShard::GetTableRangeEntry(const TableName &table_name,
                                                   const NodeGroupId ng_id,
                                                   int32_t range_id)
{
    return local_shards_.GetTableRangeEntry(table_name, ng_id, range_id);
}

const TableRangeEntry *CcShard::GetTableRangeEntryNonLocking(
    const TableName &table_name, const NodeGroupId ng_id, const TxKey *key)
{
    return local_shards_.GetTableRangeEntryNonLocking(table_name, ng_id, key);
}

uint64_t CcShard::CountRanges(const TableName &table_name,
                              const NodeGroupId ng_id,
                              const NodeGroupId key_ng_id)
{
    return local_shards_.CountRanges(table_name, ng_id, key_ng_id);
}

uint64_t CcShard::CountRangesLockless(const TableName &table_name,
                                      const NodeGroupId ng_id,
                                      const NodeGroupId key_ng_id)
{
    return local_shards_.CountRangesLockless(table_name, ng_id, key_ng_id);
}

uint64_t CcShard::CountSlices(const TableName &table_name,
                              const NodeGroupId ng_id,
                              const NodeGroupId local_ng_id) const
{
    return local_shards_.CountSlices(table_name, ng_id, local_ng_id);
}

std::pair<Statistics *, bool> CcShard::InitTableStatistics(
    const TableName &table_name, NodeGroupId ng_id)
{
    return local_shards_.InitTableStatistics(table_name, ng_id);
}

std::pair<Statistics *, bool> CcShard::InitTableStatistics(
    const TableName &table_name,
    const TableSchema *table_schema,
    NodeGroupId ng_id,
    std::unordered_map<TableName, std::pair<uint64_t, std::vector<TxKey::Uptr>>>
        &&sample_pool_map)
{
    return local_shards_.InitTableStatistics(
        table_name, table_schema, ng_id, std::move(sample_pool_map), this);
}

StatisticsEntry *CcShard::GetTableStatistics(const TableName &table_name,
                                             NodeGroupId ng_id)
{
    return local_shards_.GetTableStatistics(table_name, ng_id);
}

void CcShard::CleanTableStatistics(const TableName &table_name)
{
    return local_shards_.CleanTableStatistics(table_name);
}

void CcShard::RemoveFetchRequest(const TableName &table_name)
{
    fetch_reqs_.erase(table_name);
}

CcMap *CcShard::CreateOrUpdatePkCcMap(const TableName &table_name,
                                      const TableSchema *table_schema,
                                      NodeGroupId ng_id,
                                      uint64_t schema_ts,
                                      bool is_create,
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
                                            this,
                                            ng_id));
        // update table schema for alter table command.
        if (!is_create)
        {
            assert(!ccm_it.second);
            CcMap *ccm = ccm_it.first->second.get();
            ccm->SetTableSchema(table_schema);
            ccm->SetSchemaTs(schema_ts);
        }
        assert(ccm_it.first->first.IsStringOwner());
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
                                            this,
                                            ng_id));
        // update table schema for alter table command.
        if (!is_create)
        {
            assert(!ccm_it.second);
            CcMap *ccm = ccm_it.first->second.get();
            ccm->SetTableSchema(table_schema);
            ccm->SetSchemaTs(schema_ts);
        }
        return ccm_it.first->second.get();
    }
}

CcMap *CcShard::CreateOrUpdateSkCcMap(const TableName &index_name,
                                      const TableSchema *table_schema,
                                      NodeGroupId ng_id,
                                      uint64_t schema_ts,
                                      bool is_create)
{
    if (ng_id == node_id_)
    {
        auto ccm_it = native_ccms_.try_emplace(
            index_name,
            catalog_factory_->CreateSkCcMap(
                index_name, table_schema, schema_ts, this, ng_id));
        // update table schema for current sk cc map
        if (!is_create)
        {
            assert(!ccm_it.second);
            CcMap *ccm = ccm_it.first->second.get();
            ccm->SetTableSchema(table_schema);
            ccm->SetSchemaTs(schema_ts);
        }
        return ccm_it.first->second.get();
    }
    else
    {
        auto fail_ccm_it = failover_ccms_.try_emplace(index_name).first;
        std::unordered_map<NodeGroupId, CcMap::uptr> &ccms =
            fail_ccm_it->second;
        auto ccm_it = ccms.try_emplace(
            ng_id,
            catalog_factory_->CreateSkCcMap(
                index_name, table_schema, schema_ts, this, ng_id));
        // update table schema for current sk cc map
        if (!is_create)
        {
            assert(!ccm_it.second);
            CcMap *ccm = ccm_it.first->second.get();
            ccm->SetTableSchema(table_schema);
            ccm->SetSchemaTs(schema_ts);
        }
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

void CcShard::CreateOrUpdateRangeCcMap(const TableName &table_name,
                                       const TableSchema *table_schema,
                                       NodeGroupId ng_id,
                                       uint64_t schema_ts,
                                       bool is_create)
{
    TableName range_table_name(table_name.StringView(),
                               TableType::RangePartition);
    if (ng_id == node_id_)
    {
        auto ccm_it = native_ccms_.try_emplace(
            range_table_name,
            catalog_factory_->CreateRangeMap(
                range_table_name, table_schema, schema_ts, this, ng_id));
        // update table schema for current range cc map
        if (!is_create)
        {
            assert(!ccm_it.second);
            CcMap *ccm = ccm_it.first->second.get();
            ccm->SetTableSchema(table_schema);
            ccm->SetSchemaTs(schema_ts);
        }
    }
    else
    {
        auto fail_range_it = failover_ccms_.try_emplace(range_table_name).first;
        std::unordered_map<NodeGroupId, CcMap::uptr> &range_maps =
            fail_range_it->second;
        auto ccm_it = range_maps.try_emplace(
            ng_id,
            catalog_factory_->CreateRangeMap(
                range_table_name, table_schema, schema_ts, this, ng_id));
        // update table schema for current range cc map
        if (!is_create)
        {
            assert(!ccm_it.second);
            CcMap *ccm = ccm_it.first->second.get();
            ccm->SetTableSchema(table_schema);
            ccm->SetSchemaTs(schema_ts);
        }
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

bool CcShard::EnableMvcc() const
{
    return local_shards_.EnableMvcc();
}

void CcShard::AddActiveSiTx(TxNumber txn, uint64_t start_ts)
{
    active_si_txs_.try_emplace(txn, start_ts);

    if (active_si_txs_.size() == 1)
    {
        min_si_tx_start_ts_.store(start_ts);
        last_scan_txs_ts_ = Now();
        return;
    }

    UpdateLocalMinSiTxStartTs();
}

void CcShard::RemoveActiveSiTx(TxNumber txn)
{
    active_si_txs_.erase(txn);

    UpdateLocalMinSiTxStartTs();
}

void CcShard::ClearActvieSiTxs()
{
    active_si_txs_.clear();
    min_si_tx_start_ts_.store(Now());
}

void CcShard::UpdateLocalMinSiTxStartTs()
{
    uint64_t now_ts = Now();
    if (active_si_txs_.size() == 0)
    {
        min_si_tx_start_ts_.store(now_ts);
        last_scan_txs_ts_ = now_ts;
        return;
    }

    // Scan "active_si_txs_" to update "min_si_tx_start_ts_".
    if (now_ts - last_scan_txs_ts_ < 5000000)
    {
        return;
    }

    uint64_t min_ts = UINT64_MAX;
    for (auto it = active_si_txs_.begin(); it != active_si_txs_.end(); it++)
    {
        uint64_t start_ts = it->second;
        if (start_ts < min_ts)
        {
            min_ts = start_ts;
        }
    }

    min_si_tx_start_ts_.store(min_ts);
    last_scan_txs_ts_ = now_ts;
}

uint64_t CcShard::LocalMinSiTxStartTs()
{
    if (active_si_txs_.size() > 0)
    {
        return min_si_tx_start_ts_.load();
    }
    else
    {
        return Now();
    }
}

uint64_t CcShard::GlobalMinSiTxStartTs() const
{
    return TxStartTsCollector::Instance().GlobalMinSiTxStartTs();
}

void CcShard::DecreaseLockCount()
{
    used_lock_count_--;
}

void CcShard::TryResizeLockArray()
{
    // shrink the lock vector when it becomes sparse.
    if (lock_vec_.size() > LOCK_ARRAY_INIT_SIZE &&
        used_lock_count_ < (lock_vec_.size() >> LOCK_VECTOR_SHRINK_THRESHOLD))
    {
        // shrink lock array size if and only if the lock used count is low
        // for a period of time.
        if (lock_sparse_num_ < RESIZE_LOCK_LIMIT)
        {
            // mark lock array is sparse, but not to shrink the array now.
            lock_sparse_num_++;
            return;
        }
        lock_sparse_num_ = 0;

        uint32_t high_idx = 0;
        uint32_t low_idx = 0;

        // shrink the capacity of the lock vector.
        uint32_t old_size = (uint32_t) lock_vec_.size();
        uint32_t new_size =
            (uint32_t) (old_size >> (LOCK_VECTOR_SHRINK_THRESHOLD - 1u));

        // move the used slot whose position is larger than new_size to the
        // front of lock array.
        for (high_idx = new_size; high_idx < old_size; high_idx++)
        {
            if (lock_vec_[high_idx]->GetUsedStatus())
            {
                // find an unused slot
                while (lock_vec_[low_idx]->GetUsedStatus())
                {
                    low_idx++;
                }
                lock_vec_[low_idx++] = std::move(lock_vec_[high_idx]);
            }
        }
        lock_vec_.resize(new_size);
        lock_vec_.shrink_to_fit();
        next_lock_idx_ = next_lock_idx_ >= new_size ? 0 : next_lock_idx_;

        DLOG(INFO) << "the size of lock array decreased to: " << new_size;
    }
}

uint64_t CcShard::Now() const
{
    return local_shards_.TsBase();
}

void CcShard::UpdateTsBase(uint64_t ts)
{
    local_shards_.UpdateTsBase(ts);
}

RangeSliceId CcShard::PinRangeSlice(const TableName &table_name,
                                    const NodeGroupId ng_id,
                                    const Schema *key_schema,
                                    const Schema *rec_schema,
                                    uint64_t schema_ts,
                                    const KVCatalogInfo *kv_info,
                                    const TxKey &key,
                                    bool inclusive,
                                    CcRequestBase *cc_request,
                                    RangeSliceOpStatus &pin_status,
                                    bool force_load,
                                    uint8_t prefetch_size)
{
    return local_shards_.PinRangeSlice(table_name,
                                       ng_id,
                                       key_schema,
                                       rec_schema,
                                       schema_ts,
                                       kv_info,
                                       key,
                                       inclusive,
                                       cc_request,
                                       this,
                                       pin_status,
                                       force_load,
                                       prefetch_size);
}

RangeSliceId CcShard::PinRangeSlice(const TableName &table_name,
                                    const NodeGroupId ng_id,
                                    const Schema *key_schema,
                                    const Schema *rec_schema,
                                    uint64_t schema_ts,
                                    const KVCatalogInfo *kv_info,
                                    uint32_t range_id,
                                    const TxKey &key,
                                    bool inclusive,
                                    CcRequestBase *cc_request,
                                    RangeSliceOpStatus &pin_status,
                                    bool force_load,
                                    uint8_t prefetch_size)
{
    return local_shards_.PinRangeSlice(table_name,
                                       ng_id,
                                       key_schema,
                                       rec_schema,
                                       schema_ts,
                                       kv_info,
                                       range_id,
                                       key,
                                       inclusive,
                                       cc_request,
                                       this,
                                       pin_status,
                                       force_load,
                                       prefetch_size);
}

void CcShard::CollectLockWaitingInfo(CheckDeadLockResult &dlr)
{
    std::unordered_map<uint64_t, CheckDeadLockResult::EntryLockInfo>
        &entry_lock_info_map = dlr.entry_lock_info_vec_[core_id_];

    for (auto it_ng = lock_holding_txs_.begin();
         it_ng != lock_holding_txs_.end();
         it_ng++)
    {
        for (auto iter = it_ng->second.begin(); iter != it_ng->second.end();
             iter++)
        {
            dlr.txid_ety_lock_count_[core_id_].insert(
                {iter->first, iter->second.cce_list_.size()});
            for (auto itset = iter->second.cce_list_.begin();
                 itset != iter->second.cce_list_.end();
                 itset++)
            {
                std::vector<uint64_t> vct =
                    (*itset)->GetKeyLock().GetBlockTxIds(iter->first);
                if (vct.size() == 0)
                    continue;

                auto itet =
                    entry_lock_info_map.try_emplace((uint64_t) (*itset));
                itet.first->second.lock_txids.insert(iter->first);

                for (uint64_t id : vct)
                {
                    itet.first->second.wait_txids.insert(id);
                }
            }
        }
    }
}
}  // namespace txservice
