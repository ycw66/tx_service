#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <iostream>
#include <map>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "catalog.h"
#include "catalog_factory.h"
#include "catalog_key_record.h"
#include "cc/non_blocking_lock.h"
#include "cc_entry.h"
#include "cc_map.h"
#include "cc_req_base.h"
#include "cc_req_misc.h"
#include "error_messages.h"
#include "fault/fault_inject.h"  // CODE_FAULT_INJECTOR
#include "meter.h"
#include "metrics.h"
#include "moodycamelqueue.h"
#include "range_bucket_key_record.h"
#include "range_record.h"
#include "range_slice.h"
#include "sharder.h"
#include "tentry.h"
#include "tx_service_common.h"

namespace txservice
{
class SingleShardScanner;
class CcMapScanner;
class TxProcessor;
class TxService;
class Checkpointer;
class LocalCcShards;
struct StatisticsEntry;
struct CheckDeadLockResult;

#define LOCK_VECTOR_SHRINK_THRESHOLD 4u
#define RESIZE_LOCK_LIMIT 3u
#define LOCK_ARRAY_INIT_SIZE 8192u

// store table catalog information in ccshard
class TableCatalog
{
public:
    TableCatalog() : table_catalog_info_(""), table_catalog_version_("")
    {
    }

    // content of table catalog, same as frm file
    std::string table_catalog_info_;
    // catalog version
    std::string table_catalog_version_;
};

struct TxLockInfo
{
    TxLockInfo() = delete;
    explicit TxLockInfo(int64_t tx_coord_term)
        : tx_coord_term_(tx_coord_term),
          wlock_ts_(0),
          last_recover_ts_(0),
          cce_list_(),
          table_type_(TableType::Primary)
    {
    }

    // tx coordinator's term.
    int64_t tx_coord_term_;
    // The timestamp when the tx acquires the first write lock in the cc shard.
    // If tx has not acquired any write lock, set wlock_ts_ to 0.
    uint64_t wlock_ts_;
    // The last time when the tx is recovered or the tx acquired the latest
    // lock.
    uint64_t last_recover_ts_;
    // A list of cc entries on which the tx has acquired write/read locks.
    std::unordered_set<LruEntry *> cce_list_;
    // This cc map type is used to skip the meta table(such as: catalog, range)
    // during get ActiveTxMinTs()
    TableType table_type_;
};

class CcShard
{
public:
    CcShard() = delete;
    CcShard(const CcShard &other) = delete;

    CcShard(uint16_t core_id,
            uint32_t core_cnt,
            uint32_t node_memory_limit_mb,
            uint32_t node_log_limit_mb,
            bool realtime_sampling,
            uint32_t node_id,
            LocalCcShards &local_shards,
            CatalogFactory *catalog_factory);
    /**
     * @brief Returns the cc map at this shard given the table name and the cc
     * node group.
     *
     * @param table_name The table name.
     * @param node_group The ID of the cc node group.
     * @return CcMap* The pointer to the cc map.
     */
    CcMap *GetCcm(const TableName &table_name, uint32_t node_group);

    bool Full() const
    {
        return mem_usage_ >= memory_limit_;
    }

    /**
     * @brief Puts a cc request into the shard's request queue to be processed.
     *
     * @param thd_id The thread ID of the producer sending the cc request.
     * Providing the thread ID helps reduce contention, as internally the
     * concurrent queue uses it to dispatch the request to an internal storage
     * allocated for the thread.
     * @param req The pointer to the cc request. The request is either owned by
     * a resource pool or a stack object whose owner thread is blocking on the
     * request.
     */
    void Enqueue(uint32_t thd_id, CcRequestBase *req);

    /**
     * @brief Puts a cc request into the shard's request queue to be processed.
     *
     * @param req The pointer to the cc request.
     */
    void Enqueue(CcRequestBase *req);

    void AbortCcRequests(std::vector<CcRequestBase *> &&reqs,
                         CcErrorCode err_code);

    bool IsIdle()
    {
        return cc_queue_size_.load(std::memory_order_relaxed) == 0;
    }

    size_t ProcessRequests()
    {
        uint32_t queue_size = cc_queue_size_.load(std::memory_order_relaxed);
        // collect metrics: memory usage
        if (metrics::enable_memory_usage)
        {
            if (memory_usage_round_ == metrics::memory_usage_sample_round)
            {
                meter_->Collect(MEMORY_USAGE_NAME_, mem_usage_);
                memory_usage_round_ = 1;
            }
            else
            {
                ++memory_usage_round_;
            }
        }

        if (queue_size == 0)
        {
            return 0;
        }

        size_t total = 0;
        size_t req_cnt = 0;
        do
        {
            req_cnt = cc_queue_.try_dequeue_bulk(req_buf_, 100);
            total += req_cnt;
            assert(cc_queue_size_.load(std::memory_order_relaxed) >= req_cnt);
            cc_queue_size_.fetch_sub(req_cnt, std::memory_order_acq_rel);

            for (size_t i = 0; i < req_cnt; ++i)
            {
                bool finish = req_buf_[i]->Execute(*this);
                if (finish)
                {
                    req_buf_[i]->Free();
                }
            }
        } while (req_cnt > 50 && total < 1000);

        return total;
    }

    /**
     * @brief Find an available TEntry in tranaction array and initialize it.
     *
     */
    TEntry &NewTx(NodeGroupId tx_ng_id, uint32_t log_group_id, int64_t term);

    /**
     * @brief Find an available NonBlockingLock in lock array and initialize it.
     *
     */
    NonBlockingLock *NewLock();

    TEntry *LocateTx(const TxId &tx_id);

    /**
     * @brief Given the tx number, returns the tx entry that describes the tx
     * status.
     *
     * @param tx_number
     * @return TEntry* The pointer to the tx entry.
     */
    TEntry *LocateTx(TxNumber tx_number);

    size_t Clean();

    bool FlushEntryForTest(LruEntry *entry,
                           std::vector<FlushRecord> &ckpt_vec,
                           std::vector<FlushRecord> &archives,
                           bool only_archives);

    void NotifyCkpt(bool request_ckpt = true);

    void SetWaitingCkpt(bool is_waiting);

    /**
     * @brief Get the number of ccentries in this ccshard
     *
     */
    size_t Size() const
    {
        return size_;
    }

    uint16_t LocalCoreId() const
    {
        return core_id_;
    }

    uint32_t GlobalCoreId(NodeGroupId ng_id) const
    {
        // The global core ID is a combination of node group ID and the local
        // core ID.
        return (ng_id << 10) | core_id_;
    }

    uint64_t Now() const;
    void UpdateTsBase(uint64_t ts);

    size_t QueueSize()
    {
        return cc_queue_size_.load(std::memory_order_relaxed);
    }

    Catalog *GetCatalog()
    {
        return nullptr;
    }

    /**
     * Insert page at the end of the lru list as the most-recently accessed
     * page.
     * @param page
     */
    void UpdateLruList(LruPage *page, bool is_emplace);

    /**
     * Detaches the page from the double linked list. This function is invoked
     * in UpdateLruList or when the cc page is to be kicked out.
     * @param page
     */
    void DetachLru(LruPage *page);

    TxLockInfo *UpsertLockHoldingTx(TxNumber txn,
                                    int64_t tx_term,
                                    LruEntry *cce_ptr,
                                    bool is_key_write_lock,
                                    NodeGroupId cc_ng_id,
                                    TableType table_type);

    void DeleteLockHoldingTx(TxNumber txn,
                             LruEntry *cce_ptr,
                             NodeGroupId cc_ng_id);

    void DropLockHoldingTxs(NodeGroupId cc_ng_id)
    {
        lock_holding_txs_.erase(cc_ng_id);
    }

    /**
     * @brief When a tx fails to acquire a lock, it invokes this method to check
     * how long the conflicting tx has been holding the lock. If the conflicting
     * tx has been holding the lock for an extended period of time, tries to
     * recover the conflicting tx.
     *
     * @param txn Tx number of the conflicting tx
     * @param cc_ng_id ID of the cc node group in which the conflict happens
     * @param cc_ng_term Leader term of the cc node group
     */
    void CheckRecoverTx(TxNumber txn, uint32_t cc_ng_id, int64_t cc_ng_term);
    void CheckRecoverTx(TxNumber txn,
                        TxLockInfo &lk_info,
                        uint32_t cc_ng_id,
                        int64_t cc_ng_term);

    void ClearTx(TxNumber txn);

    uint64_t ActiveTxMinTs(NodeGroupId cc_ng_id)
    {
        uint64_t min_ts = UINT64_MAX;

        int64_t cc_ng_term = Sharder::Instance().LeaderTerm(cc_ng_id);
        auto it = lock_holding_txs_.find(cc_ng_id);
        if (it != lock_holding_txs_.end())
        {
            for (auto &tx_pair : it->second)
            {
                // Skip meta table because there is no need to do
                // checkpoint for these type table.
                if (!TableName::IsMeta(tx_pair.second.table_type_) &&
                    tx_pair.second.wlock_ts_ != 0)
                {
                    min_ts = std::min(min_ts, tx_pair.second.wlock_ts_ - 1);
                }

                // check and recover holding write lock transactions.
                CheckRecoverTx(
                    tx_pair.first, tx_pair.second, cc_ng_id, cc_ng_term);
            }
        }

        if (min_ts == UINT64_MAX)
        {
            // When there is no active tx, since the local ts base is only
            // synced with the clock in every 2 sec, the local ts may fall a
            // little far behind. Re-synced the ts base with the clock to choose
            // an update-to-date ts for checkpoint.
            using namespace std::chrono_literals;

            uint64_t clock_ts =
                std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                    .count();

            uint64_t tsb = Now();
            uint64_t max_ts = std::max(tsb, clock_ts);
            UpdateTsBase(max_ts);

            // need to return max_ts - 1 at here, since lock_holding_txs_'s
            // timestamp is read from ts_base_ which could be the same as
            // max_ts. max_ts is possible to be assigned to last_ckpt_ts, while
            // the next ckpt_ts could be read from lock_holding_txs_. Hence it
            // would be possible to trigger assert(ckpt_ts >= last_ckpt_ts_); if
            // we return max_ts directly.
            min_ts = max_ts - 1;
        }

        TryResizeLockArray();

        return min_ts;
    }

    /**
     * Try to reduce the size of lock array if it becomes sparse.
     *
     */
    void TryResizeLockArray();

    std::pair<bool, const CatalogEntry *> CreateCatalog(
        const TableName &table_name,
        NodeGroupId cc_ng_id,
        const std::string &catalog_image,
        uint64_t commit_ts);

    CatalogEntry *CreateDirtyCatalog(const TableName &table_name,
                                     NodeGroupId cc_ng_id,
                                     const std::string &catalog_image,
                                     uint64_t commit_ts);

    std::pair<bool, const CatalogEntry *> CreateReplayCatalog(
        const TableName &table_name,
        NodeGroupId cc_ng_id,
        const std::string &old_schema_image,
        const std::string &new_schema_image,
        uint64_t old_schema_ts,
        uint64_t dirty_schema_ts);

    void CommitDirtyCatalog(const TableName &table_name, NodeGroupId cc_ng_id);

    CatalogEntry *GetCatalog(const TableName &table_name, NodeGroupId cc_ng_id);

    /**
     * @brief Initialize table_ranges_ in local_cc_shard based on the
     * InitRangeEntry. StoreRange and StoreSlice will also be initialized if the
     * range belongs to this ng.
     *
     * @param table_name
     * @param init_ranges
     * @param ng_id
     * @param fully_cached If range is already fully cached. This will affect
     * the StoreSlice status of the created range. Currently set to true on
     * table create so that we don't need to visit data store once when reading
     * a just created table.
     */
    void InitTableRanges(const TableName &table_name,
                         std::vector<InitRangeEntry> &init_ranges,
                         const NodeGroupId ng_id,
                         bool fully_cached = false);

    std::map<const TxKey *, TableRangeEntry, PtrLessThan<TxKey>>
        *GetTableRangesForATable(const TableName &range_table_name,
                                 const NodeGroupId ng_id);

    const TableRangeEntry *CreateTableRange(
        const TableName &table_name,
        const NodeGroupId ng_id,
        int32_t partition_id,
        TxKey::Uptr start_key,
        const TxKey *end_key,
        uint64_t version,
        std::vector<std::tuple<TxKey::Uptr, uint32_t, SliceStatus>>
            *slice_keys = nullptr);

    const TableRangeEntry *UploadNewRangeInfo(
        const TableName &table_name,
        const NodeGroupId ng_id,
        const TxKey *key,
        const std::vector<std::unique_ptr<TxKey>> &new_key,
        const std::vector<int32_t> &new_partition_id,
        uint64_t commit_ts);

    TableRangeEntry *GetTableRangeEntry(const TableName &table_name,
                                        const NodeGroupId ng_id,
                                        const TxKey *key);

    const TableRangeEntry *GetTableRangeEntry(const TableName &table_name,
                                              const NodeGroupId ng_id,
                                              int32_t range_id);

    const TableRangeEntry *GetTableRangeEntryNoLocking(
        const TableName &table_name, const NodeGroupId ng_id, const TxKey *key);

    uint64_t CountRanges(const TableName &table_name,
                         const NodeGroupId ng_id,
                         const NodeGroupId key_ng_id);

    uint64_t CountRangesLockless(const TableName &table_name,
                                 const NodeGroupId ng_id,
                                 const NodeGroupId key_ng_id);

    uint64_t CountSlices(const TableName &table_name,
                         const NodeGroupId ng_id,
                         const NodeGroupId local_ng_id) const;

    void CleanTableRange(const TableName &table_name, const NodeGroupId ng_id);

    std::pair<std::shared_ptr<Statistics>, bool> InitTableStatistics(
        TableSchema *table_schema, NodeGroupId ng_id);

    std::pair<std::shared_ptr<Statistics>, bool> InitTableStatistics(
        TableSchema *table_name,
        TableSchema *dirty_table_schema,
        NodeGroupId ng_id,
        std::unordered_map<TableName,
                           std::pair<uint64_t, std::vector<TxKey::Uptr>>>
            sample_pool_map);

    StatisticsEntry *GetTableStatistics(const TableName &table_name,
                                        NodeGroupId ng_id);

    const StatisticsEntry *LoadRangesAndStatisticsNx(
        const TableSchema *curr_schema,
        NodeGroupId cc_ng_id,
        int64_t cc_ng_term,
        CcRequestBase *requester);

    void CleanTableStatistics(const TableName &table_name);

    void DropBucketInfo(NodeGroupId ng_id);

    const BucketInfo *GetBucketInfo(uint16_t bucket_id,
                                    NodeGroupId ng_id) const;

    const std::unordered_map<uint16_t, std::unique_ptr<BucketInfo>>
        *GetAllBucketInfos(NodeGroupId ng_id) const;

    const BucketInfo *GetRangeOwner(int32_t range_id, NodeGroupId ng_id) const;

    /**
     * @brief Fetches the table's catalog from the data store and
     * temporarily caches the demanding cc request in the cc shard. After
     * the catalog is fetched and instantiated in this node, re-enqueues the
     * cc request for re-execution.
     *
     * @param table_name The table name
     * @param requester The cc request that needs to access the input
     * table's cc map but the cc map does not exist due to the missing of
     * the catalog.
     */
    void FetchCatalog(const TableName &table_name,
                      NodeGroupId cc_ng_id,
                      int64_t cc_ng_term,
                      CcRequestBase *requester);

    void FetchTableStatistics(const TableName &table_name,
                              NodeGroupId cc_ng_id,
                              int64_t cc_ng_term,
                              CcRequestBase *requester);

    void FetchTableRanges(const TableName &range_table_name,
                          CcRequestBase *requester,
                          NodeGroupId cc_ng_id,
                          int64_t cc_ng_term);

    void RemoveFetchRequest(const TableName &table_name);

    CcMap *CreateOrUpdatePkCcMap(const TableName &table_name,
                                 const TableSchema *table_schema,
                                 NodeGroupId ng_id,
                                 uint64_t schema_ts,
                                 bool is_create = true,
                                 bool ccm_has_full_entries = false);

    CcMap *CreateOrUpdateSkCcMap(const TableName &index_name,
                                 const TableSchema *table_schema,
                                 NodeGroupId ng_id,
                                 uint64_t schema_ts,
                                 bool is_create = true);

    /**
     * @brief Initializes the request's target cc map, if the table
     * schema is available and indicates that the table exists. Sends an async
     * request to fetch the schema from the data store, if the schema is not
     * cached locally.
     *
     * @return const TableSchemaView* The pointer to the schema view of the
     * request's target cc map. Null, if the schema is not cached at the node
     * level.
     */
    const CatalogEntry *InitCcm(const TableName &table_name,
                                NodeGroupId cc_ng_id,
                                int64_t cc_ng_term,
                                CcRequestBase *requester);

    void DropCcm(const TableName &table_name, NodeGroupId ng_id);

    void CleanCcm(const TableName &table_name);

    /**
     * @brief Drops all cc maps associated with a cc node group. The method is
     * called when this node steps down as the leader of the specified cc node
     * group.
     *
     * @param ng_id The cc node group whose leader has been transferred to
     * another node.
     */
    void DropCcms(NodeGroupId ng_id);

    void CreateOrUpdateRangeCcMap(const TableName &range_table_name,
                                  const TableSchema *table_schema,
                                  NodeGroupId ng_id,
                                  uint64_t schema_ts,
                                  bool is_create = true);

    void DecrementMemory(size_t mem_size);

    void DecreaseLockCount();

    RangeSliceId PinRangeSlice(const TableName &table_name,
                               NodeGroupId cc_ng_id,
                               int64_t cc_ng_term,
                               const Schema *key_schema,
                               const Schema *rec_schema,
                               uint64_t schema_ts,
                               const KVCatalogInfo *kv_info,
                               const TxKey &key,
                               bool inclusive,
                               CcRequestBase *cc_request,
                               RangeSliceOpStatus &pin_status,
                               bool force_load,
                               uint8_t prefetch_size);

    RangeSliceId PinRangeSlice(const TableName &table_name,
                               NodeGroupId cc_ng_id,
                               int64_t cc_ng_term,
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
                               uint8_t prefetch_size);

    /**
     * Used for unit test to verify the lru link is complete.
     */
    void VerifyLruList();

    const uint32_t node_id_;
    const uint16_t core_id_;
    const uint16_t core_cnt_;
    LocalCcShards &local_shards_;

    // Memory usage of this CcShard.
    size_t mem_usage_{0};

    bool EnableMvcc() const;
    void AddActiveSiTx(TxNumber txn, uint64_t start_ts);
    void RemoveActiveSiTx(TxNumber txn);
    void ClearActvieSiTxs();
    // Scan {active_si_txs_} to update {min_si_tx_start_ts_}
    void UpdateLocalMinSiTxStartTs();
    uint64_t LocalMinSiTxStartTs();
    uint64_t GlobalMinSiTxStartTs() const;

    // shard level memory limit.
    uint64_t memory_limit_{0};
    // shard level log limit. Note that RocksDB engine based log service
    // supports persist log state machine to disk. Hence log_limit is a soft
    // limit.
    uint64_t log_limit_{0};

    const bool realtime_sampling_{true};

    // Search lock_holding_txs_, find the entrys with waited transactions and
    // save them into CheckDeadLockResult.
    void CollectLockWaitingInfo(CheckDeadLockResult &dlr);
    std::unordered_map<NodeGroupId, std::unordered_map<TxNumber, TxLockInfo>>
        &GetLockHoldingTxs()
    {
        return lock_holding_txs_;
    }

    void ResetCleanStart()
    {
        clean_start_ccp_ = nullptr;
    }

    bool OutOfMemory()
    {
        return clean_start_ccp_ != nullptr && clean_start_ccp_ == &tail_ccp_;
    }

private:
    void SetTxProcNotifier(std::atomic<TxProcessorStatus> *tx_proc_status,
                           TxProcCoordinator *tx_coordi)
    {
        tx_proc_status_ = tx_proc_status;
        tx_coordi_ = tx_coordi;
    }

    size_t memory_usage_round_ = 1;

    /**
     * @brief A collection of active tx's that have acquired locks/intentions in
     * this shard and the tx's information, including when the tx acquires the
     * latest write lock, the term of the tx node and a list of pointers to the
     * cc entries containing the tx's locks/intentions.
     *
     */
    std::unordered_map<NodeGroupId, std::unordered_map<TxNumber, TxLockInfo>>
        lock_holding_txs_;

    // below are all string owners
    std::unordered_map<TableName, CcMap::uptr> native_ccms_;
    std::unordered_map<TableName, std::unordered_map<NodeGroupId, CcMap::uptr>>
        failover_ccms_;

    std::unordered_map<TableName, std::unique_ptr<FetchCc>> fetch_reqs_;

    // CcRequest queue on this shard/core.
    moodycamel::ConcurrentQueue<CcRequestBase *> cc_queue_;
    std::atomic<uint32_t> cc_queue_size_{0};
    CcRequestBase *req_buf_[100];
    std::vector<moodycamel::ProducerToken> thd_token_;

    // all the transactions started on this ccshard. Some txs are Ongoing while
    // others are Available, new transaction request has to traverse the array
    // and find an available one.
    std::vector<TEntry> tx_vec_;
    // pointer to the next slot in tx array.
    uint32_t next_tx_idx_;

    // all the lock acquire/release on this ccshard. It used to reduce the cost
    // of allocation/dellocation of memory.
    std::vector<NonBlockingLock::Uptr> lock_vec_;
    // pointer to the next slot in lock array.
    uint32_t next_lock_idx_;
    uint32_t used_lock_count_;

    // tx identifier inside a CPU core. It's a uint32 value and will become 0
    // after wraparound. Global tx_number is 64 bits: higher 32 bits are
    // global_core_id, while lower 32 bits are tx_ident.
    uint32_t next_tx_ident_;

    // Reserved head and tail for the double-linked list of cc entries, which
    // simplifies handling of empty and one-element lists.
    LruPage head_ccp_, tail_ccp_;

    // Page to start looking for cc entries to kick out on LRU chain.
    LruPage *clean_start_ccp_;

    // The number of ccentry in all the ccmap of this ccshard.
    uint64_t size_;

    Checkpointer *ckpter_;

    /**
     * @brief The variable via which the dedicated processing thread notifies
     * the shard that it enters into the sleep mode.
     *
     */
    std::atomic<TxProcessorStatus> *tx_proc_status_{nullptr};
    TxProcCoordinator *tx_coordi_{nullptr};

    // Catalog handler which is used to execute catalog related callback
    // function at runtime side.
    CatalogFactory *const catalog_factory_;

    // The number of cc entries to free in one invocation of Clean().
    static constexpr uint64_t freeBatchSize = 100;

    // cache all tx info under SI isolation level in this shard,
    // format: {txn->start_ts}
    std::unordered_map<TxNumber, uint64_t> active_si_txs_;
    // min start_ts of tx in "active_si_txs_"
    std::atomic<uint64_t> min_si_tx_start_ts_{1U};
    // last timestamp of updating "min_si_tx_start_ts_"
    uint64_t last_scan_txs_ts_{0U};
    // track the lock sparse number and reduce lock array size if threshold
    // reached.
    uint8_t lock_sparse_num_{0};

    friend class LocalCcHandler;
    friend class LocalCcShards;
    friend class Checkpointer;

public:
    std::unique_ptr<metrics::Meter> meter_;
    const metrics::Name MEMORY_LIMIT_NAME_{"memory_limit"};
    const metrics::Name CACHE_HIT_OR_MISS_TOTAL_NAME_{
        "cache_hit_or_miss_total"};
    const metrics::Name MEMORY_USAGE_NAME_{"memory_usage"};
};
}  // namespace txservice
