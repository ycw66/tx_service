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
#include "fault/fault_inject.h"  // CODE_FAULT_INJECTOR
#include "meter.h"
#include "metrics.h"
#include "moodycamelqueue.h"
#include "range_record.h"
#include "range_slice.h"
#include "sharder.h"
#include "tentry.h"

namespace txservice
{
class SingleShardScanner;
class CcMapScanner;
class TxProcessor;
class TxService;
class Checkpointer;
class LocalCcShards;
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
          cce_list_()
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

    bool IsIdle() const
    {
        return cc_queue_.is_empty();
    }

    size_t ProcessRequests()
    {
        size_t req_cnt = cc_queue_.try_dequeue_bulk(req_buf_, 100);

        // collect metric: cc queue length
        if (metrics::enable_busy_loop_metrics)
        {
            if (busy_loop_round_ == metrics::busy_loop_sample_round)
            {
                auto len = req_cnt < 100 ? req_cnt : cc_queue_.size_approx();
                meter_->Collect("cc_queue_length", len);
                busy_loop_round_ = 1;
            }
            else
            {
                ++busy_loop_round_;
            }
        }

        // collect metrics: memory usage
        if (metrics::enable_memory_usage)
        {
            if (memory_usage_round_ == metrics::memory_usage_sample_round)
            {
                meter_->Collect("memory_usage", mem_usage_);
                memory_usage_round_ = 1;
            }
            else
            {
                ++memory_usage_round_;
            }
        }
        for (size_t i = 0; i < req_cnt; ++i)
        {
            bool finish = req_buf_[i]->Execute(*this);
            if (finish)
            {
                req_buf_[i]->Free();
            }
        }
        return req_cnt;
    };
    /**
     * @brief Find an available TEntry in tranaction array and initialize it.
     *
     */
    TEntry &NewTx();

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

    void NotifyCkpt();

    /**
     * @brief Checkpoint a table until ckpt_ts. This will notify checkpointer
     * to start a new thread and do the checkpoint.
     */
    void FlushData(const TableName &table_name,
                   const TableSchema *schema,
                   uint64_t ckpt_ts,
                   int64_t term,
                   uint64_t node_group,
                   std::vector<FlushRecord> *ckpt_vec,
                   std::vector<FlushRecord> *archive_vec,
                   std::vector<const TxKey *> *mv_vec,
                   CcHandlerResult<Void> *hres);

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

    uint32_t GlobalCoreId() const
    {
        // The global core ID is a combination of node ID and the local core ID.
        uint32_t global_id = node_id_;
        return (global_id << 10) | core_id_;
    }

    uint64_t Now() const;
    void UpdateTsBase(uint64_t ts);

    size_t QueueSize() const
    {
        return cc_queue_.size_approx();
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
    void UpdateLruList(LruPage *page);

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
                                    NodeGroupId cc_ng_id);

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
                if (tx_pair.second.wlock_ts_ != 0)
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
        const std::string &statistics_binary,
        uint64_t commit_ts);

    CatalogEntry *CreateDirtyCatalog(const TableName &table_name,
                                     NodeGroupId cc_ng_id,
                                     const std::string &catalog_image,
                                     const std::string &statistics_binary,
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

    void CleanTableRange(const TableName &table_name, const NodeGroupId ng_id);

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
                      CcRequestBase *requester);

    void FetchTableRanges(const TableName &range_table_name,
                          const KVCatalogInfo *kv_info,
                          CcRequestBase *requester,
                          NodeGroupId ng_id);

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
                               const NodeGroupId ng_id,
                               const Schema *key_schema,
                               const Schema *rec_schema,
                               uint64_t schema_ts,
                               const KVCatalogInfo *kv_info,
                               const TxKey &key,
                               bool inclusive,
                               CcRequestBase *cc_request,
                               RangeSliceOpStatus &pin_status,
                               bool force_load = false);

    RangeSliceId PinRangeSlice(const TableName &table_name,
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
                               bool force_load = false);

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

    // Search lock_holding_txs_, find the entrys with waited transactions and
    // save them into CheckDeadLockResult.
    void CollectLockWaitingInfo(CheckDeadLockResult &dlr);
    std::unordered_map<NodeGroupId, std::unordered_map<TxNumber, TxLockInfo>>
        &GetLockHoldingTxs()
    {
        return lock_holding_txs_;
    }

    std::unique_ptr<metrics::Meter> meter_;

    void ResetCleanStart()
    {
        clean_start_ccp_ = nullptr;
    }

    bool OutOfMemory()
    {
        return clean_start_ccp_ != nullptr && clean_start_ccp_ == &tail_ccp_;
    }

private:
    /**
     * @brief The method invoked by the processing thread to notify the cc shard
     * that it enters into the sleep mode.
     *
     */
    void SleepNotify()
    {
        processor_sleep_.store(true, std::memory_order_release);
    }

    /**
     * @brief The method invoked by the processing thread to notify the cc shard
     * that it wakes up from the sleep mode and is working.
     *
     */
    void WorkNotify()
    {
        processor_sleep_.store(false, std::memory_order_release);
    }

    size_t busy_loop_round_ = 1;
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
     * @brief The condition variable via which the cc shard wakes up the
     * processing thread dedicated to it from the sleep mode.
     *
     */
    std::condition_variable shard_cv_;
    std::mutex shard_mux_;

    /**
     * @brief The variable via which the dedicated processing thread notifies
     * the shard that it enters into the sleep mode.
     *
     */
    std::atomic<bool> processor_sleep_;

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
};
}  // namespace txservice
