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

#include "catalog.h"
#include "catalog_factory.h"
#include "catalog_key_record.h"
#include "cc_entry.h"
#include "cc_map.h"
#include "cc_req_base.h"
#include "cc_req_misc.h"
#include "fault/fault_inject.h"  // CODE_FAULT_INJECTOR
#include "moodycamelqueue.h"
#include "range_record.h"
#include "secondary_key.h"
#include "sharder.h"
#include "table_lock.h"
#include "tentry.h"

namespace txservice
{
class SingleShardScanner;
class CcMapScanner;
class Checkpointer;
class LocalCcShards;

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
    TxLockInfo(int64_t tx_coord_term, uint64_t ts)
        : tx_coord_term_(tx_coord_term),
          ts_(ts),
          last_recover_ts_(0),
          cce_list_(),
          key_write_lock_count_(0)
    {
    }

    bool HasWriteLock() const
    {
        return (key_write_lock_count_ > 0);
    }

    // tx coordinator's term.
    int64_t tx_coord_term_;
    // The timestamp when the tx acquires the first lock in the cc shard.
    uint64_t ts_;
    // The last time when the tx is recovered.
    uint64_t last_recover_ts_;
    // A list of cc entries on which the tx has acquired write/read locks.
    std::unordered_set<LruEntry *> cce_list_;
    // How many write locks in this tx for current shard
    int32_t key_write_lock_count_;
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
            uint64_t base_ts,
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
        for (size_t i = 0; i < req_cnt; ++i)
        {
            bool finish = req_buf_[i]->Execute(*this);
            if (finish)
            {
                req_buf_[i]->Free();
            }
        }

        return req_cnt;
    }

    /**
     * @brief Find an available TEntry in tranaction array and initialize it.
     *
     */
    TEntry &NewTx();

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

    bool FlushEntry(LruEntry *entry, bool only_archives = true);

    void NotifyCkpt();

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

    uint64_t Now() const
    {
        return ts_base_.load(std::memory_order_relaxed);
    }

    size_t QueueSize() const
    {
        return cc_queue_.size_approx();
    }

    Catalog *GetCatalog()
    {
        return nullptr;
    }

    /// <summary>
    /// Removes the cc entry from the double-linked list and re-inserts it
    /// as the second-to-last entry as the most-recently accessed entry.
    /// </summary>
    /// <param name="entry"></param>
    void UpdateLruList(LruEntry *entry);

    /**
     * @brief Detaches the input cc entry from the double linked list. The
     * operation is invoked when the cc entry is to be kicked out or is newly
     * accessed and needs to be re-positioned according to the LRU algorithm.
     *
     * @param entry The pointer to the cc entry to be detached.
     */
    static void DetachLru(LruEntry *entry);

    // Detach the cc_entry from ckpt list.
    void DetachCkpt(LruEntry *entry);

    // Update estimate log size generated by the cc_entry.
    void UpdateEstimateLogSize(LruEntry *entry,
                               size_t key_size,
                               size_t payload_size);

    TxLockInfo *UpsertLockHoldingTx(TxNumber txn,
                                    int64_t tx_term,
                                    LruEntry *cce_ptr,
                                    bool is_key_write_lock);

    void DeleteLockHoldingTx(TxNumber txn,
                             LruEntry *cce_ptr,
                             bool is_key_write_lock);

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

    void ClearTx(TxNumber txn);

    uint64_t ActiveTxMinTs()
    {
        uint64_t min_ts = UINT64_MAX;
        for (const auto &tx_pair : lock_holding_txs_)
        {
            if (tx_pair.second.HasWriteLock())
            {
                min_ts = std::min(min_ts, tx_pair.second.ts_ - 1);
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

            uint64_t tsb = ts_base_.load(std::memory_order_acquire);
            uint64_t max_ts = std::max(tsb, clock_ts);
            ts_base_.compare_exchange_strong(tsb, max_ts);

            // need to return max_ts - 1 at here, since lock_holding_txs_'s
            // timestamp is read from ts_base_ which could be the same as
            // max_ts. max_ts is possible to be assigned to last_ckpt_ts, while
            // the next ckpt_ts could be read from lock_holding_txs_. Hence it
            // would be possible to trigger assert(ckpt_ts >= last_ckpt_ts_); if
            // we return max_ts directly.
            min_ts = max_ts - 1;
        }

        if (lock_holding_txs_.size() > 0)
        {
            std::unordered_set<uint32_t> set;
            set.insert(node_id_);
            for (auto iter = failover_ccms_.begin();
                 iter != failover_ccms_.end();
                 iter++)
            {
                for (auto it = iter->second.begin(); it != iter->second.end();
                     it++)
                {
                    set.insert(it->first);
                }
            }

            for (uint32_t ng_id : set)
            {
                int64_t ng_term = Sharder::Instance().LeaderTerm(ng_id);
                for (auto iter = lock_holding_txs_.begin();
                     iter != lock_holding_txs_.end();
                     iter++)
                {
                    CheckRecoverTx(iter->first, ng_id, ng_term);
                }
            }
        }

        return min_ts;
    }

    // Statistic the min start ts of active transactions on this shard and
    // cache to "min_tx_start_ts_".
    uint64_t StatMinTxStartTs();

    uint64_t GlobalMinTxStartTs();

    const CatalogEntry *CreateCatalog(const TableName &table_name,
                                      NodeGroupId cc_ng_id,
                                      const std::string &catalog_image,
                                      uint64_t commit_ts);

    const CatalogEntry *CreateDirtyCatalog(const TableName &table_name,
                                           NodeGroupId cc_ng_id,
                                           const std::string &catalog_image,
                                           uint64_t commit_ts);

    void CommitDirtyCatalog(const TableName &table_name, NodeGroupId cc_ng_id);

    const CatalogEntry *GetCatalog(const TableName &table_name,
                                   NodeGroupId cc_ng_id);

    void InitTableRanges(const TableName &table_name,
                         std::vector<InitRangeEntry> &init_ranges);

    std::map<int32_t, TableRangeEntryWithShade> *GetAllTableRangesForATable(
        const TableName &range_table_name);

    const TableRangeEntryWithShade *CreateDirtyTableRange(
        const TableName &table_name,
        int32_t partition_id,
        std::unique_ptr<TxKey> new_key,
        int32_t new_partition_id,
        uint64_t commit_ts);

    const std::pair<TableRangeEntry *, TableRangeEntry *> CommitDirtyTableRange(
        const TableName &table_name, int32_t partition_id, uint64_t commit_ts);

    const TableRangeEntry *GetTableEffectiveRange(const TableName &table_name,
                                                  int32_t partition_id);

    const TableRangeEntryWithShade *GetTableRangeWithShade(
        const TableName &table_name, int32_t partition_id);

    void PostCommitDirtyTableRange(const TableName &table_name,
                                   int32_t partition_id);

    void CleanTableRange(const TableName &table_name, uint32_t ng_id);

    /**
     * @brief Fetches the table's catalog from the data store and temporarily
     * caches the demanding cc request in the cc shard. After the catalog is
     * fetched and instantiated in this node, re-enqueues the cc request for
     * re-execution.
     *
     * @param table_name The table name
     * @param requester The cc request that needs to access the input table's cc
     * map but the cc map does not exist due to the missing of the catalog.
     */
    void FetchCatalog(const TableName &table_name,
                      NodeGroupId cc_ng_id,
                      CcRequestBase *requester);

    void FetchTableRanges(const TableName &range_table_name,
                          const Schema *key_schema,
                          CcRequestBase *requester);

    void RemoveFetchRequest(const TableName &table_name);

    CcMap *CreatePkCcMap(const TableName &table_name,
                         const TableSchema *table_schema,
                         NodeGroupId ng_id,
                         uint64_t schema_ts,
                         bool ccm_has_full_entries = false);

    CcMap *CreateSkCcMap(const TableName &index_name,
                         const TableSchema *table_schema,
                         NodeGroupId ng_id,
                         uint64_t schema_ts);

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

    void CreateRangeCcMap(const TableName &range_table_name,
                          const TableSchema *table_schema,
                          NodeGroupId ng_id,
                          uint64_t schema_ts);

    void DecrementMemory(size_t mem_size);

    const uint32_t node_id_;
    const uint16_t core_id_;
    const uint16_t core_cnt_;
    LocalCcShards &local_shards_;

    // Memory usage of this CcShard.
    size_t mem_usage_{0};
    // Estimate size: Key + Value
    size_t estimate_ccshard_log_size_{0};

    // cache min{start_ts of all tx in this shard, ts_base} last calculated
    std::atomic<uint64_t> min_tx_start_ts_{0U};
    std::atomic<int64_t> min_tx_start_ts_term_{-1};

    // shard level memory limit.
    uint64_t memory_limit_{0};
    // shard level log limit. Note that RocksDB engine based log service
    // supports persist log state machine to disk. Hence log_limit is a soft
    // limit.
    uint64_t log_limit_{0};

private:
    std::unordered_map<TableName, CcMap::uptr> native_ccms_;
    std::unordered_map<TableName, std::unordered_map<NodeGroupId, CcMap::uptr>>
        failover_ccms_;

    std::unordered_map<TableName, std::unique_ptr<FetchCc>> fetch_reqs_;

    std::unordered_map<TableName, CcMap::uptr> native_range_maps_;
    std::unordered_map<TableName, std::unordered_map<NodeGroupId, CcMap::uptr>>
        failover_range_maps_;

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
    // tx identifier inside a CPU core. It's a uint32 value and will become 0
    // after wraparound. Global tx_number is 64 bits: higher 32 bits are
    // global_core_id, while lower 32 bits are tx_ident.
    uint32_t next_tx_ident_;
    // the base timestamp of ccshard which will be adjust by local clock and
    // commit timestamp of transaction on this ccshard to keep it up to date.
    std::atomic<uint64_t> ts_base_;

    /**
     * @brief Reserved head and tail for the double-linked list of cc entries.
     * Reservation simplifies handling of empty and one-element lists.
     *
     */
    CcEntry<VoidKey, VoidRecord> head_cce_, tail_cce_;

    // the number of ccentry in all the ccmap of this ccshard.
    uint64_t size_;

    /**
     * @brief A collection of active tx's that have acquired locks/intentions in
     * this shard and the tx's information, including when the tx acquires the
     * first lock, the term of the tx node and a list of pointers to the cc
     * entries containing the tx's locks/intentions.
     *
     */
    std::unordered_map<TxNumber, TxLockInfo> lock_holding_txs_;

    Checkpointer *ckpter_;

    // Catalog handler which is used to execute catalog related callback
    // function at runtime side.
    CatalogFactory *const catalog_factory_;

    // The number of cc entries to free in one invocation of Clean().
    static constexpr uint64_t freeBatchSize = 100;

    friend class LocalCcHandler;
    friend class LocalCcShards;
    friend class Checkpointer;
};
}  // namespace txservice
