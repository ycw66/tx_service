#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <map>
#include <mutex>
#include <unordered_map>

#include "catalog.h"
#include "cc_entry.h"
#include "cc_map.h"
#include "cc_req_base.h"
#include "moodycamelqueue.h"
#include "secondary_key.h"
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
    TxLockInfo(int64_t term, uint64_t ts)
        : term_(term), ts_(ts), last_recover_ts_(0), cce_list_()
    {
    }

    int64_t term_;
    // The timestamp when the tx acquires the first lock in the cc shard.
    uint64_t ts_;
    // The last time when the tx is recovered.
    uint64_t last_recover_ts_;
    // A list of cc entries on which the tx has acquired write/read locks.
    std::unordered_set<LruEntry *> cce_list_;
};

class CcShard
{
public:
    // Maximal number of keys a cc map can host. Once the cc map reaches the
    // cap, the cleaning logic is invoked to remove old, unused, checkpointed cc
    // entries from the cc map.
    static constexpr uint64_t capSize = 100000000;

    CcShard() = delete;
    CcShard(const CcShard &other) = delete;

    CcShard(uint16_t core_id,
            uint32_t core_cnt,
            uint64_t base_ts,
            uint32_t node_id,
            Catalog *catalog)
        : node_id_(node_id),
          core_id_(core_id),
          core_cnt_(core_cnt),
          native_ccms_(),
          failover_ccms_(),
          table_metadata_(),
          failover_table_metadata_(),
          cc_queue_(256),
          req_buf_(),
          size_(0),
          tx_vec_(),
          tx_head_(0),
          tx_cnt_(0),
          ts_base_(base_ts),
          head_cce_(nullptr),
          tail_cce_(nullptr),
          ckpter_(nullptr),
          processor_sleep_(false),
          catalog_(catalog)
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
    }

    /**
     * @brief Returns the cc map in this node given the table name and the cc
     * node group.
     *
     * @param table_name The table name.
     * @param node_group The ID of the cc node group.
     * @param error_code
     * @return CcMap* The pointer to the cc map.
     */
    CcMap *GetCcm(const TableName &table_name,
                  uint32_t node_group,
                  int8_t &error_code);

    void RemoveCcm(const TableName &table_name)
    {
        native_ccms_.erase(table_name);
        failover_ccms_.erase(table_name);
    }

    bool Full() const
    {
        return size_ >= CcShard::capSize;
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

    TEntry &NewTx(uint64_t start_ts = 0);

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
        return catalog_;
    }

    /// <summary>
    /// Removes the cc entry from the double-linked list and re-inserts it
    /// as the second-to-last entry as the most-recently accessed entry.
    /// </summary>
    /// <param name="entry"></param>
    void UpdateLruList(LruEntry *entry);

    TxLockInfo *UpsertLockHoldingTx(TxNumber txn,
                                    int64_t tx_term,
                                    LruEntry *cce_ptr);

    void DeleteLockHolidngTx(TxNumber txn, LruEntry *cce_ptr);

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
        if (lock_holding_txs_.size() == 0)
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

            return max_ts;
        }
        else
        {
            uint64_t min_ts = UINT64_MAX;
            for (const auto &tx_pair : lock_holding_txs_)
            {
                min_ts = std::min(min_ts, tx_pair.second.ts_);
            }
            return min_ts - 1;
        }
    }

    static void DetachCkpt(LruEntry *entry)
    {
        LruEntry *prev = entry->ckpt_prev_;
        LruEntry *post = entry->ckpt_next_;
        prev->ckpt_next_ = post;
        post->ckpt_prev_ = prev;
        entry->ckpt_prev_ = nullptr;
        entry->ckpt_next_ = nullptr;
    }

    bool AcquireTableReadIntention(const TableName &table_name,
                                   CcRequestBase *cc_req)
    {
        auto table_iter = table_locks_.find(table_name);

        if (table_iter == table_locks_.end())
        {
            auto iter = table_locks_.try_emplace(table_name);
            table_iter = iter.first;
        }
        TableLock &tab_lock = table_iter->second;

        return tab_lock.AcquireReadIntention(cc_req);
    }

    bool AcquireTableWriteLock(const TableName &table_name,
                               CcRequestBase *cc_req)
    {
        // TODO: refactor to use function GetTableLock to get TableLock
        // &tab_lock
        auto table_iter = table_locks_.find(table_name);

        if (table_iter == table_locks_.end())
        {
            auto iter = table_locks_.try_emplace(table_name);
            table_iter = iter.first;
        }

        TableLock &tab_lock = table_iter->second;

        return tab_lock.AcquireWrite(cc_req);
    }

    bool ReleaseTableReadIntention(const TableName &table_name,
                                   CcRequestBase *cc_req)
    {
        auto table_iter = table_locks_.find(table_name);

        if (table_iter == table_locks_.end())
        {
            auto iter = table_locks_.try_emplace(table_name);
            table_iter = iter.first;
        }

        TableLock &tab_lock = table_iter->second;

        tab_lock.ReleaseReadIntention(cc_req->Txn(), this);

        return true;
    }

    bool ReleaseTableWriteLock(const TableName &table_name,
                               CcRequestBase *cc_req)
    {
        auto table_iter = table_locks_.find(table_name);

        if (table_iter == table_locks_.end())
        {
            auto iter = table_locks_.try_emplace(table_name);
            table_iter = iter.first;
        }

        TableLock &tab_lock = table_iter->second;

        tab_lock.ReleaseWrite(cc_req->Txn(), this);

        return true;
    }

    bool ReleaseAllTableLocks(const TableName &table_name, TxNumber tx_number)
    {
        auto table_iter = table_locks_.find(table_name);

        if (table_iter == table_locks_.end())
        {
            auto iter = table_locks_.try_emplace(table_name);
            table_iter = iter.first;
        }

        TableLock &tab_lock = table_iter->second;

        tab_lock.ReleaseAllTableLocks(tx_number, this);

        return true;
    }

    bool FindCatalog(const TableName &table_name, std::string *catalog_content)
    {
        auto table_iter = table_metadata_.find(table_name);

        if (table_iter == table_metadata_.end())
        {
            auto iter = table_metadata_.try_emplace(table_name);
            table_iter = iter.first;
        }

        TableCatalog &tab_catalog = table_iter->second;

        if (tab_catalog.table_catalog_info_ == "")
        {
            return false;
        }
        else
        {
            *catalog_content = tab_catalog.table_catalog_info_;
        }

        return true;
    }

    bool CheckCatalogVersion(const TableName &table_name,
                             std::string &source_version)
    {
        auto table_iter = table_metadata_.find(table_name);

        if (table_iter == table_metadata_.end())
        {
            auto iter = table_metadata_.try_emplace(table_name);
            table_iter = iter.first;
        }

        TableCatalog &tab_catalog = table_iter->second;

        if (source_version.compare(tab_catalog.table_catalog_version_) == 0)
        {
            return true;
        }
        return false;
    }

    const uint32_t node_id_;
    const uint16_t core_id_;
    const uint16_t core_cnt_;

private:
    /**
     * @brief Detaches the input cc entry from the double linked list. The
     * operation is invoked when the cc entry is to be kicked out or is newly
     * accessed and needs to be re-positioned according to the LRU algorithm.
     *
     * @param entry The pointer to the cc entry to be detached.
     */
    static void DetachLru(LruEntry *entry);

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

    std::unordered_map<TableName, CcMap::uptr> native_ccms_;
    std::unordered_map<TableName, std::unordered_map<NodeGroupId, CcMap::uptr>>
        failover_ccms_;

    std::unordered_map<TableName, CcMap::uptr> range_func_;

    // table metadata store catalog and table lock information
    std::unordered_map<TableName, TableLock> table_locks_;
    std::unordered_map<TableName, TableCatalog> table_metadata_;

    // table_metadata_ for failover ccnode.
    // TODO: failover logic not handled yet.
    std::unordered_map<TableName, std::unordered_map<NodeGroupId, TableLock>>
        failover_table_locks_;
    std::unordered_map<TableName, std::unordered_map<NodeGroupId, TableCatalog>>
        failover_table_metadata_;

    moodycamel::ConcurrentQueue<CcRequestBase *> cc_queue_;
    CcRequestBase *req_buf_[100];
    std::vector<moodycamel::ProducerToken> thd_token_;

    /// <summary>
    /// A variable tracking the cc maps' size in memory
    /// </summary>
    uint64_t size_;

    std::vector<TEntry> tx_vec_;
    uint32_t tx_head_;
    uint32_t tx_cnt_;
    std::atomic<uint64_t> ts_base_;

    /**
     * @brief Reserved head and tail for the double-linked list of cc entries.
     * Reservation simplifies handling of empty and one-element lists.
     *
     */
    LruEntry head_cce_, tail_cce_;

    /**
     * @brief A collection of active tx's that have acquired locks/intentions in
     * this shard and the tx's information, including when the tx acquires the
     * first lock, the term of the tx node and a list of pointers to the cc
     * entries containing the tx's locks/intentions.
     *
     */
    std::unordered_map<TxNumber, TxLockInfo> lock_holding_txs_;

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

    // Catalog handlers
    Catalog *catalog_;

    // The number of cc entries to free in one invocation of Clean().
    static constexpr uint64_t freeBatchSize = 100;

    friend class LocalCcHandler;
    friend class LocalCcShards;
    friend class Checkpointer;
};
}  // namespace txservice
