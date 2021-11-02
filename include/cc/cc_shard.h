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
#include "sharder.h"
#include "table_lock.h"
#include "tentry.h"

namespace txservice
{
class SingleShardScanner;
class CcMapScanner;
class Checkpointer;

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
    TxLockInfo(uint64_t ts) : ts_(ts), last_recover_ts_(0), cce_list_()
    {
    }

    // The timestamp when the tx acquires the first write intention in the
    // cc shard.
    uint64_t ts_;
    // The time when last tx tries to recover the intention/lock.
    uint64_t last_recover_ts_;
    // A list of cc entries on which the tx has put the write intention.
    std::vector<LruEntry *> cce_list_;
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

    CcMap *GetCcm(const TableName &table_name,
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

    void RemoveCcm(const TableName &table_name)
    {
        native_ccms_.erase(table_name);
        failover_ccms_.erase(table_name);
    }

    bool Full() const
    {
        return size_ >= CcShard::capSize;
    }

    void Enqueue(uint32_t thd_id, CcRequestBase *req)
    {
        bool is_empty = cc_queue_.is_empty();

        assert(thd_id < thd_token_.size());
        bool ret = cc_queue_.enqueue(thd_token_.at(thd_id), req);
        assert(ret == true);

        if (is_empty)
        {
            shard_cv_.notify_one();
        }
    }

    void Enqueue(CcRequestBase *req)
    {
        bool is_empty = cc_queue_.is_empty();

        bool ret = cc_queue_.enqueue(req);
        assert(ret == true);

        if (is_empty)
        {
            shard_cv_.notify_one();
        }
    }

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
                                    uint64_t ts,
                                    LruEntry *cce_ptr)
    {
        auto em_it = lock_holding_txs_.try_emplace(txn, ts);
        em_it.first->second.cce_list_.emplace_back(cce_ptr);
        return &em_it.first->second;
    }

    void DeleteLockHolidngTx(TxNumber txn)
    {
        lock_holding_txs_.erase(txn);
    }

    TxLockInfo *GetActiveTxLockInfo(uint64_t txn)
    {
        auto tx_it = lock_holding_txs_.find(txn);
        if (tx_it == lock_holding_txs_.end())
        {
            return nullptr;
        }
        else
        {
            return &tx_it->second;
        }
    }

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

        tab_lock.ReleaseReadIntention(cc_req->Tx(), this);

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

        tab_lock.ReleaseWrite(cc_req->Tx(), this);

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
    /// <summary>
    /// Detaches the input cc entry from the double linked list.
    /// </summary>
    /// <param name="entry"></param>
    static void DetachLru(LruEntry *entry);

    std::unordered_map<TableName, CcMap::uptr> native_ccms_;
    std::unordered_map<TableName, std::unordered_map<NodeGroupId, CcMap::uptr>>
        failover_ccms_;

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

    /// <summary>
    /// Reserved head and tail for the double-linked list simplify handling
    /// of empty and one-element lists.
    /// </summary>
    LruEntry head_cce_, tail_cce_;

    /// <summary>
    /// A collection of active tx's that have acquired write intentions in this
    /// cc shard and lock/intention information associated with the tx,
    /// including when the tx acquires the first intention, the term of the tx
    /// node and a list of pointers to the cc entries containing the tx's
    /// intentions.
    /// </summary>
    std::unordered_map<TxNumber, TxLockInfo> lock_holding_txs_;

    Checkpointer *ckpter_;

    std::condition_variable shard_cv_;
    std::mutex shard_mux_;

    // Catalog handlers
    Catalog *catalog_;

    // The number of cc entries to free in one invocation of Clean().
    static constexpr uint64_t freeBatchSize = 100;

    friend class LocalCcHandler;
    friend class LocalCcShards;
    friend class Checkpointer;
};
}  // namespace txservice
