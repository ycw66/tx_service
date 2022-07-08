#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <iostream>
#include <shared_mutex>
#include <unordered_map>

#include "catalog.h"
#include "catalog_factory.h"
#include "catalog_key_record.h"
#include "cc_shard.h"
#include "local_cc_handler.h"
#include "raft_log.pb.h"
#include "sk_cc_map.h"
#include "store/data_store_handler.h"
#include "table_lock.h"
#include "template_cc_map.h"

namespace txservice
{
namespace remote
{
class RemoteCcHandler;
};
class Checkpointer;
class TxService;

class LocalCcShards
{
public:
    LocalCcShards(uint32_t node_id = 0,
                  uint16_t core_cnt = 1,
                  CatalogFactory *catalog_factory = nullptr,
                  store::DataStoreHandler *store_hd = nullptr,
                  TxService *tx_service = nullptr);

    ~LocalCcShards();

    LocalCcShards(LocalCcShards const &) = delete;
    void operator=(LocalCcShards const &) = delete;

    void EnqueueCcRequest(uint32_t thd_id,
                          uint32_t shard_code,
                          CcRequestBase *req)
    {
        uint32_t residual = shard_code & 0x3FF;
        size_t core_idx = residual % cc_shards_.size();

        cc_shards_[core_idx]->Enqueue(thd_id, req);
    }

    void EnqueueCcRequest(uint32_t shard_code, CcRequestBase *req)
    {
        size_t core_idx = (shard_code & 0x3FF) % cc_shards_.size();
        cc_shards_.at(core_idx)->Enqueue(req);
    }

    size_t ProcessRequests(size_t thd_id)
    {
        return cc_shards_[thd_id]->ProcessRequests();
    }

    bool IsIdle(uint32_t thd_id) const
    {
        return cc_shards_[thd_id]->IsIdle();
    }

    void SleepNotify(uint32_t thd_id)
    {
        cc_shards_[thd_id]->SleepNotify();
    }

    void WorkNotify(uint32_t thd_id)
    {
        cc_shards_[thd_id]->WorkNotify();
    }

    size_t Count() const
    {
        return cc_shards_.size();
    }

    template <typename KeyT, typename ValueT>
    void CreateCcTable(const TableName &tabname,
                       const Schema *key_schema = nullptr,
                       const Schema *rec_schema = nullptr,
                       uint32_t core_id = 0,
                       bool is_all = true)
    {
        for (uint32_t id = 0; id < cc_shards_.size(); id++)
        {
            if (is_all || id == core_id)
            {
                cc_shards_[id]->native_ccms_.try_emplace(
                    tabname,
                    std::make_unique<TemplateCcMap<KeyT, ValueT>>(
                        cc_shards_[id].get(), key_schema, rec_schema));
            }
        }
    }

    void DropCcTable(const TableName &tabname, uint32_t core_id)
    {
        cc_shards_[core_id]->native_ccms_.erase(tabname);
    }

    template <typename SkT, typename PkT>
    void CreateSkCcTable(const TableName &tabname,
                         const Schema *sk_schema = nullptr,
                         const Schema *pk_schema = nullptr,
                         uint32_t core_id = 0,
                         bool is_all = true)
    {
        for (uint32_t id = 0; id < cc_shards_.size(); id++)
        {
            if (is_all || id == core_id)
            {
                cc_shards_[id]->native_ccms_.try_emplace(
                    tabname,
                    std::make_unique<SkCcMap<SkT, PkT>>(
                        cc_shards_[id].get(), sk_schema, pk_schema));
            }
        }
    }

    /**
     * @brief Clean the ccentries from the ccmap in each ccshards.
     * Note that this function is not thread safe and should only be used by
     * test case.
     *
     * @param tabname
     */
    void CleanCcTable(const TableName &tabname)
    {
        for (uint32_t i = 0; i < cc_shards_.size(); i++)
        {
            cc_shards_[i]->CleanCcm(tabname);
        }
    }

    void NotifyCheckPointer()
    {
        for (uint32_t i = 0; i < cc_shards_.size(); i++)
        {
            cc_shards_[i]->NotifyCkpt();
        }
    }

    void PrintCcMap()
    {
        std::unordered_map<TableName, size_t> mapsizes;
        for (const auto &cc_shard : cc_shards_)
        {
            CcShard &shard = *cc_shard;

            size_t entry_cnt = 0;
            for (auto map_iter = shard.native_ccms_.begin();
                 map_iter != shard.native_ccms_.end();
                 ++map_iter)
            {
                const TableName &tab_name = map_iter->first;
                auto find_iter = mapsizes.find(tab_name);
                size_t cnt = 0;
                if (find_iter != mapsizes.end())
                {
                    cnt = find_iter->second;
                    cnt += map_iter->second->size();
                    find_iter->second = cnt;
                }
                else
                {
                    mapsizes.emplace(tab_name, map_iter->second->size());
                }

                // Excludes negative and positive infinity.
                entry_cnt += map_iter->second->size();

                std::cout << "Table '" << tab_name << "' core ID "
                          << shard.core_id_ << ": " << map_iter->second->size()
                          << std::endl;

                assert(map_iter->second->VerifyOrdering() ==
                       map_iter->second->size());
            }

            size_t list_cnt = 0;
            LruEntry *eptr = shard.head_cce_.lru_next_;
            while (eptr != &shard.tail_cce_)
            {
                ++list_cnt;
                eptr = eptr->lru_next_;
            }
            assert(entry_cnt == list_cnt);
        }

        /*for (auto map_iter = mapsizes.begin(); map_iter != mapsizes.end();
             ++map_iter)
        {
            std::cout << map_iter->first << ": " << map_iter->second
                      << std::endl;
        }*/
    }

    uint32_t NodeId() const
    {
        return node_id_;
    }

    std::condition_variable &ShardCv(uint32_t core_id)
    {
        return cc_shards_.at(core_id)->shard_cv_;
    }

    std::mutex &ShardMutex(uint32_t core_id)
    {
        return cc_shards_.at(core_id)->shard_mux_;
    }

    static uint64_t ClockTs();

    const CatalogEntry *CreateCatalog(const std::string &table_name,
                                      NodeGroupId cc_ng_id,
                                      const std::string &catalog_image,
                                      uint64_t commit_ts);

    const CatalogEntry *CreateDirtyCatalog(const std::string &table_name,
                                           NodeGroupId cc_ng_id,
                                           const std::string &catalog_image,
                                           uint64_t commit_ts);

    void CommitDirtyCatalog(const std::string &table_name,
                            NodeGroupId cc_ng_id);

    const CatalogEntry *GetCatalog(const std::string &table_name,
                                   NodeGroupId cc_ng_id);

    std::unordered_set<TableName> CatalogTableNames();

    void CreateSchemaRecoveryTx(const ::txlog::SchemaOpMessage &schema_op_msg,
                                uint64_t txn,
                                int64_t tx_term,
                                uint64_t commit_ts);
    void InitTableRanges(const TableName &range_table_name,
                         std::vector<InitRangeEntry> &init_ranges);

    const std::map<uint32_t, TableRangeEntry> *GetTableRanges(
        const TableName &range_table_name);

    const TableRangeEntry *CreateDirtyRange(const TableName &table_name,
                                            uint32_t partition_id,
                                            std::unique_ptr<TxKey> new_key,
                                            uint32_t new_partition_id,
                                            uint64_t commit_ts);

    void SetTxIdent(uint32_t latest_committed_txn_no);

    /**
     * @brief Drops all tables' catalogs associated with the specified cc node
     * group. The function is called when this node steps down from the leader
     * of the specified cc node group.
     *
     * @param cc_ng_id The cc node group whose leader has transferred to another
     * node.
     */
    void DropCatalogs(NodeGroupId cc_ng_id);

    uint64_t MinStartTsOfLocalActiveTxs(int64_t term)
    {
        uint64_t min_ts = UINT64_MAX;
        for (auto &ccs : cc_shards_)
        {
            if (ccs->min_tx_start_ts_term_.load() == term)
            {
                min_ts = std::min(ccs->min_tx_start_ts_.load(), min_ts);
            }
            else
            {
                min_ts = std::min(ccs->ts_base_.load(), min_ts);
            }
        }
        return min_ts;
    }

    store::DataStoreHandler *const store_hd_;

private:
    void TimerRun();

    const uint32_t node_id_;
    std::vector<std::unique_ptr<CcShard>> cc_shards_;

    // The background thread that periodically advances the timers of the local
    // shards to the current wall clock.
    std::thread timer_thd_;
    std::atomic<bool> timer_terminate_;
    // The static variable storing the local time. It is delayed time and
    // refreshed in roughly every 2 seconds by the background thread, so as to
    // reduce the cost of calling system functions to get the wall clock. The
    // local time is used by transaction state machines to determine if a lock
    // has been held too long and if so, invoke lock recovery.
    static std::atomic<uint64_t> local_clock;

    CatalogFactory *const catalog_factory_;
    std::unordered_map<TableName, std::unordered_map<NodeGroupId, CatalogEntry>>
        table_catalogs_;
    std::unordered_map<TableName, std::map<uint32_t, TableRangeEntry>>
        table_ranges_;
    std::shared_mutex catalog_mux_;

    TxService *tx_service_;

    friend class LocalCcHandler;
    friend class remote::RemoteCcHandler;
    friend class Checkpointer;
};
}  // namespace txservice
