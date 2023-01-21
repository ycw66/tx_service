#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <iostream>
#include <map>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "catalog.h"
#include "catalog_factory.h"
#include "catalog_key_record.h"
#include "cc_shard.h"
#include "local_cc_handler.h"
#include "metrics/metrics.h"
#include "raft_log.pb.h"
#include "range_slice.h"
#include "store/data_store_handler.h"
#include "type.h"

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
                  uint32_t memory_limit_mb = 1000,
                  uint32_t log_limit_mb = 1000,
                  CatalogFactory *catalog_factory = nullptr,
                  store::DataStoreHandler *store_hd = nullptr,
                  metrics::MetricsRegistry *metrics_registry = nullptr,
                  TxService *tx_service = nullptr,
                  bool enable_mvcc = true);

    LocalCcShards(uint32_t node_id = 0,
                  uint16_t core_cnt = 1,
                  uint32_t memory_limit_mb = 1000,
                  uint32_t log_limit_mb = 1000,
                  CatalogFactory *catalog_factory = nullptr,
                  store::DataStoreHandler *store_hd = nullptr,
                  TxService *tx_service = nullptr,
                  bool enable_mvcc = true);

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
        std::unordered_map<TableName, size_t>
            mapsizes;  // not string owner, sv -> native_ccms_
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
                    mapsizes.emplace(
                        std::piecewise_construct,
                        std::forward_as_tuple(tab_name.StringView(),
                                              tab_name.Type()),
                        std::forward_as_tuple(map_iter->second->size()));
                }

                // Excludes negative and positive infinity.
                entry_cnt += map_iter->second->size();

                std::cout << "Table '" << tab_name.StringView() << "' core ID "
                          << shard.core_id_ << ": " << map_iter->second->size()
                          << std::endl;

                assert(map_iter->second->VerifyOrdering() ==
                       map_iter->second->size());
            }
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
    uint64_t TsBase();
    void UpdateTsBase(uint64_t timestamp);

    /**
     * -------------------------------------
     *
     * Catalog Operation Interface
     *
     * -------------------------------------
     */
    /**
     * Returns false if catalog entry of higher version already exists.
     * @param table_name
     * @param cc_ng_id
     * @param catalog_image
     * @param commit_ts
     * @return
     */
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

    /**
     * Returns false if catalog entry of higher version already exists.
     * @param table_name
     * @param cc_ng_id
     * @param old_catalog_image
     * @param new_catalog_image
     * @param commit_ts
     * @return
     */
    std::pair<bool, const CatalogEntry *> CreateReplayCatalog(
        const TableName &table_name,
        NodeGroupId cc_ng_id,
        const std::string &old_catalog_image,
        const std::string &new_catalog_image,
        uint64_t old_schema_ts,
        uint64_t dirty_schema_ts);

    void CommitDirtyCatalog(const TableName &table_name, NodeGroupId cc_ng_id);

    CatalogEntry *GetCatalog(const TableName &table_name, NodeGroupId cc_ng_id);

    /**
     * @brief Drops all tables' catalogs associated with the specified cc node
     * group. The function is called when this node steps down from the leader
     * of the specified cc node group.
     *
     * @param cc_ng_id The cc node group whose leader has transferred to another
     * node.
     */
    void DropCatalogs(NodeGroupId cc_ng_id);

    std::vector<TableName> GetCatalogTableNamesForCkpt(NodeGroupId cc_ng_id);

    void CreateSchemaRecoveryTx(const ::txlog::SchemaOpMessage &schema_op_msg,
                                uint64_t txn,
                                int64_t tx_term,
                                uint64_t commit_ts);

    /**
     * ---------------------------------
     *
     * Table Range Operation Interface
     *
     * ---------------------------------
     */
    void CreateSplitRangeRecoveryTx(
        const ::txlog::SplitRangeOpMessage &ds_split_range_op_msg,
        const TableSchema *table_schema,
        const TxKey *range_key,
        std::unique_ptr<RangeRecord> splitting_range_record,
        uint32_t partition_id,
        std::unique_ptr<TxKey> new_range_key,
        uint32_t new_partition_id,
        uint32_t node_group_id,
        uint64_t txn,
        int64_t tx_term,
        uint64_t commit_ts,
        std::optional<std::pair<CcEntryAddr, ReadSetEntry>> catalog_cc_entry);

    /**
     * @brief Create a new table range entry and fill current range info with
     * given partition id and start key.
     */
    const TableRangeEntry *CreateTableRange(
        const TableName &table_name,
        const NodeGroupId ng_id,
        int32_t partition_id,
        TxKey::Uptr start_key,
        const TxKey *end_key,
        uint64_t version,
        std::vector<std::pair<TxKey::Uptr, uint32_t>> *slice_keys = nullptr);
    /**
     * @brief Initialize TableRangeEntry for a table in range_maps_.
     */
    void InitTableRanges(const TableName &range_table_name,
                         std::vector<InitRangeEntry> &init_ranges,
                         const NodeGroupId ng_id);

    /**
     * @brief Get the All Table Ranges for a table.
     */
    std::map<const TxKey *, TableRangeEntry, PtrLessThan<TxKey>>
        *GetTableRangesForATable(const TableName &range_table_name,
                                 const NodeGroupId ng_id);

    /**
     * @brief Upload new range info into range_info_ in TableRangeEntry
     * object.
     */
    const TableRangeEntry *UploadNewRangeInfo(
        const TableName &table_name,
        const NodeGroupId ng_id,
        const TxKey *key,
        const std::vector<std::unique_ptr<TxKey>> &new_key,
        const std::vector<int32_t> &new_partition_id,
        uint64_t commit_ts);

    /**
     * @brief Remove all ranges of table_name from local cc shard.
     */
    void CleanTableRange(const TableName &table_name, const NodeGroupId ng_id);

    /**
     * @brief Get the TableRangeEntry with given table name and key
     * from local cc shards. This result in a binary search with key in
     * table_ranges_.
     */
    TableRangeEntry *GetTableRangeEntry(const TableName &table_name,
                                        const NodeGroupId ng_id,
                                        const TxKey *key);

    TableRangeEntry *GetTableRangeEntry(const TableName &table_name,
                                        const NodeGroupId ng_id,
                                        int32_t range_id);

    RangeSliceId PinRangeSlice(const TableName &table_name,
                               const NodeGroupId ng_id,
                               const Schema *key_schema,
                               const Schema *rec_schema,
                               uint64_t schema_ts,
                               const KVCatalogInfo *kv_info,
                               const TxKey &key,
                               bool inclusive,
                               CcRequestBase *cc_request,
                               CcShard *cc_shard,
                               RangeSliceOpStatus &pin_status);

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
                               CcShard *cc_shard,
                               RangeSliceOpStatus &pin_status);

    StoreRange *FindRange(const TableName &table_name,
                          const NodeGroupId ng_id,
                          const TxKey &key);

    void SetTxIdent(uint32_t latest_committed_txn_no);

    void FlushData(const TableName &table_name,
                   const TableSchema *schema,
                   uint64_t ckpt_ts,
                   int64_t term,
                   uint64_t node_group,
                   std::vector<FlushRecord> *ckpt_vec,
                   std::vector<FlushRecord> *archive_vec,
                   std::vector<const TxKey *> *mv_vec,
                   CcHandlerResult<Void> &hres);

    uint64_t StatsLocalActiveSiTxs()
    {
        uint64_t min_ts = UINT64_MAX;
        for (auto &ccs : cc_shards_)
        {
            min_ts = std::min(ccs->LocalMinSiTxStartTs(), min_ts);
        }
        return min_ts;
    }

    bool EnableMvcc() const
    {
        return enable_mvcc_;
    }

    void SetWaitingCkpt(bool is_waiting)
    {
        is_waiting_ckpt_.store(is_waiting, std::memory_order_release);
    }

    bool IsWaitingCkpt()
    {
        return is_waiting_ckpt_.load(std::memory_order_acquire);
    }

    std::shared_ptr<TableSchema> GetSharedTableSchema(
        const TableName &table_name, NodeGroupId ng_id);

    bool KickoutRangeSlice(const TableName &tbl_name,
                           const NodeGroupId ng_id,
                           const TxKey &key);

    store::DataStoreHandler *const store_hd_;
    metrics::MetricsRegistry *const metrics_registry_;

private:
    void TimerRun();
    // Internal interface that exposes non const TableRangeEntry in
    // table_ranges_
    TableRangeEntry *GetTableRangeEntryInternal(
        const TableName &range_table_name,
        const NodeGroupId ng_id,
        const TxKey *key);

    TableRangeEntry *GetTableRangeEntryInternal(
        const TableName &range_table_name,
        const NodeGroupId ng_id,
        int32_t range_id);

    std::unordered_map<uint32_t, TableRangeEntry *>
        *GetTableRangeIdsForATableInternal(const TableName &range_table_name,
                                           const NodeGroupId ng_id);

    std::map<const TxKey *, TableRangeEntry, PtrLessThan<TxKey>>
        *GetTableRangesForATableInternal(const TableName &range_table_name,
                                         const NodeGroupId ng_id);
    const uint32_t node_id_;
    std::vector<std::unique_ptr<CcShard>> cc_shards_;

    // The background thread that periodically advances the timers of the local
    // shards to the current wall clock.
    std::thread timer_thd_;
    std::atomic<bool> timer_terminate_;

    // When ccshard is full and no ccentry can be kicked-out, it will notify
    // checkpointer to do checkpoint and set flag is_wait_ckpt_ to true.
    // Subsequent ccrequest is able to skip checking freeable ccentry when
    // is_wait_ckpt_ is true. After checkpoint done, set is_wait_ckpt_ to false.
    std::atomic<bool> is_waiting_ckpt_;

    // The static variable storing the local time. It is delayed time and
    // refreshed in roughly every 2 seconds by the background thread, so as to
    // reduce the cost of calling system functions to get the wall clock. The
    // local time is used by transaction state machines to determine if a lock
    // has been held too long and if so, invoke lock recovery.
    static std::atomic<uint64_t> local_clock;

    // The base timestamp  which will be adjust by local clock and commit
    // timestamp of transactions on all ccshards to keep it up to date.
    std::atomic<uint64_t> ts_base_;

    CatalogFactory *const catalog_factory_;
    std::unordered_map<TableName, std::unordered_map<NodeGroupId, CatalogEntry>>
        table_catalogs_;  // string owner

    // map<table name, map<partition id, range record>>
    std::unordered_map<
        TableName,
        std::unordered_map<
            NodeGroupId,
            std::map<const TxKey *, TableRangeEntry, PtrLessThan<TxKey>>>>
        table_ranges_;  // string owner

    // map from range id to TableRangeEntry. TableRangeEntry* here is the
    // pointer to TableRangeEntry in table_ranges_. This map is used as a
    // fast path from range id to table range in PinRangeSlice so that we
    // can avoid doing a binary search with TxKey.
    std::unordered_map<
        TableName,
        std::unordered_map<NodeGroupId,
                           std::unordered_map<uint32_t, TableRangeEntry *>>>
        table_range_ids_;

    // Protects meta data (table_ranges_ and table_catalogs_)
    std::shared_mutex meta_data_mux_;

    TxService *tx_service_;

    bool enable_mvcc_;

    friend class LocalCcHandler;
    friend class remote::RemoteCcHandler;
    friend class Checkpointer;
    friend class txservice::fault::ReplayService;
};
}  // namespace txservice
