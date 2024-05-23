#pragma once

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "catalog_factory.h"
#include "catalog_key_record.h"
#include "cc_shard.h"
#include "data_sync_task.h"
#include "error_messages.h"
#include "local_cc_handler.h"
#include "raft_log.pb.h"
#include "range_record.h"
#include "range_slice.h"
#include "store/data_store_handler.h"
#include "system_handler.h"
#include "tx_key.h"
#include "tx_service_common.h"
#include "tx_start_ts_collector.h"
#include "type.h"

namespace txservice
{
namespace remote
{
class RemoteCcHandler;
};
class Checkpointer;
class TxService;
struct ClusterScaleOp;
struct DataMigrationOp;

struct DataMigrationStatus
{
public:
    DataMigrationStatus(TxNumber cluster_scale_txn,
                        std::vector<std::vector<uint16_t>> &&bucket_ids,
                        std::vector<std::vector<NodeGroupId>> &&new_owner_ngs,
                        std::vector<TxNumber> &&migration_txns)
        : cluster_scale_txn_(cluster_scale_txn),
          bucket_ids_(std::move(bucket_ids)),
          new_owner_ngs_(std::move(new_owner_ngs)),
          migration_txns_(std::move(migration_txns)),
          next_bucket_idx_(0),
          unfinished_worker_(migration_txns_.size())
    {
    }

    TxNumber cluster_scale_txn_;
    // Each worker should migrate a batch of buckets to speed up the migration.
    std::vector<std::vector<uint16_t>> bucket_ids_;
    std::vector<std::vector<NodeGroupId>> new_owner_ngs_;
    std::vector<TxNumber> migration_txns_;
    std::atomic_size_t next_bucket_idx_;
    std::atomic_size_t unfinished_worker_;
};

struct GenerateSkStatus
{
    enum struct Status
    {
        Ongoing,
        Terminating,
        Finished
    };

    explicit GenerateSkStatus(int64_t tx_term)
        : tx_term_(tx_term),
          task_status_(Status::Finished),
          tx_term_changed_(false)
    {
    }

    GenerateSkStatus(const GenerateSkStatus &rhs) = delete;
    GenerateSkStatus(GenerateSkStatus &&rhs) = delete;

    bool StartGenerateSk(int64_t tx_term)
    {
        std::unique_lock<std::mutex> lk(status_mux_);
        if (task_status_ == Status::Ongoing ||
            task_status_ == Status::Terminating)
        {
            if (tx_term_ <= tx_term)
            {
                // Terminate the current task.
                tx_term_changed_ = true;
                status_cv_.wait(
                    lk, [this]() { return task_status_ == Status::Finished; });

                // update the task status
                task_status_ = Status::Ongoing;
                tx_term_ = tx_term;
                tx_term_changed_ = false;
            }
            else
            {
                // The @@tx_term is expired, terminate it directly.
                return false;
            }
        }
        else
        {
            task_status_ = Status::Ongoing;
            tx_term_ = tx_term;
            tx_term_changed_ = false;
        }
        return true;
    }

    void TerminateGenerateSk()
    {
        std::unique_lock<std::mutex> lk(status_mux_);
        task_status_ = Status::Terminating;
    }

    void FinishGenerateSk()
    {
        std::unique_lock<std::mutex> lk(status_mux_);
        task_status_ = Status::Finished;
        status_cv_.notify_all();
    }

    bool CheckTxTermStatus()
    {
        std::unique_lock<std::mutex> lk(status_mux_);
        return !(tx_term_changed_);
    }

    Status TaskStatus()
    {
        std::unique_lock<std::mutex> lk(status_mux_);
        return task_status_;
    }

private:
    int64_t tx_term_;
    Status task_status_;
    bool tx_term_changed_;
    std::mutex status_mux_;
    std::condition_variable status_cv_;
};
class LocalCcShards
{
public:
    static const size_t DATA_SYNC_SCAN_BATCH_SIZE = 3 * 1024;

    LocalCcShards(
        uint32_t node_id,                 // = 0,
        uint16_t core_cnt,                // = 1,
        uint32_t memory_limit_mb,         // = 1000,
        uint32_t log_limit_mb,            // = 1000,
        bool realtime_sampling,           // = false,
        CatalogFactory *catalog_factory,  // = nullptr,
        SystemHandler *system_handler,    // = nullptr,
        std::unordered_map<uint32_t, std::vector<NodeConfig>>
            *ng_configs,                    // = nullptr,
        int32_t range_bucket_seed,          // = -1,
        uint64_t cluster_config_version,    // = 0,
        store::DataStoreHandler *store_hd,  // = nullptr,
        TxService *tx_service,              // = nullptr,
        bool enable_mvcc = true,
        metrics::MetricsRegistry *metrics_registry = nullptr,
        metrics::CommonLabels common_labels = {},
        std::unordered_map<TableName, std::string> *prebuilt_tables = nullptr,
        std::function<void(std::string_view, std::string_view)> publish_func =
            nullptr);

    ~LocalCcShards();

    LocalCcShards(LocalCcShards const &) = delete;
    void operator=(LocalCcShards const &) = delete;

    CcShard *GetCcShard(size_t core_idx)
    {
        return cc_shards_[core_idx].get();
    }

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

    size_t QueueSize(size_t thd_id)
    {
        return cc_shards_[thd_id]->QueueSize();
    };

    bool IsIdle(uint32_t thd_id) const
    {
        return cc_shards_[thd_id]->IsIdle();
    }

    void SetTxProcNotifier(uint16_t thd_id,
                           std::atomic<TxProcessorStatus> *tx_proc_status,
                           TxProcCoordinator *tx_coordi)
    {
        return cc_shards_[thd_id]->SetTxProcNotifier(tx_proc_status, tx_coordi);
    }

    size_t Count() const
    {
        return cc_shards_.size();
    }

    TxService *GetTxservice()
    {
        return tx_service_;
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

    void NotifyCheckPointer(bool request_ckpt = true)
    {
        cc_shards_[0]->NotifyCkpt(request_ckpt);
    }

    void PrintCcMap()
    {
        std::unordered_map<TableName, size_t>
            mapsizes;  // not string owner, sv -> native_ccms_
        for (const auto &cc_shard : cc_shards_)
        {
            CcShard &shard = *cc_shard;

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

    void EnqueueToCcShard(uint16_t cc_shard_idx, CcRequestBase *req)
    {
        assert(cc_shard_idx < cc_shards_.size());
        auto &ccs = cc_shards_[cc_shard_idx];
        ccs->Enqueue(req);
    }

    static uint64_t ClockTs();
    uint64_t TsBase();
    void UpdateTsBase(uint64_t timestamp);
    void StartBackgroudWorkers();

    /**
     * @brief Create new heap used by table ranges only.
     *
     * Note that this function should only be invoked by threads whose life
     * cycle can last until the end of the process.
     */
    void InitializeTableRangesHeap()
    {
#ifdef RANGE_PARTITION_ENABLED
        std::unique_lock<std::mutex> lk(table_ranges_heap_mux_);
        if (!table_ranges_heap_)
        {
            table_ranges_thread_id_ = mi_thread_id();
            table_ranges_heap_ = mi_heap_new();
        }
#endif
    }

    mi_threadid_t GetTableRangesHeapThreadId() const
    {
        return table_ranges_thread_id_;
    }

    mi_heap_t *GetTableRangesHeap() const
    {
        return table_ranges_heap_;
    }

    /**
     * @brief Check whether the table ranges heap reach the limitation.
     * NOTE: Be sure that this function is called in context of table ranges
     * heap.
     */
    bool TableRangesMemoryFull()
    {
        if (table_ranges_heap_ != nullptr)
        {
            int64_t allocated, committed;
            mi_thread_stats(&allocated, &committed);
            return (static_cast<size_t>(allocated) >=
                    range_slice_memory_limit_);
        }
        else
        {
            return false;
        }
    }

    /**
     * @brief Check whether the table ranges heap has enough memory.
     * NOTE: Be sure that this function is called in context of table ranges
     * heap.
     */
    bool HasEnoughTableRangesMemory()
    {
        if (table_ranges_heap_ != nullptr)
        {
            size_t target_memory_size = range_slice_memory_limit_ / 10 * 9;
            int64_t allocated, committed;
            mi_thread_stats(&allocated, &committed);
            return (static_cast<size_t>(allocated) <= target_memory_size);
        }
        else
        {
            return false;
        }
    }

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
        uint64_t commit_ts);

    CatalogEntry *CreateDirtyCatalog(const TableName &table_name,
                                     NodeGroupId cc_ng_id,
                                     const std::string &catalog_image,
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
    CatalogEntry *GetCatalogInternal(const TableName &table_name,
                                     NodeGroupId cc_ng_id);

    /**
     * @brief Drops all tables' catalogs associated with the specified cc node
     * group. The function is called when this node steps down from the leader
     * of the specified cc node group.
     *
     * @param cc_ng_id The cc node group whose leader has transferred to another
     * node.
     */
    void DropCatalogs(NodeGroupId cc_ng_id);

    /**
     * @return Pair of TableName and bool, the bool value is used to sign
     * whether this table is dirty table(such as dirty index table).
     */
    std::unordered_map<TableName, bool> GetCatalogTableNameSnapshot(
        NodeGroupId cc_ng_id, uint64_t snapshot_ts);

    void CreateSchemaRecoveryTx(ReplayLogCc &replay_log_cc,
                                const ::txlog::SchemaOpMessage &schema_op_msg,
                                int64_t tx_term);

    /**
     * ---------------------------------
     *
     * Table Range Operation Interface
     *
     * ---------------------------------
     */
    void CreateSplitRangeRecoveryTx(
        ReplayLogCc &replay_log_cc,
        const ::txlog::SplitRangeOpMessage &ds_split_range_op_msg,
        const TableSchema *table_schema,
        int32_t partition_id,
        const RangeInfo *range_info,
        std::vector<TxKey> &&new_range_key,
        std::vector<int32_t> &&new_partition_ids,
        uint32_t node_group_id,
        int64_t tx_term);

    /**
     * @brief Create a new table range entry and fill current range info with
     * given partition id and start key.
     */
    template <typename KeyT>
    const TemplateTableRangeEntry<KeyT> *CreateTableRange(
        const TableName &table_name,
        const NodeGroupId ng_id,
        int32_t partition_id,
        const KeyT &start_key,
        uint64_t version,
        std::vector<SliceInitInfo> *slice_keys = nullptr)
    {
        std::unique_lock<std::shared_mutex> lk(meta_data_mux_);
        std::vector<TableRangeEntry *> new_entries;

        std::unique_lock<std::mutex> heap_lk(table_ranges_heap_mux_);
        mi_override_thread(table_ranges_thread_id_);
        mi_heap_t *prev_heap = mi_heap_set_default(table_ranges_heap_);

        bool range_slice_mem_full = TableRangesMemoryFull();

        std::map<TxKey, TableRangeEntry::uptr> *ranges =
            GetTableRangesForATableInternal(table_name, ng_id);
        std::unordered_map<uint32_t, TableRangeEntry *> *range_ids =
            GetTableRangeIdsForATableInternal(table_name, ng_id);
        std::unique_ptr<TemplateStoreRange<KeyT>> range_slices = nullptr;
        NodeGroupId range_ng =
            GetRangeOwnerInternal(partition_id, ng_id)->BucketOwner();

        TxKey range_tx_key(&start_key);
        auto range_it = ranges->find(range_tx_key);
        if (range_it == ranges->end())
        {
            // The created range entry copies the start key and references the
            // input end key, which points to the containing range's end key.
            std::unique_ptr<TemplateTableRangeEntry<KeyT>> new_range_entry =
                std::make_unique<TemplateTableRangeEntry<KeyT>>(
                    &start_key, version, partition_id);

            TemplateTableRangeEntry<KeyT> *new_range_ptr =
                new_range_entry.get();
            auto [new_range_it, is_insert] =
                ranges->try_emplace(TxKey(new_range_entry->RangeStartKey()),
                                    std::move(new_range_entry));
            assert(is_insert);

            auto next_range_it = std::next(new_range_it);
            if (next_range_it != ranges->end())
            {
                const TemplateTableRangeEntry<KeyT> *next_entry =
                    static_cast<const TemplateTableRangeEntry<KeyT> *>(
                        next_range_it->second.get());
                const KeyT *next_start = next_entry->RangeStartKey();
                new_range_ptr->SetRangeEndKey(next_start);
            }
            else
            {
                new_range_ptr->SetRangeEndKey(KeyT::PositiveInfinity());
            }

            // Update previous range entry's end key if the range is inserted
            // into table_ranges. The new inserted range is always not the
            // smallest range since negative inf is one of the first default
            // range start key.
            auto prev_it = std::prev(new_range_it);
            const KeyT *new_range_start = new_range_ptr->RangeStartKey();
            TemplateTableRangeEntry<KeyT> *prev_range_entry =
                static_cast<TemplateTableRangeEntry<KeyT> *>(
                    prev_it->second.get());
            prev_range_entry->SetRangeEndKey(new_range_start);

            range_ids->try_emplace(partition_id, new_range_ptr);

            if (ng_id == range_ng && slice_keys && !range_slice_mem_full)
            {
                new_range_ptr->InitRangeSlices(std::move(*slice_keys),
                                               range_ng);
            }

            mi_restore_default_thread_id();
            mi_heap_set_default(prev_heap);

            return new_range_ptr;
        }
        else if (range_it->second->Version() < version)
        {
            // Update existing range entry's version range slice info if the
            // passed in version is newer.
            TemplateTableRangeEntry<KeyT> *range_entry =
                static_cast<TemplateTableRangeEntry<KeyT> *>(
                    range_it->second.get());

            if (ng_id == range_ng && slice_keys)
            {
                const KeyT *r_start = range_entry->TypedRangeInfo()->StartKey();
                const KeyT *r_end = range_entry->TypedRangeInfo()->EndKey();
                range_slices = std::make_unique<TemplateStoreRange<KeyT>>(
                    r_start, r_end, partition_id, range_ng, *this);
                range_slices->InitSlices(std::move(*slice_keys));
            }
            range_entry->UpdateRangeEntry(version, std::move(range_slices));
        }

        mi_restore_default_thread_id();
        mi_heap_set_default(prev_heap);

        return static_cast<TemplateTableRangeEntry<KeyT> *>(
            range_it->second.get());
    }

    /**
     * @brief Initialize TableRangeEntry for a table in range_maps_.
     */
    void InitTableRanges(const TableName &range_table_name,
                         std::vector<InitRangeEntry> &init_ranges,
                         const NodeGroupId ng_id,
                         bool empty_table = false);

    /**
     * @brief Get the All Table Ranges for a table.
     */
    std::map<TxKey, TableRangeEntry::uptr> *GetTableRangesForATable(
        const TableName &range_table_name, const NodeGroupId ng_id);

    /**
     * @brief Upload new range info into range_info_ in TableRangeEntry
     * object.
     */
    template <typename KeyT>
    TemplateTableRangeEntry<KeyT> *UploadNewRangeInfo(
        const TableName &table_name,
        const NodeGroupId ng_id,
        const KeyT &key,
        const std::vector<TxKey> &new_key,
        const std::vector<int32_t> &new_partition_id,
        uint64_t commit_ts)
    {
        std::vector<TxKey> new_key_copy;
        new_key_copy.reserve(new_key.size());
        for (const TxKey &key : new_key)
        {
            const KeyT *typed_key = key.GetKey<KeyT>();
            assert(typed_key->Type() == KeyType::Normal);
            new_key_copy.emplace_back(std::make_unique<KeyT>(*typed_key));
        }
        std::vector<int32_t> partition_copy{new_partition_id};

        std::shared_lock<std::shared_mutex> lk(meta_data_mux_);
        TxKey map_key(&key);
        TemplateTableRangeEntry<KeyT> *range_entry =
            static_cast<TemplateTableRangeEntry<KeyT> *>(
                GetTableRangeEntryInternal(table_name, ng_id, map_key));
        assert(range_entry);
        // Set dirty range in local cc shard range entry.
        range_entry->UploadNewRangeInfo(
            std::move(new_key_copy), std::move(partition_copy), commit_ts);

        return range_entry;
    }

    /**
     * @brief Remove all ranges of table_name from local cc shard.
     */
    void CleanTableRange(const TableName &table_name, NodeGroupId ng_id);

    /**
     * @brief Remove all ranges of ng_id from local cc shard.
     */
    void DropTableRanges(NodeGroupId ng_id);

    /**
     * @brief Kickout least recently used range slices info from local cc
     * shards. Note that this function should not be called on txprocessor since
     * it will block for a while(up to milliseconds).
     */
    void KickoutRangeSlices();

    /**
     * @brief Get the TableRangeEntry with given table name and key
     * from local cc shards. This result in a binary search with key in
     * table_ranges_.
     */
    TableRangeEntry *GetTableRangeEntry(const TableName &table_name,
                                        const NodeGroupId ng_id,
                                        const TxKey &key);

    std::optional<std::tuple<uint64_t, TxKey, TxKey>> GetTableRangeKeys(
        const TableName &table_name, const NodeGroupId ng_id, int32_t range_id);

    bool CheckRangeVersion(const TableName &table_name,
                           const NodeGroupId ng_id,
                           int32_t range_id,
                           uint64_t range_version);

    const TableRangeEntry *GetTableRangeEntry(const TableName &table_name,
                                              const NodeGroupId ng_id,
                                              int32_t range_id);

    const TableRangeEntry *GetTableRangeEntryNoLocking(
        const TableName &table_name, const NodeGroupId ng_id, const TxKey &key);

    template <typename KeyT>
    RangeSliceId PinRangeSlice(const TableName &table_name,
                               NodeGroupId cc_ng_id,
                               int64_t cc_ng_term,
                               const Schema *key_schema,
                               const Schema *rec_schema,
                               uint64_t schema_ts,
                               const KVCatalogInfo *kv_info,
                               const KeyT &key,
                               bool inclusive,
                               CcRequestBase *cc_request,
                               CcShard *cc_shard,
                               RangeSliceOpStatus &pin_status,
                               bool force_load,
                               uint8_t prefetch_size)
    {
        std::shared_lock<std::shared_mutex> lk(meta_data_mux_);

        TableName range_table_name(table_name.StringView(),
                                   TableType::RangePartition);
        TxKey slice_key(&key);

        TemplateTableRangeEntry<KeyT> *range_entry =
            static_cast<TemplateTableRangeEntry<KeyT> *>(
                GetTableRangeEntryInternal(
                    range_table_name, cc_ng_id, slice_key));
        if (!range_entry)
        {
            // Table range info not initialized, initialize range info first
            cc_shard->FetchTableRanges(
                range_table_name, cc_request, cc_ng_id, cc_ng_term);
            pin_status = RangeSliceOpStatus::BlockedOnLoad;
            return RangeSliceId();
        }
        std::shared_lock<std::shared_mutex> range_lk(range_entry->mux_);
        TemplateStoreRange<KeyT> *store_range = range_entry->TypedStoreRange();
        if (store_range == nullptr)
        {
            // Check if range is owned by cc_ng_id. If so, load range slices
            // from data store
            if (GetBucketInfoInternal(
                    Sharder::Instance().MapRangeIdToBucketId(
                        range_entry->GetRangeInfo()->PartitionId()),
                    cc_ng_id)
                    ->BucketOwner() == cc_ng_id)
            {
                // release shared lock since FetchRangeSlices will acquire
                // unique lock.
                range_lk.unlock();
                range_entry->FetchRangeSlices(range_table_name,
                                              cc_request,
                                              cc_ng_id,
                                              cc_ng_term,
                                              cc_shard);
                pin_status = RangeSliceOpStatus::BlockedOnLoad;
            }
            else
            {
                pin_status = RangeSliceOpStatus::NotOwner;
            }
            return RangeSliceId();
        }

        store_range->UpdateLastAccessedTs(ClockTs());
        const StoreSlice *last_pinned_slice;
        return store_range->PinSlices(table_name,
                                      cc_ng_term,
                                      key,
                                      inclusive,
                                      nullptr,
                                      false,
                                      key_schema,
                                      rec_schema,
                                      schema_ts,
                                      INT64_MAX,
                                      kv_info,
                                      cc_request,
                                      cc_shard,
                                      store_hd_,
                                      force_load,
                                      prefetch_size,
                                      1,
                                      true,
                                      pin_status,
                                      last_pinned_slice);
    }

    template <typename KeyT>
    RangeSliceId PinRangeSlices(const TableName &table_name,
                                NodeGroupId cc_ng_id,
                                int64_t cc_ng_term,
                                const Schema *key_schema,
                                const Schema *rec_schema,
                                uint64_t schema_ts,
                                const KVCatalogInfo *kv_info,
                                uint32_t range_id,
                                const KeyT &start_key,
                                bool start_inclusive,
                                const KeyT *end_key,
                                bool end_inclusive,
                                CcRequestBase *cc_request,
                                CcShard *cc_shard,
                                bool force_load,
                                uint8_t prefetch_size,
                                uint8_t max_pin_cnt,
                                bool forward_pin,
                                RangeSliceOpStatus &pin_status,
                                const StoreSlice *&last_pinned_slice)
    {
        std::shared_lock<std::shared_mutex> lk(meta_data_mux_);

        TableName range_table_name(table_name.StringView(),
                                   TableType::RangePartition);

        TemplateTableRangeEntry<KeyT> *range_entry =
            static_cast<TemplateTableRangeEntry<KeyT> *>(
                GetTableRangeEntryInternal(
                    range_table_name, cc_ng_id, range_id));
        if (!range_entry)
        {
            // Table range info not initialized, initialize range info first
            cc_shard->FetchTableRanges(
                range_table_name, cc_request, cc_ng_id, cc_ng_term);
            pin_status = RangeSliceOpStatus::BlockedOnLoad;
            return RangeSliceId();
        }
        std::shared_lock<std::shared_mutex> range_lk(range_entry->mux_);
        TemplateStoreRange<KeyT> *store_range = range_entry->TypedStoreRange();
        if (store_range == nullptr)
        {
            // Check if range is owned by cc_ng_id. If so, load range slices
            // from data store
            if (GetBucketInfoInternal(
                    Sharder::Instance().MapRangeIdToBucketId(
                        range_entry->GetRangeInfo()->PartitionId()),
                    cc_ng_id)
                    ->BucketOwner() == cc_ng_id)
            {
                // release shared lock since FetchRangeSlices will acquire
                // unique lock.
                range_lk.unlock();
                range_entry->FetchRangeSlices(range_table_name,
                                              cc_request,
                                              cc_ng_id,
                                              cc_ng_term,
                                              cc_shard);
                pin_status = RangeSliceOpStatus::BlockedOnLoad;
            }
            else
            {
                pin_status = RangeSliceOpStatus::NotOwner;
            }
            return RangeSliceId();
        }
        uint64_t snapshot_ts = 0;
        if (EnableMvcc())
        {
            snapshot_ts = TxStartTsCollector::Instance().GlobalMinSiTxStartTs();
        }

        store_range->UpdateLastAccessedTs(ClockTs());
        return store_range->PinSlices(table_name,
                                      cc_ng_term,
                                      start_key,
                                      start_inclusive,
                                      end_key,
                                      end_inclusive,
                                      key_schema,
                                      rec_schema,
                                      schema_ts,
                                      snapshot_ts,
                                      kv_info,
                                      cc_request,
                                      cc_shard,
                                      store_hd_,
                                      force_load,
                                      prefetch_size,
                                      max_pin_cnt,
                                      forward_pin,
                                      pin_status,
                                      last_pinned_slice);
    }

    uint64_t CountRanges(const TableName &table_name,
                         const NodeGroupId ng_id,
                         const NodeGroupId key_ng_id) const;

    uint64_t CountRangesLockless(const TableName &table_name,
                                 const NodeGroupId ng_id,
                                 const NodeGroupId key_ng_id) const;

    uint64_t CountSlices(const TableName &table_name,
                         const NodeGroupId ng_id,
                         const NodeGroupId local_ng_id) const;

    void SetTxIdent(uint32_t latest_committed_txn_no);

    void FlushData(const TableName &table_name,
                   const TableSchema *schema,
                   uint64_t ckpt_ts,
                   int64_t term,
                   uint64_t node_group,
                   std::vector<FlushRecord> *ckpt_vec,
                   std::vector<FlushRecord> *archive_vec,
                   std::vector<TxKey> *mv_vec,
                   CcHandlerResult<Void> &hres,
                   bool delay_update_ckpt_ts);

    // Return last succ ckpt timestamp on table.
    void EnqueueDataSyncTaskForTable(
        const TableName &table_name,
        uint32_t ng_id,
        int64_t ng_term,
        uint64_t data_sync_ts,
        uint64_t &last_data_sync_ts,
        bool is_dirty = false,
        bool can_be_skipped = false,
        std::shared_ptr<DataSyncStatus> status = nullptr,
        CcHandlerResult<Void> *hres = nullptr);

    /**
     * @brief When TxService is stopping, this function will be called.
     *
     * NOTE: This function should be called after Checkpointer::Terminate
     */
    void Terminate();

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

    TxService *GetTxService() const
    {
        return tx_service_;
    }

    CatalogFactory *GetCatalogFactory() const
    {
        return catalog_factory_;
    }

    SystemHandler *GetSystemHandler()
    {
        return system_handler_;
    }

    std::shared_ptr<TableSchema> GetSharedTableSchema(
        const TableName &table_name, NodeGroupId ng_id);

    template <typename KeyT>
    bool KickoutKeyInSlice(const TableName &tbl_name,
                           const NodeGroupId ng_id,
                           const KeyT &key)
    {
        std::shared_lock<std::shared_mutex> s_lk(meta_data_mux_);

        TableName range_tbl_name(tbl_name.StringView(),
                                 TableType::RangePartition);
        TxKey tx_key(&key);
        TemplateTableRangeEntry<KeyT> *entry =
            static_cast<TemplateTableRangeEntry<KeyT> *>(
                GetTableRangeEntryInternal(range_tbl_name, ng_id, tx_key));
        if (entry == nullptr)
        {
            return true;
        }
        else
        {
            bool res = entry->KickoutKeyInSlice(key);
            if (res
                // TODO(liunyl): enable this after cluser scale is added.
                // && DuringClusterScale()
                // TODO(lzx): enable it when add "sending range data feature".
            )
            {
                // If the key is kicked out, we need to update the bucket info
                // to disallow upload batch cc since we might already
                // have kicked out newer version from cc map.
                BucketInfo *bucket_info = GetBucketInfoInternal(
                    Sharder::Instance().MapRangeIdToBucketId(
                        entry->GetRangeInfo()->PartitionId()),
                    ng_id);
                bucket_info->SetAcceptsUploadBatch(false);
            }
            return res;
        }
    }

    template <typename KeyT>
    void KickoutKeyInBucket(const TableName &tbl_name,
                            const NodeGroupId ng_id,
                            const KeyT &key)
    {
        uint16_t bucket_id = key.Hash() & 0x3FFF;
        std::shared_lock<std::shared_mutex> s_lk(meta_data_mux_);
        GetBucketInfoInternal(bucket_id, ng_id)->SetAcceptsUploadBatch(false);
    }

    /**
     * Create table statistics and bind it to table_schema
     */
    std::pair<std::shared_ptr<Statistics>, bool> InitTableStatistics(
        TableSchema *table_schema, NodeGroupId ng_id);

    /**
     * Create table statistics and bind it to table_schema and
     * dirty_table_schema if not null.
     */
    std::pair<std::shared_ptr<Statistics>, bool> InitTableStatistics(
        TableSchema *table_schema,
        TableSchema *dirty_table_schema,
        NodeGroupId ng_id,
        std::unordered_map<TableName, std::pair<uint64_t, std::vector<TxKey>>>
            sample_pool_map,
        CcShard *ccs);

    StatisticsEntry *GetTableStatistics(const TableName &table_name,
                                        NodeGroupId ng_id);

    void CleanTableStatistics(const TableName &table_name);

    void DropTableStatistics(NodeGroupId ng_id);

    void BroadcastIndexStatistics(
        TransactionExecution *txm,
        NodeGroupId ng_id,
        const TableName &table_name,
        const TableSchema *table_schema,
        const remote::NodeGroupSamplePool &sample_pool);

    void SetBucketMigrating(bool is_migrating)
    {
#ifdef RANGE_PARTITION_ENABLED
        assert(false);
#else
        buckets_migrating_.store(is_migrating, std::memory_order_release);
#endif
    }

    bool IsBucketsMigrating()
    {
#ifdef RANGE_PARTITION_ENABLED
        assert(false);
#else
        return buckets_migrating_.load(std::memory_order_relaxed);
#endif
    }

    const BucketInfo *GetBucketInfo(const uint16_t bucket_id,
                                    const NodeGroupId ng_id) const;

    NodeGroupId GetBucketOwner(const uint16_t bucket_id,
                               const NodeGroupId ng_id) const;

    const BucketInfo *GetRangeOwner(const int32_t range_id,
                                    const NodeGroupId ng_id) const;

    const BucketInfo *GetRangeOwnerNoLocking(const int32_t range_id,
                                             const NodeGroupId ng_id) const;

    const std::unordered_map<uint16_t, std::unique_ptr<BucketInfo>>
        *GetAllBucketInfos(const NodeGroupId ng_id) const;

    void DropBucketInfo(NodeGroupId ng_id);

    void InitRangeBuckets(NodeGroupId ng_id,
                          uint32_t ng_cnt,
                          uint64_t version,
                          int32_t seed);

    bool IsRangeBucketsInitialized(NodeGroupId ng_id);

    const BucketInfo *UploadNewBucketInfo(NodeGroupId ng_id,
                                          uint16_t bucket_id,
                                          NodeGroupId dirty_ng,
                                          uint64_t dirty_version);

    const BucketInfo *UploadBucketInfo(NodeGroupId ng_id,
                                       uint16_t bucket_id,
                                       NodeGroupId owner_ng,
                                       uint64_t version);

    bool DropStoreRangesInBucket(NodeGroupId ng_id, uint16_t bucket_id);

    std::unordered_map<TableName, std::unordered_set<int>> GetRangesInBucket(
        uint16_t bucket_id, NodeGroupId ng_id);

    const BucketInfo *CommitDirtyBucketInfo(NodeGroupId ng_id,
                                            uint16_t bucket_id);

    void EnqueueDataSyncTaskForBucket(
#ifdef RANGE_PARTITION_ENABLED
        const std::unordered_map<TableName, std::unordered_set<int32_t>>
            &ranges_in_bucket_snapshot,
#else
        const std::vector<uint16_t> &bucket_id,
        bool send_cache_for_migration,
#endif
        uint32_t ng_id,
        int64_t ng_term,
        uint64_t data_sync_ts,
        CcHandlerResult<Void> *hres);

    void InitPrebuiltTables(NodeGroupId ng_id);

    void PublishMessage(const std::string &chan, const std::string &message);

    using PublishArg = std::tuple<LocalCcShards *, std::string, std::string>;

    static void *Publish(void *raw_args)
    {
        std::unique_ptr<PublishArg> args_guard(
            static_cast<PublishArg *>(raw_args));

        auto [self, ch, msg] = *static_cast<PublishArg *>(raw_args);
        self->publish_func_(ch, msg);
        return nullptr;
    }

    /**
     * @brief Generate bucket migration plan based on the new node group config.
     */
    std::unordered_map<NodeGroupId, BucketMigrateInfo>
    GenerateBucketMigrationPlan(uint32_t new_ng_count, int32_t seed);
    // Memory limit of heap memory allocated by range slices info.
    // 5% of the total memory limit.
    const uint64_t range_slice_memory_limit_;

    GenerateSkStatus *GetGenerateSkStatus(NodeGroupId ng_id,
                                          uint64_t tx_number,
                                          int32_t partition_id,
                                          int64_t tx_term);
    void ClearGenerateSkStatus(NodeGroupId ng_id,
                               uint64_t tx_number,
                               int32_t partition_id);

    store::DataStoreHandler *const store_hd_;

    /*

    table_schema_op_pool_ and split_flush_range_op_pool_ are introduced to
    ensure the CcHandlerResult(stored in UpsertTableOp/SplitFlushRangeOp)
    pointer validation:

    if failover didn't happen, the pointer receivied from remote PostWriteAll
    response CcMessage should always be valid(memory not being freed). This is
    important in the case where network timeout happens and remote response(sent
    by retry) arrives after UpsertTableOp has finished(finished means txm has
    been reset and schema_op_ unique pointer in txm has been set to null).

    To avoid this pointer invalidation, the UpsertTableOp is moved from txm to
    local_cc_shards once finished the last step of the schema op to maintain
    the validity of the pointer. (note: this is different from pointer
    stability, which is guaranteed by checking the node term and whether this
    node is the leader of a node group).

    */
    std::vector<std::unique_ptr<UpsertTableOp>> table_schema_op_pool_;
    std::mutex table_schema_op_pool_mux_;
    std::vector<std::unique_ptr<SplitFlushRangeOp>> split_flush_range_op_pool_;
    std::mutex split_flush_range_op_pool_mux_;
    std::vector<std::unique_ptr<UpsertTableIndexOp>> table_index_op_pool_;
    std::mutex table_index_op_pool_mux_;

    // Since there's only 1 cluster scale event at a time across the cluster,
    // we don't need a pool for it. We just need to make sure that the op is not
    // invalidated in case late remote cc response comes in.

    std::vector<std::unique_ptr<ClusterScaleOp>> cluster_scale_op_pool_;
    std::mutex cluster_scale_op_mux_;

    std::mutex data_migration_op_pool_mux_;
    std::vector<std::unique_ptr<DataMigrationOp>> migration_op_pool_;

    // protects the table ranges heap to avoid concurrent memory requests by
    // multiple threads.
    std::mutex table_ranges_heap_mux_;

private:
    void TimerRun();
    // Internal interface that exposes non const return type and does
    // not acquire mutex lock.
    TableRangeEntry *GetTableRangeEntryInternal(
        const TableName &range_table_name,
        const NodeGroupId ng_id,
        const TxKey &key);

    TableRangeEntry *GetTableRangeEntryInternal(
        const TableName &range_table_name,
        const NodeGroupId ng_id,
        int32_t range_id);

    std::unordered_map<uint32_t, TableRangeEntry *>
        *GetTableRangeIdsForATableInternal(const TableName &range_table_name,
                                           const NodeGroupId ng_id);

    std::map<TxKey, TableRangeEntry::uptr> *GetTableRangesForATableInternal(
        const TableName &range_table_name, const NodeGroupId ng_id);

    // These 2 FindRange should only be used when we need to update range and
    // slice spec during data sync of a range. StoreRange should not be accessed
    // without protection. Any access must acquire shared lock on
    // TableRangeEntry.
    StoreRange *FindRange(const TableName &table_name,
                          const NodeGroupId ng_id,
                          const TxKey &key);

    StoreRange *FindRange(const TableName &table_name,
                          const NodeGroupId ng_id,
                          int32_t range_id);

    BucketInfo *GetBucketInfoInternal(const uint16_t bucket_id,
                                      const NodeGroupId ng_id) const;

    BucketInfo *GetRangeOwnerInternal(int32_t range_id,
                                      const NodeGroupId ng_id) const;

#ifdef RANGE_PARTITION_ENABLED
    bool EnqueueRangeDataSyncTask(const TableName &table_name,
                                  uint32_t ng_id,
                                  int64_t ng_term,
                                  TableRangeEntry *range_entry,
                                  uint64_t data_sync_ts,
                                  bool is_dirty,
                                  bool can_be_skipped,
                                  std::shared_ptr<DataSyncStatus> status,
                                  CcHandlerResult<Void> *hres);
#else
    bool EnqueueDataSyncTaskToCore(
        const TableName &table_name,
        uint32_t ng_id,
        int64_t ng_term,
        uint64_t data_sync_ts,
        uint16_t core_idx,
        bool is_dirty = false,
        bool can_be_skipped = false,
        std::shared_ptr<DataSyncStatus> status = nullptr,
        CcHandlerResult<Void> *hres = nullptr,
        bool send_cache_for_migration = false,
        std::function<bool(size_t)> filter_lambda = [](size_t) -> bool
        { return true; });
#endif

    void PopPendingTask(NodeGroupId ng_id,
                        int64_t ng_term,
                        const TableName &table_name,
#ifdef RANGE_PARTITION_ENABLED
                        uint32_t range_id
#else
                        uint16_t core_idx
#endif
    );

    void ClearAllPendingTasks(NodeGroupId ng_id,
                              int64_t ng_term,
                              const TableName &table_name,
#ifdef RANGE_PARTITION_ENABLED
                              uint32_t range_id
#else
                              uint16_t core_idx
#endif
    );

#ifndef RANGE_PARTITION_ENABLED
    void PostProcessDataSyncTask(std::shared_ptr<DataSyncTask> task,
                                 TransactionExecution *data_sync_txm,
                                 CatalogEntry *catalog_entry,
                                 DataSyncTask::CkptErrorCode ckpt_err,
                                 size_t worker_idx);
#endif

    // If one table is a prebuilt table, it means that its enable_data_store
    // option is disabled.
    bool PrebuiltTable(const TableName &table_name) const
    {
        return prebuilt_tables_.find(table_name) != prebuilt_tables_.end();
    }

    const uint32_t node_id_;
    std::vector<std::unique_ptr<CcShard>> cc_shards_;

    // The background thread that periodically advances the timers of the local
    // shards to the current wall clock.
    std::thread timer_thd_;
    bool timer_terminate_;
    std::mutex timer_terminate_mux_;
    std::condition_variable timer_terminate_cv_;
    // std::atomic<bool> timer_terminate_;

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

    SystemHandler *const system_handler_;

    std::unordered_map<TableName, std::unordered_map<NodeGroupId, CatalogEntry>>
        table_catalogs_;  // string owner

    // map<table name, map<partition id, range record>>
    std::unordered_map<TableName,
                       std::unordered_map<NodeGroupId,
                                          std::map<TxKey,
                                                   TableRangeEntry::uptr>>>
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

    std::unordered_map<TableName,
                       std::unordered_map<NodeGroupId, StatisticsEntry>>
        table_statistics_map_;

    // map to store mapping relationship from bucket id to bucket info
    // that stores the bucket owner of this bucket.
    std::unordered_map<
        NodeGroupId,
        std::unordered_map<uint16_t, std::unique_ptr<BucketInfo>>>
        bucket_infos_;

    // heap memory resource used by table ranges.
    mi_heap_t *table_ranges_heap_{nullptr};
    mi_threadid_t table_ranges_thread_id_{0};

    // Protects meta data (table_ranges_ and table_catalogs_)
    mutable std::shared_mutex meta_data_mux_;
#ifndef RANGE_PARTITION_ENABLED
    std::atomic_bool buckets_migrating_{false};
#endif
    // If enable_data_store is disabled for one table, its catalog needs to be
    // created at launch or on_leader_start. enable_data_store option comes from
    // configuration instead of table schema, hence we need a separate place to
    // store this information.
    std::unordered_map<TableName, std::string> prebuilt_tables_;

    TxService *tx_service_;

    bool enable_mvcc_;

    bool realtime_sampling_;

    /**
     * DataSync Operation Interface
     */
    WorkerThreadContext data_sync_worker_ctx_;

#ifdef RANGE_PARTITION_ENABLED
    std::deque<std::shared_ptr<DataSyncTask>> data_sync_task_queue_;
#else
    std::vector<std::deque<std::shared_ptr<DataSyncTask>>>
        data_sync_task_queue_;
#endif
    struct DataSyncTaskLimiter
    {
        // `0` means no pending task
        uint64_t latest_pending_task_ts_{0};
        std::queue<std::shared_ptr<DataSyncTask>> pending_tasks_;

        uint64_t UnsetLatestPendingTs()
        {
            uint64_t ts = latest_pending_task_ts_;
            latest_pending_task_ts_ = 0;
            return ts;
        }
    };

    struct TaskLimiterKey
    {
#ifdef RANGE_PARTITION_ENABLED
        explicit TaskLimiterKey(NodeGroupId node_group_id,
                                int64_t node_group_term,
                                std::string_view table_name,
                                TableType table_type,
                                uint32_t range_id)
            : node_group_id_(node_group_id),
              node_group_term_(node_group_term),
              table_name_(table_name, table_type),
              range_id_(range_id)
        {
        }
#else
        explicit TaskLimiterKey(NodeGroupId node_group_id,
                                int64_t node_group_term,
                                std::string_view table_name,
                                TableType table_type,
                                uint16_t core_id)
            : node_group_id_(node_group_id),
              node_group_term_(node_group_term),
              table_name_(table_name, table_type),
              core_id_(core_id)
        {
        }
#endif

        TaskLimiterKey(const TaskLimiterKey &rhs)
            : node_group_id_(rhs.node_group_id_),
              node_group_term_(rhs.node_group_term_),
              // deep copy
              table_name_(rhs.table_name_.StringView().data(),
                          rhs.table_name_.StringView().size(),
                          rhs.table_name_.Type())
#ifdef RANGE_PARTITION_ENABLED
              ,
              range_id_(rhs.range_id_)
#else
              ,
              core_id_(rhs.core_id_)
#endif
        {
        }

        TaskLimiterKey &operator=(const TaskLimiterKey &) = delete;
        TaskLimiterKey(TaskLimiterKey &&) = delete;
        TaskLimiterKey &operator=(TaskLimiterKey &&) = delete;

        bool operator==(const TaskLimiterKey &other) const
        {
#ifdef RANGE_PARTITION_ENABLED
            return node_group_id_ == other.node_group_id_ &&
                   node_group_term_ == other.node_group_term_ &&
                   table_name_ == other.table_name_ &&
                   range_id_ == other.range_id_;
#else
            return node_group_id_ == other.node_group_id_ &&
                   node_group_term_ == other.node_group_term_ &&
                   table_name_ == other.table_name_ &&
                   core_id_ == other.core_id_;
#endif
        }

        NodeGroupId node_group_id_;
        int64_t node_group_term_;
        TableName table_name_;
#ifdef RANGE_PARTITION_ENABLED
        uint32_t range_id_;
#else
        uint16_t core_id_;
#endif
    };

    struct LimiterKeyHasher
    {
        size_t operator()(const TaskLimiterKey &key) const
        {
#ifdef RANGE_PARTITION_ENABLED
            size_t h1 = std::hash<NodeGroupId>()(key.node_group_id_);
            size_t h2 = std::hash<int64_t>()(key.node_group_term_);
            size_t h3 = std::hash<TableName>()(key.table_name_);
            size_t h4 = std::hash<uint32_t>()(key.range_id_);
            return h1 ^ (h2 << 1) ^ (h3 << 3) ^ (h4 << 5);
#else
            size_t h1 = std::hash<NodeGroupId>()(key.node_group_id_);
            size_t h2 = std::hash<int64_t>()(key.node_group_term_);
            size_t h3 = std::hash<TableName>()(key.table_name_);
            size_t h4 = std::hash<uint16_t>()(key.core_id_);
            return h1 ^ (h2 << 1) ^ (h3 << 3) ^ (h4 << 5);
#endif
        }
    };

    std::mutex task_limiter_mux_;
    std::unordered_map<TaskLimiterKey,
                       std::shared_ptr<DataSyncTaskLimiter>,
                       LimiterKeyHasher>
        task_limiters_;

    void DataSyncWorker(size_t worker_idx);

    void DataSync(std::unique_lock<std::mutex> &task_worker_lk,
                  size_t worker_idx);

    /**
     * Range & Slice Update Interface
     */

    /**
     * @brief Called before checkpoint to calculate the storage slice size after
     * checkpoint and decide if the slice needs to be updated(merge/split).
     * Update slice info accordingly, but does not update the actual slice size
     * since the data is not flushed yet.
     * Also decide the range update plan based on the number of slices after the
     * slice update.
     */
    bool UpdateSliceAndCalculateRangeUpdate(
        const TableName &table_name,
        const TableSchema *schema,
        NodeGroupId node_group_id,
        int64_t node_group_term,
        std::vector<FlushRecord> &data_sync_vec,
        uint64_t data_sync_ts,
        StoreRange *store_range,
        std::vector<TxKey> &splitting_info);
    /**
     * @brief Worker thread that split the target range and flush the data into
     * data store in their new partitions. This is called during checkpoint on a
     * table, after this function returns, we can assume the splitting ranges
     * are flushed too.
     */
    void SplitFlushRange(const TableName &table_name,
                         const TableSchema *schema,
                         NodeGroupId node_group,
                         TransactionExecution *txm,
                         TableRangeEntry *range_entry,
                         std::vector<TxKey> &&split_keys,
                         std::shared_ptr<DataSyncTask> data_sync_task,
                         std::vector<FlushRecord> &&previous_data_sync_vec,
                         std::vector<FlushRecord> &&previous_archive_vec,
                         std::vector<TxKey> &&previous_mv_base_vec,
                         std::shared_ptr<void> defer_unpin);

    struct UpdateSliceSpecWork
    {
    public:
        UpdateSliceSpecWork(uint32_t node_group_id,
                            int64_t node_group_term,
                            uint64_t data_sync_ts,
                            const TableName &table_name,
                            const TableSchema *schema,
                            const std::vector<FlushRecord> &flush_vec,
                            StoreRange *range,
                            StoreSlice *slice,
                            size_t start_idx,
                            size_t end_idx,
                            std::mutex &sender_mux,
                            std::condition_variable &sender_cv,
                            size_t &finish_work_cnt,
                            bool &fail)
            : node_group_id_(node_group_id),
              node_group_term_(node_group_term),
              data_sync_ts_(data_sync_ts),
              table_name_(table_name),
              table_schema_(schema),
              flush_vec_(flush_vec),
              range_(range),
              slice_(slice),
              start_idx_(start_idx),
              end_idx_(end_idx),
              sender_mux_(sender_mux),
              sender_cv_(sender_cv),
              finish_work_cnt_(finish_work_cnt),
              fail_(fail)
        {
        }

        uint32_t node_group_id_;
        int64_t node_group_term_;
        uint64_t data_sync_ts_;
        TableName table_name_;
        const TableSchema *table_schema_;
        const std::vector<FlushRecord> &flush_vec_;
        StoreRange *range_;
        StoreSlice *slice_;
        size_t start_idx_;
        size_t end_idx_;

        std::mutex &sender_mux_;
        std::condition_variable &sender_cv_;
        // Increased by worker after finishing the retrieved work.
        size_t &finish_work_cnt_;
        // Set by worker to indicate work result
        bool &fail_;
    };
    // Workers for updating slice specs. Since update slice
    // spec would cause potential data store read, we launched
    // workers so we can have some degree of parallelism, but
    // not to the degree where it slows down regular read from data store.
    WorkerThreadContext slice_update_worker_ctx_;
    std::vector<UpdateSliceSpecWork> pending_slice_work_;

    void UpdateSliceSpecWorker();

    /**
     * @brief Called after data sync is done. Update data store slice size
     * in memory and in data store. Reset post ckpt size in store slice. It
     * is expected that flush_batch is in the same range.
     */
    bool UpdateStoreSlice(const TableName &tbl_name,
                          uint64_t schema_ts,
                          NodeGroupId node_group_id,
                          std::vector<FlushRecord> &flush_batch,
                          bool flush_res);

    /**
     * FlushData Operation Interface
     */
    struct FlushDataTask
    {
    public:
        FlushDataTask(std::shared_ptr<DataSyncTask> data_sync_task,
                      const TableSchema *schema,
                      std::unique_ptr<std::vector<FlushRecord>> data_sync_vec,
                      std::unique_ptr<std::vector<FlushRecord>> archive_vec,
                      std::unique_ptr<std::vector<TxKey>> mv_base_vec,
                      TransactionExecution *data_sync_txm,
                      bool delay_update_ckpt_ts,
                      size_t scan_task_worker_idx)
            : node_group_id_(data_sync_task->node_group_id_),
              node_group_term_(data_sync_task->node_group_term_),
              data_sync_ts_(data_sync_task->data_sync_ts_),
              table_name_(data_sync_task->table_name_),
              schema_(schema),
              data_sync_vec_(std::move(data_sync_vec)),
              archive_vec_(std::move(archive_vec)),
              mv_base_vec_(std::move(mv_base_vec)),
              vec_owner_(true),
              delay_update_ckpt_ts_(delay_update_ckpt_ts),
              scan_task_worker_idx_(scan_task_worker_idx),
              data_sync_task_(data_sync_task),
              data_sync_txm_(data_sync_txm),
              hand_res_(nullptr)
        {
        }

        FlushDataTask(uint32_t node_group_id,
                      int64_t node_group_term,
                      uint64_t data_sync_ts,
                      const TableName &table_name,
                      const TableSchema *schema,
                      std::vector<FlushRecord> *data_sync_vec,
                      std::vector<FlushRecord> *archive_vec,
                      std::vector<TxKey> *mv_base_vec,
                      CcHandlerResult<Void> *res,
                      bool delay_update_ckpt_ts)
            : node_group_id_(node_group_id),
              node_group_term_(node_group_term),
              data_sync_ts_(data_sync_ts),
              table_name_(table_name),
              schema_(schema),
              data_sync_vec_ptr_(data_sync_vec),
              archive_vec_ptr_(archive_vec),
              mv_base_vec_ptr_(mv_base_vec),
              vec_owner_(false),
              delay_update_ckpt_ts_(delay_update_ckpt_ts),
              hand_res_(res)
        {
        }

        uint32_t node_group_id_;
        int64_t node_group_term_;
        uint64_t data_sync_ts_;
        TableName table_name_;
        const TableSchema *schema_;
        std::unique_ptr<std::vector<FlushRecord>> data_sync_vec_{nullptr};
        std::unique_ptr<std::vector<FlushRecord>> archive_vec_{nullptr};
        std::unique_ptr<std::vector<TxKey>> mv_base_vec_{nullptr};
        std::vector<FlushRecord> *data_sync_vec_ptr_{nullptr};
        std::vector<FlushRecord> *archive_vec_ptr_{nullptr};
        std::vector<TxKey> *mv_base_vec_ptr_{nullptr};
        bool vec_owner_{true};
        bool delay_update_ckpt_ts_{false};
        size_t scan_task_worker_idx_{0};

        // Increased by worker after finishing the retrieved work.
        std::shared_ptr<DataSyncTask> data_sync_task_{nullptr};
        TransactionExecution *data_sync_txm_{nullptr};
        CcHandlerResult<Void> *hand_res_{nullptr};
    };
    // For flush data work
    WorkerThreadContext flush_data_worker_ctx_;
    // Flush work from data sync, and split range
    std::vector<FlushDataTask> pending_flush_work_;

    void FlushDataWorker();
    void FlushData(std::unique_lock<std::mutex> &flush_worker_lk);

    WorkerThreadContext statistics_worker_ctx_;
    void SyncTableStatisticsWorker();
    /**
     * Generate sk from pk
     */
    std::mutex generate_sk_mux_;
    using RangeGenerateSkStatus = std::unordered_map<int32_t, GenerateSkStatus>;
    using TxGenerateSkStatus =
        std::unordered_map<uint64_t, RangeGenerateSkStatus>;
    std::unordered_map<NodeGroupId, TxGenerateSkStatus> generate_sk_status_;

    WorkerThreadContext defragment_worker_ctx_;
    void DefragmentWorker();

    // For cluster Publish message
    std::function<void(std::string_view, std::string_view)> publish_func_;

    friend class LocalCcHandler;
    friend class remote::RemoteCcHandler;
    friend class Checkpointer;
    friend class txservice::fault::ReplayService;
    friend class CcShard;
};
}  // namespace txservice
