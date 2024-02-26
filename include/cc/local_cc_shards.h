#pragma once

#include <algorithm>
#include <atomic>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "catalog.h"
#include "catalog_factory.h"
#include "catalog_key_record.h"
#include "cc_shard.h"
#include "local_cc_handler.h"
#include "raft_log.pb.h"
#include "range_record.h"
#include "store/data_store_handler.h"
#include "system_handler.h"
#include "tx_service_common.h"
#include "tx_service_metrics.h"
#include "type.h"

namespace txservice
{
namespace remote
{
class RemoteCcHandler;
};
class Checkpointer;
class TxService;

struct DataSyncStatus
{
    DataSyncStatus() = default;

    uint32_t unfinished_tasks_{0};
    bool all_task_started_{false};
    CcErrorCode err_code_{CcErrorCode::NO_ERROR};
    std::mutex mux_;
    std::condition_variable cv_;
};
struct DataSyncTask
{
public:
    DataSyncTask(const TableName &table_name,
                 int32_t range_id,
                 uint64_t range_version,
                 uint32_t ng_id,
                 int64_t ng_term,
                 uint64_t data_sync_ts,
                 std::shared_ptr<DataSyncStatus> status,
                 bool need_truncate_log,
                 bool is_dirty,
                 std::function<void(std::shared_ptr<DataSyncTask> task)>
                     on_remove_pending_queue_lambda,
                 CcHandlerResult<Void> *hres = nullptr)
        : table_name_(table_name),
          range_id_(range_id),
          range_version_(range_version),
          node_group_id_(ng_id),
          node_group_term_(ng_term),
          data_sync_ts_(data_sync_ts),
          status_(status),
          need_truncate_log_(need_truncate_log),
          is_dirty_(is_dirty),
          on_remove_pending_queue_lambda_(on_remove_pending_queue_lambda),
          task_res_(hres)
    {
    }

    void SetFinish()
    {
        std::unique_lock<std::mutex> task_sender_lk(status_->mux_);
        status_->unfinished_tasks_--;
        if (status_->unfinished_tasks_ == 0 && status_->all_task_started_)
        {
            if (need_truncate_log_ &&
                status_->err_code_ == CcErrorCode::NO_ERROR)
            {
                // Truncate redo log
                LOG(INFO) << "Checkpoint of node group #" << node_group_id_
                          << " succeeded with timestamp: " << data_sync_ts_;
                Sharder::Instance().UpdateNodeGroupCkptTs(node_group_id_,
                                                          data_sync_ts_);
                Sharder::Instance().GetLogAgent()->UpdateCheckpointTs(
                    node_group_id_, node_group_term_, data_sync_ts_);
            }

            if (task_res_)
            {
                if (status_->err_code_ == CcErrorCode::NO_ERROR)
                {
                    task_res_->SetFinished();
                }
                else
                {
                    task_res_->SetError(status_->err_code_);
                }
            }
            status_->cv_.notify_all();
        }
    }
    void SetError(CcErrorCode err_code = CcErrorCode::DATA_STORE_ERR)
    {
        std::unique_lock<std::mutex> task_sender_lk(status_->mux_);
        status_->unfinished_tasks_--;
        status_->err_code_ = err_code;
        if (status_->unfinished_tasks_ == 0 && status_->all_task_started_)
        {
            if (task_res_)
            {
                task_res_->SetError(status_->err_code_);
            }
            status_->cv_.notify_all();
        }
    }
    void SetErrorCode(CcErrorCode err_code)
    {
        std::unique_lock<std::mutex> lk(status_->mux_);
        status_->err_code_ = err_code;
    }

    const TableName table_name_;
    int32_t range_id_;
    uint64_t range_version_;
    uint32_t node_group_id_;
    int64_t node_group_term_{-1};
    uint64_t data_sync_ts_{0};

#ifndef RANGE_PARTITION_ENABLED
    enum class CkptErrorCode
    {
        NO_ERROR = 0,
        // Failed on data sync scan
        SCAN_ERROR,
        // Failed on flush data
        FLUSH_ERROR,
    };

    std::mutex flight_task_mux_;
    // Flush data task cnt + 1 (Data sync task)
    int64_t flight_task_cnt_{0};
    CkptErrorCode ckpt_err_{CkptErrorCode::NO_ERROR};
#endif

    std::shared_ptr<DataSyncStatus> status_{nullptr};
    // True if need to truncate redo log when all tasks succeed.
    bool need_truncate_log_{true};
    // True if need to use the dirty schema.
    bool is_dirty_{false};
    std::function<void(std::shared_ptr<DataSyncTask> task)>
        on_remove_pending_queue_lambda_;
    // Indicate the single task result.
    CcHandlerResult<Void> *task_res_{nullptr};
};

struct DataMigrationStatus
{
public:
    DataMigrationStatus(TxNumber cluster_scale_txn,
                        std::vector<uint16_t> &&bucket_ids,
                        std::vector<NodeGroupId> &&new_owner_ngs,
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
    std::vector<uint16_t> bucket_ids_;
    std::vector<NodeGroupId> new_owner_ngs_;
    std::vector<TxNumber> migration_txns_;
    std::atomic_size_t next_bucket_idx_;
    std::atomic_size_t unfinished_worker_;
};
class LocalCcShards
{
public:
    static const size_t DATA_SYNC_SCAN_BATCH_SIZE = 3 * 1024;

    LocalCcShards(uint32_t node_id,                 // = 0,
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
                  metrics::CommonLabels common_labels = {});

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

    void EnqueueToCcShard(uint16_t cc_shard_idx, CcRequestBase *req)
    {
        assert(cc_shard_idx < cc_shards_.size());
        auto &ccs = cc_shards_[cc_shard_idx];
        ccs->Enqueue(req);
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
        const TxKey *start_key,
        const TxKey *end_key,
        const RangeInfo *range_info,
        std::vector<std::unique_ptr<TxKey>> &&new_range_key,
        std::vector<int32_t> &&new_partition_ids,
        uint32_t node_group_id,
        int64_t tx_term);

    /**
     * @brief Create a new table range entry and fill current range info with
     * given partition id and start key.
     */
    const TableRangeEntry *CreateTableRange(
        const TableName &table_name,
        const NodeGroupId ng_id,
        int32_t partition_id,
        TxKey::Uptr start_key,
        uint64_t version,
        std::vector<std::tuple<TxKey::Uptr, uint32_t, SliceStatus>>
            *slice_keys = nullptr);
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
                                        const TxKey *key);

    const TableRangeEntry *GetTableRangeEntry(const TableName &table_name,
                                              const NodeGroupId ng_id,
                                              int32_t range_id);

    const TableRangeEntry *GetTableRangeEntryNoLocking(
        const TableName &table_name, const NodeGroupId ng_id, const TxKey *key);

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
                               CcShard *cc_shard,
                               RangeSliceOpStatus &pin_status,
                               bool force_load,
                               uint8_t prefetch_size);

    RangeSliceId PinRangeSlices(const TableName &table_name,
                                NodeGroupId cc_ng_id,
                                int64_t cc_ng_term,
                                const Schema *key_schema,
                                const Schema *rec_schema,
                                uint64_t schema_ts,
                                const KVCatalogInfo *kv_info,
                                uint32_t range_id,
                                const TxKey &start_key,
                                bool start_inclusive,
                                const TxKey *end_key,
                                bool end_inclusive,
                                CcRequestBase *cc_request,
                                CcShard *cc_shard,
                                bool force_load,
                                uint8_t prefetch_size,
                                uint8_t max_pin_cnt,
                                bool forward_pin,
                                RangeSliceOpStatus &pin_status,
                                const StoreSlice *&last_pinned_slice);

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
                   std::vector<const TxKey *> *mv_vec,
                   CcHandlerResult<Void> &hres,
                   bool delay_update_ckpt_ts);

    void EnqueueDataSyncTaskForTable(
        const TableName &table_name,
        uint32_t ng_id,
        int64_t ng_term,
        uint64_t data_sync_ts,
        bool need_truncate_log = true,
        bool is_dirty = false,
        std::shared_ptr<DataSyncStatus> status = nullptr,
        CcHandlerResult<Void> *hres = nullptr);

    bool IsDataSyncQueueEmpty()
    {
        std::unique_lock<std::mutex> lk(data_sync_worker_ctx_.mux_);
        return data_sync_task_queue_.empty();
    }

    size_t DecreaseRangeSliceMemUsage(size_t size)
    {
        size_t old_size =
            range_slice_mem_usage_.fetch_sub(size, std::memory_order_relaxed);
        if (old_size < size)
        {
            // The sub has caused overflow, in this case just reset usage to
            // 0.
            range_slice_mem_usage_.store(0, std::memory_order_release);
            return 0;
        }
        return old_size - size;
    }

    size_t IncreaseRangeSliceMemUsage(size_t size)
    {
        return range_slice_mem_usage_.fetch_add(size,
                                                std::memory_order_relaxed) +
               size;
    }

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

    bool KickoutKeyInSlice(const TableName &tbl_name,
                           const NodeGroupId ng_id,
                           const TxKey &key);

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
        std::unordered_map<TableName,
                           std::pair<uint64_t, std::vector<TxKey::Uptr>>>
            sample_pool_map,
        CcShard *ccs);

    StatisticsEntry *GetTableStatistics(const TableName &table_name,
                                        NodeGroupId ng_id);

    void CleanTableStatistics(const TableName &table_name);

    void DropTableStatistics(NodeGroupId ng_id);

    const BucketInfo *GetBucketInfo(const uint16_t bucket_id,
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
        const std::unordered_map<TableName, std::unordered_set<int32_t>>
            &ranges_in_bucket_snapshot,
        uint32_t ng_id,
        int64_t ng_term,
        uint64_t data_sync_ts,
        CcHandlerResult<Void> *hres);

    /**
     * @brief Generate bucket migration plan based on the new node group config.
     */
    std::unordered_map<NodeGroupId, BucketMigrateInfo>
    GenerateBucketMigrationPlan(uint32_t new_ng_count, int32_t seed);
    // Memory limit of heap memory allocated by range slices info.
    // 5% of the total memory limit.
    const uint64_t range_slice_memory_limit_;
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
    std::unique_ptr<ClusterScaleOp> cluster_scale_op_{nullptr};
    std::mutex cluster_scale_op_mux_;

    std::mutex data_migration_op_pool_mux_;
    std::vector<std::unique_ptr<DataMigrationOp>> migration_op_pool_;

private:
    void TimerRun();
    // Internal interface that exposes non const return type and does
    // not acquire mutex lock.
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

    bool EnqueueDataSyncTask(const TableName &table_name,
                             uint32_t ng_id,
                             int64_t ng_term,
                             const TableRangeEntry *range_entry,
                             uint64_t data_sync_ts,
                             bool need_truncate_log,
                             bool is_dirty,
                             std::shared_ptr<DataSyncStatus> status,
                             CcHandlerResult<Void> *hres);
#ifndef RANGE_PARTITION_ENABLED
    void PostProcessDataSyncTask(std::shared_ptr<DataSyncTask> task,
                                 TransactionExecution *data_sync_txm,
                                 CatalogEntry *catalog_entry,
                                 DataSyncTask::CkptErrorCode ckpt_err);
#endif

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

    std::unordered_map<TableName,
                       std::unordered_map<NodeGroupId, StatisticsEntry>>
        table_statistics_map_;

    // map to store mapping relationship from bucket id to bucket info
    // that stores the bucket owner of this bucket.
    std::unordered_map<
        NodeGroupId,
        std::unordered_map<uint16_t, std::unique_ptr<BucketInfo>>>
        bucket_infos_;

    // Protects meta data (table_ranges_ and table_catalogs_)
    mutable std::shared_mutex meta_data_mux_;

    // Memory used by range slices
    std::atomic_size_t range_slice_mem_usage_{0};

    TxService *tx_service_;

    bool enable_mvcc_;

    bool realtime_sampling_;

    /**
     * DataSync Operation Interface
     */
    enum struct WorkerStatus
    {
        Active,
        Terminating,
        Terminated
    };

    struct WorkerThreadContext
    {
        WorkerThreadContext(int worker_num)
            : worker_num_(worker_num), status_(WorkerStatus::Active)
        {
        }

        void Terminate()
        {
            {
                std::unique_lock<std::mutex> lk(mux_);
                assert(status_ == WorkerStatus::Active);
                status_ = WorkerStatus::Terminated;
                cv_.notify_all();
            }

            // loop over worker threads and join them
            for (int i = 0; i < worker_num_; i++)
            {
                worker_thd_[i].join();
            }
        }
        const int worker_num_;
        std::vector<std::thread> worker_thd_;
        std::mutex mux_;
        std::condition_variable cv_;
        WorkerStatus status_;
    };

    WorkerThreadContext data_sync_worker_ctx_;
    std::deque<std::shared_ptr<DataSyncTask>> data_sync_task_queue_;

    void DataSyncWorker();

    void DataSync(std::unique_lock<std::mutex> &task_worker_lk);

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
        std::vector<const TxKey *> &splitting_info);
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
                         std::vector<const TxKey *> &&split_keys,
                         std::shared_ptr<DataSyncTask> data_sync_task,
                         std::vector<FlushRecord> &&previous_data_sync_vec,
                         std::vector<FlushRecord> &&previous_archive_vec,
                         std::vector<const TxKey *> &&previous_mv_base_vec,
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
     * in memory and in data store. Reset post ckpt size in store slice.
     */
    bool UpdateStoreSlice(const TableName &tbl_name,
                          uint64_t schema_ts,
                          NodeGroupId node_group_id,
                          std::vector<FlushRecord> &flush_batch,
                          bool flush_res);

    /**
     * FlushData Operation Interface
     */
    struct FlushDataWork
    {
    public:
        FlushDataWork(std::shared_ptr<DataSyncTask> data_sync_task,
                      const TableSchema *schema,
                      std::unique_ptr<std::vector<FlushRecord>> &&data_sync_vec,
                      std::unique_ptr<std::vector<FlushRecord>> &&archive_vec,
                      std::unique_ptr<std::vector<const TxKey *>> &&mv_base_vec,
                      TransactionExecution *data_sync_txm,
                      bool delay_update_ckpt_ts)
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
              data_sync_task_(data_sync_task),
              data_sync_txm_(data_sync_txm),
              hand_res_(nullptr)
        {
        }

        FlushDataWork(uint32_t node_group_id,
                      int64_t node_group_term,
                      uint64_t data_sync_ts,
                      const TableName &table_name,
                      const TableSchema *schema,
                      std::vector<FlushRecord> *data_sync_vec,
                      std::vector<FlushRecord> *archive_vec,
                      std::vector<const TxKey *> *mv_base_vec,
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
        std::unique_ptr<std::vector<const TxKey *>> mv_base_vec_{nullptr};
        std::vector<FlushRecord> *data_sync_vec_ptr_{nullptr};
        std::vector<FlushRecord> *archive_vec_ptr_{nullptr};
        std::vector<const TxKey *> *mv_base_vec_ptr_{nullptr};
        bool vec_owner_{true};
        bool delay_update_ckpt_ts_{false};

        // Increased by worker after finishing the retrieved work.
        std::shared_ptr<DataSyncTask> data_sync_task_{nullptr};
        TransactionExecution *data_sync_txm_{nullptr};
        CcHandlerResult<Void> *hand_res_{nullptr};
    };
    // For flush data work
    WorkerThreadContext flush_data_worker_ctx_;
    // Flush work from data sync, and split range
    std::vector<FlushDataWork> pending_flush_work_;

    void FlushDataWorker();
    void FlushData(std::unique_lock<std::mutex> &flush_worker_lk);

    WorkerThreadContext statistics_worker_ctx_;
    void SyncTableStatisticsWorker();

    WorkerThreadContext defragment_worker_ctx_;
    void DefragmentWorker();

    friend class LocalCcHandler;
    friend class remote::RemoteCcHandler;
    friend class Checkpointer;
    friend class txservice::fault::ReplayService;
    friend class CcShard;
};
}  // namespace txservice
