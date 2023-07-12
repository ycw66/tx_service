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
#include "metrics.h"
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
    static const size_t DATA_SYNC_SCAN_BATCH_SIZE = 3 * 1024;
    LocalCcShards(uint32_t node_id = 0,
                  uint16_t core_cnt = 1,
                  uint32_t memory_limit_mb = 1000,
                  uint32_t log_limit_mb = 1000,
                  bool realtime_sampling = false,
                  CatalogFactory *catalog_factory = nullptr,
                  store::DataStoreHandler *store_hd = nullptr,
                  metrics::MetricsRegistry *metrics_registry = nullptr,
                  TxService *tx_service = nullptr,
                  bool enable_mvcc = true);

    LocalCcShards(uint32_t node_id = 0,
                  uint16_t core_cnt = 1,
                  uint32_t memory_limit_mb = 1000,
                  uint32_t log_limit_mb = 1000,
                  bool realtime_sampling = false,
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

    size_t QueueSize(size_t thd_id)
    {
        return cc_shards_[thd_id]->QueueSize();
    };

    bool IsIdle(uint32_t thd_id) const
    {
        return cc_shards_[thd_id]->IsIdle();
    }

    void ProcessorSleepFlag(uint16_t thd_id,
                            std::atomic<bool> *sleep_flag,
                            std::mutex *processor_mux,
                            std::condition_variable *processor_cv)
    {
        return cc_shards_[thd_id]->SetProcessorSleepFlag(
            sleep_flag, processor_mux, processor_cv);
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

    void CreateRemoteStatisticsTx(
        TableName &&table_or_index_name,
        uint64_t schema_version,
        remote::NodeGroupSamplePool &&remote_sample_pool);

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
        int32_t partition_id,
        const TxKey *start_key,
        const TxKey *end_key,
        const RangeInfo *range_info,
        std::vector<std::unique_ptr<TxKey>> &&new_range_key,
        std::vector<int32_t> &&new_partition_ids,
        uint32_t node_group_id,
        uint64_t txn,
        int64_t tx_term,
        uint64_t commit_ts,
        std::optional<std::pair<CcEntryAddr, ReadSetEntry>> catalog_cc_entry,
        std::shared_ptr<std::atomic_uint32_t> split_tx_started);

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
        std::vector<std::tuple<TxKey::Uptr, uint32_t, SliceStatus>>
            *slice_keys = nullptr);
    /**
     * @brief Initialize TableRangeEntry for a table in range_maps_.
     */
    void InitTableRanges(const TableName &range_table_name,
                         std::vector<InitRangeEntry> &init_ranges,
                         const NodeGroupId ng_id,
                         bool fully_cached = false);

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

    const TableRangeEntry *GetTableRangeEntryNonLocking(
        const TableName &table_name, const NodeGroupId ng_id, const TxKey *key);

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
                               RangeSliceOpStatus &pin_status,
                               bool force_load,
                               uint8_t prefetch_size);

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
                               RangeSliceOpStatus &pin_status,
                               bool force_load,
                               uint8_t prefetch_size);

    StoreRange *FindRange(const TableName &table_name,
                          const NodeGroupId ng_id,
                          const TxKey &key);

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
                   CcHandlerResult<Void> &hres);

    void EnqueueDataSyncTask(const TableName &table_name,
                             uint32_t ng_id,
                             int64_t ng_term,
                             uint64_t data_sync_ts,
                             std::mutex *task_sender_mux,
                             std::condition_variable *task_sender_cv,
                             uint16_t *finished_task_cnt,
                             std::atomic_bool *tasks_failed,
                             bool is_forward = false,
                             CcHandlerResult<Void> *hres = nullptr);

    bool SetDataSyncOngoing(const TableName &table_name,
                            NodeGroupId ng_id,
                            bool is_ongoing);

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

    std::shared_ptr<TableSchema> GetSharedTableSchema(
        const TableName &table_name, NodeGroupId ng_id);

    bool KickoutRangeSlice(const TableName &tbl_name,
                           const NodeGroupId ng_id,
                           const TxKey &key);

    std::pair<Statistics *, bool> InitTableStatistics(
        const TableName &table_name, NodeGroupId ng_id);

    std::pair<Statistics *, bool> InitTableStatistics(
        const TableName &table_name,
        const TableSchema *table_schema,
        NodeGroupId ng_id,
        std::unordered_map<TableName,
                           std::pair<uint64_t, std::vector<TxKey::Uptr>>>
            &&sample_pool_map,
        CcShard *ccs);

    StatisticsEntry *GetTableStatistics(const TableName &table_name,
                                        NodeGroupId ng_id);

    void CleanTableStatistics(const TableName &table_name);

    void DropTableStatistics(NodeGroupId ng_id);

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

    std::unordered_map<TableName,
                       std::unordered_map<NodeGroupId, StatisticsEntry>>
        table_statistics_map_;

    // Protects meta data (table_ranges_ and table_catalogs_)
    mutable std::shared_mutex meta_data_mux_;

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

    struct DataSyncTask
    {
    public:
        DataSyncTask(const TableName &table_name,
                     uint32_t ng_id,
                     int64_t ng_term,
                     uint64_t data_sync_ts,
                     std::mutex *task_sender_mux,
                     std::condition_variable *task_sender_cv,
                     uint16_t *finished_task_cnt,
                     std::atomic_bool *tasks_failed,
                     bool is_forward,
                     CcHandlerResult<Void> *hres = nullptr)
            : table_name_(table_name),
              node_group_id_(ng_id),
              ng_leader_term_(ng_term),
              data_sync_ts_(data_sync_ts),
              task_sender_mux_(task_sender_mux),
              task_sender_cv_(task_sender_cv),
              finished_task_cnt_(finished_task_cnt),
              tasks_failed_(tasks_failed),
              is_forward_(is_forward),
              task_res_(hres)
        {
        }

        bool SetFinish()
        {
            if (unfinished_worker_.fetch_sub(1, std::memory_order_release) == 1)
            {
                // Notify the caller that the task finished.
                if (task_sender_mux_ != nullptr &&
                    finished_task_cnt_ != nullptr)
                {
                    std::unique_lock<std::mutex> task_sender_lk(
                        *task_sender_mux_);
                    ++(*finished_task_cnt_);
                    if (sync_task_failed_.load(std::memory_order_relaxed))
                    {
                        bool fail = false;
                        tasks_failed_->compare_exchange_strong(fail, true);
                    }
                    task_sender_cv_->notify_one();
                }

                if (task_res_ != nullptr)
                {
                    if (sync_task_failed_.load(std::memory_order_relaxed))
                    {
                        task_res_->SetError(CcErrorCode::DATA_STORE_ERR);
                    }
                    else
                    {
                        task_res_->SetFinished();
                    }
                }
                return true;
            }
            return false;
        }

        bool SetError(CcErrorCode err_code = CcErrorCode::DATA_STORE_ERR)
        {
            bool fail = false;
            sync_task_failed_.compare_exchange_strong(fail, true);
            if (unfinished_worker_.fetch_sub(1, std::memory_order_release) == 1)
            {
                // Notify the caller that the task finished.
                if (task_sender_mux_ != nullptr &&
                    finished_task_cnt_ != nullptr)
                {
                    std::unique_lock<std::mutex> task_sender_lk(
                        *task_sender_mux_);
                    tasks_failed_->compare_exchange_strong(fail, true);
                    ++(*finished_task_cnt_);
                    task_sender_cv_->notify_one();
                }

                if (task_res_ != nullptr)
                {
                    task_res_->SetError(err_code);
                }
                return true;
            }
            return false;
        }

        bool IsError()
        {
            return sync_task_failed_.load(std::memory_order_relaxed);
        }

        const TableName &table_name_;
        uint32_t node_group_id_;
        int64_t ng_leader_term_{-1};
        uint64_t data_sync_ts_{0};
        // Used to protect and synchronize the task status between task_worker
        // and task_sender.
        std::mutex *task_sender_mux_{nullptr};
        std::condition_variable *task_sender_cv_{nullptr};
        uint16_t *finished_task_cnt_{nullptr};
        // Set by range split worker and flush data worker to indicate data
        // sync task result.
        std::atomic_bool *tasks_failed_{nullptr};
        std::atomic_bool sync_task_failed_{false};
        // True if need to use the dirty schema..
        bool is_forward_{false};
        // Indicate the single task result.
        CcHandlerResult<Void> *task_res_{nullptr};
        std::atomic_uint16_t unfinished_worker_{1};
    };

    struct TableDataSyncStatus
    {
        bool is_ongoing_{false};
        uint64_t last_sync_ts_{0};
        // Multiple tasks on the same table are executed sequentially, so the
        // subsequence tasks for this table should wait here.
        std::vector<std::shared_ptr<DataSyncTask>> pending_task_;
    };

    // Protect data_sync_task_queue_ and tables_sync_status_
    std::mutex task_worker_mux_;
    std::condition_variable task_worker_cv_;
    std::deque<std::shared_ptr<DataSyncTask>> data_sync_task_queue_;
    std::vector<std::thread> data_sync_worker_thds_;
    const int data_sync_worker_num_;
    std::unordered_map<TableName,
                       std::unordered_map<NodeGroupId, TableDataSyncStatus>>
        tables_sync_status_;
    WorkerStatus data_sync_worker_status_;

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
        std::vector<FlushRecord> &data_sync_vec,
        uint64_t data_sync_ts,
        size_t &batch_idx,
        std::pair<const StoreRange *, std::vector<const TxKey *>>
            &splitting_info);
    /**
     * @brief Worker thread that split the target range and flush the data into
     * data store in their new partitions. This is called during checkpoint on a
     * table, after this function returns, we can assume the splitting ranges
     * are flushed too.
     */
    void SplitFlushRange(
        const TableName &table_name,
        NodeGroupId node_group,
        bool is_forward,
        std::pair<const StoreRange *, std::vector<const TxKey *>> split_info,
        std::shared_ptr<DataSyncTask> data_sync_task);

    /**
     * @brief Given a vector of checkpoint records and splitting ranges, moves
     * the checkpoint records not in the splitting ranges into a new vector.
     *
     * @param flush_vec A vector of checkpoint records
     * @param non_split_vec The new vector for checkpoint records not falling
     * into splitting ranges
     * @param split_ranges Ranges to be split
     * @param lower_bound_cmp comapre func of type T and const TxKey *
     */
    template <typename T, class Compare>
    void MoveNonSplittingRecords(
        std::vector<T> &flush_vec,
        std::vector<T> &non_split_vec,
        const std::vector<std::pair<const TxKey *, const TxKey *>>
            &split_ranges,
        Compare lower_bound_cmp);

    struct UpdateSliceSpecWork
    {
    public:
        UpdateSliceSpecWork(uint32_t node_group,
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
            : node_group_(node_group),
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

        uint32_t node_group_;
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
    std::mutex slice_update_mux_;
    std::condition_variable slice_update_cv_;
    std::vector<UpdateSliceSpecWork> pending_slice_work_;
    std::vector<std::thread> update_slice_spec_thds_;
    const int slice_worker_num_;
    WorkerStatus slice_thd_status_;

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
                      TransactionExecution *data_sync_txm)
            : node_group_(data_sync_task->node_group_id_),
              ng_leader_term_(data_sync_task->ng_leader_term_),
              data_sync_ts_(data_sync_task->data_sync_ts_),
              table_name_(data_sync_task->table_name_),
              schema_(schema),
              data_sync_vec_(std::move(data_sync_vec)),
              archive_vec_(std::move(archive_vec)),
              mv_base_vec_(std::move(mv_base_vec)),
              vec_owner_(true),
              data_sync_task_(data_sync_task),
              data_sync_txm_(data_sync_txm),
              hand_res_(nullptr)
        {
        }

        FlushDataWork(uint32_t node_group,
                      int64_t term,
                      uint64_t data_sync_ts,
                      const TableName &table_name,
                      const TableSchema *schema,
                      std::vector<FlushRecord> *data_sync_vec,
                      std::vector<FlushRecord> *archive_vec,
                      std::vector<const TxKey *> *mv_base_vec,
                      CcHandlerResult<Void> *res)
            : node_group_(node_group),
              ng_leader_term_(term),
              data_sync_ts_(data_sync_ts),
              table_name_(table_name),
              schema_(schema),
              data_sync_vec_ptr_(data_sync_vec),
              archive_vec_ptr_(archive_vec),
              mv_base_vec_ptr_(mv_base_vec),
              vec_owner_(false),
              hand_res_(res)
        {
        }

        uint32_t node_group_;
        int64_t ng_leader_term_;
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

        // Increased by worker after finishing the retrieved work.
        std::shared_ptr<DataSyncTask> data_sync_task_{nullptr};
        TransactionExecution *data_sync_txm_{nullptr};
        CcHandlerResult<Void> *hand_res_{nullptr};
    };
    // For flush data work
    std::mutex flush_worker_mux_;
    std::condition_variable flush_worker_cv_;
    // Flush work from data sync, and split range
    std::vector<FlushDataWork> pending_flush_work_;
    std::vector<std::thread> flush_worker_thds_;
    const int flush_worker_num_;
    WorkerStatus flush_worker_thd_status_;

    void FlushDataWorker();
    void FlushData(std::unique_lock<std::mutex> &flush_worker_lk);

    friend class LocalCcHandler;
    friend class remote::RemoteCcHandler;
    friend class Checkpointer;
    friend class txservice::fault::ReplayService;
    friend class CcShard;
};
}  // namespace txservice
