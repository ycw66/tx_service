#include "cc/local_cc_shards.h"

#include <butil/time.h>
#include <sys/stat.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <thread>
#include <tuple>
#include <unordered_map>

#include "catalog_key_record.h"
#include "cc_node_service.h"
#include "cc_request.h"
#include "cc_request.pb.h"
#include "error_messages.h"
#include "range_bucket_key_record.h"
#include "range_record.h"
#include "range_slice.h"
#include "sharder.h"
#include "sk_generator.h"
#include "store/data_store_handler.h"
#include "tx_execution.h"
#include "tx_key.h"
#include "tx_service.h"
#include "tx_util.h"
#include "type.h"

namespace txservice
{
std::atomic<uint64_t> LocalCcShards::local_clock(0);

LocalCcShards::LocalCcShards(
    uint32_t node_id,
    uint16_t core_cnt,
    uint16_t range_split_worker_cnt,
    uint32_t memory_limit_mb,
    uint32_t log_limit_mb,
    bool realtime_sampling,
    CatalogFactory *catalog_factory,
    SystemHandler *system_handler,
    std::unordered_map<uint32_t, std::vector<NodeConfig>> *ng_configs,
    int32_t range_bucket_seed,
    uint64_t cluster_config_version,
    store::DataStoreHandler *store_hd,
    TxService *tx_service,
    bool enable_mvcc,
    metrics::MetricsRegistry *metrics_registry,
    metrics::CommonLabels common_labels,
    std::unordered_map<TableName, std::string> *prebuilt_tables,
    std::function<void(std::string_view, std::string_view)> publish_func,
    bool enable_shard_heap_defragment,
    bool enable_key_cache)
    : range_slice_memory_limit_(
          ((uint64_t) MB(memory_limit_mb)) /
          ((enable_key_cache && !enable_mvcc)
               ? 10
               : 20)),  // If key cache is included in range slice mem use 10%,
                        // otherwise 5%
      store_hd_(store_hd),
      node_id_(node_id),
      timer_terminate_(false),
      is_waiting_ckpt_(false),
      catalog_factory_(catalog_factory),
      system_handler_(system_handler),
      tx_service_(tx_service),
      enable_mvcc_(enable_mvcc),
      realtime_sampling_(realtime_sampling),

#ifdef RANGE_PARTITION_ENABLED
#ifdef EXT_TX_PROC_ENABLED
      range_split_worker_ctx_(range_split_worker_cnt > 0
                                  ? range_split_worker_cnt
                                  : (core_cnt >= 2 ? (core_cnt / 2) : 1)),
#else
      range_split_worker_ctx_(
          range_split_worker_cnt > 0 ? range_split_worker_cnt : core_cnt),
#endif
#endif

#ifdef EXT_TX_PROC_ENABLED
#ifdef RANGE_PARTITION_ENABLED
      data_sync_worker_ctx_(core_cnt >= 2 ? (core_cnt / 2) : 1),
#else
      data_sync_worker_ctx_(core_cnt),
#endif
      slice_update_worker_ctx_(core_cnt),
      flush_data_worker_ctx_(core_cnt >= 2 ? std::min(core_cnt / 2, 10) : 1),
#else
      data_sync_worker_ctx_(core_cnt),
      slice_update_worker_ctx_(core_cnt * 2),
      flush_data_worker_ctx_(std::min(static_cast<int>(core_cnt), 10)),
#endif
      statistics_worker_ctx_(1),
      publish_func_(publish_func),
      enable_shard_heap_defragment_(enable_shard_heap_defragment)
{
    using namespace std::chrono_literals;
    uint64_t ts_base = std::chrono::duration_cast<std::chrono::microseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count();
    ts_base_.store(ts_base);
    local_clock.store(ts_base);
    timer_thd_ = std::thread([this] { TimerRun(); });

    // For mariadb, this thread is the main thread of the mariadb process.
    InitializeTableRangesHeap();

    InitRangeBuckets(
        node_id, ng_configs->size(), cluster_config_version, range_bucket_seed);

    if (prebuilt_tables)
    {
        for (auto &[table, image] : *prebuilt_tables)
        {
            auto ins_res = prebuilt_tables_.try_emplace(table, image);
            assert(ins_res.second);
            (void) ins_res;
        }

        InitPrebuiltTables(node_id);
    }
    for (uint16_t thd_idx = 0; thd_idx < core_cnt; ++thd_idx)
    {
        common_labels["core_id"] = std::to_string(thd_idx);
        cc_shards_.emplace_back(
            std::make_unique<CcShard>(thd_idx,
                                      core_cnt,
                                      memory_limit_mb,
                                      log_limit_mb,
                                      realtime_sampling,
                                      node_id,
                                      *this,
                                      catalog_factory_,
                                      system_handler,
                                      cluster_config_version,
                                      metrics_registry,
                                      common_labels));
    }
}

LocalCcShards::~LocalCcShards()
{
    {
        std::scoped_lock<std::mutex> lk(timer_terminate_mux_);
        timer_terminate_ = true;
        timer_terminate_cv_.notify_one();
    }
    timer_thd_.join();
    cc_shards_.clear();
}

void LocalCcShards::StartBackgroudWorkers()
{
    // Starts flush worker threads firstly.
    for (int id = 0; id < flush_data_worker_ctx_.worker_num_; id++)
    {
        flush_data_worker_ctx_.worker_thd_.push_back(
            std::thread([this] { FlushDataWorker(); }));
    }

#ifdef RANGE_PARTITION_ENABLED
    // Starts slice update worker threads.
    for (int id = 0; id < slice_update_worker_ctx_.worker_num_; id++)
    {
        slice_update_worker_ctx_.worker_thd_.push_back(
            std::thread([this] { UpdateSliceSpecWorker(); }));
    }

    LOG(INFO) << "Range Split Worker Num: "
              << range_split_worker_ctx_.worker_num_;
    for (int id = 0; id < range_split_worker_ctx_.worker_num_; id++)
    {
        range_split_worker_ctx_.worker_thd_.push_back(
            std::thread([this] { RangeSplitWorker(); }));
    }
#endif

#ifndef RANGE_PARTITION_ENABLED
    data_sync_task_queue_.resize(data_sync_worker_ctx_.worker_num_);
#endif

    // Starts datasync worker threads.
    for (int id = 0; id < data_sync_worker_ctx_.worker_num_; id++)
    {
        data_sync_worker_ctx_.worker_thd_.push_back(
            std::thread([this, id] { DataSyncWorker(id); }));
    }

    if (realtime_sampling_)
    {
        statistics_worker_ctx_.worker_thd_.push_back(
            std::thread([this] { SyncTableStatisticsWorker(); }));
    }
}

uint64_t LocalCcShards::ClockTs()
{
    return LocalCcShards::local_clock.load(std::memory_order_relaxed);
}

uint64_t LocalCcShards::TsBase()
{
    return ts_base_.load(std::memory_order_relaxed);
}

void LocalCcShards::UpdateTsBase(uint64_t timestamp)
{
    uint64_t tsb = ts_base_.load(std::memory_order_relaxed);
    // Update ts_base_ only if new timestamp is bigger. If the CAS fails, tsb
    // will be set to the actual value of ts_base_, keep retrying until success
    // or ts_base_ is already bigger than timestamp.
    while (timestamp > tsb && !ts_base_.compare_exchange_strong(tsb, timestamp))
    {
        // If the CAS fails, since timestamps always roll forward, the ts base
        // must be greater than the current time or the old ts base.
        // ts_base_ can also be updated when calculating commit timestamp, which
        // results in system clock could be smaller than ts_base_. In this case
        // we should not update ts_base_.
    }
}

void LocalCcShards::TimerRun()
{
    std::unique_lock<std::mutex> lk(timer_terminate_mux_);
    do
    {
        using namespace std::chrono_literals;

        uint64_t clock_ts =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count();
        LocalCcShards::local_clock.store(clock_ts, std::memory_order_relaxed);
        UpdateTsBase(clock_ts);

        timer_terminate_cv_.wait_for(
            lk, 1s, [this]() { return timer_terminate_ == true; });
    } while (!timer_terminate_);
}

std::pair<bool, const CatalogEntry *> LocalCcShards::CreateCatalog(
    const TableName &table_name,
    NodeGroupId cc_ng_id,
    const std::string &catalog_image,
    uint64_t commit_ts)
{
    assert(table_name.Type() == TableType::Primary);
    std::unique_lock<std::shared_mutex> lk(meta_data_mux_);

    auto ng_catalog_it = table_catalogs_.try_emplace(table_name);
    auto catalog_it = ng_catalog_it.first->second.try_emplace(cc_ng_id);
    CatalogEntry &catalog_entry = catalog_it.first->second;

    if (catalog_it.second)
    {
        // A new catalog entry is created in LocalCcShards.
        catalog_entry.InitSchema(
            catalog_image.empty() ? nullptr
                                  : catalog_factory_->CreateTableSchema(
                                        table_name, catalog_image, commit_ts),
            commit_ts);
    }
    else
    {
        // If the input schema version is greater than the existing one,
        // replaces the existing scheme with the new one.
        if (catalog_entry.Version() < commit_ts)
        {
            catalog_entry.InitSchema(
                catalog_image.empty()
                    ? nullptr
                    : catalog_factory_->CreateTableSchema(
                          table_name, catalog_image, commit_ts),
                commit_ts);
        }
        else if (catalog_entry.Version() == commit_ts)
        {
            // It is kv_store_failure and is restoring old schema, treat as
            // create successfully.
        }
        else
        {
            return {false, &catalog_entry};
        }
    }

    return {true, &catalog_entry};
}

std::pair<bool, const CatalogEntry *> LocalCcShards::CreateReplayCatalog(
    const TableName &table_name,
    NodeGroupId cc_ng_id,
    const std::string &old_catalog_image,
    const std::string &new_catalog_image,
    uint64_t old_schema_ts,
    uint64_t dirty_schema_ts)
{
    std::unique_lock<std::shared_mutex> lk(meta_data_mux_);

    auto ng_catalog_it = table_catalogs_.try_emplace(table_name);
    auto catalog_it = ng_catalog_it.first->second.try_emplace(cc_ng_id);
    CatalogEntry &catalog_entry = catalog_it.first->second;

    if (catalog_it.second || catalog_entry.Version() == 0)
    {
        // If catalog entry is not initialized yet, use the old schema image
        // stored in prepare log to restore old schema.
        catalog_entry.InitSchema(
            old_catalog_image.empty()
                ? nullptr
                : catalog_factory_->CreateTableSchema(
                      table_name, old_catalog_image, old_schema_ts),
            old_schema_ts);
    }

    if (catalog_entry.Version() < dirty_schema_ts &&
        catalog_entry.DirtyVersion() < dirty_schema_ts)
    {
        // For idempotency, only installs the dirty version when the input ts is
        // greater than the existing version and dirty version.
        catalog_entry.SetDirtySchema(
            new_catalog_image.empty()
                ? nullptr
                : catalog_factory_->CreateTableSchema(
                      table_name, new_catalog_image, dirty_schema_ts),
            dirty_schema_ts);
        return {true, &catalog_entry};
    }
    else if (catalog_entry.Version() == old_schema_ts &&
             catalog_entry.DirtyVersion() == dirty_schema_ts)
    {
        // Rerun ReplayLogCc req.
        return {true, &catalog_entry};
    }
    else if (dirty_schema_ts == 0)
    {
        // It is kv_store_failure and is restoring old schema, treat as
        // create successfully.
        return {true, &catalog_entry};
    }
    else
    {
        return {false, &catalog_entry};
    }
}

CatalogEntry *LocalCcShards::CreateDirtyCatalog(
    const TableName &table_name,
    NodeGroupId cc_ng_id,
    const std::string &catalog_image,
    uint64_t commit_ts)
{
    std::unique_lock<std::shared_mutex> lk(meta_data_mux_);

    auto ng_catalog_it = table_catalogs_.try_emplace(table_name);
    auto catalog_it = ng_catalog_it.first->second.try_emplace(cc_ng_id);
    CatalogEntry &catalog_entry = catalog_it.first->second;

    if (catalog_entry.Version() < commit_ts &&
        catalog_entry.DirtyVersion() < commit_ts)
    {
        // For idempotency, only installs the dirty version when the input ts is
        // greater than the existing version and dirty version.
        catalog_entry.SetDirtySchema(
            catalog_image.empty() ? nullptr
                                  : catalog_factory_->CreateTableSchema(
                                        table_name, catalog_image, commit_ts),
            commit_ts);
    }

    return &catalog_entry;
}

void LocalCcShards::CommitDirtyCatalog(const TableName &table_name,
                                       NodeGroupId cc_ng_id)
{
    std::shared_lock<std::shared_mutex> lk(meta_data_mux_);

    auto ng_catalog_it = table_catalogs_.find(table_name);
    if (ng_catalog_it == table_catalogs_.end())
    {
        return;
    }

    auto catalog_it = ng_catalog_it->second.find(cc_ng_id);
    if (catalog_it == ng_catalog_it->second.end())
    {
        return;
    }

    CatalogEntry &catalog_entry = catalog_it->second;
    catalog_entry.CommitDirtySchema();

    return;
}

CatalogEntry *LocalCcShards::GetCatalog(const TableName &table_name,
                                        NodeGroupId cc_ng_id)
{
    std::shared_lock<std::shared_mutex> lk(meta_data_mux_);

    auto ng_catalog_it = table_catalogs_.find(table_name);
    if (ng_catalog_it == table_catalogs_.end())
    {
        return nullptr;
    }

    auto catalog_it = ng_catalog_it->second.find(cc_ng_id);
    return catalog_it == ng_catalog_it->second.end() ? nullptr
                                                     : &catalog_it->second;
}

CatalogEntry *LocalCcShards::GetCatalogInternal(const TableName &table_name,
                                                NodeGroupId cc_ng_id)
{
    auto ng_catalog_it = table_catalogs_.find(table_name);
    if (ng_catalog_it == table_catalogs_.end())
    {
        return nullptr;
    }

    auto catalog_it = ng_catalog_it->second.find(cc_ng_id);
    return catalog_it == ng_catalog_it->second.end() ? nullptr
                                                     : &catalog_it->second;
}

std::unordered_map<TableName, bool> LocalCcShards::GetCatalogTableNameSnapshot(
    NodeGroupId cc_ng_id, uint64_t snapshot_ts)
{
    std::unordered_map<TableName, bool> tables;
    std::shared_lock<std::shared_mutex> lk(meta_data_mux_);
    for (const auto &[base_table_name, ng_catalog_map] : table_catalogs_)
    {
        auto catalog_it = ng_catalog_map.find(cc_ng_id);
        if (catalog_it != ng_catalog_map.end())
        {
            const CatalogEntry &catalog_entry = catalog_it->second;
            if (catalog_entry.schema_ != nullptr)
            {
                auto ins_it = tables.emplace(
                    std::piecewise_construct,
                    std::forward_as_tuple(base_table_name.StringView().data(),
                                          base_table_name.StringView().size(),
                                          base_table_name.Type()),
                    std::forward_as_tuple(false));
                assert(ins_it.second);
                (void) ins_it;
                for (const txservice::TableName &index_table_name :
                     catalog_entry.schema_->IndexNames())
                {
                    auto ins_it =
                        tables.emplace(std::piecewise_construct,
                                       std::forward_as_tuple(
                                           index_table_name.StringView().data(),
                                           index_table_name.StringView().size(),
                                           index_table_name.Type()),
                                       std::forward_as_tuple(false));
                    assert(ins_it.second);
                    // This silences the -Wunused-but-set-variable warning
                    // without any runtime overhead.
                    (void) ins_it;
                }

                // For alter table, should include new index tables.
                // For dirty index tables, if the dirty schema version is larger
                // than the `snapshot_ts`, it means there is no data in the
                // dirty index table before the `snapshot_ts`, then, there is no
                // need to do checkpoint or table stats sync.
                if (catalog_entry.dirty_schema_ != nullptr &&
                    catalog_entry.DirtyVersion() <= snapshot_ts)
                {
                    // Only search new index table name, because the base table
                    // and the old index have been obtained via above.
                    for (const txservice::TableName &index_table_name :
                         catalog_entry.dirty_schema_->IndexNames())
                    {
                        auto iter = tables.find(index_table_name);
                        if (iter == tables.end())
                        {
                            auto ins_it = tables.emplace(
                                std::piecewise_construct,
                                std::forward_as_tuple(
                                    index_table_name.StringView().data(),
                                    index_table_name.StringView().size(),
                                    index_table_name.Type()),
                                std::forward_as_tuple(true));
                            assert(ins_it.second);
                            // This silences the -Wunused-but-set-variable
                            // warning without any runtime overhead.
                            (void) ins_it;
                        }
                    }
                } /* End of dirty index table schema */
            }     /* End of this base table */
        }
    }
    return tables;
}

void LocalCcShards::CreateSchemaRecoveryTx(
    ReplayLogCc &replay_log_cc,
    const ::txlog::SchemaOpMessage &schema_op_msg,
    int64_t tx_term)
{
    auto schema_recover_thd = std::thread(
        [this,
         &replay_log_cc,
         schema_op_msg,  // cannot pass in reference here since msg
                         // will be freed once replay is done
         txn = replay_log_cc.Txn(),
         tx_term,
         commit_ts = replay_log_cc.CommitTs()]
        {
            TransactionExecution *txm = tx_service_->NewTx();
            ClusterConfigRecord rec;
            txm->SetRecoverTxState(txn, tx_term, commit_ts);
            ReadTxRequest read_req(&cluster_config_ccm_name,
                                   VoidKey::NegInfTxKey(),
                                   &rec,
                                   false,
                                   false,
                                   true,
                                   0,
                                   false,
                                   true);
            txm->Execute(&read_req);
            read_req.Wait();
            if (read_req.IsError())
            {
                // leadership transferred away before replay finish.
                replay_log_cc.AbortCcRequest(CcErrorCode::TX_NODE_NOT_LEADER);
                AbortTxRequest abort_req;
                txm->Execute(&abort_req);
                abort_req.Wait();
                return;
            }
            replay_log_cc.SetFinish();
            SchemaRecoveryTxRequest recover_req(schema_op_msg);
            txm->Execute(&recover_req);
            recover_req.Wait();

            if (recover_req.IsError() ||
                recover_req.Result() != UpsertResult::Succeeded)
            {
                txservice::AbortTx(txm);
            }
            else
            {
                txservice::CommitTx(txm);
            }
        });

    schema_recover_thd.detach();
}

void LocalCcShards::CreateSplitRangeRecoveryTx(
    ReplayLogCc &replay_log_cc,
    const ::txlog::SplitRangeOpMessage &ds_split_range_op_msg,
    const TableSchema *table_schema,
    int32_t partition_id,
    const RangeInfo *range_info,
    std::vector<TxKey> &&new_range_keys,
    std::vector<int32_t> &&new_partition_ids,
    uint32_t node_group_id,
    int64_t tx_term)
{
    auto split_recover_thd = std::thread(
        [this,
         &replay_log_cc,
         ds_split_range_op_msg,  // cannot pass in reference here since
                                 // msg will be freed once replay is
                                 // done
         table_schema,
         partition_id,
         txn = replay_log_cc.Txn(),
         tx_term,
         commit_ts = replay_log_cc.CommitTs(),
         range_info,
         new_range_keys = std::move(new_range_keys),
         new_partition_ids = std::move(new_partition_ids),
         node_group_id,
         split_tx_started = replay_log_cc.RangeSplitStarted()]() mutable
        {
            if (Sharder::Instance().TryPinNodeGroupData(node_group_id) !=
                tx_term)
            {
                replay_log_cc.AbortCcRequest(
                    CcErrorCode::REQUESTED_NODE_NOT_LEADER);
                return;
            }
            // guard to unpin node group on finish.
            std::shared_ptr<void> defer_unpin(
                nullptr,
                [node_group_id](void *)
                { Sharder::Instance().UnpinNodeGroupData(node_group_id); });
            // Mark the table as sync in progress to avoid concurrent data sync
            // before range split tx finishes if this is the first started range
            // split.
            TableName table_name =
                TableName(ds_split_range_op_msg.table_name(),
                          TableName::Type(ds_split_range_op_msg.table_name()));
            const TableName range_table_name = TableName{
                ds_split_range_op_msg.table_name(), TableType::RangePartition};
            const TableName base_table_name = TableName{
                range_table_name.GetBaseTableNameSV(), TableType::Primary};
            TableRangeEntry *range_entry =
                const_cast<TableRangeEntry *>(GetTableRangeEntry(
                    range_table_name, node_group_id, partition_id));

            // Checkpoint cannot start until recover is finished, we should be
            // the only one trying to sync the range.

            TransactionExecution *txm = tx_service_->NewTx();
            ClusterConfigRecord rec;
            txm->SetRecoverTxState(txn, tx_term, commit_ts);
            ReadTxRequest read_req(&cluster_config_ccm_name,
                                   VoidKey::NegInfTxKey(),
                                   &rec,
                                   false,
                                   false,
                                   true,
                                   0,
                                   false,
                                   true);
            txm->Execute(&read_req);
            read_req.Wait();
            // Only case we fail here is that leader gone before replay
            // finished.
            bool lock_meta_failed =
                read_req.IsError() ||
                read_req.Result().first != RecordStatus::Normal;

            if (!lock_meta_failed)
            {
                CatalogKey table_key(base_table_name);
                TxKey tbl_tx_key{&table_key};
                CatalogRecord catalog_rec;

                read_req.Reset();
                read_req.Set(&catalog_ccm_name,
                             &tbl_tx_key,
                             &catalog_rec,
                             false,
                             false,
                             true,
                             0,
                             false,
                             true);
                txm->Execute(&read_req);
                read_req.Wait();
                lock_meta_failed =
                    read_req.IsError() ||
                    read_req.Result().first != RecordStatus::Normal;
            }

            if (!lock_meta_failed)
            {
                RangeBucketRecord bucket_rec;
                RangeBucketKey bucket_key(
                    Sharder::Instance().MapRangeIdToBucketId(partition_id));
                TxKey bucket_tx_key{&bucket_key};
                read_req.Reset();
                read_req.Set(&range_bucket_ccm_name,
                             &bucket_tx_key,
                             &bucket_rec,
                             false,
                             false,
                             true,
                             0,
                             false,
                             true);
                txm->Execute(&read_req);
                read_req.Wait();
                lock_meta_failed =
                    read_req.IsError() ||
                    read_req.Result().first != RecordStatus::Normal;
            }
            if (lock_meta_failed)
            {
                replay_log_cc.AbortCcRequest(
                    CcErrorCode::REQUESTED_NODE_NOT_LEADER);
                txservice::AbortTx(txm);
                return;
            }

            auto task_limiter_key = TaskLimiterKey(node_group_id,
                                                   tx_term,
                                                   table_name.StringView(),
                                                   table_name.Type(),
                                                   partition_id);
        // Checkpoint cannot start at `tx_term` until recover is finished,
        // we should be the only one trying to sync the range.
#ifdef RANGE_PARTITION_ENABLED
            auto limiter = task_limiters_.emplace(
                task_limiter_key, std::make_shared<DataSyncTaskLimiter>());
            assert(limiter.second == true);
#endif

            replay_log_cc.SetFinish();

            StoreRange *store_range = range_entry->PinStoreRange();
            if (!store_range)
            {
                // Fetch range slices from data store if it's not cached.
                RunOnTxProcessorCc cc([](CcShard &ccs) {});
                range_entry->FetchRangeSlices(range_table_name,
                                              &cc,
                                              node_group_id,
                                              tx_term,
                                              cc_shards_[0].get());
                cc.Wait();
                while (cc.IsError())
                {
                    // Failed to fetch range slice. If error is caused by
                    // data store unreachable, retry.
                    if (cc.ErrorCode() == CcErrorCode::DATA_STORE_ERR)
                    {
                        std::this_thread::sleep_for(
                            std::chrono::milliseconds(500));
                        cc.Reset();
                        range_entry->FetchRangeSlices(range_table_name,
                                                      &cc,
                                                      node_group_id,
                                                      tx_term,
                                                      cc_shards_[0].get());
                        cc.Wait();
                    }
                    else if (cc.ErrorCode() == CcErrorCode::NG_TERM_CHANGED)
                    {
                        // Term is invalid, we are no longer leader. Abort tx.
                        txservice::AbortTx(txm);
                        PopPendingTask(
                            node_group_id, tx_term, table_name, partition_id);
                        return;
                    }
                    else
                    {
                        assert(false);
                    }
                }
                store_range = range_entry->PinStoreRange();
                assert(store_range != nullptr);
            }
            RangeSplitRecoveryTxRequest recover_req(
                ds_split_range_op_msg,
                table_schema,
                partition_id,
                store_range,
                range_info,
                std::move(new_range_keys),
                std::move(new_partition_ids),
                node_group_id);
            txm->Execute(&recover_req);
            recover_req.Wait();

            if (recover_req.IsError() || !recover_req.Result())
            {
                // Leader transferred away before replay finish. No need
                // to update pending task queue.
                txservice::AbortTx(txm);
            }
            else
            {
                range_entry->UpdateLastDataSyncTS(0);
                range_entry->UnPinStoreRange();
                txservice::CommitTx(txm);
            }

            PopPendingTask(node_group_id, tx_term, table_name, partition_id);
        });

    split_recover_thd.detach();
}

void LocalCcShards::InitTableRanges(const TableName &range_table_name,
                                    std::vector<InitRangeEntry> &init_ranges,
                                    NodeGroupId ng_id,
                                    bool empty_table)
{
    std::unique_lock<std::shared_mutex> lk(meta_data_mux_);

    std::unique_lock<std::mutex> heap_lk(table_ranges_heap_mux_);
    bool is_override_thd = mi_is_override_thread();
    mi_threadid_t prev_thd = mi_override_thread(table_ranges_thread_id_);
    mi_heap_t *prev_heap = mi_heap_set_default(table_ranges_heap_);

    // Init table ranges
    assert(range_table_name.Type() == TableType::RangePartition);
    auto table_it = table_ranges_.try_emplace(range_table_name);
    auto id_table_it = table_range_ids_.try_emplace(range_table_name);
    std::unordered_map<NodeGroupId, std::map<TxKey, TableRangeEntry::uptr>>
        &ranges_of_all_ngs = table_it.first->second;
    auto ngs_it = ranges_of_all_ngs.try_emplace(ng_id);
    std::map<TxKey, TableRangeEntry::uptr> &ranges = ngs_it.first->second;
    auto &ids = id_table_it.first->second.try_emplace(ng_id).first->second;
    assert(init_ranges.size() > 0);

    for (size_t pidx = 0; pidx < init_ranges.size(); ++pidx)
    {
        InitRangeEntry &range_entry = init_ranges[pidx];

        // The first range's start is negative infinity.
        assert(pidx == 0 || range_entry.key_.IsOwner());
        TxKey range_start_key = pidx == 0 ? catalog_factory_->NegativeInfKey()
                                          : std::move(range_entry.key_);
        // The tx key in the map is a shallow copy of the start key in the
        // to-be-created range entry.
        TxKey map_key = range_start_key.GetShallowCopy();

        auto range_it = ranges.find(map_key);
        if (range_it == ranges.end())
        {
            TableRangeEntry::uptr new_range =
                catalog_factory_->CreateTableRange(std::move(range_start_key),
                                                   range_entry.version_ts_,
                                                   range_entry.partition_id_);
            range_it = ranges
                           .try_emplace(new_range->RangeStartTxKey(),
                                        std::move(new_range))
                           .first;

            if (range_it != ranges.begin())
            {
                auto prev_range_it = std::prev(range_it);
                const TxKey &range_start = range_it->first;
                prev_range_it->second->SetRangeEndTxKey(
                    range_start.GetShallowCopy());
            }

            auto next_range_it = std::next(range_it);
            if (next_range_it != ranges.end())
            {
                const TxKey &next_start = next_range_it->first;
                range_it->second->SetRangeEndTxKey(next_start.GetShallowCopy());
            }
        }

        ids.insert_or_assign(range_entry.partition_id_, range_it->second.get());
    }
    ranges.rbegin()->second->SetRangeEndTxKey(
        catalog_factory_->PositiveInfKey());

    if (empty_table)
    {
        assert(init_ranges.size() == 1);
        std::vector<SliceInitInfo> slices;
        slices.emplace_back(
            catalog_factory_->NegativeInfKey(), 0, SliceStatus::FullyCached);
        ranges.begin()->second->InitRangeSlices(
            std::move(slices), ng_id, range_table_name.IsBase(), true);
    }

    mi_heap_set_default(prev_heap);
    if (is_override_thd)
    {
        mi_override_thread(prev_thd);
    }
    else
    {
        mi_restore_default_thread_id();
    }
}

void LocalCcShards::InitPrebuiltTables(NodeGroupId ng_id)
{
    for (auto &[table, image] : prebuilt_tables_)
    {
        auto table_it = table_catalogs_.try_emplace(table);
        auto ng_it = table_it.first->second.try_emplace(ng_id);
        if (ng_it.second)
        {
            ng_it.first->second.InitSchema(
                catalog_factory_->CreateTableSchema(table, image, 2), 2);
        }
    }
}

void LocalCcShards::PublishMessage(const std::string &chan,
                                   const std::string &message)
{
    assert(publish_func_ != nullptr);

    // start a new bthread to handle message publish as it might block the
    // thread
    bthread_t tid = 0;
    bthread_attr_t attr = BTHREAD_ATTR_NORMAL;

    // args will be deleted in `publish`
    auto *args_ptr = new PublishArg(this, chan, message);

    if (bthread_start_background(&tid, &attr, Publish, args_ptr) != 0)
    {
        LOG(FATAL) << "Fail to start Publish bthread";
        delete args_ptr;
        publish_func_(chan, message);
    }
}

std::map<TxKey, TableRangeEntry::uptr>
    *LocalCcShards::GetTableRangesForATableInternal(
        const TableName &range_table_name, const NodeGroupId ng_id)
{
    auto table_it = table_ranges_.find(range_table_name);
    if (table_it == table_ranges_.end())
    {
        return nullptr;
    }

    auto &ranges_of_all_ngs = table_it->second;
    auto ngs_it = ranges_of_all_ngs.find(ng_id);

    return ngs_it == ranges_of_all_ngs.end() ? nullptr : &ngs_it->second;
}

std::map<TxKey, TableRangeEntry::uptr> *LocalCcShards::GetTableRangesForATable(
    const TableName &range_table_name, const NodeGroupId ng_id)
{
    std::shared_lock<std::shared_mutex> s_lk(meta_data_mux_);
    return GetTableRangesForATableInternal(range_table_name, ng_id);
}

std::unordered_map<uint32_t, TableRangeEntry *>
    *LocalCcShards::GetTableRangeIdsForATableInternal(
        const TableName &range_table_name, const NodeGroupId ng_id)
{
    auto table_it = table_range_ids_.find(range_table_name);
    if (table_it == table_range_ids_.end())
    {
        return nullptr;
    }
    auto ng_it = table_it->second.find(ng_id);
    return ng_it == table_it->second.end() ? nullptr : &ng_it->second;
}

void LocalCcShards::CleanTableRange(const TableName &table_name,
                                    NodeGroupId ng_id)
{
    std::unique_lock<std::shared_mutex> lk(meta_data_mux_);
    auto table_it = table_ranges_.find(table_name);
    if (table_it != table_ranges_.end())
    {
        table_it->second.erase(ng_id);
    }
    auto id_table_it = table_range_ids_.find(table_name);
    if (id_table_it != table_range_ids_.end())
    {
        id_table_it->second.erase(ng_id);
    }
}

void LocalCcShards::DropTableRanges(NodeGroupId ng_id)
{
    std::unique_lock<std::shared_mutex> lk(meta_data_mux_);
    for (auto &table_range : table_ranges_)
    {
        table_range.second.erase(ng_id);
    }
    for (auto &range_id : table_range_ids_)
    {
        range_id.second.erase(ng_id);
    }
}

void LocalCcShards::KickoutRangeSlices()
{
    std::shared_lock<std::shared_mutex> lk(meta_data_mux_);
    // Since we don't maintain a lru list of store range, we just
    // kickout range slices that have not been accessed for at least
    // 10 mins. If we still haven't cleaned up enough memory. We
    // will sort the range slices by their last accessed time and
    // kickout the least recent accessed range slices.
    std::vector<std::pair<uint64_t, TableRangeEntry *>> scanned_ranges;
    uint64_t current_ts = ClockTs();
    for (auto &[table, table_ranges] : table_ranges_)
    {
        for (auto &[ng_id, ng_ranges] : table_ranges)
        {
            for (auto &[key, range_entry] : ng_ranges)
            {
                std::shared_lock<std::shared_mutex> lk(range_entry->mux_);
                StoreRange *store_range = range_entry->RangeSlices();
                if (store_range)
                {
                    uint64_t last_accessed = store_range->LastAccessedTs();
                    if (current_ts > last_accessed &&
                        current_ts - last_accessed > 600000000)
                    {
                        lk.unlock();
                        std::unique_lock<std::shared_mutex> uniq_lk(
                            range_entry->mux_);
                        if (range_entry->IsStoreRangeFree())
                        {
                            std::unique_lock<std::mutex> heap_lk(
                                table_ranges_heap_mux_);
                            bool is_override_thd = mi_is_override_thread();
                            mi_threadid_t prev_thd = mi_override_thread(
                                GetTableRangesHeapThreadId());
                            mi_heap_t *prev_heap =
                                mi_heap_set_default(GetTableRangesHeap());

                            range_entry->DropStoreRange();

                            bool has_enough_mem = HasEnoughTableRangesMemory();
                            mi_heap_set_default(prev_heap);
                            if (is_override_thd)
                            {
                                mi_override_thread(prev_thd);
                            }
                            else
                            {
                                mi_restore_default_thread_id();
                            }
                            if (has_enough_mem)
                            {
                                // We've cleaned up enough memory space.
                                return;
                            }
                        }
                    }
                    else if (current_ts > last_accessed &&
                             current_ts - last_accessed > 4000000)
                    {
                        // Put the store range into buffer for sort.
                        // Only kickout range slices that are not
                        // accessed for more than 4 seconds.
                        scanned_ranges.emplace_back(last_accessed,
                                                    range_entry.get());
                    }
                }
            }
        }
    }

    // We should not need to reach here most of the time.
    std::sort(scanned_ranges.begin(),
              scanned_ranges.end(),
              [](const std::pair<uint64_t, TableRangeEntry *> &a,
                 const std::pair<uint64_t, TableRangeEntry *> &b) -> bool
              { return a.first < b.first; });
    for (auto &[time, entry] : scanned_ranges)
    {
        std::unique_lock<std::shared_mutex> uniq_lk(entry->mux_);
        if (entry->IsStoreRangeFree())
        {
            std::unique_lock<std::mutex> heap_lk(table_ranges_heap_mux_);
            bool is_override_thd = mi_is_override_thread();
            mi_threadid_t prev_thd =
                mi_override_thread(GetTableRangesHeapThreadId());
            mi_heap_t *prev_heap = mi_heap_set_default(GetTableRangesHeap());

            entry->DropStoreRange();

            bool has_enough_mem = HasEnoughTableRangesMemory();
            mi_heap_set_default(prev_heap);
            if (is_override_thd)
            {
                mi_override_thread(prev_thd);
            }
            else
            {
                mi_restore_default_thread_id();
            }

            if (has_enough_mem)
            {
                // We've cleaned up enough memory space.
                return;
            }
        }
    }
}

TableRangeEntry *LocalCcShards::GetTableRangeEntry(const TableName &table_name,
                                                   const NodeGroupId ng_id,
                                                   const TxKey &key)
{
    std::shared_lock<std::shared_mutex> lk(meta_data_mux_);
    TableName range_table_name(table_name.StringView(),
                               TableType::RangePartition);
    return GetTableRangeEntryInternal(range_table_name, ng_id, key);
}

std::optional<std::tuple<uint64_t, TxKey, TxKey>>
LocalCcShards::GetTableRangeKeys(const TableName &table_name,
                                 const NodeGroupId ng_id,
                                 int32_t range_id)
{
    std::shared_lock<std::shared_mutex> lk(meta_data_mux_);
    TableName range_table_name(table_name.StringView(),
                               TableType::RangePartition);
    TableRangeEntry *range_entry =
        GetTableRangeEntryInternal(range_table_name, ng_id, range_id);

    if (range_entry == nullptr)
    {
        return std::nullopt;
    }

    uint64_t version_ts = range_entry->Version();
    auto start_key = range_entry->GetRangeInfo()->StartTxKey().Clone();
    auto end_key = range_entry->GetRangeInfo()->EndTxKey().Clone();

    return std::make_tuple(
        version_ts, std::move(start_key), std::move(end_key));
}

bool LocalCcShards::CheckRangeVersion(const TableName &table_name,
                                      const NodeGroupId ng_id,
                                      int32_t range_id,
                                      uint64_t range_version)
{
    std::shared_lock<std::shared_mutex> lk(meta_data_mux_);
    TableName range_table_name(table_name.StringView(),
                               TableType::RangePartition);
    TableRangeEntry *range_entry =
        GetTableRangeEntryInternal(range_table_name, ng_id, range_id);

    if (range_entry == nullptr)
    {
        return false;
    }

    return range_entry->Version() == range_version;
}

const TableRangeEntry *LocalCcShards::GetTableRangeEntry(
    const TableName &table_name, const NodeGroupId ng_id, int32_t range_id)
{
    std::shared_lock<std::shared_mutex> lk(meta_data_mux_);
    TableName range_table_name(table_name.StringView(),
                               TableType::RangePartition);
    return GetTableRangeEntryInternal(range_table_name, ng_id, range_id);
}

const TableRangeEntry *LocalCcShards::GetTableRangeEntryNoLocking(
    const TableName &table_name, const NodeGroupId ng_id, const TxKey &key)
{
    TableName range_table_name(table_name.StringView(),
                               TableType::RangePartition);
    return GetTableRangeEntryInternal(range_table_name, ng_id, key);
}

StoreRange *LocalCcShards::FindRange(const TableName &table_name,
                                     const NodeGroupId ng_id,
                                     const TxKey &key)
{
    std::shared_lock<std::shared_mutex> lk(meta_data_mux_);
    TableName range_table_name(table_name.StringView(),
                               TableType::RangePartition);

    TableRangeEntry *entry =
        GetTableRangeEntryInternal(range_table_name, ng_id, key);
    if (!entry)
    {
        DLOG(ERROR) << " The range table of " << table_name.StringView()
                    << " does not exist.";
        return nullptr;
    }

    return entry->RangeSlices();
}

StoreRange *LocalCcShards::FindRange(const TableName &table_name,
                                     const NodeGroupId ng_id,
                                     int32_t range_id)
{
    std::shared_lock<std::shared_mutex> lk(meta_data_mux_);
    TableName range_table_name(table_name.StringView(),
                               TableType::RangePartition);

    TableRangeEntry *entry =
        GetTableRangeEntryInternal(range_table_name, ng_id, range_id);
    if (!entry)
    {
        DLOG(ERROR) << " The range table of " << table_name.StringView()
                    << " does not exist.";
        return nullptr;
    }

    return entry->RangeSlices();
}

uint64_t LocalCcShards::CountRanges(const TableName &table_name,
                                    const NodeGroupId ng_id,
                                    const NodeGroupId key_ng_id) const
{
    std::shared_lock<std::shared_mutex> lk(meta_data_mux_);
    return CountRangesLockless(table_name, ng_id, key_ng_id);
}

uint64_t LocalCcShards::CountRangesLockless(const TableName &table_name,
                                            const NodeGroupId ng_id,
                                            const NodeGroupId key_ng_id) const
{
    TableName range_table_name(table_name.StringView(),
                               TableType::RangePartition);
    auto range_ngs = table_ranges_.find(range_table_name);
    if (range_ngs == table_ranges_.end())
    {
        return 0;
    }
    auto ranges = range_ngs->second.find(ng_id);
    if (ranges == range_ngs->second.end())
    {
        return 0;
    }
    uint64_t counts = std::accumulate(
        ranges->second.begin(),
        ranges->second.end(),
        0UL,
        [key_ng_id, ng_id, this](
            uint64_t a, const std::pair<const TxKey, TableRangeEntry::uptr> &b)
        {
            NodeGroupId range_ng =
                GetRangeOwnerInternal(b.second->GetRangeInfo()->PartitionId(),
                                      ng_id)
                    ->BucketOwner();
            if (range_ng == key_ng_id)
            {
                return a + 1;
            }
            else
            {
                return a;
            }
        });
    return counts;
}

uint64_t LocalCcShards::CountSlices(const TableName &table_name,
                                    const NodeGroupId ng_id,
                                    const NodeGroupId local_ng_id) const
{
    std::shared_lock<std::shared_mutex> lk(meta_data_mux_);
    TableName range_table_name(table_name.StringView(),
                               TableType::RangePartition);
    assert(Sharder::Instance().LeaderTerm(local_ng_id) >= 0 ||
           Sharder::Instance().CandidateLeaderTerm(local_ng_id) >= 0);

    uint64_t slices = 0;

    const std::map<TxKey, TableRangeEntry::uptr> &ranges =
        table_ranges_.at(range_table_name).at(ng_id);

    for (auto &[range_start_key, range_entry] : ranges)
    {
        NodeGroupId range_ng =
            GetRangeOwnerInternal(range_entry->GetRangeInfo()->PartitionId(),
                                  ng_id)
                ->BucketOwner();
        if (range_ng == local_ng_id)
        {
            const StoreRange *store_range = range_entry->RangeSlices();
            assert(store_range != nullptr);
            slices += store_range->Slices().size();
        }
    }

    return slices;
}

void LocalCcShards::SetTxIdent(uint32_t latest_committed_tx_no)
{
    // Each cc_shard's `next_tx_ident_` is concurrently accessed by log replay
    // thread in this func when native cc node finishes log replay from its
    // bound log group, and tx_processor thread in CcShard::NewTx().
    // They are coordinated by the point when native cc node's `leader_term_`
    // atomic variable becomes positive so no lock is needed.
    uint32_t next_tx_ident = latest_committed_tx_no + 1;
    for (const auto &cc_shard : cc_shards_)
    {
        if (cc_shard->next_tx_ident_ < next_tx_ident)
        {
            cc_shard->next_tx_ident_ = latest_committed_tx_no + 1;
        }
    }
}

void LocalCcShards::FlushData(const TableName &table_name,
                              const TableSchema *schema,
                              uint64_t data_sync_ts,
                              int64_t term,
                              uint64_t node_group,
                              std::vector<FlushRecord> *data_sync_vec,
                              std::vector<FlushRecord> *archive_vec,
                              std::vector<TxKey> *mv_vec,
                              CcHandlerResult<Void> &hres,
                              bool during_range_split)
{
    std::unique_lock<std::mutex> flush_worker_lk(flush_data_worker_ctx_.mux_);
    pending_flush_work_.emplace_back(node_group,
                                     term,
                                     data_sync_ts,
                                     table_name,
                                     schema,
                                     data_sync_vec,
                                     archive_vec,
                                     mv_vec,
                                     &hres,
                                     during_range_split);
    flush_data_worker_ctx_.cv_.notify_one();
}

void LocalCcShards::DropCatalogs(NodeGroupId cc_ng_id)
{
    std::unique_lock<std::shared_mutex> lk(meta_data_mux_);

    for (auto node_catalog_it = table_catalogs_.begin();
         node_catalog_it != table_catalogs_.end();
         ++node_catalog_it)
    {
        node_catalog_it->second.erase(cc_ng_id);
    }
}

std::shared_ptr<TableSchema> LocalCcShards::GetSharedTableSchema(
    const TableName &table_name, NodeGroupId ng_id)
{
    std::shared_lock<std::shared_mutex> shards_lk(meta_data_mux_);

    auto ng_catalog_it = table_catalogs_.find(table_name);
    if (ng_catalog_it == table_catalogs_.end())
    {
        return nullptr;
    }

    auto catalog_it = ng_catalog_it->second.find(ng_id);
    if (catalog_it == ng_catalog_it->second.end())
    {
        return nullptr;
    }

    CatalogEntry &catalog_entry = catalog_it->second;

    {
        std::shared_lock<std::shared_mutex> catalog_s_lk(catalog_entry.s_mux_);
        if (!catalog_entry.committing_)
        {
            return catalog_entry.schema_;
        }
    }

    std::unique_lock<std::shared_mutex> catalog_lk(catalog_entry.s_mux_);
    // Releases the shared lock on the catatalog collection while keeping the
    // exclusive lock on the specified table's catalog. This allows other tx's
    // to create, drop or alter other tables' catalogs and the cc node to clear
    // all associating table catalogs when it steps down from the leader.
    // Stepping down will de-allocate the catalog entry, which synchronizes with
    // runtime threads obtaining a catalog pointer via the catalog entry's lock.
    shards_lk.unlock();

    ++catalog_entry.waiting_thd_cnt_;
    catalog_entry.cv_.wait(
        catalog_lk, [&catalog_entry] { return !catalog_entry.committing_; });
    --catalog_entry.waiting_thd_cnt_;

    return catalog_entry.schema_;
}

TableRangeEntry *LocalCcShards::GetTableRangeEntryInternal(
    const TableName &range_tbl_name, const NodeGroupId ng_id, const TxKey &key)
{
    // The caller of the method must have acquired a shared lock. The table name
    // must be the range table name.

    auto range_map = GetTableRangesForATableInternal(range_tbl_name, ng_id);
    if (!range_map)
    {
        return nullptr;
    }
    TableRangeEntry *entry = nullptr;

    auto lower_it = range_map->lower_bound(key);

    if (lower_it == range_map->end())
    {
        // The input key is greater than the last entry of the range map, so
        // it falls into the last range.
        --lower_it;
        entry = lower_it->second.get();
    }
    else if (lower_it->first == key)
    {
        entry = lower_it->second.get();
    }
    else
    {
        --lower_it;
        entry = lower_it->second.get();
    }

    return entry;
}

TableRangeEntry *LocalCcShards::GetTableRangeEntryInternal(
    const TableName &range_tbl_name, const NodeGroupId ng_id, int32_t range_id)
{
    // The caller of the method must have acquired a shared lock. The table name
    // must be the range table name.
    auto table_it = table_range_ids_.find(range_tbl_name);
    if (table_it == table_range_ids_.end())
    {
        return nullptr;
    }
    auto ng_it = table_it->second.find(ng_id);
    if (ng_it == table_it->second.end())
    {
        return nullptr;
    }

    auto range_it = ng_it->second.find(range_id);
    return range_it == ng_it->second.end() ? nullptr : range_it->second;
}

std::pair<std::shared_ptr<Statistics>, bool> LocalCcShards::InitTableStatistics(
    TableSchema *table_schema, NodeGroupId ng_id)
{
    std::unique_lock<std::shared_mutex> lk(meta_data_mux_);

    auto ng_statistics_it =
        table_statistics_map_.try_emplace(table_schema->GetBaseTableName());
    auto statistics_it = ng_statistics_it.first->second.try_emplace(ng_id);
    if (statistics_it.second)
    {
        StatisticsEntry &statistics_entry = statistics_it.first->second;

        statistics_entry.statistics_ =
            catalog_factory_->CreateTableStatistics(table_schema, ng_id);

        table_schema->BindStatistics(statistics_entry.statistics_);
    }

    return {statistics_it.first->second.statistics_, statistics_it.second};
}

std::pair<std::shared_ptr<Statistics>, bool> LocalCcShards::InitTableStatistics(
    TableSchema *table_schema,
    TableSchema *dirty_table_schema,
    NodeGroupId ng_id,
    std::unordered_map<TableName, std::pair<uint64_t, std::vector<TxKey>>>
        sample_pool_map,
    CcShard *ccs)
{
    std::unique_lock<std::shared_mutex> lk(meta_data_mux_);

    auto ng_statistics_it =
        table_statistics_map_.try_emplace(table_schema->GetBaseTableName());
    auto statistics_it = ng_statistics_it.first->second.try_emplace(ng_id);
    if (statistics_it.second)
    {
        StatisticsEntry &statistics_entry = statistics_it.first->second;

        statistics_entry.statistics_ = catalog_factory_->CreateTableStatistics(
            table_schema, std::move(sample_pool_map), ccs, ng_id);

        table_schema->BindStatistics(statistics_entry.statistics_);
        if (dirty_table_schema)
        {
            dirty_table_schema->BindStatistics(statistics_entry.statistics_);
        }
    }

    return {statistics_it.first->second.statistics_, statistics_it.second};
}

StatisticsEntry *LocalCcShards::GetTableStatistics(const TableName &table_name,
                                                   NodeGroupId ng_id)
{
    std::shared_lock<std::shared_mutex> s_lk(meta_data_mux_);

    auto ng_statistics_it = table_statistics_map_.find(table_name);
    if (ng_statistics_it == table_statistics_map_.end())
    {
        return nullptr;
    }

    auto statistics_it = ng_statistics_it->second.find(ng_id);
    return statistics_it == ng_statistics_it->second.end()
               ? nullptr
               : &statistics_it->second;
}

void LocalCcShards::CleanTableStatistics(const TableName &table_name)
{
    std::unique_lock<std::shared_mutex> lk(meta_data_mux_);

    auto ng_statistics_it = table_statistics_map_.find(table_name);
    if (ng_statistics_it != table_statistics_map_.end())
    {
        table_statistics_map_.erase(ng_statistics_it);
    }
}

void LocalCcShards::DropTableStatistics(NodeGroupId ng_id)
{
    std::unique_lock<std::shared_mutex> lk(meta_data_mux_);
    for (auto ng_statistics_it = table_statistics_map_.begin();
         ng_statistics_it != table_statistics_map_.end();
         ++ng_statistics_it)
    {
        ng_statistics_it->second.erase(ng_id);
    }
}

void LocalCcShards::BroadcastIndexStatistics(
    TransactionExecution *txm,
    NodeGroupId ng_id,
    const TableName &table_name,
    const TableSchema *table_schema,
    const remote::NodeGroupSamplePool &sample_pool)
{
    uint32_t ng_cnt = Sharder::Instance().NodeGroupCount();

    if (ng_cnt > 1)
    {
        BroadcastStatisticsTxRequest broadcast_req(
            &table_name, table_schema->Version(), &sample_pool);
        txm->Execute(&broadcast_req);
        broadcast_req.Wait();
        if (broadcast_req.IsError())
        {
            LOG(ERROR) << "Broadcast table statistics for table: "
                       << table_name.StringView()
                       << ", error: " << broadcast_req.ErrorMsg();
        }
    }
}

const BucketInfo *LocalCcShards::GetBucketInfo(const uint16_t bucket_id,
                                               const NodeGroupId ng_id) const
{
#ifndef RANGE_PARTITION_ENABLED
    if (!buckets_migrating_.load(std::memory_order_relaxed))
    {
        return GetBucketInfoInternal(bucket_id, ng_id);
    }
    else
#endif
    {
        std::shared_lock<std::shared_mutex> lk(meta_data_mux_);
        return GetBucketInfoInternal(bucket_id, ng_id);
    }
}

BucketInfo *LocalCcShards::GetBucketInfo(const uint16_t bucket_id,
                                         const NodeGroupId ng_id)
{
    return const_cast<BucketInfo *>(
        std::as_const(*this).GetBucketInfo(bucket_id, ng_id));
}

NodeGroupId LocalCcShards::GetBucketOwner(const uint16_t bucket_id,
                                          const NodeGroupId ng_id) const
{
    const BucketInfo *bucket_info = GetBucketInfo(bucket_id, ng_id);
    if (bucket_info != nullptr)
    {
        return bucket_info->BucketOwner();
    }
    return UINT32_MAX;
}

BucketInfo *LocalCcShards::GetBucketInfoInternal(const uint16_t bucket_id,
                                                 const NodeGroupId ng_id) const
{
    assert(bucket_id < total_range_buckets);
    auto ng_bucket_it = bucket_infos_.find(ng_id);
    if (ng_bucket_it == bucket_infos_.end() || ng_bucket_it->second.empty())
    {
        return nullptr;
    }

    return ng_bucket_it->second.at(bucket_id).get();
}

const BucketInfo *LocalCcShards::GetRangeOwner(const int32_t range_id,
                                               const NodeGroupId ng_id) const
{
    return GetBucketInfo(Sharder::MapRangeIdToBucketId(range_id), ng_id);
}

const BucketInfo *LocalCcShards::GetRangeOwnerNoLocking(
    const int32_t range_id, const NodeGroupId ng_id) const
{
    return GetBucketInfoInternal(Sharder::MapRangeIdToBucketId(range_id),
                                 ng_id);
}

BucketInfo *LocalCcShards::GetRangeOwnerInternal(const int32_t range_id,
                                                 const NodeGroupId ng_id) const
{
    return GetBucketInfoInternal(Sharder::MapRangeIdToBucketId(range_id),
                                 ng_id);
}

const std::unordered_map<uint16_t, std::unique_ptr<BucketInfo>>
    *LocalCcShards::GetAllBucketInfos(NodeGroupId ng_id) const
{
    std::shared_lock<std::shared_mutex> lk(meta_data_mux_);
    auto ng_bucket_it = bucket_infos_.find(ng_id);
    if (ng_bucket_it == bucket_infos_.end())
    {
        return nullptr;
    }
    return &ng_bucket_it->second;
}

void LocalCcShards::DropBucketInfo(NodeGroupId ng_id)
{
    std::unique_lock<std::shared_mutex> lk(meta_data_mux_);
    // bucket_infos_.erase(ng_id);
    assert(bucket_infos_.find(ng_id) != bucket_infos_.end());
    bucket_infos_.at(ng_id).clear();
}

bool LocalCcShards::IsRangeBucketsInitialized(NodeGroupId ng_id)
{
    std::shared_lock<std::shared_mutex> lk(meta_data_mux_);
    auto bucket_info = bucket_infos_.find(ng_id);
    return bucket_info != bucket_infos_.end() &&
           bucket_info->second.size() == total_range_buckets;
}

void LocalCcShards::InitRangeBuckets(NodeGroupId ng_id,
                                     uint32_t ng_cnt,
                                     uint64_t version,
                                     int32_t seed)
{
    std::unique_lock<std::shared_mutex> lk(meta_data_mux_);
    if (bucket_infos_.size() != ng_cnt)
    {
        // Init bucket_info container for all node groups.
        // Case one node group failover to other machine, we just clear
        // bucket_info container instead of erase it from bucket_infos_.
        // Case other node group failover to this machine, we can just fetch
        // bucket_info container from bucket_infos_ instead of insert into
        // bucket_infos_ during failover.
        //
        // Then, the buckte_infos_ never be modified if there is no cluster
        // scaling. So, we can safely read buckte_infos_ without locking
        // meta_data_mux_ when cluster is not migrating.
        for (uint32_t ng = 0; ng < ng_cnt; ng++)
        {
            bucket_infos_.try_emplace(ng);
        }
    }

    // Construct bucket info map on startup
    // Generate 64 random numbers for each node group as virtual nodes on
    // hashing ring. Each bucket id belongs to the first virtual node that is
    // larger than the bucket id.
    std::unordered_map<uint16_t, std::unique_ptr<BucketInfo>> &ng_bucket_infos =
        bucket_infos_.at(ng_id);
    ng_bucket_infos.clear();
    std::map<uint16_t, NodeGroupId> rand_num_to_ng;
    srand(seed);
    for (uint32_t ng = 0; ng < ng_cnt; ng++)
    {
        size_t generated = 0;
        while (generated < 64)
        {
            uint16_t rand_num = rand() % total_range_buckets;
            if (rand_num_to_ng.find(rand_num) == rand_num_to_ng.end())
            {
                generated++;
                rand_num_to_ng.emplace(rand_num, ng);
            }
        }
    }

    std::unordered_map<NodeGroupId, uint16_t> ng_buckets;
    // std::unique_lock<std::shared_mutex> lk(meta_data_mux_);
    // Insert bucket ids into the map.
    auto it = rand_num_to_ng.begin();
    for (uint16_t bucket_id = 0; bucket_id < total_range_buckets; bucket_id++)
    {
        // The buckets larger than the last random number belongs to the
        // first virtual node on the ring.
        if (it != rand_num_to_ng.end() && bucket_id >= it->first)
        {
            it++;
        }
        NodeGroupId ng_id = it == rand_num_to_ng.end()
                                ? rand_num_to_ng.begin()->second
                                : it->second;
        ng_bucket_infos.try_emplace(
            bucket_id, std::make_unique<BucketInfo>(ng_id, version));
        auto res_pair = ng_buckets.try_emplace(ng_id, 0);
        res_pair.first->second++;
    }
    // bucket_infos_.try_emplace(ng_id, std::move(ng_bucket_infos));
}

const BucketInfo *LocalCcShards::UploadNewBucketInfo(NodeGroupId ng_id,
                                                     uint16_t bucket_id,
                                                     NodeGroupId dirty_ng,
                                                     uint64_t dirty_version)
{
    std::shared_lock<std::shared_mutex> lk(meta_data_mux_);
    BucketInfo *bucket_info = GetBucketInfoInternal(bucket_id, ng_id);
    bucket_info->SetDirty(dirty_ng, dirty_version);
    if (dirty_ng == ng_id)
    {
        // Allow this bucket to accept upload batch request from original
        // bucket owner to warm up cache.
        bucket_info->SetAcceptsUploadBatch(true);
    }
    return bucket_info;
}

const BucketInfo *LocalCcShards::UploadBucketInfo(NodeGroupId ng_id,
                                                  uint16_t bucket_id,
                                                  NodeGroupId owner_ng,
                                                  uint64_t version)
{
    std::shared_lock<std::shared_mutex> lk(meta_data_mux_);
    BucketInfo *bucket_info = GetBucketInfoInternal(bucket_id, ng_id);
    assert(version > bucket_info->Version() &&
           version >= bucket_info->DirtyVersion());
    bucket_info->ClearDirty();
    bucket_info->Set(owner_ng, version);
    return bucket_info;
}

bool LocalCcShards::DropStoreRangesInBucket(NodeGroupId ng_id,
                                            uint16_t bucket_id)
{
    std::shared_lock<std::shared_mutex> lk(meta_data_mux_);
    for (auto &[tbl_name, ng_ranges] : table_ranges_)
    {
        auto tbl_ranges = ng_ranges.find(ng_id);
        if (tbl_ranges != ng_ranges.end())
        {
            for (auto &[key, entry] : tbl_ranges->second)
            {
                if (Sharder::MapRangeIdToBucketId(
                        entry->GetRangeInfo()->PartitionId()) == bucket_id)
                {
                    std::unique_lock<std::shared_mutex> uniq_lk(entry->mux_);
                    if (entry->IsStoreRangeFree(true))
                    {
                        std::unique_lock<std::mutex> heap_lk(
                            table_ranges_heap_mux_);
                        bool is_override_thd = mi_is_override_thread();
                        mi_threadid_t prev_thd =
                            mi_override_thread(GetTableRangesHeapThreadId());
                        mi_heap_t *prev_heap =
                            mi_heap_set_default(GetTableRangesHeap());

                        entry->DropStoreRange();

                        mi_heap_set_default(prev_heap);
                        if (is_override_thd)
                        {
                            mi_override_thread(prev_thd);
                        }
                        else
                        {
                            mi_restore_default_thread_id();
                        }
                    }
                    else
                    {
                        return false;
                    }
                }
            }
        }
    }
    return true;
}

std::unordered_map<TableName, std::unordered_set<int>>
LocalCcShards::GetRangesInBucket(uint16_t bucket_id, NodeGroupId ng_id)
{
    std::unordered_map<TableName, std::unordered_set<int>> snapshot;
    std::shared_lock<std::shared_mutex> lk(meta_data_mux_);
    for (auto &[tbl_name, ng_ranges] : table_ranges_)
    {
        auto tbl_ranges = ng_ranges.find(ng_id);
        if (tbl_ranges != ng_ranges.end())
        {
            std::unordered_set<int> tbl_snapshot;
            for (auto &[key, entry] : tbl_ranges->second)
            {
                if (Sharder::MapRangeIdToBucketId(
                        entry->GetRangeInfo()->PartitionId()) == bucket_id)
                {
                    tbl_snapshot.insert(entry->GetRangeInfo()->PartitionId());
                }
            }
            if (!tbl_snapshot.empty())
            {
                snapshot.try_emplace(
                    TableName{tbl_name.StringView(), tbl_name.Type()},
                    std::move(tbl_snapshot));
            }
        }
    }
    return snapshot;
}

const BucketInfo *LocalCcShards::CommitDirtyBucketInfo(NodeGroupId ng_id,
                                                       uint16_t bucket_id)
{
    std::shared_lock<std::shared_mutex> lk(meta_data_mux_);
    BucketInfo *bucket_info = GetBucketInfoInternal(bucket_id, ng_id);
    bucket_info->CommitDirty();
    return bucket_info;
}

std::unordered_map<NodeGroupId, BucketMigrateInfo>
LocalCcShards::GenerateBucketMigrationPlan(uint32_t new_ng_count, int32_t seed)
{
    // Construct bucket info map on startup
    // Generate 64 random numbers for each node group as virtual nodes on
    // hashing ring. Each bucket id belongs to the first virtual node that is
    // larger than the bucket id.
    std::map<uint16_t, NodeGroupId> rand_num_to_ng;
    std::srand(seed);
    for (uint32_t ng = 0; ng < new_ng_count; ng++)
    {
        size_t generated = 0;
        while (generated < 64)
        {
            uint16_t rand_num = std::rand() % total_range_buckets;
            if (rand_num_to_ng.find(rand_num) == rand_num_to_ng.end())
            {
                generated++;
                rand_num_to_ng.emplace(rand_num, ng);
            }
        }
    }

    std::unordered_map<NodeGroupId, BucketMigrateInfo> migrate_plan;

    std::shared_lock<std::shared_mutex> lk(meta_data_mux_);
    auto it = rand_num_to_ng.begin();
    for (uint16_t bucket_id = 0; bucket_id < total_range_buckets; bucket_id++)
    {
        // The buckets larger than the last random number belongs to the
        // first virtual node on the ring.
        if (it != rand_num_to_ng.end() && bucket_id >= it->first)
        {
            it++;
        }
        NodeGroupId ng_id = it == rand_num_to_ng.end()
                                ? rand_num_to_ng.begin()->second
                                : it->second;
        // This function should only be called as preferred leader of node
        // group.
        NodeGroupId cur_owner =
            GetBucketInfoInternal(bucket_id, Sharder::Instance().NodeId())
                ->BucketOwner();
        if (cur_owner != ng_id)
        {
            auto ins_pair = migrate_plan.try_emplace(cur_owner);
            BucketMigrateInfo &bucket_migrate_info = ins_pair.first->second;
            bucket_migrate_info.bucket_ids_.push_back(bucket_id);
            bucket_migrate_info.new_owner_ngs_.push_back(ng_id);
        }
    }
    return migrate_plan;
}

#ifdef RANGE_PARTITION_ENABLED
bool LocalCcShards::EnqueueRangeDataSyncTask(
    const TableName &table_name,
    uint32_t ng_id,
    int64_t ng_term,
    TableRangeEntry *range_entry,
    uint64_t data_sync_ts,
    bool is_dirty,
    bool can_be_skipped,
    std::shared_ptr<DataSyncStatus> status,
    CcHandlerResult<Void> *hres)
{
    const RangeInfo *range_info = range_entry->GetRangeInfo();
    NodeGroupId range_ng =
        GetRangeOwnerInternal(range_info->PartitionId(), ng_id)->BucketOwner();
    if (range_ng == ng_id)
    {
        auto task_limiter_key = TaskLimiterKey(ng_id,
                                               ng_term,
                                               table_name.StringView(),
                                               table_name.Type(),
                                               range_info->PartitionId());

        std::unique_lock<std::mutex> task_limiter_lk(task_limiter_mux_);
        auto iter = task_limiters_.find(task_limiter_key);
        if (iter == task_limiters_.end())
        {
            // Create task limiter
            auto limiter = task_limiters_.emplace(
                task_limiter_key, std::make_shared<DataSyncTaskLimiter>());
            // Update `latest_pending_task_ts` to higher ts if this task can be
            // skipped(also means it's `data_sync_ts_` can be adjusted).
            if (can_be_skipped)
            {
                limiter.first->second->latest_pending_task_ts_ = data_sync_ts;
            }
            // Relase `task_limiter_mux_`
            task_limiter_lk.unlock();

            // Push task to worker task queue.
            std::lock_guard<std::mutex> task_worker_lk(
                data_sync_worker_ctx_.mux_);
            data_sync_task_queue_.emplace_back(
                std::make_shared<DataSyncTask>(table_name,
                                               range_info->PartitionId(),
                                               range_info->VersionTs(),
                                               ng_id,
                                               ng_term,
                                               data_sync_ts,
                                               status,
                                               is_dirty,
                                               can_be_skipped,
                                               hres));
            return true;
        }
        else
        {
            if (can_be_skipped)
            {
                assert(hres == nullptr);
                // '0' means have no pending task on queue.
                if (iter->second->latest_pending_task_ts_ == 0)
                {
                    iter->second->latest_pending_task_ts_ = data_sync_ts;
                    iter->second->pending_tasks_.push(
                        std::make_shared<DataSyncTask>(
                            table_name,
                            range_info->PartitionId(),
                            range_info->VersionTs(),
                            ng_id,
                            ng_term,
                            data_sync_ts,
                            status,
                            is_dirty,
                            can_be_skipped,
                            hres));
                    return true;
                }
                else
                {
                    // Already has one pending task on the pending queue. We
                    // just update `latest_pending_task_ts_`.
                    iter->second->latest_pending_task_ts_ = std::max(
                        iter->second->latest_pending_task_ts_, data_sync_ts);
                    status->SetNoTruncateLog();
                    return false;
                }
            }
            else
            {
                // This task can't be skipped(DataMigration, CraeteIndex,
                // LastCheckpoint). So we push this task to the pending task
                // queue of `Limiter`
                iter->second->pending_tasks_.push(
                    std::make_shared<DataSyncTask>(table_name,
                                                   range_info->PartitionId(),
                                                   range_info->VersionTs(),
                                                   ng_id,
                                                   ng_term,
                                                   data_sync_ts,
                                                   status,
                                                   is_dirty,
                                                   can_be_skipped,
                                                   hres));
                return true;
            }
        }
    }
    else
    {
        auto new_range_ids = range_info->NewPartitionId();
        if (new_range_ids &&
            (is_dirty || range_info->DirtyTs() <= data_sync_ts))
        {
            assert(range_info->IsDirty());
            for (int32_t new_range : *new_range_ids)
            {
                NodeGroupId new_range_ng =
                    GetRangeOwnerInternal(new_range, ng_id)->BucketOwner();
                if (new_range_ng == ng_id)
                {
                    // If range is splitting and the new range falls on
                    // current node group, we might receive forwarded
                    // messages. These messages cannot be flushed into
                    // data store yet since we cannot update their slice
                    // specs. Thus the log cannot be truncated for this
                    // round of checkpoint.
                    LOG(INFO)
                        << "Unable to truncate log since " << table_name.Trace()
                        << ", range " << range_info->PartitionId()
                        << " is forwarding message to ng " << ng_id
                        << " during range split.";

                    // Mark the task as failed since we cannot gaurantee all
                    // data before data sync ts is flushed.
                    std::lock_guard<std::mutex> status_lk(status->mux_);
                    status->err_code_ = CcErrorCode::PIN_RANGE_SLICE_FAILED;
                    break;
                }
            }
        }

        return false;
    }
}
#else
bool LocalCcShards::EnqueueDataSyncTaskToCore(
    const TableName &table_name,
    uint32_t ng_id,
    int64_t ng_term,
    uint64_t data_sync_ts,
    uint16_t core_idx,
    bool is_dirty,
    bool can_be_skipped,
    std::shared_ptr<DataSyncStatus> status,
    CcHandlerResult<Void> *hres,
    bool send_cache_for_migration,
    std::function<bool(size_t)> filter_lambda)
{
    auto task_limiter_key = TaskLimiterKey(
        ng_id, ng_term, table_name.StringView(), table_name.Type(), core_idx);
    std::unique_lock<std::mutex> task_limiter_lk(task_limiter_mux_);
    auto iter = task_limiters_.find(task_limiter_key);
    bool enqueued_task = false;
    if (iter == task_limiters_.end())
    {
        // Create task limiter
        auto limiter = task_limiters_.emplace(
            task_limiter_key, std::make_shared<DataSyncTaskLimiter>());
        // Update `latest_pending_task_ts` to higher ts if this task can be
        // skipped(also means it's `data_sync_ts_` can be adjusted).
        if (can_be_skipped)
        {
            limiter.first->second->latest_pending_task_ts_ = data_sync_ts;
        }
        // Relase `task_limiter_mux_`
        task_limiter_lk.unlock();

        auto task = std::make_shared<DataSyncTask>(table_name,
                                                   0,
                                                   0,
                                                   ng_id,
                                                   ng_term,
                                                   data_sync_ts,
                                                   status,
                                                   is_dirty,
                                                   can_be_skipped,
                                                   hres,
                                                   filter_lambda,
                                                   send_cache_for_migration);

        // Push task to worker task queue.
        {
            std::lock_guard<std::mutex> task_worker_lk(
                data_sync_worker_ctx_.mux_);
            data_sync_task_queue_[core_idx].emplace_back(task);
            data_sync_worker_ctx_.cv_.notify_all();
        }
        enqueued_task = true;
    }
    else
    {
        if (can_be_skipped)
        {
            assert(hres == nullptr);
            // '0' means have no pending task on queue. so we push this task
            // to PendingTaskQueue
            if (iter->second->latest_pending_task_ts_ == 0)
            {
                iter->second->latest_pending_task_ts_ = data_sync_ts;
                iter->second->pending_tasks_.push(
                    std::make_shared<DataSyncTask>(table_name,
                                                   0,
                                                   0,
                                                   ng_id,
                                                   ng_term,
                                                   data_sync_ts,
                                                   status,
                                                   is_dirty,
                                                   can_be_skipped,
                                                   hres,
                                                   filter_lambda,
                                                   send_cache_for_migration));
                enqueued_task = true;
            }
            else
            {
                // Already has one pending task on the PendingTaskQueue. We
                // just update `latest_pending_task_ts_` to higher ts.
                iter->second->latest_pending_task_ts_ = std::max(
                    iter->second->latest_pending_task_ts_, data_sync_ts);
                status->SetNoTruncateLog();
            }
        }
        else
        {
            // This task can't be skipped(DataMigration, CraeteIndex,
            // LastCheckpoint). Because these operations need to explicitly
            // flush data into storage, rather than relying on other
            // checkpoint tasks.
            iter->second->pending_tasks_.push(
                std::make_shared<DataSyncTask>(table_name,
                                               0,
                                               0,
                                               ng_id,
                                               ng_term,
                                               data_sync_ts,
                                               status,
                                               is_dirty,
                                               can_be_skipped,
                                               hres,
                                               filter_lambda,
                                               send_cache_for_migration));
            enqueued_task = true;
        }
    }
    return enqueued_task;
}
#endif

void LocalCcShards::EnqueueDataSyncTaskForTable(
    const TableName &table_name,
    uint32_t ng_id,
    int64_t ng_term,
    uint64_t data_sync_ts,
    uint64_t &last_data_sync_ts,
    bool is_dirty,
    bool can_be_skipped,
    std::shared_ptr<DataSyncStatus> status,
    CcHandlerResult<Void> *hres)
{
    assert(status != nullptr);

    std::shared_lock<std::shared_mutex> meta_lk(meta_data_mux_);
    last_data_sync_ts = 0;

#ifndef RANGE_PARTITION_ENABLED
    CatalogEntry *catalog_entry = GetCatalogInternal(table_name, ng_id);
    if (!catalog_entry)
    {
        if (hres)
        {
            hres->SetFinished();
        }

        return;
    }

    last_data_sync_ts = catalog_entry->GetMinLastSyncTs();
    // Release `meta_data_mux_`
    meta_lk.unlock();

    auto core_count = cc_shards_.size();
    size_t task_cnt = 0;

    for (size_t core_idx = 0; core_idx < core_count; ++core_idx)
    {
        if (EnqueueDataSyncTaskToCore(table_name,
                                      ng_id,
                                      ng_term,
                                      data_sync_ts,
                                      core_idx,
                                      is_dirty,
                                      can_be_skipped,
                                      status,
                                      hres))
        {
            task_cnt++;
        }
    }

    {
        std::lock_guard<std::mutex> status_lk(status->mux_);
        status->unfinished_tasks_ += task_cnt;
        if (hres)
        {
            status->all_task_started_ = true;
            if (status->unfinished_tasks_ == 0)
            {
                hres->SetFinished();
                return;
            }
        }
    }

#else

    TableName range_table_name(table_name.StringView(),
                               TableType::RangePartition);
    auto ranges = GetTableRangesForATableInternal(range_table_name, ng_id);
    if (ranges == nullptr)
    {
        if (hres)
        {
            hres->SetFinished();
        }
        return;
    }

    uint32_t unfinished_task_cnt = 0;

    for (auto &range : *ranges)
    {
        if (range.second->GetLastSyncTs() > 0)
        {
            last_data_sync_ts =
                std::min(last_data_sync_ts, range.second->GetLastSyncTs());
        }

        if (EnqueueRangeDataSyncTask(table_name,
                                     ng_id,
                                     ng_term,
                                     range.second.get(),
                                     data_sync_ts,
                                     is_dirty,
                                     can_be_skipped,
                                     status,
                                     hres))
        {
            // Increment local variable to reduce lock contention.
            unfinished_task_cnt++;
        }
    }

    meta_lk.unlock();

    {
        std::lock_guard<std::mutex> status_lk(status->mux_);
        status->unfinished_tasks_ += unfinished_task_cnt;
        if (hres)
        {
            status->all_task_started_ = true;
            if (status->unfinished_tasks_ == 0)
            {
                hres->SetFinished();
                return;
            }
        }
    }

    data_sync_worker_ctx_.cv_.notify_all();
#endif
}

void LocalCcShards::EnqueueDataSyncTaskForBucket(
#ifdef RANGE_PARTITION_ENABLED
    const std::unordered_map<TableName, std::unordered_set<int32_t>>
        &ranges_in_bucket_snapshot,
#else
    const std::vector<uint16_t> &bucket_ids,
    bool send_cache_for_migration,
#endif
    uint32_t ng_id,
    int64_t ng_term,
    uint64_t data_sync_ts,
    CcHandlerResult<Void> *hres)
{
#ifdef RANGE_PARTITION_ENABLED
    std::shared_lock<std::shared_mutex> meta_lk(meta_data_mux_);
    std::shared_ptr<DataSyncStatus> status =
        std::make_shared<DataSyncStatus>(false);
    uint32_t unfinished_task_cnt = 0;
    for (auto &[range_table_name, range_ids] : ranges_in_bucket_snapshot)
    {
        TableType type;
        if (TableName::IsBase(range_table_name.StringView()))
        {
            type = TableType::Primary;
        }
        else if (TableName::IsUniqueSecondary(range_table_name.StringView()))
        {
            type = TableType::UniqueSecondary;
        }
        else
        {
            type = TableType::Secondary;
        }
        TableName table_name(range_table_name.StringView(), type);
        for (int32_t range_id : range_ids)
        {
            auto range_entry =
                GetTableRangeEntryInternal(range_table_name, ng_id, range_id);
            if (range_entry && EnqueueRangeDataSyncTask(table_name,
                                                        ng_id,
                                                        ng_term,
                                                        range_entry,
                                                        data_sync_ts,
                                                        false,
                                                        false,
                                                        status,
                                                        hres))
            {
                unfinished_task_cnt++;
            }
        }
    }

    {
        std::lock_guard<std::mutex> status_lk(status->mux_);
        status->unfinished_tasks_ += unfinished_task_cnt;
        status->all_task_started_ = true;
        if (status->unfinished_tasks_ == 0)
        {
            hres->SetFinished();
            return;
        }
    }

    data_sync_worker_ctx_.cv_.notify_all();

#else
    std::shared_lock<std::shared_mutex> meta_lk(meta_data_mux_);
    std::shared_ptr<DataSyncStatus> status =
        std::make_shared<DataSyncStatus>(false);
    size_t task_cnt = 0;
    assert(!bucket_ids.empty());
    for (auto &catalog_ng : table_catalogs_)
    {
        auto catalog_it = catalog_ng.second.find(ng_id);
        if (catalog_it == catalog_ng.second.end())
        {
            // Skip the table if it is not initialized in this ng.
            continue;
        }
        if (EnqueueDataSyncTaskToCore(
                catalog_ng.first,
                ng_id,
                ng_term,
                data_sync_ts,
                Sharder::Instance().ShardBucketIdToCoreIdx(
                    bucket_ids[0]),  // all buckets passed in should land on the
                                     // same core
                false,
                false,
                status,
                hres,
                send_cache_for_migration,
                [&bucket_ids](size_t key_hash) -> bool
                {
                    uint16_t bucket_id = key_hash & 0x3FFF;
                    for (uint16_t target_id : bucket_ids)
                    {
                        if (bucket_id == target_id)
                        {
                            return true;
                        }
                    }
                    return false;
                }))
        {
            task_cnt++;
        }
    }

    {
        std::lock_guard<std::mutex> status_lk(status->mux_);
        status->all_task_started_ = true;
        status->unfinished_tasks_ += task_cnt;
        if (status->unfinished_tasks_ == 0)
        {
            hres->SetFinished();
            return;
        }
    }
#endif
}

void LocalCcShards::Terminate()
{
    // Terminate the data sync task worker thds.
    data_sync_worker_ctx_.Terminate();

    // Terminate the flush worker thds.
    flush_data_worker_ctx_.Terminate();

#ifdef RANGE_PARTITION_ENABLED
    // Terminate the slice update worker thds.
    slice_update_worker_ctx_.Terminate();

    range_split_worker_ctx_.Terminate();
#endif

    if (realtime_sampling_)
    {
        statistics_worker_ctx_.Terminate();
    }
}

void LocalCcShards::DataSyncWorker(size_t worker_idx)
{
    std::unique_lock<std::mutex> task_worker_lk(data_sync_worker_ctx_.mux_);

#ifdef RANGE_PARTITION_ENABLED
    auto &task_queue = data_sync_task_queue_;
    (void) worker_idx;
#else
    auto &task_queue = data_sync_task_queue_[worker_idx];
#endif

    while (data_sync_worker_ctx_.status_ == WorkerStatus::Active)
    {
        data_sync_worker_ctx_.cv_.wait(
            task_worker_lk,
            [this, &task_queue]
            {
                return !task_queue.empty() ||
                       data_sync_worker_ctx_.status_ != WorkerStatus::Active;
            });

        if (task_queue.empty())
        {
            continue;
        }

        DataSync(task_worker_lk, worker_idx);
    }

    // Handle pending tasks.
    while (!task_queue.empty())
    {
        DataSync(task_worker_lk, worker_idx);
    }
}

#ifdef RANGE_PARTITION_ENABLED

void LocalCcShards::DataSync(std::unique_lock<std::mutex> &task_worker_lk,
                             size_t worker_idx)
{
    std::shared_ptr<void> lock_task_worker(nullptr,
                                           [&task_worker_lk](void *)
                                           {
                                               if (!task_worker_lk.owns_lock())
                                               {
                                                   // Need to gain ownership
                                                   // before returnning back to
                                                   // caller.
                                                   task_worker_lk.lock();
                                               }
                                           });
    std::shared_ptr<DataSyncTask> data_sync_task =
        data_sync_task_queue_.front();
    data_sync_task_queue_.pop_front();
    task_worker_lk.unlock();

    // Whether other task worker is processing this table.
    const TableName &table_name = data_sync_task->table_name_;
    uint32_t ng_id = data_sync_task->node_group_id_;
    int64_t expected_ng_term = data_sync_task->node_group_term_;
    bool is_dirty = data_sync_task->is_dirty_;
    uint64_t last_sync_ts = 0;
    bool need_process = false;
    std::shared_lock<std::shared_mutex> meta_lk(meta_data_mux_);

    int32_t range_id = data_sync_task->range_id_;
    TableName range_tbl_name{table_name.StringView(),
                             TableType::RangePartition};
    TableRangeEntry *range_entry =
        GetTableRangeEntryInternal(range_tbl_name, ng_id, range_id);
    if (range_entry == nullptr)
    {
        // table dropped
        data_sync_task->SetError(CcErrorCode::REQUESTED_TABLE_NOT_EXISTS);
        ClearAllPendingTasks(ng_id, expected_ng_term, table_name, range_id);
    }
    else
    {
        NodeGroupId range_ng =
            GetRangeOwnerInternal(range_id, ng_id)->BucketOwner();
        if (range_ng == ng_id)
        {
            if (data_sync_task->SyncTsAdjustable())
            {
                auto task_limiter_key = TaskLimiterKey(ng_id,
                                                       expected_ng_term,
                                                       table_name.StringView(),
                                                       table_name.Type(),
                                                       range_id);
                std::lock_guard<std::mutex> task_limiter_lk(task_limiter_mux_);
                auto iter = task_limiters_.find(task_limiter_key);
                assert(iter != task_limiters_.end());
                uint64_t latest_pending_task_ts =
                    iter->second->UnsetLatestPendingTs();
                data_sync_task->data_sync_ts_ = std::max(
                    latest_pending_task_ts, data_sync_task->data_sync_ts_);
                data_sync_task->UnsetSyncTsAdjustable();
            }

            // For dirty tables (create index in process), data older than
            // last sync ts will be continously written into memory. We
            // cannot rely on last sync ts to determin if there's dirty data
            // that needs to be flushed.
            last_sync_ts = is_dirty ? 0 : range_entry->GetLastSyncTs();
            if (data_sync_task->data_sync_ts_ <= last_sync_ts && !is_dirty)
            {
                data_sync_task->SetFinish();
                PopPendingTask(ng_id, expected_ng_term, table_name, range_id);
                assert(need_process == false);
            }
            else
            {
                need_process = true;
            }
        }
        else
        {
            // range no longer belong to this ng.
            data_sync_task->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            PopPendingTask(ng_id, expected_ng_term, table_name, range_id);
        }
    }

    if (!need_process)
    {
        return;
    }

    meta_lk.unlock();

    // Check the leader
    int64_t ng_term = Sharder::Instance().TryPinNodeGroupData(ng_id);
    if (ng_term < 0 || ng_term != expected_ng_term)
    {
        LOG(ERROR) << "DataSync: node is not the leader of ng#" << ng_id
                   << " with leader term: " << ng_term
                   << ", and the expected leader term: " << expected_ng_term;

        // Finish this task and notify the caller.
        data_sync_task->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
        PopPendingTask(ng_id, expected_ng_term, table_name, range_id);

        if (ng_term >= 0)
        {
            Sharder::Instance().UnpinNodeGroupData(ng_id);
        }

        return;
    }

    assert(ng_term == expected_ng_term);
    if (Sharder::Instance().LeaderTerm(ng_id) < 0)
    {
        // node is still candidate leader of node group. Log replay is not
        // finished yet. In this case we can flush data and kickout cce, but we
        // cannot truncate redo log based on this data sync ts since we might
        // miss the data that has not been recovered yet.
        data_sync_task->SetErrorCode(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
    }

    // guard to unpin node group on finish.
    std::shared_ptr<void> defer_unpin(
        nullptr,
        [ng_id](void *) { Sharder::Instance().UnpinNodeGroupData(ng_id); });
    // Process this task.
    // 1. Get a new txm and init
    TransactionExecution *data_sync_txm =
        txservice::NewTxInit(tx_service_,
                             IsolationLevel::RepeatableRead,
                             CcProtocol::Locking,
                             ng_id);

    if (data_sync_txm == nullptr)
    {
        LOG(ERROR) << "DataSync init data sync transaction failed.";

        std::lock_guard<std::mutex> task_worker_lk(data_sync_worker_ctx_.mux_);
        data_sync_task_queue_.emplace_front(data_sync_task);
        data_sync_worker_ctx_.cv_.notify_one();
        return;
    }

    // 2. Issue read catalog tx_request to acquire read lock on catalog
    // cc_entry using base table name, and acquire read lock in one
    // shard is good enough to block schema change.
    // If table_name has been dropped at this point, read lock would
    // not be acquired.
    const TableName base_table_name{table_name.GetBaseTableNameSV(),
                                    TableType::Primary};

    CatalogKey table_key(base_table_name);
    TxKey tbl_tx_key{&table_key};
    CatalogRecord catalog_rec;

    ReadTxRequest read_req;
    read_req.Set(
        &catalog_ccm_name, &tbl_tx_key, &catalog_rec, false, false, true);
    data_sync_txm->Execute(&read_req);
    read_req.Wait();

    RecordStatus rec_status = read_req.Result().first;

    if (read_req.IsError() || rec_status != RecordStatus::Normal)
    {
        // Use AbortTxRequest to release read lock.
        txservice::AbortTx(data_sync_txm);

        if (rec_status != RecordStatus::Normal)
        {
            LOG(ERROR) << "DataSync try to add read lock on deleted table, "
                          "table name: "
                       << table_key.Name().StringView();
            // If table is deleted(!Normal), skip the table. Return finish
            // directly.
            data_sync_task->SetError();

            ClearAllPendingTasks(ng_id, expected_ng_term, table_name, range_id);
        }
        else
        {
            LOG(ERROR) << "DataSync add read lock on table failed, "
                          "table name: "
                       << table_key.Name().StringView();

            // If read lock acquire failed, retry next time.
            // Put back into the beginning.

            std::lock_guard<std::mutex> task_worker_lk(
                data_sync_worker_ctx_.mux_);
            data_sync_task_queue_.emplace_front(data_sync_task);
            data_sync_worker_ctx_.cv_.notify_one();
        }
        return;
    }

    // Get the table schema. The basic strategy is that, 1) for pk table, must
    // use the current table schema, 2) for the [Unique]secondary table, only
    // in the case that there is no key schema corresponding to the index table
    // in the current table schema, should use the dirty table schema.

    // FIXME(lokax): catalog_rec::CopySchema() If the node group is not
    // pinned
    const TableSchema *table_schema = catalog_rec.Schema();
    if (table_name.Type() == TableType::Secondary ||
        table_name.Type() == TableType::UniqueSecondary)
    {
        if (catalog_rec.DirtySchema() &&
            !table_schema->IndexKeySchema(table_name))
        {
            table_schema = catalog_rec.DirtySchema();
        }
        // For index table, if this table has been dropped, skip it.
        if (!table_schema->IndexKeySchema(table_name))
        {
            // Use CommitTx to release read lock.
            txservice::CommitTx(data_sync_txm);
            LOG(INFO) << "DataSync on the deleted table: " << table_name.Trace()
                      << ". Return finish directly.";

            data_sync_task->SetFinish();
            PopPendingTask(ng_id, expected_ng_term, table_name, range_id);

            return;
        }
    }

    // Lock bucket so that bucket cannot be migrated away during data sync.
    uint64_t expected_range_version = data_sync_task->range_version_;
    RangeBucketRecord bucket_rec;
    RangeBucketKey bucket_key(
        Sharder::Instance().MapRangeIdToBucketId(range_id));
    TxKey bucket_tx_key{&bucket_key};
    read_req.Reset();
    read_req.Set(&range_bucket_ccm_name,
                 &bucket_tx_key,
                 &bucket_rec,
                 false,
                 false,
                 true);
    data_sync_txm->Execute(&read_req);
    read_req.Wait();

    if (read_req.IsError())
    {
        // Use AbortTxRequest to release read lock.
        LOG(ERROR) << "DataSync add read lock on bucket failed, "
                      "bucket id: "
                   << Sharder::Instance().MapRangeIdToBucketId(range_id);

        txservice::AbortTx(data_sync_txm);
        // If read lock acquire failed, retry next time.
        // Put back into the beginning.
        std::lock_guard<std::mutex> task_worker_lk(data_sync_worker_ctx_.mux_);
        data_sync_task_queue_.emplace_front(data_sync_task);
        data_sync_worker_ctx_.cv_.notify_one();
        return;
    }

    // Now that we have acquired read lock on catalog and bucket, there won't be
    // any ddl on this range. Update store_range and check if this range is
    // still owned by this node group.
    range_entry = const_cast<TableRangeEntry *>(
        GetTableRangeEntry(range_tbl_name, ng_id, range_id));
    if (bucket_rec.GetBucketInfo()->BucketOwner() != ng_id)
    {
        assert(range_entry);
        // Skip the range, it might be dropped or migrated away.
        // Use AbortTxRequest to release read lock.
        txservice::AbortTx(data_sync_txm);

        data_sync_task->SetError();
        PopPendingTask(ng_id, expected_ng_term, table_name, range_id);

        return;
    }
    else if (range_entry->Version() != expected_range_version)
    {
        // If the range spec has been updated since we create the task,
        // we might miss the data in the new range during data sync scan.
        // So we need to mark this round of data sync as failed.
        LOG(WARNING) << "DataSync range version mismatch with data sync ts: "
                     << data_sync_task->data_sync_ts_;
        data_sync_task->SetErrorCode(CcErrorCode::GET_RANGE_ID_ERR);
    }

    // 3. Scan records.
    // The data sync worker thread is the owner of those vectors.
    std::vector<std::vector<FlushRecord>> data_sync_vecs;
    std::vector<std::vector<FlushRecord>> archive_vecs;
    std::vector<std::vector<TxKey>> mv_base_vecs;

    for (size_t i = 0; i < cc_shards_.size(); i++)
    {
        data_sync_vecs.emplace_back();
        archive_vecs.emplace_back();
        mv_base_vecs.emplace_back();
    }

    bool scan_data_drained = false;
    // Note: `DataSyncScanCc` needs to ensure that no two ckpt_rec with the
    // same Key can be generated. Our subsequent algorithms are based on this
    // assumption.

    TxKey start_tx_key = range_entry->GetRangeInfo()->StartTxKey();
    TxKey end_tx_key = range_entry->GetRangeInfo()->EndTxKey();

    DataSyncScanCc scan_cc(table_name,
                           0,
                           last_sync_ts,
                           data_sync_task->data_sync_ts_,
                           ng_id,
                           ng_term,
                           cc_shards_.size(),
                           DATA_SYNC_SCAN_BATCH_SIZE,
                           data_sync_txm->TxNumber(),
                           &start_tx_key,
                           &end_tx_key,
                           false,
                           false,
                           false,
                           table_schema->Version());

    while (!scan_data_drained)
    {
        for (size_t i = 0; i < cc_shards_.size(); i++)
        {
            EnqueueToCcShard(i, &scan_cc);
        }
        scan_cc.Wait();

        if (scan_cc.IsError())
        {
            LOG(INFO) << "DataSync scan failed on table "
                      << table_name.StringView() << " with error code: "
                      << static_cast<uint32_t>(scan_cc.ErrorCode());

            txservice::AbortTx(data_sync_txm);

            std::lock_guard<std::mutex> task_worker_lk(
                data_sync_worker_ctx_.mux_);
            data_sync_task_queue_.emplace_front(data_sync_task);
            data_sync_worker_ctx_.cv_.notify_one();

            return;
        }
        else
        {
            scan_data_drained = true;

            for (size_t i = 0; i < cc_shards_.size(); i++)
            {
                size_t offset = data_sync_vecs[i].size();

                for (size_t j = 0; j < scan_cc.accumulated_scan_cnt_[i]; ++j)
                {
                    auto &rec = scan_cc.DataSyncVec(i)[j];
                    // Clone key
                    data_sync_vecs[i].emplace_back(rec.Key().Clone(),
                                                   rec.GetPayload(),
                                                   rec.payload_status_,
                                                   rec.commit_ts_,
                                                   rec.cce_,
                                                   rec.delta_size_);
                }

                for (size_t j = 0; j < scan_cc.ArchiveVec(i).size(); ++j)
                {
                    auto &rec = scan_cc.ArchiveVec(i)[j];
                    rec.SetKey(
                        data_sync_vecs[i][rec.GetKeyIndex() + offset].Key());
                }

                for (size_t j = 0; j < scan_cc.MoveBaseIdxVec(i).size(); ++j)
                {
                    size_t key_idx = scan_cc.MoveBaseIdxVec(i)[j];
                    TxKey key_raw = data_sync_vecs[i][key_idx + offset].Key();
                    mv_base_vecs[i].emplace_back(std::move(key_raw));
                }

                // if the data is drained
                scan_data_drained = scan_cc.IsDrained(i) && scan_data_drained;

                // move the bucket into the tank
                std::move(scan_cc.ArchiveVec(i).begin(),
                          scan_cc.ArchiveVec(i).end(),
                          std::back_inserter(archive_vecs.at(i)));
            }
            scan_cc.Reset();
        }
    }

    std::unique_ptr<std::vector<FlushRecord>> data_sync_vec =
        std::make_unique<std::vector<FlushRecord>>();

    std::unique_ptr<std::vector<FlushRecord>> archive_vec =
        std::make_unique<std::vector<FlushRecord>>();

    std::unique_ptr<std::vector<TxKey>> mv_base_vec =
        std::make_unique<std::vector<TxKey>>();

    // Sort output vectors in key sorting order.
    auto key_greater = [](const TxKey &r1, const TxKey &r2) -> bool
    { return r2 < r1; };
    auto rec_greater = [](const FlushRecord &r1, const FlushRecord &r2) -> bool
    { return r2.Key() < r1.Key(); };

    MergeSortedVectors(
        std::move(mv_base_vecs), *mv_base_vec, key_greater, false);

    // Set the ckpt_ts_ of a cc entry repeatedly, which might cause the ccentry
    // become invalid in between. But, there should be no duplication here. we
    // don't need to remove duplicate record.
    MergeSortedVectors(
        std::move(data_sync_vecs), *data_sync_vec, rec_greater, false);

    // For archive vec we don't need to worry about duplicate causing
    // issue since we're not visiting their cc entry. Also we cannot
    // rely on key compare to dedup archive vec since a key could have
    // multiple version of archive versions.
    MergeSortedVectors(
        std::move(archive_vecs), *archive_vec, rec_greater, false);

    // 4. Process the data sync vec
    if (data_sync_vec->size() != 0 || archive_vec->size() != 0 ||
        mv_base_vec->size() != 0)
    {
        // 4.1 For range partition, execute range split if necessary using
        // seperate thread.
        // Fetch range slices info from data store if it's not loaded yet.
        // Pin the range so that it can't be kicked out during data sync.
        StoreRange *store_range = range_entry->PinStoreRange();
        if (!store_range)
        {
            RunOnTxProcessorCc cc([](CcShard &ccs) {});
            // Since node group is pinned, range entry will not be dropped
            // by ClearNodeGroupCc. This is the only thread that will update
            // table ranges for this table, so we don't need meta data shared
            // lock here.
            range_entry->FetchRangeSlices(
                range_tbl_name, &cc, ng_id, ng_term, cc_shards_[0].get());
            cc.Wait();
            while (cc.IsError())
            {
                // Failed to fetch range slice. If error is caused by
                // data store unreachable, retry.
                if (cc.ErrorCode() == CcErrorCode::DATA_STORE_ERR)
                {
                    std::this_thread::sleep_for(std::chrono::milliseconds(500));
                    cc.Reset();
                    range_entry->FetchRangeSlices(range_tbl_name,
                                                  &cc,
                                                  ng_id,
                                                  ng_term,
                                                  cc_shards_[0].get());
                    cc.Wait();
                }
                else if (cc.ErrorCode() == CcErrorCode::NG_TERM_CHANGED)
                {
                    data_sync_task->SetError();
                    PopPendingTask(
                        ng_id, expected_ng_term, table_name, range_id);
                    // Term is invalid, we are no longer leader. Abort data
                    // sync.
                    txservice::AbortTx(data_sync_txm);
                    return;
                }
                else
                {
                    assert(false);
                }
            }
            store_range = range_entry->PinStoreRange();
            assert(store_range != nullptr);
        }

        // Update slice specs with the scanned data
        std::vector<TxKey> split_keys;
        bool ret =
            UpdateSliceAndCalculateRangeUpdate(table_name,
                                               table_schema,
                                               ng_id,
                                               ng_term,
                                               *data_sync_vec,
                                               data_sync_task->data_sync_ts_,
                                               store_range,
                                               split_keys);

        if (!ret)
        {
            LOG(ERROR) << "Pre-data_sync slice update failed on table "
                       << table_name.StringView();

            data_sync_task->SetError();
            // Handle the pending tasks for the same range
            PopPendingTask(ng_id, expected_ng_term, table_name, range_id);

            range_entry->UnPinStoreRange();
            txservice::AbortTx(data_sync_txm);

            return;
        }

        if (!split_keys.empty())
        {
            std::lock_guard<std::mutex> range_split_worker_lk(
                range_split_worker_ctx_.mux_);

            auto range_split_task =
                std::make_unique<RangeSplitTask>(data_sync_task,
                                                 table_schema,
                                                 std::move(data_sync_vec),
                                                 std::move(archive_vec),
                                                 std::move(mv_base_vec),
                                                 std::move(split_keys),
                                                 range_entry,
                                                 data_sync_txm,
                                                 defer_unpin);

            pending_range_split_task_.push_back(std::move(range_split_task));
            range_split_worker_ctx_.cv_.notify_one();

            return;
        }

        // 4.2 Flush records into data store if the range in which the
        // records locate need't to split.
        std::unique_lock<std::mutex> worker_lk(flush_data_worker_ctx_.mux_);
        pending_flush_work_.emplace_back(data_sync_task,
                                         table_schema,
                                         std::move(data_sync_vec),
                                         std::move(archive_vec),
                                         std::move(mv_base_vec),
                                         data_sync_txm,
                                         false,
                                         worker_idx);
        flush_data_worker_ctx_.cv_.notify_one();
    }
    else
    {
        // Update the task status and last sync ts of this range.
        range_entry->UpdateLastDataSyncTS(data_sync_task->data_sync_ts_);

        data_sync_task->SetFinish();
        PopPendingTask(ng_id, expected_ng_term, table_name, range_id);
        // Nothing to flush in this range.
        // Commit the data sync txm
        txservice::CommitTx(data_sync_txm);
    }
}
#else

void LocalCcShards::PostProcessDataSyncTask(std::shared_ptr<DataSyncTask> task,
                                            TransactionExecution *data_sync_txm,
                                            CatalogEntry *catalog_entry,
                                            DataSyncTask::CkptErrorCode err,
                                            size_t worker_idx)
{
    std::unique_lock<bthread::Mutex> flight_task_lk(task->flight_task_mux_);
    int64_t flight_task_cnt = --task->flight_task_cnt_;

    if (task->ckpt_err_ == DataSyncTask::CkptErrorCode::NO_ERROR)
    {
        task->ckpt_err_ = err;
    }

    auto task_ckpt_err = task->ckpt_err_;

    if (flight_task_cnt > 0 &&
        flight_task_cnt < flush_data_worker_ctx_.worker_num_ * 3)
    {
        task->flight_task_cv_.notify_one();
    }

    flight_task_lk.unlock();

    // All flush tasks of this task are finished (flight_task_cnt == 0)
    if (flight_task_cnt == 0)
    {
        if (task_ckpt_err == DataSyncTask::CkptErrorCode::NO_ERROR)
        {
            // Commit the data sync txm
            txservice::CommitTx(data_sync_txm);
            PopPendingTask(task->node_group_id_,
                           task->node_group_term_,
                           task->table_name_,
                           worker_idx);

            bool res = store_hd_->CkptEnd(task->table_name_,
                                          catalog_entry->schema_.get(),
                                          task->node_group_id_,
                                          task->node_group_term_);
            if (!res)
            {
                task->SetError(CcErrorCode::DATA_STORE_ERR);
                return;
            }

            if (catalog_entry)
            {
                catalog_entry->UpdateLastDataSyncTS(task->data_sync_ts_,
                                                    worker_idx);
            }

            task->SetFinish();
        }
        else if (task_ckpt_err == DataSyncTask::CkptErrorCode::SCAN_ERROR)
        {
            txservice::AbortTx(data_sync_txm);

            std::lock_guard<std::mutex> task_worker_lk(
                data_sync_worker_ctx_.mux_);
            data_sync_task_queue_[worker_idx].emplace_front(task);
            data_sync_worker_ctx_.cv_.notify_all();
        }
        else
        {
            assert(task_ckpt_err == DataSyncTask::CkptErrorCode::FLUSH_ERROR);
            CcErrorCode err_code =
                Sharder::Instance().LeaderTerm(task->node_group_id_) > 0
                    ? CcErrorCode::DATA_STORE_ERR
                    : CcErrorCode::REQUESTED_NODE_NOT_LEADER;

            task->SetError(err_code);

            PopPendingTask(task->node_group_id_,
                           task->node_group_term_,
                           task->table_name_,
                           worker_idx);

            txservice::AbortTx(data_sync_txm);
        }
    }
}

void LocalCcShards::DataSync(std::unique_lock<std::mutex> &task_worker_lk,
                             size_t worker_idx)
{
    std::shared_ptr<void> lock_task_worker(nullptr,
                                           [&task_worker_lk](void *)
                                           {
                                               if (!task_worker_lk.owns_lock())
                                               {
                                                   // Need to gain ownership
                                                   // before returnning back to
                                                   // caller.
                                                   task_worker_lk.lock();
                                               }
                                           });

    std::shared_ptr<DataSyncTask> data_sync_task =
        data_sync_task_queue_[worker_idx].front();
    data_sync_task_queue_[worker_idx].pop_front();
    // Release `worker ctx mux`
    task_worker_lk.unlock();

    // Whether other task worker is processing this table.
    const TableName &table_name = data_sync_task->table_name_;
    uint32_t ng_id = data_sync_task->node_group_id_;
    int64_t expected_ng_term = data_sync_task->node_group_term_;
    bool is_dirty = data_sync_task->is_dirty_;

    std::shared_lock<std::shared_mutex> meta_lk(meta_data_mux_);
    uint64_t last_sync_ts = 0;
    bool need_process = false;

    const TableName primary_base_table_name{table_name.GetBaseTableNameSV(),
                                            TableType::Primary};
    CatalogEntry *catalog_entry =
        GetCatalogInternal(primary_base_table_name, ng_id);

    if (catalog_entry == nullptr)
    {
        data_sync_task->SetError(CcErrorCode::REQUESTED_TABLE_NOT_EXISTS);

        ClearAllPendingTasks(ng_id, expected_ng_term, table_name, worker_idx);
    }
    else
    {
        if (data_sync_task->SyncTsAdjustable())
        {
            auto task_limiter_key = TaskLimiterKey(ng_id,
                                                   expected_ng_term,
                                                   table_name.StringView(),
                                                   table_name.Type(),
                                                   worker_idx);
            std::lock_guard<std::mutex> task_limiter_lk(task_limiter_mux_);
            auto iter = task_limiters_.find(task_limiter_key);
            assert(iter != task_limiters_.end());
            // Now, This task is runing. we call `UnsetLatestPendingTs` to make
            // anothe task could push to PendingTaskQueue.
            uint64_t lateset_pending_task_ts =
                iter->second->UnsetLatestPendingTs();
            data_sync_task->data_sync_ts_ = std::max(
                lateset_pending_task_ts, data_sync_task->data_sync_ts_);
            data_sync_task->UnsetSyncTsAdjustable();
        }

        // For dirty tables (create index in process), data older than
        // last sync ts will be continously written into memory. We cannot
        // rely on last sync ts to determin if there's dirty data that needs
        // to be flushed.
        last_sync_ts = is_dirty ? 0 : catalog_entry->GetLastSyncTs(worker_idx);
        if (data_sync_task->data_sync_ts_ <= last_sync_ts && !is_dirty)
        {
            data_sync_task->SetFinish();

            PopPendingTask(ng_id, expected_ng_term, table_name, worker_idx);

            assert(need_process == false);
        }
        else
        {
            need_process = true;
        }
    }

    if (!need_process)
    {
        return;
    }

    meta_lk.unlock();

    // Check the leader
    int64_t ng_term = Sharder::Instance().TryPinNodeGroupData(ng_id);
    if (ng_term < 0 || ng_term != expected_ng_term)
    {
        LOG(ERROR) << "DataSync: node is not the leader of ng#" << ng_id
                   << " with leader term: " << ng_term
                   << ", and the expected leader term: " << expected_ng_term;
        // Finish this task and notify the caller.
        data_sync_task->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);

        PopPendingTask(ng_id, expected_ng_term, table_name, worker_idx);

        if (ng_term >= 0)
        {
            Sharder::Instance().UnpinNodeGroupData(ng_id);
        }

        return;
    }

    assert(ng_term == expected_ng_term);
    if (Sharder::Instance().LeaderTerm(ng_id) < 0)
    {
        // node is still candidate leader of node group. Log replay is not
        // finished yet. In this case we can flush data and kickout cce, but we
        // cannot truncate redo log based on this data sync ts since we might
        // miss the data that has not been recovered yet.
        data_sync_task->SetErrorCode(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
    }

    // guard to unpin node group on finish.
    std::shared_ptr<void> defer_unpin(
        nullptr,
        [ng_id](void *) { Sharder::Instance().UnpinNodeGroupData(ng_id); });
    // Process this task.
    // 1. Get a new txm and init
    TransactionExecution *data_sync_txm =
        txservice::NewTxInit(tx_service_,
                             IsolationLevel::RepeatableRead,
                             CcProtocol::Locking,
                             ng_id);

    if (data_sync_txm == nullptr)
    {
        LOG(ERROR) << "DataSync init data sync transaction failed.";

        std::lock_guard<std::mutex> task_worker_lk(data_sync_worker_ctx_.mux_);
        data_sync_task_queue_[worker_idx].emplace_front(data_sync_task);
        data_sync_worker_ctx_.cv_.notify_all();
        return;
    }

    // 2. Issue read catalog tx_request to acquire read lock on catalog
    // cc_entry using base table name, and acquire read lock in one
    // shard is good enough to block schema change.
    // If table_name has been dropped at this point, read lock would
    // not be acquired.
    const TableName base_table_name{table_name.GetBaseTableNameSV(),
                                    TableType::Primary};

    CatalogKey table_key(base_table_name);
    TxKey tbl_tx_key(&table_key);
    CatalogRecord catalog_rec;

    ReadTxRequest read_req;
    read_req.Set(
        &catalog_ccm_name, &tbl_tx_key, &catalog_rec, false, false, true);
    data_sync_txm->Execute(&read_req);
    read_req.Wait();

    RecordStatus rec_status = read_req.Result().first;

    if (read_req.IsError() || rec_status != RecordStatus::Normal)
    {
        // Use AbortTxRequest to release read lock.
        txservice::AbortTx(data_sync_txm);

        if (rec_status != RecordStatus::Normal)
        {
            LOG(ERROR) << "DataSync try to add read lock on deleted table, "
                          "table name: "
                       << table_key.Name().StringView();

            // If table is deleted(!Normal), skip the table. Return finish
            // directly.
            data_sync_task->SetError();

            ClearAllPendingTasks(
                ng_id, expected_ng_term, table_name, worker_idx);
        }
        else
        {
            LOG(ERROR) << "DataSync add read lock on table failed, "
                          "table name: "
                       << table_key.Name().StringView();

            // If read lock acquire failed, retry next time.
            // Put back into the beginning.
            std::lock_guard<std::mutex> task_worker_lk(
                data_sync_worker_ctx_.mux_);
            data_sync_task_queue_[worker_idx].emplace_front(data_sync_task);
            data_sync_worker_ctx_.cv_.notify_all();
        }
        return;
    }

    // Get the table schema. The basic strategy is that, 1) for pk table, must
    // use the current table schema, 2) for the [Unique]secondary table, only
    // in the case that there is no key schema corresponding to the index table
    // in the current table schema, should use the dirty table schema.
    const TableSchema *table_schema = catalog_rec.Schema();
    if (table_name.Type() == TableType::Secondary ||
        table_name.Type() == TableType::UniqueSecondary)
    {
        if (catalog_rec.DirtySchema() &&
            !table_schema->IndexKeySchema(table_name))
        {
            table_schema = catalog_rec.DirtySchema();
        }
        // For index table, if this table has been dropped, skip it.
        if (!table_schema->IndexKeySchema(table_name))
        {
            // Use CommitTx to release read lock.
            txservice::CommitTx(data_sync_txm);
            LOG(INFO) << "DataSync on the deleted table: " << table_name.Trace()
                      << ". Return finish directly.";

            data_sync_task->SetFinish();

            PopPendingTask(ng_id, expected_ng_term, table_name, worker_idx);

            return;
        }
    }

    meta_lk.lock();
    catalog_entry = GetCatalogInternal(primary_base_table_name, ng_id);
    assert(catalog_entry != nullptr);
    meta_lk.unlock();

    // 3. Scan records.
    bool scan_data_drained = false;
    static constexpr size_t rec_size_limit = 9216;

    auto data_sync_vec = std::make_unique<std::vector<FlushRecord>>();
    auto archive_vec = std::make_unique<std::vector<FlushRecord>>();
    auto mv_base_vec = std::make_unique<std::vector<TxKey>>();

    // Note: `DataSyncScanCc` needs to ensure that no two ckpt_rec with the
    // same Key can be generated. Our subsequent algorithms are based on this
    // assumption.
    DataSyncScanCc scan_cc(table_name,
                           0,
                           last_sync_ts,
                           data_sync_task->data_sync_ts_,
                           ng_id,
                           ng_term,
                           1,
                           DATA_SYNC_SCAN_BATCH_SIZE,
                           data_sync_txm->TxNumber(),
                           nullptr,
                           nullptr,
                           data_sync_task->forward_cache_,
                           true,
                           data_sync_task->filter_lambda_,
                           table_schema->Version());

    {
        // DataSync Worker will call PostProcessDataSyncTask() to decrement
        // flight task count
        std::lock_guard<bthread::Mutex> flight_task_lk(
            data_sync_task->flight_task_mux_);
        data_sync_task->flight_task_cnt_ += 1;
    }

    assert(worker_idx < cc_shards_.size());

    while (!scan_data_drained)
    {
        EnqueueToCcShard(worker_idx, &scan_cc);
        scan_cc.Wait();

        if (scan_cc.IsError() &&
            scan_cc.ErrorCode() != CcErrorCode::LOG_NOT_TRUNCATABLE)
        {
            LOG(ERROR) << "DataSync scan failed on table "
                       << table_name.StringView() << " with error code: "
                       << static_cast<int>(scan_cc.ErrorCode());

            PostProcessDataSyncTask(std::move(data_sync_task),
                                    data_sync_txm,
                                    catalog_entry,
                                    DataSyncTask::CkptErrorCode::SCAN_ERROR,
                                    worker_idx);

            return;
        }
        else
        {
            if (scan_cc.ErrorCode() == CcErrorCode::LOG_NOT_TRUNCATABLE)
            {
                data_sync_task->status_->SetNoTruncateLog();
            }

            scan_data_drained = true;

            // Send cache to target node group if needed.
            if (data_sync_task->forward_cache_)
            {
                std::shared_lock<std::shared_mutex> meta_lk(meta_data_mux_);
                const auto bucket_infos = GetAllBucketInfos(ng_id);
                if (bucket_infos == nullptr)
                {
                    // no longer node group owner, abort the task
                    LOG(ERROR) << "DataSync: Failed to get bucket infos for "
                                  "ng#"
                               << ng_id;
                    PostProcessDataSyncTask(
                        std::move(data_sync_task),
                        data_sync_txm,
                        catalog_entry,
                        DataSyncTask::CkptErrorCode::SCAN_ERROR,
                        worker_idx);
                    return;
                }

                std::unordered_map<NodeGroupId, UploadBatchClosure *>
                    send_cache_closures;
                for (size_t idx = 0; idx < scan_cc.accumulated_scan_cnt_[0];
                     idx++)
                {
                    FlushRecord &ref = scan_cc.DataSyncVec(0)[idx];
                    uint16_t bucket_id = ref.Key().Hash() & 0x3FFF;
                    NodeGroupId dest_ng =
                        bucket_infos->at(bucket_id)->DirtyBucketOwner();
                    assert(dest_ng != UINT32_MAX);

                    // Put the record into the request for this node group.
                    auto ins_res = send_cache_closures.try_emplace(dest_ng);
                    remote::UploadBatchRequest *req_ptr = nullptr;
                    if (ins_res.second)
                    {
                        uint32_t node_id =
                            Sharder::Instance().LeaderNodeId(dest_ng);
                        std::shared_ptr<brpc::Channel> channel =
                            Sharder::Instance().GetCcNodeServiceChannel(
                                node_id);
                        if (channel == nullptr)
                        {
                            // Fail to establish the channel to the tx node.
                            // Just skip the cache sending since it is a best
                            // effort try to performance improvement.
                            LOG(ERROR) << "UploadBatch: Failed to init the "
                                          "channel of ng#"
                                       << dest_ng;
                            send_cache_closures.erase(ins_res.first);
                        }
                        else
                        {
                            // Create a closure for the first time.
                            UploadBatchClosure *upload_batch_closure =
                                new UploadBatchClosure(
                                    [this,
                                     ng_term,
                                     ng_id,
                                     data_sync_task,
                                     data_sync_txm,
                                     catalog_entry,
                                     worker_idx](CcErrorCode res_code,
                                                 int32_t dest_ng_term)
                                    {
                                        bool term_match =
                                            Sharder::Instance().CheckLeaderTerm(
                                                ng_id, ng_term);
                                        // We don't care if
                                        // the cache send
                                        // was succeed or
                                        // not since it's a
                                        // best effort
                                        // move. Just pass in no error
                                        // so that it won't cause data sync
                                        // failure.
                                        PostProcessDataSyncTask(
                                            std::move(data_sync_task),
                                            data_sync_txm,
                                            // catalog entry ptr is only valid
                                            // if the term hasn't changed
                                            term_match ? catalog_entry
                                                       : nullptr,
                                            DataSyncTask::CkptErrorCode::
                                                NO_ERROR,
                                            worker_idx);
                                    },
                                    10000,
                                    false);

                            upload_batch_closure->SetChannel(node_id, channel);

                            ins_res.first->second = upload_batch_closure;
                            req_ptr =
                                upload_batch_closure->UploadBatchRequest();
                            req_ptr->set_node_group_id(dest_ng);
                            req_ptr->set_node_group_term(-1);
                            req_ptr->set_table_name_str(table_name.String());
                            req_ptr->set_table_type(
                                remote::ToRemoteType::ConvertTableType(
                                    table_name.Type()));
                            req_ptr->set_kind(
                                remote::UploadBatchKind::DIRTY_BUCKET_DATA);
                            req_ptr->set_batch_size(0);
                            // keys
                            req_ptr->clear_keys();
                            // records
                            req_ptr->clear_records();
                            // commit_ts
                            req_ptr->clear_commit_ts();
                            // rec_status
                            req_ptr->clear_rec_status();
                        }
                    }
                    else
                    {
                        req_ptr = ins_res.first->second->UploadBatchRequest();
                    }

                    if (req_ptr)
                    {
                        std::string *keys_str = req_ptr->mutable_keys();
                        std::string *rec_status_str =
                            req_ptr->mutable_rec_status();
                        std::string *commit_ts_str =
                            req_ptr->mutable_commit_ts();
                        size_t len_sizeof = sizeof(uint64_t);
                        const char *val_ptr = nullptr;
                        ref.Key().Serialize(*keys_str);
                        if (ref.payload_status_ == RecordStatus::Normal)
                        {
                            std::string *recs_str = req_ptr->mutable_records();
                            ref.Payload()->Serialize(*recs_str);
                        }
                        const char *status_ptr = reinterpret_cast<const char *>(
                            &(ref.payload_status_));
                        rec_status_str->append(status_ptr,
                                               sizeof(RecordStatus));
                        val_ptr =
                            reinterpret_cast<const char *>(&(ref.commit_ts_));
                        commit_ts_str->append(val_ptr, len_sizeof);
                        req_ptr->set_batch_size(req_ptr->batch_size() + 1);
                    }
                }
                meta_lk.unlock();

                {
                    std::unique_lock<bthread::Mutex> flight_lk(
                        data_sync_task->flight_task_mux_);
                    data_sync_task->flight_task_cnt_ +=
                        send_cache_closures.size();
                }
                // Send cache to target node groups.
                for (auto &[ng, upload_batch_closure] : send_cache_closures)
                {
                    remote::CcRpcService_Stub stub(
                        upload_batch_closure->Channel());
                    brpc::Controller *cntl_ptr =
                        upload_batch_closure->Controller();
                    cntl_ptr->set_timeout_ms(10000);
                    // Asynchronous mode
                    stub.UploadBatch(
                        upload_batch_closure->Controller(),
                        upload_batch_closure->UploadBatchRequest(),
                        upload_batch_closure->UploadBatchResponse(),
                        upload_batch_closure);
                }
            }

            size_t offset = data_sync_vec->size();

            for (size_t j = 0; j < scan_cc.accumulated_scan_cnt_[0]; ++j)
            {
                auto &rec = scan_cc.DataSyncVec(0)[j];
                // Note. Clone key instead of move key. The memory of
                // rec.Key() will be reused to avoid memory allocation.
                if (rec.cce_)
                {
                    // cce_ is null means the key is already persisted on kv, so
                    // we don't need to put it into the flush vec.
                    data_sync_vec->emplace_back(rec.Key().Clone(),
                                                rec.GetPayload(),
                                                rec.payload_status_,
                                                rec.commit_ts_,
                                                rec.cce_,
                                                rec.delta_size_);
                }
            }

            for (size_t j = 0; j < scan_cc.ArchiveVec(0).size(); ++j)
            {
                auto &rec = scan_cc.ArchiveVec(0)[j];
                // Note. We need to ensure the copy constructor of
                // FlushRecord could not be called.
                rec.SetKey((*data_sync_vec)[rec.GetKeyIndex() + offset].Key());
            }

            for (size_t j = 0; j < scan_cc.MoveBaseIdxVec(0).size(); ++j)
            {
                size_t key_idx = scan_cc.MoveBaseIdxVec(0)[j];
                TxKey key_raw = (*data_sync_vec)[key_idx + offset].Key();
                mv_base_vec->emplace_back(std::move(key_raw));
            }

            std::move(scan_cc.ArchiveVec(0).begin(),
                      scan_cc.ArchiveVec(0).end(),
                      std::back_inserter(*archive_vec));

            scan_data_drained = scan_cc.IsDrained(0) && scan_data_drained;

            if ((data_sync_vec->size() + archive_vec->size() +
                     mv_base_vec->size() >
                 rec_size_limit) ||
                scan_cc.force_flush_)
            {
                {
                    std::unique_lock<bthread::Mutex> flight_task_lk(
                        data_sync_task->flight_task_mux_);
                    if (data_sync_task->ckpt_err_ ==
                        DataSyncTask::CkptErrorCode::FLUSH_ERROR)
                    {
                        break;
                    }

                    // Since redis clones record out into FlushRecord during
                    // data sync scan, we want to back pressure data sync
                    // scan so that it does not alloc too much memory.
                    while (data_sync_task->flight_task_cnt_ >
                           flush_data_worker_ctx_.worker_num_ * 3)
                    {
                        data_sync_task->flight_task_cv_.wait(flight_task_lk);
                    }
                    // Flush worker will call PostProcessDataSyncTask() to
                    // decrement flight task count.
                    data_sync_task->flight_task_cnt_ += 1;
                }

                {
                    std::lock_guard<std::mutex> worker_lk(
                        flush_data_worker_ctx_.mux_);
                    pending_flush_work_.emplace_back(data_sync_task,
                                                     table_schema,
                                                     std::move(data_sync_vec),
                                                     std::move(archive_vec),
                                                     std::move(mv_base_vec),
                                                     data_sync_txm,
                                                     false,
                                                     worker_idx);

                    flush_data_worker_ctx_.cv_.notify_one();
                }

                data_sync_vec = std::make_unique<std::vector<FlushRecord>>();

                archive_vec = std::make_unique<std::vector<FlushRecord>>();

                mv_base_vec = std::make_unique<std::vector<TxKey>>();
            }

            scan_cc.Reset();
        }
    }

    if (data_sync_vec->size() > 0 || archive_vec->size() > 0 ||
        mv_base_vec->size() > 0)
    {
        std::unique_lock<bthread::Mutex> flight_task_lk(
            data_sync_task->flight_task_mux_);
        if (data_sync_task->ckpt_err_ == DataSyncTask::CkptErrorCode::NO_ERROR)
        {
            data_sync_task->flight_task_cnt_ += 1;
            flight_task_lk.unlock();

            std::lock_guard<std::mutex> worker_lk(flush_data_worker_ctx_.mux_);
            pending_flush_work_.emplace_back(data_sync_task,
                                             table_schema,
                                             std::move(data_sync_vec),
                                             std::move(archive_vec),
                                             std::move(mv_base_vec),
                                             data_sync_txm,
                                             false,
                                             worker_idx);
            flush_data_worker_ctx_.cv_.notify_one();
        }
    }

    PostProcessDataSyncTask(std::move(data_sync_task),
                            data_sync_txm,
                            catalog_entry,
                            DataSyncTask::CkptErrorCode::NO_ERROR,
                            worker_idx);
}
#endif

void LocalCcShards::PopPendingTask(NodeGroupId ng_id,
                                   int64_t ng_term,
                                   const TableName &table_name,
#ifdef RANGE_PARTITION_ENABLED
                                   uint32_t range_id
#else
                                   uint16_t core_idx
#endif
)
{
    assert(!table_name.IsMeta());
#ifdef RANGE_PARTITION_ENABLED
    auto task_limiter_key = TaskLimiterKey(
        ng_id, ng_term, table_name.StringView(), table_name.Type(), range_id);
#else
    auto task_limiter_key = TaskLimiterKey(
        ng_id, ng_term, table_name.StringView(), TableType::Primary, core_idx);
#endif

    std::unique_lock<std::mutex> task_limiter_lk(task_limiter_mux_);
    auto iter = task_limiters_.find(task_limiter_key);
    assert(iter != task_limiters_.end());

    if (!iter->second->pending_tasks_.empty())
    {
        std::shared_ptr<DataSyncTask> task =
            iter->second->pending_tasks_.front();
        iter->second->pending_tasks_.pop();
        task_limiter_lk.unlock();

        std::lock_guard<std::mutex> task_worker_lk(data_sync_worker_ctx_.mux_);
#ifdef RANGE_PARTITION_ENABLED
        data_sync_task_queue_.push_back(std::move(task));
        data_sync_worker_ctx_.cv_.notify_one();
#else

        data_sync_task_queue_[core_idx].push_back(std::move(task));
        data_sync_worker_ctx_.cv_.notify_all();
#endif
    }
    else
    {
        task_limiters_.erase(iter);
    }
}

void LocalCcShards::ClearAllPendingTasks(NodeGroupId ng_id,
                                         int64_t ng_term,
                                         const TableName &table_name,
#ifdef RANGE_PARTITION_ENABLED
                                         uint32_t range_id
#else
                                         uint16_t core_idx
#endif
)
{
    assert(!table_name.IsMeta());

#ifdef RANGE_PARTITION_ENABLED
    auto task_limiter_key = TaskLimiterKey(
        ng_id, ng_term, table_name.StringView(), table_name.Type(), range_id);
#else
    auto task_limiter_key = TaskLimiterKey(
        ng_id, ng_term, table_name.StringView(), TableType::Primary, core_idx);
#endif

    std::lock_guard<std::mutex> task_limiter_lk(task_limiter_mux_);
    auto iter = task_limiters_.find(task_limiter_key);
    assert(iter != task_limiters_.end());

    while (!iter->second->pending_tasks_.empty())
    {
        auto &task = iter->second->pending_tasks_.front();
        task->SetError(CcErrorCode::REQUESTED_TABLE_NOT_EXISTS);
        iter->second->pending_tasks_.pop();
    }

    task_limiters_.erase(iter);
}

bool LocalCcShards::UpdateSliceAndCalculateRangeUpdate(
    const TableName &table_name,
    const TableSchema *schema,
    NodeGroupId node_group_id,
    int64_t node_group_term,
    std::vector<FlushRecord> &flush_batch,
    uint64_t data_sync_ts,
    StoreRange *store_range,
    std::vector<TxKey> &splitting_info)
{
    std::mutex work_sender_mux;
    std::condition_variable work_sender_cv;
    size_t slice_update_done = 0;
    size_t slice_load_cnt = 0;
    bool fail = false;
    size_t slice_start_idx = 0;
    auto batch_it = flush_batch.begin();

    auto lower_bound_cmp = [](const FlushRecord &rec, const TxKey &key)
    { return rec.Key() < key; };

    TxKey range_end_tx_key = store_range->RangeEndTxKey();

    while (batch_it != flush_batch.end())
    {
        TxKey slice_start_key = batch_it->Key();
        StoreSlice *curr_slice = store_range->FindSlice(slice_start_key);
        TxKey slice_end_tx_key = curr_slice->EndTxKey();

        auto slice_end_it =
            slice_end_tx_key.KeyPtr() == range_end_tx_key.KeyPtr()
                ? flush_batch.end()
                : std::lower_bound(batch_it,
                                   flush_batch.end(),
                                   slice_end_tx_key,
                                   lower_bound_cmp);

        size_t slice_end_idx = std::distance(flush_batch.begin(), slice_end_it);
        int64_t slice_delta_size = 0;
        uint64_t slice_size = 0;

        for (; batch_it != slice_end_it; ++batch_it)
        {
            slice_delta_size += batch_it->delta_size_;
        }

        int64_t sum = curr_slice->Size() + slice_delta_size;
        slice_size = sum >= 0 ? sum : 0;
        curr_slice->SetPostCkptSize(slice_size);
        // Skip performing the update slice spec operation when the size of a
        // single item exceeds the slice upper bound.
        if (slice_delta_size > 0 && slice_size > StoreSlice::slice_upper_bound)
        {
            // Since update slice specs might need loading from
            // data store, hand it off to the worker and move on
            // to the next slice.
            slice_load_cnt++;
            EnqueueUpdateSliceTask(data_sync_ts,
                                   node_group_id,
                                   node_group_term,
                                   table_name,
                                   schema,
                                   store_range,
                                   curr_slice,
                                   slice_start_idx,
                                   slice_end_idx,
                                   flush_batch,
                                   work_sender_mux,
                                   work_sender_cv,
                                   slice_update_done,
                                   fail);
        }
        batch_it = slice_end_it;
        slice_start_idx = slice_end_idx;
        batch_it = slice_end_it;
    }

    {
        // Wait for all slice specs in this range are updated.
        std::unique_lock<std::mutex> work_sender_lk(work_sender_mux);
        work_sender_cv.wait(work_sender_lk,
                            [&slice_update_done, &slice_load_cnt]
                            { return slice_load_cnt == slice_update_done; });
        if (fail)
        {
            return false;
        }
    }

    size_t post_ckpt_size = store_range->PostCkptSize();
    if (post_ckpt_size > StoreRange::range_max_size)
    {
        splitting_info = store_range->CalculateRangeSplitKeys(table_name,
                                                              schema,
                                                              node_group_id,
                                                              node_group_term,
                                                              data_sync_ts,
                                                              post_ckpt_size);
        if (!splitting_info.empty())
        {
            return true;
        }
    }

    return true;
}

#ifdef RANGE_PARTITION_ENABLED
void LocalCcShards::SplitFlushRange(
    std::unique_lock<std::mutex> &task_worker_lk)
{
    std::shared_ptr<void> lock_task_worker(nullptr,
                                           [&task_worker_lk](void *)
                                           {
                                               if (!task_worker_lk.owns_lock())
                                               {
                                                   // Need to gain ownership
                                                   // before returnning back to
                                                   // caller.
                                                   task_worker_lk.lock();
                                               }
                                           });

    std::unique_ptr<RangeSplitTask> range_split_task =
        std::move(pending_range_split_task_.front());
    pending_range_split_task_.pop_front();

    task_worker_lk.unlock();

    TableRangeEntry *range_entry = range_split_task->range_entry_;
    std::shared_ptr<DataSyncTask> &data_sync_task =
        range_split_task->data_sync_task_;
    const TableName &table_name = data_sync_task->table_name_;
    const TableName range_table_name{table_name.String(),
                                     TableType::RangePartition};
    auto &split_keys = range_split_task->split_keys_;
    const TableSchema *table_schema = range_split_task->schema_;
    TransactionExecution *split_txm = range_split_task->data_sync_txm_;
    NodeGroupId node_group = data_sync_task->node_group_id_;

    std::string log_output(
        "Splitting table " + table_name.String() + " range " +
        std::to_string(range_entry->GetRangeInfo()->PartitionId()) + " into " +
        std::to_string(split_keys.size() + 1) + " ranges. New range ids ");
    // Request for new range ids from data store. The new range ids returned
    // by data store are always unique.
    std::vector<std::pair<TxKey, int32_t>> new_range_ids;
    for (TxKey &new_key : split_keys)
    {
        int32_t new_part_id;
        if (!store_hd_->GetNextRangePartitionId(table_name, &new_part_id))
        {
            LOG(ERROR) << "Split range failed due to unable to get next "
                          "partition id. table_name = "
                       << table_name.StringView();

            range_entry->UnPinStoreRange();
            data_sync_task->SetError(CcErrorCode::DATA_STORE_ERR);

            PopPendingTask(node_group,
                           data_sync_task->node_group_term_,
                           table_name,
                           data_sync_task->range_id_);
            txservice::AbortTx(split_txm);

            return;
        }
        log_output.append(std::to_string(new_part_id) + ",");
        new_range_ids.emplace_back(std::move(new_key), new_part_id);
    }

    // Start the SplitFlush tx. This would split the range, flush the data
    // and update slice metadata.
    log_output.append(" txn: " + std::to_string(split_txm->TxNumber()));
    LOG(INFO) << log_output;
    if (realtime_sampling_)
    {
        // We should consider again whether should we broadcast statistics here.
        bool updated_since_sync = false;
        std::unique_ptr<remote::NodeGroupSamplePool> sample_pool =
            table_schema->StatisticsObject()->MakeBroadcastSamplePool(
                node_group, table_name, &updated_since_sync);
        if (updated_since_sync)
        {
            BroadcastIndexStatistics(
                split_txm, node_group, table_name, table_schema, *sample_pool);
        }
    }

    DLOG(INFO) << "Begin to do range split flush, range_id:"
               << range_entry->GetRangeInfo()->PartitionId()
               << ", new range count:" << new_range_ids.size()
               << ", data size:" << range_split_task->data_sync_vec_->size();

    SplitFlushTxRequest split_req(table_name,
                                  table_schema,
                                  range_entry->RangeSlices(),
                                  range_entry->GetRangeInfo(),
                                  std::move(new_range_ids),
                                  data_sync_task->data_sync_ts_,
                                  std::move(*range_split_task->data_sync_vec_),
                                  std::move(*range_split_task->archive_vec_),
                                  std::move(*range_split_task->mv_base_vec_));

    split_txm->Execute(&split_req);
    split_req.Wait();
    if (split_req.IsError() || !split_req.Result())
    {
        LOG(ERROR) << "Split range on table " << table_name.StringView()
                   << " partition "
                   << range_entry->GetRangeInfo()->PartitionId() << " failed.";

        range_entry->UnPinStoreRange();

        data_sync_task->SetError();

        PopPendingTask(node_group,
                       data_sync_task->node_group_term_,
                       table_name,
                       data_sync_task->range_id_);
        txservice::AbortTx(split_txm);

        return;
    }

    range_entry->UpdateLastDataSyncTS(data_sync_task->data_sync_ts_);
    range_entry->UnPinStoreRange();

    data_sync_task->SetFinish();

    PopPendingTask(node_group,
                   data_sync_task->node_group_term_,
                   table_name,
                   data_sync_task->range_id_);

    LOG(INFO) << "Split range on table " << range_table_name.StringView()
              << " partition " << range_entry->GetRangeInfo()->PartitionId()
              << " succeeded.";
    txservice::CommitTx(split_txm);
}
#endif

void LocalCcShards::FlushData(std::unique_lock<std::mutex> &flush_worker_lk)
{
    // Retrieve first pending work and pop it.
    FlushDataTask &cur_work = pending_flush_work_.back();
    uint32_t node_group = cur_work.node_group_id_;
    int64_t leader_term = cur_work.node_group_term_;
    TableName table_name = cur_work.table_name_;
    const TableSchema *schema = cur_work.schema_;
#ifdef RANGE_PARTITION_ENABLED
    uint64_t data_sync_ts = cur_work.data_sync_ts_;
#else
    size_t scan_task_worker_idx = cur_work.scan_task_worker_idx_;
#endif

    bool during_range_split = cur_work.during_range_split;
    std::unique_ptr<std::vector<FlushRecord>> data_sync_vec_owner,
        archive_vec_owner;
    std::vector<FlushRecord> *data_sync_vec, *archive_vec;
    std::unique_ptr<std::vector<TxKey>> mv_base_owner;
    std::vector<TxKey> *mv_base_vec;
#ifdef RANGE_PARTITION_ENABLED
    bool vec_owner = cur_work.vec_owner_;
#endif
    if (cur_work.vec_owner_)
    {
        data_sync_vec_owner = std::move(cur_work.data_sync_vec_);
        data_sync_vec = data_sync_vec_owner.get();
        archive_vec_owner = std::move(cur_work.archive_vec_);
        archive_vec = archive_vec_owner.get();
        mv_base_owner = std::move(cur_work.mv_base_vec_);
        mv_base_vec = mv_base_owner.get();
    }
    else
    {
        data_sync_vec = cur_work.data_sync_vec_ptr_;
        archive_vec = cur_work.archive_vec_ptr_;
        mv_base_vec = cur_work.mv_base_vec_ptr_;
    }

    CcHandlerResult<Void> *hand_res = cur_work.hand_res_;
    std::shared_ptr<DataSyncTask> data_sync_task = cur_work.data_sync_task_;
    TransactionExecution *data_sync_txm = cur_work.data_sync_txm_;

    pending_flush_work_.pop_back();
    flush_worker_lk.unlock();

    bool succ = true;
    bool flush_ret = true;

    // Check the leader
    // Try to pin node group data to avoid the potentail heap-use-after-free
    // error about the cc entry and table ranges info. NOTE: The
    // `RangeRecord.range_info_` which will be used during PutAll is a raw
    // pointer that points to range info in TableRangeEntry stored in local
    // cc shards.
    int64_t ng_term = Sharder::Instance().TryPinNodeGroupData(node_group);
    // Defer unpin node group data.
    std::shared_ptr<void> defer_unpin(
        nullptr,
        [node_group, ng_term](void *)
        {
            if (ng_term >= 0)
            {
                Sharder::Instance().UnpinNodeGroupData(node_group);
            }
        });

    if (ng_term < 0 || ng_term != leader_term)
    {
        LOG(ERROR) << "FlushData: node is not the leader of ng#" << node_group
                   << ", with current leader term: " << ng_term
                   << ", and the expected leader term: " << leader_term;
        succ = false;
    }
    else
    {
        bool has_data = !data_sync_vec->empty();
        if (EnableMvcc())
        {
            has_data =
                has_data || !archive_vec->empty() || !mv_base_vec->empty();
        }

        // Flush to data store if this node group leader term does not
        // change
        if (has_data)
        {
            // Flushes to the data store
            if (EnableMvcc() && mv_base_vec->size() > 0)
            {
                flush_ret = store_hd_->CopyBaseToArchive(
                    *mv_base_vec, node_group, table_name, schema);
                if (!flush_ret)
                {
                    LOG(ERROR) << "DataSync CopyBaseToArchive flush to kv "
                                  "storage failed";
                }
            }

            if (flush_ret && !data_sync_vec->empty())
            {
                flush_ret = store_hd_->PutAll(
                    *data_sync_vec, table_name, schema, node_group);
                if (!flush_ret)
                {
                    LOG(ERROR) << "DataSync PutAll flush to kv "
                                  "storage failed";
                }
            }

            if (flush_ret && EnableMvcc())
            {
                flush_ret =
                    store_hd_->PutArchivesAll(node_group,
                                              table_name,
                                              schema->GetKVCatalogInfo(),
                                              *archive_vec);
                if (!flush_ret)
                {
                    LOG(ERROR) << "DataSync PutArchivesAll flush to "
                                  "kv storage failed";
                }
            }

            // If flush to data store succeeds, update the ckpt_ts for each
            // entry in ccmap to latest checkpoint version's commit_ts.
            if (flush_ret)
            {
                if (!during_range_split)
                {
#ifdef RANGE_PARTITION_ENABLED
                    std::vector<std::vector<FlushRecord *>>
                        flush_records_per_core(Count());
                    // In the real world, the amount of data on all cores is not
                    // exactly equal. So we reserve 512 extra spaces to avoid
                    // resize
                    size_t reserve_size =
                        (data_sync_vec->size() / Count()) + 512;
                    for (size_t core_idx = 0; core_idx < Count(); ++core_idx)
                    {
                        flush_records_per_core[core_idx].reserve(reserve_size);
                    }

                    for (size_t i = 0; i < data_sync_vec->size(); ++i)
                    {
                        auto &ref = data_sync_vec->at(i);
                        assert(ref.cce_ != nullptr);

                        size_t key_core_idx =
                            (ref.Key().Hash() & 0x3FF) % Count();
                        flush_records_per_core[key_core_idx].emplace_back(&ref);
                    }

                    UpdateCceCkptTsCc update_cce_ckpt_cc(
                        std::move(flush_records_per_core),
                        Count(),
                        node_group,
                        leader_term);
                    for (size_t core_idx = 0; core_idx < Count(); ++core_idx)
                    {
                        EnqueueToCcShard(core_idx, &update_cce_ckpt_cc);
                    }
                    update_cce_ckpt_cc.Wait();
#else
                    UpdateCceCkptTsCc update_cce_ckpt_cc(
                        data_sync_vec, 1, node_group, leader_term);
                    EnqueueToCcShard(scan_task_worker_idx, &update_cce_ckpt_cc);
                    update_cce_ckpt_cc.Wait();
#endif
                }

                if (data_sync_vec->size())
                {
#ifdef RANGE_PARTITION_ENABLED
                    // Update the slice size in data store.
                    while (!UpdateStoreSlice(table_name,
                                             data_sync_ts,
                                             node_group,
                                             *data_sync_vec,
                                             true,
                                             during_range_split))
                    {
                        // Keep retrying here since we've finished the flush
                        // already, it's too expensive to start from the
                        // beginning all over again.
                        LOG(ERROR) << "Data sync failed to update store "
                                      "slice info "
                                      "on table "
                                   << table_name.Trace() << ", retrying.";
                        std::this_thread::sleep_for(1s);
                        if (!Sharder::Instance().CheckLeaderTerm(node_group,
                                                                 leader_term))
                        {
                            LOG(ERROR) << "Leader term changed during store "
                                          "slice update";
                            succ = false;
                            break;
                        }
                    }
#endif
                }
            }
            else
            {
#ifdef RANGE_PARTITION_ENABLED
                // Reset the post ckpt size if flush failed
                bool res = UpdateStoreSlice(table_name,
                                            data_sync_ts,
                                            node_group,
                                            *data_sync_vec,
                                            false,
                                            during_range_split);
                // We're only updating in memory status here, so
                // this should always succeed.
                assert(res);
#endif
                succ = false;
            }
        } /* End of PutAll */

#ifdef RANGE_PARTITION_ENABLED
        // other wise, split flush operation will do the work
        if (vec_owner)
        {
            // reset scan start page info for the flushed ccshard
            std::vector<std::unique_ptr<std::vector<FlushRecord>>>
                data_sync_vec_per_core(Count()), archive_vec_per_core(Count());

            if (data_sync_vec_owner != nullptr)
            {
                size_t reserve_size =
                    (data_sync_vec_owner->size() / Count()) + 512;
                for (size_t core_idx = 0; core_idx < Count(); core_idx++)
                {
                    data_sync_vec_per_core[core_idx] =
                        std::make_unique<std::vector<FlushRecord>>();
                    data_sync_vec_per_core[core_idx]->reserve(reserve_size);
                }

                for (size_t i = 0; i < data_sync_vec_owner->size(); i++)
                {
                    auto flush_record = std::move(data_sync_vec_owner->at(i));
                    size_t record_core_id =
                        (flush_record.Key().Hash() & 0x3FF) % Count();
                    data_sync_vec_per_core[record_core_id]->emplace_back(
                        std::move(flush_record));
                }
            }

            if (archive_vec_owner != nullptr)
            {
                size_t reserve_size =
                    (archive_vec_owner->size() / Count()) + 512;
                for (size_t core_idx = 0; core_idx < Count(); core_idx++)
                {
                    archive_vec_per_core[core_idx] =
                        std::make_unique<std::vector<FlushRecord>>();
                    archive_vec_per_core[core_idx]->reserve(reserve_size);
                }

                for (size_t i = 0; i < archive_vec_owner->size(); i++)
                {
                    auto flush_record = std::move(archive_vec_owner->at(i));
                    size_t record_core_id =
                        (flush_record.Key().Hash() & 0x3FF) % Count();
                    archive_vec_per_core[record_core_id]->emplace_back(
                        std::move(flush_record));
                }
            }

            PostFlushDataCc reset_cc(Count(),
                                     std::move(data_sync_vec_per_core),
                                     std::move(archive_vec_per_core));
            for (size_t core_idx = 0; core_idx < Count(); ++core_idx)
            {
                EnqueueToCcShard(core_idx, &reset_cc);
            }

            reset_cc.Wait();
        }
#else
        std::vector<std::unique_ptr<std::vector<FlushRecord>>>
            data_sync_vec_per_core(1), archive_vec_per_core(1);
        data_sync_vec_per_core[0] = std::move(data_sync_vec_owner);
        archive_vec_per_core[0] = std::move(archive_vec_owner);
        PostFlushDataCc reset_cc(1,
                                 std::move(data_sync_vec_per_core),
                                 std::move(archive_vec_per_core));
        EnqueueToCcShard(scan_task_worker_idx, &reset_cc);
        reset_cc.Wait();
#endif
    } /* End of leader */

    if (data_sync_task != nullptr)
    {
        assert(data_sync_txm != nullptr);
        assert(hand_res == nullptr);

#ifdef RANGE_PARTITION_ENABLED
        if (ng_term >= 0 && ng_term == leader_term)
        {
            TableRangeEntry *range_entry =
                const_cast<TableRangeEntry *>(GetTableRangeEntry(
                    table_name, node_group, data_sync_task->range_id_));
            assert(range_entry);
            if (succ)
            {
                // Update the task status for this range.
                range_entry->UpdateLastDataSyncTS(data_sync_ts);
            }

            range_entry->UnPinStoreRange();
            PopPendingTask(
                node_group, leader_term, table_name, data_sync_task->range_id_);
        }

        if (succ)
        {
            // Commit the data sync txm
            CommitTx(data_sync_txm);
            data_sync_task->SetFinish();
        }
        else
        {
            // Abort the data sync txm
            AbortTx(data_sync_txm);

            CcErrorCode err_code =
                Sharder::Instance().LeaderTerm(node_group) > 0
                    ? CcErrorCode::DATA_STORE_ERR
                    : CcErrorCode::REQUESTED_NODE_NOT_LEADER;
            data_sync_task->SetError(err_code);
        }
#else
        CatalogEntry *catalog_entry = nullptr;
        if (ng_term >= 0 && ng_term == leader_term)
        {
            const TableName base_table_name{table_name.GetBaseTableNameSV(),
                                            TableType::Primary};
            catalog_entry = GetCatalog(base_table_name, node_group);
            assert(catalog_entry);
        }

        auto ckpt_err = DataSyncTask::CkptErrorCode::NO_ERROR;

        if (!succ)
        {
            ckpt_err = DataSyncTask::CkptErrorCode::FLUSH_ERROR;
        }

        PostProcessDataSyncTask(std::move(data_sync_task),
                                data_sync_txm,
                                catalog_entry,
                                ckpt_err,
                                scan_task_worker_idx);
#endif
    }

    if (hand_res)
    {
        assert(data_sync_task == nullptr);
        // In this case flush data is triggered by a sub op of a tx
        // operation, and the sync_status controll is done by the parent tx
        // request.
        if (!succ)
        {
            CcErrorCode err_code =
                Sharder::Instance().LeaderTerm(node_group) > 0
                    ? CcErrorCode::DATA_STORE_ERR
                    : CcErrorCode::REQUESTED_NODE_NOT_LEADER;
            hand_res->SetError(err_code);
        }
        else
        {
            hand_res->SetFinished();
        }
    }

    flush_worker_lk.lock();
}

void LocalCcShards::FlushDataWorker()
{
    std::unique_lock<std::mutex> flush_worker_lk(flush_data_worker_ctx_.mux_);
    while (flush_data_worker_ctx_.status_ == WorkerStatus::Active)
    {
        flush_data_worker_ctx_.cv_.wait(
            flush_worker_lk,
            [this]
            {
                return !pending_flush_work_.empty() ||
                       flush_data_worker_ctx_.status_ ==
                           WorkerStatus::Terminated;
            });

        if (pending_flush_work_.empty())
        {
            continue;
        }

        FlushData(flush_worker_lk);
    }

    while (!pending_flush_work_.empty())
    {
        FlushData(flush_worker_lk);
    }
}

#ifdef RANGE_PARTITION_ENABLED
void LocalCcShards::RangeSplitWorker()
{
    std::unique_lock<std::mutex> range_split_worker_lk(
        range_split_worker_ctx_.mux_);
    while (range_split_worker_ctx_.status_ == WorkerStatus::Active)
    {
        range_split_worker_ctx_.cv_.wait(
            range_split_worker_lk,
            [this]
            {
                return !pending_range_split_task_.empty() ||
                       range_split_worker_ctx_.status_ ==
                           WorkerStatus::Terminated;
            });

        if (pending_range_split_task_.empty())
        {
            continue;
        }

        SplitFlushRange(range_split_worker_lk);
    }

    while (!pending_range_split_task_.empty())
    {
        SplitFlushRange(range_split_worker_lk);
    }
}
#endif

void LocalCcShards::UpdateSliceSpecWorker()
{
    std::unique_lock<std::mutex> worker_lk(slice_update_worker_ctx_.mux_);
    while (slice_update_worker_ctx_.status_ == WorkerStatus::Active)
    {
        slice_update_worker_ctx_.cv_.wait(
            worker_lk,
            [this]
            {
                return !pending_slice_work_.empty() ||
                       slice_update_worker_ctx_.status_ ==
                           WorkerStatus::Terminated;
            });

        if (pending_slice_work_.empty())
        {
            continue;
        }

        UpdateSliceSpecWork &cur_work = pending_slice_work_.back();

        uint64_t data_sync_ts = cur_work.data_sync_ts_;
        uint32_t node_group_id = cur_work.node_group_id_;
        int64_t node_group_term = cur_work.node_group_term_;
        TableName table_name = cur_work.table_name_;
        const TableSchema *schema = cur_work.table_schema_;
        StoreRange *range = cur_work.range_;
        StoreSlice *slice = cur_work.slice_;
        size_t start_idx = cur_work.start_idx_;
        size_t end_idx = cur_work.end_idx_;
        const std::vector<FlushRecord> &flush_vec = cur_work.flush_vec_;
        std::mutex &sender_mux = cur_work.sender_mux_;
        std::condition_variable &sender_cv = cur_work.sender_cv_;
        size_t &finish_work_cnt = cur_work.finish_work_cnt_;
        bool &fail = cur_work.fail_;

        pending_slice_work_.pop_back();
        worker_lk.unlock();

        bool res = range->UpdateSliceSpec(slice,
                                          table_name,
                                          schema,
                                          node_group_id,
                                          node_group_term,
                                          data_sync_ts,
                                          flush_vec,
                                          start_idx,
                                          end_idx);
        {
            std::unique_lock<std::mutex> lk(sender_mux);
            finish_work_cnt++;
            if (!res)
            {
                fail = true;
            }
            sender_cv.notify_one();
        }
        worker_lk.lock();
    }
}

bool LocalCcShards::UpdateStoreSlice(const TableName &table_name,
                                     uint64_t ckpt_ts,
                                     NodeGroupId node_group_id,
                                     std::vector<FlushRecord> &data_sync_vec,
                                     bool flush_res,
                                     bool during_range_split)
{
    bool success = true;
    assert(data_sync_vec.size());

    uint64_t range_version = 0;
    StoreRange *range = nullptr;

    {
        std::shared_lock<std::shared_mutex> lk(meta_data_mux_);
        TableName range_table_name(table_name.StringView(),
                                   TableType::RangePartition);

        TableRangeEntry *entry = GetTableRangeEntryInternal(
            range_table_name, node_group_id, data_sync_vec[0].Key());
        assert(entry);

        range_version = entry->Version();
        // All records in data sync vec should belong to the same range.
        range = entry->RangeSlices();
        assert(range);
    }

    // Update in-memory slice size
    bool range_updated = range->UpdateSliceSizeAfterFlush(flush_res);

    if (!during_range_split)
    {
        // Update data store slice size
        if (flush_res && range_updated)
        {
            success = range->UpdateRangeSlicesInStore(
                table_name, ckpt_ts, range_version, store_hd_);
        }
    }
    // else: SplitFlushRangeOp will update range slice in store
    return success;
}

void LocalCcShards::SyncTableStatisticsWorker()
{
    std::unique_lock<std::mutex> worker_lk(statistics_worker_ctx_.mux_);
    std::unordered_map<NodeGroupId, uint64_t> ng_sync_ts;
    while (statistics_worker_ctx_.status_ == WorkerStatus::Active)
    {
        // Wake up every 10s to sync table statistics with other nodes.
        statistics_worker_ctx_.cv_.wait_for(
            worker_lk,
            10s,
            [this] {
                return statistics_worker_ctx_.status_ ==
                       WorkerStatus::Terminated;
            });

        CODE_FAULT_INJECTOR("skip_sync_table_statistics", { continue; });

        worker_lk.unlock();

        std::vector<uint32_t> node_groups =
            Sharder::Instance().LocalNodeGroups();
        for (uint32_t node_group : node_groups)
        {
            // check whether this node is group leader, pin its data if it
            // is
            int64_t leader_term =
                Sharder::Instance().TryPinNodeGroupData(node_group);
            if (leader_term < 0)
            {
                continue;
            }
            CkptTsCc ckpt_req(cc_shards_.size(), node_group);

            // Use ckpt ts as sync ts. It will be used next round to decide
            // if table has any updates since last sync.
            for (auto &ccs : cc_shards_)
            {
                ccs->Enqueue(&ckpt_req);
            }
            ckpt_req.Wait();

            uint64_t sync_ts = ckpt_req.GetCkptTs();
            bool succ = true;

            // Get table names in this node group, stats sync worker should
            // be TableName string owner.
            std::unordered_map<TableName, bool> tables =
                GetCatalogTableNameSnapshot(node_group, sync_ts);

            // Loop over all tables and sync stats.
            for (auto it = tables.begin(); it != tables.end(); ++it)
            {
                if (Sharder::Instance().LeaderTerm(node_group) != leader_term)
                {
                    // Skip the node groups that are no longer on this node.
                    break;
                }

                const TableName &table_name = it->first;
                bool is_dirty = it->second;
                if (!table_name.IsMeta())
                {
                    // Set isolation level to RepeatableRead to ensure the
                    // readlock will be set during the execution of the
                    // following ReadTxRequest.
                    TransactionExecution *txm =
                        NewTxInit(tx_service_,
                                  IsolationLevel::RepeatableRead,
                                  CcProtocol::Locking,
                                  node_group);
                    if (txm == nullptr)
                    {
                        succ = false;
                        continue;
                    }
                    const TableName base_table_name{
                        table_name.GetBaseTableNameSV(), TableType::Primary};

                    CatalogKey table_key(base_table_name);
                    TxKey tbl_tx_key{&table_key};
                    CatalogRecord catalog_rec;

                    ReadTxRequest read_req;
                    read_req.Set(&catalog_ccm_name,
                                 &tbl_tx_key,
                                 &catalog_rec,
                                 false,
                                 false,
                                 true);
                    txm->Execute(&read_req);
                    read_req.Wait();

                    RecordStatus rec_status = read_req.Result().first;
                    if (read_req.IsError() ||
                        rec_status != RecordStatus::Normal)
                    {
                        // Use AbortTxRequest to release read lock.
                        txservice::AbortTx(txm);

                        if (read_req.IsError())
                        {
                            succ = false;
                        }
                        continue;
                    }

                    const TableSchema *table_schema = catalog_rec.Schema();
                    if (is_dirty && catalog_rec.DirtySchema() &&
                        !table_schema->IndexKeySchema(table_name))
                    {
                        assert(table_name.Type() == TableType::Secondary ||
                               table_name.Type() == TableType::UniqueSecondary);
                        table_schema = catalog_rec.DirtySchema();
                    }

                    assert(table_schema != nullptr);

                    // For index table, if this table has been dropped, skip it.
                    if ((table_name.Type() == TableType::Secondary ||
                         table_name.Type() == TableType::UniqueSecondary) &&
                        table_schema->IndexKeySchema(table_name) == nullptr)
                    {
                        txservice::AbortTx(txm);
                        continue;
                    }

                    bool updated_since_sync = false;
                    std::unique_ptr<remote::NodeGroupSamplePool> sample_pool =
                        table_schema->StatisticsObject()
                            ->MakeBroadcastSamplePool(
                                node_group, table_name, &updated_since_sync);
                    if (updated_since_sync)
                    {
                        BroadcastIndexStatistics(txm,
                                                 node_group,
                                                 table_name,
                                                 table_schema,
                                                 *sample_pool);
                        table_schema->StatisticsObject()->SetUpdatedSinceSync();
                    }

                    if (Statistics::LeaderNodeGroup(table_name) == node_group)
                    {
                        // To prevent statistics in storage broken, only
                        // one node is allowed to write.
                        for (int i = 0; i < 10; i++)
                        {
                            std::unordered_map<
                                TableName,
                                std::pair<uint64_t, std::vector<TxKey>>>
                                sample_pool_map =
                                    table_schema->StatisticsObject()
                                        ->MakeStoreStatistics(
                                            &updated_since_sync);
                            if (updated_since_sync)
                            {
                                succ = store_hd_->UpsertTableStatistics(
                                    base_table_name, sample_pool_map, sync_ts);
                                if (succ)
                                {
                                    break;
                                }
                                else
                                {
                                    LOG(ERROR)
                                        << "Failed to update statistics of "
                                           "table "
                                        << table_name.Trace() << ", retrying.";
                                    std::this_thread::sleep_for(1s);
                                    // Check leader term in infinite while
                                    // loop.
                                    if (!Sharder::Instance().CheckLeaderTerm(
                                            node_group, leader_term))
                                    {
                                        LOG(ERROR)
                                            << "Leader term changed during "
                                               "table statistics update";
                                        break;
                                    }
                                }
                            }
                            else
                            {
                                break;
                            }
                        }
                        if (!succ)
                        {
                            // Set updated_since_sync_ to true, and next sync
                            // loop will retry it.
                            table_schema->StatisticsObject()
                                ->SetUpdatedSinceSync();
                        }
                    }

                    txservice::CommitTx(txm);
                }
            }

            // finish table stats sync on this node group, unpin its data
            // and clear its ccmaps and catalogs if it is no longer leader
            Sharder::Instance().UnpinNodeGroupData(node_group);
        }
        worker_lk.lock();
    }
}

GenerateSkStatus *LocalCcShards::GetGenerateSkStatus(NodeGroupId ng_id,
                                                     uint64_t tx_number,
                                                     int32_t partition_id,
                                                     int64_t tx_term)
{
    std::unique_lock<std::mutex> lk(generate_sk_mux_);

    auto ng_it = generate_sk_status_.find(ng_id);
    if (ng_it == generate_sk_status_.end())
    {
        auto insert_it = generate_sk_status_.emplace(
            std::piecewise_construct,
            std::forward_as_tuple(ng_id),
            std::forward_as_tuple(TxGenerateSkStatus()));

        ng_it = insert_it.first;
    }

    TxGenerateSkStatus &tx_status = ng_it->second;
    auto tx_it = tx_status.find(tx_number);
    if (tx_it == tx_status.end())
    {
        auto insert_it =
            tx_status.emplace(std::piecewise_construct,
                              std::forward_as_tuple(tx_number),
                              std::forward_as_tuple(RangeGenerateSkStatus()));

        tx_it = insert_it.first;
    }

    RangeGenerateSkStatus &range_status = tx_it->second;
    auto range_it = range_status.find(partition_id);
    if (range_it == range_status.end())
    {
        auto insert_it =
            range_status.emplace(std::piecewise_construct,
                                 std::forward_as_tuple(partition_id),
                                 std::forward_as_tuple(tx_term));
        range_it = insert_it.first;
    }

    return &(range_it->second);
}

void LocalCcShards::ClearGenerateSkStatus(NodeGroupId ng_id,
                                          uint64_t tx_number,
                                          int32_t partition_id)
{
    std::unique_lock<std::mutex> lk(generate_sk_mux_);

    auto ng_it = generate_sk_status_.find(ng_id);
    assert(ng_it != generate_sk_status_.end());

    TxGenerateSkStatus &tx_status = ng_it->second;
    auto tx_it = tx_status.find(tx_number);
    assert(tx_it != tx_status.end());

    RangeGenerateSkStatus &range_status = tx_it->second;
    range_status.erase(partition_id);
}

}  // namespace txservice
