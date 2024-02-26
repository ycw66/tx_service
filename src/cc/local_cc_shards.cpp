#include "cc/local_cc_shards.h"

#include <sys/stat.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <unordered_map>

#include "cc_request.h"
#include "error_messages.h"
#include "range_bucket_key_record.h"
#include "range_record.h"
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
    metrics::CommonLabels common_labels)
    : range_slice_memory_limit_(((uint64_t) MB(memory_limit_mb)) / 20),
      store_hd_(store_hd),
      node_id_(node_id),
      timer_terminate_(false),
      is_waiting_ckpt_(false),
      catalog_factory_(catalog_factory),
      system_handler_(system_handler),
      tx_service_(tx_service),
      enable_mvcc_(enable_mvcc),
      realtime_sampling_(realtime_sampling),
#ifdef EXT_TX_PROC_ENABLED
      data_sync_worker_ctx_(core_cnt >= 2 ? (core_cnt / 2) : 1),
      slice_update_worker_ctx_(core_cnt),
      flush_data_worker_ctx_(core_cnt >= 2 ? std::min(core_cnt / 2, 10) : 1),
#else
      data_sync_worker_ctx_(core_cnt),
      slice_update_worker_ctx_(core_cnt * 2),
      flush_data_worker_ctx_(std::min((int) core_cnt, 10)),
#endif
      statistics_worker_ctx_(1),
      defragment_worker_ctx_(1)
{
    using namespace std::chrono_literals;
    uint64_t ts_base = std::chrono::duration_cast<std::chrono::microseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count();
    ts_base_.store(ts_base);
    local_clock.store(ts_base);
    timer_thd_ = std::thread([this] { TimerRun(); });

    InitRangeBuckets(
        node_id, ng_configs->size(), cluster_config_version, range_bucket_seed);

    for (uint16_t thd_idx = 0; thd_idx < core_cnt; ++thd_idx)
    {
        common_labels["core_id"] = std::to_string(thd_idx);
        cc_shards_.emplace_back(std::make_unique<CcShard>(thd_idx,
                                                          core_cnt,
                                                          memory_limit_mb,
                                                          log_limit_mb,
                                                          realtime_sampling,
                                                          node_id,
                                                          *this,
                                                          catalog_factory_,
                                                          system_handler,
                                                          metrics_registry,
                                                          common_labels));
    }

    // Starts flush worker threads firstly.
    for (int id = 0; id < flush_data_worker_ctx_.worker_num_; id++)
    {
        flush_data_worker_ctx_.worker_thd_.push_back(
            std::thread([this] { FlushDataWorker(); }));
    }

    // Starts slice update worker threads.
    for (int id = 0; id < slice_update_worker_ctx_.worker_num_; id++)
    {
        slice_update_worker_ctx_.worker_thd_.push_back(
            std::thread([this] { UpdateSliceSpecWorker(); }));
    }

    // Starts datasync worker threads.
    for (int id = 0; id < data_sync_worker_ctx_.worker_num_; id++)
    {
        data_sync_worker_ctx_.worker_thd_.push_back(
            std::thread([this] { DataSyncWorker(); }));
    }

    if (realtime_sampling)
    {
        statistics_worker_ctx_.worker_thd_.push_back(
            std::thread([this] { SyncTableStatisticsWorker(); }));
    }

    defragment_worker_ctx_.worker_thd_.push_back(
        std::thread([this] { DefragmentWorker(); }));
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

uint64_t LocalCcShards::ClockTs()
{
    return LocalCcShards::local_clock.load(std::memory_order_relaxed);
}

uint64_t LocalCcShards::TsBase()
{
    return ts_base_.load(std::memory_order_acquire);
}

void LocalCcShards::UpdateTsBase(uint64_t timestamp)
{
    uint64_t tsb = ts_base_.load(std::memory_order_acquire);
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
                                   NegativeInfinity<VoidKey>::Instance(),
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
    const TxKey *start_key,
    const TxKey *end_key,
    const RangeInfo *range_info,
    std::vector<std::unique_ptr<TxKey>> &&new_range_keys,
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
         start_key,
         end_key,
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
            const TableName range_table_name = TableName{
                ds_split_range_op_msg.table_name(), TableType::RangePartition};
            const TableName base_table_name = TableName{
                range_table_name.GetBaseTableNameSV(), TableType::Primary};
            TableRangeEntry *range_entry =
                const_cast<TableRangeEntry *>(GetTableRangeEntry(
                    range_table_name, node_group_id, partition_id));
            bool res = range_entry->TrySetDataSync(true);
            // Checkpoint cannot start until recover is finished, we should be
            // the only one trying to sync the range.
            assert(res);
            TransactionExecution *txm = tx_service_->NewTx();
            ClusterConfigRecord rec;
            txm->SetRecoverTxState(txn, tx_term, commit_ts);
            ReadTxRequest read_req(&cluster_config_ccm_name,
                                   NegativeInfinity<VoidKey>::Instance(),
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
                CatalogRecord catalog_rec;

                read_req.Reset();
                read_req.Set(&catalog_ccm_name,
                             &table_key,
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
                read_req.Reset();
                read_req.Set(&range_bucket_ccm_name,
                             &bucket_key,
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
                start_key,
                end_key,
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
                range_entry->TrySetDataSync(false, nullptr, 0);
                range_entry->UnPinStoreRange();
                range_entry->PopPendingSyncTask();
                txservice::CommitTx(txm);
            }
        });

    split_recover_thd.detach();
}

void LocalCcShards::InitTableRanges(const TableName &range_table_name,
                                    std::vector<InitRangeEntry> &init_ranges,
                                    NodeGroupId ng_id,
                                    bool empty_table)
{
    std::unique_lock<std::shared_mutex> lk(meta_data_mux_);

    // Init table ranges
    assert(range_table_name.Type() == TableType::RangePartition);
    auto table_it = table_ranges_.try_emplace(range_table_name);
    auto id_table_it = table_range_ids_.try_emplace(range_table_name);
    std::unordered_map<
        NodeGroupId,
        std::map<const TxKey *, TableRangeEntry, PtrLessThan<TxKey>>>
        &ranges_of_all_ngs = table_it.first->second;
    auto ngs_it = ranges_of_all_ngs.try_emplace(ng_id);
    std::map<const TxKey *, TableRangeEntry, PtrLessThan<TxKey>> &ranges =
        ngs_it.first->second;
    auto &ids = id_table_it.first->second.try_emplace(ng_id).first->second;
    assert(init_ranges.size() > 0);

    for (size_t pidx = 0; pidx < init_ranges.size() - 1; ++pidx)
    {
        InitRangeEntry &range_entry = init_ranges[pidx];
        const TxKey *range_start_key;

        if (range_entry.key_ == nullptr)
        {
            // nullptr means negative inf key.
            range_start_key = catalog_factory_->NegativeInfKey();
        }
        else
        {
            range_start_key = range_entry.key_.get();
        }

        auto res = ranges.try_emplace(range_start_key,
                                      std::move(range_entry.key_),
                                      nullptr,
                                      range_entry.version_ts_,
                                      range_entry.partition_id_);
        InitRangeEntry &next_range_entry = init_ranges[pidx + 1];
        const TxKey *next_start_key;
        if (res.second)
        {
            // If range info for this table is initialized for the first
            // time, use the key in InitRangeEntry
            next_start_key = next_range_entry.key_.get();
        }
        else
        {
            // otherwise use the existing tx key stored in table_ranges_
            auto next_range_entry_it = ranges.find(next_range_entry.key_.get());
            next_start_key = next_range_entry_it->first;
        }
        res.first->second.SetRangeEndKey(next_start_key);
        ids.try_emplace(range_entry.partition_id_, &res.first->second);
    }

    InitRangeEntry &last_range_entry = init_ranges.back();
    const TxKey *range_start_key;
    if (last_range_entry.key_ == nullptr)
    {
        range_start_key = catalog_factory_->NegativeInfKey();
    }
    else
    {
        range_start_key = last_range_entry.key_.get();
    }

    auto res = ranges.try_emplace(range_start_key,
                                  std::move(last_range_entry.key_),
                                  nullptr,
                                  last_range_entry.version_ts_,
                                  last_range_entry.partition_id_);
    if (empty_table)
    {
        assert(init_ranges.size() == 1);
        std::vector<std::pair<TxKey::Uptr, uint32_t>> slices;
        slices.emplace_back(nullptr, 0);
        int64_t mem_change =
            res.first->second.InitRangeSlices(std::move(slices), ng_id, true);
        if (mem_change > 0)
        {
            IncreaseRangeSliceMemUsage(mem_change);
        }
        else if (mem_change < 0)
        {
            DecreaseRangeSliceMemUsage(-mem_change);
        }
    }
    ids.try_emplace(last_range_entry.partition_id_, &res.first->second);
}

std::map<const TxKey *, TableRangeEntry, PtrLessThan<TxKey>>
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

std::map<const TxKey *, TableRangeEntry, PtrLessThan<TxKey>>
    *LocalCcShards::GetTableRangesForATable(const TableName &range_table_name,
                                            const NodeGroupId ng_id)
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
        auto &ranges_of_all_ngs = table_it->second;
        for (auto &ng_range : ranges_of_all_ngs)
        {
            for (auto &[key, range] : ng_range.second)
            {
                std::shared_lock<std::shared_mutex> lk(range.mux_);
                if (range.RangeSlices())
                {
                    DecreaseRangeSliceMemUsage(range.RangeSlices()->MemUsage());
                }
            }
        }
        ranges_of_all_ngs.erase(ng_id);
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
        for (auto &ng_range : table_range.second)
        {
            for (auto &[key, range] : ng_range.second)
            {
                std::shared_lock<std::shared_mutex> lk(range.mux_);
                if (range.RangeSlices())
                {
                    DecreaseRangeSliceMemUsage(range.RangeSlices()->MemUsage());
                }
            }
        }
        table_range.second.erase(ng_id);
    }
    for (auto &range_id : table_range_ids_)
    {
        range_id.second.erase(ng_id);
    }
}

void LocalCcShards::KickoutRangeSlices()
{
    size_t target_memory_size = range_slice_memory_limit_ / 10 * 9;
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
                std::shared_lock<std::shared_mutex> lk(range_entry.mux_);
                if (range_entry.RangeSlices())
                {
                    uint64_t last_accessed =
                        range_entry.RangeSlices()->LastAccessedTs();
                    if (current_ts > last_accessed &&
                        current_ts - last_accessed > 600000000)
                    {
                        lk.unlock();
                        size_t decreased = range_entry.DropStoreRange();
                        if (decreased > 0 &&
                            DecreaseRangeSliceMemUsage(decreased) <=
                                target_memory_size)
                        {
                            // We've cleaned up enough memory space.
                            return;
                        }
                    }
                    else if (current_ts > last_accessed &&
                             current_ts - last_accessed > 4000000)
                    {
                        // Put the store range into buffer for sort.
                        // Only kickout range slices that are not
                        // accessed for more than 4 seconds.
                        scanned_ranges.emplace_back(last_accessed,
                                                    &range_entry);
                    }
                }
            }
        }
    }

    // We should not need to reach here most of the time.
    std::sort(scanned_ranges.begin(),
              scanned_ranges.end(),
              [](std::pair<uint64_t, TableRangeEntry *> a,
                 std::pair<uint64_t, TableRangeEntry *> b) -> bool
              { return a.first < b.first; });
    for (auto &[time, entry] : scanned_ranges)
    {
        size_t decreased = entry->DropStoreRange();
        if (decreased &&
            DecreaseRangeSliceMemUsage(decreased) <= target_memory_size)
        {
            // We've cleaned up enough
            // memory space.
            return;
        }
    }
}

const TableRangeEntry *LocalCcShards::UploadNewRangeInfo(
    const TableName &table_name,
    const NodeGroupId ng_id,
    const TxKey *key,
    const std::vector<std::unique_ptr<TxKey>> &new_key,
    const std::vector<int32_t> &new_partition_id,
    uint64_t commit_ts)
{
    std::shared_lock<std::shared_mutex> lk(meta_data_mux_);
    TableRangeEntry *entry = GetTableRangeEntryInternal(table_name, ng_id, key);
    assert(entry);
    // Set dirty range in local cc shard range entry.
    entry->UploadNewRangeInfo(new_key, new_partition_id, commit_ts);
    return entry;
}

TableRangeEntry *LocalCcShards::GetTableRangeEntry(const TableName &table_name,
                                                   const NodeGroupId ng_id,
                                                   const TxKey *key)
{
    std::shared_lock<std::shared_mutex> lk(meta_data_mux_);
    TableName range_table_name(table_name.StringView(),
                               TableType::RangePartition);
    return GetTableRangeEntryInternal(range_table_name, ng_id, key);
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
    const TableName &table_name, const NodeGroupId ng_id, const TxKey *key)
{
    TableName range_table_name(table_name.StringView(),
                               TableType::RangePartition);
    return GetTableRangeEntryInternal(range_table_name, ng_id, key);
}

const TableRangeEntry *LocalCcShards::CreateTableRange(
    const TableName &table_name,
    const NodeGroupId ng_id,
    int32_t partition_id,
    TxKey::Uptr start_key,
    uint64_t version,
    std::vector<std::tuple<TxKey::Uptr, uint32_t, SliceStatus>> *slice_keys)
{
    std::unique_lock<std::shared_mutex> lk(meta_data_mux_);
    std::vector<TableRangeEntry *> new_entries;
    bool range_slice_mem_full =
        range_slice_mem_usage_.load(std::memory_order_relaxed) >
        range_slice_memory_limit_;

    std::map<const TxKey *, TableRangeEntry, PtrLessThan<TxKey>> *ranges =
        GetTableRangesForATableInternal(table_name, ng_id);
    std::unordered_map<uint32_t, TableRangeEntry *> *range_ids =
        GetTableRangeIdsForATableInternal(table_name, ng_id);
    std::unique_ptr<StoreRange> range_slices = nullptr;
    NodeGroupId range_ng =
        GetRangeOwnerInternal(partition_id, ng_id)->BucketOwner();

    auto range_it = ranges->find(start_key.get());
    if (range_it == ranges->end())
    {
        const TxKey *end_key =
            GetTableRangeEntryInternal(table_name, ng_id, start_key.get())
                ->GetRangeInfo()
                ->EndKey();
        if (ng_id == range_ng && slice_keys && !range_slice_mem_full)
        {
            range_slices = std::make_unique<StoreRange>(
                start_key.get(), end_key, partition_id, range_ng, *this);
            range_slices->InitSlices(*slice_keys);
            IncreaseRangeSliceMemUsage(range_slices->MemUsage());
        }
        auto new_range_entry_pair =
            ranges->try_emplace(start_key.get(),
                                std::move(start_key),
                                end_key,
                                version,
                                partition_id,
                                std::move(range_slices));
        assert(new_range_entry_pair.second);

        // Update previous range entry's end key if the range is inserted into
        // table_ranges.
        // The new inserted range is always not the smallest range since
        // negative inf is one of the first default range start key.
        auto prev_it = std::prev(new_range_entry_pair.first);
        const TxKey *prev_range_end_key =
            new_range_entry_pair.first->second.GetRangeInfo()->StartKey();
        prev_it->second.SetRangeEndKey(prev_range_end_key);
        range_ids->try_emplace(partition_id,
                               &new_range_entry_pair.first->second);
        return &new_range_entry_pair.first->second;
    }
    else if (range_it->second.Version() < version)
    {
        // Update existing range entry's version range slice info if the passed
        // in version is newer.
        if (ng_id == range_ng && slice_keys)
        {
            range_slices = std::make_unique<StoreRange>(
                range_it->second.GetRangeInfo()->StartKey(),
                range_it->second.GetRangeInfo()->EndKey(),
                partition_id,
                range_ng,
                *this);
            range_slices->InitSlices(*slice_keys);
        }
        int64_t mem_change = range_it->second.UpdateRangeEntry(
            version,
            range_it->second.GetRangeInfo()->EndKey(),
            std::move(range_slices));
        if (mem_change > 0)
        {
            // This would only happen rarely during recover when the old range
            // slice version is read from data store. To avoid blocking tx
            // processor, do not call KickoutRangeSlices.
            IncreaseRangeSliceMemUsage(mem_change);
        }
        else if (mem_change < 0)
        {
            DecreaseRangeSliceMemUsage(-mem_change);
        }
    }
    return &range_it->second;
}

RangeSliceId LocalCcShards::PinRangeSlice(const TableName &table_name,
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
                                          uint8_t prefetch_size)
{
    std::shared_lock<std::shared_mutex> lk(meta_data_mux_);

    TableName range_table_name(table_name.StringView(),
                               TableType::RangePartition);

    TableRangeEntry *entry =
        GetTableRangeEntryInternal(range_table_name, cc_ng_id, &key);
    if (!entry)
    {
        // Table range info not initialized, initialize range info first
        cc_shard->FetchTableRanges(
            range_table_name, cc_request, cc_ng_id, cc_ng_term);
        pin_status = RangeSliceOpStatus::BlockedOnLoad;
        return RangeSliceId();
    }
    std::shared_lock<std::shared_mutex> range_lk(entry->mux_);
    if (!entry->RangeSlices())
    {
        // Check if range is owned by cc_ng_id. If so, load range slices from
        // data store
        if (GetBucketInfoInternal(Sharder::Instance().MapRangeIdToBucketId(
                                      entry->GetRangeInfo()->PartitionId()),
                                  cc_ng_id)
                ->BucketOwner() == cc_ng_id)
        {
            // release shared lock since FetchRangeSlices will acquire unique
            // lock.
            range_lk.unlock();
            entry->FetchRangeSlices(
                range_table_name, cc_request, cc_ng_id, cc_ng_term, cc_shard);
            pin_status = RangeSliceOpStatus::BlockedOnLoad;
        }
        else
        {
            pin_status = RangeSliceOpStatus::NotOwner;
        }
        return RangeSliceId();
    }

    entry->RangeSlices()->UpdateLastAccessedTs(ClockTs());
    const StoreSlice *last_pinned_slice;
    return entry->RangeSlices()->PinSlices(table_name,
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

RangeSliceId LocalCcShards::PinRangeSlices(const TableName &table_name,
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
                                           const StoreSlice *&last_pinned_slice)
{
    std::shared_lock<std::shared_mutex> lk(meta_data_mux_);

    TableName range_table_name(table_name.StringView(),
                               TableType::RangePartition);

    TableRangeEntry *entry =
        GetTableRangeEntryInternal(range_table_name, cc_ng_id, range_id);
    if (!entry)
    {
        // Table range info not initialized, initialize range info first
        cc_shard->FetchTableRanges(
            range_table_name, cc_request, cc_ng_id, cc_ng_term);
        pin_status = RangeSliceOpStatus::BlockedOnLoad;
        return RangeSliceId();
    }
    std::shared_lock<std::shared_mutex> range_lk(entry->mux_);
    if (!entry->RangeSlices())
    {
        // Check if range is owned by cc_ng_id. If so, load range slices from
        // data store
        if (GetBucketInfoInternal(Sharder::Instance().MapRangeIdToBucketId(
                                      entry->GetRangeInfo()->PartitionId()),
                                  cc_ng_id)
                ->BucketOwner() == cc_ng_id)
        {
            // release shared lock since FetchRangeSlices will acquire unique
            // lock.
            range_lk.unlock();
            entry->FetchRangeSlices(
                range_table_name, cc_request, cc_ng_id, cc_ng_term, cc_shard);
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

    entry->RangeSlices()->UpdateLastAccessedTs(ClockTs());
    return entry->RangeSlices()->PinSlices(table_name,
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

StoreRange *LocalCcShards::FindRange(const TableName &table_name,
                                     const NodeGroupId ng_id,
                                     const TxKey &key)
{
    std::shared_lock<std::shared_mutex> lk(meta_data_mux_);
    TableName range_table_name(table_name.StringView(),
                               TableType::RangePartition);

    TableRangeEntry *entry =
        GetTableRangeEntryInternal(range_table_name, ng_id, &key);
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
            uint64_t a, const std::pair<const TxKey *const, TableRangeEntry> &b)
        {
            NodeGroupId range_ng =
                GetRangeOwnerInternal(b.second.GetRangeInfo()->PartitionId(),
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

    const std::map<const TxKey *, TableRangeEntry, PtrLessThan<TxKey>> &ranges =
        table_ranges_.at(range_table_name).at(ng_id);

    for (auto &[range_start_key, range_entry] : ranges)
    {
        NodeGroupId range_ng =
            GetRangeOwnerInternal(range_entry.GetRangeInfo()->PartitionId(),
                                  ng_id)
                ->BucketOwner();
        if (range_ng == local_ng_id)
        {
            const StoreRange *store_range = range_entry.RangeSlices();
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
                              std::vector<const TxKey *> *mv_vec,
                              CcHandlerResult<Void> &hres,
                              bool delay_update_ckpt_ts)
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
                                     delay_update_ckpt_ts);
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

bool LocalCcShards::KickoutKeyInSlice(const TableName &tbl_name,
                                      const NodeGroupId ng_id,
                                      const TxKey &key)
{
    std::shared_lock<std::shared_mutex> s_lk(meta_data_mux_);

    TableName range_tbl_name(tbl_name.StringView(), TableType::RangePartition);
    TableRangeEntry *entry =
        GetTableRangeEntryInternal(range_tbl_name, ng_id, &key);
    if (entry == nullptr)
    {
        return true;
    }
    else
    {
        return entry->KickoutKeyInSlice(key);
    }
}

TableRangeEntry *LocalCcShards::GetTableRangeEntryInternal(
    const TableName &range_tbl_name, const NodeGroupId ng_id, const TxKey *key)
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
        entry = &lower_it->second;
    }
    else if (*lower_it->first == *key)
    {
        entry = &lower_it->second;
    }
    else
    {
        --lower_it;
        entry = &lower_it->second;
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

        statistics_entry.statistics_ = catalog_factory_->CreateTableStatistics(
            table_schema->GetBaseTableName());

        table_schema->BindStatistics(statistics_entry.statistics_);
    }

    return {statistics_it.first->second.statistics_, statistics_it.second};
}

std::pair<std::shared_ptr<Statistics>, bool> LocalCcShards::InitTableStatistics(
    TableSchema *table_schema,
    TableSchema *dirty_table_schema,
    NodeGroupId ng_id,
    std::unordered_map<TableName, std::pair<uint64_t, std::vector<TxKey::Uptr>>>
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
            table_schema->GetBaseTableName(),
            table_schema,
            std::move(sample_pool_map),
            ccs,
            ng_id);

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

const BucketInfo *LocalCcShards::GetBucketInfo(const uint16_t bucket_id,
                                               const NodeGroupId ng_id) const
{
    std::shared_lock<std::shared_mutex> lk(meta_data_mux_);

    return GetBucketInfoInternal(bucket_id, ng_id);
}

BucketInfo *LocalCcShards::GetBucketInfoInternal(const uint16_t bucket_id,
                                                 const NodeGroupId ng_id) const
{
    assert(bucket_id < total_range_buckets);
    auto ng_bucket_it = bucket_infos_.find(ng_id);
    if (ng_bucket_it == bucket_infos_.end())
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
    bucket_infos_.erase(ng_id);
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
    // Construct bucket info map on startup
    // Generate 64 random numbers for each node group as virtual nodes on
    // hashing ring. Each bucket id belongs to the first virtual node that is
    // larger than the bucket id.
    std::unordered_map<uint16_t, std::unique_ptr<BucketInfo>> ng_bucket_infos;
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
    std::unique_lock<std::shared_mutex> lk(meta_data_mux_);
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
    bucket_infos_.try_emplace(ng_id, std::move(ng_bucket_infos));
}

const BucketInfo *LocalCcShards::UploadNewBucketInfo(NodeGroupId ng_id,
                                                     uint16_t bucket_id,
                                                     NodeGroupId dirty_ng,
                                                     uint64_t dirty_version)
{
    std::shared_lock<std::shared_mutex> lk(meta_data_mux_);
    BucketInfo *bucket_info = GetBucketInfoInternal(bucket_id, ng_id);
    bucket_info->SetDirty(dirty_ng, dirty_version);
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
                        entry.GetRangeInfo()->PartitionId()) == bucket_id)
                {
                    size_t mem_decreased;
                    if (!entry.DropStoreRangeAndSyncInfo(mem_decreased))
                    {
                        return false;
                    }
                    if (mem_decreased > 0)
                    {
                        DecreaseRangeSliceMemUsage(mem_decreased);
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
                        entry.GetRangeInfo()->PartitionId()) == bucket_id)
                {
                    tbl_snapshot.insert(entry.GetRangeInfo()->PartitionId());
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

bool LocalCcShards::EnqueueDataSyncTask(const TableName &table_name,
                                        uint32_t ng_id,
                                        int64_t ng_term,
                                        const TableRangeEntry *range_entry,
                                        uint64_t data_sync_ts,
                                        bool need_truncate_log,
                                        bool is_dirty,
                                        std::shared_ptr<DataSyncStatus> status,
                                        CcHandlerResult<Void> *hres)
{
    TableName range_table_name(table_name.StringView(),
                               TableType::RangePartition);
    const RangeInfo *range_info = range_entry->GetRangeInfo();
    NodeGroupId range_ng =
        GetRangeOwnerInternal(range_info->PartitionId(), ng_id)->BucketOwner();
    if (range_ng == ng_id)
    {
        // Range belongs to this ng.
        data_sync_task_queue_.emplace_back(std::make_shared<DataSyncTask>(
            table_name,
            range_info->PartitionId(),
            range_info->VersionTs(),
            ng_id,
            ng_term,
            data_sync_ts,
            status,
            need_truncate_log,
            is_dirty,
            [this](std::shared_ptr<DataSyncTask> task)
            {
                std::lock_guard<std::mutex> lk(data_sync_worker_ctx_.mux_);
                data_sync_task_queue_.push_back(task);
                // Notify the data sync workers.
                data_sync_worker_ctx_.cv_.notify_one();
            },
            hres));
        return true;
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
                    status->err_code_ = CcErrorCode::PIN_RANGE_SLICE_FAILED;
                    break;
                }
            }
        }

        return false;
    }
}

void LocalCcShards::EnqueueDataSyncTaskForTable(
    const TableName &table_name,
    uint32_t ng_id,
    int64_t ng_term,
    uint64_t data_sync_ts,
    bool need_truncate_log,
    bool is_dirty,
    std::shared_ptr<DataSyncStatus> status,
    CcHandlerResult<Void> *hres)
{
    std::lock_guard<std::mutex> task_worker_lk(data_sync_worker_ctx_.mux_);
    std::shared_lock<std::shared_mutex> meta_lk(meta_data_mux_);

#ifndef RANGE_PARTITION_ENABLED
    if (status == nullptr)
    {
        // Only flushing one table and there's no thread waiting on the result.
        assert(hres != nullptr);
        status = std::make_shared<DataSyncStatus>();
    }

    data_sync_task_queue_.emplace_back(std::make_shared<DataSyncTask>(
        table_name,
        0,
        0,
        ng_id,
        ng_term,
        data_sync_ts,
        status,
        need_truncate_log,
        is_dirty,
        [this](std::shared_ptr<DataSyncTask> task)
        {
            std::lock_guard<std::mutex> lk(data_sync_worker_ctx_.mux_);
            data_sync_task_queue_.push_back(task);
            // Notify the data sync workers.
            data_sync_worker_ctx_.cv_.notify_one();
        },
        hres));

    {
        std::lock_guard<std::mutex> status_lk(status->mux_);
        status->unfinished_tasks_++;
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
    if (status == nullptr)
    {
        // Only flushing one table and there's no thread waiting on the result.
        assert(hres != nullptr);
        status = std::make_shared<DataSyncStatus>();
    }

    uint32_t unfinished_task_cnt = 0;

    for (auto &range : *ranges)
    {
        if (EnqueueDataSyncTask(table_name,
                                ng_id,
                                ng_term,
                                &range.second,
                                data_sync_ts,
                                need_truncate_log,
                                is_dirty,
                                status,
                                hres))
        {
            // Increment local variable to reduce lock contention.
            unfinished_task_cnt++;
        }
    }

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
    const std::unordered_map<TableName, std::unordered_set<int32_t>>
        &ranges_in_bucket_snapshot,
    uint32_t ng_id,
    int64_t ng_term,
    uint64_t data_sync_ts,
    CcHandlerResult<Void> *hres)
{
    std::lock_guard<std::mutex> task_worker_lk(data_sync_worker_ctx_.mux_);
    std::shared_lock<std::shared_mutex> meta_lk(meta_data_mux_);
    std::shared_ptr<DataSyncStatus> status = std::make_shared<DataSyncStatus>();
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
            if (range_entry && EnqueueDataSyncTask(table_name,
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
}

void LocalCcShards::Terminate()
{
    // Terminate the data sync task worker thds.
    data_sync_worker_ctx_.Terminate();

    // Terminate the flush worker thds.
    flush_data_worker_ctx_.Terminate();

    // Terminate the slice update worker thds.
    slice_update_worker_ctx_.Terminate();

    if (realtime_sampling_)
    {
        statistics_worker_ctx_.Terminate();
    }

    defragment_worker_ctx_.Terminate();
}

void LocalCcShards::DataSyncWorker()
{
    std::unique_lock<std::mutex> task_worker_lk(data_sync_worker_ctx_.mux_);

    while (data_sync_worker_ctx_.status_ == WorkerStatus::Active)
    {
        if (data_sync_task_queue_.empty() &&
            data_sync_worker_ctx_.status_ == WorkerStatus::Active)
        {
            // Notify checkpointer to start new round
            // of checkpoint since we've finished all
            // previous tasks.
            NotifyCheckPointer(false);
        }
        data_sync_worker_ctx_.cv_.wait(
            task_worker_lk,
            [this]
            {
                return !data_sync_task_queue_.empty() ||
                       data_sync_worker_ctx_.status_ != WorkerStatus::Active;
            });

        if (data_sync_task_queue_.empty())
        {
            continue;
        }

        DataSync(task_worker_lk);
    }

    // Handle pending tasks.
    while (!data_sync_task_queue_.empty())
    {
        DataSync(task_worker_lk);
    }
}

#ifdef RANGE_PARTITION_ENABLED
void LocalCcShards::DataSync(std::unique_lock<std::mutex> &task_worker_lk)
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
    // Whether other task worker is processing this table.
    const TableName &table_name = data_sync_task->table_name_;
    uint32_t ng_id = data_sync_task->node_group_id_;
    uint64_t expected_range_version = data_sync_task->range_version_;
    int64_t expected_ng_term = data_sync_task->node_group_term_;
    uint64_t target_data_sync_ts = data_sync_task->data_sync_ts_;
    bool is_dirty = data_sync_task->is_dirty_;
    data_sync_task_queue_.pop_front();

    task_worker_lk.unlock();

    std::shared_lock<std::shared_mutex> meta_lk(meta_data_mux_);
    uint64_t last_sync_ts = 0;
    bool need_process = false;

    int32_t range_id = data_sync_task->range_id_;
    TableName range_tbl_name{table_name.StringView(),
                             TableType::RangePartition};
    TableRangeEntry *range_entry =
        GetTableRangeEntryInternal(range_tbl_name, ng_id, range_id);
    if (range_entry == nullptr)
    {
        // table dropped
        data_sync_task->SetError(CcErrorCode::REQUESTED_TABLE_NOT_EXISTS);
    }
    else
    {
        NodeGroupId range_ng =
            GetRangeOwnerInternal(range_id, ng_id)->BucketOwner();
        if (range_ng == ng_id)
        {
            // For dirty tables (create index in process), data older than
            // last sync ts will be continously written into memory. We cannot
            // rely on last sync ts to determin if there's dirty data that needs
            // to be flushed.
            last_sync_ts = is_dirty ? 0 : range_entry->GetLastSyncTs();
            if (target_data_sync_ts <= last_sync_ts && !is_dirty)
            {
                // 1) For table that is_dirty is false, can set finish
                // directly.
                data_sync_task->SetFinish();
                // Handle the pending tasks for the same table
                range_entry->PopPendingSyncTask();
            }
            else if (range_entry->TrySetDataSync(true, data_sync_task))
            {
                need_process = true;
            }
        }
        else
        {
            // range no longer belong to this ng.
            data_sync_task->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
        }
    }

    if (!need_process)
    {
        return;
    }

    // Check the leader
    int64_t ng_term = Sharder::Instance().TryPinNodeGroupData(ng_id);
    if (ng_term < 0 || ng_term != expected_ng_term)
    {
        LOG(ERROR) << "DataSync: node is not the leader of ng#" << ng_id
                   << " with leader term: " << ng_term
                   << ", and the expected leader term: " << expected_ng_term;

        // Set range sync status.
        range_entry->TrySetDataSync(false);
        // Handle the pending tasks for the same range
        range_entry->PopPendingSyncTask();

        if (ng_term >= 0)
        {
            Sharder::Instance().UnpinNodeGroupData(ng_id);
        }
        // Finish this task and notify the caller.
        data_sync_task->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
        return;
    }
    meta_lk.unlock();
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
        task_worker_lk.lock();
        // Put back into the beginning.
        data_sync_task_queue_.emplace_front(std::move(data_sync_task));
        // The txm has been freed.

        // Update the table sync status
        meta_lk.lock();
        range_entry =
            GetTableRangeEntryInternal(range_tbl_name, ng_id, range_id);
        if (range_entry)
        {
            range_entry->TrySetDataSync(false);
        }

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
    CatalogRecord catalog_rec;

    ReadTxRequest read_req;
    read_req.Set(
        &catalog_ccm_name, &table_key, &catalog_rec, false, false, true);
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
        }
        else
        {
            LOG(ERROR) << "DataSync add read lock on table failed, "
                          "table name: "
                       << table_key.Name().StringView();

            task_worker_lk.lock();
            // If read lock acquire failed, retry next time.
            // Put back into the beginning.
            data_sync_task_queue_.emplace_front(std::move(data_sync_task));

            meta_lk.lock();
            range_entry =
                GetTableRangeEntryInternal(range_tbl_name, ng_id, range_id);
            if (range_entry)
            {
                range_entry->TrySetDataSync(false);
            }
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
            return;
        }
    }

    // Lock bucket so that bucket cannot be migrated away during data sync.
    RangeBucketRecord bucket_rec;
    RangeBucketKey bucket_key(
        Sharder::Instance().MapRangeIdToBucketId(range_id));
    read_req.Reset();
    read_req.Set(
        &range_bucket_ccm_name, &bucket_key, &bucket_rec, false, false, true);
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
        task_worker_lk.lock();
        data_sync_task_queue_.emplace_front(std::move(data_sync_task));
        meta_lk.lock();
        range_entry =
            GetTableRangeEntryInternal(range_tbl_name, ng_id, range_id);
        if (range_entry)
        {
            range_entry->TrySetDataSync(false);
        }

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
        range_entry->TrySetDataSync(false);
        range_entry->PopPendingSyncTask();

        // Skip the range, it might be dropped or migrated away.
        // Use AbortTxRequest to release read lock.
        txservice::AbortTx(data_sync_txm);
        data_sync_task->SetError();
        return;
    }
    else if (range_entry->Version() != expected_range_version)
    {
        // If the range spec has been updated since we create the task,
        // we might miss the data in the new range during data sync scan.
        // So we need to mark this round of data sync as failed.
        data_sync_task->SetErrorCode(CcErrorCode::GET_RANGE_ID_ERR);
    }

    // 3. Scan records.
    // The data sync worker thread is the owner of those vectors.
    std::vector<std::vector<FlushRecord>> data_sync_vecs;
    std::vector<std::vector<FlushRecord>> archive_vecs;
    std::vector<std::vector<const TxKey *>> mv_base_vecs;

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

    DataSyncScanCc scan_cc(table_name,
                           0,
                           last_sync_ts,
                           target_data_sync_ts,
                           ng_id,
                           ng_term,
                           cc_shards_.size(),
                           DATA_SYNC_SCAN_BATCH_SIZE,
                           range_entry->GetRangeInfo()->StartKey(),
                           range_entry->GetRangeInfo()->EndKey());

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
                      << table_name.StringView();
            // Update the table data sync status.
            range_entry->TrySetDataSync(false);

            txservice::AbortTx(data_sync_txm);
            task_worker_lk.lock();
            // Put back into the beginning.
            data_sync_task_queue_.emplace_front(std::move(data_sync_task));

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
                    data_sync_vecs[i].emplace_back(rec.Key()->Clone(),
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
                    const TxKey *key_raw_ptr =
                        data_sync_vecs[i][key_idx + offset].Key();
                    mv_base_vecs[i].push_back(key_raw_ptr);
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

    std::unique_ptr<std::vector<const TxKey *>> mv_base_vec =
        std::make_unique<std::vector<const TxKey *>>();

    // Sort output vectors in key sorting order.
    auto key_greater = [](const TxKey *r1, const TxKey *r2) -> bool
    { return *r2 < *r1; };
    auto rec_greater = [](const FlushRecord &r1, const FlushRecord &r2) -> bool
    { return *r2.Key() < *r1.Key(); };

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
                    range_entry->TrySetDataSync(false);
                    // Handle the pending tasks for the same range
                    range_entry->PopPendingSyncTask();
                    // Term is invalid, we are no longer leader. Abort data
                    // sync.
                    txservice::AbortTx(data_sync_txm);
                    data_sync_task->SetError();
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
        std::vector<const TxKey *> split_keys;
        bool ret = UpdateSliceAndCalculateRangeUpdate(table_name,
                                                      table_schema,
                                                      ng_id,
                                                      ng_term,
                                                      *data_sync_vec,
                                                      target_data_sync_ts,
                                                      store_range,
                                                      split_keys);

        if (!ret)
        {
            LOG(ERROR) << "Pre-data_sync slice update failed on table "
                       << table_name.StringView();
            // Indicating that the task worker has processed this task.
            range_entry->TrySetDataSync(false);

            // Handle the pending tasks for the same range
            range_entry->PopPendingSyncTask();
            range_entry->UnPinStoreRange();
            txservice::AbortTx(data_sync_txm);
            data_sync_task->SetError();

            return;
        }

        if (!split_keys.empty())
        {
            // Create a new thread to execute range split.
            auto range_split_worker = std::thread(
                [this,
                 &table_name,
                 table_schema,
                 range_entry,
                 split_keys = std::move(split_keys),
                 &ng_id,
                 defer_unpin,
                 data_sync_txm,
                 data_sync_task,
                 previous_data_sync_vec = std::move(data_sync_vec),
                 previous_archive_vec = std::move(archive_vec),
                 previous_mv_base_vec = std::move(mv_base_vec)]() mutable
                {
                    SplitFlushRange(table_name,
                                    table_schema,
                                    ng_id,
                                    data_sync_txm,
                                    range_entry,
                                    std::move(split_keys),
                                    data_sync_task,
                                    std::move(*previous_data_sync_vec),
                                    std::move(*previous_archive_vec),
                                    std::move(*previous_mv_base_vec),
                                    defer_unpin);
                });
            range_split_worker.detach();
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
                                         false);
        flush_data_worker_ctx_.cv_.notify_one();
    }
    else
    {
        // Update the task status and last sync ts of this range.
        range_entry->TrySetDataSync(false, nullptr, target_data_sync_ts);

        // Handle the pending tasks for the same table
        range_entry->PopPendingSyncTask();
        // Nothing to flush in this range.
        // Commit the data sync txm
        txservice::CommitTx(data_sync_txm);

        data_sync_task->SetFinish();
    }
}
#else

void LocalCcShards::PostProcessDataSyncTask(
    std::shared_ptr<DataSyncTask> task,
    TransactionExecution *data_sync_txm,
    CatalogEntry *catalog_entry,
    DataSyncTask::CkptErrorCode ckpt_err)
{
    std::unique_lock<std::mutex> flight_task_lk(task->flight_task_mux_);
    int64_t flush_task_cnt = --task->flight_task_cnt_;

    if (task->ckpt_err_ == DataSyncTask::CkptErrorCode::NO_ERROR)
    {
        task->ckpt_err_ = ckpt_err;
    }
    else
    {
        ckpt_err = task->ckpt_err_;
    }

    assert(ckpt_err == task->ckpt_err_);

    flight_task_lk.unlock();

    if (flush_task_cnt == 0)
    {
        if (ckpt_err == DataSyncTask::CkptErrorCode::NO_ERROR)
        {
            if (catalog_entry)
            {
                catalog_entry->TrySetDataSync(
                    false, nullptr, task->data_sync_ts_);
                // Handle the pending tasks for the same table
                catalog_entry->PopPendingSyncTask();
            }

            // Commit the data sync txm
            txservice::CommitTx(data_sync_txm);

            task->SetFinish();
        }
        else if (ckpt_err == DataSyncTask::CkptErrorCode::SCAN_ERROR)
        {
            if (catalog_entry)
            {
                // Update the table data sync status.
                catalog_entry->TrySetDataSync(false);
            }

            txservice::AbortTx(data_sync_txm);

            data_sync_worker_ctx_.mux_.lock();
            data_sync_task_queue_.emplace_front(std::move(task));
        }
        else
        {
            assert(ckpt_err == DataSyncTask::CkptErrorCode::FLUSH_ERROR);
            if (catalog_entry)
            {
                catalog_entry->TrySetDataSync(false);
                catalog_entry->PopPendingSyncTask();
            }

            txservice::AbortTx(data_sync_txm);

            CcErrorCode err_code =
                Sharder::Instance().LeaderTerm(task->node_group_id_) > 0
                    ? CcErrorCode::DATA_STORE_ERR
                    : CcErrorCode::REQUESTED_NODE_NOT_LEADER;
            task->SetError(err_code);
        }
    }
}

void LocalCcShards::DataSync(std::unique_lock<std::mutex> &task_worker_lk)
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
    // Whether other task worker is processing this table.
    const TableName &table_name = data_sync_task->table_name_;
    uint32_t ng_id = data_sync_task->node_group_id_;
    int64_t expected_ng_term = data_sync_task->node_group_term_;
    uint64_t target_data_sync_ts = data_sync_task->data_sync_ts_;
    bool is_dirty = data_sync_task->is_dirty_;
    data_sync_task_queue_.pop_front();

    // Release worker lock
    task_worker_lk.unlock();

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
    }
    else
    {
        // For dirty tables (create index in process), data older than
        // last sync ts will be continously written into memory. We cannot
        // rely on last sync ts to determin if there's dirty data that needs
        // to be flushed.
        last_sync_ts = is_dirty ? 0 : catalog_entry->GetLastSyncTs();
        if (target_data_sync_ts <= last_sync_ts && !is_dirty)
        {
            // 1) For table that is_dirty is false, can set finish
            // directly.
            data_sync_task->SetFinish();
            // Handle the pending tasks for the same table
            catalog_entry->PopPendingSyncTask();
        }
        else if (catalog_entry->TrySetDataSync(true, data_sync_task))
        {
            need_process = true;
        }
    }

    if (!need_process)
    {
        return;
    }

    // Check the leader
    int64_t ng_term = Sharder::Instance().TryPinNodeGroupData(ng_id);
    if (ng_term < 0 || ng_term != expected_ng_term)
    {
        LOG(ERROR) << "DataSync: node is not the leader of ng#" << ng_id
                   << " with leader term: " << ng_term
                   << ", and the expected leader term: " << expected_ng_term;
        // Set table sync status.
        catalog_entry->TrySetDataSync(false);
        // Handle the pending tasks for the same table
        catalog_entry->PopPendingSyncTask();

        if (ng_term >= 0)
        {
            Sharder::Instance().UnpinNodeGroupData(ng_id);
        }
        // Finish this task and notify the caller.
        data_sync_task->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
        return;
    }

    meta_lk.unlock();
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

        // Put back into the beginning.
        task_worker_lk.lock();
        data_sync_task_queue_.emplace_front(std::move(data_sync_task));
        // The txm has been freed.
        meta_lk.lock();
        catalog_entry = GetCatalogInternal(primary_base_table_name, ng_id);
        if (catalog_entry)
        {
            catalog_entry->TrySetDataSync(false);
        }

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
    CatalogRecord catalog_rec;

    ReadTxRequest read_req;
    read_req.Set(
        &catalog_ccm_name, &table_key, &catalog_rec, false, false, true);
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
        }
        else
        {
            LOG(ERROR) << "DataSync add read lock on table failed, "
                          "table name: "
                       << table_key.Name().StringView();

            // If read lock acquire failed, retry next time.
            // Put back into the beginning.
            task_worker_lk.lock();
            data_sync_task_queue_.emplace_front(std::move(data_sync_task));

            meta_lk.lock();
            catalog_entry = GetCatalogInternal(primary_base_table_name, ng_id);
            if (catalog_entry)
            {
                catalog_entry->TrySetDataSync(false);
            }
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
    auto mv_base_vec = std::make_unique<std::vector<const TxKey *>>();

    // Note: `DataSyncScanCc` needs to ensure that no two ckpt_rec with the
    // same Key can be generated. Our subsequent algorithms are based on this
    // assumption.
    DataSyncScanCc scan_cc(table_name,
                           0,
                           last_sync_ts,
                           target_data_sync_ts,
                           ng_id,
                           ng_term,
                           cc_shards_.size(),
                           DATA_SYNC_SCAN_BATCH_SIZE);

    {
        // DataSync Worker will call PostProcessDataSyncTask() to decrement
        // flight task count
        std::lock_guard<std::mutex> flight_task_lk(
            data_sync_task->flight_task_mux_);
        data_sync_task->flight_task_cnt_ += 1;
    }

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
                      << table_name.StringView();

            PostProcessDataSyncTask(std::move(data_sync_task),
                                    data_sync_txm,
                                    catalog_entry,
                                    DataSyncTask::CkptErrorCode::SCAN_ERROR);

            return;
        }
        else
        {
            scan_data_drained = true;

            for (size_t i = 0; i < cc_shards_.size(); i++)
            {
                size_t offset = data_sync_vec->size();

                for (size_t j = 0; j < scan_cc.accumulated_scan_cnt_[i]; ++j)
                {
                    auto &rec = scan_cc.DataSyncVec(i)[j];
                    // Note. Clone key instead of move key. The memory of
                    // rec.Key() will be reused to avoid memory allocation.
                    data_sync_vec->emplace_back(rec.Key()->Clone(),
                                                rec.GetPayload(),
                                                rec.payload_status_,
                                                rec.commit_ts_,
                                                rec.cce_,
                                                rec.delta_size_);
                }

                for (size_t j = 0; j < scan_cc.ArchiveVec(i).size(); ++j)
                {
                    auto &rec = scan_cc.ArchiveVec(i)[j];
                    // Note. We need to ensure the copy constructor of
                    // FlushRecord could not be called.
                    rec.SetKey(
                        (*data_sync_vec)[rec.GetKeyIndex() + offset].Key());
                }

                for (size_t j = 0; j < scan_cc.MoveBaseIdxVec(i).size(); ++j)
                {
                    size_t key_idx = scan_cc.MoveBaseIdxVec(i)[j];
                    const TxKey *key_raw_ptr =
                        (*data_sync_vec)[key_idx + offset].Key();
                    mv_base_vec->emplace_back(key_raw_ptr);
                }

                std::move(scan_cc.ArchiveVec(i).begin(),
                          scan_cc.ArchiveVec(i).end(),
                          std::back_inserter(*archive_vec));

                scan_data_drained = scan_cc.IsDrained(i) && scan_data_drained;

                if (data_sync_vec->size() + archive_vec->size() +
                        mv_base_vec->size() >
                    rec_size_limit)
                {
                    {
                        std::lock_guard<std::mutex> flight_task_lk(
                            data_sync_task->flight_task_mux_);
                        if (data_sync_task->ckpt_err_ ==
                            DataSyncTask::CkptErrorCode::FLUSH_ERROR)
                        {
                            break;
                        }
                        // Flush worker will call PostProcessDataSyncTask() to
                        // decrement flight task count.
                        data_sync_task->flight_task_cnt_ += 1;
                    }

                    {
                        std::lock_guard<std::mutex> worker_lk(
                            flush_data_worker_ctx_.mux_);
                        pending_flush_work_.emplace_back(
                            data_sync_task,
                            table_schema,
                            std::move(data_sync_vec),
                            std::move(archive_vec),
                            std::move(mv_base_vec),
                            data_sync_txm,
                            false);

                        flush_data_worker_ctx_.cv_.notify_one();
                    }

                    data_sync_vec =
                        std::make_unique<std::vector<FlushRecord>>();

                    archive_vec = std::make_unique<std::vector<FlushRecord>>();

                    mv_base_vec =
                        std::make_unique<std::vector<const TxKey *>>();
                }
            }

            scan_cc.Reset();
        }
    }

    if (data_sync_vec->size() > 0 || archive_vec->size() > 0 ||
        mv_base_vec->size() > 0)
    {
        std::unique_lock<std::mutex> flight_task_lk(
            data_sync_task->flight_task_mux_);
        // Flush task failed. Stop data sync task.
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
                                             false);
            flush_data_worker_ctx_.cv_.notify_one();
        }
    }

    PostProcessDataSyncTask(std::move(data_sync_task),
                            data_sync_txm,
                            catalog_entry,
                            DataSyncTask::CkptErrorCode::NO_ERROR);
}
#endif

bool LocalCcShards::UpdateSliceAndCalculateRangeUpdate(
    const TableName &table_name,
    const TableSchema *schema,
    NodeGroupId node_group_id,
    int64_t node_group_term,
    std::vector<FlushRecord> &flush_batch,
    uint64_t data_sync_ts,
    StoreRange *store_range,
    std::vector<const TxKey *> &splitting_info)
{
    std::mutex work_sender_mux;
    std::condition_variable work_sender_cv;
    size_t slice_update_done = 0;
    size_t slice_load_cnt = 0;
    bool fail = false;
    size_t slice_start_idx = 0;
    auto batch_it = flush_batch.begin();

    auto lower_bound_cmp = [](const FlushRecord &rec, const TxKey &key)
    { return *rec.Key() < key; };

    while (batch_it != flush_batch.end())
    {
        const TxKey &slice_start_key = *batch_it->Key();
        StoreSlice *curr_slice = store_range->FindSlice(slice_start_key);

        auto slice_end_it = curr_slice->EndKey() == store_range->RangeEndKey()
                                ? flush_batch.end()
                                : std::lower_bound(batch_it,
                                                   flush_batch.end(),
                                                   *curr_slice->EndKey(),
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
            {
                std::unique_lock<std::mutex> worker_lk(
                    slice_update_worker_ctx_.mux_);
                pending_slice_work_.emplace_back(node_group_id,
                                                 node_group_term,
                                                 data_sync_ts,
                                                 table_name,
                                                 schema,
                                                 flush_batch,
                                                 store_range,
                                                 curr_slice,
                                                 slice_start_idx,
                                                 slice_end_idx,
                                                 work_sender_mux,
                                                 work_sender_cv,
                                                 slice_update_done,
                                                 fail);
                slice_update_worker_ctx_.cv_.notify_one();
            }
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
        splitting_info =
            store_range->CalculateRangeSplitKeys(table_name,
                                                 schema,
                                                 node_group_id,
                                                 node_group_term,
                                                 data_sync_ts,
                                                 post_ckpt_size,
                                                 flush_batch.begin(),
                                                 flush_batch.end(),
                                                 flush_batch);
        if (!splitting_info.empty())
        {
            return true;
        }
    }

    return true;
}

void LocalCcShards::SplitFlushRange(
    const TableName &table_name,
    const TableSchema *table_schema,
    NodeGroupId node_group,
    TransactionExecution *split_txm,
    TableRangeEntry *range_entry,
    std::vector<const TxKey *> &&split_keys,
    std::shared_ptr<DataSyncTask> data_sync_task,
    std::vector<FlushRecord> &&previous_data_sync_vec,
    std::vector<FlushRecord> &&previous_archive_vec,
    std::vector<const TxKey *> &&previous_mv_base_vec,
    std::shared_ptr<void> defer_unpin)
{
    const TableName range_table_name{table_name.String(),
                                     TableType::RangePartition};
    std::string log_output(
        "Splitting table " + table_name.String() + " range " +
        std::to_string(range_entry->GetRangeInfo()->PartitionId()) + " into " +
        std::to_string(split_keys.size() + 1) + " ranges. New range ids ");
    // Request for new range ids from data store. The new range ids returned
    // by data store are always unique.
    std::vector<std::pair<TxKey::Uptr, int32_t>> new_range_ids;
    for (auto &new_key : split_keys)
    {
        int32_t new_part_id;
        if (!store_hd_->GetNextRangePartitionId(table_name, &new_part_id))
        {
            LOG(ERROR) << "Split range failed due to unable to get next "
                          "partition id.";

            range_entry->TrySetDataSync(false);
            range_entry->UnPinStoreRange();
            range_entry->PopPendingSyncTask();
            txservice::AbortTx(split_txm);
            data_sync_task->SetError(CcErrorCode::DATA_STORE_ERR);

            return;
        }
        log_output.append(std::to_string(new_part_id) + ",");
        new_range_ids.emplace_back(new_key->Clone(), new_part_id);
    }

    const TxKey *old_start_key = range_entry->GetRangeInfo()->StartKey();
    if (old_start_key == nullptr)
    {
        old_start_key = catalog_factory_->NegativeInfKey();
    }
    const TxKey *old_end_key = range_entry->GetRangeInfo()->EndKey();
    if (old_end_key == nullptr)
    {
        old_end_key = catalog_factory_->PositiveInfKey();
    }

    assert(old_start_key != nullptr);
    assert(old_end_key != nullptr);

    // Start the SplitFlush tx. This would split the range, flush the data
    // and update slice metadata.
    log_output.append(" txn: " + std::to_string(split_txm->TxNumber()));
    LOG(INFO) << log_output;
    if (realtime_sampling_)
    {
        table_schema->StatisticsObject()->PriorSplitRange(
            table_name, table_schema, node_group);
    }

    SplitFlushTxRequest split_req(table_name,
                                  table_schema,
                                  old_start_key,
                                  old_end_key,
                                  range_entry->RangeSlices(),
                                  range_entry->GetRangeInfo(),
                                  std::move(new_range_ids),
                                  data_sync_task->data_sync_ts_,
                                  std::move(previous_data_sync_vec),
                                  std::move(previous_archive_vec),
                                  std::move(previous_mv_base_vec));
    split_txm->Execute(&split_req);
    split_req.Wait();
    if (split_req.IsError() || !split_req.Result())
    {
        LOG(ERROR) << "Split range on table " << table_name.StringView()
                   << " partition "
                   << range_entry->GetRangeInfo()->PartitionId() << " failed.";

        range_entry->TrySetDataSync(false);
        range_entry->UnPinStoreRange();
        range_entry->PopPendingSyncTask();
        txservice::AbortTx(split_txm);
        data_sync_task->SetError();

        return;
    }

    range_entry->TrySetDataSync(false, nullptr, data_sync_task->data_sync_ts_);
    range_entry->UnPinStoreRange();
    range_entry->PopPendingSyncTask();

    LOG(INFO) << "Split range on table " << range_table_name.StringView()
              << " partition " << range_entry->GetRangeInfo()->PartitionId()
              << " succeeded.";
    txservice::CommitTx(split_txm);

    data_sync_task->SetFinish();
}

void LocalCcShards::FlushData(std::unique_lock<std::mutex> &flush_worker_lk)
{
    // Retrieve first pending work and pop it.
    FlushDataWork &cur_work = pending_flush_work_.back();
    uint32_t node_group = cur_work.node_group_id_;
    int64_t leader_term = cur_work.node_group_term_;
    TableName table_name = cur_work.table_name_;
    const TableSchema *schema = cur_work.schema_;
    uint64_t data_sync_ts = cur_work.data_sync_ts_;
    bool is_delay_update_ckpt_ts = cur_work.delay_update_ckpt_ts_;
    std::unique_ptr<std::vector<FlushRecord>> data_sync_vec_owner,
        archive_vec_owner;
    std::vector<FlushRecord> *data_sync_vec, *archive_vec;
    std::unique_ptr<std::vector<const TxKey *>> mv_base_owner;
    std::vector<const TxKey *> *mv_base_vec;
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
                if (!is_delay_update_ckpt_ts)
                {
                    for (size_t i = 0; i < data_sync_vec->size(); i++)
                    {
                        auto &ref = data_sync_vec->at(i);

                        assert(ref.cce_ != nullptr);
                        // todo: remove cce_
                        ref.cce_->ckpt_ts_.store(ref.commit_ts_,
                                                 std::memory_order_release);
                        ref.cce_->data_store_size_.fetch_add(ref.delta_size_);
                    }
                    ResetCleanStartPageCc reset_cc(cc_shards_.size());
                    for (auto &ccs : cc_shards_)
                    {
                        ccs->Enqueue(&reset_cc);
                    }
                    reset_cc.Wait();
                }

                if (data_sync_vec->size())
                {
#ifdef RANGE_PARTITION_ENABLED
                    // Update the slice size in data store.
                    while (!UpdateStoreSlice(table_name,
                                             schema->Version(),
                                             node_group,
                                             *data_sync_vec,
                                             true))
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
                                            schema->Version(),
                                            node_group,
                                            *data_sync_vec,
                                            false);
                // We're only updating in memory status here, so
                // this should always succeed.
                assert(res);
#endif
                succ = false;
            }
        } /* End of PutAll */

    } /* End of leader */

#ifndef RANGE_PARTITION_ENABLED
    CatalogEntry *catalog_entry = nullptr;
#endif

    if (ng_term >= 0 && ng_term == leader_term)
    {
        if (data_sync_task != nullptr)
        {
#ifdef RANGE_PARTITION_ENABLED
            TableRangeEntry *range_entry =
                const_cast<TableRangeEntry *>(GetTableRangeEntry(
                    table_name, node_group, data_sync_task->range_id_));
            assert(range_entry);
            if (succ)
            {
                // Update the task status for this range.
                range_entry->TrySetDataSync(false, nullptr, data_sync_ts);
            }
            else
            {
                range_entry->TrySetDataSync(false);
            }
            range_entry->UnPinStoreRange();
            range_entry->PopPendingSyncTask();
#else
            const TableName base_table_name{table_name.GetBaseTableNameSV(),
                                            TableType::Primary};
            catalog_entry = GetCatalog(base_table_name, node_group);
            assert(catalog_entry);
#endif
        }
    }

    // Update the work count if the work's sender is waiting.
    if (data_sync_task != nullptr)
    {
        assert(data_sync_txm != nullptr);
        assert(hand_res == nullptr);

#ifdef RANGE_PARTITION_ENABLED
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
        auto ckpt_err = DataSyncTask::CkptErrorCode::NO_ERROR;

        if (!succ)
        {
            ckpt_err = DataSyncTask::CkptErrorCode::FLUSH_ERROR;
        }

        PostProcessDataSyncTask(
            std::move(data_sync_task), data_sync_txm, catalog_entry, ckpt_err);
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
                                     uint64_t schema_ts,
                                     NodeGroupId node_group_id,
                                     std::vector<FlushRecord> &data_sync_vec,
                                     bool flush_res)
{
    bool success = true;
    bool range_updated = false;
    StoreRange *curr_range = nullptr;
    StoreSlice *curr_slice = nullptr;
    bool new_slice = true;

    for (size_t idx = 0; idx < data_sync_vec.size(); ++idx)
    {
        if (new_slice)
        {
            const TxKey &data_sync_key = *data_sync_vec[idx].Key();

            if (curr_range == nullptr ||
                (curr_range->RangeEndKey() != nullptr &&
                 (*curr_range->RangeEndKey() < data_sync_key ||
                  *curr_range->RangeEndKey() == data_sync_key)))
            {
                if (curr_range != nullptr && flush_res && range_updated)
                {
                    bool ret = curr_range->UpdateRangeSlicesInStore(
                        table_name, schema_ts, true, store_hd_);
                    success = ret && success;
                }

                // The current datasync key falls into a new range. Finds
                // the range.
                curr_range =
                    FindRange(table_name, node_group_id, data_sync_key);
                // TODO: verify bucket info instead of relying on FindRange
                // result
                while (curr_range == nullptr)
                {
                    // Items that does not belong to this are skipped
                    // during flush data.
                    idx++;
                    if (idx == data_sync_vec.size())
                    {
                        return true;
                    }
                    curr_range = FindRange(
                        table_name, node_group_id, *data_sync_vec[idx].Key());
                }
                range_updated = false;
            }

            curr_slice = curr_range->FindSlice(data_sync_key);
        }

        // Have iterated all flushed data items falling into the
        // current slice. Re-calculates the slice's size.
        if (idx == data_sync_vec.size() - 1 ||
            (curr_slice->EndKey() != nullptr &&
             !(*data_sync_vec[idx + 1].Key() < *curr_slice->EndKey())))
        {
            if (flush_res)
            {
                range_updated |= curr_slice->UpdateSize();
            }
            else
            {
                curr_slice->SetPostCkptSize(UINT64_MAX);
            }

            // The next entry falls into a new slice.
            new_slice = true;
        }
    }

    if (flush_res && range_updated)
    {
        bool ret = curr_range->UpdateRangeSlicesInStore(
            table_name, schema_ts, true, store_hd_);
        success = success && ret;
    }
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

        if (statistics_worker_ctx_.status_ == WorkerStatus::Terminated)
        {
            break;
        }
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
            auto sync_ts_pair = ng_sync_ts.try_emplace(node_group, 0);
            uint64_t &last_sync_ts = sync_ts_pair.first->second;
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
                    CatalogRecord catalog_rec;

                    ReadTxRequest read_req;
                    read_req.Set(&catalog_ccm_name,
                                 &table_key,
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

                    while (
                        !table_schema->StatisticsObject()->SyncTableStatistics(
                            store_hd_,
                            table_name,
                            table_schema,
                            node_group,
                            sync_ts))
                    {
                        LOG(ERROR) << "Failed to update statistics of table "
                                   << table_name.Trace() << ", retrying.";
                        std::this_thread::sleep_for(1s);
                        // Check leader term in infinite while loop.
                        if (!Sharder::Instance().CheckLeaderTerm(node_group,
                                                                 leader_term))
                        {
                            LOG(ERROR) << "Leader term changed during table "
                                          "statistics update";
                            succ = false;
                            break;
                        }
                    }

                    txservice::CommitTx(txm);
                }
            }
            if (succ)
            {
                last_sync_ts = sync_ts;
            }

            // finish table stats sync on this node group, unpin its data
            // and clear its ccmaps and catalogs if it is no longer leader
            Sharder::Instance().UnpinNodeGroupData(node_group);
        }
        worker_lk.lock();
    }
}

void LocalCcShards::DefragmentWorker()
{
    std::unique_lock<std::mutex> worker_lk(defragment_worker_ctx_.mux_);
    while (defragment_worker_ctx_.status_ == WorkerStatus::Active)
    {
        defragment_worker_ctx_.cv_.wait_for(
            worker_lk,
            10s,
            [this] {
                return defragment_worker_ctx_.status_ ==
                       WorkerStatus::Terminated;
            });
        if (defragment_worker_ctx_.status_ == WorkerStatus::Terminated)
        {
            break;
        }

        worker_lk.unlock();
        for (auto &ccs : cc_shards_)
        {
            auto heap = ccs->GetShardHeap();
            if (heap == nullptr)
            {
                continue;
            }

            HeapMemStats stats;
            CollectMemStatsCc cc(&stats);
            ccs->Enqueue(&cc);
            cc.Wait();

            LOG(INFO) << "ccs " << ccs->core_id_
                      << " memory usage report, committed " << stats.committed_
                      << ", allocated " << stats.allocated_;
            if (stats.committed_ > ccs->memory_limit_ * 0.7 &&
                stats.allocated_ < stats.committed_ * 0.8)
            {
                LOG(INFO) << "Found memory fragementation in ccs "
                          << ccs->core_id_
                          << ", total comitted memory: " << stats.committed_
                          << ", actual used memory " << stats.allocated_;
            }
        }

        worker_lk.lock();
    }
}
}  // namespace txservice
