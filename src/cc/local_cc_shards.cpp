#include "cc/local_cc_shards.h"

#include "store/data_store_handler.h"
#include "tx_execution.h"
#include "tx_service.h"
#include "tx_util.h"

namespace txservice
{
std::atomic<uint64_t> LocalCcShards::local_clock(0);

LocalCcShards::LocalCcShards(uint32_t node_id,
                             uint16_t core_cnt,
                             uint32_t memory_limit_mb,
                             uint32_t log_limit_mb,
                             bool realtime_sampling,
                             CatalogFactory *catalog_factory,
                             store::DataStoreHandler *store_hd,
                             metrics::MetricsRegistry *metrics_registry,
                             TxService *tx_service,
                             bool enable_mvcc)
    : store_hd_(store_hd),
      metrics_registry_(metrics_registry),
      node_id_(node_id),
      timer_terminate_(false),
      is_waiting_ckpt_(false),
      catalog_factory_(catalog_factory),
      tx_service_(tx_service),
      enable_mvcc_(enable_mvcc),
      data_sync_worker_num_(core_cnt),
      data_sync_worker_status_(WorkerStatus::Active),
      slice_worker_num_(core_cnt * 2),
      slice_thd_status_(WorkerStatus::Active),
      flush_worker_num_(core_cnt),
      flush_worker_thd_status_(WorkerStatus::Active)
{
    using namespace std::chrono_literals;
    uint64_t ts_base = std::chrono::duration_cast<std::chrono::microseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count();
    ts_base_.store(ts_base);
    local_clock.store(ts_base);
    timer_thd_ = std::thread([this] { TimerRun(); });

    for (uint16_t thd_idx = 0; thd_idx < core_cnt; ++thd_idx)
    {
        cc_shards_.emplace_back(std::make_unique<CcShard>(thd_idx,
                                                          core_cnt,
                                                          memory_limit_mb,
                                                          log_limit_mb,
                                                          realtime_sampling,
                                                          node_id,
                                                          *this,
                                                          catalog_factory_));
    }

    // Starts flush worker threads firstly.
    for (int id = 0; id < flush_worker_num_; id++)
    {
        flush_worker_thds_.push_back(
            std::thread([this] { FlushDataWorker(); }));
    }

    for (int id = 0; id < slice_worker_num_; id++)
    {
        update_slice_spec_thds_.push_back(
            std::thread([this] { UpdateSliceSpecWorker(); }));
    }

    // Starts datasync worker threads.
    for (int id = 0; id < data_sync_worker_num_; id++)
    {
        data_sync_worker_thds_.push_back(
            std::thread([this] { DataSyncWorker(); }));
    }
}

LocalCcShards::LocalCcShards(uint32_t node_id,
                             uint16_t core_cnt,
                             uint32_t memory_limit_mb,
                             uint32_t log_limit_mb,
                             bool realtime_sampling,
                             CatalogFactory *catalog_factory,
                             store::DataStoreHandler *store_hd,
                             TxService *tx_service,
                             bool enable_mvcc)
    : LocalCcShards(node_id,
                    core_cnt,
                    memory_limit_mb,
                    log_limit_mb,
                    realtime_sampling,
                    catalog_factory,
                    store_hd,
                    nullptr,
                    tx_service,
                    enable_mvcc)
{
}

LocalCcShards::~LocalCcShards()
{
    timer_terminate_.store(true, std::memory_order_release);
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
    while (!timer_terminate_.load(std::memory_order_acquire))
    {
        using namespace std::chrono_literals;

        uint64_t clock_ts =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count();
        LocalCcShards::local_clock.store(clock_ts, std::memory_order_relaxed);
        UpdateTsBase(clock_ts);

        std::this_thread::sleep_for(2s);
    }
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
            catalog_image.empty()
                ? nullptr
                : catalog_factory_->CreateTableSchema(
                      table_name, catalog_image, commit_ts, cc_ng_id),
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
                          table_name, catalog_image, commit_ts, cc_ng_id),
                commit_ts);
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
                      table_name, old_catalog_image, old_schema_ts, cc_ng_id),
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
                      table_name, new_catalog_image, dirty_schema_ts, cc_ng_id),
            dirty_schema_ts);
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
            catalog_image.empty()
                ? nullptr
                : catalog_factory_->CreateTableSchema(
                      table_name, catalog_image, commit_ts, cc_ng_id),
            commit_ts);
    }

    return &catalog_entry;
}

void LocalCcShards::CommitDirtyCatalog(const TableName &table_name,
                                       NodeGroupId cc_ng_id)
{
    std::unique_lock<std::shared_mutex> lk(meta_data_mux_);

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

std::vector<TableName> LocalCcShards::GetCatalogTableNamesForCkpt(
    NodeGroupId cc_ng_id)
{
    std::vector<TableName> tables;
    std::shared_lock<std::shared_mutex> lk(meta_data_mux_);
    for (const auto &[base_table_name, ng_catalog_map] : table_catalogs_)
    {
        auto catalog_it = ng_catalog_map.find(cc_ng_id);
        if (catalog_it != ng_catalog_map.end())
        {
            const CatalogEntry &catalog_entry = catalog_it->second;
            if (catalog_entry.schema_ != nullptr)
            {
                tables.emplace_back(base_table_name.StringView().data(),
                                    base_table_name.StringView().size(),
                                    base_table_name.Type());
                for (const txservice::TableName &index_table_name :
                     catalog_entry.schema_->IndexNames())
                {
                    tables.emplace_back(index_table_name.StringView().data(),
                                        index_table_name.StringView().size(),
                                        index_table_name.Type());
                }
            }
        }
    }
    return tables;
}

void LocalCcShards::CreateSchemaRecoveryTx(
    const ::txlog::SchemaOpMessage &schema_op_msg,
    uint64_t txn,
    int64_t tx_term,
    uint64_t commit_ts)
{
    TransactionExecution *txm = tx_service_->NewTx();
    txm->RecoverSchemaTx(schema_op_msg, txn, tx_term, commit_ts);
}

void LocalCcShards::CreateRemoteStatisticsTx(
    TableName &&table_or_index_name,
    uint64_t schema_version,
    remote::NodeGroupSamplePool &&remote_sample_pool)
{
    TransactionExecution *txm = NewTxInit(
        tx_service_, IsolationLevel::Serializable, CcProtocol::Locking);
    if (txm)
    {
        txm->RemoteStatisticsTx(
            table_or_index_name, schema_version, remote_sample_pool);

        CommitTxRequest commit_req;
        commit_req.Reset();
        txm->Execute(&commit_req);
        commit_req.Wait();
        assert(commit_req.Result() == true);
    }
}

void LocalCcShards::CreateSplitRangeRecoveryTx(
    const ::txlog::SplitRangeOpMessage &ds_split_range_op_msg,
    const TableSchema *table_schema,
    int32_t partition_id,
    const TxKey *start_key,
    const TxKey *end_key,
    const RangeInfo *range_info,
    std::vector<std::unique_ptr<TxKey>> &&new_range_keys,
    std::vector<int32_t> &&new_partition_ids,
    uint32_t node_group_id,
    uint64_t txn,
    int64_t tx_term,
    uint64_t commit_ts,
    std::optional<std::pair<CcEntryAddr, ReadSetEntry>> catalog_cc_entry,
    std::shared_ptr<std::atomic_uint32_t> split_tx_started)
{
    // Mark the table as sync in progress to avoid concurrent data sync before
    // range split tx finishes if this is the first started range split.
    if (split_tx_started->fetch_add(1) == 0)
    {
        const TableName range_table_name = TableName{
            ds_split_range_op_msg.table_name(), TableType::RangePartition};
        const TableName base_table_name = TableName{
            range_table_name.GetBaseTableNameSV(), TableType::Primary};
        SetDataSyncOngoing(base_table_name, node_group_id, true);
    }

    TransactionExecution *txm = tx_service_->NewTx();
    txm->RecoverSplitRangeTx(ds_split_range_op_msg,
                             table_schema,
                             partition_id,
                             start_key,
                             end_key,
                             range_info,
                             std::move(new_range_keys),
                             std::move(new_partition_ids),
                             node_group_id,
                             txn,
                             tx_term,
                             commit_ts,
                             std::move(catalog_cc_entry),
                             split_tx_started);
}

void LocalCcShards::InitTableRanges(const TableName &range_table_name,
                                    std::vector<InitRangeEntry> &init_ranges,
                                    NodeGroupId ng_id,
                                    bool fully_cached)
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
    if (!ngs_it.second)
    {
        // Table range already initialized by another FecthTableRangesCc
        return;
    }
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

        std::unique_ptr<StoreRange> range_slices = nullptr;
        if (ng_id ==
            range_entry.partition_id_ % Sharder::Instance().NodeGroupCount())
        {
            InitRangeEntry &next_range_entry = init_ranges[pidx + 1];
            range_slices =
                std::make_unique<StoreRange>(range_start_key,
                                             next_range_entry.key_.get(),
                                             range_entry.partition_id_,
                                             *this);
            range_slices->InitSlices(range_entry.slice_keys_, fully_cached);
        }
        auto res = ranges.try_emplace(range_start_key,
                                      std::move(range_entry.key_),
                                      range_entry.version_ts_,
                                      range_entry.partition_id_,
                                      range_entry.Bytes(),
                                      std::move(range_slices));
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
    std::unique_ptr<StoreRange> range_slices = nullptr;
    if (ng_id ==
        last_range_entry.partition_id_ % Sharder::Instance().NodeGroupCount())
    {
        range_slices = std::make_unique<StoreRange>(
            range_start_key, nullptr, last_range_entry.partition_id_, *this);
        range_slices->InitSlices(last_range_entry.slice_keys_, fully_cached);
    }

    auto res = ranges.try_emplace(range_start_key,
                                  std::move(last_range_entry.key_),
                                  last_range_entry.version_ts_,
                                  last_range_entry.partition_id_,
                                  last_range_entry.Bytes(),
                                  std::move(range_slices));
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
        table_range.second.erase(ng_id);
    }
    for (auto &range_id : table_range_ids_)
    {
        range_id.second.erase(ng_id);
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
    std::unique_lock<std::shared_mutex> lk(meta_data_mux_);
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

const TableRangeEntry *LocalCcShards::GetTableRangeEntryNonLocking(
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
    const TxKey *end_key,
    uint64_t version,
    std::vector<std::tuple<TxKey::Uptr, uint32_t, SliceStatus>> *slice_keys)
{
    std::unique_lock<std::shared_mutex> lk(meta_data_mux_);
    std::vector<TableRangeEntry *> new_entries;

    std::map<const TxKey *, TableRangeEntry, PtrLessThan<TxKey>> *ranges =
        GetTableRangesForATableInternal(table_name, ng_id);
    std::unordered_map<uint32_t, TableRangeEntry *> *range_ids =
        GetTableRangeIdsForATableInternal(table_name, ng_id);
    std::unique_ptr<StoreRange> range_slices = nullptr;
    if (ng_id == partition_id % Sharder::Instance().NodeGroupCount())
    {
        range_slices = std::make_unique<StoreRange>(
            start_key.get(), end_key, partition_id, *this);
        range_slices->InitSlices(*slice_keys);
    }

    uint64_t range_bytes =
        slice_keys == nullptr
            ? 0
            : std::accumulate(
                  slice_keys->begin(),
                  slice_keys->end(),
                  0UL,
                  [](uint64_t a,
                     const std::tuple<TxKey::Uptr, uint32_t, SliceStatus> &b)
                  { return a + std::get<1>(b); });
    auto new_range_entry_pair = ranges->try_emplace(start_key.get(),
                                                    std::move(start_key),
                                                    version,
                                                    partition_id,
                                                    range_bytes,
                                                    std::move(range_slices));

    bool updated = false;

    if (!new_range_entry_pair.second)
    {
        if (new_range_entry_pair.first->second.Version() > version)
        {
            if (ng_id == partition_id % Sharder::Instance().NodeGroupCount())
            {
                range_slices = std::make_unique<StoreRange>(
                    new_range_entry_pair.first->second.GetRangeInfo()
                        ->StartKey(),
                    end_key,
                    partition_id,
                    *this);
                range_slices->InitSlices(*slice_keys);
            }
            new_range_entry_pair.first->second.UpdateRangeEntry(
                version, range_bytes, std::move(range_slices));
            updated = true;
        }
    }
    else
    {
        updated = true;
    }

    if (updated)
    {
        // The new inserted range is always not the smallest range since
        // negative inf is one of the first default range start key.
        auto prev_it = std::prev(new_range_entry_pair.first);
        if (prev_it->second.RangeSlices())
        {
            prev_it->second.RangeSlices()->SetRangeEndKey(
                new_range_entry_pair.first->second.GetRangeInfo()->StartKey());
        }
        range_ids->try_emplace(partition_id,
                               &new_range_entry_pair.first->second);
    }

    return &new_range_entry_pair.first->second;
}

RangeSliceId LocalCcShards::PinRangeSlice(const TableName &table_name,
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
                                          bool force_load)
{
    std::shared_lock<std::shared_mutex> lk(meta_data_mux_);

    TableName range_table_name(table_name.StringView(),
                               TableType::RangePartition);

    TableRangeEntry *entry =
        GetTableRangeEntryInternal(range_table_name, ng_id, &key);
    if (!entry)
    {
        // Table range info not initialized, initialize range info first
        cc_shard->FetchTableRanges(
            range_table_name, kv_info, cc_request, ng_id);
        pin_status = RangeSliceOpStatus::BlockedOnLoad;
        return RangeSliceId(nullptr, nullptr);
    }
    if (!entry->RangeSlices())
    {
        pin_status = RangeSliceOpStatus::NotOwner;
        return RangeSliceId(nullptr, nullptr);
    }

    return entry->RangeSlices()->PinSlice(table_name,
                                          key,
                                          inclusive,
                                          key_schema,
                                          rec_schema,
                                          schema_ts,
                                          INT64_MAX,
                                          kv_info,
                                          cc_request,
                                          cc_shard,
                                          store_hd_,
                                          pin_status,
                                          force_load);
}

RangeSliceId LocalCcShards::PinRangeSlice(const TableName &table_name,
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
                                          bool force_load)
{
    std::shared_lock<std::shared_mutex> lk(meta_data_mux_);

    TableName range_table_name(table_name.StringView(),
                               TableType::RangePartition);

    TableRangeEntry *entry =
        GetTableRangeEntryInternal(range_table_name, ng_id, range_id);
    if (!entry)
    {
        // Table range info not initialized, initialize range info first
        cc_shard->FetchTableRanges(
            range_table_name, kv_info, cc_request, ng_id);
        pin_status = RangeSliceOpStatus::BlockedOnLoad;
        return RangeSliceId(nullptr, nullptr);
    }
    if (!entry->RangeSlices())
    {
        pin_status = RangeSliceOpStatus::NotOwner;
        return RangeSliceId(nullptr, nullptr);
    }

    uint64_t snapshot_ts = 0;
    if (EnableMvcc())
    {
        snapshot_ts = TxStartTsCollector::Instance().GlobalMinSiTxStartTs();
    }

    return entry->RangeSlices()->PinSlice(table_name,
                                          key,
                                          inclusive,
                                          key_schema,
                                          rec_schema,
                                          schema_ts,
                                          snapshot_ts,
                                          kv_info,
                                          cc_request,
                                          cc_shard,
                                          store_hd_,
                                          pin_status,
                                          force_load);
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
        LOG(ERROR) << " The range table of " << table_name.StringView()
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
    TableName range_table_name(table_name.StringView(),
                               TableType::RangePartition);
    const std::map<const TxKey *, TableRangeEntry, PtrLessThan<TxKey>> &ranges =
        table_ranges_.at(range_table_name).at(ng_id);
    uint64_t counts = std::accumulate(
        ranges.begin(),
        ranges.end(),
        0UL,
        [key_ng_id](uint64_t a,
                    const std::pair<const TxKey *const, TableRangeEntry> &b)
        {
            int32_t partition_id = b.second.GetRangeInfo()->PartitionId();
            uint32_t ng_cnt = Sharder::Instance().NodeGroupCount();
            if (partition_id % ng_cnt == key_ng_id)
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
    uint32_t ng_cnt = Sharder::Instance().NodeGroupCount();

    const std::map<const TxKey *, TableRangeEntry, PtrLessThan<TxKey>> &ranges =
        table_ranges_.at(range_table_name).at(ng_id);

    for (auto &[range_start_key, range_entry] : ranges)
    {
        if (range_entry.GetRangeInfo()->PartitionId() % ng_cnt == local_ng_id)
        {
            const StoreRange *store_range = range_entry.RangeSlices();
            assert(store_range != nullptr);
            slices += store_range->Slices().size();
        }
    }

    return slices;
}

std::vector<uint64_t> LocalCcShards::AllNodeGroupBytesAtFetchRange(
    const TableName &table_name, const NodeGroupId ng_id) const
{
    std::shared_lock<std::shared_mutex> lk(meta_data_mux_);
    TableName range_table_name(table_name.StringView(),
                               TableType::RangePartition);

    uint32_t ng_cnt = Sharder::Instance().NodeGroupCount();
    std::vector<uint64_t> ng_bytes_vec(ng_cnt, 0);

    const std::map<const TxKey *, TableRangeEntry, PtrLessThan<TxKey>> &ranges =
        table_ranges_.at(range_table_name).at(ng_id);

    for (auto &[range_start_key, range_entry] : ranges)
    {
        NodeGroupId ng_id = range_entry.GetRangeInfo()->PartitionId() % ng_cnt;
        ng_bytes_vec.at(ng_id) += range_entry.RangeBytesAtFetch();
    }

    return ng_bytes_vec;
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
                              CcHandlerResult<Void> &hres)
{
    std::unique_lock<std::mutex> flush_worker_lk(flush_worker_mux_);
    pending_flush_work_.emplace_back(node_group,
                                     term,
                                     data_sync_ts,
                                     table_name,
                                     schema,
                                     data_sync_vec,
                                     archive_vec,
                                     mv_vec,
                                     &hres);
    flush_worker_cv_.notify_one();
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

bool LocalCcShards::KickoutRangeSlice(const TableName &tbl_name,
                                      const NodeGroupId ng_id,
                                      const TxKey &key)
{
    std::shared_lock<std::shared_mutex> s_lk(meta_data_mux_);

    TableName range_tbl_name(tbl_name.StringView(), TableType::RangePartition);
    TableRangeEntry *entry =
        GetTableRangeEntryInternal(range_tbl_name, ng_id, &key);
    if (entry == nullptr || entry->RangeSlices() == nullptr)
    {
        return true;
    }
    else
    {
        return entry->RangeSlices()->KickoutSlice(key);
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

std::pair<Statistics *, bool> LocalCcShards::InitTableStatistics(
    const TableName &table_name, NodeGroupId ng_id)
{
    std::unique_lock<std::shared_mutex> lk(meta_data_mux_);

    auto ng_statistics_it = table_statistics_map_.try_emplace(table_name);
    auto statistics_it = ng_statistics_it.first->second.try_emplace(ng_id);
    if (statistics_it.second)
    {
        StatisticsEntry &statistics_entry = statistics_it.first->second;

        statistics_entry.statistics_ =
            catalog_factory_->CreateTableStatistics(table_name);
    }

    return {statistics_it.first->second.statistics_.get(),
            statistics_it.second};
}

std::pair<Statistics *, bool> LocalCcShards::InitTableStatistics(
    const TableName &table_name,
    const TableSchema *table_schema,
    NodeGroupId ng_id,
    std::unordered_map<TableName, std::pair<uint64_t, std::vector<TxKey::Uptr>>>
        &&sample_pool_map,
    const std::unordered_map<TableName, std::vector<uint64_t>> &ng_weights_map,
    CcShard *ccs)
{
    std::unique_lock<std::shared_mutex> lk(meta_data_mux_);

    auto ng_statistics_it = table_statistics_map_.try_emplace(table_name);
    auto statistics_it = ng_statistics_it.first->second.try_emplace(ng_id);
    if (statistics_it.second)
    {
        StatisticsEntry &statistics_entry = statistics_it.first->second;

        statistics_entry.statistics_ =
            catalog_factory_->CreateTableStatistics(table_name,
                                                    table_schema,
                                                    std::move(sample_pool_map),
                                                    ng_weights_map,
                                                    ccs,
                                                    ng_id);
    }

    return {statistics_it.first->second.statistics_.get(),
            statistics_it.second};
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

void LocalCcShards::EnqueueDataSyncTask(const TableName &table_name,
                                        uint32_t ng_id,
                                        int64_t ng_term,
                                        uint64_t data_sync_ts,
                                        std::mutex *task_sender_mux,
                                        std::condition_variable *task_sender_cv,
                                        uint16_t *finished_task_cnt,
                                        std::atomic_bool *tasks_failed,
                                        bool is_forward,
                                        CcHandlerResult<Void> *hres)
{
    std::lock_guard<std::mutex> task_worker_lk(task_worker_mux_);
    data_sync_task_queue_.emplace_back(
        std::make_shared<DataSyncTask>(table_name,
                                       ng_id,
                                       ng_term,
                                       data_sync_ts,
                                       task_sender_mux,
                                       task_sender_cv,
                                       finished_task_cnt,
                                       tasks_failed,
                                       is_forward,
                                       hres));
    task_worker_cv_.notify_one();
}

void LocalCcShards::Terminate()
{
    // Terminate the data sync task worker thds.
    {
        std::unique_lock<std::mutex> task_worker_lk(task_worker_mux_);
        assert(data_sync_worker_status_ == WorkerStatus::Active);
        data_sync_worker_status_ = WorkerStatus::Terminated;
    }
    task_worker_cv_.notify_all();

    for (int idx = 0; idx < data_sync_worker_num_; ++idx)
    {
        data_sync_worker_thds_.at(idx).join();
    }

    // Terminate the flush worker thds.
    {
        std::unique_lock<std::mutex> flush_worker_lk(flush_worker_mux_);
        assert(flush_worker_thd_status_ == WorkerStatus::Active);
        flush_worker_thd_status_ = WorkerStatus::Terminated;
    }
    flush_worker_cv_.notify_all();

    for (int idx = 0; idx < flush_worker_num_; ++idx)
    {
        flush_worker_thds_.at(idx).join();
    }

    {
        std::unique_lock<std::mutex> worker_lk(slice_update_mux_);
        slice_thd_status_ = WorkerStatus::Terminated;
    }

    slice_update_cv_.notify_all();
    for (int id = 0; id < slice_worker_num_; id++)
    {
        update_slice_spec_thds_.at(id).join();
    }
}

bool LocalCcShards::SetDataSyncOngoing(const TableName &base_table_name,
                                       NodeGroupId node_group_id,
                                       bool is_ongoing)
{
    // Find or emplace item from table sync status for this table.
    std::unique_lock<std::mutex> task_worker_lk(task_worker_mux_);
    auto tbl_statuses_it =
        tables_sync_status_.try_emplace(base_table_name).first;
    std::unordered_map<NodeGroupId, TableDataSyncStatus> &ng_tbl_statuses =
        tbl_statuses_it->second;
    auto res_pair = ng_tbl_statuses.try_emplace(node_group_id);
    TableDataSyncStatus &sync_status = res_pair.first->second;

    if (sync_status.is_ongoing_ && is_ongoing)
    {
        return false;
    }
    sync_status.is_ongoing_ = is_ongoing;
    if (!is_ongoing && sync_status.pending_task_.size() > 0)
    {
        // Put the waiting task back in processing queue.
        data_sync_task_queue_.push_back(
            std::move(sync_status.pending_task_.back()));
        sync_status.pending_task_.pop_back();
        task_worker_cv_.notify_one();
    }
    task_worker_lk.unlock();
    return true;
}

void LocalCcShards::DataSyncWorker()
{
    std::unique_lock<std::mutex> task_worker_lk(task_worker_mux_);

    while (data_sync_worker_status_ == WorkerStatus::Active)
    {
        task_worker_cv_.wait(task_worker_lk,
                             [this]
                             {
                                 return !data_sync_task_queue_.empty() ||
                                        data_sync_worker_status_ !=
                                            WorkerStatus::Active;
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

void LocalCcShards::DataSync(std::unique_lock<std::mutex> &task_worker_lk)
{
    std::shared_ptr<DataSyncTask> data_sync_task =
        data_sync_task_queue_.front();
    // Whether other task worker is processing this table.
    const TableName &table_name = data_sync_task->table_name_;
    uint32_t ng_id = data_sync_task->node_group_id_;
    uint64_t target_data_sync_ts = data_sync_task->data_sync_ts_;
    bool is_forward = data_sync_task->is_forward_;

    // Find or emplace item from table sync status for this table.
    auto tbl_statuses_it = tables_sync_status_.try_emplace(table_name).first;
    std::unordered_map<NodeGroupId, TableDataSyncStatus> &ng_tbl_statuses =
        tbl_statuses_it->second;
    auto res_pair = ng_tbl_statuses.try_emplace(ng_id);
    TableDataSyncStatus &sync_status = res_pair.first->second;
    bool need_process = true;
    if (!res_pair.second)
    {
        // Fast path in two cases to process this task as below:
        need_process = false;
        if (sync_status.last_sync_ts_ >= target_data_sync_ts)
        {
            // 1) Have been synchronized by other task worker, return finish
            // directly.
            data_sync_task->SetFinish();
        }
        else if (sync_status.is_ongoing_)
        {
            // 2) Another task is processing this table, waitting.
            // To avoid the possible busy loop when there are fewer tasks, put
            // this task into `pending_task` instead of put back into
            // `data_sync_task_queue_`.
            sync_status.pending_task_.push_back(std::move(data_sync_task));
        }
        else
        {
            // Indicate this task worker is processing the task.
            assert(sync_status.is_ongoing_ == false);
            sync_status.is_ongoing_ = true;
            need_process = true;
        }
    }
    else
    {
        assert(sync_status.is_ongoing_ == false);
        sync_status.is_ongoing_ = true;
    }

    data_sync_task_queue_.pop_front();
    if (!need_process)
    {
        return;
    }

    assert(sync_status.is_ongoing_ == true);
    task_worker_lk.unlock();

    // Check the leader
    int64_t ng_term = Sharder::Instance().LeaderTerm(ng_id);
    if (ng_term < 0)
    {
        LOG(ERROR) << "DataSync: node not the leader of this node group.";
        // Finish this task and notify the caller.
        data_sync_task->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);

        task_worker_lk.lock();
        sync_status.is_ongoing_ = false;
        // Handle the pending tasks for the same table
        if (sync_status.pending_task_.size() > 0)
        {
            data_sync_task_queue_.push_back(
                std::move(sync_status.pending_task_.back()));
            sync_status.pending_task_.pop_back();
        }
        return;
    }

    // Process this task.
    // 1. Get a new txm and init
    TransactionExecution *data_sync_txm = tx_service_->NewTx();

    InitTxRequest init_req;
    // Set isolation level to RepeatableRead to ensure the readlock
    // will be set during the execution of the following
    // ReadTxRequest.
    init_req.iso_level_ = IsolationLevel::RepeatableRead;
    init_req.protocol_ = CcProtocol::Locking;
    init_req.Reset();
    data_sync_txm->Execute(&init_req);
    init_req.Wait();

    if (init_req.IsError())
    {
        LOG(ERROR) << "DataSync init data sync transaction failed.";
        task_worker_lk.lock();
        // Put back into the beginning.
        data_sync_task_queue_.emplace_front(std::move(data_sync_task));
        // The txm has been freed.
        // Update the table sync status
        sync_status.is_ongoing_ = false;
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

    if (read_req.IsError() || read_req.Result() != RecordStatus::Normal)
    {
        // Use AbortTxRequest to release read lock.
        AbortTxRequest abort_req;
        data_sync_txm->Execute(&abort_req);
        abort_req.Wait();
        assert(abort_req.Result() == false);

        task_worker_lk.lock();
        if (read_req.Result() != RecordStatus::Normal)
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
            data_sync_task_queue_.emplace_front(std::move(data_sync_task));
        }
        sync_status.is_ongoing_ = false;
        return;
    }

    // 3. Scan records.
    // The data sync worker thread is the owner of those vectors.
    std::vector<std::vector<FlushRecord>> data_sync_vecs;
    std::vector<std::vector<FlushRecord>> archive_vecs;
    std::vector<std::vector<const TxKey *>> mv_base_vecs;

    std::vector<std::pair<TxKey::Uptr, bool>> resume_pos;
    for (size_t i = 0; i < cc_shards_.size(); i++)
    {
        data_sync_vecs.emplace_back();
        archive_vecs.emplace_back();
        mv_base_vecs.emplace_back();
        resume_pos.emplace_back(nullptr, false);
    }

    bool scan_data_drained = false;
    DataSyncScanCc scan_cc(table_name,
                           target_data_sync_ts,
                           ng_id,
                           cc_shards_.size(),
                           std::move(resume_pos),
                           DATA_SYNC_SCAN_BATCH_SIZE);
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
            AbortTxRequest abort_req;
            abort_req.Reset();
            data_sync_txm->Execute(&abort_req);
            abort_req.Wait();
            task_worker_lk.lock();
            // Put back into the beginning.
            data_sync_task_queue_.emplace_front(std::move(data_sync_task));
            // The txm has been freed.
            // Update the table data sync status.
            sync_status.is_ongoing_ = false;
            return;
        }
        else
        {
            auto &res = scan_cc.Result();
            scan_data_drained = true;

            for (size_t i = 0; i < cc_shards_.size(); i++)
            {
                // if the data is drained
                scan_data_drained = res.at(i).second && scan_data_drained;
                // move the bucket into the tank
                std::move(scan_cc.DataSyncVec(i).begin(),
                          scan_cc.DataSyncVec(i).end(),
                          std::back_inserter(data_sync_vecs.at(i)));

                std::move(scan_cc.ArchiveVec(i).begin(),
                          scan_cc.ArchiveVec(i).end(),
                          std::back_inserter(archive_vecs.at(i)));

                std::move(scan_cc.MoveBaseVec(i).begin(),
                          scan_cc.MoveBaseVec(i).end(),
                          std::back_inserter(mv_base_vecs.at(i)));
            }
            scan_cc.Reset(std::move(res));
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
    MergeSortedVectors(
        std::move(mv_base_vecs), *mv_base_vec, key_greater, true);
    auto rec_greater = [](const FlushRecord &r1, const FlushRecord &r2) -> bool
    { return *r2.Key() < *r1.Key(); };
    // To avoid repeatedly set the ckpt_ts_ of a cc entry, which might
    // cause the ccentry become invalid in between, remove duplicate
    // flush record from ckpt_vec.
    MergeSortedVectors(
        std::move(data_sync_vecs), *data_sync_vec, rec_greater, true);
    // For archive vec we don't need to worry about duplicate causing
    // issue since we're not visiting their cc entry. Also we cannot
    // rely on key compare to dedup archive vec since a key could have
    // multiple version of archive versions.
    MergeSortedVectors(
        std::move(archive_vecs), *archive_vec, rec_greater, false);

    // 4. Process the data sync vec
    // Since we do not know how many split range workers are needed,
    // we set the start value of unfinished_worker as 1, which is the flush
    // worker. In this case the split range workers that are launched first
    // will never see unfinished_worker == 0 if they finish earlier
    // than the last launched flush data worker.
    data_sync_task->unfinished_worker_ = 1;
    const TableSchema *table_schema =
        is_forward ? catalog_rec.DirtySchema() : catalog_rec.Schema();
#ifdef RANGE_PARTITION_ENABLED
    // 4.1 For range partition, execute range split if necessary using
    // seperate thread per range.
    std::vector<std::pair<const TxKey *, const TxKey *>> split_ranges;
    size_t batch_idx = 0;

    while (batch_idx < data_sync_vec->size())
    {
        std::pair<const StoreRange *, std::vector<const TxKey *>> split_pair;
        split_pair.first = nullptr;

        bool ret = UpdateSliceAndCalculateRangeUpdate(table_name,
                                                      table_schema,
                                                      ng_id,
                                                      *data_sync_vec,
                                                      target_data_sync_ts,
                                                      batch_idx,
                                                      split_pair);

        if (!ret)
        {
            LOG(ERROR) << "Pre-data_sync slice update failed on table "
                       << table_name.StringView();
            AbortTxRequest abort_req;
            abort_req.Reset();
            data_sync_txm->Execute(&abort_req);
            abort_req.Wait();
            task_worker_lk.lock();
            if (data_sync_task->SetError())
            {
                // Indicating that the task worker has processed this task.
                sync_status.is_ongoing_ = false;

                // Handle the pending tasks for the same table
                if (sync_status.pending_task_.size() > 0)
                {
                    data_sync_task_queue_.push_back(
                        std::move(sync_status.pending_task_.back()));
                    sync_status.pending_task_.pop_back();
                }
            }

            return;
        }

        if (split_pair.first != nullptr)
        {
            const StoreRange *split_range = split_pair.first;
            split_ranges.emplace_back(split_range->RangeStartKey(),
                                      split_range->RangeEndKey());

            // Create a new thread to execute range split.
            data_sync_task->unfinished_worker_.fetch_add(
                1, std::memory_order_relaxed);
            auto range_split_worker = std::thread(
                [this,
                 &table_name,
                 split_info = std::move(split_pair),
                 &ng_id,
                 is_forward,
                 data_sync_task]
                {
                    SplitFlushRange(table_name,
                                    ng_id,
                                    is_forward,
                                    split_info,
                                    data_sync_task);
                });
            range_split_worker.detach();
        }
    }

    if (!split_ranges.empty())
    {
        // Remove the records that are in the splitting ranges.
        // They will  be handled by the splitting  worker.

        // Remove mv base vec first since it contains raw pointers
        // to the records in ckpt_vec and archive_vec
        auto key_lower_bound_cmp = [](const TxKey *key1, const TxKey &key2)
        { return *key1 < key2; };
        std::unique_ptr<std::vector<const TxKey *>> flush_mv_base_vec =
            std::make_unique<std::vector<const TxKey *>>();
        MoveNonSplittingRecords(*mv_base_vec,
                                *flush_mv_base_vec,
                                split_ranges,
                                key_lower_bound_cmp);
        mv_base_vec = std::move(flush_mv_base_vec);

        auto lower_bound_cmp = [](const FlushRecord &rec, const TxKey &key)
        { return *rec.Key() < key; };
        std::unique_ptr<std::vector<FlushRecord>> flush_ckpt_vec =
            std::make_unique<std::vector<FlushRecord>>();
        MoveNonSplittingRecords(
            *data_sync_vec, *flush_ckpt_vec, split_ranges, lower_bound_cmp);
        data_sync_vec = std::move(flush_ckpt_vec);

        std::unique_ptr<std::vector<FlushRecord>> flush_archive_vec =
            std::make_unique<std::vector<FlushRecord>>();
        MoveNonSplittingRecords(
            *archive_vec, *flush_archive_vec, split_ranges, lower_bound_cmp);
        archive_vec = std::move(flush_archive_vec);
    }
#endif

    // 4.2 Flush records into data store if the range in which the records
    // locate need't to split.
    // Put those records into flush data worker thread, and wait the result.

    if (data_sync_vec->size() != 0 || archive_vec->size() != 0 ||
        mv_base_vec->size() != 0)
    {
        std::unique_lock<std::mutex> worker_lk(flush_worker_mux_);
        pending_flush_work_.emplace_back(data_sync_task,
                                         table_schema,
                                         std::move(data_sync_vec),
                                         std::move(archive_vec),
                                         std::move(mv_base_vec),
                                         data_sync_txm);
        flush_worker_cv_.notify_one();
    }
    else
    {
        bool ok = catalog_rec.Schema()->StatisticsObject()->PostCheckpoint(
            store_hd_,
            table_name,
            table_schema,
            ng_id,
            target_data_sync_ts,
            true);
        if (!ok)
        {
            AbortTxRequest abort_req;
            data_sync_txm->Execute(&abort_req);
            abort_req.Wait();
            if (data_sync_task->SetError())
            {
                // Go to process next task.
                task_worker_lk.lock();
                // Indicating that the task worker has processed this task.
                sync_status.is_ongoing_ = false;

                // Handle the pending tasks for the same table
                if (sync_status.pending_task_.size() > 0)
                {
                    data_sync_task_queue_.push_back(
                        std::move(sync_status.pending_task_.back()));
                    sync_status.pending_task_.pop_back();
                }
            }
        }
        else
        {
            // Commit the data sync txm
            CommitTxRequest commit_req;
            commit_req.Reset();
            data_sync_txm->Execute(&commit_req);
            commit_req.Wait();
            // Since we count the flush data worker in by default at the
            // beginning, we need to decrease the worker cnt here.
            if (data_sync_task->SetFinish())
            {
                // Go to process next task.
                task_worker_lk.lock();
                if (!data_sync_task->IsError())
                {
                    // Update the task status for this table.
                    sync_status.last_sync_ts_ = target_data_sync_ts;
                }
                // Indicating that the task worker has processed this task.
                sync_status.is_ongoing_ = false;

                // Handle the pending tasks for the same table
                if (sync_status.pending_task_.size() > 0)
                {
                    data_sync_task_queue_.push_back(
                        std::move(sync_status.pending_task_.back()));
                    sync_status.pending_task_.pop_back();
                }
            }
        }
    }

    if (!task_worker_lk.owns_lock())
    {
        // Need to gain ownership before returnning back to caller.
        task_worker_lk.lock();
    }
}

bool LocalCcShards::UpdateSliceAndCalculateRangeUpdate(
    const TableName &table_name,
    const TableSchema *schema,
    NodeGroupId node_group_id,
    std::vector<FlushRecord> &flush_batch,
    uint64_t data_sync_ts,
    size_t &batch_idx,
    std::pair<const StoreRange *, std::vector<const TxKey *>> &splitting_info)
{
    std::mutex work_sender_mux;
    std::condition_variable work_sender_cv;
    size_t slice_update_done = 0;
    size_t slice_load_cnt = 0;
    bool fail = false;

    auto lower_bound_cmp = [](const FlushRecord &rec, const TxKey &key)
    { return *rec.Key() < key; };

    while (batch_idx < flush_batch.size())
    {
        const TxKey &range_start_key = *flush_batch[batch_idx].Key();

        // Finds the range.
        TableRangeEntry *range_entry = const_cast<TableRangeEntry *>(
            GetTableRangeEntry(table_name, node_group_id, &range_start_key));
        if (range_entry == nullptr)
        {
            // Range table not initialized yet. Issue a read request
            // to range table to create it and initialize its range
            // info in local cc shards.
            // We need to release the read lock on range immediately
            // after fetching the range info from data store so that it
            // does not block potential split-flush tx.
            TransactionExecution *txm = tx_service_->NewTx();
            InitTxRequest init_req;
            // Set isolation level to RepeatableRead to ensure the
            // readlock will be set during the execution of the
            // following ReadTxRequest.
            init_req.iso_level_ = IsolationLevel::RepeatableRead;
            init_req.protocol_ = CcProtocol::Locking;
            init_req.Reset();
            txm->Execute(&init_req);
            init_req.Wait();

            if (init_req.IsError())
            {
                AbortTxRequest abort_req;
                abort_req.Reset();
                txm->Execute(&abort_req);
                abort_req.Wait();

                LOG(ERROR) << "Fail to find the range for the checkpoint key, "
                           << table_name.StringView();
                return false;
            }
            TableName range_table_name(table_name.StringView(),
                                       TableType::RangePartition);
            RangeRecord rec;
            ReadTxRequest read_range_req(
                &range_table_name, &range_start_key, &rec, false, false, true);
            txm->Execute(&read_range_req);
            read_range_req.Wait();
            if (read_range_req.IsError())
            {
                LOG(ERROR) << "Fail to find the range for the checkpoint key, "
                           << table_name.StringView();
                return false;
            }

            CommitTxRequest commit_req;
            txm->Execute(&commit_req);
            commit_req.Wait();
            range_entry = const_cast<TableRangeEntry *>(GetTableRangeEntry(
                table_name, node_group_id, &range_start_key));
        }

        assert(range_entry != nullptr);
        StoreRange *curr_range = range_entry->RangeSlices();
        if (curr_range == nullptr)
        {
            // Range does not belong to this ng, skips flushing records
            // belonging to this range.
            // TODO: Jumps to the record greater than or equal to the end of the
            // range.
            ++batch_idx;
            continue;
        }

        auto batch_it = flush_batch.begin() + batch_idx;
        auto range_start_it = batch_it;
        auto range_end_it = curr_range->RangeEndKey() == nullptr
                                ? flush_batch.end()
                                : std::lower_bound(batch_it,
                                                   flush_batch.end(),
                                                   *curr_range->RangeEndKey(),
                                                   lower_bound_cmp);

        while (batch_it != range_end_it)
        {
            const TxKey &slice_start_key = *batch_it->Key();
            StoreSlice *curr_slice = curr_range->FindSlice(slice_start_key);

            auto slice_end_it =
                curr_slice->EndKey() == curr_range->RangeEndKey()
                    ? range_end_it
                    : std::lower_bound(batch_it,
                                       range_end_it,
                                       *curr_slice->EndKey(),
                                       lower_bound_cmp);

            int32_t slice_delta_size = 0;
            uint32_t slice_size = 0;

            for (; batch_it != slice_end_it; ++batch_it)
            {
                slice_delta_size += batch_it->delta_size_;
            }

            int32_t sum = curr_slice->Size() + slice_delta_size;
            slice_size = sum >= 0 ? sum : 0;
            curr_slice->SetPostCkptSize(slice_size);
            batch_it = slice_end_it;
        }

        batch_idx = std::distance(flush_batch.begin(), range_end_it);

        size_t post_ckpt_size = curr_range->PostCkptSize();
        if (post_ckpt_size > StoreRange::range_max_size)
        {
            std::vector<const TxKey *> new_range_keys =
                curr_range->CalculateRangeSplitKeys(table_name,
                                                    schema,
                                                    node_group_id,
                                                    data_sync_ts,
                                                    post_ckpt_size,
                                                    range_start_it,
                                                    range_end_it,
                                                    flush_batch);

            // If the range is to be split, passes it to the caller via
            // splitting_info and stops the iteration.
            if (!new_range_keys.empty())
            {
                splitting_info.first = curr_range;
                splitting_info.second = std::move(new_range_keys);
                return true;
            }
        }
        else
        {
            // Range does not need to be splitted, to through the slices and
            // update their specs if necessary.
            auto range_batch_it = range_start_it;
            size_t slice_start_idx =
                std::distance(flush_batch.begin(), range_start_it);
            while (range_batch_it != range_end_it)
            {
                const TxKey &slice_start_key = *range_batch_it->Key();
                StoreSlice *curr_slice = curr_range->FindSlice(slice_start_key);

                auto slice_end_it =
                    curr_slice->EndKey() == curr_range->RangeEndKey()
                        ? range_end_it
                        : std::lower_bound(range_batch_it,
                                           range_end_it,
                                           *curr_slice->EndKey(),
                                           lower_bound_cmp);

                size_t slice_end_idx =
                    std::distance(flush_batch.begin(), slice_end_it);
                int32_t slice_delta_size = 0;
                uint32_t slice_size = 0;

                for (; range_batch_it != slice_end_it; ++range_batch_it)
                {
                    slice_delta_size += range_batch_it->delta_size_;
                }

                int32_t sum = curr_slice->Size() + slice_delta_size;
                slice_size = sum >= 0 ? sum : 0;
                curr_slice->SetPostCkptSize(slice_size);
                if (slice_size > StoreSlice::slice_upper_bound)
                {
                    // Since update slice specs might need loading from
                    // data store, hand it off to the worker and move on
                    // to the next slice.
                    slice_load_cnt++;
                    {
                        std::unique_lock<std::mutex> worker_lk(
                            slice_update_mux_);
                        pending_slice_work_.emplace_back(node_group_id,
                                                         data_sync_ts,
                                                         table_name,
                                                         schema,
                                                         flush_batch,
                                                         curr_range,
                                                         curr_slice,
                                                         slice_start_idx,
                                                         slice_end_idx,
                                                         work_sender_mux,
                                                         work_sender_cv,
                                                         slice_update_done,
                                                         fail);
                        slice_update_cv_.notify_one();
                    }
                }
                range_batch_it = slice_end_it;
                slice_start_idx = slice_end_idx;
            }
        }

        {
            // Wait for all slice specs in this range are updated before moving
            // on to the next range.
            std::unique_lock<std::mutex> work_sender_lk(work_sender_mux);
            work_sender_cv.wait(work_sender_lk,
                                [&slice_update_done, &slice_load_cnt] {
                                    return slice_load_cnt == slice_update_done;
                                });
            if (fail)
            {
                return false;
            }
        }

        slice_load_cnt = 0;
        slice_update_done = 0;
    }

    return true;
}

template <typename T, class Compare>
void LocalCcShards::MoveNonSplittingRecords(
    std::vector<T> &flush_vec,
    std::vector<T> &non_split_vec,
    const std::vector<std::pair<const TxKey *, const TxKey *>> &split_ranges,
    Compare lower_bound_cmp)
{
    auto flush_vec_it = flush_vec.begin();

    for (const auto &[start_key, end_key] : split_ranges)
    {
        // The inclusive start of the splitting range is the
        // exclusive end of the gap preceding of the splitting
        // range.
        auto range_start_it = std::lower_bound(
            flush_vec_it, flush_vec.end(), *start_key, lower_bound_cmp);

        size_t copy_size = std::distance(flush_vec_it, range_start_it);
        non_split_vec.reserve(non_split_vec.size() + copy_size);

        // Moves the records in the gap preceding the splitting
        // range.
        std::move(
            flush_vec_it, range_start_it, std::back_inserter(non_split_vec));

        // Jumps to the exclusive end of the splitting range, which
        // is the inclusive start of the gap succeeding the
        // splitting range.
        flush_vec_it = end_key == nullptr ? flush_vec.end()
                                          : std::lower_bound(range_start_it,
                                                             flush_vec.end(),
                                                             *end_key,
                                                             lower_bound_cmp);
    }
    // Moves the records in the gap succeeding the last splitting
    // range.
    size_t copy_size = std::distance(flush_vec_it, flush_vec.end());
    non_split_vec.reserve(non_split_vec.size() + copy_size);
    std::move(flush_vec_it, flush_vec.end(), std::back_inserter(non_split_vec));
}

void LocalCcShards::SplitFlushRange(
    const TableName &table_name,
    NodeGroupId node_group,
    bool is_forward,
    std::pair<const StoreRange *, std::vector<const TxKey *>> split_info,
    std::shared_ptr<DataSyncTask> data_sync_task)
{
    std::string log_output(
        "Splitting table " + table_name.String() + " range " +
        std::to_string(split_info.first->PartitionId()) + " into " +
        std::to_string(split_info.second.size() + 1) +
        " ranges. New range ids ");
    // Request for new range ids from data store. The new range ids returned by
    // data store are always unique.
    std::vector<std::pair<TxKey::Uptr, int32_t>> new_range_ids;
    for (auto &new_key : split_info.second)
    {
        int32_t new_part_id;
        if (!store_hd_->GetNextRangePartitionId(table_name, &new_part_id))
        {
            LOG(ERROR)
                << "Split range failed due to unable to get next partition id.";
            if (data_sync_task->SetError())
            {
                std::unique_lock<std::mutex> data_sync_task_lk(
                    task_worker_mux_);
                // Find item from table sync status for this table.
                TableDataSyncStatus &sync_status =
                    tables_sync_status_.at(table_name).at(node_group);
                // Indicating that the task worker has processed this task.
                sync_status.is_ongoing_ = false;

                // Handle the pending tasks for the same table
                if (sync_status.pending_task_.size() > 0)
                {
                    data_sync_task_queue_.push_back(
                        std::move(sync_status.pending_task_.back()));
                    sync_status.pending_task_.pop_back();
                }
            }
            return;
        }
        log_output.append(std::to_string(new_part_id) + ",");
        new_range_ids.emplace_back(std::move(new_key->Clone()), new_part_id);
    }
    // Issue read catalog tx_request to acquire read lock on catalog
    // cc_entry using base table name, and acquire read lock in one
    // shard is good enough to block schema change.
    const TableName base_table_name{table_name.GetBaseTableNameSV(),
                                    TableType::Primary};

    TransactionExecution *split_txm = tx_service_->NewTx();
    InitTxRequest init_req;
    // Set isolation level to RepeatableRead to ensure the readlock will
    // be set during the execution of the following ReadTxRequest.
    init_req.iso_level_ = IsolationLevel::RepeatableRead;
    init_req.protocol_ = CcProtocol::Locking;
    init_req.Reset();
    split_txm->Execute(&init_req);
    init_req.Wait();

    if (init_req.IsError())
    {
        if (data_sync_task->SetError())
        {
            std::unique_lock<std::mutex> data_sync_task_lk(task_worker_mux_);
            // Find item from table sync status for this table.
            TableDataSyncStatus &sync_status =
                tables_sync_status_.at(table_name).at(node_group);
            // Indicating that the task worker has processed this task.
            sync_status.is_ongoing_ = false;

            // Handle the pending tasks for the same table
            if (sync_status.pending_task_.size() > 0)
            {
                data_sync_task_queue_.push_back(
                    std::move(sync_status.pending_task_.back()));
                sync_status.pending_task_.pop_back();
            }
        }
        return;
    }

    // If table_name has been dropped at this point, read lock would not
    // be acquired.
    CatalogKey table_key(base_table_name);
    CatalogRecord catalog_rec;

    ReadTxRequest read_req;
    read_req.Reset();
    read_req.Set(
        &catalog_ccm_name, &table_key, &catalog_rec, false, false, true);
    split_txm->Execute(&read_req);
    read_req.Wait();

    if (read_req.IsError() || read_req.Result() != RecordStatus::Normal)
    {
        // Use AbortTxRequest to release read lock.
        AbortTxRequest abort_req;
        abort_req.Reset();
        split_txm->Execute(&abort_req);
        abort_req.Wait();
        assert(abort_req.Result() == false);
        LOG(ERROR) << "Split flush range add read lock on table failed, "
                      "table name: "
                   << table_key.Name().StringView();
        if (data_sync_task->SetError())
        {
            std::unique_lock<std::mutex> data_sync_task_lk(task_worker_mux_);
            // Find item from table sync status for this table.
            TableDataSyncStatus &sync_status =
                tables_sync_status_.at(table_name).at(node_group);
            // Indicating that the task worker has processed this task.
            sync_status.is_ongoing_ = false;

            // Handle the pending tasks for the same table
            if (sync_status.pending_task_.size() > 0)
            {
                data_sync_task_queue_.push_back(
                    std::move(sync_status.pending_task_.back()));
                sync_status.pending_task_.pop_back();
            }
        }
        return;
    }

    catalog_rec.Schema()->StatisticsObject()->PriorSplitRange(
        table_name, catalog_rec.Schema(), node_group);

    // Start the SplitFlush tx. This would split the range, flush the data and
    // update slice metadata.
    TableName range_table_name(table_name.StringView(),
                               TableType::RangePartition);
    const TableRangeEntry *entry = GetTableRangeEntry(
        range_table_name, node_group, split_info.first->RangeStartKey());
    assert(entry != nullptr);
    const TxKey *old_start_key = split_info.first->RangeStartKey();
    if (old_start_key == nullptr)
    {
        old_start_key = catalog_factory_->NegativeInfKey();
    }
    const TxKey *old_end_key = split_info.first->RangeEndKey();
    if (old_end_key == nullptr)
    {
        old_end_key = catalog_factory_->PositiveInfKey();
    }

    log_output.append(" txn: " + std::to_string(split_txm->TxNumber()));
    LOG(INFO) << log_output;
    const TableSchema *table_schema =
        is_forward ? catalog_rec.DirtySchema() : catalog_rec.Schema();

    SplitFlushTxRequest split_req(table_name,
                                  table_schema,
                                  node_group,
                                  old_start_key,
                                  old_end_key,
                                  entry->GetRangeInfo(),
                                  std::move(new_range_ids));
    split_txm->Execute(&split_req);
    split_req.Wait();
    if (split_req.IsError() || !split_req.Result())
    {
        LOG(ERROR) << "Split range on table " << table_name.StringView()
                   << " partition " << entry->GetRangeInfo()->PartitionId()
                   << " failed.";
        AbortTxRequest abort_req;
        abort_req.Reset();
        split_txm->Execute(&abort_req);
        abort_req.Wait();
        assert(abort_req.Result() == false);
        if (data_sync_task->SetError())
        {
            std::unique_lock<std::mutex> data_sync_task_lk(task_worker_mux_);
            // Find item from table sync status for this table.
            TableDataSyncStatus &sync_status =
                tables_sync_status_.at(table_name).at(node_group);
            // Indicating that the task worker has processed this task.
            sync_status.is_ongoing_ = false;

            // Handle the pending tasks for the same table
            if (sync_status.pending_task_.size() > 0)
            {
                data_sync_task_queue_.push_back(
                    std::move(sync_status.pending_task_.back()));
                sync_status.pending_task_.pop_back();
            }
        }
        return;
    }
    CommitTxRequest commit_req;

    commit_req.Reset();
    split_txm->Execute(&commit_req);
    commit_req.Wait();
    LOG(INFO) << "Split range on table " << table_name.StringView()
              << " partition " << split_info.first->PartitionId()
              << " succeeded.";
    if (data_sync_task->SetFinish())
    {
        std::unique_lock<std::mutex> data_sync_task_lk(task_worker_mux_);
        // Find item from table sync status for this table.
        TableDataSyncStatus &sync_status =
            tables_sync_status_.at(table_name).at(node_group);
        if (!data_sync_task->IsError())
        {
            // Update the task status for this table.
            sync_status.last_sync_ts_ = data_sync_task->data_sync_ts_;
        }
        // Indicating that the task worker has processed this task.
        sync_status.is_ongoing_ = false;

        // Handle the pending tasks for the same table
        if (sync_status.pending_task_.size() > 0)
        {
            data_sync_task_queue_.push_back(
                std::move(sync_status.pending_task_.back()));
            sync_status.pending_task_.pop_back();
        }
    }
}

void LocalCcShards::FlushData(std::unique_lock<std::mutex> &flush_worker_lk)
{
    // Retrieve first pending work and pop it.
    FlushDataWork &cur_work = pending_flush_work_.back();
    uint32_t node_group = cur_work.node_group_;
    int64_t leader_term = cur_work.ng_leader_term_;
    TableName table_name = cur_work.table_name_;
    const TableSchema *schema = cur_work.schema_;
    uint64_t data_sync_ts = cur_work.data_sync_ts_;
    std::unique_ptr<vector<FlushRecord>> data_sync_vec_owner, archive_vec_owner;
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

    // Flush to data store if this node group leader term does not
    // change
    if (!(data_sync_vec->empty() && archive_vec->empty() &&
          mv_base_vec->empty()) &&
        Sharder::Instance().LeaderTerm(node_group) == leader_term)
    {
        // Flushes to the data store
        bool flush_ret = true;
        std::unordered_set<uint32_t> skipped_record;

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
                *data_sync_vec, table_name, schema, node_group, skipped_record);
            if (!flush_ret)
            {
                LOG(ERROR) << "DataSync PutAll flush to kv "
                              "storage failed";
            }
        }

        if (flush_ret && EnableMvcc())
        {
            flush_ret = store_hd_->PutArchivesAll(node_group,
                                                  table_name,
                                                  schema->GetKVCatalogInfo(),
                                                  *archive_vec);

            if (!flush_ret)
            {
                // If ckpt succeeds and flushing undo fails, it is safe
                // to update the local checkpoint timestamp, but not
                // safe to truncate the redo log.
                succ = false;
                LOG(ERROR) << "DataSync PutArchivesAll flush to "
                              "kv storage failed";
            }
        }

        // If flush to data store succeeds, update the ckpt_ts for each
        // entry in ccmap to latest checkpoint version's commit_ts.
        if (flush_ret)
        {
            if (skipped_record.size())
            {
                // There are records that are skipped during put all. We cannot
                // truncate log, but we can update local checkpoint ts on
                // ccentries.
                succ = false;
            }
            for (size_t i = 0; i < data_sync_vec->size(); i++)
            {
                if (skipped_record.find(i) != skipped_record.end())
                {
                    continue;
                }
                auto &ref = data_sync_vec->at(i);
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
#ifdef RANGE_PARTITION_ENABLED
            // Update the slice size in data store.
            if (data_sync_vec->size())
            {
                while (!UpdateStoreSlice(table_name,
                                         schema->Version(),
                                         node_group,
                                         *data_sync_vec,
                                         true))
                {
                    // Keep retrying here since we've finished the flush
                    // already, it's too expensive to start from the beginning
                    // all over again.
                    LOG(ERROR) << "Data sync failed to update store slice info "
                                  "on table "
                               << table_name.Trace() << ".";
                    std::this_thread::sleep_for(1s);
                }
            }
#endif
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

        if (succ)
        {
            succ = schema->StatisticsObject()->PostCheckpoint(
                store_hd_, table_name, schema, node_group, data_sync_ts, false);
        }
    }

    // Update the work count if the work's sender is waiting.
    if (data_sync_task != nullptr)
    {
        // Commit the data sync txm
        assert(data_sync_txm != nullptr);
        CommitTxRequest commit_req;
        commit_req.Reset();
        data_sync_txm->Execute(&commit_req);
        commit_req.Wait();
        bool last_worker = false;
        if (succ)
        {
            last_worker = data_sync_task->SetFinish();
        }
        else
        {
            last_worker = data_sync_task->SetError();
        }
        if (last_worker)
        {
            std::unique_lock<std::mutex> data_sync_task_lk(task_worker_mux_);
            // Find item from table sync status for this table.
            TableDataSyncStatus &sync_status =
                tables_sync_status_.at(table_name).at(node_group);
            if (succ)
            {
                // Update the task status for this table.
                sync_status.last_sync_ts_ = data_sync_ts;
            }
            // Indicating that the task worker has processed this task.
            sync_status.is_ongoing_ = false;

            // Handle the pending tasks for the same table
            if (sync_status.pending_task_.size() > 0)
            {
                data_sync_task_queue_.push_back(
                    std::move(sync_status.pending_task_.back()));
                sync_status.pending_task_.pop_back();
            }
        }
    }

    if (hand_res)
    {
        // In this case flush data is triggered by a sub op of a tx
        // operation, and the sync_status controll is done by the parent tx
        // request.
        if (!succ)
        {
            hand_res->SetError(CcErrorCode::DATA_STORE_ERR);
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
    std::unique_lock<std::mutex> flush_worker_lk(flush_worker_mux_);
    while (flush_worker_thd_status_ == WorkerStatus::Active)
    {
        flush_worker_cv_.wait(flush_worker_lk,
                              [this]
                              {
                                  return !pending_flush_work_.empty() ||
                                         flush_worker_thd_status_ ==
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
    std::unique_lock<std::mutex> worker_lk(slice_update_mux_);
    while (slice_thd_status_ == WorkerStatus::Active)
    {
        slice_update_cv_.wait(worker_lk,
                              [this]
                              {
                                  return !pending_slice_work_.empty() ||
                                         slice_thd_status_ ==
                                             WorkerStatus::Terminated;
                              });

        if (pending_slice_work_.empty())
        {
            continue;
        }

        UpdateSliceSpecWork &cur_work = pending_slice_work_.back();

        uint64_t data_sync_ts = cur_work.data_sync_ts_;
        uint32_t node_group = cur_work.node_group_;
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
                                          node_group,
                                          data_sync_ts,
                                          flush_vec,
                                          start_idx,
                                          end_idx,
                                          false);
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
                curr_range->RangeEndKey() != nullptr &&
                    (*curr_range->RangeEndKey() < data_sync_key ||
                     *curr_range->RangeEndKey() == data_sync_key))
            {
                if (curr_range != nullptr && flush_res && range_updated)
                {
                    bool ret = curr_range->UpdateRangeSlicesInStore(
                        table_name, schema_ts, true, store_hd_);
                    success = ret && success;
                }

                // The current datasync key falls into a new range. Finds the
                // range.
                curr_range =
                    FindRange(table_name, node_group_id, data_sync_key);
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
            curr_slice->EndKey() != nullptr &&
                !(*data_sync_vec[idx + 1].Key() < *curr_slice->EndKey()))
        {
            if (flush_res)
            {
                range_updated |= curr_slice->UpdateSize();
            }
            else
            {
                curr_slice->SetPostCkptSize(UINT32_MAX);
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
}  // namespace txservice
