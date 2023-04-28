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
      enable_mvcc_(enable_mvcc)
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
    const TxKey *range_key,
    std::unique_ptr<RangeRecord> splitting_range_record,
    uint32_t partition_id,
    std::unique_ptr<TxKey> new_range_key,
    uint32_t new_partition_id,
    uint32_t node_group_id,
    uint64_t txn,
    int64_t tx_term,
    uint64_t commit_ts,
    std::optional<std::pair<CcEntryAddr, ReadSetEntry>> catalog_cc_entry)
{
    TransactionExecution *txm = tx_service_->NewTx();
    txm->RecoverSplitRangeTx(ds_split_range_op_msg,
                             table_schema,
                             range_key,
                             std::move(splitting_range_record),
                             partition_id,
                             std::move(new_range_key),
                             new_partition_id,
                             txn,
                             tx_term,
                             commit_ts,
                             std::move(catalog_cc_entry));
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
                                    const NodeGroupId ng_id)
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

    assert(new_range_entry_pair.second);
    range_ids->try_emplace(partition_id, &new_range_entry_pair.first->second);
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
                              uint64_t ckpt_ts,
                              int64_t term,
                              uint64_t node_group,
                              std::vector<FlushRecord> *ckpt_vec,
                              std::vector<FlushRecord> *archive_vec,
                              std::vector<const TxKey *> *mv_vec,
                              CcHandlerResult<Void> &hres)
{
    cc_shards_.at(0)->FlushData(table_name,
                                schema,
                                ckpt_ts,
                                term,
                                node_group,
                                ckpt_vec,
                                archive_vec,
                                mv_vec,
                                &hres);
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
    const TableName &table_name,
    NodeGroupId ng_id,
    const TableSchema *table_schema)
{
    std::unique_lock<std::shared_mutex> lk(meta_data_mux_);

    auto ng_statistics_it = table_statistics_map_.try_emplace(table_name);
    auto statistics_it = ng_statistics_it.first->second.try_emplace(ng_id);
    if (statistics_it.second)
    {
        StatisticsEntry &statistics_entry = statistics_it.first->second;

        statistics_entry.statistics_ =
            catalog_factory_->CreateTableStatistics(table_schema);
    }

    return {statistics_it.first->second.statistics_.get(),
            statistics_it.second};
}

std::pair<Statistics *, bool> LocalCcShards::InitTableStatistics(
    const TableName &table_name,
    NodeGroupId ng_id,
    const TableSchema *table_schema,
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
            catalog_factory_->CreateTableStatistics(table_schema,
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

}  // namespace txservice
