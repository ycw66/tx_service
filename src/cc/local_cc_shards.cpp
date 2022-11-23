#include "cc/local_cc_shards.h"

#include "store/data_store_handler.h"
#include "tx_execution.h"
#include "tx_service.h"

namespace txservice
{
std::atomic<uint64_t> LocalCcShards::local_clock(0);

LocalCcShards::LocalCcShards(uint32_t node_id,
                             uint16_t core_cnt,
                             uint32_t memory_limit_mb,
                             uint32_t log_limit_mb,
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
                                                          node_id,
                                                          *this,
                                                          catalog_factory_));
    }
}

LocalCcShards::LocalCcShards(uint32_t node_id,
                             uint16_t core_cnt,
                             uint32_t memory_limit_mb,
                             uint32_t log_limit_mb,
                             CatalogFactory *catalog_factory,
                             store::DataStoreHandler *store_hd,
                             TxService *tx_service,
                             bool enable_mvcc)
    : LocalCcShards(node_id,
                    core_cnt,
                    memory_limit_mb,
                    log_limit_mb,
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
    while (timestamp > tsb && !ts_base_.compare_exchange_strong(tsb, timestamp))
    {
        // If the CAS fails, since timestamps always roll forward, the ts base
        // must be greater than the current time or the old ts base.
        // ts_base_ can also be updated when calculating commit timestamp, which
        // results in system clock could be smaller than ts_base_. In this case
        // we should not update ts_base_.
        tsb = ts_base_.load(std::memory_order_acquire);
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
    std::unique_lock<std::shared_mutex> lk(catalog_mux_);

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
        // replaces the existing schemaw with the new one.
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
    std::unique_lock<std::shared_mutex> lk(catalog_mux_);

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
            1);
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

const CatalogEntry *LocalCcShards::CreateDirtyCatalog(
    const TableName &table_name,
    NodeGroupId cc_ng_id,
    const std::string &catalog_image,
    uint64_t commit_ts)
{
    std::unique_lock<std::shared_mutex> lk(catalog_mux_);

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
    std::unique_lock<std::shared_mutex> lk(catalog_mux_);

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

const CatalogEntry *LocalCcShards::GetCatalog(const TableName &table_name,
                                              NodeGroupId cc_ng_id)
{
    std::shared_lock<std::shared_mutex> lk(catalog_mux_);

    auto ng_catalog_it = table_catalogs_.find(table_name);
    if (ng_catalog_it == table_catalogs_.end())
    {
        return nullptr;
    }

    auto catalog_it = ng_catalog_it->second.find(cc_ng_id);
    return catalog_it == ng_catalog_it->second.end() ? nullptr
                                                     : &catalog_it->second;
}

std::unordered_set<TableName> LocalCcShards::GetCatalogTableNamesForCkpt(
    NodeGroupId cc_ng_id)
{
    std::unordered_set<TableName> table_set;
    std::shared_lock<std::shared_mutex> lk(catalog_mux_);
    for (const auto &[base_table_name, ng_catalog_map] : table_catalogs_)
    {
        auto catalog_it = ng_catalog_map.find(cc_ng_id);
        if (catalog_it != ng_catalog_map.end())
        {
            const CatalogEntry &catalog_entry = catalog_it->second;
            if (catalog_entry.schema_ != nullptr)
            {
                table_set.emplace(base_table_name.StringView().data(),
                                  base_table_name.StringView().size(),
                                  base_table_name.Type());
                for (txservice::TableName &index_table_name :
                     catalog_entry.schema_->IndexNames())
                {
                    table_set.emplace(index_table_name.StringView().data(),
                                      index_table_name.StringView().size(),
                                      index_table_name.Type());
                }
            }
        }
    }
    return table_set;
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
    uint64_t commit_ts)
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
                             commit_ts);
}

void LocalCcShards::InitTableRanges(const TableName &range_table_name,
                                    std::vector<InitRangeEntry> &init_ranges)
{
    std::unique_lock<std::shared_mutex> lk(catalog_mux_);

    assert(range_table_name.Type() == TableType::RangePartition);
    auto table_it = table_ranges_.try_emplace(range_table_name);
    assert(table_it.second);
    std::map<int32_t, TableRangeEntryWithShade> &ranges =
        table_it.first->second;

    assert(init_ranges.size() > 0);
    for (size_t pidx = 0; pidx < init_ranges.size() - 1; ++pidx)
    {
        InitRangeEntry &range_entry = init_ranges[pidx];
        InitRangeEntry &next_range_entry = init_ranges[pidx + 1];

        ranges.try_emplace(range_entry.partition_id_,
                           std::move(range_entry.key_),
                           range_entry.version_ts_,
                           range_entry.partition_id_,
                           next_range_entry.partition_id_);
    }

    InitRangeEntry &last_range_entry = init_ranges.back();
    ranges.try_emplace(last_range_entry.partition_id_,
                       std::move(last_range_entry.key_),
                       last_range_entry.version_ts_,
                       last_range_entry.partition_id_,
                       INT32_MAX);
}

std::map<int32_t, TableRangeEntryWithShade> *
LocalCcShards::GetAllTableRangesForATable(const TableName &range_table_name)
{
    std::shared_lock<std::shared_mutex> s_lk(catalog_mux_);

    auto table_it = table_ranges_.find(range_table_name);
    return table_it == table_ranges_.end() ? nullptr : &table_it->second;
}

const TableRangeEntryWithShade *LocalCcShards::CreateDirtyTableRange(
    const TableName &table_name,
    int32_t partition_id,
    std::unique_ptr<TxKey> new_key,
    int32_t new_partition_id,
    uint64_t commit_ts)
{
    std::unique_lock<std::shared_mutex> lk(catalog_mux_);

    auto table_it = table_ranges_.find(table_name);
    assert(table_it != table_ranges_.end());

    std::map<int32_t, TableRangeEntryWithShade> &ranges = table_it->second;
    auto range_it = ranges.find(partition_id);
    assert(range_it != ranges.end());
    TableRangeEntry *shader = range_it->second.shader_.get();
    assert(shader->dirty_ts_ < commit_ts);

    // clone the shader to shade, and set dirty of the shade
    range_it->second.shade_ = shader->Clone();
    TableRangeEntry *shade = range_it->second.shade_.get();
    shade->SetDirty(std::move(new_key), new_partition_id, commit_ts);

    return &range_it->second;
}

const std::pair<TableRangeEntry *, TableRangeEntry *>
LocalCcShards::CommitDirtyTableRange(const TableName &table_name,
                                     int32_t partition_id,
                                     uint64_t commit_ts)
{
    std::unique_lock<std::shared_mutex> lk(catalog_mux_);

    auto table_it = table_ranges_.find(table_name);
    assert(table_it != table_ranges_.end());

    std::map<int32_t, TableRangeEntryWithShade> &ranges = table_it->second;
    auto range_it = ranges.find(partition_id);
    assert(range_it != ranges.end());
    // get the old dirty range for splitting
    TableRangeEntry *dirty_range_entry = range_it->second.shade_.get();
    assert(dirty_range_entry->IsDirty());
    TableRangeEntry *old_range_entry = range_it->second.shader_.get();

    // create new range
    std::unique_ptr<TxKey> new_key_clone = dirty_range_entry->new_key_->Clone();
    auto new_range_entry_pair =
        ranges.try_emplace(dirty_range_entry->new_partition_id_,
                           std::move(new_key_clone),
                           commit_ts,
                           dirty_range_entry->new_partition_id_,
                           dirty_range_entry->next_partition_id_);

    // point old range next partition id to the new partition id
    old_range_entry->next_partition_id_ = dirty_range_entry->new_partition_id_;

    // return the new range entry, insert new partition must be succeed
    assert(new_range_entry_pair.second);
    std::map<int32_t, TableRangeEntryWithShade>::iterator
        new_range_entry_with_shade = new_range_entry_pair.first;
    TableRangeEntry *new_range_entry =
        new_range_entry_with_shade->second.shader_.get();
    return std::pair<TableRangeEntry *, TableRangeEntry *>(old_range_entry,
                                                           new_range_entry);
}

void LocalCcShards::PostCommitDirtyTableRange(const TableName &table_name,
                                              int32_t partition_id)
{
    std::unique_lock<std::shared_mutex> lk(catalog_mux_);

    auto table_it = table_ranges_.find(table_name);
    assert(table_it != table_ranges_.end());

    std::map<int32_t, TableRangeEntryWithShade> &ranges = table_it->second;
    auto range_it = ranges.find(partition_id);
    assert(range_it != ranges.end());
    range_it->second.shade_ = std::unique_ptr<TableRangeEntry>(nullptr);
}

void LocalCcShards::CleanTableRange(const TableName &table_name, uint32_t ng_id)
{
    std::unique_lock<std::shared_mutex> lk(catalog_mux_);
    table_ranges_.erase(table_name);
}

const TableRangeEntry *LocalCcShards::GetTableEffectiveRangeEntry(
    const TableName &table_name, int32_t partition_id)
{
    std::unique_lock<std::shared_mutex> lk(catalog_mux_);

    auto table_it = table_ranges_.find(table_name);
    assert(table_it != table_ranges_.end());

    std::map<int32_t, TableRangeEntryWithShade> &ranges = table_it->second;
    auto range_it = ranges.find(partition_id);
    assert(range_it != ranges.end());
    TableRangeEntry *shader = range_it->second.shader_.get();
    TableRangeEntry *shade = range_it->second.shade_.get();

    return shade != nullptr ? shade : shader;
}

const TableRangeEntryWithShade *LocalCcShards::GetTableRangeWithShade(
    const TableName &table_name, int32_t partition_id)
{
    std::unique_lock<std::shared_mutex> lk(catalog_mux_);

    auto table_it = table_ranges_.find(table_name);
    assert(table_it != table_ranges_.end());

    std::map<int32_t, TableRangeEntryWithShade> &ranges = table_it->second;
    auto range_it = ranges.find(partition_id);
    assert(range_it != ranges.end());
    return &range_it->second;
}

void LocalCcShards::SetTxIdent(uint32_t latest_committed_tx_no)
{
    for (const auto &cc_shard : cc_shards_)
    {
        LOG(INFO) << "cc shard on core: " << cc_shard->core_id_
                  << " set next_tx_ident_ to " << latest_committed_tx_no + 1;
        cc_shard->next_tx_ident_ = latest_committed_tx_no + 1;
    }
}

void LocalCcShards::DropCatalogs(NodeGroupId cc_ng_id)
{
    std::unique_lock<std::shared_mutex> lk(catalog_mux_);

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
    std::shared_lock<std::shared_mutex> shards_lk(catalog_mux_);

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
}  // namespace txservice