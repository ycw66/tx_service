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
                             TxService *tx_service,
                             bool enable_mvcc)
    : store_hd_(store_hd),
      node_id_(node_id),
      timer_terminate_(false),
      catalog_factory_(catalog_factory),
      tx_service_(tx_service),
      enable_mvcc_(enable_mvcc)
{
    using namespace std::chrono_literals;
    uint64_t ts_base = std::chrono::duration_cast<std::chrono::microseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count();
    timer_thd_ = std::thread([this] { TimerRun(); });

    for (uint16_t thd_idx = 0; thd_idx < core_cnt; ++thd_idx)
    {
        cc_shards_.emplace_back(std::make_unique<CcShard>(thd_idx,
                                                          core_cnt,
                                                          memory_limit_mb,
                                                          log_limit_mb,
                                                          ts_base,
                                                          node_id,
                                                          *this,
                                                          catalog_factory_));
    }
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

uint64_t LocalCcShards::ShardClockTs(uint16_t core_id)
{
    assert(core_id < cc_shards_.size());
    return cc_shards_[core_id]->Now();
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

        for (std::unique_ptr<CcShard> &ccs : cc_shards_)
        {
            uint64_t tsb = ccs->ts_base_.load(std::memory_order_acquire);
            // If the CAS fails, since timestamps always roll forward, the
            // ts base must be greater than the current time or the old ts
            // base.
            ccs->ts_base_.compare_exchange_strong(tsb, std::max(tsb, clock_ts));
        }

        std::this_thread::sleep_for(2s);
    }
}

const CatalogEntry *LocalCcShards::CreateCatalog(
    const std::string &table_name,
    NodeGroupId cc_ng_id,
    const std::string &catalog_image,
    uint64_t commit_ts)
{
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
    }

    return &catalog_entry;
}

const CatalogEntry *LocalCcShards::CreateReplayCatalog(
    const std::string &table_name,
    NodeGroupId cc_ng_id,
    const std::string &old_catalog_image,
    const std::string &new_catalog_image,
    uint64_t commit_ts)
{
    std::unique_lock<std::shared_mutex> lk(catalog_mux_);

    auto ng_catalog_it = table_catalogs_.try_emplace(table_name);
    auto catalog_it = ng_catalog_it.first->second.try_emplace(cc_ng_id);
    CatalogEntry &catalog_entry = catalog_it.first->second;

    if (catalog_it.second || catalog_entry.Version() == 0)
    {
        // If catalog entry is not initialized yet, use the old schema image
        // stored in prepare log to restore old schema.
        // Using 1 as commit ts here as a place holder. Commit ts here should
        // not matter since the old schema should be removed once replay is
        // done.
        catalog_entry.InitSchema(
            old_catalog_image.empty()
                ? nullptr
                : catalog_factory_->CreateTableSchema(
                      table_name, old_catalog_image, 1, cc_ng_id),
            1);
    }
    if (catalog_entry.Version() < commit_ts &&
        catalog_entry.DirtyVersion() < commit_ts)
    {
        // For idempotency, only installs the dirty version when the input ts is
        // greater than the existing version and dirty version.
        catalog_entry.SetDirtySchema(
            new_catalog_image.empty()
                ? nullptr
                : catalog_factory_->CreateTableSchema(
                      table_name, new_catalog_image, commit_ts, cc_ng_id),
            commit_ts);
    }
    return &catalog_entry;
}

const CatalogEntry *LocalCcShards::CreateDirtyCatalog(
    const std::string &table_name,
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

void LocalCcShards::CommitDirtyCatalog(const std::string &table_name,
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

const CatalogEntry *LocalCcShards::GetCatalog(const std::string &table_name,
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

std::unordered_set<TableName> LocalCcShards::CatalogTableNames(
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
                table_set.emplace(base_table_name);
                for (txservice::TableName &index_table_name :
                     catalog_entry.schema_->IndexNames())
                {
                    table_set.emplace(index_table_name);
                }
            }
        }
    }
    return table_set;
}

void LocalCcShards::CreateSchemaRecoveryTx(
    const ::txlog::SchemaOpMessage &schema_op_msg,
    const CatalogRecord *catalog_record,
    uint64_t txn,
    int64_t tx_term,
    uint64_t commit_ts)
{
    TransactionExecution *txm = tx_service_->NewTx();
    txm->RecoverSchemaTx(
        schema_op_msg, catalog_record, txn, tx_term, commit_ts);
}

void LocalCcShards::InitTableRanges(const TableName &range_table_name,
                                    std::vector<InitRangeEntry> &init_ranges)
{
    std::unique_lock<std::shared_mutex> lk(catalog_mux_);

    auto table_it = table_ranges_.try_emplace(range_table_name);
    assert(table_it.second);
    std::map<int32_t, TableRangeEntryWithShade> &ranges =
        table_it.first->second;

    if (init_ranges.empty())
    {
        ranges.try_emplace(0, nullptr, 1, 0, INT32_MAX);
    }
    else
    {
        ranges.try_emplace(0, nullptr, 1, 0, init_ranges[0].partition_id_);

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
    shader->SetDirty(std::move(new_key), new_partition_id, commit_ts);

    range_it->second.shade_ = shader->Clone();

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
    TableRangeEntry *dirty_range_entry = range_it->second.shader_.get();
    assert(dirty_range_entry->IsDirty());

    // create new range
    auto new_range_entry_pair =
        ranges.try_emplace(dirty_range_entry->new_partition_id_,
                           std::move(dirty_range_entry->new_key_),
                           commit_ts,
                           dirty_range_entry->new_partition_id_,
                           dirty_range_entry->next_partition_id_);

    // point old range next partition id to the new partition id
    dirty_range_entry->next_partition_id_ =
        dirty_range_entry->new_partition_id_;
    // clear dirty range
    dirty_range_entry->ClearDirty();

    // return the new range entry, insert new partition must be succeed
    assert(new_range_entry_pair.second);
    std::map<int32_t, TableRangeEntryWithShade>::iterator
        new_range_entry_with_shade = new_range_entry_pair.first;
    TableRangeEntry *new_range_entry =
        new_range_entry_with_shade->second.shader_.get();
    return std::pair<TableRangeEntry *, TableRangeEntry *>(dirty_range_entry,
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

const TableRangeEntry *LocalCcShards::GetTableEffectiveRange(
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

void LocalCcShards::UpdateTsBase(uint64_t timestamp)
{
    for (std::unique_ptr<CcShard> &ccs : cc_shards_)
    {
        uint64_t tsb = ccs->ts_base_.load(std::memory_order_acquire);
        while (timestamp > tsb &&
               !ccs->ts_base_.compare_exchange_strong(tsb, timestamp))
        {
            tsb = ccs->ts_base_.load(std::memory_order_acquire);
        }
        if (timestamp > tsb)
        {
            LOG(INFO) << "cc shard on core: " << ccs->core_id_
                      << " update ts_base_ from: " << tsb
                      << " to: " << timestamp;
        }
    }
}

void LocalCcShards::DropCatalogs(NodeGroupId cc_ng_id)
{
    for (auto node_catalog_it = table_catalogs_.begin();
         node_catalog_it != table_catalogs_.end();
         ++node_catalog_it)
    {
        node_catalog_it->second.erase(cc_ng_id);
    }
}

void LocalCcShards::WakeUpTxProcessor(uint16_t core_id)
{
    tx_service_->WakeUpTxProcessor(core_id);
}
}  // namespace txservice
