#include "cc/local_cc_shards.h"

#include "tx_execution.h"
#include "tx_service.h"

namespace txservice
{
std::atomic<uint64_t> LocalCcShards::local_clock(0);

LocalCcShards::LocalCcShards(uint32_t node_id,
                             uint16_t core_cnt,
                             CatalogFactory *catalog_factory,
                             store::DataStoreWriteHandler *store_hd,
                             TxService *tx_service)
    : store_hd_(store_hd),
      node_id_(node_id),
      timer_terminate_(false),
      catalog_factory_(catalog_factory),
      tx_service_(tx_service)
{
    using namespace std::chrono_literals;
    uint64_t ts_base = std::chrono::duration_cast<std::chrono::microseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count();
    timer_thd_ = std::thread([this] { TimerRun(); });

    for (uint16_t thd_idx = 0; thd_idx < core_cnt; ++thd_idx)
    {
        cc_shards_.emplace_back(std::make_unique<CcShard>(
            thd_idx, core_cnt, ts_base, node_id, *this, catalog_factory_));
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
    return LocalCcShards::local_clock.load(std::memory_order_acquire);
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

const TableSchemaView *LocalCcShards::CreateCatalog(
    const std::string &table_name,
    const std::string &catalog_image,
    uint64_t commit_ts)
{
    std::unique_lock<std::shared_mutex> lk(catalog_mux_);
    auto catalog_it = table_catalogs_.try_emplace(table_name);
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
        const TableSchemaView *schema_view = catalog_entry.SchemaView();
        // If the input schema version is greater than the existing one,
        // replaces the existing schemaw with the new one.
        if (schema_view->version_ts_ < commit_ts)
        {
            catalog_entry.InitSchema(
                catalog_image.empty()
                    ? nullptr
                    : catalog_factory_->CreateTableSchema(
                          table_name, catalog_image, commit_ts),
                commit_ts);
        }
    }

    return catalog_entry.SchemaView();
}

const TableSchemaView *LocalCcShards::CreateDirtyCatalog(
    const std::string &table_name,
    const std::string &catalog_image,
    uint64_t commit_ts)
{
    std::unique_lock<std::shared_mutex> lk(catalog_mux_);
    auto catalog_it = table_catalogs_.find(table_name);
    if (catalog_it == table_catalogs_.end())
    {
        auto em_it = table_catalogs_.try_emplace(table_name);
        catalog_it = em_it.first;
    }

    CatalogEntry &catalog_entry = catalog_it->second;
    if (catalog_entry.schema_view_.version_ts_ < commit_ts &&
        catalog_entry.schema_view_.dirty_version_ts_ < commit_ts)
    {
        // For idempotency, only installs the dirty version when the input ts is
        // greater than the existing version and dirty version.
        catalog_entry.SetDirtySchema(
            catalog_image.empty() ? nullptr
                                  : catalog_factory_->CreateTableSchema(
                                        table_name, catalog_image, commit_ts),
            commit_ts);
    }

    return catalog_entry.SchemaView();
}

const TableSchemaView *LocalCcShards::CommitDirtyCatalog(
    const std::string &table_name)
{
    std::unique_lock<std::shared_mutex> lk(catalog_mux_);
    auto catalog_it = table_catalogs_.find(table_name);

    if (catalog_it == table_catalogs_.end())
    {
        return nullptr;
    }

    CatalogEntry &catalog_entry = catalog_it->second;
    catalog_entry.CommitDirtySchema();

    return catalog_entry.SchemaView();
}

const TableSchemaView *LocalCcShards::GetCatalog(const std::string &table_name)
{
    std::shared_lock<std::shared_mutex> lk(catalog_mux_);
    auto catalog_it = table_catalogs_.find(table_name);
    return catalog_it == table_catalogs_.end()
               ? nullptr
               : catalog_it->second.SchemaView();
}

std::unordered_set<TableName> LocalCcShards::CatalogTableNames()
{
    std::unordered_set<TableName> table_set;
    std::shared_lock<std::shared_mutex> lk(catalog_mux_);
    for (auto catalog_it = table_catalogs_.begin();
         catalog_it != table_catalogs_.end();
         ++catalog_it)
    {
        table_set.emplace(catalog_it->first);
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

void LocalCcShards::InitTableRanges(const TableName &range_table_name,
                                    std::vector<InitRangeEntry> &init_ranges)
{
    std::unique_lock<std::shared_mutex> lk(catalog_mux_);

    auto table_it = table_ranges_.try_emplace(range_table_name);
    // assert(!table_it.second);
    std::map<uint32_t, TableRangeEntry> &ranges = table_it.first->second;

    if (init_ranges.empty())
    {
        ranges.try_emplace(0, nullptr, 1, 0, UINT32_MAX);
    }
    else
    {
        ranges.try_emplace(0, nullptr, 1, 0, init_ranges[0].partition_id_);

        for (size_t pidx = 0; pidx < ranges.size() - 1; ++pidx)
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
                           UINT32_MAX);
    }
}

const std::map<uint32_t, TableRangeEntry> *LocalCcShards::GetTableRanges(
    const TableName &range_table_name)
{
    std::shared_lock<std::shared_mutex> s_lk(catalog_mux_);

    auto table_it = table_ranges_.find(range_table_name);
    return table_it == table_ranges_.end() ? nullptr : &table_it->second;
}

const TableRangeEntry *LocalCcShards::CreateDirtyRange(
    const TableName &table_name,
    uint32_t partition_id,
    std::unique_ptr<TxKey> new_key,
    uint32_t new_partition_id,
    uint64_t commit_ts)
{
    std::unique_lock<std::shared_mutex> lk(catalog_mux_);

    auto table_it = table_ranges_.find(table_name);
    assert(table_it != table_ranges_.end());

    std::map<uint32_t, TableRangeEntry> &ranges = table_it->second;
    auto range_it = ranges.find(partition_id);
    assert(range_it != ranges.end());
    TableRangeEntry &range_entry = range_it->second;

    if (range_entry.dirty_ts_ < commit_ts)
    {
        range_entry.new_key_ = std::move(new_key);
        range_entry.new_partition_id_ = new_partition_id;
        range_entry.dirty_ts_ = commit_ts;
    }

    return &range_entry;
}

bool LocalCcShards::SetTxIdent(uint32_t latest_committed_tx_no)
{
    for (const auto &cc_shard : cc_shards_)
    {
        LOG(INFO) << "cc shard on core: " << cc_shard->core_id_
                  << " set next_tx_ident_ to " << latest_committed_tx_no + 1;
        cc_shard->next_tx_ident_ = latest_committed_tx_no + 1;
    }
}
}  // namespace txservice
