#include "cc/local_cc_shards.h"

namespace txservice
{
std::atomic<uint64_t> LocalCcShards::local_clock(0);

LocalCcShards::LocalCcShards(uint32_t node_id,
                             uint16_t core_cnt,
                             CatalogFactory *catalog_factory,
                             store::DataStoreWriteHandler *store_hd)
    : store_hd_(store_hd),
      node_id_(node_id),
      timer_terminate_(false),
      catalog_factory_(catalog_factory)
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
}  // namespace txservice