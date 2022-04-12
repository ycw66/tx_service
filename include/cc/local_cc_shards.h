#pragma once

#include <chrono>
#include <iostream>
#include <shared_mutex>
#include <unordered_map>

#include "catalog.h"
#include "catalog_factory.h"
#include "catalog_key_record.h"
#include "cc_shard.h"
#include "local_cc_handler.h"
#include "raft_log.pb.h"
#include "sk_cc_map.h"
#include "table_lock.h"
#include "template_cc_map.h"

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
    LocalCcShards(uint32_t node_id = 0,
                  uint16_t core_cnt = 1,
                  CatalogFactory *catalog_factory = nullptr,
                  store::DataStoreWriteHandler *store_hd = nullptr,
                  TxService *tx_service = nullptr);

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

    bool IsIdle(uint32_t thd_id) const
    {
        return cc_shards_[thd_id]->IsIdle();
    }

    void SleepNotify(uint32_t thd_id)
    {
        cc_shards_[thd_id]->SleepNotify();
    }

    void WorkNotify(uint32_t thd_id)
    {
        cc_shards_[thd_id]->WorkNotify();
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

    void PrintCcMap()
    {
        std::unordered_map<TableName, size_t> mapsizes;
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
                    mapsizes.emplace(tab_name, map_iter->second->size());
                }

                // Excludes negative and positive infinity.
                entry_cnt += map_iter->second->size();

                std::cout << "Table '" << tab_name << "' core ID "
                          << shard.core_id_ << ": " << map_iter->second->size()
                          << std::endl;

                size_t order_cnt = map_iter->second->VerifyOrdering();
                assert(order_cnt == map_iter->second->size());
            }

            size_t list_cnt = 0;
            LruEntry *eptr = shard.head_cce_.lru_next_;
            while (eptr != &shard.tail_cce_)
            {
                ++list_cnt;
                eptr = eptr->lru_next_;
            }
            assert(entry_cnt == list_cnt);
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

    std::condition_variable &ShardCv(uint32_t core_id)
    {
        return cc_shards_.at(core_id)->shard_cv_;
    }

    std::mutex &ShardMutex(uint32_t core_id)
    {
        return cc_shards_.at(core_id)->shard_mux_;
    }

    static uint64_t ClockTs();

    const TableSchemaView *CreateCatalog(const std::string &table_name,
                                         const std::string &catalog_image,
                                         uint64_t commit_ts);

    const TableSchemaView *CreateDirtyCatalog(const std::string &table_name,
                                              const std::string &catalog_image,
                                              uint64_t commit_ts);

    const TableSchemaView *CommitDirtyCatalog(const std::string &table_name);

    const TableSchemaView *GetCatalog(const std::string &table_name);

    void CreateSchemaRecoveryTx(const ::txlog::SchemaOpMessage &schema_op_msg,
                                uint64_t txn,
                                int64_t tx_term,
                                uint64_t commit_ts);
    void InitTableRanges(const TableName &range_table_name,
                         std::vector<InitRangeEntry> &init_ranges);

    const std::map<uint32_t, TableRangeEntry> *GetTableRanges(
        const TableName &range_table_name);

    const TableRangeEntry *CreateDirtyRange(const TableName &table_name,
                                            uint32_t partition_id,
                                            std::unique_ptr<TxKey> new_key,
                                            uint32_t new_partition_id,
                                            uint64_t commit_ts);

    store::DataStoreWriteHandler *const store_hd_;

private:
    void TimerRun();

    const uint32_t node_id_;
    std::vector<std::unique_ptr<CcShard>> cc_shards_;

    // The background thread that periodically advances the timers of the local
    // shards to the current wall clock.
    std::thread timer_thd_;
    std::atomic<bool> timer_terminate_;
    // The static variable storing the local time. It is delayed time and
    // refreshed in roughly every 2 seconds by the background thread, so as to
    // reduce the cost of calling system functions to get the wall clock. The
    // local time is used by transaction state machines to determine if a lock
    // has been held too long and if so, invoke lock recovery.
    static std::atomic<uint64_t> local_clock;

    struct CatalogEntry
    {
        CatalogEntry() = default;

        void InitSchema(std::unique_ptr<TableSchema> schema,
                        uint64_t version_ts)
        {
            assert(version_ts > 0);

            if (schema_view_.version_ts_ < version_ts)
            {
                schema_ = std::move(schema);
                schema_view_.schema_ =
                    schema_ != nullptr ? schema_.get() : nullptr;
                schema_view_.version_ts_ = version_ts;
            }

            if (schema_view_.dirty_version_ts_ <= version_ts)
            {
                dirty_schema_ = nullptr;
                schema_view_.dirty_schema_ = nullptr;
                schema_view_.dirty_version_ts_ = 0;
            }
        }

        void SetDirtySchema(std::unique_ptr<TableSchema> dirty_schema,
                            uint64_t dirty_version_ts)
        {
            if (dirty_version_ts > schema_view_.dirty_version_ts_ &&
                dirty_version_ts > schema_view_.version_ts_)
            {
                dirty_schema_ = std::move(dirty_schema);
                schema_view_.dirty_schema_ =
                    dirty_schema_ != nullptr ? dirty_schema_.get() : nullptr;
                schema_view_.dirty_version_ts_ = dirty_version_ts;
            }
        }

        void CommitDirtySchema()
        {
            if (schema_view_.dirty_version_ts_ > schema_view_.version_ts_)
            {
                schema_ = std::move(dirty_schema_);
                schema_view_.schema_ =
                    schema_ != nullptr ? schema_.get() : nullptr;
                schema_view_.version_ts_ = schema_view_.dirty_version_ts_;
                schema_view_.dirty_schema_ = nullptr;
                schema_view_.dirty_version_ts_ = 0;
            }
            else
            {
                dirty_schema_ = nullptr;
                schema_view_.dirty_schema_ = nullptr;
                schema_view_.dirty_version_ts_ = 0;
            }
        }

        const TableSchemaView *SchemaView() const
        {
            return &schema_view_;
        }

        std::unique_ptr<TableSchema> schema_{nullptr};
        std::unique_ptr<TableSchema> dirty_schema_{nullptr};
        TableSchemaView schema_view_;
    };

    CatalogFactory *const catalog_factory_;
    std::unordered_map<TableName, CatalogEntry> table_catalogs_;
    std::unordered_map<TableName, std::map<uint32_t, TableRangeEntry>>
        table_ranges_;
    std::shared_mutex catalog_mux_;

    TxService *tx_service_;

    friend class LocalCcHandler;
    friend class remote::RemoteCcHandler;
    friend class Checkpointer;
};
}  // namespace txservice
