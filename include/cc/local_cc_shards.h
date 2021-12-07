#pragma once

#include <chrono>
#include <iostream>
#include <unordered_map>

#include "catalog.h"
#include "cc_shard.h"
#include "local_cc_handler.h"
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

class LocalCcShards
{
public:
    LocalCcShards(uint32_t node_id = 0,
                  uint16_t core_cnt = 1,
                  Catalog *catalog = nullptr);

    ~LocalCcShards()
    {
        timer_terminate_.store(true, std::memory_order_release);
        timer_thd_.join();
        cc_shards_.clear();
    }

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

    // store table catalog information in ccshard.
    // core_id specify which ccshard to store the table catalog.
    // if is_all is true (e.g. during startup of tx service), then all the
    // ccshard store the table catalog.
    void FillTableCatalog(const TableName &tabname,
                          std::string &catalog_content,
                          std::string &table_version,
                          uint32_t core_id,
                          bool is_all)
    {
        for (uint32_t id = 0; id < cc_shards_.size(); id++)
        {
            if (is_all || id == core_id)
            {
                auto table_iter = cc_shards_[id]->table_metadata_.find(tabname);

                if (table_iter == cc_shards_[id]->table_metadata_.end())
                {
                    auto iter =
                        cc_shards_[id]->table_metadata_.try_emplace(tabname);
                    table_iter = iter.first;
                }

                TableCatalog &tab_catalog = table_iter->second;
                tab_catalog.table_catalog_info_ = catalog_content;
                tab_catalog.table_catalog_version_ = table_version;
            }
        }
    }

    // table is dropped, we erase the catalog information in cchard as well
    void RemoveTableCatalog(const TableName &tabname, uint32_t core_id)
    {
        cc_shards_[core_id]->table_metadata_.erase(tabname);
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

    friend class LocalCcHandler;
    friend class remote::RemoteCcHandler;
    friend class Checkpointer;
};
}  // namespace txservice
