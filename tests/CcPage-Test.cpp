#define CATCH_CONFIG_MAIN

#include <catch2/catch.hpp>
#include <chrono>
#include <random>

#include "cc_entry.h"
#include "cc_shard.h"
#include "local_cc_shards.h"
#include "template_cc_map.h"
#include "tx_key.h"     // CompositeKey
#include "tx_record.h"  // CompositeRecord

namespace txservice
{

void PrepareCcMap(
    TemplateCcMap<CompositeKey<std::string, int>, CompositeRecord<int>> &cc_map,
    size_t cnt,
    std::string &table_name,
    bool random = true)
{
    LOG(INFO) << "preparing ccmap of table: " << table_name;
    std::vector<CompositeKey<std::string, int>> t1_keys;
    for (size_t i = 0; i < cnt; i++)
    {
        t1_keys.emplace_back(std::make_tuple(table_name, i));
    }
    std::vector<CompositeKey<std::string, int> *> t1_key_ptrs;
    for (auto &key : t1_keys)
    {
        t1_key_ptrs.emplace_back(&key);
    }
    if (random)
    {
        auto rng = std::default_random_engine{};
        std::shuffle(t1_key_ptrs.begin(), t1_key_ptrs.end(), rng);
    }
    LOG(INFO) << "BulkEmplace into: " << table_name;
    auto start = std::chrono::high_resolution_clock::now();
    bool ok = cc_map.BulkEmplaceForTest(t1_key_ptrs);
    auto stop = std::chrono::high_resolution_clock::now();
    int64_t ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(stop - start)
            .count();
    LOG(INFO) << (random ? "randomly" : "sequentially") << " inserting " << cnt
              << " keys into ccmap takes: " << ms << " ms";
    REQUIRE(ok == true);
}

TEST_CASE("CcPage clean tests", "[cc-page]")
{
    LocalCcShards local_cc_shards(
        0, 1, 10000, 10000, nullptr, nullptr, nullptr, nullptr, true);
    CcShard shard(0, 1, 10000, 10000, 0, local_cc_shards, nullptr);
    Sharder::Instance(
        0, nullptr, nullptr, nullptr, nullptr, &local_cc_shards, nullptr);

    const size_t MAP_NUM = 20;
    const size_t MAP_SIZE = 10000;
    std::vector<std::string> tables;
    std::vector<TableName> table_names;
    std::vector<std::unique_ptr<
        TemplateCcMap<CompositeKey<std::string, int>, CompositeRecord<int>>>>
        ccmaps;
    std::vector<std::vector<CompositeKey<std::string, int>>> map_keys;
    std::vector<std::vector<CompositeKey<std::string, int> *>> map_key_ptrs;

    for (size_t i = 0; i < MAP_NUM; i++)
    {
        tables.emplace_back("t" + std::to_string(i));
        table_names.emplace_back(tables[i], TableType::Primary);
        ccmaps.emplace_back(
            std::make_unique<TemplateCcMap<CompositeKey<std::string, int>,
                                           CompositeRecord<int>>>(
                &shard, 0, table_names[i], 1, nullptr, true));
    }

    for (size_t i = 0; i < MAP_NUM; i++)
    {
        auto &cc_map = *ccmaps[i];
        PrepareCcMap(cc_map, MAP_SIZE, tables[i], false);
    }

    for (auto &up : ccmaps)
    {
        auto &cc_map = *up;
        size_t size = cc_map.VerifyOrdering();
        REQUIRE(size == MAP_SIZE);
    }

    shard.VerifyLruList();

    LOG(INFO) << "clean all freeable entries...";
    size_t total_free = 0;
    while (true)
    {
        size_t free_cnt = shard.Clean();
        shard.VerifyLruList();
        if (free_cnt == 0)
        {
            break;
        }
        total_free += free_cnt;
    }
    LOG(INFO) << "total freed: " << total_free;
    shard.VerifyLruList();

    size_t total_remain = 0;
    for (size_t i = 0; i < MAP_NUM; i++)
    {
        auto &cc_map = *ccmaps.at(i);
        size_t remain = cc_map.VerifyOrdering();
        LOG(INFO) << "after clean, ccmap of table " << tables[i]
                  << " remain: " << remain;
        total_remain += remain;
    }

    REQUIRE(total_remain + total_free == MAP_NUM * MAP_SIZE);
}

}  // namespace txservice
