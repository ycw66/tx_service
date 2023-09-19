#pragma once

#include <assert.h>

#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "proto/cc_request.pb.h"
#include "schema.h"
#include "sharder.h"
#include "store/data_store_handler.h"
#include "tx_worker_pool.h"
#include "type.h"

namespace txservice
{
class CcShard;

class CcMapSamplePool
{
public:
    virtual ~CcMapSamplePool() = default;
};

class Distribution
{
public:
    virtual ~Distribution() = default;
};

class Statistics
{
public:
    static uint32_t ShardCode(std::string_view base_table_name)
    {
        uint32_t shard_code = Sharder::Instance().ShardCode(
            std::hash<std::string_view>{}(base_table_name));
        return shard_code;
    }

    static uint16_t CoreDoSample(const TableName &table_or_index_name)
    {
        uint32_t shard_code =
            ShardCode(table_or_index_name.GetBaseTableNameSV());
        uint16_t core_id =
            (shard_code & 0x3FF) % Sharder::Instance().GetLocalCcShardsCount();
        return core_id;
    }

    static NodeGroupId NodeGroupDoStore(const TableName &table_or_index_name)
    {
        uint32_t shard_code =
            ShardCode(table_or_index_name.GetBaseTableNameSV());
        NodeGroupId ng_id = Sharder::Instance().ShardToCcNodeGroup(shard_code);
        return ng_id;
    }

public:
    virtual ~Statistics() = default;

    virtual std::shared_ptr<Distribution> GetDistribution(
        const TableName &table_or_index_name) const = 0;

    virtual void DropIndex(const TableName &index_name) = 0;

    virtual void OnRemoteStatisticsMessage(
        TableName table_or_index_name,
        const TableSchema *table_schema,
        remote::NodeGroupSamplePool remote_sample_pool) = 0;

    virtual void PriorSplitRange(const TableName &table_or_index_name,
                                 const TableSchema *table_schema,
                                 NodeGroupId ng_id) const = 0;

    virtual bool SyncTableStatistics(store::DataStoreHandler *store_hd,
                                     const TableName &table_or_index_name,
                                     const TableSchema *table_schema,
                                     NodeGroupId ng_id,
                                     uint64_t version,
                                     bool updated) const = 0;
};

struct StatisticsEntry
{
    // The CcNode::on_leader_stop() method free memory after pinning_threads
    // down to zero.
    //
    // To use cross-threads pointer safely, one can:
    // (1) Protect that pointer with Sharder::TryPinNodeGroupData(). Once
    // pinned, row pointer can be used directly.
    // (2) Use a shared_pointer version of that pointer.
    //
    // Currently, sql threads choose method (1), and other threads choose
    // methods (2).
    std::shared_ptr<Statistics> statistics_{nullptr};
};

}  // namespace txservice
