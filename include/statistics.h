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
    virtual uint64_t Records() const = 0;
    virtual uint64_t Records(const KeySchema *key_schema,
                             const TxKey &min_key,
                             const TxKey &max_key) const = 0;
    virtual std::vector<double> RecordsPerKey() const = 0;
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

    static uint16_t LeaderCore(const TableName &table_or_index_name)
    {
        uint32_t shard_code =
            ShardCode(table_or_index_name.GetBaseTableNameSV());
        uint16_t core_id =
            (shard_code & 0x3FF) % Sharder::Instance().GetLocalCcShardsCount();
        return core_id;
    }

    static NodeGroupId LeaderNodeGroup(const TableName &table_or_index_name)
    {
        uint32_t shard_code =
            ShardCode(table_or_index_name.GetBaseTableNameSV());
        return Sharder::Instance().ShardToCcNodeGroup(shard_code);
    }

public:
    virtual ~Statistics() = default;

    virtual const TableName &BaseTableName() const = 0;

    virtual const Distribution *GetDistribution(
        const TableName &table_or_index_name) const = 0;

    virtual void CreateIndex(const TableName &index_name,
                             const KeySchema *key_schema,
                             NodeGroupId cc_ng_id) = 0;

    virtual void DropIndex(const TableName &index_name) = 0;

    virtual void OnRemoteStatisticsMessage(
        const TableName &table_or_index_name,
        const TableSchema *table_schema,
        const remote::NodeGroupSamplePool &remote_sample_pool) = 0;

    virtual std::unique_ptr<remote::NodeGroupSamplePool>
    MakeBroadcastSamplePool(NodeGroupId ng_id,
                            const TableName &table_or_index_name,
                            bool *updated_since_sync) const = 0;

    virtual std::unordered_map<TableName,
                               std::pair<uint64_t, std::vector<TxKey>>>
    MakeStoreStatistics(bool *updated_since_sync) const = 0;

    virtual void SetUpdatedSinceSync() = 0;

    virtual void SetEstimateRecordSize(size_t size) = 0;

    virtual size_t EstimateRecordSize() const = 0;
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
