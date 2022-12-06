#pragma once

#include <memory>
#include <string>

#include "proto/statistics.pb.h"
#include "type.h"

namespace txservice
{
// Here Shard is corresponding to one ccmap.
//
// Under current architecture, it is expensive to do full table scan. And it is
// impossible to do a global uniform sampling. An alternative way is maintaining
// statistics on insert/delete.
class ShardProfile
{
public:
    virtual ~ShardProfile() = default;
    virtual void ToSerializableObj(
        store::IndexStatistics *store_index_statistics) const = 0;
};

// This class is correspond to proto::Statistics.
class Statistics
{
public:
    virtual ~Statistics() = default;

    virtual std::shared_ptr<ShardProfile> GetShardProfile(
        const TableName &table_name) const = 0;

    virtual void Reset(const std::string &statistics_binary,
                       bool discard_sample_pool) = 0;

    virtual void ToSerializableObj(
        store::Statistics *store_statistics) const = 0;

    static inline std::string EMPTY_STATISTICS_BINARY{""};
};
}  // namespace txservice
