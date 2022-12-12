#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "cc/cc_map.h"
#include "schema.h"
#include "statistics.h"

namespace txservice
{
struct KVCatalogInfo
{
    using uptr = std::unique_ptr<KVCatalogInfo>;

    KVCatalogInfo() = default;
    virtual ~KVCatalogInfo() = default;
    virtual std::string Serialize() const = 0;
    virtual void Deserialize(const char *buf, size_t &offset) = 0;

    std::string kv_table_name_;
    // map of <mysql_index_table_name, kv_index_table_name>
    std::unordered_map<txservice::TableName, std::string> kv_index_names_;
};

struct TableSchema
{
    using uptr = std::unique_ptr<TableSchema>;

    virtual ~TableSchema() = default;
    virtual const TableName &GetBaseTableName() const = 0;
    virtual const Schema *KeySchema() const = 0;
    virtual const Schema *RecordSchema() const = 0;
    virtual const std::string &SchemaImage() const = 0;
    virtual KVCatalogInfo *GetKVCatalogInfo() const = 0;
    virtual void SetKVCatalogInfo(const std::string &kv_info_str) = 0;
    virtual Statistics *StatisticsObject() const = 0;
    virtual const std::string &StatisticsBinary() const = 0;
    virtual uint64_t Version() const = 0;
    virtual std::string_view VersionStringView() const = 0;
    virtual std::vector<TableName> IndexNames() const = 0;
    virtual size_t IndexesSize() const = 0;
    virtual const SecondaryKeySchema *IndexKeySchema(
        const TableName &index_name) const = 0;
};

class CatalogFactory
{
public:
    CatalogFactory() = default;
    virtual ~CatalogFactory() = default;

    virtual TableSchema::uptr CreateTableSchema(
        const TableName &table_name,
        const std::string &catalog_image,
        const std::string &statistics_binary,
        uint64_t version,
        NodeGroupId cc_ng_id) = 0;

    virtual CcMap::uptr CreatePkCcMap(const TableName &table_name,
                                      const TableSchema *table_schema,
                                      uint64_t schema_ts,
                                      bool ccm_has_full_entries,
                                      CcShard *shard,
                                      NodeGroupId cc_ng_id) = 0;

    virtual CcMap::uptr CreateSkCcMap(const TableName &table_name,
                                      const TableSchema *table_schema,
                                      uint64_t schema_ts,
                                      CcShard *shard,
                                      NodeGroupId cc_ng_id) = 0;

    virtual CcMap::uptr CreateRangeMap(const TableName &range_table_name,
                                       const TableSchema *table_schema,
                                       uint64_t schema_ts,
                                       CcShard *shard,
                                       NodeGroupId ng_id) = 0;

    virtual std::unique_ptr<CcScanner> CreatePkCcmScanner(
        ScanDirection direction, const Schema *key_schema) = 0;

    virtual std::unique_ptr<CcScanner> CreateSkCcmScanner(
        ScanDirection direction, const Schema *compound_key_schema) = 0;

    virtual std::unique_ptr<CcScanner> CreateRangeCcmScanner(
        ScanDirection direction,
        const Schema *key_schema,
        const TableName &range_table_name) = 0;
};
}  // namespace txservice
