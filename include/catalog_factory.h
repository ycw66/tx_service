#pragma once

#include <memory>

#include "cc/cc_map.h"
#include "schema.h"

namespace txservice
{
struct TableSchema
{
    using uptr = std::unique_ptr<TableSchema>;

    virtual ~TableSchema() = default;
    virtual const Schema *KeySchema() const = 0;
    virtual const Schema *RecordSchema() const = 0;
    virtual const std::string &SchemaImage() const = 0;
    virtual uint64_t Version() const = 0;
    virtual std::string_view VersionStringView() const = 0;
    virtual std::vector<TableName> IndexNames() const = 0;
    virtual const Schema *IndexKeySchema(const TableName &index_name) const = 0;
};

class CatalogFactory
{
public:
    CatalogFactory() = default;
    virtual ~CatalogFactory() = default;

    virtual TableSchema::uptr CreateTableSchema(
        const std::string &table_name,
        const std::string &catalog_image,
        uint64_t version,
        NodeGroupId cc_ng_id) = 0;

    virtual CcMap::uptr CreatePkCcMap(const TableName &table_name,
                                      const TableSchema *table_schema,
                                      uint64_t schema_ts,
                                      bool ccm_has_full_entries,
                                      CcShard *shard) = 0;

    virtual CcMap::uptr CreateSkCcMap(const TableName &table_name,
                                      const TableSchema *table_schema,
                                      uint64_t schema_ts,
                                      CcShard *shard) = 0;

    virtual CcMap::uptr CreatePkRangeMap(const TableName &range_pk_table_name,
                                         CcShard *shard) = 0;

    virtual std::unique_ptr<CcScanner> CreatePkCcmScanner(
        ScanDirection direction, const Schema *key_schema) = 0;

    virtual std::unique_ptr<CcScanner> CreateSkCcmScanner(
        ScanDirection direction, const Schema *compound_key_schema) = 0;
};
}  // namespace txservice