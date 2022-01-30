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
};

class CatalogFactory
{
public:
    CatalogFactory() = default;
    virtual ~CatalogFactory() = default;

    virtual TableSchema::uptr CreateTableSchema(
        const std::string &table_name,
        const std::string &catalog_image,
        uint64_t version) = 0;

    virtual CcMap::uptr CreatePkCcMap(const TableSchema *table_schema,
                                      CcShard *shard) = 0;

    virtual CcMap::uptr CreateSkCcMap(const TableName &table_name,
                                      const TableSchema *table_schema,
                                      CcShard *shard) = 0;
};
}  // namespace txservice