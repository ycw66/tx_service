#pragma once

#include "catalog_factory.h"     // TableSchema,CatalogFactory
#include "cc/template_cc_map.h"  // CcMap,TemplateCcMap
#include "schema.h"              // Schema
#include "tx_key.h"              // CompositeKey
#include "tx_record.h"           // CompositeRecord

namespace txservice
{

struct MockKeySchema : public txservice::Schema
{
public:
    using Uptr = std::unique_ptr<MockKeySchema>;

    MockKeySchema()
    {
    }

    Schema::Uptr Clone() const override
    {
        return std::make_unique<MockKeySchema>();
    }
};

class MockRecordSchema : public txservice::Schema
{
public:
    using Uptr = std::unique_ptr<MockRecordSchema>;

    MockRecordSchema()
    {
    }

    Schema::Uptr Clone() const override
    {
        return std::make_unique<MockRecordSchema>();
    }
};

struct MockTableSchema : public TableSchema
{
public:
    MockTableSchema(const std::string &table_name,
                    const std::string &catalog_image,
                    uint64_t version)
        : schema_image_(catalog_image), version_(version)
    {
    }
    ~MockTableSchema()
    {
    }

    const Schema *KeySchema() const override
    {
        return key_schema_.get();
    }
    const Schema *RecordSchema() const override
    {
        return &record_schema_;
    }
    const std::string &SchemaImage() const override
    {
        return schema_image_;
    }
    uint64_t Version() const override
    {
        return version_;
    }
    std::string_view VersionStringView() const override
    {
        std::string_view(std::to_string(version_));
    }
    std::vector<TableName> IndexNames() const override
    {
        std::vector<txservice::TableName> index_names;
        index_names.reserve(indexes_.size());
        for (const auto &index_entry : indexes_)
        {
            index_names.emplace_back(index_entry.second.first);
        }

        return index_names;
    }

private:
    std::unordered_map<uint, std::pair<std::string, MockKeySchema>> indexes_;
    std::string schema_image_;
    uint64_t version_;
    std::unique_ptr<MockKeySchema> key_schema_;
    MockRecordSchema record_schema_;
};

class MockCatalogFactory : public CatalogFactory
{
public:
    MockCatalogFactory() = default;
    ~MockCatalogFactory()
    {
    }

    TableSchema::uptr CreateTableSchema(const std::string &table_name,
                                        const std::string &catalog_image,
                                        uint64_t version) override
    {
        return std::make_unique<MockTableSchema>(
            table_name, catalog_image, version);
    }

    CcMap::uptr CreatePkCcMap(const TableName &table_name,
                              const TableSchema *table_schema,
                              uint64_t schema_ts,
                              bool ccm_has_full_entries,
                              CcShard *shard) override
    {
        return std::make_unique<
            txservice::TemplateCcMap<CompositeKey<int>, CompositeRecord<int>>>(
            shard,
            table_name,
            schema_ts,
            table_schema->KeySchema(),
            table_schema->RecordSchema(),
            ccm_has_full_entries);
    }

    CcMap::uptr CreateSkCcMap(const txservice::TableName &index_name,
                              const txservice::TableSchema *table_schema,
                              uint64_t schema_ts,
                              txservice::CcShard *shard) override
    {
        return nullptr;
    }

    CcMap::uptr CreatePkRangeMap(const TableName &base_table,
                                 CcShard *shard) override
    {
        return nullptr;
    }
};

}  // namespace txservice
