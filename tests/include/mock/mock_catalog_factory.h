#pragma once

#include <memory>  //unique_ptr
#include <unordered_map>
#include <utility>  //pair
#include <vector>

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
    MockTableSchema(const TableName &table_name,
                    const std::string &catalog_image,
                    uint64_t version)
        : table_name_(table_name.StringView().data(),
                      table_name.StringView().size(),
                      table_name.Type()),
          schema_image_(catalog_image),
          version_(version)
    {
    }
    ~MockTableSchema()
    {
    }

    const TableName &GetTableName() const override
    {
        return table_name_;
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
        return std::string_view(std::to_string(version_));
    }
    std::vector<TableName> IndexNames() const override
    {
        std::vector<TableName> index_names;
        index_names.reserve(indexes_.size());
        for (const auto &index_entry : indexes_)
        {
            index_names.emplace_back(
                index_entry.second.first.StringView().data(),
                index_entry.second.first.StringView().size(),
                TableType::Secondary);
        }

        return index_names;
    }
    const SecondaryKeySchema *IndexKeySchema(
        const TableName &index_name) const override
    {
        assert(false);
        return nullptr;
    }
    KVCatalogInfo *GetKVCatalogInfo() const override
    {
        assert(false);
        return nullptr;
    }
    void SetKVCatalogInfo(const std::string &kv_info_str) override
    {
        assert(false);
    }

private:
    std::unordered_map<uint, std::pair<TableName, MockKeySchema>>
        indexes_;           // string owner
    TableName table_name_;  // string owner
    std::string schema_image_;
    uint64_t version_;
    std::unique_ptr<MockKeySchema> key_schema_;
    MockRecordSchema record_schema_;
    KVCatalogInfo::uptr kv_info_;
};

class MockCatalogFactory : public CatalogFactory
{
public:
    MockCatalogFactory() = default;
    ~MockCatalogFactory()
    {
    }

    TableSchema::uptr CreateTableSchema(const TableName &table_name,
                                        const std::string &catalog_image,
                                        uint64_t version,
                                        NodeGroupId cc_ng_id) override
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
            false,
            table_schema,
            ccm_has_full_entries);
    }

    CcMap::uptr CreateSkCcMap(const txservice::TableName &index_name,
                              const txservice::TableSchema *table_schema,
                              uint64_t schema_ts,
                              txservice::CcShard *shard) override
    {
        assert(false);
        return nullptr;
    }

    CcMap::uptr CreatePkRangeMap(const txservice::TableName &base_table,
                                 const txservice::TableSchema *table_schema,
                                 uint64_t schema_ts,
                                 txservice::CcShard *shard) override
    {
        assert(false);
        return nullptr;
    }

    std::unique_ptr<txservice::CcScanner> CreatePkRangeCcmScanner(
        txservice::ScanDirection direction,
        const txservice::Schema *key_schema) override
    {
        assert(false);
        return nullptr;
    }

    std::unique_ptr<CcScanner> CreatePkCcmScanner(
        ScanDirection direction, const Schema *key_schema) override
    {
        assert(false);
        return nullptr;
    }

    std::unique_ptr<CcScanner> CreateSkCcmScanner(
        ScanDirection direction, const Schema *compound_key_schema) override
    {
        assert(false);
        return nullptr;
    }
};

}  // namespace txservice
