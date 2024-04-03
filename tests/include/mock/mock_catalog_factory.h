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

struct MockKeySchema : public txservice::KeySchema
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

    bool CompareKeys(const TxKey &key1,
                     const TxKey &key2,
                     size_t *const column_index) const override
    {
        return false;
    }

    uint16_t ExtendKeyParts() const override
    {
        return UINT16_MAX;
    }

    uint64_t SchemaTs() const override
    {
        return 1;
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

    const TableName &GetBaseTableName() const override
    {
        return table_name_;
    }

    const txservice::KeySchema *KeySchema() const override
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

    void BindStatistics(
        std::shared_ptr<txservice::Statistics> statistics) override
    {
    }

    std::shared_ptr<Statistics> StatisticsObject() const override
    {
        return nullptr;
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
    size_t IndexesSize() const override
    {
        return indexes_.size();
    }
    bool HasAutoIncrement() const override
    {
        return false;
    }
    const TableName *GetSequenceTableName() const override
    {
        assert(false);
        return nullptr;
    }
    std::pair<TxKey::Uptr, TxRecord::Uptr> GetSequenceKeyAndInitRecord(
        const TableName &table_name) const override
    {
        assert(false);
        return std::pair(nullptr, nullptr);
    }
    void AddDirtyIndex(const TableName &index_name) override
    {
        assert(false);
    }
    const std::unordered_set<TableName> *DirtyIndexNames() const override
    {
        assert(false);
        return nullptr;
    }
    void CleanDirtyInfo() override
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
                                        uint64_t version) override
    {
        return std::make_unique<MockTableSchema>(
            table_name, catalog_image, version);
    }

    CcMap::uptr CreatePkCcMap(const TableName &table_name,
                              const TableSchema *table_schema,
                              bool ccm_has_full_entries,
                              CcShard *shard,
                              txservice::NodeGroupId cc_ng_id) override
    {
        uint64_t schema_ts = table_schema->KeySchema()->SchemaTs();
        return std::make_unique<
            txservice::TemplateCcMap<CompositeKey<int>, CompositeRecord<int>>>(
            shard,
            cc_ng_id,
            table_name,
            schema_ts,
            table_schema,
            ccm_has_full_entries);
    }

    CcMap::uptr CreateSkCcMap(const txservice::TableName &index_name,
                              const txservice::TableSchema *table_schema,
                              txservice::CcShard *shard,
                              txservice::NodeGroupId cc_ng_id) override
    {
        assert(false);
        return nullptr;
    }

    CcMap::uptr CreateRangeMap(const txservice::TableName &range_table_name,
                               const txservice::TableSchema *table_schema,
                               uint64_t schema_ts,
                               txservice::CcShard *shard,
                               txservice::NodeGroupId cc_ng_id) override
    {
        assert(false);
        return nullptr;
    }

    std::unique_ptr<txservice::CcScanner> CreateRangeCcmScanner(
        txservice::ScanDirection direction,
        const txservice::Schema *key_schema,
        const TableName &range_table_name) override
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

    std::unique_ptr<Statistics> CreateTableStatistics(
        const TableSchema *table_schema, NodeGroupId cc_ng_id) override
    {
        assert(false);
        return nullptr;
    }

    std::unique_ptr<Statistics> CreateTableStatistics(
        const TableSchema *table_schema,
        std::unordered_map<TableName,
                           std::pair<uint64_t, std::vector<TxKey::Uptr>>>
            sample_pool_map,
        CcShard *ccs,
        NodeGroupId cc_ng_id) override
    {
        assert(false);
        return nullptr;
    }

    const TxKey *NegativeInfKey() override
    {
        assert(false);
        return nullptr;
    }

    const TxKey *PositiveInfKey() override
    {
        assert(false);
        return nullptr;
    }
};

class MockSystemHandler : public txservice::SystemHandler
{
public:
    static MockSystemHandler &Instance()
    {
        static MockSystemHandler instance_;
        return instance_;
    }

private:
    MockSystemHandler() = default;
    virtual ~MockSystemHandler() = default;
};

}  // namespace txservice
