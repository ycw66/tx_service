#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "cc/cc_map.h"
#include "schema.h"
#include "tx_command.h"

namespace txservice
{
struct TableRangeEntry;
class StoreRange;

struct KVCatalogInfo
{
    using uptr = std::unique_ptr<KVCatalogInfo>;

    KVCatalogInfo() = default;
    virtual ~KVCatalogInfo() = default;
    virtual std::string Serialize() const = 0;
    virtual void Deserialize(const char *buf, size_t &offset) = 0;

    virtual const std::string &GetKvTableName(const TableName &table_name) const
    {
        const TableType table_type = table_name.Type();
        assert(table_type == TableType::Primary ||
               table_type == TableType::Secondary ||
               table_type == TableType::UniqueSecondary);
        if (table_type == TableType::Primary)
        {
            return kv_table_name_;
        }
        else
        {
            return kv_index_names_.at(table_name);
        }
    }

    std::string kv_table_name_;
    // map of <mysql_index_table_name, kv_index_table_name>
    std::unordered_map<TableName, std::string> kv_index_names_;
};

class Statistics;

struct WriteEntry
{
    WriteEntry() = delete;
    WriteEntry(TxKey key, TxRecord::Uptr rec, uint64_t commit_ts)
        : key_(std::move(key)), rec_(std::move(rec)), commit_ts_(commit_ts)
    {
    }

    WriteEntry(const WriteEntry &rhs) = delete;
    WriteEntry(WriteEntry &&rhs)
    {
        key_ = std::move(rhs.key_);
        rec_ = std::move(rhs.rec_);
        commit_ts_ = rhs.commit_ts_;
    }

    WriteEntry &operator=(WriteEntry &&rhs)
    {
        if (this == &rhs)
        {
            return *this;
        }

        key_ = std::move(rhs.key_);
        rec_ = std::move(rhs.rec_);
        commit_ts_ = rhs.commit_ts_;

        return *this;
    }

    TxKey key_;
    TxRecord::Uptr rec_;
    uint64_t commit_ts_;
};

struct SkEncoder
{
    using uptr = std::unique_ptr<SkEncoder>;

    virtual ~SkEncoder() = default;
    /**
     * @brief Generate packed secondary key using TxKey and TxRecord.
     *
     * @param sk_idx Secondary key index of this table excluding the primary
     * key.
     */
    virtual bool AppendPackedSk(const TxKey *pk,
                                const TxRecord *record,
                                uint64_t version_ts,
                                std::vector<WriteEntry> &dest_vec) const = 0;
    virtual void Reset()
    {
    }
};

struct TableSchema
{
    using uptr = std::unique_ptr<TableSchema>;

    virtual ~TableSchema() = default;
    virtual const TableName &GetBaseTableName() const = 0;
    virtual const txservice::KeySchema *KeySchema() const = 0;
    virtual const Schema *RecordSchema() const = 0;
    virtual const std::string &SchemaImage() const = 0;
    virtual KVCatalogInfo *GetKVCatalogInfo() const = 0;
    virtual void SetKVCatalogInfo(const std::string &kv_info_str) = 0;
    virtual uint64_t Version() const = 0;
    virtual std::string_view VersionStringView() const = 0;
    virtual std::vector<TableName> IndexNames() const = 0;
    virtual size_t IndexesSize() const = 0;
    virtual const SecondaryKeySchema *IndexKeySchema(
        const TableName &index_name) const = 0;
    virtual void BindStatistics(std::shared_ptr<Statistics> statistics) = 0;
    virtual std::shared_ptr<Statistics> StatisticsObject() const = 0;

    /**
     * Create TxCommand from serialized command image. Used when processing
     * remote command request and replaying command log. This function is
     * necessary as the command type and object type is unknown in tx_service
     * layer.
     * @param cmd_image
     * @return
     */
    virtual std::unique_ptr<TxCommand> CreateTxCommand(
        std::string_view cmd_image) const
    {
        return nullptr;
    }

    virtual SkEncoder::uptr CreateSkEncoder(const TableName &index_name) const
    {
        return nullptr;
    }

    virtual bool HasAutoIncrement() const = 0;
    virtual const TableName *GetSequenceTableName() const = 0;
    virtual std::pair<TxKey, TxRecord::Uptr> GetSequenceKeyAndInitRecord(
        const TableName &table_name) const = 0;
    virtual void AddDirtyIndex(const TableName &index_name) = 0;
    virtual const std::unordered_set<TableName> *DirtyIndexNames() const = 0;
    virtual void CleanDirtyInfo() = 0;
};

class CatalogFactory
{
public:
    CatalogFactory() = default;
    virtual ~CatalogFactory() = default;

    virtual TableSchema::uptr CreateTableSchema(
        const TableName &table_name,
        const std::string &catalog_image,
        uint64_t version) = 0;

    virtual CcMap::uptr CreatePkCcMap(const TableName &table_name,
                                      const TableSchema *table_schema,
                                      bool ccm_has_full_entries,
                                      CcShard *shard,
                                      NodeGroupId cc_ng_id) = 0;

    virtual CcMap::uptr CreateSkCcMap(const TableName &table_name,
                                      const TableSchema *table_schema,
                                      CcShard *shard,
                                      NodeGroupId cc_ng_id) = 0;

    virtual CcMap::uptr CreateRangeMap(const TableName &range_table_name,
                                       const TableSchema *table_schema,
                                       uint64_t schema_ts,
                                       CcShard *shard,
                                       NodeGroupId ng_id) = 0;

    virtual std::unique_ptr<TableRangeEntry> CreateTableRange(
        TxKey start_key,
        uint64_t version_ts,
        int64_t partition_id,
        std::unique_ptr<StoreRange> slices = nullptr) = 0;

    virtual std::unique_ptr<CcScanner> CreatePkCcmScanner(
        ScanDirection direction, const Schema *key_schema) = 0;

    virtual std::unique_ptr<CcScanner> CreateSkCcmScanner(
        ScanDirection direction, const Schema *compound_key_schema) = 0;

    virtual std::unique_ptr<CcScanner> CreateRangeCcmScanner(
        ScanDirection direction,
        const Schema *key_schema,
        const TableName &range_table_name) = 0;

    virtual std::unique_ptr<Statistics> CreateTableStatistics(
        const TableSchema *table_schema, NodeGroupId cc_ng_id) = 0;

    /**
     * @param sample_pool_map is declared as value-type. Copy won't happen if it
     * is a right-value since C++11.
     */
    virtual std::unique_ptr<Statistics> CreateTableStatistics(
        const TableSchema *table_schema,
        std::unordered_map<TableName, std::pair<uint64_t, std::vector<TxKey>>>
            sample_pool_map,
        CcShard *ccs,
        NodeGroupId cc_ng_id) = 0;

    virtual TxKey NegativeInfKey() = 0;
    virtual TxKey PositiveInfKey() = 0;
    virtual size_t KeyHash(const char *buf,
                           size_t offset,
                           const txservice::Schema *key_schema)
    {
        return 0;
    }
};
}  // namespace txservice
