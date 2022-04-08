#pragma once

#include "catalog_factory.h"
#include "tx_key.h"
#include "tx_record.h"

namespace txservice
{
/**
 * @brief A special type of tx keys for concurrency control (cc) maps of table
 * catalogs.
 *
 */
struct CatalogKey : public TxKey
{
public:
    CatalogKey();
    CatalogKey(const TableName &name);
    ~CatalogKey() = default;

    bool operator==(const TxKey &rhs) const override;
    bool operator<(const TxKey &rhs) const override;
    size_t Hash() const override;
    void Serialize(std::vector<char> &buf, size_t &offset) const override;
    void Serialize(std::string &str) const override;
    void Deserialize(const char *buf, size_t &offset, const Schema *) override;
    TxKey::Uptr Clone() const override;
    std::string ToString() const override;

    KeyType Type() const override
    {
        return KeyType::Normal;
    }

    friend bool operator==(const CatalogKey &lhs, const CatalogKey &rhs);
    friend bool operator<(const CatalogKey &lhs, const CatalogKey &rhs);
    const TableName &Name() const;
    TableName &Name();

private:
    TableName table_name_;
};

struct TableSchemaView
{
    const TableSchema *schema_{nullptr};
    /**
     * @brief The version of the schema, represented by the commit timestamp
     * when the schema is last modified. Timestamp being 0 means that the schema
     * is unspecified.
     *
     */
    uint64_t version_ts_{0};
    const TableSchema *dirty_schema_{nullptr};
    uint64_t dirty_version_ts_{0};
};

/**
 * @brief A special type of records for cc maps of table catalogs. A catalog
 * record contains either (1) a serialized representation of the catalog, or (2)
 * a pointer to the catalog instance at this node (in LocalCcShards). The former
 * is used when instantiating a table catalog instance at this node and creating
 * the corresponding cc map(s). The latter provides fast access to a view of the
 * current and dirty schemas for online tx's. A view of the dirty schema is
 * necessary for certain schema operations, e.g., creating secondary indexes
 * while not blocking online tx's.
 *
 */
struct CatalogRecord : public TxRecord
{
public:
    CatalogRecord() = default;
    CatalogRecord(const std::string &schema_blob);
    CatalogRecord(const char *schema_ptr, size_t schema_len);
    ~CatalogRecord() = default;

    void Serialize(std::vector<char> &buf, size_t &offset) const override;
    void Serialize(std::string &str) const override;
    void Deserialize(const char *buf, size_t &offset) override;
    TxRecord::Uptr Clone() const override;
    void Copy(const TxRecord &rhs) override;
    std::string ToString() const override;

    const TableSchemaView *SchemaView() const;
    void SetSchemaView(const TableSchemaView *view);
    const std::string &SchemaBlob() const;
    std::string &SchemaBlob();

    CatalogRecord &operator=(const CatalogRecord &rhs);

private:
    const TableSchemaView *view_{nullptr};
    std::string schema_blob_{};
};
}  // namespace txservice