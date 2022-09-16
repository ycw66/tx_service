#pragma once

#include <condition_variable>
#include <shared_mutex>
#include <string>
#include <vector>

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
    CatalogKey(const CatalogKey &rhs, const Schema *);
    ~CatalogKey() = default;

    bool operator==(const TxKey &rhs) const override;
    bool operator<(const TxKey &rhs) const override;
    size_t Hash() const override;
    void Serialize(std::vector<char> &buf, size_t &offset) const override;
    void Serialize(std::string &str) const override;
    void Deserialize(const char *buf, size_t &offset, const Schema *) override;
    TxKey::Uptr Clone() const override;
    std::string ToString() const override;
    void Copy(const TxKey &rhs) override;

    KeyType Type() const override
    {
        return KeyType::Normal;
    }

    friend bool operator==(const CatalogKey &lhs, const CatalogKey &rhs);
    friend bool operator<(const CatalogKey &lhs, const CatalogKey &rhs);
    const TableName &Name() const;
    TableName &Name();

private:
    // table_name_ is string owner if the CatalogKey is stored in CcMap. When
    // the CatalogKey is constructed to perform a lookup(most of the time), it
    // should use string_view.
    TableName table_name_;
};

/**
 * @brief Owner of current and dirty schema on one node. Mutilple ccshards will
 * have their own copies of CatalogRecord in catolog_cc_map. Their current and
 * dirty schema point to the corresponding CatalogEntry.
 *
 * Note that it's possible that different shard's have different current schema
 * or dirty schema, since CreateDirty() and CommitDirty() steps happen in
 * parallel. But there are at most two schema on a single node, which is
 * described by CatalogEntry.
 *
 */
struct CatalogEntry
{
    CatalogEntry() = default;

    ~CatalogEntry()
    {
        {
            std::unique_lock<std::shared_mutex> lk(s_mux_);
            committing_ = false;
        }
        cv_.notify_all();

        std::unique_lock<std::shared_mutex> lk(s_mux_);
        cv_.wait(lk, [this] { return waiting_thd_cnt_ == 0; });
    }

    void InitSchema(std::unique_ptr<TableSchema> schema, uint64_t version_ts)
    {
        assert(version_ts > 0);

        if (Version() < version_ts)
        {
            schema_ = std::move(schema);
            schema_version_ = version_ts;
        }
        if (DirtyVersion() <= version_ts)
        {
            dirty_schema_ = nullptr;
            dirty_schema_version_ = 0;
        }
    }

    void SetDirtySchema(std::unique_ptr<TableSchema> dirty_schema,
                        uint64_t dirty_version_ts)
    {
        if (dirty_version_ts > dirty_schema_version_ &&
            dirty_version_ts > schema_version_)
        {
            dirty_schema_ = std::move(dirty_schema);
            dirty_schema_version_ = dirty_version_ts;
        }
    }

    void CommitDirtySchema()
    {
        if (dirty_schema_version_ > schema_version_)
        {
            schema_ = std::move(dirty_schema_);
            schema_version_ = dirty_schema_version_;
        }
        else
        {
            dirty_schema_ = nullptr;
        }
        dirty_schema_version_ = 0;
    }

    /**
     * @brief The version of the schema, represented by the commit timestamp
     * when the schema is last modified. Timestamp being 0 means that the schema
     * is unspecified.
     *
     */
    uint64_t Version() const
    {
        return schema_version_;
    }

    uint64_t DirtyVersion() const
    {
        return dirty_schema_version_;
    }

    std::shared_ptr<TableSchema> schema_{nullptr};
    std::shared_ptr<TableSchema> dirty_schema_{nullptr};
    uint64_t schema_version_{0};
    uint64_t dirty_schema_version_{0};

    std::shared_mutex s_mux_;
    std::condition_variable_any cv_;
    bool committing_{false};
    uint32_t waiting_thd_cnt_{0};
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
    ~CatalogRecord() = default;

    void Serialize(std::vector<char> &buf, size_t &offset) const override;
    void Serialize(std::string &str) const override;
    void Deserialize(const char *buf, size_t &offset) override;
    TxRecord::Uptr Clone() const override;
    void Copy(const TxRecord &rhs) override;
    std::string ToString() const override;

    void Set(TableSchema *schema,
             TableSchema *dirty_schema,
             uint64_t schema_ts);
    const std::string &SchemaImage() const;
    void SetSchemaImage(std::string &&schema_image);
    void SetSchemaImage(const std::string &schema_image);
    const std::string &DirtySchemaImage() const;
    void SetDirtySchemaImage(std::string &&schema_image);
    void SetDirtySchemaImage(const std::string &schema_image);
    const TableSchema *Schema() const;
    uint64_t SchemaTs() const;
    const TableSchema *DirtySchema() const;

    CatalogRecord &operator=(const CatalogRecord &rhs);

private:
    /**
     * @brief The CatalogRecord serves three purposes:
     * (1) a tx looks up a table's schema. schema_ points to the current schema
     * and dirty_schema_ points to the dirty schema of the table. Allowing
     * ongoing tx's to see the dirty schema is crucial to make schema operations
     * non-blocking. (2) Initialization of a table's catalog at a node reads the
     * table's schema from the data store and instantiates the schema instance
     * in memory. The schema_image_ in this case contains serialized images of
     * the schema. (3) A tx modifies a table's schema and uses the catalog
     * record to install a dirty version of the schema in the tx service. The
     * schema_image_ is the binary image of the current schema and
     * dirty_schema_image_ is the binary image of the new schema.
     *
     */
    const TableSchema *schema_{nullptr};
    const TableSchema *dirty_schema_{nullptr};
    uint64_t schema_ts_{0};
    std::string schema_image_{""};
    std::string dirty_schema_image_{""};
};
}  // namespace txservice
