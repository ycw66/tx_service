#pragma once

#include <stdint.h>

#include <memory>
#include <string>

#include "type.h"

namespace txservice
{
class TxKey;
struct TxRecord;

struct Schema
{
public:
    using Uptr = std::unique_ptr<Schema>;

    virtual ~Schema() = default;

    virtual std::string GetColumnString(const TxKey &key, size_t col_idx) const
    {
        std::string s;
        return s;
    }

    virtual std::string GetColumnString(const TxRecord &rec,
                                        size_t col_idx) const
    {
        std::string s;
        return s;
    }

    virtual Schema::Uptr Clone() const = 0;
};

struct KeySchema : public Schema
{
    virtual bool CompareKeys(const TxKey &key1,
                             const TxKey &key2,
                             size_t *const column_index) const = 0;

    virtual uint16_t ExtendKeyParts() const = 0;
    virtual uint64_t SchemaTs() const = 0;
};

struct SecondaryKeySchema : public KeySchema
{
public:
    SecondaryKeySchema() = delete;

    SecondaryKeySchema(const Schema *sk_sch, const Schema *pk_sch)
        : SecondaryKeySchema(sk_sch->Clone(), pk_sch->Clone())
    {
    }

    SecondaryKeySchema(std::unique_ptr<const Schema> sk_sch,
                       std::unique_ptr<const Schema> pk_sch)
        : sk_schema_(static_cast<const KeySchema *>(sk_sch.release())),
          pk_schema_(static_cast<const KeySchema *>(pk_sch.release()))
    {
    }

    SecondaryKeySchema(const SecondaryKeySchema &sch)
        : SecondaryKeySchema(sch.sk_schema_->Clone(), sch.pk_schema_->Clone())
    {
    }

    Schema::Uptr Clone() const override
    {
        return std::make_unique<SecondaryKeySchema>(*this);
    }

    bool CompareKeys(const TxKey &key1,
                     const TxKey &key2,
                     size_t *const column_index) const override
    {
        return sk_schema_->CompareKeys(key1, key2, column_index);
    }

    // Number of key parts in the index (including "index extension")
    uint16_t ExtendKeyParts() const override
    {
        return sk_schema_->ExtendKeyParts();
    }

    uint64_t SchemaTs() const override
    {
        return sk_schema_->SchemaTs();
    }

    std::unique_ptr<const KeySchema> sk_schema_;
    std::unique_ptr<const KeySchema> pk_schema_;
};

struct TableKeySchemaTs
{
    TableKeySchemaTs() = default;
    TableKeySchemaTs(const std::string &key_schemas_ts_str)
    {
        std::stringstream ts_ss(key_schemas_ts_str);
        std::istream_iterator<std::string> ts_b(ts_ss);
        std::istream_iterator<std::string> ts_e;
        std::vector<std::string> schemas_ts(ts_b, ts_e);
        pk_schema_ts_ = std::stoull(schemas_ts.at(0));
        for (size_t idx = 1; idx < schemas_ts.size(); ++idx)
        {
            txservice::TableType table_type;
            if (schemas_ts[idx].find(txservice::UNIQUE_INDEX_NAME_PREFIX) !=
                std::string::npos)
            {
                table_type = txservice::TableType::UniqueSecondary;
            }
            else if (schemas_ts[idx].find(txservice::INDEX_NAME_PREFIX) !=
                     std::string::npos)
            {
                table_type = txservice::TableType::Secondary;
            }
            else
            {
                assert(false && "Unknown secondary key type.");
            }
            txservice::TableName table_name(schemas_ts[idx], table_type);
            ++idx;
            sk_schemas_ts_.try_emplace(std::move(table_name),
                                       std::stoull(schemas_ts[idx]));
        }
    }

    std::string Serialize() const
    {
        std::string output_str;
        size_t len_sizeof = sizeof(size_t);

        std::string table_ts(std::to_string(pk_schema_ts_));
        size_t len_val = table_ts.size();
        char *len_ptr = reinterpret_cast<char *>(&len_val);
        output_str.append(len_ptr, len_sizeof);
        output_str.append(table_ts.data(), len_val);

        std::string index_tables_ts;
        if (sk_schemas_ts_.size() != 0)
        {
            for (auto it = sk_schemas_ts_.cbegin(); it != sk_schemas_ts_.cend();
                 ++it)
            {
                index_tables_ts.append(it->first.StringView())
                    .append(" ")
                    .append(std::to_string(it->second))
                    .append(" ");
            }
            index_tables_ts.erase(index_tables_ts.size() - 1);
        }
        else
        {
            index_tables_ts.clear();
        }

        len_val = index_tables_ts.size();
        output_str.append(len_ptr, len_sizeof);
        output_str.append(index_tables_ts.data(), len_val);

        return output_str;
    }

    void Deserialize(const char *buf, size_t &offset)
    {
        if (buf == nullptr || buf[0] == '\0')
        {
            return;
        }
        size_t len_sizeof = sizeof(size_t);
        size_t *len_ptr = (size_t *) (buf + offset);
        size_t len_val = *len_ptr;
        offset += len_sizeof;

        pk_schema_ts_ = std::stoull(std::string(buf + offset, len_val));
        offset += len_val;

        len_ptr = (size_t *) (buf + offset);
        len_val = *len_ptr;
        offset += len_sizeof;
        if (len_val != 0)
        {
            std::string index_tables_ts(buf + offset, len_val);
            std::stringstream sk_ss(index_tables_ts);
            std::istream_iterator<std::string> sk_b(sk_ss);
            std::istream_iterator<std::string> sk_e;
            std::vector<std::string> sk_iter(sk_b, sk_e);
            for (auto it = sk_iter.begin(); it != sk_iter.end(); ++it)
            {
                txservice::TableType table_type;
                if (it->find(txservice::UNIQUE_INDEX_NAME_PREFIX) !=
                    std::string::npos)
                {
                    table_type = txservice::TableType::UniqueSecondary;
                }
                else if (it->find(txservice::INDEX_NAME_PREFIX) !=
                         std::string::npos)
                {
                    table_type = txservice::TableType::Secondary;
                }
                else
                {
                    assert(false && "Unknown secondary key type.");
                }
                txservice::TableName table_name(*it, table_type);
                sk_schemas_ts_.try_emplace(std::move(table_name),
                                           std::stoull(*(++it)));
            }
        }
        else
        {
            sk_schemas_ts_.clear();
        }
        offset += len_val;
    }

    /**
     * @brief Get the key schema ts of the specified [primary/secondary] key.
     *
     * @param table_name key schema name.
     *
     * @return If the key is newly added, return 1. Otherwise, return normal ts.
     */
    uint64_t GetKeySchemaTs(const txservice::TableName &table_name) const
    {
        if (table_name.Type() == txservice::TableType::Primary)
        {
            return pk_schema_ts_;
        }
        else
        {
            auto v_it = sk_schemas_ts_.find(table_name);
            return v_it == sk_schemas_ts_.end() ? 1 : v_it->second;
        }
    }

    uint64_t pk_schema_ts_{1};
    std::unordered_map<txservice::TableName, uint64_t> sk_schemas_ts_;
};

}  // namespace txservice
