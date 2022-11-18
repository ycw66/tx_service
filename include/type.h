#pragma once

#include <cassert>
#include <iostream>
#include <iterator>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>  //move
#include <vector>

#include "constants.h"

namespace txservice
{
#define KB(x) ((size_t) (x) << 10);
#define MB(x) ((size_t) (x) << 20);
#define GB(x) ((size_t) (x) << 30);

struct Void
{
};

constexpr Void void_ = Void();

#define void_return return void_;

// @brief OperationType contain SQL DML and SQL DDL.
enum class OperationType
{
    Update = 1,
    Delete,
    Insert,
    Upsert,
    CreateTable,
    DropTable,
    AddIndex,
    DropIndex
};

enum class TxnStatus
{
    Ongoing = 0,
    Committed,
    Aborted,
    /**
     * @brief A tx starts committing after receiving the commit command from
     * query runtime. From this point forward, the tx runs toward the end,
     * either committed or aborted, and cannot be interrupted, e.g., the user
     * closes the connection to the runtime. This status marks the period
     * between when the tx starts committing and when the tx's fate is finalized
     * (committed or aborted).
     *
     */
    Committing,
    /**
     * @brief A tx has tried to commit, but log service is unreachable and the
     * tx's commit result is unknown. The tx should not do postprocess and
     * release the locks it holds.
     */
    Unknown,
    /**
     * @brief A tx has committed or aborted and finished post-processing. This
     * state signals that the tx state machine can be recycled. Note that this
     * state shall not be uploaded to the tx entry in the tx service, which
     * notifies other (local or remote) participants in the service the fate of
     * the tx, i.e., committed or aborted.
     *
     */
    Finished,
    /**
     * @brief A tx in the recovering state resumes execution of unfinished,
     * multi-stage operations that are guaranteed to succeed. Example operations
     * include schema evolution and range splitting and merging.
     *
     */
    Recovering,
    /**
     * @brief A tx is ready to be recycled for the next user tx.
     *
     */
    Recycled
};

using NodeGroupId = uint32_t;

enum class TableType : uint8_t
{
    Primary = 0,
    Secondary,
    Catalog,
    RangePartition
};

struct TableName
{
    TableName() = delete;
    TableName &operator=(const TableName &) = delete;

    explicit TableName(std::string_view name_view, TableType type)
        : name_view_(name_view), own_string_(false), type_(type)
    {
    }

    explicit TableName(const char *name_ptr, size_t name_len, TableType type)
        : name_str_(name_ptr, name_len), own_string_(true), type_(type)
    {
    }

    // Copy constructor always creates a string owner
    TableName(const TableName &rhs)
        : name_str_(rhs.StringView().data(), rhs.StringView().size()),
          own_string_(true),
          type_(rhs.type_)
    {
    }

    // TableName needs to be MoveInsertable in case like
    // std::vector<txservice::TableName>
    TableName(TableName &&rhs)
    {
        if (rhs.own_string_)
        {
            new (&name_str_) std::string(rhs.name_str_);
        }
        else
        {
            name_view_ = rhs.name_view_;
        }
        type_ = rhs.type_;
        own_string_ = rhs.own_string_;
    }

    TableName &operator=(TableName &&rhs)
    {
        if (this == &rhs)
        {
            return *this;
        }

        if (rhs.own_string_)
        {
            if (own_string_)
            {
                name_str_ = std::move(rhs.name_str_);
            }
            else
            {
                new (&name_str_) std::string(std::move(rhs.name_str_));
            }
        }
        else
        {
            if (own_string_)
            {
                name_str_.~basic_string();
            }

            name_view_ = rhs.StringView();
        }

        type_ = rhs.type_;
        own_string_ = rhs.own_string_;

        return *this;
    }

    ~TableName()
    {
        if (own_string_)
        {
            name_str_.~basic_string();
        }
    }

    bool operator==(const TableName &rhs) const
    {
        return type_ == rhs.type_ && this->StringView() == rhs.StringView();
    }

    bool operator<(const TableName &rhs) const
    {
        return type_ == rhs.type_ && this->StringView() < rhs.StringView();
    }

    std::string_view StringView() const
    {
        if (own_string_)
        {
            return {name_str_.data(), name_str_.size()};
        }
        else
        {
            return name_view_;
        }
    }

    std::string String() const
    {
        if (own_string_)
        {
            return name_str_;
        }
        else
        {
            return std::string(name_view_);
        }
    }

    std::string Trace() const
    {
        if (own_string_)
        {
            if (type_ == TableType::RangePartition)
            {
                return name_str_ + "_ranges";
            }
            else
            {
                return name_str_;
            }
        }
        else
        {
            if (type_ == TableType::RangePartition)
            {
                return std::string(name_view_) + "_ranges";
            }
            else
            {
                return std::string(name_view_);
            }
        }
    }

    void CopyFrom(const TableName &other)
    {
        if (other.own_string_)
        {
            if (own_string_)
            {
                name_str_ = other.name_str_;
            }
            else
            {
                new (&name_str_) std::string(other.name_str_);
            }
        }
        else
        {
            if (own_string_)
            {
                name_str_.~basic_string();
            }

            name_view_ = other.StringView();
        }

        type_ = other.type_;
        own_string_ = other.own_string_;
    }

    const std::string_view GetBaseTableNameSV() const
    {
        if (type_ == TableType::Secondary || type_ == TableType::RangePartition)
        {
            size_t pos = this->StringView().find(INDEX_NAME_PREFIX);
            std::string_view base_table_name =
                this->StringView().substr(0, pos);

            return base_table_name;
        }
        return this->StringView();
    }

    bool IsBase() const
    {
        size_t pos = this->StringView().find(INDEX_NAME_PREFIX);
        return (pos == std::string_view::npos) ? true : false;
    }

    bool IsStringOwner() const
    {
        return own_string_;
    }

    const TableType &Type() const
    {
        return type_;
    }

private:
    // base or index table name
    union
    {
        std::string name_str_;
        std::string_view name_view_;
    };

    bool own_string_;
    TableType type_;
};

enum struct ReadType
{
    // Starts concurrency control for the input key and returns the key's
    // value.
    Inside = 0,
    // Starts concurrency control for the input key-value pair retrieved from
    // the data store.
    OutsideNormal,
    // Starts concurrency control for the input key that does not exist in the
    // data store.
    OutsideDeleted
};

enum class PostWriteType
{
    // Single commit installs the committed value and removes the write
    // lock/intent.
    Commit,
    // PrepareCommit uploads a dirty value but does not release the write
    // intent/lock acquired previously. After the prepare commit log flushed,
    // the operation is guaranteed to succeed and can only roll forward upon
    // failures.
    PrepareCommit,
    // PostCommit releases the write lock/intent and turns the dirty value to
    // the committed value.
    PostCommit
};

inline static std::string_view empty_sv{"__empty"};
inline static std::string_view catalog_ccm_name_sv{"__catalog"};

inline static TableName catalog_ccm_name{
    catalog_ccm_name_sv.data(), catalog_ccm_name_sv.size(), TableType::Catalog};
}  // namespace txservice

namespace std
{
template <>
struct hash<txservice::TableName>
{
    size_t operator()(const txservice::TableName &name) const
    {
        return std::hash<std::string_view>()(name.StringView());
    }
};
}  // namespace std

namespace txservice
{
struct AlterTableInfo
{
    AlterTableInfo() : index_add_count_(0), index_drop_count_(0)
    {
    }

    /**
     * Serialized altered table info string:
     * ------------------------------------------------------------------------
     * | add index count | add index names len | add index names(consist of
     * ------------------------------------------------------------------------
     * ------------------------------------------------------------------------
     * TableName and kv table name) | drop index count | drop index names len |
     * ------------------------------------------------------------------------
     * ----------------------------------------------------------
     * drop index names(consist of TableName and kv table name) |
     * ----------------------------------------------------------
     */
    std::string SerializeAlteredTableInfo()
    {
        std::string res;
        if (index_add_count_ == 0 && index_drop_count_ == 0)
        {
            return res.append("");
        }
        // add index
        res.append(reinterpret_cast<const char *>(&(index_add_count_)),
                   sizeof(uint8_t));
        size_t index_name_len;
        std::string add_index_name;
        for (auto add_index_it = index_add_names_.cbegin();
             add_index_it != index_add_names_.cend();
             add_index_it++)
        {
            add_index_name.append(add_index_it->first.String())
                .append(" ")
                .append(add_index_it->second)
                .append(" ");
        }
        index_name_len = add_index_name.length();
        res.append(reinterpret_cast<const char *>(&index_name_len),
                   sizeof(add_index_name.length()));
        res.append(add_index_name.data(), add_index_name.length());

        // drop index
        std::string drop_index_name;
        res.append(reinterpret_cast<const char *>(&(index_drop_count_)),
                   sizeof(uint8_t));
        for (auto drop_index_it = index_drop_names_.cbegin();
             drop_index_it != index_drop_names_.cend();
             drop_index_it++)
        {
            drop_index_name.append(drop_index_it->first.String())
                .append(" ")
                .append(drop_index_it->second)
                .append(" ");
        }
        index_name_len = drop_index_name.length();
        res.append(reinterpret_cast<const char *>(&index_name_len),
                   sizeof(drop_index_name.length()));
        res.append(drop_index_name.data(), drop_index_name.length());

        return res;
    }

    void DeserializeAlteredTableInfo(
        const std::string &altered_table_info_image)
    {
        if (altered_table_info_image.length() <= 0)
        {
            index_add_count_ = 0;
            index_drop_count_ = 0;
            return;
        }
        size_t offset = 0;
        const char *buf = altered_table_info_image.data();

        index_add_count_ = *(uint8_t *) (buf + offset);
        offset += sizeof(uint8_t);
        size_t add_index_names_len = *(size_t *) (buf + offset);
        offset += sizeof(add_index_names_len);
        if (index_add_count_ > 0)
        {
            std::string add_index_names(buf + offset, add_index_names_len);

            std::stringstream add_ss(add_index_names);
            std::istream_iterator<std::string> begin(add_ss);
            std::istream_iterator<std::string> end;
            std::vector<std::string> tokens(begin, end);
            for (auto it = tokens.begin(); it != tokens.end(); ++it)
            {
                txservice::TableName add_index_name(
                    std::string_view(*it), txservice::TableType::Secondary);
                const std::string &add_index_kv_name = *(++it);

                index_add_names_.emplace(add_index_name, add_index_kv_name);
            }
        }
        else
        {
            index_add_names_.clear();
        }
        offset += add_index_names_len;

        index_drop_count_ = *(uint8_t *) (buf + offset);
        offset += sizeof(uint8_t);
        size_t drop_index_names_len = *(size_t *) (buf + offset);
        offset += sizeof(drop_index_names_len);
        if (index_drop_count_ > 0)
        {
            std::string drop_index_names(buf + offset, drop_index_names_len);

            std::stringstream drop_ss(drop_index_names);
            std::istream_iterator<std::string> begin(drop_ss);
            std::istream_iterator<std::string> end;
            std::vector<std::string> tokens(begin, end);
            for (auto it = tokens.begin(); it != tokens.end(); ++it)
            {
                txservice::TableName drop_index_name(
                    std::string_view(*it), txservice::TableType::Secondary);
                const std::string &drop_index_kv_name = *(++it);

                index_drop_names_.emplace(drop_index_name, drop_index_kv_name);
            }
        }
        else
        {
            index_drop_names_.clear();
        }
        offset += drop_index_names_len;

        assert(offset == altered_table_info_image.length());
    }

    uint8_t index_add_count_;
    uint8_t index_drop_count_;
    // map of <mysql_index_table_name, kv_index_table_name>
    std::unordered_map<txservice::TableName, std::string> index_add_names_;
    std::unordered_map<txservice::TableName, std::string> index_drop_names_;
};
}  // namespace txservice
