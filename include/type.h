#pragma once

#include <cassert>
#include <functional>
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
    DropIndex,

    // redis object command operation
    RedisCommand
};

/**
 * @brief Operations of on KV store. This is used when a composite operation
 * needs to run multiple operation on KV store in different phases. Currently
 * used in SplitFlushOp.
 */
enum class DsOperation
{
    // Copy data from old partition to new partition and flush in memory data
    // to new partition.
    CopyAndFlush = 1,
    // Upsert new range to range table.
    UpsertRange,
    // Clean data that have been copied to new partition in old partition.
    CleanOldRange
};

enum class ClusterScaleOpType
{
    AddNode = 1,
    RemoveNode
};

struct NodeConfig
{
public:
    NodeConfig() = default;
    NodeConfig(uint32_t node_id, const std::string &host_name, uint16_t port)
        : node_id_(node_id), host_name_(host_name), port_(port)
    {
    }

    NodeConfig(const NodeConfig &rhs)
        : node_id_(rhs.node_id_), host_name_(rhs.host_name_), port_(rhs.port_)
    {
    }
    uint32_t node_id_{UINT32_MAX};
    std::string host_name_{""};
    uint16_t port_{0};
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
    UniqueSecondary,
    Catalog,
    RangePartition,
    RangeBucket,
    ClusterConfig
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

    explicit TableName(const std::string &name_str, TableType type)
        : name_str_(name_str), own_string_(true), type_(type)
    {
    }

    explicit TableName(std::string &&name_str, TableType type)
        : name_str_(std::move(name_str)), own_string_(true), type_(type)
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

    bool operator!=(const TableName &rhs) const
    {
        return !this->operator==(rhs);
    }

    bool operator<(const TableName &rhs) const
    {
        return type_ < rhs.type_ ||
               type_ == rhs.type_ && this->StringView() < rhs.StringView();
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
        std::string_view base_name_view = StringView();
        size_t pos = base_name_view.find(INDEX_NAME_PREFIX);
        if (pos != std::string_view::npos)
        {
            return base_name_view.substr(0, pos);
        }
        else
        {
            pos = base_name_view.find(UNIQUE_INDEX_NAME_PREFIX);
            if (pos != std::string_view::npos)
            {
                return base_name_view.substr(0, pos);
            }
            return base_name_view;
        }
    }

    // Check whether it is range table for primary key
    bool IsBase() const
    {
        return TableName::IsBase(this->StringView());
    }

    static bool IsBase(const std::string_view &table_name_sv)
    {
        size_t pos = table_name_sv.find(INDEX_NAME_PREFIX);
        if (pos == std::string_view::npos)
        {
            pos = table_name_sv.find(UNIQUE_INDEX_NAME_PREFIX);
        }
        return (pos == std::string_view::npos) ? true : false;
    }

    bool IsMeta() const
    {
        return type_ == TableType::RangeBucket || type_ == TableType::Catalog ||
               type_ == TableType::RangePartition ||
               type_ == TableType::ClusterConfig;
    }

    static bool IsMeta(TableType type)
    {
        return type == TableType::RangeBucket || type == TableType::Catalog ||
               type == TableType::RangePartition ||
               type == TableType::ClusterConfig;
    }

    // Check whether it is range table for unique secondary key
    bool IsUniqueSecondary() const
    {
        return TableName::IsUniqueSecondary(this->StringView());
    }

    static bool IsUniqueSecondary(const std::string_view &table_name_sv)
    {
        size_t pos = table_name_sv.find(UNIQUE_INDEX_NAME_PREFIX);
        return (pos != std::string_view::npos) ? true : false;
    }

    bool IsStringOwner() const
    {
        return own_string_;
    }

    const TableType &Type() const
    {
        return type_;
    }

    // @brief Get table type base on table name, only return Primary and
    // Secondary
    static TableType Type(const std::string_view &table_name_sv)
    {
        if (IsBase(table_name_sv))
        {
            return TableType::Primary;
        }
        else if (IsUniqueSecondary(table_name_sv))
        {
            return TableType::UniqueSecondary;
        }
        else
        {
            return TableType::Secondary;
        }
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
    /**
     * @brief Starts concurrency control for the input key and returns the key's
     * value if the value is cached.
     *
     */
    Inside = 0,
    /**
     * @brief Starts concurrency control for the input key-value pair retrieved
     * from the data store.
     *
     */
    OutsideNormal,
    /**
     * @brief Starts concurrency control for the input key that does not exist
     * in the data store.
     *
     */
    OutsideDeleted,
    /**
     * @brief Given the input key k0, starts concurrency control for the range
     * [start, end) such that start <= k0 < end. This is to lock the next range
     * when scanning forward.
     *
     */
    RangeLeftInclusive,
    /**
     * @brief Given the input key k0, starts concurrency control for the range
     * [start, end) such that start < k0 <= end. This is to lock the next range
     * when scanning backward.
     *
     */
    RangeRightExclusive,
    /**
     * @breif Read in recovering status.
     */
    RecoveringRead
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
inline static std::string_view redis_table_name_sv{"redis_table"};
inline static std::string_view range_bucket_ccm_name_sv{"__range_bucekt"};
inline static std::string_view cluster_config_ccm_name_sv{"__cluster_config"};

inline static TableName catalog_ccm_name{
    catalog_ccm_name_sv.data(), catalog_ccm_name_sv.size(), TableType::Catalog};

inline static TableName range_bucket_ccm_name{range_bucket_ccm_name_sv.data(),
                                              range_bucket_ccm_name_sv.size(),
                                              TableType::RangeBucket};
inline static TableName cluster_config_ccm_name{
    cluster_config_ccm_name_sv.data(),
    cluster_config_ccm_name_sv.size(),
    TableType::ClusterConfig};
inline static const uint16_t total_range_buckets = 4096;

enum struct SlicePosition
{
    Middle = 0,
    /**
     * @brief The scanned slice is the first slice in its range.
     *
     */
    FirstSliceInRange,
    /**
     * @brief The scanned slice is the last slice in its range.
     *
     */
    LastSliceInRange,
    /**
     * @brief The scanned slice is the first slice in the first range. In
     * other words, the slice's start key is negative infinity.
     *
     */
    FirstSlice,
    /**
     * @brief The scanned slice is the last slice in the last range. In
     * other words, the slice's end key is positive infinity.
     *
     */
    LastSlice
};
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
template <typename KeyT>
struct Copy
{
    constexpr void operator()(KeyT &lhs, const KeyT &rhs) const
    {
        lhs = rhs;
    }
};
}  // namespace txservice

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
            // Clear this buff.
            index_add_names_.clear();
            std::string add_index_names(buf + offset, add_index_names_len);

            std::stringstream add_ss(add_index_names);
            std::istream_iterator<std::string> begin(add_ss);
            std::istream_iterator<std::string> end;
            std::vector<std::string> tokens(begin, end);
            for (auto it = tokens.begin(); it != tokens.end(); ++it)
            {
                bool is_unique_sk = std::next(it, 1)->front() == 'u';
                txservice::TableName add_index_name(
                    std::string_view(*it),
                    is_unique_sk ? TableType::UniqueSecondary
                                 : TableType::Secondary);
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
            // Clear this buff.
            index_drop_names_.clear();
            std::string drop_index_names(buf + offset, drop_index_names_len);

            std::stringstream drop_ss(drop_index_names);
            std::istream_iterator<std::string> begin(drop_ss);
            std::istream_iterator<std::string> end;
            std::vector<std::string> tokens(begin, end);
            for (auto it = tokens.begin(); it != tokens.end(); ++it)
            {
                bool is_unique_sk = std::next(it, 1)->front() == 'u';
                txservice::TableName drop_index_name(
                    std::string_view(*it),
                    is_unique_sk ? TableType::UniqueSecondary
                                 : TableType::Secondary);
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

    void Reset()
    {
        index_add_count_ = 0;
        index_drop_count_ = 0;
        index_add_names_.clear();
        index_drop_names_.clear();
    }

    uint8_t index_add_count_;
    uint8_t index_drop_count_;
    // map of <mysql_index_table_name, kv_index_table_name>
    std::unordered_map<txservice::TableName, std::string> index_add_names_;
    std::unordered_map<txservice::TableName, std::string> index_drop_names_;
};
}  // namespace txservice
