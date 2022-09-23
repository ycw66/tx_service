#pragma once

#include <cassert>
#include <iostream>
#include <string>
#include <string_view>
#include <utility>  //move

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

enum class DmlOperation
{
    Update,
    Delete,
    Insert,
    Upsert
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

    const std::string_view GetBaseTableName() const
    {
        if (type_ == TableType::Secondary)
        {
            size_t pos = this->StringView().find(INDEX_NAME_PREFIX);
            assert(pos != std::string_view::npos);
            std::string_view base_table_name =
                this->StringView().substr(0, pos);

            return base_table_name;
        }
        return this->StringView();
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

enum class LockType
{
    NoLock = 0,
    ReadIntent,
    WriteIntent,
    ReadLock,
    WriteLock,
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

using namespace std::string_view_literals;

inline static std::string_view empty_sv = "__empty"sv;
inline static std::string_view catalog_ccm_name_sv = "__catalog"sv;

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
