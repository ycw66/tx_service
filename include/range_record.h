#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "tx_key.h"
#include "tx_record.h"

namespace txservice
{
struct TableRangeEntry
{
    TableRangeEntry() = delete;
    TableRangeEntry(std::unique_ptr<TxKey> start_key,
                    uint64_t version_ts,
                    int32_t partition_id,
                    int32_t next_partition_id)
        : start_key_(std::move(start_key)),
          version_ts_(version_ts),
          partition_id_(partition_id),
          next_partition_id_(next_partition_id),
          new_key_(nullptr),
          new_partition_id_(-1),
          dirty_ts_(0)
    {
    }

    std::unique_ptr<TableRangeEntry> Clone() const
    {
        std::unique_ptr<TxKey> start_key_clone =
            start_key_ == nullptr ? nullptr : start_key_->Clone();
        TableRangeEntry *that = new TableRangeEntry(std::move(start_key_clone),
                                                    version_ts_,
                                                    partition_id_,
                                                    next_partition_id_);
        if (new_key_ == nullptr)
        {
            that->new_key_ = nullptr;
        }
        else
        {
            std::unique_ptr<TxKey> new_key_clone = new_key_->Clone();
            that->new_key_ = std::move(new_key_clone);
        }
        that->new_partition_id_ = new_partition_id_;
        return std::unique_ptr<TableRangeEntry>(that);
    }

    void SetDirty(std::unique_ptr<TxKey> new_key,
                  uint32_t new_partition_id,
                  uint64_t dirty_ts)
    {
        new_key_ = std::move(new_key);
        new_partition_id_ = new_partition_id;
        dirty_ts_ = dirty_ts;
    }

    void ClearDirty(uint64_t commit_ts = 0)
    {
        new_key_ = nullptr;
        new_partition_id_ = -1;
        dirty_ts_ = 0;
        if (commit_ts != 0)
        {
            version_ts_ = commit_ts;
        }
    }

    bool IsDirty()
    {
        return (new_key_ != nullptr) && (new_partition_id_ != 0);
    }

    void Serialize(std::string &str)
    {
    }

    void Deserialize(const char *buf, size_t &offset)
    {
    }

    // TODO(Xiao Ji): Replace unique_ptr with shared_ptr, so we can make sure
    // the TxKey pointer is still valid even when the range entry is is deleted
    std::unique_ptr<TxKey> start_key_;
    uint64_t version_ts_{1};
    int32_t partition_id_{0};
    int32_t next_partition_id_{0};

    std::unique_ptr<TxKey> new_key_{nullptr};
    int32_t new_partition_id_{-1};

    uint64_t dirty_ts_{0};
};

struct TableRangeEntryWithShade
{
    TableRangeEntryWithShade(std::unique_ptr<TxKey> start_key,
                             uint64_t version_ts,
                             int32_t partition_id,
                             int32_t next_partition_id)
        : shader_(std::make_unique<TableRangeEntry>(std::move(start_key),
                                                    version_ts,
                                                    partition_id,
                                                    next_partition_id)),
          shade_(std::unique_ptr<TableRangeEntry>(nullptr)){};

    std::unique_ptr<TableRangeEntry> shader_;
    std::unique_ptr<TableRangeEntry> shade_;
};

struct InitRangeEntry
{
    InitRangeEntry(const InitRangeEntry &rhs) = delete;

    InitRangeEntry(std::unique_ptr<TxKey> start_key,
                   int32_t partition_id,
                   uint64_t version_ts)
        : key_(std::move(start_key)),
          partition_id_(partition_id),
          version_ts_(version_ts)
    {
    }

    InitRangeEntry(InitRangeEntry &&rhs)
        : key_(std::move(rhs.key_)),
          partition_id_(rhs.partition_id_),
          version_ts_(rhs.version_ts_)
    {
    }

    std::unique_ptr<TxKey> key_{nullptr};
    int32_t partition_id_{0};
    uint64_t version_ts_{0};
};

struct RangeRecord : public TxRecord
{
public:
    RangeRecord() = default;
    RangeRecord(const RangeRecord &rhs) : range_entry_(rhs.range_entry_)
    {
    }

    ~RangeRecord() = default;

    void Serialize(std::vector<char> &buf, size_t &offset) const override
    {
    }

    void Serialize(std::string &str) const override
    {
    }

    void Deserialize(const char *buf, size_t &offset) override
    {
    }

    TxRecord::Uptr Clone() const override
    {
        return std::make_unique<RangeRecord>(*this);
    }

    void Copy(const TxRecord &rhs) override
    {
        const RangeRecord &that = static_cast<const RangeRecord &>(rhs);
        range_entry_ = that.range_entry_;
    }

    std::string ToString() const override
    {
        return "";
    }

    RangeRecord &operator=(const RangeRecord &rhs)
    {
        if (this == &rhs)
        {
            return *this;
        }
        range_entry_ = rhs.range_entry_;
        return *this;
    }

    const TableRangeEntry *RangeEntry() const
    {
        return range_entry_;
    }

    const TableRangeEntry *range_entry_{nullptr};
};
}  // namespace txservice
