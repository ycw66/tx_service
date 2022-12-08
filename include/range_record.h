#pragma once

#include <algorithm>
#include <cassert>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "butil/logging.h"
#include "range_slice.h"
#include "tx_key.h"
#include "tx_record.h"
#include "tx_serialize.h"

namespace txservice
{
struct TableRangeEntry
{
    TableRangeEntry() = delete;

    TableRangeEntry(std::unique_ptr<TxKey> start_key,
                    uint64_t version_ts,
                    uint32_t partition_id,
                    uint32_t next_partition_id)
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
                             int32_t next_partition_id,
                             std::unique_ptr<StoreRange> slices = nullptr)
        : shader_(std::make_unique<TableRangeEntry>(std::move(start_key),
                                                    version_ts,
                                                    partition_id,
                                                    next_partition_id)),
          shade_(std::unique_ptr<TableRangeEntry>(nullptr)),
          range_slices_(std::move(slices)){};

    std::unique_ptr<TableRangeEntry> shader_;
    std::unique_ptr<TableRangeEntry> shade_;

    std::unique_ptr<StoreRange> range_slices_{nullptr};
};

struct InitRangeEntry
{
    InitRangeEntry() : key_(nullptr), partition_id_(-1), version_ts_(-1)
    {
    }

    InitRangeEntry(const InitRangeEntry &rhs)
        : key_(nullptr),
          partition_id_(rhs.partition_id_),
          version_ts_(rhs.version_ts_)
    {
        std::unique_ptr<TxKey> key =
            rhs.key_ == nullptr ? nullptr : rhs.key_->Clone();
        key_ = std::move(key);
    }

    InitRangeEntry(std::unique_ptr<TxKey> start_key,
                   int32_t partition_id,
                   uint64_t version_ts)
        : key_(std::move(start_key)),
          partition_id_(partition_id),
          version_ts_(version_ts)
    {
    }

#ifdef RANGE_PARTITIONED
    InitRangeEntry(
        std::unique_ptr<TxKey> start_key,
        int32_t partition_id,
        uint64_t version_ts,
        std::vector<std::pair<std::unique_ptr<TxKey>, uint32_t>> keys)
        : key_(std::move(start_key)),
          partition_id_(partition_id),
          version_ts_(version_ts),
          slice_keys_(std::move(keys))
    {
    }
#endif

    InitRangeEntry(InitRangeEntry &&rhs)
        : key_(std::move(rhs.key_)),
          partition_id_(rhs.partition_id_),
          version_ts_(rhs.version_ts_)
#ifdef RANGE_PARTITIONED
          ,
          slice_keys_(std::move(rhs.slice_keys_))
#endif
    {
    }

    InitRangeEntry &operator=(const InitRangeEntry &rhs)
    {
        if (this == &rhs)
        {
            return *this;
        }
        std::unique_ptr<TxKey> key =
            rhs.key_ == nullptr ? nullptr : rhs.key_->Clone();
        key_ = std::move(key);
        partition_id_ = rhs.partition_id_;
        version_ts_ = rhs.version_ts_;

        return *this;
    }

    std::unique_ptr<TxKey> key_{nullptr};
    int32_t partition_id_{0};
    uint64_t version_ts_{0};
#ifdef RANGE_PARTITIONED
    std::vector<std::pair<std::unique_ptr<TxKey>, uint32_t>> slice_keys_;
#endif
};

struct RangeRecord : public TxRecord
{
public:
    RangeRecord() = default;
    RangeRecord(const RangeRecord &rhs)
        : range_entry_(rhs.range_entry_), end_key_(rhs.end_key_)
    {
    }

    ~RangeRecord() = default;

    void Serialize(std::vector<char> &buf, size_t &offset) const override
    {
        assert(false);
    }

    void Serialize(std::string &str) const override
    {
        range_entry_->new_key_->Serialize(str);
        serialize_to_str(&range_entry_->partition_id_, str);
        serialize_to_str(&range_entry_->new_partition_id_, str);
    }

    void Deserialize(const char *buf, size_t &offset) override
    {
        assert(false);
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

    size_t Size() const override
    {
        return 8 + 8;
    }

    const TableRangeEntry *range_entry_{nullptr};
    /**
     * @brief The exclusive end of the range, which is also the start of the
     * next range. Null, if this is the last range and end key points to
     * positive infinity.
     *
     */
    const TxKey *end_key_{nullptr};
};
}  // namespace txservice
