#pragma once

#include <variant>

#include "tx_key.h"
#include "tx_record.h"

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
          next_partition_id_(next_partition_id)
    {
    }

    std::unique_ptr<TxKey> start_key_;
    uint64_t version_ts_{1};
    uint32_t partition_id_;
    uint32_t next_partition_id_;

    std::unique_ptr<TxKey> new_key_{nullptr};
    uint32_t new_partition_id_{0};

    uint64_t dirty_ts_{0};
};

struct InitRangeEntry
{
    InitRangeEntry(const InitRangeEntry &rhs) = delete;

    InitRangeEntry(std::unique_ptr<TxKey> start_key,
                   uint32_t partition_id,
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
    uint32_t partition_id_{0};
    uint64_t version_ts_{0};
};

struct RangePair
{
    uint32_t partition_id_{0};
    uint32_t next_partition_id_{UINT32_MAX};
};

struct RangeRecord : public TxRecord
{
public:
    RangeRecord() = default;
    RangeRecord(const RangeRecord &rhs) : binary_value_(rhs.binary_value_)
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
        binary_value_ = that.binary_value_;
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
        binary_value_ = rhs.binary_value_;
        return *this;
    }

    const TableRangeEntry *RangeEntry() const
    {
        const auto range_entry = std::get_if<0>(&binary_value_);
        return *range_entry;
    }

    const TxKey *Key()
    {
        const auto key = std::get_if<1>(&binary_value_);
        return *key;
    }

    /**
     * @brief The binary value of a range record serves two purposes: (1) a
     * tx searches the partition ID of a range containing the input key. The
     * returned range record's value points to a table range entry, which gives
     * the range's partition ID and the new partition ID if the range is being
     * split or merged. (2) A tx splits/merges an existing range and uses the
     * range record to install a "dirty version" of the range in the tx service.
     * The record's value points to the start key of a range that is either (a)
     * split from the existing range or (b) merged with the existing range.
     *
     */
    std::variant<const TableRangeEntry *, const TxKey *> binary_value_;
};
}  // namespace txservice