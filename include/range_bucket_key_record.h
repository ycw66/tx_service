#pragma once

#include <map>
#include <unordered_set>

#include "tx_key.h"
#include "tx_record.h"
#include "type.h"

namespace txservice
{
struct BucketInfo
{
public:
    BucketInfo() = default;
    BucketInfo(const NodeGroupId bucket_owner, uint64_t version)
        : bucket_owner_(bucket_owner), version_(version)
    {
    }

    NodeGroupId BucketOwner() const
    {
        return bucket_owner_;
    }

    uint64_t Version() const
    {
        return version_;
    }

    NodeGroupId DirtyBucketOwner() const
    {
        return dirty_bucket_owner_;
    }

    uint64_t DirtyVersion() const
    {
        return dirty_version_;
    }

    std::unordered_map<TableName, std::unordered_set<int32_t>> &RangesInBucket()
    {
        return ranges_in_bucket_;
    }

private:
    NodeGroupId bucket_owner_{UINT32_MAX};
    // Keep track of the ranges in this bucket. We only track
    // the ranges that are loaded into memory.
    // Note that the table name here is of type RangePartition.
    std::unordered_map<TableName, std::unordered_set<int32_t>>
        ranges_in_bucket_;
    uint64_t version_;

    NodeGroupId dirty_bucket_owner_{UINT32_MAX};
    uint64_t dirty_version_;
};

struct RangeBucketKey : public TxKey
{
public:
    RangeBucketKey() = default;
    RangeBucketKey(const int16_t bucket_id) : bucket_id_(bucket_id)
    {
    }
    RangeBucketKey(RangeBucketKey &&rhs) = default;
    RangeBucketKey(const RangeBucketKey &rhs) = default;
    RangeBucketKey &operator=(RangeBucketKey &&) = default;
    ~RangeBucketKey() = default;

    bool operator==(const TxKey &rhs) const override;
    bool operator<(const TxKey &rhs) const override;
    size_t Hash() const override;
    void Serialize(std::vector<char> &buf, size_t &offset) const override;
    void Serialize(std::string &str) const override;
    size_t SerializedLength() const override;
    void Deserialize(const char *buf, size_t &offset, const Schema *) override;
    TxKey::Uptr Clone() const override;
    std::string ToString() const override;
    void Copy(const TxKey &rhs) override;
    KeyType Type() const override
    {
        return KeyType::Normal;
    }
    size_t Size() const override
    {
        return sizeof(bucket_id_);
    }

    friend bool operator==(const RangeBucketKey &lhs,
                           const RangeBucketKey &rhs);
    friend bool operator!=(const RangeBucketKey &lhs,
                           const RangeBucketKey &rhs);
    friend bool operator<(const RangeBucketKey &lhs, const RangeBucketKey &rhs);
    friend bool operator<=(const RangeBucketKey &lhs,
                           const RangeBucketKey &rhs);

private:
    uint16_t bucket_id_{UINT16_MAX};
};

struct RangeBucketRecord : public TxRecord
{
public:
    RangeBucketRecord() = default;
    RangeBucketRecord(BucketInfo *bucket_info) : bucket_info_(bucket_info)
    {
    }
    RangeBucketRecord(RangeBucketRecord &&rhs) = default;
    RangeBucketRecord(const RangeBucketRecord &rhs) = default;
    RangeBucketRecord &operator=(const RangeBucketRecord &rhs) = default;
    ~RangeBucketRecord() = default;

    void Serialize(std::vector<char> &buf, size_t &offset) const override;
    void Serialize(std::string &str) const override;
    size_t SerializedLength() const override;
    void Deserialize(const char *buf, size_t &offset) override;
    TxRecord::Uptr Clone() const override;
    void Copy(const TxRecord &rhs) override;
    std::string ToString() const override;

    const BucketInfo *GetBucketInfo() const
    {
        return bucket_info_;
    }

    size_t Size() const override
    {
        return sizeof(bucket_info_);
    }

    size_t MemUsage() const override
    {
        return sizeof(*this);
    }

private:
    BucketInfo *bucket_info_{nullptr};
};
}  // namespace txservice