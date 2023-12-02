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

    BucketInfo(const BucketInfo &other) = delete;

    BucketInfo &operator=(const BucketInfo &rhs)
    {
        if (this == &rhs)
        {
            return *this;
        }
        bucket_owner_ = rhs.bucket_owner_;
        version_ = rhs.version_;
        dirty_bucket_owner_ = rhs.dirty_bucket_owner_;
        dirty_version_ = rhs.dirty_version_;
        return *this;
    }

    void Reset()
    {
        bucket_owner_ = UINT32_MAX;
        version_ = 0;
        dirty_bucket_owner_ = UINT32_MAX;
        dirty_version_ = 0;
    }

    void ClearDirty()
    {
        dirty_bucket_owner_ = UINT32_MAX;
        dirty_version_ = 0;
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

    std::unique_ptr<BucketInfo> Clone() const
    {
        std::unique_ptr<BucketInfo> clone =
            std::make_unique<BucketInfo>(bucket_owner_, version_);
        clone->SetDirty(dirty_bucket_owner_, dirty_version_);
        return clone;
    }

    void Set(NodeGroupId owner_ng, uint64_t version)
    {
        if (version > version_)
        {
            bucket_owner_ = owner_ng;
            version_ = version;
            assert(dirty_version_ == 0);
        }
    }

    void SetDirty(NodeGroupId dirty_bucket_owner, uint64_t dirty_version)
    {
        if (dirty_version > version_ && dirty_version > dirty_version_)
        {
            dirty_bucket_owner_ = dirty_bucket_owner;
            dirty_version_ = dirty_version;
        }
    }

    void CommitDirty()
    {
        if (dirty_version_ > version_)
        {
            version_ = dirty_version_;
            bucket_owner_ = dirty_bucket_owner_;
        }
        dirty_bucket_owner_ = UINT32_MAX;
        dirty_version_ = 0;
    }

private:
    NodeGroupId bucket_owner_{UINT32_MAX};
    uint64_t version_{0};

    NodeGroupId dirty_bucket_owner_{UINT32_MAX};
    uint64_t dirty_version_{0};
    friend struct RangeBucketRecord;
    friend class RangeBucketCcMap;
};

struct BucketMigrateInfo
{
    BucketMigrateInfo() = default;

    std::vector<uint16_t> bucket_ids_;
    std::vector<NodeGroupId> new_owner_ngs_;
    bool has_migration_tx_{false};
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

    friend class RangeBucketCcMap;
};

struct RangeBucketRecord : public TxRecord
{
public:
    RangeBucketRecord() : bucket_info_(nullptr), is_owner_(false)
    {
    }
    RangeBucketRecord(BucketInfo *bucket_info)
        : bucket_info_(bucket_info), is_owner_(false)
    {
    }
    RangeBucketRecord(RangeBucketRecord &&rhs) : is_owner_(rhs.is_owner_)
    {
        if (is_owner_)
        {
            bucket_info_uptr_ = std::move(rhs.bucket_info_uptr_);
        }
        else
        {
            bucket_info_ = rhs.bucket_info_;
        }
    }
    RangeBucketRecord(const RangeBucketRecord &rhs) : is_owner_(rhs.is_owner_)
    {
        if (is_owner_)
        {
            bucket_info_uptr_ = rhs.bucket_info_uptr_->Clone();
        }
        else
        {
            bucket_info_ = rhs.bucket_info_;
        }
    }
    RangeBucketRecord &operator=(const RangeBucketRecord &rhs)
    {
        if (&rhs == this)
        {
            return *this;
        }
        if (!is_owner_)
        {
            // clear pointer so that we don't accidently frees the bucket info
            // when moving in unique ptr.
            bucket_info_ = nullptr;
        }
        else if (bucket_info_uptr_)
        {
            // Free the bucket info since we might not call destructor of bucket
            // info if rhs is not owner.
            bucket_info_uptr_.reset();
        }

        is_owner_ = rhs.is_owner_;
        if (is_owner_)
        {
            bucket_info_uptr_ = rhs.bucket_info_uptr_->Clone();
        }
        else
        {
            bucket_info_ = rhs.bucket_info_;
        }
        return *this;
    }
    ~RangeBucketRecord()
    {
        if (is_owner_ && bucket_info_uptr_)
        {
            bucket_info_uptr_.reset();
        }
    }

    void Serialize(std::vector<char> &buf, size_t &offset) const override;
    void Serialize(std::string &str) const override;
    size_t SerializedLength() const override;
    void Deserialize(const char *buf, size_t &offset) override;
    TxRecord::Uptr Clone() const override;
    void Copy(const TxRecord &rhs) override;
    std::string ToString() const override;

    const BucketInfo *GetBucketInfo() const
    {
        return is_owner_ ? bucket_info_uptr_.get() : bucket_info_;
    }

    size_t Size() const override
    {
        return sizeof(bucket_info_);
    }

    size_t MemUsage() const override
    {
        return sizeof(*this);
    }

    void SetBucketInfo(const BucketInfo *bucket_info)
    {
        if (is_owner_ && bucket_info_uptr_)
        {
            bucket_info_uptr_.reset();
        }
        is_owner_ = false;
        bucket_info_ = bucket_info;
    }

    void SetBucketInfo(std::unique_ptr<BucketInfo> bucket_info)
    {
        if (!is_owner_)
        {
            bucket_info_ = nullptr;
        }
        is_owner_ = true;
        bucket_info_uptr_ = std::move(bucket_info);
    }

private:
    union
    {
        const BucketInfo *bucket_info_;
        std::unique_ptr<BucketInfo> bucket_info_uptr_;
    };

    bool is_owner_{false};
};
}  // namespace txservice