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
// struct that stores range related info that we read from
// KV storage during table range initialization.
struct InitRangeEntry
{
    InitRangeEntry() : key_(nullptr), partition_id_(-1), version_ts_(-1)
    {
    }

    InitRangeEntry(const InitRangeEntry &rhs) = delete;
    InitRangeEntry &operator=(const InitRangeEntry &rhs) = delete;

    InitRangeEntry(std::unique_ptr<TxKey> start_key,
                   int32_t partition_id,
                   uint64_t version_ts)
        : key_(std::move(start_key)),
          partition_id_(partition_id),
          version_ts_(version_ts)
    {
    }

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

    InitRangeEntry(InitRangeEntry &&rhs)
        : key_(std::move(rhs.key_)),
          partition_id_(rhs.partition_id_),
          version_ts_(rhs.version_ts_),
          slice_keys_(std::move(rhs.slice_keys_))
    {
    }

    std::unique_ptr<TxKey> key_{nullptr};
    int32_t partition_id_{0};
    uint64_t version_ts_{0};
    std::vector<std::pair<std::unique_ptr<TxKey>, uint32_t>> slice_keys_;
};

struct RangeInfo
{
    RangeInfo() = delete;
    RangeInfo(std::unique_ptr<TxKey> start_key,
              uint64_t version_ts,
              uint32_t partition_id,
              bool is_dirty = false)
        : start_key_(std::move(start_key)),
          partition_id_(partition_id),
          version_ts_(version_ts),
          dirty_ts_(0),
          is_dirty_(is_dirty)
    {
    }

    RangeInfo(const RangeInfo &other)
        : partition_id_(other.partition_id_),
          version_ts_(other.version_ts_),
          new_partition_id_(other.new_partition_id_),
          dirty_ts_(other.dirty_ts_),
          is_dirty_(other.is_dirty_)
    {
        if (!other.start_key_)
        {
            start_key_ = nullptr;
        }
        else
        {
            start_key_ = other.start_key_->Clone();
        }
        for (auto &key : other.new_key_)
        {
            new_key_.push_back(key->Clone());
        }
    }

    std::unique_ptr<RangeInfo> Clone() const
    {
        std::unique_ptr<TxKey> start_key_clone =
            start_key_ == nullptr ? nullptr : start_key_->Clone();
        RangeInfo *that = new RangeInfo(
            std::move(start_key_clone), version_ts_, partition_id_);
        for (auto &key : new_key_)
        {
            that->new_key_.push_back(key->Clone());
        }
        that->new_partition_id_ = new_partition_id_;
        that->is_dirty_ = is_dirty_;
        return std::unique_ptr<RangeInfo>(that);
    }

    void SetDirty(const std::vector<std::unique_ptr<TxKey>> &new_key,
                  const std::vector<int32_t> &new_partition_id,
                  uint64_t dirty_ts)
    {
        for (auto &key_uptr : new_key)
        {
            new_key_.push_back(key_uptr->Clone());
        }
        new_partition_id_ = new_partition_id;
        dirty_ts_ = dirty_ts;
        is_dirty_ = true;
    }

    void SetDirty(std::vector<std::unique_ptr<TxKey>> &&new_key,
                  std::vector<int32_t> &&new_partition_id,
                  uint64_t dirty_ts)
    {
        new_key_ = std::move(new_key);
        new_partition_id_ = std::move(new_partition_id);
        dirty_ts_ = dirty_ts;
        is_dirty_ = true;
    }

    void CommitDirty()
    {
        if (dirty_ts_ > version_ts_)
        {
            version_ts_ = dirty_ts_;
            is_dirty_ = false;
        }
    }

    void ClearDirty(uint64_t commit_ts = 0)
    {
        new_key_.clear();
        new_partition_id_.clear();
        dirty_ts_ = 0;
        if (commit_ts != 0)
        {
            version_ts_ = commit_ts;
        }
        is_dirty_ = false;
    }

    bool IsDirty() const
    {
        return is_dirty_;
    }

    int32_t GetKeyNewRangeId(const TxKey *key) const
    {
        if (!IsDirty())
        {
            return -1;
        }

        // Does not belong to any of the new ranges
        if (*key < *new_key_.front())
        {
            return -1;
        }

        uint idx = 1;
        for (; idx < new_key_.size(); idx++)
        {
            if (*key < *new_key_.at(idx))
            {
                break;
            }
        }

        return new_partition_id_.at(idx - 1);
    }

    const TxKey *StartKey() const
    {
        return start_key_.get();
    }

    int32_t PartitionId() const
    {
        return partition_id_;
    }

    uint64_t VersionTs() const
    {
        return version_ts_;
    }

    const std::vector<std::unique_ptr<TxKey>> *NewKey() const
    {
        return is_dirty_ ? &new_key_ : nullptr;
    }

    const std::vector<int32_t> *NewPartitionId() const
    {
        return is_dirty_ ? &new_partition_id_ : nullptr;
    }

    uint64_t DirtyTs() const
    {
        return is_dirty_ ? dirty_ts_ : 0;
    }

    size_t MemUsage() const
    {
        size_t mem_usage = sizeof(RangeInfo);
        if (start_key_)
        {
            mem_usage += start_key_->MemUsage();
        }
        for (auto &key : new_key_)
        {
            mem_usage += key->MemUsage();
        }
        return mem_usage;
    }

private:
    std::unique_ptr<TxKey> start_key_;
    int32_t partition_id_{0};
    uint64_t version_ts_{1};

    std::vector<std::unique_ptr<TxKey>> new_key_;
    std::vector<int32_t> new_partition_id_;
    uint64_t dirty_ts_{0};
    // is_dirty_ means if the new key and partition ids are visible to regular
    // requests. During post commit phase of range split, we have a short period
    // where we need to keep the new partition info but make them invisible to
    // regular range read request.
    bool is_dirty_{false};
    template <typename KeyT>
    friend class RangeCcMap;
    friend struct RangeRecord;
    friend struct SplitFlushRangeOp;
};

struct TableRangeEntry
{
public:
    TableRangeEntry() = default;

    TableRangeEntry(std::unique_ptr<TxKey> start_key,
                    uint64_t version_ts,
                    int64_t partition_id,
                    std::unique_ptr<StoreRange> slices = nullptr)
        : range_info_(std::make_unique<RangeInfo>(
              std::move(start_key), version_ts, partition_id)),
          range_slices_(std::move(slices))
    {
    }

    /**
     * @brief Set new table range info in range_info_.
     */
    void UploadNewRangeInfo(const std::vector<std::unique_ptr<TxKey>> &new_key,
                            const std::vector<int32_t> &new_partition_id,
                            uint64_t commit_ts)
    {
        assert(commit_ts >= range_info_->DirtyTs());

        range_info_->SetDirty(new_key, new_partition_id, commit_ts);
    }

    const RangeInfo *GetRangeInfo() const
    {
        return range_info_.get();
    }

    uint64_t Version() const
    {
        return range_info_->VersionTs();
    }

    uint64_t DirtyVersion() const
    {
        return range_info_->DirtyTs();
    }

    StoreRange *RangeSlices()
    {
        return range_slices_.get();
    }

private:
    std::unique_ptr<RangeInfo> range_info_{nullptr};
    std::unique_ptr<StoreRange> range_slices_;
    template <typename KeyT>
    friend class RangeCcMap;
};

struct RangeRecord : public TxRecord
{
public:
    RangeRecord()
        : range_info_{nullptr},
          is_info_owner_(false),
          range_slices_(nullptr),
          end_key_(nullptr)
    {
    }
    RangeRecord(const RangeRecord &rhs)
        : range_info_(rhs.GetRangeInfo()),
          is_info_owner_(false),
          range_slices_(rhs.range_slices_),
          end_key_(rhs.end_key_)
    {
    }

    RangeRecord(const RangeInfo *info,
                const std::vector<std::pair<TxKey::Uptr, size_t>> *slices,
                const TxKey *end_key)
        : range_info_(info),
          is_info_owner_(false),
          range_slices_(slices),
          end_key_(end_key)
    {
    }

    RangeRecord(std::unique_ptr<RangeInfo> info,
                const std::vector<std::pair<TxKey::Uptr, size_t>> *slices,
                const TxKey *end_key)
        : range_info_uptr_(std::move(info)),
          is_info_owner_(true),
          range_slices_(slices),
          end_key_(end_key)
    {
    }

    ~RangeRecord()
    {
        if (is_info_owner_)
        {
            range_info_uptr_.reset();
        }
    }

    void Serialize(std::vector<char> &buf, size_t &offset) const override
    {
        assert(false);
    }

    void Serialize(std::string &str) const override
    {
        // handle neg inf key
        bool is_normal = range_info_->start_key_ != nullptr &&
                         range_info_->start_key_->Type() == KeyType::Normal;
        SerializeToStr(&is_normal, str);
        if (is_normal)
        {
            range_info_->start_key_->Serialize(str);
        }
        SerializeToStr(&range_info_->partition_id_, str);
        SerializeToStr(&range_info_->version_ts_, str);

        // serialize dirty range info
        uint16_t new_part_size = range_info_->new_key_.size();
        SerializeToStr(&new_part_size, str);
        for (const TxKey::Uptr &new_key : range_info_->new_key_)
        {
            new_key->Serialize(str);
        }
        for (const int32_t &new_id : range_info_->new_partition_id_)
        {
            SerializeToStr(&new_id, str);
        }
        SerializeToStr(&range_info_->dirty_ts_, str);
        // Serialize end key
        is_normal = end_key_ != nullptr && end_key_->Type() == KeyType::Normal;
        SerializeToStr(&is_normal, str);
        if (is_normal)
        {
            end_key_->Serialize(str);
        }
        uint16_t slice_cnt;
        if (range_slices_ == nullptr)
        {
            slice_cnt = 0;
            SerializeToStr(&slice_cnt, str);
        }
        else
        {
            slice_cnt = range_slices_->size();
            SerializeToStr(&slice_cnt, str);
            for (auto slice_it = range_slices_->cbegin();
                 slice_it != range_slices_->cend();
                 slice_it++)
            {
                uint32_t slice_size = slice_it->second;
                SerializeToStr(&slice_size, str);
            }
            // skip first slice key since it will reuse range start key
            for (auto slice_it = range_slices_->cbegin() + 1;
                 slice_it != range_slices_->cend();
                 slice_it++)
            {
                slice_it->first->Serialize(str);
            }
        }
    }

    void Deserialize(const char *buf, size_t &offset) override
    {
        // This should not be called, use DeserializeRangeRecord
        // in range_cc_map. We cannot create KeyT object since
        // it is not passed into RangeRecord.
        assert(false);
    }

    size_t SerializedLength() const override
    {
        size_t size = 0;
        size += sizeof(bool);
        if (range_info_->start_key_ != nullptr &&
            range_info_->start_key_->Type() == KeyType::Normal)
        {
            size += range_info_->start_key_->SerializedLength();
        }
        // version_ts, dirty_ts, partition_id, new_key_cnt, slice_cnt
        size += (2 * sizeof(uint64_t) + sizeof(int32_t) + 2 * sizeof(uint16_t));
        for (const TxKey::Uptr &new_key : range_info_->new_key_)
        {
            size += new_key->SerializedLength();
        }
        size += (sizeof(int32_t) * range_info_->new_partition_id_.size());
        if (range_slices_)
        {
            size += (sizeof(uint32_t) * range_slices_->size());
            for (auto slice_it = range_slices_->cbegin() + 1;
                 slice_it != range_slices_->cend();
                 slice_it++)
            {
                size += slice_it->first->SerializedLength();
            }
        }
        return size;
    }

    TxRecord::Uptr Clone() const override
    {
        return std::make_unique<RangeRecord>(*this);
    }

    void Copy(const TxRecord &rhs) override
    {
        const RangeRecord &that = static_cast<const RangeRecord &>(rhs);
        is_info_owner_ = false;
        range_info_ = that.GetRangeInfo();
        range_slices_ = that.range_slices_;
        end_key_ = that.end_key_;
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
        range_info_ = rhs.range_info_;
        range_slices_ = rhs.range_slices_;
        end_key_ = rhs.end_key_;
        return *this;
    }

    const RangeInfo *GetRangeInfo() const
    {
        return is_info_owner_ ? range_info_uptr_.get() : range_info_;
    }

    void SetRangeInfo(std::unique_ptr<RangeInfo> &&range_info)
    {
        if (!is_info_owner_)
        {
            range_info_ = nullptr;
            is_info_owner_ = true;
        }
        range_info_uptr_ = std::move(range_info);
    }

    void SetRangeInfo(const RangeInfo *range_info)
    {
        if (is_info_owner_)
        {
            range_info_uptr_.reset();
            is_info_owner_ = false;
        }
        range_info_ = range_info;
    }

    size_t Size() const override
    {
        return 8 + 8 + 8 + 1;
    }

    size_t MemUsage() const override
    {
        size_t mem_usage = sizeof(RangeRecord);
        if (is_info_owner_)
        {
            mem_usage += range_info_uptr_->MemUsage();
        }
        return mem_usage;
    }

    // Usually range_info_ is a raw pointer that points to range info in
    // TableRangeEntry stored in local cc shards. But when it is used in
    // post write all to pass the value to cc request, it will be the owner
    // of a temp range info object.
    union
    {
        const RangeInfo *range_info_;
        std::unique_ptr<RangeInfo> range_info_uptr_;
    };
    bool is_info_owner_{false};
    // Only used in range split tx to broadcast slice
    // info to all nodes.
    const std::vector<std::pair<TxKey::Uptr, size_t>> *range_slices_{nullptr};
    /**
     * @brief The exclusive end of the range, which is also the start of the
     * next range. Null, if this is the last range and end key points to
     * positive infinity.
     *
     */
    const TxKey *end_key_{nullptr};
};
}  // namespace txservice
