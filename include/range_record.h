#pragma once

#include <atomic>
#include <cassert>
#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

#include "cc_req_misc.h"
#include "range_bucket_key_record.h"
#include "range_slice.h"
#include "sharder.h"
#include "tx_key.h"
#include "tx_record.h"
#include "tx_serialize.h"

namespace txservice
{
struct DataSyncTask;
struct FetchRangeSlicesReq;
// struct that stores range related info that we read from
// KV storage during table range initialization.
struct InitRangeEntry
{
    InitRangeEntry() : key_(nullptr), partition_id_(-1), version_ts_(0)
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

struct RangeInfo
{
    RangeInfo() = delete;
    RangeInfo(std::unique_ptr<TxKey> start_key,
              const TxKey *end_key,
              uint64_t version_ts,
              uint32_t partition_id,
              bool is_dirty = false)
        : start_key_(std::move(start_key)),
          end_key_(end_key),
          partition_id_(partition_id),
          version_ts_(version_ts),
          dirty_ts_(0),
          is_dirty_(is_dirty)
    {
    }

    RangeInfo(const RangeInfo &other)
        : end_key_(other.end_key_),
          partition_id_(other.partition_id_),
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

    void Clear()
    {
        start_key_ = nullptr;
        end_key_ = nullptr;
        partition_id_ = 0;
        version_ts_ = 1;
        new_key_.clear();
        new_partition_id_.clear();
        dirty_ts_ = 0;
        is_dirty_ = false;
    }

    RangeInfo &operator=(const RangeInfo &other)
    {
        if (this != &other)
        {
            end_key_ = other.end_key_;
            partition_id_ = other.partition_id_;
            version_ts_ = other.version_ts_;
            new_partition_id_ = other.new_partition_id_;
            dirty_ts_ = other.dirty_ts_;
            is_dirty_ = other.is_dirty_;

            if (!other.start_key_)
            {
                start_key_ = nullptr;
            }
            else
            {
                start_key_ = other.start_key_->Clone();
            }

            new_key_.clear();
            for (const auto &key : other.new_key_)
            {
                new_key_.push_back(key->Clone());
            }

            assert(new_partition_id_.size() == new_key_.size());
        }
        return *this;
    }

    std::unique_ptr<RangeInfo> Clone() const
    {
        std::unique_ptr<TxKey> start_key_clone =
            start_key_ == nullptr ? nullptr : start_key_->Clone();
        RangeInfo *that = new RangeInfo(
            std::move(start_key_clone), end_key_, version_ts_, partition_id_);
        for (auto &key : new_key_)
        {
            that->new_key_.push_back(key->Clone());
        }
        that->new_partition_id_ = new_partition_id_;
        that->is_dirty_ = is_dirty_;
        assert(that->new_partition_id_.size() == that->new_key_.size());
        return std::unique_ptr<RangeInfo>(that);
    }

    void SetDirty(const std::vector<std::unique_ptr<TxKey>> &new_key,
                  const std::vector<int32_t> &new_partition_id,
                  uint64_t dirty_ts)
    {
        if (dirty_ts >= version_ts_ && dirty_ts >= dirty_ts_)
        {
            new_key_.clear();
            for (auto &key_uptr : new_key)
            {
                new_key_.push_back(key_uptr->Clone());
            }
            new_partition_id_ = new_partition_id;
            assert(new_key_.size() == new_partition_id_.size());
            dirty_ts_ = dirty_ts;
            is_dirty_ = true;
        }
    }

    void SetDirty(std::vector<std::unique_ptr<TxKey>> &&new_key,
                  std::vector<int32_t> &&new_partition_id,
                  uint64_t dirty_ts)
    {
        if (dirty_ts >= version_ts_ && dirty_ts >= dirty_ts_)
        {
            new_key_ = std::move(new_key);
            new_partition_id_ = std::move(new_partition_id);
            assert(new_key_.size() == new_partition_id_.size());
            dirty_ts_ = dirty_ts;
            is_dirty_ = true;
        }
    }

    void CommitDirty()
    {
        if (dirty_ts_ >= version_ts_)
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
        if (commit_ts != 0 && commit_ts > version_ts_)
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

    const TxKey *EndKey() const
    {
        return end_key_;
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

    const std::vector<int32_t> &NewPartitionIdUncheckDirty() const
    {
        return new_partition_id_;
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
    const TxKey *end_key_;
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
    friend struct TableRangeEntry;
    friend struct SplitFlushRangeOp;
};

struct TableRangeEntry
{
public:
    TableRangeEntry() = default;
    TableRangeEntry(const TableRangeEntry &) = delete;
    TableRangeEntry &operator=(const TableRangeEntry &) = delete;

    TableRangeEntry(std::unique_ptr<TxKey> start_key,
                    const TxKey *end_key,
                    uint64_t version_ts,
                    int64_t partition_id,
                    std::unique_ptr<StoreRange> slices = nullptr)
        : range_info_(std::make_unique<RangeInfo>(
              std::move(start_key), end_key, version_ts, partition_id)),
          mux_(),
          range_slices_(std::move(slices)),
          fetch_range_slices_req_(nullptr),
          sync_info_(nullptr)
    {
    }

    ~TableRangeEntry();

    int64_t UpdateRangeEntry(uint64_t version_ts,
                             const TxKey *end_key,
                             std::unique_ptr<StoreRange> slices)
    {
        range_info_->version_ts_ = version_ts;
        range_info_->end_key_ = end_key;
        std::lock_guard<std::shared_mutex> lk(mux_);
        int64_t orig_size = range_slices_ ? range_slices_->MemUsage() : 0;
        int64_t new_size = slices ? slices->MemUsage() : 0;
        range_slices_ = std::move(slices);
        return new_size - orig_size;
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

    StoreRange *PinStoreRange()
    {
        std::shared_lock<std::shared_mutex> lk(mux_);
        if (range_slices_)
        {
            range_slices_->pins_.fetch_add(1, std::memory_order_release);
            return range_slices_.get();
        }
        return nullptr;
    }

    void UnPinStoreRange()
    {
        std::shared_lock<std::shared_mutex> lk(mux_);
        if (range_slices_)
        {
            range_slices_->pins_.fetch_sub(1, std::memory_order_release);
        }
    }

    bool KickoutKeyInSlice(const TxKey &key)
    {
        std::shared_lock<std::shared_mutex> lk(mux_);
        if (range_slices_)
        {
            return range_slices_->KickoutSlice(key);
        }
        return true;
    }

    bool DropStoreRangeAndSyncInfo(size_t &mem_decreased);

    void SetRangeEndKey(const TxKey *end_key)
    {
        range_info_->end_key_ = end_key;
        if (range_slices_)
        {
            range_slices_->SetRangeEndKey(end_key);
        }
    }

    void SetVersion(uint64_t version)
    {
        range_info_->version_ts_ = version;
    }

    int64_t InitRangeSlices(
        std::vector<std::pair<TxKey::Uptr, uint32_t>> &&slices,
        NodeGroupId ng_id,
        bool fully_cached = false);

    size_t DropStoreRange()
    {
        // We need to make sure that there's no one accesing StoreRange before
        // dropping store range.
        std::unique_lock<std::shared_mutex> lk(mux_);
        size_t mem_decreased = 0;
        if (range_slices_ && range_slices_->Pins() == 0)
        {
            mem_decreased += range_slices_->MemUsage();
            range_slices_ = nullptr;
        }
        return mem_decreased;
    }

    uint64_t GetLastSyncTs()
    {
        std::shared_lock<std::shared_mutex> lk(mux_);
        if (sync_info_)
        {
            return sync_info_->last_sync_ts_;
        }
        else
        {
            return 0;
        }
    }

    bool TrySetDataSync(bool ongoing,
                        std::shared_ptr<DataSyncTask> task = nullptr,
                        uint64_t last_sync_ts = 0)
    {
        std::unique_lock<std::shared_mutex> lk(mux_);
        if (!sync_info_)
        {
            // Only initialize sync_info_ when it is needed.
            // If we're setting ongoing to false that means
            // sync_info_ is deleted when data sync worker tries
            // to sync this range, which means either term has
            // changed or range is migrated away.
            if (!ongoing)
            {
                return true;
            }
            sync_info_ = std::make_unique<RangeSyncInfo>();
        }
        if (ongoing && sync_info_->sync_ongoing_)
        {
            // Another task is processing this range.
            // To avoid the possible busy loop when there are fewer tasks, put
            // this task into `pending_task` instead of put back into
            // `data_sync_task_queue_`.
            sync_info_->pending_sync_task_.push(task);
            return false;
        }
        if (!ongoing && last_sync_ts > sync_info_->last_sync_ts_)
        {
            // data sync succeeded, update last sync ts
            sync_info_->last_sync_ts_ = last_sync_ts;
        }
        sync_info_->sync_ongoing_ = ongoing;
        return true;
    }

    void PopPendingSyncTask();

    void PushPendingSyncTask(std::shared_ptr<DataSyncTask> task)
    {
        std::unique_lock<std::shared_mutex> lk(mux_);
        if (!sync_info_)
        {
            sync_info_ = std::make_unique<RangeSyncInfo>();
        }
        sync_info_->pending_sync_task_.emplace(task);
    }

    void FetchRangeSlices(const TableName &range_tbl_name,
                          CcRequestBase *requester,
                          NodeGroupId ng_id,
                          int64_t ng_term,
                          CcShard *cc_shard);

private:
    StoreRange *RangeSlices()
    {
        return range_slices_.get();
    }

    const StoreRange *RangeSlices() const
    {
        return range_slices_.get();
    }
    struct RangeSyncInfo
    {
        bool sync_ongoing_{false};
        uint64_t last_sync_ts_{0};
        // Multiple tasks on the same range are executed sequentially, so the
        // subsequence tasks for this range should wait here.
        std::queue<std::shared_ptr<DataSyncTask>> pending_sync_task_;
    };
    std::unique_ptr<RangeInfo> range_info_{nullptr};

    // Protects range_slices_, fetch_range_slices_cc_ and sync_info_
    // Any update on these pointers requres unique lock on mux. But updating
    // the object that these pointers point to only requires shared lock.
    std::shared_mutex mux_;

    // range_slices_ stores the slice info in this range. This is only
    // initialized on the node group that owns this range, and it is initialized
    // lazily when needed. range_slices_ is only safe to accessed in the
    // following cases:
    // 1. StoreRange is pinned.
    // 2. mutex lock is acquried on TableRangeEntry.mux_.
    std::unique_ptr<StoreRange> range_slices_{nullptr};
    std::unique_ptr<FetchRangeSlicesReq> fetch_range_slices_req_{nullptr};
    std::unique_ptr<RangeSyncInfo> sync_info_{nullptr};

    template <typename KeyT>
    friend class RangeCcMap;
    friend class LocalCcShards;
    friend struct FetchRangeSlicesReq;
};
struct RangeRecord : public TxRecord
{
public:
    RangeRecord()
        : range_info_{nullptr},
          is_info_owner_(false),
          range_slices_(nullptr),
          range_owner_rec_(nullptr),
          new_range_owner_rec_(nullptr),
          is_read_result_(false)
    {
    }

    RangeRecord(const RangeRecord &rhs)
        : range_info_(nullptr),
          is_info_owner_(rhs.is_info_owner_),
          range_slices_(rhs.range_slices_),
          is_read_result_(rhs.is_read_result_)
    {
        if (rhs.is_info_owner_)
        {
            assert(range_info_uptr_ == nullptr);
            range_info_uptr_ = rhs.range_info_uptr_->Clone();
        }
        else
        {
            range_info_ = rhs.range_info_;
        }
        if (rhs.is_read_result_)
        {
            range_owner_bucket_ = rhs.range_owner_bucket_;
            if (rhs.new_range_owner_bucket_)
            {
                new_range_owner_bucket_ =
                    std::make_unique<std::vector<const BucketInfo *>>();
                for (auto &info : *rhs.new_range_owner_bucket_)
                {
                    new_range_owner_bucket_->push_back(info);
                }
            }
            else
            {
                new_range_owner_bucket_ = nullptr;
            }
        }
        else
        {
            // Copy new_range_owner_rec_
            range_owner_rec_ = rhs.range_owner_rec_;
            if (rhs.new_range_owner_rec_)
            {
                new_range_owner_rec_ =
                    std::make_unique<std::vector<LruEntry *>>();
                for (auto &entry : *rhs.new_range_owner_rec_)
                {
                    new_range_owner_rec_->push_back(entry);
                }
            }
            else
            {
                new_range_owner_rec_ = nullptr;
            }
        }
    }

    RangeRecord(const RangeInfo *info,
                const std::vector<std::pair<TxKey::Uptr, size_t>> *slices,
                LruEntry *range_owner)
        : range_info_(info),
          is_info_owner_(false),
          range_slices_(slices),
          range_owner_rec_(range_owner),
          new_range_owner_rec_(nullptr),
          is_read_result_(false)
    {
    }

    RangeRecord(std::unique_ptr<RangeInfo> info,
                const std::vector<std::pair<TxKey::Uptr, size_t>> *slices,
                LruEntry *range_owner)
        : range_info_uptr_(std::move(info)),
          is_info_owner_(true),
          range_slices_(slices),
          range_owner_rec_(range_owner),
          new_range_owner_rec_(nullptr),
          is_read_result_(false)
    {
    }

    ~RangeRecord()
    {
        if (is_info_owner_)
        {
            range_info_uptr_.reset();
        }
        if (is_read_result_ && new_range_owner_bucket_)
        {
            new_range_owner_bucket_.reset();
        }
        else if (!is_read_result_ && new_range_owner_rec_)
        {
            new_range_owner_rec_.reset();
        }
    }

    void Serialize(std::vector<char> &buf, size_t &offset) const override
    {
        assert(false);
    }

    void Serialize(std::string &str) const override
    {
        assert(!is_read_result_);
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
        is_normal = range_info_->end_key_ != nullptr &&
                    range_info_->end_key_->Type() == KeyType::Normal;
        SerializeToStr(&is_normal, str);
        if (is_normal)
        {
            range_info_->end_key_->Serialize(str);
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
        assert(!is_read_result_);
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
        auto &rhs_range_record = static_cast<const RangeRecord &>(rhs);
        *this = rhs_range_record;
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

        // Release RangeInfo ownership
        if (is_info_owner_)
        {
            range_info_uptr_.reset();
            is_info_owner_ = false;
        }
        else
        {
            range_info_ = nullptr;
        }

        assert(is_info_owner_ == false);

        if (rhs.is_info_owner_)
        {
            range_info_uptr_ = rhs.range_info_uptr_->Clone();
        }
        else
        {
            range_info_ = rhs.range_info_;
        }

        is_info_owner_ = rhs.is_info_owner_;

        range_slices_ = rhs.range_slices_;

        // Free own unique ptr.
        if (!is_read_result_ && new_range_owner_rec_)
        {
            new_range_owner_rec_.reset();
        }
        else if (is_read_result_ && new_range_owner_bucket_)
        {
            new_range_owner_bucket_.reset();
        }
        is_read_result_ = rhs.is_read_result_;

        if (rhs.is_read_result_)
        {
            range_owner_bucket_ = rhs.range_owner_bucket_;
            if (rhs.new_range_owner_bucket_)
            {
                new_range_owner_bucket_ =
                    std::make_unique<std::vector<const BucketInfo *>>();
                for (auto &bucket : *rhs.new_range_owner_bucket_)
                {
                    new_range_owner_bucket_->push_back(bucket);
                }
            }
            else
            {
                new_range_owner_bucket_ = nullptr;
            }
        }
        else
        {
            range_owner_rec_ = rhs.range_owner_rec_;
            if (rhs.new_range_owner_rec_)
            {
                new_range_owner_rec_ =
                    std::make_unique<std::vector<LruEntry *>>();
                for (auto &entry : *rhs.new_range_owner_rec_)
                {
                    new_range_owner_rec_->push_back(entry);
                }
            }
            else
            {
                new_range_owner_rec_ = nullptr;
            }
        }
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

    void CopyForReadResult(const RangeRecord &other)
    {
        // Release RangeInfo ownership
        if (is_info_owner_)
        {
            range_info_uptr_.reset();
            is_info_owner_ = false;
        }

        assert(!other.is_info_owner_);
        range_info_ = other.range_info_;
        is_info_owner_ = other.is_info_owner_;

        range_slices_ = other.range_slices_;

        // Free own unique ptr.
        if (is_read_result_ && new_range_owner_bucket_)
        {
            new_range_owner_bucket_.reset();
        }
        else if (!is_read_result_ && new_range_owner_rec_)
        {
            new_range_owner_rec_.reset();
        }
        assert(!other.is_read_result_);
        is_read_result_ = true;

        range_owner_bucket_ =
            static_cast<const CcEntry<RangeBucketKey, RangeBucketRecord> *>(
                other.range_owner_rec_)
                ->payload_->GetBucketInfo();

        if (other.new_range_owner_rec_)
        {
            new_range_owner_bucket_ =
                std::make_unique<std::vector<const BucketInfo *>>();
            for (auto &entry : *other.new_range_owner_rec_)
            {
                new_range_owner_bucket_->push_back(
                    static_cast<const CcEntry<RangeBucketKey, RangeBucketRecord>
                                    *>(entry)
                        ->payload_->GetBucketInfo());
            }
        }
        else
        {
            new_range_owner_bucket_ = nullptr;
        }
    }

    /**
     * @brief Get range owner node group. Should only be called if
     * this record is from ReadKeyResult returned by ReadCc.
     */
    const BucketInfo *GetRangeOwnerNg() const
    {
        assert(is_read_result_);
        return range_owner_bucket_;
    }

    /**
     * @brief Get splitting range owner node group. Should only be called if
     * this record is from ReadKeyResult returned by ReadCc.
     */
    const std::vector<const BucketInfo *> *GetNewRangeOwnerNgs() const
    {
        assert(is_read_result_);
        return new_range_owner_bucket_.get();
    }

    void SetNewRangeOwnerRec(
        std::unique_ptr<std::vector<LruEntry *>> new_range_rec)
    {
        if (new_range_owner_rec_)
        {
            new_range_owner_rec_.reset();
        }
        new_range_owner_rec_ = std::move(new_range_rec);
    }

    size_t Size() const override
    {
        return 5 * 8 + 2;
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
     * @brief The bucket record that owns this range.
     */
    union
    {
        // We use range_owner_rec_ to store pointer to range_bucket_ccm in
        // range cc map cc entries. But the range bucket cc entry should not
        // be exposed when we copy range record into ReadKeyResult. In that case
        // we will only copy the BucketInfo pointer owned by local cc shards.
        LruEntry *range_owner_rec_{nullptr};
        const BucketInfo *range_owner_bucket_;
    };

    /**
     * @brief The bucket record for new splitted ranges. This is only used
     * during range split, and reset back to nullptr once range split is done.
     */
    union
    {
        std::unique_ptr<std::vector<LruEntry *>> new_range_owner_rec_{nullptr};
        std::unique_ptr<std::vector<const BucketInfo *>>
            new_range_owner_bucket_;
    };

    // If this is record copied into ReadKeyResult during read result.
    bool is_read_result_{false};
};
}  // namespace txservice
