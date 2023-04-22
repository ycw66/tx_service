#pragma once

#include <condition_variable>
#include <deque>
#include <shared_mutex>
#include <unordered_set>
#include <vector>

#include "catalog_factory.h"
#include "cc_req_base.h"
#include "tx_key.h"
#include "type.h"

namespace txservice
{
class CcShard;
class CcMap;
class LocalCcShards;
struct FillStoreSliceCc;
class StoreRange;

namespace store
{
class DataStoreHandler;
}

enum struct SliceStatus
{
    /**
     * @brief The slice's data are not fully cached in memory.
     *
     */
    PartiallyCached = 0,
    /**
     * @brief The slice's data are fully cached in memory.
     *
     */
    FullyCached,
    /**
     * @brief The slice's data are being loaded into memory. The flag prevents
     * cache replacement from kicking out records from the slice while a cc
     * request is loading the slice's records into cc maps.
     *
     */
    BeingLoaded
};

enum struct RangeSliceOpStatus
{
    /**
     * The slice is fully cached in memory and has been pinned.
     */
    Successful = 0,
    /**
     * @brief The demanding cc request either performs an async data store read
     * or awaits for an on-the-fly data store read. The cc request will be
     * re-enqueued for execution once the async read finishes and fills the
     * slice into memory (though filling may fail due to out of memory).
     *
     */
    BlockedOnLoad,
    /**
     * @brief The request is temporarily pushed back due to concurrent
     * modifications of the slice. The request is re-enqueued and re-tries
     * immediately.
     *
     */
    Retry,
    /**
     * @brief The cc request demands a slice that was loaded shortly (less than
     * 4 seconds). Given that the demanding slice becomes partially cached in
     * such a short period, the cache is likely experiencing thrashing. To
     * mitigate the issue, loading is not performed and the request is pushed
     * back for retry. If the range in which the slice resides is experiencing
     * splitting, the request is aborted on the OOM error.
     *
     */
    Delay,
    Error,
};

struct SliceChangeInfo
{
    SliceChangeInfo(const SliceChangeInfo &rhs) = delete;
    SliceChangeInfo &operator=(const SliceChangeInfo &rhs) = delete;
    SliceChangeInfo(const TxKey *start_key,
                    uint32_t cur_slice_size,
                    uint32_t post_update_slice_size)
        : is_key_owner_(false),
          cur_slice_size_(cur_slice_size),
          post_update_slice_size_(post_update_slice_size)
    {
        key_.ptr_ = std::move(start_key);
    }
    SliceChangeInfo(TxKey::Uptr start_key,
                    uint32_t cur_slice_size,
                    uint32_t post_update_slice_size)
        : is_key_owner_(true),
          cur_slice_size_(cur_slice_size),
          post_update_slice_size_(post_update_slice_size)
    {
        key_.uptr_ = std::move(start_key);
    }

    SliceChangeInfo &operator=(SliceChangeInfo &&rhs)
    {
        if (this == &rhs)
        {
            return *this;
        }

        if (rhs.is_key_owner_)
        {
            SetKey(std::move(rhs.key_.uptr_));
            rhs.is_key_owner_ = false;
        }
        else
        {
            SetKey(rhs.key_.ptr_);
        }

        cur_slice_size_ = rhs.cur_slice_size_;
        post_update_slice_size_ = rhs.post_update_slice_size_;
        return *this;
    }

    SliceChangeInfo(SliceChangeInfo &&rhs)
    {
        if (rhs.is_key_owner_)
        {
            SetKey(std::move(rhs.key_.uptr_));
            rhs.is_key_owner_ = false;
        }
        else
        {
            SetKey(rhs.key_.ptr_);
        }
        cur_slice_size_ = rhs.cur_slice_size_;
        post_update_slice_size_ = rhs.post_update_slice_size_;
    }

    void SetKey(const TxKey *ptr)
    {
        if (is_key_owner_)
        {
            key_.uptr_.reset();
        }
        key_.ptr_ = ptr;
        is_key_owner_ = false;
    }

    void SetKey(std::unique_ptr<TxKey> uptr)
    {
        if (is_key_owner_)
        {
            // The move op will de-allocate the old record and obtain the
            // ownership of the input record.
            key_.uptr_ = std::move(uptr);
        }
        else
        {
            // key_ is treated as a unique_ptr, first release ownership,
            // otherwise ptr_ will be deleted
            key_.uptr_.release();
            key_.uptr_ = std::move(uptr);
        }
        is_key_owner_ = true;
    }

    const TxKey *SliceStartKey() const
    {
        return is_key_owner_ ? key_.uptr_.get() : key_.ptr_;
    }

    ~SliceChangeInfo()
    {
        if (is_key_owner_)
        {
            key_.uptr_.reset();
        }
    }

    union KeyPtr
    {
        const TxKey *ptr_;
        std::unique_ptr<TxKey> uptr_;
        ~KeyPtr()
        {
        }
    };
    KeyPtr key_{nullptr};
    bool is_key_owner_{false};
    uint32_t cur_slice_size_{0};
    uint32_t post_update_slice_size_{0};
};

class StoreSlice
{
public:
    /**
     * @brief The size of a slice.
     *
     */
    static constexpr uint32_t slice_upper_bound = 16 * 1024;

    StoreSlice() = default;
    ~StoreSlice();

    StoreSlice(const StoreSlice &) = delete;

    void StartLoading(FillStoreSliceCc *fill_req, LocalCcShards &cc_shards);
    void CommitLoading(uint32_t slice_size);

    const TxKey *StartKey() const
    {
        return start_key_;
    }

    const TxKey *EndKey() const
    {
        return end_key_;
    }

    FillStoreSliceCc *FillCcRequest();

    void SetLoadingError(StoreRange &range);

    bool NeedSplitOrMerge() const
    {
        return size_ > StoreSlice::slice_upper_bound ||
               size_ < (StoreSlice::slice_upper_bound >> 2);
    }

    /**
     * @brief Marks the slice to be incomplete in memory. The method is called
     * when a cached key falling in the slice is to be kicked out from memory.
     *
     * @return true, if the slice is allowed to be kicked out and the slice
     * status is marked as PartiallyCached.
     * @return false, if the slice is pinned or being loaded, and thus the
     * caller cannot kickout any keys in the slice.
     */
    bool Kickout()
    {
        std::unique_lock<std::mutex> lk(slice_mux_);
        if (pins_ > 0 || status_ == SliceStatus::BeingLoaded)
        {
            return false;
        }
        else
        {
            status_ = SliceStatus::PartiallyCached;
            return true;
        }
    }

    uint32_t Size() const
    {
        return size_;
    }

    void UpdateSize(int32_t slice_size)
    {
        size_ = slice_size;
    }

    bool UpdateSize()
    {
        if (post_ckpt_size_ >= 0)
        {
            size_ = post_ckpt_size_;
            post_ckpt_size_ = -1;
            return true;
        }
        return false;
    }

    int32_t PostCkptSize() const
    {
        return post_ckpt_size_;
    }

    void SetPostCkptSize(int32_t size)
    {
        post_ckpt_size_ = size;
    }

    uint16_t PinCount()
    {
        std::unique_lock<std::mutex> lk(slice_mux_);
        return pins_;
    }

    bool ChangeAllowed()
    {
        std::unique_lock<std::mutex> lk(slice_mux_);
        return pins_ == 0 && status_ != SliceStatus::BeingLoaded;
    }

private:
    bool IsRecentLoad() const;

    const TxKey *start_key_{nullptr};
    const TxKey *end_key_{nullptr};

    uint32_t size_{0};
    int32_t post_ckpt_size_{-1};

    SliceStatus status_{SliceStatus::PartiallyCached};

    /**
     * @brief The number of times a range slice has been pinned by online
     * tx's. A pin is put by a tx when it intends to read/scan the slice and
     * prevents the cache replacement algorithm from kicking out any keys in
     * the slice.
     *
     */
    uint16_t pins_{0};
    bool to_alter_{false};

    std::unique_ptr<FillStoreSliceCc> fetch_slice_cc_{nullptr};

    /**
     * @brief A queue of cc requests waiting for the slice to be loaded into
     * memory.
     *
     */
    std::vector<std::pair<CcRequestBase *, CcShard *>> cc_queue_;

    /**
     * @brief The timestamp when the slice was last loaded into memory. The last
     * load may fail, due to failures in the data store. When failures happen,
     * the timestamp is still updated and the slice's status is set to an error
     * state.
     *
     */
    uint64_t last_load_ts_{1};

    std::mutex slice_mux_;

    friend class StoreRange;
};

struct RangeSliceId;

class StoreRange
{
public:
    /**
     * @brief Max number of slices in range. The total size of a range
     * is (8*1024) * (16*1024) = 128MB
     *
     */
#ifdef SMALL_RANGE
    static constexpr uint32_t range_max_size =
        1024 * 1024;  // 1MB range size for testing range split
#else
    static constexpr uint32_t range_max_size = 134217728;
#endif

    StoreRange(const TxKey *start_key,
               const TxKey *end_key,
               uint32_t partition_id,
               LocalCcShards &cc_shards);

    StoreRange(const StoreRange &) = delete;

    ~StoreRange() = default;

    RangeSliceId PinSlice(const TableName &tbl_name,
                          const TxKey &search_key,
                          bool inclusive,
                          const Schema *key_schema,
                          const Schema *rec_schema,
                          uint64_t schema_ts,
                          uint64_t snapshot_ts,
                          const KVCatalogInfo *kv_info,
                          CcRequestBase *cc_request,
                          CcShard *cc_shard,
                          store::DataStoreHandler *store_hd,
                          RangeSliceOpStatus &pin_status,
                          bool force_load = false);

    RangeSliceOpStatus PinSlice(const TableName &tbl_name,
                                StoreSlice *slice,
                                const Schema *key_schema,
                                const Schema *rec_schema,
                                uint64_t schema_ts,
                                uint64_t snapshot_ts,
                                const KVCatalogInfo *kv_info,
                                CcRequestBase *cc_request,
                                CcShard *cc_shard,
                                store::DataStoreHandler *store_hd,
                                bool force_load = false);

    void UnpinSlice(StoreSlice *slice);

    void UpdateRange(const TxKey *start_key,
                     const TxKey *end_key,
                     int32_t partition_id);

    bool UpdateRangeSlicesInStore(const TableName &table_name,
                                  uint64_t schema_ts,
                                  bool update_slice_keys,
                                  store::DataStoreHandler *store_hd);

    bool UpdateSliceSpec(StoreSlice *slice,
                         const TableName &table_name,
                         NodeGroupId ng_id,
                         uint64_t flush_ts,
                         const std::vector<FlushRecord> &flush_vec,
                         size_t start_idx,
                         size_t end_idx,
                         bool range_locked = false);

    /**
     * This function is NOT THREAD SAFE. Only checkpointer should be calling
     * this function and update range specs.
     */
    std::vector<const TxKey *> CalculateRangeSplitKeys(
        const TableName &,
        NodeGroupId ng_id,
        uint64_t flush_ts,
        size_t post_ckpt_size,
        std::vector<FlushRecord>::const_iterator range_start_it,
        std::vector<FlushRecord>::const_iterator range_end_it,
        const std::vector<FlushRecord> &flush_vec);

    const TxKey *RangeStartKey() const
    {
        return range_start_key_;
    }

    const TxKey *RangeEndKey() const
    {
        return range_end_key_;
    }

    bool KickoutSlice(const TxKey &kickout_key);

    uint32_t PartitionId() const
    {
        return partition_id_;
    }

    StoreSlice *FindSlice(const TxKey &key);

    StoreSlice *FindSlice(size_t idx)
    {
        return slices_.at(idx).get();
    }

    void InitSlices(std::vector<std::pair<TxKey::Uptr, uint32_t>> &slice_keys,
                    bool fully_cached = false);

    void InitSlices(std::vector<std::tuple<TxKey::Uptr, uint32_t, SliceStatus>>
                        &slice_keys);

    const std::vector<std::unique_ptr<StoreSlice>> &Slices() const
    {
        return slices_;
    }

    size_t PostCkptSize();

    /**
     * @brief Split the range with new_end. new_end will be the new
     * end key of this range, and every slice after new_end will be removed
     * from this range and returned to the caller.
     *
     * @param new_end
     * @return std::vector<std::pair<TxKey::Uptr, uint32_t, SliceStatus>>
     */
    std::vector<std::tuple<TxKey::Uptr, uint32_t, SliceStatus>> SplitRange(
        const TxKey *new_end)
    {
        std::unique_lock<std::shared_mutex> range_lk(mux_);
        std::vector<std::tuple<TxKey::Uptr, uint32_t, SliceStatus>>
            removed_slices;
        size_t remove_offset = SearchSlice(*new_end, true);
        auto boundary = boundary_keys_.begin() + remove_offset - 1;
        auto slice = slices_.begin() + remove_offset;
        while (slice != slices_.end())
        {
            // first check if any of the slices that will be removed is pinned
            // or being loaded
            std::unique_lock<std::mutex> slice_lk((*slice)->slice_mux_);
            if ((*slice)->pins_ > 0 ||
                (*slice)->status_ == SliceStatus::BeingLoaded)
            {
                if ((*slice)->pins_)
                {
                    LOG(INFO) << "slice pinned when trying to split range";
                }
                else
                {
                    LOG(INFO) << "slice loading when tyring to split range";
                }
                return removed_slices;
            }
            slice++;
        }
        slice = slices_.begin() + remove_offset;
        while (boundary != boundary_keys_.end())
        {
            // Remove boundary keys >= new end key
            removed_slices.emplace_back(
                std::move(*boundary), (*slice)->Size(), (*slice)->status_);
            boundary++;
            slice++;
        }
        slices_.erase(slices_.begin() + remove_offset, slices_.end());
        boundary_keys_.erase(boundary_keys_.begin() + remove_offset - 1,
                             boundary_keys_.end());
        range_end_key_ = std::get<0>(removed_slices.front()).get();
        return removed_slices;
    }

    void Lock()
    {
        has_write_lock_.store(true, std::memory_order_relaxed);
    }

    bool HasLock()
    {
        return has_write_lock_.load(std::memory_order_relaxed);
    }

    void Unlock()
    {
        return has_write_lock_.store(false, std::memory_order_relaxed);
    }

private:
    static size_t LowerBound(
        const std::vector<std::unique_ptr<TxKey>> &slice_key_,
        const TxKey &search_key);

    size_t SearchSlice(const TxKey &search_key, bool inclusive) const;

    enum struct LoadSliceStatus
    {
        Success,
        Delay,
        Error
    };

    LoadSliceStatus LoadSlice(const TableName &tbl_name,
                              StoreSlice &slice,
                              const Schema *key_schema,
                              const Schema *rec_schema,
                              uint64_t schema_ts,
                              uint64_t snapshot_ts,
                              const KVCatalogInfo *kv_info,
                              CcRequestBase *cc_request,
                              CcShard *cc_shard,
                              store::DataStoreHandler *store_hd,
                              bool force_load,
                              std::unique_lock<std::mutex> &slice_lk);

    /**
     * @brief The start and end keys of the range. The two boundary keys are raw
     * pointers and point to external keys in the table's range entries at
     * LocalCcShards.
     *
     */
    const TxKey *range_start_key_, *range_end_key_;

    /**
     * @brief The partition ID of the range.
     *
     */
    uint32_t partition_id_;

    NodeGroupId cc_ng_id_;

    /**
     * @brief A sorted vector of keys that split the range into slices.
     *
     */
    std::vector<std::unique_ptr<TxKey>> boundary_keys_;

    std::vector<std::unique_ptr<StoreSlice>> slices_;

    /**
     * @brief A collection of slices the checkpointer intends to alter. The
     * checkpointer adds a slice to this collection, when it detects that the
     * slice's size in the data store exceeds the pre-defined threshold after
     * flushing changed data items in this slice. Slices in the collection
     * cannot be pinned by online tx's, until the checkpointer finishes
     * splitting or merging the slice. The checkpointer is single-threaded and
     * only splits one or merges two slices at a time. We use the vector rather
     * than the hash map, because for very small collections searching in the
     * vector is fast enough.
     *
     */

    std::shared_mutex mux_;

    std::condition_variable_any wait_cv_;

    LocalCcShards &local_cc_shards_;

    std::atomic<bool> has_write_lock_{false};

    friend class StoreSlice;
    friend struct TableRangeEntry;
};

/**
 * @brief A wrapper that wraps an internal pointer to a range slice. It is
 * passed to external users, e.g., tx state machines, who later use it to
 * re-access the slice, e.g., unpin the slice in memory.
 *
 */
struct RangeSliceId
{
public:
    RangeSliceId() : range_ptr_(nullptr), slice_ptr_(nullptr)
    {
    }

    RangeSliceId(StoreRange *range, StoreSlice *slice_ptr)
        : range_ptr_(range), slice_ptr_(slice_ptr)
    {
    }

    RangeSliceId(const RangeSliceId &rhs)
        : range_ptr_(rhs.range_ptr_), slice_ptr_(rhs.slice_ptr_)
    {
    }

    RangeSliceId &operator=(const RangeSliceId &rhs)
    {
        if (this == &rhs)
        {
            return *this;
        }

        range_ptr_ = rhs.range_ptr_;
        slice_ptr_ = rhs.slice_ptr_;

        return *this;
    }

    void Unpin()
    {
        range_ptr_->UnpinSlice(slice_ptr_);
    }

    StoreRange *Range()
    {
        return range_ptr_;
    }

    StoreSlice *Slice()
    {
        return slice_ptr_;
    }

    const StoreSlice *Slice() const
    {
        return slice_ptr_;
    }

    const TxKey *RangeStartKey() const
    {
        return range_ptr_->RangeStartKey();
    }

    const TxKey *RangeEndKey() const
    {
        return range_ptr_->RangeEndKey();
    }

    void Reset()
    {
        range_ptr_ = nullptr;
        slice_ptr_ = nullptr;
    }

private:
    StoreRange *range_ptr_;
    StoreSlice *slice_ptr_;

    friend class StoreRange;
};
}  // namespace txservice
