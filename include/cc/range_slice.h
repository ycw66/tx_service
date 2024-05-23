#pragma once

#include <atomic>
#include <cmath>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <vector>

#include "cc_req_misc.h"
#include "error_messages.h"
#include "range_slice_type.h"
#include "tx_key.h"
#include "type.h"

namespace txservice
{
class CcShard;
class CcMap;
struct FillStoreSliceCc;
class StoreRange;
class StoreSlice;
template <typename KeyT>
class TemplateStoreRange;
struct LoadRangeSliceRequest;
class LocalCcShards;
struct KVCatalogInfo;
struct CcRequestBase;
struct TableSchema;
struct FlushRecord;

namespace store
{
class DataStoreHandler;
}

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
    /**
     * @brief The node group is not the owner of the slice. This should only
     * happen if ng failover after a range split just finished  but before
     * checkpointer is able to truncate the log. In this case the log records of
     * the data that now falls on another ng will still be replayed on the old
     * ng on recover.
     */
    NotOwner,
    Error,
};

struct SliceChangeInfo
{
    SliceChangeInfo() = default;
    SliceChangeInfo(const SliceChangeInfo &rhs) = delete;
    SliceChangeInfo &operator=(const SliceChangeInfo &rhs) = delete;

    SliceChangeInfo(TxKey key, uint32_t cur_size, uint32_t post_update_size)
        : key_(std::move(key)),
          cur_size_(cur_size),
          post_update_size_(post_update_size)
    {
    }

    SliceChangeInfo &operator=(SliceChangeInfo &&rhs)
    {
        if (this == &rhs)
        {
            return *this;
        }

        key_ = std::move(rhs.key_);
        cur_size_ = rhs.cur_size_;
        post_update_size_ = rhs.post_update_size_;
        return *this;
    }

    SliceChangeInfo(SliceChangeInfo &&rhs)
        : key_(std::move(rhs.key_)),
          cur_size_(rhs.cur_size_),
          post_update_size_(rhs.post_update_size_)
    {
    }

    void SetKey(TxKey key)
    {
        key_ = std::move(key);
    }

    ~SliceChangeInfo() = default;

    TxKey key_;
    uint32_t cur_size_{0};
    uint32_t post_update_size_{0};
};

struct SplitRangeInfo
{
    SplitRangeInfo() = delete;

    SplitRangeInfo(TxKey start_key,
                   int32_t partition_id,
                   std::vector<const StoreSlice *> slices)
        : start_key_(std::move(start_key)),
          partition_id_(partition_id),
          slices_(std::move(slices))
    {
    }

    SplitRangeInfo(SplitRangeInfo &&rhs)
        : start_key_(std::move(rhs.start_key_)),
          partition_id_(rhs.partition_id_),
          slices_(std::move(rhs.slices_))
    {
    }

    SplitRangeInfo(const SplitRangeInfo &rhs)
    {
        start_key_ = rhs.start_key_.GetShallowCopy();
        partition_id_ = rhs.partition_id_;
        slices_ = rhs.slices_;
    }

    TxKey start_key_;
    int32_t partition_id_;
    std::vector<const StoreSlice *> slices_;
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

    explicit RangeSliceId(StoreRange *range, StoreSlice *slice)
        : range_ptr_(range), slice_ptr_(slice)
    {
    }

    RangeSliceId(const RangeSliceId &rhs)
        : range_ptr_(rhs.range_ptr_), slice_ptr_(rhs.slice_ptr_)
    {
    }

    RangeSliceId(RangeSliceId &&rhs)
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

    void Unpin();

    StoreRange *Range()
    {
        return range_ptr_;
    }

    const StoreRange *Range() const
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

    TxKey RangeStartTxKey() const;
    TxKey RangeEndTxKey() const;

    void Reset()
    {
        range_ptr_ = nullptr;
        slice_ptr_ = nullptr;
    }

    friend bool operator==(const RangeSliceId &lhs, const RangeSliceId &rhs)
    {
        if (&lhs != &rhs)
        {
            return lhs.range_ptr_ == rhs.range_ptr_ &&
                   lhs.slice_ptr_ == rhs.slice_ptr_;
        }
        else
        {
            return true;
        }
    }

    friend bool operator!=(const RangeSliceId &lhs, const RangeSliceId &rhs)
    {
        return !(lhs == rhs);
    }

private:
    StoreRange *range_ptr_;
    StoreSlice *slice_ptr_;

    friend class StoreRange;
};

class StoreSlice
{
public:
    using uptr = std::unique_ptr<StoreSlice>;

    /**
     * @brief The size of a slice.
     *
     */
    static constexpr uint32_t slice_upper_bound = 16 * 1024;

    StoreSlice(size_t size, SliceStatus status)
        : size_(size), status_(status), fetch_slice_cc_(nullptr)
    {
    }

    virtual ~StoreSlice();

    StoreSlice(const StoreSlice &) = delete;

    void StartLoading(FillStoreSliceCc *fill_req, LocalCcShards &cc_shards);
    void CommitLoading(StoreRange &range, uint32_t slice_size);

    virtual TxKey StartTxKey() const = 0;
    virtual TxKey EndTxKey() const = 0;

    FillStoreSliceCc *FillCcRequest();

    void SetLoadingError(StoreRange &range, CcErrorCode err_code);

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

    uint64_t Size() const
    {
        return size_;
    }

    void UpdateSize(uint64_t slice_size)
    {
        size_ = slice_size;
    }

    bool UpdateSize()
    {
        if (post_ckpt_size_ != UINT64_MAX)
        {
            size_ = post_ckpt_size_;
            post_ckpt_size_ = UINT64_MAX;
            return true;
        }
        return false;
    }

    uint64_t PostCkptSize() const
    {
        return post_ckpt_size_;
    }

    void SetPostCkptSize(uint64_t size)
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
        return pins_ == 1;
    }

    size_t MemUsage() const
    {
        // The start and end key pointers are stored in the templated subclass.
        return sizeof(StoreSlice) + 16;
    }

    void UpdateLastLoadTs(uint64_t load_ts)
    {
        last_load_ts_ = load_ts;
    }

protected:
    bool IsRecentLoad() const;

    uint64_t size_{0};
    // Use UINT64_MAX to indicate invalid post checkpoint slice size
    uint64_t post_ckpt_size_{UINT64_MAX};

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
    template <typename KeyT>
    friend class TemplateStoreRange;
};

template <typename KeyT>
class TemplateStoreSlice : public StoreSlice
{
public:
    TemplateStoreSlice(const KeyT *start,
                       const KeyT *end,
                       size_t size = 0,
                       SliceStatus status = SliceStatus::PartiallyCached)
        : StoreSlice(size, status), start_key_(start), end_key_(end)
    {
        assert(start != nullptr && end != nullptr);
    }

    TxKey StartTxKey() const override
    {
        return TxKey(start_key_);
    }

    TxKey EndTxKey() const override
    {
        return TxKey(end_key_);
    }

    const KeyT *StartKey() const
    {
        return start_key_;
    }

    const KeyT *EndKey() const
    {
        return end_key_;
    }

    void SetEndKey(const KeyT *end)
    {
        end_key_ = end;
    }

private:
    const KeyT *start_key_{nullptr};
    const KeyT *end_key_{nullptr};
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

    StoreRange(uint32_t partition_id,
               NodeGroupId range_owner,
               LocalCcShards &cc_shards);

    StoreRange(const StoreRange &) = delete;

    virtual ~StoreRange() = default;

    RangeSliceOpStatus PinSlice(const TableName &tbl_name,
                                int64_t ng_term,
                                StoreSlice *slice,
                                const Schema *key_schema,
                                const Schema *rec_schema,
                                uint64_t schema_ts,
                                uint64_t snapshot_ts,
                                const KVCatalogInfo *kv_info,
                                CcRequestBase *cc_request,
                                CcShard *cc_shard,
                                store::DataStoreHandler *store_hd,
                                bool force_load = false,
                                uint8_t prefetch_size = 0);

    void UnpinSlice(StoreSlice *slice, bool need_lock_range);

    void BatchUnpinSlices(StoreSlice *start_slice,
                          const StoreSlice *end_slice,
                          bool forward_dir);

    bool UpdateRangeSlicesInStore(const TableName &table_name,
                                  uint64_t schema_ts,
                                  bool update_slice_keys,
                                  store::DataStoreHandler *store_hd);

    bool UpdateSliceSpec(StoreSlice *slice,
                         const TableName &table_name,
                         const TableSchema *schema,
                         NodeGroupId ng_id,
                         int64_t ng_term,
                         uint64_t flush_ts,
                         const std::vector<FlushRecord> &flush_vec,
                         size_t start_idx,
                         size_t end_idx);

    /**
     * This function is NOT THREAD SAFE. Only checkpointer should be calling
     * this function and update range specs.
     */
    virtual std::vector<TxKey> CalculateRangeSplitKeys(
        const TableName &table_name,
        const TableSchema *schema,
        NodeGroupId ng_id,
        int64_t ng_term,
        uint64_t flush_ts,
        size_t post_ckpt_size) = 0;

    // Update slice size after data flush. If flush is successful,
    // update slice size to precalculated post ckpt size. Otherwise,
    // reset post ckpt size.
    virtual bool UpdateSliceSizeAfterFlush(bool flush_res) = 0;

    virtual TxKey RangeStartTxKey() const = 0;
    virtual TxKey RangeEndTxKey() const = 0;

    uint32_t PartitionId() const
    {
        return partition_id_;
    }

    virtual StoreSlice *FindSlice(const TxKey &key) = 0;
    virtual StoreSlice *FindSlice(size_t idx) = 0;

    virtual std::vector<const StoreSlice *> Slices() const = 0;

    virtual size_t PostCkptSize() = 0;

    uint32_t Pins()
    {
        return pins_.load(std::memory_order_acquire);
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

    // virtual void SetRangeEndTxKey(TxKey end_key) = 0;

    void UpdateLastAccessedTs(uint64_t ts)
    {
        last_accessed_ts_.store(ts, std::memory_order_relaxed);
    }

    uint64_t LastAccessedTs() const
    {
        return last_accessed_ts_.load(std::memory_order_relaxed);
    }

protected:
    enum struct LoadSliceStatus
    {
        Success,
        Delay,
        Retry,
        Error
    };

    LoadSliceStatus LoadSlice(const TableName &tbl_name,
                              int64_t cc_ng_term,
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

    virtual std::pair<size_t, size_t> SearchSlice(
        const StoreSlice *slice) const = 0;
    virtual StoreSlice *GetSlice(size_t slice_idx) const = 0;
    virtual void UpdateSlice(StoreSlice *slice,
                             std::vector<SliceChangeInfo> &split_info) = 0;

    void CollectCacheHit(CcShard &ccs);
    void CollectCacheMiss(CcShard &ccs);

    /**
     * @brief The partition ID of the range.
     *
     */
    uint32_t partition_id_;

    NodeGroupId cc_ng_id_;

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

    mutable std::shared_mutex mux_;

    std::condition_variable_any wait_cv_;

    LocalCcShards &local_cc_shards_;

    std::atomic<bool> has_write_lock_{false};

    std::atomic_uint64_t last_accessed_ts_;
    // pins_ will increase in these cases:
    // 1. Child store slice is pinned
    // 2. Child store slice is being loaded
    // 3. TableRangeEntry.PinStoreRange() is called.
    // This is the value we rely on to decide if a StoreRange can be
    // safely evicted from memory.
    std::atomic_uint32_t pins_{0};

    friend class StoreSlice;
    friend struct TableRangeEntry;
    friend struct RangeSliceId;
};

template <typename KeyT>
class TemplateStoreRange : public StoreRange
{
public:
    TemplateStoreRange(const KeyT *start_key,
                       const KeyT *end_key,
                       uint32_t partition_id,
                       NodeGroupId range_owner,
                       LocalCcShards &cc_shards)
        : StoreRange(partition_id, range_owner, cc_shards),
          range_start_key_(start_key),
          range_end_key_(end_key)
    {
        std::unique_ptr<TemplateStoreSlice<KeyT>> slice =
            std::make_unique<TemplateStoreSlice<KeyT>>(start_key, end_key);
        slices_.emplace_back(std::move(slice));
    }

    ~TemplateStoreRange() = default;

    void SetRangeEndKey(const KeyT *end_key)
    {
        range_end_key_ = end_key;
        assert(slices_.size() > 0);
        slices_.back()->SetEndKey(end_key);
    }

    TxKey RangeStartTxKey() const override
    {
        return TxKey(range_start_key_);
    }

    TxKey RangeEndTxKey() const override
    {
        return TxKey(range_end_key_);
    }

    const KeyT *RangeStartKey() const
    {
        return range_start_key_;
    }

    const KeyT *RangeEndKey() const
    {
        return range_end_key_;
    }

    void InitSlices(std::vector<SliceInitInfo> &&slice_keys)
    {
        slices_.clear();
        boundary_keys_.clear();

        const KeyT *slice_start = range_start_key_;
        const KeyT *slice_end = slice_keys.size() > 1
                                    ? slice_keys[1].key_.GetKey<KeyT>()
                                    : range_end_key_;
        // The start of the first slice of the first range points to
        // KeyT::NegativeInfinity().
        assert(slice_start != nullptr);
        // The end of the last slice of the last range points to positive
        // infinity.
        assert(slice_end != nullptr);
        size_t slice_size = slice_keys.size() > 0 ? slice_keys[0].size_ : 0;
        SliceStatus slice_status = slice_keys[0].status_;

        std::unique_ptr<TemplateStoreSlice<KeyT>> slice =
            std::make_unique<TemplateStoreSlice<KeyT>>(
                slice_start, slice_end, slice_size, slice_status);

        slices_.emplace_back(std::move(slice));

        for (size_t idx = 1; idx < slice_keys.size(); ++idx)
        {
            slice_start = slice_keys[idx].key_.GetKey<KeyT>();
            slice_end = idx == slice_keys.size() - 1
                            ? range_end_key_
                            : slice_keys[idx + 1].key_.GetKey<KeyT>();
            slice_size = slice_keys[idx].size_;
            slice_status = slice_keys[idx].status_;

            slice = std::make_unique<TemplateStoreSlice<KeyT>>(
                slice_start, slice_end, slice_size, slice_status);

            slices_.emplace_back(std::move(slice));

            assert(slice_keys[idx].key_.IsOwner());
            std::unique_ptr<KeyT> boundary_key =
                slice_keys[idx].key_.MoveKey<KeyT>();
            boundary_keys_.emplace_back(std::move(boundary_key));
        }

        assert(slices_.size() == boundary_keys_.size() + 1);
    }

    StoreSlice *FindSlice(const TxKey &key) override
    {
        std::shared_lock<std::shared_mutex> s_lk(mux_);
        const KeyT *typed_key = key.GetKey<KeyT>();
        size_t slice_idx = SearchSlice(*typed_key, true);
        return slices_[slice_idx].get();
    }

    StoreSlice *FindSlice(size_t idx) override
    {
        std::shared_lock<std::shared_mutex> s_lk(mux_);
        return slices_.at(idx).get();
    }

    std::vector<const StoreSlice *> Slices() const override
    {
        std::vector<const StoreSlice *> slice_vec;
        slice_vec.reserve(slices_.size());

        for (const auto &slice : slices_)
        {
            slice_vec.emplace_back(slice.get());
        }

        return slice_vec;
    }

    const std::vector<std::unique_ptr<TemplateStoreSlice<KeyT>>> &TypedSlices()
        const
    {
        return slices_;
    }

    /**
     * @brief Split the range with new_end. new_end will be the new
     * end key of this range, and every slice after new_end will be removed
     * from this range and returned to the caller.
     *
     * @param new_end
     * @return std::vector<std::pair<TxKey::Uptr, uint32_t, SliceStatus>>
     */
    bool SplitRange(const KeyT *new_end,
                    std::vector<SliceInitInfo> &removed_slices)
    {
        std::unique_lock<std::shared_mutex> range_lk(mux_);
        size_t remove_offset = SearchSlice(*new_end, true);
        if (remove_offset == 0)
        {
            // All slices are smaller than new end
            return true;
        }
        auto boundary = boundary_keys_.begin() + remove_offset - 1;
        for (auto slice = slices_.begin(); slice != slices_.end(); slice++)
        {
            // first check if any of the slices in range is pinned. We should
            // not update StoreRange if it is used by any cc req. Normally this
            // should not happen since we've already acquired range write lock.
            // But if a node acuired range read lock then failed over, we might
            // have cc request accessing this range even if range split tx has
            // acquired range write lock. In this case, we should wait for them
            // to complete before continuing.
            std::unique_lock<std::mutex> slice_lk((*slice)->slice_mux_);
            if ((*slice)->pins_ > 0)
            {
                DLOG(INFO) << "slice pinned when trying to split range";
                return false;
            }
            else if ((*slice)->status_ == SliceStatus::BeingLoaded)
            {
                DLOG(INFO) << "slice filling into memory when trying to "
                              "split range";
                return false;
            }
            else if ((*slice)->FillCcRequest() != nullptr)
            {
                DLOG(INFO) << "slice loading from data store when trying to "
                              "split range";
                return false;
            }
        }
        auto slice = slices_.begin() + remove_offset;
        while (boundary != boundary_keys_.end())
        {
            // Remove boundary keys >= new end key
            removed_slices.emplace_back(TxKey(std::move(*boundary)),
                                        (*slice)->Size(),
                                        (*slice)->status_);
            boundary++;
            slice++;
        }
        slices_.erase(slices_.begin() + remove_offset, slices_.end());
        boundary_keys_.erase(boundary_keys_.begin() + remove_offset - 1,
                             boundary_keys_.end());
        assert(!removed_slices.empty());
        // We set the range's end key to nullptr for now. It will be updated
        // when new ranges are inserted into the table range table in
        // LocalCcShards, and set to the start of the first splitted range.
        range_end_key_ = nullptr;

        return true;
    }

    RangeSliceId PinSlices(const TableName &tbl_name,
                           int64_t ng_term,
                           const KeyT &search_key,
                           bool inclusive,
                           const KeyT *end_key,
                           bool end_inclusive,
                           const Schema *key_schema,
                           const Schema *rec_schema,
                           uint64_t schema_ts,
                           uint64_t snapshot_ts,
                           const KVCatalogInfo *kv_info,
                           CcRequestBase *cc_request,
                           CcShard *cc_shard,
                           store::DataStoreHandler *store_hd,
                           bool force_load,
                           uint8_t prefetch_size,
                           uint8_t max_pin_cnt,
                           bool forward_pin,
                           RangeSliceOpStatus &pin_status,
                           const StoreSlice *&last_pinned_slice)
    {
        // A shared lock on the range to prevent concurrent splitting or merging
        // of slices.
        std::shared_lock<std::shared_mutex> s_lk(mux_);
        size_t slice_idx = SearchSlice(search_key, inclusive);
        StoreSlice *slice = slices_[slice_idx].get();
        std::unique_lock<std::mutex> slice_lk(slice->slice_mux_);

        if (slice->to_alter_)
        {
            // The checkpointer is waiting to alter this slice. The calling tx
            // is pushed back for re-execution, if the request is processed for
            // the first time. If the slice has been pinned, forcing the
            // checkpointer to wait, the request must be allowed to proceed to
            // finish and unpin the slice.
            pin_status = RangeSliceOpStatus::Retry;
            return RangeSliceId(this, slice);
        }

        if (slice->status_ == SliceStatus::FullyCached)
        {
            // collect metrics: slice cache hits
            CollectCacheHit(*cc_shard);

            ++slice->pins_;
            pin_status = RangeSliceOpStatus::Successful;
            last_pinned_slice = slice;

            slice_lk.unlock();

            size_t pin_slice_cnt = 1;

            if (forward_pin)
            {
                for (size_t s_idx = slice_idx + 1;
                     s_idx < slices_.size() && pin_slice_cnt < max_pin_cnt;
                     ++s_idx)
                {
                    TemplateStoreSlice<KeyT> *prepin_slice =
                        slices_[s_idx].get();

                    if (end_key != nullptr)
                    {
                        // If the request (e.g., a scan) specifies the end key,
                        // does not pin slices beyond the end key.
                        const KeyT *slice_start = prepin_slice->StartKey();
                        if (!(*slice_start < *end_key ||
                              (end_inclusive && *slice_start == *end_key)))
                        {
                            break;
                        }
                    }

                    std::unique_lock<std::mutex> s_lk(prepin_slice->slice_mux_);
                    if (!prepin_slice->to_alter_ &&
                        prepin_slice->status_ == SliceStatus::FullyCached)
                    {
                        last_pinned_slice = prepin_slice;
                        ++prepin_slice->pins_;
                        ++pin_slice_cnt;
                    }
                    else
                    {
                        break;
                    }
                }
            }
            else if (slice_idx > 0)
            {
                for (int32_t s_idx = slice_idx - 1;
                     s_idx >= 0 && pin_slice_cnt < max_pin_cnt;
                     --s_idx)
                {
                    TemplateStoreSlice<KeyT> *prepin_slice =
                        slices_[s_idx].get();

                    if (end_key != nullptr)
                    {
                        // If the request (e.g., a scan) specifies the end key,
                        // does not pin slices beyond the end key.
                        const KeyT *slice_end = prepin_slice->EndKey();
                        if (!(*end_key < *slice_end))
                        {
                            break;
                        }
                    }

                    std::unique_lock<std::mutex> s_lk(prepin_slice->slice_mux_);
                    if (!prepin_slice->to_alter_ &&
                        prepin_slice->status_ == SliceStatus::FullyCached)
                    {
                        last_pinned_slice = prepin_slice;
                        ++prepin_slice->pins_;
                        ++pin_slice_cnt;
                    }
                    else
                    {
                        break;
                    }
                }
            }
            pins_.fetch_add(pin_slice_cnt, std::memory_order_release);
        }
        else
        {
            last_pinned_slice = nullptr;

            // collect metrics: slice cache miss
            CollectCacheMiss(*cc_shard);

            LoadSliceStatus load_ret = LoadSlice(tbl_name,
                                                 ng_term,
                                                 *slice,
                                                 key_schema,
                                                 rec_schema,
                                                 schema_ts,
                                                 snapshot_ts,
                                                 kv_info,
                                                 cc_request,
                                                 cc_shard,
                                                 store_hd,
                                                 force_load,
                                                 slice_lk);
            switch (load_ret)
            {
            case LoadSliceStatus::Success:
                pin_status = RangeSliceOpStatus::BlockedOnLoad;
                break;
            case LoadSliceStatus::Delay:
                pin_status = RangeSliceOpStatus::Delay;
                break;
            case LoadSliceStatus::Retry:
                pin_status = RangeSliceOpStatus::Retry;
                break;
            default:
                pin_status = RangeSliceOpStatus::Error;
                break;
            }

            slice_lk.unlock();

            size_t sid = slice_idx + 1;
            for (size_t fid = 0; fid < prefetch_size && sid < slices_.size();
                 ++fid, ++sid)
            {
                StoreSlice *prefetch_slice = slices_[sid].get();
                std::unique_lock<std::mutex> prefetch_lk(
                    prefetch_slice->slice_mux_);

                if (prefetch_slice->status_ == SliceStatus::PartiallyCached)
                {
                    LoadSlice(tbl_name,
                              ng_term,
                              *prefetch_slice,
                              key_schema,
                              rec_schema,
                              schema_ts,
                              snapshot_ts,
                              kv_info,
                              nullptr,
                              cc_shard,
                              store_hd,
                              false,
                              prefetch_lk);
                }
            }
        }

        return RangeSliceId(this, slice);
    }

    std::vector<TxKey> CalculateRangeSplitKeys(const TableName &table_name,
                                               const TableSchema *schema,
                                               NodeGroupId ng_id,
                                               int64_t ng_term,
                                               uint64_t flush_ts,
                                               size_t post_ckpt_size) override
    {
        std::vector<TxKey> new_range_keys;
        uint32_t slice_idx = 0;
        uint32_t subrange_slice_idx = 0;
        size_t subrange_cnt =
            std::ceil(post_ckpt_size / (StoreRange::range_max_size * 0.7));
        size_t avg_subrange_size = post_ckpt_size / subrange_cnt;

        while (slice_idx < slices_.size())
        {
            size_t curr_subrange_size = 0;
            for (; curr_subrange_size < avg_subrange_size &&
                   slice_idx < slices_.size();
                 slice_idx++)
            {
                if (slices_.at(slice_idx)->PostCkptSize() != UINT64_MAX)
                {
                    curr_subrange_size += slices_.at(slice_idx)->PostCkptSize();
                }
                else
                {
                    curr_subrange_size += slices_.at(slice_idx)->Size();
                }
            }
            // Skip the first subrange since it will reuse the
            // current range entry
            if (subrange_slice_idx != 0)
            {
                const KeyT *slice_start =
                    slices_[subrange_slice_idx]->StartKey();
                assert(slice_start->Type() == KeyType::Normal);
                new_range_keys.emplace_back(
                    std::make_unique<KeyT>(*slice_start));
            }
            subrange_slice_idx = slice_idx;
        }
        return new_range_keys;
    }

    bool KickoutSlice(const KeyT &kickout_key)
    {
        std::shared_lock<std::shared_mutex> s_lk(mux_);
        size_t slice_idx = SearchSlice(kickout_key, true);
        return slices_[slice_idx]->Kickout();
    }

    size_t PostCkptSize() override
    {
        std::shared_lock<std::shared_mutex> s_lk(mux_);
        size_t size = 0;
        for (size_t idx = 0; idx < slices_.size(); idx++)
        {
            if (slices_.at(idx)->post_ckpt_size_ != UINT64_MAX)
            {
                size += slices_.at(idx)->PostCkptSize();
            }
            else
            {
                size += slices_.at(idx)->Size();
            }
        }
        return size;
    }

    bool UpdateSliceSizeAfterFlush(bool flush_res) override
    {
        std::shared_lock<std::shared_mutex> s_lk(mux_);
        bool updated = false;
        // Iterate over all slices
        for (auto &slice : slices_)
        {
            if (flush_res)
            {
                updated |= slice->UpdateSize();
            }
            else
            {
                slice->SetPostCkptSize(UINT64_MAX);
            }
        }

        return updated;
    }

private:
    static size_t LowerBound(
        const std::vector<std::unique_ptr<KeyT>> &slice_keys,
        const KeyT &search_key);

    size_t SearchSlice(const KeyT &search_key, bool inclusive) const
    {
        size_t slice_idx = 0;
        size_t lower_bound_idx = LowerBound(boundary_keys_, search_key);

        if (lower_bound_idx == boundary_keys_.size())
        {
            // The search key is greater than or equal to the last slice's
            // starting key.

            if (boundary_keys_.empty())
            {
                // The range contains a single slice. The slice ending with
                // range_end_key_ is the slice [range_start_key,
                // range_end_key_).
                slice_idx = 0;
            }
            else if (*boundary_keys_.back() == search_key && !inclusive)
            {
                // The search key equals to the last boundary key and the
                // inclusive flag is false, the containing slice is the second
                // to last slice.
                slice_idx = boundary_keys_.size() - 1;
            }
            else
            {
                // The search key falls into the slice [slice_key_.last,
                // range_end_key_).
                slice_idx = boundary_keys_.size();
            }
        }
        else
        {
            // The search key equals to or is less than
            // slice_key_[lower_bound_idx].

            if (*boundary_keys_[lower_bound_idx] == search_key && inclusive)
            {
                // If the search key equals to slice_key_[lower_bound_idx], the
                // containing slice is [lower_bound_idx, lower_bound_idx + 1),
                // if the inclusive flag is true.
                slice_idx = lower_bound_idx + 1;
            }
            else
            {
                // If the search key is less than slice_key_[lower_bound_idx],
                // the containing slice is [lower_bound_idx - 1,
                // lower_bound_idx).
                slice_idx = lower_bound_idx;
            }
        }

        return slice_idx;
    }

    std::pair<size_t, size_t> SearchSlice(
        const StoreSlice *slice) const override
    {
        const TemplateStoreSlice<KeyT> *typed_slice =
            static_cast<const TemplateStoreSlice<KeyT> *>(slice);
        const KeyT *slice_start = typed_slice->StartKey();
        assert(slice_start != nullptr);
        size_t slice_idx = SearchSlice(*slice_start, true);
        return {slice_idx, slices_.size()};
    }

    StoreSlice *GetSlice(size_t slice_idx) const override
    {
        return slice_idx < slices_.size() ? slices_[slice_idx].get() : nullptr;
    }

    void UpdateSlice(StoreSlice *slice,
                     std::vector<SliceChangeInfo> &split_keys) override
    {
        assert(split_keys.size() > 1);

        TemplateStoreSlice<KeyT> *typed_slice =
            static_cast<TemplateStoreSlice<KeyT> *>(slice);

        // A slice's start is never null. It points to negative infinity, if
        // this is the first slice of the first range.
        assert(typed_slice->StartKey() != nullptr);
        size_t slice_idx = typed_slice->StartKey() == KeyT::NegativeInfinity()
                               ? 0
                               : SearchSlice(*typed_slice->StartKey(), true);
        const KeyT *slice_end_key = typed_slice->EndKey();

        std::unique_ptr<KeyT> next_slice_start_key = nullptr;
        if (split_keys[1].key_.IsOwner())
        {
            next_slice_start_key = split_keys[1].key_.MoveKey<KeyT>();
        }
        else
        {
            next_slice_start_key =
                std::make_unique<KeyT>(*split_keys[1].key_.GetKey<KeyT>());
        }

        typed_slice->SetEndKey(next_slice_start_key.get());
        slice->size_ = split_keys[0].cur_size_;
        slice->post_ckpt_size_ = split_keys[0].post_update_size_;

        for (size_t idx = 1; idx < split_keys.size(); ++idx)
        {
            const KeyT *sub_slice_start = next_slice_start_key.get();
            const KeyT *sub_slice_end = nullptr;

            size_t boundary_keys_idx = slice_idx + idx - 1;
            // Inserts the new boundary keys.
            boundary_keys_.emplace(boundary_keys_.begin() + boundary_keys_idx,
                                   std::move(next_slice_start_key));

            if (idx < split_keys.size() - 1)
            {
                if (split_keys[idx + 1].key_.IsOwner())
                {
                    next_slice_start_key =
                        split_keys[idx + 1].key_.MoveKey<KeyT>();
                }
                else
                {
                    next_slice_start_key = std::make_unique<KeyT>(
                        *split_keys[idx + 1].key_.GetKey<KeyT>());
                }
                sub_slice_end = next_slice_start_key.get();
            }
            else
            {
                // The last sub-slice's end key points to that of the
                // original slice.
                next_slice_start_key = nullptr;
                sub_slice_end = slice_end_key;
            }

            std::unique_ptr<TemplateStoreSlice<KeyT>> sub_slice =
                std::make_unique<TemplateStoreSlice<KeyT>>(
                    sub_slice_start,
                    sub_slice_end,
                    split_keys[idx].cur_size_,
                    SliceStatus::PartiallyCached);

            sub_slice->post_ckpt_size_ = split_keys[idx].post_update_size_;
            sub_slice->status_ = slice->status_;
            sub_slice->last_load_ts_ = slice->last_load_ts_;

            // Inserts the new sub-slices following the first sub-slice.
            slices_.emplace(slices_.begin() + slice_idx + idx,
                            std::move(sub_slice));
        }
    }

    /**
     * @brief The start and end keys of the range. The two boundary keys are
     * raw pointers and point to external keys in the table's range entries
     * at LocalCcShards.
     *
     */
    const KeyT *range_start_key_, *range_end_key_;

    /**
     * @brief A sorted vector of keys that split the range into slices.
     *
     */
    std::vector<std::unique_ptr<KeyT>> boundary_keys_;

    std::vector<std::unique_ptr<TemplateStoreSlice<KeyT>>> slices_;
};  // namespace txservice

template <typename KeyT>
size_t TemplateStoreRange<KeyT>::LowerBound(
    const std::vector<std::unique_ptr<KeyT>> &middle_keys,
    const KeyT &search_key)
{
    size_t len = middle_keys.size();
    size_t first_idx = 0;

    while (len > 0)
    {
        size_t half_len = len >> 1;
        size_t middle_idx = first_idx + half_len;
        if (*middle_keys[middle_idx] < search_key)
        {
            first_idx = middle_idx + 1;
            len = len - half_len - 1;
        }
        else
        {
            len = half_len;
        }
    }

    return first_idx;
}

}  // namespace txservice
