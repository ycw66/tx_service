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
    BeingLoaded,
    Errored
};

enum struct RangeSliceOpStatus
{
    Successful = 0,
    Blocked,
    Errored,
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

    void SetLoadingError(uint64_t load_ts);

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
            if (status_ != SliceStatus::Errored)
            {
                status_ = SliceStatus::PartiallyCached;
            }
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

    uint16_t PinCount()
    {
        std::unique_lock<std::mutex> lk(slice_mux_);
        return pins_;
    }

private:
    const TxKey *start_key_{nullptr};
    const TxKey *end_key_{nullptr};

    uint32_t size_{0};

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
    static constexpr uint32_t range_max_size = 134217728;

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
                          RangeSliceOpStatus &pin_status);

    RangeSliceOpStatus PinSlice(const TableName &tbl_name,
                                StoreSlice *slice,
                                const Schema *key_schema,
                                const Schema *rec_schema,
                                uint64_t schema_ts,
                                uint64_t snapshot_ts,
                                const KVCatalogInfo *kv_info,
                                CcRequestBase *cc_request,
                                CcShard *cc_shard,
                                store::DataStoreHandler *store_hd);

    void UnpinSlice(StoreSlice *slice);

    void UpdateRange(const TxKey *start_key,
                     const TxKey *end_key,
                     int32_t partition_id);

    bool UpdateRangeSlicesInStore(const TableName &table_name,
                                  uint64_t schema_ts,
                                  bool update_slice_keys,
                                  store::DataStoreHandler *store_hd);

    /**
     * @brief Updates the range and splits the input slice into specified
     * sub-slices. This method is called exclusively by the checkpointer, before
     * it flushes changed data items in the slice and decides to split the slice
     * into multiple ones.
     *
     * @param slice Slice to split
     * @param split_slices Sub-slices after splitting, specified by slices'
     * start keys and their sizes.
     */
    void UpdateSlice(
        StoreSlice *slice,
        std::vector<std::pair<std::unique_ptr<TxKey>, uint32_t>> &sub_slices);

    void SetLoadError(uint64_t load_ts);

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

    void InitSlices(std::vector<std::pair<TxKey::Uptr, uint32_t>> &slice_keys,
                    bool fully_cached = false);

    const std::vector<std::unique_ptr<StoreSlice>> &Slices() const
    {
        return slices_;
    }

    bool NeedSplit(uint64_t new_range_size) const
    {
        return new_range_size > StoreRange::range_max_size;
    }

    /**
     * @brief Split the range with new_end. new_end will be the new
     * end key of this range, and every slice after new_end will be removed
     * from this range and returned to the caller.
     *
     * @param new_end
     * @return std::vector<std::pair<TxKey::Uptr, uint32_t>>
     */
    std::vector<std::pair<TxKey::Uptr, uint32_t>> SplitRange(
        const TxKey *new_end)
    {
        std::unique_lock<std::shared_mutex> range_lk(mux_);
        std::vector<std::pair<TxKey::Uptr, uint32_t>> removed_slices;
        std::vector<TxKey::Uptr> remain_boundary;
        std::vector<std::unique_ptr<StoreSlice>> remain_slices;
        auto boundary = boundary_keys_.begin();
        auto slice = slices_.begin();
        // The first slice always belongs to the old range and
        // is not in boundary_keys_.
        remain_slices.push_back(std::move(*slice));
        slice++;
        while (boundary != boundary_keys_.end())
        {
            if (!(**boundary < *new_end))
            {
                // Remove boundary keys >= new end key
                removed_slices.emplace_back(std::move(*boundary),
                                            (*slice)->Size());
            }
            else
            {
                remain_boundary.push_back(std::move(*boundary));
                remain_slices.push_back(std::move(*slice));
            }
            boundary++;
            slice++;
        }
        boundary_keys_ = std::move(remain_boundary);
        slices_ = std::move(remain_slices);
        range_end_key_ = removed_slices.front().first.get();
        return removed_slices;
    }

private:
    static size_t LowerBound(
        const std::vector<std::unique_ptr<TxKey>> &slice_key_,
        const TxKey &search_key);

    size_t SearchSlice(const TxKey &search_key, bool inclusive) const;

    bool LoadSlice(const TableName &tbl_name,
                   StoreSlice &slice,
                   const Schema *key_schema,
                   const Schema *rec_schema,
                   uint64_t schema_ts,
                   uint64_t snapshot_ts,
                   const KVCatalogInfo *kv_info,
                   CcRequestBase *cc_request,
                   CcShard *cc_shard,
                   store::DataStoreHandler *store_hd);

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

    friend class StoreSlice;
    friend class TableRangeEntry;
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