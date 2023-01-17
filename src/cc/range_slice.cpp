#include "range_slice.h"

#include <cassert>
#include <memory>
#include <vector>

#include "cc_req_misc.h"
#include "cc_shard.h"
#include "local_cc_shards.h"
#include "sharder.h"
#include "store/data_store_handler.h"

namespace txservice
{
StoreSlice::~StoreSlice() = default;

void StoreSlice::StartLoading(FillStoreSliceCc *fill_req,
                              LocalCcShards &cc_shards)
{
    std::unique_lock<std::mutex> lk(slice_mux_);

    assert(pins_ == 0);
    status_ = SliceStatus::BeingLoaded;

    for (uint16_t core_id = 0; core_id < cc_shards.Count(); ++core_id)
    {
        cc_shards.EnqueueCcRequest(core_id, fill_req);
    }
}

void StoreSlice::CommitLoading(uint32_t slice_size)
{
    std::unique_lock<std::mutex> slice_lk(slice_mux_);
    assert(pins_ == 0);
    assert(status_ == SliceStatus::BeingLoaded);

    status_ = SliceStatus::FullyCached;
    size_ = slice_size;

    for (auto &[cc_req, cc_shard] : cc_queue_)
    {
        cc_shard->Enqueue(cc_req);
    }
    if (cc_queue_.size() > 128)
    {
        cc_queue_.resize(8);
        cc_queue_.shrink_to_fit();
    }
    cc_queue_.clear();

    fetch_slice_cc_ = nullptr;
}

FillStoreSliceCc *StoreSlice::FillCcRequest()
{
    return fetch_slice_cc_.get();
}

void StoreSlice::SetLoadingError(uint64_t load_ts)
{
    std::lock_guard<std::mutex> lk(slice_mux_);

    assert(pins_ == 0);

    last_load_ts_ = load_ts;
    status_ = SliceStatus::Errored;

    fetch_slice_cc_ = nullptr;

    for (auto &[cc_req, cc_shard] : cc_queue_)
    {
        cc_shard->Enqueue(cc_req);
    }

    if (cc_queue_.size() > 8)
    {
        cc_queue_.resize(8);
        cc_queue_.shrink_to_fit();
    }
    cc_queue_.clear();
}

StoreRange::StoreRange(const TxKey *start_key,
                       const TxKey *end_key,
                       uint32_t partition_id,
                       LocalCcShards &cc_shards)
    : range_start_key_(start_key),
      range_end_key_(end_key),
      partition_id_(partition_id),
      cc_ng_id_(partition_id_ % Sharder::Instance().GetNodeCount()),
      local_cc_shards_(cc_shards)
{
    std::unique_ptr<StoreSlice> slice = std::make_unique<StoreSlice>();
    slice->start_key_ = start_key;
    slice->end_key_ = end_key;
    slices_.emplace_back(std::move(slice));
}

RangeSliceId StoreRange::PinSlice(const TableName &tbl_name,
                                  const TxKey &search_key,
                                  bool inclusive,
                                  const Schema *key_schema,
                                  const Schema *rec_schema,
                                  uint64_t schema_ts,
                                  uint64_t last_ckpt_ts,
                                  const KVCatalogInfo *kv_info,
                                  CcRequestBase *cc_request,
                                  CcShard *cc_shard,
                                  store::DataStoreHandler *store_hd,
                                  RangeSliceOpStatus &pin_status)
{
    // A shared lock on the range to prevent concurrent splitting or merging of
    // slices.
    std::shared_lock<std::shared_mutex> s_lk(mux_);
    size_t slice_idx = SearchSlice(search_key, inclusive);
    StoreSlice *slice = slices_[slice_idx].get();
    std::unique_lock<std::mutex> slice_lk(slice->slice_mux_);

    if (slice->to_alter_)
    {
        // The checkpointer is waiting to alter this slice. The calling tx is
        // pushed back for re-execution.
        pin_status = RangeSliceOpStatus::Blocked;
        return RangeSliceId(this, slice);
    }

    if (slice->status_ == SliceStatus::FullyCached)
    {
        ++slice->pins_;
        pin_status = RangeSliceOpStatus::Successful;
    }
    else
    {
        bool load_success = LoadSlice(tbl_name,
                                      *slice,
                                      key_schema,
                                      rec_schema,
                                      schema_ts,
                                      last_ckpt_ts,
                                      kv_info,
                                      cc_request,
                                      cc_shard,
                                      store_hd);
        pin_status = load_success ? RangeSliceOpStatus::Blocked
                                  : RangeSliceOpStatus::Errored;
    }

    return RangeSliceId(this, slice);
}

RangeSliceOpStatus StoreRange::PinSlice(const TableName &tbl_name,
                                        StoreSlice *slice,
                                        const Schema *key_schema,
                                        const Schema *rec_schema,
                                        uint64_t schema_ts,
                                        uint64_t last_ckpt_ts,
                                        const KVCatalogInfo *kv_info,
                                        CcRequestBase *cc_request,
                                        CcShard *cc_shard,
                                        store::DataStoreHandler *store_hd)
{
    // A shared lock on the range to prevent concurrent splitting or merging of
    // slices.
    std::shared_lock<std::shared_mutex> s_lk(mux_);
    std::unique_lock<std::mutex> slice_lk(slice->slice_mux_);

    // Only the checkpointer calls this function to pin a slice given the slice
    // Id. The to_alter_ flag is only set by the checkpointer, so this flag must
    // be false here.
    assert(!slice->to_alter_);

    if (slice->status_ == SliceStatus::FullyCached)
    {
        ++slice->pins_;
        return RangeSliceOpStatus::Successful;
    }
    else
    {
        bool load_success = LoadSlice(tbl_name,
                                      *slice,
                                      key_schema,
                                      rec_schema,
                                      schema_ts,
                                      last_ckpt_ts,
                                      kv_info,
                                      cc_request,
                                      cc_shard,
                                      store_hd);
        return load_success ? RangeSliceOpStatus::Blocked
                            : RangeSliceOpStatus::Errored;
    }
}

void StoreRange::UnpinSlice(StoreSlice *slice)
{
    std::unique_lock<std::mutex> slice_lk(slice->slice_mux_);
    if (slice->pins_ > 0)
    {
        --slice->pins_;
    }

    // The slice is unpinned. If the checkpointer has requested to alter the
    // slice, wakes up the checkpointer.
    if (slice->pins_ == 0 && slice->to_alter_)
    {
        // Unlocks the slice before locking the range. This is because all
        // locking operations follow the range-slice order to avoid deadlocks.
        // Since there is a gap between releasing the slice lock and locking the
        // range, someone else may jump in and lock the slice. However, the
        // jumping-in tx won't be able to pin the slice, because the
        // checkpionter has marked the slice to be altered.
        slice_lk.unlock();
        std::unique_lock<std::shared_mutex> range_lk(mux_);
        wait_cv_.notify_one();
    }
}
void StoreRange::UpdateRange(const TxKey *start_key,
                             const TxKey *end_key,
                             int32_t partition_id)
{
    range_start_key_ = start_key;
    range_end_key_ = end_key;
    partition_id_ = partition_id;
    if (slices_.size())
    {
        slices_.front()->start_key_ = start_key;
        slices_.back()->end_key_ = end_key;
    }
}

void StoreRange::UpdateSlice(
    StoreSlice *slice,
    std::vector<std::pair<std::unique_ptr<TxKey>, uint32_t>> &split_keys)
{
    assert(split_keys.size() > 1);

    std::unique_lock<std::shared_mutex> range_lk(mux_);
    std::unique_lock<std::mutex> slice_lk(slice->slice_mux_);

    assert(!slice->to_alter_);
    slice->to_alter_ = true;

    // Unlocks the slice before checking the slice's pin count. If some tx's are
    // pinning the slice, the calling thread, i.e., the checkpointer, is put
    // into sleep on the condition variable.
    slice_lk.unlock();
    wait_cv_.wait(range_lk,
                  [slice_ptr = slice] { return slice_ptr->PinCount() == 0; });

    assert(slice->PinCount() == 0);

    size_t slice_idx = slice->start_key_ == nullptr
                           ? 0
                           : SearchSlice(*slice->start_key_, true);

    const TxKey *slice_end_key = slice->EndKey();
    slice->end_key_ = split_keys[1].first.get();
    slice->size_ = split_keys[0].second;

    for (size_t idx = 1; idx < split_keys.size(); ++idx)
    {
        std::unique_ptr<StoreSlice> sub_slice = std::make_unique<StoreSlice>();
        sub_slice->start_key_ = split_keys[idx].first.get();

        if (idx < split_keys.size() - 1)
        {
            sub_slice->end_key_ = split_keys[idx + 1].first.get();
        }
        else
        {
            // The last sub-slice's end key points to that of the original
            // slice.
            sub_slice->end_key_ = slice_end_key;
        }

        sub_slice->size_ = split_keys[idx].second;
        // Sub-slices inherit the original slice's status, e.g., if the original
        // slice is fully cached, all sub-slices are too cached.
        sub_slice->status_ = slice->status_;
        sub_slice->last_load_ts_ = slice->last_load_ts_;

        // Inserts the new sub-slices following the first sub-slice.
        slices_.emplace(slices_.begin() + slice_idx + idx,
                        std::move(sub_slice));

        // Inserts the new boundary keys.
        boundary_keys_.emplace(boundary_keys_.begin() + slice_idx - 1 + idx,
                               std::move(split_keys[idx].first));
    }

    slice->to_alter_ = false;
}

bool StoreRange::UpdateRangeSlicesInStore(const TableName &table_name,
                                          const KVCatalogInfo *kv_info,
                                          uint64_t schema_ts,
                                          bool update_slice_keys,
                                          store::DataStoreHandler *store_hd)
{
    std::unique_lock<std::shared_mutex> range_lk(mux_);
    return store_hd->UpdateRangeSlices(table_name,
                                       kv_info,
                                       schema_ts,
                                       range_start_key_,
                                       slices_,
                                       update_slice_keys);
}

size_t StoreRange::LowerBound(
    const std::vector<std::unique_ptr<TxKey>> &middle_keys,
    const TxKey &search_key)
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

size_t StoreRange::SearchSlice(const TxKey &search_key, bool inclusive) const
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
            // The search key equals to the last boundary key and the inclusive
            // flag is false, the containing slice is the second to last slice.
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
            // containing slice is [lower_bound_idx, lower_bound_idx + 1), if
            // the inclusive flag is true.
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

bool StoreRange::LoadSlice(const TableName &tbl_name,
                           StoreSlice &slice,
                           const Schema *key_schema,
                           const Schema *rec_schema,
                           uint64_t schema_ts,
                           uint64_t last_ckpt_ts,
                           const KVCatalogInfo *kv_info,
                           CcRequestBase *cc_request,
                           CcShard *cc_shard,
                           store::DataStoreHandler *store_hd)
{
    // The caller of LoadSlice() is holding an exclusive lock on the slice.

    using namespace std::chrono_literals;
    uint64_t now_ts = LocalCcShards::ClockTs();

    // If the slice is in the errored state (because the previous load of the
    // slice failed due to data store failures), only try reloading after a
    // period.
    if (slice.status_ == SliceStatus::Errored &&
        (int64_t) (now_ts - slice.last_load_ts_) <
            std::chrono::duration_cast<std::chrono::microseconds>(4s).count())
    {
        return false;
    }

    if (slice.fetch_slice_cc_ == nullptr)
    {
        // Calls the data store's async API to load the slice
        // [slice_start, slice_end) into memory.
        slice.fetch_slice_cc_ =
            std::make_unique<FillStoreSliceCc>(tbl_name,
                                               cc_ng_id_,
                                               key_schema,
                                               rec_schema,
                                               schema_ts,
                                               slice,
                                               *this,
                                               last_ckpt_ts,
                                               local_cc_shards_);

        if (cc_request != nullptr)
        {
            slice.cc_queue_.emplace_back(cc_request, cc_shard);
        }

        bool success =
            store_hd->LoadRangeSlice(tbl_name,
                                     kv_info,
                                     partition_id_,
                                     slice.fetch_slice_cc_->LoadRequest());
        if (success)
        {
            return true;
        }
        else
        {
            slice.status_ = SliceStatus::Errored;
            slice.last_load_ts_ = LocalCcShards::ClockTs();
            slice.fetch_slice_cc_ = nullptr;
            return false;
        }
    }
    else
    {
        if (cc_request != nullptr)
        {
            slice.cc_queue_.emplace_back(cc_request, cc_shard);
        }

        return true;
    }
}

bool StoreRange::KickoutSlice(const TxKey &kickout_key)
{
    std::shared_lock<std::shared_mutex> s_lk(mux_);
    size_t slice_idx = SearchSlice(kickout_key, true);
    return slices_[slice_idx]->Kickout();
}

StoreSlice *StoreRange::FindSlice(const TxKey &key)
{
    size_t slice_idx = SearchSlice(key, true);
    return slices_[slice_idx].get();
}

void StoreRange::InitSlices(
    std::vector<std::pair<TxKey::Uptr, uint32_t>> &slice_keys)
{
    slices_.clear();
    boundary_keys_.clear();
    std::unique_ptr<StoreSlice> slice = std::make_unique<StoreSlice>();
    slice->start_key_ = range_start_key_;
    slice->end_key_ =
        slice_keys.size() > 1 ? slice_keys[1].first.get() : range_end_key_;
    slice->size_ = slice_keys.size() > 0 ? slice_keys[0].second : 0;
    slices_.emplace_back(std::move(slice));

    for (size_t idx = 1; idx < slice_keys.size(); ++idx)
    {
        std::unique_ptr<StoreSlice> slice = std::make_unique<StoreSlice>();

        slice->start_key_ = slice_keys[idx].first.get();
        slice->end_key_ = idx == slice_keys.size() - 1
                              ? range_end_key_
                              : slice_keys[idx + 1].first.get();
        slice->size_ = slice_keys[idx].second;

        slices_.emplace_back(std::move(slice));
        boundary_keys_.emplace_back(std::move(slice_keys[idx].first));
    }

    assert(slices_.size() == boundary_keys_.size() + 1);
}

}  // namespace txservice