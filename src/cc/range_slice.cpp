#include "range_slice.h"

#include <cassert>
#include <memory>
#include <vector>

#include "cc_req_misc.h"
#include "cc_shard.h"
#include "local_cc_shards.h"
#include "sharder.h"
#include "store/data_store_handler.h"
#include "tx_start_ts_collector.h"

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

void StoreSlice::CommitLoading(StoreRange &range, uint32_t slice_size)
{
    std::unique_lock<std::mutex> slice_lk(slice_mux_);
    assert(pins_ == 0);
    assert(status_ == SliceStatus::BeingLoaded);

    status_ = SliceStatus::FullyCached;
    size_ = slice_size;

    if (to_alter_)
    {
        std::unique_lock<std::shared_mutex> range_lk(range.mux_);
        // Wake up all waiting threads since there could be multiple slices
        // waiting on the same range wait_cv_.
        range.wait_cv_.notify_all();
    }

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

void StoreSlice::SetLoadingError(StoreRange &range, CcErrorCode err_code)
{
    std::lock_guard<std::mutex> lk(slice_mux_);

    assert(pins_ == 0);
    status_ = SliceStatus::PartiallyCached;

    if (to_alter_)
    {
        std::unique_lock<std::shared_mutex> range_lk(range.mux_);
        // Wake up all waiting threads since there could be multiple slices
        // waiting on the same range wait_cv_.
        range.wait_cv_.notify_all();
    }

    for (auto &[cc_req, cc_shard] : cc_queue_)
    {
        cc_req->AbortCcRequest(err_code);
    }

    if (cc_queue_.size() > 8)
    {
        cc_queue_.resize(8);
        cc_queue_.shrink_to_fit();
    }
    cc_queue_.clear();

    fetch_slice_cc_ = nullptr;
}

bool StoreSlice::IsRecentLoad() const
{
    int64_t delta = LocalCcShards::ClockTs() - last_load_ts_;
    return delta < 4000000;
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
                                  uint64_t snapshot_ts,
                                  const KVCatalogInfo *kv_info,
                                  CcRequestBase *cc_request,
                                  CcShard *cc_shard,
                                  store::DataStoreHandler *store_hd,
                                  RangeSliceOpStatus &pin_status,
                                  bool force_load)
{
    // A shared lock on the range to prevent concurrent splitting or merging of
    // slices.
    std::shared_lock<std::shared_mutex> s_lk(mux_);
    size_t slice_idx = SearchSlice(search_key, inclusive);
    StoreSlice *slice = slices_[slice_idx].get();
    std::unique_lock<std::mutex> slice_lk(slice->slice_mux_);

    if (slice->to_alter_ && slice->pins_ == 0)
    {
        // The checkpointer is waiting to alter this slice. The calling tx is
        // pushed back for re-execution, if the request is processed for the
        // first time. If the slice has been pinned, forcing the checkpointer to
        // wait, the request must be allowed to proceed to finish and unpin the
        // slice.
        pin_status = RangeSliceOpStatus::Retry;
        return RangeSliceId(this, slice);
    }

    if (slice->status_ == SliceStatus::FullyCached)
    {
        // collect metrics: slice cache hits
        if (metrics::enable_cache_hit_rate)
        {
            auto meter = cc_shard->meter_.get();
            meter->Collect(cc_shard->CACHE_HIT_OR_MISS_TOTAL_NAME_, 1, "hits");
        }

        ++slice->pins_;
        pin_status = RangeSliceOpStatus::Successful;
    }
    else
    {
        // collect metrics: slice cache miss
        if (metrics::enable_cache_hit_rate)
        {
            auto meter = cc_shard->meter_.get();
            meter->Collect(cc_shard->CACHE_HIT_OR_MISS_TOTAL_NAME_, 1, "miss");
        }

        LoadSliceStatus load_ret = LoadSlice(tbl_name,
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
        default:
            pin_status = RangeSliceOpStatus::Error;
            break;
        }
    }

    return RangeSliceId(this, slice);
}

RangeSliceOpStatus StoreRange::PinSlice(const TableName &tbl_name,
                                        StoreSlice *slice,
                                        const Schema *key_schema,
                                        const Schema *rec_schema,
                                        uint64_t schema_ts,
                                        uint64_t snapshot_ts,
                                        const KVCatalogInfo *kv_info,
                                        CcRequestBase *cc_request,
                                        CcShard *cc_shard,
                                        store::DataStoreHandler *store_hd,
                                        bool force_load)
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
        // collect metrics: slice cache hits
        if (metrics::enable_cache_hit_rate)
        {
            auto meter = cc_shard->meter_.get();
            meter->Collect(cc_shard->CACHE_HIT_OR_MISS_TOTAL_NAME_, 1, "hits");
        }

        ++slice->pins_;
        return RangeSliceOpStatus::Successful;
    }
    else
    {
        // collect metrics: slice cache miss
        if (metrics::enable_cache_hit_rate)
        {
            auto meter = cc_shard->meter_.get();
            meter->Collect(cc_shard->CACHE_HIT_OR_MISS_TOTAL_NAME_, 1, "miss");
        }

        LoadSliceStatus load_ret = LoadSlice(tbl_name,
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

        if (load_ret == LoadSliceStatus::Success)
        {
            return RangeSliceOpStatus::BlockedOnLoad;
        }
        else
        {
            // This method is only called by the checkpointer, who sets the
            // force_load flag to true. So, LoadSlice() in this method always
            // reads the slice from the data store, even if there is thrashing.
            assert(load_ret == LoadSliceStatus::Error);
            return RangeSliceOpStatus::Error;
        }
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
        // Wake up all waiting threads since there could be multiple slices
        // waiting on the same range wait_cv_.
        wait_cv_.notify_all();
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

bool StoreRange::UpdateSliceSpec(StoreSlice *slice,
                                 const TableName &table_name,
                                 const TableSchema *schema,
                                 NodeGroupId ng_id,
                                 uint64_t flush_ts,
                                 const std::vector<FlushRecord> &flush_vec,
                                 size_t slice_first_idx,
                                 size_t slice_end_idx,
                                 bool range_locked)
{
    std::unique_lock<std::mutex> slice_lk(slice->slice_mux_);
    std::vector<SliceChangeInfo> item_vec;
    if (slice->status_ != SliceStatus::FullyCached)
    {
        // Load the slice from data store
        slice_lk.unlock();

        std::mutex load_slice_mux;
        std::condition_variable load_slice_cv;
        bool finished = false;
        uint64_t snapshot_ts =
            local_cc_shards_.EnableMvcc()
                ? TxStartTsCollector::Instance().GlobalMinSiTxStartTs()
                : 0;
        const Schema *key_schema;
        if (table_name.Type() == TableType::Secondary)
        {
            key_schema = schema->IndexKeySchema(table_name);
        }
        else
        {
            key_schema = schema->KeySchema();
        }
        LoadRangeSliceRequest load_req(table_name,
                                       key_schema,
                                       schema->RecordSchema(),
                                       schema->Version(),
                                       slice->StartKey(),
                                       slice->EndKey(),
                                       snapshot_ts);
        load_req.post_lambda_ =
            [this, &load_slice_mux, &load_slice_cv, &finished](
                LoadRangeSliceRequest *load_req)
        {
            // Signal the caller that the slice is loaded
            std::unique_lock<std::mutex> lk(load_slice_mux);
            finished = true;
            load_slice_cv.notify_one();
        };
        // Load the slice from data store
        while (true)
        {
            if (!local_cc_shards_.store_hd_->LoadRangeSlice(
                    table_name,
                    schema->GetKVCatalogInfo(),
                    partition_id_,
                    &load_req))
            {
                // There is a data store error when loading the slice, retry
                // until succeed.
                LOG(ERROR) << "Get post ckpt slice failed "
                           << table_name.StringView();
                // sleep for a second before retrying so that we don't consume
                // too much data store traffic
                std::this_thread::sleep_for(std::chrono::seconds(1));
                continue;
            }

            std::unique_lock<std::mutex> load_slice_lk(load_slice_mux);
            load_slice_cv.wait(load_slice_lk, [&finished] { return finished; });
            if (!load_req.IsError())
            {
                break;
            }
            // There is a data store error when loading the slice.
            LOG(ERROR) << "Get post ckpt slice failed "
                       << table_name.StringView();
            finished = false;
            load_req.Reset();
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        // Process the slice
        auto &slice_data = load_req.SliceData();
        if (slice_data.empty())
        {
            // If the slice is empty in data store, mark the slice as fully
            // cached
            slice_lk.lock();
            if (slice->status_ != SliceStatus::BeingLoaded)
            {
                slice->status_ = SliceStatus::FullyCached;
            }
            slice_lk.unlock();
        }
        auto flush_vec_it = flush_vec.begin() + slice_first_idx;
        auto slice_end_it = flush_vec.begin() + slice_end_idx;
        auto loaded_it = slice_data.begin();
        while (flush_vec_it != slice_end_it || loaded_it != slice_data.end())
        {
            int32_t item_size = 0;
            if (loaded_it == slice_data.end() ||
                (flush_vec_it != slice_end_it &&
                 (*flush_vec_it->Key() == *loaded_it->key_ ||
                  *flush_vec_it->Key() < *loaded_it->key_)))
            {
                // Take the item from flush vector
                if (flush_vec_it->payload_status_ == RecordStatus::Deleted)
                {
                    item_size = 0;
                }
                else
                {
                    item_size = flush_vec_it->Key()->Size() +
                                flush_vec_it->PayloadSize();
                }
                item_vec.emplace_back(flush_vec_it->Key(),
                                      item_size - flush_vec_it->delta_size_,
                                      item_size);

                if (loaded_it != slice_data.end() &&
                    *flush_vec_it->Key() == *loaded_it->key_)
                {
                    loaded_it++;
                }
                flush_vec_it++;
            }
            else
            {
                if (!loaded_it->is_deleted_)
                {
                    // Take the item from loaded slice, this item will
                    // not change in this round of checkpoint
                    item_size =
                        loaded_it->key_->Size() + loaded_it->record_->Size();
                    item_vec.emplace_back(
                        std::move(loaded_it->key_), item_size, item_size);
                }
                loaded_it++;
            }
        }
    }
    else
    {
        // If slice is already fully cached, pin the slice so that it won't get
        // kickouted before GetPostCkptSlice is executed.
        slice->pins_++;
        slice_lk.unlock();
        GetPostCkptSlice post_ckpt_slice(table_name,
                                         ng_id,
                                         slice,
                                         this,
                                         flush_vec,
                                         slice_first_idx,
                                         slice_end_idx,
                                         flush_ts,
                                         item_vec);

        local_cc_shards_.EnqueueCcRequest(0, &post_ckpt_slice);
        post_ckpt_slice.Wait();
        // GetPostCkptSlice should never fail.
        assert(post_ckpt_slice.ErrorCode() == CcErrorCode::NO_ERROR);

        // unpin the slice
        UnpinSlice(slice);
    }

    // Split the slice based on post checkpoint item size, but do
    // not update the slice size with the post checkpoint yet since
    // the data is still not flushed into data store yet.
    uint32_t post_flush_size = slice->PostCkptSize();
    assert(post_flush_size != UINT32_MAX);
    uint32_t subslice_cnt = post_flush_size / StoreSlice::slice_upper_bound + 1;
    uint32_t avg_subslice_size = post_flush_size / subslice_cnt;
    std::vector<SliceChangeInfo> split_keys;
    split_keys.reserve(subslice_cnt);

    uint32_t post_ckpt_subslice_size = 0;
    uint32_t curr_subslice_size = 0;
    uint32_t subslice_start = 0;
    for (size_t pos = 0; pos < item_vec.size(); ++pos)
    {
        post_ckpt_subslice_size += item_vec[pos].post_update_slice_size_;
        curr_subslice_size += item_vec[pos].cur_slice_size_;

        if (post_ckpt_subslice_size >= avg_subslice_size ||
            pos == item_vec.size() - 1)
        {
            if (split_keys.empty())
            {
                // The first sub-slice's start key re-uses
                // the old slice's start key, so there is no
                // need to allocate a new key.
                split_keys.emplace_back(
                    nullptr, curr_subslice_size, post_ckpt_subslice_size);
            }
            else
            {
                if (item_vec[subslice_start].is_key_owner_)
                {
                    split_keys.emplace_back(
                        std::move(item_vec[subslice_start].key_.uptr_),
                        curr_subslice_size,
                        post_ckpt_subslice_size);
                    item_vec[subslice_start].is_key_owner_ = false;
                }
                else
                {
                    split_keys.emplace_back(item_vec[subslice_start].key_.ptr_,
                                            curr_subslice_size,
                                            post_ckpt_subslice_size);
                }
            }
            // current pos will be the start key for next slice.
            post_ckpt_subslice_size = item_vec[pos].post_update_slice_size_;
            curr_subslice_size = item_vec[pos].cur_slice_size_;
            subslice_start = pos;
        }
    }
    // Split StoreSlice in memory. Slice info in KV store
    // will be updated after checkpoint.
    if (split_keys.size() > 1)
    {
        std::unique_lock<std::shared_mutex> range_lk(mux_);
        slice_lk.lock();

        assert(!slice->to_alter_);
        slice->to_alter_ = true;

        // Unlocks the slice before checking the slice's pin count. If some
        // tx's are pinning the slice, the calling thread, i.e., the
        // checkpointer, is put into sleep on the condition variable.
        slice_lk.unlock();
        wait_cv_.wait(range_lk,
                      [slice_ptr = slice]
                      { return slice_ptr->ChangeAllowed(); });

        slice_lk.lock();
        assert(slice->pins_ == 0);

        size_t slice_idx = slice->start_key_ == nullptr
                               ? 0
                               : SearchSlice(*slice->start_key_, true);
        const TxKey *slice_end_key = slice->EndKey();

        TxKey::Uptr next_slice_start_key = nullptr;
        if (split_keys[1].is_key_owner_)
        {
            next_slice_start_key = std::move(split_keys[1].key_.uptr_);
            split_keys[1].is_key_owner_ = false;
        }
        else
        {
            next_slice_start_key = split_keys[1].key_.ptr_->Clone();
        }
        slice->end_key_ = next_slice_start_key.get();
        slice->size_ = split_keys[0].cur_slice_size_;
        slice->post_ckpt_size_ = split_keys[0].post_update_slice_size_;

        for (size_t idx = 1; idx < split_keys.size(); ++idx)
        {
            std::unique_ptr<StoreSlice> sub_slice =
                std::make_unique<StoreSlice>();
            sub_slice->start_key_ = next_slice_start_key.get();

            // Inserts the new boundary keys.
            boundary_keys_.emplace(boundary_keys_.begin() + slice_idx - 1 + idx,
                                   std::move(next_slice_start_key));

            if (idx < split_keys.size() - 1)
            {
                if (split_keys[idx + 1].is_key_owner_)
                {
                    next_slice_start_key =
                        std::move(split_keys[idx + 1].key_.uptr_);
                    split_keys[idx + 1].is_key_owner_ = false;
                }
                else
                {
                    next_slice_start_key =
                        split_keys[idx + 1].key_.ptr_->Clone();
                }
                sub_slice->end_key_ = next_slice_start_key.get();
            }
            else
            {
                // The last sub-slice's end key points to that of the original
                // slice.
                next_slice_start_key = nullptr;
                sub_slice->end_key_ = slice_end_key;
            }

            sub_slice->size_ = split_keys[idx].cur_slice_size_;
            sub_slice->post_ckpt_size_ =
                split_keys[idx].post_update_slice_size_;
            // Sub-slices inherit the original slice's status, e.g., if the
            // original slice is fully cached, all sub-slices are too cached.
            sub_slice->status_ = slice->status_;
            sub_slice->last_load_ts_ = slice->last_load_ts_;

            // Inserts the new sub-slices following the first sub-slice.
            slices_.emplace(slices_.begin() + slice_idx + idx,
                            std::move(sub_slice));
        }
        slice->to_alter_ = false;
    }

    return true;
}

std::vector<const TxKey *> StoreRange::CalculateRangeSplitKeys(
    const TableName &table_name,
    const TableSchema *schema,
    NodeGroupId ng_id,
    uint64_t flush_ts,
    size_t post_ckpt_size,
    std::vector<FlushRecord>::const_iterator range_start_it,
    std::vector<FlushRecord>::const_iterator range_end_it,
    const std::vector<FlushRecord> &flush_vec)
{
    auto lower_bound_cmp = [](const FlushRecord &rec, const TxKey &key)
    { return *rec.Key() < key; };

    std::vector<const TxKey *> new_range_keys;
    uint32_t slice_idx = 0;
    uint32_t subrange_slice_idx = 0;
    size_t subrange_cnt =
        std::ceil(post_ckpt_size / (StoreRange::range_max_size * 0.7));
    size_t avg_subrange_size = post_ckpt_size / subrange_cnt;
    auto slice_it = range_start_it;
    auto slice_end_it = range_start_it;
    while (slice_idx < slices_.size())
    {
        size_t curr_subrange_size = 0;
        for (; curr_subrange_size < avg_subrange_size &&
               slice_idx < slices_.size();
             slice_idx++)
        {
            if (slices_.at(slice_idx)->PostCkptSize() != UINT32_MAX)
            {
                curr_subrange_size += slices_.at(slice_idx)->PostCkptSize();
            }
            else
            {
                curr_subrange_size += slices_.at(slice_idx)->Size();
            }
        }
        // This should be the last slice in the previous range. Check if
        // the slice needs to be splitted, if so, split it here. This is
        // to avoid a single hot slice being very big and putting it
        // into the previous range will cause the range go way beyond
        // range max limit.
        if (slices_.at(slice_idx - 1)->PostCkptSize() != UINT32_MAX &&
            slices_.at(slice_idx - 1)->PostCkptSize() >
                StoreSlice::slice_upper_bound)
        {
            // New slice_it will be between last slice_end_it and
            // range_end_it.
            slice_it =
                slices_.at(slice_idx - 1)->StartKey() == nullptr
                    ? range_start_it
                    : std::lower_bound(slice_end_it,
                                       range_end_it,
                                       *slices_.at(slice_idx - 1)->StartKey(),
                                       lower_bound_cmp);

            // New slice_end_it will be between current slice_it and
            // range_end_it.
            slice_end_it =
                slices_.at(slice_idx - 1)->EndKey() == nullptr
                    ? range_end_it
                    : std::lower_bound(slice_it,
                                       range_end_it,
                                       *slices_.at(slice_idx - 1)->EndKey(),
                                       lower_bound_cmp);
            curr_subrange_size -= slices_.at(slice_idx - 1)->PostCkptSize();
            UpdateSliceSpec(slices_.at(slice_idx - 1).get(),
                            table_name,
                            schema,
                            ng_id,
                            flush_ts,
                            flush_vec,
                            std::distance(flush_vec.begin(), slice_it),
                            std::distance(flush_vec.begin(), slice_end_it),
                            true);
            // Now that the slice has been splitted, find the new slice
            // that will be the first slice in the new subrange.
            for (; curr_subrange_size < avg_subrange_size &&
                   slice_idx < slices_.size();
                 slice_idx++)
            {
                if (slices_.at(slice_idx)->PostCkptSize() != UINT32_MAX)
                {
                    curr_subrange_size += slices_.at(slice_idx)->PostCkptSize();
                }
                else
                {
                    curr_subrange_size += slices_.at(slice_idx)->Size();
                }
            }
        }

        // Skip the first subrange since it will reuse the
        // current range entry
        if (subrange_slice_idx != 0)
        {
            new_range_keys.emplace_back(
                slices_[subrange_slice_idx]->StartKey());
        }
        subrange_slice_idx = slice_idx;
    }
    return new_range_keys;
}

bool StoreRange::UpdateRangeSlicesInStore(const TableName &table_name,
                                          uint64_t schema_ts,
                                          bool update_slice_keys,
                                          store::DataStoreHandler *store_hd)
{
    // no range lock is needed since it is updated only by checkpointer.
    return store_hd->UpdateRangeSlices(
        table_name, schema_ts, range_start_key_, slices_, update_slice_keys);
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

StoreRange::LoadSliceStatus StoreRange::LoadSlice(
    const TableName &tbl_name,
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
    std::unique_lock<std::mutex> &slice_lk)
{
    // The caller of this method has acquired the slice lock on the input mutex.

    if (slice.fetch_slice_cc_ == nullptr)
    {
        if (!force_load && slice.IsRecentLoad())
        {
            return LoadSliceStatus::Delay;
        }

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
                                               force_load,
                                               snapshot_ts,
                                               local_cc_shards_);

        if (cc_request != nullptr)
        {
            slice.cc_queue_.emplace_back(cc_request, cc_shard);
        }

        slice_lk.unlock();
        bool success =
            store_hd->LoadRangeSlice(tbl_name,
                                     kv_info,
                                     partition_id_,
                                     slice.fetch_slice_cc_->LoadRequest());
        slice_lk.lock();

        // By the time LoadRangeSlice() returns, the slice's status is
        // either BeingLoaded or PartiallyCached. This is because this
        // method is called by PinSlice(), which is called by one of tx
        // processors who will process the fill slice cc request. Hence,
        // loading slice into memory cannot complete at this point.

        slice.last_load_ts_ = LocalCcShards::ClockTs();
        if (success)
        {
            return LoadSliceStatus::Success;
        }
        else
        {
            slice.fetch_slice_cc_ = nullptr;
            return LoadSliceStatus::Error;
        }
    }
    else
    {
        if (force_load && !slice.fetch_slice_cc_->ForceLoad() &&
            slice.status_ != SliceStatus::BeingLoaded)
        {
            // If the demanding request sets the force_load flag and the
            // fetching request does not, the demanding request is allowed to
            // change the flag if filling into memory has not started.
            slice.fetch_slice_cc_->SetForceLoad(true);
        }

        if (cc_request != nullptr)
        {
            slice.cc_queue_.emplace_back(cc_request, cc_shard);
        }

        return LoadSliceStatus::Success;
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
    std::shared_lock<std::shared_mutex> s_lk(mux_);
    size_t slice_idx = SearchSlice(key, true);
    return slices_[slice_idx].get();
}

size_t StoreRange::PostCkptSize()
{
    std::shared_lock<std::shared_mutex> s_lk(mux_);
    size_t size = 0;
    for (size_t idx = 0; idx < slices_.size(); idx++)
    {
        if (slices_.at(idx)->post_ckpt_size_ != UINT32_MAX)
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

void StoreRange::InitSlices(
    std::vector<std::pair<TxKey::Uptr, uint32_t>> &slice_keys,
    bool fully_cached)
{
    slices_.clear();
    boundary_keys_.clear();
    std::unique_ptr<StoreSlice> slice = std::make_unique<StoreSlice>();
    slice->start_key_ = range_start_key_;
    slice->end_key_ =
        slice_keys.size() > 1 ? slice_keys[1].first.get() : range_end_key_;
    slice->size_ = slice_keys.size() > 0 ? slice_keys[0].second : 0;
    if (fully_cached)
    {
        slice->status_ = SliceStatus::FullyCached;
    }
    slices_.emplace_back(std::move(slice));

    for (size_t idx = 1; idx < slice_keys.size(); ++idx)
    {
        std::unique_ptr<StoreSlice> slice = std::make_unique<StoreSlice>();

        slice->start_key_ = slice_keys[idx].first.get();
        slice->end_key_ = idx == slice_keys.size() - 1
                              ? range_end_key_
                              : slice_keys[idx + 1].first.get();
        slice->size_ = slice_keys[idx].second;
        if (fully_cached)
        {
            slice->status_ = SliceStatus::FullyCached;
        }

        slices_.emplace_back(std::move(slice));
        boundary_keys_.emplace_back(std::move(slice_keys[idx].first));
    }

    assert(slices_.size() == boundary_keys_.size() + 1);
}

void StoreRange::InitSlices(
    std::vector<std::tuple<TxKey::Uptr, uint32_t, SliceStatus>> &slice_keys)
{
    slices_.clear();
    boundary_keys_.clear();
    std::unique_ptr<StoreSlice> slice = std::make_unique<StoreSlice>();
    slice->start_key_ = range_start_key_;
    slice->end_key_ = slice_keys.size() > 1 ? std::get<0>(slice_keys[1]).get()
                                            : range_end_key_;
    slice->size_ = slice_keys.size() > 0 ? std::get<1>(slice_keys[0]) : 0;
    slice->status_ = std::get<2>(slice_keys[0]);
    slices_.emplace_back(std::move(slice));

    for (size_t idx = 1; idx < slice_keys.size(); ++idx)
    {
        std::unique_ptr<StoreSlice> slice = std::make_unique<StoreSlice>();

        slice->start_key_ = std::get<0>(slice_keys[idx]).get();
        slice->end_key_ = idx == slice_keys.size() - 1
                              ? range_end_key_
                              : std::get<0>(slice_keys[idx + 1]).get();
        slice->size_ = std::get<1>(slice_keys[idx]);
        slice->status_ = std::get<2>(slice_keys[idx]);

        slices_.emplace_back(std::move(slice));
        boundary_keys_.emplace_back(std::move(std::get<0>(slice_keys[idx])));
    }

    assert(slices_.size() == boundary_keys_.size() + 1);
}
}  // namespace txservice
