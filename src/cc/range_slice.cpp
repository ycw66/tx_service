#include "range_slice.h"

#include <cassert>
#include <memory>
#include <vector>

#include "cc_req_misc.h"
#include "cc_shard.h"
#include "error_messages.h"
#include "local_cc_shards.h"
#include "metrics.h"
#include "sharder.h"
#include "store/data_store_handler.h"
#include "tx_key.h"
#include "tx_start_ts_collector.h"
#include "util.h"

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
                       NodeGroupId range_owner,
                       LocalCcShards &cc_shards)
    : range_start_key_(start_key),
      range_end_key_(end_key),
      partition_id_(partition_id),
      cc_ng_id_(range_owner),
      local_cc_shards_(cc_shards)
{
    std::unique_ptr<StoreSlice> slice = std::make_unique<StoreSlice>();
    slice->start_key_ = start_key;
    slice->end_key_ = end_key;
    slices_.emplace_back(std::move(slice));
}

RangeSliceId StoreRange::PinSlice(const TableName &tbl_name,
                                  int64_t ng_term,
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
                                  bool force_load,
                                  uint8_t prefetch_size)
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

RangeSliceOpStatus StoreRange::PinSlice(const TableName &tbl_name,
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
                                        bool force_load,
                                        uint8_t prefetch_size)
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

        RangeSliceOpStatus pin_status;

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
        default:
            // This method is only called by the checkpointer, who sets the
            // force_load flag to true. So, LoadSlice() in this method always
            // reads the slice from the data store, even if there is thrashing.
            assert(load_ret == LoadSliceStatus::Error);
            pin_status = RangeSliceOpStatus::Error;
            break;
        }

        slice_lk.unlock();

        if (prefetch_size > 0)
        {
            size_t slice_idx = 0;
            if (slice->StartKey() != nullptr)
            {
                slice_idx = SearchSlice(*slice->StartKey(), true);
            }
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

        return pin_status;
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
    if (slice->pins_ == 1 && slice->to_alter_)
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

bool StoreRange::UpdateSliceSpec(StoreSlice *slice,
                                 const TableName &table_name,
                                 const TableSchema *schema,
                                 NodeGroupId ng_id,
                                 int64_t ng_term,
                                 uint64_t flush_ts,
                                 const std::vector<FlushRecord> &flush_vec,
                                 size_t slice_first_idx,
                                 size_t slice_end_idx)
{
    std::vector<SliceChangeInfo> item_vec;

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

    // Dispatch notify_cc to the first core
    RunOnTxProcessorCc notify_cc([](CcShard &ccs) {});
    CcShard *notify_cc_shard = local_cc_shards_.GetCcShard(0);

    uint8_t unused_prefetch_size = 0;

    while (true)
    {
        RangeSliceOpStatus status = PinSlice(table_name,
                                             ng_term,
                                             slice,
                                             key_schema,
                                             schema->RecordSchema(),
                                             schema->Version(),
                                             snapshot_ts,
                                             schema->GetKVCatalogInfo(),
                                             &notify_cc,
                                             notify_cc_shard,
                                             local_cc_shards_.store_hd_,
                                             true,
                                             unused_prefetch_size);

        if (status == RangeSliceOpStatus::Successful)
        {
            assert(slice->pins_ > 0 &&
                   slice->status_ == SliceStatus::FullyCached);
            break;
        }
        else if (status == RangeSliceOpStatus::Error)
        {
            LOG(ERROR) << "There is a data store error when loading the slice, "
                          "table name: "
                       << table_name.StringView();
            // There is a data store error when loading the slice, retry
            // until succeed. sleep for a second before retrying so that we
            // don't consume too much data store traffic
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
        else
        {
            notify_cc.Wait();
            if (notify_cc.IsError())
            {
                if (notify_cc.ErrorCode() == CcErrorCode::DATA_STORE_ERR)
                {
                    LOG(ERROR) << "There is a data store error when loading "
                                  "the slice, "
                                  "table name: "
                               << table_name.StringView();
                    // sleep for a second before retrying so that we don't
                    // consume too much data store traffic
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                }
                else if (notify_cc.ErrorCode() == CcErrorCode::OUT_OF_MEMORY)
                {
                    // The force load flag was set to true, so we will force
                    // fill the range data to memory in the next round.
                }
                else
                {
                    assert(notify_cc.ErrorCode() ==
                           CcErrorCode::NG_TERM_CHANGED);
                    // Not leader anymore
                    return false;
                }
            }
        }
        notify_cc.Reset();
    }

    assert(slice->pins_ > 0 && slice->status_ == SliceStatus::FullyCached);

    size_t core_cnt = Sharder::Instance().GetLocalCcShardsCount();
    assert(core_cnt > 0);

    std::vector<std::vector<uintptr_t>> ckpt_cce_raw_ptr_vecs_inmut(core_cnt);

    bool scan_data_drained = false;
    std::vector<std::vector<SliceChangeInfo>> slice_change_info_vecs(core_cnt +
                                                                     1);

    assert(slice_end_idx <= flush_vec.size());

    for (size_t idx = slice_first_idx; idx < slice_end_idx; ++idx)
    {
        if (flush_vec[idx].cce_ == nullptr)
        {
            // This record was load from storage. We can't safely access cce,
            // because it may be kicked out.
            continue;
        }

        auto hash_value = flush_vec[idx].Key()->Hash();
        int32_t ckpt_size = 0;
        if (flush_vec[idx].payload_status_ != RecordStatus::Deleted)
        {
            ckpt_size =
                flush_vec[idx].Key()->Size() + flush_vec[idx].PayloadSize();
        }
        else
        {
            ckpt_size = 0;
        }
        size_t shard_index = (hash_value & 0x3FF) & (core_cnt - 1);
        ckpt_cce_raw_ptr_vecs_inmut[shard_index].push_back(
            reinterpret_cast<uintptr_t>(flush_vec[idx].cce_));
        slice_change_info_vecs[core_cnt].emplace_back(
            flush_vec[idx].Key(),
            ckpt_size - flush_vec[idx].delta_size_,
            ckpt_size);
    }

    std::vector<bool> is_last_one_vec(core_cnt, true);

    GetPostCkptSlice post_ckpt_slice(table_name,
                                     ng_id,
                                     slice,
                                     this,
                                     ckpt_cce_raw_ptr_vecs_inmut,
                                     flush_ts,
                                     core_cnt,
                                     is_last_one_vec);

    while (!scan_data_drained)
    {
        scan_data_drained = true;
        for (size_t shard_idx = 0; shard_idx < core_cnt; ++shard_idx)
        {
            local_cc_shards_.EnqueueCcRequest(shard_idx, &post_ckpt_slice);
        }

        post_ckpt_slice.Wait();
        if (post_ckpt_slice.ErrorCode() != CcErrorCode::NO_ERROR)
        {
            LOG(WARNING) << "UpdateSliceSpec on the non-leader node of ng#"
                         << ng_id << " for table: " << table_name.Trace();
            assert(post_ckpt_slice.ErrorCode() ==
                   CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            UnpinSlice(slice);
            return false;
        }

        for (size_t shard_idx = 0; shard_idx < core_cnt; ++shard_idx)
        {
            scan_data_drained =
                scan_data_drained && post_ckpt_slice.IsDrained(shard_idx);

            auto &cc_shard_slice_change_info_vec =
                post_ckpt_slice.SliceChangeInfoVec(shard_idx);

            for (size_t idx = 0;
                 idx < post_ckpt_slice.item_vec_size_[shard_idx];
                 ++idx)
            {
                // Need to clone key
                slice_change_info_vecs[shard_idx].emplace_back(
                    cc_shard_slice_change_info_vec[idx].key_.uptr_->Clone(),
                    cc_shard_slice_change_info_vec[idx].cur_size_,
                    cc_shard_slice_change_info_vec[idx].post_update_size_);
            }
        }

        post_ckpt_slice.Reset(ckpt_cce_raw_ptr_vecs_inmut, is_last_one_vec);
    }

    auto key_greater =
        [](const SliceChangeInfo &lhs, const SliceChangeInfo &rhs)
    {
        const TxKey *l_key = lhs.SliceStartKey();
        const TxKey *r_key = rhs.SliceStartKey();
        return *r_key < *l_key;
    };

    MergeSortedVectors(
        std::move(slice_change_info_vecs), item_vec, key_greater, false);

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
        post_ckpt_subslice_size += item_vec[pos].post_update_size_;
        curr_subslice_size += item_vec[pos].cur_size_;

        if (post_ckpt_subslice_size >= avg_subslice_size)
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
            // next pos will be the start key for next slice.
            post_ckpt_subslice_size = 0;
            curr_subslice_size = 0;
            subslice_start = pos + 1;
        }
    }

    if (post_ckpt_subslice_size > 0)
    {
        assert(subslice_start < item_vec.size());
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

    // Split StoreSlice in memory. Slice info in KV store
    // will be updated after checkpoint.
    if (split_keys.size() > 1)
    {
        std::unique_lock<std::shared_mutex> range_lk(mux_);
        std::unique_lock<std::mutex> slice_lk(slice->slice_mux_);

        assert(!slice->to_alter_);
        slice->to_alter_ = true;

        // Unlocks the slice before checking the slice's pin count. If some
        // tx's are pinning the slice, the calling thread, i.e., the
        // checkpointer, is put into sleep on the condition variable.
        slice_lk.unlock();
        wait_cv_.wait(range_lk,
                      [slice_ptr = slice]
                      { return slice_ptr->ChangeAllowed(); });
        assert(slice->pins_ == 1 && slice->status_ == SliceStatus::FullyCached);

        slice_lk.lock();

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
        slice->size_ = split_keys[0].cur_size_;
        slice->post_ckpt_size_ = split_keys[0].post_update_size_;

        for (size_t idx = 1; idx < split_keys.size(); ++idx)
        {
            std::unique_ptr<StoreSlice> sub_slice =
                std::make_unique<StoreSlice>();
            sub_slice->start_key_ = next_slice_start_key.get();

            size_t boundary_keys_idx = slice_idx + idx - 1;

            // Inserts the new boundary keys.
            boundary_keys_.emplace(boundary_keys_.begin() + boundary_keys_idx,
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

            sub_slice->size_ = split_keys[idx].cur_size_;
            sub_slice->post_ckpt_size_ = split_keys[idx].post_update_size_;

            sub_slice->status_ = slice->status_;
            sub_slice->last_load_ts_ = slice->last_load_ts_;

            // Inserts the new sub-slices following the first sub-slice.
            slices_.emplace(slices_.begin() + slice_idx + idx,
                            std::move(sub_slice));
        }
        slice->to_alter_ = false;
    }

    UnpinSlice(slice);

    return true;
}

std::vector<const TxKey *> StoreRange::CalculateRangeSplitKeys(
    const TableName &table_name,
    const TableSchema *schema,
    NodeGroupId ng_id,
    int64_t ng_term,
    uint64_t flush_ts,
    size_t post_ckpt_size,
    std::vector<FlushRecord>::const_iterator range_start_it,
    std::vector<FlushRecord>::const_iterator range_end_it,
    const std::vector<FlushRecord> &flush_vec)
{
    std::vector<const TxKey *> new_range_keys;
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
            if (slices_.at(slice_idx)->PostCkptSize() != UINT32_MAX)
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

StoreRange::LoadSliceStatus StoreRange::LoadSlice(
    const TableName &tbl_name,
    int64_t ng_term,
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
    // The caller of this method has acquired the slice lock on the input
    // mutex.

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
                                               ng_term,
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
            // fetching request does not, the demanding request is allowed
            // to change the flag if filling into memory has not started.
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
    std::vector<std::pair<TxKey::Uptr, uint32_t>> &&slice_keys,
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
