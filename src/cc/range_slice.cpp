#include "range_slice.h"

#include <atomic>
#include <cassert>
#include <cstddef>
#include <memory>
#include <shared_mutex>
#include <vector>

#include "catalog_factory.h"
#include "cc_req_misc.h"
#include "cc_shard.h"
#include "error_messages.h"
#include "local_cc_shards.h"
#include "sharder.h"
#include "store/data_store_handler.h"
#include "tx_key.h"
#include "tx_service_metrics.h"
#include "tx_start_ts_collector.h"
#include "util.h"

namespace txservice
{
void RangeSliceId::Unpin()
{
    range_ptr_->UnpinSlice(slice_ptr_, true);
}

TxKey RangeSliceId::RangeStartTxKey() const
{
    return range_ptr_->RangeStartTxKey();
}

TxKey RangeSliceId::RangeEndTxKey() const
{
    return range_ptr_->RangeEndTxKey();
}

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
    range.pins_.fetch_sub(1, std::memory_order_release);
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

    // We need to make sure that the CcMap::Execute(CcRequest ) and
    // CcRequest::ABortCcRequest(...) functions occur on the same thread.
    // Otherwise, AbortCcRequest is not safe behavior.
    std::unordered_map<CcShard *, std::vector<CcRequestBase *>> waiting_reqs;
    for (auto &[cc_req, cc_shard] : cc_queue_)
    {
        waiting_reqs[cc_shard].push_back(cc_req);
    }

    for (auto &[cc_shard, reqs] : waiting_reqs)
    {
        cc_shard->AbortCcRequests(std::move(reqs), err_code);
    }

    if (cc_queue_.size() > 8)
    {
        cc_queue_.resize(8);
        cc_queue_.shrink_to_fit();
    }
    cc_queue_.clear();

    fetch_slice_cc_ = nullptr;
    range.pins_.fetch_sub(1, std::memory_order_release);
}

bool StoreSlice::IsRecentLoad() const
{
    int64_t delta = LocalCcShards::ClockTs() - last_load_ts_;
    return delta < 4000000;
}

void StoreSlice::InitKeyCache(StoreRange *range,
                              const TableName *tbl_name,
                              NodeGroupId ng_id,
                              int64_t term)
{
    std::lock_guard<std::mutex> lk(slice_mux_);
    if (status_ == SliceStatus::FullyCached && !init_key_cache_cc_)
    {
        // Pin the range so that the StoreRange won't be evicted.
        range->pins_.fetch_add(1, std::memory_order_acquire);
        // Pin the slice so that it won't be kicked out during key cache init.
        pins_++;
        init_key_cache_cc_ =
            std::make_unique<InitKeyCacheCc>(range,
                                             this,
                                             range->local_cc_shards_.Count(),
                                             tbl_name,
                                             term,
                                             ng_id);
        uint16_t core_cnt = range->local_cc_shards_.Count();
        for (uint16_t core_id = 0; core_id < core_cnt; core_id++)
        {
            Sharder::Instance().GetLocalCcShards()->EnqueueToCcShard(
                core_id, init_key_cache_cc_.get());
        }
    }
}

StoreRange::StoreRange(uint32_t partition_id,
                       NodeGroupId range_owner,
                       LocalCcShards &cc_shards,
                       bool init_key_cache)
    : partition_id_(partition_id),
      cc_ng_id_(range_owner),
      local_cc_shards_(cc_shards),
      last_accessed_ts_(local_cc_shards_.ClockTs())
{
    if (init_key_cache && txservice_enable_key_cache)
    {
        uint16_t core_cnt = Sharder::Instance().GetLocalCcShardsCount();
        for (uint16_t id = 0; id < core_cnt; id++)
        {
            // Assume each record is 200 bytes, calculate the size of the key
            // cache.
            key_cache_.push_back(
                std::make_unique<cuckoofilter::CuckooFilter<size_t, 12>>(
                    StoreRange::range_max_size /
                    StoreRange::key_cache_default_load_factor / 200 /
                    core_cnt));
        }
    }
    else
    {
        key_cache_.resize(0);
    }
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
            auto meter = cc_shard->GetMeter();
            meter->Collect(metrics::NAME_CACHE_HIT_OR_MISS_TOTAL, 1, "hits");
        }

        ++slice->pins_;
        pins_.fetch_add(1, std::memory_order_release);
        return RangeSliceOpStatus::Successful;
    }
    else
    {
        // collect metrics: slice cache miss
        if (metrics::enable_cache_hit_rate)
        {
            auto meter = cc_shard->GetMeter();
            meter->Collect(metrics::NAME_CACHE_HIT_OR_MISS_TOTAL, 1, "miss");
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
        case LoadSliceStatus::Retry:
            // When load slice from data store, may using the prepapre
            // statement, if the PS is being built, it will return Retry.
            pin_status = RangeSliceOpStatus::Retry;
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
            auto [slice_idx, slice_cnt] = SearchSlice(slice);
            size_t sid = slice_idx + 1;
            for (size_t fid = 0; fid < prefetch_size && sid < slice_cnt;
                 ++fid, ++sid)
            {
                StoreSlice *prefetch_slice = GetSlice(sid);
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

void StoreRange::UnpinSlice(StoreSlice *slice, bool need_lock_range)
{
    std::unique_lock<std::mutex> slice_lk(slice->slice_mux_);
    if (slice->pins_ > 0)
    {
        --slice->pins_;
        pins_.fetch_sub(1, std::memory_order_release);
    }

    // The slice is unpinned. If the update slice spec worker has requested to
    // alter the slice, wakes up the worker thread.
    if (slice->pins_ == 1 && slice->to_alter_)
    {
        // Wake up all waiting threads since there could be multiple slices
        // waiting on the same range wait_cv_.
        if (need_lock_range)
        {
            slice_lk.unlock();
            std::unique_lock<std::shared_mutex> range_lk(mux_);
            wait_cv_.notify_all();
        }
        else
        {
            wait_cv_.notify_all();
        }
    }
}

void StoreRange::BatchUnpinSlices(StoreSlice *start_slice,
                                  const StoreSlice *end_slice,
                                  bool forward_dir)
{
    std::shared_lock<std::shared_mutex> s_lk(mux_);
    auto [slice_idx, slice_cnt] = SearchSlice(start_slice);
    assert(GetSlice(slice_idx) == start_slice);
    StoreSlice *slice = start_slice;
    while (slice != end_slice)
    {
        UnpinSlice(slice, false);
        if (forward_dir)
        {
            slice_idx++;
        }
        else
        {
            slice_idx--;
        }
        assert(slice_idx < slice_cnt);
        slice = GetSlice(slice_idx);
    }
    UnpinSlice(slice, false);
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
    const KeySchema *key_schema;
    if (table_name.Type() == TableType::Secondary ||
        table_name.Type() == TableType::UniqueSecondary)
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
    int sleep_time = 0;

    while (true)
    {
        RangeSliceOpStatus status = PinSlice(table_name,
                                             ng_term,
                                             slice,
                                             key_schema,
                                             schema->RecordSchema(),
                                             key_schema->SchemaTs(),
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
            std::this_thread::sleep_for(std::chrono::seconds(sleep_time));
        }
        else if (status == RangeSliceOpStatus::Retry)
        {
            LOG(INFO) << "Waiting 1s for the prepare statement when loading "
                      << "the slice of table: " << table_name.Trace()
                      << " with slice start key: ";
            std::this_thread::sleep_for(std::chrono::seconds(sleep_time));
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
                    std::this_thread::sleep_for(
                        std::chrono::seconds(sleep_time));
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
        if (sleep_time < 10)
        {
            sleep_time += 2;
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

        auto hash_value = flush_vec[idx].Key().Hash();
        int32_t ckpt_size = 0;
        if (flush_vec[idx].payload_status_ != RecordStatus::Deleted)
        {
            ckpt_size =
                flush_vec[idx].Key().Size() + flush_vec[idx].PayloadSize();
        }
        else
        {
            ckpt_size = 0;
        }
        size_t shard_index = (hash_value & 0x3FF) % core_cnt;
        ckpt_cce_raw_ptr_vecs_inmut[shard_index].push_back(
            reinterpret_cast<uintptr_t>(flush_vec[idx].cce_));
        slice_change_info_vecs[core_cnt].emplace_back(
            flush_vec[idx].Key().GetShallowCopy(),
            ckpt_size - flush_vec[idx].delta_size_,
            ckpt_size);
    }

    GetPostCkptSlice post_ckpt_slice(table_name,
                                     ng_id,
                                     slice,
                                     this,
                                     ckpt_cce_raw_ptr_vecs_inmut,
                                     core_cnt,
                                     flush_ts);

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
            UnpinSlice(slice, true);
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
                    std::move(cc_shard_slice_change_info_vec[idx].key_),
                    cc_shard_slice_change_info_vec[idx].cur_size_,
                    cc_shard_slice_change_info_vec[idx].post_update_size_);
            }
        }

        post_ckpt_slice.Reset(ckpt_cce_raw_ptr_vecs_inmut);
    }

    auto key_greater =
        [](const SliceChangeInfo &lhs, const SliceChangeInfo &rhs)
    { return rhs.key_ < lhs.key_; };

    MergeSortedVectors(
        std::move(slice_change_info_vecs), item_vec, key_greater, false);

    // Split the slice based on post checkpoint item size, but do
    // not update the slice size with the post checkpoint yet since
    // the data is still not flushed into data store yet.
    uint64_t post_flush_size = slice->PostCkptSize();
    assert(post_flush_size != UINT64_MAX);
    uint32_t subslice_cnt =
        post_flush_size / (StoreSlice::slice_upper_bound * 0.5);
    if (subslice_cnt < 2)
    {
        subslice_cnt = 2;
    }
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
                split_keys.emplace_back(RangeStartTxKey(),
                                        curr_subslice_size,
                                        post_ckpt_subslice_size);
            }
            else
            {
                split_keys.emplace_back(
                    std::move(item_vec[subslice_start].key_),
                    curr_subslice_size,
                    post_ckpt_subslice_size);
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
        split_keys.emplace_back(std::move(item_vec[subslice_start].key_),
                                curr_subslice_size,
                                post_ckpt_subslice_size);
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

        std::unique_lock<std::mutex> heap_lk(
            local_cc_shards_.table_ranges_heap_mux_);
        bool is_override_thd = mi_is_override_thread();
        mi_threadid_t prev_thd =
            mi_override_thread(local_cc_shards_.GetTableRangesHeapThreadId());
        mi_heap_t *prev_heap =
            mi_heap_set_default(local_cc_shards_.GetTableRangesHeap());

        UpdateSlice(slice, split_keys);

        bool range_slice_mem_full = local_cc_shards_.TableRangesMemoryFull();
        mi_heap_set_default(prev_heap);
        if (is_override_thd)
        {
            mi_override_thread(prev_thd);
        }
        else
        {
            mi_restore_default_thread_id();
        }
        heap_lk.unlock();

        slice->to_alter_ = false;
        if (range_slice_mem_full)
        {
            range_lk.unlock();
            slice_lk.unlock();
            local_cc_shards_.KickoutRangeSlices();
        }
    }

    UnpinSlice(slice, true);

    return true;
}

bool StoreRange::UpdateRangeSlicesInStore(const TableName &table_name,
                                          uint64_t ckpt_ts,
                                          uint64_t range_version,
                                          store::DataStoreHandler *store_hd)
{
    // no range lock is needed since it is updated only by checkpointer.

    return store_hd->UpdateRangeSlices(table_name,
                                       ckpt_ts,
                                       RangeStartTxKey(),
                                       Slices(),
                                       partition_id_,
                                       range_version);
}

bool StoreRange::SetLastInitKeyCacheTs()
{
    // If the range key cache is just initialized in the last 10s, do not try
    // to reinitialize it. The in memory range might be too big to fit into the
    // key cache, in which case we should avoid spamming InitializeKeyCache.
    uint64_t last_ts =
        last_init_key_cache_time_.load(std::memory_order_acquire);
    if (last_ts + 10000000 < local_cc_shards_.ClockTs() &&
        last_init_key_cache_time_.compare_exchange_strong(
            last_ts, local_cc_shards_.ClockTs(), std::memory_order_acq_rel))
    {
        return true;
    }

    // last ts just updated by another thread
    return false;
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
        pins_.fetch_add(1, std::memory_order_release);

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
        slice.fetch_slice_cc_->LoadRequest()->start_ = metrics::Clock::now();
        store::DataStoreHandler::DataStoreOpStatus kv_load_status =
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

        switch (kv_load_status)
        {
        case store::DataStoreHandler::DataStoreOpStatus::Success:
            return LoadSliceStatus::Success;
        case store::DataStoreHandler::DataStoreOpStatus::Retry:
            // Put the ccrequests back to txprocessor queue except the first
            // one which will be put back to txprocessor queue by the caller.
            for (size_t i = 1; i < slice.cc_queue_.size(); ++i)
            {
                auto *cc_req = std::get<0>(slice.cc_queue_[i]);
                auto *cc_shard = std::get<1>(slice.cc_queue_[i]);
                cc_shard->Enqueue(cc_req);
            }
            slice.cc_queue_.clear();
            slice.fetch_slice_cc_ = nullptr;
            pins_.fetch_sub(1, std::memory_order_release);
            return LoadSliceStatus::Retry;
        default:
            // Abort those ccrequests except the first one which will be aborted
            // by the caller. We need to make sure that the
            // CcMap::Execute(CcRequest ) and CcRequest::ABortCcRequest(...)
            // functions occur on the same thread. Otherwise, AbortCcRequest is
            // not safe behavior.
            std::unordered_map<CcShard *, std::vector<CcRequestBase *>>
                waiting_reqs;
            for (size_t i = 1; i < slice.cc_queue_.size(); ++i)
            {
                auto *cc_req = std::get<0>(slice.cc_queue_[i]);
                auto *cc_shard = std::get<1>(slice.cc_queue_[i]);
                waiting_reqs[cc_shard].push_back(cc_req);
            }

            for (auto &[cc_shard, reqs] : waiting_reqs)
            {
                cc_shard->AbortCcRequests(std::move(reqs),
                                          CcErrorCode::DATA_STORE_ERR);
            }
            slice.cc_queue_.clear();
            slice.fetch_slice_cc_ = nullptr;
            pins_.fetch_sub(1, std::memory_order_release);
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
        else if (force_load && !slice.fetch_slice_cc_->ForceLoad())
        {
            // Retry this request whose force_load flag is true, rather than put
            // it into ccrequest queue. If OOM, the queued request will be
            // aborted. Setting force load to true means that requests will
            // ignore OOM errors, such as DataSyncScanCc.
            return LoadSliceStatus::Retry;
        }

        if (cc_request != nullptr)
        {
            slice.cc_queue_.emplace_back(cc_request, cc_shard);
        }

        return LoadSliceStatus::Success;
    }
}

void StoreRange::CollectCacheHit(CcShard &ccs)
{
    // collect metrics: slice cache hits
    if (metrics::enable_cache_hit_rate)
    {
        auto meter = ccs.GetMeter();
        meter->Collect(metrics::NAME_CACHE_HIT_OR_MISS_TOTAL, 1, "hits");
    }
}

void StoreRange::CollectCacheMiss(CcShard &ccs)
{
    // collect metrics: slice cache miss
    if (metrics::enable_cache_hit_rate)
    {
        auto meter = ccs.GetMeter();
        meter->Collect(metrics::NAME_CACHE_HIT_OR_MISS_TOTAL, 1, "miss");
    }
}

}  // namespace txservice
