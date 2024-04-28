#include "cc/cc_req_misc.h"

#include <unordered_map>

#include "cc/cc_map.h"
#include "cc/cc_shard.h"
#include "cc/local_cc_shards.h"
#include "error_messages.h"
#include "range_record.h"
#include "range_slice.h"
#include "sharder.h"
#include "statistics.h"

namespace txservice
{
FetchCc::FetchCc(CcShard &ccs, NodeGroupId cc_ng_id, int64_t cc_ng_term)
    : ccs_(ccs), cc_ng_id_(cc_ng_id), cc_ng_term_(cc_ng_term)
{
}

void FetchCc::AddRequester(CcRequestBase *requester)
{
    requesters_.emplace_back(requester);
}

size_t FetchCc::RequesterCount() const
{
    return requesters_.size();
}

NodeGroupId FetchCc::GetNodeGroupId() const
{
    return cc_ng_id_;
}

int64_t FetchCc::LeaderTerm() const
{
    return cc_ng_term_;
}

FetchCatalogCc::FetchCatalogCc(const TableName &table_name,
                               CcShard &ccs,
                               uint32_t cc_ng_id,
                               int64_t cc_ng_term)
    : FetchCc(ccs, cc_ng_id, cc_ng_term),
      table_name_(table_name.StringView().data(),
                  table_name.StringView().size(),
                  table_name.Type())
{
}

bool FetchCatalogCc::Execute(CcShard &ccs)
{
    if (error_code_ == 0)
    {
        int64_t cc_ng_candid_term =
            Sharder::Instance().CandidateLeaderTerm(cc_ng_id_);
        int64_t cc_ng_term = Sharder::Instance().LeaderTerm(cc_ng_id_);

        if (std::max(cc_ng_candid_term, cc_ng_term) == cc_ng_term_)
        {
            // If on_leader_stop and Enqueue(ClearCcNodeGroup) happens at this
            // time, the creating catalog will be cleaned by ClearCcNodeGroup,
            // and the running cc_requests will check term invalid.

            if (status_ == RecordStatus::Normal)
            {
                assert(commit_ts_ > 0);
                ccs.CreateCatalog(
                    table_name_, cc_ng_id_, catalog_image_, commit_ts_);
            }
            else
            {
                assert(status_ == RecordStatus::Deleted);
                assert(catalog_image_.empty());
                // The catalog of the specified table does not exists. The
                // version of the non-existent catalog starts from the beginning
                // of history, i.e., ts=1.
                ccs.CreateCatalog(table_name_, cc_ng_id_, catalog_image_, 1);
            }

            for (CcRequestBase *req : requesters_)
            {
                ccs.Enqueue(ccs.core_id_, req);
            }
        }
        else
        {
            for (CcRequestBase *req : requesters_)
            {
                req->AbortCcRequest(CcErrorCode::NG_TERM_CHANGED);
            }
        }
    }
    else
    {
        for (CcRequestBase *req : requesters_)
        {
            req->AbortCcRequest(CcErrorCode::DATA_STORE_ERR);
        }
    }

    ccs.RemoveFetchRequest(table_name_);
    return false;
}

void FetchCatalogCc::SetFinish(RecordStatus status, int err)
{
    status_ = status;
    error_code_ = err;

    CODE_FAULT_INJECTOR("FetchCatalogCc_SetFinish_Error", {
        status_ = RecordStatus::Unknown;
        error_code_ = static_cast<int>(CcErrorCode::DATA_STORE_ERR);
        commit_ts_ = 0;
        catalog_image_.clear();
    });
    ccs_.Enqueue(this);
}

FetchTableStatisticsCc::FetchTableStatisticsCc(const TableName &table_name,
                                               CcShard &ccs,
                                               uint32_t cc_ng_id,
                                               int64_t cc_ng_term)
    : FetchCc(ccs, cc_ng_id, cc_ng_term),
      table_name_(table_name.StringView().data(),
                  table_name.StringView().size(),
                  table_name.Type())
{
}

bool FetchTableStatisticsCc::Execute(CcShard &ccs)
{
    if (error_code_ == 0)
    {
        int64_t cc_ng_candid_term =
            Sharder::Instance().CandidateLeaderTerm(cc_ng_id_);
        int64_t cc_ng_term = Sharder::Instance().LeaderTerm(cc_ng_id_);

        if (std::max(cc_ng_candid_term, cc_ng_term) == cc_ng_term_)
        {
            // If on_leader_stop and Enqueue(ClearCcNodeGroup) happens at this
            // time, the creating catalog will be cleaned by ClearCcNodeGroup,
            // and the running cc_requests will check term invalid.

            CatalogEntry *catalog_entry =
                ccs.GetCatalog(table_name_, cc_ng_id_);
            ccs.InitTableStatistics(catalog_entry->schema_.get(),
                                    catalog_entry->dirty_schema_.get(),
                                    cc_ng_id_,
                                    std::move(sample_pool_map_));
            for (CcRequestBase *req : requesters_)
            {
                ccs.Enqueue(ccs.core_id_, req);
            }
        }
        else
        {
            for (CcRequestBase *req : requesters_)
            {
                req->AbortCcRequest(CcErrorCode::NG_TERM_CHANGED);
            }
        }
    }
    else
    {
        for (CcRequestBase *req : requesters_)
        {
            req->AbortCcRequest(CcErrorCode::DATA_STORE_ERR);
        }
    }

    ccs.RemoveFetchRequest(table_name_);
    return false;
}

void FetchTableStatisticsCc::SetFinish(int err)
{
    error_code_ = err;

    CODE_FAULT_INJECTOR("FetchTableStatisticsCc_SetFinish_Error", {
        error_code_ = static_cast<int>(CcErrorCode::DATA_STORE_ERR);
        current_version_ = 0;
        sample_pool_map_.clear();
    });
    ccs_.Enqueue(this);
}

FetchTableRangesCc::FetchTableRangesCc(const TableName &table_name,
                                       CcShard &ccs,
                                       NodeGroupId cc_ng_id,
                                       int64_t cc_ng_term)
    : FetchCc(ccs, cc_ng_id, cc_ng_term), table_name_(table_name)
{
}

bool FetchTableRangesCc::Execute(CcShard &ccs)
{
    if (error_code_ == 0)
    {
        int64_t cc_ng_candid_term =
            Sharder::Instance().CandidateLeaderTerm(cc_ng_id_);
        int64_t cc_ng_term = Sharder::Instance().LeaderTerm(cc_ng_id_);

        if (std::max(cc_ng_candid_term, cc_ng_term) == cc_ng_term_)
        {
            // If on_leader_stop and Enqueue(ClearCcNodeGroup) happens at this
            // time, the creating catalog will be cleaned by ClearCcNodeGroup,
            // and the running cc_requests will check term invalid.

            ccs.InitTableRanges(table_name_, ranges_vec_, cc_ng_id_);
            for (CcRequestBase *req : requesters_)
            {
                ccs.Enqueue(ccs.core_id_, req);
            }
        }
        else
        {
            for (CcRequestBase *req : requesters_)
            {
                req->AbortCcRequest(CcErrorCode::NG_TERM_CHANGED);
            }
        }
    }
    else
    {
        for (CcRequestBase *req : requesters_)
        {
            req->AbortCcRequest(CcErrorCode::DATA_STORE_ERR);
        }
    }

    ccs.RemoveFetchRequest(table_name_);
    return false;
}

void FetchTableRangesCc::AppendTableRanges(std::vector<InitRangeEntry> &&ranges)
{
    for (auto &range : ranges)
    {
        ranges_vec_.push_back(std::move(range));
    }
}

void FetchTableRangesCc::AppendTableRange(InitRangeEntry &&range)
{
    ranges_vec_.push_back(std::move(range));
}

bool FetchTableRangesCc::EmptyRanges() const
{
    return ranges_vec_.empty();
}

void FetchTableRangesCc::SetFinish(int err)
{
    error_code_ = err;
    CODE_FAULT_INJECTOR("FetchTableRangesCc_SetFinish_Error", {
        error_code_ = static_cast<int>(CcErrorCode::DATA_STORE_ERR);
        ranges_vec_.clear();
    });
    ccs_.Enqueue(this);
}

void FetchRangeSlicesReq::SetFinish(CcErrorCode err)
{
    if (err == CcErrorCode::NO_ERROR)
    {
        std::unique_lock<std::shared_mutex> lk(range_entry_->mux_);
        assert(range_entry_->RangeSlices() == nullptr);
        int64_t size_change =
            range_entry_->InitRangeSlices(std::move(slice_info_), cc_ng_id_);
        LocalCcShards *shards = Sharder::Instance().GetLocalCcShards();
        size_t mem_usage = shards->IncreaseRangeSliceMemUsage(size_change);

        for (auto [req, ccs] : requesters_)
        {
            ccs->Enqueue(req);
        }
        if (mem_usage > shards->range_slice_memory_limit_)
        {
            range_entry_->fetch_range_slices_req_ = nullptr;
            lk.unlock();
            shards->KickoutRangeSlices();
            return;
        }
    }
    else
    {
        // We need to make sure that the CcMap::Execute(CcRequest ) and
        // CcRequest::ABortCcRequest(...) functions occur on the same thread.
        // Otherwise, AbortCcRequest is not safe behavior.
        std::unordered_map<CcShard *, std::vector<CcRequestBase *>>
            waiting_reqs;

        for (auto [req, ccs] : requesters_)
        {
            waiting_reqs[ccs].push_back(req);
        }

        for (auto &[ccs, reqs] : waiting_reqs)
        {
            ccs->AbortCcRequests(std::move(reqs), err);
        }
    }

    std::unique_lock<std::shared_mutex> lk(range_entry_->mux_);
    range_entry_->fetch_range_slices_req_ = nullptr;
}

bool ClearCcNodeGroup::Execute(CcShard &ccs)
{
    ccs.DropLockHoldingTxs(cc_ng_id_);
    ccs.DropCcms(cc_ng_id_);

    if (cc_ng_id_ == ccs.node_id_)
    {
        ccs.ClearActvieSiTxs();
    }

    std::unique_lock<std::mutex> lk(mux_);
    ++finish_cnt_;
    if (finish_cnt_ == core_cnt_)
    {
        ccs.local_shards_.DropTableStatistics(cc_ng_id_);
        ccs.local_shards_.DropCatalogs(cc_ng_id_);
#ifdef RANGE_PARTITION_ENABLED
        ccs.local_shards_.DropTableRanges(cc_ng_id_);
#endif
        ccs.local_shards_.DropBucketInfo(cc_ng_id_);
        LOG(INFO) << "ccshard: " << ccs.core_id_
                  << "; clear ccmaps and catalogs of node group: " << cc_ng_id_;
        wait_cv_.notify_one();
    }

    // The owner of this request is the raft thread that downgrades the cc
    // ng leader to a non-leader node. The request is not in a resource pool
    // and re-used. So, always returns false.
    return false;
}

void LoadRangeSliceRequest::SetFinish()
{
    CODE_FAULT_INJECTOR("LoadRangeSliceRequest_SetFinish_Error", {
        failed_ = true;
        slice_data_.clear();
        slice_size_ = 0;
        snapshot_ts_ = 0;
    });

    if (post_lambda_)
    {
        post_lambda_(this);
    }
    if (metrics::enable_kv_metrics)
    {
        metrics::kv_meter->Collect(metrics::NAME_KV_LOAD_SLICE_TOTAL, 1);
        metrics::kv_meter->CollectDuration(metrics::NAME_KV_LOAD_SLICE_DURATION,
                                           start_);
    }
}

void LoadRangeSliceRequest::SetError()
{
    failed_ = true;
    SetFinish();
}

FillStoreSliceCc::FillStoreSliceCc(const TableName &table_name,
                                   NodeGroupId cc_ng_id,
                                   int64_t cc_ng_term,
                                   const Schema *key_schema,
                                   const Schema *rec_schema,
                                   uint64_t schema_ts,
                                   StoreSlice &slice,
                                   StoreRange &range,
                                   bool force_load,
                                   uint64_t snapshot_ts,
                                   LocalCcShards &cc_shards)
    : table_name_(&table_name),
      cc_ng_id_(cc_ng_id),
      cc_ng_term_(cc_ng_term),
      force_load_(force_load),
      finish_cnt_(0),
      load_slice_req_(table_name,
                      key_schema,
                      rec_schema,
                      schema_ts,
                      slice.StartTxKey(),
                      slice.EndTxKey(),
                      snapshot_ts,
                      cc_ng_id,
                      cc_ng_term),
      range_slice_(slice),
      range_(range),
      local_cc_shards_(cc_shards)
{
    partitioned_slice_data_.resize(cc_shards.Count());
    next_idxs_.resize(cc_shards.Count(), 0);
    load_slice_req_.post_lambda_ = [this](LoadRangeSliceRequest *req)
    {
        if (req->IsError())
        {
            TerminateFilling();
        }
        else
        {
            for (SliceDataItem &data_item : req->SliceData())
            {
                AddDataItem(std::move(data_item.key_),
                            std::move(data_item.record_),
                            data_item.version_ts_,
                            data_item.is_deleted_);
            }
            StartFilling();
        }
    };
}

bool FillStoreSliceCc::Execute(CcShard &ccs)
{
    int64_t cc_ng_candid_term =
        Sharder::Instance().CandidateLeaderTerm(cc_ng_id_);
    int64_t cc_ng_term = Sharder::Instance().LeaderTerm(cc_ng_id_);
    if (std::max(cc_ng_candid_term, cc_ng_term) != cc_ng_term_)
    {
        SetError(CcErrorCode::NG_TERM_CHANGED);
        return false;
    }

    CcMap *ccm = ccs.GetCcm(*table_name_, cc_ng_id_);

    if (ccm == nullptr)
    {
        const CatalogEntry *catalog_entry =
            ccs.InitCcm(*table_name_,
                        cc_ng_id_,
                        std::max(cc_ng_term, cc_ng_candid_term),
                        this);

        if (catalog_entry != nullptr)
        {
            // Successfully load table catalog from data store.
            assert(catalog_entry->Version() > 0);

            // For a filling range slice request, there must be a prior
            // request reading and locking the table's schema, to prevent
            // others from dropping the table. Hence, the table's schema
            // must be avaliable.
            assert(catalog_entry->schema_ != nullptr);
            ccm = ccs.GetCcm(*table_name_, cc_ng_id_);
            assert(ccm != nullptr);
        }
        else
        {
            // The table's schema is not available yet. Cannot initialize the cc
            // map. The request will be re-executed after the schema is fetched
            // from the data store.
            return false;
        }
    }

    ccm->Execute(*this);

    return false;
}

void FillStoreSliceCc::AddDataItem(
    TxKey key,
    std::unique_ptr<txservice::TxRecord> &&record,
    uint64_t version_ts,
    bool is_deleted)
{
    size_t hash = key.Hash();
    // Uses the lower 10 bits of the hash code to shard the key across
    // CPU cores at this node.
    uint16_t core_code = hash & 0x3FF;
    uint16_t core_id = core_code % local_cc_shards_.Count();

    partitioned_slice_data_[core_id].emplace_back(
        std::move(key), std::move(record), version_ts, is_deleted);
}

void FillStoreSliceCc::SetFinish()
{
    bool finish_all = false;
    CcErrorCode err_code;
    {
        std::lock_guard<std::mutex> lk(mux_);
        ++finish_cnt_;

        if (finish_cnt_ == local_cc_shards_.Count())
        {
            finish_all = true;
            err_code = err_code_;
        }
    }

    if (finish_all)
    {
        if (err_code == CcErrorCode::NO_ERROR)
        {
            range_slice_.CommitLoading(range_, load_slice_req_.SliceSize());
        }
        else
        {
            range_slice_.SetLoadingError(range_, err_code);
        }
    }
}

void FillStoreSliceCc::SetError(CcErrorCode err_code)
{
    bool finish_all = false;
    {
        std::lock_guard<std::mutex> lk(mux_);
        ++finish_cnt_;
        err_code_ = err_code;

        if (finish_cnt_ == local_cc_shards_.Count())
        {
            finish_all = true;
        }
    }

    if (finish_all)
    {
        range_slice_.SetLoadingError(range_, err_code_);
    }
}

void FillStoreSliceCc::StartFilling()
{
    range_slice_.StartLoading(this, local_cc_shards_);
}

void FillStoreSliceCc::TerminateFilling()
{
    // The method is called when there is an error of reading the data store.
    // The slice has not been filled into memory. So, the out-of-memory flag is
    // false.
    range_slice_.SetLoadingError(range_, CcErrorCode::DATA_STORE_ERR);
}

GetPostCkptSlice::GetPostCkptSlice(
    const TableName &table_name,
    NodeGroupId ng_id,
    StoreSlice *slice,
    StoreRange *range,
    std::vector<std::vector<uintptr_t>> &ckpt_cce_raw_ptr_vec,
    size_t core_cnt)
    : table_name_(table_name),
      cc_ng_id_(ng_id),
      slice_(slice),
      range_(range),
      ckpt_cce_raw_ptr_vecs_(ckpt_cce_raw_ptr_vec)
{
    unfinished_cnt_ = core_cnt;
    for (size_t i = 0; i < core_cnt; ++i)
    {
        item_vec_size_.emplace_back(0);
        slice_first_idxs_.emplace_back(0);
        slice_items_.emplace_back();
        slice_items_.back().resize(ScanBatchSize);
        pause_keys_.emplace_back(TxKey(), false);
    }
}

bool GetPostCkptSlice::Execute(CcShard &ccs)
{
    int64_t cc_ng_term = Sharder::Instance().LeaderTerm(cc_ng_id_);
    if (cc_ng_term < 0)
    {
        slice_items_.clear();
        SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
        return false;
    }

    CcMap *ccm = ccs.GetCcm(table_name_, cc_ng_id_);
    assert(ccm != nullptr);
    return ccm->Execute(*this);
}

FetchRecordCc::FetchRecordCc(const TableName *tbl_name,
                             const TableSchema *tbl_schema,
                             TxKey tx_key,
                             LruEntry *cce,
                             CcMap *ccm,
                             CcShard &ccs,
                             NodeGroupId cc_ng_id,
                             int64_t cc_ng_term)
    : FetchCc(ccs, cc_ng_id, cc_ng_term),
      table_name_(tbl_name),
      table_schema_(tbl_schema),
      tx_key_(std::move(tx_key)),
      cce_(cce),
      ccm_(ccm)
{
}

bool FetchRecordCc::Execute(CcShard &ccs)
{
    if (error_code_ == 0)
    {
        int64_t cc_ng_candid_term =
            Sharder::Instance().CandidateLeaderTerm(cc_ng_id_);
        int64_t cc_ng_term = Sharder::Instance().LeaderTerm(cc_ng_id_);

        if (std::max(cc_ng_candid_term, cc_ng_term) == cc_ng_term_)
        {
            ccm_->BackFill(cce_, rec_ts_, rec_status_, std::move(rec_));

            for (CcRequestBase *req : requesters_)
            {
                if (req)
                {
                    ccs.Enqueue(ccs.core_id_, req);
                }
            }
        }
        else
        {
            for (CcRequestBase *req : requesters_)
            {
                if (req)
                {
                    req->AbortCcRequest(CcErrorCode::NG_TERM_CHANGED);
                }
            }
        }
    }
    else
    {
        for (CcRequestBase *req : requesters_)
        {
            if (req)
            {
                req->AbortCcRequest(CcErrorCode::DATA_STORE_ERR);
            }
        }
    }

    ccs.RemoveFetchRecordRequest(cce_);
    return false;
}

void FetchRecordCc::SetFinish(int err)
{
    error_code_ = err;
    ccs_.Enqueue(this);
}

}  // namespace txservice
