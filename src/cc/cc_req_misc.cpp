#include "cc/cc_req_misc.h"

#include "cc/cc_map.h"
#include "cc/cc_shard.h"
#include "cc/local_cc_shards.h"
#include "range_record.h"
#include "range_slice.h"
#include "statistics.h"

namespace txservice
{
FetchCc::FetchCc(CcShard &ccs, NodeGroupId cc_ng_id)
    : ccs_(ccs), cc_ng_id_(cc_ng_id)
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

FetchCatalogCc::FetchCatalogCc(const TableName &table_name,
                               CcShard &ccs,
                               uint32_t cc_ng_id)
    : FetchCc(ccs, cc_ng_id),
      table_name_(table_name.StringView().data(),
                  table_name.StringView().size(),
                  table_name.Type())
{
}

bool FetchCatalogCc::Execute(CcShard &ccs)
{
    int64_t cc_ng_candid_term =
        Sharder::Instance().CandidateLeaderTerm(cc_ng_id_);
    int64_t cc_ng_term = Sharder::Instance().LeaderTerm(cc_ng_id_);

    if (cc_ng_candid_term >= 0 || cc_ng_term >= 0)
    {
        if (status_ == RecordStatus::Normal)
        {
            assert(commit_ts_ > 0);
            ccs.CreateCatalog(
                table_name_, cc_ng_id_, catalog_image_, commit_ts_);
        }
        else if (status_ == RecordStatus::Deleted)
        {
            assert(catalog_image_.empty());
            // The catalog of the specified table does not exists. The version
            // of the non-existent catalog starts from the beginning of history,
            // i.e., ts=1.
            ccs.CreateCatalog(table_name_, cc_ng_id_, catalog_image_, 1);
        }
        else
        {
            // Timestamp being 0 means that there is an error when fetching from
            // the data store and the catalog status is unknown.
            ccs.CreateCatalog(table_name_, cc_ng_id_, catalog_image_, 0);
        }
    }

    for (CcRequestBase *&req : requesters_)
    {
        ccs.Enqueue(ccs.core_id_, req);
    }

    ccs.RemoveFetchRequest(table_name_);
    return false;
}

void FetchCatalogCc::SetFinish(RecordStatus status, int err)
{
    status_ = status;
    error_code_ = err;
    ccs_.Enqueue(this);
}

FetchTableStatisticsCc::FetchTableStatisticsCc(const TableName &table_name,
                                               CcShard &ccs,
                                               uint32_t cc_ng_id)
    : FetchCc(ccs, cc_ng_id),
      table_name_(table_name.StringView().data(),
                  table_name.StringView().size(),
                  table_name.Type())
{
}

bool FetchTableStatisticsCc::Execute(CcShard &ccs)
{
    int64_t cc_ng_candid_term =
        Sharder::Instance().CandidateLeaderTerm(cc_ng_id_);
    int64_t cc_ng_term = Sharder::Instance().LeaderTerm(cc_ng_id_);

    if (cc_ng_candid_term >= 0 || cc_ng_term >= 0)
    {
        CatalogEntry *catalog_entry = ccs.GetCatalog(table_name_, cc_ng_id_);
        TableSchema *table_schema = catalog_entry->schema_.get();

        std::unordered_map<TableName, std::vector<uint64_t>> ng_weights_map;

#ifdef RANGE_PARTITION_ENABLED
        ng_weights_map.try_emplace(
            table_name_,
            ccs.AllNodeGroupBytesAtFetchRange(table_name_, cc_ng_id_));
        for (const TableName &index_name : table_schema->IndexNames())
        {
            ng_weights_map.try_emplace(
                index_name,
                ccs.AllNodeGroupBytesAtFetchRange(index_name, cc_ng_id_));
        }

#else
        uint32_t ng_cnt = Sharder::Instance().NodeGroupCount();
        ng_weights_map.try_emplace(table_name_,
                                   std::vector<uint64_t>(ng_cnt, 1UL));
        for (const TableName &index_name : table_schema->IndexNames())
        {
            ng_weights_map.try_emplace(index_name, std::vector(ng_cnt, 1UL));
        }
#endif

        auto [statistics, inserted] =
            ccs.InitTableStatistics(table_name_,
                                    cc_ng_id_,
                                    std::move(sample_pool_map_),
                                    ng_weights_map);
        if (inserted)
        {
            table_schema->BindStatistics(statistics);
        }
    }

    for (CcRequestBase *&req : requesters_)
    {
        ccs.Enqueue(ccs.core_id_, req);
    }

    ccs.RemoveFetchRequest(table_name_);
    return false;
}

void FetchTableStatisticsCc::SetFinish(int err)
{
    error_code_ = err;
    ccs_.Enqueue(this);
}

FetchTableRangesCc::FetchTableRangesCc(const TableName &table_name,
                                       CcShard &ccs,
                                       NodeGroupId ng_id)
    : FetchCc(ccs, ng_id), table_name_(table_name)
{
}

bool FetchTableRangesCc::Execute(CcShard &ccs)
{
    ccs.InitTableRanges(table_name_, ranges_vec_, cc_ng_id_);

    for (CcRequestBase *&req : requesters_)
    {
        ccs.Enqueue(ccs.core_id_, req);
    }

    ccs.RemoveFetchRequest(table_name_);
    return false;
}

void FetchTableRangesCc::SetFinish(std::vector<InitRangeEntry> &&ranges,
                                   int err)
{
    ranges_vec_ = std::move(ranges);
    error_code_ = err;
    ccs_.Enqueue(this);
}

void FetchTableRangesCc::AppendTableRanges(std::vector<InitRangeEntry> &&ranges)
{
    for (auto &range : ranges)
    {
        ranges_vec_.push_back(std::move(range));
    }
}

void FetchTableRangesCc::SetFinish(int err)
{
    error_code_ = err;
    ccs_.Enqueue(this);
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
    if (post_lambda_)
    {
        post_lambda_(this);
    }
}

void LoadRangeSliceRequest::SetError()
{
    failed_ = true;
    SetFinish();
}

FillStoreSliceCc::FillStoreSliceCc(const TableName &table_name,
                                   NodeGroupId cc_ng,
                                   const Schema *key_schema,
                                   const Schema *rec_schema,
                                   uint64_t schema_ts,
                                   StoreSlice &slice,
                                   StoreRange &range,
                                   bool force_load,
                                   uint64_t snapshot_ts,
                                   LocalCcShards &cc_shards)
    : table_name_(&table_name),
      cc_ng_id_(cc_ng),
      force_load_(force_load),
      finish_cnt_(0),
      load_slice_req_(table_name,
                      key_schema,
                      rec_schema,
                      schema_ts,
                      slice.StartKey(),
                      slice.EndKey(),
                      snapshot_ts),
      range_slice_(slice),
      range_(range),
      local_cc_shards_(cc_shards)
{
    partitioned_slice_data_.resize(cc_shards.Count());
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
    if (cc_ng_candid_term < 0 && cc_ng_term < 0)
    {
        SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
        return false;
    }

    CcMap *ccm = ccs.GetCcm(*table_name_, cc_ng_id_);

    if (ccm == nullptr)
    {
        const CatalogEntry *catalog_entry =
            InitCcm(*table_name_, cc_ng_id_, ccs);

        if (catalog_entry != nullptr)
        {
            if (catalog_entry->Version() == 0)
            {
                // The schema view is initialized but the current schema is
                // unset (version_ts is 0). This means that there is an error
                // when reading the catalog from the data store. Returns an
                // error.
                LOG(INFO) << "Filling range slice request is directed to a "
                             "non-existent cc "
                             "map. Table name: "
                          << table_name_->StringView() << ", cc ng#"
                          << cc_ng_id_
                          << ". Fail to initialize the ccm, as there is a data "
                             "store error when reading the schema.";
                SetError(CcErrorCode::DATA_STORE_ERR);
                return false;
            }
            else
            {
                // For a filling range slice request, there must be a prior
                // request reading and locking the table's schema, to prevent
                // others from dropping the table. Hence, the table's schema
                // must be avaliable.
                assert(catalog_entry->schema_ != nullptr);
                ccm = ccs.GetCcm(*table_name_, cc_ng_id_);
                assert(ccm != nullptr);
            }
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

void FillStoreSliceCc::AddDataItem(txservice::TxKey::Uptr key,
                                   txservice::TxRecord::Uptr record,
                                   uint64_t version_ts,
                                   bool is_deleted)
{
    size_t hash = key->Hash();
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
            range_slice_.SetLoadingError(range_);
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
        range_slice_.SetLoadingError(range_);
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
    range_slice_.SetLoadingError(range_);
}

const TxKey *FillStoreSliceCc::SliceStart() const
{
    return range_slice_.StartKey();
}

const TxKey *FillStoreSliceCc::SliceEnd() const
{
    return range_slice_.EndKey();
}

GetPostCkptSlice::GetPostCkptSlice(const TableName &table_name,
                                   NodeGroupId ng_id,
                                   StoreSlice *slice,
                                   StoreRange *range,
                                   const std::vector<FlushRecord> &ckpt_vec,
                                   uint32_t slice_first_idx,
                                   uint32_t slice_last_idx,
                                   uint64_t ckpt_ts,
                                   std::vector<SliceChangeInfo> &slice_items)
    : table_name_(table_name),
      cc_ng_id_(ng_id),
      slice_(slice),
      range_(range),
      ckpt_vec_(ckpt_vec),
      slice_first_idx_(slice_first_idx),
      slice_last_idx_(slice_last_idx),
      ckpt_ts_(ckpt_ts),
      slice_items_(slice_items)
{
}

bool GetPostCkptSlice::Execute(CcShard &ccs)
{
    int64_t cc_ng_term = Sharder::Instance().LeaderTerm(cc_ng_id_);
    if (cc_ng_term < 0)
    {
        slice_items_.clear();
        return false;
    }

    CcMap *ccm = ccs.GetCcm(table_name_, cc_ng_id_);
    assert(ccm != nullptr);
    return ccm->Execute(*this);
}

RangeSliceId GetPostCkptSlice::SliceId()
{
    return RangeSliceId(range_, slice_);
}
}  // namespace txservice
