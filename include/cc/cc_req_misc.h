#pragma once

#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog_factory.h"  //TableSchema
#include "cc/cc_entry.h"      // LruEntry
#include "cc_req_base.h"
#include "error_messages.h"
// #include "range_slice.h"
#include "range_slice_type.h"
#include "tx_key.h"
#include "tx_record.h"
#include "tx_service_metrics.h"
#include "type.h"

namespace txservice
{
class CcMap;
class CcShard;
class LocalCcShards;
class StoreSlice;
class StoreRange;
struct RangeSliceId;
struct InitRangeEntry;
struct TableRangeEntry;
struct SliceChangeInfo;
namespace store
{
class DataStoreHandler;
};

struct FetchCc : public CcRequestBase
{
public:
    virtual ~FetchCc() = default;
    void AddRequester(CcRequestBase *requester);
    size_t RequesterCount() const;
    NodeGroupId GetNodeGroupId() const;
    int64_t LeaderTerm() const;
    metrics::TimePoint start_;

protected:
    FetchCc(CcShard &ccs, NodeGroupId cc_ng_id, int64_t cc_ng_term);

    std::vector<CcRequestBase *> requesters_;
    CcShard &ccs_;
    NodeGroupId cc_ng_id_;
    int64_t cc_ng_term_;
};

struct FetchCatalogCc : public FetchCc
{
public:
    FetchCatalogCc() = delete;
    FetchCatalogCc(const TableName &table_name,
                   CcShard &ccs,
                   NodeGroupId cc_ng_id,
                   int64_t cc_ng_term);
    ~FetchCatalogCc() = default;

    bool Execute(CcShard &ccs) override;

    std::string &CatalogImage()
    {
        return catalog_image_;
    }

    uint64_t &CommitTs()
    {
        return commit_ts_;
    }

    const TableName &CatalogName() const
    {
        return table_name_;
    }

    void SetFinish(RecordStatus status, int err);

private:
    const TableName table_name_;
    std::string catalog_image_;
    uint64_t commit_ts_;
    RecordStatus status_;
    int error_code_{0};
};

struct FetchTableStatisticsCc : public FetchCc
{
public:
    FetchTableStatisticsCc() = delete;
    FetchTableStatisticsCc(const TableName &table_name,
                           CcShard &ccs,
                           NodeGroupId cc_ng_id,
                           int64_t cc_ng_term);
    ~FetchTableStatisticsCc() = default;

    bool Execute(CcShard &ccs) override;

    const TableName &CatalogName() const
    {
        return table_name_;
    }

    void SetCurrentVersion(uint64_t current_version)
    {
        current_version_ = current_version;
    }

    uint64_t CurrentVersion() const
    {
        return current_version_;
    }

    void SamplePoolMergeFrom(const TableName &table_or_index_name,
                             std::vector<TxKey> &&samplekeys)
    {
        for (TxKey &samplekey : samplekeys)
        {
            sample_pool_map_[table_or_index_name].second.emplace_back(
                std::move(samplekey));
        }
    }

    void SetRecords(const TableName &table_or_index_name, uint64_t records)
    {
        sample_pool_map_[table_or_index_name].first = records;
    }

    void SetStoreHandler(store::DataStoreHandler *store_hd)
    {
        store_hd_ = store_hd;
    }

    store::DataStoreHandler *StoreHandler()
    {
        return store_hd_;
    }

    void SetFinish(int err);

private:
    const TableName table_name_;
    store::DataStoreHandler *store_hd_{nullptr};
    uint64_t current_version_{0};
    std::unordered_map<TableName, std::pair<uint64_t, std::vector<TxKey>>>
        sample_pool_map_;
    int error_code_{0};
};

struct FetchTableRangesCc : public FetchCc
{
public:
    FetchTableRangesCc(const TableName &table_name,
                       CcShard &ccs,
                       NodeGroupId cc_ng_id,
                       int64_t cc_ng_term);

    bool Execute(CcShard &ccs) override;
    void AppendTableRanges(std::vector<InitRangeEntry> &&ranges);
    void AppendTableRange(InitRangeEntry &&range);
    bool EmptyRanges() const;
    void SetFinish(int err);

public:
    const TableName table_name_;
    int error_code_{0};
    std::vector<InitRangeEntry> ranges_vec_;
};

struct FetchRangeSlicesReq
{
public:
    FetchRangeSlicesReq(const TableName &table_name,
                        TableRangeEntry *range_entry,
                        NodeGroupId ng_id,
                        int64_t cc_ng_term)
        : table_name_(table_name),
          cc_ng_id_(ng_id),
          cc_ng_term_(cc_ng_term),
          range_entry_(range_entry)
    {
    }

    void SetFinish(CcErrorCode err);
    void AddRequester(CcRequestBase *requester, CcShard *ccs)
    {
        requesters_.emplace_back(requester, ccs);
    }
    size_t RequesterCount() const
    {
        return requesters_.size();
    }

    const TableName table_name_;
    NodeGroupId cc_ng_id_;
    int64_t cc_ng_term_;
    TableRangeEntry *range_entry_;
    std::vector<std::pair<CcRequestBase *, CcShard *>> requesters_;
    std::vector<SliceInitInfo> slice_info_;
};

/**
 * @brief The request sent by a cc node when the cc node steps down as the
 * leader of its node group, so as to clear cc maps associated with the cc node
 * group at this node.
 *
 */
struct ClearCcNodeGroup : public CcRequestBase
{
public:
    ClearCcNodeGroup(uint32_t cc_ng_id, uint16_t core_cnt)
        : cc_ng_id_(cc_ng_id), core_cnt_(core_cnt)
    {
    }

    ClearCcNodeGroup() = delete;
    ClearCcNodeGroup(const ClearCcNodeGroup &) = delete;

    bool Execute(CcShard &ccs) override;

    void Wait()
    {
        std::unique_lock<std::mutex> lk(mux_);
        wait_cv_.wait(lk, [this]() { return finish_cnt_ == core_cnt_; });
    }

private:
    const uint32_t cc_ng_id_;
    const uint16_t core_cnt_;
    uint16_t finish_cnt_{0};
    std::mutex mux_;
    std::condition_variable wait_cv_;
};

struct SliceDataItem
{
    SliceDataItem() = delete;

    SliceDataItem(txservice::TxKey key,
                  std::unique_ptr<txservice::TxRecord> &&rec,
                  uint64_t version_ts,
                  bool is_deleted)
        : key_(std::move(key)),
          record_(std::move(rec)),
          version_ts_(version_ts),
          is_deleted_(is_deleted)
    {
    }

    txservice::TxKey key_;
    std::unique_ptr<txservice::TxRecord> record_;
    uint64_t version_ts_;
    bool is_deleted_;
};

struct FillStoreSliceCc;

struct LoadRangeSliceRequest
{
public:
    LoadRangeSliceRequest() = delete;

    LoadRangeSliceRequest(const TableName &tbl_name,
                          const Schema *key_schema,
                          const Schema *rec_schema,
                          uint64_t schema_ts,
                          TxKey start_key,
                          TxKey end_key,
                          uint64_t snapshot_ts,
                          NodeGroupId cc_ng_id,
                          int64_t cc_ng_term)
        : table_name_(&tbl_name),
          key_schema_(key_schema),
          rec_schema_(rec_schema),
          schema_ts_(schema_ts),
          start_key_(std::move(start_key)),
          end_key_(std::move(end_key)),
          snapshot_ts_(snapshot_ts),
          slice_size_(0),
          cc_ng_id_(cc_ng_id),
          cc_ng_term_(cc_ng_term)
    {
    }

    void Reset()
    {
        failed_ = false;
        slice_size_ = 0;
        slice_data_.clear();
    }

    void AddDataItem(txservice::TxKey key,
                     std::unique_ptr<txservice::TxRecord> &&record,
                     uint64_t version_ts,
                     bool is_deleted)
    {
        slice_size_ += key.Size();
        slice_size_ += record->Size();
        slice_data_.emplace_back(
            std::move(key), std::move(record), version_ts, is_deleted);
    }

    std::vector<SliceDataItem> &SliceData()
    {
        return slice_data_;
    }

    void SetFinish();
    void SetError();

    const TableName &TblName() const
    {
        return *table_name_;
    }

    const Schema *KeySchema() const
    {
        return key_schema_;
    }

    const Schema *RecordSchema() const
    {
        return rec_schema_;
    }

    uint64_t SchemaTs() const
    {
        return schema_ts_;
    }

    const TxKey &StartKey() const
    {
        return start_key_;
    }

    const TxKey &EndKey() const
    {
        return end_key_;
    }

    uint32_t SliceSize() const
    {
        return slice_size_;
    }

    uint64_t SnapshotTs() const
    {
        return snapshot_ts_;
    }

    NodeGroupId GetNodeGroupId() const
    {
        return cc_ng_id_;
    }

    int64_t LeaderTerm() const
    {
        return cc_ng_term_;
    }

    bool IsError() const
    {
        return failed_;
    }

    std::function<void(LoadRangeSliceRequest *)> post_lambda_;
    metrics::TimePoint start_;

private:
    const TableName *table_name_;

    std::vector<SliceDataItem> slice_data_;
    const Schema *key_schema_;
    const Schema *rec_schema_;
    const uint64_t schema_ts_;
    TxKey start_key_;
    TxKey end_key_;
    uint64_t snapshot_ts_;
    uint32_t slice_size_;
    NodeGroupId cc_ng_id_;
    int64_t cc_ng_term_;
    bool failed_{false};
};

struct FillStoreSliceCc : public CcRequestBase
{
public:
    static constexpr size_t MaxScanBatchSize = 64;

    FillStoreSliceCc(const TableName &table_name,
                     NodeGroupId cc_ng_id,
                     int64_t cc_ng_term,
                     const Schema *key_schema,
                     const Schema *rec_schema,
                     uint64_t schema_ts,
                     StoreSlice &slice,
                     StoreRange &range,
                     bool force_load,
                     uint64_t snapshot_ts,
                     LocalCcShards &cc_shards);

    ~FillStoreSliceCc() = default;

    bool Execute(CcShard &ccs) override;

    std::vector<SliceDataItem> &SliceData(uint16_t core_id)
    {
        assert(core_id < partitioned_slice_data_.size());
        return partitioned_slice_data_[core_id];
    }

    void AddDataItem(TxKey key,
                     std::unique_ptr<txservice::TxRecord> &&record,
                     uint64_t version_ts,
                     bool is_deleted);

    void SetFinish();
    void SetError(CcErrorCode err_code);

    const TableName &TblName() const
    {
        return *table_name_;
    }

    void StartFilling();
    void TerminateFilling();

    StoreRange &Range()
    {
        return range_;
    }

    LoadRangeSliceRequest *LoadRequest()
    {
        return &load_slice_req_;
    }

    bool ForceLoad()
    {
        std::unique_lock<std::mutex> lk(mux_);
        return force_load_;
    }

    void SetForceLoad(bool force_load)
    {
        std::unique_lock<std::mutex> lk(mux_);
        force_load_ = force_load;
    }

    size_t NextIndex(size_t core_idx) const
    {
        size_t next_idx = next_idxs_[core_idx];
        assert(next_idx <= partitioned_slice_data_[core_idx].size());
        return next_idx;
    }

    void SetNextIndex(size_t core_idx, size_t index)
    {
        assert(index <= partitioned_slice_data_[core_idx].size());
        next_idxs_[core_idx] = index;
    }

private:
    const TableName *table_name_;
    NodeGroupId cc_ng_id_;
    int64_t cc_ng_term_;
    bool force_load_;
    uint16_t finish_cnt_;
    std::mutex mux_;
    CcErrorCode err_code_{CcErrorCode::NO_ERROR};

    std::vector<size_t> next_idxs_;
    std::vector<std::vector<SliceDataItem>> partitioned_slice_data_;
    LoadRangeSliceRequest load_slice_req_;

    StoreSlice &range_slice_;
    StoreRange &range_;
    LocalCcShards &local_cc_shards_;
};

struct GetPostCkptSlice : public CcRequestBase
{
public:
    static constexpr size_t ScanBatchSize = 128;

    GetPostCkptSlice() = delete;
    GetPostCkptSlice(const TableName &table_name,
                     NodeGroupId ng_id,
                     StoreSlice *slice,
                     StoreRange *range,
                     std::vector<std::vector<uintptr_t>> &ckpt_cce_raw_ptr_vec,
                     size_t core_cnt);

    bool Execute(CcShard &ccs) override;

    std::vector<SliceChangeInfo> &SliceChangeInfoVec(size_t core_idx)
    {
        return slice_items_[core_idx];
    }

    StoreSlice *Slice()
    {
        return slice_;
    }

    size_t SliceFirstIdx(size_t core_idx) const
    {
        return slice_first_idxs_[core_idx];
    }

    void UpdateFirstIdx(size_t core_idx, size_t new_slice_first_idx)
    {
        slice_first_idxs_[core_idx] = new_slice_first_idx;
    }

    const std::vector<uintptr_t> &CkptCceRawPtrVec(size_t core_idx) const
    {
        return ckpt_cce_raw_ptr_vecs_[core_idx];
    }

    bool IsDrained(size_t core_idx) const
    {
        return pause_keys_[core_idx].second;
    }

    std::pair<TxKey, bool> &PauseKey(size_t core_idx)
    {
        return pause_keys_[core_idx];
    }

    void Wait()
    {
        std::unique_lock<std::mutex> lk(mux_);
        cv_.wait(lk, [this] { return unfinished_cnt_ == 0; });
    }

    void SetFinish()
    {
        std::unique_lock<std::mutex> lk(mux_);
        --unfinished_cnt_;
        if (unfinished_cnt_ == 0)
        {
            cv_.notify_one();
        }
    }

    void SetError(CcErrorCode err_code)
    {
        std::unique_lock<std::mutex> lk(mux_);
        --unfinished_cnt_;
        err_code_ = err_code;
        if (unfinished_cnt_ == 0)
        {
            cv_.notify_one();
        }
    }

    CcErrorCode ErrorCode()
    {
        std::lock_guard<std::mutex> lk(mux_);
        return err_code_;
    }

    bool IsFinish()
    {
        std::lock_guard<std::mutex> lk(mux_);
        return unfinished_cnt_ == 0;
    }

    void Reset(std::vector<std::vector<uintptr_t>> &ckpt_cce_raw_ptr_vecs)
    {
        std::lock_guard<std::mutex> lk(mux_);

        unfinished_cnt_ = ckpt_cce_raw_ptr_vecs.size();
        ckpt_cce_raw_ptr_vecs_ = ckpt_cce_raw_ptr_vecs;

        for (size_t i = 0; i < slice_first_idxs_.size(); ++i)
        {
            item_vec_size_[i] = 0;
        }

        err_code_ = CcErrorCode::NO_ERROR;
    }

    std::chrono::time_point<std::chrono::steady_clock> load_start_;

    std::vector<size_t> item_vec_size_;

private:
    const TableName &table_name_;
    NodeGroupId cc_ng_id_;
    StoreSlice *slice_;
    StoreRange *range_;
    std::vector<size_t> slice_first_idxs_;

    std::vector<std::pair<TxKey, bool>> pause_keys_;
    std::vector<std::vector<uintptr_t>> &ckpt_cce_raw_ptr_vecs_;

    /**
     * @brief A collection of keys and their curr and post ckpt record sizes in
     * the slice in the data store.
     *
     */
    std::vector<std::vector<SliceChangeInfo>> slice_items_;

    size_t unfinished_cnt_;
    CcErrorCode err_code_{CcErrorCode::NO_ERROR};
    std::mutex mux_;
    std::condition_variable cv_;
};

struct FetchRecordCc : public FetchCc
{
public:
    FetchRecordCc() = delete;
    FetchRecordCc(const TableName *tbl_name,
                  const TableSchema *tbl_schema,
                  TxKey tx_key,
                  LruEntry *cce,
                  CcMap *ccm,
                  CcShard &ccs,
                  NodeGroupId cc_ng_id,
                  int64_t cc_ng_term);
    ~FetchRecordCc() = default;

    bool Execute(CcShard &ccs) override;

    void SetFinish(int err);

    const TableName *table_name_{nullptr};
    const TableSchema *table_schema_{nullptr};
    TxKey tx_key_;
    LruEntry *cce_{nullptr};
    CcMap *ccm_;
    uint64_t rec_ts_{0};
    RecordStatus rec_status_{RecordStatus::Unknown};
    std::unique_ptr<TxRecord> rec_{nullptr};
    int error_code_{0};
};

// This cc request is used to convert parallel access on ccmap/samplepool into
// serial access.
struct RunOnTxProcessorCc : public CcRequestBase
{
public:
    explicit RunOnTxProcessorCc(std::function<void(CcShard &ccs)> task)
        : task_(std::move(task)), is_finished_(false), mux_(), cv_()
    {
    }

    void Reset()
    {
        is_finished_ = false;
        error_code_ = CcErrorCode::NO_ERROR;
    }

    void Wait()
    {
        std::unique_lock<std::mutex> lk(mux_);
        cv_.wait(lk, [this]() { return is_finished_; });
    }

    bool IsError()
    {
        std::lock_guard<std::mutex> lk(mux_);
        return error_code_ != CcErrorCode::NO_ERROR;
    }

    CcErrorCode ErrorCode()
    {
        std::lock_guard<std::mutex> lk(mux_);
        return error_code_;
    }

    void AbortCcRequest(CcErrorCode error_code) override
    {
        std::unique_lock<std::mutex> lk(mux_);
        is_finished_ = true;
        error_code_ = error_code;
        cv_.notify_one();
    }

    bool Execute(CcShard &ccs) override
    {
        std::unique_lock<std::mutex> lk(mux_);

        task_(ccs);

        error_code_ = CcErrorCode::NO_ERROR;
        is_finished_ = true;
        cv_.notify_one();

        return false;
    }

private:
    std::function<void(CcShard &ccs)> task_;
    bool is_finished_{false};
    CcErrorCode error_code_{CcErrorCode::NO_ERROR};
    std::mutex mux_;
    std::condition_variable cv_;
};

struct UpdateCceCkptTsCc : public CcRequestBase
{
public:
#ifdef RANGE_PARTITION_ENABLED
    static constexpr size_t SCAN_BATCH_SIZE = 1024;
#else
    static constexpr size_t SCAN_BATCH_SIZE = 64;
#endif

#ifdef RANGE_PARTITION_ENABLED
    UpdateCceCkptTsCc(
        std::vector<std::vector<FlushRecord *>> &&flush_records_per_core,
        size_t core_cnt,
        NodeGroupId node_group,
        int64_t term)
        : flush_records_per_core_(std::move(flush_records_per_core)),
          unfinished_core_cnt_(core_cnt),
          node_group_(node_group),
          term_(term)
    {
        idxs_.resize(core_cnt, 0);
    }
#else
    UpdateCceCkptTsCc(std::vector<FlushRecord> *flush_records,
                      size_t core_cnt,
                      NodeGroupId node_group,
                      int64_t term)
        : flush_records_(flush_records),
          idx_(0),
          unfinished_core_cnt_(core_cnt),
          node_group_(node_group),
          term_(term)
    {
    }

#endif

    bool Execute(CcShard &ccs) override;

    void SetFinished(CcErrorCode error_code)
    {
        std::lock_guard<std::mutex> lk(mux_);
        unfinished_core_cnt_--;
        if (unfinished_core_cnt_ == 0)
        {
            cv_.notify_one();
        }
    }

    bool IsError()
    {
        std::lock_guard<std::mutex> lk(mux_);
        return error_code_ != CcErrorCode::NO_ERROR;
    }

    void Wait()
    {
        std::unique_lock<std::mutex> lk(mux_);
        cv_.wait(lk, [&]() { return unfinished_core_cnt_ == 0; });
    }

private:
#ifdef RANGE_PARTITION_ENABLED
    std::vector<std::vector<FlushRecord *>> flush_records_per_core_;
    std::vector<size_t> idxs_;
#else
    std::vector<FlushRecord> *flush_records_{nullptr};
    size_t idx_{0};
#endif
    size_t unfinished_core_cnt_;
    NodeGroupId node_group_;
    int64_t term_;
    std::mutex mux_;
    std::condition_variable cv_;
    CcErrorCode error_code_{CcErrorCode::NO_ERROR};
};

}  // namespace txservice
