#pragma once

#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "cc_req_base.h"
#include "range_record.h"
#include "tx_record.h"
#include "type.h"

namespace txservice
{
class CcMap;
class CcShard;
class LocalCcShards;
class StoreSlice;
class StoreRange;
struct RangeSliceId;

struct FetchCc : public CcRequestBase
{
public:
    virtual ~FetchCc() = default;
    void AddRequester(CcRequestBase *requester);
    size_t RequesterCount() const;

protected:
    FetchCc(CcShard &ccs, NodeGroupId cc_ng_id);

    std::vector<CcRequestBase *> requesters_;
    CcShard &ccs_;
    NodeGroupId cc_ng_id_;
};

struct FetchCatalogCc : public FetchCc
{
public:
    FetchCatalogCc() = delete;
    FetchCatalogCc(const TableName &table_name,
                   CcShard &ccs,
                   NodeGroupId cc_ng_id);
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
                           NodeGroupId cc_ng_id);
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
                             std::vector<TxKey::Uptr> &&samplekeys)
    {
        for (TxKey::Uptr &samplekey : samplekeys)
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
    std::unordered_map<TableName, std::pair<uint64_t, std::vector<TxKey::Uptr>>>
        sample_pool_map_;
    int error_code_{0};
};

struct FetchTableRangesCc : public FetchCc
{
public:
    FetchTableRangesCc(const TableName &table_name,
                       CcShard &ccs,
                       NodeGroupId ng_id);

    bool Execute(CcShard &ccs) override;
    void AppendTableRanges(std::vector<InitRangeEntry> &&ranges);
    void SetFinish(std::vector<InitRangeEntry> &&ranges, int err);
    void SetFinish(int err);

public:
    const TableName table_name_;
    int error_code_{0};
    std::vector<InitRangeEntry> ranges_vec_;
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

    SliceDataItem(txservice::TxKey::Uptr key,
                  txservice::TxRecord::Uptr rec,
                  uint64_t version_ts,
                  bool is_deleted)
        : key_(std::move(key)),
          record_(std::move(rec)),
          version_ts_(version_ts),
          is_deleted_(is_deleted)
    {
    }

    txservice::TxKey::Uptr key_;
    txservice::TxRecord::Uptr record_;
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
                          const TxKey *start_key,
                          const TxKey *end_key,
                          uint64_t snapshot_ts)
        : table_name_(&tbl_name),
          key_schema_(key_schema),
          rec_schema_(rec_schema),
          schema_ts_(schema_ts),
          start_key_(start_key),
          end_key_(end_key),
          snapshot_ts_(snapshot_ts),
          slice_size_(0)
    {
    }

    void Reset()
    {
        failed_ = false;
        slice_size_ = 0;
        slice_data_.clear();
    }

    void AddDataItem(txservice::TxKey::Uptr key,
                     txservice::TxRecord::Uptr record,
                     uint64_t version_ts,
                     bool is_deleted)
    {
        slice_size_ += key->Size();
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

    const TxKey *StartKey() const
    {
        return start_key_;
    }

    const TxKey *EndKey() const
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

    bool IsError() const
    {
        return failed_;
    }

    std::function<void(LoadRangeSliceRequest *)> post_lambda_;

private:
    const TableName *table_name_;

    std::vector<SliceDataItem> slice_data_;
    const Schema *key_schema_;
    const Schema *rec_schema_;
    const uint64_t schema_ts_;
    const TxKey *start_key_;
    const TxKey *end_key_;
    uint64_t snapshot_ts_;
    uint32_t slice_size_;
    bool failed_{false};
};

struct FillStoreSliceCc : public CcRequestBase
{
public:
    FillStoreSliceCc(const TableName &table_name,
                     NodeGroupId cc_ng,
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

    const std::vector<SliceDataItem> &SliceData(uint16_t core_id) const
    {
        assert(core_id < partitioned_slice_data_.size());
        return partitioned_slice_data_[core_id];
    }

    void AddDataItem(txservice::TxKey::Uptr key,
                     txservice::TxRecord::Uptr record,
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

    const TxKey *SliceStart() const;
    const TxKey *SliceEnd() const;

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

private:
    const TableName *table_name_;
    NodeGroupId cc_ng_id_;
    bool force_load_;
    uint16_t finish_cnt_;
    std::mutex mux_;
    CcErrorCode err_code_{CcErrorCode::NO_ERROR};

    std::vector<std::vector<SliceDataItem>> partitioned_slice_data_;
    LoadRangeSliceRequest load_slice_req_;

    StoreSlice &range_slice_;
    StoreRange &range_;
    LocalCcShards &local_cc_shards_;
};

struct GetPostCkptSlice : public CcRequestBase
{
public:
    GetPostCkptSlice() = delete;
    GetPostCkptSlice(const TableName &table_name,
                     NodeGroupId ng_id,
                     StoreSlice *slice,
                     StoreRange *range,
                     const std::vector<FlushRecord> &ckpt_vec,
                     uint32_t first_slice_idx,
                     uint32_t last_slice_idx,
                     uint64_t ckpt_ts,
                     std::vector<SliceChangeInfo> &slice_items);

    bool Execute(CcShard &ccs) override;

    std::vector<SliceChangeInfo> &SliceRecordCollection()
    {
        return slice_items_;
    }

    RangeSliceId SliceId();

    uint64_t CkptTs() const
    {
        return ckpt_ts_;
    }

    uint32_t SliceFirstIdx() const
    {
        return slice_first_idx_;
    }

    uint32_t SliceLastIdx() const
    {
        return slice_last_idx_;
    }

    const std::vector<FlushRecord> &CkptVec() const
    {
        return ckpt_vec_;
    }

    void Wait()
    {
        std::unique_lock<std::mutex> lk(mux_);
        cv_.wait(lk, [this] { return is_finished_; });
    }

    void SetFinish()
    {
        std::unique_lock<std::mutex> lk(mux_);
        is_finished_ = true;
        err_code_ = CcErrorCode::NO_ERROR;
        cv_.notify_one();
    }

    void SetError(CcErrorCode err_code)
    {
        std::unique_lock<std::mutex> lk(mux_);
        is_finished_ = true;
        err_code_ = err_code;
        cv_.notify_one();
    }

    CcErrorCode ErrorCode()
    {
        std::unique_lock<std::mutex> lk(mux_);
        return err_code_;
    }

    bool IsFinish()
    {
        std::unique_lock<std::mutex> lk(mux_);
        return is_finished_;
    }

    void SetOnLoad(bool on_load)
    {
        on_load_ = on_load;
    }

    bool OnLoad() const
    {
        return on_load_;
    }

    void Reset()
    {
        std::unique_lock<std::mutex> lk(mux_);

        slice_items_.clear();
        is_finished_ = false;
        err_code_ = CcErrorCode::NO_ERROR;

        on_load_ = false;
    }

    std::chrono::time_point<std::chrono::steady_clock> load_start_;

private:
    const TableName &table_name_;
    NodeGroupId cc_ng_id_;
    StoreSlice *slice_;
    StoreRange *range_;
    const std::vector<FlushRecord> &ckpt_vec_;
    uint32_t slice_first_idx_;
    uint32_t slice_last_idx_;
    uint64_t ckpt_ts_;
    /**
     * @brief A collection of keys and their curr and post ckpt record sizes in
     * the slice in the data store.
     *
     */
    std::vector<SliceChangeInfo> &slice_items_;

    bool is_finished_{false};
    CcErrorCode err_code_{CcErrorCode::NO_ERROR};
    std::mutex mux_;
    std::condition_variable cv_;

    bool on_load_{false};
};
}  // namespace txservice
