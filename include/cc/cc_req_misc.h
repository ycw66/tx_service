#pragma once

#include <condition_variable>
#include <mutex>

#include "cc_req_base.h"
#include "tx_record.h"
#include "type.h"

namespace txservice
{
class CcMap;
class CcShard;
class LocalCcShards;
struct InitRangeEntry;
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

    std::string &StatisticsBinary()
    {
        return statistics_binary_;
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
    std::string statistics_binary_;
    uint64_t commit_ts_;
    RecordStatus status_;
    int error_code_{0};
};

struct FetchTableRangesCc : public FetchCc
{
public:
    FetchTableRangesCc(const TableName &range_table_name,
                       const Schema *key_schema,
                       CcShard &ccs);

    bool Execute(CcShard &ccs) override;
    void SetFinish(std::vector<InitRangeEntry> &&ranges, int err);

public:
    const TableName &range_table_name_;
    const Schema *key_schema_;
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
                  uint64_t version_ts)
        : key_(std::move(key)), record_(std::move(rec)), version_ts_(version_ts)
    {
    }

    txservice::TxKey::Uptr key_;
    txservice::TxRecord::Uptr record_;
    uint64_t version_ts_;
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
                          uint64_t last_ckpt_ts,
                          FillStoreSliceCc *fill_slice_cc = nullptr)
        : table_name_(&tbl_name),
          key_schema_(key_schema),
          rec_schema_(rec_schema),
          schema_ts_(schema_ts),
          start_key_(start_key),
          end_key_(end_key),
          last_ckpt_ts_(last_ckpt_ts),
          slice_size_(0),
          fill_slice_cc_(fill_slice_cc)
    {
    }

    void AddDataItem(txservice::TxKey::Uptr key,
                     txservice::TxRecord::Uptr record,
                     uint64_t version_ts)
    {
        slice_size_ += key->Size();
        slice_size_ += record->Size();
        slice_data_.emplace_back(std::move(key), std::move(record), version_ts);
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

    uint64_t LastCkptTs() const
    {
        return last_ckpt_ts_;
    }

private:
    const TableName *table_name_;

    std::vector<SliceDataItem> slice_data_;
    const Schema *key_schema_;
    const Schema *rec_schema_;
    const uint64_t schema_ts_;
    const TxKey *start_key_;
    const TxKey *end_key_;
    uint64_t last_ckpt_ts_;
    uint32_t slice_size_;

    FillStoreSliceCc *fill_slice_cc_;
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
                     uint64_t last_ckpt_ts,
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
                     uint64_t version_ts);

    void SetFinish();
    void SetError();

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

private:
    const TableName *table_name_;
    NodeGroupId cc_ng_id_;
    uint16_t finish_cnt_;
    uint16_t error_cnt_;
    std::mutex mux_;

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
                     uint64_t last_ckpt_ts,
                     uint64_t ckpt_ts);

    bool Execute(CcShard &ccs) override;

    std::vector<std::pair<const TxKey *, uint32_t>> &SliceRecordCollection()
    {
        return slice_items_;
    }

    RangeSliceId SliceId();

    uint64_t LastCkptTs() const
    {
        return last_ckpt_ts_;
    }

    uint64_t CkptTs() const
    {
        return ckpt_ts_;
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
        is_errored_ = false;
        cv_.notify_one();
    }

    void SetError()
    {
        std::unique_lock<std::mutex> lk(mux_);
        is_finished_ = true;
        is_errored_ = true;
        cv_.notify_one();
    }

    bool IsError() const
    {
        return is_errored_;
    }

    bool IsFinish()
    {
        std::unique_lock<std::mutex> lk(mux_);
        return is_finished_;
    }

private:
    const TableName &table_name_;
    NodeGroupId cc_ng_id_;
    StoreSlice *slice_;
    StoreRange *range_;
    uint64_t last_ckpt_ts_;
    uint64_t ckpt_ts_;
    /**
     * @brief A collection of keys and their record sizes in the slice in the
     * data store after the specified checkpoint.
     *
     */
    std::vector<std::pair<const TxKey *, uint32_t>> slice_items_;

    bool is_finished_{false};
    bool is_errored_{false};
    std::mutex mux_;
    std::condition_variable cv_;
};
}  // namespace txservice