#pragma once

#include <condition_variable>
#include <mutex>

#include "cc_req_base.h"
#include "range_record.h"
#include "tx_record.h"
#include "type.h"

namespace txservice
{
class CcShard;

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
}  // namespace txservice