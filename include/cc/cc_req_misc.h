#pragma once

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
    FetchCc(CcShard &ccs);

    std::vector<CcRequestBase *> requesters_;
    CcShard &ccs_;
};

struct FetchCatalogCc : public FetchCc
{
public:
    FetchCatalogCc() = delete;
    FetchCatalogCc(const TableName &table_name, CcShard &ccs);
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
}  // namespace txservice