#pragma once

#include "cc_req_base.h"
#include "tx_record.h"
#include "type.h"

namespace txservice
{
class CcShard;

struct FetchCatalogCc : public CcRequestBase
{
public:
    FetchCatalogCc() = delete;
    FetchCatalogCc(const TableName &table_name, CcShard &ccs);
    ~FetchCatalogCc() = default;

    bool Execute(CcShard &ccs) override;
    void AddRequester(CcRequestBase *requester);

    size_t RequesterCount() const
    {
        return requesters_.size();
    }

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
    CcShard &ccs_;
    std::vector<CcRequestBase *> requesters_;
    RecordStatus status_;
    int error_code_{0};
};
}  // namespace txservice