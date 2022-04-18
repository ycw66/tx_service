#include "cc/cc_req_misc.h"

#include "cc/cc_shard.h"

namespace txservice
{
FetchCc::FetchCc(CcShard &ccs) : ccs_(ccs)
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

FetchCatalogCc::FetchCatalogCc(const TableName &table_name, CcShard &ccs)
    : FetchCc(ccs), table_name_(table_name)
{
}

bool FetchCatalogCc::Execute(CcShard &ccs)
{
    if (status_ == RecordStatus::Normal)
    {
        assert(commit_ts_ > 0);
        ccs.CreateCatalog(table_name_, catalog_image_, commit_ts_);
    }
    else if (status_ == RecordStatus::Deleted)
    {
        assert(catalog_image_.empty());
        ccs.CreateCatalog(table_name_, catalog_image_, ccs.Now());
    }
    else
    {
        // Timestamp being 0 means that there is an error when fetching from the
        // data store and the catalog status is unknown.
        ccs.CreateCatalog(table_name_, catalog_image_, 0);
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

FetchTableRangesCc::FetchTableRangesCc(const TableName &range_table_name,
                                       const Schema *key_schema,
                                       CcShard &ccs)
    : FetchCc(ccs), range_table_name_(range_table_name), key_schema_(key_schema)
{
}

bool FetchTableRangesCc::Execute(CcShard &ccs)
{
    ccs.InitTableRanges(range_table_name_, ranges_vec_);

    for (CcRequestBase *&req : requesters_)
    {
        ccs.Enqueue(ccs.core_id_, req);
    }

    ccs.RemoveFetchRequest(range_table_name_);
    return false;
}

void FetchTableRangesCc::SetFinish(std::vector<InitRangeEntry> &&ranges,
                                   int err)
{
    ranges_vec_ = std::move(ranges);
    error_code_ = err;
    ccs_.Enqueue(this);
}
}  // namespace txservice
