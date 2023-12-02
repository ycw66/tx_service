#include "range_record.h"

#include "local_cc_shards.h"

namespace txservice
{

TableRangeEntry::~TableRangeEntry()
{
    std::unique_lock<std::mutex> lk(mux_);
    if (sync_info_)
    {
        while (!sync_info_->pending_sync_task_.empty())
        {
            sync_info_->pending_sync_task_.front()->SetError(
                CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            sync_info_->pending_sync_task_.pop();
        }
        sync_info_ = nullptr;
    }
}

void TableRangeEntry::DropStoreRangeAndSyncInfo()
{
    // No one should be trying to load slice info when we drop
    // the range slices.
    std::unique_lock<std::mutex> lk(mux_);
    assert(fetch_range_slices_req_ == nullptr);
    range_slices_ = nullptr;
    if (sync_info_)
    {
        while (!sync_info_->pending_sync_task_.empty())
        {
            sync_info_->pending_sync_task_.front()->SetError(
                CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            sync_info_->pending_sync_task_.pop();
        }
        sync_info_ = nullptr;
    }
}

void TableRangeEntry::PopPendingSyncTask()
{
    std::unique_lock<std::mutex> lk(mux_);
    assert(sync_info_ != nullptr);
    if (!sync_info_->pending_sync_task_.empty())
    {
        sync_info_->pending_sync_task_.front()->on_remove_pending_queue_lambda_(
            sync_info_->pending_sync_task_.front());
        sync_info_->pending_sync_task_.pop();
    }
}

void TableRangeEntry::FetchRangeSlices(const TableName &range_tbl_name,
                                       CcRequestBase *requester,
                                       NodeGroupId ng_id,
                                       int64_t ng_term,
                                       CcShard *cc_shard)
{
    std::unique_lock<std::mutex> lk(mux_);
    if (range_slices_ != nullptr)
    {
        cc_shard->Enqueue(requester);
        return;
    }
    if (!fetch_range_slices_req_)
    {
        fetch_range_slices_req_ = std::make_unique<FetchRangeSlicesCc>(
            range_tbl_name, this, ng_id, ng_term);
    }
    fetch_range_slices_req_->AddRequester(requester, cc_shard);
    if (fetch_range_slices_req_->RequesterCount() == 1)
    {
        Sharder::Instance().GetLocalCcShards()->store_hd_->FetchRangeSlices(
            fetch_range_slices_req_.get());
    }
}
};  // namespace txservice