#include "range_record.h"

#include <mutex>

#include "local_cc_shards.h"
#include "range_slice.h"
#include "type.h"

namespace txservice
{

TableRangeEntry::~TableRangeEntry()
{
}

bool TableRangeEntry::DropStoreRangeAndSyncInfo(size_t &mem_decreased)
{
    std::unique_lock<std::shared_mutex> lk(mux_);
    mem_decreased = 0;
    if (range_slices_)
    {
        if (range_slices_->Pins() == 0)
        {
            mem_decreased = range_slices_->MemUsage();
            range_slices_ = nullptr;
        }
        else
        {
            return false;
        }
    }
    else if (fetch_range_slices_req_ != nullptr)
    {
        // This function is only called during bucket migration and we're
        // cleaning up range slices that are migrated away. In this case we
        // should make sure that no range slices in this bucket is loaded into
        // memory after this function returns true. So we need to wait til the
        // current fetch req is finished.
        return false;
    }

    return true;
}

void TableRangeEntry::FetchRangeSlices(const TableName &range_tbl_name,
                                       CcRequestBase *requester,
                                       NodeGroupId ng_id,
                                       int64_t ng_term,
                                       CcShard *cc_shard)
{
    std::unique_lock<std::shared_mutex> lk(mux_);
    if (range_slices_ != nullptr)
    {
        cc_shard->Enqueue(requester);
        return;
    }
    if (!fetch_range_slices_req_)
    {
        fetch_range_slices_req_ = std::make_unique<FetchRangeSlicesReq>(
            range_tbl_name, this, ng_id, ng_term);
    }
    fetch_range_slices_req_->AddRequester(requester, cc_shard);
    if (fetch_range_slices_req_->RequesterCount() == 1)
    {
        lk.unlock();
        Sharder::Instance().GetLocalCcShards()->store_hd_->FetchRangeSlices(
            fetch_range_slices_req_.get());
    }
}

int64_t TableRangeEntry::InitRangeSlices(
    std::vector<std::pair<TxKey::Uptr, uint32_t>> &&slices,
    NodeGroupId ng_id,
    bool fully_cached)
{
    auto range_slices =
        std::make_unique<StoreRange>(range_info_->StartKey(),
                                     range_info_->EndKey(),
                                     range_info_->PartitionId(),
                                     ng_id,
                                     *Sharder::Instance().GetLocalCcShards());
    range_slices->InitSlices(std::move(slices), fully_cached);
    size_t old_size = 0;
    if (range_slices_)
    {
        old_size = range_slices_->MemUsage();
    }
    size_t current_size = range_slices->MemUsage();
    range_slices_ = std::move(range_slices);
    return current_size - old_size;
}
};  // namespace txservice