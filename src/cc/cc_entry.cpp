#include "cc/cc_entry.h"

#include "cc/cc_shard.h"

namespace txservice
{
LruEntry::LruEntry(CcMap *parent) : parent_map_(parent)
{
    if (parent != nullptr)
    {
        uint64_t now_ts = parent->shard_->Now();
        last_vali_ts_ = now_ts;
        gap_last_vali_ts_ = now_ts;
    }
    else
    {
        last_vali_ts_ = 1;
        gap_last_vali_ts_ = 1;
    }
}

bool LruEntry::IsFree() const
{
    return key_lock_.IsEmpty() && gap_lock_.IsEmpty() &&
           commit_ts_ <= ckpt_ts_.load(std::memory_order_acquire);
}
}  // namespace txservice