#include "cc/cc_map.h"

#include "cc/local_cc_shards.h"

namespace txservice
{
void CcMap::MoveRequest(CcRequestBase *cc_req, uint32_t target_core_id)
{
    shard_->local_shards_.EnqueueCcRequest(
        shard_->core_id_, target_core_id, cc_req);
}
}  // namespace txservice