#pragma once

#include "cc_map.h"
#include "template_cc_map.h"
#include "tx_key.h"
#include "tx_record.h"

namespace txservice
{
class ClusterConfigCcMap : public TemplateCcMap<VoidKey, VoidRecord>
{
public:
    ClusterConfigCcMap(const ClusterConfigCcMap &rhs) = delete;

    ClusterConfigCcMap(CcShard *shard,
                       NodeGroupId cc_ng_id,
                       const TableName &table_name)
        : TemplateCcMap<VoidKey, VoidRecord>(
              shard, cc_ng_id, table_name, 1, nullptr, true)
    {
        // We only store one record in ClusterConfigCcMap as neg_inf_ key. It is
        // is only used for concurrency control purpose.
        neg_inf_.payload_ = std::make_shared<VoidRecord>();
        neg_inf_.commit_ts_ = 0;
        neg_inf_.payload_status_ = RecordStatus::Normal;
    }
};
}  // namespace txservice