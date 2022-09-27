#pragma once

#include <condition_variable>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

#include "cc_handler_result.h"
#include "tx_key.h"
#include "tx_record.h"
#include "type.h"

namespace txservice
{
class LocalCcShards;

class DsEvaluateRangeSizeWorkSettings
{
public:
    DsEvaluateRangeSizeWorkSettings(const txservice::TableName &table_name,
                                    std::map<int32_t, const TxKey *> ranges)
        : table_name_(table_name.StringView(),
                      txservice::TableType::RangePartition),
          ranges_(std::move(ranges)){};

    DsEvaluateRangeSizeWorkSettings(DsEvaluateRangeSizeWorkSettings &&ws)
        : table_name_(ws.table_name_.StringView(),
                      txservice::TableType::RangePartition),
          ranges_(std::move(ws.ranges_)){};

    const txservice::TableName
        table_name_;  // not string owner, sv -> MysqlTableSchema
    std::map<int32_t /*range partition id*/, const TxKey * /*range key*/>
        ranges_;
};

class DsRangeEvaluateOperationService
{
public:
    DsRangeEvaluateOperationService(const LocalCcShards &local_shards);
    ~DsRangeEvaluateOperationService() = default;

    void Shutdown();

    void SubmitEvaluateRangeSizeWork(
        const txservice::TableName &range_table_name,
        std::map<int32_t /*range partition id*/, const TxKey * /*range key*/>
            &&ranges);

private:
    const LocalCcShards &local_cc_shards_;
    std::thread worker_thread_;
    std::deque<DsEvaluateRangeSizeWorkSettings>
        ds_evaluate_range_size_work_queue_;
    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
    std::atomic<bool> quit_indicator_{false};
};
}  // namespace txservice
