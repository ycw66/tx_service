#pragma once

#include <condition_variable>
#include <deque>
#include <mutex>
#include <thread>
#include <vector>

#include "catalog_factory.h"
#include "cc_handler_result.h"
#include "tx_key.h"
#include "tx_record.h"
#include "type.h"

namespace txservice
{
class LocalCcShards;
struct RangeMedianKeyResult;

struct DsFindRangeMedianKeyWorkSettings
{
    DsFindRangeMedianKeyWorkSettings(
        int32_t partition_id,
        const TableSchema *table_schema,
        CcHandlerResult<RangeMedianKeyResult> *hd_res);

    int32_t partition_id_;
    const TableSchema *table_schema_;
    CcHandlerResult<RangeMedianKeyResult> *hd_res_;
};

struct DsCopyRangeDataWorkSettings
{
    DsCopyRangeDataWorkSettings(int32_t old_partition_id,
                                int32_t new_partition_id,
                                const TxKey *start_key,
                                uint64_t tx_ts,
                                const TableSchema *table_schema,
                                CcHandlerResult<Void> *hd_res);

    int32_t old_partition_id_;
    int32_t new_partition_id_;
    const TxKey *start_key_;
    uint64_t tx_ts_;
    const TableSchema *table_schema_;
    CcHandlerResult<Void> *hd_res_;
};

struct DsUpsertRangeWorkingSetting
{
    DsUpsertRangeWorkingSetting(const TableSchema *table_schema,
                                TxKey *key,
                                int32_t partition_id,
                                int64_t ts,
                                CcHandlerResult<Void> *hd_result);

    const TableSchema *table_schema_;
    TxKey *key_;
    int32_t partition_id_;
    int64_t ts_;
    CcHandlerResult<Void> *hd_res_;
};

struct DsDeleteOutOfRangeDataWorkSettings
{
    DsDeleteOutOfRangeDataWorkSettings(int32_t partition_id,
                                       const TxKey *start_key,
                                       const TableSchema *table_schema,
                                       CcHandlerResult<Void> *hd_res);

    int32_t partition_id_;
    const TxKey *start_key_;
    const TableSchema *table_schema_;
    CcHandlerResult<Void> *hd_res_;
};

class DsRangeSplitOperationService
{
public:
    DsRangeSplitOperationService(const LocalCcShards &local_shards);
    ~DsRangeSplitOperationService() = default;

    void Shutdown();

    void SubmitFindRangeMedianKeyWork(
        int32_t partition_id,
        const TableSchema *table_schema,
        CcHandlerResult<RangeMedianKeyResult> *hd_res);

    void SubmitCopyRangeDataWork(int32_t old_partition_id,
                                 int32_t new_partition_id,
                                 const TxKey *start_key,
                                 uint64_t tx_ts,
                                 const TableSchema *table_schema,
                                 CcHandlerResult<Void> *hd_res);

    void SubmitDeleteOutOfRangeDataWork(int32_t partition_id,
                                        const TxKey *start_key,
                                        const TableSchema *table_schema,
                                        CcHandlerResult<Void> *hd_res);

    void SubmitUpsertRangeWork(const TableSchema *table_schema,
                               TxKey *key,
                               int32_t partition_id,
                               int64_t ts,
                               CcHandlerResult<Void> *hd_result);

private:
    const LocalCcShards &local_cc_shards_;
    std::thread worker_thread_;
    std::deque<DsFindRangeMedianKeyWorkSettings>
        ds_find_range_median_key_work_queue_;
    std::deque<DsCopyRangeDataWorkSettings> ds_copy_range_data_work_queue_;
    std::deque<DsDeleteOutOfRangeDataWorkSettings>
        ds_delete_out_of_range_data_work_queue_;
    std::deque<DsUpsertRangeWorkingSetting> ds_upsert_range_work_queue_;
    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
    std::atomic<bool> quit_indicator_{false};
};
}  // namespace txservice
