#include "ds_range_split_service.h"

#include "local_cc_shards.h"

namespace txservice
{
DsFindRangeMedianKeyWorkSettings::DsFindRangeMedianKeyWorkSettings(
    const txservice::TableName &table_name,
    int32_t partition_id,
    const txservice::Schema *key_schema,
    CcHandlerResult<RangeMedianKeyResult> *hd_res)
    : table_name_(table_name),
      partition_id_(partition_id),
      key_schema_(key_schema),
      hd_res_(hd_res)
{
}

DsCopyRangeDataWorkSettings::DsCopyRangeDataWorkSettings(
    const txservice::TableName &table_name,
    int32_t old_partition_id,
    int32_t new_partition_id,
    const TxKey *start_key,
    uint64_t tx_ts,
    const txservice::Schema *key_schema,
    const txservice::Schema *rec_schema,
    CcHandlerResult<Void> *hd_res)
    : table_name_(table_name),
      old_partition_id_(old_partition_id),
      new_partition_id_(new_partition_id),
      start_key_(start_key),
      tx_ts_(tx_ts),
      key_schema_(key_schema),
      rec_schema_(rec_schema),
      hd_res_(hd_res)
{
}

DsUpsertRangeWorkingSetting::DsUpsertRangeWorkingSetting(
    const TableName &range_table_name,
    const Schema *key_schema,
    TxKey *key,
    int32_t partition_id,
    int64_t ts,
    CcHandlerResult<Void> *hd_result)
    : range_table_name_(range_table_name),
      key_schema_(key_schema),
      key_(key),
      partition_id_(partition_id),
      ts_(ts),
      hd_res_(hd_result)
{
}

DsDeleteOutOfRangeDataWorkSettings::DsDeleteOutOfRangeDataWorkSettings(
    const txservice::TableName &table_name,
    int32_t partition_id,
    const TxKey *start_key,
    const txservice::Schema *key_schema,
    CcHandlerResult<Void> *hd_res)
    : table_name_(table_name),
      partition_id_(partition_id),
      start_key_(start_key),
      key_schema_(key_schema),
      hd_res_(hd_res)
{
}

DsRangeSplitOperationService::DsRangeSplitOperationService(
    const txservice::LocalCcShards &local_shards)
    : local_cc_shards_(local_shards), quit_indicator_(false)
{
    worker_thread_ = std::thread(
        [this]
        {
            while (!quit_indicator_.load(std::memory_order_acquire))
            {
                std::unique_lock<std::mutex> lk(queue_mutex_);
                queue_cv_.wait(
                    lk,
                    [this]
                    {
                        return !ds_find_range_median_key_work_queue_.empty() ||
                               !ds_copy_range_data_work_queue_.empty() ||
                               !ds_delete_out_of_range_data_work_queue_
                                    .empty() ||
                               !ds_upsert_range_work_queue_.empty() ||
                               quit_indicator_.load(std::memory_order_acquire);
                    });

                if (quit_indicator_.load(std::memory_order_acquire))
                {
                    lk.unlock();
                    break;
                }
                if (!ds_find_range_median_key_work_queue_.empty())
                {
                    DsFindRangeMedianKeyWorkSettings ws =
                        ds_find_range_median_key_work_queue_.front();
                    ds_find_range_median_key_work_queue_.pop_front();
                    lk.unlock();

                    bool succ = local_cc_shards_.store_hd_->FindRangeMedianKey(
                        ws.table_name_,
                        ws.partition_id_,
                        ws.key_schema_,
                        ws.hd_res_);
                    if (!succ)
                    {
                        ws.hd_res_->SetError(-1);
                    }
                    else
                    {
                        succ =
                            local_cc_shards_.store_hd_->GetNextRangePartitionId(
                                ws.table_name_,
                                &ws.hd_res_->Value().new_partition_id_);
                        if (succ)
                        {
                            ws.hd_res_->SetFinished();
                        }
                        else
                        {
                            ws.hd_res_->SetError(-1);
                        }
                    }
                }
                else if (!ds_copy_range_data_work_queue_.empty())
                {
                    DsCopyRangeDataWorkSettings ws =
                        ds_copy_range_data_work_queue_.front();
                    ds_copy_range_data_work_queue_.pop_front();
                    lk.unlock();

                    bool succ = local_cc_shards_.store_hd_->CopyRangeData(
                        ws.table_name_,
                        ws.old_partition_id_,
                        ws.new_partition_id_,
                        ws.start_key_,
                        ws.tx_ts_,
                        ws.key_schema_,
                        ws.rec_schema_);
                    if (succ)
                    {
                        ws.hd_res_->SetFinished();
                    }
                    else
                    {
                        ws.hd_res_->SetError(-1);
                    }
                }
                else if (!ds_delete_out_of_range_data_work_queue_.empty())
                {
                    DsDeleteOutOfRangeDataWorkSettings ws =
                        ds_delete_out_of_range_data_work_queue_.front();
                    ds_delete_out_of_range_data_work_queue_.pop_front();
                    lk.unlock();

                    bool succ =
                        local_cc_shards_.store_hd_->DeleteOutOfRangeData(
                            ws.table_name_,
                            ws.partition_id_,
                            ws.start_key_,
                            ws.key_schema_);
                    if (succ)
                    {
                        ws.hd_res_->SetFinished();
                    }
                    else
                    {
                        ws.hd_res_->SetError(-1);
                    }
                }
                else if (!ds_upsert_range_work_queue_.empty())
                {
                    DsUpsertRangeWorkingSetting ws =
                        ds_upsert_range_work_queue_.front();
                    ds_upsert_range_work_queue_.pop_front();
                    lk.unlock();

                    bool succ = local_cc_shards_.store_hd_->UpsertRange(
                        ws.range_table_name_,
                        ws.key_schema_,
                        ws.key_,
                        ws.partition_id_,
                        ws.ts_);
                    if (succ)
                    {
                        ws.hd_res_->SetFinished();
                    }
                    else
                    {
                        ws.hd_res_->SetError(-1);
                    }
                }
            }
        });
}

void DsRangeSplitOperationService::SubmitFindRangeMedianKeyWork(
    const txservice::TableName &table_name,
    int32_t partition_id,
    const txservice::Schema *key_schema,
    CcHandlerResult<RangeMedianKeyResult> *hd_res)
{
    std::unique_lock lk(queue_mutex_);
    ds_find_range_median_key_work_queue_.emplace_back(
        table_name, partition_id, key_schema, hd_res);
    queue_cv_.notify_one();
}

void DsRangeSplitOperationService::SubmitCopyRangeDataWork(
    const txservice::TableName &table_name,
    int32_t old_partition_id,
    int32_t new_partition_id,
    const TxKey *start_key,
    uint64_t tx_ts,
    const txservice::Schema *key_schema,
    const txservice::Schema *rec_schema,
    CcHandlerResult<Void> *hd_res)
{
    std::unique_lock lk(queue_mutex_);
    ds_copy_range_data_work_queue_.emplace_back(table_name,
                                                old_partition_id,
                                                new_partition_id,
                                                start_key,
                                                tx_ts,
                                                key_schema,
                                                rec_schema,
                                                hd_res);
    queue_cv_.notify_one();
}

void DsRangeSplitOperationService::SubmitUpsertRangeWork(
    const TableName &range_table_name,
    const Schema *key_schema,
    TxKey *key,
    int32_t partition_id,
    int64_t ts,
    CcHandlerResult<Void> *hd_result)
{
    std::unique_lock lk(queue_mutex_);
    ds_upsert_range_work_queue_.emplace_back(
        range_table_name, key_schema, key, partition_id, ts, hd_result);
    queue_cv_.notify_one();
}

void DsRangeSplitOperationService::SubmitDeleteOutOfRangeDataWork(
    const txservice::TableName &table_name,
    int32_t partition_id,
    const TxKey *start_key,
    const txservice::Schema *key_schema,
    CcHandlerResult<Void> *hd_res)
{
    std::unique_lock lk(queue_mutex_);
    ds_delete_out_of_range_data_work_queue_.emplace_back(
        table_name, partition_id, start_key, key_schema, hd_res);
    queue_cv_.notify_one();
}

void DsRangeSplitOperationService::Shutdown()
{
    {
        std::unique_lock<std::mutex> lk(queue_mutex_);
        quit_indicator_.store(true, std::memory_order_release);
        queue_cv_.notify_one();
    }
    worker_thread_.join();
}
}  // namespace txservice
