#pragma once

#include <condition_variable>
#include <mutex>

#include "cc_handler_result.h"
#include "sharder.h"
#include "type.h"

namespace txservice
{
struct DataSyncStatus
{
    explicit DataSyncStatus(bool need_truncate_log)
        : need_truncate_log_(need_truncate_log)
    {
    }

    void SetNoTruncateLog()
    {
        std::lock_guard<std::mutex> lk(mux_);
        need_truncate_log_ = false;
    }

    int32_t unfinished_tasks_{0};
    bool all_task_started_{false};
    CcErrorCode err_code_{CcErrorCode::NO_ERROR};
    // True if need to truncate redo log when all tasks succeed.
    bool need_truncate_log_{true};
    uint64_t truncate_log_ts_{0};
    std::mutex mux_;
    std::condition_variable cv_;
};

struct DataSyncTask
{
public:
    DataSyncTask(const TableName &table_name,
                 int32_t range_id,
                 uint64_t range_version,
                 uint32_t ng_id,
                 int64_t ng_term,
                 uint64_t data_sync_ts,
                 std::shared_ptr<DataSyncStatus> status,
                 bool is_dirty,
                 bool need_adjust_ts,
                 CcHandlerResult<Void> *hres
#ifndef RANGE_PARTITION_ENABLED
                 ,
                 std::function<bool(size_t)> filter_lambda
#endif
                 )
        : table_name_(table_name),
          range_id_(range_id),
          range_version_(range_version),
          node_group_id_(ng_id),
          node_group_term_(ng_term),
          data_sync_ts_(data_sync_ts)
#ifndef RANGE_PARTITION_ENABLED
          ,
          filter_lambda_(filter_lambda)
#endif
          ,
          status_(status),
          is_dirty_(is_dirty),
          sync_ts_adjustable_(need_adjust_ts),
          task_res_(hres)
    {
    }

    void SetFinish()
    {
        std::unique_lock<std::mutex> task_sender_lk(status_->mux_);
        status_->unfinished_tasks_--;
        // The default value of `truncate_log_ts_` is `0`.
        if (status_->truncate_log_ts_ == 0)
        {
            status_->truncate_log_ts_ = data_sync_ts_;
        }
        else
        {
            // Update minimum checkpoint timestamp. We use this timestamp to
            // truncate log at the end.
            status_->truncate_log_ts_ =
                std::min(status_->truncate_log_ts_, data_sync_ts_);
        }

        if (status_->unfinished_tasks_ == 0 && status_->all_task_started_)
        {
            if (status_->need_truncate_log_)
            {
                if (status_->err_code_ == CcErrorCode::NO_ERROR)
                {
                    // Truncate redo log
                    LOG(INFO) << "Checkpoint of node group #" << node_group_id_
                              << " succeeded with timestamp: "
                              << status_->truncate_log_ts_;
                    Sharder::Instance().UpdateNodeGroupCkptTs(
                        node_group_id_, status_->truncate_log_ts_);
                    Sharder::Instance().GetLogAgent()->UpdateCheckpointTs(
                        node_group_id_,
                        node_group_term_,
                        status_->truncate_log_ts_);
                }
                else
                {
                    LOG(INFO) << "Checkpoint of node group #" << node_group_id_
                              << " finished with timestamp: " << data_sync_ts_
                              << " with result code: "
                              << static_cast<uint32_t>(status_->err_code_);
                }
            }

            if (task_res_)
            {
                if (status_->err_code_ == CcErrorCode::NO_ERROR)
                {
                    task_res_->SetFinished();
                }
                else
                {
                    task_res_->SetError(status_->err_code_);
                }
            }
            status_->cv_.notify_all();
        }
    }

    void SetError(CcErrorCode err_code = CcErrorCode::DATA_STORE_ERR)
    {
        std::unique_lock<std::mutex> task_sender_lk(status_->mux_);
        status_->unfinished_tasks_--;
        status_->err_code_ = err_code;
        // The default value of `truncate_log_ts_` is `0`.
        if (status_->truncate_log_ts_ == 0)
        {
            status_->truncate_log_ts_ = data_sync_ts_;
        }
        else
        {
            // Update minimum checkpoint timestamp. We use this timestamp to
            // truncate log at the end.
            status_->truncate_log_ts_ =
                std::min(status_->truncate_log_ts_, data_sync_ts_);
        }

        if (status_->unfinished_tasks_ == 0 && status_->all_task_started_)
        {
            if (task_res_)
            {
                task_res_->SetError(status_->err_code_);
            }
            status_->cv_.notify_all();
        }
    }

    void SetErrorCode(CcErrorCode err_code)
    {
        std::unique_lock<std::mutex> lk(status_->mux_);
        status_->err_code_ = err_code;
    }

    bool SyncTsAdjustable() const
    {
        return sync_ts_adjustable_;
    }

    void UnsetSyncTsAdjustable()
    {
        sync_ts_adjustable_ = false;
    }

    const TableName table_name_;
    int32_t range_id_;
    uint64_t range_version_;
    uint32_t node_group_id_;
    int64_t node_group_term_{-1};
    uint64_t data_sync_ts_{0};

#ifndef RANGE_PARTITION_ENABLED
    enum class CkptErrorCode
    {
        NO_ERROR = 0,
        // Failed on data sync scan
        SCAN_ERROR,
        // Failed on flush data
        FLUSH_ERROR,
    };

    std::mutex flight_task_mux_;
    std::condition_variable flight_task_cv_;
    // Flush data task cnt + 1 (Data sync task)
    int64_t flight_task_cnt_{0};
    CkptErrorCode ckpt_err_{CkptErrorCode::NO_ERROR};
    std::function<bool(size_t)> filter_lambda_;
#endif

    std::shared_ptr<DataSyncStatus> status_{nullptr};
    // True if need to use the dirty schema.
    bool is_dirty_{false};
    // The PendingTaskQueue allows only one normal checkpoint task. Subsequent
    // tasks with larger timestamps will not be added to the PendingTaskqueue.
    // Instead, it will only update the latest_pending_ts_. When the task in the
    // queue is executed, the data_sync_ts_ of task will be updated using
    // latest_pending_ts_.
    bool sync_ts_adjustable_{true};
    // Indicate the single task result.
    CcHandlerResult<Void> *task_res_{nullptr};
};
}  // namespace txservice