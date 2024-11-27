#pragma once

#include <bthread/condition_variable.h>

#include <condition_variable>
#include <functional>
#include <mutex>

#include "cc_handler_result.h"
#include "cc_req_misc.h"
#include "sharder.h"
#include "tx_key.h"
#include "type.h"

namespace txservice
{
extern bool txservice_skip_wal;

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

struct TableRangeEntry;

struct DataSyncTask
{
public:
    DataSyncTask(const TableName &table_name,
                 int32_t range_id,
                 uint64_t range_version,
                 uint32_t ng_id,
                 int64_t ng_term,
                 uint64_t data_sync_ts,
                 uint64_t flush_data_mem_quote,
                 std::shared_ptr<DataSyncStatus> status,
                 bool is_dirty,
                 bool need_adjust_ts,
                 CcHandlerResult<Void> *hres
#ifndef RANGE_PARTITION_ENABLED
                 ,
                 std::function<bool(size_t)> filter_lambda,
                 bool forward_cache,
                 bool is_standby_node_ckpt
#endif
                 )
        : table_name_(table_name),
          range_id_(range_id),
          range_version_(range_version),
          node_group_id_(ng_id),
          node_group_term_(ng_term),
          data_sync_ts_(data_sync_ts),
          flush_data_mem_quote_(flush_data_mem_quote)
#ifndef RANGE_PARTITION_ENABLED
          ,
          filter_lambda_(filter_lambda),
          forward_cache_(forward_cache),
          is_standby_node_ckpt_(is_standby_node_ckpt)
#endif
          ,
          status_(status),
          is_dirty_(is_dirty),
          sync_ts_adjustable_(need_adjust_ts),
          task_res_(hres)
    {
    }

#ifdef RANGE_PARTITION_ENABLED
    DataSyncTask(const TableName &table_name,
                 uint32_t ng_id,
                 int64_t ng_term,
                 TableRangeEntry *range_entry,
                 const TxKey &start_key,
                 const TxKey &end_key,
                 uint64_t data_sync_ts,
                 bool is_dirty,
                 bool export_base_table_items,
                 uint64_t txn,
                 std::shared_ptr<DataSyncStatus> status,
                 CcHandlerResult<Void> *hres);
#endif

    void SetFinish();

    void SetError(CcErrorCode err_code = CcErrorCode::DATA_STORE_ERR);

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

    // Function to allocate memory quote
    uint64_t AllocateFlushDataMemQuote(uint64_t quote)
    {
        std::unique_lock<bthread::Mutex> lk(mem_mutex_);

        // Lambda to check if there's enough available memory
        auto has_enough_memory = [this, quote]()
        {
            // if quote is avaliable
            if ((flush_data_mem_usage_ + quote) <= flush_data_mem_quote_)
            {
                return true;
            }
            // Or a single object quote is bigger than
            // overall quote which means the object is also bigger than scan
            // heap limit, we have to allow it to be flushed, otherwise it will
            // block ckpt
            if (quote > flush_data_mem_quote_)
            {
                LOG(WARNING)
                    << "Flush object is too large (size: " << quote
                    << ") which excceds the flush data mem quote (size: "
                    << flush_data_mem_quote_ << ")";
                return true;
            }

            return false;
        };

        // Wait until enough memory is available
        while (!has_enough_memory())
        {
            DLOG(INFO) << "Flush data memory quote is full "
                       << flush_data_mem_usage_ << " ,request quote: " << quote
                       << " total quote: " << flush_data_mem_quote_
                       << " ,flight task cnt: " << flight_task_cnt_;
            mem_cv_.wait(lk);
        }

        // Allocate the memory quote
        uint64_t old_usage = flush_data_mem_usage_;
        flush_data_mem_usage_ += quote;
        return old_usage;
    }

    // return the quote to flush data memory usage pool and notify waiting data
    // sync thread
    uint64_t DeallocateFlushMemQuote(uint64_t quote)
    {
        std::lock_guard<bthread::Mutex> lock(mem_mutex_);

        assert(quote <= flush_data_mem_usage_);

        // Deallocate the memory quote
        uint64_t old_usage = flush_data_mem_usage_;
        flush_data_mem_usage_ -= quote;

        // Notify all waiting threads that memory has been freed
        mem_cv_.notify_one();

        return old_usage;
    }

    uint64_t FlushMemQuote() const
    {
        return flush_data_mem_quote_;
    }

    const TableName table_name_;
    int32_t range_id_;
    uint64_t range_version_;
    uint32_t node_group_id_;
    int64_t node_group_term_{-1};
    uint64_t data_sync_ts_{0};

    enum class CkptErrorCode
    {
        NO_ERROR = 0,
        // Failed on data sync scan
        SCAN_ERROR,
        // Failed on flush data
        FLUSH_ERROR,
    };

    bthread::Mutex flight_task_mux_;

    // Accumulated pending flush data memory usage for back pressure the
    // DataSyncScan
    // Synchronization primitives
    bthread::Mutex mem_mutex_;
    bthread::ConditionVariable mem_cv_;
    // Memory usage tracking
    uint64_t flush_data_mem_usage_{0};
    const uint64_t flush_data_mem_quote_{0};

    // Flush data task cnt + 1 (Data sync task)
    int64_t flight_task_cnt_{0};
    CkptErrorCode ckpt_err_{CkptErrorCode::NO_ERROR};
#ifndef RANGE_PARTITION_ENABLED
    std::function<bool(size_t)> filter_lambda_;
    bool forward_cache_{false};
    bool is_standby_node_ckpt_{false};
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

#ifdef RANGE_PARTITION_ENABLED
    const TxKey start_key_;
    const TxKey end_key_;
    TableRangeEntry *range_entry_{nullptr};
    bool during_split_range_{false};
    bool export_base_table_items_{false};
    uint64_t tx_number_{0};
#endif
};
}  // namespace txservice
