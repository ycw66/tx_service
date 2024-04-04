#pragma once

#include <glog/logging.h>

#include <vector>

#include "cc_req_pool.h"
#include "local_cc_shards.h"
#include "read_write_entry.h"
#include "rpc_closure.h"
#include "tx_request.h"
#include "type.h"

namespace txservice
{
class TransactionExecution;

class SkGenerator
{
    using NGWriteEntry =
        std::unordered_map<NodeGroupId, std::vector<WriteEntry *>>;
#ifdef NDEBUG
    static constexpr uint16_t UploadTimeout = 10000;  // ms
    static constexpr size_t UploadBatchWorkerSize = 5;
#else
    static constexpr uint16_t UploadTimeout = 1000;  // ms
    static constexpr size_t UploadBatchWorkerSize = 2;
#endif

public:
    SkGenerator(const TableName &base_table_name,
                NodeGroupId node_group_id,
                int32_t partition_id)
        : base_table_name_(base_table_name),
          node_group_id_(node_group_id),
          partition_id_(partition_id)
    {
    }
    ~SkGenerator() = default;

    void GenerateSkFromPk(const TxKey *start_key,
                          const TxKey *end_key,
                          uint64_t scan_ts,
                          std::vector<TableName> &new_indexes_name,
                          size_t &scanned_pk_count,
                          CcErrorCode &res_code,
                          GenerateSkStatus &task_status);

    void RemoteGenerateSkFromPk(const std::string &start_key_str,
                                const std::string &end_key_str,
                                uint64_t scan_ts,
                                std::vector<TableName> &new_indexes_name,
                                size_t &scanned_pk_count,
                                CcErrorCode &res_code,
                                GenerateSkStatus &task_status);

    const std::vector<int64_t> &NodeGroupTerms() const
    {
        return leader_terms_;
    }

private:
    enum struct UploadTaskStatus
    {
        Free = 0,
        Ongoing,
        Pending
    };

    struct UploadBatchTask
    {
        // The original WriteEntry set for each index table.
        std::unordered_map<TableName, std::vector<WriteEntry>> write_entry_set_;
        UploadTaskStatus task_status_{UploadTaskStatus::Free};
    };

    /**
     * @brief Scan pk items, and generate sk items.
     *
     * @return The result code.
     */
    CcErrorCode ScanPkAndGenerateSk(
        const TxKey *start_key,
        const TxKey *end_key,
        uint64_t scan_ts,
        int64_t ng_term,
        uint64_t tx_number,
        const std::vector<TableName> &new_indexes_name,
        size_t &scanned_pk_count,
        GenerateSkStatus &task_status);
    CcErrorCode UploadWithoutDataLog(
        UploadBatchTask &upload_task,
        std::vector<std::unique_ptr<UploadBatchCc>> &upload_req_pool);
    CcErrorCode UploadSkInternal(
        std::unordered_map<TableName, NGWriteEntry> &ng_write_set,
        std::vector<std::unique_ptr<UploadBatchCc>> &upload_req_pool);
    void UploadBatch(
        const TableName &table_name,
        NodeGroupId dest_ng_id,
        int64_t &ng_term,
        const std::vector<WriteEntry *> &write_entry_vec,
        size_t batch_size,
        size_t start_key_idx,
        bthread::Mutex &req_mux,
        bthread::ConditionVariable &req_cv,
        size_t &finished_req_cnt,
        CcErrorCode &res_code,
        std::vector<std::unique_ptr<UploadBatchCc>> &upload_req_pool);
    // Acquire and release range read lock.
    CcErrorCode AcquireRangeReadLocks(
        TransactionExecution *acq_lock_txm,
        UploadBatchTask &upload_task,
        std::unordered_map<TableName, NGWriteEntry> &ng_write_set);
    void ReleaseRangeReadLocks(TransactionExecution *acq_lock_txm,
                               bool is_success);
    void AdvanceWriteEntryForRangeInfo(
        const RangeRecord &range_record,
        std::vector<WriteEntry>::iterator &cur_write_entry_it,
        const std::vector<WriteEntry>::iterator &write_entry_end,
        NGWriteEntry &ng_write_entrys);
    void UploadBatchWorker();

    const TableName &base_table_name_;
    NodeGroupId node_group_id_{0};
    int32_t partition_id_{0};
    WorkerThreadContext upload_batch_worker_ctx_{UploadBatchWorkerSize};
    std::array<UploadBatchTask, UploadBatchWorkerSize> upload_batch_queue_;
    uint8_t pending_upload_task_size_{0};
    uint8_t upload_task_head_{UINT8_MAX};
    std::mutex upload_sender_mux_;
    std::condition_variable upload_sender_cv_;
    uint8_t ongoing_upload_task_size_{0};
    CcErrorCode upload_task_result_{CcErrorCode::NO_ERROR};
    size_t scan_batch_size_{LocalCcShards::DATA_SYNC_SCAN_BATCH_SIZE};
    // Store the node group leader terms after acquired them.
    std::vector<int64_t> leader_terms_;
    uint32_t upload_batch_size_{128};
};

}  // namespace txservice