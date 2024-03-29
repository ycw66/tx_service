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
#else
    static constexpr uint16_t UploadTimeout = 1000;  // ms
#endif

public:
    SkGenerator() = default;
    ~SkGenerator() = default;

    void GenerateSkFromPk(const TableName &table_name,
                          int32_t partition_id,
                          const TxKey *start_key,
                          const TxKey *end_key,
                          NodeGroupId range_owner,
                          uint64_t scan_ts,
                          std::vector<TableName> &new_indexes_name,
                          uint32_t &scanned_pk_count,
                          CcErrorCode &res_code,
                          GenerateSkStatus &task_status);

    void RemoteGenerateSkFromPk(const TableName &table_name,
                                int32_t partition_id,
                                const std::string &start_key_str,
                                const std::string &end_key_str,
                                NodeGroupId ng_id,
                                uint64_t scan_ts,
                                std::vector<TableName> &new_indexes_name,
                                uint32_t &scanned_pk_count,
                                CcErrorCode &res_code,
                                GenerateSkStatus &task_status);

    const std::vector<int64_t> &NodeGroupTerms() const
    {
        return leader_terms_;
    }

private:
    /**
     * @brief Scan pk items, and generate sk items.
     *
     * @return A pair, of which the first element is the total items count that
     * scanned from pk, and the second is the result code.
     */
    std::pair<size_t, CcErrorCode> ScanPkAndGenerateSk(
        const TableName &table_name,
        NodeGroupId range_owner,
        const std::vector<TableName> &new_indexes_name,
        DataSyncScanCc &scan_req,
        GenerateSkStatus &task_status);
    CcErrorCode UploadWithoutDataLog(NodeGroupId ng_id,
                                     GenerateSkStatus &task_status);
    CcErrorCode UploadSkInternal();
    void UploadBatch(const TableName &table_name,
                     NodeGroupId dest_ng_id,
                     int64_t &ng_term,
                     const std::vector<WriteEntry *> &write_entry_vec,
                     size_t batch_size,
                     size_t start_key_idx,
                     bthread::Mutex &req_mux,
                     bthread::ConditionVariable &req_cv,
                     size_t &finished_req_cnt,
                     CcErrorCode &res_code);
    // Acquire and release range read lock.
    CcErrorCode AcquireRangeReadLocks(TransactionExecution *acq_lock_txm);
    void ReleaseRangeReadLocks(TransactionExecution *acq_lock_txm,
                               bool is_success);
    void AdvanceWriteEntryForRangeInfo(
        const RangeRecord &range_record,
        std::vector<WriteEntry>::iterator &cur_write_entry_it,
        const std::vector<WriteEntry>::iterator &write_entry_end,
        NGWriteEntry &ng_write_entrys);
    uint8_t SleepDuration()
    {
        uint16_t new_duration = sleep_duration_ + 60;
        sleep_duration_ = new_duration > 240 ? 240 : new_duration;
        return sleep_duration_;
    }
    bool UpdateUploadBatchSize(uint32_t max_size)
    {
        assert(max_size > 0);
        uint32_t new_batch_size = upload_batch_size_ * 4;
        upload_batch_size_ =
            new_batch_size > max_size ? max_size : new_batch_size;
        return new_batch_size <= max_size;
    }

    // The original WriteEntry set for each index table.
    std::unordered_map<TableName, std::vector<WriteEntry>> write_entry_set_;
    // The relationship between one WriteEntry and another, this indicate that
    // for the specific node group, which TxKeys belong to it.
    std::unordered_map<TableName, NGWriteEntry> ng_write_entry_set_;
    // Store the node group leader terms after acquired them.
    std::vector<int64_t> leader_terms_;
    // For each node group, and each index table
    std::vector<std::unique_ptr<UploadBatchCc>> upload_batch_req_vec_;
    uint32_t upload_batch_size_{128};
    uint8_t sleep_duration_{0};
};

}  // namespace txservice