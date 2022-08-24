#pragma once

#include <memory>
#include <string>
#include <vector>

#include "catalog_key_record.h"
#include "cc_handler.h"
#include "log_closure.h"
#include "range_record.h"
#include "tx_key.h"
#include "tx_operation_result.h"
#include "tx_record.h"
#include "tx_req_result.h"

namespace txservice
{
class TransactionExecution;
struct ReadTxRequest;
struct ReadOutsideTxRequest;
struct ScanOpenTxRequest;
struct ScanNextTxRequest;

#define RETRY_NUM 5

enum class TxLogType
{
    DATA,
    PREPARE,
    COMMIT,
    CLEAN
};

struct TransactionOperation
{
    TransactionOperation()
    {
    }
    virtual ~TransactionOperation() = default;
    virtual void Forward(TransactionExecution *txm) = 0;

    /**
     * @brief If operation fails since remote node dies, auto-failover will
     * elect a new leader and recover the dead node group. Re-run the operator
     * automatically to avoid client to re-run the whole query.
     *
     */
    void ReRunOp(TransactionExecution *txm);

    int retry_num_{RETRY_NUM};
    bool is_running_{false};
};

struct CompositeTransactionOperation : TransactionOperation
{
    CompositeTransactionOperation();

    virtual ~CompositeTransactionOperation() = default;

    template <typename Op>
    void ForwardToSubOperation(TransactionExecution *txm, Op *next_op);

    template <typename Op>
    void RetrySubOperation(TransactionExecution *txm, Op *next_op);

    /**
     * @brief The current stage of this multi-stage schema operation
     */
    TransactionOperation *op_{nullptr};
};

struct ReadOperation : TransactionOperation
{
public:
    ReadOperation(TransactionExecution *txm);

    void Reset();
    void Forward(TransactionExecution *txm) override;

    ReadType read_type_{ReadType::Inside};
    CcProtocol protocol_{CcProtocol::OCC};
    IsolationLevel iso_level_{IsolationLevel::ReadCommitted};
    LockType lock_type_{LockType::ReadLock};
    ReadTxRequest *read_tx_req_{nullptr};
    ReadOutsideTxRequest *read_outside_tx_req_{nullptr};
    CcHandlerResult<ReadKeyResult> hd_result_;
};

struct SetCommitTsOperation : TransactionOperation
{
public:
    SetCommitTsOperation(TransactionExecution *txm);
    void Reset();
    void Forward(TransactionExecution *txm) override;

    CcHandlerResult<uint64_t> hd_result_;
};

struct ValidateOperation : TransactionOperation
{
public:
    static const uint32_t default_read_set_capacity = 16;

    ValidateOperation(TransactionExecution *txm);
    void Reset(size_t read_cnt);
    bool IsError();
    void Forward(TransactionExecution *txm) override;

    CcHandlerResult<PostProcessResult> hd_result_;
};

/**
 * @brief
 * Acquire write lock operation.
 * Write are cached in local rwset for each transaction, hence no write lock is
 * held at write operation. This operation is called right after Commit request.
 */
struct AcquireWriteOperation : TransactionOperation
{
public:
    AcquireWriteOperation(TransactionExecution *txm);
    void Reset(size_t acquire_write_cnt);
    void Reset();
    void AggregateAcquiredKeys(TransactionExecution *txm);
    void Forward(TransactionExecution *txm) override;

    CcHandlerResult<std::vector<AcquireKeyResult>> hd_result_;
    std::vector<WriteSetEntry *> acquire_write_entries_{16};
    // uint32_t acquire_write_cnt_{0};

    // Number of remote keys on which the acquire write operation needs to
    // acquire write intentions/locks.
    std::atomic<int32_t> remote_ack_cnt_{0};
    // Identify whether any keys in rset are expired (may be updated by other
    // tx) under the RepeatableRead or Serializable isolation level.
    bool rset_has_expired_{false};
};

struct FaultInjectOp : TransactionOperation
{
public:
    FaultInjectOp(TransactionExecution *txm);

    void Set(const std::string &fault_name,
             const std::string &fault_paras,
             std::vector<int> vct_node_id)
    {
        fault_name_ = fault_name;
        fault_paras_ = fault_paras;
        vct_node_id_ = vct_node_id;
        succeed_ = false;
    }

    void Reset();
    void Forward(TransactionExecution *txm) override;

    std::string fault_name_;
    std::string fault_paras_;
    std::vector<int> vct_node_id_;
    bool succeed_;
    CcHandlerResult<bool> hd_result_;
};

struct WriteToLogOp : TransactionOperation
{
    WriteToLogOp(TransactionExecution *txm);
    void Forward(TransactionExecution *txm) override;
    void Reset();

    TxLogType log_type_{TxLogType::DATA};
    uint32_t log_group_id_{0};
    CcHandlerResult<Void> hd_result_;
    LogClosure log_closure_{&hd_result_};
};

struct UpdateTxnStatus : TransactionOperation
{
    UpdateTxnStatus(TransactionExecution *txm);
    void Reset();
    void Forward(TransactionExecution *txm) override;

    CcHandlerResult<Void> hd_result_;
};

struct PostProcessOp : TransactionOperation
{
    PostProcessOp(TransactionExecution *txm);
    void Reset(size_t write_cnt, size_t read_cnt);
    void Forward(TransactionExecution *txm) override;

    CcHandlerResult<PostProcessResult> hd_result_;
    uint32_t write_cnt_;
    uint32_t read_cnt_;
};

struct InitTxnOperation : TransactionOperation
{
    InitTxnOperation(TransactionExecution *txm);
    void Reset();
    void Forward(TransactionExecution *txm) override;

    CcHandlerResult<InitTxResult> hd_result_;
};

struct ScanOpenOperation : TransactionOperation
{
    ScanOpenOperation(TransactionExecution *txm);
    void Forward(TransactionExecution *txm) override;

    void Set(const TableName *table_name,
             ScanIndexType index_type,
             const TxKey *key,
             bool inclusive,
             ScanDirection direction,
             bool is_ckpt_delta)
    {
        table_name_ = table_name;
        index_type_ = index_type;
        start_key_ = key;
        inclusive_ = inclusive;
        direction_ = direction;
        is_ckpt_delta_ = is_ckpt_delta;
    }

    void Reset();

    CcHandlerResult<ScanOpenResult> hd_result_;

    const TableName *table_name_{nullptr};
    ScanIndexType index_type_{ScanIndexType::Primary};
    const TxKey *start_key_{nullptr};
    bool inclusive_{true};
    ScanDirection direction_{ScanDirection::Forward};
    bool is_ckpt_delta_{false};
    ScanOpenTxRequest *tx_req_{nullptr};
};

struct ScanNextOperation : TransactionOperation
{
    ScanNextOperation(TransactionExecution *txm);
    void Forward(TransactionExecution *txm) override;

    void Set(size_t alias, CcScanner *scanner)
    {
        alias_ = alias;
        scanner_ = scanner;
    }

    void Reset();

    CcHandlerResult<ScanNextResult> hd_result_;
    size_t alias_{0};
    CcScanner *scanner_{nullptr};
    ScanNextTxRequest *tx_req_{nullptr};
};

struct AcquireAllOp : public TransactionOperation
{
    AcquireAllOp(TransactionExecution *txm);
    void Resize(size_t new_size);
    void Reset(size_t node_cnt);
    void Forward(TransactionExecution *txm) override;
    /**
     * @brief Get the max commit/validate ts of the all result
     */
    uint64_t MaxTs();

    std::vector<CcHandlerResult<AcquireAllResult>> hd_results_;
    uint32_t upload_cnt_{0};
    std::atomic<uint32_t> finish_cnt_{0};
    std::atomic<uint32_t> fail_cnt_{0};
    // Number of remote keys on which the upload operation needs to acquire
    // write intentions/locks.
    std::atomic<int32_t> remote_ack_cnt_{0};

    const TableName *table_name_{nullptr};
    const TxKey *key_{nullptr};
    LockType lk_type_{LockType::WriteIntent};
    CcProtocol protocol_{CcProtocol::OCC};
};

struct PostWriteAllOp : public TransactionOperation
{
    PostWriteAllOp(TransactionExecution *txm);
    void Reset(uint32_t ng_cnt);
    void Forward(TransactionExecution *txm) override;
    bool IsFailed();

    CcHandlerResult<PostProcessResult> hd_result_;

    const TableName *table_name_{nullptr};
    const TxKey *key_{nullptr};
    TxRecord *rec_{nullptr};
    DmlOperation dml_op_{DmlOperation::Upsert};
    PostWriteType write_type_{PostWriteType::PrepareCommit};
};

struct DsUpsertTableOp : public TransactionOperation
{
    DsUpsertTableOp() = delete;
    DsUpsertTableOp(const TableName *table_name,
                    bool is_deleted,
                    TransactionExecution *txm);

    void Reset();
    void Forward(TransactionExecution *txm) override;

    const TableName *table_name_{nullptr};
    const TableSchema *table_schema_{nullptr};
    // Store a copy of index table names for DDL
    std::vector<txservice::TableName> index_names_;
    bool is_deleted_{false};
    CcHandlerResult<Void> hd_result_;
};

struct SchemaOp : public TransactionOperation
{
    SchemaOp() = delete;
    SchemaOp(const TableName &table_name,
             const char *image_ptr,
             size_t image_len);

    CatalogKey table_key_;
    CatalogRecord catalog_rec_;
    std::string image_str_{""};
};

struct UpsertTableOp : public SchemaOp
{
    UpsertTableOp() = delete;
    UpsertTableOp(const TableName &table_name,
                  const char *image_ptr,
                  size_t len,
                  bool is_deleted,
                  TransactionExecution *txm);

    void Forward(TransactionExecution *txm) override;

    bool is_deleted_{false};
    /**
     * @brief The current stage of this multi-stage schema operation.
     *
     */
    TransactionOperation *op_{nullptr};
    /**
     * @brief Acquires write intents on the table's catalog in all nodes to
     * prevent concurrent schema modifications.
     *
     */
    AcquireAllOp acquire_all_intent_op_;
    /**
     * @brief Flushes the prepare log to the log service. The schema operation
     * is guaranteed to succeed after this stage.
     *
     */
    WriteToLogOp prepare_log_op_;
    /**
     * @brief Installs the dirty schema in the tx service and returns a local
     * view (pointer) of it.
     *
     */
    PostWriteAllOp post_all_intent_op_;
    /**
     * @brief Creates/deletes the data store table and persists/removes the
     * binary representation of the catalog in the data store.
     *
     */
    DsUpsertTableOp upsert_kv_table_op_;
    /**
     * @brief Upgrades acquired write intents to write locks in all nodes.
     *
     */
    AcquireAllOp acquire_all_lock_op_;
    /**
     * @brief Flushes the commit log to the log service. The commit log confirms
     * that the data store operation succeeds and does not need redo upon
     * failures.
     *
     */
    WriteToLogOp commit_log_op_;
    /**
     * @brief Removes write locks in all nodes. If the schema operation
     * succeeds, also installs the new schema in all nodes.
     *
     */
    PostWriteAllOp post_all_lock_op_;
    /**
     * @brief The last log operation that removes the schema record from the log
     * state machine.
     *
     */
    WriteToLogOp clean_log_op_;

private:
    void FillPrepareLogRequest(TransactionExecution *txm);
    void FillCommitLogRequest(TransactionExecution *txm);
    void FillCleanLogRequest(TransactionExecution *txm);
    void ForceToFinish(TransactionExecution *txm);
};

struct SleepOperation : TransactionOperation
{
public:
    SleepOperation(TransactionExecution *txm);

    void Forward(TransactionExecution *txm) override;

    int sleep_secs_{0};
};

struct CleanCcEntryForTestOp : TransactionOperation
{
public:
    explicit CleanCcEntryForTestOp(TransactionExecution *txm);

    void Set(const TableName *tn,
             const TxKey *key,
             bool only_archives,
             bool flush)
    {
        tab_name_ = tn;
        key_ = key;
        only_archives_ = only_archives;
        flush_ = flush;
        succeed_ = false;
    }

    void Reset()
    {
        succeed_ = false;
        hd_result_.Reset();
    }
    void Forward(TransactionExecution *txm) override;

    const TableName *tab_name_{nullptr};
    const TxKey *key_{nullptr};
    bool only_archives_{false};
    bool flush_{true};

    bool succeed_{false};
    CcHandlerResult<bool> hd_result_;
};

struct DsFindRangeMedianKeyOp : public TransactionOperation
{
    DsFindRangeMedianKeyOp() = delete;

    DsFindRangeMedianKeyOp(const TableName *table_name,
                           TransactionExecution *txm);

    void Forward(TransactionExecution *txm) override;
    void Reset();

    const TableName *table_name_;
    int32_t partition_id_;
    const Schema *key_schema;
    CcHandlerResult<RangeMedianKeyResult> hd_result_;
};

struct DsCopyRangeDataOp : public TransactionOperation
{
    DsCopyRangeDataOp() = delete;

    DsCopyRangeDataOp(const TableName &table_name, TransactionExecution *txm);

    void Forward(TransactionExecution *txm) override;
    void Reset();

    const TableName &table_name_;
    TxKey *middle_key_;
    int32_t old_partition_id_;
    int32_t new_partition_id_;
    const Schema *key_schema_;
    const Schema *record_schema_;
    // additional filtering condition beside middle key
    // which guarantte a definite data set for copying
    uint64_t filter_ts_;
    CcHandlerResult<Void> hd_result_;
};

struct DsUpsertRangeOp : TransactionOperation
{
    DsUpsertRangeOp() = delete;
    DsUpsertRangeOp(const TableName &range_table_name,
                    TransactionExecution *txm);
    void Forward(TransactionExecution *txm) override;
    void Reset();

    const TableName &range_table_name_;
    const Schema *key_schema_;
    TxKey *key_;
    int32_t partition_id_;
    int64_t ts_;
    CcHandlerResult<Void> hd_result_;
};

struct DsDeleteOutOfRangeDataOp : public TransactionOperation
{
    DsDeleteOutOfRangeDataOp() = delete;

    DsDeleteOutOfRangeDataOp(const TableName &table_name,
                             TransactionExecution *txm);

    void Forward(TransactionExecution *txm) override;
    void Reset();

    const TableName &table_name_;
    int32_t partition_id_;
    TxKey *middle_key_{nullptr};
    const Schema *key_schema_;
    CcHandlerResult<Void> hd_result_;
};

struct NoOp : public TransactionOperation
{
    NoOp(TransactionExecution *txm);
    void Forward(TransactionExecution *txm) override;
    CcHandlerResult<Void> hd_result_;
};

struct DsSplitRangeOp : public CompositeTransactionOperation
{
    DsSplitRangeOp() = delete;

    DsSplitRangeOp(const TableName &table_name,
                   const Schema *key_schema,
                   const Schema *record_schema,
                   const TxKey *range_key,
                   RangeRecord *splitting_range_record,
                   TransactionExecution *txm);

    void FillTxLogForUpdateOldRange(TransactionExecution *txm);
    void FillTxLogForCopyOldRangeData(TransactionExecution *txm);
    void FillTxLogForDirtyOldRangeData(TransactionExecution *txm);
    void FillTxLogForDeleteOutOfOldRangeData(TransactionExecution *txm);
    void FillTxLogForCleanLog(TransactionExecution *txm);
    void ForceToFinish(TransactionExecution *txm);
    void Forward(TransactionExecution *txm) override;

    const TableName &table_name_{""};
    TableName range_table_name_{""};
    const Schema *key_schema_{nullptr};
    const Schema *record_schema_{nullptr};
    int32_t partition_id_{-1};
    const TxKey *range_key_{nullptr};
    RangeRecord *old_range_record_{nullptr};
    std::unique_ptr<TxKey> new_range_key_{nullptr};
    std::unique_ptr<TableRangeEntry> upload_range_entry_{nullptr};
    std::unique_ptr<RangeRecord> upload_range_record_{nullptr};
    int32_t new_partition_id_{-1};

    /**
     * @brief Acquire write intents on the range to split at all shards. This is
     * to prevent concurrent modifications on the same range.
     */
    AcquireAllOp acquire_all_intent_for_update_old_range_op_;
    /**
     * @brief Find the median key value of the old range
     */
    DsFindRangeMedianKeyOp ds_find_median_key_for_old_range_op_;
    /**
     * @brief Upgrades the write intents to write locks. This is to wait for
     * existing queries reading or writing the range to finish and to block new
     * reads and writes on the range to start.
     */
    AcquireAllOp acquire_all_lock_for_update_old_range_op_;
    /**
     * @brief Write log to mark the split range is started, with the information
     * of the old range and new range information, after this it is guaranted to
     * succeed after this
     */
    // WriteToLogOp prepare_log_for_update_old_range_op_;
    NoOp prepare_log_for_update_old_range_op_;
    /**
     * @brief
     * 1. Upload the new key, ne_partition_id to the
     * old range entry, it is visible to all nodes
     * 2. Downgrade lock to write intent
     */
    PostWriteAllOp post_all_lock_for_update_old_range_op_;
    /**
     * @brief
     * 1.Start a new thread to do work of copy data from the old range to the
     * new ranges 2.The working thread updates the running status to the
     * ds_copy_old_range_data_op_
     */
    DsCopyRangeDataOp ds_copy_old_range_data_op_;
    /**
     * @brief Write log to mark the range split is finished
     */
    // WriteToLogOp ds_copy_old_range_data_finished_log_op_;
    NoOp ds_copy_old_range_data_finished_log_op_;
    /**
     * @brief
     * 1. Upgrade the write intent on old range entry to write lock on all nodes
     * 2. Add write lock on new range entries to write locks on all nodes
     */
    AcquireAllOp acquire_all_lock_for_dirty_old_range_op_;
    /**
     * @brief Write commit log to mark the split range transaction is succeed
     */
    // WriteToLogOp commit_log_for_dirty_old_range_op_;
    NoOp commit_log_for_dirty_old_range_op_;
    /**
     * @brief
     * 1. Update the dirty old range, clean new key and new partition id
     * 2. Upload the new range
     * 3. Remove all write locks on all nodes
     */
    PostWriteAllOp post_write_all_for_dirty_old_range_op_;
    /**
     * @brief Flush all updated range entries into Cassandra
     */
    DsUpsertRangeOp ds_upsert_new_range_op_;
    /**
     * @brief Write log to mark removing out of data from old range
     */
    // WriteToLogOp delete_out_of_old_range_data_log_op_;
    NoOp delete_out_of_old_range_data_log_op_;
    /**
     * @brief Delete out of range data from the Cassandra partition
     */
    DsDeleteOutOfRangeDataOp delete_out_of_old_range_data_op_;
    /**
     * @brief Remove split range log from the log state machine
     */
    // WriteToLogOp clean_log_op_;
    NoOp clean_log_op_;
};
}  // namespace txservice
