#pragma once

#include "catalog_key_record.h"
#include "cc_handler.h"
#include "log_closure.h"
#include "tx_key.h"
#include "tx_operation_result.h"
#include "tx_record.h"
#include "tx_req_result.h"

namespace txservice
{
class TransactionExecution;

struct TransactionOperation
{
    virtual ~TransactionOperation() = default;
    virtual void Forward(TransactionExecution *txm) = 0;
};

struct ReadOperation : TransactionOperation
{
public:
    ReadOperation(TransactionExecution *txm);

    void Reset();
    void Forward(TransactionExecution *txm) override;

    ReadType read_type_{ReadType::Inside};
    CcHandlerResult<ReadKeyResult> cc_result_;
    CcProtocol protocol_{CcProtocol::OCC};
    IsolationLevel iso_level_{IsolationLevel::ReadCommitted};
};

struct SetCommitTsOperation : TransactionOperation
{
public:
    SetCommitTsOperation(TransactionExecution *txm);
    void Reset();
    void Forward(TransactionExecution *txm) override;

    CcHandlerResult<uint64_t> result_of_set_commit_ts_;
};

struct ValidateOperation : TransactionOperation
{
public:
    ValidateOperation(TransactionExecution *txm);
    void Resize(size_t new_size);
    void Reset(size_t vali_cnt);
    void Forward(TransactionExecution *txm) override;

    std::vector<CcHandlerResult<std::vector<TxId>>> results_;
    std::vector<const CcEntryAddr *> vali_cce_addr_;
    size_t vali_cnt_{0};
    std::atomic<size_t> finish_cnt_{0};
    std::atomic<bool> error_{false};
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
    void Resize(size_t new_size);
    void Reset(size_t acquire_write_cnt);
    void Forward(TransactionExecution *txm) override;

    std::vector<CcHandlerResult<AcquireKeyResult>> results_;
    std::vector<WriteSetEntry *> acquire_write_entries_{16};
    uint32_t acquire_write_cnt_{0};
    std::atomic<uint32_t> finish_cnt_{0};
    std::atomic<uint32_t> fail_cnt_{0};
    // Number of remote keys on which the acquire write operation needs to
    // acquire write intentions/locks.
    std::atomic<int32_t> remote_ack_cnt_{0};
};

struct FaultInjectOp : TransactionOperation
{
public:
    FaultInjectOp(TransactionExecution *txm) : cc_result_(txm)
    {
    }

    void Forward(TransactionExecution *txm) override;

    CcHandlerResult<bool> cc_result_;
};

struct PushConflictTxnCommitTsLowerBound : TransactionOperation
{
    PushConflictTxnCommitTsLowerBound(TransactionExecution *txm);
    void Reset(TxId txn_id);

private:
    CcHandlerResult<uint64_t> result_of_update_commit_lower_bound_;
    TxId txid_;
};

struct WriteToLogOp : TransactionOperation
{
    WriteToLogOp(TransactionExecution *txm);
    void Forward(TransactionExecution *txm) override;
    void Reset();

    CcHandlerResult<Void> hd_result_;
    LogClosure log_closure_{&hd_result_};
};

struct UpdateTxnStatus : TransactionOperation
{
    UpdateTxnStatus(TransactionExecution *txm);
    void Reset();
    void Forward(TransactionExecution *txm) override;

    CcHandlerResult<Void> res_;
};

struct PostProcessOp : TransactionOperation
{
    PostProcessOp(TransactionExecution *txm);
    void Reset(size_t read_cnt, size_t write_cnt);
    void Resize(size_t read_cnt, size_t write_cnt);
    void Forward(TransactionExecution *txm) override;

    std::vector<CcHandlerResult<Void>> write_results_;
    std::vector<CcHandlerResult<std::vector<TxId>>> read_results_;
    size_t acquire_write_cnt_{0};
    std::atomic<size_t> finish_cnt_{0};
};

struct InitTxnOperation : TransactionOperation
{
    InitTxnOperation(TransactionExecution *txm);
    void Reset();
    void Forward(TransactionExecution *txm) override;

    CcHandlerResult<InitTxResult> result_;
};

struct ScanOpenOperation : TransactionOperation
{
    ScanOpenOperation(TransactionExecution *txm);
    void Forward(TransactionExecution *txm) override;

    void Set(const TableName *table_name,
             const TxKey *key,
             bool inclusive,
             ScanDirection direction)
    {
        table_name_ = table_name;
        start_key_ = key;
        inclusive_ = inclusive;
        direction_ = direction;
    }

    CcHandlerResult<ScanOpenResult> cc_result_;

    const TableName *table_name_{nullptr};
    const TxKey *start_key_{nullptr};
    bool inclusive_{true};
    ScanDirection direction_{ScanDirection::Forward};
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

    CcHandlerResult<ScanNextResult> cc_result_;
    size_t alias_{0};
    CcScanner *scanner_{nullptr};
};

struct AcquireAllOp : public TransactionOperation
{
    AcquireAllOp(TransactionExecution *txm);
    void Resize(size_t new_size);
    void Reset(size_t node_cnt);
    void Forward(TransactionExecution *txm) override;

    std::vector<CcHandlerResult<AcquireAllResult>> hd_results_;
    uint32_t upload_cnt_{0};
    std::atomic<uint32_t> finish_cnt_{0};
    std::atomic<uint32_t> fail_cnt_{0};
    // Number of remote keys on which the upload operation needs to acquire
    // write intentions/locks.
    std::atomic<int32_t> remote_ack_cnt_{0};

    const TableName *table_name_;
    const TxKey *key_{nullptr};
    LockType lk_type_{LockType::WriteIntent};
    CcProtocol protocol_{CcProtocol::OCC};
};

struct PostWriteAllOp : public TransactionOperation
{
    PostWriteAllOp(TransactionExecution *txm);
    void Reset(uint32_t ng_cnt);
    void Resize(uint32_t ng_cnt);
    void Forward(TransactionExecution *txm) override;

    std::vector<CcHandlerResult<Void>> hd_results_;
    size_t upload_cnt_{0};
    std::atomic<size_t> finish_cnt_{0};

    const TableName *table_name_{nullptr};
    const TxKey *key_{nullptr};
    TxRecord *rec_{nullptr};
    DmlOperation dml_op_{DmlOperation::Upsert};
    PostWriteType write_type_{PostWriteType::PrepareCommit};
};

struct DataStoreOp : public TransactionOperation
{
    DataStoreOp(TransactionExecution *txm);
    void Reset();
    void Forward(TransactionExecution *txm) override;

    CcHandlerResult<Void> result_;
};

struct DsUpsertTableOp : public DataStoreOp
{
    DsUpsertTableOp() = delete;
    DsUpsertTableOp(const TableName *table_name,
                    const TableName *kv_table_name,
                    bool is_deleted,
                    TransactionExecution *txm);

    using DataStoreOp::Forward;

    const TableName *table_name_;
    const TableName *kv_table_name_;
    const TableSchema *table_schema_;
    bool is_deleted_;
};

struct SchemaOp : public TransactionOperation
{
    SchemaOp() = delete;
    SchemaOp(const TableName &table_name,
             const char *image_ptr,
             size_t image_len);

    CatalogKey table_key_;
    CatalogRecord catalog_rec_;
};

struct UpsertTableOp : public SchemaOp
{
    UpsertTableOp() = delete;
    UpsertTableOp(const TableName &table_name,
                  const TableName &kv_table_name,
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
    void FlushPrepareLog(TransactionExecution *txm);
    void FlushCommitLog(TransactionExecution *txm);
    void FlushCleanLog(TransactionExecution *txm);
};

}  // namespace txservice
