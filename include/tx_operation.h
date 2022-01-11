#pragma once

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

struct UploadOperation : TransactionOperation
{
public:
    UploadOperation(TransactionExecution *txm);
    void Resize(size_t new_size);
    void Reset(size_t upload_cnt);
    void Forward(TransactionExecution *txm) override;

    std::vector<CcHandlerResult<AcquireKeyResult>> results_;
    std::vector<WriteSetEntry *> upload_entries_{16};
    uint32_t upload_cnt_{0};
    std::atomic<uint32_t> finish_cnt_{0};
    std::atomic<uint32_t> fail_cnt_{0};
    // Number of remote keys on which the upload operation needs to acquire
    // write intentions/locks.
    std::atomic<int32_t> remote_ack_cnt_{0};
};

struct AcquireTableWriteLockOp : TransactionOperation
{
public:
    AcquireTableWriteLockOp(TransactionExecution *txm) : cc_result_(txm)
    {
    }

    void Forward(TransactionExecution *txm) override;

    CcHandlerResult<std::unordered_map<uint32_t, int64_t>> cc_result_;
};

struct ReleaseTableWriteLockOp : TransactionOperation
{
public:
    ReleaseTableWriteLockOp(TransactionExecution *txm) : cc_result_(txm)
    {
    }

    void Forward(TransactionExecution *txm) override;

    CcHandlerResult<Void> cc_result_;
};

struct FindCatalogInCCShardOp : TransactionOperation
{
public:
    FindCatalogInCCShardOp(TransactionExecution *txm) : cc_result_(txm)
    {
    }

    void Forward(TransactionExecution *txm) override;

    CcHandlerResult<bool> cc_result_;
};

struct CheckCatalogInCCShardOp : TransactionOperation
{
public:
    CheckCatalogInCCShardOp(TransactionExecution *txm) : cc_result_(txm)
    {
    }

    void Forward(TransactionExecution *txm) override;

    CcHandlerResult<bool> cc_result_;
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

struct ReleaseAllTableLocksOp : TransactionOperation
{
public:
    ReleaseAllTableLocksOp(TransactionExecution *txm) : cc_result_(txm)
    {
    }

    void Forward(TransactionExecution *txm) override;

    CcHandlerResult<bool> cc_result_;
};

struct WriteDDLLogOp : TransactionOperation
{
public:
    WriteDDLLogOp(TransactionExecution *txm)
        : cc_result_(txm), log_closure_(&cc_result_)
    {
    }

    void Reset();
    void Forward(TransactionExecution *txm) override;

    CcHandlerResult<Void> cc_result_;
    LogClosure log_closure_;
};

struct PushConflictTxnCommitTsLowerBound : TransactionOperation
{
    PushConflictTxnCommitTsLowerBound(TransactionExecution *txm);
    void Reset(TxId txn_id);

private:
    CcHandlerResult<uint64_t> result_of_update_commit_lower_bound_;
    TxId txid_;
};

struct WriteToLog : TransactionOperation
{
    WriteToLog(TransactionExecution *txm);
    void Forward(TransactionExecution *txm) override;
    void Reset();

    CcHandlerResult<Void> res_;
    LogClosure log_closure_{&res_};
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
    size_t upload_cnt_{0};
    std::atomic<size_t> finish_cnt_{0};
};

struct PostProcessDDLOp : TransactionOperation
{
    PostProcessDDLOp(TransactionExecution *txm) : results_(txm)
    {
    }

    void Forward(TransactionExecution *txm) override;

    CcHandlerResult<Void> results_;
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

}  // namespace txservice
