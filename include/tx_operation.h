#pragma once

#include "cc_handler.h"
#include "log_closure.h"
#include "tx_key.h"
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

    ReadType read_type_;
    CcHandlerResult<std::tuple<TxRecord *, uint64_t, CcEntryAddr, RecordStatus>>
        cc_result_;
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
    size_t vali_cnt_;
    std::atomic<size_t> finish_cnt_;
    std::atomic<bool> error_;
};

struct UploadOperation : TransactionOperation
{
public:
    UploadOperation(TransactionExecution *txm);
    void Resize(size_t new_size);
    void Reset(size_t upload_cnt);
    void Forward(TransactionExecution *txm) override;

    std::vector<CcHandlerResult<std::pair<uint64_t, CcEntryAddr>>> results_;
    std::vector<WriteSetEntry *> upload_entries_;
    size_t upload_cnt_;
    std::atomic<size_t> finish_cnt_;
    std::atomic<size_t> fail_cnt_;
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
};  // namespace txservice::transaction

struct WriteToLog : TransactionOperation
{
    WriteToLog(TransactionExecution *txm);
    void Forward(TransactionExecution *txm) override;
    void Reset();

    CcHandlerResult<Void> res_;
    LogClosure log_closure_;
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
    void Reset(size_t upload_cnt);
    void Resize(size_t new_size);
    void Forward(TransactionExecution *txm) override;

    std::vector<CcHandlerResult<Void>> results_;
    size_t upload_cnt_;
    std::atomic<size_t> finish_cnt_;
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
    void Reset(uint64_t start_ts = 0);
    void Forward(TransactionExecution *txm) override;

    CcHandlerResult<std::tuple<TxId, uint64_t, int64_t>> result_of_new_txn_;
    uint64_t initi_ts_ = 0;
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

    CcHandlerResult<std::pair<size_t, std::unique_ptr<CcScanner>>> cc_result_;

    const TableName *table_name_;
    const TxKey *start_key_;
    bool inclusive_;
    ScanDirection direction_;
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

    CcHandlerResult<uint32_t> cc_result_;

    size_t alias_;
    CcScanner *scanner_;
};

}  // namespace txservice
