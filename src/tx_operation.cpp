#include "tx_operation.h"

#include <algorithm>
#include <iostream>

#include "sharder.h"
#include "tx_execution.h"

namespace txservice
{
class AbortReason
{
public:
    enum : uint32_t
    {
        kUploadVersion = 0,
        kSetCommitTs = 1,
        kNegativeCommitTs = 2,
        kUpdateMaxCommitTs = 3,
        kRereadBlank = 4,
        kNewVersionCreated = 5,
        kUpdateCommitLowerBound = 6,
        kConflictTsIsSmaller = 7,
        kMax
    };
};

ReadOperation::ReadOperation(TransactionExecution *txm)
    : read_type_(ReadType::Inside), cc_result_(txm)
{
}

void ReadOperation::Reset()
{
    cc_result_.Reset();
}

void ReadOperation::Forward(TransactionExecution *txm)
{
    if (cc_result_.IsFinished())
    {
        if (cc_result_.ErrorCode() == -1)
        {
            // The read request was directed to a non-leader node. Updates the
            // leader cache.
            const CcEntryAddr &cce_addr = std::get<2>(cc_result_.Value());
            Sharder::Instance().UpdateLeader(cce_addr.NodeGroupId());
        }

        txm->PostRead();
    }
}

UploadOperation::UploadOperation(TransactionExecution *txm)
    : upload_entries_(16), upload_cnt_(0), finish_cnt_(0), fail_cnt_(0)
{
    results_.reserve(16);

    for (size_t idx = 0; idx < 16; ++idx)
    {
        auto &res = results_.emplace_back(txm);

        res.post_lambda_ =
            [this](CcHandlerResult<std::pair<uint64_t, CcEntryAddr>> *hres)
        {
            if (hres->IsError())
            {
                // error_.store(true);
                fail_cnt_.fetch_add(1);
            }

            finish_cnt_.fetch_add(1);
        };
    }
}

void UploadOperation::Reset(size_t upload_cnt)
{
    finish_cnt_.store(0);
    fail_cnt_.store(0);
    upload_cnt_ = upload_cnt;
    Resize(upload_cnt);
}

void UploadOperation::Resize(size_t new_size)
{
    size_t old_size = results_.size();

    if (new_size <= old_size)
    {
        if (old_size > TransactionExecution::LargeTxKeySize)
        {
            size_t shrink_size = std::max(new_size, (size_t) 16);
            results_.erase(results_.begin() + shrink_size, results_.end());
            results_.shrink_to_fit();
            upload_entries_.resize(shrink_size);
            upload_entries_.shrink_to_fit();
        }
    }
    else
    {
        for (size_t idx = old_size; idx < new_size; ++idx)
        {
            // All cc handler results in an operation points to the same tx
            // machine.
            auto &res = results_.emplace_back(results_.at(0).Txm());

            res.post_lambda_ =
                [this](CcHandlerResult<std::pair<uint64_t, CcEntryAddr>> *hres)
            {
                if (hres->IsError())
                {
                    // error_.store(true);
                    fail_cnt_.fetch_add(1);
                }

                finish_cnt_.fetch_add(1);
            };
        }

        upload_entries_.resize(new_size);
    }
}

void UploadOperation::Forward(TransactionExecution *txm)
{
    if (finish_cnt_.load() == upload_cnt_)
    {
        std::unordered_set<uint32_t> outdated_node_set;

        for (size_t idx = 0; idx < upload_cnt_; ++idx)
        {
            if (!results_.at(idx).IsError())
            {
                WriteSetEntry &write_entry = *upload_entries_.at(idx);
                // Assigns to the write entry the cc entry address obtained
                // in the acquire phase.
                write_entry.cce_addr_ = results_.at(idx).Value().second;
                txm->rw_set_.DedupRead(write_entry.cce_addr_);
            }
            else if (results_.at(idx).ErrorCode() == -1)
            {
                const CcEntryAddr &addr = results_.at(idx).Value().second;
                auto find_it = outdated_node_set.find(addr.NodeGroupId());
                if (find_it == outdated_node_set.end())
                {
                    Sharder::Instance().UpdateLeader(addr.NodeGroupId());
                    outdated_node_set.emplace(addr.NodeGroupId());
                }
            }
        }

        if (fail_cnt_.load(std::memory_order_acquire) > 0)
        {
            txm->Abort();
        }
        else
        {
            txm->PostUpload();
        }
    }
}

void AcquireTableWriteLockOp::Forward(TransactionExecution *txm)
{
    if (cc_result_.IsFinished())
    {
        txm->table_lock_term_map_ = cc_result_.Value();
        if (cc_result_.IsError())
        {
            txm->Abort();
        }
        else
        {
            txm->Commit();
        }
    }
}

void ReleaseTableWriteLockOp::Forward(TransactionExecution *txm)
{
    if (cc_result_.IsFinished())
    {
        if (cc_result_.IsError())
        {
            // FIXME: transaction has been written to tlog, should not abort
            // here. we need to add retry logic here: case 1: node failover,
            // then new node will not held the table write lock, we need to the
            // confirmation from the new node. case 2: network issue. we need to
            // retry.
            txm->ReleaseTableWriteLock();
        }
        else
        {
            txm->ReleaseAllTableLocks();
        }
    }
}

void WriteDDLLogOp::Reset()
{
    cc_result_.Reset();
    log_closure_.Reset();
}

void WriteDDLLogOp::Forward(TransactionExecution *txm)
{
    if (cc_result_.IsFinished())
    {
        if (cc_result_.IsError())
        {
            txm->tx_status_ = TxnStatus::Aborted;
            txm->PostWriteLog();
        }
        else
        {
            txm->tx_status_ = TxnStatus::Committed;
            txm->PostWriteLog();
        }
    }
}

ScanOpenOperation::ScanOpenOperation(TransactionExecution *txm)
    : cc_result_(txm),
      table_name_(nullptr),
      start_key_(nullptr),
      inclusive_(false),
      direction_(ScanDirection::Forward)
{
}

SetCommitTsOperation::SetCommitTsOperation(TransactionExecution *txm)
    : result_of_set_commit_ts_(txm)
{
}

void SetCommitTsOperation::Reset()
{
    result_of_set_commit_ts_.Reset();
}

void SetCommitTsOperation::Forward(TransactionExecution *txm)
{
    if (result_of_set_commit_ts_.IsFinished())
    {
        if (result_of_set_commit_ts_.IsError())
        {
            txm->Abort();
        }
        else
        {
            txm->PostSetTs();
        }
    }
}

ValidateOperation::ValidateOperation(TransactionExecution *txm)
    : vali_cnt_(0), finish_cnt_(0), error_(false)
{
    results_.reserve(16);

    for (size_t idx = 0; idx < 16; ++idx)
    {
        auto &res = results_.emplace_back(txm);
        res.post_lambda_ = [this](CcHandlerResult<std::vector<TxId>> *hres)
        {
            if (hres->IsError())
            {
                error_.store(true);
            }

            finish_cnt_.fetch_add(1);
        };
    }
}

void ValidateOperation::Reset(size_t vali_cnt)
{
    vali_cnt_ = vali_cnt;
    finish_cnt_.store(0);
    error_.store(false);
    Resize(vali_cnt);
}

void ValidateOperation::Resize(size_t new_size)
{
    size_t old_size = results_.size();

    if (new_size <= old_size)
    {
        if (old_size > TransactionExecution::LargeTxKeySize)
        {
            size_t shrink_size = std::max(new_size, (size_t) 16);
            results_.erase(results_.begin() + shrink_size, results_.end());
            results_.shrink_to_fit();
        }
    }
    else
    {
        for (size_t idx = old_size; idx < new_size; ++idx)
        {
            // All cc handler results in an operation points to the same tx
            // machine.
            auto &res = results_.emplace_back(results_.at(0).Txm());

            res.post_lambda_ = [this](CcHandlerResult<std::vector<TxId>> *hres)
            {
                if (hres->IsError())
                {
                    error_.store(true);
                }

                finish_cnt_.fetch_add(1);
            };
        }
    }
}

void ValidateOperation::Forward(TransactionExecution *txm)
{
    if (finish_cnt_.load() == vali_cnt_)
    {
        // Validation has finished. Post-processing skips read-set keys.
        txm->rw_set_.ClearReadSet();
        txm->rw_set_.ClearScanSet();

        txm->PostVali();

        // if (error_)
        //{
        //    txm->Abort();
        //}
        // else
        //{
        //    size_t idx = 0;
        //    for (; idx < vali_cnt_; ++idx)
        //    {
        //        if (results_[idx].Value().size() > 0)
        //        {
        //            break;
        //        }
        //    }

        //    if (idx == vali_cnt_)
        //    {
        //        txm->PostVali();
        //    }
        //    else
        //    {
        //        // Skips tx negotiations for now.
        //        txm->Abort();
        //    }
        //}
    }
}

PushConflictTxnCommitTsLowerBound::PushConflictTxnCommitTsLowerBound(
    TransactionExecution *txm)
    : result_of_update_commit_lower_bound_(txm)
{
}

void PushConflictTxnCommitTsLowerBound::Reset(TxId txn_id)
{
    result_of_update_commit_lower_bound_.Reset();
    txid_ = txn_id;
}

WriteToLog::WriteToLog(TransactionExecution *txm)
    : res_(txm), log_closure_(&res_)
{
}

void WriteToLog::Forward(TransactionExecution *txm)
{
    if (res_.IsFinished())
    {
        if (res_.IsError())
        {
            txm->tx_status_ = TxnStatus::Aborted;
            txm->PostWriteLog();
        }
        else
        {
            txm->tx_status_ = TxnStatus::Committed;
            txm->PostWriteLog();
        }
    }
}

void WriteToLog::Reset()
{
    res_.Reset();
    log_closure_.Reset();
}

UpdateTxnStatus::UpdateTxnStatus(TransactionExecution *txm) : res_(txm)
{
}

void UpdateTxnStatus::Reset()
{
    res_.Reset();
}

void UpdateTxnStatus::Forward(TransactionExecution *txm)
{
    if (res_.IsFinished())
    {
        if (res_.IsError())
        {
            // Updating the tx's status should never fail.
            txm->PostSetTxStatus();
        }
        else
        {
            txm->PostSetTxStatus();
        }
    }
}

InitTxnOperation::InitTxnOperation(TransactionExecution *txm)
    : result_of_new_txn_(txm)
{
}

void InitTxnOperation::Reset(uint64_t start_ts)
{
    result_of_new_txn_.Reset();
    initi_ts_ = start_ts;
}

void InitTxnOperation::Forward(TransactionExecution *txm)
{
    if (result_of_new_txn_.IsFinished())
    {
        if (result_of_new_txn_.IsError())
        {
            // Beginning a tx should never fail.
            txm->PostBegin();
        }
        else
        {
            txm->PostBegin();
        }
    }
}

PostProcessOp::PostProcessOp(TransactionExecution *txm)
    : upload_cnt_(0), finish_cnt_(0)
{
    results_.reserve(16);

    for (size_t idx = 0; idx < 16; ++idx)
    {
        CcHandlerResult<Void> &res = results_.emplace_back(txm);

        res.post_lambda_ = [this](CcHandlerResult<Void> *)
        { finish_cnt_.fetch_add(1); };
    }
}

void PostProcessOp::Reset(size_t upload_cnt)
{
    finish_cnt_.store(0);
    upload_cnt_ = upload_cnt;
    Resize(upload_cnt);
}

void PostProcessOp::Resize(size_t new_size)
{
    size_t old_size = results_.size();

    if (new_size <= old_size)
    {
        if (old_size > TransactionExecution::LargeTxKeySize)
        {
            size_t shrink_size = std::max(new_size, (size_t) 16);
            results_.erase(results_.begin() + shrink_size, results_.end());
            results_.shrink_to_fit();
        }
    }
    else
    {
        for (size_t idx = old_size; idx < new_size; ++idx)
        {
            auto &res = results_.emplace_back(results_.at(0).Txm());
            res.post_lambda_ = [this](CcHandlerResult<Void> *)
            { finish_cnt_.fetch_add(1); };
        }
    }
}

void PostProcessOp::Forward(TransactionExecution *txm)
{
    if (finish_cnt_.load() == upload_cnt_)
    {
        txm->ReleaseAllTableLocks();
    }
}

void PostProcessDDLOp::Forward(TransactionExecution *txm)
{
    // FIXME: what happens when results_.IsError() in post process
    if (results_.IsFinished())
    {
        txm->ReleaseTableWriteLock();
    }
}

void ReleaseAllTableLocksOp::Forward(TransactionExecution *txm)
{
    if (cc_result_.IsFinished())
    {
        txm->PostPostProcess();
    }
}

void FindCatalogInCCShardOp::Forward(TransactionExecution *txm)
{
    if (cc_result_.IsFinished())
    {
        if (cc_result_.Value() == true)
        {
            txm->FindCatalogFinish(true);
        }
        else
        {
            txm->FindCatalogFinish(false);
        }
    }
}

void CheckCatalogInCCShardOp::Forward(TransactionExecution *txm)
{
    if (cc_result_.IsFinished())
    {
        if (cc_result_.Value() == true)
        {
            txm->RequestFinish(true);
        }
        else
        {
            txm->RequestFinish(false);
        }
    }
}

void FaultInjectOp::Forward(TransactionExecution *txm)
{
    if (cc_result_.IsFinished())
    {
        if (cc_result_.Value() == true)
        {
            txm->RequestFinish(true);
        }
        else
        {
            txm->RequestFinish(false);
        }
    }
}

void ScanOpenOperation::Forward(TransactionExecution *txm)
{
    if (cc_result_.IsFinished())
    {
        if (cc_result_.ErrorCode() == -1)
        {
            Sharder::Instance().UpdateLeaders();
        }

        txm->PostScanOpen();
    }
}

ScanNextOperation::ScanNextOperation(TransactionExecution *txm)
    : cc_result_(txm)
{
}

void ScanNextOperation::Forward(TransactionExecution *txm)
{
    if (cc_result_.IsFinished())
    {
        if (cc_result_.IsError())
        {
            if (cc_result_.ErrorCode() == -1)
            {
                Sharder::Instance().UpdateLeader(cc_result_.Value());
            }

            txm->Abort();
        }
        else
        {
            txm->PostScanNext();
        }
    }
}
}  // namespace txservice
