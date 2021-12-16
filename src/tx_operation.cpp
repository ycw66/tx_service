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

ReadOperation::ReadOperation(TransactionExecution *txm) : cc_result_(txm)
{
}

void ReadOperation::Reset()
{
    cc_result_.Reset();
}

void ReadOperation::Forward(TransactionExecution *txm)
{
    const CcEntryAddr &cce_addr = cc_result_.Value().cce_addr_;

    if (cce_addr.Term() < 0 && txm->IsTimeOut())
    {
        // For non-blocking concurrency control protocols, the read request
        // is expected to return instantly. For lock-based protocols, if the
        // read request is blocked, the cc node will send an acknowledgement to
        // update the key's term. In either case, if the read key's term is not
        // set, the tx has not received any response or acknowledgement from the
        // key's cc node group. The read request is forced to be errored upon
        // timeout.
        cc_result_.ForceError();
        txm->PostRead();
    }
    else if (cc_result_.IsFinished())
    {
        if (cc_result_.ErrorCode() == -1)
        {
            // The read request was directed to a non-leader node. Updates the
            // leader cache.
            Sharder::Instance().UpdateLeader(cce_addr.NodeGroupId());
        }

        txm->PostRead();
    }
    // TODO: for locking-based protocols, even though the tx may be blocked
    // arbitrarily long after the read request is acknowledged, we still need
    // to periodically check liveness of the remote node and force the tx to
    // cancel if the remote node is unresponsive.
}

UploadOperation::UploadOperation(TransactionExecution *txm)
{
    results_.reserve(16);

    for (size_t idx = 0; idx < 16; ++idx)
    {
        auto &res = results_.emplace_back(txm);

        res.post_lambda_ = [this](CcHandlerResult<AcquireKeyResult> *hres)
        {
            if (hres->IsError())
            {
                fail_cnt_.fetch_add(1);
            }

            if (hres->Value().remote_ack_cnt_ != nullptr)
            {
                hres->Value().remote_ack_cnt_->fetch_sub(1);
            }

            finish_cnt_.fetch_add(1);
        };
    }
}

void UploadOperation::Reset(size_t upload_cnt)
{
    finish_cnt_.store(0);
    fail_cnt_.store(0);
    remote_ack_cnt_.store(0);
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

            res.post_lambda_ = [this](CcHandlerResult<AcquireKeyResult> *hres)
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
    if (remote_ack_cnt_.load(std::memory_order_acquire) > 0)
    {
        bool time_out = txm->IsTimeOut();

        if (time_out)
        {
            // At least one remote acquire request has not received
            // acknowledgement and the upload phase has timed out. Forces
            // un-acknowledged requests to finish with an error.
            for (size_t idx = 0; idx < upload_cnt_; ++idx)
            {
                CcHandlerResult<AcquireKeyResult> &hd_result = results_.at(idx);
                const CcEntryAddr &cce_addr = hd_result.Value().cce_addr_;

                if (cce_addr.Term() < 0)
                {
                    bool success = hd_result.ForceError();
                    if (success)
                    {
                        // Up until this point, we consider that the acquire
                        // request has failed. The tx will proceed to abort
                        // without trying to release the lock claimed by this
                        // request. In rare circumstances, it is still possible
                        // that the acquire request's response arrives after
                        // this point. We rely on the lock recovery mechanism in
                        // the remote node to clear such an orphan lock.
                        continue;
                    }
                }

                // The acquire request may have 1) finished successfully, 2)
                // finished with an error, 3) acknowledged. Only 2) does not
                // need post-processing to clear the lock or to delete the
                // acquire request from the blocking queue in the remote node.
                if (!hd_result.IsError())
                {
                    txm->rw_set_.DedupRead(cce_addr);
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
    else if (finish_cnt_.load() == upload_cnt_)
    {
        // TODO: for locking-based protocols, though the tx may be blocked
        // arbitrarily long, after all acquire requests are acknowledged, we
        // still need to periodically check liveness of the remote node.
        std::unordered_set<uint32_t> outdated_node_set;

        for (size_t idx = 0; idx < upload_cnt_; ++idx)
        {
            if (!results_.at(idx).IsError())
            {
                WriteSetEntry &write_entry = *upload_entries_.at(idx);
                // Assigns to the write entry the cc entry address obtained
                // in the acquire phase.
                write_entry.cce_addr_ = results_.at(idx).Value().cce_addr_;
                assert(write_entry.cce_addr_.CcePtr() != 0);
                txm->rw_set_.DedupRead(write_entry.cce_addr_);
            }
            else if (results_.at(idx).ErrorCode() == -1)
            {
                const CcEntryAddr &addr = results_.at(idx).Value().cce_addr_;
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
    size_t finish_cnt = finish_cnt_.load(std::memory_order_acquire);

    if (finish_cnt < vali_cnt_ && txm->IsTimeOut())
    {
        for (size_t idx = 0; idx < results_.size(); ++idx)
        {
            // For every validation request, attempts to force the request
            // to be errored. If the request has not received response
            // and is forced to be errored, the corresponding key needs
            // post-processing. If the request finishes, either successfuly
            // or with an error code, forcing the request will not succeed and
            // the request not need post-processing.
            auto &vali_result = results_.at(idx);
            bool success = vali_result.ForceError();
            if (!success)
            {
                txm->rw_set_.DedupRead(*vali_cce_addr_.at(idx));
            }
        }

        txm->Abort();
    }
    else if (finish_cnt == vali_cnt_)
    {
        // All validation requests have returned, either successfully or with
        // error codes. Post-processing skips read-set keys.
        txm->rw_set_.ClearReadSet();
        txm->rw_set_.ClearScanSet();

        if (error_.load(std::memory_order_acquire))
        {
            txm->Abort();
        }
        else
        {
            size_t idx = 0;
            for (; idx < vali_cnt_; ++idx)
            {
                if (results_[idx].Value().size() > 0)
                {
                    break;
                }
            }

            if (idx == vali_cnt_)
            {
                txm->PostVali();
            }
            else
            {
                // Skips tx negotiations for now.
                txm->Abort();
            }
        }
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

WriteToLog::WriteToLog(TransactionExecution *txm) : res_(txm)
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

InitTxnOperation::InitTxnOperation(TransactionExecution *txm) : result_(txm)
{
}

void InitTxnOperation::Reset()
{
    result_.Reset();
}

void InitTxnOperation::Forward(TransactionExecution *txm)
{
    if (result_.IsFinished())
    {
        if (result_.IsError())
        {
            txm->Abort();
        }
        else
        {
            txm->PostBegin();
        }
    }
}

PostProcessOp::PostProcessOp(TransactionExecution *txm)
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
    if (finish_cnt_.load(std::memory_order_acquire) == upload_cnt_)
    {
        txm->ReleaseAllTableLocks();
    }
    else
    {
        bool time_out = txm->IsTimeOut();
        if (time_out)
        {
            txm->ReleaseAllTableLocks();
        }
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
    if (!cc_result_.IsFinished())
    {
        bool time_out = txm->IsTimeOut();

        if (time_out)
        {
            cc_result_.ForceError();
            // TODO: So far we do not store scanned keys in the tx's scan set.
            // In future, we need ScanOpenResult to check which cc nodes have
            // returned and to release scan locks in these cc nodes in
            // post-processing.
            txm->PostScanOpen();
        }
    }
    else
    {
        if (cc_result_.ErrorCode() == -1)
        {
            Sharder::Instance().UpdateLeaders();
        }

        txm->PostScanOpen();
    }
}

void ScanNextOperation::Reset()
{
    cc_result_.Reset();
    scanner_ = nullptr;
    alias_ = 0;
}

ScanNextOperation::ScanNextOperation(TransactionExecution *txm)
    : cc_result_(txm)
{
}

void ScanNextOperation::Forward(TransactionExecution *txm)
{
    if (!cc_result_.IsFinished())
    {
        bool time_out = txm->IsTimeOut();
        if (time_out)
        {
            cc_result_.ForceError();
            txm->PostScanNext();
        }
    }
    else
    {
        if (cc_result_.ErrorCode() == -1)
        {
            Sharder::Instance().UpdateLeader(cc_result_.Value().node_group_id_);
        }

        txm->PostScanNext();
    }
}
}  // namespace txservice
