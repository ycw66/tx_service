#include "tx_operation.h"

#include <algorithm>
#include <iostream>
#include <string>

#include "cc/cc_handler_result.h"
#include "fault/fault_inject.h"
#include "sharder.h"
#include "tx_execution.h"
#include "tx_trace.h"

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

/**
 * @brief Re-process the failed operation if the error code is -1, which
 * indicated a term change or send message failure. Retry at most RETRY_NUM
 * number of times. The first retry waits 0 secs, the second one waits 2
 * seconds, the third one waits 4 seconds and so on.
 *
 */
void TransactionOperation::ReRunOp(TransactionExecution *txm)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        txm,
        (
            [txm, this]() -> std::string
            {
                return std::string(",\"tx_number\":")
                    .append(std::to_string(txm->TxNumber()))
                    .append(",\"term\":")
                    .append(std::to_string(txm->TxTerm()))
                    .append(",\"retry_num_\":")
                    .append(std::to_string(this->retry_num_));
            }));
    if (retry_num_ <= 0)
    {
        return;
    }

    // sleep for a while and then execute the new operation.
    // sleep time is based on retry_num_.
    txm->sleep_op_.sleep_secs_ = (RETRY_NUM - retry_num_) * 2;
    is_running_ = false;
    retry_num_--;

    // put sleep operation on top of the stack.
    txm->PushOperation(&txm->sleep_op_);
    txm->StartTiming();
}

ReadOperation::ReadOperation(TransactionExecution *txm) : hd_result_(txm)
{
    TX_TRACE_ASSOCIATE(this, &hd_result_);
}

void ReadOperation::Reset()
{
    hd_result_.Reset();
}

void ReadOperation::Forward(TransactionExecution *txm)
{
    // start the state machine if not running.
    if (!is_running_)
    {
        txm->Process(*this);
    }

    const CcEntryAddr &cce_addr = hd_result_.Value().cce_addr_;

    if (cce_addr.Term() < 0 && txm->IsTimeOut())
    {
        TX_TRACE_ACTION_WITH_CONTEXT(
            this,
            "Forward.Term<0.IsTimeout",
            txm,
            (
                [txm]() -> std::string
                {
                    return std::string(",\"tx_number\":")
                        .append(std::to_string(txm->TxNumber()))
                        .append(",\"term\":")
                        .append(std::to_string(txm->TxTerm()));
                }));
        // For non-blocking concurrency control protocols, the read request
        // is expected to return instantly. For lock-based protocols, if the
        // read request is blocked, the cc node will send an acknowledgement to
        // update the key's term. In either case, if the read key's term is not
        // set, the tx has not received any response or acknowledgement from the
        // key's cc node group. The read request is forced to be errored upon
        // timeout.
        hd_result_.ForceError();
        txm->PostProcess(*this);
    }
    else if (hd_result_.IsFinished())
    {
        if (hd_result_.ErrorCode() == -1)
        {
            // The read request was directed to a non-leader node. Updates the
            // leader cache. Sine UpdateLeader() is a sync call, we only do it
            // when re-run the operation fails.
            if (retry_num_ == 0)
            {
                Sharder::Instance().UpdateLeader(cce_addr.NodeGroupId());
            }
            else if (retry_num_ > 0)
            {
                ReRunOp(txm);
                return;
            }
        }

        txm->PostProcess(*this);
    }
    // TODO: for locking-based protocols, even though the tx may be blocked
    // arbitrarily long after the read request is acknowledged, we still need
    // to periodically check liveness of the remote node and force the tx to
    // cancel if the remote node is unresponsive.
}

AcquireWriteOperation::AcquireWriteOperation(TransactionExecution *txm)
{
    results_.reserve(16);

    for (size_t idx = 0; idx < 16; ++idx)
    {
        auto &res = results_.emplace_back(txm);
        TX_TRACE_ASSOCIATE(this, &res);

        res.post_lambda_ = [this](CcHandlerResult<AcquireKeyResult> *hres)
        {
            if (hres->IsError())
            {
                fail_cnt_.fetch_add(1);
            }
            finish_cnt_.fetch_add(1);
        };
    }
}

void AcquireWriteOperation::Reset(size_t acquire_write_cnt)
{
    finish_cnt_.store(0);
    fail_cnt_.store(0);
    remote_ack_cnt_.store(0);
    rset_has_expired_.store(false);
    acquire_write_cnt_ = acquire_write_cnt;
    Resize(acquire_write_cnt);
}

void AcquireWriteOperation::Resize(size_t new_size)
{
    size_t old_size = results_.size();

    if (new_size <= old_size)
    {
        if (old_size > TransactionExecution::LargeTxKeySize)
        {
            size_t shrink_size = std::max(new_size, (size_t) 16);
            results_.erase(results_.begin() + shrink_size, results_.end());
            results_.shrink_to_fit();
            acquire_write_entries_.resize(shrink_size);
            acquire_write_entries_.shrink_to_fit();
        }
    }
    else
    {
        for (size_t idx = old_size; idx < new_size; ++idx)
        {
            // All cc handler results in an operation points to the same tx
            // machine.
            auto &res = results_.emplace_back(results_.at(0).Txm());
            TX_TRACE_ASSOCIATE(this, &res);

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

        acquire_write_entries_.resize(new_size);
    }
}

void AcquireWriteOperation::Forward(TransactionExecution *txm)
{
    // start the state machine if not running.
    if (!is_running_)
    {
        txm->Process(*this);
    }

    if (remote_ack_cnt_.load(std::memory_order_acquire) > 0)
    {
        bool time_out = txm->IsTimeOut();

        if (time_out)
        {
            // At least one remote acquire request has not received
            // acknowledgement and the acquire write phase has timed out. Forces
            // un-acknowledged requests to finish with an error.
            for (size_t idx = 0; idx < acquire_write_cnt_; ++idx)
            {
                CcHandlerResult<AcquireKeyResult> &hd_result = results_.at(idx);
                const AcquireKeyResult &acquire_key_res = hd_result.Value();
                const CcEntryAddr &cce_addr = acquire_key_res.cce_addr_;

                if (cce_addr.Term() < 0)
                {
                    bool success = hd_result.ForceError();
                    if (success)
                    {
                        // Up until this point, we consider that the acquire
                        // request has failed. The tx will proceed to abort
                        // without trying to release the lock claimed by this
                        // request. However, it is still possible that the
                        // acquire request actually succeeds, either because the
                        // response message is lost or the remote node is
                        // extremely slow and the response arrives after this
                        // point. We rely on the lock recovery mechanism in the
                        // remote node to clear such an orphan lock.
                        continue;
                    }
                }

                // The acquire request may have 1) finished successfully, 2)
                // finished with an error, 3) acknowledged. Only 2) does not
                // need post-processing to clear the lock or to delete the
                // acquire request from the blocking queue in the remote node.
                if (!hd_result.IsError())
                {
                    WriteSetEntry &write_entry =
                        *acquire_write_entries_.at(idx);
                    // Assigns to the write entry the cc entry address obtained
                    // in the acquire phase.
                    write_entry.cce_addr_ = cce_addr;

                    // Only tx's under repeatable read or serializability have
                    // non-empty read sets.
                    uint64_t read_version = txm->rw_set_.DedupRead(cce_addr);
                    if (read_version > 0 &&
                        read_version != acquire_key_res.commit_ts_)
                    {
                        // Each write-set key acquires a write lock and gets the
                        // key's last validation ts and commit ts. If the write
                        // key has been read before and the key's commit ts
                        // mismatches the prior version, this is not a
                        // repeatable read.
                        fail_cnt_.fetch_add(1, std::memory_order_relaxed);
                    }
                }
            }

            txm->PostProcess(*this);
            return;
        }
    }

    // For case remote_ack_cnt_ > 0, we should also check whether we have gotten
    // enough finish_cnt_. For example, remote node is dead and SendMessage
    // fails.
    if (finish_cnt_.load() == acquire_write_cnt_)
    {
        // TODO: for locking-based protocols, though the tx may be blocked
        // arbitrarily long, after all acquire requests are acknowledged, we
        // still need to periodically check liveness of the remote node.
        std::unordered_set<uint32_t> outdated_node_set;

        for (size_t idx = 0; idx < acquire_write_cnt_; ++idx)
        {
            const AcquireKeyResult &acquire_key_res = results_.at(idx).Value();
            const CcEntryAddr &addr = acquire_key_res.cce_addr_;

            if (!results_.at(idx).IsError())
            {
                WriteSetEntry &write_entry = *acquire_write_entries_.at(idx);
                // Assigns to the write entry the cc entry address obtained
                // in the acquire phase.
                write_entry.cce_addr_ = addr;
                uint64_t read_version = txm->rw_set_.DedupRead(addr);
                if (read_version > 0 &&
                    read_version != acquire_key_res.commit_ts_)
                {
                    // Each write-set key acquires a write lock and gets the
                    // key's last validation ts and commit ts. If the write
                    // key has been read before and the key's commit ts
                    // mismatches the prior version, this is not a
                    // repeatable read.
                    rset_has_expired_.store(true, std::memory_order_relaxed);
                }
            }
            else if (results_.at(idx).ErrorCode() == -1)
            {
                if (retry_num_ == 0)
                {
                    auto find_it = outdated_node_set.find(addr.NodeGroupId());
                    if (find_it == outdated_node_set.end())
                    {
                        Sharder::Instance().UpdateLeader(addr.NodeGroupId());
                        outdated_node_set.emplace(addr.NodeGroupId());
                    }
                }
                else if (retry_num_ > 0)
                {
                    ReRunOp(txm);
                    return;
                }
            }
        }

        txm->PostProcess(*this);
    }
    return;
}

SetCommitTsOperation::SetCommitTsOperation(TransactionExecution *txm)
    : hd_result_(txm)
{
    TX_TRACE_ASSOCIATE(this, &hd_result_);
}

void SetCommitTsOperation::Reset()
{
    hd_result_.Reset();
}

void SetCommitTsOperation::Forward(TransactionExecution *txm)
{
    // start the state machine if not running.
    if (!is_running_)
    {
        txm->Process(*this);
    }

    // SetCommitTsOperation is a local call, should always succeeds.
    if (hd_result_.IsFinished())
    {
        txm->PostProcess(*this);
    }
}

ValidateOperation::ValidateOperation(TransactionExecution *txm)
{
    results_.reserve(16);

    for (size_t idx = 0; idx < 16; ++idx)
    {
        auto &res = results_.emplace_back(txm);
        TX_TRACE_ASSOCIATE(this, &res);
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
            TX_TRACE_ASSOCIATE(this, &res);

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
    // start the state machine if running.
    if (!is_running_)
    {
        txm->Process(*this);
    }

    size_t finish_cnt = finish_cnt_.load(std::memory_order_acquire);

    if (finish_cnt < vali_cnt_ && txm->IsTimeOut())
    {
        for (size_t idx = 0; idx < vali_cce_addr_.size(); ++idx)
        {
            // For every validation request, attempts to force the request
            // to be errored. If the request has not received response
            // and is forced to be errored, the corresponding key needs
            // post-processing. If the request finishes, either successfuly
            // or with an error code, forcing the request will not succeed and
            // the request does not need post-processing.
            auto &vali_result = results_.at(idx);
            bool success = vali_result.ForceError();
            if (!success)
            {
                txm->rw_set_.DedupRead(*vali_cce_addr_.at(idx));
            }
        }

        txm->PostProcess(*this);
    }
    else if (finish_cnt == vali_cnt_)
    {
        // All validation requests have returned, either successfully or with
        // error codes. Post-processing skips read-set keys.
        txm->rw_set_.ClearReadSet();
        txm->rw_set_.ClearScanSet();

        if (!error_.load(std::memory_order_acquire))
        {
            size_t idx = 0;
            for (; idx < vali_cnt_; ++idx)
            {
                if (results_[idx].Value().size() > 0)
                {
                    // If some entries's validation results contain conflict
                    // transactions, e.g. the target entry holds a write lock
                    // during validation. Abort the transaction now.
                    // TODO: Abort() is too strict here. Consider the following
                    // cases: 1. the commit_ts of the write(held write lock)
                    // transaction is bigger than validate transaction, 2. the
                    // write transaction abort when we re-check at here. The
                    // above cases allow the validate transaction to commit
                    // successfully.
                    error_.store(true, std::memory_order_relaxed);
                    break;
                }
            }
        }

        // validation cannot re-run since the remote locks of the readset are
        // lost during auto-failover, we should abort the transaction if remote
        // node, which contains read entries, is dead.
        txm->PostProcess(*this);
    }
}

WriteToLogOp::WriteToLogOp(TransactionExecution *txm) : hd_result_(txm)
{
    TX_TRACE_ASSOCIATE(this, &hd_result_);
}

void WriteToLogOp::Forward(TransactionExecution *txm)
{
    // start the state machine if not running.
    if (!is_running_)
    {
        txm->Process(*this);
    }

    if (hd_result_.IsFinished())
    {
        if (hd_result_.ErrorCode() ==
                (int8_t) HandlerResultErrorType::Unknown &&
            retry_num_ > 0)
        {
            LOG(INFO) << "Retry Write Log Request, tx_number: "
                      << txm->tx_number_;
            // log request return unknown status, we need to check tx status
            // from log service.
            ::txlog::LogRequest &log_req = log_closure_.LogRequest();
            ::txlog::WriteLogRequest *log_rec =
                log_req.mutable_write_log_request();
            log_rec->set_retry(true);

            ReRunOp(txm);
            return;
        }

        ACTION_FAULT_INJECTOR("write_log_finished");
        txm->PostProcess(*this);
    }
}

void WriteToLogOp::Reset()
{
    log_group_id_ = 0;
    hd_result_.Reset();
    log_closure_.Reset();
}

UpdateTxnStatus::UpdateTxnStatus(TransactionExecution *txm) : hd_result_(txm)
{
    TX_TRACE_ASSOCIATE(this, &hd_result_);
}

void UpdateTxnStatus::Reset()
{
    hd_result_.Reset();
}

void UpdateTxnStatus::Forward(TransactionExecution *txm)
{
    // start the state machine if not running.
    if (!is_running_)
    {
        txm->Process(*this);
    }

    if (hd_result_.IsFinished())
    {
        if (hd_result_.IsError())
        {
            // Updating the tx's status should never fail.
            txm->PostProcess(*this);
        }
        else
        {
            txm->PostProcess(*this);
        }
    }
}

InitTxnOperation::InitTxnOperation(TransactionExecution *txm) : hd_result_(txm)
{
    TX_TRACE_ASSOCIATE(this, &hd_result_);
}

void InitTxnOperation::Reset()
{
    hd_result_.Reset();
}

void InitTxnOperation::Forward(TransactionExecution *txm)
{
    // start the state machine if not running.
    if (!is_running_)
    {
        txm->Process(*this);
    }

    if (hd_result_.IsFinished())
    {
        txm->PostProcess(*this);
    }
}

PostProcessOp::PostProcessOp(TransactionExecution *txm)
{
    read_results_.reserve(8);
    write_results_.reserve(8);

    for (size_t idx = 0; idx < 8; ++idx)
    {
        CcHandlerResult<std::vector<TxId>> &res =
            read_results_.emplace_back(txm);
        TX_TRACE_ASSOCIATE(this, &res);

        res.post_lambda_ = [this](CcHandlerResult<std::vector<TxId>> *)
        { finish_cnt_.fetch_add(1); };
    }

    for (size_t idx = 0; idx < 8; ++idx)
    {
        CcHandlerResult<Void> &res = write_results_.emplace_back(txm);
        TX_TRACE_ASSOCIATE(this, &res);

        res.post_lambda_ = [this](CcHandlerResult<Void> *)
        { finish_cnt_.fetch_add(1); };
    }
}

void PostProcessOp::Reset(size_t read_cnt, size_t write_cnt)
{
    finish_cnt_.store(0);
    acquire_write_cnt_ = read_cnt + write_cnt;
    Resize(read_cnt, write_cnt);
}

void PostProcessOp::Resize(size_t read_cnt, size_t write_cnt)
{
    size_t read_old_size = read_results_.size();

    if (read_cnt < read_old_size)
    {
        if (read_old_size > TransactionExecution::LargeTxKeySize)
        {
            size_t shrink_size = std::max(read_cnt, (size_t) 8);
            read_results_.erase(read_results_.begin() + shrink_size,
                                read_results_.end());
            read_results_.shrink_to_fit();
        }
    }
    else if (read_cnt > read_old_size)
    {
        for (size_t idx = read_old_size; idx < read_cnt; ++idx)
        {
            auto &res = read_results_.emplace_back(read_results_[0].Txm());
            TX_TRACE_ASSOCIATE(this, &res);
            res.post_lambda_ = [this](CcHandlerResult<std::vector<TxId>> *)
            { finish_cnt_.fetch_add(1); };
        }
    }

    size_t write_old_size = write_results_.size();
    if (write_cnt < write_old_size)
    {
        if (write_old_size > TransactionExecution::LargeTxKeySize)
        {
            size_t shrink_size = std::max(write_cnt, (size_t) 8);
            write_results_.erase(write_results_.begin() + shrink_size,
                                 write_results_.end());
            write_results_.shrink_to_fit();
        }
    }
    else if (write_cnt > write_old_size)
    {
        for (size_t idx = write_old_size; idx < write_cnt; ++idx)
        {
            auto &res = write_results_.emplace_back(write_results_[0].Txm());
            TX_TRACE_ASSOCIATE(this, &res);
            res.post_lambda_ = [this](CcHandlerResult<Void> *)
            { finish_cnt_.fetch_add(1); };
        }
    }
}

void PostProcessOp::Forward(TransactionExecution *txm)
{
    // start the state machine if not running.
    if (!is_running_)
    {
        txm->Process(*this);
    }

    if (txm->IsTimeOut())
    {
        TX_TRACE_ACTION_WITH_CONTEXT(
            this,
            "Forward.IsTimeout",
            txm,
            [txm]() -> std::string
            {
                return std::string(",\"tx_number\":")
                    .append(std::to_string(txm->TxNumber()))
                    .append(",\"term\":")
                    .append(std::to_string(txm->TxTerm()));
            });
    }

    if (finish_cnt_.load(std::memory_order_acquire) == acquire_write_cnt_ ||
        txm->IsTimeOut())
    {
        // Post-processing does not retry. A failed request leaves an orphan
        // lock/intent, which are recovered separately.
        txm->PostProcess(*this);
    }
}

FaultInjectOp::FaultInjectOp(TransactionExecution *txm) : hd_result_(txm)
{
    TX_TRACE_ASSOCIATE(this, &hd_result_);
}

void FaultInjectOp::Reset()
{
    hd_result_.Reset();
}

void FaultInjectOp::Forward(TransactionExecution *txm)
{
    // start the state machine if not running.
    if (!is_running_)
    {
        txm->Process(*this);
    }

    if (hd_result_.IsFinished())
    {
        if (hd_result_.Value() == true)
        {
            succeed_ = true;
            txm->PostProcess(*this);
        }
        else
        {
            succeed_ = false;
            txm->PostProcess(*this);
        }
    }
}

ScanOpenOperation::ScanOpenOperation(TransactionExecution *txm)
    : hd_result_(txm)
{
    TX_TRACE_ASSOCIATE(this, &hd_result_);
}

void ScanOpenOperation::Reset()
{
    hd_result_.Reset();
}

void ScanOpenOperation::Forward(TransactionExecution *txm)
{
    // start the state machine if not running.
    if (!is_running_)
    {
        txm->Process(*this);
    }

    if (!hd_result_.IsFinished())
    {
        bool time_out = txm->IsTimeOut();

        if (time_out)
        {
            TX_TRACE_ACTION_WITH_CONTEXT(
                this,
                "Forward.IsTimeout",
                txm,
                [txm]() -> std::string
                {
                    return std::string(",\"tx_number\":")
                        .append(std::to_string(txm->TxNumber()))
                        .append(",\"term\":")
                        .append(std::to_string(txm->TxTerm()));
                });

            if (retry_num_ > 0)
            {
                ReRunOp(txm);
                return;
            }
            else
            {
                hd_result_.ForceError();
                // TODO: So far we do not store scanned keys in the tx's scan
                // set. In future, we need ScanOpenResult to check which cc
                // nodes have returned and to release scan locks in these cc
                // nodes in post-processing.
                txm->PostProcess(*this);
            }
        }
    }
    else
    {
        // Error code -1 indicates send message failed or term changed.
        if (hd_result_.ErrorCode() == -1 && retry_num_ > 0)
        {
            if (retry_num_ == 0)
            {
                Sharder::Instance().UpdateLeaders();
            }
            else if (retry_num_ > 0)
            {
                ReRunOp(txm);
                return;
            }
        }
        else
        {
            txm->PostProcess(*this);
        }
    }
}

ScanNextOperation::ScanNextOperation(TransactionExecution *txm)
    : hd_result_(txm)
{
    TX_TRACE_ASSOCIATE(this, &hd_result_);
}

void ScanNextOperation::Reset()
{
    hd_result_.Reset();
    scanner_ = nullptr;
    alias_ = 0;
}

void ScanNextOperation::Forward(TransactionExecution *txm)
{
    // start the state machine if not running.
    if (!is_running_)
    {
        txm->Process(*this);
    }

    if (!hd_result_.IsFinished())
    {
        bool time_out = txm->IsTimeOut();
        if (time_out)
        {
            TX_TRACE_ACTION_WITH_CONTEXT(
                this,
                "Forward.IsTimeout",
                txm,
                [txm]() -> std::string
                {
                    return std::string(",\"tx_number\":")
                        .append(std::to_string(txm->TxNumber()))
                        .append(",\"term\":")
                        .append(std::to_string(txm->TxTerm()));
                });
            hd_result_.ForceError();
            txm->PostProcess(*this);
        }
    }
    else
    {
        // Error code -1 indicates send message failed or term changed.
        if (hd_result_.ErrorCode() == -1)
        {
            if (retry_num_ == 0)
            {
                Sharder::Instance().UpdateLeader(
                    hd_result_.Value().node_group_id_);
            }
            else if (retry_num_ > 0)
            {
                ReRunOp(txm);
                return;
            }
        }
        else
        {
            txm->PostProcess(*this);
        }
    }
}

AcquireAllOp::AcquireAllOp(TransactionExecution *txm)
{
    hd_results_.reserve(8);
    retry_num_ = 1;

    for (size_t idx = 0; idx < 8; ++idx)
    {
        auto &res = hd_results_.emplace_back(txm);
        TX_TRACE_ASSOCIATE(this, &res);

        res.post_lambda_ = [this](CcHandlerResult<AcquireAllResult> *hres)
        {
            if (hres->IsError())
            {
                fail_cnt_.fetch_add(1);

                const AcquireAllResult &acq_result = hres->Value();
                if (acq_result.node_term_ < 0 &&
                    acq_result.remote_ack_cnt_ != nullptr)
                {
                    remote_ack_cnt_.fetch_sub(1);
                }
            }

            finish_cnt_.fetch_add(1);
        };
    }
}

void AcquireAllOp::Resize(size_t new_size)
{
    size_t old_size = hd_results_.size();

    if (new_size > old_size)
    {
        for (size_t idx = old_size; idx < new_size; ++idx)
        {
            // All cc handler results in an operation points to the same tx
            // machine.
            auto &res = hd_results_.emplace_back(hd_results_.at(0).Txm());
            TX_TRACE_ASSOCIATE(this, &res);

            res.post_lambda_ = [this](CcHandlerResult<AcquireAllResult> *hres)
            {
                if (hres->IsError())
                {
                    fail_cnt_.fetch_add(1);

                    const AcquireAllResult &acq_result = hres->Value();
                    if (acq_result.node_term_ < 0 &&
                        acq_result.remote_ack_cnt_ != nullptr)
                    {
                        remote_ack_cnt_.fetch_sub(1);
                    }
                }
                finish_cnt_.fetch_add(1);
            };
        }
    }
}

void AcquireAllOp::Reset(size_t node_cnt)
{
    finish_cnt_.store(0);
    fail_cnt_.store(0);
    remote_ack_cnt_.store(0);
    upload_cnt_ = node_cnt;
    Resize(node_cnt);
}

void AcquireAllOp::Forward(TransactionExecution *txm)
{
    // start the state machine if not running.
    if (!is_running_)
    {
        txm->Process(*this);
    }
    if (remote_ack_cnt_.load(std::memory_order_acquire) > 0)
    {
        bool time_out = txm->IsTimeOut();

        if (time_out)
        {
            TX_TRACE_ACTION_WITH_CONTEXT(
                this,
                "Forward.IsTimeOut",
                txm,
                [txm]() -> std::string
                {
                    return std::string(",\"tx_number\":")
                        .append(std::to_string(txm->TxNumber()))
                        .append(",\"term\":")
                        .append(std::to_string(txm->TxTerm()));
                });
            // At least one remote acquire request has not received
            // acknowledgement and the upload phase has timed out. Forces
            // un-acknowledged requests to finish with an error.
            for (size_t nid = 0; nid < upload_cnt_; ++nid)
            {
                CcHandlerResult<AcquireAllResult> &hd_result = hd_results_[nid];
                const AcquireAllResult &acquire_res = hd_result.Value();

                uint64_t ts = std::max(acquire_res.commit_ts_ + 1,
                                       acquire_res.last_vali_ts_ + 1);
                txm->commit_ts_bound_ = std::max(txm->commit_ts_bound_, ts);

                if (acquire_res.node_term_ < 0)
                {
                    bool success = hd_result.ForceError();
                    if (success || hd_result.ErrorCode() == -1)
                    {
                        if (retry_num_ == 0)
                        {
                            Sharder::Instance().UpdateLeader(nid);
                        }
                        else if (retry_num_ > 0)
                        {
                            ReRunOp(txm);
                            return;
                        }
                    }
                }
            }

            txm->PostProcess(*this);
        }
    }
    else if (finish_cnt_.load() == upload_cnt_)
    {
        // TODO: for locking-based protocols, though the tx may be blocked
        // arbitrarily long, after all acquire requests are acknowledged, we
        // still need to periodically check liveness of the remote node.

        if (fail_cnt_.load(std::memory_order_acquire) == 0)
        {
            for (size_t idx = 0; idx < upload_cnt_; ++idx)
            {
                const AcquireAllResult &acquire_res = hd_results_[idx].Value();
                uint64_t ts = std::max(acquire_res.commit_ts_ + 1,
                                       acquire_res.last_vali_ts_ + 1);
                txm->commit_ts_bound_ = std::max(txm->commit_ts_bound_, ts);

                assert(acquire_res.node_term_ >= 0);
            }
        }
        else
        {
            for (size_t nid = 0; nid < upload_cnt_; ++nid)
            {
                if (hd_results_[nid].ErrorCode() == -1)
                {
                    if (retry_num_ == 0)
                    {
                        Sharder::Instance().UpdateLeader(nid);
                    }
                    else if (retry_num_ > 0)
                    {
                        ReRunOp(txm);
                        return;
                    }
                }
            }
        }

        txm->PostProcess(*this);
    }
}

PostWriteAllOp::PostWriteAllOp(TransactionExecution *txm)
{
    hd_results_.reserve(8);
    retry_num_ = 1;

    for (size_t idx = 0; idx < 8; ++idx)
    {
        CcHandlerResult<Void> &res = hd_results_.emplace_back(txm);
        TX_TRACE_ASSOCIATE(this, &res);

        res.post_lambda_ = [this](CcHandlerResult<Void> *)
        { finish_cnt_.fetch_add(1); };
    }
}

void PostWriteAllOp::Reset(uint32_t ng_cnt)
{
    finish_cnt_.store(0);
    upload_cnt_ = ng_cnt;
    Resize(ng_cnt);
}

void PostWriteAllOp::Resize(uint32_t ng_cnt)
{
    size_t old_size = hd_results_.size();
    if (ng_cnt > old_size)
    {
        for (size_t idx = old_size; idx < ng_cnt; ++idx)
        {
            auto &res = hd_results_.emplace_back(hd_results_[0].Txm());
            TX_TRACE_ASSOCIATE(this, &res);
            res.post_lambda_ = [this](CcHandlerResult<Void> *)
            { finish_cnt_.fetch_add(1); };
        }
    }
}

void PostWriteAllOp::Forward(TransactionExecution *txm)
{
    // start the state machine if not running.
    if (!is_running_)
    {
        txm->Process(*this);
    }

    if (finish_cnt_.load(std::memory_order_acquire) == upload_cnt_)
    {
        txm->PostProcess(*this);
    }
    else if (txm->IsTimeOut())
    {
        TX_TRACE_ACTION_WITH_CONTEXT(
            this,
            "Forward.IsTimeOut",
            txm,
            [txm]() -> std::string
            {
                return std::string(",\"tx_number\":")
                    .append(std::to_string(txm->TxNumber()))
                    .append(",\"term\":")
                    .append(std::to_string(txm->TxTerm()));
            });
        for (size_t nid = 0; nid < upload_cnt_; ++nid)
        {
            hd_results_[nid].ForceError();
        }
        txm->PostProcess(*this);
    }
}

DsUpsertTableOp::DsUpsertTableOp(const TableName *table_name,
                                 bool is_deleted,
                                 TransactionExecution *txm)
    : table_name_(table_name), is_deleted_(is_deleted), hd_result_(txm)
{
    TX_TRACE_ASSOCIATE(this, &hd_result_);
}

void DsUpsertTableOp::Reset()
{
    hd_result_.Reset();
}

void DsUpsertTableOp::Forward(TransactionExecution *txm)
{
    // start the state machine if not running.
    if (!is_running_)
    {
        txm->Process(*this);
    }

    if (hd_result_.IsFinished())
    {
        txm->PostProcess(*this);
    }
}

SchemaOp::SchemaOp(const TableName &table_name,
                   const char *image_ptr,
                   size_t image_len)
    : table_key_(table_name)
{
    image_str_ = std::string(image_ptr, image_len);
    catalog_rec_.SetSchemaImage(image_str_);
}

UpsertTableOp::UpsertTableOp(const TableName &table_name,
                             const char *image_ptr,
                             size_t len,
                             bool is_deleted,
                             TransactionExecution *txm)
    : SchemaOp(table_name, image_ptr, len),
      is_deleted_(is_deleted),
      acquire_all_intent_op_(txm),
      prepare_log_op_(txm),
      post_all_intent_op_(txm),
      upsert_kv_table_op_(&table_key_.Name(), is_deleted, txm),
      acquire_all_lock_op_(txm),
      commit_log_op_(txm),
      post_all_lock_op_(txm),
      clean_log_op_(txm)
{
    acquire_all_intent_op_.table_name_ = &catalog_ccm_name;
    acquire_all_intent_op_.key_ = &table_key_;
    acquire_all_intent_op_.lk_type_ = LockType::WriteIntent;
    acquire_all_intent_op_.protocol_ = CcProtocol::OCC;

    post_all_intent_op_.table_name_ = &catalog_ccm_name;
    post_all_intent_op_.key_ = &table_key_;
    post_all_intent_op_.rec_ = &catalog_rec_;
    post_all_intent_op_.dml_op_ =
        is_deleted ? DmlOperation::Delete : DmlOperation::Upsert;
    post_all_intent_op_.write_type_ = PostWriteType::PrepareCommit;

    acquire_all_lock_op_.table_name_ = &catalog_ccm_name;
    acquire_all_lock_op_.key_ = &table_key_;
    acquire_all_lock_op_.lk_type_ = LockType::WriteLock;
    acquire_all_lock_op_.protocol_ = CcProtocol::Locking;

    post_all_lock_op_.table_name_ = &catalog_ccm_name;
    post_all_lock_op_.key_ = &table_key_;
    post_all_lock_op_.rec_ = &catalog_rec_;
    post_all_lock_op_.dml_op_ =
        is_deleted ? DmlOperation::Delete : DmlOperation::Upsert;
    post_all_lock_op_.write_type_ = PostWriteType::PostCommit;

    TX_TRACE_ASSOCIATE(this, &acquire_all_intent_op_, "acquire_all_intent_op_");
    TX_TRACE_ASSOCIATE(this, &prepare_log_op_, "prepare_log_op_");
    TX_TRACE_ASSOCIATE(this, &post_all_intent_op_, "post_all_intent_op_");
    TX_TRACE_ASSOCIATE(this, &upsert_kv_table_op_, "upsert_kv_table_op_");
    TX_TRACE_ASSOCIATE(this, &acquire_all_lock_op_, "acquire_all_lock_op_");
    TX_TRACE_ASSOCIATE(this, &commit_log_op_, "commit_log_op_");
    TX_TRACE_ASSOCIATE(this, &post_all_lock_op_, "post_all_lock_op_");
    TX_TRACE_ASSOCIATE(this, &clean_log_op_, "clean_log_op_");
}

void UpsertTableOp::Forward(TransactionExecution *txm)
{
    if (op_ == nullptr)
    {
        op_ = &acquire_all_intent_op_;
        txm->PushOperation(&acquire_all_intent_op_);
        txm->Process(acquire_all_intent_op_);
    }
    else if (op_ == &acquire_all_intent_op_)
    {
        if (acquire_all_intent_op_.fail_cnt_.load(std::memory_order_acquire) >
            0)
        {
            DLOG(ERROR)
                << "Upsert table acquire write intent failed, tx_number:"
                << txm->tx_number_;
            txm->bool_resp_->SetErrorCode(
                TxErrorCode::UPSERT_TABLE_ACQUIRE_WRITE_INTENT_FAIL);
            // Fails to acquire the write intent on the schema. Since write
            // intents only conflict with other writes, there must be
            // another tx trying to modify the same table's schema. Stops
            // the schema operation. Set the commit ts to 0 to signal that
            // the following post write operation releases all write
            // intents.
            txm->commit_ts_ = 0;
            // Moves to the last operation that removes all write
            // intents/locks.
            op_ = &post_all_lock_op_;
            txm->PushOperation(&post_all_lock_op_);
            txm->Process(post_all_lock_op_);
            return;
        }

        // Assigns a commit timestamp to the tx state machine as the version
        // of the new schema. Unlike the conventional commit ts, the
        // schema's commit ts is not chosen via the SetTs stage. But it
        // follows a similar set of rules: the new schema's version should
        // be greater than 1) the current version, 2) the maximal commit ts
        // of all tx's that have read the schema, and 3) the local time when
        // the tx starts.
        txm->commit_ts_ = txm->commit_ts_bound_ + 1;

        for (size_t idx = 0; idx < acquire_all_intent_op_.upload_cnt_; ++idx)
        {
            const AcquireAllResult &acq_all_res =
                acquire_all_intent_op_.hd_results_[idx].Value();
            uint64_t ts = std::max(acq_all_res.commit_ts_ + 1,
                                   acq_all_res.last_vali_ts_ + 1);
            txm->commit_ts_ = std::max(txm->commit_ts_, ts);
        }

        op_ = &prepare_log_op_;
        FillPrepareLogRequest(txm);
        txm->PushOperation(&prepare_log_op_);
        txm->Process(prepare_log_op_);
    }
    else if (op_ == &prepare_log_op_)
    {
        if (prepare_log_op_.hd_result_.IsError())
        {
            DLOG(ERROR) << "Upsert table write prepare log failed, tx_number:"
                        << txm->tx_number_;
            // Fails to flush the prepare log. The schema operation is
            // considered failed if the prepare log is not flushed. The
            // commit ts is set to 0 to signal that the following post write
            // operation releases all write intents.
            txm->commit_ts_ = 0;
            // Moves to the last operation that removes all write
            // intents/locks.
            op_ = &post_all_lock_op_;

            txm->bool_resp_->SetErrorCode(
                TxErrorCode::UPSERT_TABLE_PREPARE_FAIL);

            txm->PushOperation(&post_all_lock_op_);
            txm->Process(post_all_lock_op_);
        }
        else
        {
            op_ = &post_all_intent_op_;

            txm->PushOperation(&post_all_intent_op_);
            txm->Process(post_all_intent_op_);
        }
    }
    else if (op_ == &post_all_intent_op_)
    {
        bool failed = false;
        for (size_t idx = 0; idx < post_all_intent_op_.upload_cnt_; ++idx)
        {
            if (post_all_intent_op_.hd_results_[idx].IsError())
            {
                failed = true;
                break;
            }
        }

        if (failed)
        {
            // When a cc node leader begins recovery, the candidate term is set
            // to the Raft term. When recovery finishes, the candidate term is
            // set to -1 after the leader term. So, obtains the candidate term
            // before the leader term.
            int64_t tx_node_candid_term =
                Sharder::Instance().CandidateLeaderTerm(txm->TxCcNodeId());
            int64_t tx_node_term =
                Sharder::Instance().LeaderTerm(txm->TxCcNodeId());

            // After the prepare log is flushed, the schema op is guaranteed to
            // succeed and can only roll forward. Retry this step to install the
            // dirty schema in the tx service, if the tx node is still the
            // leader. The tx is also allowed to proceed if the tx is in the
            // recovery mode and the tx node is a leader candidate.

            if (tx_node_term >= 0 ||
                (txm->tx_status_ == TxnStatus::Recovering &&
                 tx_node_candid_term >= 0))
            {
                // set catalog_rec_'s binary_value_ to image_str since it
                // could be set to TableSchemaView pointer in localshard.
                catalog_rec_.SetSchemaImage(image_str_);
                txm->PushOperation(&post_all_intent_op_);
                txm->Process(post_all_intent_op_);
            }
            else
            {
                ForceToFinish(txm);
            }
        }
        else if (is_deleted_)
        {
            // Store a copy of index table names for UpsertSkTable()
            upsert_kv_table_op_.index_names_ =
                catalog_rec_.Schema()->IndexNames();

            // For DROP TABLE operations, the data store operation of
            // deleting the k-v table happens after the commit log is
            // flushed.
            op_ = &acquire_all_lock_op_;
            txm->PushOperation(&acquire_all_lock_op_);
            txm->Process(acquire_all_lock_op_);
        }
        else
        {
            op_ = &upsert_kv_table_op_;
            // The post write request right after flushing the prepare log
            // installs the dirty schema in the tx service and returns a
            // local view (pointer) of the committed and dirty schema.
            upsert_kv_table_op_.table_schema_ = catalog_rec_.DirtySchema();
            txm->PushOperation(&upsert_kv_table_op_);
            txm->Process(upsert_kv_table_op_);
        }
    }
    else if (op_ == &upsert_kv_table_op_)
    {
        if (upsert_kv_table_op_.hd_result_.IsError())
        {
            // The candidate term is set when the cc node becomes the Raft
            // leader of the cc node group. It is set to -1 after the cc node
            // leader has replayed the log and the leader term is set. Since the
            // candidate term is set to -1 after the leader term , obtains
            // the candidate term before the leader term.
            int64_t tx_node_candid_term =
                Sharder::Instance().CandidateLeaderTerm(txm->TxCcNodeId());
            int64_t tx_node_term =
                Sharder::Instance().LeaderTerm(txm->TxCcNodeId());

            // The data store operation failed. Retries the operation if the
            // tx node is the leader or the tx is in the recovery mode and the
            // cc node is a leader candidate.

            if (tx_node_term >= 0 ||
                (txm->tx_status_ == TxnStatus::Recovering &&
                 tx_node_candid_term >= 0))
            {
                txm->PushOperation(&upsert_kv_table_op_);
                txm->Process(upsert_kv_table_op_);
            }
            else
            {
                ForceToFinish(txm);
            }
        }
        else if (is_deleted_)
        {
            // For DROP TABLE statements, the data store operation happens
            // after the commit log is flushed.
            op_ = &post_all_lock_op_;

            txm->PushOperation(&post_all_lock_op_);
            txm->Process(post_all_lock_op_);
        }
        else
        {
            op_ = &acquire_all_lock_op_;
            txm->PushOperation(&acquire_all_lock_op_);
            txm->Process(acquire_all_lock_op_);
        }
    }
    else if (op_ == &acquire_all_lock_op_)
    {
        if (acquire_all_lock_op_.fail_cnt_.load(std::memory_order_acquire) > 0)
        {
            // When a cc node leader begins recovery, the candidate term is set
            // to the Raft term. When recovery finishes, the candidate term is
            // set to -1 after the leader term. So, obtains the candidate term
            // before the leader term.
            int64_t tx_node_candid_term =
                Sharder::Instance().CandidateLeaderTerm(txm->TxCcNodeId());
            int64_t tx_node_term =
                Sharder::Instance().LeaderTerm(txm->TxCcNodeId());

            // Fails to acquire the write lock. The schema operation can
            // only roll forward after flushing the prepare log. Retries the
            // request if the tx node is still the leader or the tx is in the
            // recovery mode and the cc node is a leader candidate.
            if (tx_node_term >= 0 ||
                (txm->tx_status_ == TxnStatus::Recovering &&
                 tx_node_candid_term >= 0))
            {
                txm->PushOperation(&acquire_all_lock_op_);
                txm->Process(acquire_all_lock_op_);
            }
            else
            {
                ForceToFinish(txm);
            }
        }
        else
        {
            op_ = &commit_log_op_;
            FillCommitLogRequest(txm);
            txm->PushOperation(&commit_log_op_);
            txm->Process(commit_log_op_);
        }
    }
    else if (op_ == &commit_log_op_)
    {
        if (commit_log_op_.hd_result_.IsError())
        {
            // When a cc node leader begins recovery, the candidate term is set
            // to the Raft term. When recovery finishes, the candidate term is
            // set to -1 after the leader term. So, obtains the candidate term
            // before the leader term.
            int64_t tx_node_candid_term =
                Sharder::Instance().CandidateLeaderTerm(txm->TxCcNodeId());
            int64_t tx_node_term =
                Sharder::Instance().LeaderTerm(txm->TxCcNodeId());

            // Fails to flush the commit log. Retries the operation if the tx
            // node is still the leader or the tx is in the  recovery mode and
            // the cc node is a leader candidate.
            if (tx_node_term >= 0 ||
                (txm->tx_status_ == TxnStatus::Recovering &&
                 tx_node_candid_term >= 0))
            {
                txm->PushOperation(&commit_log_op_);
                txm->Process(commit_log_op_);
            }
            else
            {
                ForceToFinish(txm);
            }
        }
        else
        {
            if (is_deleted_)
            {
                op_ = &upsert_kv_table_op_;
                upsert_kv_table_op_.table_schema_ = nullptr;
                txm->PushOperation(&upsert_kv_table_op_);
                txm->Process(upsert_kv_table_op_);
            }
            {
                op_ = &post_all_lock_op_;

                txm->PushOperation(&post_all_lock_op_);
                txm->Process(post_all_lock_op_);
            }
        }
    }
    else if (op_ == &post_all_lock_op_)
    {
        bool failed = false;
        for (size_t idx = 0; idx < post_all_lock_op_.upload_cnt_; ++idx)
        {
            if (post_all_lock_op_.hd_results_[idx].IsError())
            {
                failed = true;
                break;
            }
        }

        if (txm->commit_ts_ == 0)
        {
            // The schema operation failed without flushing the prepare log.
            // Do not retry post-processing (release write intents) even if
            // it fails. Remaining write intents on the schema, if there are
            // any, will be recovered by individual cc nodes separately.
            txm->bool_resp_->Finish(false);

            txm->state_stack_.pop_back();
            assert(txm->state_stack_.empty());
            txm->schema_op_ = nullptr;
        }
        else if (failed)
        {
            // When a cc node leader begins recovery, the candidate term is set
            // to the Raft term. When recovery finishes, the candidate term is
            // set to -1 after the leader term. So, obtains the candidate term
            // before the leader term.
            int64_t tx_node_candid_term =
                Sharder::Instance().CandidateLeaderTerm(txm->TxCcNodeId());
            int64_t tx_node_term =
                Sharder::Instance().LeaderTerm(txm->TxCcNodeId());

            // After the prepare log is flushed, the schema op is guaranteed to
            // succeed and can only roll forward. Retry this step to install the
            // committed schema and remove write locks, if the tx node is still
            // the leader or the tx is in the recovery mode and the cc node is a
            // leader candidate.
            if (tx_node_term >= 0 ||
                (txm->tx_status_ == TxnStatus::Recovering &&
                 tx_node_candid_term >= 0))
            {
                txm->PushOperation(&post_all_lock_op_);
                txm->Process(post_all_lock_op_);
            }
            else
            {
                ForceToFinish(txm);
            }
        }
        else
        {
            // The tx's modification of the schema has succeeded. If the tx
            // has previously read the same schema and keeps a pointer in
            // the read set to the cc entry of the schema, removes it from
            // the read set. As a result, the tx will not try to release the
            // read lock of the schema when committing.
            const CcEntryAddr &schema_entry_addr =
                acquire_all_lock_op_.hd_results_[txm->TxCcNodeId()]
                    .Value()
                    .local_cce_addr_;
            txm->rw_set_.DedupRead(schema_entry_addr);

            op_ = &clean_log_op_;
            FillCleanLogRequest(txm);
            txm->PushOperation(&clean_log_op_);
            txm->Process(clean_log_op_);
        }
    }
    else if (op_ == &clean_log_op_)
    {
        // When a cc node leader begins recovery, the candidate term is set
        // to the Raft term. When recovery finishes, the candidate term is
        // set to -1 after the leader term. So, obtains the candidate term
        // before the leader term.
        int64_t tx_node_candid_term =
            Sharder::Instance().CandidateLeaderTerm(txm->TxCcNodeId());
        int64_t tx_node_term =
            Sharder::Instance().LeaderTerm(txm->TxCcNodeId());

        if (clean_log_op_.hd_result_.IsError() &&
            (tx_node_term >= 0 || (txm->tx_status_ == TxnStatus::Recovering &&
                                   tx_node_candid_term >= 0)))
        {
            txm->PushOperation(&clean_log_op_);
            txm->Process(clean_log_op_);
        }
        else if (txm->tx_status_ == TxnStatus::Recovering)
        {
            // When the tx is in the recovery state, no external caller is
            // waiting for the response. So, txm->bool_resp_ is null.

            txm->Reset();
            // Setting the tx's status to finished signals that this tx
            // state machine can be recycled for a new tx.
            txm->tx_status_.store(TxnStatus::Finished,
                                  std::memory_order_release);
        }
        else
        {
            if (is_deleted_)
            {
                txm->rw_set_.ClearTable(table_key_.Name());
            }

            txm->bool_resp_->Finish(true);
            txm->state_stack_.pop_back();
            assert(txm->state_stack_.empty());
            txm->schema_op_ = nullptr;
        }
    }
}

void UpsertTableOp::FillPrepareLogRequest(TransactionExecution *txm)
{
    prepare_log_op_.log_type_ = TxLogType::PREPARE;

    prepare_log_op_.log_closure_.LogRequest().Clear();

    ::txlog::WriteLogRequest *prepare_log_rec =
        prepare_log_op_.log_closure_.LogRequest().mutable_write_log_request();

    prepare_log_rec->set_tx_term(txm->tx_term_);
    prepare_log_rec->set_txn_number(txm->tx_number_);
    prepare_log_rec->set_commit_timestamp(txm->commit_ts_);

    ::txlog::SchemaOpMessage *prepare_schema_msg =
        prepare_log_rec->mutable_log_content()->mutable_schema_log();
    prepare_schema_msg->set_table_name(table_key_.Name());

    if (is_deleted_)
    {
        prepare_schema_msg->mutable_table_op()->set_is_deleted(true);
        prepare_schema_msg->clear_catalog_blob();
    }
    else
    {
        prepare_schema_msg->mutable_table_op()->set_is_deleted(false);
        prepare_schema_msg->set_catalog_blob(catalog_rec_.SchemaImage());
    }
    prepare_schema_msg->set_stage(::txlog::SchemaOpMessage_Stage_PrepareSchema);

    auto &node_terms = *prepare_log_rec->mutable_node_terms();
    node_terms.clear();
    for (uint32_t nid = 0; nid < acquire_all_intent_op_.upload_cnt_; ++nid)
    {
        node_terms[nid] =
            acquire_all_intent_op_.hd_results_[nid].Value().node_term_;
    }
}

void UpsertTableOp::FillCommitLogRequest(TransactionExecution *txm)
{
    commit_log_op_.log_type_ = TxLogType::COMMIT;

    commit_log_op_.log_closure_.LogRequest().Clear();

    ::txlog::WriteLogRequest *commit_log_rec =
        commit_log_op_.log_closure_.LogRequest().mutable_write_log_request();

    commit_log_rec->set_tx_term(txm->tx_term_);
    commit_log_rec->set_txn_number(txm->tx_number_);
    commit_log_rec->set_commit_timestamp(txm->commit_ts_);

    auto commit_schema_msg =
        commit_log_rec->mutable_log_content()->mutable_schema_log();
    commit_schema_msg->set_stage(::txlog::SchemaOpMessage_Stage_CommitSchema);

    // The prepare log keeps all cc nodes' terms and match them in the log
    // service to detect invalidated write intents. The commit log, however,
    // does not match terms in the log service. This is because all
    // operations after the prepare log are retried or replayed upon
    // failures to guarantee that the schema operation always roll forward.
    // If a cc node fails over, the new node must restore write intents
    // gained prior to the prepare log and then replay operations between
    // the prepare log and the commit log, which in this case upgrade write
    // intents to write locks. So, there is no need to check the liveness of
    // write locks when flushing the commit log.
    commit_log_rec->mutable_node_terms()->clear();
}

void UpsertTableOp::FillCleanLogRequest(TransactionExecution *txm)
{
    clean_log_op_.log_type_ = TxLogType::CLEAN;

    clean_log_op_.log_closure_.LogRequest().Clear();

    ::txlog::WriteLogRequest *clean_log_rec =
        clean_log_op_.log_closure_.LogRequest().mutable_write_log_request();

    clean_log_rec->set_tx_term(txm->tx_term_);
    clean_log_rec->set_txn_number(txm->tx_number_);

    ::txlog::SchemaOpMessage *clean_schema_msg =
        clean_log_rec->mutable_log_content()->mutable_schema_log();

    clean_schema_msg->set_stage(::txlog::SchemaOpMessage_Stage_CleanSchema);
    clean_log_rec->mutable_node_terms()->clear();
}

void UpsertTableOp::ForceToFinish(TransactionExecution *txm)
{
    clean_log_op_.hd_result_.SetFinished();
    op_ = &clean_log_op_;
    Forward(txm);
}

SleepOperation::SleepOperation(TransactionExecution *txm)
{
}

void SleepOperation::Forward(TransactionExecution *txm)
{
    // forward of sleep op will check whether the sleep time reached. Note
    // that we cannot use sleep_for API since the TxProcessor thread cannot
    // be blocked. Instead we use the TimeOut interface to simulate sleep.
    // It may not be accurate, but retry logic is not sensitive to it.
    if (txm->IsTimeOut(sleep_secs_))
    {
        TX_TRACE_ACTION_WITH_CONTEXT(
            this,
            "Forward.IsTimeOut",
            txm,
            [txm]() -> std::string
            {
                return std::string(",\"tx_number\":")
                    .append(std::to_string(txm->TxNumber()))
                    .append(",\"term\":")
                    .append(std::to_string(txm->TxTerm()));
            });
        // pop the sleep op and re-execute the last failed op.
        txm->state_stack_.pop_back();
        txm->Forward();
    }
}

CleanCcEntryForTestOp::CleanCcEntryForTestOp(TransactionExecution *txm)
    : hd_result_(txm)
{
    TX_TRACE_ASSOCIATE(this, &hd_result_);
}

void CleanCcEntryForTestOp::Forward(TransactionExecution *txm)
{
    // start the state machine if not running.
    if (!is_running_)
    {
        txm->Process(*this);
    }

    if (hd_result_.IsFinished())
    {
        if (hd_result_.Value() == true)
        {
            succeed_ = true;
            txm->PostProcess(*this);
        }
        else
        {
            succeed_ = false;
            txm->PostProcess(*this);
        }
    }
}

}  // namespace txservice
