#include "tx_operation.h"

#include <algorithm>
#include <iostream>
#include <string>

#include "../log_service/include/log_type.h"
#include "cc/cc_handler_result.h"
#include "cc_handler.h"
#include "checkpointer.h"
#include "error_messages.h"  //CcErrorCode
#include "fault/fault_inject.h"
#include "local_cc_shards.h"
#include "range_record.h"
#include "sharder.h"
#include "store/data_store_handler.h"
#include "tx_execution.h"
#include "tx_request.h"
#include "tx_service.h"
#include "tx_trace.h"
#include "tx_worker_pool.h"
#include "util.h"

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

ReadOperation::ReadOperation(TransactionExecution *txm)
    : hd_result_(txm)
#ifdef RANGE_PARTITION_ENABLED
      ,
      lock_range_result_(txm)
#endif
{
    TX_TRACE_ASSOCIATE(this, &hd_result_);
}

void ReadOperation::Reset()
{
    hd_result_.Value().Reset();
    hd_result_.Reset();
    local_cache_miss_ = false;
#ifdef RANGE_PARTITION_ENABLED
    lock_range_result_.Value().Reset();
    lock_range_result_.Reset();
#endif
    op_start_ = metrics::TimePoint::max();
}

void ReadOperation::Forward(TransactionExecution *txm)
{
    if (!is_running_)
    {
#ifdef RANGE_PARTITION_ENABLED
        if (read_tx_req_->read_local_)
        {
            txm->Process(*this);
            return;
        }

        // Just returned from LockReadRangeOp, check lock_range_result_.
        assert(lock_range_result_.IsFinished());
        if (lock_range_result_.IsError())
        {
            // There is an error when getting the input key's range. The
            // read operation is set to be errored.
            hd_result_.SetError(CcErrorCode::GET_RANGE_ID_ERR);

            bool force_success = hd_result_.ForceError();
            assert(force_success);

            txm->PostProcess(*this);
            return;
        }
#endif
        txm->Process(*this);
    }

    const CcEntryAddr &cce_addr = hd_result_.Value().cce_addr_;

    if (hd_result_.IsFinished())
    {
        if ((hd_result_.ErrorCode() == CcErrorCode::PIN_RANGE_SLICE_FAILED ||
             hd_result_.ErrorCode() ==
                 CcErrorCode::REQUESTED_NODE_NOT_LEADER) &&
            retry_num_ >= 0)
        {
            // The read request was directed to a non-leader node. Updates
            // the leader cache. Sine UpdateLeader() is a sync call, we only
            // do it when re-run the operation fails.
            if (hd_result_.ErrorCode() ==
                    CcErrorCode::REQUESTED_NODE_NOT_LEADER &&
                retry_num_ == 0)
            {
                Sharder::Instance().UpdateLeader(cce_addr.NodeGroupId());
                retry_num_ = -1;
            }
            else if (retry_num_ > 0)
            {
                hd_result_.Value().Reset();
                hd_result_.Reset();
                ReRunOp(txm);
                return;
            }
        }

#ifndef RANGE_PARTITION_ENABLED
        if (hd_result_.ErrorCode() == CcErrorCode::OUT_OF_MEMORY)
        {
            // If shard is full, keep retrying since checkpoint will
            // clean up memory for new insert.
            retry_num_++;
            hd_result_.Value().Reset();
            hd_result_.Reset();
            ReRunOp(txm);
            return;
        }
#endif
        txm->PostProcess(*this);
    }
    else
    {
        bool timeout = txm->IsTimeOut();
        CODE_FAULT_INJECTOR("read_operation_timeout", {
            LOG(INFO) << "FaultInject  read_operation_timeout";
            timeout = true;
            FaultInject::Instance().InjectFault("read_operation_timeout",
                                                "remove");
        });

        if (!hd_result_.Value().is_local_ && cce_addr.Term() < 0 && timeout)
        {
            TX_TRACE_ACTION_WITH_CONTEXT(
                this,
                "Forward.Term<0,IsTimeout || TxNodeFail",
                txm,
                (
                    [txm]() -> std::string
                    {
                        return std::string(",\"tx_number\":")
                            .append(std::to_string(txm->TxNumber()))
                            .append(",\"term\":")
                            .append(std::to_string(txm->TxTerm()));
                    }));
            // For non-blocking concurrency control protocols, the read
            // request is expected to return instantly. For lock-based
            // protocols, if the read request is blocked, the cc node will
            // send an acknowledgement to update the key's term. In either
            // case, if the read key's term is not set, the tx has not
            // received any response or acknowledgement from the key's cc
            // node group. The read request is forced to be errored upon
            // timeout.
            // FIXME(lzx): Is it more appropriate to retry?
            // If the tx node fails, also force the tx to abort instantly.
            bool force_success = hd_result_.ForceError();
            if (force_success)
            {
                txm->PostProcess(*this);
            }
            // If forcing error fails, it means that the remote response
            // returns normally and the tx has been moved from the waiting
            // queue to the execution queue. Does not continue execution.
            // The tx will be re-executed when the tx processor visits it in
            // the execution queue.
        }
        else if (cce_addr.Term() > 0 && timeout)
        {
            txm->cc_handler_->BlockCcReqCheck(
                txm->TxNumber(),
                txm->TxTerm(),
                txm->CommandId(),
                cce_addr,
                &hd_result_,
                ResultTemplateType::ReadKeyResult);
        }
    }
    // TODO: for locking-based protocols, even though the tx may be blocked
    // arbitrarily long after the read request is acknowledged, we still
    // need to periodically check liveness of the remote node and force the
    // tx to cancel if the remote node is unresponsive.
}

#ifdef RANGE_PARTITION_ENABLED
void LockReadRangeOperation::Reset()
{
    key_ = nullptr;
    range_table_name_ = TableName{empty_sv, TableType::RangePartition};
    range_rec_ = nullptr;
    lock_range_result_ = nullptr;
}

void LockReadRangeOperation::Forward(txservice::TransactionExecution *txm)
{
    if (lock_range_result_->IsFinished())
    {
        // Pop out this LockRangeOperation from the stack and return control to
        // the caller operation by forwarding the transaction state machine.
        txm->PostProcess(*this);
    }
    else
    {
        // TODO(zkl): wait some time and timeout, for async FetchTableRanges
        //  from KV.
        // The get-range request has not finished. The caller operation
        // of this LockRangeOperation cannot proceed without knowing the input
        // key's range. Since lock read range is always a local request, we
        // don't need to check for timeout.
        return;
    }
}

UnlockReadRangeOperation::UnlockReadRangeOperation(
    txservice::TransactionExecution *txm)
    : unlock_range_result_(txm)
{
}

void UnlockReadRangeOperation::Reset()
{
    cce_addr_ = nullptr;
    unlock_range_result_.Reset();
}

void UnlockReadRangeOperation::Forward(txservice::TransactionExecution *txm)
{
    if (unlock_range_result_.IsFinished())
    {
        txm->PostProcess(*this);
    }
    else
    {
        // The unlock-range request has not finished. Just wait since it is a
        // local operation.
        return;
    }
}
#endif

PostReadOperation::PostReadOperation(TransactionExecution *txm)
    : hd_result_(txm)
{
}

void PostReadOperation::ResetHandlerTxm(TransactionExecution *txm)
{
    hd_result_.ResetTxm(txm);
}

void PostReadOperation::Reset(
    std::pair<CcEntryAddr *, ReadSetEntry *> cce_entry)
{
    cce_entry_ = cce_entry;
    hd_result_.Reset();
}

void PostReadOperation::Forward(TransactionExecution *txm)
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

AcquireWriteOperation::AcquireWriteOperation(TransactionExecution *txm)
    : hd_result_(txm)
{
    TX_TRACE_ASSOCIATE(this, &hd_result_);
}

void AcquireWriteOperation::Reset(size_t acquire_write_cnt)
{
    hd_result_.Reset();
    hd_result_.SetRefCnt(acquire_write_cnt);

    std::vector<AcquireKeyResult> &acquire_key_vec = hd_result_.Value();
    size_t old_size = acquire_key_vec.size();
    acquire_key_vec.resize(acquire_write_cnt);
    for (size_t idx = old_size; idx < acquire_write_cnt; ++idx)
    {
        acquire_key_vec[idx].remote_ack_cnt_ = &remote_ack_cnt_;
    }

    remote_ack_cnt_.store(0, std::memory_order_relaxed);
    acquire_write_entries_.resize(acquire_write_cnt);

    rset_has_expired_ = false;
    op_start_ = metrics::TimePoint::max();
}

void AcquireWriteOperation::Reset()
{
    std::vector<AcquireKeyResult> &acquire_key_vec = hd_result_.Value();
    if (acquire_key_vec.capacity() > TransactionExecution::LargeTxKeySize)
    {
        acquire_key_vec.resize(16);
        acquire_key_vec.shrink_to_fit();
    }
    op_start_ = metrics::TimePoint::max();
}

void AcquireWriteOperation::AggregateAcquiredKeys(TransactionExecution *txm)
{
    std::vector<AcquireKeyResult> &acquire_key_vec = hd_result_.Value();
    for (size_t idx = 0; idx < acquire_key_vec.size(); ++idx)
    {
        const AcquireKeyResult &acquire_key_res = acquire_key_vec[idx];
        const CcEntryAddr &addr = acquire_key_res.cce_addr_;
        WriteSetEntry &write_entry = *acquire_write_entries_[idx];

        int64_t term = addr.Term();
        if (term < 0)
        {
            write_entry.cce_addr_.SetCce(0, -1, 0);
            continue;
        }
        else
        {
            // Assigns to the write entry the cc entry address obtained
            // in the acquire phase.
            write_entry.cce_addr_ = addr;
        }

        uint64_t read_version = txm->rw_set_.DedupRead(addr);
        if (read_version > 0 && read_version != acquire_key_res.commit_ts_)
        {
            // Each write-set key acquires a write lock and gets the
            // key's last validation ts and commit ts. If the write
            // key has been read before and the key's commit ts
            // mismatches the prior version, this is not a
            // repeatable read.
            rset_has_expired_ = true;
        }
    }
}

void AcquireWriteOperation::Forward(TransactionExecution *txm)
{
    // start the state machine if not running.
    if (!is_running_)
    {
        txm->Process(*this);
    }

    if (hd_result_.IsFinished())
    {
        if (hd_result_.ErrorCode() == CcErrorCode::REQUESTED_NODE_NOT_LEADER)
        {
            if (retry_num_ == 0)
            {
                // Sharder::Instance().UpdateLeaders();
            }
            else if (retry_num_ > 0)
            {
                ReRunOp(txm);
                return;
            }
        }

        AggregateAcquiredKeys(txm);
        txm->PostProcess(*this);
    }
    else
    {
        bool timeout = txm->IsTimeOut();
        CODE_FAULT_INJECTOR("acquire_operation_timeout", {
            LOG(INFO)
                << "FaultInject  acquire_operation_timeout remote_ack_cnt_:"
                << remote_ack_cnt_;
            timeout = true;
            FaultInject::Instance().InjectFault("acquire_operation_timeout",
                                                "remove");
        });

        if (remote_ack_cnt_.load(std::memory_order_acquire) > 0 && timeout)
        {
            // FIXME(lzx): Is it more appropriate to retry if
            // remote_ack_cnt_>0 ? If the tx node fails, force the tx to
            // abort instantly.
            // TODO: for 2PL, the tx may be blocked arbitrarily long, even
            // after all acquire requests are acknowledged. We still need to
            // periodically check liveness of the remote node.
            bool success = hd_result_.ForceError();
            if (success)
            {
                AggregateAcquiredKeys(txm);
                txm->PostProcess(*this);
            }
            // Else, all acquire-write requests finish normally. The tx must
            // have been moved from the waiting queue to the execution
            // queue. Does not forword the tx now, as it will be re-executed
            // when the tx processor visits the execution queue.
        }
        else if (timeout)
        {
            std::vector<AcquireKeyResult> &vct_akr = hd_result_.Value();
            for (size_t i = 0; i < vct_akr.size(); i++)
            {
                AcquireKeyResult &akr = vct_akr[i];

                if (akr.cce_addr_.Term() > 0 /*&& akr.commit_ts_ == 0*/)
                {
                    txm->cc_handler_->BlockCcReqCheck(
                        txm->TxNumber(),
                        txm->TxTerm(),
                        txm->CommandId(),
                        akr.cce_addr_,
                        &hd_result_,
                        ResultTemplateType::AcquireKeyResult);
                }
            }
        }
    }
}

void LockWriteRangesOp::Forward(TransactionExecution *txm)
{
    if (!is_running_)
    {
        txm->Process(*this);
    }
    else if (lock_range_result_.IsFinished())
    {
        txm->PostProcess(*this);
    }
}

void LockWriteRangesOp::Advance()
{
    // Advances the write key iterator such that it points to the first key
    // belonging to the next range.
    const TxKey *range_end_key = range_rec_.end_key_;
    auto next_range_start = write_key_it_;
    if (range_end_key == nullptr ||
        range_end_key->Type() == KeyType::PositiveInf)
    {
        next_range_start = write_key_end_;
    }
    else
    {
        TableWriteSet &table_write_set = table_it_->second;
        next_range_start = table_write_set.lower_bound(range_end_key);
    }

    NodeGroupId range_owner = range_rec_.GetRangeOwnerNg()->BucketOwner();
    const std::vector<const BucketInfo *> *splitting_range_owners =
        range_rec_.GetNewRangeOwnerNgs();
    // Updates the sharding codes of the write-set keys belonging to this
    // range. The higher 22 bits represent the range ID.
    NodeGroupId new_range_owner = UINT32_MAX;
    size_t new_range_idx = 0;

    auto *range_info = range_rec_.GetRangeInfo();
    while (write_key_it_ != next_range_start)
    {
        WriteSetEntry &write_entry = write_key_it_->second;
        size_t hash = write_entry.key_->Hash();
        write_entry.key_shard_code_ = (range_owner << 10) | (hash & 0x3FF);

        // If range is splitting and the key will fall on a new range after
        // split is finished, register forward_key_shard_code_ to indicate
        // entry needs to be double written.
        while (range_info->IsDirty() &&
               new_range_idx < range_info->NewKey()->size() &&
               !(*write_entry.key_ < *range_info->NewKey()->at(new_range_idx)))
        {
            new_range_owner =
                splitting_range_owners->at(new_range_idx++)->BucketOwner();
        }
        if (new_range_owner != UINT32_MAX && new_range_owner != range_owner)
        {
            write_entry.forward_key_shard_code_ =
                (new_range_owner << 10) | (hash & 0x3FF);
        }
        ++write_key_it_;
    }

    if (write_key_it_ == write_key_end_)
    {
        // Has acquired range locks for all write keys in the current table.
        // Moves to the next table, if there are any.
        ++table_it_;
        if (table_it_ != table_end_)
        {
            const TableName &next_tbl_name = table_it_->first;
            range_table_name_ = TableName(next_tbl_name.StringView(),
                                          TableType::RangePartition);
            write_key_it_ = table_it_->second.begin();
            write_key_end_ = table_it_->second.end();
        }
    }
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
    : hd_result_(txm)
{
}

void ValidateOperation::Reset(size_t read_cnt)
{
    hd_result_.Reset();
    hd_result_.SetRefCnt(read_cnt);
    hd_result_.Value().Clear();
    op_start_ = metrics::TimePoint::max();
}

bool ValidateOperation::IsError()
{
    // If validating read keys returns one or more conflicting tx's who are
    // holding write locks on the read keys during validation, validation is
    // considered failed and the tx is aborted. In theory, it's possible to
    // negotiate conflicting tx's such that if conflicting tx's agree to
    // commit at timestamps later than this (read) tx's commit timestamp,
    // validation still succeeds and this tx is allowed to commit. For
    // simplicity, we skip the negotiation step for now. Note that for 2PL,
    // the validation phase releases read locks acquired earlier. Since read
    // locks block writes, validation always succeeds.

    return hd_result_.IsFinished() &&
           (hd_result_.IsError() || hd_result_.Value().Size() > 0);
}

void ValidateOperation::Forward(TransactionExecution *txm)
{
    // start the state machine if running.
    if (!is_running_)
    {
        txm->Process(*this);
    }

    if (hd_result_.IsFinished())
    {
        // All validation requests have returned, either successfully or
        // with error codes. Post-processing skips read-set keys.
        txm->rw_set_.ClearReadSet();
        txm->rw_set_.ClearScanSet();

        // validation cannot re-run since the remote locks of the readset
        // are lost during auto-failover, we should abort the transaction if
        // remote node, which contains read entries, is dead.
        txm->PostProcess(*this);
    }
    else if (txm->IsTimeOut())
    {
        bool success = hd_result_.ForceError();
        if (success)
        {
            txm->PostProcess(*this);
        }
        // Else, all post-read requests finish normally, meaning the tx has
        // been moved from the waiting queue to the execution queue. Does
        // not forword the tx now, as it will be re-executed when the tx
        // processor visits the execution queue.
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
        // For DML transactions, the coordinator must keep retrying the
        // WriteLog request until getting a clear response, either success
        // or failure, or the coordinator itself is no longer leader. In the
        // last case, the committing process interrupts with an unknown
        // result, and an error message "Log service is unreachable,
        // transaction status is unknown" is returned. For these result
        // unknown txns, The coordinator must skip the PostProcess and the
        // locks on participants remain. The participants ccnodes will do
        // the PostProcess individually via orphan lock recovery mechanism.
        if (hd_result_.ErrorCode() ==
                CcErrorCode::LOG_CLOSURE_RESULT_UNKNOWN_ERR &&
            log_type_ == TxLogType::DATA &&
            Sharder::Instance().LeaderTerm(txm->TxCcNodeId()) > 0)
        {
            CODE_FAULT_INJECTOR("write_log_result_unknown", {
                LOG(INFO) << "skipping to updatetxn";
                txm->PostProcess(*this);
                return;
            });

            LOG(WARNING)
                << "Write Log Request result unknown, retrying, tx_number: "
                << txm->TxNumber();
            // log request return unknown status, we need to set retry flag
            // to inform log service that this is a retried request
            ::txlog::LogRequest &log_req = log_closure_.LogRequest();
            ::txlog::WriteLogRequest *log_rec =
                log_req.mutable_write_log_request();
            log_rec->set_retry(true);
            // ReRunOp sleep for 2 seconds
            retry_num_ = 4;

            ReRunOp(txm);
            return;
        }

        txm->PostProcess(*this);
    }
}

void WriteToLogOp::Reset()
{
    log_group_id_ = 0;
    hd_result_.Reset();
    log_closure_.Reset();
    op_start_ = metrics::TimePoint::max();
}

void WriteToLogOp::ResetHandlerTxm(TransactionExecution *txm)
{
    hd_result_.ResetTxm(txm);
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
    : hd_result_(txm), catalog_range_hd_result_(txm)
{
}

void PostProcessOp::Reset(size_t write_cnt,
                          size_t data_read_cnt,
                          size_t catalog_range_read_cnt)
{
    hd_result_.Reset();
    hd_result_.Value().Clear();
    if (write_cnt + data_read_cnt == 0)
    {
        hd_result_.SetFinished();
    }
    else
    {
        hd_result_.SetRefCnt(write_cnt + data_read_cnt);
    }

    catalog_range_hd_result_.Reset();
    catalog_range_hd_result_.Value().Clear();

    if (catalog_range_read_cnt == 0)
    {
        catalog_range_hd_result_.SetFinished();
    }
    else
    {
        catalog_range_hd_result_.SetRefCnt(catalog_range_read_cnt);
    }
    op_start_ = metrics::TimePoint::max();
}

void PostProcessOp::Forward(TransactionExecution *txm)
{
    if (hd_result_.IsFinished())
    {
        if (catalog_range_hd_result_.IsFinished())
        {
            txm->PostProcess(*this);
        }
        else if (!is_running_)
        {
            is_running_ = true;
            txm->ReleaseCatalogRangeLock(catalog_range_hd_result_);
        }
    }
    else if (hd_result_.LocalRefCnt() == 0 && txm->IsTimeOut())
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

        bool force_error = hd_result_.ForceError();
        if (force_error)
        {
            is_running_ = true;
            txm->ReleaseCatalogRangeLock(catalog_range_hd_result_);
        }
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

    if (hd_result_.IsFinished())
    {
        // Error code -1 indicates send message failed or term changed.
        if (hd_result_.ErrorCode() == CcErrorCode::REQUESTED_NODE_NOT_LEADER &&
            retry_num_ > 0)
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
    else if (txm->IsTimeOut())
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
            bool force_success = hd_result_.ForceError();
            if (force_success)
            {
                txm->PostProcess(*this);
            }
        }
    }
}

ScanNextOperation::ScanNextOperation(TransactionExecution *txm)
    : hd_result_(txm)
#ifdef RANGE_PARTITION_ENABLED
      ,
      slice_hd_result_(txm),
      lock_range_result_(txm),
      unlock_range_result_(txm)
#endif
{
    TX_TRACE_ASSOCIATE(this, &hd_result_);
}

void ScanNextOperation::Reset()
{
    alias_ = 0;
    scan_state_ = nullptr;
    op_start_ = metrics::TimePoint::max();
    ResetResult();
}

void ScanNextOperation::ResetResult()
{
#ifdef RANGE_PARTITION_ENABLED
    slice_hd_result_.Reset();
    unlock_range_result_.Reset();
    lock_range_result_.Reset();
#else
    hd_result_.Reset();
#endif
}

void ScanNextOperation::Forward(TransactionExecution *txm)
{
    CcScanner &scanner = *scan_state_->scanner_;

    // start the state machine if not running.
    if (!is_running_)
    {
        if (scanner.Type() == CcmScannerType::HashPartition)
        {
            txm->Process(*this);
        }
#ifdef RANGE_PARTITION_ENABLED
        else
        {
            if (!lock_range_result_.IsFinished())
            {
                // The locking-next-range request has not finished. The scan
                // next operation cannot proceed without locking the range.
                return;
            }

            assert(lock_range_result_.IsFinished());

            if (lock_range_result_.IsError())
            {
                // There is an error when getting the next range's lock and
                // ID. The scan next operation is set to be errored.
                slice_hd_result_.SetError(CcErrorCode::GET_RANGE_ID_ERR);
                unlock_range_result_.SetFinished();
            }
            else
            {
                const ReadKeyResult &read_res = lock_range_result_.Value();
                // For scans, range locks are released when the last/first slice
                // of the range is scanned. Hence, the range lock is always put
                // into the read set. If the tx terminates the scan early before
                // the last/first slice is encountered, the range lock is
                // released in post-processing.
                // TODO: release the range lock in the scan close phase.
                txm->rw_set_.AddRead(
                    read_res.cce_addr_, read_res.ts_, &range_table_name_);

                scan_state_->range_cce_addr_ = read_res.cce_addr_;
                scan_state_->range_id_ =
                    range_rec_.GetRangeInfo()->PartitionId();
                scan_state_->range_owner_ =
                    range_rec_.GetRangeOwnerNg()->BucketOwner();
                txm->Process(*this);
                return;
            }
        }
#endif
    }

    if (scanner.Type() == CcmScannerType::HashPartition &&
        hd_result_.IsFinished())
    {
        // Error code REQUESTED_NODE_NOT_LEADER indicates send message failed or
        // term changed.
        if (hd_result_.ErrorCode() == CcErrorCode::REQUESTED_NODE_NOT_LEADER)
        {
            if (retry_num_ == 0)
            {
                Sharder::Instance().UpdateLeader(
                    hd_result_.Value().node_group_id_);
            }
            else if (retry_num_ > 0)
            {
                hd_result_.Reset();
                ReRunOp(txm);
                return;
            }
        }

        scanner.SetStatus(ScannerStatus::Open);

        txm->PostProcess(*this);
    }
#ifdef RANGE_PARTITION_ENABLED
    else if (scanner.Type() == CcmScannerType::RangePartition &&
             slice_hd_result_.IsFinished())
    {
        if (slice_hd_result_.ErrorCode() ==
                CcErrorCode::REQUESTED_NODE_NOT_LEADER ||
            slice_hd_result_.ErrorCode() == CcErrorCode::PIN_RANGE_SLICE_FAILED)
        {
            if (retry_num_ == 0)
            {
                // Sharder::Instance().UpdateLeader(
                //     hd_result_.Value().node_group_id_);
            }
            else if (retry_num_ > 0)
            {
                slice_hd_result_.Reset();
                ReRunOp(txm);
                return;
            }
        }

        if (!slice_hd_result_.IsError() &&
            scanner.Status() == ScannerStatus::Blocked)
        {
            RangeScanSliceResult &scan_slice_result = slice_hd_result_.Value();

            // If the scanned slice is the last (or first) of the range and
            // the tx's isolation level is less than Repeatable Read,
            // unlocks the range now.
            if (scan_slice_result.slice_position_ != SlicePosition::Middle &&
                txm->iso_level_ < IsolationLevel::RepeatableRead &&
                scan_state_->range_cce_addr_.CcePtr() != 0)
            {
                if (lock_range_result_.IsFinished())
                {
                    txm->rw_set_.DedupRead(range_table_name_,
                                           scan_state_->range_cce_addr_);

                    txm->cc_handler_->PostRead(txm->TxNumber(),
                                               txm->TxTerm(),
                                               txm->CommandId(),
                                               0,
                                               0,
                                               0,
                                               scan_state_->range_cce_addr_,
                                               unlock_range_result_);

                    // After the unlock range request is sent,
                    // lock_range_result_ is reset. When the tx machine is
                    // re-executed, its status is unfinished, indicating
                    // that the scan next operation is waiting for the
                    // response of unlocking the current range.
                    lock_range_result_.Reset();
                    return;
                }
                else if (!unlock_range_result_.IsFinished())
                {
                    return;
                }
            }

            scan_state_->SetSliceLastKey(
                std::move(scan_slice_result.last_key_));
            scan_state_->inclusive_ =
                Direction() == ScanDirection::Forward ? false : true;
            scan_state_->slice_position_ = scan_slice_result.slice_position_;
            scanner.SetStatus(ScannerStatus::Open);
        }

        txm->PostProcess(*this);
    }
    else if (txm->IsTimeOut() && !slice_hd_result_.Value().is_local_)
#else
    else if (txm->IsTimeOut() && !hd_result_.Value().is_local_)
#endif
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

#ifdef RANGE_PARTITION_ENABLED
        bool force_success = slice_hd_result_.ForceError();
#else
        bool force_success = hd_result_.ForceError();
#endif
        if (force_success)
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
                fail_cnt_.fetch_add(1, std::memory_order_relaxed);

                const AcquireAllResult &acq_result = hres->Value();
                if (acq_result.node_term_ < 0 &&
                    acq_result.remote_ack_cnt_ != nullptr)
                {
                    remote_ack_cnt_.fetch_sub(1, std::memory_order_relaxed);
                }
            }

            finish_cnt_.fetch_add(1, std::memory_order_relaxed);
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
                    fail_cnt_.fetch_add(1, std::memory_order_relaxed);

                    const AcquireAllResult &acq_result = hres->Value();
                    if (acq_result.node_term_ < 0 &&
                        acq_result.remote_ack_cnt_ != nullptr)
                    {
                        remote_ack_cnt_.fetch_sub(1, std::memory_order_relaxed);
                    }
                }
                finish_cnt_.fetch_add(1, std::memory_order_relaxed);
            };
        }
    }
}

void AcquireAllOp::Reset(size_t node_cnt)
{
    finish_cnt_.store(0, std::memory_order_relaxed);
    fail_cnt_.store(0, std::memory_order_relaxed);
    remote_ack_cnt_.store(0, std::memory_order_relaxed);
    upload_cnt_ = node_cnt;

    // Reset results since we rely on node term to decide if
    // the received ack is the first ack message. See OnReceiveCcMsg
    // in cc_stream_receiver.cpp
    for (size_t idx = 0; idx < hd_results_.size(); ++idx)
    {
        AcquireAllResult &res = hd_results_.at(idx).Value();
        res.node_term_ = -1;
        res.last_vali_ts_ = 1;
        res.commit_ts_ = 1;
    }
    Resize(node_cnt);
}

void AcquireAllOp::ResetHandlerTxm(TransactionExecution *txm)
{
    for (size_t idx = 0; idx < hd_results_.size(); ++idx)
    {
        hd_results_.at(idx).ResetTxm(txm);
    }
}

void AcquireAllOp::Forward(TransactionExecution *txm)
{
    // start the state machine if not running.
    if (!is_running_)
    {
        txm->Process(*this);
    }

    if (remote_ack_cnt_.load(std::memory_order_relaxed) > 0)
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
            size_t force_error_cnt = 0;
            for (size_t nid = 0; nid < upload_cnt_; ++nid)
            {
                CcHandlerResult<AcquireAllResult> &hd_result = hd_results_[nid];
                const AcquireAllResult &acquire_res = hd_result.Value();

                uint64_t ts = std::max(acquire_res.commit_ts_ + 1,
                                       acquire_res.last_vali_ts_ + 1);
                txm->commit_ts_bound_ = std::max(txm->commit_ts_bound_, ts);

                // Dedup read set for successful acquire
                if (!hd_result.IsError())
                {
                    const CcEntryAddr &cce_addr = acquire_res.local_cce_addr_;
                    uint64_t read_version = txm->rw_set_.DedupRead(cce_addr);
                    if (read_version > 0 &&
                        read_version != acquire_res.commit_ts_)
                    {
                        // Each write-set key acquires a write lock and gets
                        // the key's last validation ts and commit ts. If
                        // the write key has been read before and the key's
                        // commit ts mismatches the prior version, this is
                        // not a repeatable read.
                        fail_cnt_.fetch_add(1, std::memory_order_relaxed);
                    }
                }

                if (acquire_res.node_term_ < 0)
                {
                    bool success = hd_result.ForceError();
                    if (success)
                    {
                        ++force_error_cnt;
                    }
                    else if (hd_result.ErrorCode() ==
                             CcErrorCode::REQUESTED_NODE_NOT_LEADER)
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

            if (force_error_cnt > 0)
            {
                txm->PostProcess(*this);
            }
            // Else, all remote requests finish normally, meaning the tx has
            // been moved from the waiting queue to the execution queue.
            // Does not forword the tx now, as it will be re-executed when
            // the tx processor visits the execution queue.
        }
    }
    else if (finish_cnt_.load(std::memory_order_relaxed) == upload_cnt_)
    {
        // TODO: for locking-based protocols, though the tx may be blocked
        // arbitrarily long, after all acquire requests are acknowledged, we
        // still need to periodically check liveness of the remote node.

        if (fail_cnt_.load(std::memory_order_relaxed) == 0)
        {
            for (size_t idx = 0; idx < upload_cnt_; ++idx)
            {
                const AcquireAllResult &acquire_res = hd_results_[idx].Value();
                uint64_t ts = std::max(acquire_res.commit_ts_ + 1,
                                       acquire_res.last_vali_ts_ + 1);
                txm->commit_ts_bound_ = std::max(txm->commit_ts_bound_, ts);

                assert(acquire_res.node_term_ >= 0);

                // Dedup read set
                const CcEntryAddr &cce_addr = acquire_res.local_cce_addr_;
                uint64_t read_version = txm->rw_set_.DedupRead(cce_addr);
                if (read_version > 0 && read_version != acquire_res.commit_ts_)
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
        else
        {
            for (size_t nid = 0; nid < upload_cnt_; ++nid)
            {
                CcHandlerResult<AcquireAllResult> &hd_result = hd_results_[nid];
                if (hd_result.IsError())
                {
                    if (hd_result.ErrorCode() ==
                        CcErrorCode::REQUESTED_NODE_NOT_LEADER)
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
                    fail_cnt_.fetch_add(1, std::memory_order_relaxed);
                }
                else
                {
                    // Dedup read set for successful acquire
                    const AcquireAllResult &acquire_all_result =
                        hd_result.Value();
                    const CcEntryAddr &cce_addr =
                        acquire_all_result.local_cce_addr_;
                    DLOG(INFO)
                        << "rwset.DedupRead tx_numer: " << txm->TxNumber()
                        << " ,cce_addr: " << std::hex << cce_addr.CcePtr()
                        << " ,ErrorCode: " << (int32_t) hd_result.ErrorCode();
                    uint64_t read_version = txm->rw_set_.DedupRead(cce_addr);
                    if (read_version > 0 &&
                        read_version != acquire_all_result.commit_ts_)
                    {
                        // Each write-set key acquires a write lock and gets
                        // the key's last validation ts and commit ts. If
                        // the write key has been read before and the key's
                        // commit ts mismatches the prior version, this is
                        // not a repeatable read.
                        fail_cnt_.fetch_add(1, std::memory_order_relaxed);
                    }
                }
            }
        }

        txm->PostProcess(*this);
    }
}

uint64_t AcquireAllOp::MaxTs()
{
    uint64_t max_ts = 0;
    for (size_t idx = 0; idx < upload_cnt_; ++idx)
    {
        const AcquireAllResult &acq_all_res = hd_results_[idx].Value();
        uint64_t ts =
            std::max(acq_all_res.commit_ts_, acq_all_res.last_vali_ts_);
        max_ts = std::max(max_ts, ts);
    }
    return max_ts;
}

PostWriteAllOp::PostWriteAllOp(TransactionExecution *txm) : hd_result_(txm)
{
}

void PostWriteAllOp::Reset(uint32_t ng_cnt)
{
    hd_result_.Reset();
    hd_result_.SetRefCnt(ng_cnt);
}

void PostWriteAllOp::ResetHandlerTxm(TransactionExecution *txm)
{
    hd_result_.ResetTxm(txm);
}

void PostWriteAllOp::Forward(TransactionExecution *txm)
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
    else if (txm->IsTimeOut(4))
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

        bool force_error = hd_result_.ForceError();
        if (force_error)
        {
            txm->PostProcess(*this);
        }
    }
}

bool PostWriteAllOp::IsFailed()
{
    assert(hd_result_.IsFinished());
    return hd_result_.IsError();
}

DsUpsertTableOp::DsUpsertTableOp(const TableName *table_name,
                                 OperationType op_type,
                                 TransactionExecution *txm)
    : table_name_(table_name), op_type_(op_type), hd_result_(txm)
{
    TX_TRACE_ASSOCIATE(this, &hd_result_);
}

void DsUpsertTableOp::Reset()
{
    hd_result_.Reset();
}

void DsUpsertTableOp::ResetHandlerTxm(TransactionExecution *txm)
{
    hd_result_.ResetTxm(txm);
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
        if (hd_result_.IsError())
        {
            assert(hd_result_.ErrorCode() == CcErrorCode::DATA_STORE_ERR);
            if (retry_num_ == 0)
            {
                DLOG(ERROR) << "flush schema error: can not create table "
                               "in kv store";

                if (txm->tx_status_ != TxnStatus::Recovering)
                {
                    txm->bool_resp_->SetErrorCode(
                        TxErrorCode::DATA_STORE_ERROR);
                }

                // Set txm->commit_ts_ to 0 to indicate there is a flush
                // error during upsert_kv_table_op_.
                txm->commit_ts_ = tx_op_failed_ts_;
                txm->PostProcess(*this);
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

SchemaOp::SchemaOp(const std::string_view table_name_sv,
                   const std::string &current_image,
                   const std::string &dirty_image,
                   uint64_t schema_ts,
                   const std::string *alter_table_info_image)
    : table_key_(TableName(
          table_name_sv.data(), table_name_sv.size(), TableType::Primary))
{
    catalog_rec_.SetSchemaImage(current_image);
    catalog_rec_.SetDirtySchemaImage(dirty_image);
    image_str_ = current_image;
    dirty_image_str_ = dirty_image;
    curr_schema_ts_ = schema_ts;
    alter_table_info_image_str_ = alter_table_info_image != nullptr
                                      ? *alter_table_info_image
                                      : std::string("");
}

UpsertTableOp::UpsertTableOp(const std::string_view table_name_str,
                             const std::string &current_image,
                             uint64_t curr_schema_ts,
                             const std::string &dirty_image,
                             OperationType op_type,
                             TransactionExecution *txm,
                             const std::string *alter_table_info_image)
    : SchemaOp(table_name_str,
               current_image,
               dirty_image,
               curr_schema_ts,
               alter_table_info_image),
      op_type_(op_type),
      acquire_all_intent_op_(txm),
      prepare_log_op_(txm),
      post_all_intent_op_(txm),
      upsert_kv_table_op_(&table_key_.Name(), op_type, txm),
      acquire_all_lock_op_(txm),
      commit_log_op_(txm),
      post_all_lock_op_(txm),
      clean_log_op_(txm)
{
    acquire_all_intent_op_.table_name_ = &catalog_ccm_name;
    acquire_all_intent_op_.key_ = &table_key_;
    acquire_all_intent_op_.cc_op_ = CcOperation::ReadForWrite;
    acquire_all_intent_op_.protocol_ = CcProtocol::OCC;

    post_all_intent_op_.table_name_ = &catalog_ccm_name;
    post_all_intent_op_.key_ = &table_key_;
    post_all_intent_op_.rec_ = &catalog_rec_;
    post_all_intent_op_.op_type_ = op_type_;
    post_all_intent_op_.write_type_ = PostWriteType::PrepareCommit;

    acquire_all_lock_op_.table_name_ = &catalog_ccm_name;
    acquire_all_lock_op_.key_ = &table_key_;
    acquire_all_lock_op_.cc_op_ = CcOperation::Write;
    acquire_all_lock_op_.protocol_ = CcProtocol::Locking;

    post_all_lock_op_.table_name_ = &catalog_ccm_name;
    post_all_lock_op_.key_ = &table_key_;
    post_all_lock_op_.rec_ = &catalog_rec_;
    post_all_lock_op_.op_type_ = op_type_;
    post_all_lock_op_.write_type_ = PostWriteType::PostCommit;

    alter_table_info_.DeserializeAlteredTableInfo(alter_table_info_image_str_);

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
        if (acquire_all_intent_op_.fail_cnt_.load(std::memory_order_relaxed) >
            0)
        {
            DLOG(ERROR)
                << "Upsert table acquire write intent failed, tx_number:"
                << txm->TxNumber();
            txm->bool_resp_->SetErrorCode(
                TxErrorCode::UPSERT_TABLE_ACQUIRE_WRITE_INTENT_FAIL);
            // Fails to acquire the write intent on the schema. Since write
            // intents only conflict with other writes, there must be
            // another tx trying to modify the same table's schema. Stops
            // the schema operation. Set the commit ts to 0 to signal that
            // the following post write operation releases all write
            // intents.
            txm->commit_ts_ = tx_op_failed_ts_;
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
            if (prepare_log_op_.hd_result_.ErrorCode() ==
                CcErrorCode::LOG_CLOSURE_RESULT_UNKNOWN_ERR)
            {
                // prepare log result unknown, keep retrying until getting a
                // clear response, either success or failure, or the
                // coordinator itself is no longer leader
                int64_t tx_node_term =
                    Sharder::Instance().LeaderTerm(txm->TxCcNodeId());
                if (tx_node_term > 0)
                {
                    DLOG(WARNING)
                        << "Upsert table write prepare log result unknown, "
                           "tx_number:"
                        << txm->TxNumber() << ", keep retrying";
                    // set retry flag and retry prepare log
                    ::txlog::WriteLogRequest *log_req =
                        prepare_log_op_.log_closure_.LogRequest()
                            .mutable_write_log_request();
                    log_req->set_retry(true);
                    txm->PushOperation(&prepare_log_op_);
                    txm->Process(prepare_log_op_);
                }
                else
                {
                    DLOG(ERROR) << "Upsert table write prepare log result "
                                   "unknown, tx_number:"
                                << txm->TxNumber()
                                << ", not leader any more, stop retrying";
                    // Not leader anymore, just quit. New leader will know
                    // whether prepare log succeeds and continue the rest if
                    // it does. Should not release the write intents. If
                    // prepare log is not written, the write intents will be
                    // released individually via orphan lock recovery
                    // mechanism.
                    txm->bool_resp_->SetErrorCode(
                        TxErrorCode::LOG_SERVICE_UNREACHABLE);

                    txm->bool_resp_->Finish(false);
                    txm->state_stack_.pop_back();
                    assert(txm->state_stack_.empty());
                    LocalCcShards *local_shards =
                        Sharder::Instance().GetLocalCcShards();
                    std::unique_lock<std::mutex> lk(
                        local_shards->table_schema_op_pool_mux_);
                    local_shards->table_schema_op_pool_.emplace_back(
                        std::move(txm->schema_op_));
                }
            }
            else
            {
                DLOG(ERROR)
                    << "Upsert table write prepare log failed, tx_number:"
                    << txm->TxNumber();
                // Fails to flush the prepare log. The schema operation is
                // considered failed if the prepare log is not flushed. The
                // commit ts is set to 0 to signal that the following post
                // write operation releases all write intents.
                txm->commit_ts_ = tx_op_failed_ts_;
                // Moves to the last operation that removes all write
                // intents/locks.
                op_ = &post_all_lock_op_;

                txm->bool_resp_->SetErrorCode(
                    TxErrorCode::UPSERT_TABLE_PREPARE_FAIL);

                txm->PushOperation(&post_all_lock_op_);
                txm->Process(post_all_lock_op_);
            }
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
        if (post_all_intent_op_.IsFailed())
        {
            // When a cc node leader begins recovery, the candidate term is
            // set to the Raft term. When recovery finishes, the candidate
            // term is set to -1 after the leader term. So, obtains the
            // candidate term before the leader term.
            int64_t tx_node_candid_term =
                Sharder::Instance().CandidateLeaderTerm(txm->TxCcNodeId());
            int64_t tx_node_term =
                Sharder::Instance().LeaderTerm(txm->TxCcNodeId());

            // After the prepare log is flushed, the schema op is guaranteed
            // to proceed. Retry this step to install the dirty schema in the tx
            // service, if the tx node is still the leader. The tx is also
            // allowed to proceed if the tx is in the recovery mode and the tx
            // node is a leader candidate.

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
        else if (op_type_ == OperationType::DropTable ||
                 op_type_ == OperationType::DropIndex)
        {
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
            upsert_kv_table_op_.alter_table_info_ = &alter_table_info_;
            txm->PushOperation(&upsert_kv_table_op_);
            txm->Process(upsert_kv_table_op_);
        }
    }
    else if (op_ == &upsert_kv_table_op_)
    {
        if (upsert_kv_table_op_.hd_result_.IsError())
        {
            // The candidate term is set when the cc node becomes the Raft
            // leader of the cc node group. It is set to -1 after the cc
            // node leader has replayed the log and the leader term is set.
            // Since the candidate term is set to -1 after the leader term ,
            // obtains the candidate term before the leader term.
            int64_t tx_node_candid_term =
                Sharder::Instance().CandidateLeaderTerm(txm->TxCcNodeId());
            int64_t tx_node_term =
                Sharder::Instance().LeaderTerm(txm->TxCcNodeId());

            // The data store operation failed. Retries the operation if the
            // tx node is the leader or the tx is in the recovery mode and
            // the cc node is a leader candidate.

            if (tx_node_term >= 0 ||
                (txm->tx_status_ == TxnStatus::Recovering &&
                 tx_node_candid_term >= 0))
            {
                // Keep retrying if it is DropTable or DropIndex.
                if (op_type_ == OperationType::DropTable ||
                    op_type_ == OperationType::DropIndex)
                {
                    txm->PushOperation(&upsert_kv_table_op_);
                    txm->Process(upsert_kv_table_op_);
                }
                else
                {
                    /*

                    After upsert kv fails, we need to flush a commit log to
                    indicate this error.

                    If we skip this commit log and jump to post_all_lock_op_
                    directly, once the participant crashes at the point between
                    it releases write intent and the coordinator flushes
                    clean_log, then during recovery, the participant sees a
                    prepare_log(whose commit_ts is not 0) and recovers write
                    lock and dirty_catalog.

                    Since the coordinator has finished its job, the write
                    lock recovered by participant becomes orphan lock, and the
                    dirty catalog can not be rejected either.

                    Also, in the current design, post_all_intent_op_ does not
                    release the write intent, which means the write intent is
                    still being held after upsert_kv_table_op_(during
                    CreateTable or AddIndex). If create table or add index in kv
                    fails, the only thing we should do after writing commit_log
                    is to reject dirty schema. So there is no need to upgrade
                    write intent to write lock, and it is safe to skip
                    acquire_all_lock_op_ and jump directly to commit_log_op_.

                    */

                    op_ = &commit_log_op_;
                    FillCommitLogRequest(txm);
                    txm->PushOperation(&commit_log_op_);
                    txm->Process(commit_log_op_);
                }
            }
            else
            {
                ForceToFinish(txm);
            }
        }
        else if (op_type_ == OperationType::DropTable ||
                 op_type_ == OperationType::DropIndex)
        {
            // Clear write set before commit dirty schema.
            txm->rw_set_.ClearTable(table_key_.Name());
            txm->rw_set_.ClearReadSet(table_key_.Name());

            // For DROP TABLE, the data store operation happens after all
            // write locks are acquired and commit log is flushed.
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
        if (acquire_all_lock_op_.fail_cnt_.load(std::memory_order_relaxed) > 0)
        {
            // When a cc node leader begins recovery, the candidate term is
            // set to the Raft term. When recovery finishes, the candidate
            // term is set to -1 after the leader term. So, obtains the
            // candidate term before the leader term.
            int64_t tx_node_candid_term =
                Sharder::Instance().CandidateLeaderTerm(txm->TxCcNodeId());
            int64_t tx_node_term =
                Sharder::Instance().LeaderTerm(txm->TxCcNodeId());

            // Fails to acquire the write lock. The schema operation can
            // only roll forward after flushing the prepare log. Retries the
            // request if the tx node is still the leader or the tx is in
            // the recovery mode and the cc node is a leader candidate.
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
            // When a cc node leader begins recovery, the candidate term is
            // set to the Raft term. When recovery finishes, the candidate
            // term is set to -1 after the leader term. So, obtains the
            // candidate term before the leader term.
            int64_t tx_node_candid_term =
                Sharder::Instance().CandidateLeaderTerm(txm->TxCcNodeId());
            int64_t tx_node_term =
                Sharder::Instance().LeaderTerm(txm->TxCcNodeId());

            // Fails to flush the commit log. Retries the operation if the
            // tx node is still the leader or the tx is in the  recovery
            // mode and the cc node is a leader candidate.
            if (tx_node_term >= 0 ||
                (txm->tx_status_ == TxnStatus::Recovering &&
                 tx_node_candid_term >= 0))
            {
                // set retry flag and retry commit log
                ::txlog::WriteLogRequest *log_req =
                    commit_log_op_.log_closure_.LogRequest()
                        .mutable_write_log_request();
                log_req->set_retry(true);
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
            if (op_type_ == OperationType::DropTable ||
                op_type_ == OperationType::DropIndex)
            {
                op_ = &upsert_kv_table_op_;
                // Read table schema from local cc shard. This is because we
                // could be recovering from commit stage, in which case we
                // have skipped post_all_intent_op_ and the schema in
                // catalog_rec_ would be empty.
                LocalCcShards *shards = Sharder::Instance().GetLocalCcShards();
                auto catalog_entry =
                    shards->GetCatalog(table_key_.Name(), txm->TxCcNodeId());
                upsert_kv_table_op_.table_schema_ =
                    (op_type_ == OperationType::DropTable)
                        ? catalog_entry->schema_.get()
                        : catalog_entry->dirty_schema_.get();
                upsert_kv_table_op_.alter_table_info_ = &alter_table_info_;
                txm->PushOperation(&upsert_kv_table_op_);
                txm->Process(upsert_kv_table_op_);
            }
            else
            {
                op_ = &post_all_lock_op_;
                txm->PushOperation(&post_all_lock_op_);
                txm->Process(post_all_lock_op_);
            }
        }
    }
    else if (op_ == &post_all_lock_op_)
    {
        // When a cc node leader begins recovery, the candidate term is
        // set to the Raft term. When recovery finishes, the candidate
        // term is set to -1 after the leader term. So, obtains the
        // candidate term before the leader term.
        int64_t tx_node_candid_term =
            Sharder::Instance().CandidateLeaderTerm(txm->TxCcNodeId());
        int64_t tx_node_term =
            Sharder::Instance().LeaderTerm(txm->TxCcNodeId());
        bool is_leader =
            tx_node_term >= 0 || (txm->tx_status_ == TxnStatus::Recovering &&
                                  tx_node_candid_term >= 0);

        if (!is_leader)
        {
            // The tx node is no longer the leader or leader candidate(during
            // recovery), ForceToFinish.
            ForceToFinish(txm);
        }
        else if (acquire_all_intent_op_.fail_cnt_.load(
                     std::memory_order_relaxed) > 0)
        {
            // The schema operation failed at acquire_all_intent_op_, without
            // flushing the prepare log. Do not retry post-processing (release
            // write intents) even if it fails. Remaining write intents on the
            // schema, if there are any, will be recovered by individual cc
            // nodes separately.

            txm->bool_resp_->Finish(false);

            txm->state_stack_.pop_back();
            assert(txm->state_stack_.empty());

            LocalCcShards *shards = Sharder::Instance().GetLocalCcShards();
            std::unique_lock<std::mutex> lk(shards->table_schema_op_pool_mux_);
            shards->table_schema_op_pool_.emplace_back(
                std::move(txm->schema_op_));
        }
        else if (post_all_lock_op_.hd_result_.IsError())
        {
            // post_all_lock_op_ returns an error:
            // 1. if flush kv succeeds, the schema op is guaranteed to succeed
            // and can only roll forward. Retry this step to install the
            // committed schema and remove write locks;
            // 2. if flush kx fails, the schema op has to roll backward. Retry
            // this step to reject dirty schema and remove write locks.
            txm->PushOperation(&post_all_lock_op_);
            txm->Process(post_all_lock_op_);
            return;
        }
        else
        {
            // post_all_lock_op_ has finished without an error.
            assert(!post_all_lock_op_.IsFailed());

            if (txm->commit_ts_ != tx_op_failed_ts_)
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
            }
            else
            {
                // Flush kv failed, or it is recovering from a flush kv failure.
                // This schema op has already been rolled back by now, only need
                // to flush clean log here.
                // Also, flush kv failure does not require lock upgrade(write
                // intent to write lock). So the CcEntryAddr needs to be kept in
                // rset in order to release the read lock when committing.
            }

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

        LocalCcShards *shards = Sharder::Instance().GetLocalCcShards();
        if (clean_log_op_.hd_result_.IsError() &&
            (tx_node_term >= 0 || (txm->tx_status_ == TxnStatus::Recovering &&
                                   tx_node_candid_term >= 0)))
        {
            // set retry flag and retry clean log
            ::txlog::WriteLogRequest *log_req =
                clean_log_op_.log_closure_.LogRequest()
                    .mutable_write_log_request();
            log_req->set_retry(true);
            txm->PushOperation(&clean_log_op_);
            txm->Process(clean_log_op_);
        }
        else if (txm->tx_status_ == TxnStatus::Recovering)
        {
            // When the tx is in the recovery state, no external caller is
            // waiting for the response. So, txm->bool_resp_ is null.

            {
                std::unique_lock<std::mutex> lk(
                    shards->table_schema_op_pool_mux_);
                shards->table_schema_op_pool_.emplace_back(
                    std::move(txm->schema_op_));
            }
            txm->Reset();
            // Setting the tx's status to finished signals that this tx
            // state machine can be recycled for a new tx.
            txm->tx_status_.store(TxnStatus::Finished,
                                  std::memory_order_release);
        }
        else
        {
            if (txm->commit_ts_ == tx_op_failed_ts_)
            {
                // Flush kv error or fail to flush prepare_log.
                txm->bool_resp_->Finish(false);
            }
            else
            {
                assert(txm->commit_ts_ > 0);
                txm->bool_resp_->Finish(true);
            }

            txm->state_stack_.pop_back();
            assert(txm->state_stack_.empty());

            std::unique_lock<std::mutex> lk(shards->table_schema_op_pool_mux_);
            shards->table_schema_op_pool_.emplace_back(
                std::move(txm->schema_op_));
        }
    }
}

void UpsertTableOp::Reset(const std::string_view table_name_str,
                          const std::string &current_image,
                          uint64_t curr_schema_ts,
                          const std::string &dirty_image,
                          OperationType op_type,
                          TransactionExecution *txm,
                          const std::string *alter_table_info_image)
{
    // reset TransactionOperation
    retry_num_ = RETRY_NUM;
    is_running_ = false;

    // reset SchemaOp
    table_key_.Name() = TableName(
        table_name_str.data(), table_name_str.size(), TableType::Primary);
    catalog_rec_.SetSchemaImage(current_image);
    catalog_rec_.SetDirtySchemaImage(dirty_image);
    image_str_ = current_image;
    dirty_image_str_ = dirty_image;
    curr_schema_ts_ = curr_schema_ts;
    alter_table_info_image_str_ = alter_table_info_image != nullptr
                                      ? *alter_table_info_image
                                      : std::string("");

    // reset UpsertTableOp
    op_type_ = op_type;
    op_ = nullptr;

    // reset op
    uint32_t node_group_cnt = Sharder::Instance().NodeGroupCount();
    acquire_all_intent_op_.Reset(node_group_cnt);
    prepare_log_op_.Reset();
    post_all_intent_op_.Reset(node_group_cnt);
    upsert_kv_table_op_.Reset();
    acquire_all_lock_op_.Reset(node_group_cnt);
    commit_log_op_.Reset();
    post_all_lock_op_.Reset(node_group_cnt);
    clean_log_op_.Reset();

    acquire_all_intent_op_.table_name_ = &catalog_ccm_name;
    acquire_all_intent_op_.key_ = &table_key_;
    acquire_all_intent_op_.cc_op_ = CcOperation::ReadForWrite;
    acquire_all_intent_op_.protocol_ = CcProtocol::OCC;

    post_all_intent_op_.table_name_ = &catalog_ccm_name;
    post_all_intent_op_.key_ = &table_key_;
    post_all_intent_op_.rec_ = &catalog_rec_;
    post_all_intent_op_.op_type_ = op_type_;
    post_all_intent_op_.write_type_ = PostWriteType::PrepareCommit;

    alter_table_info_.DeserializeAlteredTableInfo(alter_table_info_image_str_);
    upsert_kv_table_op_.alter_table_info_ = &alter_table_info_;
    upsert_kv_table_op_.op_type_ = op_type_;

    acquire_all_lock_op_.table_name_ = &catalog_ccm_name;
    acquire_all_lock_op_.key_ = &table_key_;
    acquire_all_lock_op_.cc_op_ = CcOperation::Write;
    acquire_all_lock_op_.protocol_ = CcProtocol::Locking;

    post_all_lock_op_.table_name_ = &catalog_ccm_name;
    post_all_lock_op_.key_ = &table_key_;
    post_all_lock_op_.rec_ = &catalog_rec_;
    post_all_lock_op_.op_type_ = op_type_;
    post_all_lock_op_.write_type_ = PostWriteType::PostCommit;

    // reset cc_handler_res txm
    acquire_all_intent_op_.ResetHandlerTxm(txm);
    prepare_log_op_.ResetHandlerTxm(txm);
    post_all_intent_op_.ResetHandlerTxm(txm);
    upsert_kv_table_op_.ResetHandlerTxm(txm);
    acquire_all_lock_op_.ResetHandlerTxm(txm);
    commit_log_op_.ResetHandlerTxm(txm);
    post_all_lock_op_.ResetHandlerTxm(txm);
    clean_log_op_.ResetHandlerTxm(txm);
}

void UpsertTableOp::FillPrepareLogRequest(TransactionExecution *txm)
{
    prepare_log_op_.log_type_ = TxLogType::PREPARE;

    prepare_log_op_.log_closure_.LogRequest().Clear();

    ::txlog::WriteLogRequest *prepare_log_rec =
        prepare_log_op_.log_closure_.LogRequest().mutable_write_log_request();

    prepare_log_rec->set_tx_term(txm->tx_term_);
    prepare_log_rec->set_txn_number(txm->TxNumber());
    prepare_log_rec->set_commit_timestamp(txm->commit_ts_);

    ::txlog::SchemaOpMessage *prepare_schema_msg =
        prepare_log_rec->mutable_log_content()->mutable_schema_log();
    prepare_schema_msg->set_table_name_str(table_key_.Name().String());
    prepare_schema_msg->set_table_type(
        ::txlog::ToRemoteType::ConvertTableType(table_key_.Name().Type()));
    prepare_schema_msg->set_old_catalog_blob(catalog_rec_.SchemaImage());
    prepare_schema_msg->set_catalog_ts(curr_schema_ts_);
    prepare_schema_msg->set_new_catalog_blob(catalog_rec_.DirtySchemaImage());
    prepare_schema_msg->set_alter_table_info_blob(alter_table_info_image_str_);
    prepare_schema_msg->mutable_table_op()->set_op_type(
        static_cast<::google::protobuf::uint32>(op_type_));
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
    commit_log_rec->set_txn_number(txm->TxNumber());

    ::txlog::SchemaOpMessage *commit_schema_msg =
        commit_log_rec->mutable_log_content()->mutable_schema_log();

    if (upsert_kv_table_op_.hd_result_.IsError())
    {
        // Serve as new catalog_ts. Set to 0 if flush kv fails.
        commit_log_rec->set_commit_timestamp(tx_op_failed_ts_);
    }
    else
    {
        assert(txm->commit_ts_ != tx_op_failed_ts_);
        commit_log_rec->set_commit_timestamp(txm->commit_ts_);
    }
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
    clean_log_rec->set_txn_number(txm->TxNumber());

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

NoOp::NoOp(TransactionExecution *txm) : hd_result_(txm)
{
    TX_TRACE_ASSOCIATE(this, &hd_result_);
}

void NoOp::Forward(TransactionExecution *txm)
{
    if (!is_running_)
    {
        txm->Process(*this);
    }

    if (hd_result_.IsFinished())
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
        hd_result_.ForceError();
        txm->PostProcess(*this);
    }
}

template <typename ResultType>
AsyncOp<ResultType>::AsyncOp(TransactionExecution *txm) : hd_result_(txm)
{
    TX_TRACE_ASSOCIATE(this, &hd_result_);
}

template <typename ResultType>
void AsyncOp<ResultType>::ResetHandlerTxm(TransactionExecution *txm)
{
    hd_result_.ResetTxm(txm);
}

template <typename ResultType>
void AsyncOp<ResultType>::Forward(TransactionExecution *txm)
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
        // Not necessary to have timeout for ds operation, you don't
        // estimate the proper op time out secs, and the dsop will finished
        // anyway

        // bool succ = hd_result_.ForceError();
        // if (succ)
        //{
        // txm->PostProcess(*this);
        //}
    }
}

template <typename ResultType>
void AsyncOp<ResultType>::Reset()
{
    hd_result_.Reset();
}

CompositeTransactionOperation::CompositeTransactionOperation() : op_(nullptr)
{
}

template <typename Op>
void CompositeTransactionOperation::ForwardToSubOperation(
    TransactionExecution *txm, Op *next_op)
{
    op_ = next_op;
    txm->PushOperation(next_op);
    txm->Process(*next_op);
}

template <typename Op>
void CompositeTransactionOperation::RetrySubOperation(TransactionExecution *txm,
                                                      Op *last_sub_op)
{
    ForwardToSubOperation(txm, last_sub_op);
}

bool CompositeTransactionOperation::CheckLeaderTerm(uint32_t ng_id,
                                                    int64_t term,
                                                    TxnStatus txn_status) const
{
    if (Sharder::Instance().CheckLeaderTerm(ng_id, term) ||
        (txn_status == TxnStatus::Recovering &&
         Sharder::Instance().CandidateLeaderTerm(ng_id) >= 0))
    {
        return true;
    }

    return false;
}

FlushDataOp::FlushDataOp(TransactionExecution *txm) : hd_result_(txm)
{
    TX_TRACE_ASSOCIATE(this, &hd_result_);
}

void FlushDataOp::ResetHandlerTxm(TransactionExecution *txm)
{
    hd_result_.ResetTxm(txm);
}

void FlushDataOp::Forward(TransactionExecution *txm)
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

void FlushDataOp::Reset()
{
    hd_result_.Reset();
}

KickoutDataOp::KickoutDataOp(TransactionExecution *txm) : hd_result_(txm)
{
}

void KickoutDataOp::Reset()
{
    table_name_ = nullptr;
    start_key_ = nullptr;
    end_key_ = nullptr;
    commit_ts_ = 0;
    node_group_ = 0;
    hd_result_.Reset();
}

void KickoutDataOp::ResetHandlerTxm(TransactionExecution *txm)
{
    hd_result_.ResetTxm(txm);
}

void KickoutDataOp::Forward(TransactionExecution *txm)
{
    // Start the state machine if not running.
    if (!is_running_)
    {
        txm->Process(*this);
    }

    if (hd_result_.IsFinished())
    {
        // If leader-transferred, do not need to kickout data any more.
        if (hd_result_.IsError() &&
            hd_result_.ErrorCode() != CcErrorCode::REQUESTED_NODE_NOT_LEADER &&
            hd_result_.ErrorCode() != CcErrorCode::TX_NODE_NOT_LEADER)
        {
            if (retry_num_ > 0)
            {
                ReRunOp(txm);
                return;
            }
        }

        txm->PostProcess(*this);
    }
}

/**
 * @brief Construct a new Split Flush Range Op:: Split Flush Range Op object
 */
SplitFlushRangeOp::SplitFlushRangeOp(
    const TableName &table_name,
    const TableSchema *table_schema,
    NodeGroupId node_group,
    const TxKey *old_start_key,
    const TxKey *old_end_key,
    const RangeInfo *old_range_info,
    std::vector<std::pair<TxKey::Uptr, int32_t>> &&new_range_info,
    TransactionExecution *txm)
    : CompositeTransactionOperation(),
      table_schema_(table_schema),
      table_name_(table_name.String(), table_name.Type()),
      range_table_name_(table_name_.StringView(), TableType::RangePartition),
      node_group_(node_group),
      range_info_(*old_range_info),
      old_end_key_(old_end_key),
      new_range_info_(std::move(new_range_info)),
      prepare_acquire_all_write_op_(txm),
      prepare_log_op_(txm),
      install_new_range_op_(txm),
      ds_migrate_old_partition_op_(txm),
      data_sync_scan_op_(txm),
      flush_op_(txm),
      commit_acquire_all_write_op_(txm),
      commit_log_op_(txm),
      ds_upsert_range_op_(txm),
      kickout_old_range_data_op_(txm),
      post_all_lock_op_(txm),
      ds_clean_old_range_op_(txm),
      clean_log_op_(txm),
      release_catalog_read_lock_op_(txm)
{
    range_record_ = std::make_unique<RangeRecord>(
        &range_info_, nullptr, old_end_key, nullptr);
    old_start_key_ = range_info_.StartKey() != nullptr ? range_info_.StartKey()
                                                       : old_start_key;
    prepare_acquire_all_write_op_.table_name_ = &range_table_name_;
    prepare_acquire_all_write_op_.cc_op_ = CcOperation::Write;
    prepare_acquire_all_write_op_.protocol_ = CcProtocol::Locking;
    prepare_acquire_all_write_op_.key_ = old_start_key_;

    install_new_range_op_.table_name_ = &range_table_name_;
    install_new_range_op_.write_type_ = PostWriteType::PrepareCommit;
    install_new_range_op_.op_type_ = OperationType::Update;
    install_new_range_op_.key_ = old_start_key_;

    flush_op_.tab_name_ = &table_name_;
    flush_op_.data_sync_vec_ = &data_sync_vec_;
    flush_op_.archive_vec_ = &archive_vec_;
    flush_op_.mv_vec_ = &mv_base_vec_;
    flush_op_.schema_ = table_schema_;
    flush_op_.node_group_ = node_group_;

    commit_acquire_all_write_op_.table_name_ = &range_table_name_;
    commit_acquire_all_write_op_.cc_op_ = CcOperation::Write;
    commit_acquire_all_write_op_.protocol_ = CcProtocol::Locking;
    commit_acquire_all_write_op_.key_ = old_start_key_;

    kickout_old_range_data_op_.table_name_ = &table_name_;
    kickout_old_range_data_op_.node_group_ = node_group;

    post_all_lock_op_.table_name_ = &range_table_name_;
    post_all_lock_op_.write_type_ = PostWriteType::PostCommit;
    post_all_lock_op_.op_type_ = OperationType::Update;
    post_all_lock_op_.key_ = old_start_key_;

    TX_TRACE_ASSOCIATE(
        this, &prepare_acquire_all_write_op_, "prepare_acquire_all_op_");
    TX_TRACE_ASSOCIATE(this, &prepare_log_op_, "prepare_log_op_");
    TX_TRACE_ASSOCIATE(this, &install_new_range_op_, "install_new_range_op_");
    TX_TRACE_ASSOCIATE(this, &data_sync_scan_op_, "data_sync_scan_op_");
    TX_TRACE_ASSOCIATE(this, &flush_op_, "flush_op_");
    TX_TRACE_ASSOCIATE(
        this, &commit_acquire_all_write_op_, "commit_acquire_all_op_");
    TX_TRACE_ASSOCIATE(this, &commit_log_op_, "commit_log_op_");
    TX_TRACE_ASSOCIATE(this, &ds_upsert_range_op_, "ds_upsert_range_op_");
    TX_TRACE_ASSOCIATE(this, &post_all_lock_op_, "post_all_lock_op_");
    TX_TRACE_ASSOCIATE(this, &ds_clean_old_range_op_, "ds_clean_old_range_op_");
    TX_TRACE_ASSOCIATE(this, &clean_log_op_, "clean_log_op_");
}

void SplitFlushRangeOp::Reset(
    const TableName &table_name,
    const TableSchema *table_schema,
    NodeGroupId node_group,
    const TxKey *old_start_key,
    const TxKey *old_end_key,
    const RangeInfo *old_range_info,
    std::vector<std::pair<TxKey::Uptr, int32_t>> &&new_range_info,
    TransactionExecution *txm)
{
    // Reset TransactionOperation
    retry_num_ = RETRY_NUM;
    is_running_ = false;
    op_start_ = metrics::TimePoint::max();

    // Reset CompositeTransactionOperation
    op_ = nullptr;

    // Reset SplitFlushRangeOp
    table_name_ = TableName(table_name.String(), table_name.Type());
    table_schema_ = table_schema;
    range_table_name_ =
        TableName(table_name_.StringView(), TableType::RangePartition);
    node_group_ = node_group;

    assert(old_range_info->new_partition_id_.size() ==
           old_range_info->new_key_.size());

    range_info_ = *old_range_info;
    assert(range_info_.new_partition_id_.size() == range_info_.new_key_.size());

    range_record_ = std::make_unique<RangeRecord>(
        &range_info_, nullptr, old_end_key, nullptr);

    old_end_key_ = old_end_key;

    assert(slice_info_.empty());
    assert(data_sync_vec_.empty());
    assert(archive_vec_.empty());
    assert(mv_base_vec_.empty());
    assert(new_range_info_.empty());
    assert(recover_split_started_ == nullptr);

    new_range_info_ = std::move(new_range_info);

    // Reset all sub-operations
    auto node_group_count = Sharder::Instance().NodeGroupCount();

    prepare_acquire_all_write_op_.Reset(node_group_count);
    prepare_acquire_all_write_op_.ResetHandlerTxm(txm);

    prepare_log_op_.Reset();
    prepare_log_op_.ResetHandlerTxm(txm);

    install_new_range_op_.Reset(node_group_count);
    install_new_range_op_.ResetHandlerTxm(txm);

    ds_migrate_old_partition_op_.Reset();
    ds_migrate_old_partition_op_.ResetHandlerTxm(txm);

    data_sync_scan_op_.Reset();
    data_sync_scan_op_.ResetHandlerTxm(txm);

    flush_op_.Reset();
    flush_op_.ResetHandlerTxm(txm);

    commit_acquire_all_write_op_.Reset(node_group_count);
    commit_acquire_all_write_op_.ResetHandlerTxm(txm);

    commit_log_op_.Reset();
    commit_log_op_.ResetHandlerTxm(txm);

    ds_upsert_range_op_.Reset();
    ds_upsert_range_op_.ResetHandlerTxm(txm);

    kickout_old_range_data_op_.Reset();
    kickout_old_range_data_op_.ResetHandlerTxm(txm);

    post_all_lock_op_.Reset(node_group_count);
    post_all_lock_op_.ResetHandlerTxm(txm);

    ds_clean_old_range_op_.Reset();
    ds_clean_old_range_op_.ResetHandlerTxm(txm);

    clean_log_op_.Reset();
    clean_log_op_.ResetHandlerTxm(txm);

    release_catalog_read_lock_op_.Reset({});
    release_catalog_read_lock_op_.ResetHandlerTxm(txm);

    old_start_key_ = range_info_.StartKey() != nullptr ? range_info_.StartKey()
                                                       : old_start_key;
    prepare_acquire_all_write_op_.table_name_ = &range_table_name_;
    prepare_acquire_all_write_op_.cc_op_ = CcOperation::Write;
    prepare_acquire_all_write_op_.protocol_ = CcProtocol::Locking;
    prepare_acquire_all_write_op_.key_ = old_start_key_;

    install_new_range_op_.table_name_ = &range_table_name_;
    install_new_range_op_.write_type_ = PostWriteType::PrepareCommit;
    install_new_range_op_.op_type_ = OperationType::Update;
    install_new_range_op_.key_ = old_start_key_;

    flush_op_.tab_name_ = &table_name_;
    flush_op_.data_sync_vec_ = &data_sync_vec_;
    flush_op_.archive_vec_ = &archive_vec_;
    flush_op_.mv_vec_ = &mv_base_vec_;
    flush_op_.schema_ = table_schema_;
    flush_op_.node_group_ = node_group_;

    commit_acquire_all_write_op_.table_name_ = &range_table_name_;
    commit_acquire_all_write_op_.cc_op_ = CcOperation::Write;
    commit_acquire_all_write_op_.protocol_ = CcProtocol::Locking;
    commit_acquire_all_write_op_.key_ = old_start_key_;

    kickout_old_range_data_op_.table_name_ = &table_name_;
    kickout_old_range_data_op_.node_group_ = node_group;

    post_all_lock_op_.table_name_ = &range_table_name_;
    post_all_lock_op_.write_type_ = PostWriteType::PostCommit;
    post_all_lock_op_.op_type_ = OperationType::Update;
    post_all_lock_op_.key_ = old_start_key_;

    kickout_data_it_ = {};

    catalog_cc_entry_ = std::nullopt;
    recover_split_started_ = nullptr;
    pending_pin_data_ = false;

    TX_TRACE_ASSOCIATE(
        this, &prepare_acquire_all_write_op_, "prepare_acquire_all_op_");
    TX_TRACE_ASSOCIATE(this, &prepare_log_op_, "prepare_log_op_");
    TX_TRACE_ASSOCIATE(this, &install_new_range_op_, "install_new_range_op_");
    TX_TRACE_ASSOCIATE(this, &data_sync_scan_op_, "data_sync_scan_op_");
    TX_TRACE_ASSOCIATE(this, &flush_op_, "flush_op_");
    TX_TRACE_ASSOCIATE(
        this, &commit_acquire_all_write_op_, "commit_acquire_all_op_");
    TX_TRACE_ASSOCIATE(this, &commit_log_op_, "commit_log_op_");
    TX_TRACE_ASSOCIATE(this, &ds_upsert_range_op_, "ds_upsert_range_op_");
    TX_TRACE_ASSOCIATE(this, &post_all_lock_op_, "post_all_lock_op_");
    TX_TRACE_ASSOCIATE(this, &ds_clean_old_range_op_, "ds_clean_old_range_op_");
    TX_TRACE_ASSOCIATE(this, &clean_log_op_, "clean_log_op_");
}

void SplitFlushRangeOp::ClearDataSyncVec()
{
    data_sync_vec_.clear();
    data_sync_vec_.shrink_to_fit();
    archive_vec_.clear();
    archive_vec_.shrink_to_fit();
    mv_base_vec_.clear();
    mv_base_vec_.shrink_to_fit();
}

void SplitFlushRangeOp::ClearInfos()
{
    // release TxKey ownership to reduce memory usage
    range_info_.Clear();
    new_range_info_.clear();
    slice_info_.clear();

    new_range_info_.shrink_to_fit();
    slice_info_.shrink_to_fit();

    range_record_ = nullptr;
}

void SplitFlushRangeOp::Forward(TransactionExecution *txm)
{
    if (op_ == nullptr)
    {
        // Initialize commit ts as the start time of tx. This value will
        // be updated after prepaire_acquire_all_write_op_.
        txm->commit_ts_ = txm->start_ts_ + 1;
        // Pin node group data during split flush tx.
        int64_t leader_term =
            Sharder::Instance().TryPinNodeGroupData(node_group_);
        if (leader_term < 0)
        {
            // No longer leader.
            ForceToFinish(txm);
            return;
        }

        // Acquire Write lock on range entry on all node groups and
        // calculate commit ts for tx.
        LOG(INFO) << "Split Flush transaction prepare acquire all, range id "
                  << range_info_.PartitionId() << ", txn: " << txm->TxNumber();
        ForwardToSubOperation(txm, &prepare_acquire_all_write_op_);
    }
    else if (op_ == &prepare_acquire_all_write_op_)
    {
        if (!CheckLeaderTerm(node_group_, txm->tx_term_, txm->tx_status_))
        {
            ForceToFinish(txm);
            return;
        }
        if (prepare_acquire_all_write_op_.fail_cnt_.load(
                std::memory_order_relaxed) > 0)
        {
            LOG(ERROR) << "Split Flush transaction failed to obtain write "
                          "lock, tx_number:"
                       << txm->TxNumber();

            // Set commit ts to 0 to indicate transaction failure.
            // post_all_lock_op_ will release locks acquired.
            txm->commit_ts_ = tx_op_failed_ts_;
            ForwardToSubOperation(txm, &post_all_lock_op_);
            return;
        }

        // Update the commit ts. The commit ts is the max value between
        // 1) max commit ts of last tx that updated records in this range
        // partition on all node groups, 2) local timestamp when tx started.
        // The commit ts is used to decide whether a record needs to be
        // flushed during the next phase. We want to make sure every records
        // modified before commit ts is flushed to new partition in KV
        // store.
        for (size_t idx = 0; idx < prepare_acquire_all_write_op_.upload_cnt_;
             ++idx)
        {
            const AcquireAllResult &acq_all_res =
                prepare_acquire_all_write_op_.hd_results_[idx].Value();
            uint64_t ts = std::max(acq_all_res.commit_ts_ + 1,
                                   acq_all_res.last_vali_ts_ + 1);
            txm->commit_ts_ = std::max(txm->commit_ts_, ts);
        }

        // Write prepare log in next subop.
        FillPrepareLogRequest(txm);
        LOG(INFO) << "Split Flush transaction write prepare log, range id "
                  << range_info_.PartitionId() << ", txn: " << txm->TxNumber();
        ForwardToSubOperation(txm, &prepare_log_op_);
    }
    else if (op_ == &prepare_log_op_)
    {
        assert(txm->rw_set_.WriteSetSize() == 0);
        if (!CheckLeaderTerm(node_group_, txm->tx_term_, txm->tx_status_))
        {
            ForceToFinish(txm);
            return;
        }
        if (prepare_log_op_.hd_result_.IsError())
        {
            if (prepare_log_op_.hd_result_.ErrorCode() ==
                CcErrorCode::LOG_CLOSURE_RESULT_UNKNOWN_ERR)
            {
                // prepare log result unknown, keep retrying until getting a
                // clear response, either success or failure, or the
                // coordinator itself is no longer leader
                int64_t tx_node_term =
                    Sharder::Instance().LeaderTerm(txm->TxCcNodeId());
                if (tx_node_term > 0)
                {
                    DLOG(WARNING)
                        << "Split range write prepare log result unknown, "
                           "tx_number:"
                        << txm->TxNumber() << ", keep retrying";
                    // set retry flag and retry prepare log
                    ::txlog::WriteLogRequest *log_req =
                        prepare_log_op_.log_closure_.LogRequest()
                            .mutable_write_log_request();
                    log_req->set_retry(true);
                    RetrySubOperation(txm, &prepare_log_op_);
                }
                else
                {
                    DLOG(ERROR) << "Split range write prepare log result "
                                   "unknown, tx_number:"
                                << txm->TxNumber()
                                << ", not leader any more, stop retrying";
                    // Not leader anymore, just quit. New leader will know
                    // whether prepare log succeeds and continue the rest if
                    // it does. Should not release the write intents. If
                    // prepare log is not written, the write intents will be
                    // released individually via orphan lock recovery
                    // mechanism.
                    ForceToFinish(txm);
                }
            }
            else
            {
                // Set commit ts to 0 to indicate transaction failure.
                // post_all_lock_op_ will release locks acquired.
                txm->commit_ts_ = tx_op_failed_ts_;
                ForwardToSubOperation(txm, &post_all_lock_op_);
            }
            return;
        }

        // Fill in new range info to old range record.
        range_info_.dirty_ts_ = txm->commit_ts_;
        for (auto &range_info : new_range_info_)
        {
            range_info_.new_partition_id_.push_back(range_info.second);
            range_info_.new_key_.push_back(range_info.first->Clone());
        }

        // Install dirty range info on all node groups and downgrade to
        // write intent lock in next subop.
        install_new_range_op_.rec_ = range_record_.get();
        range_record_->SetRangeInfo(&range_info_);

        LOG(INFO) << "Split Flush transaction install dirty range, range id "
                  << range_info_.PartitionId() << ", txn: " << txm->TxNumber();
        ForwardToSubOperation(txm, &install_new_range_op_);
    }
    else if (op_ == &install_new_range_op_)
    {
        if (!CheckLeaderTerm(node_group_, txm->tx_term_, txm->tx_status_))
        {
            ForceToFinish(txm);
            return;
        }
        if (txm->tx_status_ == TxnStatus::Recovering && pending_pin_data_)
        {
            int64_t leader_term =
                Sharder::Instance().TryPinNodeGroupData(node_group_);
            if (leader_term < 0)
            {
                // Not leader yet, keep trying.
                return;
            }
            pending_pin_data_ = false;
        }
        if (install_new_range_op_.hd_result_.IsError())
        {
            LOG(ERROR) << "Split Flush transaction failed to install dirty "
                          "range, tx number "
                       << txm->TxNumber();
            install_new_range_op_.rec_ = range_record_.get();
            range_record_->SetRangeInfo(&range_info_);
            RetrySubOperation(txm, &install_new_range_op_);
            return;
        }

        // Copy data from old partition to its new partition in data store.
        ds_migrate_old_partition_op_.op_func_ =
            [&table_name = table_name_,
             old_partition_id = range_info_.partition_id_,
             old_end_key = old_end_key_,
             &new_partition_info = new_range_info_,
             tx_ts = txm->commit_ts_,
             table_schema = table_schema_,
             &hd_res = ds_migrate_old_partition_op_.hd_result_]
        {
            TxWorkerPool *tx_worker_pool =
                Sharder::Instance().GetTxWorkerPool();
            store::DataStoreHandler *const store_hd =
                Sharder::Instance().GetLocalCcShards()->store_hd_;
            tx_worker_pool->SubmitWork(
                [table_name,
                 old_partition_id,
                 old_end_key,
                 &new_partition_info,
                 tx_ts,
                 table_schema,
                 &hd_res,
                 store_hd]
                {
                    bool succ = store_hd->CopyRangeData(table_name,
                                                        old_partition_id,
                                                        old_end_key,
                                                        new_partition_info,
                                                        tx_ts,
                                                        table_schema);
                    if (succ)
                    {
                        hd_res.SetFinished();
                    }
                    else
                    {
                        LOG(INFO) << "Copying data fails for split range "
                                  << old_partition_id;
                        hd_res.SetError(CcErrorCode::DATA_STORE_ERR);
                    }
                });
        };

        LOG(INFO) << "Split Flush transaction migrate old range data, range id "
                  << range_info_.PartitionId() << ", txn: " << txm->TxNumber();
        ForwardToSubOperation(txm, &ds_migrate_old_partition_op_);
    }
    else if (op_ == &ds_migrate_old_partition_op_)
    {
        if (!CheckLeaderTerm(node_group_, txm->tx_term_, txm->tx_status_))
        {
            ForceToFinish(txm);
            return;
        }
        if (ds_migrate_old_partition_op_.hd_result_.IsError())
        {
            LOG(ERROR) << "Split Flush transaction failed to migrate old "
                          "partition, tx number "
                       << txm->TxNumber();
            // Set commit ts to 0 to indicate transaction failure.
            // post_all_lock_op_ will release locks acquired.
            RetrySubOperation(txm, &ds_migrate_old_partition_op_);
            return;
        }
        data_sync_scan_op_.op_func_ =
            [&table_name = table_name_,
             table_schema = table_schema_,
             start_key = old_start_key_,
             end_key = old_end_key_,
             data_sync_vec = &data_sync_vec_,
             archive_vec = &archive_vec_,
             mv_base_vec = &mv_base_vec_,
             node_group = node_group_,
             ckpt_ts = txm->commit_ts_,
             &local_cc_shards = txm->GetTxProcessor()->local_cc_shards_,
             &hd_res = data_sync_scan_op_.hd_result_]
        {
            TxWorkerPool *tx_worker_pool =
                Sharder::Instance().GetTxWorkerPool();
            tx_worker_pool->SubmitWork(
                [table_name,
                 table_schema,
                 start_key,
                 end_key,
                 data_sync_vec,
                 archive_vec,
                 mv_base_vec,
                 node_group,
                 ckpt_ts,
                 &local_cc_shards,
                 &hd_res]
                {
                    std::vector<std::vector<FlushRecord>> data_sync_vecs;
                    std::vector<std::vector<FlushRecord>> archive_vecs;
                    std::vector<std::vector<const TxKey *>> mv_base_vecs;

                    std::vector<std::pair<TxKey::Uptr, bool>> resume_pos;

                    for (size_t i = 0;
                         i < Sharder::Instance().GetLocalCcShardsCount();
                         i++)
                    {
                        data_sync_vecs.emplace_back();
                        archive_vecs.emplace_back();
                        mv_base_vecs.emplace_back();
                        resume_pos.emplace_back(nullptr, false);
                    }

                    bool scan_data_drained = false;
                    DataSyncScanCc scan_cc(
                        table_name,
                        ckpt_ts,
                        node_group,
                        Sharder::Instance().GetLocalCcShardsCount(),
                        std::move(resume_pos),
                        LocalCcShards::DATA_SYNC_SCAN_BATCH_SIZE,
                        start_key,
                        end_key);
                    while (!scan_data_drained)
                    {
                        for (size_t i = 0;
                             i < Sharder::Instance().GetLocalCcShardsCount();
                             i++)
                        {
                            local_cc_shards.EnqueueToCcShard(i, &scan_cc);
                        }
                        scan_cc.Wait();

                        if (scan_cc.IsError())
                        {
                            LOG(INFO) << "DataSync scan failed on table "
                                      << table_name.StringView();
                            hd_res.SetError(scan_cc.ErrorCode());
                            return;
                        }
                        else
                        {
                            auto &res = scan_cc.Result();
                            scan_data_drained = true;

                            for (size_t i = 0;
                                 i <
                                 Sharder::Instance().GetLocalCcShardsCount();
                                 i++)
                            {
                                // if the data is drained
                                scan_data_drained =
                                    res.at(i).second && scan_data_drained;
                                // move the bucket into the tank
                                std::move(
                                    scan_cc.DataSyncVec(i).begin(),
                                    scan_cc.DataSyncVec(i).end(),
                                    std::back_inserter(data_sync_vecs.at(i)));

                                std::move(
                                    scan_cc.ArchiveVec(i).begin(),
                                    scan_cc.ArchiveVec(i).end(),
                                    std::back_inserter(archive_vecs.at(i)));

                                std::move(
                                    scan_cc.MoveBaseVec(i).begin(),
                                    scan_cc.MoveBaseVec(i).end(),
                                    std::back_inserter(mv_base_vecs.at(i)));
                            }
                            scan_cc.Reset(std::move(res));
                        }
                    }

                    // Sort output vectors in key sorting order.
                    auto key_greater = [](const TxKey *r1,
                                          const TxKey *r2) -> bool
                    { return *r2 < *r1; };
                    MergeSortedVectors(std::move(mv_base_vecs),
                                       *mv_base_vec,
                                       key_greater,
                                       true);
                    auto rec_greater = [](const FlushRecord &r1,
                                          const FlushRecord &r2) -> bool
                    { return *r2.Key() < *r1.Key(); };
                    // To avoid repeatedly set the ckpt_ts_ of a cc entry, which
                    // might cause the ccentry become invalid in between, remove
                    // duplicate flush record from ckpt_vec.
                    MergeSortedVectors(std::move(data_sync_vecs),
                                       *data_sync_vec,
                                       rec_greater,
                                       true);
                    // For archive vec we don't need to worry about duplicate
                    // causing issue since we're not visiting their cc entry.
                    // Also we cannot rely on key compare to dedup archive vec
                    // since a key could have multiple version of archive
                    // versions.
                    MergeSortedVectors(std::move(archive_vecs),
                                       *archive_vec,
                                       rec_greater,
                                       false);

                    auto lower_bound_cmp =
                        [](const FlushRecord &rec, const TxKey &key)
                    { return *rec.Key() < key; };
                    auto batch_it = data_sync_vec->begin();
                    size_t slice_start_idx = 0;
                    size_t slice_end_idx = 0;
                    StoreRange *range = local_cc_shards.FindRange(
                        table_name, node_group, *start_key);

                    while (batch_it != data_sync_vec->end())
                    {
                        const TxKey &slice_start_key = *batch_it->Key();
                        StoreSlice *curr_slice =
                            range->FindSlice(slice_start_key);

                        auto slice_end_it =
                            curr_slice->EndKey() == range->RangeEndKey()
                                ? data_sync_vec->end()
                                : std::lower_bound(batch_it,
                                                   data_sync_vec->end(),
                                                   *curr_slice->EndKey(),
                                                   lower_bound_cmp);

                        slice_end_idx =
                            std::distance(data_sync_vec->begin(), slice_end_it);
                        int32_t slice_delta_size = 0;
                        uint32_t slice_size = 0;

                        for (; batch_it != slice_end_it; ++batch_it)
                        {
                            slice_delta_size += batch_it->delta_size_;
                        }

                        int32_t sum = curr_slice->Size() + slice_delta_size;
                        slice_size = sum >= 0 ? sum : 0;
                        curr_slice->SetPostCkptSize(slice_size);
                        if (slice_size > StoreSlice::slice_upper_bound &&
                            !range->UpdateSliceSpec(curr_slice,
                                                    table_name,
                                                    table_schema,
                                                    node_group,
                                                    ckpt_ts,
                                                    *data_sync_vec,
                                                    slice_start_idx,
                                                    slice_end_idx))
                        {
                            hd_res.SetError(CcErrorCode::DATA_STORE_ERR);
                            return;
                        }

                        batch_it = slice_end_it;
                        slice_start_idx = slice_end_idx;
                    }

                    hd_res.SetFinished();
                });
        };
        LOG(INFO) << "Split Flush transaction data sync scan, range id "
                  << range_info_.PartitionId() << ", txn: " << txm->TxNumber();
        ForwardToSubOperation(txm, &data_sync_scan_op_);
    }
    else if (op_ == &data_sync_scan_op_)
    {
        if (!CheckLeaderTerm(node_group_, txm->tx_term_, txm->tx_status_))
        {
            ClearDataSyncVec();
            ForceToFinish(txm);
            return;
        }
        if (data_sync_scan_op_.hd_result_.IsError())
        {
            LOG(ERROR) << "Split Flush transaction failed to scan for "
                          "data sync, tx number "
                       << txm->TxNumber();
            ClearDataSyncVec();
            // Set commit ts to 0 to indicate transaction failure.
            // post_all_lock_op_ will release locks acquired.
            RetrySubOperation(txm, &data_sync_scan_op_);
            return;
        }
        flush_op_.data_sync_ts_ = txm->commit_ts_;
        flush_op_.tx_term_ = txm->tx_term_;

        LOG(INFO) << "Split Flush transaction flush data, range id "
                  << range_info_.PartitionId() << ", txn: " << txm->TxNumber();
        ForwardToSubOperation(txm, &flush_op_);
    }
    else if (op_ == &flush_op_)
    {
        if (!CheckLeaderTerm(node_group_, txm->tx_term_, txm->tx_status_))
        {
            ClearDataSyncVec();
            ForceToFinish(txm);
            return;
        }
        if (flush_op_.hd_result_.IsError())
        {
            LOG(ERROR) << "Split Flush transaction failed to flush data, "
                          "tx number "
                       << txm->TxNumber();

            RetrySubOperation(txm, &flush_op_);
            return;
        }

        CODE_FAULT_INJECTOR("term_SplitFlushOp_FlushOp_Continue", {
            LOG(INFO) << "FaultInject  term_SplitFlushOp_FlushOp_Continue";
            return;
        });
        // clear and release ckpt vecs
        ClearDataSyncVec();
        // Now we can copy out the slice info since it's finalized after
        // flush data
        LocalCcShards *shards = Sharder::Instance().GetLocalCcShards();
        const StoreRange *range =
            shards->FindRange(table_name_, node_group_, *old_start_key_);
        const auto &slices = range->Slices();
        for (auto slice_it = slices.cbegin(); slice_it != slices.cend();
             ++slice_it)
        {
            if (slice_it == slices.cbegin())
            {
                slice_info_.emplace_back(nullptr, (*slice_it)->Size());
            }
            else
            {
                slice_info_.emplace_back((*slice_it)->StartKey()->Clone(),
                                         (*slice_it)->Size());
            }
        }

        LOG(INFO) << "Split Flush transaction commit acqurie all, range id "
                  << range_info_.PartitionId() << ", txn: " << txm->TxNumber();
        // Upgrade to write lock again for commit phase.
        ForwardToSubOperation(txm, &commit_acquire_all_write_op_);
    }
    else if (op_ == &commit_acquire_all_write_op_)
    {
        if (!CheckLeaderTerm(node_group_, txm->tx_term_, txm->tx_status_))
        {
            ForceToFinish(txm);
            return;
        }
        if (commit_acquire_all_write_op_.fail_cnt_.load(
                std::memory_order_relaxed) > 0)
        {
            LOG(ERROR) << "Split Flush transaction failed to obtain write "
                          "lock, tx_number:"
                       << txm->TxNumber();

            RetrySubOperation(txm, &commit_acquire_all_write_op_);
            return;
        }

        ACTION_FAULT_INJECTOR("range_split_commit_acquire_all");
        FillCommitLogRequest(txm);
        LOG(INFO) << "Split Flush transaction write commit log, range id "
                  << range_info_.PartitionId() << ", txn: " << txm->TxNumber();
        ForwardToSubOperation(txm, &commit_log_op_);
    }
    else if (op_ == &commit_log_op_)
    {
        if (!CheckLeaderTerm(node_group_, txm->tx_term_, txm->tx_status_))
        {
            ForceToFinish(txm);
            return;
        }
        if (commit_log_op_.hd_result_.IsError())
        {
            // error & retry
            ::txlog::WriteLogRequest *log_req =
                commit_log_op_.log_closure_.LogRequest()
                    .mutable_write_log_request();
            log_req->set_retry(true);
            RetrySubOperation(txm, &commit_log_op_);
            return;
        }
        // Split the range slices based on the range split keys.
        LocalCcShards *shards = Sharder::Instance().GetLocalCcShards();
        std::vector<
            std::tuple<const TxKey *, int32_t, std::vector<StoreSlice *>>>
            splitted_range_info;
        StoreRange *old_range =
            shards->FindRange(table_name_, node_group_, *old_start_key_);
        auto &slices = old_range->Slices();
        auto slice_it = slices.begin();
        const TxKey *start_key = old_range->RangeStartKey();
        int32_t range_id = old_range->PartitionId();
        std::vector<StoreSlice *> subrange_slices;
        // First slice is always left in the old range. Put it into vector
        // first to avoid dealing with null start key.
        subrange_slices.push_back(slice_it->get());
        slice_it++;
        for (auto &info : new_range_info_)
        {
            while (slice_it != slices.end() &&
                   *(*slice_it)->StartKey() < *info.first)
            {
                subrange_slices.push_back(slice_it->get());
                slice_it++;
            }
            splitted_range_info.emplace_back(
                start_key, range_id, std::move(subrange_slices));
            subrange_slices.clear();
            start_key = info.first.get();
            range_id = info.second;
        }
        // The rest of the slices belong the last new range.
        for (; slice_it != slices.end(); slice_it++)
        {
            subrange_slices.push_back(slice_it->get());
        }
        splitted_range_info.emplace_back(
            start_key, range_id, std::move(subrange_slices));

        // Insert new ranges into data store range table. Update
        // range slice size of the old range.
        ds_upsert_range_op_.op_func_ =
            [&table_name = table_name_,
             old_range = old_range,
             range_info = std::move(splitted_range_info),
             tx_ts = txm->commit_ts_,
             table_schema = table_schema_,
             &hd_res = ds_upsert_range_op_.hd_result_]
        {
            // Launch a new thread instead of sending it to workerpool to avoid
            // being blocked during write lock is held.
            std::thread worker = std::thread(
                [table_name,
                 old_range,
                 range_info = std::move(range_info),
                 tx_ts,
                 table_schema,
                 &hd_res]
                {
                    store::DataStoreHandler *const store_hd =
                        Sharder::Instance().GetLocalCcShards()->store_hd_;
                    bool succ =
                        store_hd->UpsertRanges(table_name, range_info, tx_ts);
                    if (succ)
                    {
                        hd_res.SetFinished();
                    }
                    else
                    {
                        hd_res.SetError(CcErrorCode::DATA_STORE_ERR);
                    }
                });
            worker.detach();
        };

        LOG(INFO) << "Split Flush transaction upsert new range spec, range id "
                  << range_info_.PartitionId() << ", txn: " << txm->TxNumber();
        ForwardToSubOperation(txm, &ds_upsert_range_op_);
    }
    else if (op_ == &ds_upsert_range_op_)
    {
        if (!CheckLeaderTerm(node_group_, txm->tx_term_, txm->tx_status_))
        {
            ForceToFinish(txm);
            return;
        }
        if (txm->tx_status_ == TxnStatus::Recovering && pending_pin_data_)
        {
            int64_t leader_term =
                Sharder::Instance().TryPinNodeGroupData(node_group_);
            if (leader_term < 0)
            {
                // Not leader yet, keep trying.
                return;
            }
            pending_pin_data_ = false;
        }
        if (ds_upsert_range_op_.hd_result_.IsError())
        {
            // error & retry
            LOG(ERROR) << "Split Flush transaction failed to update range info "
                          "in data store, tx_number:"
                       << txm->TxNumber();
            RetrySubOperation(txm, &ds_upsert_range_op_);
            return;
        }
        // Kickout old range data. For those data that now falls on a
        // new node, we need to kickout them out from the old node's ccmap.
        // We don't care about the commit ts of the target cc entry, all entries
        // fall into the migrated new ranges should be kicked out no matter
        // what.
        kickout_old_range_data_op_.commit_ts_ = UINT64_MAX;
        auto local_shards = Sharder::Instance().GetLocalCcShards();
        for (kickout_data_it_ = new_range_info_.cbegin();
             kickout_data_it_ != new_range_info_.cend();
             kickout_data_it_++)
        {
            NodeGroupId new_owner =
                local_shards
                    ->GetRangeOwner(kickout_data_it_->second, node_group_)
                    ->BucketOwner();
            if (new_owner != node_group_)
            {
                // Note that even if the new node group falls on the same node,
                // we still need to clean the cc entry from native ccmap since
                // failover and native ccmaps are separated.
                kickout_old_range_data_op_.start_key_ =
                    kickout_data_it_->first.get();
                if (std::next(kickout_data_it_) == new_range_info_.cend())
                {
                    kickout_old_range_data_op_.end_key_ = old_end_key_;
                }
                else
                {
                    kickout_old_range_data_op_.end_key_ =
                        std::next(kickout_data_it_)->first.get();
                }

                LOG(INFO)
                    << "Split Flush transaction kickout old data in range "
                    << kickout_data_it_->second << ", original range id "
                    << range_info_.PartitionId()
                    << ", txn: " << txm->TxNumber();
                break;
            }
        }

        if (kickout_data_it_ == new_range_info_.cend())
        {
            // All of the new ranges falls on the same node, proceed to post
            // write all. Now broadcast slice info to all nodes through
            // PostWriteAll. New ranges might land on other nodes.
            post_all_lock_op_.rec_ = range_record_.get();
            range_record_->range_slices_ = &slice_info_;
            range_record_->end_key_ = old_end_key_;
            range_record_->SetRangeInfo(&range_info_);

            LOG(INFO) << "Split Flush transaction post all lock, range id "
                      << range_info_.PartitionId()
                      << ", txn: " << txm->TxNumber();
            ForwardToSubOperation(txm, &post_all_lock_op_);
        }
        else
        {
            ForwardToSubOperation(txm, &kickout_old_range_data_op_);
        }
    }
    else if (op_ == &kickout_old_range_data_op_)
    {
        if (!CheckLeaderTerm(node_group_, txm->tx_term_, txm->tx_status_))
        {
            ForceToFinish(txm);
            return;
        }
        if (kickout_old_range_data_op_.hd_result_.IsError())
        {
            // error & retry
            LOG(ERROR)
                << "Split Flush transaction failed to kickout old range data"
                   ", tx_number:"
                << txm->TxNumber();
            RetrySubOperation(txm, &kickout_old_range_data_op_);
            return;
        }
        kickout_data_it_++;
        auto local_shards = Sharder::Instance().GetLocalCcShards();
        for (; kickout_data_it_ != new_range_info_.cend(); kickout_data_it_++)
        {
            NodeGroupId new_owner =
                local_shards
                    ->GetRangeOwner(kickout_data_it_->second, node_group_)
                    ->BucketOwner();
            if (new_owner != node_group_)
            {
                kickout_old_range_data_op_.start_key_ =
                    kickout_data_it_->first.get();
                if (std::next(kickout_data_it_) == new_range_info_.cend())
                {
                    kickout_old_range_data_op_.end_key_ = old_end_key_;
                }
                else
                {
                    kickout_old_range_data_op_.end_key_ =
                        std::next(kickout_data_it_)->first.get();
                }

                LOG(INFO)
                    << "Split Flush transaction kickout old data in range "
                    << kickout_data_it_->second << ", original range id "
                    << range_info_.PartitionId()
                    << ", txn: " << txm->TxNumber();
                break;
            }
        }

        if (kickout_data_it_ == new_range_info_.cend())
        {
            // Now broadcast slice info to all nodes through PostWriteAll. New
            // ranges might land on other nodes.
            post_all_lock_op_.rec_ = range_record_.get();
            range_record_->range_slices_ = &slice_info_;
            range_record_->end_key_ = old_end_key_;
            range_record_->SetRangeInfo(&range_info_);
            LOG(INFO) << "Split Flush transaction post all lock, range id "
                      << range_info_.PartitionId()
                      << ", txn: " << txm->TxNumber();
            ForwardToSubOperation(txm, &post_all_lock_op_);
        }
        else
        {
            ForwardToSubOperation(txm, &kickout_old_range_data_op_);
        }
    }
    else if (op_ == &post_all_lock_op_)
    {
        if (!CheckLeaderTerm(node_group_, txm->tx_term_, txm->tx_status_))
        {
            ForceToFinish(txm);
            return;
        }
        if (post_all_lock_op_.hd_result_.IsError())
        {
            // error & retry
            LOG(ERROR) << "Split Flush transaction failed at post all "
                          "lock, tx_number:"
                       << txm->TxNumber();
            post_all_lock_op_.rec_ = range_record_.get();
            range_record_->range_slices_ = &slice_info_;
            range_record_->end_key_ = old_end_key_;
            range_record_->SetRangeInfo(&range_info_);
            RetrySubOperation(txm, &post_all_lock_op_);
            return;
        }

        // Delete stale data from old partition
        ds_clean_old_range_op_.op_func_ =
            [partition_id = range_info_.partition_id_,
             start_key = new_range_info_.front().first.get(),
             &table_name = table_name_,
             table_schema = table_schema_,
             &hd_res = ds_clean_old_range_op_.hd_result_]
        {
            TxWorkerPool *tx_worker_pool =
                Sharder::Instance().GetTxWorkerPool();
            store::DataStoreHandler *const store_hd =
                Sharder::Instance().GetLocalCcShards()->store_hd_;
            tx_worker_pool->SubmitWork(
                [partition_id,
                 start_key,
                 table_name,
                 table_schema,
                 &hd_res,
                 store_hd]
                {
                    bool succ = store_hd->DeleteOutOfRangeData(
                        table_name, partition_id, start_key, table_schema);
                    if (succ)
                    {
                        hd_res.SetFinished();
                    }
                    else
                    {
                        hd_res.SetError(CcErrorCode::DATA_STORE_ERR);
                    }
                });
        };
        LOG(INFO) << "Split Flush transaction clean old range data in kv "
                     "store, range id "
                  << range_info_.PartitionId() << ", txn: " << txm->TxNumber();
        ForwardToSubOperation(txm, &ds_clean_old_range_op_);
    }
    else if (op_ == &ds_clean_old_range_op_)
    {
        if (!CheckLeaderTerm(node_group_, txm->tx_term_, txm->tx_status_))
        {
            ForceToFinish(txm);
            return;
        }
        if (ds_clean_old_range_op_.hd_result_.IsError())
        {
            // error & retry
            LOG(ERROR) << "Split Flush transaction failed to delete data "
                          "in old range "
                          "in data store, tx_number:"
                       << txm->TxNumber();
            RetrySubOperation(txm, &ds_clean_old_range_op_);
            return;
        }

        FillCleanLogRequest(txm);
        LOG(INFO) << "Split Flush transaction write clean log, range id "
                  << range_info_.PartitionId() << ", txn: " << txm->TxNumber();
        ForwardToSubOperation(txm, &clean_log_op_);
    }
    else if (op_ == &clean_log_op_)
    {
        if (clean_log_op_.hd_result_.IsError() &&
            CheckLeaderTerm(node_group_, txm->tx_term_, txm->tx_status_))
        {
            // set retry flag and retry clean log
            ::txlog::WriteLogRequest *log_req =
                clean_log_op_.log_closure_.LogRequest()
                    .mutable_write_log_request();
            log_req->set_retry(true);
            RetrySubOperation(txm, &clean_log_op_);
            return;
        }
        else if (txm->tx_status_ == TxnStatus::Recovering)
        {
            // When the tx is in the recovery state, we have to release
            // the catalog read lock here in the op since there's no external
            // caller that commits the tx for us.
            assert(catalog_cc_entry_ != std::nullopt);
            release_catalog_read_lock_op_.Reset(std::make_pair(
                &catalog_cc_entry_->first, &catalog_cc_entry_->second));
            ForwardToSubOperation(txm, &release_catalog_read_lock_op_);
            return;
        }
        else
        {
            if (txm->commit_ts_ == tx_op_failed_ts_)
            {
                txm->bool_resp_->Finish(false);
            }
            else
            {
                txm->bool_resp_->Finish(true);
            }
            txm->state_stack_.pop_back();
            Sharder::Instance().UnpinNodeGroupData(node_group_);
            assert(txm->state_stack_.empty());

            ClearInfos();

            assert(this == txm->split_flush_op_.get());
            assert(recover_split_started_ == nullptr);
            LocalCcShards *shards = Sharder::Instance().GetLocalCcShards();
            std::unique_lock<std::mutex> lk(
                shards->split_flush_range_op_pool_mux_);
            shards->split_flush_range_op_pool_.emplace_back(
                std::move(txm->split_flush_op_));
            assert(txm->split_flush_op_ == nullptr);
        }
    }
    else if (op_ == &release_catalog_read_lock_op_)
    {
        assert(txm->tx_status_ == TxnStatus::Recovering);
        if (release_catalog_read_lock_op_.hd_result_.IsError() &&
            CheckLeaderTerm(node_group_, txm->tx_term_, txm->tx_status_))
        {
            RetrySubOperation(txm, &release_catalog_read_lock_op_);
            return;
        }
        if (recover_split_started_->fetch_sub(1) == 1)
        {
            // The last recovering range split op has now finished, set
            // data sync flag to false to unblock checkpoint on this table.
            Sharder::Instance().GetLocalCcShards()->SetDataSyncOngoing(
                TableName{table_name_.GetBaseTableNameSV(), TableType::Primary},
                node_group_,
                false);
        }
        LOG(INFO) << "Recover range split tx succeeded on range "
                  << range_info_.PartitionId() << ", txn: " << txm->TxNumber();

        Sharder::Instance().UnpinNodeGroupData(node_group_);

        ClearInfos();
        recover_split_started_ = nullptr;

        assert(this == txm->split_flush_op_.get());

        {
            LocalCcShards *shards = Sharder::Instance().GetLocalCcShards();
            std::unique_lock<std::mutex> lk(
                shards->split_flush_range_op_pool_mux_);
            shards->split_flush_range_op_pool_.emplace_back(
                std::move(txm->split_flush_op_));
        }

        assert(txm->split_flush_op_ == nullptr);
        txm->Reset();
        // Setting the tx's status to finished signals that this tx
        // state machine can be recycled for a new tx.
        txm->tx_status_.store(TxnStatus::Finished, std::memory_order_release);
    }
}

/**
 * @brief Fill prepare log for Split-Flush Tx. We need to have old range
 * info and new range info, commit ts (for deciding the records that needs
 * to be flushed) in the log record.
 *
 * @param txm
 */
void SplitFlushRangeOp::FillPrepareLogRequest(TransactionExecution *txm)
{
    prepare_log_op_.log_type_ = TxLogType::PREPARE;
    prepare_log_op_.log_closure_.LogRequest().Clear();

    ::txlog::WriteLogRequest *prepare_log_rec =
        prepare_log_op_.log_closure_.LogRequest().mutable_write_log_request();

    // Set general transaction information
    prepare_log_rec->set_tx_term(txm->tx_term_);
    prepare_log_rec->set_txn_number(txm->TxNumber());
    prepare_log_rec->set_commit_timestamp(txm->commit_ts_);
    prepare_log_rec->clear_node_terms();
    auto &node_terms = *prepare_log_rec->mutable_node_terms();
    for (uint32_t nid = 0; nid < prepare_acquire_all_write_op_.upload_cnt_;
         ++nid)
    {
        node_terms[nid] =
            prepare_acquire_all_write_op_.hd_results_[nid].Value().node_term_;
    }

    // Set split-flush tx information
    ::txlog::SplitRangeOpMessage *prepare_split_msg =
        prepare_log_rec->mutable_log_content()->mutable_split_range_log();
    prepare_split_msg->set_table_name(range_table_name_.String());
    prepare_split_msg->set_stage(
        ::txlog::SplitRangeOpMessage_Stage_PrepareSplit);
    // Set range info for splitting range
    prepare_split_msg->set_partition_id(range_info_.partition_id_);
    prepare_split_msg->set_range_key_neg_inf(false);
    switch (old_start_key_->Type())
    {
    case KeyType::NegativeInf:
        prepare_split_msg->set_range_key_neg_inf(true);
        break;
    default:
        old_start_key_->Serialize(
            *prepare_split_msg->mutable_range_key_value());
        break;
    }
    for (auto &new_range : new_range_info_)
    {
        prepare_split_msg->add_new_partition_id(new_range.second);
        std::string new_range_key;
        new_range.first->Serialize(new_range_key);
        prepare_split_msg->add_new_range_key(new_range_key);
    }
}

void SplitFlushRangeOp::FillCommitLogRequest(TransactionExecution *txm)
{
    commit_log_op_.log_type_ = TxLogType::COMMIT;
    commit_log_op_.log_closure_.LogRequest().Clear();
    ::txlog::WriteLogRequest *commit_log_rec =
        commit_log_op_.log_closure_.LogRequest().mutable_write_log_request();

    commit_log_rec->set_tx_term(txm->tx_term_);
    commit_log_rec->set_txn_number(txm->TxNumber());
    commit_log_rec->set_commit_timestamp(txm->commit_ts_);
    auto commit_split_msg =
        commit_log_rec->mutable_log_content()->mutable_split_range_log();
    commit_split_msg->set_stage(::txlog::SplitRangeOpMessage_Stage_CommitSplit);

    // Fill the slice info
    LocalCcShards *shards = Sharder::Instance().GetLocalCcShards();
    StoreRange *old_range =
        shards->FindRange(table_name_, node_group_, *old_start_key_);
    auto &slices = old_range->Slices();
    auto slice_it = slices.begin();
    commit_split_msg->add_slice_sizes((*slice_it)->Size());
    slice_it++;
    for (; slice_it != slices.end(); slice_it++)
    {
        std::string slice_key;
        (*slice_it)->StartKey()->Serialize(slice_key);
        commit_split_msg->add_slice_keys(slice_key);
        commit_split_msg->add_slice_sizes((*slice_it)->Size());
    }

    // The prepare log keeps all cc nodes' terms and match them in the log
    // service to detect invalidated write intents. The commit log, however,
    // does not match terms in the log service.
    // If a cc node fails over, the new node must restore write intents
    // gained prior to the prepare log and then replay operations between
    // the prepare log and the commit log, which in this case upgrade write
    // intents to write locks. So, there is no need to check the liveness of
    // write locks when flushing the commit log.
    commit_log_rec->mutable_node_terms()->clear();
}

void SplitFlushRangeOp::FillCleanLogRequest(TransactionExecution *txm)
{
    clean_log_op_.log_type_ = TxLogType::CLEAN;

    clean_log_op_.log_closure_.LogRequest().Clear();

    ::txlog::WriteLogRequest *clean_log_rec =
        clean_log_op_.log_closure_.LogRequest().mutable_write_log_request();

    clean_log_rec->set_tx_term(txm->tx_term_);
    clean_log_rec->set_txn_number(txm->TxNumber());

    ::txlog::SplitRangeOpMessage *clean_split_msg =
        clean_log_rec->mutable_log_content()->mutable_split_range_log();

    clean_split_msg->set_stage(::txlog::SplitRangeOpMessage_Stage_CleanSplit);
    clean_log_rec->mutable_node_terms()->clear();
}
void SplitFlushRangeOp::ForceToFinish(TransactionExecution *txm)
{
    clean_log_op_.hd_result_.SetFinished();
    op_ = &clean_log_op_;
    Forward(txm);
}
ReleaseScanExtraLockOp::ReleaseScanExtraLockOp(TransactionExecution *txm)
    : hd_result_(txm)
{
}

void ReleaseScanExtraLockOp::Reset()
{
    hd_result_.Reset();
    hd_result_.Value().Clear();
}

void ReleaseScanExtraLockOp::Forward(TransactionExecution *txm)
{
    if (hd_result_.IsFinished())
    {
        txm->PostProcess(*this);
    }
    else if (txm->IsTimeOut())
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

        bool force_error = hd_result_.ForceError();
        if (force_error)
        {
            txm->PostProcess(*this);
        }
    }
}

AnalyzeTableAllOp::AnalyzeTableAllOp(TransactionExecution *txm)
    : hd_result_(txm)
{
    TX_TRACE_ASSOCIATE(this, &hd_result_);
}

void AnalyzeTableAllOp::Reset(uint32_t hres_ref_cnt)
{
    hd_result_.Reset();
    hd_result_.SetRefCnt(hres_ref_cnt);
}

void AnalyzeTableAllOp::Forward(TransactionExecution *txm)
{
    if (!is_running_)
    {
        txm->Process(*this);
        return;
    }

    if (hd_result_.IsFinished())
    {
        txm->PostProcess(*this);
    }
    else if (hd_result_.LocalRefCnt() == 0 && txm->IsTimeOut(600))
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

        bool force_error = hd_result_.ForceError();
        if (force_error)
        {
            txm->PostProcess(*this);
        }
    }
}

ObjectCommandOp::ObjectCommandOp(TransactionExecution *txm)
    : hd_result_(txm)
#ifdef RANGE_PARTITION_ENABLED
      ,
      lock_range_result_(txm)
#endif
{
    TX_TRACE_ASSOCIATE(this, &hd_result_);
}

void ObjectCommandOp::Reset(const TableName *table_name,
                            const TxKey *key,
                            const TxCommand *command,
                            TxCommandResult *cmd_result,
                            bool auto_commit)
{
    table_name_ = table_name;
    key_ = key;
    command_ = command;
    cmd_result_ = cmd_result;
    hd_result_.Reset();
    hd_result_.Value().Reset();
    auto_commit_ = auto_commit;
}

void ObjectCommandOp::Forward(TransactionExecution *txm)
{
    if (!is_running_)
    {
#ifdef RANGE_PARTITION_ENABLED
        // Just returned from LockReadRangeOp, check lock_range_result_.
        assert(lock_range_result_.IsFinished());
        if (lock_range_result_.IsError())
        {
            // There is an error when getting the input key's range. The
            // read operation is set to be errored.
            hd_result_.SetError(CcErrorCode::GET_RANGE_ID_ERR);

            bool force_success = hd_result_.ForceError();
            assert(force_success);

            txm->PostProcess(*this);
            return;
        }
#endif
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
        // For non-blocking concurrency control protocols, the object command is
        // expected to return instantly. For 2PL, if the request is blocked, the
        // cc node will send an acknowledgement to update the key's term. In
        // either case, if the object's term is not set, the tx has not received
        // any response or acknowledgement from the key's cc node group. The
        // request is forced to be errored upon timeout.
        hd_result_.ForceError();
        txm->PostProcess(*this);
    }
    else if (hd_result_.IsFinished())
    {
        if (hd_result_.ErrorCode() == CcErrorCode::REQUESTED_NODE_NOT_LEADER)
        {
            // The request was directed to a non-leader node. Updates the
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
}

ClusterScaleOp::ClusterScaleOp(
    ClusterScaleOpType event_type,
    std::vector<std::pair<std::string, uint16_t>> *new_nodes,
    std::vector<std::pair<std::string, uint16_t>> *removed_nodes,
    uint16_t *remove_node_count,
    std::mutex &prepare_log_mux,
    std::condition_variable &prepare_log_cv,
    bool &prepare_log_finished,
    CcErrorCode &err,
    TransactionExecution *txm)
    : CompositeTransactionOperation(),
      event_type_(event_type),
      prepare_log_mux_(&prepare_log_mux),
      prepare_log_cv_(&prepare_log_cv),
      prepare_log_finished_(&prepare_log_finished),
      err_(&err),
      prepare_log_op_(txm),
      update_cluster_configs_(txm),
      data_migration_op_(txm),
      clean_log_op_(txm),
      status_mux_(),
      status_(remote::ClusterScaleStatus::NOT_IN_PROGRESS)
{
    if (event_type == ClusterScaleOpType::AddNode)
    {
        delta_nodes_ = *new_nodes;
        new_ng_config_ = Sharder::Instance().AddNodeToCluster(delta_nodes_);
    }
    else if (event_type == ClusterScaleOpType::RemoveNode)
    {
        remove_node_count_ = *remove_node_count;
        new_ng_config_ = Sharder::Instance().RemoveNodeFromCluster(
            remove_node_count_, delta_nodes_);
        if (removed_nodes)
        {
            *removed_nodes = delta_nodes_;
        }
    }
    bucket_migrate_infos_ =
        Sharder::Instance().GetLocalCcShards()->GenerateBucketMigrationPlan(
            new_ng_config_, 9001);
}

void ClusterScaleOp::Reset(
    ClusterScaleOpType event_type,
    std::vector<std::pair<std::string, uint16_t>> *new_nodes,
    std::vector<std::pair<std::string, uint16_t>> *removed_nodes,
    uint16_t *remove_node_count,
    std::mutex &prepare_log_mux,
    std::condition_variable &prepare_log_cv,
    bool &prepare_log_finished,
    CcErrorCode &err,
    TransactionExecution *txm)
{
    op_ = nullptr;
    event_type_ = event_type;
    delta_nodes_.clear();
    remove_node_count_ = 0;
    if (event_type == ClusterScaleOpType::AddNode)
    {
        delta_nodes_ = *new_nodes;
        new_ng_config_ = Sharder::Instance().AddNodeToCluster(delta_nodes_);
    }
    else if (event_type == ClusterScaleOpType::RemoveNode)
    {
        remove_node_count_ = *remove_node_count;
        new_ng_config_ = Sharder::Instance().RemoveNodeFromCluster(
            remove_node_count_, delta_nodes_);
        if (removed_nodes)
        {
            *removed_nodes = delta_nodes_;
        }
    }
    prepare_log_mux_ = &prepare_log_mux;
    prepare_log_cv_ = &prepare_log_cv;
    prepare_log_finished_ = &prepare_log_finished;
    err_ = &err;
    status_ = remote::ClusterScaleStatus::NOT_IN_PROGRESS;
    prepare_log_op_.ResetHandlerTxm(txm);
    update_cluster_configs_.ResetHandlerTxm(txm);
    data_migration_op_.ResetHandlerTxm(txm);

    clean_log_op_.ResetHandlerTxm(txm);
    new_ng_config_.clear();
    bucket_migrate_infos_.clear();
    bucket_migrate_infos_ =
        Sharder::Instance().GetLocalCcShards()->GenerateBucketMigrationPlan(
            new_ng_config_, 9001);
}

void ClusterScaleOp::SetStatus(remote::ClusterScaleStatus status)
{
    std::unique_lock<std::mutex> lock(status_mux_);
    status_ = status;
}

remote::ClusterScaleStatus ClusterScaleOp::GetStatus()
{
    std::unique_lock<std::mutex> lock(status_mux_);
    return status_;
}

void ClusterScaleOp::Forward(TransactionExecution *txm)
{
    if (op_ == nullptr)
    {
        op_ = &prepare_log_op_;
        FillPrepareLogRequest(txm);
        LOG(INFO) << "Cluster Scale transaction write prepare log, txn: "
                  << txm->TxNumber();
        ForwardToSubOperation(txm, &prepare_log_op_);
    }
    else if (op_ == &prepare_log_op_)
    {
        if (!CheckLeaderTerm(txm->TxCcNodeId(), txm->tx_term_, txm->tx_status_))
        {
            // Failed before write log succeed due to leader transfer. Notify
            // caller.
            {
                std::unique_lock<std::mutex> lock(*prepare_log_mux_);
                *err_ = CcErrorCode::TX_NODE_NOT_LEADER;
                *prepare_log_finished_ = true;
                prepare_log_cv_->notify_all();
            }
            ForceToFinish(txm);
            return;
        }
        if (prepare_log_op_.hd_result_.IsError())
        {
            if (prepare_log_op_.hd_result_.ErrorCode() ==
                CcErrorCode::LOG_CLOSURE_RESULT_UNKNOWN_ERR)
            {
                // prepare log result unknown, keep retrying until getting a
                // clear response, either success or failure, or the
                // coordinator itself is no longer leader
                int64_t tx_node_term =
                    Sharder::Instance().LeaderTerm(txm->TxCcNodeId());
                if (tx_node_term > 0)
                {
                    DLOG(WARNING)
                        << "Cluster scale write prepare log result unknown, "
                           "tx_number:"
                        << txm->TxNumber() << ", keep retrying";
                    // set retry flag and retry prepare log
                    ::txlog::WriteLogRequest *log_req =
                        prepare_log_op_.log_closure_.LogRequest()
                            .mutable_write_log_request();
                    log_req->set_retry(true);
                    RetrySubOperation(txm, &prepare_log_op_);
                }
                else
                {
                    DLOG(ERROR) << "Cluster scale write prepare log result "
                                   "unknown, tx_number:"
                                << txm->TxNumber()
                                << ", not leader any more, stop retrying";
                    // Not leader anymore, just quit. New leader will know
                    // whether prepare log succeeds and continue the rest if
                    // it does. Caller need to query new leader of node group
                    // to know if write log has succeeded.
                    {
                        std::unique_lock<std::mutex> lock(*prepare_log_mux_);
                        *err_ = CcErrorCode::LOG_CLOSURE_RESULT_UNKNOWN_ERR;
                        *prepare_log_finished_ = true;
                        prepare_log_cv_->notify_all();
                    }
                    ForceToFinish(txm);
                }
            }
            else
            {
                // Notify called that the operation has failed
                {
                    std::unique_lock<std::mutex> lock(*prepare_log_mux_);
                    *err_ = CcErrorCode::WRITE_LOG_FAILED;
                    *prepare_log_finished_ = true;
                    prepare_log_cv_->notify_all();
                }
                ForceToFinish(txm);
            }
            return;
        }

        // Notify caller that log has been written.
        {
            std::unique_lock<std::mutex> lock(*prepare_log_mux_);
            *err_ = CcErrorCode::NO_ERROR;
            *prepare_log_finished_ = true;
            prepare_log_cv_->notify_all();
        }

        if (event_type_ == ClusterScaleOpType::AddNode)
        {
            // If we're adding new nodes, connect to new nodes first
            // before starting migration.
            ForwardToSubOperation(txm, &update_cluster_configs_);
        }
        else
        {
            // For remove nodes, just start migration right away. We will
            // update cluster config and remove nodes when migration is done.
            ForwardToSubOperation(txm, &data_migration_op_);
        }
    }
}

void ClusterScaleOp::ForceToFinish(TransactionExecution *txm)
{
    clean_log_op_.hd_result_.SetFinished();
    op_ = &clean_log_op_;
    Forward(txm);
}

void ClusterScaleOp::FillPrepareLogRequest(TransactionExecution *txm)
{
    prepare_log_op_.log_type_ = TxLogType::PREPARE;

    prepare_log_op_.log_closure_.LogRequest().Clear();

    ::txlog::WriteLogRequest *prepare_log_rec =
        prepare_log_op_.log_closure_.LogRequest().mutable_write_log_request();

    prepare_log_rec->set_tx_term(txm->tx_term_);
    prepare_log_rec->set_txn_number(txm->TxNumber());

    // TODO{liunyl}: commit ts should not matter with cluster scale, need double
    // check
    prepare_log_rec->set_commit_timestamp(txm->commit_ts_);
    ::txlog::ClusterScaleOpMessage *cluster_scale_msg =
        prepare_log_rec->mutable_log_content()->mutable_cluster_scale_log();
    switch (event_type_)
    {
    case ClusterScaleOpType::AddNode:
        cluster_scale_msg->set_event_type(
            ::txlog::ClusterScaleOpMessage_ScaleOpType_AddNode);
        break;
    case ClusterScaleOpType::RemoveNode:
        cluster_scale_msg->set_event_type(
            ::txlog::ClusterScaleOpMessage_ScaleOpType_RemoveNode);
        break;
    default:
        assert(false);
    }
    cluster_scale_msg->set_stage(
        ::txlog::ClusterScaleOpMessage_Stage_PrepareScale);
    for (auto conf_pair : new_ng_config_)
    {
        ::txlog::NodegroupConfig *ng_conf =
            cluster_scale_msg->add_new_ng_configs();
        ng_conf->set_ng_id(conf_pair.first);
        for (auto &node : conf_pair.second)
        {
            ng_conf->add_member_nodes(node.node_id_);
        }
        ::txlog::NodeConfig *node_conf = cluster_scale_msg->add_node_configs();
        node_conf->set_node_id(conf_pair.second[0].node_id_);
        node_conf->set_host_name(conf_pair.second[0].host_name_);
        node_conf->set_port(conf_pair.second[0].port_);
    }

    // Fill data migration plan
    for (auto &bucket_plan : bucket_migrate_infos_)
    {
        auto migrate_process = cluster_scale_msg->add_migrate_process();
        migrate_process->set_bucket_id(bucket_plan.first);
        migrate_process->set_old_owner(bucket_plan.second.orig_owner_);
        migrate_process->set_new_owner(bucket_plan.second.new_owner_);
        migrate_process->set_stage(
            txlog::BucketMigrateMessage_Stage_NotStarted);
        // This will set when the real migrate starts.
        migrate_process->set_migrate_ts(0);
    }
}
}  // namespace txservice
