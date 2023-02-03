#include "tx_operation.h"

#include <algorithm>
#include <iostream>
#include <string>

#include "../log_service/include/log_type.h"
#include "cc/cc_handler_result.h"
#include "error_messages.h"  //CcErrorCode
#include "fault/fault_inject.h"
#include "local_cc_shards.h"
#include "range_record.h"
#include "sharder.h"
#include "store/data_store_handler.h"
#include "tx_execution.h"
#include "tx_request.h"
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
      lock_range_result_(txm),
      unlock_range_result_(txm)
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
    lock_range_result_.Reset();
    unlock_range_result_.Reset();
#endif
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

        if (lock_range_result_.IsFinished())
        {
            if (lock_range_result_.IsError())
            {
                // There is an error when getting the input key's range. The
                // read operation is set to be errored.
                hd_result_.SetError(CcErrorCode::GET_RANGE_ID_ERR);
            }
            else
            {
                if (iso_level_ >= IsolationLevel::RepeatableRead)
                {
                    // For isolation levels greater than or equal to Repeatable
                    // Read, keeps the read lock on the range because there will
                    // be a post read on the key in this range. The range cannot
                    // be changed before this tx finishes post-processing.
                    const ReadKeyResult &read_res = lock_range_result_.Value();
                    txm->rw_set_.AddRead(read_res.cce_addr_,
                                         read_res.ts_,
                                         CcProtocol::Locking,
                                         LockType::ReadLock,
                                         &range_table_name_);
                }

                txm->Process(*this);
            }
        }
        else
        {
            // The get-range request has not finished. The read operation cannot
            // proceed without knowing the input key's range.
            return;
        }
#else
        txm->Process(*this);
#endif
    }

    const CcEntryAddr &cce_addr = hd_result_.Value().cce_addr_;

    if (hd_result_.IsFinished())
    {
        if (hd_result_.ErrorCode() == CcErrorCode::REQUESTED_NODE_NOT_LEADER &&
            retry_num_ >= 0)
        {
            // The read request was directed to a non-leader node. Updates
            // the leader cache. Sine UpdateLeader() is a sync call, we only
            // do it when re-run the operation fails.
            if (retry_num_ == 0)
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

#ifdef RANGE_PARTITION_ENABLED
        if (!read_tx_req_->read_local_ &&
            iso_level_ < IsolationLevel::RepeatableRead)
        {
            if (lock_range_result_.IsFinished())
            {
                unlock_range_result_.Reset();
                txm->handler->PostRead(txm->TxNumber(),
                                       txm->TxTerm(),
                                       txm->CommandId(),
                                       0,
                                       0,
                                       0,
                                       lock_range_result_.Value().cce_addr_,
                                       unlock_range_result_,
                                       CcProtocol::Locking,
                                       LockType::ReadLock);

                // After the unlock range request is sent,
                // lock_range_result_ is reset, so that when the tx machine
                // is re-executed, its status is unfinished, indicating that
                // the read operation has finished and is waiting for the
                // response of unlocking the range.
                lock_range_result_.Reset();
            }
            else if (unlock_range_result_.IsFinished())
            {
                txm->PostProcess(*this);
            }
        }
        else
        {
            txm->PostProcess(*this);
        }
#else
        txm->PostProcess(*this);
#endif
    }
    else if (cce_addr.Term() < 0 && txm->IsTimeOut() ||
             !Sharder::Instance().CheckLeaderTerm(txm->TxCcNodeId(),
                                                  txm->TxTerm()))
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
        // For non-blocking concurrency control protocols, the read request
        // is expected to return instantly. For lock-based protocols, if the
        // read request is blocked, the cc node will send an acknowledgement to
        // update the key's term. In either case, if the read key's term is not
        // set, the tx has not received any response or acknowledgement from the
        // key's cc node group. The read request is forced to be errored upon
        // timeout.
        // FIXME(lzx): Is it more appropriate to retry?
        // If the tx node fails, also force the tx to abort instantly.
        bool force_success = hd_result_.ForceError();
        if (force_success)
        {
            txm->PostProcess(*this);
        }
        // If forcing error fails, it means that the remote response returns
        // normally and the tx has been moved from the waiting queue to the
        // execution queue. Does not continue execution. The tx will be
        // re-executed when the tx processor visits it in the execution
        // queue.
    }
    // TODO: for locking-based protocols, even though the tx may be blocked
    // arbitrarily long after the read request is acknowledged, we still
    // need to periodically check liveness of the remote node and force the
    // tx to cancel if the remote node is unresponsive.
}

PostReadOperation::PostReadOperation(TransactionExecution *txm)
    : hd_result_(txm)
{
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
}

void AcquireWriteOperation::Reset()
{
    std::vector<AcquireKeyResult> &acquire_key_vec = hd_result_.Value();
    if (acquire_key_vec.capacity() > TransactionExecution::LargeTxKeySize)
    {
        acquire_key_vec.resize(16);
        acquire_key_vec.shrink_to_fit();
    }
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
            write_entry.cce_addr_.SetCce(0, -1);
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
    else if (remote_ack_cnt_.load(std::memory_order_acquire) > 0 &&
                 txm->IsTimeOut() ||
             !Sharder::Instance().CheckLeaderTerm(txm->TxCcNodeId(),
                                                  txm->TxTerm()))
    {
        // FIXME(lzx): Is it more appropriate to retry if remote_ack_cnt_>0 ?
        // If the tx node fails, force the tx to abort instantly.
        // TODO: for 2PL, the tx may be blocked arbitrarily long, even after all
        // acquire requests are acknowledged. We still need to periodically
        // check liveness of the remote node.
        bool success = hd_result_.ForceError();
        if (success)
        {
            AggregateAcquiredKeys(txm);
            txm->PostProcess(*this);
        }
        // Else, all acquire-write requests finish normally. The tx must
        // have been moved from the waiting queue to the execution queue.
        // Does not forword the tx now, as it will be re-executed when the
        // tx processor visits the execution queue.
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

    uint32_t range_id = range_rec_.GetRangeInfo()->partition_id_;
    // Updates the sharding codes of the write-set keys belonging to this
    // range. The higher 22 bits represent the range ID.
    while (write_key_it_ != next_range_start)
    {
        WriteSetEntry &write_entry = write_key_it_->second;
        size_t hash = write_entry.key_->Hash();
        write_entry.key_shard_code_ = (range_id << 10) | (hash & 0x3FF);

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
        // For DML transactions, the coordinator must keep retrying the WriteLog
        // request until getting a clear response, either success or failure, or
        // the coordinator itself is no longer leader. In the last case, the
        // committing process interrupts with an unknown result, and an error
        // message "Log service is unreachable, transaction status is unknown"
        // is returned. For these result unknown txns, The coordinator must skip
        // the PostProcess and the locks on participants remain. The
        // participants ccnodes will do the PostProcess individually via orphan
        // lock recovery mechanism.
        if (hd_result_.ErrorCode() ==
                CcErrorCode::LOG_CLOSURE_RESULT_UNKOWN_ERR &&
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
                << txm->tx_number_;
            // log request return unknown status, we need to set retry flag to
            // inform log service that this is a retried request
            ::txlog::LogRequest &log_req = log_closure_.LogRequest();
            ::txlog::WriteLogRequest *log_rec =
                log_req.mutable_write_log_request();
            log_rec->set_retry(true);
            // ReRunOp sleep for 2 seconds
            retry_num_ = 4;

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
    : hd_result_(txm), catalog_hd_result_(txm)
{
}

void PostProcessOp::Reset(size_t write_cnt,
                          size_t data_read_cnt,
                          size_t catalog_read_cnt)
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

    catalog_hd_result_.Reset();
    catalog_hd_result_.Value().Clear();

    if (catalog_read_cnt == 0)
    {
        catalog_hd_result_.SetFinished();
    }
    else
    {
        catalog_hd_result_.SetRefCnt(catalog_read_cnt);
    }
}

void PostProcessOp::Forward(TransactionExecution *txm)
{
    if (hd_result_.IsFinished())
    {
        if (catalog_hd_result_.IsFinished())
        {
            txm->PostProcess(*this);
        }
        else if (!is_running_)
        {
            is_running_ = true;
            txm->ReleaseCatalogLock(catalog_hd_result_);
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

        bool force_error = hd_result_.ForceError();
        if (force_error)
        {
            txm->ReleaseCatalogLock(catalog_hd_result_);
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
    hd_result_.Reset();
    alias_ = 0;
    scan_state_ = nullptr;
#ifdef RANGE_PARTITION_ENABLED
    slice_hd_result_.Reset();
    unlock_range_result_.Reset();
    lock_range_result_.Reset();
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
                // There is an error when getting the next range's lock and ID.
                // The scan next operation is set to be errored.
                hd_result_.SetError(CcErrorCode::GET_RANGE_ID_ERR);
                unlock_range_result_.SetFinished();
            }
            else
            {
                const ReadKeyResult &read_res = lock_range_result_.Value();

                if (txm->iso_level_ >= IsolationLevel::RepeatableRead)
                {
                    // For isolation levels greater than or equal to Repeatable
                    // Read, keeps the read lock on the range because there will
                    // be a post read on the key in this range. The range cannot
                    // be split or merged before the tx finishes
                    // post-processing.
                    txm->rw_set_.AddRead(read_res.cce_addr_,
                                         read_res.ts_,
                                         CcProtocol::Locking,
                                         LockType::ReadLock,
                                         &range_table_name_);
                }

                scan_state_->range_cce_addr_ = read_res.cce_addr_;
                scan_state_->range_id_ =
                    range_rec_.GetRangeInfo()->partition_id_;
                txm->Process(*this);
                return;
            }
        }
#endif
    }

    if (scanner.Type() == CcmScannerType::HashPartition &&
        hd_result_.IsFinished())
    {
        // Error code -1 indicates send message failed or term changed.
        if (hd_result_.ErrorCode() == CcErrorCode::REQUESTED_NODE_NOT_LEADER)
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

        scanner.SetStatus(ScannerStatus::Open);

        txm->PostProcess(*this);
    }
#ifdef RANGE_PARTITION_ENABLED
    else if (scanner.Type() == CcmScannerType::RangePartition &&
             slice_hd_result_.IsFinished())
    {
        // Error code -1 indicates send message failed or term changed.
        if (hd_result_.ErrorCode() == CcErrorCode::REQUESTED_NODE_NOT_LEADER)
        {
            if (retry_num_ == 0)
            {
                // Sharder::Instance().UpdateLeader(
                //     hd_result_.Value().node_group_id_);
            }
            else if (retry_num_ > 0)
            {
                ReRunOp(txm);
                return;
            }
        }

        if (scanner.Status() == ScannerStatus::Blocked)
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
                    txm->handler->PostRead(txm->TxNumber(),
                                           txm->TxTerm(),
                                           txm->CommandId(),
                                           0,
                                           0,
                                           0,
                                           scan_state_->range_cce_addr_,
                                           unlock_range_result_,
                                           CcProtocol::Locking,
                                           LockType::ReadLock);

                    // After the unlock range request is sent,
                    // lock_range_result_ is reset. When the tx machine is
                    // re-executed, its status is unfinished, indicating that
                    // the scan next operation is waiting for the response of
                    // unlocking the current range.
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

            if (scanner.Current() == nullptr &&
                ((scanner.Direction() == ScanDirection::Forward &&
                  scan_state_->slice_position_ != SlicePosition::LastSlice) ||
                 (scanner.Direction() == ScanDirection::Backward &&
                  scan_state_->slice_position_ != SlicePosition::FirstSlice)))
            {
                // Scan next batch in range partition scans a slice at a time.
                // Keep scanning until we reach the last slice in last range or
                // we get something from the last slice scanned.
                slice_hd_result_.Reset();
                txm->Process(*this);
                return;
            }
        }

        txm->PostProcess(*this);
    }
#endif
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

        bool force_success = hd_result_.ForceError();
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
    else if (txm->IsTimeOut(2))
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
        txm->PostProcess(*this);
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
                << txm->tx_number_;
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
                CcErrorCode::LOG_CLOSURE_RESULT_UNKOWN_ERR)
            {
                // prepare log result unknown, keep retrying until getting a
                // clear response, either success or failure, or the coordinator
                // itself is no longer leader
                int64_t tx_node_term =
                    Sharder::Instance().LeaderTerm(txm->TxCcNodeId());
                if (tx_node_term > 0)
                {
                    DLOG(WARNING)
                        << "Upsert table write prepare log result unknown, "
                           "tx_number:"
                        << txm->tx_number_ << ", keep retrying";
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
                                << txm->tx_number_
                                << ", not leader any more, stop retrying";
                    // Not leader anymore, just quit. New leader will know
                    // whether prepare log succeeds and continue the rest if it
                    // does. Should not release the write intents. If prepare
                    // log is not written, the write intents will be released
                    // individually via orphan lock recovery mechanism.
                    txm->bool_resp_->SetErrorCode(
                        TxErrorCode::LOG_SERVICE_UNREACHABLE);

                    txm->bool_resp_->Finish(false);
                    txm->state_stack_.pop_back();
                    assert(txm->state_stack_.empty());
                    txm->handler->table_schema_op_pool_.emplace_back(
                        std::move(txm->schema_op_));
                }
            }
            else
            {
                DLOG(ERROR)
                    << "Upsert table write prepare log failed, tx_number:"
                    << txm->tx_number_;
                // Fails to flush the prepare log. The schema operation is
                // considered failed if the prepare log is not flushed. The
                // commit ts is set to 0 to signal that the following post write
                // operation releases all write intents.
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
        bool failed = post_all_intent_op_.hd_result_.IsError();

        if (failed)
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
            // to succeed and can only roll forward. Retry this step to
            // install the dirty schema in the tx service, if the tx node is
            // still the leader. The tx is also allowed to proceed if the tx
            // is in the recovery mode and the tx node is a leader
            // candidate.

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
                // Retry 5 times before issue flush schema error.
                if (retry_num_ > 0)
                {
                    txm->PushOperation(&upsert_kv_table_op_);
                    txm->Process(upsert_kv_table_op_);
                    retry_num_--;
                }
                else if (retry_num_ == 0)
                {
                    DLOG(ERROR) << "flush schema error: can not create table "
                                   "in kv store";

                    if (txm->tx_status_ != TxnStatus::Recovering)
                    {
                        txm->bool_resp_->SetErrorCode(
                            TxErrorCode::DATA_STORE_WRITE_ERR);
                    }
                    // Set txm->commit_ts_ to 0 to indicate there is a flush
                    // error during upsert_kv_table_op_.
                    txm->commit_ts_ = tx_op_failed_ts_;
                    op_ = &acquire_all_lock_op_;
                    txm->PushOperation(&acquire_all_lock_op_);
                    txm->Process(acquire_all_lock_op_);
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
                // could be recovering from commit stage, in which case we have
                // skipped post_all_intent_op_ and the schema in catalog_rec_
                // would be empty.
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
        bool failed = post_all_lock_op_.hd_result_.IsError();

        if (txm->commit_ts_ == tx_op_failed_ts_ &&
            post_all_lock_op_.write_type_ == PostWriteType::PrepareCommit)
        {
            // The schema operation failed without flushing the prepare log.
            // Do not retry post-processing (release write intents) even if
            // it fails. Remaining write intents on the schema, if there are
            // any, will be recovered by individual cc nodes separately.
            txm->bool_resp_->Finish(false);

            txm->state_stack_.pop_back();
            assert(txm->state_stack_.empty());
            txm->handler->table_schema_op_pool_.emplace_back(
                std::move(txm->schema_op_));
        }
        else if (failed)
        {
            // When a cc node leader begins recovery, the candidate term is
            // set to the Raft term. When recovery finishes, the candidate
            // term is set to -1 after the leader term. So, obtains the
            // candidate term before the leader term.
            int64_t tx_node_candid_term =
                Sharder::Instance().CandidateLeaderTerm(txm->TxCcNodeId());
            int64_t tx_node_term =
                Sharder::Instance().LeaderTerm(txm->TxCcNodeId());

            // After the prepare log is flushed, if flush kv succeeds, the
            // schema op is guaranteed to succeed and can only roll forward.
            // Retry this step to install the committed schema and remove write
            // locks, if the tx node is still the leader or the tx is in the
            // recovery mode and the cc node is a leader candidate. However, if
            // flush kv fails, this schema op has already been rolled back while
            // processing this post_all_lock_op_, so here we only need to
            // ForceToFinish.
            if ((tx_node_term >= 0 ||
                 (txm->tx_status_ == TxnStatus::Recovering &&
                  tx_node_candid_term >= 0)) &&
                txm->commit_ts_ != tx_op_failed_ts_)
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

            txm->handler->table_schema_op_pool_.emplace_back(
                std::move(txm->schema_op_));
            txm->Reset();
            // Setting the tx's status to finished signals that this tx
            // state machine can be recycled for a new tx.
            txm->tx_status_.store(TxnStatus::Finished,
                                  std::memory_order_release);
        }
        else
        {
            if (txm->commit_ts_ == tx_op_failed_ts_ &&
                post_all_lock_op_.write_type_ == PostWriteType::PostCommit)
            {
                // Flush kv error.
                txm->bool_resp_->Finish(false);
            }
            else
            {
                assert(txm->commit_ts_ > 0 && post_all_lock_op_.write_type_ ==
                                                  PostWriteType::PostCommit);
                txm->bool_resp_->Finish(true);
            }

            txm->state_stack_.pop_back();
            assert(txm->state_stack_.empty());
            txm->handler->table_schema_op_pool_.emplace_back(
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
    acquire_all_intent_op_.protocol_ = CcProtocol::OccRead;

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
    prepare_log_rec->set_txn_number(txm->tx_number_);
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
    commit_log_rec->set_txn_number(txm->tx_number_);

    ::txlog::SchemaOpMessage *commit_schema_msg =
        commit_log_rec->mutable_log_content()->mutable_schema_log();

    if (this->upsert_kv_table_op_.hd_result_.IsError())
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
DsOp<ResultType>::DsOp(TransactionExecution *txm) : hd_result_(txm)
{
    TX_TRACE_ASSOCIATE(this, &hd_result_);
}

template <typename ResultType>
void DsOp<ResultType>::Forward(TransactionExecution *txm)
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
        // Not necessary to have timeout for ds operation, you don't estimate
        // the proper op time out secs, and the dsop will finished anyway

        // bool succ = hd_result_.ForceError();
        // if (succ)
        //{
        // txm->PostProcess(*this);
        //}
    }
}

template <typename ResultType>
void DsOp<ResultType>::Reset()
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

CkptScanOp::CkptScanOp(TransactionExecution *txm) : hd_result_(txm)
{
    TX_TRACE_ASSOCIATE(this, &hd_result_);
}

CkptScanOp::CkptScanOp(const TableName &table_name,
                       uint64_t ckpt_ts,
                       NodeGroupId node_group,
                       std::vector<FlushRecord> *ckpt_vec,
                       std::vector<FlushRecord> *archive_vec,
                       std::vector<const TxKey *> *mv_vec,
                       TransactionExecution *txm,
                       const TxKey *start_key,
                       const TxKey *end_key)
    : tab_name_(&table_name),
      ckpt_ts_(ckpt_ts),
      node_group_(node_group),
      ckpt_vec_(ckpt_vec),
      archive_vec_(archive_vec),
      mv_vec_(mv_vec),
      start_key_(start_key),
      end_key_(end_key),
      hd_result_(txm)
{
    TX_TRACE_ASSOCIATE(this, &hd_result_);
}

void CkptScanOp::Forward(TransactionExecution *txm)
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
        hd_result_.ForceError();
        txm->PostProcess(*this);
    }
}

void CkptScanOp::Reset()
{
    hd_result_.Reset();
}

FlushDataOp::FlushDataOp(TransactionExecution *txm) : hd_result_(txm)
{
    TX_TRACE_ASSOCIATE(this, &hd_result_);
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
    // TODO{liunyl} : figure out how to handle timeout. Flush data
    // can take a long time to finish.
    // potential solution: check flush data worker heartbeat
}

void FlushDataOp::Reset()
{
    hd_result_.Reset();
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
      table_name_(table_name),
      range_table_name_(table_name.StringView(), TableType::RangePartition),
      node_group_(node_group),
      range_info_(*old_range_info),
      range_record_(&range_info_, old_end_key),
      old_end_key_(old_end_key),
      new_range_info_(std::move(new_range_info)),
      prepare_acquire_all_write_op_(txm),
      prepare_log_op_(txm),
      install_new_range_op_(txm),
      ds_migrate_old_partition_op_(txm),
      ckpt_scan_op_(txm),
      flush_op_(txm),
      commit_acquire_all_write_op_(txm),
      commit_log_op_(txm),
      ds_upsert_range_op_(txm),
      post_all_lock_op_(txm),
      ds_clean_old_range_op_(txm),
      clean_log_op_(txm)

{
    old_start_key_ = range_info_.start_key_ != nullptr
                         ? range_info_.start_key_.get()
                         : old_start_key;
    prepare_acquire_all_write_op_.table_name_ = &range_table_name_;
    prepare_acquire_all_write_op_.cc_op_ = CcOperation::Write;
    prepare_acquire_all_write_op_.protocol_ = CcProtocol::OCC;
    prepare_acquire_all_write_op_.key_ = old_start_key_;

    install_new_range_op_.table_name_ = &range_table_name_;
    install_new_range_op_.write_type_ = PostWriteType::PrepareCommit;
    install_new_range_op_.op_type_ = OperationType::Update;
    install_new_range_op_.key_ = old_start_key_;

    ckpt_scan_op_.tab_name_ = &table_name_;
    ckpt_scan_op_.start_key_ = old_start_key_;
    ckpt_scan_op_.end_key_ = old_end_key_;
    ckpt_scan_op_.ckpt_vec_ = &ckpt_vec_;
    ckpt_scan_op_.archive_vec_ = &archive_vec_;
    ckpt_scan_op_.mv_vec_ = &mv_base_vec_;
    ckpt_scan_op_.node_group_ = node_group_;

    flush_op_.tab_name_ = &table_name_;
    flush_op_.ckpt_vec_ = &ckpt_vec_;
    flush_op_.archive_vec_ = &archive_vec_;
    flush_op_.mv_vec_ = &mv_base_vec_;
    flush_op_.schema_ = table_schema_;
    flush_op_.node_group_ = node_group_;

    commit_acquire_all_write_op_.table_name_ = &range_table_name_;
    commit_acquire_all_write_op_.cc_op_ = CcOperation::Write;
    commit_acquire_all_write_op_.protocol_ = CcProtocol::Locking;
    commit_acquire_all_write_op_.key_ = old_start_key_;

    post_all_lock_op_.table_name_ = &range_table_name_;
    post_all_lock_op_.write_type_ = PostWriteType::PostCommit;
    post_all_lock_op_.op_type_ = OperationType::Update;
    post_all_lock_op_.key_ = old_start_key_;

    TX_TRACE_ASSOCIATE(
        this, &prepare_acquire_all_write_op_, "prepare_acquire_all_op_");
    TX_TRACE_ASSOCIATE(this, &prepare_log_op_, "prepare_log_op_");
    TX_TRACE_ASSOCIATE(this, &install_new_range_op_, "install_new_range_op_");
    TX_TRACE_ASSOCIATE(this, &ckpt_scan_op_, "ckpt_scan_op_");
    TX_TRACE_ASSOCIATE(this, &flush_op_, "flush_op_");
    TX_TRACE_ASSOCIATE(
        this, &commit_acquire_all_write_op_, "commit_acquire_all_op_");
    TX_TRACE_ASSOCIATE(this, &commit_log_op_, "commit_log_op_");
    TX_TRACE_ASSOCIATE(this, &ds_upsert_range_op_, "ds_upsert_range_op_");
    TX_TRACE_ASSOCIATE(this, &post_all_lock_op_, "post_all_lock_op_");
    TX_TRACE_ASSOCIATE(this, &ds_clean_old_range_op_, "ds_clean_old_range_op_");
    TX_TRACE_ASSOCIATE(this, &clean_log_op_, "clean_log_op_");
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

        // Acquire Write lock on range entry on all node groups and calculate
        // commit ts for tx.
        ForwardToSubOperation(txm, &prepare_acquire_all_write_op_);
    }
    else if (op_ == &prepare_acquire_all_write_op_)
    {
        if (!CheckLeaderTerm(node_group_, txm->tx_term_, txm->tx_status_))
        {
            Sharder::Instance().UnpinNodeGroupData(node_group_);
            ForceToFinish(txm);
            return;
        }
        if (prepare_acquire_all_write_op_.fail_cnt_.load(
                std::memory_order_relaxed) > 0)
        {
            LOG(ERROR) << "Split Flush transaction failed to obtain write "
                          "lock, tx_number:"
                       << txm->tx_number_;

            // Set commit ts to 0 to indicate transaction failure.
            // post_all_lock_op_ will release locks acquired.
            txm->commit_ts_ = tx_op_failed_ts_;
            ForwardToSubOperation(txm, &post_all_lock_op_);
            return;
        }

        // Update the commit ts. The commit ts is the max value between
        // 1) max commit ts of last tx that updated records in this range
        // partition on all node groups, 2) local timestamp when tx started.
        // The commit ts is used to decide whether a record needs to be flushed
        // during the next phase. We want to make sure every records modified
        // before commit ts is flushed to new partition in KV store.
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
        ForwardToSubOperation(txm, &prepare_log_op_);
    }
    else if (op_ == &prepare_log_op_)
    {
        if (!CheckLeaderTerm(node_group_, txm->tx_term_, txm->tx_status_))
        {
            Sharder::Instance().UnpinNodeGroupData(node_group_);
            ForceToFinish(txm);
            return;
        }
        if (prepare_log_op_.hd_result_.IsError())
        {
            LOG(ERROR) << "Split Flush transaction failed to write prepare "
                          "log, tx number "
                       << txm->tx_number_;
            // Set commit ts to 0 to indicate transaction failure.
            // post_all_lock_op_ will release locks acquired.
            txm->commit_ts_ = tx_op_failed_ts_;
            ForwardToSubOperation(txm, &post_all_lock_op_);
        }

        // Fill in new range info to old range record.
        range_info_.dirty_ts_ = txm->commit_ts_;
        for (auto &range_info : new_range_info_)
        {
            range_info_.new_partition_id_.push_back(range_info.second);
            range_info_.new_key_.push_back(range_info.first->Clone());
        }

        // Install dirty range info on all node groups and downgrade to write
        // intent lock in next subop.
        install_new_range_op_.rec_ = &range_record_;
        ForwardToSubOperation(txm, &install_new_range_op_);
    }
    else if (op_ == &install_new_range_op_)
    {
        if (!CheckLeaderTerm(node_group_, txm->tx_term_, txm->tx_status_))
        {
            Sharder::Instance().UnpinNodeGroupData(node_group_);
            ForceToFinish(txm);
            return;
        }
        if (install_new_range_op_.hd_result_.IsError())
        {
            LOG(ERROR) << "Split Flush transaction failed to install new range "
                          "info, tx number "
                       << txm->tx_number_;
            // Set commit ts to 0 to indicate transaction failure.
            // post_all_lock_op_ will release locks acquired.
            txm->commit_ts_ = tx_op_failed_ts_;
            ForwardToSubOperation(txm, &post_all_lock_op_);
        }

        // Copy data from old partition to its new partition in data store.
        ds_migrate_old_partition_op_.op_func_ =
            [&table_name = table_name_,
             old_partition_id = range_info_.partition_id_,
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
                 &new_partition_info,
                 tx_ts,
                 table_schema,
                 &hd_res,
                 store_hd]
                {
                    bool succ = store_hd->CopyRangeData(table_name,
                                                        old_partition_id,
                                                        new_partition_info,
                                                        tx_ts,
                                                        table_schema);
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

        ForwardToSubOperation(txm, &ds_migrate_old_partition_op_);
    }
    else if (op_ == &ds_migrate_old_partition_op_)
    {
        if (!CheckLeaderTerm(node_group_, txm->tx_term_, txm->tx_status_))
        {
            Sharder::Instance().UnpinNodeGroupData(node_group_);
            ForceToFinish(txm);
            return;
        }
        if (ds_migrate_old_partition_op_.hd_result_.IsError())
        {
            LOG(ERROR) << "Split Flush transaction failed to migrate old "
                          "partition, tx number "
                       << txm->tx_number_;
            // Set commit ts to 0 to indicate transaction failure.
            // post_all_lock_op_ will release locks acquired.
            txm->commit_ts_ = tx_op_failed_ts_;
            ForwardToSubOperation(txm, &post_all_lock_op_);
        }
        ckpt_scan_op_.ckpt_ts_ = txm->commit_ts_;
        ForwardToSubOperation(txm, &ckpt_scan_op_);
    }
    else if (op_ == &ckpt_scan_op_)
    {
        if (!CheckLeaderTerm(node_group_, txm->tx_term_, txm->tx_status_))
        {
            Sharder::Instance().UnpinNodeGroupData(node_group_);
            ForceToFinish(txm);
            return;
        }
        if (ckpt_scan_op_.hd_result_.IsError())
        {
            LOG(ERROR) << "Split Flush transaction failed to scan for "
                          "checkpoint, tx number "
                       << txm->tx_number_;
            // Set commit ts to 0 to indicate transaction failure.
            // post_all_lock_op_ will release locks acquired.
            txm->commit_ts_ = tx_op_failed_ts_;
            ForwardToSubOperation(txm, &post_all_lock_op_);
        }
        flush_op_.ckpt_ts_ = txm->commit_ts_;
        flush_op_.tx_term_ = txm->tx_term_;
        ForwardToSubOperation(txm, &flush_op_);
    }
    else if (op_ == &flush_op_)
    {
        if (!CheckLeaderTerm(node_group_, txm->tx_term_, txm->tx_status_))
        {
            Sharder::Instance().UnpinNodeGroupData(node_group_);
            ForceToFinish(txm);
            return;
        }
        if (flush_op_.hd_result_.IsError())
        {
            LOG(ERROR)
                << "Split Flush transaction failed to flush data, tx number "
                << txm->tx_number_;

            // Set commit ts to 0 to indicate transaction failure.
            // post_all_lock_op_ will release locks acquired.
            txm->commit_ts_ = tx_op_failed_ts_;
            ForwardToSubOperation(txm, &post_all_lock_op_);
        }
        // Upgrade to write lock again for commit phase.
        ForwardToSubOperation(txm, &commit_acquire_all_write_op_);
    }
    else if (op_ == &commit_acquire_all_write_op_)
    {
        if (!CheckLeaderTerm(node_group_, txm->tx_term_, txm->tx_status_))
        {
            Sharder::Instance().UnpinNodeGroupData(node_group_);
            ForceToFinish(txm);
            return;
        }
        if (commit_acquire_all_write_op_.fail_cnt_.load(
                std::memory_order_relaxed) > 0)
        {
            LOG(ERROR) << "Split Flush transaction failed to obtain write "
                          "lock, tx_number:"
                       << txm->tx_number_;

            // Set commit ts to 0 to indicate transaction failure.
            // post_all_lock_op_ will release locks acquired.
            txm->commit_ts_ = tx_op_failed_ts_;
            ForwardToSubOperation(txm, &post_all_lock_op_);
            return;
        }

        FillCommitLogRequest(txm);
        ForwardToSubOperation(txm, &commit_log_op_);
    }
    else if (op_ == &commit_log_op_)
    {
        if (commit_log_op_.hd_result_.IsError())
        {
            // error
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
        // First slice is always left in the old range. Put it into vector first
        // to avoid dealing with null start key.
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
             &range_info = splitted_range_info,
             tx_ts = txm->commit_ts_,
             table_schema = table_schema_,
             &hd_res = ds_upsert_range_op_.hd_result_]
        {
            TxWorkerPool *tx_worker_pool =
                Sharder::Instance().GetTxWorkerPool();
            store::DataStoreHandler *const store_hd =
                Sharder::Instance().GetLocalCcShards()->store_hd_;
            tx_worker_pool->SubmitWork(
                [table_name, range_info, tx_ts, table_schema, &hd_res, store_hd]
                {
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
        };

        ForwardToSubOperation(txm, &ds_upsert_range_op_);
    }
    else if (op_ == &ds_upsert_range_op_)
    {
        if (ds_upsert_range_op_.hd_result_.IsError())
        {
            // error & retry
        }
        post_all_lock_op_.rec_ = &range_record_;
        ForwardToSubOperation(txm, &post_all_lock_op_);
    }
    else if (op_ == &post_all_lock_op_)
    {
        // Delete stale data from old partition
        ds_clean_old_range_op_.op_func_ =
            [partition_id = range_info_.partition_id_,
             start_key = new_range_info_.begin()->first.get(),
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
        ForwardToSubOperation(txm, &ds_clean_old_range_op_);
    }
    else if (op_ == &ds_clean_old_range_op_)
    {
        if (ds_clean_old_range_op_.hd_result_.IsError())
        {
            // error & retry
        }
        FillCleanLogRequest(txm);
        ForwardToSubOperation(txm, &clean_log_op_);
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
            // set retry flag and retry clean log
            ::txlog::WriteLogRequest *log_req =
                clean_log_op_.log_closure_.LogRequest()
                    .mutable_write_log_request();
            log_req->set_retry(true);
            RetrySubOperation(txm, &clean_log_op_);
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
            txm->bool_resp_->Finish(true);
            txm->state_stack_.pop_back();
            Sharder::Instance().UnpinNodeGroupData(node_group_);
            assert(txm->state_stack_.empty());
            txm->split_flush_op_ = nullptr;
        }
    }
}

/**
 * @brief Fill prepare log for Split-Flush Tx. We need to have old range info
 * and new range info, commit ts (for deciding the records that needs to be
 * flushed) in the log record.
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
    prepare_log_rec->set_txn_number(txm->tx_number_);
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
    ::txlog::SplitFlushOpMessage *prepare_split_msg =
        prepare_log_rec->mutable_log_content()->mutable_split_flush_log();
    prepare_split_msg->set_table_name(range_table_name_.String());
    prepare_split_msg->set_stage(
        ::txlog::SplitFlushOpMessage_Stage_PrepareSplit);
    // Set range info for splitting range
    prepare_split_msg->set_partition_id(range_info_.partition_id_);
    prepare_split_msg->set_range_key_neg_inf(false);
    prepare_split_msg->set_range_key_pos_inf(false);
    switch (old_start_key_->Type())
    {
    case KeyType::NegativeInf:
        prepare_split_msg->set_range_key_neg_inf(true);
        break;
    case KeyType::PositiveInf:
        prepare_split_msg->set_range_key_pos_inf(true);
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
        prepare_split_msg->add_new_range_key(new_range_key.data());
    }
}

void SplitFlushRangeOp::FillCommitLogRequest(TransactionExecution *txm)
{
    commit_log_op_.log_type_ = TxLogType::COMMIT;
    commit_log_op_.log_closure_.LogRequest().Clear();
    ::txlog::WriteLogRequest *commit_log_rec =
        commit_log_op_.log_closure_.LogRequest().mutable_write_log_request();

    commit_log_rec->set_tx_term(txm->tx_term_);
    commit_log_rec->set_txn_number(txm->tx_number_);
    commit_log_rec->set_commit_timestamp(txm->commit_ts_);
    auto commit_schema_msg =
        commit_log_rec->mutable_log_content()->mutable_split_flush_log();
    commit_schema_msg->set_stage(
        ::txlog::SplitFlushOpMessage_Stage_CommitSplit);

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
    clean_log_rec->set_txn_number(txm->tx_number_);

    ::txlog::SplitFlushOpMessage *clean_split_msg =
        clean_log_rec->mutable_log_content()->mutable_split_flush_log();

    clean_split_msg->set_stage(::txlog::SplitFlushOpMessage_Stage_CleanSplit);
    clean_log_rec->mutable_node_terms()->clear();
}
void SplitFlushRangeOp::ForceToFinish(TransactionExecution *txm)
{
    clean_log_op_.hd_result_.SetFinished();
    op_ = &clean_log_op_;
    Forward(txm);
}
DsSplitRangeOp::DsSplitRangeOp(
    const TableName &table_name,
    const TableSchema *table_schema,
    const TxKey *range_key,
    std::unique_ptr<RangeRecord> splitting_range_record,
    TransactionExecution *txm,
    std::optional<std::pair<CcEntryAddr, ReadSetEntry>> catalog_cc_entry)
    : CompositeTransactionOperation(),
      table_name_(table_name.StringView().data(),
                  table_name.StringView().size(),
                  table_name.Type()),
      range_table_name_(table_name.StringView().data(),
                        table_name.StringView().size(),
                        TableType::RangePartition),
      table_schema_(table_schema),
      range_key_(range_key),
      old_range_record_(std::move(splitting_range_record)),
      upload_range_entry_(nullptr),
      upload_range_record_(nullptr),
      new_partition_id_{-1},
      catalog_cc_entry_(std::move(catalog_cc_entry)),
      acquire_all_intent_for_update_old_range_op_(txm),
      ds_find_median_key_for_old_range_op_(txm),
      acquire_all_lock_for_update_old_range_op_(txm),
      prepare_log_for_update_old_range_op_(txm),
      post_all_lock_for_update_old_range_op_(txm),
      ds_copy_old_range_data_op_(txm),
      ds_copy_old_range_data_finished_log_op_(txm),
      acquire_all_lock_for_dirty_old_range_op_(txm),
      commit_log_for_dirty_old_range_op_(txm),
      post_write_all_for_dirty_old_range_op_(txm),
      ds_upsert_new_range_op_(txm),
      delete_out_of_old_range_data_log_op_(txm),
      delete_out_of_old_range_data_op_(txm),
      clean_log_op_(txm),
      catalog_post_read_op_(txm)
{
    partition_id_ = old_range_record_->GetRangeInfo()->partition_id_;
    upload_range_entry_ = old_range_record_->GetRangeInfo()->Clone();
    upload_range_record_ = std::make_unique<RangeRecord>();
    upload_range_record_->range_info_ = upload_range_entry_.get();

    // prepare acquire_all_intent_for_update_old_range_op_
    acquire_all_intent_for_update_old_range_op_.table_name_ =
        &range_table_name_;
    acquire_all_intent_for_update_old_range_op_.key_ = range_key_;
    acquire_all_intent_for_update_old_range_op_.cc_op_ =
        CcOperation::ReadForWrite;
    acquire_all_intent_for_update_old_range_op_.protocol_ = CcProtocol::OCC;

    // prepare acquire_all_lock_for_update_old_range_op_
    acquire_all_lock_for_update_old_range_op_.table_name_ = &range_table_name_;
    acquire_all_lock_for_update_old_range_op_.key_ = range_key_;
    acquire_all_lock_for_update_old_range_op_.cc_op_ = CcOperation::Write;
    acquire_all_lock_for_update_old_range_op_.protocol_ = CcProtocol::Locking;

    post_all_lock_for_update_old_range_op_.table_name_ = &range_table_name_;
    post_all_lock_for_update_old_range_op_.key_ = range_key_;
    post_all_lock_for_update_old_range_op_.op_type_ = OperationType::Update;
    post_all_lock_for_update_old_range_op_.write_type_ =
        PostWriteType::PrepareCommit;

    // prepare acquire_all_lock_for_dirty_old_range_op_
    acquire_all_lock_for_dirty_old_range_op_.table_name_ = &range_table_name_;
    acquire_all_lock_for_dirty_old_range_op_.key_ = range_key_;
    acquire_all_lock_for_dirty_old_range_op_.cc_op_ = CcOperation::Write;
    acquire_all_lock_for_dirty_old_range_op_.protocol_ = CcProtocol::Locking;

    // prepare the post_write_all_for_dirty_old_range_op_ for failed case
    post_write_all_for_dirty_old_range_op_.table_name_ = &range_table_name_;
    post_write_all_for_dirty_old_range_op_.key_ = range_key_;
    post_write_all_for_dirty_old_range_op_.op_type_ = OperationType::Update;
    post_write_all_for_dirty_old_range_op_.write_type_ =
        PostWriteType::PostCommit;

    TX_TRACE_ASSOCIATE(this,
                       &acquire_all_intent_for_update_old_range_op_,
                       "acquire_all_intent_for_update_old_range_op_");
    TX_TRACE_ASSOCIATE(this,
                       &ds_find_median_key_for_old_range_op_,
                       "ds_find_median_key_for_old_range_op_");
    TX_TRACE_ASSOCIATE(this,
                       &acquire_all_lock_for_update_old_range_op_,
                       "acquire_all_lock_for_update_old_range_op_");
    TX_TRACE_ASSOCIATE(this,
                       &prepare_log_for_update_old_range_op_,
                       "prepare_log_for_update_old_range_op_");
    TX_TRACE_ASSOCIATE(this,
                       &post_all_lock_for_update_old_range_op_,
                       "post_all_lock_for_update_old_range_op_");
    TX_TRACE_ASSOCIATE(
        this, &ds_copy_old_range_data_op_, "ds_copy_old_range_data_op_");
    TX_TRACE_ASSOCIATE(this,
                       &ds_copy_old_range_data_finished_log_op_,
                       "ds_copy_old_range_data_finished_log_op_");
    TX_TRACE_ASSOCIATE(this,
                       &acquire_all_lock_for_dirty_old_range_op_,
                       "acquire_all_lock_for_dirty_old_range_op_");
    TX_TRACE_ASSOCIATE(this,
                       &commit_log_for_dirty_old_range_op_,
                       "commit_log_for_dirty_old_range_op_");
    TX_TRACE_ASSOCIATE(this,
                       &post_write_all_for_dirty_old_range_op_,
                       "post_write_all_for_dirty_old_range_op_");
    TX_TRACE_ASSOCIATE(
        this, &ds_upsert_new_range_op_, "ds_upsert_new_range_op_");
    TX_TRACE_ASSOCIATE(this,
                       &delete_out_of_old_range_data_log_op_,
                       "delete_out_of_old_range_data_log_op_");
    TX_TRACE_ASSOCIATE(this,
                       &delete_out_of_old_range_data_op_,
                       "delete_out_of_old_range_data_op_");
    TX_TRACE_ASSOCIATE(this, &clean_log_op_, "clean_log_op_");
}

void DsSplitRangeOp::FillTxLog(TransactionExecution *txm,
                               WriteToLogOp &log_op,
                               TxLogType log_type,
                               ::txlog::SplitRangeOpMessage::Stage stage)
{
    log_op.log_type_ = log_type;
    log_op.log_closure_.LogRequest().Clear();
    ::txlog::WriteLogRequest *log_range_rec =
        log_op.log_closure_.LogRequest().mutable_write_log_request();

    log_range_rec->set_tx_term(txm->tx_term_);
    log_range_rec->set_txn_number(txm->tx_number_);
    log_range_rec->set_commit_timestamp(txm->commit_ts_);

    ::txlog::SplitRangeOpMessage *log_range_msg =
        log_range_rec->mutable_log_content()->mutable_split_range_log();
    log_range_msg->set_stage(stage);

    log_range_msg->set_table_name(range_table_name_.String());
    log_range_msg->set_table_schema(table_schema_->SchemaImage());
    log_range_msg->set_partition_id(partition_id_);
    log_range_msg->set_new_partition_id(new_partition_id_);

    log_range_msg->set_range_key_neg_inf(false);
    log_range_msg->set_range_key_pos_inf(false);
    switch (range_key_->Type())
    {
    case KeyType::NegativeInf:
        log_range_msg->set_range_key_neg_inf(true);
        break;
    case KeyType::PositiveInf:
        log_range_msg->set_range_key_pos_inf(true);
        break;
    default:
        range_key_->Serialize(*log_range_msg->mutable_range_key_value());
        break;
    }

    new_range_key_->Serialize(*log_range_msg->mutable_new_range_key());

    log_range_rec->mutable_node_terms()->clear();
}

void DsSplitRangeOp::FillTxLogForCleanLog(TransactionExecution *txm)
{
    clean_log_op_.log_type_ = TxLogType::CLEAN;

    clean_log_op_.log_closure_.LogRequest().Clear();

    ::txlog::WriteLogRequest *clean_log_rec =
        clean_log_op_.log_closure_.LogRequest().mutable_write_log_request();

    clean_log_rec->set_tx_term(txm->tx_term_);
    clean_log_rec->set_txn_number(txm->tx_number_);

    ::txlog::SplitRangeOpMessage *log_range_msg =
        clean_log_rec->mutable_log_content()->mutable_split_range_log();

    log_range_msg->set_stage(::txlog::SplitRangeOpMessage::CleanLog);
    clean_log_rec->mutable_node_terms()->clear();
}

void DsSplitRangeOp::ForceToFinish(TransactionExecution *txm)
{
    clean_log_op_.hd_result_.SetFinished();
    op_ = &clean_log_op_;
    Forward(txm);
}

void DsSplitRangeOp::PrepareUploadRangeRecord()
{
    // upload_range_entry_ = old_range_record_->RangeEntry()->Clone();
    // upload_range_entry_->new_key_ =
    //     new_range_key_ != nullptr ? new_range_key_->Clone() : nullptr;
    // upload_range_entry_->new_partition_id_ = new_partition_id_;

    // upload_range_record_ = std::make_unique<RangeRecord>();
    // upload_range_record_->range_entry_ = upload_range_entry_.get();
}

void DsSplitRangeOp::Forward(TransactionExecution *txm)
{
    if (op_ == nullptr)
    {
        // Pin data only when DsSplitRangeOp begin
        uint32_t node_group_id = txm->TxCcNodeId();
        int64_t leader_term =
            Sharder::Instance().TryPinNodeGroupData(node_group_id);

        if (FAULT_INJECTOR_CONDITION_WRAP(
                "cw_term_fail_before_split_range_op_start", leader_term < 0))
        {
            DLOG(ERROR) << "DsSplitRangeOp abort: TRANSACTION_NODE_NOT_LEADER";
            txm->MarkFailed();
            return;
        }

        ACTION_FAULT_INJECTOR("af_split_range_acquire_write_intent");

        DLOG(INFO) << "acquire_all_intent_for_update_old_range_op_";
        ForwardToSubOperation(txm,
                              &acquire_all_intent_for_update_old_range_op_);
    }
    else if (op_ == &acquire_all_intent_for_update_old_range_op_)
    {
        if (FAULT_INJECTOR_CONDITION_WRAP(
                "cw_split_range_acquire_write_intent",
                acquire_all_intent_for_update_old_range_op_.fail_cnt_.load(
                    std::memory_order_acquire) > 0))
        {
            // txm->bool_resp_->SetErrorCode(
            // TxErrorCode::SPLIT_RANGE_ACQUIRE_WRITE_INTENT_FAIL);
            DLOG(ERROR) << "DsSplitRangeOp failed: "
                           "SPLIT_RANGE_ACQUIRE_WRITE_INTENT_FAIL";
            txm->MarkFailed();
            PrepareUploadRangeRecord();
            post_write_all_for_dirty_old_range_op_.rec_ =
                upload_range_record_.get();
            ForwardToSubOperation(txm, &post_write_all_for_dirty_old_range_op_);
        }
        else
        {
            if (!CheckLeaderTerm(
                    txm->TxCcNodeId(), txm->tx_term_, txm->tx_status_))
            {
                uint32_t node_group_id = txm->TxCcNodeId();
                Sharder::Instance().UnpinNodeGroupData(node_group_id);
                ForceToFinish(txm);
            }
            else
            {
                ACTION_FAULT_INJECTOR("af_ds_find_median_key");

                // TODO(Xiao Ji): Check old_range_record_.version_ts_ against
                // the acquire_all_result.commit_ts_ to make sure the split
                // range is not changed since been read.
                uint64_t candidate_max_ts =
                    acquire_all_intent_for_update_old_range_op_.MaxTs();
                txm->ForwardTs(candidate_max_ts);

                // prepare ds_find_median_key_for_old_range_op_
                ds_find_median_key_for_old_range_op_.op_func_ =
                    [partition_id = partition_id_,
                     &table_name = table_name_,
                     table_schema = table_schema_,
                     &hd_res = ds_find_median_key_for_old_range_op_.hd_result_]
                {
                    TxWorkerPool *tx_worker_pool =
                        Sharder::Instance().GetTxWorkerPool();
                    store::DataStoreHandler *const store_hd =
                        Sharder::Instance().GetLocalCcShards()->store_hd_;

                    tx_worker_pool->SubmitWork(
                        [partition_id,
                         table_name,
                         table_schema,
                         store_hd,
                         &hd_res]
                        {
                            bool succ =
                                store_hd->FindRangeMedianKey(table_name,
                                                             partition_id,
                                                             table_schema,
                                                             &hd_res);
                            if (!succ)
                            {
                                DLOG(INFO) << "FindRangeMedianKey failed: "
                                           << table_name.String() << " "
                                           << partition_id;
                                hd_res.SetError(
                                    CcErrorCode::REQUESTED_NODE_NOT_LEADER);
                            }
                            else
                            {
                                succ = store_hd->GetNextRangePartitionId(
                                    table_name,
                                    &hd_res.Value().new_partition_id_);
                                if (succ)
                                {
                                    hd_res.SetFinished();
                                }
                                else
                                {
                                    hd_res.SetError(
                                        CcErrorCode::REQUESTED_NODE_NOT_LEADER);
                                }
                            }
                        });
                };

                ForwardToSubOperation(txm,
                                      &ds_find_median_key_for_old_range_op_);
            }
        }
    }
    else if (op_ == &ds_find_median_key_for_old_range_op_)
    {
        if (FAULT_INJECTOR_CONDITION_WRAP(
                "cw_ds_find_median_key",
                ds_find_median_key_for_old_range_op_.hd_result_.IsError()))
        {
            DLOG(ERROR) << "DsSplitRangeOp failed: DATA_STORE_READ_ERR";
            txm->MarkFailed();
            PrepareUploadRangeRecord();
            post_write_all_for_dirty_old_range_op_.rec_ =
                upload_range_record_.get();
            ForwardToSubOperation(txm, &post_write_all_for_dirty_old_range_op_);
        }
        else
        {
            if (!CheckLeaderTerm(
                    txm->TxCcNodeId(), txm->tx_term_, txm->tx_status_))
            {
                uint32_t node_group_id = txm->TxCcNodeId();
                Sharder::Instance().UnpinNodeGroupData(node_group_id);
                ForceToFinish(txm);
            }
            else
            {
                RangeMedianKeyResult &median_key_result =
                    ds_find_median_key_for_old_range_op_.hd_result_.Value();
                new_range_key_ = std::move(median_key_result.median_key_);
                new_partition_id_ = median_key_result.new_partition_id_;

                ACTION_FAULT_INJECTOR(
                    "af_acquire_all_lock_for_update_old_range");

                // Going to lock the range table with the old range start key
                ForwardToSubOperation(
                    txm, &acquire_all_lock_for_update_old_range_op_);
            }
        }
    }
    else if (op_ == &acquire_all_lock_for_update_old_range_op_)
    {
        if (FAULT_INJECTOR_CONDITION_WRAP(
                "cw_acquire_all_lock_for_update_old_range",
                acquire_all_lock_for_update_old_range_op_.fail_cnt_.load(
                    std::memory_order_acquire) > 0))
        {
            DLOG(ERROR)
                << "DsSplitRangeOp failed: SPLIT_RANGE_ACQUIRE_WRITE_LOCK_FAIL";
            txm->MarkFailed();
            PrepareUploadRangeRecord();
            post_write_all_for_dirty_old_range_op_.rec_ =
                upload_range_record_.get();
            ForwardToSubOperation(txm, &post_write_all_for_dirty_old_range_op_);
        }
        else
        {
            if (!CheckLeaderTerm(
                    txm->TxCcNodeId(), txm->tx_term_, txm->tx_status_))
            {
                uint32_t node_group_id = txm->TxCcNodeId();
                Sharder::Instance().UnpinNodeGroupData(node_group_id);
                ForceToFinish(txm);
            }
            else
            {
                ACTION_FAULT_INJECTOR(
                    "af_prepare_log_for_update_for_old_range");

                FillTxLog(txm,
                          prepare_log_for_update_old_range_op_,
                          TxLogType::PREPARE,
                          ::txlog::SplitRangeOpMessage::PrepareDirtyOldRange);
                ForwardToSubOperation(txm,
                                      &prepare_log_for_update_old_range_op_);
            }
        }
    }
    else if (op_ == &prepare_log_for_update_old_range_op_)
    {
        if (FAULT_INJECTOR_CONDITION_WRAP(
                "cw_prepare_log_for_update_old_range",
                prepare_log_for_update_old_range_op_.hd_result_.IsError()))
        {
            DLOG(ERROR) << "DsSplitRangeOp failed: "
                           "SPLIT_RANGE_PREPARE_LOG_FOR_OLD_RANGE_FAIL";

            if (prepare_log_for_update_old_range_op_.hd_result_.ErrorCode() ==
                CcErrorCode::LOG_CLOSURE_RESULT_UNKOWN_ERR)
            {
                // prepare log result unknown, keep retrying until getting a
                // clear response, either success or failure, or the coordinator
                // itself is no longer leader
                if (!CheckLeaderTerm(
                        txm->TxCcNodeId(), txm->tx_term_, txm->tx_status_))
                {
                    uint32_t node_group_id = txm->TxCcNodeId();
                    Sharder::Instance().UnpinNodeGroupData(node_group_id);
                    ForceToFinish(txm);
                }
                else
                {
                    RetrySubOperation(txm,
                                      &prepare_log_for_update_old_range_op_);
                }
            }
            else
            {
                txm->MarkFailed();
                PrepareUploadRangeRecord();
                post_write_all_for_dirty_old_range_op_.rec_ =
                    upload_range_record_.get();
                ForwardToSubOperation(txm,
                                      &post_write_all_for_dirty_old_range_op_);
            }
        }
        else
        {
            if (!CheckLeaderTerm(
                    txm->TxCcNodeId(), txm->tx_term_, txm->tx_status_))
            {
                uint32_t node_group_id = txm->TxCcNodeId();
                Sharder::Instance().UnpinNodeGroupData(node_group_id);
                ForceToFinish(txm);
            }
            else
            {
                ACTION_FAULT_INJECTOR("af_post_all_lock_for_update_range");

                // Going to update the old range with new key and new partition
                // id as the splitting key and the old range became dirty
                DLOG(INFO) << "post_all_lock_for_update_old_range_op_";
                PrepareUploadRangeRecord();
                post_all_lock_for_update_old_range_op_.rec_ =
                    upload_range_record_.get();
                ForwardToSubOperation(txm,
                                      &post_all_lock_for_update_old_range_op_);
            }
        }
    }
    else if (op_ == &post_all_lock_for_update_old_range_op_)
    {
        bool failed = post_all_lock_for_update_old_range_op_.IsFailed();

        if (FAULT_INJECTOR_CONDITION_WRAP("cw_post_all_lock_for_update_range",
                                          failed))
        {
            // After prepare log is succeed, this range split operation is
            // guaranteed to succeed, and can only roll forward, retry if failed
            // unless the leader is changed.
            if (CheckLeaderTerm(
                    txm->TxCcNodeId(), txm->tx_term_, txm->tx_status_))
            {
                RetrySubOperation(txm, &post_all_lock_for_update_old_range_op_);
            }
            else
            {
                // txm->bool_resp_->SetErrorCode(
                // TxErrorCode::TRANSACTION_NODE_NOT_LEADER);
                ForceToFinish(txm);
            }
        }
        else
        {
            if (!CheckLeaderTerm(
                    txm->TxCcNodeId(), txm->tx_term_, txm->tx_status_))
            {
                DLOG(ERROR) << "DsSplitRangeOp is forced to finish: "
                               "TRANSACTION_NODE_NOT_LEADER";
                ForceToFinish(txm);
            }
            else
            {
                ACTION_FAULT_INJECTOR("af_ds_copy_old_range_data_finished_log");

                FillTxLog(txm,
                          ds_copy_old_range_data_finished_log_op_,
                          TxLogType::DATA,
                          ::txlog::SplitRangeOpMessage::CopingOldRangeData);
                DLOG(INFO) << "ds_copy_old_range_data_finished_log_op_";
                ForwardToSubOperation(txm,
                                      &ds_copy_old_range_data_finished_log_op_);
            }
        }
    }
    else if (op_ == &ds_copy_old_range_data_finished_log_op_)
    {
        if (FAULT_INJECTOR_CONDITION_WRAP(
                "cw_ds_copy_old_range_data_finished_log",
                ds_copy_old_range_data_finished_log_op_.hd_result_.IsError()))
        {
            if (CheckLeaderTerm(
                    txm->TxCcNodeId(), txm->tx_term_, txm->tx_status_))
            {
                // Retry if failed
                RetrySubOperation(txm,
                                  &ds_copy_old_range_data_finished_log_op_);
            }
            else
            {
                DLOG(ERROR) << "DsSplitRangeOp is forced to finish: "
                               "TRANSACTION_NODE_NOT_LEADER";
                ForceToFinish(txm);
            }
        }
        else
        {
            if (!CheckLeaderTerm(
                    txm->TxCcNodeId(), txm->tx_term_, txm->tx_status_))
            {
                DLOG(ERROR) << "DsSplitRangeOp is forced to finish: "
                               "TRANSACTION_NODE_NOT_LEADER";
                ForceToFinish(txm);
            }
            else
            {
                ACTION_FAULT_INJECTOR("af_ds_copy_old_range_data");

                // Going to copy data after middle key in the old range to the
                // new range
                ds_copy_old_range_data_op_.op_func_ =
                    [&table_name = table_name_,
                     old_partition_id = partition_id_,
                     new_partition_id = new_partition_id_,
                     start_key = new_range_key_.get(),
                     tx_ts = txm->commit_ts_,
                     table_schema = table_schema_,
                     &hd_res = ds_copy_old_range_data_op_.hd_result_]
                {
                    TxWorkerPool *tx_worker_pool =
                        Sharder::Instance().GetTxWorkerPool();
                    store::DataStoreHandler *const store_hd =
                        Sharder::Instance().GetLocalCcShards()->store_hd_;
                    tx_worker_pool->SubmitWork(
                        [table_name,
                         old_partition_id,
                         new_partition_id,
                         start_key,
                         tx_ts,
                         table_schema,
                         &hd_res,
                         store_hd]
                        {
                            // bool succ =
                            //     store_hd->CopyRangeData(table_name,
                            //                             old_partition_id,
                            //                             new_partition_id,
                            //                             start_key,
                            //                             tx_ts,
                            //                             table_schema);
                            // if (succ)
                            //{
                            //     hd_res.SetFinished();
                            // }
                            // else
                            //{
                            //     hd_res.SetError(CcErrorCode::REQUEST_NODE_NOT_LEADER);
                            // }
                        });
                };

                ForwardToSubOperation(txm, &ds_copy_old_range_data_op_);
            }
        }
    }
    else if (op_ == &ds_copy_old_range_data_op_)
    {
        if (FAULT_INJECTOR_CONDITION_WRAP(
                "cw_af_ds_copy_old_range_data",
                ds_copy_old_range_data_op_.hd_result_.IsError()))
        {
            if (CheckLeaderTerm(
                    txm->TxCcNodeId(), txm->tx_term_, txm->tx_status_))
            {
                RetrySubOperation(txm, &ds_copy_old_range_data_op_);
            }
            else
            {
                DLOG(ERROR) << "DsSplitRangeOp is forced to finish: "
                               "TRANSACTION_NODE_NOT_LEADER";
                ForceToFinish(txm);
            }
        }
        else
        {
            if (!CheckLeaderTerm(
                    txm->TxCcNodeId(), txm->tx_term_, txm->tx_status_))
            {
                DLOG(ERROR) << "DsSplitRangeOp is forced to finish: "
                               "TRANSACTION_NODE_NOT_LEADER";
                ForceToFinish(txm);
            }
            else
            {
                ACTION_FAULT_INJECTOR(
                    "af_acquire_all_lock_for_dirty_old_range");

                ForwardToSubOperation(
                    txm, &acquire_all_lock_for_dirty_old_range_op_);
            }
        }
    }
    else if (op_ == &acquire_all_lock_for_dirty_old_range_op_)
    {
        if (FAULT_INJECTOR_CONDITION_WRAP(
                "cw_acquire_all_lock_for_dirty_old_range",
                acquire_all_lock_for_dirty_old_range_op_.fail_cnt_.load(
                    std::memory_order_acquire) > 0))
        {
            if (CheckLeaderTerm(
                    txm->TxCcNodeId(), txm->tx_term_, txm->tx_status_))
            {
                RetrySubOperation(txm,
                                  &acquire_all_lock_for_dirty_old_range_op_);
            }
            else
            {
                DLOG(ERROR) << "DsSplitRangeOp is forced to finish: "
                               "TRANSACTION_NODE_NOT_LEADER";
                ForceToFinish(txm);
            }
        }
        else
        {
            if (!CheckLeaderTerm(
                    txm->TxCcNodeId(), txm->tx_term_, txm->tx_status_))
            {
                DLOG(ERROR) << "DsSplitRangeOp is forced to finish: "
                               "TRANSACTION_NODE_NOT_LEADER";
                ForceToFinish(txm);
            }
            else
            {
                ACTION_FAULT_INJECTOR("af_commit_log_for_dirty_old_range");

                FillTxLog(txm,
                          commit_log_for_dirty_old_range_op_,
                          TxLogType::COMMIT,
                          ::txlog::SplitRangeOpMessage::CommitOldRangeNewRange);
                ForwardToSubOperation(txm, &commit_log_for_dirty_old_range_op_);
            }
        }
    }
    else if (op_ == &commit_log_for_dirty_old_range_op_)
    {
        if (FAULT_INJECTOR_CONDITION_WRAP(
                "cw_commit_log_for_dirty_old_range",
                commit_log_for_dirty_old_range_op_.hd_result_.IsError()))
        {
            // Fails to flush the commit log. Retries the operation if the
            // tx node is still the leader.
            if (CheckLeaderTerm(
                    txm->TxCcNodeId(), txm->tx_term_, txm->tx_status_))
            {
                RetrySubOperation(txm, &commit_log_for_dirty_old_range_op_);
            }
            else
            {
                DLOG(ERROR) << "DsSplitRangeOp is forced to finish: "
                               "TRANSACTION_NODE_NOT_LEADER";
                ForceToFinish(txm);
            }
        }
        else
        {
            if (!CheckLeaderTerm(
                    txm->TxCcNodeId(), txm->tx_term_, txm->tx_status_))
            {
                DLOG(ERROR) << "DsSplitRangeOp is forced to finish: "
                               "TRANSACTION_NODE_NOT_LEADER";
                ForceToFinish(txm);
            }
            else
            {
                ACTION_FAULT_INJECTOR("af_post_write_all_for_dirty_old_range");

                // Restore the dirty range record to the old one,
                // and fork the new range
                PrepareUploadRangeRecord();
                post_write_all_for_dirty_old_range_op_.rec_ =
                    upload_range_record_.get();
                ForwardToSubOperation(txm,
                                      &post_write_all_for_dirty_old_range_op_);
            }
        }
    }
    else if (op_ == &post_write_all_for_dirty_old_range_op_)
    {
        bool failed = post_write_all_for_dirty_old_range_op_.IsFailed();

        if (FAULT_INJECTOR_CONDITION_WRAP(
                "cw_post_write_all_for_dirty_old_range", failed))
        {
            if (CheckLeaderTerm(
                    txm->TxCcNodeId(), txm->tx_term_, txm->tx_status_))
            {
                PrepareUploadRangeRecord();
                post_write_all_for_dirty_old_range_op_.rec_ =
                    upload_range_record_.get();
                RetrySubOperation(txm, &post_write_all_for_dirty_old_range_op_);
            }
            else
            {
                DLOG(ERROR) << "DsSplitRangeOp is forced to finish: "
                               "TRANSACTION_NODE_NOT_LEADER";
                ForceToFinish(txm);
            }
        }
        else
        {
            if (!CheckLeaderTerm(
                    txm->TxCcNodeId(), txm->tx_term_, txm->tx_status_))
            {
                DLOG(ERROR) << "DsSplitRangeOp is forced to finish: "
                               "TRANSACTION_NODE_NOT_LEADER";
                ForceToFinish(txm);
            }
            else
            {
                ACTION_FAULT_INJECTOR("af_ds_upsert_new_range");

                if (txm->commit_ts_ == 0)
                {
                    // The split op failed without prepare log, bypass any retry
                    DLOG(ERROR)
                        << "DsSplitRangeOp is aborted without prepare log";
                    txm->state_stack_.pop_back();
                    assert(txm->state_stack_.empty());
                    txm->ds_split_range_op_ = nullptr;

                    if (Sharder::Instance().CheckLeaderTerm(txm->TxCcNodeId(),
                                                            txm->tx_term_))
                    {
                        txm->Abort();
                    }

                    uint32_t node_group_id = txm->TxCcNodeId();
                    Sharder::Instance().UnpinNodeGroupData(node_group_id);
                }
                else
                {
                    // prepare ds_upsert_new_range_op_
                    ds_upsert_new_range_op_.op_func_ =
                        [&range_table_name = range_table_name_,
                         table_schema = table_schema_,
                         key = new_range_key_.get(),
                         partition_id = new_partition_id_,
                         ts = txm->commit_ts_,
                         &hd_res = ds_upsert_new_range_op_.hd_result_]
                    {
                        TxWorkerPool *tx_worker_pool =
                            Sharder::Instance().GetTxWorkerPool();
                        store::DataStoreHandler *const store_hd =
                            Sharder::Instance().GetLocalCcShards()->store_hd_;
                        tx_worker_pool->SubmitWork(
                            [range_table_name,
                             table_schema,
                             key,
                             partition_id,
                             ts,
                             &hd_res,
                             store_hd]
                            {
                                // bool succ =
                                //     store_hd->UpsertRange(range_table_name,
                                //                           table_schema,
                                //                           key,
                                //                           partition_id,
                                //                           ts);
                                // if (succ)
                                //{
                                //     hd_res.SetFinished();
                                // }
                                // else
                                //{
                                //     hd_res.SetError(
                                //         CcErrorCode::REQUEST_NODE_NOT_LEADER);
                                // }
                            });
                    };

                    DLOG(INFO) << "ds_upsert_new_range_op_";
                    ForwardToSubOperation(txm, &ds_upsert_new_range_op_);
                }
            }
        }
    }
    else if (op_ == &ds_upsert_new_range_op_)
    {
        if (FAULT_INJECTOR_CONDITION_WRAP(
                "cw_ds_upsert_new_range",
                ds_upsert_new_range_op_.hd_result_.IsError()))
        {
            if (CheckLeaderTerm(
                    txm->TxCcNodeId(), txm->tx_term_, txm->tx_status_))
            {
                // Retry if failed
                RetrySubOperation(txm, &ds_upsert_new_range_op_);
            }
            else
            {
                DLOG(ERROR) << "DsSplitRangeOp is forced to finish: "
                               "TRANSACTION_NODE_NOT_LEADER";
                ForceToFinish(txm);
            }
        }
        else
        {
            if (!CheckLeaderTerm(
                    txm->TxCcNodeId(), txm->tx_term_, txm->tx_status_))
            {
                DLOG(ERROR) << "DsSplitRangeOp is forced to finish: "
                               "TRANSACTION_NODE_NOT_LEADER";
                ForceToFinish(txm);
            }
            else
            {
                ACTION_FAULT_INJECTOR("af_delete_out_of_old_range_data_log");

                FillTxLog(txm,
                          delete_out_of_old_range_data_log_op_,
                          TxLogType::DATA,
                          ::txlog::SplitRangeOpMessage::DeletingOldRangeData);
                DLOG(INFO) << "delete_out_of_old_range_data_log_op_";
                ForwardToSubOperation(txm,
                                      &delete_out_of_old_range_data_log_op_);
            }
        }
    }
    else if (op_ == &delete_out_of_old_range_data_log_op_)
    {
        if (FAULT_INJECTOR_CONDITION_WRAP(
                "cw_delete_out_of_old_range_data_log",
                delete_out_of_old_range_data_log_op_.hd_result_.IsError()))
        {
            // Fails to flush the commit log. Retries the operation if the
            // tx node is still the leader.
            if (CheckLeaderTerm(
                    txm->TxCcNodeId(), txm->tx_term_, txm->tx_status_))
            {
                RetrySubOperation(txm, &delete_out_of_old_range_data_log_op_);
            }
            else
            {
                DLOG(ERROR) << "DsSplitRangeOp is forced to finish: "
                               "TRANSACTION_NODE_NOT_LEADER";
                ForceToFinish(txm);
            }
        }
        else
        {
            if (!CheckLeaderTerm(
                    txm->TxCcNodeId(), txm->tx_term_, txm->tx_status_))
            {
                DLOG(ERROR) << "DsSplitRangeOp is forced to finish: "
                               "TRANSACTION_NODE_NOT_LEADER";
                ForceToFinish(txm);
            }
            else
            {
                ACTION_FAULT_INJECTOR("af_delete_out_of_old_range_data");

                // prepare delete_out_of_old_range_data_op_
                delete_out_of_old_range_data_op_.op_func_ =
                    [partition_id = partition_id_,
                     start_key = new_range_key_.get(),
                     &table_name = table_name_,
                     table_schema = table_schema_,
                     &hd_res = delete_out_of_old_range_data_op_.hd_result_]
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
                            bool succ =
                                store_hd->DeleteOutOfRangeData(table_name,
                                                               partition_id,
                                                               start_key,
                                                               table_schema);
                            if (succ)
                            {
                                hd_res.SetFinished();
                            }
                            else
                            {
                                hd_res.SetError(
                                    CcErrorCode::REQUESTED_NODE_NOT_LEADER);
                            }
                        });
                };

                ForwardToSubOperation(txm, &delete_out_of_old_range_data_op_);
            }
        }
    }
    else if (op_ == &delete_out_of_old_range_data_op_)
    {
        if (FAULT_INJECTOR_CONDITION_WRAP(
                "cw_delete_out_of_old_range_data",
                delete_out_of_old_range_data_op_.hd_result_.IsError()))
        {
            // The data store operation failed. Retry the operation if the
            // tx node is still the leader.
            if (CheckLeaderTerm(
                    txm->TxCcNodeId(), txm->tx_term_, txm->tx_status_))
            {
                RetrySubOperation(txm, &delete_out_of_old_range_data_op_);
            }
            else
            {
                DLOG(ERROR) << "DsSplitRangeOp is forced to finish: "
                               "TRANSACTION_NODE_NOT_LEADER";
                ForceToFinish(txm);
            }
        }
        else
        {
            if (!CheckLeaderTerm(
                    txm->TxCcNodeId(), txm->tx_term_, txm->tx_status_))
            {
                DLOG(ERROR) << "DsSplitRangeOp is forced to finish: "
                               "TRANSACTION_NODE_NOT_LEADER";
                ForceToFinish(txm);
            }
            else if (txm->tx_status_ == TxnStatus::Recovering)
            {
                assert(catalog_cc_entry_ != std::nullopt);
                catalog_post_read_op_.Reset(std::make_pair(
                    &catalog_cc_entry_->first, &catalog_cc_entry_->second));
                ForwardToSubOperation(txm, &catalog_post_read_op_);
            }
            else
            {
                ACTION_FAULT_INJECTOR("af_clean_log");

                FillTxLogForCleanLog(txm);
                ForwardToSubOperation(txm, &clean_log_op_);
            }
        }
    }
    else if (op_ == &catalog_post_read_op_)
    {
        if (FAULT_INJECTOR_CONDITION_WRAP(
                "cw_catalog_post_read_op",
                catalog_post_read_op_.hd_result_.IsError()))
        {
            if (CheckLeaderTerm(
                    txm->TxCcNodeId(), txm->tx_term_, txm->tx_status_))
            {
                RetrySubOperation(txm, &catalog_post_read_op_);
            }
            else
            {
                DLOG(ERROR) << "DsSplitRangeOp is forced to finish: "
                               "TRANSACTION_NODE_NOT_LEADER";
                ForceToFinish(txm);
            }
        }
        else
        {
            if (!CheckLeaderTerm(
                    txm->TxCcNodeId(), txm->tx_term_, txm->tx_status_))
            {
                DLOG(ERROR) << "DsSplitRangeOp is forced to finish: "
                               "TRANSACTION_NODE_NOT_LEADER";
                ForceToFinish(txm);
            }
            else
            {
                ACTION_FAULT_INJECTOR("af_clean_log");

                FillTxLogForCleanLog(txm);
                ForwardToSubOperation(txm, &clean_log_op_);
            }
        }
    }
    else if (op_ == &clean_log_op_)
    {
        if (FAULT_INJECTOR_CONDITION_WRAP(
                "cw_clean_log",
                clean_log_op_.hd_result_.IsError() &&
                    Sharder::Instance().CheckLeaderTerm(txm->TxCcNodeId(),
                                                        txm->tx_term_)))
        {
            RetrySubOperation(txm, &clean_log_op_);
        }
        else if (txm->tx_status_ == TxnStatus::Recovering)
        {
            txm->Reset();
            txm->tx_status_.store(TxnStatus::Finished,
                                  std::memory_order_release);
        }
        else
        {
            txm->state_stack_.pop_back();
            assert(txm->state_stack_.empty());
            if (Sharder::Instance().CheckLeaderTerm(txm->TxCcNodeId(),
                                                    txm->tx_term_))
            {
                // Prepare for commit
                txm->Commit();
            }
            uint32_t node_group_id = txm->TxCcNodeId();
            Sharder::Instance().UnpinNodeGroupData(node_group_id);
            txm->ds_split_range_op_ = nullptr;
        }
    }
}

ReleaseScanExtraLockOp::ReleaseScanExtraLockOp(TransactionExecution *txm)
    : hd_result_(txm),
      scan_open_tx_result_(nullptr),
      scan_close_tx_result_(nullptr)
{
}

void ReleaseScanExtraLockOp::Reset(std::vector<ScanBatchTuple> *scan_batch,
                                   size_t scan_batch_idx,
                                   const TableName *table_name,
                                   CcScanner *scanner,
                                   TxResult<size_t> *scan_open_tx_result,
                                   TxResult<Void> *scan_close_tx_result)
{
    hd_result_.Reset();
    hd_result_.Value().Clear();
    scan_open_tx_result_ = scan_open_tx_result;
    scan_close_tx_result_ = scan_close_tx_result;
    scan_batch_ = scan_batch;
    scan_batch_idx_ = scan_batch_idx;
    table_name_ = table_name;
    scanner_ = scanner;
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
}  // namespace txservice
