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

AcquireWriteOperation::AcquireWriteOperation(TransactionExecution *txm)
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
            finish_cnt_.fetch_add(1);
        };
    }
}

void AcquireWriteOperation::Reset(size_t acquire_write_cnt)
{
    finish_cnt_.store(0);
    fail_cnt_.store(0);
    remote_ack_cnt_.store(0);
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

            txm->PostAcquireWrite();
        }
    }
    else if (finish_cnt_.load() == acquire_write_cnt_)
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
                    fail_cnt_.fetch_add(1, std::memory_order_relaxed);
                }
            }
            else if (results_.at(idx).ErrorCode() == -1)
            {
                auto find_it = outdated_node_set.find(addr.NodeGroupId());
                if (find_it == outdated_node_set.end())
                {
                    Sharder::Instance().UpdateLeader(addr.NodeGroupId());
                    outdated_node_set.emplace(addr.NodeGroupId());
                }
            }
        }

        txm->PostAcquireWrite();
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
        txm->PostSetTs();
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
            // the request does not need post-processing.
            auto &vali_result = results_.at(idx);
            bool success = vali_result.ForceError();
            if (!success)
            {
                txm->rw_set_.DedupRead(*vali_cce_addr_.at(idx));
            }
        }

        txm->PostVali();
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

        txm->PostVali();
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

WriteToLog::WriteToLog(TransactionExecution *txm) : hd_result_(txm)
{
}

void WriteToLog::Forward(TransactionExecution *txm)
{
    if (hd_result_.IsFinished())
    {
        txm->PostWriteLog();
    }
}

void WriteToLog::Reset()
{
    hd_result_.Reset();
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
        txm->PostBegin();
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

        res.post_lambda_ = [this](CcHandlerResult<std::vector<TxId>> *)
        { finish_cnt_.fetch_add(1); };
    }

    for (size_t idx = 0; idx < 8; ++idx)
    {
        CcHandlerResult<Void> &res = write_results_.emplace_back(txm);

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
            res.post_lambda_ = [this](CcHandlerResult<Void> *)
            { finish_cnt_.fetch_add(1); };
        }
    }
}

void PostProcessOp::Forward(TransactionExecution *txm)
{
    if (finish_cnt_.load(std::memory_order_acquire) == acquire_write_cnt_ ||
        txm->IsTimeOut())
    {
        // Post-processing does not retry. A failed request leaves an orphan
        // lock/intent, which are recovered separately.
        txm->PostPostProcess();
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

AcquireAllOp::AcquireAllOp(TransactionExecution *txm)
{
    hd_results_.reserve(8);

    for (size_t idx = 0; idx < 8; ++idx)
    {
        auto &res = hd_results_.emplace_back(txm);

        res.post_lambda_ = [this](CcHandlerResult<AcquireAllResult> *hres)
        {
            if (hres->IsError())
            {
                fail_cnt_.fetch_add(1);
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

            res.post_lambda_ = [this](CcHandlerResult<AcquireAllResult> *hres)
            {
                if (hres->IsError())
                {
                    fail_cnt_.fetch_add(1);
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
    remote_ack_cnt_.store(node_cnt - 1);
    upload_cnt_ = node_cnt;
    Resize(node_cnt);
}

void AcquireAllOp::Forward(TransactionExecution *txm)
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
                CcHandlerResult<AcquireAllResult> &hd_result = hd_results_[idx];
                const AcquireAllResult &acquire_res = hd_result.Value();

                uint64_t ts = std::max(acquire_res.commit_ts_ + 1,
                                       acquire_res.last_vali_ts_ + 1);
                txm->commit_ts_bound_ = std::max(txm->commit_ts_bound_, ts);

                if (acquire_res.node_term_ < 0)
                {
                    hd_result.ForceError();
                }
            }

            txm->PostAcquireAll();
        }
    }
    else if (finish_cnt_.load() == upload_cnt_)
    {
        // TODO: for locking-based protocols, though the tx may be blocked
        // arbitrarily long, after all acquire requests are acknowledged, we
        // still need to periodically check liveness of the remote node.

        for (size_t nid = 0; nid < upload_cnt_; ++nid)
        {
            if (hd_results_[nid].ErrorCode() == -1)
            {
                Sharder::Instance().UpdateLeader(nid);
            }
        }

        if (fail_cnt_.load(std::memory_order_acquire) == 0)
        {
            for (size_t idx = 0; idx < upload_cnt_; ++idx)
            {
                const AcquireAllResult &acquire_res = hd_results_[idx].Value();
                uint64_t ts = std::max(acquire_res.commit_ts_ + 1,
                                       acquire_res.last_vali_ts_ + 1);
                txm->commit_ts_bound_ = std::max(txm->commit_ts_bound_, ts);

                assert(acquire_res.node_term_ > 0);
            }
        }

        txm->PostAcquireAll();
    }
}

PostWriteAllOp::PostWriteAllOp(TransactionExecution *txm)
{
    hd_results_.reserve(8);

    for (size_t idx = 0; idx < 8; ++idx)
    {
        CcHandlerResult<Void> &res = hd_results_.emplace_back(txm);

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
            res.post_lambda_ = [this](CcHandlerResult<Void> *)
            { finish_cnt_.fetch_add(1); };
        }
    }
}

void PostWriteAllOp::Forward(TransactionExecution *txm)
{
    if (finish_cnt_.load(std::memory_order_acquire) == upload_cnt_)
    {
        txm->PostPostWriteAll();
    }
    else if (txm->IsTimeOut())
    {
        for (size_t idx = 0; idx < upload_cnt_; ++idx)
        {
            hd_results_[idx].ForceError();
        }
        txm->PostPostWriteAll();
    }
}

DataStoreOp::DataStoreOp(TransactionExecution *txm) : result_(txm)
{
}

void DataStoreOp::Reset()
{
    result_.Reset();
}

void DataStoreOp::Forward(TransactionExecution *txm)
{
    if (result_.IsFinished())
    {
        txm->PostDataStoreOp();
    }
}

DsUpsertTableOp::DsUpsertTableOp(const TableName *table_name,
                                 const TableName *kv_table_name,
                                 bool is_deleted,
                                 TransactionExecution *txm)
    : DataStoreOp(txm),
      table_name_(table_name),
      kv_table_name_(kv_table_name),
      is_deleted_(is_deleted)
{
}

SchemaOp::SchemaOp(const TableName &table_name,
                   const char *image_ptr,
                   size_t image_len)
    : table_key_(table_name), catalog_rec_(image_ptr, image_len)
{
}

UpsertTableOp::UpsertTableOp(const TableName &table_name,
                             const TableName &kv_table_name,
                             const char *image_ptr,
                             size_t len,
                             bool is_deleted,
                             TransactionExecution *txm)
    : SchemaOp(table_name, image_ptr, len),
      is_deleted_(is_deleted),
      acquire_all_intent_op_(txm),
      prepare_log_op_(txm),
      post_all_intent_op_(txm),
      upsert_kv_table_op_(&table_name, &kv_table_name, is_deleted, txm),
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
}

void UpsertTableOp::Forward(TransactionExecution *txm)
{
    if (op_ == nullptr)
    {
        op_ = &acquire_all_intent_op_;
        txm->Process(acquire_all_intent_op_);
    }
    else if (op_ == &acquire_all_intent_op_)
    {
        if (acquire_all_intent_op_.fail_cnt_.load(std::memory_order_acquire) >
            0)
        {
            // Fails to acquire the write intent on the schema. Since write
            // intents only conflict with other writes, there must be
            // another tx trying to modify the same table's schema. Stops the
            // schema operation. Set the commit ts to 0 to signal that the
            // following post write operation releases all write intents.
            txm->commit_ts_ = 0;
            // Moves to the last operation that removes all write intents/locks.
            op_ = &post_all_lock_op_;
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

        FlushPrepareLog(txm);
    }
    else if (op_ == &prepare_log_op_)
    {
        if (prepare_log_op_.hd_result_.IsError())
        {
            // Fails to flush the prepare log. The schema operation is
            // considered failed if the prepare log is not flushed. The commit
            // ts is set to 0 to signal that the following post write operation
            // releases all write intents.
            txm->commit_ts_ = 0;
            // Moves to the last operation that removes all write intents/locks.
            op_ = &post_all_lock_op_;
            txm->Process(post_all_lock_op_);
        }
        else
        {
            op_ = &post_all_intent_op_;
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
            // After the prepare log is flushed, the schema op is guaranteed to
            // succeed and can only roll forward. Retry this step to install the
            // dirty schema in the tx service.
            txm->Process(post_all_intent_op_);
        }
        else if (is_deleted_)
        {
            // For DROP TABLE operations, the data store operation of deleting
            // the k-v table happens after the commit log is flushed.
            op_ = &acquire_all_lock_op_;
            txm->Process(acquire_all_lock_op_);
        }
        else
        {
            op_ = &upsert_kv_table_op_;
            // The post write request right after flushing the prepare log
            // installs the dirty schema in the tx service and returns a local
            // view (pointer) of the committed and dirty schema.
            upsert_kv_table_op_.table_schema_ =
                catalog_rec_.SchemaView()->second;
            txm->Process(upsert_kv_table_op_);
        }
    }
    else if (op_ == &upsert_kv_table_op_)
    {
        if (upsert_kv_table_op_.result_.IsError())
        {
            // The data store operation failed. Retry the operation.
            txm->Process(upsert_kv_table_op_);
        }
        else if (is_deleted_)
        {
            // For DROP TABLE statements, the data store operation happens after
            // the commit log is flushed and is the second to the last step.
            FlushCleanLog(txm);
        }
        else
        {
            op_ = &acquire_all_lock_op_;
            txm->Process(acquire_all_lock_op_);
        }
    }
    else if (op_ == &acquire_all_lock_op_)
    {
        if (acquire_all_lock_op_.fail_cnt_.load(std::memory_order_acquire) > 0)
        {
            // Fails to acquire the write lock. The schema operation can only
            // roll forward after flushing the prepare log. Retries the request.
            txm->Process(acquire_all_lock_op_);
        }
        else
        {
            FlushCommitLog(txm);
        }
    }
    else if (op_ == &commit_log_op_)
    {
        if (commit_log_op_.hd_result_.IsError())
        {
            // Fails to flush the commit log. Retries the operation.
            txm->Process(commit_log_op_);
        }
        else
        {
            op_ = &post_all_lock_op_;
            txm->Process(post_all_lock_op_);
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
            // The schema operation failed without flushing the prepare log. Do
            // not retry post-processing (release write intents) even if it
            // fails. Remaining write intents on the schema, if there are any,
            // will be recovered by individual cc nodes separately.
            txm->bool_resp_->Finish(false);

            txm->state_stack_.pop_back();
            assert(txm->state_stack_.empty());
            txm->schema_op_ = nullptr;
        }
        else if (failed)
        {
            // After the prepare log is flushed, the schema op is guaranteed to
            // succeed and can only roll forward. Retry this step to install the
            // committed schema and remove write locks.
            txm->Process(post_all_lock_op_);
        }
        else
        {
            // The tx's modification of the schema has succeeded. If the tx has
            // previously read the same schema and keeps a pointer in the read
            // set to the cc entry of the schema, removes it from the read set.
            // As a result, the tx will not try to release the read lock of the
            // schema when committing.
            const CcEntryAddr &schema_entry_addr =
                acquire_all_lock_op_.hd_results_[txm->txid_.GetNodeId()]
                    .Value()
                    .local_cce_addr_;
            txm->rw_set_.DedupRead(schema_entry_addr);

            if (is_deleted_)
            {
                op_ = &upsert_kv_table_op_;
                upsert_kv_table_op_.table_schema_ = nullptr;
                txm->Process(upsert_kv_table_op_);
            }
            else
            {
                FlushCleanLog(txm);
            }
        }
    }
    else if (op_ == &clean_log_op_)
    {
        if (clean_log_op_.hd_result_.IsError())
        {
            txm->Process(clean_log_op_);
        }
        else
        {
            txm->bool_resp_->Finish(true);
            txm->state_stack_.pop_back();
            assert(txm->state_stack_.empty());
            txm->schema_op_ = nullptr;
        }
    }
}

void UpsertTableOp::FlushPrepareLog(TransactionExecution *txm)
{
    op_ = &prepare_log_op_;

    prepare_log_op_.log_closure_.LogResponse()
        .mutable_write_log_response()
        ->clear_redirect();

    ::txlog::WriteLogRequest *prepare_log_rec =
        prepare_log_op_.log_closure_.LogRequest().mutable_write_log_request();

    prepare_log_rec->set_txn_number(txm->tx_number_);
    prepare_log_rec->set_commit_timestamp(txm->commit_ts_);

    auto prepare_schema_msg =
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
        prepare_schema_msg->set_catalog_blob(catalog_rec_.SchemaBlob());
    }
    prepare_schema_msg->set_stage(::txlog::SchemaOpMessage_Stage_PrepareSchema);

    auto &node_terms = *prepare_log_rec->mutable_node_terms();
    node_terms.clear();
    for (uint32_t nid = 0; nid < acquire_all_intent_op_.upload_cnt_; ++nid)
    {
        node_terms[nid] =
            acquire_all_intent_op_.hd_results_[nid].Value().node_term_;
    }

    txm->Process(prepare_log_op_);
}

void UpsertTableOp::FlushCommitLog(TransactionExecution *txm)
{
    op_ = &commit_log_op_;

    commit_log_op_.log_closure_.LogResponse()
        .mutable_write_log_response()
        ->clear_redirect();

    ::txlog::WriteLogRequest *commit_log_rec =
        commit_log_op_.log_closure_.LogRequest().mutable_write_log_request();

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
    // If a cc node fails over, the new node must restore write intents gained
    // prior to the prepare log and then replay operations between the prepare
    // log and the commit log, which in this case upgrade write intents to write
    // locks. So, there is no need to check the liveness of write locks when
    // flushing the commit log.
    commit_log_rec->mutable_node_terms()->clear();

    txm->Process(commit_log_op_);
}

void UpsertTableOp::FlushCleanLog(TransactionExecution *txm)
{
    op_ = &clean_log_op_;

    clean_log_op_.log_closure_.LogResponse()
        .mutable_write_log_response()
        ->clear_redirect();

    ::txlog::WriteLogRequest *clean_log_rec =
        clean_log_op_.log_closure_.LogRequest().mutable_write_log_request();
    ::txlog::SchemaOpMessage *clean_schema_msg =
        clean_log_rec->mutable_log_content()->mutable_schema_log();

    clean_schema_msg->set_table_name(table_key_.Name());
    clean_schema_msg->set_stage(::txlog::SchemaOpMessage_Stage_CleanSchema);
    clean_log_rec->mutable_node_terms()->clear();

    txm->Process(clean_log_op_);
}
}  // namespace txservice
