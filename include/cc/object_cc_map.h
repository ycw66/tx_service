#pragma once

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog_factory.h"
#include "cc_entry.h"
#include "cc_map.h"
#include "error_messages.h"
#include "local_cc_shards.h"
#include "non_blocking_lock.h"
#include "template_cc_map.h"
#include "tx_command.h"
#include "tx_key.h"
#include "tx_record.h"

namespace txservice
{
// whether skip accessing KV when cc map cache misses.
extern bool txservice_skip_kv;

template <typename KeyT, typename ValueT>
class ObjectCcMap : public TemplateCcMap<KeyT, ValueT>
{
public:
    ObjectCcMap(const ObjectCcMap &rhs) = delete;
    ~ObjectCcMap() = default;

    /**
     * @brief Constructs a new object cc map object. The object cc map has no
     * schema, so the schema's timestamp is set to 1 (the beginning of history).
     *
     * @param shard
     */
    ObjectCcMap(CcShard *shard,
                NodeGroupId cc_ng_id,
                const TableName &table_name,
                uint64_t schema_ts,
                const TableSchema *table_schema = nullptr,
                bool ccm_has_full_entries = false)
        : TemplateCcMap<KeyT, ValueT>(shard,
                                      cc_ng_id,
                                      table_name,
                                      schema_ts,
                                      table_schema,
                                      ccm_has_full_entries)
    {
        DLOG(INFO) << "creating ObjectCcmap on shard: " << shard_->core_id_
                   << ", table name: " << table_name.StringView()
                   << ", table_schema: " << table_schema_
                   << ", schema_ts: " << schema_ts_;
    }

    using CcMap::AcquireCceKeyLock;
    using CcMap::cc_ng_id_;
    using CcMap::ccm_has_full_entries_;
    using CcMap::last_dirty_commit_ts_;
    using CcMap::LockHandleForResumedRequest;
    using CcMap::MoveRequest;
    using CcMap::ReleaseCceLock;
    using CcMap::schema_ts_;
    using CcMap::shard_;
    using CcMap::table_name_;
    using CcMap::table_schema_;
    using TemplateCcMap<KeyT, ValueT>::Find;
    using TemplateCcMap<KeyT, ValueT>::FindEmplace;
    using TemplateCcMap<KeyT, ValueT>::Iterator;
    using TemplateCcMap<KeyT, ValueT>::KeySchema;
    using TemplateCcMap<KeyT, ValueT>::RecordSchema;
    using TemplateCcMap<KeyT, ValueT>::Type;
    using TemplateCcMap<KeyT, ValueT>::CleanEntry;

    bool Execute(ApplyCc &req)
    {
        TX_TRACE_ACTION_WITH_CONTEXT(
            (txservice::CcMap *) this,
            &req,
            [&req]() -> std::string
            {
                return std::string("\"cc_map_type\":\"template_cc_map\"")
                    .append(",\"tx_number\":")
                    .append(std::to_string(req.Txn()))
                    .append(",\"term\":")
                    .append(std::to_string(req.TxTerm()));
            });
        TX_TRACE_DUMP(&req);

        CcHandlerResult<ObjectCommandResult> *hd_res = req.Result();
        ObjectCommandResult &obj_result = hd_res->Value();
        CcEntryAddr &cce_addr = obj_result.cce_addr_;
        // TODO(lzx): replace "cmd_success" with TxCommand::IsPassed()
        bool &object_modified = obj_result.object_modified_;
        CcEntry<KeyT, ValueT> *cce = nullptr;
        CcPage<KeyT, ValueT> *ccp = nullptr;
        const KeyT *look_key = nullptr;
        KeyT decoded_key;

        uint32_t ng_id = req.NodeGroupId();
        TxNumber txn = req.Txn();
        int64_t ng_term = Sharder::Instance().LeaderTerm(ng_id);
        CODE_FAULT_INJECTOR("term_TemplateCcMap_Execute_ApplyCc", {
            LOG(INFO) << "FaultInject  term_TemplateCcMap_Execute_ApplyCc";
            ng_term = -1;
        });
        if (ng_term < 0)
        {
            LOG(INFO) << "ApplyCc, node_group(#" << ng_id
                      << ") term < 0, tx:" << txn;
            hd_res->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            return true;
        }

        // TODO(zkl): Read and PinRangeSlice, load from kv; wait for replay to
        //  finish

        LockType acquired_lock = LockType::NoLock;
        CcErrorCode err_code = CcErrorCode::NO_ERROR;
        bool override_kv_val = false;

        // Should create command before calling req.IsReadOnly().
        TxCommand *cmd = nullptr;
        if (req.IsLocal())
        {
            cmd = req.CommandPtr();
        }
        else
        {
            if (req.HasCommand())
            {
                cmd = req.remote_input_.cmd_;
            }
            else
            {
                std::unique_ptr<TxCommand> cmd_uptr =
                    CreateTxCommand(*req.CommandImage());
                cmd = cmd_uptr.get();
                req.SetCommand(cmd_uptr.release());
            }
        }

        // Always read the cce first to check if the object exists.
        CcOperation cc_op =
            req.IsReadOnly() ? CcOperation::Read : CcOperation::ReadForWrite;

        if (req.CcePtr() != nullptr)
        {
            // the request was blocked and is now unblocked and lock acquired
            cce = static_cast<CcEntry<KeyT, ValueT> *>(req.CcePtr());
            ccp = static_cast<CcPage<KeyT, ValueT> *>(cce->GetCcPage());

            if (req.block_type_ == ApplyCc::ApplyBlockType::BlockOnRead ||
                req.block_type_ == ApplyCc::ApplyBlockType::BlockOnWriteLock)
            {
                if (req.block_type_ ==
                    ApplyCc::ApplyBlockType::BlockOnWriteLock)
                {
                    cc_op = CcOperation::Write;
                }

                // For ON_KEY_OBJECT, we add lock regardless of whether the
                // record is deleted, so just pass RecordStatus::Normal.
                std::tie(acquired_lock, err_code) =
                    LockHandleForResumedRequest(cce,
                                                RecordStatus::Normal,
                                                &req,
                                                req.NodeGroupId(),
                                                ng_term,
                                                req.TxTerm(),
                                                cc_op,
                                                req.Isolation(),
                                                req.Protocol(),
                                                0,
                                                false);
            }
            else if (req.block_type_ == ApplyCc::ApplyBlockType::BlockOnFetch)
            {
                // Already finished lock acquire.
                err_code = CcErrorCode::NO_ERROR;
                acquired_lock = obj_result.lock_acquired_;
            }
            req.block_type_ = ApplyCc::ApplyBlockType::NoBlocking;
        }
        else if (cce_addr.CcePtr() == 0)
        {
            // first time the request is processed
            const TxKey *req_key = req.Key();
            if (req_key != nullptr)
            {
                look_key = req_key->GetKey<KeyT>();
            }
            else
            {
                const std::string *key_str = req.KeyImage();
                assert(key_str != nullptr);
                size_t offset = 0;
                decoded_key.Deserialize(key_str->data(), offset, KeySchema());
                look_key = &decoded_key;
            }

            auto it = FindEmplace(*look_key, false, req.IsReadOnly());
            cce = it->second;
            ccp = it.GetPage();
            if (cmd->GetBlockOperationType() == BlockOperation::Discard)
            {
                assert(!req.apply_and_commit_);
                if (cce != nullptr)
                {
                    cce->AbortBlockRequest(txn, CcErrorCode::TASK_EXPIRED);
                    cce->RecycleKeyLock(*shard_);
                }

                if (req.is_local_)
                {
                    // Only local command need to call SetFinished to avoid
                    // visit freed memory.
                    hd_res->SetError(CcErrorCode::TASK_EXPIRED);
                }

                return true;
            }

            if (cce == nullptr)
            {
                // The apply request needs a new cc entry but the cc map has
                // reached the maximal capacity. Blocks the request by putting
                // it into wait list until capacity is avaliable.
                shard_->EnqueueWaitList(&req);
                return false;
            }

            // Check if this cce does not exists in ccmap at all.
            // We need to double check that there is no dirty payload
            // status on the cce since a previous cmd might ignores
            // old payload value and directly applied dirty payload
            // status.
            if (cce->PayloadStatus() == RecordStatus::Unknown &&
                (!cce->GetKeyLock() ||
                 cce->DirtyPayloadStatus() == RecordStatus::NonExistent))
            {
                // if ccm contains all the ccentries, then unknown status means
                // that we can skip accessing kv store and return deleted status
                // directly.
                if (ccm_has_full_entries_ || txservice_skip_kv)
                {
                    cce->SetCommitTsPayloadStatus(1U, RecordStatus::Deleted);
                    cce->SetCkptTs(1U);
                }
                else
                {
                    // if command does not care about previous value of the key,
                    // we do not need to fetch kv value. We will assume the key
                    // does not exist.
                    if (!cmd->IgnoreKvValue())
                    {
                        shard_->FetchRecord(table_name_,
                                            table_schema_,
                                            TxKey(look_key),
                                            cce,
                                            this,
                                            cc_ng_id_,
                                            ng_term,
                                            &req);

                        req.block_type_ = ApplyCc::ApplyBlockType::BlockOnFetch;
                        // Acquire a read intent on this cce with the
                        // special txn to avoid cce being kicked out before
                        // fetch record returns.
                        cce->GetOrCreateKeyLock(shard_, this, ccp)
                            .AcquireReadIntent(fetch_record_txn);

                        if (metrics::enable_cache_hit_rate)
                        {
                            auto meter = shard_->GetMeter();
                            meter->Collect(
                                metrics::NAME_CACHE_HIT_OR_MISS_TOTAL,
                                1,
                                "miss");
                        }
                        return false;
                    }
                    else
                    {
                        // We will apply a DELETED dirty payload status after
                        // lock is acquired.
                        override_kv_val = true;
                    }
                }
            }
            if (metrics::enable_cache_hit_rate)
            {
                auto meter = shard_->GetMeter();
                meter->Collect(
                    metrics::NAME_CACHE_HIT_OR_MISS_TOTAL, 1, "hits");
            }
            req.SetCcePtr(cce);

            // If record expired in KV, it is possible the the cce reply list is
            // not empty due to replay command list and cce commit_ts version
            // mismatch
            if (cce->HasReplayCommandList() &&
                cce->PayloadStatus() == RecordStatus::Deleted &&
                cce->CommitTs() == 1)
            {
                ReplayTxnCmdList &replay_cmd_list = cce->ReplayCommandList();
                replay_cmd_list.Clear();
                cce->RecycleKeyLock(*shard_);
            }

            // For ON_KEY_OBJECT, we add lock regardless of whether the record
            // is deleted, so just pass RecordStatus::Normal.
            std::tie(acquired_lock, err_code) =
                AcquireCceKeyLock(cce,
                                  ccp,
                                  RecordStatus::Normal,
                                  &req,
                                  req.NodeGroupId(),
                                  ng_term,
                                  req.TxTerm(),
                                  cc_op,
                                  req.Isolation(),
                                  req.Protocol(),
                                  0,
                                  false);
        }
        else
        {
            assert(false);
            LOG(ERROR) << "!!!! Must not enter here !!!!";
        }

        switch (err_code)
        {
        case CcErrorCode::NO_ERROR:
        {
            // Lock acquired, set the result.
            obj_result.lock_acquired_ = acquired_lock;
            if (acquired_lock != LockType::NoLock)
            {
                assert(cce != nullptr);
                cce_addr.SetCce(
                    reinterpret_cast<uint64_t>(cce), ng_term, shard_->core_id_);
            }
            break;
        }
        case CcErrorCode::ACQUIRE_LOCK_BLOCKED:
        {
            // If the read request comes from a remote node, sends
            // acknowledgement to the sender when the request is
            // blocked.
            if (!req.IsLocal())
            {
                //                req.Acknowledge();
            }
            req.block_type_ = ApplyCc::ApplyBlockType::BlockOnRead;
            // Acquire lock fail should stop the execution of current
            // ApplyCc request since it's already in blocking queue.
            return false;
        }
        default:
        {
            // lock confilct: back off and retry.
            req.Result()->SetError(err_code);
            return true;
        }
        }
        if (override_kv_val)
        {
            // cmd that ignores kv value should be applied
            // regardless of current value.
            assert(cmd->ProceedOnNonExistentObject() &&
                   cmd->ProceedOnExistentObject() &&
                   acquired_lock == LockType::WriteIntent);
            // We will pretend that there's a delete on this cce
            // just before this cmd to ignore value in kv.
            cce->SetDirtyPayloadStatus(RecordStatus::Deleted);
            cce->SetCkptTs(1);
        }

        // check if the payload is expired
        TxObject *obj = static_cast<TxObject *>(cce->payload_.get());
        if (obj != nullptr && obj->HasTTL())
        {
            // If ttl is expired
            if (obj->GetTTL() < shard_->Now())
            {
                // return deleted
                if (req.IsReadOnly())
                {
                    obj_result.rec_status_ = RecordStatus::Deleted;
                    obj_result.commit_ts_ = obj->GetTTL();
                    hd_res->SetFinished();
                    return true;
                }
                // mark deleted on cce if not read only
                else
                {
                    cce->SetDirtyPayloadStatus(RecordStatus::Deleted);
                    cce->SetDirtyPayload(nullptr);
                    cce->SetPendingCmd(nullptr);
                    obj_result.ttl_expired_ = true;
                    obj_result.ttl_ = obj->GetTTL();
                }
            }
        }

        if (req.Isolation() > IsolationLevel::ReadCommitted ||
            !cmd->IsReadOnly())
        {
            // Create the dirty object if there is already a pending command on
            // this object.
            RecordStatus dirty_payload_status = cce->DirtyPayloadStatus();
            if (dirty_payload_status == RecordStatus::Uncreated)
            {
                auto var_cmd = cce->PendingCmd();
                TxCommand *pending_cmd = nullptr;
                if (std::holds_alternative<TxCommand *>(var_cmd))
                {
                    pending_cmd = std::get<TxCommand *>(var_cmd);
                }
                else
                {
                    pending_cmd =
                        std::get<std::unique_ptr<TxCommand>>(var_cmd).get();
                }
                std::unique_ptr<ValueT> dirty_payload = cce->DirtyPayload();
                // Since pending_cmd_ exists, the payload must also exist.
                // Otherwise, the dirty payload should have already been created
                // by the last command.
                assert(pending_cmd != nullptr);
                assert(cce->PayloadStatus() == RecordStatus::Normal &&
                       cce->payload_ != nullptr);

                std::tie(dirty_payload, dirty_payload_status) =
                    CreateDirtyPayloadFromExistingPayload(cce->payload_.get());
                assert(dirty_payload_status == RecordStatus::Normal);

                // Commit the pending command.
                CommitCommandOnDirtyPayload(
                    dirty_payload, dirty_payload_status, *pending_cmd);
                cce->SetDirtyPayload(std::move(dirty_payload));
                cce->SetDirtyPayloadStatus(dirty_payload_status);
                cce->SetPendingCmd(nullptr);
            }
        }

        // Now we know whether the object exists or not
        bool object_not_exist;
        if (req.Isolation() == IsolationLevel::ReadCommitted &&
            cmd->IsReadOnly())
        {
            // Read only commands in read committed isolation level just checks
            // the payload. `Deleted` status means current payload is deleted,
            // `Unknown` status means the current payload is being created.
            object_not_exist = cce->PayloadStatus() == RecordStatus::Deleted ||
                               cce->PayloadStatus() == RecordStatus::Unknown;
        }
        else
        {
            // If dirty payload exists, use dirty_payload_status. Use payload
            // status only if dirty payload doesn't exist.
            object_not_exist =
                cce->DirtyPayloadStatus() == RecordStatus::Deleted ||
                (cce->DirtyPayloadStatus() == RecordStatus::NonExistent &&
                 cce->PayloadStatus() == RecordStatus::Deleted);
        }

        // This branch processes and returns the results for all read-only
        // commands.
        if (cmd->IsReadOnly())
        {
            // Early return logic for read-only command.

            if (object_not_exist)
            {
                assert(!cmd->ProceedOnNonExistentObject());

                obj_result.rec_status_ = RecordStatus::Deleted;
            }
            else if (!cmd->ProceedOnExistentObject())
            {
                obj_result.rec_status_ = RecordStatus::Normal;
            }
            // Object exists and proceeds
            else if (req.Isolation() == IsolationLevel::ReadCommitted)
            {
                assert(cce->PayloadStatus() == RecordStatus::Normal);
                assert(cce->payload_ != nullptr);
                ValueT &object = *cce->payload_;
                cmd->ExecuteOn(object);
                obj_result.rec_status_ = cce->PayloadStatus();
            }
            else
            {
                assert(req.Isolation() > IsolationLevel::ReadCommitted);

                RecordStatus dirty_payload_status = cce->DirtyPayloadStatus();
                if (dirty_payload_status == RecordStatus::Normal)
                {
                    std::unique_ptr<ValueT> dirty_payload = cce->DirtyPayload();
                    assert(dirty_payload != nullptr);

                    // Temporary object exists, execute and commit the command
                    // on the temporary object.
                    ValueT &dirty_object = *dirty_payload;
                    cmd->ExecuteOn(dirty_object);
                    cce->SetDirtyPayload(std::move(dirty_payload));
                    cce->SetDirtyPayloadStatus(dirty_payload_status);
                    obj_result.rec_status_ = dirty_payload_status;
                }
                else
                {
                    assert(cce->PayloadStatus() == RecordStatus::Normal);
                    assert(cce->IsNullPendingCmd());
                    ValueT &object = *cce->payload_;
                    cmd->ExecuteOn(object);
                    obj_result.rec_status_ = cce->PayloadStatus();
                }
            }

            if (req.apply_and_commit_)
            {
                // Release and try to recycle the lock.
                ReleaseCceLock(
                    cce->GetKeyLock(), cce, txn, ng_id, acquired_lock);
                obj_result.lock_acquired_ = LockType::NoLock;
            }

            assert(obj_result.rec_status_ != RecordStatus::Unknown);
            obj_result.commit_ts_ = cce->CommitTs();
            hd_res->SetFinished();
            return true;
        }

        // This branch processes read-write commands that have not acquired
        // writelock yet.
        if (acquired_lock != LockType::WriteLock)
        {
            bool procceed =
                (!object_not_exist && cmd->ProceedOnExistentObject()) ||
                (object_not_exist && cmd->ProceedOnNonExistentObject());

            if (procceed)
            {
                // Upgrade to write lock
                std::tie(acquired_lock, err_code) =
                    AcquireCceKeyLock(cce,
                                      ccp,
                                      RecordStatus::Normal,
                                      &req,
                                      req.NodeGroupId(),
                                      ng_term,
                                      req.TxTerm(),
                                      CcOperation::Write,
                                      req.Isolation(),
                                      req.Protocol(),
                                      0,
                                      false);
            }
            else
            {
                // Early return logic for read-write command.
                if (req.apply_and_commit_)
                {
                    // Release and try to recycle the lock.
                    ReleaseCceLock(
                        cce->GetKeyLock(), cce, txn, ng_id, acquired_lock);
                    obj_result.lock_acquired_ = LockType::NoLock;
                }

                obj_result.rec_status_ = object_not_exist
                                             ? RecordStatus::Deleted
                                             : RecordStatus::Normal;

                obj_result.commit_ts_ = cce->CommitTs();
                hd_res->SetFinished();
                return true;
            }

            switch (err_code)
            {
            case CcErrorCode::NO_ERROR:
            {
                // lock acquired
                assert(acquired_lock == LockType::WriteLock);
                obj_result.lock_acquired_ = acquired_lock;
                break;
            }
            case CcErrorCode::ACQUIRE_LOCK_BLOCKED:
            {
                // If the read request comes from a remote node, sends
                // acknowledgement to the sender when the request is
                // blocked.
                if (!req.IsLocal())
                {
                    //                req.Acknowledge();
                }
                req.block_type_ = ApplyCc::ApplyBlockType::BlockOnWriteLock;
                // Acquire lock fail should stop the execution of current
                // ApplyCc request since it's already in blocking queue.
                return false;
            }
            default:
            {
                // lock confilct: back off and retry.
                req.Result()->SetError(err_code);
                return true;
            }
            }
            assert(ccp != nullptr);
        }

        assert(obj_result.lock_acquired_ == LockType::WriteLock);

        RecordStatus dirty_payload_status = cce->DirtyPayloadStatus();
        if (object_not_exist)
        {
            assert(cmd->ProceedOnNonExistentObject());

            // Create an empty temporary object to process the commands, the
            // dirty payload will be uploaded to payload in PostWriteCc if
            // the txn commits.
            std::unique_ptr<ValueT> dirty_payload = cce->DirtyPayload();
            std::tie(dirty_payload, dirty_payload_status) =
                CreateDirtyPayloadFromCommand(cmd);
            cce->SetDirtyPayload(std::move(dirty_payload));
            cce->SetDirtyPayloadStatus(dirty_payload_status);
            cce->SetPendingCmd(nullptr);
        }

        ExecResult exec_rst = ExecResult::Fail;
        if (dirty_payload_status == RecordStatus::Normal)
        {
            std::unique_ptr<ValueT> dirty_payload = cce->DirtyPayload();
            assert(dirty_payload != nullptr);

            // Temporary object exists, execute and commit the command on
            // the temporary object.
            ValueT &dirty_object = *dirty_payload;
            exec_rst = cmd->ExecuteOn(dirty_object);
            object_modified = (exec_rst == ExecResult::Write);

            if (object_modified)
            {
                CommitCommandOnDirtyPayload(
                    dirty_payload, dirty_payload_status, *cmd);
            }
            cce->SetDirtyPayload(std::move(dirty_payload));
            cce->SetDirtyPayloadStatus(dirty_payload_status);
        }
        else if (cce->PayloadStatus() == RecordStatus::Normal)
        {
            // The dirty payload does not exist. This is the first command.
            // Execute and copy the command. The command will be committed
            // in PostWriteCc if the txn commits.
            assert(cce->IsNullPendingCmd());
            assert(cce->payload_ != nullptr);
            ValueT &object = *cce->payload_;
            exec_rst = cmd->ExecuteOn(object);
            object_modified = (exec_rst == ExecResult::Write);

            if (object_modified && !req.apply_and_commit_)
            {
                // Copy the command to be committed in PostWriteCc or when
                // executing subsequent commands of the same txn.
                if (req.IsLocal())
                {
                    if (cmd->IsVolatile())
                    {
                        // If this command is volatile, it will need to clone a
                        // new instance to ensure it can be commit in
                        // PostWriteCc.
                        cce->SetPendingCmd(cmd->Clone());
                    }
                    else
                    {
                        // If the command is exist until transaction committed,
                        // it does not need to clone a new instance and use
                        // original cmd in PostWriteCc.
                        cce->SetPendingCmd(cmd);
                    }
                }
                else
                {
                    // For remote ApplyCC, it will transfer the ownership from
                    // ApplyCC into pending cmd, so ApplyCC does not need to
                    // release this command.
                    cce->SetPendingCmd(std::unique_ptr<TxCommand>(cmd));
                    req.RemoveOwnership();
                }

                // The object is being modified, set dirty_payload_status_ to
                // Uncreated so that a temporary object will be created when
                // processing subsequent commands of the same txn. In
                // PostWriteCc, the original object will be replaced by the
                // temporary object if the txn commits.
                cce->SetDirtyPayloadStatus(RecordStatus::Uncreated);
            }
        }

        if (exec_rst == ExecResult::Block)
        {
            assert(!req.apply_and_commit_);
            cce->PushBlockRequest(&req);
            cce->SetDirtyPayload(nullptr);
            cce->SetDirtyPayloadStatus(RecordStatus::NonExistent);
            ReleaseCceLock(cce->GetKeyLock(), cce, txn, ng_id, acquired_lock);
            obj_result.lock_acquired_ = LockType::NoLock;
            return false;
        }
        else if (exec_rst == ExecResult::Unlock)
        {
            assert(!req.apply_and_commit_);
            cce->SetDirtyPayload(nullptr);
            cce->SetDirtyPayloadStatus(RecordStatus::NonExistent);
            ReleaseCceLock(cce->GetKeyLock(), cce, txn, ng_id, acquired_lock);
            obj_result.lock_acquired_ = LockType::NoLock;
            obj_result.commit_ts_ = shard_->Now();
            obj_result.rec_status_ = RecordStatus::Deleted;
            hd_res->SetFinished();
            return true;
        }
        else if (req.apply_and_commit_)
        {
            if (object_modified)
            {
                // Skipping writing log, do the PostWrite and release the lock.
                assert(acquired_lock == LockType::WriteLock);
                RecordStatus status = cce->PayloadStatus();
                if (dirty_payload_status == RecordStatus::Normal ||
                    dirty_payload_status == RecordStatus::Deleted)
                {
                    // Dirty payload exists. Use it to replace payload.
                    cce->payload_ = cce->DirtyPayload();
                    status = dirty_payload_status;
                }
                else
                {
                    CommitCommandOnPayload(cce->payload_, status, *cmd);
                }

                // Reset the dirty status.
                cce->SetDirtyPayload(nullptr);
                cce->SetDirtyPayloadStatus(RecordStatus::NonExistent);
                cce->SetPendingCmd(nullptr);

                // Set commit ts based on the TxTs since there is no PostWriteCc
                // if apply_and_commit_.
                const uint64_t commit_ts =
                    std::max({cce->CommitTs() + 1, req.TxTs(), shard_->Now()});
                cce->SetCommitTsPayloadStatus(commit_ts, status);

                if (last_dirty_commit_ts_ < commit_ts)
                {
                    last_dirty_commit_ts_ = commit_ts;
                }
                if (commit_ts > ccp->last_dirty_commit_ts_)
                {
                    ccp->last_dirty_commit_ts_ = commit_ts;
                }
            }
            else
            {
                cce->SetDirtyPayload(nullptr);
                cce->SetDirtyPayloadStatus(RecordStatus::NonExistent);
            }

            // Release and try to recycle the lock.
            ReleaseCceLock(cce->GetKeyLock(), cce, txn, ng_id, acquired_lock);
            obj_result.lock_acquired_ = LockType::NoLock;
            if (object_modified)
            {
                cce->PopBlockRequest(shard_, cce->payload_.get());
            }
        }

        // Updates last_vali_ts after successfully acquiring the write
        // lock such that it is not smaller than the current time of
        // the shard. The net effect is that the tx acquiring the write
        // lock is forced not to commit at a time earlier than the
        // clock of this cc node, even if the clock of the tx's
        // coordinator node drifts and falls behind. Checkpointing
        // relies on this property to avoid picking a checkpoint ts in
        // this shard that may overlap with the ongoing tx.
        obj_result.last_vali_ts_ =
            std::max(shard_->LastReadTs(), shard_->Now());

        if (cce->PayloadStatus() == RecordStatus::Unknown)
        {
            // If this command ignores the old kv value, just pass
            // in as deleted and current ts so that the tx will
            // commit at a larger commit ts.
            obj_result.commit_ts_ = shard_->Now();
            obj_result.rec_status_ = RecordStatus::Deleted;
        }
        else
        {
            obj_result.commit_ts_ = cce->CommitTs();
            obj_result.rec_status_ = cce->PayloadStatus();
        }

        hd_res->SetFinished();
        return true;
    }

    bool Execute(PostWriteCc &req)
    {
        TX_TRACE_ACTION_WITH_CONTEXT(
            (txservice::CcMap *) this,
            &req,
            [&req]() -> std::string
            {
                return std::string("\"cc_map_type\":\"template_cc_map\"")
                    .append(",\"tx_number\":")
                    .append(std::to_string(req.Txn()))
                    .append(",\"term\":")
                    .append("0");
            });
        TX_TRACE_DUMP(&req);

        TxNumber txn = req.Txn();
        uint64_t commit_ts = req.CommitTs();
        OperationType op_type = req.GetOperationType();
        assert(op_type == OperationType::CommitCommands);
        (void) op_type;

        const CcEntryAddr *cce_addr = req.CceAddr();

        CcEntry<KeyT, ValueT> *cce =
            reinterpret_cast<CcEntry<KeyT, ValueT> *>(cce_addr->CcePtr());

        // check that this txn is lock owner
        NonBlockingLock *lk = cce->GetKeyLock();
        if (lk == nullptr || !lk->HasWriteLock() || lk->WriteLockTx() != txn)
        {
            req.Result()->SetFinished();
            return true;
        }

        CcPage<KeyT, ValueT> *ccp =
            static_cast<CcPage<KeyT, ValueT> *>(cce->GetCcPage());
        assert(ccp != nullptr);
        if (commit_ts > 0)
        {
            RecordStatus dirty_payload_status = cce->DirtyPayloadStatus();
            RecordStatus payload_status = cce->PayloadStatus();
            // The txn commits. Upload the change.
            if (dirty_payload_status == RecordStatus::Normal ||
                dirty_payload_status == RecordStatus::Deleted)
            {
                // Dirty payload exists. Use it to replace payload.
                payload_status = dirty_payload_status;
                cce->payload_ = cce->DirtyPayload();
            }
            else
            {
                // Commit the pending command.
                auto var_cmd = cce->PendingCmd();
                TxCommand *pending_cmd = nullptr;
                if (std::holds_alternative<TxCommand *>(var_cmd))
                {
                    pending_cmd = std::get<TxCommand *>(var_cmd);
                }
                else
                {
                    pending_cmd =
                        std::get<std::unique_ptr<TxCommand>>(var_cmd).get();
                }

                if (pending_cmd != nullptr)
                {
                    assert(cce->payload_ != nullptr);
                    CommitCommandOnPayload(
                        cce->payload_, payload_status, *pending_cmd);
                }
                else
                {
                    assert(false);
                }
            }

            cce->SetCommitTsPayloadStatus(commit_ts, payload_status);
            if (last_dirty_commit_ts_ < commit_ts)
            {
                last_dirty_commit_ts_ = commit_ts;
            }

            if (commit_ts > ccp->last_dirty_commit_ts_)
            {
                ccp->last_dirty_commit_ts_ = commit_ts;
            }
        }

        // Reset the dirty status.
        cce->SetDirtyPayload(nullptr);
        cce->SetDirtyPayloadStatus(RecordStatus::NonExistent);
        cce->SetPendingCmd(nullptr);

        ReleaseCceLock(lk, cce, txn, req.NodeGroupId(), LockType::WriteLock);
        cce->PopBlockRequest(shard_, cce->payload_.get());
        if (cce->PayloadStatus() == RecordStatus::Unknown && cce->IsFree())
        {
            // If the finished cmd ignores kv value and the tx aborts, we will
            // end up with a cce with unknown status after dirty payload is
            // cleared. Remove the unused cce.
            CleanEntry(cce, ccp);
        }
        req.Result()->SetFinished();
        return true;
    }

    bool Execute(UploadBatchCc &req) override
    {
        TX_TRACE_ACTION_WITH_CONTEXT(
            (txservice::CcMap *) this,
            &req,
            [&req]() -> std::string
            {
                return std::string("\"cc_map_type\":\"template_cc_map\"")
                    .append(",\"term\":")
                    .append(std::to_string(req.CcNgTerm()));
            });
        TX_TRACE_DUMP(&req);

        if (!shard_->IsBucketsMigrating())
        {
            req.SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
        }
        auto entry_tuples = req.EntryTuple();
        size_t batch_size = req.BatchSize();

        const KeyT *key = nullptr;
        KeyT decoded_key;
        ValueT decoded_rec;
        TxRecord::Uptr object_uptr = nullptr;
        uint64_t commit_ts = 0;
        RecordStatus rec_status = RecordStatus::Normal;

        // object cc map only handles remote upload batch cc reqeust for now.
        auto &resume_pos = req.GetPausedPosition(shard_->core_id_);
        size_t key_pos = std::get<0>(resume_pos);
        size_t key_offset = std::get<1>(resume_pos);
        size_t rec_offset = std::get<2>(resume_pos);
        size_t ts_offset = std::get<3>(resume_pos);
        size_t status_offset = std::get<4>(resume_pos);
        size_t hash = 0;

        CcEntry<KeyT, ValueT> *cce;
        CcPage<KeyT, ValueT> *cc_page = nullptr;
        size_t next_key_offset = 0;
        size_t next_rec_offset = 0;
        size_t next_ts_offset = 0;
        size_t next_status_offset = 0;
        for (size_t cnt = 0;
             key_pos < batch_size && cnt < UploadBatchCc::UploadBatchBatchSize;
             ++key_pos, ++cnt)
        {
            next_key_offset = key_offset;
            next_rec_offset = rec_offset;
            next_ts_offset = ts_offset;
            next_status_offset = status_offset;

            auto [key_str, rec_str, ts_str, status_str] = *entry_tuples;
            // deserialize key
            decoded_key.Deserialize(
                key_str.data(), next_key_offset, KeySchema());
            key = &decoded_key;
            // deserialize record status
            rec_status =
                *((RecordStatus *) (status_str.data() + next_status_offset));
            next_status_offset += sizeof(RecordStatus);
            if (rec_status == RecordStatus::Normal)
            {
                // deserialize rec
                object_uptr = decoded_rec.DeserializeObject(rec_str.data(),
                                                            next_rec_offset);
            }

            // deserialize commit ts
            commit_ts = *((uint64_t *) (ts_str.data() + next_ts_offset));
            next_ts_offset += sizeof(uint64_t);

            hash = key->Hash();
            uint16_t bucket_id = hash & 0x3FFF;
            size_t core_idx = (hash & 0x3FF) % shard_->core_cnt_;
            if (!(core_idx == shard_->core_id_) || commit_ts <= 1 ||
                !shard_->GetBucketInfo(bucket_id, cc_ng_id_)
                     ->AcceptsUploadBatch())
            {
                // Skip the key if
                // 1) key does not land on this core
                // 2) commit ts is invalid
                // 3) bucket stops accepting upload batch reqeust
                // Move to next key.
                key_offset = next_key_offset;
                rec_offset = next_rec_offset;
                ts_offset = next_ts_offset;
                status_offset = next_status_offset;
                continue;
            }

            auto it = FindEmplace(*key);
            cce = it->second;
            cc_page = it.GetPage();
            if (cce == nullptr)
            {
                DLOG(WARNING) << "!!!WARNING!!! UploadBatchCc OOM on core: "
                              << shard_->core_id_ << ". Txn: " << req.Txn()
                              << ", table name: " << this->table_name_.Trace();
                // This cc shard has reached max memory limit. Currently upload
                // batch for object cc map is only used for sending cache to new
                // data owner during migration. This is a best effort try and
                // does not need to be successful. Just return immediately.
                return req.SetError(CcErrorCode::OUT_OF_MEMORY);
            }

            assert(commit_ts > 1);
            if (cce->CommitTs() >= commit_ts)
            {
                // Concurrent upsert_tx has write the latest value, so discard
                // the old value directly. For example, during add index
                // transaction, we will write the packed sk data that generate
                // from old pk records into the new sk ccmap, and before this
                // post write request, we do not acquire the write lock on this
                // TxKey, so this value has been updated by a concurrent
                // transaction.
                key_offset = next_key_offset;
                rec_offset = next_rec_offset;
                ts_offset = next_ts_offset;
                status_offset = next_status_offset;
                continue;
            }

            if (rec_status == RecordStatus::Normal)
            {
                cce->payload_.reset(
                    static_cast<ValueT *>(object_uptr.release()));
                object_uptr = nullptr;
            }
            else
            {
                cce->payload_ = nullptr;
            }

            cce->SetCommitTsPayloadStatus(commit_ts, rec_status);
            if (req.Kind() == UploadBatchType::DirtyBucketData)
            {
                cce->SetCkptTs(commit_ts);
            }

            if (cce->HasReplayCommandList())
            {
                ReplayTxnCmdList &replay_cmd_list = cce->ReplayCommandList();
                // Clear cmds with smaller version than uploaded version.
                for (auto it = replay_cmd_list.txn_cmd_list_.begin();
                     it != replay_cmd_list.txn_cmd_list_.end();)
                {
                    if (it->obj_version_ >= commit_ts)
                    {
                        break;
                    }
                    it = replay_cmd_list.txn_cmd_list_.erase(it);
                }

                replay_cmd_list.cur_version_ = commit_ts;
                TryCommitReplayCommands(
                    cce->payload_, replay_cmd_list, commit_ts);
            }

            if (cce->payload_)
            {
                cce->SetCommitTsPayloadStatus(commit_ts, RecordStatus::Normal);
            }
            else
            {
                cce->SetCommitTsPayloadStatus(commit_ts, RecordStatus::Deleted);
            }
            DLOG_IF(INFO, TRACE_OCC_ERR)
                << "UploadBatchCc, txn:" << req.Txn() << " ,cce: " << cce
                << " ,commit_ts: " << commit_ts;

            if (commit_ts > last_dirty_commit_ts_)
            {
                last_dirty_commit_ts_ = commit_ts;
            }
            if (commit_ts > cc_page->last_dirty_commit_ts_)
            {
                cc_page->last_dirty_commit_ts_ = commit_ts;
            }

            // update the key offset
            key_offset = next_key_offset;
            rec_offset = next_rec_offset;
            ts_offset = next_ts_offset;
            status_offset = next_status_offset;
        }
        if (key_pos < batch_size)
        {
            // Only insert UploadBatchBatchSize keys in one round.  set the
            // paused key to mark resume position and put the request into cc
            // queue again.
            req.SetPausedPosition(shard_->core_id_,
                                  key_pos,
                                  key_offset,
                                  rec_offset,
                                  ts_offset,
                                  status_offset);
            shard_->Enqueue(shard_->LocalCoreId(), &req);
            return false;
        }

        return req.SetFinish();
    }

    bool Execute(UploadTxCommandsCc &req)
    {
        TxNumber txn = req.Txn();
        uint64_t obj_version = req.ObjectVersion();
        uint64_t commit_ts = req.CommitTs();
        bool has_overwrite = req.HasOverWrite();
        const std::vector<std::string> *cmd_str_list = req.CommandList();

        const CcEntryAddr *cce_addr = req.CceAddr();

        CcEntry<KeyT, ValueT> *cce =
            reinterpret_cast<CcEntry<KeyT, ValueT> *>(cce_addr->CcePtr());

        // check that this txn is lock owner
        NonBlockingLock *lk = cce->GetKeyLock();
        if (lk == nullptr || !lk->HasWriteLock() || lk->WriteLockTx() != txn)
        {
            assert(false);
            req.Result()->SetFinished();
            return true;
        }

        if (commit_ts > 0)
        {
            CcPage<KeyT, ValueT> *ccp =
                static_cast<CcPage<KeyT, ValueT> *>(cce->GetCcPage());

            std::vector<std::unique_ptr<TxCommand>> cmd_list;
            cmd_list.reserve(cmd_str_list->size());
            for (const std::string &cmd_str : *cmd_str_list)
            {
                std::unique_ptr<TxCommand> tx_cmd = CreateTxCommand(cmd_str);
                cmd_list.emplace_back(std::move(tx_cmd));
            }

            TxnCmd txn_cmd(
                obj_version, commit_ts, has_overwrite, std::move(cmd_list));

            ReplayTxnCmdList &replay_cmd_list = cce->ReplayCommandList();

            // Emplace txn_cmd and try to commit all pending commands.
            uint64_t commit_version = cce->CommitTs();
            RecordStatus payload_status = cce->PayloadStatus();
            if (txn_cmd.obj_version_ >= commit_version)
            {
                EmplaceAndCommitReplayTxnCommand(cce->payload_,
                                                 replay_cmd_list,
                                                 txn_cmd,
                                                 commit_version,
                                                 payload_status);
                cce->SetCommitTsPayloadStatus(commit_version, payload_status);
            }

            // if replay_cmd_list is null, key_lock_extra_data will be recycled
            // when release lock.

            // Must update dirty_commit_ts. Otherwise, this entry may be
            // skipped by checkpointer.
            if (commit_ts > last_dirty_commit_ts_)
            {
                last_dirty_commit_ts_ = commit_ts;
            }
            if (commit_ts > last_dirty_commit_ts_)
            {
                last_dirty_commit_ts_ = commit_ts;
            }
            if (commit_ts > ccp->last_dirty_commit_ts_)
            {
                ccp->last_dirty_commit_ts_ = commit_ts;
            }
        }

        ReleaseCceLock(lk, cce, txn, req.NodeGroupId(), LockType::WriteLock);
        req.Result()->SetFinished();
        return true;
    }

    bool Execute(ReplayLogCc &req)
    {
        TX_TRACE_ACTION_WITH_CONTEXT(
            (txservice::CcMap *) this,
            &req,
            [&req]() -> std::string
            {
                return std::string("\"cc_map_type\":\"template_cc_map\"")
                    .append(",\"tx_number\":")
                    .append(std::to_string(req.Txn()))
                    .append(",\"term\":")
                    .append("0");
            });
        TX_TRACE_DUMP(&req);

        // If the log record's commit ts is smaller than that of the cc map,
        // this record is generated before the latest schema of the table
        // and hence should skip the replay process.
        uint64_t commit_ts = req.CommitTs();
        if (commit_ts < schema_ts_)
        {
            DLOG(INFO) << "discard log, commit_ts: " << commit_ts
                       << ", schema_ts: " << schema_ts_;
            req.SetFinish();
            return true;
        }

        KeyT key;
        size_t offset = req.Offset();
        const std::string_view &log_blob = req.LogContentView();
        uint16_t next_core = req.NextCore();
        req.SetNextCore(UINT16_MAX);

        while (offset < log_blob.size())
        {
            size_t prev_offset = offset;
            // the format of log_blob is: key_str, object_version, commands str
            // length, commands str
            key.Deserialize(log_blob.data(), offset, KeySchema());
            const uint64_t obj_version =
                *reinterpret_cast<decltype(obj_version) *>(log_blob.data() +
                                                           offset);
            offset += sizeof(obj_version);
            const uint32_t cmds_len = *reinterpret_cast<decltype(cmds_len) *>(
                log_blob.data() + offset);
            offset += sizeof(cmds_len);

            // If key not belongs to current ng, skip it.
            uint64_t key_hash = key.Hash();
            uint16_t bucket_id =
                Sharder::Instance().MapKeyHashToBucketId(key_hash);
            if (shard_->GetBucketOwner(bucket_id, cc_ng_id_) != cc_ng_id_)
            {
                offset += cmds_len;
                continue;
            }

            uint16_t core_id = (key_hash & 0x3FF) % shard_->core_cnt_;
            if (core_id != shard_->core_id_)
            {
                // Skips the key in the log record that is not sharded to this
                // core.
                offset += cmds_len;
                if (shard_->core_id_ == req.FirstCore() ||
                    (core_id != req.FirstCore() && core_id > shard_->core_id_))
                {
                    // Move to the smallest unvisited core id
                    next_core = std::min(core_id, next_core);
                }
                continue;
            }

            auto it = FindEmplace(key);
            CcEntry<KeyT, ValueT> *cce = it->second;
            CcPage<KeyT, ValueT> *ccp = it.GetPage();

            if (cce == nullptr)
            {
                // The cc map has
                // reached the maximal capacity. Blocks the request by putting
                // it into wait list until capacity is avaliable.
                req.SetOffset(prev_offset);
                req.SetNextCore(next_core);
                shard_->EnqueueWaitList(&req);
                return false;
            }

            bool has_overwrite =
                *reinterpret_cast<const uint8_t *>(log_blob.data() + offset);
            offset += sizeof(uint8_t);

            DLOG(INFO) << "replay log key: " << key.ToString()
                       << ", obj_ver: " << obj_version
                       << ", commit ts: " << commit_ts
                       << ", cmds len: " << cmds_len << ", cmds str: "
                       << std::string_view(log_blob.data() + offset, cmds_len)
                       << " has_overwrite: " << has_overwrite;

            // load payload from kvstore before committing pending commands.
            // If there's already read intent on cce, that means a previous
            // replay cc has already sent fetch record.
            if (!has_overwrite &&
                cce->PayloadStatus() == RecordStatus::Unknown &&
                (!cce->GetKeyLock() || cce->GetKeyLock()->IsEmpty()))
            {
                int64_t cc_ng_candid_term =
                    Sharder::Instance().CandidateLeaderTerm(cc_ng_id_);
                int64_t cc_ng_term = Sharder::Instance().LeaderTerm(cc_ng_id_);
                int64_t ng_term = std::max(cc_ng_candid_term, cc_ng_term);
                assert(ng_term > 0);
                // If kv is skipped then log should always be skipped too.
                assert(!txservice_skip_kv);

                // load payload asynchronously, pass in null as requester cc
                // since we will buffer the cmd in replay cmd list so there's no
                // need to put this req back in queue after record is fetched.
                shard_->FetchRecord(table_name_,
                                    table_schema_,
                                    TxKey(&key),
                                    cce,
                                    this,
                                    cc_ng_id_,
                                    ng_term,
                                    nullptr);
                // Acquire a read intent on this cce with the
                // special txn to avoid cce being kicked out before
                // fetch record returns.
                cce->GetOrCreateKeyLock(shard_, this, ccp)
                    .AcquireReadIntent(fetch_record_txn);
            }
            // extract command list
            const uint16_t cmd_cnt = *reinterpret_cast<decltype(cmd_cnt) *>(
                log_blob.data() + offset);
            offset += sizeof(cmd_cnt);
            std::vector<std::unique_ptr<TxCommand>> cmd_list;
            for (size_t i = 0; i < cmd_cnt; i++)
            {
                const uint32_t cmd_len = *reinterpret_cast<decltype(cmd_len) *>(
                    log_blob.data() + offset);
                offset += sizeof(cmd_len);
                std::unique_ptr<TxCommand> tx_cmd = CreateTxCommand(
                    std::string_view(log_blob.data() + offset, cmd_len));
                offset += cmd_len;
                cmd_list.emplace_back(std::move(tx_cmd));
            }
            bool acquired_extra_data = false;
            if (cce->GetKeyLock() == nullptr)
            {
                cce->GetOrCreateKeyLock(shard_, this, ccp);
                assert(cce->GetKeyLock() != nullptr);
                acquired_extra_data = true;
            }

            // Emplace txn_cmd and try to commit all pending commands.
            uint64_t current_version = cce->CommitTs();
            RecordStatus payload_status = cce->PayloadStatus();

            ReplayTxnCmdList &replay_cmd_list = cce->ReplayCommandList();
            TxnCmd txn_cmd(
                obj_version, commit_ts, has_overwrite, std::move(cmd_list));

            if (txn_cmd.obj_version_ >= current_version)
            {
                EmplaceAndCommitReplayTxnCommand(cce->payload_,
                                                 replay_cmd_list,
                                                 txn_cmd,
                                                 current_version,
                                                 payload_status);
                cce->SetCommitTsPayloadStatus(current_version, payload_status);
            }
            else
            {
                DLOG(INFO)
                    << "discard TxnCmd with a version smaller than cur_ver";
            }
            if (replay_cmd_list.IsNull())
            {
                // Recycles the lock if this and prior commands have been
                // applied and there is no pending command.
                bool lock_recycled = cce->RecycleKeyLock(*shard_);
                if (acquired_extra_data)
                {
                    // The lock is newly assigned, recycle must succeed.
                    assert(lock_recycled);
                }
                (void) lock_recycled;
            }

            // Must update dirty_commit_ts. Otherwise, this entry may be
            // skipped by checkpointer.
            if (commit_ts > last_dirty_commit_ts_)
            {
                last_dirty_commit_ts_ = commit_ts;
            }
            if (commit_ts > ccp->last_dirty_commit_ts_)
            {
                ccp->last_dirty_commit_ts_ = commit_ts;
            }

            NonBlockingLock *lk = cce->GetKeyLock();
            if (lk != nullptr && lk->HasWriteLock())
            {
                // If the record in the log has a commit ts greater than
                // that of the cc entry and the cc entry has a write
                // lock, the lock's owner must be the tx that commits
                // the log record.
                // TODO: it is safer if we ship the tx ID with the
                // recovering message and match it against the lock holder.

                // Reset the dirty status since the committed commands are
                // already committed on the object.
                cce->SetDirtyPayload(nullptr);
                cce->SetDirtyPayloadStatus(RecordStatus::NonExistent);
                cce->SetPendingCmd(nullptr);

                TxNumber txn = lk->WriteLockTx();
                ReleaseCceLock(
                    lk, cce, txn, req.NodeGroupId(), LockType::WriteLock);
            }
        }

        if (next_core != UINT16_MAX)
        {
            req.ResetCcm();
            MoveRequest(&req, next_core);
            return false;
        }
        else
        {
            req.SetFinish();
            return true;
        }
    }

    bool BackFill(LruEntry *entry,
                  uint64_t commit_ts,
                  RecordStatus status,
                  std::unique_ptr<TxRecord> rec_uptr) override
    {
        CcEntry<KeyT, ValueT> *cce =
            static_cast<CcEntry<KeyT, ValueT> *>(entry);
        ValueT *rec_ptr = static_cast<ValueT *>(rec_uptr.get());
        LruPage *ccp = cce->GetCcPage();
        // Release the read intent acquried by fetch record.
        ReleaseCceLock(cce->GetKeyLock(),
                       cce,
                       fetch_record_txn,
                       cc_ng_id_,
                       LockType::ReadIntent);
        if (status == RecordStatus::Unknown)
        {
            // fetch record fails.
            if (cce->IsFree())
            {
                // Remove cce if it is not referenced by anyone.
                CleanEntry(entry, ccp);
            }
            return true;
        }
        // It's possible that first ReplayLogCc triggers FetchRecord and the
        // second ReplayLogCc has_overwrite and overrides the cce.
        if (cce->PayloadStatus() == RecordStatus::Unknown)
        {
            cce->SetCommitTsPayloadStatus(commit_ts, status);
            cce->SetCkptTs(commit_ts);

            if (rec_ptr)
            {
                cce->payload_.reset(
                    static_cast<ValueT *>(rec_ptr->Clone().release()));
            }
            else
            {
                assert(cce->payload_ == nullptr);
            }

            // Check if there's any buffered replay cmds, and try to
            // commit them.
            if (cce->HasReplayCommandList())
            {
                ReplayTxnCmdList &replay_cmd_list = cce->ReplayCommandList();
                // Clear cmds with smaller version than kv version.
                for (auto it = replay_cmd_list.txn_cmd_list_.begin();
                     it != replay_cmd_list.txn_cmd_list_.end();)
                {
                    if (it->obj_version_ >= commit_ts)
                    {
                        break;
                    }
                    it = replay_cmd_list.txn_cmd_list_.erase(it);
                }

                replay_cmd_list.cur_version_ = commit_ts;

                uint64_t commit_version = commit_ts;
                TryCommitReplayCommands(
                    cce->payload_, replay_cmd_list, commit_version);
                RecordStatus commit_status = cce->payload_ == nullptr
                                                 ? RecordStatus::Deleted
                                                 : RecordStatus::Normal;
                cce->SetCommitTsPayloadStatus(commit_version, commit_status);

                if (replay_cmd_list.IsNull())
                {
                    // Recycles the lock if all the replay commands have been
                    // applied.
                    cce->RecycleKeyLock(*shard_);
                }

                // After completing the log replay and attempting the command
                // replay on cce,
                // if the record status remains deleted and commit_ts is 1,
                // and the replay_cmd_list is still not empty, this indicates
                // the record has expired in the KV store.
                if (Sharder::Instance().LeaderTerm(cc_ng_id_) != -1 &&
                    !replay_cmd_list.IsNull() &&
                    commit_status == RecordStatus::Deleted && commit_ts == 1)
                {
                    replay_cmd_list.Clear();
                    cce->RecycleKeyLock(*shard_);
                }
            }
        }

        return true;
    }

private:
    std::unique_ptr<TxCommand> CreateTxCommand(std::string_view cmd_image)
    {
        assert(table_schema_ != nullptr);
        auto cmd_uptr = table_schema_->CreateTxCommand(cmd_image);
        assert(cmd_uptr != nullptr);
        return cmd_uptr;
    }

    std::pair<std::unique_ptr<ValueT>, RecordStatus>
    CreateDirtyPayloadFromExistingPayload(ValueT *payload)
    {
        assert(payload != nullptr);
        ValueT &object = *payload;
        std::unique_ptr<TxRecord> tx_rec_uptr = object.Clone();
        auto *obj_ptr = static_cast<ValueT *>(tx_rec_uptr.release());
        return {std::unique_ptr<ValueT>(obj_ptr), RecordStatus::Normal};
    }

    std::pair<std::unique_ptr<ValueT>, RecordStatus>
    CreateDirtyPayloadFromCommand(TxCommand *cmd)
    {
        auto *obj_ptr =
            static_cast<ValueT *>(cmd->CreateObject(nullptr).release());
        return {std::unique_ptr<ValueT>(obj_ptr), RecordStatus::Normal};
    }

    void CommitCommandOnPayload(std::unique_ptr<ValueT> &payload,
                                RecordStatus &payload_status,
                                TxCommand &cmd)
    {
        assert(payload != nullptr && payload_status == RecordStatus::Normal);
        auto *obj_ptr = payload.get();
        TxObject *new_obj_ptr = cmd.CommitOn(obj_ptr);
        if (new_obj_ptr != obj_ptr)
        {
            if (new_obj_ptr == nullptr)
            {
                // This is a DEL command and the object is deleted.
                payload_status = RecordStatus::Deleted;
                payload = nullptr;
            }
            else
            {
                // The object has been changed by cmd.
                payload_status = RecordStatus::Normal;
                payload =
                    std::unique_ptr<ValueT>(static_cast<ValueT *>(new_obj_ptr));
            }
        }
    }

    void CommitCommandOnDirtyPayload(std::unique_ptr<ValueT> &dirty_payload,
                                     RecordStatus &dirty_payload_status,
                                     TxCommand &cmd)
    {
        assert(dirty_payload != nullptr &&
               dirty_payload_status == RecordStatus::Normal);
        TxObject *old_obj_ptr = dirty_payload.get();
        TxObject *new_obj_ptr = cmd.CommitOn(old_obj_ptr);
        if (new_obj_ptr != old_obj_ptr)
        {
            if (new_obj_ptr == nullptr)
            {
                // This is a DEL command and the object is deleted.
                dirty_payload = nullptr;
                dirty_payload_status = RecordStatus::Deleted;
            }
            else
            {
                // The object has been changed by cmd.
                dirty_payload =
                    std::unique_ptr<ValueT>(static_cast<ValueT *>(new_obj_ptr));
                dirty_payload_status = RecordStatus::Normal;
            }
        }
    }

    /**
     * If the a record is according to the conditions, return true, or return
     * false to neglect this record.
     */
    bool FilterRecord(const KeyT *key,
                      const CcEntry<KeyT, ValueT> *cce,
                      int32_t obj_type,
                      const std::string_view &scan_pattern) override
    {
        if (cce->PayloadStatus() == RecordStatus::Deleted &&
            (!cce->NeedCkpt() || txservice_skip_kv))
        {
            return false;
        }
        if (obj_type >= 0 && cce->payload_ != nullptr &&
            !cce->payload_->IsMatchType(obj_type))
        {
            return false;
        }
        if (scan_pattern.size() > 0 && !key->IsMatch(scan_pattern))
        {
            return false;
        }

        return true;
    }

    void SetExpire(LruEntry *cce, uint64_t expire_ts)
    {
        auto it = expires_.emplace(cce, expire_ts);
        if (!it.second)
        {
            it.first->second = expire_ts;
        }
    }

    bool IsExpired(LruEntry *cce, uint64_t now_ts) const
    {
        auto it = expires_.find(cce);
        if (it != expires_.end() && it->second <= now_ts)
        {
            return true;
        }
        return false;
    }

    void RemoveExpire(LruEntry *cce)
    {
        expires_.erase(cce);
    }

    // Expire timestamp of keys with expire_ts set.
    std::unordered_map<LruEntry *, uint64_t> expires_;
};
}  // namespace txservice
