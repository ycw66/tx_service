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
        bool &cmd_success = obj_result.cmd_success_;
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

        // Should create command before calling req.IsReadOnly().
        TxCommand *cmd = nullptr;
        if (req.IsLocal())
        {
            cmd = req.CommandPtr();
        }
        else
        {
            if (req.OwnCommand())
            {
                cmd = req.remote_input_.cmd_uptr_.get();
            }
            else
            {
                std::unique_ptr<TxCommand> cmd_uptr =
                    CreateTxCommand(*req.CommandImage());
                cmd = cmd_uptr.get();
                req.SetCommand(std::move(cmd_uptr));
            }
        }

        CcOperation cc_op =
            req.IsReadOnly() ? CcOperation::Read : CcOperation::Write;

        if (req.CcePtr() != nullptr)
        {
            // the request was blocked and is now unblocked and lock acquired
            cce = static_cast<CcEntry<KeyT, ValueT> *>(req.CcePtr());
            ccp = static_cast<CcPage<KeyT, ValueT> *>(cce->GetCcPage());

            if (req.block_type_ == ApplyCc::ApplyBlockType::BlockOnLock)
            {
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
                look_key = static_cast<const KeyT *>(req_key);
            }
            else
            {
                const std::string *key_str = req.KeyImage();
                assert(key_str != nullptr);
                size_t offset = 0;
                decoded_key.Deserialize(key_str->data(), offset, KeySchema());
                look_key = &decoded_key;
            }

            auto it = FindEmplace(*look_key);
            cce = it->second;
            ccp = it.GetPage();

            if (cce == nullptr)
            {
                // The apply request needs a new cc entry but the cc map has
                // reached the maximal capacity. Blocks the request by putting
                // it back to the cc request queue.
                shard_->Enqueue(shard_->LocalCoreId(), &req);
                return false;
            }

            req.SetCcePtr(cce);

            assert(cce != nullptr);
            cce_addr.SetCce(
                reinterpret_cast<uint64_t>(cce), ng_term, shard_->core_id_);

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

        // check locking result
        switch (err_code)
        {
        case CcErrorCode::NO_ERROR:
        {
            // lock acquired
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
            req.block_type_ = ApplyCc::ApplyBlockType::BlockOnLock;
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

        // Lock acquired, set the result.
        obj_result.lock_acquired_ = acquired_lock;

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
                if (cmd->IgnoreKvValue())
                {
                    // cmd that ignores kv value should be applied regardless of
                    // current value.
                    assert(cmd->ProceedOnNonExistentObject() &&
                           cmd->ProceedOnExistentObject() &&
                           acquired_lock == LockType::WriteLock);
                    // We will pretend that there's a delete on this cce just
                    // before this cmd to ignore value in kv.
                    cce->SetDirtyPayloadStatus(RecordStatus::Deleted);
                    cce->SetCkptTs(1);
                }
                else
                {
                    shard_->FetchRecord(table_name_,
                                        table_schema_,
                                        look_key,
                                        cce,
                                        this,
                                        cc_ng_id_,
                                        ng_term,
                                        &req);
                    req.block_type_ = ApplyCc::ApplyBlockType::BlockOnFetch;

                    if (metrics::enable_cache_hit_rate)
                    {
                        auto meter = shard_->GetMeter();
                        if (cce->PayloadStatus() == RecordStatus::Unknown)
                        {
                            meter->Collect(
                                metrics::NAME_CACHE_HIT_OR_MISS_TOTAL,
                                1,
                                "miss");
                        }
                        else
                        {
                            meter->Collect(
                                metrics::NAME_CACHE_HIT_OR_MISS_TOTAL,
                                1,
                                "hits");
                        }
                    }
                    return false;
                }
            }
        }

        // Read only commands in read committed isolation level just checks the
        // payload.
        if (req.Isolation() == IsolationLevel::ReadCommitted &&
            cmd->IsReadOnly())
        {
            assert(acquired_lock == LockType::NoLock);
            bool object_not_exist =
                cce->PayloadStatus() == RecordStatus::Deleted;
            if (object_not_exist)
            {
                bool proceed = cmd->ProceedOnNonExistentObject();
                // Read only commands should never proceed if object doesn't
                // exist.
                assert(!proceed);
                obj_result.rec_status_ = RecordStatus::Deleted;
                obj_result.commit_ts_ = cce->CommitTs();
                hd_res->SetFinished();
                return true;
            }

            // Execute the command on payload.
            ValueT &object = *cce->payload_;
            cmd_success = cmd->ExecuteOn(object);

            obj_result.commit_ts_ = cce->CommitTs();
            obj_result.rec_status_ = cce->PayloadStatus();
            hd_res->SetFinished();
            return true;
        }

        // Lock must have been acquired and it is not NoLock.
        assert(cce->GetKeyLock() != nullptr);

        RecordStatus dirty_payload_status = cce->DirtyPayloadStatus();
        // Create the dirty object if there is already a pending command on this
        // object.
        if (dirty_payload_status == RecordStatus::Uncreated)
        {
            std::unique_ptr<TxCommand> pending_cmd = cce->PendingCmd();
            std::unique_ptr<ValueT> dirty_payload = cce->DirtyPayload();
            // Since pending_cmd_ exists, the payload must also exist.
            // Otherwise, the dirty payload should have already been created by
            // the last command.
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

        // Check whether the object exists.
        // If dirty payload exists, use dirty_payload_status. Use payload status
        // only if dirty payload doesn't exist.
        bool object_not_exist =
            dirty_payload_status == RecordStatus::Deleted ||
            (dirty_payload_status == RecordStatus::NonExistent &&
             cce->PayloadStatus() == RecordStatus::Deleted);

        // Create the temporary object if the object does not exist.
        if (object_not_exist)
        {
            bool proceed = cmd->ProceedOnNonExistentObject();
            if (!proceed)
            {
                if (req.apply_and_commit_)
                {
                    // Release and try to recycle the lock.
                    ReleaseCceLock(
                        cce->GetKeyLock(), cce, txn, ng_id, acquired_lock);
                    obj_result.lock_acquired_ = LockType::NoLock;
                }

                // Del command on a non-existent object also returns directly.
                obj_result.rec_status_ = RecordStatus::Deleted;
                obj_result.commit_ts_ = cce->CommitTs();
                hd_res->SetFinished();
                return true;
            }
            else
            {
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
        }
        else
        {
            bool proceed = cmd->ProceedOnExistentObject();
            if (!proceed)
            {
                if (req.apply_and_commit_)
                {
                    // Release and try to recycle the lock.
                    ReleaseCceLock(
                        cce->GetKeyLock(), cce, txn, ng_id, acquired_lock);
                    obj_result.lock_acquired_ = LockType::NoLock;
                }

                obj_result.rec_status_ = RecordStatus::Normal;
                obj_result.commit_ts_ = cce->CommitTs();
                hd_res->SetFinished();
                return true;
            }
        }

        if (dirty_payload_status == RecordStatus::Normal)
        {
            std::unique_ptr<ValueT> dirty_payload = cce->DirtyPayload();
            assert(dirty_payload != nullptr);

            // Temporary object exists, execute and commit the command on
            // the temporary object.
            ValueT &dirty_object = *dirty_payload;
            cmd_success = cmd->ExecuteOn(dirty_object);
            DLOG(INFO) << "execute and commit current command on dirty payload";
            if (cmd_success && !cmd->IsReadOnly())
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
            std::unique_ptr<TxCommand> pending_cmd = cce->PendingCmd();
            assert(pending_cmd == nullptr);
            ValueT &object = *cce->payload_;
            cmd_success = cmd->ExecuteOn(object);

            if (cmd_success && !cmd->IsReadOnly() && !req.apply_and_commit_)
            {
                // Copy the command to be committed in PostWriteCc or when
                // executing subsequent commands of the same txn.
                cce->SetPendingCmd(cmd->Clone());

                // The object is being modified, set dirty_payload_status_ to
                // Uncreated so that a temporary object will be created when
                // processing subsequent commands of the same txn. In
                // PostWriteCc, the original object will be replaced by the
                // temporary object if the txn commits.
                cce->SetDirtyPayloadStatus(RecordStatus::Uncreated);
            }
        }

        if (cmd_success && req.apply_and_commit_ && !cmd->IsReadOnly())
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

            // Set commit ts based on the TxTs since there is no PostWriteCc if
            // apply_and_commit_.
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

            // Release and try to recycle the lock.
            ReleaseCceLock(cce->GetKeyLock(), cce, txn, ng_id, acquired_lock);
            obj_result.lock_acquired_ = LockType::NoLock;
        }

        // Updates last_vali_ts after successfully acquiring the write
        // lock such that it is not smaller than the current time of
        // the shard. The net effect is that the tx acquiring the write
        // lock is forced not to commit at a time earlier than the
        // clock of this cc node, even if the clock of the tx's
        // coordinator node drifts and falls behind. Checkpointing
        // relies on this property to avoid picking a checkpoint ts in
        // this shard that may overlap with the ongoing tx.
        if (!req.IsReadOnly())
        {
            obj_result.last_vali_ts_ =
                std::max(shard_->LastReadTs(), shard_->Now());
        }

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
                std::unique_ptr<TxCommand> pending_cmd = cce->PendingCmd();
                if (pending_cmd != nullptr)
                {
                    assert(cce->payload_ != nullptr);
                    CommitCommandOnPayload(
                        cce->payload_, payload_status, *pending_cmd);
                }
            }

            cce->SetCommitTsPayloadStatus(commit_ts, payload_status);
            if (last_dirty_commit_ts_ < commit_ts)
            {
                last_dirty_commit_ts_ = commit_ts;
            }

            CcPage<KeyT, ValueT> *ccp =
                static_cast<CcPage<KeyT, ValueT> *>(cce->GetCcPage());
            assert(ccp != nullptr);
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
            uint16_t core_id = (key.Hash() & 0x3FF) % shard_->core_cnt_;
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
                // Renqueue cc req, wait for checkpoint and kickout flushed cce.
                req.SetOffset(prev_offset);
                req.SetNextCore(next_core);
                shard_->Enqueue(shard_->LocalCoreId(), &req);
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
            // If there's already buffered cmd, that means a previous replaycc
            // has already sent FetchRecord.
            if (!has_overwrite &&
                cce->PayloadStatus() == RecordStatus::Unknown &&
                !cce->HasReplayCommandList())
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
                                    &key,
                                    cce,
                                    this,
                                    cc_ng_id_,
                                    ng_term,
                                    nullptr);
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

            TxnCmd txn_cmd(
                obj_version, commit_ts, has_overwrite, std::move(cmd_list));

            bool acquired_extra_data = false;
            if (cce->GetKeyLock() == nullptr)
            {
                cce->GetOrCreateKeyLock(shard_, this, ccp);
                assert(cce->GetKeyLock() != nullptr);
                acquired_extra_data = true;
            }

            std::unique_ptr<ReplayTxnCmdList> replay_cmd_list =
                cce->ReplayCommandList();

            // Emplace txn_cmd and try to commit all pending commands.
            uint64_t commit_version = cce->CommitTs();
            RecordStatus payload_status = cce->PayloadStatus();
            EmplaceAndCommitReplayTxnCommand(cce->payload_,
                                             replay_cmd_list,
                                             txn_cmd,
                                             commit_version,
                                             payload_status);
            cce->SetCommitTsPayloadStatus(commit_version, payload_status);

            if (replay_cmd_list == nullptr)
            {
                // Recycles the lock if this and prior commands have been
                // applied and there is no pending command.
                bool lock_recycled = cce->RecycleKeyLock(*shard_);
                if (acquired_extra_data)
                {
                    // The lock is newly assigned, recycle must succeed.
                    assert(lock_recycled);
                }
            }
            else
            {
                // Passes the replay command list back to the cc entry.
                cce->SetReplayCommandList(std::move(replay_cmd_list));
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

    void BackFill(LruEntry *entry,
                  uint64_t commit_ts,
                  RecordStatus status,
                  std::unique_ptr<TxRecord> rec_uptr) override
    {
        assert(status != RecordStatus::Unknown);
        CcEntry<KeyT, ValueT> *cce =
            static_cast<CcEntry<KeyT, ValueT> *>(entry);
        ValueT *rec_ptr = static_cast<ValueT *>(rec_uptr.get());
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
                std::unique_ptr<ReplayTxnCmdList> replay_cmd_list =
                    cce->ReplayCommandList();
                // Clear cmds with smaller version than kv version.
                for (auto it = replay_cmd_list->txn_cmd_list_.begin();
                     it != replay_cmd_list->txn_cmd_list_.end();)
                {
                    if (it->obj_version_ >= commit_ts)
                    {
                        break;
                    }
                    it = replay_cmd_list->txn_cmd_list_.erase(it);
                }

                replay_cmd_list->cur_version_ = commit_ts;

                uint64_t commit_version = commit_ts;
                TryCommitReplayCommands(
                    cce->payload_, replay_cmd_list, commit_version);
                RecordStatus commit_status = cce->payload_ == nullptr
                                                 ? RecordStatus::Deleted
                                                 : RecordStatus::Normal;
                cce->SetCommitTsPayloadStatus(commit_version, commit_status);

                if (replay_cmd_list == nullptr)
                {
                    // Recycles the lock if all the replay commands have been
                    // applied.
                    cce->RecycleKeyLock(*shard_);
                }
                else
                {
                    // Passes the replay command list back to the cc entry.
                    cce->SetReplayCommandList(std::move(replay_cmd_list));
                }
            }
        }
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
        DLOG(INFO) << "creating dirty payload from existing payload";
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
        DLOG(INFO) << "commit pending_cmd on dirty_payload";
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
