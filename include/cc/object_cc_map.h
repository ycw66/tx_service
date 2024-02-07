#pragma once

#include <algorithm>
#include <cassert>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog_factory.h"
#include "cc_entry.h"
#include "cc_map.h"
#include "non_blocking_lock.h"
#include "template_cc_map.h"
#include "tx_record.h"

namespace txservice
{
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
        LOG(INFO) << "creating ObjectCcmap, table name: "
                  << table_name.StringView();
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
            if (cc_op == CcOperation::Read &&
                req.Isolation() == IsolationLevel::ReadCommitted)
            {
                // Unpin CcEntry: release ReadIntent that added before backfill.
                assert(cce->GetKeyLock() != nullptr &&
                       cce->GetKeyLock()->ReadIntents().count(req.Txn()) == 1);
                ReleaseCceLock(cce->GetKeyLock(),
                               cce,
                               req.Txn(),
                               req.NodeGroupId(),
                               LockType::ReadIntent);
            }
            else
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

#ifdef RANGE_PARTITION_ENABLED
            Iterator it = Find(*look_key);
            cce = it->second;
            ccp = it.GetPage();

            // collect metrics: slice cache hits
            if (metrics::enable_cache_hit_rate)
            {
                auto meter = shard_->meter_.get();
                if (cce != nullptr)
                {
                    meter->Collect(
                        shard_->CACHE_HIT_OR_MISS_TOTAL_NAME_, 1, "hits");
                }
            }

            if (cce == nullptr)
            {
                if (Type() == TableType::Primary ||
                    Type() == TableType::UniqueSecondary)
                {
                    RangeSliceOpStatus pin_status;
                    RangeSliceId slice_id =
                        shard_->PinRangeSlice(table_name_,
                                              cc_ng_id_,
                                              ng_term,
                                              KeySchema(),
                                              RecordSchema(),
                                              schema_ts_,
                                              table_schema_->GetKVCatalogInfo(),
                                              *look_key,
                                              true,
                                              &req,
                                              pin_status,
                                              false,
                                              0);

                    if (pin_status == RangeSliceOpStatus::Successful)
                    {
                        // The slice is unpinned immediately. This is
                        // because the prior pin operation brings all
                        // records in the slice into memory, including the
                        // target record sharded to this core. Since cache
                        // cleaning is done by the tx processor associated
                        // with this core, the target record cannot be
                        // kicked out before this read request finishes.
                        slice_id.Unpin();

                        if (cc_op == CcOperation::Write)
                        {
                            auto it = FindEmplace(*look_key);
                            cce = it->second;
                            ccp = it.GetPage();
                            if (cce == nullptr)
                            {
                                hd_res->SetError(CcErrorCode::OUT_OF_MEMORY);
                                return true;
                            }

                            if (cce->payload_status_ == RecordStatus::Unknown)
                            {
                                cce->payload_status_ = RecordStatus::Deleted;
                                cce->commit_ts_ = 1U;
                                cce->gap_commit_ts_ = 1U;
                                cce->ckpt_ts_.store(1U,
                                                    std::memory_order_relaxed);
                            }
                            else
                            {
                                assert(cce->commit_ts_ > 1);
                            }
                        }
                        else
                        {
                            assert(cc_op == CcOperation::Read);
                            Iterator it = Find(*look_key);
                            cce = it->second;
                            ccp = it.GetPage();

                            if (cce == nullptr)
                            {
                                bool proceed =
                                    cmd->ProceedOnNonExistentObject();
                                if (!proceed)
                                {
                                    // Del command on a non-existent object also
                                    // returns directly.
                                    obj_result.commit_ts_ = 1;
                                    obj_result.rec_status_ =
                                        RecordStatus::Deleted;
                                    hd_res->SetFinished();
                                    return true;
                                }
                                else
                                {
                                    assert(false);
                                    hd_res->ForceError();
                                    return true;
                                }
                            }
                        }
                    }
                    else if (pin_status == RangeSliceOpStatus::BlockedOnLoad)
                    {
                        return false;
                    }
                    else if (pin_status == RangeSliceOpStatus::Retry)
                    {
                        shard_->Enqueue(shard_->LocalCoreId(), &req);
                        return false;
                    }
                    else if (pin_status == RangeSliceOpStatus::Delay)
                    {
                        if (slice_id.Range()->HasLock())
                        {
                            hd_res->SetError(CcErrorCode::OUT_OF_MEMORY);
                            return true;
                        }
                        else
                        {
                            shard_->Enqueue(shard_->LocalCoreId(), &req);
                            return false;
                        }
                    }
                    else
                    {
                        // If the pin operation returns an error, the data
                        // store is inaccessible.
                        hd_res->SetError(CcErrorCode::PIN_RANGE_SLICE_FAILED);
                        return true;
                    }
                }
                else
                {
                    assert(Type() == TableType::Catalog);
                    auto it = FindEmplace(*look_key);
                    cce = it->second;
                    ccp = it.GetPage();
                    if (cce == nullptr)
                    {
                        hd_res->SetError(CcErrorCode::OUT_OF_MEMORY);
                        return true;
                    }
                }
            }
#else
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

            // if ccm contains all the ccentries, then unknown status means
            // that we can skip accessing kv store and return deleted status
            // directly.
            if (ccm_has_full_entries_ &&
                cce->payload_status_ == RecordStatus::Unknown)
            {
                cce->payload_status_ = RecordStatus::Deleted;
                cce->commit_ts_ = 1U;
                cce->ckpt_ts_.store(1U);
            }
#endif
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

        assert(ccp != nullptr && cce != nullptr);
        if (cce->payload_status_ == RecordStatus::Unknown)
        {
            if (FLAGS_skip_kv)
            {
                cce->payload_status_ = RecordStatus::Deleted;
                cce->commit_ts_ = 1U;
            }
            else
            {
                if (acquired_lock == LockType::NoLock)
                {
                    assert(cc_op == CcOperation::Read &&
                           req.Isolation() == IsolationLevel::ReadCommitted);
                    // Add ReadIntent to pin ccentry for asynchronously
                    // backfill.
                    std::tie(acquired_lock, err_code) =
                        AcquireCceKeyLock(cce,
                                          ccp,
                                          RecordStatus::Normal,
                                          &req,
                                          req.NodeGroupId(),
                                          ng_term,
                                          req.TxTerm(),
                                          CcOperation::Read,
                                          IsolationLevel::RepeatableRead,
                                          CcProtocol::OCC,
                                          0,
                                          false);
                    assert(acquired_lock == LockType::ReadIntent &&
                           err_code == CcErrorCode::NO_ERROR);
                }
                shard_->FetchRecord(table_name_,
                                    table_schema_,
                                    look_key,
                                    cce,
                                    this,
                                    cc_ng_id_,
                                    ng_term,
                                    &req);
                return false;
            }
        }

        // Create the dirty object if there is already a pending command on this
        // object.
        if (cce->dirty_payload_status_ == RecordStatus::Uncreated)
        {
            // Since pending_cmd_ exists, the payload must also exist.
            // Otherwise, the dirty payload should have already been created by
            // the last command.
            assert(cce->pending_cmd_ != nullptr);
            assert(cce->payload_status_ == RecordStatus::Normal &&
                   cce->payload_ != nullptr);

            std::tie(cce->dirty_payload_, cce->dirty_payload_status_) =
                CreateDirtyPayloadFromExistingPayload(cce->payload_.get());
            assert(cce->dirty_payload_status_ == RecordStatus::Normal);

            // Commit the pending command.
            CommitCommandOnDirtyPayload(cce->dirty_payload_,
                                        cce->dirty_payload_status_,
                                        *cce->pending_cmd_);
            cce->pending_cmd_ = nullptr;
        }

        bool object_not_exist =
            cce->dirty_payload_status_ == RecordStatus::Deleted ||
            (cce->payload_status_ == RecordStatus::Deleted &&
             cce->dirty_payload_status_ == RecordStatus::NonExistent);

        // Create the temporary object if the object does not exist.
        if (object_not_exist)
        {
            bool proceed = cmd->ProceedOnNonExistentObject();
            if (!proceed)
            {
                if (req.apply_and_commit_)
                {
                    ReleaseCceLock(
                        cce->GetKeyLock(), cce, txn, ng_id, acquired_lock);
                    obj_result.lock_acquired_ = LockType::NoLock;
                }

                // Del command on a non-existent object also returns directly.
                obj_result.rec_status_ = RecordStatus::Deleted;
                obj_result.commit_ts_ = cce->commit_ts_;
                hd_res->SetFinished();
                return true;
            }
            else
            {
                // Create an empty temporary object to process the commands, the
                // dirty payload will be uploaded to payload in PostWriteCc if
                // the txn commits.
                std::tie(cce->dirty_payload_, cce->dirty_payload_status_) =
                    CreateDirtyPayloadFromCommand(cmd);
                cce->pending_cmd_ = nullptr;
            }
        }
        else
        {
            bool proceed = cmd->ProceedOnExistentObject();
            if (!proceed)
            {
                if (req.apply_and_commit_)
                {
                    ReleaseCceLock(
                        cce->GetKeyLock(), cce, txn, ng_id, acquired_lock);
                    obj_result.lock_acquired_ = LockType::NoLock;
                }

                obj_result.rec_status_ = RecordStatus::Normal;
                obj_result.commit_ts_ = cce->commit_ts_;
                hd_res->SetFinished();
                return true;
            }
        }

        if (cce->dirty_payload_status_ == RecordStatus::Normal)
        {
            assert(cce->dirty_payload_ != nullptr);
            // Temporary object exists, execute and commit the command on
            // the temporary object.
            ValueT &tmp_object = *cce->dirty_payload_;
            cmd_success = cmd->ExecuteOn(tmp_object);
            DLOG(INFO) << "execute and commit current command on dirty payload";
            if (cmd_success && !cmd->IsReadOnly())
            {
                CommitCommandOnDirtyPayload(
                    cce->dirty_payload_, cce->dirty_payload_status_, *cmd);
            }
        }
        else if (cce->payload_status_ == RecordStatus::Normal)
        {
            // The dirty payload does not exist. This is the first command.
            // Execute and copy the command. The command will be committed
            // in PostWriteCc if the txn commits.
            assert(cce->pending_cmd_ == nullptr);
            ValueT &object = *cce->payload_;
            cmd_success = cmd->ExecuteOn(object);

            if (cmd_success && !cmd->IsReadOnly() && !req.apply_and_commit_)
            {
                // Copy the command to be committed in PostWriteCc or when
                // executing subsequent commands of the same txn.
                cce->pending_cmd_ = cmd->Clone();

                // The object is being modified, set dirty_payload_status_ to
                // Uncreated so that a temporary object will be created when
                // processing subsequent commands of the same txn. In
                // PostWriteCc, the original object will be replaced by the
                // temporary object if the txn commits.
                cce->dirty_payload_status_ = RecordStatus::Uncreated;
            }
        }

        if (cmd_success && req.apply_and_commit_ && !cmd->IsReadOnly())
        {
            // Skipping writing log, do the PostWrite and release the lock.
            assert(acquired_lock == LockType::WriteLock);
            if (cce->dirty_payload_status_ == RecordStatus::Normal ||
                cce->dirty_payload_status_ == RecordStatus::Deleted)
            {
                // Dirty payload exists. Use it to replace payload.
                cce->payload_ = std::move(cce->dirty_payload_);
                cce->payload_status_ = cce->dirty_payload_status_;
            }
            else
            {
                CommitCommandOnPayload(
                    cce->payload_, cce->payload_status_, *cmd);
            }

            // Reset the dirty status.
            cce->dirty_payload_ = nullptr;
            cce->dirty_payload_status_ = RecordStatus::NonExistent;
            cce->pending_cmd_ = nullptr;
            // Set commit ts based on the TxTs since there is no PostWriteCc if
            // apply_and_commit_.
            cce->commit_ts_ =
                std::max({cce->commit_ts_ + 1, req.TxTs(), shard_->Now()});

            if (last_dirty_commit_ts_ < cce->commit_ts_)
            {
                last_dirty_commit_ts_ = cce->commit_ts_;
            }
            if (cce->commit_ts_ > ccp->last_dirty_commit_ts_)
            {
                ccp->last_dirty_commit_ts_ = cce->commit_ts_;
            }

            shard_->mem_usage_ += cce->PayloadMemUsage();

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

        obj_result.commit_ts_ = cce->commit_ts_;
        obj_result.rec_status_ = cce->payload_status_;
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
            // The txn commits. Upload the change.
#ifdef RANGE_PARTITION_ENABLED
            if (req.GetOperationType() == OperationType::Insert &&
                cce->commit_ts_ == 1)
            {
                // At post write we have already loaded the latest version
                // of cce into memory. So if commit ts is 1 (entry does not
                // exist and has no previous version), that means it does
                // not exist in data store at all.
                cce->data_store_size_.store(0, std::memory_order_relaxed);
            }
#endif

            if (cce->dirty_payload_status_ == RecordStatus::Normal ||
                cce->dirty_payload_status_ == RecordStatus::Deleted)
            {
                // Dirty payload exists. Use it to replace payload.
                cce->payload_status_ = cce->dirty_payload_status_;
                cce->payload_ = std::move(cce->dirty_payload_);
            }
            else if (cce->pending_cmd_ != nullptr)
            {
                assert(cce->payload_ != nullptr);
                CommitCommandOnPayload(
                    cce->payload_, cce->payload_status_, *cce->pending_cmd_);
            }

            cce->commit_ts_ = commit_ts;
            if (last_dirty_commit_ts_ < commit_ts)
            {
                last_dirty_commit_ts_ = commit_ts;
            }

            CcPage<KeyT, ValueT> *ccp =
                static_cast<CcPage<KeyT, ValueT> *>(cce->GetCcPage());
            assert(ccp != nullptr);
            if (cce->commit_ts_ > ccp->last_dirty_commit_ts_)
            {
                ccp->last_dirty_commit_ts_ = cce->commit_ts_;
            }
        }

        // Reset the dirty status.
        cce->dirty_payload_ = nullptr;
        cce->dirty_payload_status_ = RecordStatus::NonExistent;
        cce->pending_cmd_ = nullptr;

        shard_->mem_usage_ += cce->PayloadMemUsage();

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
            req.SetFinish();
            return false;
        }

        KeyT key;
        size_t offset = req.Offset();
        const std::string_view &log_blob = req.LogContentView();

        size_t prev_offset = offset;
        while (offset < log_blob.size())
        {
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
                    if (core_id < req.NextCore())
                    {
                        req.SetNextCore(core_id);
                    }
                }
                continue;
            }

            auto it = FindEmplace(key);
            CcEntry<KeyT, ValueT> *cce = it->second;
            CcPage<KeyT, ValueT> *ccp = it.GetPage();

            if (cce == nullptr)
            {
                req.Result()->SetError(CcErrorCode::OUT_OF_MEMORY);
                return true;
            }

            bool has_del =
                *reinterpret_cast<const uint8_t *>(log_blob.data() + offset);
            offset += sizeof(uint8_t);

            DLOG(INFO) << "replay log key: " << key.ToString()
                       << ", obj_ver: " << obj_version
                       << ", commit ts: " << commit_ts
                       << ", cmds len: " << cmds_len << ", cmds str: "
                       << std::string_view(log_blob.data() + offset, cmds_len)
                       << " has_del: " << has_del;

            // load payload from kvstore before committing pending commands
            if (!has_del && cce->payload_status_ == RecordStatus::Unknown)
            {
                int64_t cc_ng_candid_term =
                    Sharder::Instance().CandidateLeaderTerm(cc_ng_id_);
                int64_t cc_ng_term = Sharder::Instance().LeaderTerm(cc_ng_id_);
                int64_t ng_term = std::max(cc_ng_candid_term, cc_ng_term);
                assert(ng_term > 0);
                req.SetOffset(prev_offset);

#ifdef RANGE_PARTITION_ENABLED
                assert(Type() == TableType::Primary ||
                       Type() == TableType::UniqueSecondary);

                RangeSliceOpStatus pin_status;
                RangeSliceId slice_id =
                    shard_->PinRangeSlice(table_name_,
                                          cc_ng_id_,
                                          ng_term,
                                          KeySchema(),
                                          RecordSchema(),
                                          schema_ts_,
                                          table_schema_->GetKVCatalogInfo(),
                                          key,
                                          true,
                                          &req,
                                          pin_status,
                                          false,
                                          0);

                if (pin_status == RangeSliceOpStatus::Successful)
                {
                    // The slice is unpinned immediately. This is
                    // because the prior pin operation brings all
                    // records in the slice into memory, including the
                    // target record sharded to this core. Since cache
                    // cleaning is done by the tx processor associated
                    // with this core, the target record cannot be
                    // kicked out before this read request finishes.
                    slice_id.Unpin();

                    if (cce->payload_status_ == RecordStatus::Unknown)
                    {
                        cce->payload_status_ = RecordStatus::Deleted;
                        cce->commit_ts_ = 1U;
                        cce->gap_commit_ts_ = 1U;
                        cce->ckpt_ts_.store(1U, std::memory_order_relaxed);
                    }
                    else
                    {
                        assert(cce->commit_ts_ > 1);
                    }
                }
                else if (pin_status == RangeSliceOpStatus::BlockedOnLoad)
                {
                    return false;
                }
                else if (pin_status == RangeSliceOpStatus::Retry)
                {
                    shard_->Enqueue(shard_->LocalCoreId(), &req);
                    return false;
                }
                else if (pin_status == RangeSliceOpStatus::Delay)
                {
                    if (slice_id.Range()->HasLock())
                    {
                        req.Result()->SetError(CcErrorCode::OUT_OF_MEMORY);
                        return true;
                    }
                    else
                    {
                        shard_->Enqueue(shard_->LocalCoreId(), &req);
                        return false;
                    }
                }
                else
                {
                    // If the pin operation returns an error, the data
                    // store is inaccessible.
                    req.Result()->SetError(CcErrorCode::PIN_RANGE_SLICE_FAILED);
                    return true;
                }

#else

                // load payload asynchronously
                shard_->FetchRecord(table_name_,
                                    table_schema_,
                                    &key,
                                    cce,
                                    this,
                                    cc_ng_id_,
                                    ng_term,
                                    &req);
                return false;
#endif
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
                obj_version, commit_ts, has_del, std::move(cmd_list));

            // Emplace txn_cmd and try to commit all pending commands.
            EmplaceAndCommitReplayTxnCommand(
                cce->payload_, cce->replay_cmd_list_, txn_cmd, cce->commit_ts_);

            cce->payload_status_ = cce->payload_ == nullptr
                                       ? RecordStatus::Deleted
                                       : RecordStatus::Normal;

            // Must update dirty_commit_ts. Otherwise, this entry may be skipped
            // by checkpointer.
            if (cce->commit_ts_ > last_dirty_commit_ts_)
            {
                last_dirty_commit_ts_ = cce->commit_ts_;
            }
            if (cce->commit_ts_ > ccp->last_dirty_commit_ts_)
            {
                ccp->last_dirty_commit_ts_ = cce->commit_ts_;
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
                TxNumber txn = lk->WriteLockTx();
                ReleaseCceLock(
                    lk, cce, txn, req.NodeGroupId(), LockType::WriteLock);
            }
        }

        if (req.NextCore() != UINT16_MAX)
        {
            req.ResetCcm();
            MoveRequest(&req, req.NextCore());
        }
        else
        {
            req.SetFinish();
        }

        return false;
    }

    void BackFill(LruEntry *entry,
                  uint64_t commit_ts,
                  RecordStatus status,
                  std::unique_ptr<TxRecord> rec_uptr) override
    {
        assert(status != RecordStatus::Unknown);
        CcEntry<KeyT, ValueT> *cce =
            dynamic_cast<CcEntry<KeyT, ValueT> *>(entry);
        // It's possible that first ReplayLogCc triggers FetchRecord and the
        // second ReplayLogCc has_del and overrides the cce.
        if (cce->payload_status_ == RecordStatus::Unknown)
        {
            cce->ckpt_ts_ = commit_ts;
            cce->commit_ts_ = commit_ts;
            cce->payload_status_ = status;
            cce->payload_.reset(static_cast<ValueT *>(rec_uptr.release()));
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

    void ScanKey(const KeyT *key,
                 CcEntry<KeyT, ValueT> *cce,
                 RemoteScanCache *remote_cache,
                 bool include_gap,
                 int64_t ng_term,
                 uint64_t read_ts,
                 bool is_read_snapshot,
                 bool keep_deleted,
                 bool is_ckpt_delta = false) const override
    {
        remote::ScanTuple_msg *tuple = nullptr;
        uint32_t tuple_size = 0;

#ifdef RANGE_PARTITION_ENABLED
        if (cce->payload_status_ == RecordStatus::Normal ||
            cce->payload_status_ == RecordStatus::Deleted && keep_deleted)
        {
            tuple = remote_cache->cache_msg_->add_scan_tuple();
        }
        else
        {
            return;
        }
#else
        tuple = remote_cache->cache_msg_->add_scan_tuple();
#endif
        key->Serialize(*tuple->mutable_key());
        tuple_size += key->Size();

        tuple->clear_record();
        if (cce->payload_ != nullptr)
        {
            cce->payload_->ValueT::Serialize(*tuple->mutable_record());
            tuple_size += sizeof(int8_t);
        }
        else
        {
            int8_t obj_type = -1;
            tuple->mutable_record()->append(
                reinterpret_cast<const char *>(&obj_type), sizeof(int8_t));
            tuple_size += sizeof(int8_t);
        }

        tuple->set_rec_status(
            remote::ToRemoteType::ConvertRecordStatus(cce->payload_status_));
        tuple->set_key_ts(cce->commit_ts_);

        if (include_gap)
        {
            tuple->set_gap_ts(0);
        }
        else
        {
            tuple->set_gap_ts(0);
        }

        remote::CceAddr_msg *cce_addr = tuple->mutable_cce_addr();
        cce_addr->set_cce_ptr(reinterpret_cast<uint64_t>(cce));
        cce_addr->set_term(ng_term);
        // For remote scans, the returned cc entries' node group ID is set
        // on the sender side when the sender receives the response.

        remote_cache->cache_mem_size_ += tuple_size;
    }
};
}  // namespace txservice
