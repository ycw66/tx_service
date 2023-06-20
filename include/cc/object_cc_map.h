#pragma once

#include <algorithm>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "cc_map.h"
#include "template_cc_map.h"

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
    using CcMap::LockHandleForResumedRequest;
    using CcMap::MoveRequest;
    using CcMap::ReleaseCceKeyLock;
    using CcMap::schema_ts_;
    using CcMap::shard_;
    using CcMap::table_schema_;
    using TemplateCcMap<KeyT, ValueT>::FindEmplace;
    using TemplateCcMap<KeyT, ValueT>::Iterator;
    using TemplateCcMap<KeyT, ValueT>::KeySchema;

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
        CcEntry<KeyT, ValueT> *cce = nullptr;
        bool resume = false;
        const KeyT *target_key = nullptr;
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

        CcOperation cc_op =
            req.IsReadOnly() ? CcOperation::Read : CcOperation::Write;

        if (req.CcePtr() != nullptr)
        {
            // the request was blocked and is now unblocked and lock acquired
            resume = true;
            cce = static_cast<CcEntry<KeyT, ValueT> *>(req.CcePtr());

            std::tie(acquired_lock, err_code) =
                LockHandleForResumedRequest(cce,
                                            cce->payload_status_,
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
        else if (cce_addr.CcePtr() == 0)
        {
            // first time the request is processed
            const TxKey *req_key = req.Key();
            if (req_key != nullptr)
            {
                target_key = static_cast<const KeyT *>(req_key);
            }
            else
            {
                const std::string *key_str = req.KeyImage();
                assert(key_str != nullptr);
                size_t offset = 0;
                decoded_key.Deserialize(key_str->data(), offset, KeySchema());
                target_key = &decoded_key;
            }

            auto it = FindEmplace(*target_key);
            cce = it->second;

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

            std::tie(acquired_lock, err_code) =
                AcquireCceKeyLock(cce,
                                  cce->payload_status_,
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
            assert(ng_id == cce_addr.NodeGroupId());
            cce = reinterpret_cast<CcEntry<KeyT, ValueT> *>(cce_addr.CcePtr());
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

        // Lock acquired
        std::unique_ptr<TxCommand> cmd_uptr = nullptr;
        TxCommand *cmd = nullptr;

        if (req.IsLocal())
        {
            cmd = req.CommandPtr();
        }
        else
        {
            if (req.OwnCommand())
            {
                cmd_uptr = req.ReleaseCommand();
            }
            else
            {
                cmd_uptr = CreateTxCommand(*req.CommandImage());
                cmd = cmd_uptr.get();
            }
        }

        if (cce->payload_status_ == RecordStatus::Unknown)
        {
            cce->commit_ts_ = 1;
            // TODO(zkl): load slice fom kv
            cce->payload_status_ = RecordStatus::Deleted;
        }

        if (cce->payload_status_ == RecordStatus::Deleted)
        {
            // The specified key does not exist. The command is directed at a
            // non-existent object. Whether proceed or not depends on the
            // command.
            bool proceed = cmd->ProceedOnNonExistentObject();
            if (!proceed)
            {
                obj_result.rec_status_ = RecordStatus::Deleted;
                hd_res->SetFinished();
                return true;
            }

            // The command proceeds. Create an empty object.
            // todo: CreateObject return shared_ptr to save one allocation
            std::shared_ptr<TxRecord> obj = cmd->CreateObject(nullptr);
            cce->payload_ = std::dynamic_pointer_cast<ValueT>(obj);
            cce->payload_status_ = RecordStatus::Normal;
        }

        ValueT &object = *cce->payload_;

        {
            // TODO(zkl): make a temp object or use undo op
            // Temporarily commit pending commands here so that txn can read its
            // own write if it issues many commands against the same object.
            object.CommitCommands();
        }

        cmd->ExecuteOn(object);

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
                std::max(cce->last_read_ts_, shard_->Now());
        }
        obj_result.commit_ts_ = cce->commit_ts_;
        obj_result.rec_status_ = cce->payload_status_;

        if (req.apply_and_commit_ && !cmd->IsReadOnly())
        {
            assert(acquired_lock == LockType::WriteLock);
            // skipping writing log, release the lock and commit the command
            object.CommitCommands();
            ReleaseCceKeyLock(cce, txn, ng_id);
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
        bool is_del = op_type == OperationType::Delete;

        const CcEntryAddr *cce_addr = req.CceAddr();

        CcEntry<KeyT, ValueT> *cce =
            reinterpret_cast<CcEntry<KeyT, ValueT> *>(cce_addr->CcePtr());

        if (cce->key_lock_ptr_ != nullptr &&
            cce->key_lock_ptr_->HasWriteLock() &&
            cce->key_lock_ptr_->WriteLockTx() != txn)
        {
            req.Result()->SetFinished();
            return true;
        }

        if (commit_ts > 0)
        {
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

            shard_->DecrementMemory(cce->PayloadMemUsage());
            if (is_del)
            {
                cce->payload_ = nullptr;
            }
            else
            {
                cce->payload_->CommitCommands();
            }
            shard_->mem_usage_ += cce->PayloadMemUsage();

            cce->commit_ts_ = commit_ts;
            cce->payload_status_ =
                is_del ? RecordStatus::Deleted : RecordStatus::Normal;
        }

        ReleaseCceKeyLock(cce, txn, req.NodeGroupId());
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
        size_t offset = 0;
        const std::string_view &log_blob = req.LogContentView();
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
                continue;
            }

            LOG(INFO) << "replay log key: " << key.ToString()
                      << ", obj_ver: " << obj_version
                      << ", commit ts: " << commit_ts
                      << ", cmds len: " << cmds_len << ", cmds str: "
                      << std::string_view(log_blob.data() + offset, cmds_len);

            // TODO(zkl): get object from kv asynchronously, concurrent with
            //  replaying log? First read from kv then have the RedisMonoObject,
            //  or create RedisMonoObject and fill the record later?
            auto it = FindEmplace(key);
            CcEntry<KeyT, ValueT> *cce = it->second;

            if (cce == nullptr)
            {
                req.Result()->SetError(CcErrorCode::OUT_OF_MEMORY);
                return true;
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

            if (cce->payload_ == nullptr)
            {
                assert(!cmd_list.empty());
                // create object using one of the TxCommand
                TxCommand *cmd = cmd_list.at(0).get();
                std::shared_ptr<TxRecord> tx_rec_ptr =
                    cmd->CreateObject(nullptr);

                std::shared_ptr<ValueT> obj_ptr =
                    std::dynamic_pointer_cast<ValueT>(tx_rec_ptr);

                cce->payload_ = std::move(obj_ptr);
                cce->payload_status_ = RecordStatus::Normal;
            }

            ValueT &object = *cce->payload_;

            // Emplace commands and try to commit them.
            object.EmplaceAndCommitReplayTxnCommand(
                obj_version, commit_ts, std::move(cmd_list), cce->commit_ts_);

            if (cce->key_lock_ptr_ != nullptr &&
                cce->key_lock_ptr_->HasWriteLock())
            {
                // If the record in the log has a commit ts greater than
                // that of the cc entry and the cc entry has a write
                // lock, the lock's owner must be the tx that commits
                // the log record.
                // TODO: it is safer if we ship the tx ID with the
                // recovering message and match it against the lock holder.
                TxNumber txn = cce->key_lock_ptr_->WriteLockTx();
                ReleaseCceKeyLock(cce, txn, req.NodeGroupId());
            }
        }

        if (shard_->core_id_ < shard_->core_cnt_ - 1)
        {
            req.ResetCcm();
            MoveRequest(&req, shard_->core_id_ + 1);
        }
        else
        {
            req.SetFinish();
        }

        return false;
    }

private:
    std::unique_ptr<TxCommand> CreateTxCommand(std::string_view cmd_image)
    {
        assert(table_schema_ != nullptr);
        auto cmd_uptr = table_schema_->CreateTxCommand(cmd_image);
        assert(cmd_uptr != nullptr);
        return cmd_uptr;
    }
};
}  // namespace txservice
