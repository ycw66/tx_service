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
        int64_t ng_term = Sharder::Instance().LeaderTerm(ng_id);
        CODE_FAULT_INJECTOR("term_TemplateCcMap_Execute_ApplyCc", {
            LOG(INFO) << "FaultInject  term_TemplateCcMap_Execute_ApplyCc";
            ng_term = -1;
        });
        if (ng_term < 0)
        {
            LOG(INFO) << "ApplyCc, node_group(#" << ng_id
                      << ") term < 0, tx:" << req.Txn();
            hd_res->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            return true;
        }

        // TODO(zkl): Read and PinRangeSlice, load from kv; wait for replay to
        //  finish

        if (req.CcePtr() != nullptr)
        {
            // the request was blocked and is now unblocked
            resume = true;
            cce = static_cast<CcEntry<KeyT, ValueT> *>(req.CcePtr());
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

            assert(cce != nullptr);
            cce_addr.SetCce(reinterpret_cast<uint64_t>(cce), ng_term, ng_id);
        }
        else
        {
            // the request is re-executed due to memory exceed
            assert(ng_id == cce_addr.NodeGroupId());
            cce = reinterpret_cast<CcEntry<KeyT, ValueT> *>(cce_addr.CcePtr());
        }

        std::unique_ptr<TxCommand> cmd_uptr = nullptr;
        const TxCommand *cmd = nullptr;
        std::unique_ptr<TxCommandResult> cmd_result_uptr = nullptr;
        TxCommandResult *cmd_result = nullptr;

        if (req.IsLocal())
        {
            cmd = req.CommandPtr();
            cmd_result = req.CommandResultPtr();
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

        if (req.apply_and_commit_ || cmd->IsReadOnly())
        {
            // If this is a simple auto commit command and skip_wal is set,
            // apply the command to get the result and commit it, skipping
            // acquiring lock and writing log; if this is a read only command,
            // apply the command on the object to get the result and return it.
            if (req.IsRemote())
            {
                cmd_result_uptr = cmd->CreateCommandResult();
                cmd_result = cmd_result_uptr.get();
                req.SetCommandResult(std::move(cmd_result_uptr));
            }

            cmd->Apply(*cce->payload_, *cmd_result);
            if (!cmd->IsReadOnly())
            {
                // skipping acquiring lock and write log, commit the command
                cce->payload_->CommitCommands();
            }

            obj_result.commit_ts_ = cce->commit_ts_;
            obj_result.last_read_ts_ = cce->last_read_ts_;
            obj_result.rec_status_ = cce->payload_status_;

            hd_res->SetFinished();
            return true;
        }

        // This is a read-modify-write command. First acquire write lock on
        // the object.
        int64_t tx_term = req.TxTerm();
        NonBlockingLock *cce_lock_ptr = &cce->GetKeyLock();
        bool lock_success =
            resume || cce_lock_ptr->AcquireWriteLock(&req, req.Protocol());
        if (lock_success)
        {
            shard_->UpsertLockHoldingTx(req.Txn(), tx_term, cce, true, ng_id);
            // for mvcc
            uint64_t lock_ts = std::max(req.TxTs(), shard_->Now());
            cce_lock_ptr->SetWLockTs(lock_ts);

            // Updates last_vali_ts after successfully acquiring the write
            // lock such that it is not smaller than the current time of
            // the shard. The net effect is that the tx acquiring the write
            // lock is forced not to commit at a time earlier than the
            // clock of this cc node, even if the clock of the tx's
            // coordinator node drifts and falls behind. Checkpointing
            // relies on this property to avoid picking a checkpoint ts in
            // this shard that may overlap with the ongoing tx.
            obj_result.last_read_ts_ = std::max(cce->last_read_ts_, lock_ts);

            if (req.IsRemote())
            {
                cmd_result_uptr = cmd->CreateCommandResult();
                cmd_result = cmd_result_uptr.get();
                req.SetCommandResult(std::move(cmd_result_uptr));
            }

            cmd->Apply(*cce->payload_, *cmd_result);

            obj_result.commit_ts_ = cce->commit_ts_;
            obj_result.last_read_ts_ = cce->last_read_ts_;
            obj_result.rec_status_ = cce->payload_status_;

            hd_res->SetFinished();
            return true;
        }
        else
        {
            TX_TRACE_ACTION_WITH_CONTEXT(
                &req,
                "AcquireWriteLock.Fail",
                cce,
                [&req]() -> std::string
                {
                    return std::string(",\"tx_number\":")
                        .append(std::to_string(req.Txn()))
                        .append(",\"term\":")
                        .append(std::to_string(req.TxTerm()));
                });

            // When the request is blocked due to failing to acquire the write
            // lock, transfers the ownership of the command to the cc request,
            // so that later when the request is unblocked and re-executed, the
            // command is re-used.
            if (req.IsRemote())
            {
                req.SetCommand(std::move(cmd_uptr));
            }

            CcErrorCode error_code;
            // If the request fails to acquire write lock because of read locks,
            // check each read lock and recover it if needed.
            const std::unordered_set<TxNumber> &read_locks =
                cce_lock_ptr->ReadLocks();
            if (read_locks.size() > 0)
            {
                TX_TRACE_DUMP_WITH_CONTEXT(
                    &read_locks,
                    [&cc_entry]() -> std::string
                    {
                        return std::string("\"CcEntry\":")
                            .append(FMT_POINTER_TO_UINT64T(cce))
                            .append(",\"associate\":\"key_lock_.read_locks\"");
                    });
                error_code =
                    CcErrorCode::ACQUIRE_KEY_LOCK_FAILED_FOR_RW_CONFLICT;
                for (const auto &read_tx : read_locks)
                {
                    shard_->CheckRecoverTx(read_tx, ng_id, ng_term);
                }
            }
            else  // acquire lock fails due to write-write conflict
            {
                TX_TRACE_DUMP_WITH_CONTEXT(
                    cce->key_lock_.WriteLockTx(),
                    [cce]() -> std::string
                    {
                        return std::string("\"CcEntry\":")
                            .append(FMT_POINTER_TO_UINT64T(cce))
                            .append(",\"associate\":\"key_lock_.write_lock\"");
                    });
                assert(cce_lock_ptr->HasWriteLock());
                error_code =
                    CcErrorCode::ACQUIRE_KEY_LOCK_FAILED_FOR_WW_CONFLICT;
                shard_->CheckRecoverTx(
                    cce_lock_ptr->WriteLockTx(), ng_id, ng_term);
            }

            if (req.Protocol() == CcProtocol::OCC || shard_->EnableMvcc())
            {
                // For OCC/MVCC, a conflict causes the tx to abort immediately.
                hd_res->SetError(error_code);
            }
            else
            {
                // For 2PL, a conflict blocks the tx by putting the request into
                // the lock's blocking queue.
                int32_t tx_node = (req.Txn() >> 32L) >> 10;
                if (tx_node != req.NodeGroupId())
                {
                    // If the acquire request comes from a remote node,
                    // sends acknowledgement to the sender when the request
                    // is blocked.

                    // remote::RemoteAcquire &remote_req =
                    //     static_cast<remote::RemoteAcquire &>(req);
                    // remote_req.Acknowledge();
                }
                return false;
            }
        }

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
                //                LOG(INFO) << "create replaying command len: "
                //                << cmd_len;
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

            // try to commit replay commands and update commit ts
            object.TryCommitReplayCommands(cce->commit_ts_);
        }

        if (shard_->core_id_ < shard_->core_cnt_ - 1)
        {
            req.ResetCcm();
            CcMap::MoveRequest(&req, shard_->core_id_ + 1);
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
