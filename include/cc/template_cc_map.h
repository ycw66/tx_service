#pragma once

#include <algorithm>  // std::max
#include <chrono>
#include <map>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>  // std::pair
#include <vector>

#include "cc_entry.h"
#include "cc_map.h"
#include "cc_protocol.h"
#include "cc_req_misc.h"
#include "cc_request.h"
#include "cc_shard.h"
#include "error_messages.h"  //CcErrorCode
#include "fault/fault_inject.h"
#include "local_cc_shards.h"
#include "proto/cc_request.pb.h"
#include "remote/remote_cc_handler.h"  //RemoteCcHandler
#include "remote/remote_cc_request.h"
#include "remote/remote_type.h"
#include "sharder.h"
#include "statistics.h"
#include "store/data_store_handler.h"
#include "tx_execution.h"
#include "tx_id.h"
#include "tx_key.h"
#include "tx_trace.h"
#include "type.h"
#include "typed_statistics.h"

#ifdef RANGE_PARTITION_ENABLED
#include "range_slice.h"
#endif

namespace txservice
{
template <typename KeyT, typename ValueT>
class TemplateCcMap : public CcMap
{
public:
    TemplateCcMap() = delete;
    TemplateCcMap(const TemplateCcMap &rhs) = delete;
    explicit TemplateCcMap(CcMap &&rhs) = delete;

    TemplateCcMap(CcShard *shard,
                  NodeGroupId cc_ng_id,
                  const TableName &table_name,
                  uint64_t schema_ts,
                  const TableSchema *table_schema = nullptr,
                  bool ccm_has_full_entries = false)
        : CcMap(shard,
                cc_ng_id,
                table_name,
                table_schema,
                schema_ts,
                ccm_has_full_entries),
          pg_ng_inf_(this),
          pg_ps_inf_(this),
          neg_inf_(this, &pg_ng_inf_),
          pos_inf_(this, &pg_ps_inf_),
          maintain_statistics_(false),
          shard_profile_(nullptr)
    {
        pg_ng_inf_.prev_page_ = nullptr;
        pg_ng_inf_.next_page_ = &pg_ps_inf_;
        pg_ps_inf_.prev_page_ = &pg_ng_inf_;
        pg_ps_inf_.next_page_ = nullptr;

        pg_ng_inf_.ckpt_next_ = &pg_ps_inf_;
        pg_ps_inf_.ckpt_prev_ = &pg_ng_inf_;

        if (table_name.Type() == TableType::Primary ||
            table_name.Type() == TableType::Secondary)
        {
            uint32_t shard_code = Sharder::Instance().ShardCode(
                std::hash<std::string_view>{}(table_name.GetBaseTableNameSV()));
            NodeGroupId shard_id =
                Sharder::Instance().ShardToCcNodeGroup(shard_code);
            uint16_t core_id = (shard_code & 0x3FF) %
                               Sharder::Instance().GetLocalCcShards()->Count();
            maintain_statistics_ =
                (shard_id == cc_ng_id) && (core_id == shard->LocalCoreId());

            if (maintain_statistics_ && table_schema)
            {
                assert(table_schema->StatisticsObject());
                ShardProfile *shard_profile = table_schema->StatisticsObject()
                                                  ->GetShardProfile(table_name)
                                                  .get();
                assert(shard_profile);
                shard_profile_ =
                    static_cast<TypedShardProfile<KeyT> *>(shard_profile);
            }
        }

        TX_TRACE_ASSOCIATE_WITH_CONTEXT(
            (txservice::CcMap *) this,
            (txservice::LruEntry *) &neg_inf_,
            [this]() -> std::string
            {
                return std::string("\"associate\":\"neg_inf_\", \"cce_ptr_\":")
                    .append(std::to_string(
                        reinterpret_cast<uint64_t>(&this->neg_inf_)))
                    .append("\"key_\":")
                    .append(std::to_string(
                        reinterpret_cast<uint64_t>(&this->neg_inf_.Key())))
                    .append("\"table_name_\":")
                    .append(this->table_name_.StringView());
            });

        TX_TRACE_ASSOCIATE_WITH_CONTEXT(
            (txservice::CcMap *) this,
            (txservice::LruEntry *) &pos_inf_,
            [this]() -> std::string
            {
                return std::string("\"associate\":\"neg_inf_\", \"cce_ptr_\":")
                    .append(std::to_string(
                        reinterpret_cast<uint64_t>(&this->pos_inf_)))
                    .append("\"key_\":")
                    .append(std::to_string(
                        reinterpret_cast<uint64_t>(&this->pos_inf_.key_)))
                    .append("\"table_name_\":")
                    .append(this->table_name_.StringView());
            });
    }

    virtual ~TemplateCcMap()
    {
        Clean();
    }

    bool Execute(AcquireCc &req) override
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

        CcHandlerResult<std::vector<AcquireKeyResult>> *hd_res = req.Result();
        AcquireKeyResult &acquire_key_result =
            req.IsLocal() ? hd_res->Value()[req.HandlerResultIndex()]
                          : hd_res->Value()[0];
        CcEntryAddr &cce_addr = acquire_key_result.cce_addr_;
        CcEntry<KeyT, ValueT> *cce_ptr = nullptr;
        bool resume = false;
        const KeyT *target_key = nullptr;
        KeyT decoded_key;

        int64_t ng_term = Sharder::Instance().LeaderTerm(req.NodeGroupId());
        CODE_FAULT_INJECTOR("term_TemplateCcMap_Execute_AcquireCc", {
            LOG(INFO) << "FaultInject  term_TemplateCcMap_Execute_AcquireCc";
            ng_term = -1;
        });
        if (ng_term < 0)
        {
            hd_res->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            return true;
        }

        LockType acquired_lock = LockType::NoLock;
        CcErrorCode err_code = CcErrorCode::NO_ERROR;
        if (req.CcePtr() != nullptr)
        {
            // The request was blocked before and is now unblocked.
            resume = true;
            cce_ptr = static_cast<CcEntry<KeyT, ValueT> *>(req.CcePtr());

            std::tie(acquired_lock, err_code) =
                LockHandleForResumedRequest(cce_ptr,
                                            cce_ptr->payload_status_,
                                            &req,
                                            req.NodeGroupId(),
                                            ng_term,
                                            req.TxTerm(),
                                            CcOperation::Write,
                                            req.Isolation(),
                                            req.Protocol(),
                                            0);
        }
        else
        {
            // First time the request is processed.

            const TxKey *req_key = req.Key();
            if (req_key != nullptr)
            {
                target_key = static_cast<const KeyT *>(req_key);
            }
            else
            {
                const std::string *key_str = req.KeyStr();

                assert(key_str != nullptr);

                size_t offset = 0;
                decoded_key.Deserialize(key_str->data(), offset, KeySchema());
                target_key = &decoded_key;
            }

            if (req.IsInsert())
            {
                Iterator it = Floor(*target_key);
                const KeyT *key_ptr = it->first;
                cce_ptr = it->second;

                if (cce_ptr != &neg_inf_ && *key_ptr == *target_key)
                {
                    // The floor entry's key is equal to the insert key. If the
                    // key is deleted, the insert becomes an update. Or the
                    // insert is aborted due to the duplidate key conflict.
                    if (cce_ptr->payload_status_ == RecordStatus::Deleted)
                    {
                        cce_addr.SetCce(reinterpret_cast<uint64_t>(cce_ptr),
                                        ng_term,
                                        req.NodeGroupId(),
                                        shard_->LocalCoreId());
                    }
                    else
                    {
                        // Inserts a duplicate key.
                        hd_res->SetError(CcErrorCode::DUPLICATE_INSERT_ERR);
                        return true;
                    }
                }

                req.SetCcePtr(cce_ptr);
            }
            else
            {
                Iterator it = FindEmplace(*target_key);
                cce_ptr = it->second;

                if (cce_ptr == nullptr)
                {
                    // The acquire request needs a new cc entry but the cc map
                    // has reached the maximal capacity.
                    req.Result()->SetError(CcErrorCode::OUT_OF_MEMORY);
                    return true;
                }

                assert(cce_ptr != nullptr);
                cce_addr.SetCce(reinterpret_cast<uint64_t>(cce_ptr),
                                ng_term,
                                req.NodeGroupId(),
                                shard_->LocalCoreId());
                req.SetCcePtr(cce_ptr);
            }
        }

        // Cce ptr either points to the cc entry whose gap will accommodate the
        // new insert, or the cc entry whose key will be updated/deleted.
        CcEntry<KeyT, ValueT> &cc_entry = *cce_ptr;

        if (cce_addr.CcePtr() == 0)
        {
            if (table_name_.Type() == TableType::Secondary)
            {
                // TODO: Sk Insert branch needs rethinking, currently useless.
                assert(false);
            }

            // This is an insert. The new insert results in an insert entry in
            // the intention set of the preceding key's gap.

            auto ins_it = cc_entry.insert_intention_set_.find(target_key);
            if (ins_it != cc_entry.insert_intention_set_.end())
            {
                InsertEntry<KeyT, ValueT> &insert_entry = *ins_it->second;
                if (!(insert_entry.txn_ == req.Txn()))
                {
                    // If the same key is already in the insert intention set,
                    // and its tx ID does not matches the request's, this is a
                    // duplicate insert. Aborts the tx.
                    hd_res->SetError(CcErrorCode::DUPLICATE_INSERT_ERR);
                    return true;
                }
            }

            std::unique_ptr<InsertEntry<KeyT, ValueT>> insert_entry =
                std::make_unique<InsertEntry<KeyT, ValueT>>(
                    *target_key, req.Txn(), cce_ptr);
            cce_addr.SetInsert(reinterpret_cast<uint64_t>(insert_entry.get()),
                               ng_term,
                               req.NodeGroupId(),
                               shard_->LocalCoreId());

            cc_entry.insert_intention_set_.emplace(&insert_entry->key_,
                                                   std::move(insert_entry));
            // Cc entry address has been updated. Only reset the result's last
            // validation ts.
            acquire_key_result.last_vali_ts_ = cc_entry.gap_last_read_ts_;
            acquire_key_result.commit_ts_ = cc_entry.commit_ts_;
            hd_res->SetFinished();
        }
        else
        {
            if (!resume)
            {
                std::tie(acquired_lock, err_code) =
                    AcquireCceKeyLock(&cc_entry,
                                      cc_entry.payload_status_,
                                      &req,
                                      req.NodeGroupId(),
                                      ng_term,
                                      req.TxTerm(),
                                      CcOperation::Write,
                                      req.Isolation(),
                                      req.Protocol(),
                                      0);
            }

            if (err_code == CcErrorCode::NO_ERROR)
            {
                assert(acquired_lock == LockType::WriteLock);
                // for mvcc
                uint64_t lock_ts = std::max(req.Ts(), shard_->Now());
                cc_entry.key_lock_ptr_->SetWLockTs(lock_ts);

                // Updates last_vali_ts after successfully acquiring the write
                // lock such that it is no smaller than the current time of
                // the shard. The net effect is that the tx acquiring the write
                // lock is forced not to commit at a time earlier than the
                // clock of this cc node, even if the clock of the tx's
                // coordinator node drifts and falls behind. Checkpointing
                // relies on this property to avoid picking a checkpoint ts in
                // this shard that may overlap with the ongoing tx.
                acquire_key_result.last_vali_ts_ =
                    std::max(cc_entry.last_read_ts_, lock_ts);
                acquire_key_result.commit_ts_ = cc_entry.commit_ts_;

                hd_res->SetFinished();
            }
            else if (err_code == CcErrorCode::ACQUIRE_LOCK_BLOCKED)
            {
                // For 2PL, a conflict blocks the tx by putting it into the
                // lock's blocking queue.

                uint32_t tx_node = (req.Txn() >> 32L) >> 10;
                if (tx_node != req.NodeGroupId())
                {
                    // If the acquire request comes from a remote node,
                    // sends acknowledgement to the sender when the request
                    // is blocked.
                    remote::RemoteAcquire &remote_req =
                        static_cast<remote::RemoteAcquire &>(req);
                    remote_req.Acknowledge();
                }

                return false;
            }
            else
            {
                // lock confilct: back off and retry.
                req.Result()->SetError(err_code);
                return true;
            }
        }

        return true;
    }

    bool Execute(PostWriteCc &req) override
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

        CODE_FAULT_INJECTOR("delay_release_write_lock_on_pk", {
            if (table_name_.Type() == TableType::Primary)
            {
                // throw back to cc_queue
                shard_->Enqueue(shard_->LocalCoreId(), &req);
                return false;
            }
        });

        const CcEntryAddr *cce_addr = req.CceAddr();
        bool is_forward = cce_addr == nullptr;

        CODE_FAULT_INJECTOR("term_TemplateCcMap_Execute_PostWriteCc", {
            if (table_name_.Type() == TableType::Primary)
            {
                LOG(INFO) << "FaultInject  "
                             "term_TemplateCcMap_Execute_PostWriteCc";
                req.Result()->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
                return true;
            }
        });

        if (!is_forward && !Sharder::Instance().CheckLeaderTerm(
                               cce_addr->NodeGroupId(), cce_addr->Term()))
        {
            req.Result()->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            return true;
        }

        const ValueT *commit_val = static_cast<const ValueT *>(req.Payload());
        TxNumber txn = req.Txn();
        uint64_t commit_ts = req.CommitTs();
        const std::string *payload_str = req.PayloadStr();
        OperationType op_type = req.GetOperationType();
        bool is_del = op_type == OperationType::Delete;

        if (!is_forward && cce_addr->InsertPtr() != 0)
        {
            // DEAD BRANCH FOR NOW
            if (table_name_.Type() == TableType::Secondary)
            {
                // TODO: Sk Insert branch needs rethinking, currently useless.
                assert(false);
                return true;
            }

            // insert branch.
            assert(is_del == false);

            InsertEntry<KeyT, ValueT> &insert_entry =
                *reinterpret_cast<InsertEntry<KeyT, ValueT> *>(
                    cce_addr->InsertPtr());
            CcEntry<KeyT, ValueT> &prior_cce = *insert_entry.parent_entry_;

            if (commit_ts == 0)
            {
                // When the commit ts is 0, this request has a sole purpose:
                // undoes any effects left by the write operation. This is used
                // when the tx receives the abort command before entering the
                // commit phase. For an insert, in addition to removing the
                // write lock, the undo operation also includes removing the
                // temporary insert entry in the gap.
                CcEntry<KeyT, ValueT> &parent_entry =
                    *insert_entry.parent_entry_;
                parent_entry.insert_intention_set_.erase(&insert_entry.key_);
            }
            else
            {
                Iterator insert_it = Emplace(insert_entry.key_);
                const KeyT *key_ptr = insert_it->first;
                CcEntry<KeyT, ValueT> *new_cce = insert_it->second;

                if (new_cce == nullptr)
                {
                    // The cc map has reached the maximal capacity.
                    req.Result()->SetError(CcErrorCode::OUT_OF_MEMORY);
                    return true;
                }

                auto ite =
                    prior_cce.insert_intention_set_.find(&insert_entry.key_);
                assert(ite != prior_cce.insert_intention_set_.end());
                assert(ite->second->txn_ == txn);

                shard_->DecrementMemory(new_cce->PayloadMemUsage());
                if (payload_str == nullptr)
                {
                    new_cce->payload_ = std::make_shared<ValueT>(*commit_val);
                }
                else
                {
                    size_t offset = 0;
                    new_cce->payload_ = std::make_shared<ValueT>();
                    new_cce->payload_->Deserialize(payload_str->data(), offset);
                }
                shard_->mem_usage_ += new_cce->PayloadMemUsage();
                new_cce->payload_status_ = RecordStatus::Normal;

                ++ite;
                for (auto it = ite; it != prior_cce.insert_intention_set_.end();
                     ++it)
                {
                    InsertEntry<KeyT, ValueT> &insert_entry = *it->second.get();
                    insert_entry.parent_entry_ = new_cce;
                    new_cce->insert_intention_set_.emplace(
                        it->first, std::move(it->second));
                }

                new_cce->gap_commit_ts_ = commit_ts;
                new_cce->commit_ts_ = commit_ts;
                new_cce->gap_last_read_ts_ = prior_cce.gap_last_read_ts_;

                prior_cce.gap_commit_ts_ = commit_ts;
                prior_cce.insert_intention_set_.erase(
                    --ite, prior_cce.insert_intention_set_.end());

                TryInsertCkptList(new_cce);

                size_t key_size = key_ptr->SerializedLength();
                size_t payload_size = new_cce->PayloadSerializedLength();
                shard_->UpdateEstimateLogSize(new_cce, key_size, payload_size);

                if (maintain_statistics_)
                {
                    shard_profile_->OnInsert(*key_ptr);
                }
            }

            // The insert places a write lock on the prior cc entry's gap.
            ReleaseCceGapLock(&prior_cce, txn, req.NodeGroupId());
            req.Result()->SetFinished();
            return true;
        }
        else
        {
            // upsert and delete branch.
            CcEntry<KeyT, ValueT> *cce;
            if (is_forward)
            {
                // Find the cce location first
                const TxKey *req_key = req.Key();
                const KeyT *key;
                KeyT decoded_key;
                if (req_key != nullptr)
                {
                    key = static_cast<const KeyT *>(req_key);
                }
                else
                {
                    const std::string *key_str = req.KeyStr();

                    assert(key_str != nullptr);

                    size_t offset = 0;
                    decoded_key.Deserialize(
                        key_str->data(), offset, KeySchema());
                    key = &decoded_key;
                }

                Iterator it = FindEmplace(*key);
                cce = it->second;

                if (cce == nullptr)
                {
                    // The acquire request needs a new cc entry but the cc map
                    // has reached the maximal capacity. Blocks the request by
                    // putting it back to the cc request queue.
                    req.Result()->SetError(CcErrorCode::OUT_OF_MEMORY);
                    return true;
                }
                // Since this is a forward req, we assume this entry is not
                // visible on this ng yet so no need to check for lock.
            }
            else
            {
                cce = reinterpret_cast<CcEntry<KeyT, ValueT> *>(
                    cce_addr->CcePtr());

                if (cce->key_lock_ptr_ != nullptr &&
                    cce->key_lock_ptr_->HasWriteLock() &&
                    cce->key_lock_ptr_->WriteLockTx() != txn)
                {
                    req.Result()->SetFinished();
                    return true;
                }
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

                // for mvcc
                if (shard_->EnableMvcc())
                {
                    // Archives whose version are bigger than ccentry's ckpt_ts_
                    // may be in use by checkpointer and must not be deleted
                    // here. These expired archives will be deleted at next
                    // checkpoint.
                    uint64_t recycle_ts = std::min(
                        shard_->GlobalMinSiTxStartTs(), cce->ckpt_ts_.load());
                    shard_->DecrementMemory(
                        cce->KickOutArchiveRecords(recycle_ts));
                    size_t added_mem_usage = cce->ArchiveBeforeUpdate(Type());
                    shard_->mem_usage_ += added_mem_usage;
                }

                cce->commit_ts_ = commit_ts;

                // FIXME: when working with MySQL, the key contains a binary
                // image and a sturcture for unpack info. Unfortunately, the
                // unpack info is stored as part of the record. As a result,
                // if we want to preserve the full encoding of the key in
                // the data store when the row is deleted, we'd have to keep
                // the whole record in the cc entry. This is a bad design
                // and should be fixed: the unpack info is part of the key,
                // not the record.
                //
                // Now, all versions of SecondaryIndex key shared the unpack
                // info in current version's payload, though the unpack info
                // will not be used for deleted key, we must not change the
                // payload of secondary key ccentry if it is not null.
                if (Type() != TableType::Secondary || cce->payload_ == nullptr)
                {
                    shard_->DecrementMemory(cce->PayloadMemUsage());
                    if (is_del)
                    {
                        cce->payload_ = nullptr;
                    }
                    else if (payload_str == nullptr)
                    {
                        cce->payload_ = std::make_shared<ValueT>(*commit_val);
                    }
                    else
                    {
                        size_t offset = 0;
                        cce->payload_ = std::make_shared<ValueT>();
                        cce->payload_->Deserialize(payload_str->data(), offset);
                    }
                    shard_->mem_usage_ += cce->PayloadMemUsage();
                }

                // todo: get key from cce_addr
                size_t key_size = cce->Key()->SerializedLength();
                size_t payload_size = cce->PayloadSerializedLength();
                shard_->UpdateEstimateLogSize(cce, key_size, payload_size);

                cce->payload_status_ =
                    is_del ? RecordStatus::Deleted : RecordStatus::Normal;
                TryInsertCkptList(cce);

                DLOG_IF(INFO, TRACE_OCC_ERR)
                    << "PostWriteCc, txn:" << txn << " ,cce: " << cce
                    << " ,commit_ts: " << commit_ts;

                if (maintain_statistics_)
                {
                    if (op_type == OperationType::Insert)
                    {
                        shard_profile_->OnInsert(
                            *static_cast<const KeyT *>(cce->Key()));
                    }
                    else if (op_type == OperationType::Delete)
                    {
                        shard_profile_->OnDelete(
                            *static_cast<const KeyT *>(cce->Key()));
                    }
                }
            }

            ReleaseCceKeyLock(cce, txn, req.NodeGroupId());
            req.Result()->SetFinished();
            return true;
        }
    }

    bool Execute(AcquireAllCc &req) override
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

        if (table_name_.Type() == TableType::Secondary)
        {
            assert(false);
            return false;
        }

        CcHandlerResult<AcquireAllResult> *hd_res = req.Result();
        AcquireAllResult &acquire_all_result = hd_res->Value();
        CcEntry<KeyT, ValueT> *cce_ptr = nullptr;
        bool resume = false;
        const KeyT *target_key = nullptr;
        bool will_insert = false;

        CODE_FAULT_INJECTOR("term_TemplateCcMap_Execute_AcquireAllCc", {
            LOG(INFO) << "FaultInject  term_TemplateCcMap_Execute_AcquireAllCc";
            hd_res->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            return true;
        });

        uint32_t ng_id = req.NodeGroupId();
        int64_t ng_term = Sharder::Instance().LeaderTerm(ng_id);
        if (ng_term < 0)
        {
            hd_res->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            return true;
        }

        uint16_t tx_core_id = ((req.Txn() >> 32L) & 0x3FF) % shard_->core_cnt_;

        LockType acquired_lock = LockType::NoLock;
        CcErrorCode err_code = CcErrorCode::NO_ERROR;
        if (req.CcePtr() != nullptr)
        {
            // The request was blocked before and is now unblocked.
            resume = true;
            cce_ptr = static_cast<CcEntry<KeyT, ValueT> *>(req.CcePtr());
            std::tie(acquired_lock, err_code) =
                LockHandleForResumedRequest(cce_ptr,
                                            cce_ptr->payload_status_,
                                            &req,
                                            ng_id,
                                            ng_term,
                                            req.TxTerm(),
                                            req.CcOp(),
                                            req.Isolation(),
                                            req.Protocol(),
                                            0);
        }
        else
        {
            // First time the request is processed in this shard. Or the request
            // is blocked previously because the cc map is full.

            const TxKey *req_key = req.Key();
            if (req_key != nullptr)
            {
                target_key = static_cast<const KeyT *>(req_key);
            }
            else
            {
                switch (*req.KeyStrType())
                {
                case KeyType::NegativeInf:
                    target_key = NegativeInfinity<KeyT>::Instance();
                    break;
                case KeyType::PositiveInf:
                    target_key = PositiveInfinity<KeyT>::Instance();
                    break;
                case KeyType::Normal:
                    const std::string *key_str = req.KeyStr();
                    assert(key_str != nullptr);
                    std::unique_ptr<KeyT> decoded_key =
                        std::make_unique<KeyT>();
                    size_t offset = 0;
                    decoded_key->Deserialize(
                        key_str->data(), offset, KeySchema());
                    target_key = decoded_key.get();
                    req.SetDecodedKey(std::move(decoded_key));
                    break;
                }
            }

            if (req.IsInsert())
            {
                // For insert requests, finds a cc entry whose gap will
                // accommodate the insert key.
                Iterator it = Floor(*target_key);
                const KeyT *key_ptr = it->first;
                cce_ptr = it->second;

                if (cce_ptr != &neg_inf_ && *key_ptr == *target_key)
                {
                    // The floor entry's key is equal to the insert key. If the
                    // key is deleted, the insert becomes an update. Or the
                    // insert is aborted due to the duplidate key conflict.
                    if (cce_ptr->payload_status_ != RecordStatus::Deleted)
                    {
                        // Inserts a duplicate key.
                        hd_res->SetError(CcErrorCode::DUPLICATE_INSERT_ERR);
                        return true;
                    }
                    else
                    {
                        if (shard_->core_id_ == tx_core_id)
                        {
                            acquire_all_result.local_cce_addr_.SetCce(
                                reinterpret_cast<uint64_t>(cce_ptr),
                                ng_term,
                                req.NodeGroupId(),
                                shard_->LocalCoreId());
                        }

                        req.SetCcePtr(cce_ptr);
                    }
                }
                else
                {
                    will_insert = true;
                }
            }
            else
            {
                Iterator it = FindEmplace(*target_key);
                cce_ptr = it->second;

                if (cce_ptr == nullptr)
                {
                    // The acquire request needs a new cc entry but the cc map
                    // has reached the maximal capacity. Blocks the request by
                    // putting it back to the cc request queue.
                    hd_res->SetError(CcErrorCode::OUT_OF_MEMORY);
                    return true;
                }

                req.SetCcePtr(cce_ptr);
            }
        }

        // Cce ptr either points to the cc entry whose gap will accommodate the
        // new insert, or the cc entry whose key will be updated/deleted.
        CcEntry<KeyT, ValueT> &cc_entry = *cce_ptr;
        TxNumber txn = req.Txn();

        if (will_insert)
        {
            // This is an insert. The new insert results in an insert entry in
            // the intention set of the preceding key's gap.

            auto ins_it = cc_entry.insert_intention_set_.find(target_key);
            if (ins_it != cc_entry.insert_intention_set_.end())
            {
                InsertEntry<KeyT, ValueT> &insert_entry = *ins_it->second;
                if (!(insert_entry.txn_ == txn))
                {
                    // If the same key is already in the insert intention set,
                    // and its tx ID does not matches the request's, this is a
                    // duplicate insert. Aborts the tx.
                    hd_res->SetError(CcErrorCode::DUPLICATE_INSERT_ERR);
                    return true;
                }
            }

            std::unique_ptr<InsertEntry<KeyT, ValueT>> insert_entry =
                std::make_unique<InsertEntry<KeyT, ValueT>>(
                    *target_key, txn, cce_ptr);

            cc_entry.insert_intention_set_.emplace(&insert_entry->key_,
                                                   std::move(insert_entry));
            // Cc entry address has been updated. Only reset the result's last
            // validation ts.
            acquire_all_result.last_vali_ts_ = std::max(
                cc_entry.gap_last_read_ts_, acquire_all_result.last_vali_ts_);
            acquire_all_result.commit_ts_ = cc_entry.commit_ts_;
            acquire_all_result.node_term_ = ng_term;

            hd_res->SetFinished();
        }
        else
        {
            int64_t tx_term = req.TxTerm();
            IsolationLevel iso_lvl = req.Isolation();
            CcProtocol cc_proto = req.Protocol();
            CcOperation cc_op = req.CcOp();

            // On execution resumption, the write lock has been acquired when
            // being unblocked.
            if (!resume)
            {
                std::tie(acquired_lock, err_code) =
                    AcquireCceKeyLock(&cc_entry,
                                      cc_entry.payload_status_,
                                      &req,
                                      req.NodeGroupId(),
                                      ng_term,
                                      tx_term,
                                      cc_op,
                                      iso_lvl,
                                      cc_proto,
                                      0);
            }

            switch (err_code)
            {
            case CcErrorCode::NO_ERROR:
            {
                if (cc_entry.payload_status_ != RecordStatus::Deleted)
                {
                    assert(acquired_lock == LockType::WriteIntent ||
                           acquired_lock == LockType::WriteLock);
                }

                // Updates last_vali_ts such that it is no smaller than (1) all
                // read transactions that have read the item in all shards, and
                // (2) the local time.
                if (shard_->core_id_ == 0)
                {
                    acquire_all_result.last_vali_ts_ = cc_entry.last_read_ts_;
                }
                else
                {
                    acquire_all_result.last_vali_ts_ =
                        std::max(cc_entry.last_read_ts_,
                                 acquire_all_result.last_vali_ts_);
                }

                if (shard_->core_id_ == tx_core_id)
                {
                    acquire_all_result.local_cce_addr_.SetCce(
                        reinterpret_cast<uint64_t>(cce_ptr),
                        ng_term,
                        req.NodeGroupId(),
                        shard_->LocalCoreId());
                    acquire_all_result.commit_ts_ = cc_entry.commit_ts_;
                    acquire_all_result.node_term_ = ng_term;
                }

                // AcquireAllCc request is executed at all shards consecutively.
                // The request is set to be finished after executed at the last
                // shard. At other shards, after the request is executed,
                // MoveRequest() moves the request to the next shard to iterate
                // through all shards.
                if (shard_->core_id_ == shard_->core_cnt_ - 1)
                {
                    hd_res->SetFinished();
                }
                else
                {
                    req.SetCcePtr(nullptr);
                    req.ResetCcm();
                    MoveRequest(&req, shard_->core_id_ + 1);
                    return false;
                }
                break;
            }
            case CcErrorCode::ACQUIRE_LOCK_BLOCKED:
            {
                // If the request comes from a remote node, sends
                // acknowledgement to the sender when the request is
                // blocked.
                if (!req.IsLocal())
                {
                    req.Result()->Value().node_term_ = ng_term;

                    remote::RemoteAcquireAll &remote_req =
                        static_cast<remote::RemoteAcquireAll &>(req);
                    remote_req.Acknowledge();
                }

                return false;
            }
            default:
            {
                // lock confilct: back off and retry.
                req.Result()->SetError(err_code);
                return true;
            }
            }  //-- end: switch
        }      //-- end: acquire lock

        return true;
    }

    bool Execute(PostWriteAllCc &req) override
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

        if (table_name_.Type() == TableType::Secondary)
        {
            assert(false);
            return false;
        }

        int64_t ng_term = Sharder::Instance().LeaderTerm(req.NodeGroupId());
        if (ng_term < 0)
        {
            req.Result()->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            return true;
        }

        const KeyT *target_key = nullptr;
        const TxKey *req_key = req.Key();
        if (req_key != nullptr)
        {
            target_key = static_cast<const KeyT *>(req_key);
        }
        else
        {
            switch (*req.KeyStrType())
            {
            case KeyType::NegativeInf:
                target_key = NegativeInfinity<KeyT>::Instance();
                break;
            case KeyType::PositiveInf:
                target_key = PositiveInfinity<KeyT>::Instance();
                break;
            case KeyType::Normal:
                const std::string *key_str = req.KeyStr();
                assert(key_str != nullptr);
                std::unique_ptr<KeyT> decoded_key = std::make_unique<KeyT>();
                size_t offset = 0;
                decoded_key->Deserialize(key_str->data(), offset, KeySchema());
                target_key = decoded_key.get();
                req.SetDecodedKey(std::move(decoded_key));
                break;
            }
        }

        const ValueT *payload = nullptr;
        const TxRecord *rec = req.Payload();
        if (rec != nullptr)
        {
            payload = static_cast<const ValueT *>(rec);
        }
        // commit_ts = 0 means transaction failed (e.g. failed at prepare
        // phase), we have nothing to upload, only need to release write intent.
        else if (req.CommitTs() > 0)
        {
            if (req.DecodedPayload() == nullptr)
            {
                const std::string *payload_str = req.PayloadStr();
                assert(payload_str != nullptr);
                std::unique_ptr<ValueT> decoded_rec =
                    std::make_unique<ValueT>();
                size_t offset = 0;
                decoded_rec->Deserialize(payload_str->data(), offset);
                payload = decoded_rec.get();
                req.SetDecodedPayload(std::move(decoded_rec));
            }
            else
            {
                payload = static_cast<const ValueT *>(req.DecodedPayload());
            }
        }

        const KeyT *key_ptr = nullptr;
        CcEntry<KeyT, ValueT> *cce_ptr = nullptr;
        if (req.OpType() == OperationType::Insert)
        {
            Iterator it = Floor(*target_key);
            key_ptr = it->first;
            cce_ptr = it->second;
        }
        else
        {
            Iterator it = FindEmplace(*target_key);
            key_ptr = it->first;
            cce_ptr = it->second;
        }

        if (cce_ptr == nullptr)
        {
            req.Result()->SetError(CcErrorCode::OUT_OF_MEMORY);
            return true;
        }

        TxNumber txn = req.Txn();
        uint64_t commit_ts = req.CommitTs();

        if (req.OpType() == OperationType::Insert && *key_ptr != *target_key)
        {
            auto insert_it = cce_ptr->insert_intention_set_.find(target_key);
            if (insert_it != cce_ptr->insert_intention_set_.end() &&
                insert_it->second->txn_ == txn)
            {
                if (commit_ts == 0)
                {
                    // When the commit ts is 0, this request has a sole purpose:
                    // undoes any effects left by the write operation. This is
                    // used when the tx receives the abort command before
                    // entering the commit phase. For an insert, in addition to
                    // removing the write lock, the undo operation also includes
                    // removing the temporary insert entry in the gap.
                    cce_ptr->insert_intention_set_.erase(insert_it);
                }
                else
                {
                    Iterator iterator = Emplace(insert_it->second->key_);
                    CcEntry<KeyT, ValueT> *new_cce = iterator->second;

                    if (new_cce == nullptr)
                    {
                        // The cc map has reached the maximal capacity.
                        req.Result()->SetError(CcErrorCode::OUT_OF_MEMORY);
                        return true;
                    }

                    shard_->DecrementMemory(new_cce->PayloadMemUsage());
                    new_cce->payload_ = std::make_shared<ValueT>(*payload);
                    new_cce->payload_status_ = RecordStatus::Normal;
                    shard_->mem_usage_ += new_cce->PayloadMemUsage();

                    // Splits the gap.
                    ++insert_it;
                    for (auto it = insert_it;
                         it != cce_ptr->insert_intention_set_.end();
                         ++it)
                    {
                        InsertEntry<KeyT, ValueT> &insert_entry =
                            *it->second.get();
                        insert_entry.parent_entry_ = new_cce;
                        new_cce->insert_intention_set_.emplace(
                            it->first, std::move(it->second));
                    }

                    new_cce->gap_commit_ts_ = commit_ts;
                    new_cce->commit_ts_ = commit_ts;
                    new_cce->gap_last_read_ts_ = cce_ptr->gap_last_read_ts_;

                    cce_ptr->gap_commit_ts_ = commit_ts;
                    cce_ptr->insert_intention_set_.erase(
                        --insert_it, cce_ptr->insert_intention_set_.end());

                    TryInsertCkptList(new_cce);
                }
            }

            if (req.CommitType() != PostWriteType::PrepareCommit)
            {
                // The insert places a write lock on the prior cc entry's gap.
                ReleaseCceGapLock(cce_ptr, txn, req.NodeGroupId());
            }

            if (shard_->core_id_ == shard_->core_cnt_ - 1)
            {
                req.Result()->SetFinished();
                req.SetDecodedPayload(nullptr);
                return true;
            }
            else
            {
                req.ResetCcm();
                MoveRequest(&req, shard_->core_id_ + 1);
                return false;
            }
        }
        else
        {
            LockType lk_type = LockType::NoLock;
            if (cce_ptr->key_lock_ptr_ != nullptr)
            {
                // AcquireAllCc only acquire WriteIntent or WriteLock
                if (cce_ptr->key_lock_ptr_->HasWriteLock() &&
                    cce_ptr->key_lock_ptr_->WriteLockTx() == txn)
                {
                    lk_type = LockType::WriteLock;
                }
                else if (cce_ptr->key_lock_ptr_->HasWriteIntent() &&
                         cce_ptr->key_lock_ptr_->WriteIntentTx() == txn)
                {
                    lk_type = LockType::WriteIntent;
                }
            }

            if (lk_type != LockType::NoLock)
            {
                if (commit_ts > 0)
                {
                    shard_->DecrementMemory(cce_ptr->PayloadMemUsage());
                    cce_ptr->payload_ = std::make_shared<ValueT>(*payload);
                    shard_->mem_usage_ += cce_ptr->PayloadMemUsage();

                    // A prepare commit request only installs the dirty value,
                    // and does not change the record status and commit_ts.
                    if (req.CommitType() == PostWriteType::PostCommit)
                    {
                        cce_ptr->commit_ts_ = commit_ts;
                        cce_ptr->payload_status_ =
                            (req.OpType() == OperationType::Delete ||
                             req.OpType() == OperationType::DropTable)
                                ? RecordStatus::Deleted
                                : RecordStatus::Normal;

                        TryInsertCkptList(cce_ptr);
                    }
                }

                // When commit_ts = 0, the request removes the write lock
                // without installing a new value.

                if (req.CommitType() != PostWriteType::PrepareCommit)
                {
                    // For prepare commit, the request installs the value,
                    // but does not release the write intent/lock.
                    ReleaseCceKeyLock(cce_ptr, txn, req.NodeGroupId());
                }
                else
                {
                    // For prepare commit, the post-write-all request installs
                    // the dirty value, and downgrades the write lock and to the
                    // write intent.

                    if (lk_type == LockType::WriteLock)
                    {
                        DowngradeCceKeyWriteLock(cce_ptr, txn);
                    }
                }
            }

            if (shard_->core_id_ == shard_->core_cnt_ - 1)
            {
                req.Result()->SetFinished();
                req.SetDecodedPayload(nullptr);
                return true;
            }
            else
            {
                req.ResetCcm();
                MoveRequest(&req, shard_->core_id_ + 1);
                return false;
            }
        }
    }

    bool Execute(PostReadCc &req) override
    {
        const CcEntryAddr &cce_addr = *req.CceAddr();
        CcEntry<KeyT, ValueT> &cc_entry =
            *reinterpret_cast<CcEntry<KeyT, ValueT> *>(cce_addr.CcePtr());

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

        ACTION_FAULT_INJECTOR("before_post_read");
        auto hd_res = req.Result();
        CODE_FAULT_INJECTOR(
            "term_TemplateCcMap_Execute_PostReadCc", {
                if (strstr(typeid(*this).name(), "CatalogCcMap") == nullptr &&
                    table_name_.Type() == TableType::Primary)
                {
                    LOG(INFO)
                        << "FaultInject  term_TemplateCcMap_Execute_PostReadCc";
                    hd_res->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
                    return true;
                }
            });

        if (!Sharder::Instance().CheckLeaderTerm(cce_addr.NodeGroupId(),
                                                 cce_addr.Term()))
        {
            LOG(INFO) << "PostReadCc, node_group(#" << cce_addr.NodeGroupId()
                      << ") term < 0, tx:" << req.Txn() << " ,cce: "
                      << reinterpret_cast<void *>(cce_addr.CcePtr());
            hd_res->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            return true;
        }

        uint64_t key_ts = req.KeyTs();
        uint64_t gap_ts = req.GapTs();
        uint64_t commit_ts = req.CommitTs();
        TxNumber txn = req.Txn();

        // FIXME(lzx): Now, we don't backfill for "Unkown" entry when scanning.
        // So, Validate operation fails if another tx backfilled it. Temporary
        // fix is that we don't validate for "Unkown" status results.
        if (cc_entry.payload_status_ != RecordStatus::Unknown &&
            ((key_ts > 0 && key_ts != cc_entry.commit_ts_) ||
             (gap_ts > 0 && gap_ts != cc_entry.gap_commit_ts_)))
        {
            ReleaseCceKeyLock(&cc_entry, txn, req.NodeGroupId());
            ReleaseCceGapLock(&cc_entry, txn, req.NodeGroupId());
            // broken repeatable read, set error.
            hd_res->SetError(
                CcErrorCode::VALIDATION_FAILED_FOR_VERSION_MISMATCH);
            DLOG_IF(INFO, TRACE_OCC_ERR)
                << "PostReadCc, occ_err, txn:" << txn << " ,cce: " << &cc_entry
                << " ,payload_status: "
                << static_cast<int>(cc_entry.payload_status_)
                << " ,key_ts: " << key_ts
                << " ,cc_entry.commit_ts_: " << cc_entry.commit_ts_
                << " ,gap_ts: " << gap_ts
                << " ,cc_entry.gap_commit_ts_: " << cc_entry.gap_commit_ts_;
        }
        else
        {
            // For 2PL, read validation is equivalent to releasing the read
            // lock. In contrast to the conventional 2PL where read locks
            // are released after logging, our protocol releases the read
            // lock before the log is persisted. This difference demands
            // that future write transactions modifying this key cannot commit
            // prior to this read tx. This is achieved via updating the
            // last_read_ts field of the cc entry, which pushes future
            // transactions' commit timestamps larger than the largest commit
            // timestamp of all read transactions that have released the read
            // lock on the key.

            PostProcessResult &conflicting_txs = hd_res->Value();

            if (gap_ts > 0)
            {
                cc_entry.gap_last_read_ts_ =
                    std::max(cc_entry.gap_last_read_ts_, commit_ts);

                for (auto it = cc_entry.insert_intention_set_.begin();
                     it != cc_entry.insert_intention_set_.end();
                     ++it)
                {
                    conflicting_txs.AddConflictingTx(it->second->txn_);

                    DLOG_IF(INFO, TRACE_OCC_ERR)
                        << "PostReadCc, occ_err, txn:" << txn
                        << " ,cce: " << &cc_entry
                        << " ,gap conflict tx: " << it->second->txn_;
                }
            }

            if (key_ts > 0)
            {
                cc_entry.last_read_ts_ =
                    std::max(cc_entry.last_read_ts_, commit_ts);

                // Using locking protocol, this never happens.
                if (cc_entry.key_lock_ptr_ != nullptr &&
                    cc_entry.key_lock_ptr_->HasWriteLock() &&
                    cc_entry.key_lock_ptr_->WriteLockTx() != txn)
                {
                    int64_t ng_term =
                        Sharder::Instance().LeaderTerm(req.NodeGroupId());
                    shard_->CheckRecoverTx(
                        cc_entry.key_lock_ptr_->WriteLockTx(),
                        req.NodeGroupId(),
                        ng_term);
                    conflicting_txs.AddConflictingTx(
                        cc_entry.key_lock_ptr_->WriteLockTx());

                    DLOG_IF(INFO, TRACE_OCC_ERR)
                        << "PostReadCc, occ_err, txn:" << txn
                        << " ,cce: " << &cc_entry << " ,key conflict tx: "
                        << cc_entry.key_lock_ptr_->WriteLockTx();
                }
            }

            ReleaseCceKeyLock(&cc_entry, txn, req.NodeGroupId());
            ReleaseCceGapLock(&cc_entry, txn, req.NodeGroupId());
            if (conflicting_txs.Size() > 0)
            {
                // Does not perform tx negotiations so far.
                hd_res->SetError(
                    CcErrorCode::VALIDATION_FAILED_FOR_CONFILICTED_TXS);
            }
            else
            {
                hd_res->SetFinished();
            }
        }

        return true;
    }

    bool Execute(ReadCc &req) override
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

        auto hd_res = req.Result();
        CODE_FAULT_INJECTOR("term_TemplateCcMap_Execute_ReadCc", {
            if (strstr(typeid(*this).name(), "CatalogCcMap") == nullptr)
            {
                LOG(INFO) << "FaultInject  term_TemplateCcMap_Execute_ReadCc";
                hd_res->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
                return true;
            }
        });

        // To avoid lock the record and simulate message missed.
        CODE_FAULT_INJECTOR("remote_read_msg_missed", {
            if (req.CcePtr() != nullptr)
            {
                LOG(INFO) << "FaultInject  remote_read_msg_missed"
                          << "txID: " << req.Txn();
                hd_res->SetFinished();

                return true;
            }
        });

        // To avoid lock the record and simulate term changed.
        CODE_FAULT_INJECTOR("block_req_term_changed", {
            if (req.CcePtr() != nullptr)
            {
                LOG(INFO) << "FaultInject  block_req_term_changed";
                hd_res->SetFinished();
                return true;
            }
        });

        uint32_t ng_id = req.NodeGroupId();
        int64_t ng_term = -1;
        if (req.IsInRecovering())
        {
            ng_term = Sharder::Instance().CandidateLeaderTerm(ng_id);
        }
        else
        {
            ng_term = Sharder::Instance().LeaderTerm(ng_id);
        }

        if (ng_term < 0)
        {
            LOG(INFO) << "ReadCc, node_group(#" << ng_id
                      << ") term < 0, tx:" << req.Txn();
            hd_res->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            return true;
        }

        int64_t tx_term = req.TxTerm();
        IsolationLevel iso_lvl = req.Isolation();
        CcProtocol cc_proto = req.Protocol();
        bool is_read_snapshot;
        CcOperation cc_op;
        if (table_name_.Type() == TableType::Secondary)
        {
            cc_op = CcOperation::ReadSkIndex;
            is_read_snapshot = (iso_lvl == IsolationLevel::Snapshot);
        }
        else
        {
            cc_op = req.IsForWrite() ? CcOperation::ReadForWrite
                                     : CcOperation::Read;
            is_read_snapshot =
                (iso_lvl == IsolationLevel::Snapshot && !req.IsForWrite());
        }

        CcEntryAddr &cce_addr = hd_res->Value().cce_addr_;
        CcEntry<KeyT, ValueT> *cce = nullptr;

        if (req.Type() == ReadType::Inside)
        {
            LockType acquired_lock = LockType::NoLock;
            CcErrorCode err_code = CcErrorCode::NO_ERROR;

            if (req.CcePtr() != nullptr)
            {
                // The request was blocked before. This is execution resumption
                // after the request is unblocked. The read lock/intention must
                // have been acquired.
                cce = static_cast<CcEntry<KeyT, ValueT> *>(req.CcePtr());

                if (req.IsWaitForPostWrite())
                {
                    req.SetIsWaitForPostWrite(false);
                    // Since when we are waiting for PostWrite, the ReadCc
                    // request is put into the blocking queue with ReadLock.
                    // After PostWrite finished, this ReadLock should be
                    // released.
                    cce->key_lock_ptr_->ReleaseLock(
                        req.Txn(), shard_, LockType::ReadLock);
                    acquired_lock = LockType::NoLock;
                    err_code = CcErrorCode::NO_ERROR;
                }
                else
                {
                    std::tie(acquired_lock, err_code) =
                        LockHandleForResumedRequest(cce,
                                                    cce->payload_status_,
                                                    &req,
                                                    ng_id,
                                                    ng_term,
                                                    req.TxTerm(),
                                                    cc_op,
                                                    iso_lvl,
                                                    cc_proto,
                                                    req.ReadTimestamp());
                }
            }
            else
            {
                const KeyT *look_key = nullptr;
                KeyT decoded_key;

                if (req.Key() != nullptr)
                {
                    look_key = static_cast<const KeyT *>(req.Key());
                }
                else
                {
                    assert(req.KeyBlob() != nullptr);
                    size_t offset = 0;
                    decoded_key.Deserialize(
                        req.KeyBlob()->data(), offset, KeySchema());
                    look_key = &decoded_key;
                }

#ifdef RANGE_PARTITION_ENABLED
                cce = Find(*look_key).second;
                if (cce == nullptr)
                {
                    if (Type() == TableType::Primary)
                    {
                        RangeSliceOpStatus pin_status;
                        uint32_t range_id = req.KeyShardCode() >> 10;
                        RangeSliceId slice_id = shard_->PinRangeSlice(
                            table_name_,
                            cc_ng_id_,
                            KeySchema(),
                            RecordSchema(),
                            schema_ts_,
                            table_schema_->GetKVCatalogInfo(),
                            range_id,
                            *look_key,
                            true,
                            &req,
                            pin_status);

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

                            if (cc_op == CcOperation::ReadForWrite)
                            {
                                Iterator it = FindEmplace(*look_key);
                                cce = it->second;
                                if (cce == nullptr)
                                {
                                    hd_res->SetError(
                                        CcErrorCode::OUT_OF_MEMORY);
                                    return true;
                                }

                                if (cce->payload_status_ ==
                                    RecordStatus::Unknown)
                                {
                                    cce->payload_status_ =
                                        RecordStatus::Deleted;
                                    cce->commit_ts_ = 1U;
                                    cce->gap_commit_ts_ = 1U;
                                    cce->ckpt_ts_.store(
                                        1U, std::memory_order_relaxed);
                                }
                                else
                                {
                                    assert(cce->commit_ts_ > 1);
                                }
                            }
                            else
                            {
                                cce = Find(*look_key).second;
                                if (cce == nullptr)
                                {
                                    hd_res->Value().ts_ = 1;
                                    hd_res->Value().rec_status_ =
                                        RecordStatus::Deleted;
                                    hd_res->SetFinished();

                                    return true;
                                }
                            }
                        }
                        else if (pin_status == RangeSliceOpStatus::Blocked)
                        {
                            return false;
                        }
                        else
                        {
                            // If the pin operation returns an error, the data
                            // store is inaccessible.
                            hd_res->SetError(
                                CcErrorCode::PIN_RANGE_SLICE_FAILED);
                            return true;
                        }
                    }
                    else
                    {
                        Iterator it = FindEmplace(*look_key);
                        cce = it->second;
                        if (cce == nullptr)
                        {
                            hd_res->SetError(CcErrorCode::OUT_OF_MEMORY);
                            return true;
                        }
                    }
                }
#else
                Iterator it = FindEmplace(*look_key);
                cce = it->second;

                // The read request accesses a new key not in the cc map. But
                // the cc map is full and cannot allocates a new entry.
                if (cce == nullptr)
                {
                    hd_res->SetError(CcErrorCode::OUT_OF_MEMORY);
                    return true;
                }

                // if ccm contains all the ccentries, then unknown status means
                // that we can skip accessing kv store and return deleted status
                // directly.
                if (ccm_has_full_entries_ &&
                    cce->payload_status_ == RecordStatus::Unknown)
                {
                    cce->payload_status_ = RecordStatus::Deleted;
                    cce->commit_ts_ = 1U;
                    cce->gap_commit_ts_ = 1U;
                    cce->ckpt_ts_.store(1U);
                }
#endif

                req.SetCcePtr(cce);
                cce_addr.SetCce(reinterpret_cast<uint64_t>(cce),
                                ng_term,
                                req.NodeGroupId(),
                                shard_->LocalCoreId());

                // Try to acquire lock
                std::tie(acquired_lock, err_code) =
                    AcquireCceKeyLock(cce,
                                      cce->payload_status_,
                                      &req,
                                      ng_id,
                                      ng_term,
                                      tx_term,
                                      cc_op,
                                      iso_lvl,
                                      cc_proto,
                                      req.ReadTimestamp());
            }

            // After acquiring lock
            switch (err_code)
            {
            case CcErrorCode::MVCC_READ_MUST_WAIT_WRITE:
            {
                req.SetIsWaitForPostWrite(true);
                // Put the request to top of key lock's blocking queue with
                // acquring readlock. And then should release the readlock
                // before handling this requst when PostWriteCc finished.
                cce->key_lock_ptr_->InsertBlockingQueue(&req,
                                                        LockType::ReadLock);
                shard_->CheckRecoverTx(
                    cce->key_lock_ptr_->WriteLockTx(), ng_id, ng_term);
                // After inserting to blocking queue, the execution of current
                // ReadCc request should stop.
                return false;
            }
            case CcErrorCode::NO_ERROR:
            {
                hd_res->Value().lock_type_ = acquired_lock;
                break;
            }
            case CcErrorCode::ACQUIRE_LOCK_BLOCKED:
            {
                // If the read request comes from a remote node, sends
                // acknowledgement to the sender when the request is
                // blocked.
                if (!req.IsLocal())
                {
                    remote::RemoteRead &remote_req =
                        static_cast<remote::RemoteRead &>(req);
                    remote_req.Acknowledge();
                }
                // ReadLock fail should stop the execution of current
                // ReadCc request since it's already in blocking queue.
                return false;
            }
            default:
            {
                // lock confilct: back off and retry.
                req.Result()->SetError(err_code);
                return true;
            }
            }  //-- end: switch
        }      //-- end: read insde
        else
        {
            // For the read-outside request whose goal is to bring in a
            // record from the data store for caching, the cc entry's
            // address is known.
            assert(req.NodeGroupId() == cce_addr.NodeGroupId());
            assert(cce_addr.CcePtr() != 0);
            cce = reinterpret_cast<CcEntry<KeyT, ValueT> *>(cce_addr.CcePtr());
        }  //-- end: read outside

        // The request brings in the record to the cc entry for caching if
        // cce->payload_status_ is Unknown which means it doesn't override by
        // another transaction yet.
        if (req.Type() != ReadType::Inside)
        {
            RecordStatus tmp_payload_status = RecordStatus::Normal;
            std::unique_ptr<ValueT> tmp_payload = std::make_unique<ValueT>();
            if (req.Type() == ReadType::OutsideNormal)
            {
                tmp_payload_status = RecordStatus::Normal;
                if (req.Record() != nullptr)
                {
                    ValueT *typed_rec = static_cast<ValueT *>(req.Record());
                    tmp_payload = std::make_unique<ValueT>(*typed_rec);
                }
                else
                {
                    assert(req.RecordBlob() != nullptr);
                    size_t offset = 0;
                    tmp_payload = std::make_unique<ValueT>();
                    tmp_payload->Deserialize(req.RecordBlob()->data(), offset);
                }
            }
            else
            {
                // set tomb ccentry to prevent access data store again.
                tmp_payload_status = RecordStatus::Deleted;
            }

            if (cce->payload_status_ == RecordStatus::Unknown)
            {
                shard_->DecrementMemory(cce->PayloadMemUsage());
                cce->payload_ = std::move(tmp_payload);
                cce->payload_status_ = tmp_payload_status;
                shard_->mem_usage_ += cce->PayloadMemUsage();
                cce->commit_ts_ = req.ReadTimestamp();
                // set "ckpt_ts_" to identify the entry is refilled
                uint64_t tmp_ts = 0U;
                cce->ckpt_ts_.compare_exchange_strong(tmp_ts,
                                                      req.ReadTimestamp());
            }
            else if (shard_->EnableMvcc() && cce->ckpt_ts_ == 0U &&
                     cce->commit_ts_ > req.ReadTimestamp())
            {
                // Trying to insert the record to backfill into archives is
                // needed, because the entry may be created when executing
                // "ReplayLogCc".
                shard_->mem_usage_ +=
                    cce->AddArchiveRecord(std::move(tmp_payload),
                                          tmp_payload_status,
                                          req.ReadTimestamp());
                // set "ckpt_ts_" to identify the entry is refilled
                uint64_t tmp_ts = 0U;
                cce->ckpt_ts_.compare_exchange_strong(tmp_ts,
                                                      req.ReadTimestamp());
            }

            // Refill mvcc archives
            if (shard_->EnableMvcc() &&
                (req.Type() == ReadType::OutsideNormal ||
                 req.Type() == ReadType::OutsideDeleted) &&
                req.ArchivesPtr() != nullptr && req.ArchivesPtr()->size() > 0)
            {
                shard_->mem_usage_ +=
                    cce->AddArchiveRecords(*req.ArchivesPtr());
            }
        }

        if (is_read_snapshot)
        {
            assert(req.Type() == ReadType::Inside);

            VersionResultRecord<ValueT> v_rec;
            cce->MvccGet(req.ReadTimestamp(), Type(), v_rec);
            if (v_rec.payload_status_ == RecordStatus::Normal)
            {
                if (req.Record() != nullptr)
                {
                    ValueT *typed_rec = static_cast<ValueT *>(req.Record());
                    *typed_rec = *(v_rec.payload_ptr_);
                }
                else
                {
                    assert(req.RecordBlob() != nullptr);
                    v_rec.payload_ptr_->Serialize(*req.RecordBlob());
                }
            }
            hd_res->Value().ts_ = v_rec.commit_ts_;
            hd_res->Value().rec_status_ = v_rec.payload_status_;
            hd_res->SetFinished();
            return true;
        }
        else if (cce->payload_status_ == RecordStatus::Normal &&
                 (req.Type() == ReadType::Inside || cce->commit_ts_ > 1))
        {
            // Copies the newest committed payload to the read result, if (1)
            // this is a read request that starts concurrency control for
            // the input key (i.e., read inside), or (2) this is a read request
            // that brings in the record from the data store for caching, but
            // the key has been updated by another committed tx since the first
            // read request.
            // TODO: TxExecution and runtime also use this new value as read
            // result to avoid future PostRead abort.

            if (req.Isolation() == IsolationLevel::ReadCommitted &&
                cce->commit_ts_ > 0 && cce->commit_ts_ < req.ReadTimestamp())
            {
                // When backtracking the content of primary key record according
                // to the secondary index key, if the commit_ts of this
                // primary key record is smaller than the commit_ts of the
                // secondary index key("req.ReadTimestamp()"), it means that the
                // current primary key has not been updated and there must be a
                // PostWriteCc request waiting to be executed. So, this read
                // should wait for the PostWriteCc completed.
                req.SetIsWaitForPostWrite(true);
                assert(cce->key_lock_ptr_ != nullptr &&
                       cce->key_lock_ptr_->HasWriteLock() &&
                       cce->key_lock_ptr_->WriteLockTx() != req.Txn());
                // Put the request to top of key lock's blocking queue with
                // acquring readlock. And then should release the readlock
                // before handling this requst when PostWriteCc finished.
                cce->key_lock_ptr_->InsertBlockingQueue(&req,
                                                        LockType::ReadLock);
                shard_->CheckRecoverTx(
                    cce->key_lock_ptr_->WriteLockTx(), ng_id, ng_term);

                // After inserting to blocking queue, the execution of current
                // ReadCc request should stop.
                return false;
            }
            else
            {
                // safe to read the record
                if (req.Record() != nullptr)
                {
                    ValueT *typed_rec = static_cast<ValueT *>(req.Record());
                    *typed_rec = *(cce->payload_);
                }
                else
                {
                    assert(req.RecordBlob() != nullptr);
                    cce->payload_->Serialize(*req.RecordBlob());
                }
            }
        }

        hd_res->Value().ts_ = cce->commit_ts_;
        hd_res->Value().rec_status_ = cce->payload_status_;

        hd_res->SetFinished();

        return true;
    }  // namespace txservice

    bool Execute(remote::RemoteReadOutside &req) override
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

        if (table_name_.Type() == TableType::Secondary)
        {
            assert(false);
            return true;
        }

        const CcEntryAddr &cce_addr = req.cce_addr_;
        if (!Sharder::Instance().CheckLeaderTerm(cce_addr.NodeGroupId(),
                                                 cce_addr.Term()))
        {
            LOG(INFO) << "RemoteReadOutside, node_group(#"
                      << cce_addr.NodeGroupId()
                      << ") term < 0, tx:" << req.Txn() << " ,cce: "
                      << reinterpret_cast<void *>(cce_addr.CcePtr());
            req.Finish();
            return true;
        }

        CcEntry<KeyT, ValueT> *cce =
            reinterpret_cast<CcEntry<KeyT, ValueT> *>(cce_addr.CcePtr());

        if (cce->payload_status_ == RecordStatus::Unknown)
        {
            assert(cce->commit_ts_ == 1);
            if (req.RecordStatus() == RecordStatus::Normal)
            {
                size_t offset = 0;
                shard_->DecrementMemory(cce->PayloadMemUsage());
                cce->payload_ = std::make_shared<ValueT>();
                cce->payload_->Deserialize(req.rec_str_->data(), offset);
                shard_->mem_usage_ += cce->PayloadMemUsage();
            }
            cce->commit_ts_ = req.CommitTs();
            cce->payload_status_ = req.RecordStatus();

            // set "ckpt_ts_" to identify the entry is refilled
            uint64_t tmp_ts = 0U;
            cce->ckpt_ts_.compare_exchange_strong(tmp_ts, req.CommitTs());
        }
        else if (shard_->EnableMvcc() && cce->ckpt_ts_ == 0U &&
                 cce->commit_ts_ > req.CommitTs())
        {
            // Trying to insert the record to backfill into archives is needed,
            // because the entry may be created when executing "ReplayLogCc".
            std::unique_ptr<ValueT> tmp_payload = std::make_unique<ValueT>();
            if (req.RecordStatus() == RecordStatus::Normal)
            {
                size_t offset = 0;
                tmp_payload->Deserialize(req.rec_str_->data(), offset);
            }
            shard_->mem_usage_ += cce->AddArchiveRecord(
                std::move(tmp_payload), req.RecordStatus(), req.CommitTs());

            // set "ckpt_ts_" to identify the entry is refilled
            uint64_t tmp_ts = 0U;
            cce->ckpt_ts_.compare_exchange_strong(tmp_ts, req.CommitTs());
        }
        // Refill mvcc archives.
        if (shard_->EnableMvcc())
        {
            const remote::ReadOutsideRequest &tmp_req =
                req.input_msg_->read_outside_req();
            if (tmp_req.archives_size() > 0)
            {
                // de-serialize records
                std::vector<VersionTxRecord> archives;
                for (auto &vrec_msg : tmp_req.archives())
                {
                    auto &v_rec = archives.emplace_back();
                    v_rec.commit_ts_ = vrec_msg.version_ts();
                    v_rec.record_status_ =
                        remote::ToLocalType::ConvertRecordStatusType(
                            vrec_msg.rec_status());
                    v_rec.record_ = std::make_unique<ValueT>();
                    size_t offset = 0;
                    v_rec.record_->Deserialize(vrec_msg.record().data(),
                                               offset);
                }
                shard_->mem_usage_ += cce->AddArchiveRecords(archives);
            }
        }

        req.Finish();
        return true;
    }

    bool Execute(ScanCloseCc &req) override
    {
        return true;
    }

    void AddScanTuple(const KeyT *key,
                      CcEntry<KeyT, ValueT> *cce,
                      TemplateScanCache<KeyT, ValueT> *typed_cache,
                      ScanType scan_type,
                      uint32_t ng_id,
                      int64_t ng_term,
                      uint64_t read_ts,
                      bool is_read_snapshot,
                      bool keep_deleted = true,
                      bool is_ckpt_delta = false)
    {
        assert(scan_type != ScanType::ScanUnknow);

        switch (scan_type)
        {
        case ScanType::ScanGap:
        {
            TemplateScanTuple<KeyT, ValueT> *scan_tuple =
                typed_cache->AddScanTuple();
            ScanGap(key, cce, scan_tuple, ng_id, ng_term);
            break;
        }
        case ScanType::ScanBoth:
            ScanKey(
                key,
                cce,
                typed_cache,
                true,
                ng_id,
                ng_term,
                read_ts,
                is_read_snapshot,
                keep_deleted,
                (table_name_.Type() != TableType::Secondary) && is_ckpt_delta);
            break;
        case ScanType::ScanKey:
            ScanKey(
                key,
                cce,
                typed_cache,
                false,
                ng_id,
                ng_term,
                read_ts,
                is_read_snapshot,
                (table_name_.Type() != TableType::Secondary) && is_ckpt_delta);
            break;
        default:
            break;
        }
    }

    bool Execute(ScanOpenBatchCc &req) override
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

        // Before the scan open request is enqueued, the local node's term
        // is obtained and kept in the cc request. This is to avoid getting
        // the node's terms repeatedly in each core, as the scan request is
        // dispatched to all cores.

        uint32_t ng_id = req.NodeGroupId();
        int64_t ng_term = Sharder::Instance().LeaderTerm(ng_id);
        int64_t tx_term = req.TxTerm();
        // fault inject
        CODE_FAULT_INJECTOR("term_TemplateCcMap_Execute_ScanOpenBatchCc", {
            LOG(INFO) << "FaultInject  "
                         "term_TemplateCcMap_Execute_ScanOpenBatchCc";
            ng_term = -1;
            FaultInject::Instance().InjectFault(
                "term_TemplateCcMap_Execute_ScanOpenBatchCc", "remove");
        });
        if (ng_term < 0)
        {
            req.Result()->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            return true;
        }

        const KeyT *look_key = static_cast<const KeyT *>(req.start_key_);
        TemplateScanCache<KeyT, ValueT> *typed_cache =
            static_cast<TemplateScanCache<KeyT, ValueT> *>(req.scan_cache_);

        Iterator scan_ccm_it;
        IsolationLevel iso_lvl = req.Isolation();
        CcProtocol cc_proto = req.Protocol();
        CcOperation cc_op;
        bool is_read_snapshot;
        if (table_name_.Type() == TableType::Secondary)
        {
            cc_op = CcOperation::ReadSkIndex;
            is_read_snapshot = (iso_lvl == IsolationLevel::Snapshot);
        }
        else
        {
            cc_op = req.IsForWrite() ? CcOperation::ReadForWrite
                                     : CcOperation::Read;
            is_read_snapshot =
                (iso_lvl == IsolationLevel::Snapshot && !req.IsForWrite());
        }

        const KeyT *key_ptr = nullptr;
        CcEntry<KeyT, ValueT> *cce = nullptr;

        if (req.CcePtr() != nullptr)
        {
            cce = static_cast<CcEntry<KeyT, ValueT> *>(req.CcePtr());
            scan_ccm_it = Iterator(cce, &neg_inf_, &pos_inf_);
            key_ptr = scan_ccm_it->first;
            ScanType scan_type = req.CcePtrScanType();

            req.SetCcePtr(nullptr);
            req.SetCcePtrScanType(ScanType::ScanUnknow);

            if (req.IsWaitForPostWrite())
            {
                req.SetIsWaitForPostWrite(false);
                cce->key_lock_ptr_->ReleaseLock(
                    req.Txn(), shard_, LockType::ReadLock);
            }
            else
            {
                // Lock has been acquired, UpsertLockHoldingTx
                auto lock_pair =
                    LockHandleForResumedRequest(cce,
                                                cce->payload_status_,
                                                &req,
                                                ng_id,
                                                ng_term,
                                                tx_term,
                                                cc_op,
                                                iso_lvl,
                                                cc_proto,
                                                req.ReadTimestamp());
                if (lock_pair.second != CcErrorCode::NO_ERROR)
                {
                    assert(lock_pair.second ==
                           CcErrorCode::MVCC_READ_FOR_WRITE_CONFLICT);
                    req.Result()->SetError(lock_pair.second);
                    return true;
                }
            }

            AddScanTuple(key_ptr,
                         cce,
                         typed_cache,
                         scan_type,
                         ng_id,
                         ng_term,
                         req.ReadTimestamp(),
                         is_read_snapshot);
        }
        else
        {
            std::pair<Iterator, ScanType> start_pair =
                req.direct_ == ScanDirection::Forward
                    ? ForwardScanStart(
                          *look_key,
                          req.inclusive_,
                          (table_name_.Type() != TableType::Secondary) &&
                              req.is_include_floor_cce_)
                    : BackwardScanStart(*look_key, req.inclusive_);

            scan_ccm_it = start_pair.first;
            key_ptr = scan_ccm_it->first;
            cce = scan_ccm_it->second;
            ScanType scan_type = start_pair.second;

            req.SetCcePtr(cce);
            req.SetCcePtrScanType(scan_type);

            if (scan_type != ScanType::ScanGap)
            {
                auto lock_pair = AcquireCceKeyLock(cce,
                                                   cce->payload_status_,
                                                   &req,
                                                   ng_id,
                                                   ng_term,
                                                   tx_term,
                                                   cc_op,
                                                   iso_lvl,
                                                   cc_proto,
                                                   req.ReadTimestamp());
                switch (lock_pair.second)
                {
                case CcErrorCode::NO_ERROR:
                    break;
                case CcErrorCode::MVCC_READ_MUST_WAIT_WRITE:
                {
                    req.SetIsWaitForPostWrite(true);
                    // Put the request to top of key lock's blocking queue with
                    // acquring readlock. And then should release the readlock
                    // before handling this requst when PostWriteCc finished.
                    cce->key_lock_ptr_->InsertBlockingQueue(&req,
                                                            LockType::ReadLock);
                    shard_->CheckRecoverTx(
                        cce->key_lock_ptr_->WriteLockTx(), ng_id, ng_term);
                    // After inserting to blocking queue, the execution of
                    // current ReadCc request should stop.
                    return false;
                }
                case CcErrorCode::ACQUIRE_LOCK_BLOCKED:
                {
                    // Lock fail should stop the execution of current
                    // CC request since it's already in blocking queue.
                    return false;
                }
                default:
                {
                    // lock confilct: back off and retry.
                    req.Result()->SetError(lock_pair.second);
                    return true;
                }
                }  //-- end: switch
            }
            else
            {
                // TODO(lzx): handle gap lock
            }

            AddScanTuple(key_ptr,
                         cce,
                         typed_cache,
                         scan_type,
                         ng_id,
                         ng_term,
                         req.ReadTimestamp(),
                         is_read_snapshot);
        }

        if (req.direct_ == ScanDirection::Forward)
        {
            ++scan_ccm_it;

            Iterator pos_inf_it = End();
            for (; scan_ccm_it != pos_inf_it && !typed_cache->Full();
                 ++scan_ccm_it)
            {
                key_ptr = scan_ccm_it->first;
                cce = scan_ccm_it->second;
                req.SetCcePtr(cce);
                req.SetCcePtrScanType(ScanType::ScanBoth);

                auto lock_pair = AcquireCceKeyLock(cce,
                                                   cce->payload_status_,
                                                   &req,
                                                   ng_id,
                                                   ng_term,
                                                   tx_term,
                                                   cc_op,
                                                   iso_lvl,
                                                   cc_proto,
                                                   req.ReadTimestamp());
                switch (lock_pair.second)
                {
                case CcErrorCode::NO_ERROR:
                    break;
                case CcErrorCode::MVCC_READ_MUST_WAIT_WRITE:
                {
                    req.SetIsWaitForPostWrite(true);
                    // Put the request to top of key lock's blocking queue with
                    // acquring readlock. And then should release the readlock
                    // before handling this requst when PostWriteCc finished.
                    cce->key_lock_ptr_->InsertBlockingQueue(&req,
                                                            LockType::ReadLock);
                    shard_->CheckRecoverTx(
                        cce->key_lock_ptr_->WriteLockTx(), ng_id, ng_term);
                    // After inserting to blocking queue, the execution of
                    // current ReadCc request should stop.
                    return false;
                }
                case CcErrorCode::ACQUIRE_LOCK_BLOCKED:
                {
                    // Lock fail should stop the execution of current
                    // CC request since it's already in blocking queue.
                    return false;
                }
                default:
                {
                    // lock confilct: back off and retry.
                    req.Result()->SetError(lock_pair.second);
                    return true;
                }
                }  //-- end: switch

                AddScanTuple(key_ptr,
                             cce,
                             typed_cache,
                             ScanType::ScanBoth,
                             ng_id,
                             ng_term,
                             req.ReadTimestamp(),
                             is_read_snapshot,
                             true,
                             req.is_ckpt_delta_);
            }
        }
        else
        {
            --scan_ccm_it;

            Iterator neg_inf_it = Begin();
            for (; scan_ccm_it != neg_inf_it && !typed_cache->Full();
                 --scan_ccm_it)
            {
                key_ptr = scan_ccm_it->first;
                cce = scan_ccm_it->second;
                req.SetCcePtr(cce);
                req.SetCcePtrScanType(ScanType::ScanBoth);

                auto lock_pair = AcquireCceKeyLock(cce,
                                                   cce->payload_status_,
                                                   &req,
                                                   ng_id,
                                                   ng_term,
                                                   tx_term,
                                                   cc_op,
                                                   iso_lvl,
                                                   cc_proto,
                                                   req.ReadTimestamp());
                switch (lock_pair.second)
                {
                case CcErrorCode::NO_ERROR:
                    break;
                case CcErrorCode::MVCC_READ_MUST_WAIT_WRITE:
                {
                    req.SetIsWaitForPostWrite(true);
                    // Put the request to top of key lock's blocking queue with
                    // acquring readlock. And then should release the readlock
                    // before handling this requst when PostWriteCc finished.
                    cce->key_lock_ptr_->InsertBlockingQueue(&req,
                                                            LockType::ReadLock);
                    shard_->CheckRecoverTx(
                        cce->key_lock_ptr_->WriteLockTx(), ng_id, ng_term);
                    // After inserting to blocking queue, the execution of
                    // current ReadCc request should stop.
                    return false;
                }
                case CcErrorCode::ACQUIRE_LOCK_BLOCKED:
                {
                    // Lock fail should stop the execution of current
                    // CC request since it's already in blocking queue.
                    return false;
                }
                default:
                {
                    // lock confilct: back off and retry.
                    req.Result()->SetError(lock_pair.second);
                    return true;
                }
                }  //-- end: switch

                AddScanTuple(key_ptr,
                             cce,
                             typed_cache,
                             ScanType::ScanBoth,
                             ng_id,
                             ng_term,
                             req.ReadTimestamp(),
                             is_read_snapshot,
                             true,
                             req.is_ckpt_delta_);
            }
        }

        req.Result()->SetFinished();
        return true;
    }

    bool Execute(ScanNextBatchCc &req) override
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

        uint32_t ng_id = req.NodeGroupId();
        int64_t ng_term = Sharder::Instance().LeaderTerm(ng_id);
        int64_t tx_term = req.TxTerm();
        if (ng_term < 0)
        {
            req.Result()->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            return false;
        }
        req.Result()->Value().term_ = ng_term;

        IsolationLevel iso_lvl = req.Isolation();
        CcProtocol cc_proto = req.Protocol();
        CcOperation cc_op;
        bool is_read_snapshot;
        if (table_name_.Type() == TableType::Secondary)
        {
            cc_op = CcOperation::ReadSkIndex;
            is_read_snapshot = (iso_lvl == IsolationLevel::Snapshot);
        }
        else
        {
            cc_op = req.IsForWrite() ? CcOperation::ReadForWrite
                                     : CcOperation::Read;
            is_read_snapshot =
                (iso_lvl == IsolationLevel::Snapshot && !req.IsForWrite());
        }

        TemplateScanCache<KeyT, ValueT> *typed_cache =
            static_cast<TemplateScanCache<KeyT, ValueT> *>(req.scan_cache_);
        assert(typed_cache->Full());

        ScanDirection direction = typed_cache->Scanner()->Direction();
        Iterator scan_ccm_it;
        if (req.CcePtr() != nullptr)
        {
            CcEntry<KeyT, ValueT> *prior_cce =
                static_cast<CcEntry<KeyT, ValueT> *>(req.CcePtr());
            scan_ccm_it = Iterator(prior_cce, &neg_inf_, &pos_inf_);
            const KeyT *prior_cce_key = scan_ccm_it->first;
            ScanType scan_type = req.CcePtrScanType();

            req.SetCcePtr(nullptr);
            req.SetCcePtrScanType(ScanType::ScanUnknow);

            if (req.IsWaitForPostWrite())
            {
                req.SetIsWaitForPostWrite(false);
                prior_cce->key_lock_ptr_->ReleaseLock(
                    req.Txn(), shard_, LockType::ReadLock);
            }
            else
            {
                // Lock has been acquired, UpsertLockHoldingTx
                auto lock_pair =
                    LockHandleForResumedRequest(prior_cce,
                                                prior_cce->payload_status_,
                                                &req,
                                                ng_id,
                                                ng_term,
                                                tx_term,
                                                cc_op,
                                                iso_lvl,
                                                cc_proto,
                                                req.ReadTimestamp());
                if (lock_pair.second != CcErrorCode::NO_ERROR)
                {
                    assert(lock_pair.second ==
                           CcErrorCode::MVCC_READ_FOR_WRITE_CONFLICT);
                    req.Result()->SetError(lock_pair.second);
                    return true;
                }
            }

            AddScanTuple(prior_cce_key,
                         prior_cce,
                         typed_cache,
                         scan_type,
                         ng_id,
                         ng_term,
                         req.ReadTimestamp(),
                         is_read_snapshot,
                         true,
                         req.is_ckpt_delta_);
        }
        else
        {
            CcEntry<KeyT, ValueT> *prior_cce =
                reinterpret_cast<CcEntry<KeyT, ValueT> *>(
                    typed_cache->Last()->cce_addr_.CcePtr());
            scan_ccm_it = Iterator(prior_cce, &neg_inf_, &pos_inf_);
            typed_cache->Reset();
        }

        if (direction == ScanDirection::Forward)
        {
            ++scan_ccm_it;

            Iterator pos_inf_it = End();
            for (; scan_ccm_it != pos_inf_it && !typed_cache->Full();
                 ++scan_ccm_it)
            {
                const KeyT *key = scan_ccm_it->first;
                CcEntry<KeyT, ValueT> *cce = scan_ccm_it->second;
                if (req.is_ckpt_delta_ &&
                    cce->commit_ts_ <=
                        cce->ckpt_ts_.load(std::memory_order_acquire))
                {
                    // If this is a scan for modified records since last
                    // checkpoint, skips those that have been checkpointed.
                    continue;
                }

                req.SetCcePtr(cce);
                req.SetCcePtrScanType(ScanType::ScanBoth);

                auto lock_pair = AcquireCceKeyLock(cce,
                                                   cce->payload_status_,
                                                   &req,
                                                   ng_id,
                                                   ng_term,
                                                   tx_term,
                                                   cc_op,
                                                   iso_lvl,
                                                   cc_proto,
                                                   req.ReadTimestamp());
                switch (lock_pair.second)
                {
                case CcErrorCode::NO_ERROR:
                    break;
                case CcErrorCode::MVCC_READ_MUST_WAIT_WRITE:
                {
                    req.SetIsWaitForPostWrite(true);
                    // Put the request to top of key lock's blocking queue with
                    // acquring readlock. And then should release the readlock
                    // before handling this requst when PostWriteCc finished.
                    cce->key_lock_ptr_->InsertBlockingQueue(&req,
                                                            LockType::ReadLock);
                    shard_->CheckRecoverTx(
                        cce->key_lock_ptr_->WriteLockTx(), ng_id, ng_term);
                    // After inserting to blocking queue, the execution of
                    // current ReadCc request should stop.
                    return false;
                }
                case CcErrorCode::ACQUIRE_LOCK_BLOCKED:
                {
                    // Lock fail should stop the execution of current
                    // CC request since it's already in blocking queue.
                    return false;
                }
                default:
                {
                    // lock confilct: back off and retry.
                    req.Result()->SetError(lock_pair.second);
                    return true;
                }
                }  //-- end: switch

                AddScanTuple(key,
                             cce,
                             typed_cache,
                             ScanType::ScanBoth,
                             ng_id,
                             ng_term,
                             req.ReadTimestamp(),
                             is_read_snapshot,
                             true,
                             req.is_ckpt_delta_);
            }
        }
        else
        {
            --scan_ccm_it;
            Iterator neg_inf_it = Begin();
            for (; !typed_cache->Full(); --scan_ccm_it)
            {
                const KeyT *key = scan_ccm_it->first;
                CcEntry<KeyT, ValueT> *cce = scan_ccm_it->second;
                if (scan_ccm_it == neg_inf_it)
                {
                    req.SetCcePtr(cce);
                    req.SetCcePtrScanType(ScanType::ScanGap);

                    // TODO(lzx): handle gap lock
                    AddScanTuple(key,
                                 cce,
                                 typed_cache,
                                 ScanType::ScanGap,
                                 ng_id,
                                 ng_term,
                                 req.ReadTimestamp(),
                                 is_read_snapshot,
                                 true,
                                 req.is_ckpt_delta_);
                    break;
                }
                else
                {
                    req.SetCcePtr(cce);
                    req.SetCcePtrScanType(ScanType::ScanBoth);

                    auto lock_pair = AcquireCceKeyLock(cce,
                                                       cce->payload_status_,
                                                       &req,
                                                       ng_id,
                                                       ng_term,
                                                       tx_term,
                                                       cc_op,
                                                       iso_lvl,
                                                       cc_proto,
                                                       req.ReadTimestamp());
                    switch (lock_pair.second)
                    {
                    case CcErrorCode::NO_ERROR:
                        break;
                    case CcErrorCode::MVCC_READ_MUST_WAIT_WRITE:
                    {
                        req.SetIsWaitForPostWrite(true);
                        // Put the request to top of key lock's blocking queue
                        // with acquring readlock. And then should release the
                        // readlock before handling this requst when PostWriteCc
                        // finished.
                        cce->key_lock_ptr_->InsertBlockingQueue(
                            &req, LockType::ReadLock);
                        shard_->CheckRecoverTx(
                            cce->key_lock_ptr_->WriteLockTx(), ng_id, ng_term);
                        // After inserting to blocking queue, the execution of
                        // current ReadCc request should stop.
                        return false;
                    }
                    case CcErrorCode::ACQUIRE_LOCK_BLOCKED:
                    {
                        // Lock fail should stop the execution of current
                        // CC request since it's already in blocking queue.
                        return false;
                    }
                    default:
                    {
                        // lock confilct: back off and retry.
                        req.Result()->SetError(lock_pair.second);
                        return true;
                    }
                    }  //-- end: switch

                    AddScanTuple(key,
                                 cce,
                                 typed_cache,
                                 ScanType::ScanBoth,
                                 ng_id,
                                 ng_term,
                                 req.ReadTimestamp(),
                                 is_read_snapshot,
                                 true,
                                 req.is_ckpt_delta_);
                }
            }
        }

        req.Result()->SetFinished();
        return true;
    }

    void AddScanTupleMsg(const KeyT *key,
                         CcEntry<KeyT, ValueT> *cce,
                         RemoteScanCache *remote_cache,
                         ScanType scan_type,
                         int64_t ng_term,
                         uint64_t read_ts,
                         bool is_read_snapshot,
                         bool keep_deleted = true,
                         bool is_ckpt_delta = false)
    {
        assert(scan_type != ScanType::ScanUnknow);

        switch (scan_type)
        {
        case ScanType::ScanGap:
            if (!is_ckpt_delta)
            {
                remote::ScanTuple_msg *tuple =
                    remote_cache->cache_msg_->add_scan_tuple();
                ScanGap(key, cce, tuple, ng_term);
            }
            break;
        case ScanType::ScanBoth:
            ScanKey(
                key,
                cce,
                remote_cache,
                true,
                ng_term,
                read_ts,
                is_read_snapshot,
                keep_deleted,
                (table_name_.Type() != TableType::Secondary) && is_ckpt_delta);
            break;
        case ScanType::ScanKey:
            ScanKey(
                key,
                cce,
                remote_cache,
                false,
                ng_term,
                read_ts,
                is_read_snapshot,
                (table_name_.Type() != TableType::Secondary) && is_ckpt_delta);
            break;
        default:
            break;
        }
    }

    bool Execute(remote::RemoteScanOpen &req) override
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

        uint32_t ng_id = req.NodeGroupId();
        int64_t ng_term = Sharder::Instance().LeaderTerm(ng_id);
        int64_t tx_term = req.TxTerm();
        CODE_FAULT_INJECTOR("term_TemplateCcMap_Execute_RemoteScanOpen", {
            LOG(INFO) << "FaultInject  "
                         "term_TemplateCcMap_Execute_RemoteScanOpen";
            ng_term = -1;
            FaultInject::Instance().InjectFault(
                "term_TemplateCcMap_Execute_RemoteScanOpen", "remove");
        });
        if (ng_term < 0)
        {
            req.Result()->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            return true;
        }

        IsolationLevel iso_lvl = req.Isolation();
        CcProtocol cc_proto = req.Protocol();
        CcOperation cc_op;
        bool is_read_snapshot;
        if (table_name_.Type() == TableType::Secondary)
        {
            cc_op = CcOperation::ReadSkIndex;
            is_read_snapshot = (iso_lvl == IsolationLevel::Snapshot);
        }
        else
        {
            cc_op = req.IsForWrite() ? CcOperation::ReadForWrite
                                     : CcOperation::Read;
            is_read_snapshot =
                (iso_lvl == IsolationLevel::Snapshot && !req.IsForWrite());
        }

        const KeyT *look_key;
        KeyT key_obj;

        switch (req.key_type_)
        {
        case KeyType::NegativeInf:
            look_key = NegativeInfinity<KeyT>::Instance();
            break;
        case KeyType::PositiveInf:
            look_key = PositiveInfinity<KeyT>::Instance();
            break;
        default:
            size_t offset = 0;
            key_obj.Deserialize(
                req.start_key_str_->data(), offset, KeySchema());
            look_key = &key_obj;
            break;
        }

        RemoteScanCache &scan_cache = req.scan_caches_[shard_->LocalCoreId()];

        Iterator scan_ccm_it;
        const KeyT *key_ptr = nullptr;
        CcEntry<KeyT, ValueT> *cce = nullptr;

        if (req.CcePtr(shard_->LocalCoreId()) != nullptr)
        {
            cce = static_cast<CcEntry<KeyT, ValueT> *>(
                req.CcePtr(shard_->LocalCoreId()));
            scan_ccm_it = Iterator(cce, &neg_inf_, &pos_inf_);
            key_ptr = scan_ccm_it->first;
            ScanType scan_type = req.CcePtrScanType(shard_->LocalCoreId());

            req.SetCcePtr(nullptr, shard_->LocalCoreId());
            req.SetCcePtrScanType(ScanType::ScanUnknow, shard_->LocalCoreId());

            if (req.IsWaitForPostWrite(shard_->LocalCoreId()))
            {
                req.SetIsWaitForPostWrite(false, shard_->LocalCoreId());
                cce->key_lock_ptr_->ReleaseLock(
                    req.Txn(), shard_, LockType::ReadLock);
            }
            else
            {
                // Lock has been acquired, UpsertLockHoldingTx
                auto lock_pair =
                    LockHandleForResumedRequest(cce,
                                                cce->payload_status_,
                                                &req,
                                                ng_id,
                                                ng_term,
                                                tx_term,
                                                cc_op,
                                                iso_lvl,
                                                cc_proto,
                                                req.ReadTimestamp());
                if (lock_pair.second != CcErrorCode::NO_ERROR)
                {
                    assert(lock_pair.second ==
                           CcErrorCode::MVCC_READ_FOR_WRITE_CONFLICT);
                    req.Result()->SetError(lock_pair.second);
                    return true;
                }
            }

            AddScanTupleMsg(key_ptr,
                            cce,
                            &scan_cache,
                            scan_type,
                            ng_term,
                            req.ReadTimestamp(),
                            is_read_snapshot,
                            req.is_ckpt_delta_);
            scan_ccm_it = Iterator(cce, &neg_inf_, &pos_inf_);
        }
        else
        {
            std::pair<Iterator, ScanType> start_pair =
                req.direct_ == ScanDirection::Forward
                    ? ForwardScanStart(*look_key, req.inclusive_)
                    : BackwardScanStart(*look_key, req.inclusive_);

            scan_ccm_it = start_pair.first;
            ScanType scan_type = start_pair.second;
            key_ptr = scan_ccm_it->first;
            cce = scan_ccm_it->second;

            req.SetCcePtr(cce, shard_->LocalCoreId());
            req.SetCcePtrScanType(scan_type, shard_->LocalCoreId());

            if (scan_type != ScanType::ScanGap)
            {
                auto lock_pair = AcquireCceKeyLock(cce,
                                                   cce->payload_status_,
                                                   &req,
                                                   ng_id,
                                                   ng_term,
                                                   tx_term,
                                                   cc_op,
                                                   iso_lvl,
                                                   cc_proto,
                                                   req.ReadTimestamp());
                switch (lock_pair.second)
                {
                case CcErrorCode::NO_ERROR:
                    break;
                case CcErrorCode::MVCC_READ_MUST_WAIT_WRITE:
                {
                    req.SetIsWaitForPostWrite(true, shard_->LocalCoreId());
                    // Put the request to top of key lock's blocking queue
                    // with acquring readlock. And then should release the
                    // readlock before handling this requst when PostWriteCc
                    // finished.
                    cce->key_lock_ptr_->InsertBlockingQueue(&req,
                                                            LockType::ReadLock);
                    shard_->CheckRecoverTx(
                        cce->key_lock_ptr_->WriteLockTx(), ng_id, ng_term);
                    // After inserting to blocking queue, the execution of
                    // current ReadCc request should stop.
                    return false;
                }
                case CcErrorCode::ACQUIRE_LOCK_BLOCKED:
                {
                    // Lock fail should stop the execution of current
                    // CC request since it's already in blocking queue.
                    // TODO(lzx): Add remote acknowlege when lock fail
                    return false;
                }
                default:
                {
                    // lock confilct: back off and retry.
                    req.Result()->SetError(lock_pair.second);
                    return true;
                }
                }  //-- end: switch
            }
            else
            {
                // TODO(lzx): handle gap lock
            }

            AddScanTupleMsg(key_ptr,
                            cce,
                            &scan_cache,
                            scan_type,
                            ng_term,
                            req.ReadTimestamp(),
                            is_read_snapshot,
                            req.is_ckpt_delta_);
        }

        if (req.direct_ == ScanDirection::Forward)
        {
            ++scan_ccm_it;

            Iterator pos_inf_it = End();
            for (; scan_ccm_it != pos_inf_it &&
                   scan_cache.Size() < ScanCache::ScanBatchSize;
                 ++scan_ccm_it)
            {
                key_ptr = scan_ccm_it->first;
                cce = scan_ccm_it->second;
                if (req.is_ckpt_delta_ &&
                    cce->commit_ts_ <=
                        cce->ckpt_ts_.load(std::memory_order_acquire))
                {
                    ++scan_ccm_it;
                    continue;
                }

                req.SetCcePtr(cce, shard_->LocalCoreId());
                req.SetCcePtrScanType(ScanType::ScanBoth,
                                      shard_->LocalCoreId());

                auto lock_pair = AcquireCceKeyLock(cce,
                                                   cce->payload_status_,
                                                   &req,
                                                   ng_id,
                                                   ng_term,
                                                   tx_term,
                                                   cc_op,
                                                   iso_lvl,
                                                   cc_proto,
                                                   req.ReadTimestamp());
                switch (lock_pair.second)
                {
                case CcErrorCode::NO_ERROR:
                    break;
                case CcErrorCode::MVCC_READ_MUST_WAIT_WRITE:
                {
                    req.SetIsWaitForPostWrite(true, shard_->LocalCoreId());
                    // Put the request to top of key lock's blocking queue
                    // with acquring readlock. And then should release the
                    // readlock before handling this requst when PostWriteCc
                    // finished.
                    cce->key_lock_ptr_->InsertBlockingQueue(&req,
                                                            LockType::ReadLock);
                    shard_->CheckRecoverTx(
                        cce->key_lock_ptr_->WriteLockTx(), ng_id, ng_term);
                    // After inserting to blocking queue, the execution of
                    // current ReadCc request should stop.
                    return false;
                }
                case CcErrorCode::ACQUIRE_LOCK_BLOCKED:
                {
                    // Lock fail should stop the execution of current
                    // CC request since it's already in blocking queue.
                    // TODO(lzx): Add remote acknowlege when lock fail
                    return false;
                }
                default:
                {
                    // lock confilct: back off and retry.
                    req.Result()->SetError(lock_pair.second);
                    return true;
                }
                }  //-- end: switch

                AddScanTupleMsg(key_ptr,
                                cce,
                                &scan_cache,
                                ScanType::ScanBoth,
                                ng_term,
                                req.ReadTimestamp(),
                                is_read_snapshot,
                                req.is_ckpt_delta_);
            }
        }
        else
        {
            --scan_ccm_it;

            Iterator neg_inf_it = Begin();
            for (; scan_ccm_it != neg_inf_it &&
                   scan_cache.Size() < ScanCache::ScanBatchSize;
                 --scan_ccm_it)
            {
                key_ptr = scan_ccm_it->first;
                cce = scan_ccm_it->second;
                if (req.is_ckpt_delta_ &&
                    cce->commit_ts_ <=
                        cce->ckpt_ts_.load(std::memory_order_acquire))
                {
                    --scan_ccm_it;
                    continue;
                }

                req.SetCcePtr(cce, shard_->LocalCoreId());
                req.SetCcePtrScanType(ScanType::ScanBoth,
                                      shard_->LocalCoreId());

                auto lock_pair = AcquireCceKeyLock(cce,
                                                   cce->payload_status_,
                                                   &req,
                                                   ng_id,
                                                   ng_term,
                                                   tx_term,
                                                   cc_op,
                                                   iso_lvl,
                                                   cc_proto,
                                                   req.ReadTimestamp());
                switch (lock_pair.second)
                {
                case CcErrorCode::NO_ERROR:
                    break;
                case CcErrorCode::MVCC_READ_MUST_WAIT_WRITE:
                {
                    req.SetIsWaitForPostWrite(true, shard_->LocalCoreId());
                    // Put the request to top of key lock's blocking queue
                    // with acquring readlock. And then should release the
                    // readlock before handling this requst when PostWriteCc
                    // finished.
                    cce->key_lock_ptr_->InsertBlockingQueue(&req,
                                                            LockType::ReadLock);
                    shard_->CheckRecoverTx(
                        cce->key_lock_ptr_->WriteLockTx(), ng_id, ng_term);
                    // After inserting to blocking queue, the execution of
                    // current ReadCc request should stop.
                    return false;
                }
                case CcErrorCode::ACQUIRE_LOCK_BLOCKED:
                {
                    // Lock fail should stop the execution of current
                    // CC request since it's already in blocking queue.
                    // TODO(lzx): Add remote acknowlege when lock fail
                    return false;
                }
                default:
                {
                    // lock confilct: back off and retry.
                    req.Result()->SetError(lock_pair.second);
                    return true;
                }
                }  //-- end: switch

                AddScanTupleMsg(key_ptr,
                                cce,
                                &scan_cache,
                                ScanType::ScanBoth,
                                ng_term,
                                req.ReadTimestamp(),
                                is_read_snapshot,
                                req.is_ckpt_delta_);
                scan_ccm_it = Iterator(cce, &neg_inf_, &pos_inf_);
            }
        }

        req.Result()->SetFinished();
        return true;
    }

    bool Execute(remote::RemoteScanNextBatch &req) override
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

        uint32_t ng_id = req.NodeGroupId();
        int64_t ng_term = Sharder::Instance().LeaderTerm(ng_id);
        int64_t tx_term = req.TxTerm();
        if (ng_term < 0)
        {
            req.Result()->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            return true;
        }

        IsolationLevel iso_lvl = req.Isolation();
        CcProtocol cc_proto = req.Protocol();
        CcOperation cc_op;
        bool is_read_snapshot;
        if (table_name_.Type() == TableType::Secondary)
        {
            cc_op = CcOperation::ReadSkIndex;
            is_read_snapshot = (iso_lvl == IsolationLevel::Snapshot);
        }
        else
        {
            cc_op = req.IsForWrite() ? CcOperation::ReadForWrite
                                     : CcOperation::Read;
            is_read_snapshot =
                (iso_lvl == IsolationLevel::Snapshot && !req.IsForWrite());
        }

        Iterator scan_ccm_it;
        ScanDirection direction = req.direct_;
        CcEntry<KeyT, ValueT> *prior_cce = nullptr;

        if (req.CcePtr() != nullptr)
        {
            prior_cce = static_cast<CcEntry<KeyT, ValueT> *>(req.CcePtr());
            ScanType scan_type = req.CcePtrScanType();
            scan_ccm_it = Iterator(prior_cce, &neg_inf_, &pos_inf_);
            const KeyT *prior_cce_key = scan_ccm_it->first;

            req.SetCcePtr(nullptr);
            req.SetCcePtrScanType(ScanType::ScanUnknow);

            if (req.IsWaitForPostWrite())
            {
                req.SetIsWaitForPostWrite(false);
                prior_cce->key_lock_ptr_->ReleaseLock(
                    req.Txn(), shard_, LockType::ReadLock);
            }
            else
            {
                // Lock has been acquired, UpsertLockHoldingTx
                auto lock_pair =
                    LockHandleForResumedRequest(prior_cce,
                                                prior_cce->payload_status_,
                                                &req,
                                                ng_id,
                                                ng_term,
                                                tx_term,
                                                cc_op,
                                                iso_lvl,
                                                cc_proto,
                                                req.ReadTimestamp());

                if (lock_pair.second != CcErrorCode::NO_ERROR)
                {
                    assert(lock_pair.second ==
                           CcErrorCode::MVCC_READ_FOR_WRITE_CONFLICT);
                    req.Result()->SetError(lock_pair.second);
                    return true;
                }
            }

            AddScanTupleMsg(prior_cce_key,
                            prior_cce,
                            &req.scan_cache_,
                            scan_type,
                            ng_term,
                            req.ReadTimestamp(),
                            is_read_snapshot,
                            req.is_ckpt_delta_);
        }
        else
        {
            prior_cce =
                reinterpret_cast<CcEntry<KeyT, ValueT> *>(req.prior_cce_addr_);
            scan_ccm_it = Iterator(prior_cce, &neg_inf_, &pos_inf_);
        }

        if (direction == ScanDirection::Forward)
        {
            ++scan_ccm_it;
            Iterator pos_inf_it = End();
            for (; scan_ccm_it != pos_inf_it &&
                   req.scan_cache_.Size() < ScanCache::ScanBatchSize;
                 ++scan_ccm_it)
            {
                const KeyT *key = scan_ccm_it->first;
                CcEntry<KeyT, ValueT> *cce = scan_ccm_it->second;

                if (req.is_ckpt_delta_ &&
                    cce->commit_ts_ <=
                        cce->ckpt_ts_.load(std::memory_order_acquire))
                {
                    continue;
                }

                req.SetCcePtr(cce);
                req.SetCcePtrScanType(ScanType::ScanBoth);

                auto lock_pair = AcquireCceKeyLock(cce,
                                                   cce->payload_status_,
                                                   &req,
                                                   ng_id,
                                                   ng_term,
                                                   tx_term,
                                                   cc_op,
                                                   iso_lvl,
                                                   cc_proto,
                                                   req.ReadTimestamp());
                switch (lock_pair.second)
                {
                case CcErrorCode::NO_ERROR:
                    break;
                case CcErrorCode::MVCC_READ_MUST_WAIT_WRITE:
                {
                    req.SetIsWaitForPostWrite(true);
                    // Put the request to top of key lock's blocking queue
                    // with acquring readlock. And then should release the
                    // readlock before handling this requst when PostWriteCc
                    // finished.
                    cce->key_lock_ptr_->InsertBlockingQueue(&req,
                                                            LockType::ReadLock);
                    shard_->CheckRecoverTx(
                        cce->key_lock_ptr_->WriteLockTx(), ng_id, ng_term);
                    // After inserting to blocking queue, the execution of
                    // current ReadCc request should stop.
                    return false;
                }
                case CcErrorCode::ACQUIRE_LOCK_BLOCKED:
                {
                    // Lock fail should stop the execution of current
                    // CC request since it's already in blocking queue.
                    // TODO(lzx): Add remote acknowlege when lock fail
                    return false;
                }
                default:
                {
                    // lock confilct: back off and retry.
                    req.Result()->SetError(lock_pair.second);
                    return true;
                }
                }  //-- end: switch

                AddScanTupleMsg(key,
                                cce,
                                &req.scan_cache_,
                                ScanType::ScanBoth,
                                ng_term,
                                req.ReadTimestamp(),
                                is_read_snapshot,
                                req.is_ckpt_delta_);
            }
        }
        else
        {
            --scan_ccm_it;
            Iterator neg_inf_it = Begin();
            for (; req.scan_cache_.Size() < ScanCache::ScanBatchSize;
                 --scan_ccm_it)
            {
                const KeyT *key = scan_ccm_it->first;
                CcEntry<KeyT, ValueT> *cce = scan_ccm_it->second;
                if (scan_ccm_it == neg_inf_it)
                {
                    req.SetCcePtr(cce);
                    req.SetCcePtrScanType(ScanType::ScanGap);

                    // TODO(lzx): handle gap lock
                    AddScanTupleMsg(key,
                                    cce,
                                    &req.scan_cache_,
                                    ScanType::ScanGap,
                                    ng_term,
                                    req.ReadTimestamp(),
                                    is_read_snapshot,
                                    req.is_ckpt_delta_);
                    break;
                }
                else
                {
                    req.SetCcePtr(cce);
                    req.SetCcePtrScanType(ScanType::ScanBoth);

                    auto lock_pair = AcquireCceKeyLock(cce,
                                                       cce->payload_status_,
                                                       &req,
                                                       ng_id,
                                                       ng_term,
                                                       tx_term,
                                                       cc_op,
                                                       iso_lvl,
                                                       cc_proto,
                                                       req.ReadTimestamp());
                    switch (lock_pair.second)
                    {
                    case CcErrorCode::NO_ERROR:
                        break;
                    case CcErrorCode::MVCC_READ_MUST_WAIT_WRITE:
                    {
                        req.SetIsWaitForPostWrite(true);
                        // Put the request to top of key lock's blocking queue
                        // with acquring readlock. And then should release the
                        // readlock before handling this requst when PostWriteCc
                        // finished.
                        cce->key_lock_ptr_->InsertBlockingQueue(
                            &req, LockType::ReadLock);
                        shard_->CheckRecoverTx(
                            cce->key_lock_ptr_->WriteLockTx(), ng_id, ng_term);
                        // After inserting to blocking queue, the execution of
                        // current ReadCc request should stop.
                        return false;
                    }
                    case CcErrorCode::ACQUIRE_LOCK_BLOCKED:
                    {
                        // Lock fail should stop the execution of current
                        // CC request since it's already in blocking queue.
                        // TODO(lzx): Add remote acknowlege when lock fail
                        return false;
                    }
                    default:
                    {
                        // lock confilct: back off and retry.
                        req.Result()->SetError(lock_pair.second);
                        return true;
                    }
                    }  //-- end: switch

                    AddScanTupleMsg(key,
                                    cce,
                                    &req.scan_cache_,
                                    ScanType::ScanBoth,
                                    ng_term,
                                    req.ReadTimestamp(),
                                    is_read_snapshot,
                                    req.is_ckpt_delta_);
                }
            }
        }

        req.Result()->SetFinished();
        return true;
    }

    bool Execute(ScanSliceCc &req) override
    {
        int64_t ng_term = Sharder::Instance().LeaderTerm(req.NodeGroupId());
        if (ng_term < 0 ||
            req.RangeCcNgTerm() > 0 && req.RangeCcNgTerm() != ng_term)
        {
            req.Result()->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            return req.SetFinish();
        }

        IsolationLevel iso_lvl = req.Isolation();
        CcProtocol cc_proto = req.Protocol();
        CcOperation cc_op;
        bool is_read_snapshot;
        if (table_name_.Type() == TableType::Secondary)
        {
            cc_op = CcOperation::ReadSkIndex;
            is_read_snapshot = (iso_lvl == IsolationLevel::Snapshot);
        }
        else
        {
            cc_op = req.IsForWrite() ? CcOperation::ReadForWrite
                                     : CcOperation::Read;
            is_read_snapshot =
                (iso_lvl == IsolationLevel::Snapshot && !req.IsForWrite());
        }

        CcHandlerResult<RangeScanSliceResult> *hd_res = req.Result();
        const KeyT *req_start_key = nullptr;
        if (req.StartKey() != nullptr)
        {
            req_start_key = static_cast<const KeyT *>(req.StartKey());
        }
        else if (req.StartKeyStr() != nullptr && !req.StartKeyStr()->empty())
        {
            // For a remote scan request, the request is first enqueued into the
            // 1st core, where the start key is de-serialized and re-used for
            // scans in remaining cores.
            std::unique_ptr<KeyT> decoded_key = std::make_unique<KeyT>();
            size_t offset = 0;
            decoded_key->Deserialize(
                req.StartKeyStr()->data(), offset, KeySchema());
            req_start_key = decoded_key.get();

            req.SetStartKey(std::move(decoded_key));
        }
        else if (req.Direction() == ScanDirection::Forward)
        {
            req_start_key = NegativeInfinity<KeyT>::Instance();
        }
        else
        {
            req_start_key = PositiveInfinity<KeyT>::Instance();
        }

        const KeyT *req_end_key = nullptr;
        if (req.EndKey() != nullptr)
        {
            req_end_key = static_cast<const KeyT *>(req.EndKey());
        }
        else if (req.EndKeyStr() != nullptr && !req.EndKeyStr()->empty())
        {
            std::unique_ptr<KeyT> decoded_end_key = std::make_unique<KeyT>();
            size_t offset = 0;
            decoded_end_key->Deserialize(
                req.EndKeyStr()->data(), offset, KeySchema());
            req_end_key = decoded_end_key.get();

            req.SetEndKey(std::move(decoded_end_key));
        }

        uint16_t core_id = shard_->LocalCoreId();
        TemplateScanCache<KeyT, ValueT> *scan_cache = nullptr;
        RemoteScanCache *remote_scan_cache = nullptr;
        if (req.IsLocal())
        {
            scan_cache = static_cast<TemplateScanCache<KeyT, ValueT> *>(
                req.GetLocalScanCache(core_id));
            assert(scan_cache != nullptr);
        }
        else
        {
            remote_scan_cache = req.GetRemoteScanCache(core_id);
            assert(remote_scan_cache != nullptr);
        }

        RangeSliceId slice_id;
        if (shard_->core_id_ == 0 && req.SliceId().Slice() == nullptr)
        {
            // The scan slice request is first dispatched to the 1st core, which
            // pins the slice in memory. Processing at the 1st core also sets
            // the scan batch's boundary, in case the slice in memory is too
            // large to fit into a single batch. The same request is dispatched
            // to other cores to scan in parallel. The slice is unpinned by the
            // last core finishing the scan batch.
            RangeSliceOpStatus pin_status;
            slice_id = shard_->PinRangeSlice(table_name_,
                                             req.NodeGroupId(),
                                             KeySchema(),
                                             RecordSchema(),
                                             schema_ts_,
                                             table_schema_->GetKVCatalogInfo(),
                                             req.RangeId(),
                                             *req_start_key,
                                             req.StartInclusive(),
                                             &req,
                                             pin_status);

            if (pin_status == RangeSliceOpStatus::Blocked)
            {
                return false;
            }
            else if (pin_status == RangeSliceOpStatus::Errored)
            {
                // If the pin operation returns an error, the data store
                // is inaccessible.
                hd_res->SetError(CcErrorCode::PIN_RANGE_SLICE_FAILED);
                return true;
            }

            req.SetSliceId(slice_id);
        }
        else
        {
            slice_id = req.SliceId();
            assert(slice_id.Slice() != nullptr);
        }

        Iterator scan_ccm_it;
        const KeyT *cce_key = nullptr;
        CcEntry<KeyT, ValueT> *cce = nullptr;
        uint32_t ng_id = req.NodeGroupId();
        int64_t tx_term = req.TxTerm();

        if (req.CcePtr(core_id) != nullptr)
        {
            cce = static_cast<CcEntry<KeyT, ValueT> *>(req.CcePtr(core_id));
            ScanType scan_type = req.BlockedCceScanType(core_id);
            scan_ccm_it = Iterator(cce, &neg_inf_, &pos_inf_);
            cce_key = scan_ccm_it->first;

            req.SetCcePtr(nullptr, core_id);
            req.SetCceScanType(ScanType::ScanUnknow, core_id);

            bool is_locked = false;
            if (req.IsWaitForPostWrite())
            {
                req.SetIsWaitForPostWrite(false);
                cce->key_lock_ptr_->ReleaseLock(
                    req.Txn(), shard_, LockType::ReadLock);
            }
            else
            {
                // Lock has been acquired, UpsertLockHoldingTx
                auto lock_pair =
                    LockHandleForResumedRequest(cce,
                                                cce->payload_status_,
                                                &req,
                                                ng_id,
                                                ng_term,
                                                tx_term,
                                                cc_op,
                                                iso_lvl,
                                                req.Protocol(),
                                                req.ReadTimestamp());

                if (lock_pair.second != CcErrorCode::NO_ERROR)
                {
                    assert(lock_pair.second ==
                           CcErrorCode::MVCC_READ_FOR_WRITE_CONFLICT);
                    req.Result()->SetError(lock_pair.second);
                    return true;
                }

                is_locked = true;
            }

            if (req.IsLocal())
            {
                AddScanTuple(cce_key,
                             cce,
                             scan_cache,
                             scan_type,
                             ng_id,
                             ng_term,
                             req.ReadTimestamp(),
                             is_read_snapshot,
                             is_locked);
            }
            else
            {
                AddScanTupleMsg(cce_key,
                                cce,
                                remote_scan_cache,
                                scan_type,
                                ng_term,
                                req.ReadTimestamp(),
                                is_read_snapshot,
                                is_locked);
            }
        }
        else if (req.PriorCceAddr(core_id) != 0)
        {
            cce = reinterpret_cast<CcEntry<KeyT, ValueT> *>(
                req.PriorCceAddr(core_id));
            scan_ccm_it = Iterator(cce, &neg_inf_, &pos_inf_);
        }
        else
        {
            std::pair<Iterator, ScanType> start_pair =
                req.Direction() == ScanDirection::Forward
                    ? ForwardScanStart(*req_start_key, req.StartInclusive())
                    : BackwardScanStart(*req_start_key, req.StartInclusive());

            scan_ccm_it = start_pair.first;
            ScanType scan_type = start_pair.second;
            cce_key = scan_ccm_it->first;
            cce = scan_ccm_it->second;

            // Adds the first tuple pointed by the iterator. If the request
            // specifies the end key, checks if the first tuple is within the
            // boundary specified by the end key.
            bool within_boundary;
            if (req_end_key == nullptr)
            {
                within_boundary = true;
            }
            else
            {
                within_boundary =
                    req.Direction() == ScanDirection::Forward &&
                        *scan_ccm_it->first < *req_end_key ||
                    req.Direction() == ScanDirection::Backward &&
                        *req_end_key < *scan_ccm_it->first ||
                    req.EndInclusive() && *req_end_key == *scan_ccm_it->first;
            }

            if (scan_type != ScanType::ScanGap && within_boundary)
            {
                req.SetCcePtr(cce, core_id);
                req.SetCceScanType(scan_type, core_id);

                auto lock_pair = AcquireCceKeyLock(cce,
                                                   cce->payload_status_,
                                                   &req,
                                                   ng_id,
                                                   ng_term,
                                                   tx_term,
                                                   cc_op,
                                                   iso_lvl,
                                                   cc_proto,
                                                   req.ReadTimestamp());
                switch (lock_pair.second)
                {
                case CcErrorCode::NO_ERROR:
                    break;
                case CcErrorCode::MVCC_READ_MUST_WAIT_WRITE:
                {
                    req.SetIsWaitForPostWrite(true);
                    // Put the request to top of key lock's blocking queue with
                    // acquring readlock. And then should release the readlock
                    // before handling this requst when PostWriteCc finished.
                    cce->key_lock_ptr_->InsertBlockingQueue(&req,
                                                            LockType::ReadLock);
                    shard_->CheckRecoverTx(
                        cce->key_lock_ptr_->WriteLockTx(), ng_id, ng_term);
                    // After inserting to blocking queue, the execution of
                    // current ReadCc request should stop.
                    return false;
                }
                case CcErrorCode::ACQUIRE_LOCK_BLOCKED:
                {
                    req.SetRangeCcNgTerm(ng_term);
                    // Lock fail should stop the execution of current
                    // CC request since it's already in blocking queue.
                    return false;
                }
                default:
                {
                    // lock confilct: back off and retry.
                    req.Result()->SetError(lock_pair.second);
                    return true;
                }
                }  //-- end: switch

                bool is_locked = lock_pair.first != LockType::NoLock;
                if (req.IsLocal())
                {
                    AddScanTuple(cce_key,
                                 cce,
                                 scan_cache,
                                 scan_type,
                                 ng_id,
                                 ng_term,
                                 req.ReadTimestamp(),
                                 is_read_snapshot,
                                 is_locked);
                }
                else
                {
                    AddScanTupleMsg(cce_key,
                                    cce,
                                    remote_scan_cache,
                                    scan_type,
                                    ng_term,
                                    req.ReadTimestamp(),
                                    is_read_snapshot,
                                    is_locked);
                }
            }
        }

        if (req.Direction() == ScanDirection::Forward)
        {
            ++scan_ccm_it;
            const StoreSlice *slice = slice_id.Slice();

            // The scan at core 0 sets the scan's end key. By default, the
            // scan's end is the exclusive end of the slice or the request's
            // specified end key, whichever is smaller. In case keys in the
            // slice are too many to fit into the scan cache, the key right
            // after the last scanned tuple at core 0 becomes the exclusive end
            // of scans at other cores. In such a case, it is mandatory that all
            // keys smaller than the end key at other cores are returned in this
            // batch. So, scans at other cores may slightly exceed the scan
            // cache's capacity.

            const KeyT *scan_end = nullptr;
            bool scan_end_inclusive = false;
            if (shard_->core_id_ == 0)
            {
                const KeyT *slice_end =
                    static_cast<const KeyT *>(slice->EndKey());
                if (slice_end == nullptr)
                {
                    slice_end = PositiveInfinity<KeyT>::Instance();
                }

                if (req_end_key == nullptr)
                {
                    // The request does not specify the end key. This scan
                    // batch's end is initialized to the slice's end.
                    scan_end = slice_end;
                    scan_end_inclusive = false;
                }
                else
                {
                    // If the request's specified end key falls into the slice,
                    // initializes the scan end to the request's end key. Or,
                    // the scan end is the slice's end;
                    if (*req_end_key < *slice_end ||
                        *req_end_key == *slice_end && !req.EndInclusive())
                    {
                        scan_end = req_end_key;
                        scan_end_inclusive = req.EndInclusive();
                    }
                    else
                    {
                        scan_end = slice_end;
                        scan_end_inclusive = false;
                    }
                }
            }
            else
            {
                // When the scan's end key is not set in the result after
                // scanning the first core, it means that either the request
                // specifies the scan's end, which falls into the slice, or the
                // scanned slice is the last ending with positive infinity.
                if (hd_res->Value().last_key_ == nullptr)
                {
                    if (req_end_key != nullptr)
                    {
                        scan_end = req_end_key;
                        scan_end_inclusive = req.EndInclusive();
                    }
                    else
                    {
                        scan_end = PositiveInfinity<KeyT>::Instance();
                        scan_end_inclusive = false;
                    }
                }
                else
                {
                    scan_end = static_cast<const KeyT *>(
                        hd_res->Value().last_key_.get());
                    scan_end_inclusive = false;
                }
            }

            Iterator pos_inf_it = End();
            cce_key = scan_ccm_it->first;
            cce = scan_ccm_it->second;
            bool is_cache_full = req.IsLocal() ? scan_cache->IsFull()
                                               : remote_scan_cache->IsFull();

            assert(scan_end != nullptr);

            while (scan_ccm_it != pos_inf_it &&
                   (shard_->core_id_ > 0 || !is_cache_full) &&
                   (*cce_key < *scan_end ||
                    scan_end_inclusive && *cce_key == *scan_end))
            {
                req.SetCcePtr(cce, core_id);
                req.SetCceScanType(ScanType::ScanBoth, core_id);

                auto lock_pair = AcquireCceKeyLock(cce,
                                                   cce->payload_status_,
                                                   &req,
                                                   ng_id,
                                                   ng_term,
                                                   tx_term,
                                                   cc_op,
                                                   iso_lvl,
                                                   cc_proto,
                                                   req.ReadTimestamp());
                switch (lock_pair.second)
                {
                case CcErrorCode::NO_ERROR:
                    break;
                case CcErrorCode::MVCC_READ_MUST_WAIT_WRITE:
                {
                    req.SetIsWaitForPostWrite(true);
                    // Put the request to top of key lock's blocking queue with
                    // acquring readlock. And then should release the readlock
                    // before handling this requst when PostWriteCc finished.
                    cce->key_lock_ptr_->InsertBlockingQueue(&req,
                                                            LockType::ReadLock);
                    shard_->CheckRecoverTx(
                        cce->key_lock_ptr_->WriteLockTx(), ng_id, ng_term);
                    // After inserting to blocking queue, the execution of
                    // current ReadCc request should stop.
                    return false;
                }
                case CcErrorCode::ACQUIRE_LOCK_BLOCKED:
                {
                    req.SetRangeCcNgTerm(ng_term);
                    // Lock fail should stop the execution of current
                    // CC request since it's already in blocking queue.
                    return false;
                }
                default:
                {
                    // lock confilct: back off and retry.
                    req.Result()->SetError(lock_pair.second);
                    return true;
                }
                }  //-- end: switch

                bool is_locked = lock_pair.first != LockType::NoLock;
                if (req.IsLocal())
                {
                    AddScanTuple(cce_key,
                                 cce,
                                 scan_cache,
                                 ScanType::ScanBoth,
                                 ng_id,
                                 ng_term,
                                 req.ReadTimestamp(),
                                 is_read_snapshot,
                                 is_locked);
                }
                else
                {
                    AddScanTupleMsg(cce_key,
                                    cce,
                                    remote_scan_cache,
                                    ScanType::ScanBoth,
                                    ng_term,
                                    req.ReadTimestamp(),
                                    is_read_snapshot,
                                    is_locked);
                }

                ++scan_ccm_it;
                cce_key = scan_ccm_it->first;
                cce = scan_ccm_it->second;
                is_cache_full = req.IsLocal() ? scan_cache->IsFull()
                                              : remote_scan_cache->IsFull();
            }

            // Only sets the result once at the first core.
            if (shard_->core_id_ == 0)
            {
                const KeyT *scan_end_key = nullptr;
                SlicePosition slice_position;

                // scan_ccm_it points to the entry after the last scanned tuple.
                // If the slice ends with positive infinity and has been fully
                // scanned, scan_ccm_it would point to positive infinity.
                if (scan_ccm_it != pos_inf_it &&
                    (*scan_ccm_it->first < *scan_end ||
                     scan_end_inclusive && *scan_ccm_it->first == *scan_end))
                {
                    // The slice is too large. The scan has not fully scanned
                    // the slice, before reaching the cache's size limit.
                    // Pretends the slice's exclusive end to be the key after
                    // the last scanned tuple, from which the next scan batch
                    // resume.
                    scan_end_key = scan_ccm_it->first;
                    slice_position = SlicePosition::Middle;
                }
                else
                {
                    // The slice has been fully scanned. If the request
                    // specifies the end key, which falls into the slice, given
                    // that the slice has been fully scanned, no future scan
                    // batches are needed. So, we pretend that the scan has
                    // reached the last slice ending with positive infinity.
                    // The calling tx will terminate the scan.
                    if (scan_end == PositiveInfinity<KeyT>::Instance() ||
                        req_end_key == scan_end)
                    {
                        slice_position = SlicePosition::LastSlice;
                    }
                    else
                    {
                        // scan_end must be the end of the slice.
                        scan_end_key = scan_end;

                        const KeyT *range_end =
                            static_cast<const KeyT *>(slice_id.RangeEndKey());
                        if (range_end != nullptr && *scan_end == *range_end)
                        {
                            slice_position = SlicePosition::LastSliceInRange;
                        }
                        else
                        {
                            slice_position = SlicePosition::Middle;
                        }
                    }
                }

                RangeScanSliceResult &slice_result = hd_res->Value();
                slice_result.last_key_ =
                    scan_end_key != nullptr ? scan_end_key->Clone() : nullptr;
                slice_result.slice_position_ = slice_position;
                req.SetRangeCcNgTerm(ng_term);

                // Dispatches to remaining cores to scan the slice in parallel.
                for (uint16_t core_id = 1;
                     core_id < shard_->local_shards_.Count();
                     ++core_id)
                {
                    shard_->local_shards_.EnqueueCcRequest(
                        shard_->core_id_, core_id, &req);
                }
            }
        }
        else
        {
            --scan_ccm_it;
            const StoreSlice *slice = slice_id.Slice();

            const KeyT *scan_end = nullptr;
            bool scan_end_inclusive = false;
            if (shard_->core_id_ == 0)
            {
                const KeyT *slice_begin =
                    static_cast<const KeyT *>(slice->StartKey());
                if (slice_begin == nullptr)
                {
                    slice_begin = NegativeInfinity<KeyT>::Instance();
                }

                if (req_end_key == nullptr)
                {
                    // The request does not specify the end key. This scan
                    // batch's end is initialized to the slice's start (backward
                    // scans).
                    scan_end = slice_begin;
                    scan_end_inclusive = true;
                }
                else
                {
                    // If the request's specified end key falls into the slice,
                    // initializes the scan end to the request's end key. Or,
                    // the scan end is the slice's begin.
                    if (*slice_begin < *req_end_key ||
                        *slice_begin == *req_end_key)
                    {
                        scan_end = req_end_key;
                        scan_end_inclusive = req.EndInclusive();
                    }
                    else
                    {
                        scan_end = slice_begin;
                        scan_end_inclusive = true;
                    }
                }
            }
            else
            {
                // When the scan's end key is not set in the result after
                // scanning the first core, it means that either the request
                // specifies the scan's end key, which falls into the slice, or
                // the scanned slice is the first beginning from negative
                // infinity.
                if (hd_res->Value().last_key_ == nullptr)
                {
                    if (req_end_key != nullptr)
                    {
                        scan_end = req_end_key;
                        scan_end_inclusive = req.EndInclusive();
                    }
                    else
                    {
                        scan_end = NegativeInfinity<KeyT>::Instance();
                        scan_end_inclusive = true;
                    }
                }
                else
                {
                    scan_end = static_cast<const KeyT *>(
                        hd_res->Value().last_key_.get());
                    scan_end_inclusive = true;
                }
            }

            Iterator neg_inf_it = Begin();
            cce_key = scan_ccm_it->first;
            cce = scan_ccm_it->second;
            bool is_cache_full = req.IsLocal() ? scan_cache->IsFull()
                                               : remote_scan_cache->IsFull();

            assert(scan_end != nullptr);

            while (scan_ccm_it != neg_inf_it &&
                   (shard_->core_id_ > 0 || !is_cache_full) &&
                   (*scan_end < *cce_key ||
                    scan_end_inclusive && *scan_end == *cce_key))
            {
                req.SetCcePtr(cce, core_id);
                req.SetCceScanType(ScanType::ScanBoth, core_id);

                auto lock_pair = AcquireCceKeyLock(cce,
                                                   cce->payload_status_,
                                                   &req,
                                                   ng_id,
                                                   ng_term,
                                                   tx_term,
                                                   cc_op,
                                                   iso_lvl,
                                                   cc_proto,
                                                   req.ReadTimestamp());
                switch (lock_pair.second)
                {
                case CcErrorCode::NO_ERROR:
                    break;
                case CcErrorCode::MVCC_READ_MUST_WAIT_WRITE:
                {
                    req.SetIsWaitForPostWrite(true);
                    // Put the request to top of key lock's blocking queue with
                    // acquring readlock. And then should release the readlock
                    // before handling this requst when PostWriteCc finished.
                    cce->key_lock_ptr_->InsertBlockingQueue(&req,
                                                            LockType::ReadLock);
                    shard_->CheckRecoverTx(
                        cce->key_lock_ptr_->WriteLockTx(), ng_id, ng_term);
                    // After inserting to blocking queue, the execution of
                    // current ReadCc request should stop.
                    return false;
                }
                case CcErrorCode::ACQUIRE_LOCK_BLOCKED:
                {
                    req.SetRangeCcNgTerm(ng_term);
                    // Lock fail should stop the execution of current
                    // CC request since it's already in blocking queue.
                    return false;
                }
                default:
                {
                    // lock confilct: back off and retry.
                    req.Result()->SetError(lock_pair.second);
                    return true;
                }
                }  //-- end: switch

                bool is_locked = lock_pair.first != LockType::NoLock;
                if (req.IsLocal())
                {
                    AddScanTuple(cce_key,
                                 cce,
                                 scan_cache,
                                 ScanType::ScanBoth,
                                 ng_id,
                                 ng_term,
                                 req.ReadTimestamp(),
                                 is_read_snapshot,
                                 is_locked);
                }
                else
                {
                    AddScanTupleMsg(cce_key,
                                    cce,
                                    remote_scan_cache,
                                    ScanType::ScanBoth,
                                    ng_term,
                                    req.ReadTimestamp(),
                                    is_read_snapshot,
                                    is_locked);
                }

                --scan_ccm_it;
                cce_key = scan_ccm_it->first;
                cce = scan_ccm_it->second;
                is_cache_full = req.IsLocal() ? scan_cache->IsFull()
                                              : remote_scan_cache->IsFull();
            }

            if (shard_->core_id_ == 0)
            {
                const KeyT *scan_start_key = nullptr;
                SlicePosition slice_position;

                // scan_ccm_it points to the entry before the last scanned
                // tuple.
                if (scan_ccm_it != neg_inf_it &&
                    (*scan_end < *scan_ccm_it->first ||
                     scan_end_inclusive && *scan_ccm_it->first == *scan_end))
                {
                    // The slice is too large. The scan has not fully scanned
                    // the slice, before reaching the cache's size limit.
                    // Pretends the slice's inclusive start to be the last
                    // scanned key, from which the next scan batch resumes.
                    ++scan_ccm_it;
                    scan_start_key = scan_ccm_it->first;
                    slice_position = SlicePosition::Middle;
                }
                else
                {
                    // The slice has been fully scanned. If the request
                    // specifies the end key, which falls into the slice, given
                    // that the slice has been fully scanned, no future scan
                    // batches are needed. So, we pretend that the scan has
                    // reached the first slice (starting with negative
                    // infinity). The calling tx will terminate the scan.
                    if (scan_end == NegativeInfinity<KeyT>::Instance() ||
                        req_end_key == scan_end)
                    {
                        slice_position = SlicePosition::FirstSlice;
                    }
                    else
                    {
                        // scan_end must be the start of the slice.
                        scan_start_key = scan_end;

                        const KeyT *range_start =
                            static_cast<const KeyT *>(slice_id.RangeStartKey());
                        if (range_start != nullptr && *scan_end == *range_start)
                        {
                            slice_position = SlicePosition::FirstSliceInRange;
                        }
                        else
                        {
                            slice_position = SlicePosition::Middle;
                        }
                    }
                }

                RangeScanSliceResult &slice_result = hd_res->Value();
                slice_result.last_key_ = scan_start_key != nullptr
                                             ? scan_start_key->Clone()
                                             : nullptr;
                slice_result.slice_position_ = slice_position;
                req.SetRangeCcNgTerm(ng_term);

                // Dispatches to remaining cores to scan in parallel.
                for (uint16_t core_id = 1;
                     core_id < shard_->local_shards_.Count();
                     ++core_id)
                {
                    shard_->local_shards_.EnqueueCcRequest(
                        shard_->core_id_, core_id, &req);
                }
            }
        }

        bool finish = req.SetFinish();
        if (finish)
        {
            slice_id.Unpin();
            return true;
        }
        else
        {
            return false;
        }
    }

    bool Execute(CkptScanCc &req) override
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

        LruPage *lru_ccp = req.start_page_ == nullptr ? pg_ng_inf_.ckpt_next_
                                                      : req.start_page_;
        CcPage<KeyT, ValueT> *ccp =
            static_cast<CcPage<KeyT, ValueT> *>(lru_ccp);
        // a page is pinned when CkptScan stops at it, Unpin the page when
        // CkptScan resumes
        if (req.start_page_ != nullptr)
        {
            ccp->UnpinPage();
        }

        const KeyT *start_key = static_cast<const KeyT *>(req.start_key_);
        const KeyT *end_key = static_cast<const KeyT *>(req.end_key_);

        int64_t ng_term = Sharder::Instance().LeaderTerm(req.NodeGroupId());
        if (ng_term < 0)
        {
            req.Result()->SetError(CcErrorCode::TX_NODE_NOT_LEADER);
            return true;
        }

        uint64_t recycle_ts = 1U;
        if (shard_->EnableMvcc())
        {
            recycle_ts = shard_->GlobalMinSiTxStartTs();
        }
        // todo: calculate memory accumulated
        // size_t collected_size = 0;

        // CkptScanCc is running on TxProcessor thread. To avoid blocking
        // other transaction for a long time, we only process CkptScanBatch
        // number of pages in each round. So CkptScan might stop at some page
        // and this page must not be removed or change its position in
        // checkpoint list because its address is taken by CkptScanCc. Also, the
        // page might get cleaned and become empty. To avoid dealing with empty
        // pages in ccmap, we do not clean the page ongoing CkptScan stops at.
        for (size_t scan_cnt = 0;
             scan_cnt < CkptScanCc::CkptScanBatchSize && ccp != &pg_ps_inf_;)
        {
            // a page is detached from the checkpoint list if all entries in it
            // have been flushed, i.e, a page is lazily detached from the
            // checkpoint list in the next round of checkpoint scan.
            bool detachable = true;
            auto key_it = ccp->keys_.begin();
            auto entry_it = ccp->entries_.begin();
            for (; key_it != ccp->keys_.end(); key_it++, entry_it++)
            {
                const KeyT &key = *key_it;
                CcEntry<KeyT, ValueT> *cce = entry_it->get();

                if (shard_->EnableMvcc())
                {
                    shard_->DecrementMemory(
                        cce->KickOutArchiveRecords(recycle_ts));
                }

                if (cce->NeedCkpt())
                {
                    detachable = false;
                    if (KeyInRange(&key, start_key, end_key))
                    {
#ifdef RANGE_PARTITION_ENABLED
                        if (cce->data_store_size_.load(
                                std::memory_order_acquire) == INT32_MAX)
                        {
                            // Load data store size by pinning the slice. Data
                            // store size is required to decide slice & range
                            // update plan.
                            RangeSliceOpStatus pin_status;
                            RangeSliceId slice_id = shard_->PinRangeSlice(
                                table_name_,
                                req.NodeGroupId(),
                                KeySchema(),
                                RecordSchema(),
                                table_schema_->Version(),
                                table_schema_->GetKVCatalogInfo(),
                                key,
                                true,
                                &req,
                                pin_status);
                            if (pin_status == RangeSliceOpStatus::Successful)
                            {
                                if (cce->data_store_size_.load(
                                        std::memory_order_acquire) == INT32_MAX)
                                {
                                    // If data store size is still unavailable
                                    // after the slice is loaded from data
                                    // store, that means this entry does not
                                    // exist in data store.
                                    cce->data_store_size_.store(0);
                                }
                                slice_id.Unpin();
                            }
                            else if (pin_status == RangeSliceOpStatus::Blocked)
                            {
                                ccp->PinPage();
                                req.start_page_ = ccp;
                                return false;
                            }
                            else
                            {
                                req.Result()->SetError(
                                    CcErrorCode::PIN_RANGE_SLICE_FAILED);
                                return true;
                            }
                        }
#endif
                        cce->ExportForCkpt(key,
                                           *req.ckpt_vec_,
                                           *req.archive_vec_,
                                           *req.mv_base_vec_,
                                           req.ckpt_ts_,
                                           recycle_ts,
                                           Type(),
                                           shard_->EnableMvcc());

                        if (cce->commit_ts_ <= req.ckpt_ts_)
                        {
                            // todo: decrement log size after notify log service
                            // of ckpt_ts
                            shard_->estimate_ccshard_log_size_ -=
                                cce->estimate_ccentry_log_size_;
                            cce->estimate_ccentry_log_size_ = 0;
                        }
                    }
                }
                scan_cnt++;
            }

            LruPage *next = ccp->ckpt_next_;
            if (detachable && !ccp->IsPinned())
            {
                // scan over for this page and this page can be detached from
                // the checkpoint list
                DetachFromCkptList(ccp);
            }
            // move to next page
            ccp = static_cast<CcPage<KeyT, ValueT> *>(next);
        }

        if (ccp == &pg_ps_inf_)
        {
            if (shard_->core_id_ == shard_->core_cnt_ - 1)
            {
                // Sort output vectors in key sorting order.
                std::vector<FlushRecord> &ckpt_vec = *req.ckpt_vec_;
                std::sort(ckpt_vec.begin(),
                          ckpt_vec.end(),
                          [](const FlushRecord &lhs, const FlushRecord &rhs)
                          { return *lhs.Key() < *rhs.Key(); });
                std::vector<FlushRecord> &archive_vec = *req.archive_vec_;
                std::sort(archive_vec.begin(),
                          archive_vec.end(),
                          [](const FlushRecord &lhs, const FlushRecord &rhs)
                          { return *lhs.Key() < *rhs.Key(); });

                req.Result()->SetFinished();
                return true;
            }
            else
            {
                req.Reset(req.NodeGroupId());
                MoveRequest(&req, shard_->core_id_ + 1);
            }
        }
        else
        {
            // set the start_page_ and put the CkptScanCc request into CcQueue
            // again.
            ccp->PinPage();
            req.start_page_ = ccp;
            shard_->Enqueue(&req);
        }

        return false;
    }

    bool Execute(FaultInjectCC &req) override
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

        return true;
    }

    bool Execute(ReplayLogCc &req) override
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

        KeyT key;
        // A psuedo record that is used to deserialize and move forward the
        // record that is not sharded to the core.
        ValueT rec;
        size_t offset = 0;
        const std::string_view &log_blob = req.LogContentView();

        // If the log record's commit ts is smaller than that of the cc map,
        // this record is generated before the latest schema of the table
        // and hence should skip the replay process.
        if (req.CommitTs() < schema_ts_)
        {
            req.SetFinish();
            return false;
        }

        while (offset < log_blob.size())
        {
            key.Deserialize(log_blob.data(), offset, KeySchema());
            uint8_t delete_flag =
                *reinterpret_cast<const uint8_t *>(log_blob.data() + offset);
            offset += sizeof(uint8_t);

            uint16_t core_id = (key.Hash() & 0x3FF) % shard_->core_cnt_;
            if (core_id != shard_->core_id_)
            {
                // Skips the the key in the log record that is not sharded
                // to this core.
                if (delete_flag == 0)
                {
                    rec.Deserialize(log_blob.data(), offset);
                }
                continue;
            }

            Iterator it = FindEmplace(key);
            CcEntry<KeyT, ValueT> *cce = it->second;

            if (cce == nullptr)
            {
                req.Result()->SetError(CcErrorCode::OUT_OF_MEMORY);
                return true;
            }

            if (cce->commit_ts_ >= req.CommitTs())
            {
                // If the key exists in the cc map and its commit ts is
                // greater than that of the log record, and if (1) mvcc is
                // enabled, then install  the log record into archives; (2)
                // mvcc is not enabled, then skips installing the log record
                // in the cc map and moves to the next key in the log
                // record.
                if (shard_->EnableMvcc())
                {
                    auto rec_ptr = std::make_unique<ValueT>();
                    RecordStatus rec_status = RecordStatus::Normal;
                    if (delete_flag == 0)
                    {
                        rec_ptr->Deserialize(log_blob.data(), offset);
                    }
                    else
                    {
                        rec_status = RecordStatus::Deleted;
                    }
                    shard_->mem_usage_ += cce->AddArchiveRecord(
                        std::move(rec_ptr), rec_status, req.CommitTs());
                }
                else if (delete_flag == 0)
                {
                    rec.Deserialize(log_blob.data(), offset);
                }
            }
            else
            {
                if (shard_->EnableMvcc())
                {
                    shard_->mem_usage_ += cce->ArchiveBeforeUpdate(Type());
                }
                if (delete_flag == 0)
                {
                    shard_->DecrementMemory(cce->PayloadMemUsage());
                    cce->payload_ = std::make_shared<ValueT>();
                    cce->payload_->Deserialize(log_blob.data(), offset);
                    cce->payload_status_ = RecordStatus::Normal;
                    shard_->mem_usage_ += cce->PayloadMemUsage();
                }
                else
                {
                    if (Type() != TableType::Secondary)
                    {
                        cce->payload_ = nullptr;
                    }
                    cce->payload_status_ = RecordStatus::Deleted;
                }
                cce->commit_ts_ = req.CommitTs();

                TryInsertCkptList(cce);

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

    bool Execute(CleanCcEntryForTestCc &req) override
    {
        const TxKey *key_ptr = req.Key();
        bool only_archives = req.OnlyCleanArchives();
        assert(key_ptr != nullptr);
        if (key_ptr != nullptr)
        {
            // find cc entry
            const KeyT *typed_key_ptr = dynamic_cast<const KeyT *>(key_ptr);
            const KeyT &key = *typed_key_ptr;
            auto [cce_key, cce] = Find(key);

            if (cce != nullptr)
            {
                if (req.WithFlush())
                {
                    std::vector<FlushRecord> tmp_ckpt_vec;

                    std::vector<FlushRecord> tmp_akv_vec;
                    std::vector<const TxKey *> tmp_mv_base_vec;
                    cce->ExportForCkpt(*cce_key,
                                       tmp_ckpt_vec,
                                       tmp_akv_vec,
                                       tmp_mv_base_vec,
                                       cce->commit_ts_,
                                       1U,
                                       Type(),
                                       shard_->EnableMvcc());
                    bool res = shard_->FlushEntryForTest(
                        cce, tmp_ckpt_vec, tmp_akv_vec, only_archives);
                    assert(res == true);
                }
                if (only_archives)
                {
                    cce->ClearArchives();
                }
                else
                {
                    ccm_has_full_entries_ = false;
                    Clean(cce);
                }
            }
        }
        req.Result()->SetValue(true);
        req.Result()->SetFinished();
        return true;
    }

    bool Execute(FillStoreSliceCc &req) override
    {
        const std::vector<SliceDataItem> &slice_vec =
            req.SliceData(shard_->core_id_);

        for (const SliceDataItem &data_item : slice_vec)
        {
            const KeyT *key = static_cast<const KeyT *>(data_item.key_.get());
            const ValueT *record =
                static_cast<const ValueT *>(data_item.record_.get());

            Iterator it = FindEmplace(*key);
            const KeyT *cce_key = it->first;
            CcEntry<KeyT, ValueT> *cce = it->second;
            if (cce == nullptr)
            {
                // Memory reaches capacity while bringing a range slice into
                // memory.
                req.SetError();
                return true;
            }

            uint32_t rec_store_size =
                data_item.is_deleted_ ? 0 : cce_key->Size() + record->Size();

            // The ckpt_ts_ field represents the newest version stored in the
            // data store. If ckpt_ts_ has not been set, sets the field.
            if (cce->ckpt_ts_.load(std::memory_order_relaxed) <
                data_item.version_ts_)
            {
                cce->ckpt_ts_.store(data_item.version_ts_,
                                    std::memory_order_relaxed);
            }

            if (cce->commit_ts_ > 1)
            {
                assert(data_item.version_ts_ <= cce->commit_ts_);

                // Initialize the data store size if it is unspecified before
                if (cce->data_store_size_.load(std::memory_order_acquire) ==
                    INT32_MAX)
                {
                    cce->data_store_size_.store(rec_store_size,
                                                std::memory_order_relaxed);
                }

                // The cc entry's commit ts is 1 when it is initialized.
                // Commit ts greater than 1 means that the key is already
                // cached in memory.
                continue;
            }

            if (cce->payload_ == nullptr)
            {
                cce->payload_ = std::make_shared<ValueT>(*record);
            }
            shard_->DecrementMemory(cce->payload_->MemUsage());
            cce->commit_ts_ = data_item.version_ts_;
            cce->payload_status_ = data_item.is_deleted_ ? RecordStatus::Deleted
                                                         : RecordStatus::Normal;
            cce->data_store_size_.store(rec_store_size,
                                        std::memory_order_relaxed);

            shard_->mem_usage_ += cce->payload_->MemUsage();
        }

        req.SetFinish();
        return false;
    }

    bool Execute(GetPostCkptSlice &req) override
    {
        RangeSliceId slice_id = req.SliceId();
        std::vector<SliceChangeInfo> &item_vec = req.SliceRecordCollection();

        if (shard_->core_id_ == 0)
        {
            uint64_t snapshot_ts =
                shard_->EnableMvcc() ? shard_->GlobalMinSiTxStartTs() : 0;
            RangeSliceOpStatus pin_status =
                slice_id.Range()->PinSlice(table_name_,
                                           slice_id.Slice(),
                                           KeySchema(),
                                           RecordSchema(),
                                           schema_ts_,
                                           snapshot_ts,
                                           table_schema_->GetKVCatalogInfo(),
                                           &req,
                                           shard_,
                                           shard_->local_shards_.store_hd_);

            if (pin_status == RangeSliceOpStatus::Blocked)
            {
                return false;
            }
            else if (pin_status == RangeSliceOpStatus::Errored)
            {
                item_vec.clear();
                req.SetError();
                return false;
            }
        }

        Iterator map_it, map_end_it;

        const KeyT *start_key =
            static_cast<const KeyT *>(slice_id.Slice()->StartKey());
        if (start_key == nullptr || start_key->Type() == KeyType::NegativeInf)
        {
            map_it = Begin();
        }
        else
        {
            std::pair<Iterator, ScanType> start_pair =
                ForwardScanStart(*start_key, true);
            map_it = start_pair.first;
            if (start_pair.second == ScanType::ScanGap)
            {
                ++map_it;
            }
        }

        const KeyT *end_key =
            static_cast<const KeyT *>(slice_id.Slice()->EndKey());
        // nullptr end key means PositiveInfinity
        if (end_key == nullptr || end_key->Type() == KeyType::PositiveInf)
        {
            map_end_it = End();
        }
        else
        {
            std::pair<Iterator, ScanType> end_pair =
                ForwardScanStart(*end_key, true);
            map_end_it = end_pair.first;
            if (end_pair.second == ScanType::ScanGap)
            {
                ++map_end_it;
            }
        }

        auto &ckpt_vec = req.CkptVec();
        size_t start_idx = req.SliceFirstIdx();
        size_t end_idx = req.SliceLastIdx();
        size_t ckpt_idx = start_idx;

        for (; map_it != map_end_it; ++map_it)
        {
            const KeyT *cce_key = map_it->first;
            CcEntry<KeyT, ValueT> *cce = map_it->second;

            if (cce->commit_ts_ <= 1)
            {
                // This is a new inserted key that the tx has not finished
                // post-processing.
                continue;
            }

            // Skip until the ckpt item belongs to this core.
            while (ckpt_idx < end_idx &&
                   (ckpt_vec[ckpt_idx].Key()->Hash() & 0x3FF) %
                           shard_->core_cnt_ !=
                       shard_->core_id_)
            {
                ckpt_idx++;
            }

            if (ckpt_idx < end_idx && cce == ckpt_vec[ckpt_idx].cce_)
            {
                // This entry will be flushed in this round of checkpoint.
                int32_t ckpt_size = 0;
                if (ckpt_vec[ckpt_idx].payload_status_ == RecordStatus::Deleted)
                {
                    ckpt_size = 0;
                }
                else
                {
                    ckpt_size = ckpt_vec[ckpt_idx].Key()->Size() +
                                ckpt_vec[ckpt_idx].PayloadSize();
                }
                item_vec.emplace_back(
                    cce_key,
                    ckpt_size - ckpt_vec[ckpt_idx].delta_size_,
                    ckpt_size);

                ckpt_idx++;
            }
            else
            {
                int32_t data_store_size =
                    cce->data_store_size_.load(std::memory_order_relaxed);
                if (data_store_size == INT32_MAX)
                {
                    // If data_store_size is unset (INT32_MAX) after the
                    // slice is pinned, it means that this key does not
                    // exist in the data store.
                    cce->data_store_size_.store(0, std::memory_order_release);
                    data_store_size = 0;
                }
                // This entry is not going to be flushed in this checkpoint, so
                // the data store size before and post ckpt are the same.
                item_vec.emplace_back(
                    cce_key, data_store_size, data_store_size);
            }
        }

        if (shard_->core_id_ == shard_->core_cnt_ - 1)
        {
            slice_id.Unpin();
            std::sort(item_vec.begin(),
                      item_vec.end(),
                      [](const SliceChangeInfo &lhs, const SliceChangeInfo &rhs)
                      {
                          const TxKey *l_key = lhs.slice_start_key_;
                          const TxKey *r_key = rhs.slice_start_key_;
                          return *l_key < *r_key;
                      });
            req.SetFinish();
        }
        else
        {
            MoveRequest(&req, shard_->core_id_ + 1);
        }

        return false;
    }

    size_t size() const override
    {
        return size_;
    }

    void Clean(LruEntry *remove_entry) override
    {
        CcEntry<KeyT, ValueT> *cc_entry =
            static_cast<CcEntry<KeyT, ValueT> *>(remove_entry);

#ifdef RANGE_PARTITION_ENABLED
        bool kick_ret = shard_->local_shards_.KickoutRangeSlice(
            table_name_, cc_ng_id_, *cc_entry->Key());
        if (!kick_ret)
        {
            return;
        }
#endif

        // remove entry and decrement memory usage
        CcPage<KeyT, ValueT> *page = cc_entry->parent_page_;
        const KeyT old_page_key(page->FirstKey());
        size_t mem_decreased = page->Remove(cc_entry);
        if (page->Empty())
        {
            mem_decreased += page->MemUsage();
            ccmp_.erase(old_page_key);
        }
        else if (page->FirstKey() != old_page_key)
        {
            auto page_it = ccmp_.find(old_page_key);
            assert(page_it != ccmp_.end());
            TryUpdatePageKey(page_it);
        }
        shard_->mem_usage_ -= mem_decreased;
        assert(size_ > 0);
        size_--;
    }

    /**
     * Clean erasable entries in lru_page, re-balance pages after clean.
     *
     * @param lru_page
     * @return free count and the lru_next_ of page
     */
    std::pair<size_t, LruPage *> CleanPageAndReBalance(
        LruPage *lru_page) override
    {
        size_t free_cnt = 0;
        LruPage *next_in_lru = lru_page->lru_next_;
        if (lru_page->IsPinned())
        {
            // pinned page cannot be removed, skip cleaning pinned page to avoid
            // empty page
            return {free_cnt, next_in_lru};
        }
        size_t mem_decreased = 0;

        // clean page
        CcPage<KeyT, ValueT> *page =
            static_cast<CcPage<KeyT, ValueT> *>(lru_page);
        const KeyT old_page_key(page->FirstKey());
        uint64_t last_read_ts = CleanPage(page, mem_decreased, free_cnt);

        if (page->Empty())  // remove page if empty
        {
            // only non-empty page will be pinned by ongoing CkptScanCc
            assert(!page->IsPinned());
            mem_decreased += page->MemUsage();
            if (page->lru_next_ != nullptr)
            {
                shard_->DetachLru(page);
            }
            ccmp_.erase(old_page_key);
        }
        else if (page->Size() >= CcPage<KeyT, ValueT>::merge_threshold_)
        {
            // page is still half full, no redistribution or merge needed
            auto page_it = ccmp_.find(old_page_key);
            assert(page_it != ccmp_.end());
            TryUpdatePageKey(page_it);
        }
        else
        {
            // redistribute or merge page with its siblings
            CcPage<KeyT, ValueT> *prev = page->prev_page_;
            CcPage<KeyT, ValueT> *next = page->next_page_;
            bool can_borrow_from_prev =
                prev != &pg_ng_inf_ &&
                page->Size() + prev->Size() >
                    CcPage<KeyT, ValueT>::split_threshold_;
            bool can_borrow_from_next =
                next != &pg_ps_inf_ &&
                page->Size() + next->Size() >
                    CcPage<KeyT, ValueT>::split_threshold_;
            bool can_merge_with_prev =
                prev != &pg_ng_inf_ &&
                page->Size() + prev->Size() <=
                    CcPage<KeyT, ValueT>::split_threshold_;
            bool can_merge_with_next =
                next != &pg_ps_inf_ &&
                page->Size() + next->Size() <=
                    CcPage<KeyT, ValueT>::split_threshold_;
            if (can_borrow_from_prev || can_borrow_from_next)
            {
                // map needs to be updated through iterator
                auto page_it = ccmp_.find(old_page_key);
                // the two pages whose entries need to be redistributed are
                // identified by page1 and page2, page1 is the page with smaller
                // key
                auto page1_it = page_it;
                auto page2_it = page_it;
                // decide the relative order in LRU list of the two pages by
                // comparing their last_read_ts
                uint64_t page1_last_read_ts = last_read_ts;
                uint64_t page2_last_read_ts = last_read_ts;
                if (can_borrow_from_prev)
                {
                    // borrow entries from previous page
                    page1_it--;
                    page1_last_read_ts = page1_it->second.LastReadTs();
                }
                else if (can_borrow_from_next)
                {
                    // borrow entries from next page
                    page2_it++;
                    page2_last_read_ts = page2_it->second.LastReadTs();
                }

                RedistributeBetweenPages(
                    page1_it, page2_it, page1_last_read_ts, page2_last_read_ts);
            }
            else if (can_merge_with_prev || can_merge_with_next)
            {
                // map needs to be updated through iterator
                auto page_it = ccmp_.find(old_page_key);
                // the two pages to be merged are identified by page1 and page2,
                // page1 is the page with smaller key
                auto page1_it = page_it;
                auto page2_it = page_it;
                // decide the relative order in LRU list of the two pages by
                // comparing their last_read_ts
                uint64_t page1_last_read_ts = last_read_ts;
                uint64_t page2_last_read_ts = last_read_ts;

                if (can_merge_with_prev)
                {
                    // merge `page` with its previous page
                    page1_it--;
                    page1_last_read_ts = page1_it->second.LastReadTs();
                }
                else if (can_merge_with_next)
                {
                    // merge `page` with its next page
                    page2_it++;
                    page2_last_read_ts = page2_it->second.LastReadTs();
                }

                // merge page1 and page2
                next_in_lru = MergePages(page1_it,
                                         page2_it,
                                         page1_last_read_ts,
                                         page2_last_read_ts,
                                         page,
                                         mem_decreased);
            }
        }

        shard_->DecrementMemory(mem_decreased);
        size_ -= free_cnt;
        if (free_cnt > 0)
        {
            ccm_has_full_entries_ = false;
        }

        return {free_cnt, next_in_lru};
    }

    void Clean() override
    {
        size_t mem_decreased = 0;
        for (auto it = ccmp_.begin(); it != ccmp_.end(); it++)
        {
            //            const CcPage<KeyT, ValueT> &page = it->second;
            CcPage<KeyT, ValueT> &page = it->second;
            if (page.ckpt_next_ != nullptr)
            {
                DetachFromCkptList(&page);
            }
            if (page.lru_next_ != nullptr)
            {
                shard_->DetachLru(&page);
            }
            mem_decreased += page.TotalMemUsage();
        }

        shard_->DecrementMemory(mem_decreased);
        size_ = 0;
        ccmp_.clear();
    }

    TableType Type() const override
    {
        return table_name_.Type();
    }

    void TryInsertCkptList(LruEntry *entry) override
    {
        CcEntry<KeyT, ValueT> *cce =
            static_cast<CcEntry<KeyT, ValueT> *>(entry);
        TryInsertCkptList(cce->parent_page_);
    }

    const Schema *KeySchema() const override
    {
        if (table_schema_ != nullptr)
        {
            if (table_name_.Type() == TableType::Secondary)
            {
                return table_schema_->IndexKeySchema(table_name_);
            }
            else
            {
                return table_schema_->KeySchema();
            }
        }
        return nullptr;
    }

    const Schema *RecordSchema() const override
    {
        if ((table_name_.Type() != TableType::Secondary) &&
            (table_schema_ != nullptr))
        {
            return table_schema_->RecordSchema();
        }
        return nullptr;
    }

    /**
     * Used for debug to verify the map_link is complete and keys are in order.
     */
    size_t VerifyOrdering() override
    {
        // verify page order in map
        CcPage<KeyT, ValueT> *prev_page = &pg_ng_inf_;
        for (auto it = ccmp_.begin(); it != ccmp_.end(); it++)
        {
            const KeyT &page_key = it->first;
            CcPage<KeyT, ValueT> *page = &it->second;
            assert(page_key == page->FirstKey());
            assert(page->prev_page_ == prev_page &&
                   prev_page->next_page_ == page);
            prev_page = page;
        }
        assert(prev_page->next_page_ == &pg_ps_inf_ &&
               pg_ps_inf_.prev_page_ == prev_page);

        // verify key order in all pages
        Iterator ccm_it = Begin();
        Iterator pos_inf_it = End();
        const KeyT *prev_key = ccm_it->first;
        ccm_it++;
        size_t cnt = 0;
        for (; ccm_it != pos_inf_it; ccm_it++)
        {
            const KeyT *key = ccm_it->first;
            assert(*prev_key < *key);
            prev_key = key;
            ++cnt;
        }
        assert(cnt > 0);

        return cnt;
    }

    /**
     * Used for unit test to verify the ckpt link is complete.
     */
    void VerifyCkptList()
    {
        LruPage *pre = &pg_ng_inf_;
        for (LruPage *cur = pg_ng_inf_.ckpt_next_; cur != nullptr;
             cur = cur->ckpt_next_)
        {
            assert(pre->ckpt_next_ == cur && cur->ckpt_prev_ == pre);
            pre = cur;
        }
        assert(pre == &pg_ps_inf_);
    }

    bool BulkEmplaceForTest(std::vector<KeyT *> &keys)
    {
        std::random_device rd;
        std::default_random_engine generator(rd());
        std::uniform_int_distribution<uint64_t> distribution(0, 0xFFFFFFFF);
        for (auto key : keys)
        {
            bool emplace = false;
            auto it = FindEmplace(*key, emplace);
            if (!emplace)
            {
                assert(false);
                return false;
            }
            CcEntry<KeyT, ValueT> *cce = it->second;
            cce->payload_status_ = RecordStatus::Normal;
            // randomly set ckpt_ts and commit_ts
            cce->ckpt_ts_ = distribution(generator);
            cce->commit_ts_ = distribution(generator);
            TryInsertCkptList(cce->parent_page_);
        }
        return true;
    }

protected:
    class Iterator
    {
        using iterator_category = std::bidirectional_iterator_tag;
        using difference_type = std::ptrdiff_t;
        using value_type = std::pair<const KeyT *, CcEntry<KeyT, ValueT> *>;
        using pointer = value_type *;    // or also value_type*
        using reference = value_type &;  // or also value_type&

    public:
        Iterator() = default;

        void DebugPrint()
        {
            bool is_neg_inf =
                (current_.first == NegativeInfinity<KeyT>::Instance());
            bool is_pos_inf =
                (current_.first == PositiveInfinity<KeyT>::Instance());
            LOG(INFO) << "key: " << current_.first
                      << ", cce: " << current_.second
                      << ", is neg inf: " << is_neg_inf
                      << ", is pos inf: " << is_pos_inf
                      << ", current_page_: " << current_page_
                      << ", idx_in_page_: " << idx_in_page_;
            LOG(INFO) << "neg_inf key: " << NegativeInfinity<KeyT>::Instance()
                      << ", pos_inf key: " << PositiveInfinity<KeyT>::Instance()
                      << ", neg_inf cce: " << neg_inf_cce_;
            LOG(INFO) << "pg_ng_inf_: " << neg_inf_cce_->parent_page_
                      << ", pg_ps_inf_: "
                      << &(neg_inf_cce_->parent_page_->parent_map_->pg_ps_inf_);
            if (!is_neg_inf && !is_pos_inf)
            {
                LOG(INFO) << ", cce parent_page_: "
                          << current_.second->parent_page_;
                current_.second->parent_page_->DebugPrint();
            }
        }

        Iterator(CcPage<KeyT, ValueT> *page,
                 size_t idx,
                 CcEntry<KeyT, ValueT> *neg_inf_cce)
            : neg_inf_cce_(neg_inf_cce), current_page_(page), idx_in_page_(idx)
        {
            assert(current_page_ != nullptr);
            UpdateCurrent();
        }

        Iterator(CcEntry<KeyT, ValueT> *cce,
                 CcEntry<KeyT, ValueT> *neg_inf_cce,
                 CcEntry<KeyT, ValueT> *pos_inf_cce = nullptr)
            : neg_inf_cce_(neg_inf_cce)
        {
            if (cce == neg_inf_cce)
            {
                current_.first = NegativeInfinity<KeyT>::Instance();
                current_.second = neg_inf_cce_;
                current_page_ = nullptr;
            }
            else if (cce == pos_inf_cce)
            {
                current_.first = PositiveInfinity<KeyT>::Instance();
                current_.second = nullptr;
                current_page_ = nullptr;
            }
            else
            {
                current_page_ = cce->parent_page_;
                idx_in_page_ = current_page_->FindEntry(cce);
                UpdateCurrent();
            }
        }

        Iterator(CcPage<KeyT, ValueT> *page, CcEntry<KeyT, ValueT> *cce)
        {
        }

        Iterator(CcPage<KeyT, ValueT> *page, size_t idx_in_page)
        {
        }

        Iterator(Iterator &&rhs)
            : current_(rhs.current_),
              neg_inf_cce_(rhs.neg_inf_cce_),
              current_page_(rhs.current_page_),
              idx_in_page_(rhs.idx_in_page_)
        {
        }

        Iterator(const Iterator &rhs)
            : current_(rhs.current_),
              neg_inf_cce_(rhs.neg_inf_cce_),
              current_page_(rhs.current_page_),
              idx_in_page_(rhs.idx_in_page_)
        {
        }

        Iterator &operator=(const Iterator &rhs)
        {
            current_ = rhs.current_;
            neg_inf_cce_ = rhs.neg_inf_cce_;
            current_page_ = rhs.current_page_;
            idx_in_page_ = rhs.idx_in_page_;
            return *this;
        }

        reference operator*() const
        {
            return current_;
        }

        pointer operator->()
        {
            return &current_;
        }

        // Prefix increment
        Iterator &operator++()
        {
            if (current_.first == NegativeInfinity<KeyT>::Instance())
            {
                // The iterator points to negative infinity. Increments the
                // iterator to the first page in the map, if the map is not
                // empty.
                std::map<KeyT, CcPage<KeyT, ValueT>> &internal_map =
                    static_cast<TemplateCcMap<KeyT, ValueT> *>(
                        neg_inf_cce_->parent_page_->parent_map_)
                        ->ccmp_;

                auto map_it = internal_map.begin();
                if (map_it != internal_map.end())
                {
                    // pages in cc_map should never be empty
                    current_page_ = &map_it->second;
                    idx_in_page_ = 0;
                    UpdateCurrent();
                }
                else
                {
                    // The map is empty. The next entry of negative infinity
                    // is positive infinity.
                    current_.first = PositiveInfinity<KeyT>::Instance();
                    current_.second = nullptr;
                }
            }
            else if (current_.first != PositiveInfinity<KeyT>::Instance())
            {
                if (idx_in_page_ + 1 == current_page_->Size())
                {
                    current_page_ = current_page_->next_page_;
                    if (current_page_->IsPosInf())
                    {
                        // The next entry points to positive infinity.
                        current_.first = PositiveInfinity<KeyT>::Instance();
                        current_.second = nullptr;
                    }
                    else
                    {
                        idx_in_page_ = 0;
                        UpdateCurrent();
                    }
                }
                else
                {
                    idx_in_page_++;
                    UpdateCurrent();
                }
            }
            // If the current points to positive infinity, keeps the iterator
            // unchanged.

            return *this;
        }

        // Prefix decrement
        Iterator &operator--()
        {
            if (current_.first == PositiveInfinity<KeyT>::Instance())
            {
                // The iterator points to positive infinity. Decrements the
                // iterator to the last entry in the map, if the map is not
                // empty.
                std::map<KeyT, CcPage<KeyT, ValueT>> &internal_map =
                    static_cast<TemplateCcMap<KeyT, ValueT> *>(
                        neg_inf_cce_->parent_page_->parent_map_)
                        ->ccmp_;
                auto map_it = internal_map.end();
                if (map_it != internal_map.begin())
                {
                    --map_it;
                    current_page_ = &map_it->second;
                    idx_in_page_ = current_page_->Size() - 1;
                    UpdateCurrent();
                }
                else
                {
                    // The map is empty. The prior entry of positive
                    // infinity is negative infinity.
                    current_.first = NegativeInfinity<KeyT>::Instance();
                    current_.second = neg_inf_cce_;
                    current_page_ = nullptr;
                }
            }
            else if (current_.first != NegativeInfinity<KeyT>::Instance())
            {
                if (idx_in_page_ == 0)
                {
                    // update current_page to previous page, if any
                    current_page_ = current_page_->prev_page_;
                    if (current_page_->IsNegInf())
                    {
                        current_.first = NegativeInfinity<KeyT>::Instance();
                        current_.second = neg_inf_cce_;
                    }
                    else
                    {
                        idx_in_page_ = current_page_->Size() - 1;
                        UpdateCurrent();
                    }
                }
                else
                {
                    idx_in_page_--;
                    UpdateCurrent();
                }
            }
            // If the current points to negative infinity, keeps the
            // iterator unchanged.

            return *this;
        }

        // Postfix increment
        Iterator operator++(int)
        {
            Iterator tmp = *this;
            ++(*this);
            return tmp;
        }

        // Postfix increment
        Iterator operator--(int)
        {
            Iterator tmp = *this;
            --(*this);
            return tmp;
        }

        friend bool operator==(const Iterator &lhs, const Iterator &rhs)
        {
            // The two iterators are equal, if they point to the same cc
            // entry. Note that when the iterator points to positive
            // infinity, the pointed cc entry is null.
            return lhs.current_.second == rhs.current_.second;
        };

        friend bool operator!=(const Iterator &lhs, const Iterator &rhs)
        {
            return lhs.current_.second != rhs.current_.second;
        };

    private:
        void UpdateCurrent()
        {
            assert(current_page_ != nullptr &&
                   idx_in_page_ < current_page_->Size());
            current_.first = &current_page_->keys_.at(idx_in_page_);
            current_.second = current_page_->entries_.at(idx_in_page_).get();
        }

    protected:
        std::pair<const KeyT *, CcEntry<KeyT, ValueT> *> current_{nullptr,
                                                                  nullptr};
        // neg_inf_cce_ is necessary for its gap
        CcEntry<KeyT, ValueT> *neg_inf_cce_{nullptr};

        CcPage<KeyT, ValueT> *current_page_{nullptr};
        size_t idx_in_page_{};
    };

    /**
     * @brief Returns an iterator that points to negative infinity.
     *
     * @return Iterator
     */
    Iterator Begin()
    {
        return Iterator(&neg_inf_, &neg_inf_, &pos_inf_);
    }

    /**
     * @brief Returns an iterator that points to positive infinity.
     *
     * @return Iterator
     */
    Iterator End()
    {
        return Iterator(&pos_inf_, &neg_inf_, &pos_inf_);
    }

    std::pair<const KeyT *, CcEntry<KeyT, ValueT> *> Find(const KeyT &key)
    {
        if (&key == NegativeInfinity<KeyT>::Instance())
        {
            return {NegativeInfinity<KeyT>::Instance(), &neg_inf_};
        }

        Iterator lb_it = LowerBound(key);
        if (lb_it != End() && *lb_it->first == key)
        {
            CcEntry<KeyT, ValueT> *cce = lb_it->second;
            shard_->UpdateLruList(cce->parent_page_);
            return {lb_it->first, cce};
        }
        else
        {
            // The input key does not exist.
            return {nullptr, nullptr};
        }
    }

    Iterator FindEmplace(const KeyT &key)
    {
        bool emplace;
        return FindEmplace(key, emplace);
    }

    /**
     * Find or Emplace the CcEntry with key @param key.
     *
     * @param key
     * @return The Iterator pointing to the target CcEntry
     */
    Iterator FindEmplace(const KeyT &key, bool &emplace)
    {
        emplace = false;
        if (&key == NegativeInfinity<KeyT>::Instance())
        {
            return Begin();
        }
        if (&key == PositiveInfinity<KeyT>::Instance())
        {
            return End();
        }

        // catalog ccmap bypass shard memory limit. since checkpointer may
        // emplace ccentry into ccmap.
        if (shard_->Full() && !(table_name_.Type() == TableType::Catalog) &&
            !(table_name_.Type() == TableType::RangePartition))
        {
            // The shard has reached the maximal capacity. Tries to clean cc
            // entries that have been checkpointed but are not being
            // accessed by active tx's.
            size_t free_cnt = shard_->Clean();
            if (free_cnt == 0)
            {
                return End();
            }
        }

        size_t mem_increased = 0;
        if (ccmp_.begin() == ccmp_.end())
        {
            // ccmap is empty, insert a page
            auto [it, inserted] =
                ccmp_.try_emplace(key, this, &pg_ng_inf_, &pg_ps_inf_);
            assert(inserted);
            mem_increased += it->second.MemUsage();
        }

        // First locate target page, then find or emplace `key` in the page.
        auto ub_it = ccmp_.upper_bound(key);
        auto target_it = ub_it;
        if (target_it != ccmp_.begin())
        {
            target_it--;
        }
        CcPage<KeyT, ValueT> *target_page = &target_it->second;

        size_t idx_in_page = target_page->Find(key);
        if (idx_in_page < target_page->Size())
        {
            // found, return Iterator
            Iterator iterator(target_page, idx_in_page, &neg_inf_);
            CcEntry<KeyT, ValueT> *cce_ptr = iterator->second;
            shard_->UpdateLruList(cce_ptr->parent_page_);
            return iterator;
        }

        // not found, emplace key into target page, split the page if it's full
        if (target_page->Full() && target_page->LastKey() < key)
        {
            // target page is full, choose the next page if `key` can be
            // inserted into next page
            target_it++;
            if (target_it == ccmp_.end())
            {
                // create a new page
                target_it = ccmp_.try_emplace(
                    target_it, key, this, target_page, target_page->next_page_);
                mem_increased += target_it->second.MemUsage();
            }
            target_page = &target_it->second;
        }

        if (target_page->Full())
        {
            // split this page
            std::vector<KeyT> new_page_keys;
            std::vector<std::unique_ptr<CcEntry<KeyT, ValueT>>>
                new_page_entries;
            target_page->Split(new_page_keys, new_page_entries);

            const KeyT &key_of_new_page = *new_page_keys.begin();
            auto new_page_it = ccmp_.try_emplace(target_it,
                                                 key_of_new_page,
                                                 this,
                                                 std::move(new_page_keys),
                                                 std::move(new_page_entries),
                                                 target_page,
                                                 target_page->next_page_);
            CcPage<KeyT, ValueT> *new_page = &new_page_it->second;
            mem_increased += new_page->MemUsage();

            // insert new page into checkpoint list and lru list right after old
            // page
            if (target_page->ckpt_next_ != nullptr)
            {
                LruPage *next = target_page->ckpt_next_;
                new_page->ckpt_next_ = next;
                next->ckpt_prev_ = new_page;
                target_page->ckpt_next_ = new_page;
                new_page->ckpt_prev_ = target_page;
            }
            if (target_page->lru_next_ != nullptr)
            {
                LruPage *next = target_page->lru_next_;
                new_page->lru_next_ = next;
                next->lru_prev_ = new_page;
                target_page->lru_next_ = new_page;
                new_page->lru_prev_ = target_page;
            }

            if (new_page->FirstKey() <= key)
            {
                target_it = new_page_it;
                target_page = new_page;
            }
        }

        idx_in_page = target_page->Emplace(key, mem_increased);
        emplace = true;
        // modify page key in the map if it changed
        TryUpdatePageKey(target_it);

        // update lru list
        shard_->UpdateLruList(target_page);
        shard_->mem_usage_ += mem_increased;
        size_++;

        return Iterator(target_page, idx_in_page, &neg_inf_);
    }

    Iterator Emplace(const KeyT &key)
    {
        return FindEmplace(key);
    }

    ScanType GetScanType(bool is_include_floor_cce)
    {
        if (is_include_floor_cce)
        {
            return ScanType::ScanBoth;
        }
        else
        {
            return ScanType::ScanGap;
        }
    }

    /**
     * Whether ScanGap or ScanBoth depends on whether this is range_cc_map
     * scan. For template_cc_map, start from it's gap; for range_cc_map,
     * start from it's key and gap.
     * @param it
     * @param is_include_floor_cce
     * @return
     */
    std::pair<Iterator, ScanType> MakeForwardScanPair(Iterator it,
                                                      bool is_include_floor_cce)
    {
        ScanType scan_type = GetScanType(is_include_floor_cce);

        return std::make_pair(it, scan_type);
    }

    /**
     * Find lower bound of @param key in map, i.e. the first entry whose key is
     * equal to or greater than @param key. Return an Iterator pointing to this
     * entry, if no such entry, return Begin() which points to neg_inf_.
     * @param key
     * @return
     */
    Iterator LowerBound(const KeyT &key)
    {
        if (&key == NegativeInfinity<KeyT>::Instance())
        {
            return Begin();
        }

        // ccmp_ key is each page's smallest key, so the lower bound of `key`
        // might fall into either of two adjacent pages
        auto lb_it = ccmp_.lower_bound(key);
        CcPage<KeyT, ValueT> *page1 = nullptr;
        CcPage<KeyT, ValueT> *page2 = nullptr;
        auto pg_it1 = lb_it;
        auto pg_it2 = lb_it;
        if (pg_it2 != ccmp_.end())
        {
            page2 = &pg_it2->second;
        }
        if (pg_it1 != ccmp_.begin())
        {
            pg_it1--;
            page1 = &pg_it1->second;
        }
        // now we have the order:
        // neg_inf < page1(if exist)->FirstKey() < key <= page2(if
        // exist)->FirstKey() < pos_inf_

        if (page2 == nullptr && (page1 == nullptr || page1->LastKey() < key))
        {
            // no key bigger than or equal to `key`
            return End();
        }
        else if (page1 != nullptr && key <= page1->LastKey())
        {
            // page1->FirstKey < key <= page1->LastKey(), the lower bound of
            // `key` must locate in page1
            size_t idx_in_page = page1->LowerBound(key);
            return Iterator(page1, idx_in_page, &neg_inf_);
        }
        else
        {
            // page1->LastKey() < key <= page2->FirstKey(), the lower bound of
            // `key` must be the first key of page2
            return Iterator(page2, 0, &neg_inf_);
        }
    }

    /**
     * Find upper bound of @param key in map, i.e. the first entry whose key is
     * greater than @param key. Return an Iterator pointing to this entry, if
     * no such entry, return End() which points to pos_inf_.
     * @param key
     * @return
     */
    Iterator UpperBound(const KeyT &key)
    {
        // ccmp_ key is each page's smallest key, so the upper bound of `key`
        // might fall into either of two adjacent pages
        auto ub_it = ccmp_.upper_bound(key);
        CcPage<KeyT, ValueT> *page1 = nullptr;
        CcPage<KeyT, ValueT> *page2 = nullptr;
        auto pg_it1 = ub_it;
        auto pg_it2 = ub_it;
        if (pg_it2 != ccmp_.end())
        {
            page2 = &pg_it2->second;
        }
        if (pg_it1 != ccmp_.begin())
        {
            pg_it1--;
            page1 = &pg_it1->second;
        }
        // now we have the order:
        // neg_inf <= page1(if exist)->FirstKey() <= key < page2(if
        // exist)->FirstKey() < pos_inf

        if (page2 == nullptr && (page1 == nullptr || page1->LastKey() <= key))
        {
            // no key bigger than `key`
            return End();
        }
        else if (page1 != nullptr && key < page1->LastKey())
        {
            // page1->FirstKey <= key < page1->LastKey(), the upper bound of
            // `key` must locate in page1
            size_t idx_in_page = page1->UpperBound(key);
            return Iterator(page1, idx_in_page, &neg_inf_);
        }
        else
        {
            // page1->LastKey() <= key < page2->FirstKey(), the upper bound of
            // `key` must be the first key of page2
            return Iterator(page2, 0, &neg_inf_);
        }
    }

    /**
     * @brief Finds the greatest cc entry whose key is less than or equal to
     * the input key. If the map is empty, the floor key is negative
     * infinity.
     *
     * @param key The input key
     * @return The Iterator pointing to the cc entry whose key is the greatest
     * key less than or equal to the input key. If `key` is positive infinity,
     * the Iterator points to the last key in the map.
     */
    Iterator Floor(const KeyT &key)
    {
        Iterator it = LowerBound(key);
        if (*it->first != key ||
            it == End())  // special case for positive infinity
        {
            // lower bound of a non-negative infinity key should never be
            // Begin()
            assert(it != Begin());
            it--;
        }
        return it;
    }

    /**
     * @brief Searches the start cc entry of a forward scan.
     *
     * @param key Search key
     * @param inclusive Whether or not the start key is included in the scan
     * @param is_include_floor_cce This param is used only by range_cc_map
     * scan, and is always true. Range scan searches for the floor of the
     * search key and returns both its key and gap.
     * @return std::pair<typename std::map<KeyT, CcEntry<KeyT,
     * ValueT>>::const_iterator, ScanType> A pair of a forward map iterator
     * starting from the start cc entry and whether the scan includes the
     * start cc entry's key or gap or both.
     */
    std::pair<Iterator, ScanType> ForwardScanStart(
        const KeyT &key, bool inclusive, bool is_include_floor_cce = false)
    {
        if (key.Type() == KeyType::NegativeInf)
        {
            return MakeForwardScanPair(Begin(), is_include_floor_cce);
        }
        if (key.Type() == KeyType::PositiveInf)
        {
            return MakeForwardScanPair(End(), is_include_floor_cce);
        }

        if (inclusive ||
            is_include_floor_cce)  // >= `key` or range_cc_map scan,
                                   // search for lower_bound(key)
        {
            auto lb_it = LowerBound(key);
            if (lb_it == End() || !(*lb_it->first == key))
            {
                // for template_cc_map, start from previous entry's gap;
                // for range_cc_map, start from previous entry's key and gap
                if (lb_it == Begin())
                {
                    return MakeForwardScanPair(lb_it, is_include_floor_cce);
                }
                else
                {
                    --lb_it;
                    return MakeForwardScanPair(lb_it, is_include_floor_cce);
                }
            }
            else
            {
                // lb_it's key is exactly equal to `key`, start from lb_it and
                // its gap, but not including previous key gap
                return std::make_pair(lb_it, ScanType::ScanBoth);
            }
        }
        else  // > `key`, search for the entry before upper_bound(key)
        {
            auto ub_it = UpperBound(key);
            // start from the gap of previous entry of ub_it
            if (ub_it == Begin())
            {
                return std::make_pair(ub_it, ScanType::ScanGap);
            }
            else
            {
                ub_it--;
                return std::make_pair(ub_it, ScanType::ScanGap);
            }
        }
    }

    /**
     * @brief Searches the start cc entry of a backward scan.
     *
     * @param key Search key
     * @param inclusive Whether or not the start key is included in the scan
     * @return std::pair<typename std::map<KeyT, CcEntry<KeyT,
     * ValueT>>::const_iterator, ScanType> A pair of a backward map iterator
     * starting from the start cc entry and whether the scan includes the
     * start cc entry's key or gap or both.
     */
    std::pair<Iterator, ScanType> BackwardScanStart(const KeyT &key,
                                                    bool inclusive)
    {
        if (key.Type() == KeyType::PositiveInf)
        {
            auto start_it = End();
            --start_it;
            if (start_it->first == NegativeInfinity<KeyT>::Instance())
            {
                return std::make_pair(start_it, ScanType::ScanGap);
            }
            else
            {
                return std::make_pair(start_it, ScanType::ScanBoth);
            }
        }

        if (inclusive)  // <= `key`, search for the entry before
                        // upper_bound(key)
        {
            Iterator ub_it = UpperBound(key);
            if (ub_it == Begin())
            {
                // map empty or every key in map is greater than `key`, return
                // neg_inf_'s gap
                return std::make_pair(ub_it, ScanType::ScanGap);
            }
            else
            {
                ub_it--;
                // now, ub_it is the greatest entry equal to or less than `key`

                if (!(*ub_it->first == key))
                {
                    // key not equal, should include the gap
                    return std::make_pair(ub_it, ScanType::ScanBoth);
                }
                else
                {
                    // exactly full key match, not include the gap
                    return std::make_pair(ub_it, ScanType::ScanKey);
                }
            }
        }
        else  // < `key`, search for the entry before lower_bound(key)
        {
            Iterator lb_it = LowerBound(key);
            if (lb_it == Begin())
            {
                // map empty or every key in ccm_ is greater than or equal to
                // `key`, return neg_inf_ gap
                return std::make_pair(lb_it, ScanType::ScanGap);
            }
            else
            {
                // starting from the gap and key of entry before lb_it
                lb_it--;
                return std::make_pair(lb_it, ScanType::ScanBoth);
            }
        }
    }

    void ScanKey(const KeyT *key,
                 CcEntry<KeyT, ValueT> *cce,
                 TemplateScanCache<KeyT, ValueT> *typed_cache,
                 bool include_gap,
                 uint32_t ng_id,
                 int64_t ng_term,
                 uint64_t read_ts,
                 bool is_read_snapshot,
                 bool keep_deleted,
                 bool is_ckpt_delta = false)
    {
        TemplateScanTuple<KeyT, ValueT> *tuple = nullptr;
        uint32_t tuple_size = 0;

        if (is_read_snapshot)
        {
            VersionResultRecord<ValueT> v_rec;
            cce->MvccGet(read_ts, Type(), v_rec);

#ifdef RANGE_PARTITION_ENABLED
            // For snapshot reads, only if the visible version's record status
            // is deleted and no lock has been put on it, should the record be
            // skipped in the result set. Note that if the visible version is
            // mising in memory, the key still needs to be returned. Runtime
            // will use the key to retrieve the visible version from the data
            // store.
            if (v_rec.payload_status_ == RecordStatus::Deleted && !keep_deleted)
            {
                return;
            }
            else
            {
                tuple = typed_cache->AddScanTuple();
            }
#else
            tuple = typed_cache->AddScanTuple();
#endif
            tuple->KeyObj().Copy(*key);
            tuple_size = key->Size();

            if (v_rec.payload_status_ == RecordStatus::Normal ||
                (is_ckpt_delta &&
                 v_rec.payload_status_ == RecordStatus::Deleted))
            {
                if (v_rec.payload_ptr_ != nullptr)
                {
                    tuple->SetRecord(v_rec.payload_ptr_);
                    tuple_size += v_rec.payload_ptr_->Size();
                }
            }
            tuple->key_ts_ = v_rec.commit_ts_;
            tuple->rec_status_ = v_rec.payload_status_;
        }
        else
        {
#ifdef RANGE_PARTITION_ENABLED
            if (cce->payload_status_ == RecordStatus::Normal ||
                cce->payload_status_ == RecordStatus::Deleted && keep_deleted)
            {
                tuple = typed_cache->AddScanTuple();
            }
            else
            {
                return;
            }
#else
            tuple = typed_cache->AddScanTuple();
#endif
            tuple->KeyObj().Copy(*key);
            tuple_size = key->Size();

            if (cce->payload_status_ == RecordStatus::Normal ||
                (is_ckpt_delta &&
                 cce->payload_status_ == RecordStatus::Deleted))
            {
                if (cce->payload_ != nullptr)
                {
                    tuple->SetRecord(cce->payload_);
                    tuple_size += cce->payload_->Size();
                }
            }
            tuple->rec_status_ = cce->payload_status_;
            tuple->key_ts_ = cce->commit_ts_;
        }

        tuple->gap_ts_ = include_gap ? cce->gap_commit_ts_ : 0;
        tuple->cce_addr_.SetCce(reinterpret_cast<uint64_t>(cce),
                                ng_term,
                                ng_id,
                                shard_->LocalCoreId());

        typed_cache->AddScanTupleSize(tuple_size);
    }

    void ScanKey(const KeyT *key,
                 CcEntry<KeyT, ValueT> *cce,
                 RemoteScanCache *remote_cache,
                 bool include_gap,
                 int64_t ng_term,
                 uint64_t read_ts,
                 bool is_read_snapshot,
                 bool keep_deleted,
                 bool is_ckpt_delta = false) const
    {
        remote::ScanTuple_msg *tuple = nullptr;
        uint32_t tuple_size = 0;

        if (is_read_snapshot)
        {
            VersionResultRecord<ValueT> v_rec;
            cce->MvccGet(read_ts, Type(), v_rec);

#ifdef RANGE_PARTITION_ENABLED
            // For snapshot reads, only if the visible version's record status
            // is deleted and no lock has been put on it, should the record be
            // skipped in the result set. Note that if the visible version is
            // mising in memory, the key still needs to be returned. Runtime
            // will use the key to retrieve the visible version from the data
            // store.
            if (v_rec.payload_status_ == RecordStatus::Deleted && !keep_deleted)
            {
                return;
            }
            else
            {
                tuple = remote_cache->cache_msg_->add_scan_tuple();
            }
#else
            tuple = remote_cache->cache_msg_->add_scan_tuple();
#endif
            key->Serialize(*tuple->mutable_key());
            tuple_size += key->Size();

            if (v_rec.payload_status_ == RecordStatus::Normal ||
                (is_ckpt_delta &&
                 v_rec.payload_status_ == RecordStatus::Deleted))
            {
                tuple->clear_record();
                if (v_rec.payload_ptr_ != nullptr)
                {
                    v_rec.payload_ptr_->Serialize(*tuple->mutable_record());
                    tuple_size += v_rec.payload_ptr_->Size();
                }
            }
            tuple->set_rec_status(remote::ToRemoteType::ConvertRecordStatus(
                v_rec.payload_status_));
            tuple->set_key_ts(v_rec.commit_ts_);
        }
        else
        {
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

            if (cce->payload_status_ == RecordStatus::Normal ||
                (is_ckpt_delta &&
                 cce->payload_status_ == RecordStatus::Deleted))
            {
                tuple->clear_record();
                if (cce->payload_ != nullptr)
                {
                    cce->payload_->Serialize(*tuple->mutable_record());
                    tuple_size += cce->payload_->Size();
                }
            }
            tuple->set_rec_status(remote::ToRemoteType::ConvertRecordStatus(
                cce->payload_status_));
            tuple->set_key_ts(cce->commit_ts_);
        }

        if (include_gap)
        {
            tuple->set_gap_ts(cce->gap_commit_ts_);
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

    void ScanGap(const KeyT *key,
                 CcEntry<KeyT, ValueT> *cce,
                 TemplateScanTuple<KeyT, ValueT> *tuple,
                 uint32_t ng_id,
                 int64_t ng_term) const
    {
        tuple->key_ts_ = 0;
        tuple->gap_ts_ = cce->gap_commit_ts_;
        tuple->cce_addr_.SetCce(reinterpret_cast<uint64_t>(cce),
                                ng_term,
                                ng_id,
                                shard_->LocalCoreId());
    }

    void ScanGap(const KeyT *key,
                 CcEntry<KeyT, ValueT> *cce,
                 remote::ScanTuple_msg *tuple,
                 int64_t ng_term) const
    {
        tuple->set_key_ts(0);
        tuple->set_gap_ts(cce->gap_commit_ts_);

        remote::CceAddr_msg *cce_addr = tuple->mutable_cce_addr();
        cce_addr->set_cce_ptr(reinterpret_cast<uint64_t>(cce));
        cce_addr->set_term(ng_term);

        // For remote scans, the returned cc entries' node group ID is set
        // on the sender side when the sender receives the response.
    }

    /**
     * @brief If key is in range of [start key, end key), left inclusive right
     * open.
     *
     * @param key
     * @param start_key
     * @param end_key
     * @return true
     * @return false
     */
    bool KeyInRange(const KeyT *key, const KeyT *start_key, const KeyT *end_key)
    {
        if (start_key == nullptr && end_key == nullptr)
        {
            // Range is negative inf to positive inf
            return true;
        }

        if (*start_key < *key || *start_key == *key)
        {
            return *key < *end_key;
        }

        return false;
    }

    void DetachFromCkptList(LruPage *page)
    {
        LruPage *prev = page->ckpt_prev_;
        LruPage *next = page->ckpt_next_;
        assert(prev != nullptr && next != nullptr);
        prev->ckpt_next_ = next;
        next->ckpt_prev_ = prev;
        page->ckpt_prev_ = nullptr;
        page->ckpt_next_ = nullptr;
    }

    void TryInsertCkptList(LruPage *page)
    {
        if (page->ckpt_next_ == nullptr)
        {
            LruPage *old_tail = pg_ps_inf_.ckpt_prev_;
            old_tail->ckpt_next_ = page;
            page->ckpt_prev_ = old_tail;
            page->ckpt_next_ = &pg_ps_inf_;
            pg_ps_inf_.ckpt_prev_ = page;
        }
    }

    void TryUpdatePageKey(
        typename std::map<KeyT, CcPage<KeyT, ValueT>>::iterator &page_it)
    {
        CcPage<KeyT, ValueT> &page = page_it->second;
        if (page_it->first != page.FirstKey())
        {
            auto node_handle = ccmp_.extract(page_it);
            KeyT new_key(page.FirstKey());
            node_handle.key() = std::move(new_key);
            auto insert_res = ccmp_.insert(std::move(node_handle));
            assert(insert_res.inserted);
            page_it = insert_res.position;
        }
    }

    /**
     * Clean page and return the last_read_ts of page.
     *
     * @param page
     * @param mem_decreased
     * @param free_cnt
     * @return
     */
    uint64_t CleanPage(CcPage<KeyT, ValueT> *page,
                       size_t &mem_decreased,
                       size_t &free_cnt)
    {
        uint64_t last_read_ts = 0;
        std::vector<KeyT> &keys = page->keys_;
        std::vector<std::unique_ptr<CcEntry<KeyT, ValueT>>> &entries =
            page->entries_;
        auto key_insert_it = keys.begin();
        auto entry_insert_it = entries.begin();

        bool detach_ckpt = true;
        auto key_it = keys.begin();
        auto entry_it = entries.begin();
        for (; key_it != keys.end(); key_it++, entry_it++)
        {
            CcEntry<KeyT, ValueT> *cce = entry_it->get();
            last_read_ts = std::max(last_read_ts, cce->last_read_ts_);
            if (cce->NeedCkpt())
            {
                detach_ckpt = false;
            }
            if (cce->IsFree())
            {
                // free entries will be erased
                mem_decreased += cce->GetCcEntryMemUsage() +
                                 key_it->MemUsage() - sizeof(KeyT);
                free_cnt++;
            }
            else
            {
                detach_ckpt = false;
                // keep the entries that are not free
                *key_insert_it = std::move(*key_it);
                *entry_insert_it = std::move(*entry_it);
                key_insert_it++;
                entry_insert_it++;
            }
        }
        keys.erase(key_insert_it, keys.end());
        entries.erase(entry_insert_it, entries.end());

        if (detach_ckpt && page->ckpt_next_ != nullptr && !page->IsPinned())
        {
            // detach page from the checkpoint list if it is not pinned
            DetachFromCkptList(page);
        }
        return last_read_ts;
    }

    /**
     * Redistribute entries between page1 and page2. This happens when one page
     * is cleaned and its size is below merge threshold and it needs to borrow
     * entries from its siblings to keep the tree balanced.
     *
     * @param page1_it
     * @param page2_it
     * @param page1_last_read_ts
     * @param page2_last_read_ts
     * @return
     */
    void RedistributeBetweenPages(
        typename std::map<KeyT, CcPage<KeyT, ValueT>>::iterator &page1_it,
        typename std::map<KeyT, CcPage<KeyT, ValueT>>::iterator &page2_it,
        uint64_t page1_last_read_ts,
        uint64_t page2_last_read_ts)
    {
        CcPage<KeyT, ValueT> &page1 = page1_it->second;
        CcPage<KeyT, ValueT> &page2 = page2_it->second;

        if (page1.Size() > page2.Size())
        {
            // move keys and entries from page1's tail to page2's head
            size_t move_pos = (page1.Size() + page2.Size()) / 2;
            page2.keys_.insert(
                page2.keys_.begin(),
                std::make_move_iterator(page1.keys_.begin() + move_pos),
                std::make_move_iterator(page1.keys_.end()));
            page1.keys_.erase(page1.keys_.begin() + move_pos,
                              page1.keys_.end());
            // update parent page of entries to be moved
            for (auto entry_ptr_it = page1.entries_.begin() + move_pos;
                 entry_ptr_it != page1.entries_.end();
                 entry_ptr_it++)
            {
                (*entry_ptr_it)->parent_page_ = &page2;
            }
            page2.entries_.insert(
                page2.entries_.begin(),
                std::make_move_iterator(page1.entries_.begin() + move_pos),
                std::make_move_iterator(page1.entries_.end()));
            page1.entries_.erase(page1.entries_.begin() + move_pos,
                                 page1.entries_.end());
        }
        else
        {
            // move keys and entries from page2's head to page1's tail
            size_t move_idx = page2.Size() - (page1.Size() + page2.Size()) / 2;
            page1.keys_.insert(
                page1.keys_.end(),
                std::make_move_iterator(page2.keys_.begin()),
                std::make_move_iterator(page2.keys_.begin() + move_idx));
            page2.keys_.erase(page2.keys_.begin(),
                              page2.keys_.begin() + move_idx);
            // update parent page of entries to be moved
            for (auto entry_ptr_it = page2.entries_.begin();
                 entry_ptr_it != page2.entries_.begin() + move_idx;
                 entry_ptr_it++)
            {
                (*entry_ptr_it)->parent_page_ = &page1;
            }
            page1.entries_.insert(
                page1.entries_.end(),
                std::make_move_iterator(page2.entries_.begin()),
                std::make_move_iterator(page2.entries_.begin() + move_idx));
            page2.entries_.erase(page2.entries_.begin(),
                                 page2.entries_.begin() + move_idx);
        }

        // update page key in the map
        TryUpdatePageKey(page1_it);
        TryUpdatePageKey(page2_it);

        // update checkpoint list
        // checkpoint scan might stop at a pinned page and page redistribution
        // could happen while the checkpoint scan is ongoing. The requirements
        // are as follows:
        // 1. the pinned page and its position in checkpoint list should remain
        // unchanged;
        // 2. pages behind the pinned page in checkpoint list should not be
        // skipped by the ongoing checkpoint scan after the redistribution.
        if (page1.IsPinned() || page2.IsPinned())
        {
            // if either page is pinned by ongoing checkpoint scan, keep the
            // pinned page unchanged and insert the other after the pinned page
            CcPage<KeyT, ValueT> *pinned_page =
                page1.IsPinned() ? &page1 : &page2;
            CcPage<KeyT, ValueT> *other = page1.IsPinned() ? &page2 : &page1;
            if (other->ckpt_next_ != nullptr)
            {
                DetachFromCkptList(other);
            }
            LruPage *next = pinned_page->ckpt_next_;
            other->ckpt_next_ = next;
            other->ckpt_prev_ = pinned_page;
            next->ckpt_prev_ = other;
            pinned_page->ckpt_next_ = other;
        }
        else if (page1.ckpt_next_ != nullptr || page2.ckpt_next_ != nullptr)
        {
            // if either page is in checkpoint list, reinsert the two pages at
            // the tail
            if (page1.ckpt_next_ != nullptr)
            {
                DetachFromCkptList(&page1);
            }
            if (page2.ckpt_next_ != nullptr)
            {
                DetachFromCkptList(&page2);
            }
            TryInsertCkptList(&page1);
            TryInsertCkptList(&page2);
        }
        else
        {
            // neither page is in checkpoint list, no entry needs to be scanned
            // by CkptScanCc, do nothing
        }

        // update LRU list
        // after redistribution, the two pages should be seen as one in the LRU
        // list, insert the less recently used page after the more recently used
        // one
        LruPage *less_recently_used =
            page1_last_read_ts > page2_last_read_ts ? &page1 : &page2;
        LruPage *more_recently_used =
            page1_last_read_ts > page2_last_read_ts ? &page2 : &page1;
        if (less_recently_used->lru_next_ != nullptr)
        {
            shard_->DetachLru(less_recently_used);
        }
        LruPage *next = more_recently_used->lru_next_;
        less_recently_used->lru_next_ = next;
        next->lru_prev_ = less_recently_used;
        less_recently_used->lru_prev_ = more_recently_used;
        more_recently_used->lru_next_ = less_recently_used;
    }

    /**
     * Merge page1 and page2. Update the map, checkpoint list and lru list after
     * merge.
     *
     * @param page
     * @param page_key
     * @param mem_decreased
     * @return the lru_next_ of page
     */
    LruPage *MergePages(
        typename std::map<KeyT, CcPage<KeyT, ValueT>>::iterator &page1_it,
        typename std::map<KeyT, CcPage<KeyT, ValueT>>::iterator &page2_it,
        uint64_t page1_last_read_ts,
        uint64_t page2_last_read_ts,
        CcPage<KeyT, ValueT> *page,
        size_t &mem_decreased)
    {
        CcPage<KeyT, ValueT> *page1 = &page1_it->second;
        CcPage<KeyT, ValueT> *page2 = &page2_it->second;

        // if either page is pinned, use the pinned page as the merged page and
        // discard the other
        auto merged_page_it = page2_it->second.IsPinned() ? page2_it : page1_it;
        auto discarded_page_it =
            page2_it->second.IsPinned() ? page1_it : page2_it;
        CcPage<KeyT, ValueT> *merged_page = &merged_page_it->second;
        CcPage<KeyT, ValueT> *discarded_page = &discarded_page_it->second;

        mem_decreased += discarded_page->MemUsage();

        // merge the key vector and entry vector
        std::vector<KeyT> merged_keys = std::move(page1->keys_);
        merged_keys.insert(merged_keys.end(),
                           std::make_move_iterator(page2->keys_.begin()),
                           std::make_move_iterator(page2->keys_.end()));
        std::vector<std::unique_ptr<CcEntry<KeyT, ValueT>>> merged_entries =
            std::move(page1->entries_);
        merged_entries.insert(merged_entries.end(),
                              std::make_move_iterator(page2->entries_.begin()),
                              std::make_move_iterator(page2->entries_.end()));
        // update entry parent_page_
        for (auto &entry_ptr : merged_entries)
        {
            entry_ptr->parent_page_ = merged_page;
        }
        merged_page->keys_ = std::move(merged_keys);
        merged_page->entries_ = std::move(merged_entries);

        // Update the page order list.
        CcPage<KeyT, ValueT> *map_prev = page1->prev_page_;
        CcPage<KeyT, ValueT> *map_next = page2->next_page_;
        merged_page->prev_page_ = map_prev;
        merged_page->next_page_ = map_next;
        map_prev->next_page_ = merged_page;
        map_next->prev_page_ = merged_page;

        // Update the checkpoint list.
        // There are some issues about the merged page's position in the
        // checkpoint list, because checkpoint scan might stop at a pinned page
        // and page merge could happen while the checkpoint scan is ongoing.
        // The requirements are as follows:
        // 1. the pinned page and its position in checkpoint list should remain
        // unchanged;
        // 2. pages behind the pinned page in checkpoint list should not be
        // skipped by the ongoing checkpoint scan after the merge.
        if (merged_page_it->second.IsPinned())
        {
            // merged_page is pinned, keep its position in checkpoint list
            // unchanged, detach discard_page from the checkpoint list
            DetachFromCkptList(discarded_page);
        }
        else if (merged_page->ckpt_next_ != nullptr ||
                 discarded_page->ckpt_next_ != nullptr)
        {
            // detach both pages and insert merged_page to the end of the
            // checkpoint list so that entries in the two pages being merged are
            // guaranteed to be scanned at least once by the ongoing CkptScanCc
            if (merged_page->ckpt_next_ != nullptr)
            {
                DetachFromCkptList(merged_page);
            }
            if (discarded_page->ckpt_next_ != nullptr)
            {
                DetachFromCkptList(discarded_page);
            }
            TryInsertCkptList(merged_page);
        }
        else
        {
            // neither of the two page is in checkpoint list, no entry in the
            // merged page needs to be scanned by CkptScanCc, do nothing
        }

        // Update the LRU list.
        // record page's original lru_next as it will change after the Lru list
        // is updated
        LruPage *next = page->lru_next_;
        // skip the discarded page
        if (next == discarded_page)
        {
            next = discarded_page->lru_next_;
        }
        // the merged page should take the more recently used page's position in
        // the LRU list
        if (page1->lru_next_ == page2 || page1->lru_prev_ == page2)
        {
            // corner case: the two pages are adjacent in LRU list, just detach
            // the discarded page
            if (discarded_page->lru_next_ != nullptr)
            {
                shard_->DetachLru(discarded_page);
            }
        }
        else
        {
            LruPage *lru_prev = page1_last_read_ts > page2_last_read_ts
                                    ? page1->lru_prev_
                                    : page2->lru_prev_;
            LruPage *lru_next = page1_last_read_ts > page2_last_read_ts
                                    ? page1->lru_next_
                                    : page2->lru_next_;

            if (merged_page->lru_next_ != nullptr)
            {
                shard_->DetachLru(merged_page);
            }
            if (discarded_page->lru_next_ != nullptr)
            {
                shard_->DetachLru(discarded_page);
            }
            // reinsert the merged page into lru list
            merged_page->lru_prev_ = lru_prev;
            merged_page->lru_next_ = lru_next;
            lru_prev->lru_next_ = merged_page;
            lru_next->lru_prev_ = merged_page;
        }
        // remove discarded page from the map
        ccmp_.erase(discarded_page_it);
        // modify merged page's key in the map
        if (merged_page->FirstKey() != merged_page_it->first)
        {
            // merged page key has changed
            TryUpdatePageKey(merged_page_it);
        }

        return next;
    }

    CcPage<KeyT, ValueT> *PageNegInf()
    {
        return &pg_ng_inf_;
    }

    CcPage<KeyT, ValueT> *PagePosInf()
    {
        return &pg_ps_inf_;
    }

    std::map<KeyT, CcPage<KeyT, ValueT>> ccmp_;
    CcPage<KeyT, ValueT> pg_ng_inf_, pg_ps_inf_;
    CcEntry<KeyT, ValueT> neg_inf_, pos_inf_;
    size_t size_{};

    // When maintain_statistics_ is true, shard_profile_ is valid.
    bool maintain_statistics_;

    // shard_profile_ points to TypedShardProfile in TableSchema.
    TypedShardProfile<KeyT> *shard_profile_;
};
}  // namespace txservice
