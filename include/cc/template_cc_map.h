#pragma once

#include <algorithm>  // std::max
#include <chrono>
#include <map>
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

#ifdef RANGE_PARTITIONED
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
          ccm_(),
          neg_inf_(this),
          pos_inf_(this),
          maintain_statistics_(false),
          shard_profile_(nullptr)
    {
        neg_inf_.key_ = NegativeInfinity<KeyT>::Instance();
        pos_inf_.key_ = PositiveInfinity<KeyT>::Instance();

        neg_inf_.map_prev_ = nullptr;
        neg_inf_.map_next_ = &pos_inf_;
        pos_inf_.map_prev_ = &neg_inf_;
        pos_inf_.map_next_ = nullptr;

        neg_inf_.ckpt_prev_ = nullptr;
        neg_inf_.ckpt_next_ = &pos_inf_;
        pos_inf_.ckpt_prev_ = &neg_inf_;
        pos_inf_.ckpt_next_ = nullptr;

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
                        reinterpret_cast<uint64_t>(&this->neg_inf_.key_)))
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
                cce_ptr = Floor(*target_key);

                if (cce_ptr != &neg_inf_ && *cce_ptr->key_ == *target_key)
                {
                    // The floor entry's key is equal to the insert key. If the
                    // key is deleted, the insert becomes an update. Or the
                    // insert is aborted due to the duplidate key conflict.
                    if (cce_ptr->payload_status_ == RecordStatus::Deleted)
                    {
                        cce_addr.SetCce(reinterpret_cast<uint64_t>(cce_ptr),
                                        ng_term,
                                        req.NodeGroupId());
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
                cce_ptr = FindEmplace(*target_key);

                if (cce_ptr == nullptr)
                {
                    // The acquire request needs a new cc entry but the cc map
                    // has reached the maximal capacity. Blocks the request by
                    // putting it back to the cc request queue.
                    shard_->Enqueue(shard_->LocalCoreId(), &req);
                    return false;
                }

                assert(cce_ptr != nullptr);
                cce_addr.SetCce(reinterpret_cast<uint64_t>(cce_ptr),
                                ng_term,
                                req.NodeGroupId());
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
                               req.NodeGroupId());

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

        const CcEntryAddr &cce_addr = *req.CceAddr();

        CODE_FAULT_INJECTOR("term_TemplateCcMap_Execute_PostWriteCc", {
            if (table_name_.Type() == TableType::Primary)
            {
                LOG(INFO) << "FaultInject  "
                             "term_TemplateCcMap_Execute_PostWriteCc";
                req.Result()->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
                return true;
            }
        });

        if (!Sharder::Instance().CheckLeaderTerm(cce_addr.NodeGroupId(),
                                                 cce_addr.Term()))
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

        if (cce_addr.InsertPtr() != 0)
        {
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
                    cce_addr.InsertPtr());
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
                CcEntry<KeyT, ValueT> *new_cce = Emplace(insert_entry.key_);

                if (new_cce == nullptr)
                {
                    // The cc map has reached the maximal capacity.
                    shard_->Enqueue(shard_->LocalCoreId(), &req);
                    return false;
                }

                auto ite =
                    prior_cce.insert_intention_set_.find(&insert_entry.key_);
                assert(ite != prior_cce.insert_intention_set_.end());
                assert(ite->second->txn_ == txn);

                shard_->DecrementMemory(new_cce->PayloadMemUsage());
                if (payload_str == nullptr)
                {
                    new_cce->payload_ = std::make_unique<ValueT>(*commit_val);
                }
                else
                {
                    size_t offset = 0;
                    new_cce->payload_ = std::make_unique<ValueT>();
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

                size_t key_size = new_cce->key_->MemUsage();
                size_t payload_size = new_cce->PayloadMemUsage();
                shard_->UpdateEstimateLogSize(new_cce, key_size, payload_size);

                if (maintain_statistics_)
                {
                    shard_profile_->OnInsert(*new_cce->key_);
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
            assert(cce_addr.CcePtr() != 0);

            CcEntry<KeyT, ValueT> &cce =
                *reinterpret_cast<CcEntry<KeyT, ValueT> *>(cce_addr.CcePtr());

            if (cce.key_lock_ptr_ != nullptr &&
                cce.key_lock_ptr_->HasWriteLock() &&
                cce.key_lock_ptr_->WriteLockTx() != txn)
            {
                req.Result()->SetFinished();
                return true;
            }

            if (commit_ts > 0)
            {
#ifdef RANGE_PARTITIONED
                if (cce.delta_size_ != INT32_MAX)
                {
                    int32_t change_size = 0;
                    if (is_del)
                    {
                        // Deleted record.
                        change_size -= cce.key_->Size();
                        change_size -= cce.PayloadSize();
                    }
                    else if (cce.payload_status_ == RecordStatus::Deleted ||
                             cce.commit_ts_ == 1)
                    {
                        // A new inserted record.
                        change_size += cce.key_->Size();
                        change_size += commit_val != nullptr
                                           ? commit_val->Size()
                                           : payload_str->size();
                    }
                    else
                    {
                        // Updated record.
                        change_size -= cce.PayloadSize();
                        change_size += commit_val != nullptr
                                           ? commit_val->Size()
                                           : payload_str->size();
                    }

                    cce.delta_size_.fetch_add(change_size,
                                              std::memory_order_acq_rel);
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
                        shard_->GlobalMinSiTxStartTs(), cce.ckpt_ts_.load());
                    shard_->DecrementMemory(
                        cce.KickOutArchiveRecords(recycle_ts));
                    size_t added_mem_usage = cce.ArchiveBeforeUpdate(Type());
                    shard_->mem_usage_ += added_mem_usage;
                }

                cce.commit_ts_ = commit_ts;

                shard_->DecrementMemory(cce.PayloadMemUsage());
                if (payload_str == nullptr && !is_del)
                {
                    cce.payload_ = std::make_unique<ValueT>(*commit_val);
                }
                else if (!is_del)
                {
                    size_t offset = 0;
                    cce.payload_ = std::make_unique<ValueT>();
                    cce.payload_->Deserialize(payload_str->data(), offset);
                }
                shard_->mem_usage_ += cce.PayloadMemUsage();

                size_t key_size = cce.key_->MemUsage();
                size_t payload_size = cce.PayloadMemUsage();
                shard_->UpdateEstimateLogSize(&cce, key_size, payload_size);

                cce.payload_status_ =
                    is_del ? RecordStatus::Deleted : RecordStatus::Normal;
                TryInsertCkptList(&cce);

                DLOG_IF(INFO, TRACE_OCC_ERR)
                    << "PostWriteCc, txn:" << txn << " ,cce: " << &cce
                    << " ,commit_ts: " << commit_ts;

                if (maintain_statistics_)
                {
                    if (op_type == OperationType::Insert)
                    {
                        shard_profile_->OnInsert(*cce.key_);
                    }
                    else if (op_type == OperationType::Delete)
                    {
                        shard_profile_->OnDelete(*cce.key_);
                    }
                }
            }

            ReleaseCceKeyLock(&cce, txn, req.NodeGroupId());
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
                    target_key = neg_inf_.key_;
                    break;
                case KeyType::PositiveInf:
                    target_key = pos_inf_.key_;
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
                cce_ptr = Floor(*target_key);

                if (cce_ptr != &neg_inf_ && *cce_ptr->key_ == *target_key)
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
                                req.NodeGroupId());
                        }

                        req.SetCcePtr(cce_ptr);
                    }
                }
            }
            else
            {
                cce_ptr = FindEmplace(*target_key);

                if (cce_ptr == nullptr)
                {
                    // The acquire request needs a new cc entry but the cc map
                    // has reached the maximal capacity. Blocks the request by
                    // putting it back to the cc request queue.
                    shard_->Enqueue(shard_->LocalCoreId(), &req);
                    return false;
                }

                req.SetCcePtr(cce_ptr);
            }
        }

        // Cce ptr either points to the cc entry whose gap will accommodate the
        // new insert, or the cc entry whose key will be updated/deleted.
        CcEntry<KeyT, ValueT> &cc_entry = *cce_ptr;
        TxNumber txn = req.Txn();

        if (req.IsInsert() &&
            (cce_ptr == &neg_inf_ || !(*cce_ptr->key_ == *req.Key())))
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
                        req.NodeGroupId());
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

        CcEntry<KeyT, ValueT> *cce_ptr = nullptr;
        if (req.OpType() == OperationType::Insert)
        {
            cce_ptr = Floor(*target_key);
        }
        else
        {
            cce_ptr = FindEmplace(*target_key);
        }

        if (cce_ptr == nullptr)
        {
            shard_->Enqueue(shard_->LocalCoreId(), &req);
            return false;
        }

        TxNumber txn = req.Txn();
        uint64_t commit_ts = req.CommitTs();

        if (req.OpType() == OperationType::Insert &&
            !(*cce_ptr->key_ == *target_key))
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
                    CcEntry<KeyT, ValueT> *new_cce =
                        Emplace(insert_it->second->key_);

                    if (new_cce == nullptr)
                    {
                        // The cc map has reached the maximal capacity.
                        shard_->Enqueue(shard_->LocalCoreId(), &req);
                        return false;
                    }

                    new_cce->payload_ = std::make_unique<ValueT>(*payload);
                    new_cce->payload_status_ = RecordStatus::Normal;

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
                    cce_ptr->payload_ = std::make_unique<ValueT>(*payload);

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
            // 2PL is a blocking protocol. Once a read lock is acquired, no one
            // can possibly change the key. So, this branch is only reachable
            // for OCC/OccRead protocol validating version stability.
            assert(req.Protocol() == CcProtocol::OCC ||
                   req.Protocol() == CcProtocol::OccRead);

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
        else if (req.Protocol() == CcProtocol::OCC ||
                 req.Protocol() == CcProtocol::OccRead)
        {
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
        else if (req.Protocol() == CcProtocol::Locking)
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

            if (gap_ts > 0)
            {
                cc_entry.gap_last_read_ts_ =
                    std::max(cc_entry.gap_last_read_ts_, commit_ts);
            }

            if (key_ts > 0)
            {
                cc_entry.last_read_ts_ =
                    std::max(cc_entry.last_read_ts_, commit_ts);
            }

            ReleaseCceKeyLock(&cc_entry, txn, req.NodeGroupId());
            ReleaseCceGapLock(&cc_entry, txn, req.NodeGroupId());
            hd_res->SetFinished();
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
                    cce = FindEmplace(*look_key);
                }
                else
                {
                    assert(req.KeyBlob() != nullptr);
                    size_t offset = 0;
                    decoded_key.Deserialize(
                        req.KeyBlob()->data(), offset, KeySchema());
                    look_key = &decoded_key;
                }

#ifdef RANGE_PARTITIONED
                cce = Find(*look_key);
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
                            cce = Find(*look_key);
                            if (cce == nullptr)
                            {
                                slice_id.Unpin();

                                hd_res->Value().ts_ = 1;
                                hd_res->Value().rec_status_ =
                                    RecordStatus::Deleted;
                                hd_res->SetFinished();

                                return true;
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
                        cce = FindEmplace(*look_key);
                    }
                }
#else
                cce = FindEmplace(*look_key);

                // The read request accesses a new key not in the cc map. But
                // the cc map is full and cannot allocates a new entry.
                if (cce == nullptr)
                {
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
                    cce->gap_commit_ts_ = 1U;
                    cce->ckpt_ts_.store(1U);
                }
#endif

                req.SetCcePtr(cce);
                cce_addr.SetCce(reinterpret_cast<uint64_t>(cce),
                                ng_term,
                                req.NodeGroupId());

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
                cce->payload_ = std::move(tmp_payload);
                cce->payload_status_ = tmp_payload_status;
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
    }

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
                cce->payload_ = std::make_unique<ValueT>();
                cce->payload_->Deserialize(req.rec_str_->data(), offset);
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

    void AddScanTuple(CcEntry<KeyT, ValueT> *cce,
                      TemplateScanCache<KeyT, ValueT> *typed_cache,
                      ScanType scan_type,
                      uint32_t ng_id,
                      int64_t ng_term,
                      uint64_t read_ts,
                      bool is_read_snapshot,
                      bool is_ckpt_delta = false)
    {
        assert(scan_type != ScanType::ScanUnknow);

        switch (scan_type)
        {
        case ScanType::ScanGap:
        {
            TemplateScanTuple<KeyT, ValueT> *scan_tuple =
                typed_cache->AddScanTuple();
            ScanGap(cce, scan_tuple, ng_id, ng_term);
            break;
        }
        case ScanType::ScanBoth:
            ScanKey(
                cce,
                typed_cache,
                true,
                ng_id,
                ng_term,
                read_ts,
                is_read_snapshot,
                (table_name_.Type() != TableType::Secondary) && is_ckpt_delta);
            break;
        case ScanType::ScanKey:
            ScanKey(
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

        // Before the scan open request is enqueued, the local node's term is
        // obtained and kept in the cc request. This is to avoid getting the
        // node's terms repeatedly in each core, as the scan request is
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

        CcEntry<KeyT, ValueT> *cce = nullptr;

        if (req.CcePtr() != nullptr)
        {
            cce = static_cast<CcEntry<KeyT, ValueT> *>(req.CcePtr());
            const KeyT *continue_look_key = cce->key_;
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

            AddScanTuple(cce,
                         typed_cache,
                         scan_type,
                         ng_id,
                         ng_term,
                         req.ReadTimestamp(),
                         is_read_snapshot);

            std::pair<Iterator, ScanType> start_pair =
                req.direct_ == ScanDirection::Forward
                    ? ForwardScanStart(
                          *continue_look_key,
                          req.inclusive_,
                          (table_name_.Type() != TableType::Secondary) &&
                              req.is_include_floor_cce_)
                    : BackwardScanStart(*continue_look_key, req.inclusive_);
            scan_ccm_it = start_pair.first;
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
            ScanType scan_type = start_pair.second;
            cce = scan_ccm_it->second;

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

            AddScanTuple(cce,
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

                AddScanTuple(cce,
                             typed_cache,
                             ScanType::ScanBoth,
                             ng_id,
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
            for (; scan_ccm_it != neg_inf_it && !typed_cache->Full();
                 --scan_ccm_it)
            {
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

                AddScanTuple(cce,
                             typed_cache,
                             ScanType::ScanBoth,
                             ng_id,
                             ng_term,
                             req.ReadTimestamp(),
                             is_read_snapshot,
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
        CcEntry<KeyT, ValueT> *prior_cce = nullptr;
        if (req.CcePtr() != nullptr)
        {
            prior_cce = static_cast<CcEntry<KeyT, ValueT> *>(req.CcePtr());
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

            AddScanTuple(prior_cce,
                         typed_cache,
                         scan_type,
                         ng_id,
                         ng_term,
                         req.ReadTimestamp(),
                         is_read_snapshot,
                         req.is_ckpt_delta_);
        }
        else
        {
            prior_cce = reinterpret_cast<CcEntry<KeyT, ValueT> *>(
                typed_cache->Last()->cce_addr_.CcePtr());
            typed_cache->Reset();
        }

        if (direction == ScanDirection::Forward)
        {
            CcEntry<KeyT, ValueT> *cce = prior_cce->map_next_;
            while (cce != &pos_inf_ && !typed_cache->Full())
            {
                if (req.is_ckpt_delta_ &&
                    cce->commit_ts_ <=
                        cce->ckpt_ts_.load(std::memory_order_acquire))
                {
                    // If this is a scan for modified records since last
                    // checkpoint, skips those that have been checkpointed.
                    cce = cce->map_next_;
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

                AddScanTuple(cce,
                             typed_cache,
                             ScanType::ScanBoth,
                             ng_id,
                             ng_term,
                             req.ReadTimestamp(),
                             is_read_snapshot,
                             req.is_ckpt_delta_);
                cce = cce->map_next_;
            }
        }
        else
        {
            CcEntry<KeyT, ValueT> *cce = prior_cce->map_prev_;
            while (cce != nullptr && !typed_cache->Full())
            {
                if (cce == &neg_inf_)
                {
                    req.SetCcePtr(cce);
                    req.SetCcePtrScanType(ScanType::ScanGap);

                    // TODO(lzx): handle gap lock
                    AddScanTuple(cce,
                                 typed_cache,
                                 ScanType::ScanGap,
                                 ng_id,
                                 ng_term,
                                 req.ReadTimestamp(),
                                 is_read_snapshot,
                                 req.is_ckpt_delta_);
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

                    AddScanTuple(cce,
                                 typed_cache,
                                 ScanType::ScanBoth,
                                 ng_id,
                                 ng_term,
                                 req.ReadTimestamp(),
                                 is_read_snapshot,
                                 req.is_ckpt_delta_);
                }

                cce = cce->map_prev_;
            }
        }

        req.Result()->SetFinished();
        return true;
    }

    void AddScanTupleMsg(CcEntry<KeyT, ValueT> *cce,
                         std::vector<remote::ScanTuple_msg *> &cache,
                         size_t &tuple_idx,
                         ScanType scan_type,
                         int64_t ng_term,
                         uint64_t read_ts,
                         bool is_read_snapshot,
                         bool is_ckpt_delta)
    {
        assert(scan_type != ScanType::ScanUnknow);

        remote::ScanTuple_msg *tuple = cache.at(tuple_idx++);
        switch (scan_type)
        {
        case ScanType::ScanGap:
            if (!is_ckpt_delta)
            {
                ScanGap(cce, tuple, ng_term);
            }
            break;
        case ScanType::ScanBoth:
            ScanKey(
                cce,
                tuple,
                true,
                ng_term,
                read_ts,
                is_read_snapshot,
                (table_name_.Type() != TableType::Secondary) && is_ckpt_delta);
            break;
        case ScanType::ScanKey:
            ScanKey(
                cce,
                tuple,
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

        std::vector<remote::ScanTuple_msg *> &cache =
            req.scan_caches_.at(shard_->LocalCoreId());
        size_t &tuple_idx = req.scan_caches_idxs_.at(shard_->LocalCoreId());

        Iterator scan_ccm_it;
        CcEntry<KeyT, ValueT> *cce = nullptr;

        if (req.CcePtr(shard_->LocalCoreId()) != nullptr)
        {
            cce = static_cast<CcEntry<KeyT, ValueT> *>(
                req.CcePtr(shard_->LocalCoreId()));
            ScanType scan_type = req.CcePtrScanType(shard_->LocalCoreId());

            req.SetCcePtr(nullptr, shard_->LocalCoreId());
            req.SetCcePtrScanType(ScanType::ScanUnknow, shard_->LocalCoreId());

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

            AddScanTupleMsg(cce,
                            cache,
                            tuple_idx,
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
            }
            else
            {
                // TODO(lzx): handle gap lock
            }

            AddScanTupleMsg(cce,
                            cache,
                            tuple_idx,
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
            for (; scan_ccm_it != pos_inf_it && tuple_idx < cache.size();
                 ++scan_ccm_it)
            {
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

                AddScanTupleMsg(cce,
                                cache,
                                tuple_idx,
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
            for (; scan_ccm_it != neg_inf_it && tuple_idx < cache.size();
                 --scan_ccm_it)
            {
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

                AddScanTupleMsg(cce,
                                cache,
                                tuple_idx,
                                ScanType::ScanBoth,
                                ng_term,
                                req.ReadTimestamp(),
                                is_read_snapshot,
                                req.is_ckpt_delta_);
                scan_ccm_it = Iterator(cce, &neg_inf_, &pos_inf_);
            }
        }
        cache.resize(tuple_idx);

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

        ScanDirection direction = req.direct_;
        CcEntry<KeyT, ValueT> *prior_cce = nullptr;

        if (req.CcePtr() != nullptr)
        {
            prior_cce = static_cast<CcEntry<KeyT, ValueT> *>(req.CcePtr());
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

            AddScanTupleMsg(prior_cce,
                            req.scan_cache_,
                            req.scan_cache_idx_,
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
        }

        if (direction == ScanDirection::Forward)
        {
            CcEntry<KeyT, ValueT> *cce = prior_cce->map_next_;
            while (cce != &pos_inf_ &&
                   req.scan_cache_idx_ < req.scan_cache_.size())
            {
                if (req.is_ckpt_delta_ &&
                    cce->commit_ts_ <=
                        cce->ckpt_ts_.load(std::memory_order_acquire))
                {
                    cce = cce->map_next_;
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

                AddScanTupleMsg(cce,
                                req.scan_cache_,
                                req.scan_cache_idx_,
                                ScanType::ScanBoth,
                                ng_term,
                                req.ReadTimestamp(),
                                is_read_snapshot,
                                req.is_ckpt_delta_);

                cce = cce->map_next_;
            }
        }
        else
        {
            CcEntry<KeyT, ValueT> *cce = prior_cce->map_prev_;
            while (cce != nullptr &&
                   req.scan_cache_idx_ < req.scan_cache_.size())
            {
                if (cce == &neg_inf_)
                {
                    req.SetCcePtr(cce);
                    req.SetCcePtrScanType(ScanType::ScanGap);

                    // TODO(lzx): handle gap lock
                    AddScanTupleMsg(cce,
                                    req.scan_cache_,
                                    req.scan_cache_idx_,
                                    ScanType::ScanGap,
                                    ng_term,
                                    req.ReadTimestamp(),
                                    is_read_snapshot,
                                    req.is_ckpt_delta_);
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

                    AddScanTupleMsg(cce,
                                    req.scan_cache_,
                                    req.scan_cache_idx_,
                                    ScanType::ScanBoth,
                                    ng_term,
                                    req.ReadTimestamp(),
                                    is_read_snapshot,
                                    req.is_ckpt_delta_);
                }
                cce = cce->map_prev_;
            }
        }
        req.scan_cache_.resize(req.scan_cache_idx_);

        req.Result()->SetFinished();
        return true;
    }

    bool Execute(ScanSliceCc &req) override
    {
        int64_t ng_term = Sharder::Instance().LeaderTerm(req.NodeGroupId());
        if (ng_term < 0)
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
        const KeyT *start_key = nullptr;
        if (req.StartKey() != nullptr)
        {
            start_key = static_cast<const KeyT *>(req.StartKey());
        }
        else if (req.Direction() == ScanDirection::Forward)
        {
            start_key = NegativeInfinity<KeyT>::Instance();
        }
        else
        {
            start_key = PositiveInfinity<KeyT>::Instance();
        }

        uint16_t core_id = shard_->LocalCoreId();
        TemplateScanCache<KeyT, ValueT> *scan_cache =
            static_cast<TemplateScanCache<KeyT, ValueT> *>(
                req.GetScanCache(core_id));

        RangeSliceId slice_id;
        if (shard_->core_id_ == 0 && req.SliceId().Slice() == nullptr)
        {
            // The scan slice request is first dispatched to the 1st core, which
            // pins the slice in memory. After the slice is pinned, the same
            // request is dispatched to other cores to scan in parallel.
            RangeSliceOpStatus pin_status;
            slice_id = shard_->PinRangeSlice(table_name_,
                                             req.NodeGroupId(),
                                             KeySchema(),
                                             RecordSchema(),
                                             schema_ts_,
                                             table_schema_->GetKVCatalogInfo(),
                                             req.RangeId(),
                                             *start_key,
                                             req.Inclusive(),
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
                return req.SetFinish();
            }

            req.SetSliceId(slice_id);
        }
        else
        {
            slice_id = req.SliceId();
            assert(slice_id.Slice() != nullptr);
        }

        Iterator scan_ccm_it;
        CcEntry<KeyT, ValueT> *cce = nullptr;
        uint32_t ng_id = req.NodeGroupId();
        int64_t tx_term = req.TxTerm();

        if (req.CcePtr(core_id) != nullptr)
        {
            cce = static_cast<CcEntry<KeyT, ValueT> *>(req.CcePtr(core_id));
            ScanType scan_type = req.BlockedCceScanType(core_id);

            req.SetCcePtr(nullptr, core_id);
            req.SetCceScanType(ScanType::ScanUnknow, core_id);

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
            }

            AddScanTuple(cce,
                         scan_cache,
                         scan_type,
                         ng_id,
                         ng_term,
                         req.ReadTimestamp(),
                         is_read_snapshot);

            scan_ccm_it = Iterator(cce, &neg_inf_, &pos_inf_);
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
                    ? ForwardScanStart(*start_key, req.Inclusive())
                    : BackwardScanStart(*start_key, req.Inclusive());

            scan_ccm_it = start_pair.first;
            ScanType scan_type = start_pair.second;
            cce = scan_ccm_it->second;

            if (scan_type != ScanType::ScanGap)
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

                AddScanTuple(cce,
                             scan_cache,
                             scan_type,
                             ng_id,
                             ng_term,
                             req.ReadTimestamp(),
                             is_read_snapshot);
            }
        }

        if (req.Direction() == ScanDirection::Forward)
        {
            ++scan_ccm_it;
            const StoreSlice *slice = slice_id.Slice();

            // The scan at core 0 sets the scan's end key. By default, the
            // scan's end is the end of the slice. In case keys in the slice are
            // too many to fit into the scan cache, the last key of the scan at
            // core 0 becomes the end key of scans at other cores. In such a
            // case, it is mandatory that all keys smaller than the end key at
            // other cores are returned in this batch. So, scans at other cores
            // may slightly exceed the scan cache's capacity.
            const KeyT *slice_end =
                shard_->core_id_ == 0
                    ? static_cast<const KeyT *>(slice->EndKey())
                    : static_cast<const KeyT *>(
                          hd_res->Value().last_key_.get());

            Iterator pos_inf_it = End();
            cce = scan_ccm_it->second;
            while (scan_ccm_it != pos_inf_it &&
                   (shard_->core_id_ > 0 || !scan_cache->IsFull()) &&
                   (slice_end == nullptr || *cce->key_ < *slice_end))
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

                AddScanTuple(cce,
                             scan_cache,
                             ScanType::ScanBoth,
                             ng_id,
                             ng_term,
                             req.ReadTimestamp(),
                             is_read_snapshot);

                ++scan_ccm_it;
                cce = scan_ccm_it->second;
            }

            // Only sets the result once at the first core.
            if (shard_->core_id_ == 0)
            {
                const KeyT *scan_end_key = nullptr;
                SlicePosition slice_position;

                if (scan_ccm_it != pos_inf_it &&
                    (slice_end == nullptr || *scan_ccm_it->first < *slice_end))
                {
                    // The slice is too large. The scan has not fully scanned
                    // the slice, before reaching the cache's size limit.
                    scan_end_key = &scan_cache->Last()->KeyObj();
                    slice_position = SlicePosition::Middle;
                }
                else
                {
                    // The slice has been fully scanned.
                    if (slice_end == nullptr)
                    {
                        slice_position = SlicePosition::LastSlice;
                    }
                    else
                    {
                        scan_end_key = slice_end;

                        const KeyT *range_end =
                            static_cast<const KeyT *>(slice_id.RangeEndKey());
                        if (range_end != nullptr && *slice_end == *range_end)
                        {
                            slice_position = SlicePosition::LastSliceInRange;
                        }
                        else
                        {
                            slice_position = SlicePosition::Middle;
                        }
                    }
                }

                hd_res->SetValue(RangeScanSliceResult(
                    scan_end_key != nullptr ? scan_end_key->Clone() : nullptr,
                    slice_position));

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
        else
        {
            --scan_ccm_it;
            const StoreSlice *slice = slice_id.Slice();
            const KeyT *slice_begin =
                shard_->core_id_ == 0
                    ? static_cast<const KeyT *>(slice->StartKey())
                    : static_cast<const KeyT *>(
                          hd_res->Value().last_key_.get());

            Iterator neg_inf_it = Begin();
            cce = scan_ccm_it->second;
            while (scan_ccm_it != neg_inf_it &&
                   (shard_->core_id_ > 0 || !scan_cache->IsFull()) &&
                   (slice_begin == nullptr || !(*cce->key_ < *slice_begin)))
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

                AddScanTuple(cce,
                             scan_cache,
                             ScanType::ScanBoth,
                             ng_id,
                             ng_term,
                             req.ReadTimestamp(),
                             is_read_snapshot);

                --scan_ccm_it;
                cce = scan_ccm_it->second;
            }

            if (shard_->core_id_ == 0)
            {
                const KeyT *scan_start_key = nullptr;
                SlicePosition slice_position;

                if (scan_ccm_it != neg_inf_it &&
                    (slice_begin == nullptr ||
                     !(*scan_ccm_it->first < *slice_begin)))
                {
                    // The slice is too large. The scan has not fully
                    // scanned the slice, before reaching the cache's size
                    // limit.
                    scan_start_key = &scan_cache->Last()->KeyObj();
                    slice_position = SlicePosition::Middle;
                }
                else
                {
                    // The slice has been fully scanned.
                    if (slice_begin == nullptr)
                    {
                        slice_position = SlicePosition::FirstSlice;
                    }
                    else
                    {
                        scan_start_key = slice_begin;

                        const KeyT *range_start =
                            static_cast<const KeyT *>(slice_id.RangeStartKey());
                        if (range_start != nullptr &&
                            *slice_begin == *range_start)
                        {
                            slice_position = SlicePosition::FirstSliceInRange;
                        }
                        else
                        {
                            slice_position = SlicePosition::Middle;
                        }
                    }
                }

                hd_res->SetValue(RangeScanSliceResult(
                    scan_start_key != nullptr ? scan_start_key->Clone()
                                              : nullptr,
                    slice_position));

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

        // If all of the entries older than ckpt_ts in this map have been
        // flushed to KV store, skip the scan.
        if (ckpt_ts_.load(std::memory_order_acquire) >= req.ckpt_ts_)
        {
            req.Notify();
            return false;
        }

        LruEntry *lru_cce = req.start_entry_ == nullptr ? neg_inf_.ckpt_next_
                                                        : req.start_entry_;
        CcEntry<KeyT, ValueT> *cce =
            static_cast<CcEntry<KeyT, ValueT> *>(lru_cce);

        // CkptScanCc is running on TxProcessor thread. To avoid blocking
        // other transaction for a long time, we only process CkptScanBatch
        // number of entries in each round.
        size_t cnt = 0;
        int64_t ng_term = Sharder::Instance().LeaderTerm(req.GetNodeGroup());
        if (ng_term < 0)
        {
            req.Notify();
            return false;
        }

        uint64_t recycle_ts = 1U;
        if (shard_->EnableMvcc())
        {
            recycle_ts = shard_->GlobalMinSiTxStartTs();
        }
        while (cnt < CkptScanCc::CkptScanBatch && cce != &pos_inf_)
        {
            if (shard_->EnableMvcc())
            {
                shard_->DecrementMemory(cce->KickOutArchiveRecords(recycle_ts));
            }

            if (cce->commit_ts_ > cce->ckpt_ts_.load(std::memory_order_acquire))
            {
                cce->ExportForCkpt(req.ckpt_vec_,
                                   req.archive_vec_,
                                   req.mv_base_vec_,
                                   req.ckpt_ts_,
                                   recycle_ts,
                                   Type(),
                                   shard_->EnableMvcc());

                if (cce->commit_ts_ <= req.ckpt_ts_)
                {
                    shard_->estimate_ccshard_log_size_ -=
                        cce->estimate_ccentry_log_size_;
                    cce->estimate_ccentry_log_size_ = 0;
                }
            }
            else if (cce->commit_ts_ <=
                     cce->ckpt_ts_.load(std::memory_order_acquire))
            {
                // If the key has been checkpointed, removes it from the
                // checkpoint list.
                ++cnt;
                LruEntry *next = cce->ckpt_next_;
                shard_->DetachCkpt(cce);
                cce = static_cast<CcEntry<KeyT, ValueT> *>(next);
                continue;
            }

            ++cnt;
            cce = static_cast<CcEntry<KeyT, ValueT> *>(cce->ckpt_next_);
        }

        if (cce == &pos_inf_)
        {
            if (shard_->core_id_ == shard_->core_cnt_ - 1)
            {
                std::vector<FlushRecord> &ckpt_vec = req.ckpt_vec_;
                std::sort(ckpt_vec.begin(),
                          ckpt_vec.end(),
                          [](const FlushRecord &lhs, const FlushRecord &rhs)
                          {
                              const CcEntry<KeyT, ValueT> *l_cce =
                                  static_cast<const CcEntry<KeyT, ValueT> *>(
                                      lhs.cce_);
                              const CcEntry<KeyT, ValueT> *r_cce =
                                  static_cast<const CcEntry<KeyT, ValueT> *>(
                                      lhs.cce_);
                              return *l_cce->key_ < *r_cce->key_;
                          });
            }

            req.Notify();
            return false;
        }
        else
        {
            // set the start_entry and put the CkptScanCc request in to
            // CcQueue again.
            req.start_entry_ = cce;
            shard_->Enqueue(&req);
            return false;
        }
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

            CcEntry<KeyT, ValueT> *cce = FindEmplace(key);

            if (cce == nullptr)
            {
                shard_->Enqueue(shard_->LocalCoreId(), &req);
                return false;
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
                    cce->payload_ = std::make_unique<ValueT>();
                    cce->payload_->Deserialize(log_blob.data(), offset);
                    cce->payload_status_ = RecordStatus::Normal;
                }
                else
                {
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
        CcEntry<KeyT, ValueT> *cce = nullptr;
        assert(key_ptr != nullptr);
        if (key_ptr != nullptr)
        {
            // find cc entry
            const KeyT *typed_key_ptr = dynamic_cast<const KeyT *>(key_ptr);
            const KeyT &key = *typed_key_ptr;
            auto lb_it = ccm_.lower_bound(key);
            if (lb_it != ccm_.end() && lb_it->first == key)
            {
                cce = &lb_it->second;
            }

            if (cce != nullptr)
            {
                if (req.WithFlush())
                {
                    std::vector<FlushRecord> tmp_ckpt_vec;

                    std::vector<FlushRecord> tmp_akv_vec;
                    std::vector<LruEntry *> tmp_mv_base_vec;
                    cce->ExportForCkpt(tmp_ckpt_vec,
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

            CcEntry<KeyT, ValueT> *cce = FindEmplace(*key);
            if (cce == nullptr)
            {
                // Memory reaches capacity while bringing a range slice into
                // memory.
                shard_->Enqueue(shard_->LocalCoreId(), &req);
                return false;
            }

            if (cce->commit_ts_ > 1)
            {
                assert(data_item.version_ts_ <= cce->commit_ts_);

                if (cce->delta_size_ == INT32_MAX)
                {
                    if (cce->commit_ts_ == data_item.version_ts_)
                    {
                        cce->delta_size_.store(0, std::memory_order_relaxed);
                    }
                    else if (cce->payload_status_ == RecordStatus::Deleted)
                    {
                        int32_t delta = 0;
                        delta -= cce->key_->Size();
                        delta -= record->Size();
                        cce->delta_size_.store(delta,
                                               std::memory_order_relaxed);
                    }
                    else
                    {
                        cce->delta_size_.store(
                            cce->PayloadSize() - record->Size(),
                            std::memory_order_relaxed);
                    }
                }

                // The cc entry's commit ts is 1 when it is initialized.
                // Commit ts greater than 1 means that the key is already
                // cached in memory.
                continue;
            }

            shard_->DecrementMemory(cce->payload_->MemUsage());
            *cce->payload_ = *record;
            cce->commit_ts_ = data_item.version_ts_;
            cce->payload_status_ = RecordStatus::Normal;
            cce->delta_size_ = 0;

            shard_->mem_usage_ += cce->payload_->MemUsage();
        }

        req.SetFinish();
        return false;
    }

    bool Execute(GetPostCkptSlice &req) override
    {
        RangeSliceId slice_id = req.SliceId();
        std::vector<std::pair<const TxKey *, uint32_t>> &item_vec =
            req.SliceRecordCollection();

        if (shard_->core_id_ == 0)
        {
            uint64_t last_ckpt_ts =
                req.LastCkptTs() > 1 ? req.LastCkptTs() : req.CkptTs();
            RangeSliceOpStatus pin_status =
                slice_id.Range()->PinSlice(table_name_,
                                           slice_id.Slice(),
                                           KeySchema(),
                                           RecordSchema(),
                                           schema_ts_,
                                           last_ckpt_ts,
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

        Iterator it;
        if (slice_id.Slice()->StartKey() == nullptr)
        {
            it = Begin();
            ++it;
        }
        else
        {
            const KeyT *start_key =
                static_cast<const KeyT *>(slice_id.Slice()->StartKey());

            std::pair<Iterator, ScanType> start_pair =
                ForwardScanStart(*start_key, true);
            it = start_pair.first;
            if (start_pair.second == ScanType::ScanGap)
            {
                ++it;
            }
        }

        Iterator end_it;
        if (slice_id.Slice()->EndKey() == nullptr)
        {
            end_it = End();
        }
        else
        {
            const KeyT *end_key =
                static_cast<const KeyT *>(slice_id.Slice()->EndKey());
            std::pair<Iterator, ScanType> end_pair =
                ForwardScanStart(*end_key, true);
            end_it = end_pair.first;
            if (end_pair.second == ScanType::ScanGap)
            {
                ++end_it;
            }
        }

        for (; it != end_it; ++it)
        {
            CcEntry<KeyT, ValueT> *cce = it->second;

            if (cce->commit_ts_ <= 1)
            {
                // This is a new inserted key that the tx has not finished
                // post-processing.
                continue;
            }

            // For keys in the specified slice, calculates their sizes in the
            // data store after this round of checkpointing.
            if (cce->commit_ts_ <= req.CkptTs())
            {
                // The newest version have been flushed to the data store.
                // The record's size in the data store is the size of the
                // newest payload.
                if (cce->payload_status_ != RecordStatus::Deleted)
                {
                    item_vec.emplace_back(
                        cce->key_, cce->key_->Size() + cce->PayloadSize());
                }
            }
            else
            {
                // If delta_size is unset (INT32_MAX) after the slice is pinned,
                // it means that this key does not exist in the data store. So
                // the delta size is set to the size of the key and the record.
                if (cce->delta_size_.load(std::memory_order_relaxed) ==
                    INT32_MAX)
                {
                    cce->delta_size_.store(
                        cce->key_->Size() + cce->PayloadSize(),
                        std::memory_order_relaxed);
                }

                // The key's newest version is greater than the checkpoint
                // timestamp, meaning the record is not flushed in this round of
                // checkpointing. The record's size in the data store is the
                // size of record when it is last flushed. The delta_size_ of
                // cce bookkeeps the accumulated size change since last
                // checkpoint. So, the record's size in the data store is the
                // size of the newest payload subtracting the delta size.
                int32_t record_size = 0;
                if (cce->payload_status_ != RecordStatus::Deleted)
                {
                    record_size += cce->key_->Size();
                    record_size += cce->PayloadSize();
                }
                record_size -= cce->delta_size_.load(std::memory_order_relaxed);

                if (record_size > 0)
                {
                    item_vec.emplace_back(cce->key_, record_size);
                }
                // Else, the record in the last checkpoint must be in the
                // deleted status in the data store.
            }
        }

        if (shard_->core_id_ == shard_->core_cnt_ - 1)
        {
            slice_id.Unpin();
            std::sort(
                item_vec.begin(),
                item_vec.end(),
                [](const std::pair<const TxKey *, uint32_t> &lhs,
                   const std::pair<const TxKey *, uint32_t> &rhs)
                {
                    const KeyT *l_key = static_cast<const KeyT *>(lhs.first);
                    const KeyT *r_key = static_cast<const KeyT *>(rhs.first);
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
        return ccm_.size();
    }

    /**
     * Used for debug to verify the map_link is complete.
     */
    size_t VerifyOrdering() override
    {
        CcEntry<KeyT, ValueT> *cce_prev = nullptr;
        CcEntry<KeyT, ValueT> *cce = neg_inf_.map_next_;
        assert(cce != nullptr);
        size_t cnt = 0;
        while (cce != &pos_inf_)
        {
            assert(cce_prev == nullptr || *cce_prev->key_ < *cce->key_);
            cce_prev = cce;
            cce = cce->map_next_;
            ++cnt;
        }

        return cnt;
    }

    void Clean(LruEntry *remove_entry) override
    {
        CcEntry<KeyT, ValueT> *cc_entry =
            static_cast<CcEntry<KeyT, ValueT> *>(remove_entry);

#ifdef RANGE_PARTITIONED
        bool kick_ret = shard_->local_shards_.KickoutRangeSlice(
            table_name_, cc_ng_id_, *cc_entry->key_);
        if (!kick_ret)
        {
            return;
        }
#endif
        // Don't call DetachLru if entry is not in lru list(Emplaced by
        // force)
        if (remove_entry->lru_prev_ != nullptr &&
            remove_entry->lru_next_ != nullptr)
        {
            CcShard::DetachLru(remove_entry);
        }

        if (remove_entry->ckpt_next_ != nullptr)
        {
            // If the cc entry is in the checkpoint list, removes it from
            // the checkpoint list.
            shard_->DetachCkpt(remove_entry);
        }

        CcEntry<KeyT, ValueT> *prior = cc_entry->map_prev_;
        CcEntry<KeyT, ValueT> *next = cc_entry->map_next_;

        prior->map_next_ = next;
        next->map_prev_ = prior;

        shard_->DecrementMemory(cc_entry->GetCcEntryMemUsage());

        ccm_.erase(*cc_entry->key_);
        ccm_has_full_entries_ = false;
    }

    void Clean() override
    {
        while (neg_inf_.map_next_ != &pos_inf_)
        {
            Clean(neg_inf_.map_next_);
        }
    }

    TableType Type() const override
    {
        return table_name_.Type();
    }

    void TryInsertCkptList(LruEntry *entry) override
    {
        if (entry->ckpt_next_ == nullptr)
        {
            LruEntry *second_last = pos_inf_.ckpt_prev_;
            second_last->ckpt_next_ = entry;
            entry->ckpt_prev_ = second_last;
            entry->ckpt_next_ = &pos_inf_;
            pos_inf_.ckpt_prev_ = entry;
        }
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

    std::pair<std::unique_ptr<TxKey>, size_t> SliceMiddleKey(
        const TxKey *start_key, const TxKey *end_key) const override
    {
        const KeyT *start = start_key == nullptr
                                ? NegativeInfinity<KeyT>::Instance()
                                : static_cast<const KeyT *>(start_key);
        const KeyT *end = end_key == nullptr
                              ? PositiveInfinity<KeyT>::Instance()
                              : static_cast<const KeyT *>(end_key);

        const CcEntry<KeyT, ValueT> *cce_head = FindFloor(*start);
        const CcEntry<KeyT, ValueT> *cce_tail = cce_head;
        size_t half_size = 0;

        while (cce_tail != &pos_inf_ && *cce_tail->key_ < *end &&
               cce_tail->map_next_ != &pos_inf_ &&
               *cce_tail->map_next_->key_ < *end)
        {
            if (cce_head != &neg_inf_)
            {
                half_size += cce_head->key_->Size();
                half_size += cce_head->PayloadSize();
            }

            cce_head = cce_head->map_next_;
            cce_tail = cce_tail->map_next_;
            cce_tail = cce_tail->map_next_;
        }

        size_t est_global_size = half_size * shard_->core_cnt_;

        if (cce_head->key_ == start)
        {
            return {nullptr, est_global_size};
        }
        else
        {
            return {std::make_unique<KeyT>(*cce_head->key_), est_global_size};
        }
    }

protected:
    CcEntry<KeyT, ValueT> *FindEmplace(const KeyT &key)
    {
        bool emplace;
        return FindEmplace(key, emplace);
    }

    CcEntry<KeyT, ValueT> *FindEmplace(const KeyT &key, bool &emplace)
    {
        emplace = false;

        if (&key == neg_inf_.key_)
        {
            return &neg_inf_;
        }
        else if (&key == pos_inf_.key_)
        {
            return &pos_inf_;
        }

        auto lb_it = ccm_.lower_bound(key);
        if (lb_it != ccm_.end() && lb_it->first == key)
        {
            shard_->UpdateLruList(&lb_it->second);
            return &lb_it->second;
        }

        // catalog ccmap bypass shard memory limit. since checkpointer may
        // emplace ccentry into ccmap.
        if (shard_->Full() && !(table_name_.Type() == TableType::Catalog))
        {
            // The shard has reached the maximal capacity. Tries to clean cc
            // entries that have been checkpointed but are not being
            // accessed by active tx's.
            size_t free_cnt = shard_->Clean();
            if (free_cnt == 0)
            {
                return nullptr;
            }
            lb_it = ccm_.lower_bound(key);
        }

        CcEntry<KeyT, ValueT> *new_cce_ptr = nullptr;
        auto em_it = ccm_.emplace_hint(lb_it, std::move(key), this);
        new_cce_ptr = &em_it->second;
        new_cce_ptr->key_ = &em_it->first;

        CcEntry<KeyT, ValueT> *prev_cce = nullptr;
        if (em_it == ccm_.begin())
        {
            prev_cce = &neg_inf_;
        }
        else
        {
            --em_it;
            prev_cce = &em_it->second;
            ++em_it;
        }

        ++em_it;
        CcEntry<KeyT, ValueT> *next_cce =
            em_it == ccm_.end() ? &pos_inf_ : &em_it->second;

        new_cce_ptr->map_prev_ = prev_cce;
        new_cce_ptr->map_next_ = next_cce;
        prev_cce->map_next_ = new_cce_ptr;
        next_cce->map_prev_ = new_cce_ptr;

        shard_->UpdateLruList(new_cce_ptr);

        shard_->mem_usage_ += new_cce_ptr->GetCcEntryMemUsage();

        emplace = true;
        return new_cce_ptr;
    }

    CcEntry<KeyT, ValueT> *Find(const KeyT &key)
    {
        if (&key == NegativeInfinity<KeyT>::Instance())
        {
            return &neg_inf_;
        }

        auto lb_it = ccm_.lower_bound(key);
        if (lb_it != ccm_.end() && lb_it->first == key)
        {
            shard_->UpdateLruList(&lb_it->second);
            return &lb_it->second;
        }
        else
        {
            // The input key does not exist.
            return nullptr;
        }
    }

    const CcEntry<KeyT, ValueT> *FindFloor(const KeyT &key) const
    {
        if (&key == NegativeInfinity<KeyT>::Instance())
        {
            return &neg_inf_;
        }

        auto lb_it = ccm_.lower_bound(key);
        if (lb_it == ccm_.end())
        {
            // The input key is greater than the largest key in the cc map.
            // Returns the last cc entry.
            return &ccm_.rbegin()->second;
        }
        else if (lb_it == ccm_.begin())
        {
            // The inut key is smaller than or equal to the first key in the
            // cc map.
            if (lb_it->first == key)
            {
                return &lb_it->second;
            }
            else
            {
                return &neg_inf_;
            }
        }
        else
        {
            if (!(lb_it->first == key))
            {
                --lb_it;
            }
            return &lb_it->second;
        }
    }

    CcEntry<KeyT, ValueT> *Emplace(const KeyT &key)
    {
        // catalog ccmap bypass shard memory limit. since checkpointer may
        // emplace ccentry into ccmap.
        if (shard_->Full() && !(table_name_.Type() == TableType::Catalog))
        {
            // The shard has reached the maximal capacity. Try cleaning cc
            // entries that has been checkpointed and is not accessed by
            // active tx's.
            size_t free_cnt = shard_->Clean();
            if (free_cnt == 0)
            {
                return nullptr;
            }
        }

        CcEntry<KeyT, ValueT> *new_cce_ptr = nullptr;
        auto em_it = ccm_.try_emplace(KeyT(key), this);
        new_cce_ptr = &em_it.first->second;

        if (em_it.second)
        {
            // If a new cc entry is inserted, updates the ordered
            // double-linked list of cc entries.

            new_cce_ptr->key_ = &em_it.first->first;

            CcEntry<KeyT, ValueT> *prev_cce = nullptr;
            if (em_it.first == ccm_.begin())
            {
                prev_cce = &neg_inf_;
            }
            else
            {
                --em_it.first;
                prev_cce = &em_it.first->second;
                ++em_it.first;
            }

            ++em_it.first;
            CcEntry<KeyT, ValueT> *next_cce =
                em_it.first == ccm_.end() ? &pos_inf_ : &em_it.first->second;

            new_cce_ptr->map_prev_ = prev_cce;
            new_cce_ptr->map_next_ = next_cce;
            prev_cce->map_next_ = new_cce_ptr;
            next_cce->map_prev_ = new_cce_ptr;
        }

        shard_->UpdateLruList(new_cce_ptr);
        shard_->mem_usage_ += new_cce_ptr->GetCcEntryMemUsage();

        return new_cce_ptr;
    }

    /**
     * @brief Finds the greatest cc entry whose key is less than or equal to
     * the input key. If the map is empty, the floor key is negative
     * infinity.
     *
     * @param key The input key
     * @return CcEntry<KeyT, ValueT>* The pointer to the cc entry whose key
     * is the greatest key less than or equal to the input key.
     */
    CcEntry<KeyT, ValueT> *Floor(const KeyT &key)
    {
        auto it = ccm_.lower_bound(key);
        if (it == ccm_.end())
        {
            if (ccm_.empty())
            {
                return &neg_inf_;
            }
            else
            {
                return &ccm_.rbegin()->second;
            }
        }

        if (!(it->first == key))
        {
            if (it == ccm_.begin())
            {
                return &neg_inf_;
            }

            --it;
        }

        return &it->second;
    }

    class Iterator
    {
        using iterator_category = std::bidirectional_iterator_tag;
        using difference_type = std::ptrdiff_t;
        using value_type = std::pair<const KeyT *, CcEntry<KeyT, ValueT> *>;
        using pointer = value_type *;    // or also value_type*
        using reference = value_type &;  // or also value_type&

    public:
        Iterator() = default;

        Iterator(
            typename std::map<KeyT, CcEntry<KeyT, ValueT>>::iterator &map_it,
            CcEntry<KeyT, ValueT> *neg_inf_cce)
            : internal_it_(map_it), neg_inf_cce_(neg_inf_cce)
        {
            UpdateCurrent();
        }

        Iterator(CcEntry<KeyT, ValueT> *cce,
                 CcEntry<KeyT, ValueT> *neg_inf_cce,
                 CcEntry<KeyT, ValueT> *pos_inf_cce = nullptr)
            : neg_inf_cce_(neg_inf_cce)
        {
            std::map<KeyT, CcEntry<KeyT, ValueT>> &internal_map =
                static_cast<TemplateCcMap<KeyT, ValueT> *>(cce->parent_map_)
                    ->ccm_;

            if (cce == neg_inf_cce)
            {
                current_.first = NegativeInfinity<KeyT>::Instance();
                current_.second = neg_inf_cce_;
                internal_it_ = internal_map.begin();
            }
            else if (cce == pos_inf_cce)
            {
                current_.first = PositiveInfinity<KeyT>::Instance();
                current_.second = nullptr;
                internal_it_ = internal_map.end();
            }
            else
            {
                internal_it_ = internal_map.find(*cce->key_);
                assert(internal_it_ != internal_map.end());

                UpdateCurrent();
            }
        }

        Iterator(Iterator &&rhs)
            : internal_it_(rhs.internal_it_),
              current_(rhs.current_),
              neg_inf_cce_(rhs.neg_inf_cce_)
        {
        }

        Iterator(const Iterator &rhs)
            : internal_it_(rhs.internal_it_),
              current_(rhs.current_),
              neg_inf_cce_(rhs.neg_inf_cce_)
        {
        }

        Iterator &operator=(const Iterator &rhs)
        {
            internal_it_ = rhs.internal_it_;
            current_ = rhs.current_;
            neg_inf_cce_ = rhs.neg_inf_cce_;
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
                // iterator to the first entry in the map, if the map is not
                // empty.

                std::map<KeyT, CcEntry<KeyT, ValueT>> &internal_map =
                    static_cast<TemplateCcMap<KeyT, ValueT> *>(
                        neg_inf_cce_->parent_map_)
                        ->ccm_;
                internal_it_ = internal_map.begin();

                if (internal_it_ != internal_map.end())
                {
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
                std::map<KeyT, CcEntry<KeyT, ValueT>> &internal_map =
                    static_cast<TemplateCcMap<KeyT, ValueT> *>(
                        current_.second->parent_map_)
                        ->ccm_;
                ++internal_it_;

                // The next entry points to positive infinity.
                if (internal_it_ == internal_map.end())
                {
                    current_.first = PositiveInfinity<KeyT>::Instance();
                    current_.second = nullptr;
                }
                else
                {
                    UpdateCurrent();
                }
            }
            // If the current points to positive infinity, keeps the
            // iterator unchanged.

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
                std::map<KeyT, CcEntry<KeyT, ValueT>> &internal_map =
                    static_cast<TemplateCcMap<KeyT, ValueT> *>(
                        neg_inf_cce_->parent_map_)
                        ->ccm_;
                internal_it_ = internal_map.end();

                if (internal_it_ != internal_map.begin())
                {
                    --internal_it_;
                    UpdateCurrent();
                }
                else
                {
                    // The map is empty. The prior entry of positive
                    // infinity is negative infinity.
                    current_.first = NegativeInfinity<KeyT>::Instance();
                    current_.second = neg_inf_cce_;
                }
            }
            else if (current_.first != NegativeInfinity<KeyT>::Instance())
            {
                std::map<KeyT, CcEntry<KeyT, ValueT>> &internal_map =
                    static_cast<TemplateCcMap<KeyT, ValueT> *>(
                        current_.second->parent_map_)
                        ->ccm_;

                // If the current points to the beginning of the map, the
                // prior entry is negative infinity.
                if (internal_it_ == internal_map.begin())
                {
                    current_.first = NegativeInfinity<KeyT>::Instance();
                    current_.second = neg_inf_cce_;
                }
                else
                {
                    --internal_it_;
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
            current_.first = &internal_it_->first;
            current_.second = &internal_it_->second;
        }

        typename std::map<KeyT, CcEntry<KeyT, ValueT>>::iterator internal_it_;
        std::pair<const KeyT *, CcEntry<KeyT, ValueT> *> current_{nullptr,
                                                                  nullptr};
        CcEntry<KeyT, ValueT> *neg_inf_cce_{nullptr};
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
     * Whether ScanGap or ScanBoth depends on whether this is range_cc_map scan.
     * For template_cc_map, start from it's gap;
     * for range_cc_map, start from it's key and gap.
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
     * @brief Searches the start cc entry of a forward scan.
     *
     * @param key Search key
     * @param inclusive Whether or not the start key is included in the scan
     * @param is_include_floor_cce This param is used only by range_cc_map scan,
     * and is always true. Range scan searches for the floor of the search key
     * and returns both its key and gap.
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

        if (inclusive || is_include_floor_cce)  // >= key or range_cc_map scan,
                                                // search for lower_bound(key)
        {
            auto lb_it = ccm_.lower_bound(key);
            if (lb_it == ccm_.end() || !(lb_it->first == key))
            {
                // for template_cc_map, start from previous entry's gap;
                // for range_cc_map, start from previous entry's key and gap
                if (lb_it == ccm_.begin())
                {
                    return MakeForwardScanPair(Begin(), is_include_floor_cce);
                }
                else
                {
                    --lb_it;
                    return MakeForwardScanPair(Iterator(lb_it, &neg_inf_),
                                               is_include_floor_cce);
                }
            }
            else
            {
                // lb_it's key is exactly equal to key, start from lb_it and its
                // gap, but not including previous key gap
                return std::make_pair(Iterator(lb_it, &neg_inf_),
                                      ScanType::ScanBoth);
            }
        }
        else  // > key, search for the entry before upper_bound(key)
        {
            auto ub_it = ccm_.upper_bound(key);
            // start from the gap of previous entry of ub_it
            if (ub_it == ccm_.begin())
            {
                return std::make_pair(Begin(), ScanType::ScanGap);
            }
            else
            {
                ub_it--;
                return std::make_pair(Iterator(ub_it, &neg_inf_),
                                      ScanType::ScanGap);
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
     * starting from the start cc entry and whether the scan includes the start
     * cc entry's key or gap or both.
     */
    std::pair<Iterator, ScanType> BackwardScanStart(const KeyT &key,
                                                    bool inclusive)
    {
        if (key.Type() == KeyType::PositiveInf)
        {
            auto start_it = End();
            --start_it;
            return std::make_pair(start_it, ScanType::ScanBoth);
        }

        if (inclusive)  // <= key, search for the entry before upper_bound(key)
        {
            auto ub_it = ccm_.upper_bound(key);
            if (ub_it == ccm_.begin())
            {
                // map empty or every key in ccm_ is greater than key, return
                // neg_inf_'s gap
                return std::make_pair(Begin(), ScanType::ScanGap);
            }
            else
            {
                ub_it--;
                // now, ub_it is the greatest entry equal to or less than key

                if (!(ub_it->first == key))
                {
                    // key not equal should include the
                    // gap
                    return std::make_pair(Iterator(ub_it, &neg_inf_),
                                          ScanType::ScanBoth);
                }
                else
                {
                    // exactly full key match, not include the gap
                    return std::make_pair(Iterator(ub_it, &neg_inf_),
                                          ScanType::ScanKey);
                }
            }
        }
        else  // < key, search for the entry before lower_bound(key)
        {
            auto lb_it = ccm_.lower_bound(key);
            if (lb_it == ccm_.begin())
            {
                // map empty or every key in ccm_ is greater than or equal to
                // key, return neg_inf_ gap
                return std::make_pair(Begin(), ScanType::ScanGap);
            }
            else
            {
                // starting from the gap and key of entry before lb_it
                lb_it--;
                return std::make_pair(Iterator(lb_it, &neg_inf_),
                                      ScanType::ScanBoth);
            }
        }
    }

    void ScanKey(CcEntry<KeyT, ValueT> *cce,
                 TemplateScanCache<KeyT, ValueT> *typed_cache,
                 bool include_gap,
                 uint32_t ng_id,
                 int64_t ng_term,
                 uint64_t read_ts,
                 bool is_read_snapshot,
                 bool is_ckpt_delta = false)
    {
        TemplateScanTuple<KeyT, ValueT> *tuple = nullptr;
        uint32_t tuple_size = 0;

        if (is_read_snapshot)
        {
            VersionResultRecord<ValueT> v_rec;
            cce->MvccGet(read_ts, Type(), v_rec);

#ifdef RANGE_PARTITIONED
            if (v_rec.payload_status_ == RecordStatus::Normal)
            {
                tuple = typed_cache->AddScanTuple();
            }
#else
            tuple = typed_cache->AddScanTuple();
#endif
            if (tuple == nullptr)
            {
                return;
            }
            tuple->KeyObj().Copy(*cce->key_);
            tuple_size = cce->key_->Size();

            if (v_rec.payload_status_ == RecordStatus::Normal ||
                (is_ckpt_delta &&
                 v_rec.payload_status_ == RecordStatus::Deleted))
            {
                tuple->RecordObj() = *v_rec.payload_ptr_;
                tuple_size += v_rec.payload_ptr_->Size();
            }
            tuple->key_ts_ = v_rec.commit_ts_;
            tuple->rec_status_ = v_rec.payload_status_;
        }
        else
        {
#ifdef RANGE_PARTITIONED
            if (cce->payload_status_ == RecordStatus::Normal)
            {
                tuple = typed_cache->AddScanTuple();
            }
#else
            tuple = typed_cache->AddScanTuple();
#endif

            if (tuple == nullptr)
            {
                return;
            }
            tuple->KeyObj().Copy(*cce->key_);
            tuple_size = cce->key_->Size();

            if (cce->payload_status_ == RecordStatus::Normal ||
                (is_ckpt_delta &&
                 cce->payload_status_ == RecordStatus::Deleted))
            {
                tuple->RecordObj() = *(cce->payload_);
                tuple_size += cce->payload_->Size();
            }
            tuple->rec_status_ = cce->payload_status_;
            tuple->key_ts_ = cce->commit_ts_;
        }

        tuple->gap_ts_ = include_gap ? cce->gap_commit_ts_ : 0;
        tuple->cce_addr_.SetCce(
            reinterpret_cast<uint64_t>(cce), ng_term, ng_id);

        typed_cache->AddScanTupleSize(tuple_size);
    }

    void ScanKey(CcEntry<KeyT, ValueT> *cce,
                 remote::ScanTuple_msg *tuple,
                 bool include_gap,
                 int64_t ng_term,
                 uint64_t read_ts,
                 bool is_read_snapshot,
                 bool is_ckpt_delta = false) const
    {
        tuple->clear_key();
        cce->key_->Serialize(*tuple->mutable_key());

        if (is_read_snapshot)
        {
            VersionResultRecord<ValueT> v_rec;
            cce->MvccGet(read_ts, Type(), v_rec);
            if (v_rec.payload_status_ == RecordStatus::Normal ||
                (is_ckpt_delta &&
                 v_rec.payload_status_ == RecordStatus::Deleted))
            {
                tuple->clear_record();
                v_rec.payload_ptr_->Serialize(*tuple->mutable_record());
            }
            tuple->set_rec_status(remote::ToRemoteType::ConvertRecordStatus(
                v_rec.payload_status_));
            tuple->set_key_ts(v_rec.commit_ts_);
        }
        else
        {
            if (cce->payload_status_ == RecordStatus::Normal ||
                (is_ckpt_delta &&
                 cce->payload_status_ == RecordStatus::Deleted))
            {
                tuple->clear_record();
                cce->payload_->Serialize(*tuple->mutable_record());
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
    }

    void ScanGap(CcEntry<KeyT, ValueT> *cce,
                 TemplateScanTuple<KeyT, ValueT> *tuple,
                 uint32_t ng_id,
                 int64_t ng_term) const
    {
        tuple->key_ts_ = 0;
        tuple->gap_ts_ = cce->gap_commit_ts_;
        tuple->cce_addr_.SetCce(
            reinterpret_cast<uint64_t>(cce), ng_term, ng_id);
    }

    void ScanGap(CcEntry<KeyT, ValueT> *cce,
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

    std::map<KeyT, CcEntry<KeyT, ValueT>> ccm_;
    CcEntry<KeyT, ValueT> neg_inf_, pos_inf_;

    // When maintain_statistics_ is true, shard_profile_ is valid.
    bool maintain_statistics_;

    // shard_profile_ points to TypedShardProfile in TableSchema.
    TypedShardProfile<KeyT> *shard_profile_;
};
}  // namespace txservice
