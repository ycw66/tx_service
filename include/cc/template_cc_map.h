#pragma once

#include <algorithm>  // std::max
#include <map>
#include <string>
#include <unordered_set>
#include <utility>  // std::pair
#include <vector>

#include "cc_entry.h"
#include "cc_map.h"
#include "cc_protocol.h"
#include "cc_request.h"
#include "cc_shard.h"
#include "fault/fault_inject.h"
#include "proto/cc_request.pb.h"
#include "remote/remote_cc_handler.h"  //RemoteCcHandler
#include "remote/remote_cc_request.h"
#include "remote/remote_type.h"
#include "sharder.h"
#include "store/data_store_handler.h"
#include "tx_execution.h"
#include "tx_id.h"
#include "tx_key.h"
#include "tx_trace.h"
#include "type.h"

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
                  const TableName &table_name,
                  uint64_t schema_ts,
                  const TableSchema *table_schema = nullptr,
                  bool ccm_has_full_entries = false,
                  bool is_catalog_cc_map = false)
        : CcMap(
              shard, table_name, table_schema, schema_ts, ccm_has_full_entries),
          ccm_(),
          neg_inf_(this),
          pos_inf_(this),
          is_catalog_cc_map_(is_catalog_cc_map)
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
            [this]() -> string
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
            hd_res->SetError(-1);
            return true;
        }

        LockType acquired_lock;
        LockOpStatus lock_op_status;
        if (req.CcePtr() != nullptr)
        {
            // The request was blocked before and is now unblocked.
            resume = true;
            cce_ptr = static_cast<CcEntry<KeyT, ValueT> *>(req.CcePtr());

            acquired_lock = LockHandleForResumedRequest(
                &req, req.TxTerm(), cce_ptr, cce_ptr->payload_status_);
            lock_op_status = LockOpStatus::Successful;
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
                        hd_res->SetError(1);
                        return true;
                    }
                }

                req.SetCcePtr(cce_ptr);
            }
            else
            {
                cce_ptr = FindEmplace(*target_key, req.Ts());

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
                    hd_res->SetError(1);
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
                std::tie(acquired_lock, lock_op_status) =
                    AcquireCceKeyLock(&cc_entry,
                                      cc_entry.payload_status_,
                                      &req,
                                      req.NodeGroupId(),
                                      ng_term,
                                      req.TxTerm(),
                                      CcOperation::Write,
                                      req.Isolation(),
                                      req.Protocol());
            }

            if (lock_op_status == LockOpStatus::Successful)
            {
                assert(acquired_lock == LockType::WriteLock);
                // for mvcc
                uint64_t lock_ts = std::max(req.Ts(), shard_->Now());
                cc_entry.wlock_ts_ = lock_ts;

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
            else if (lock_op_status == LockOpStatus::Failed)
            {
                // lock confilct: back off and retry.
                req.Result()->SetError(1);
                return true;
            }
            else
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
                req.Result()->SetError(-1);
                return true;
            }
        });

        if (!Sharder::Instance().CheckLeaderTerm(cce_addr.NodeGroupId(),
                                                 cce_addr.Term()))
        {
            req.Result()->SetError(-1);
            return true;
        }

        const ValueT *commit_val = static_cast<const ValueT *>(req.Payload());
        TxNumber txn = req.Txn();
        uint64_t commit_ts = req.CommitTs();
        const std::string *payload_str = req.PayloadStr();
        bool is_del = req.GetDmlOperation() == DmlOperation::Delete;

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
                CcEntry<KeyT, ValueT> *new_cce =
                    Emplace(insert_entry.key_, commit_ts);

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
                new_cce->parent_map_->shard_->UpdateEstimateLogSize(
                    new_cce, key_size, payload_size);
            }

            req.Result()->SetFinished();
            // The insert places a write lock on the prior cc entry's gap.
            ReleaseCceGapLock(&prior_cce, txn);
            return true;
        }
        else
        {
            // upsert and delete branch.
            assert(cce_addr.CcePtr() != 0);

            CcEntry<KeyT, ValueT> &cce =
                *reinterpret_cast<CcEntry<KeyT, ValueT> *>(cce_addr.CcePtr());

            if (cce.key_lock_.HasWriteLock() &&
                cce.key_lock_.WriteLockTx() != txn)
            {
                req.Result()->SetFinished();
                return true;
            }

            if (commit_ts > 0)
            {
                // for mvcc
                if (shard_->EnableMvcc())
                {
                    uint64_t recycle_ts = shard_->GlobalMinSiTxStartTs();
                    cce.KickOutArchiveRecords(recycle_ts);
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
                cce.parent_map_->shard_->UpdateEstimateLogSize(
                    &cce, key_size, payload_size);

                cce.payload_status_ =
                    is_del ? RecordStatus::Deleted : RecordStatus::Normal;
                TryInsertCkptList(&cce);

                DLOG_IF(INFO, TRACE_OCC_ERR)
                    << "PostWriteCc, txn:" << txn << " ,cce: " << &cce
                    << " ,commit_ts: " << commit_ts;
            }

            req.Result()->SetFinished();
            ReleaseCceKeyLock(&cce, txn);
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
            hd_res->SetError(-1);
            return true;
        });

        uint32_t ng_id = req.NodeGroupId();
        int64_t ng_term = Sharder::Instance().LeaderTerm(ng_id);
        if (ng_term < 0)
        {
            hd_res->SetError(-1);
            return true;
        }

        uint16_t tx_core_id = ((req.Txn() >> 32L) & 0x3FF) % shard_->core_cnt_;

        LockType acquired_lock;
        LockOpStatus lock_op_status;
        if (req.CcePtr() != nullptr)
        {
            // The request was blocked before and is now unblocked.
            resume = true;
            cce_ptr = static_cast<CcEntry<KeyT, ValueT> *>(req.CcePtr());
            acquired_lock = LockHandleForResumedRequest(
                &req, req.TxTerm(), cce_ptr, cce_ptr->payload_status_);
            lock_op_status = LockOpStatus::Successful;
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
                const std::string *key_str = req.KeyStr();
                assert(key_str != nullptr);
                std::unique_ptr<KeyT> decoded_key = std::make_unique<KeyT>();
                size_t offset = 0;
                decoded_key->Deserialize(key_str->data(), offset, KeySchema());
                target_key = decoded_key.get();
                req.SetDecodedKey(std::move(decoded_key));
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
                        hd_res->SetError(1);
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
                cce_ptr = FindEmplace(*target_key, 0);

                if (cce_ptr == nullptr)
                {
                    // The acquire request needs a new cc entry but the cc map
                    // has reached the maximal capacity. Blocks the request by
                    // putting it back to the cc request queue.
                    shard_->Enqueue(shard_->LocalCoreId(), &req);
                    return false;
                }

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
                    hd_res->SetError(1);
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
                std::tie(acquired_lock, lock_op_status) =
                    AcquireCceKeyLock(&cc_entry,
                                      cc_entry.payload_status_,
                                      &req,
                                      req.NodeGroupId(),
                                      ng_term,
                                      tx_term,
                                      cc_op,
                                      iso_lvl,
                                      cc_proto);
            }

            switch (lock_op_status)
            {
            case LockOpStatus::Successful:
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

                // AcquireAllCc request is executed at all shards consecutively.
                // The request is set to be finished after executed at the last
                // shard. At other shards, after the request is executed,
                // MoveRequest() moves the request to the next shard to iterate
                // through all shards.
                if (shard_->core_id_ == shard_->core_cnt_ - 1)
                {
                    acquire_all_result.last_vali_ts_ = std::max(
                        acquire_all_result.last_vali_ts_, shard_->Now());
                    acquire_all_result.commit_ts_ = cc_entry.commit_ts_;
                    acquire_all_result.node_term_ = ng_term;

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
            case LockOpStatus::Failed:
            {
                // lock confilct: back off and retry.
                req.Result()->SetError(1);
                return true;
            }
            case LockOpStatus::Blocked:
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
            req.Result()->SetError(-1);
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
            const std::string *key_str = req.KeyStr();
            assert(key_str != nullptr);
            std::unique_ptr<KeyT> decoded_key = std::make_unique<KeyT>();
            size_t offset = 0;
            decoded_key->Deserialize(key_str->data(), offset, KeySchema());
            target_key = decoded_key.get();
            req.SetDecodedKey(std::move(decoded_key));
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
            const std::string *payload_str = req.PayloadStr();
            assert(payload_str != nullptr);
            std::unique_ptr<ValueT> decoded_rec = std::make_unique<ValueT>();
            size_t offset = 0;
            decoded_rec->Deserialize(payload_str->data(), offset);
            payload = decoded_rec.get();
            req.SetDecodedPayload(std::move(decoded_rec));
        }

        CcEntry<KeyT, ValueT> *cce_ptr = nullptr;
        if (req.DmlOp() == DmlOperation::Insert)
        {
            cce_ptr = Floor(*target_key);
        }
        else
        {
            cce_ptr = FindEmplace(*target_key, 0);
        }

        if (cce_ptr == nullptr)
        {
            shard_->Enqueue(shard_->LocalCoreId(), &req);
            return false;
        }

        TxNumber txn = req.Txn();
        uint64_t commit_ts = req.CommitTs();

        if (req.DmlOp() == DmlOperation::Insert &&
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
                        Emplace(insert_it->second->key_, commit_ts);

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
                ReleaseCceGapLock(cce_ptr, txn);
            }

            if (shard_->core_id_ == shard_->core_cnt_ - 1)
            {
                req.Result()->SetFinished();
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
            LockType lk_type = CceKeyLockTypeHeldByTx(cce_ptr, txn);

            if (lk_type == LockType::WriteLock ||
                lk_type == LockType::WriteIntent)
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
                            req.DmlOp() == DmlOperation::Delete
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
                    ReleaseCceKeyLock(cce_ptr, txn);
                }
                else if (req.CommitType() == PostWriteType::PrepareCommit)
                {
                    // downgrade write lock to write intent
                    if (lk_type == LockType::WriteLock)
                    {
                        DowngradeCceKeyWriteLock(cce_ptr, txn);
                    }
                }
            }

            if (shard_->core_id_ == shard_->core_cnt_ - 1)
            {
                req.Result()->SetFinished();
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
                    hd_res->SetError(-1);
                    return true;
                }
            });

        const CcEntryAddr &cce_addr = *req.CceAddr();
        if (!Sharder::Instance().CheckLeaderTerm(cce_addr.NodeGroupId(),
                                                 cce_addr.Term()))
        {
            LOG(INFO) << "PostReadCc, node_group(#" << cce_addr.NodeGroupId()
                      << ") term < 0, tx:" << req.Txn() << " ,cce: "
                      << reinterpret_cast<void *>(cce_addr.CcePtr());
            hd_res->SetError(-1);
            return true;
        }

        uint64_t key_ts = req.KeyTs();
        uint64_t gap_ts = req.GapTs();
        uint64_t commit_ts = req.CommitTs();
        TxNumber txn = req.Txn();

        CcEntry<KeyT, ValueT> &cc_entry =
            *reinterpret_cast<CcEntry<KeyT, ValueT> *>(cce_addr.CcePtr());

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
            hd_res->SetError(1);  // broken repeatable read, set error.
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

                if (cc_entry.key_lock_.HasWriteLock() &&
                    txn != cc_entry.key_lock_.HasWriteLock())
                {
                    int64_t ng_term =
                        Sharder::Instance().LeaderTerm(req.NodeGroupId());
                    shard_->CheckRecoverTx(cc_entry.key_lock_.WriteLockTx(),
                                           req.NodeGroupId(),
                                           ng_term);
                    conflicting_txs.AddConflictingTx(
                        cc_entry.key_lock_.WriteLockTx());

                    DLOG_IF(INFO, TRACE_OCC_ERR)
                        << "PostReadCc, occ_err, txn:" << txn
                        << " ,cce: " << &cc_entry << " ,key conflict tx: "
                        << cc_entry.key_lock_.WriteLockTx();
                }
            }

            hd_res->SetFinished();
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

            // For 2PL, releasing read locks may spend extra cycles to
            // process unblocked requests. Sets the handler's finish signal
            // before releasing read locks, so that if blocking requests come
            // from a different core or a remote node, their tx's can move
            // forward immediately.

            hd_res->SetFinished();
        }

        // ReadCc may use different lock type when acquiring the lock, for
        // example, select for update would acquire write intent. As a
        // result, we should also release the corresponding lock/intent as
        // well.
        ReleaseCceKeyLock(&cc_entry, txn);
        ReleaseCceGapLock(&cc_entry, txn);

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
                hd_res->SetError(-1);
                return true;
            }
        });

        uint32_t ng_id = req.NodeGroupId();
        int64_t ng_term = Sharder::Instance().LeaderTerm(ng_id);
        if (ng_term < 0)
        {
            LOG(INFO) << "ReadCc, node_group(#" << ng_id
                      << ") term < 0, tx:" << req.Txn();
            hd_res->SetError(-1);
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
            LockType acquired_lock;
            LockOpStatus lock_op_status;

            if (req.CcePtr() != nullptr)
            {
                // The request was blocked before. This is execution resumption
                // after the request is unblocked. The read lock/intention must
                // have been acquired.
                cce = static_cast<CcEntry<KeyT, ValueT> *>(req.CcePtr());

                acquired_lock = LockHandleForResumedRequest(
                    &req, req.TxTerm(), cce, cce->payload_status_);
                lock_op_status = LockOpStatus::Successful;
            }
            else
            {
                if (req.Key() != nullptr)
                {
                    const KeyT *look_key = static_cast<const KeyT *>(req.Key());
                    cce = FindEmplace(*look_key, req.ReadTimestamp());
                }
                else
                {
                    assert(req.KeyBlob() != nullptr);
                    KeyT decoded_key;
                    size_t offset = 0;
                    decoded_key.Deserialize(
                        req.KeyBlob()->data(), offset, KeySchema());
                    cce = FindEmplace(decoded_key, req.ReadTimestamp());
                }

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
                    cce->ckpt_ts_.store(2U);
                }

                req.SetCcePtr(cce);
                cce_addr.SetCce(reinterpret_cast<uint64_t>(cce),
                                ng_term,
                                req.NodeGroupId());

                // Try to acquire lock
                std::tie(acquired_lock, lock_op_status) =
                    AcquireCceKeyLock(cce,
                                      cce->payload_status_,
                                      &req,
                                      ng_id,
                                      ng_term,
                                      tx_term,
                                      cc_op,
                                      iso_lvl,
                                      cc_proto);
            }

            // After acquiring lock
            switch (lock_op_status)
            {
            case LockOpStatus::Successful:
            {
                hd_res->Value().lock_type_ = acquired_lock;
                break;
            }
            case LockOpStatus::Failed:
            {
                // lock confilct: back off and retry.
                req.Result()->SetError(1);
                return true;
            }
            case LockOpStatus::Blocked:
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
            }  //-- end: switch

        }  //-- end: read insde
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
                uint64_t tmp_ts = 1U;
                cce->ckpt_ts_.compare_exchange_strong(tmp_ts,
                                                      req.ReadTimestamp() - 1);
            }
            else if (shard_->EnableMvcc() && cce->ckpt_ts_ == 1U &&
                     cce->commit_ts_ > req.ReadTimestamp())
            {
                // Trying to insert the record to backfill into archives is
                // needed, because the entry may be created when executing
                // "ReplayLogCc".
                cce->AddArchiveRecord(std::move(tmp_payload),
                                      tmp_payload_status,
                                      req.ReadTimestamp());
                // set "ckpt_ts_" to identify the entry is refilled
                uint64_t tmp_ts = 1U;
                cce->ckpt_ts_.compare_exchange_strong(tmp_ts,
                                                      req.ReadTimestamp() - 1);
            }

            // Refill mvcc archives
            if (shard_->EnableMvcc() &&
                (req.Type() == ReadType::OutsideNormal ||
                 req.Type() == ReadType::OutsideDeleted) &&
                req.ArchivesPtr() != nullptr && req.ArchivesPtr()->size() > 0)
            {
                cce->AddArchiveRecords(*req.ArchivesPtr());
            }
        }

        // If 'req.IsForWrite()' is true, should read latest version;
        if (is_read_snapshot)
        {
            assert(req.Type() == ReadType::Inside);

            VersionResultRecord<ValueT> v_rec;
            bool res = cce->MvccGet(req.ReadTimestamp(), v_rec, Type());
            if (res)  // Finds a visible version.
            {
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
            }
            else
            {
                LOG(INFO) << "ReadCc, tx(" << req.Txn()
                          << ") read snapshot version error";
                hd_res->SetError(1);  // Not Found, return error.
            }
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
                cce->key_lock_.InsertBlockingQueue(&req, req.TxTerm());

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
            uint64_t tmp_ts = 1U;
            cce->ckpt_ts_.compare_exchange_strong(tmp_ts, req.CommitTs() - 1);
        }
        else if (shard_->EnableMvcc() && cce->ckpt_ts_ == 1U &&
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
            cce->AddArchiveRecord(
                std::move(tmp_payload), req.RecordStatus(), req.CommitTs());

            // set "ckpt_ts_" to identify the entry is refilled
            uint64_t tmp_ts = 1U;
            cce->ckpt_ts_.compare_exchange_strong(tmp_ts, req.CommitTs() - 1);
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
                cce->AddArchiveRecords(archives);
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

        TemplateScanTuple<KeyT, ValueT> *scan_tuple =
            typed_cache->AddScanTuple();

        switch (scan_type)
        {
        case ScanType::ScanGap:
            ScanGap(cce, scan_tuple, ng_id, ng_term);
            break;
        case ScanType::ScanBoth:
            ScanKey(
                cce,
                scan_tuple,
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
                scan_tuple,
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
            req.Result()->SetError(-1);
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
            DLOG(INFO) << "This is for debug, Reacquire lock for cce: " << cce;
            const KeyT *continue_look_key = cce->key_;
            ScanType scan_type = req.CcePtrScanType();

            req.SetCcePtr(nullptr);
            req.SetCcePtrScanType(ScanType::ScanUnknow);

            // Lock has been acquired, UpsertLockHoldingTx
            LockHandleForResumedRequest(
                &req, tx_term, cce, cce->payload_status_);

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
                                                   cc_proto);
                if (lock_pair.second == LockOpStatus::Failed)
                {
                    // lock confilct: back off and retry.
                    req.Result()->SetError(1);
                    return true;
                }
                else if (lock_pair.second == LockOpStatus::Blocked)
                {
                    // Lock fail should stop the execution of current
                    // CC request since it's already in blocking queue.
                    return false;
                }
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
                                                   cc_proto);
                if (lock_pair.second == LockOpStatus::Failed)
                {
                    // lock confilct: back off and retry.
                    req.Result()->SetError(1);
                    return true;
                }
                else if (lock_pair.second == LockOpStatus::Blocked)
                {
                    // Lock fail should stop the execution of current
                    // CC request since it's already in blocking queue.
                    return false;
                }

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
                                                   cc_proto);
                if (lock_pair.second == LockOpStatus::Failed)
                {
                    // lock confilct: back off and retry.
                    req.Result()->SetError(1);
                    return true;
                }
                else if (lock_pair.second == LockOpStatus::Blocked)
                {
                    // Lock fail should stop the execution of current
                    // CC request since it's already in blocking queue.
                    return false;
                }

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
            req.Result()->SetError(-1);
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

            // Lock has been acquired, UpsertLockHoldingTx
            LockHandleForResumedRequest(
                &req, tx_term, prior_cce, prior_cce->payload_status_);

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
                                                   cc_proto);
                if (lock_pair.second == LockOpStatus::Failed)
                {
                    // lock confilct: back off and retry.
                    req.Result()->SetError(1);
                    return true;
                }
                else if (lock_pair.second == LockOpStatus::Blocked)
                {
                    // Lock fail should stop the execution of current
                    // CC request since it's already in blocking queue.
                    return false;
                }

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
                                                       cc_proto);
                    if (lock_pair.second == LockOpStatus::Failed)
                    {
                        // lock confilct: back off and retry.
                        req.Result()->SetError(1);
                        return true;
                    }
                    else if (lock_pair.second == LockOpStatus::Blocked)
                    {
                        // Lock fail should stop the execution of current
                        // CC request since it's already in blocking queue.
                        return false;
                    }

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
            req.Result()->SetError(-1);
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

            // Lock has been acquired, UpsertLockHoldingTx
            LockHandleForResumedRequest(
                &req, tx_term, cce, cce->payload_status_);

            AddScanTupleMsg(cce,
                            cache,
                            tuple_idx,
                            scan_type,
                            ng_term,
                            req.ReadTimestamp(),
                            is_read_snapshot,
                            req.is_ckpt_delta_);
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
                                                   cc_proto);
                if (lock_pair.second == LockOpStatus::Failed)
                {
                    // lock confilct: back off and retry.
                    req.Result()->SetError(1);
                    return true;
                }
                else if (lock_pair.second == LockOpStatus::Blocked)
                {
                    // Lock fail should stop the execution of current
                    // CC request since it's already in blocking queue.

                    // TODO(lzx): Add remote acknowlege when lock fail
                    return false;
                }
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
                                                   cc_proto);
                if (lock_pair.second == LockOpStatus::Failed)
                {
                    // lock confilct: back off and retry.
                    req.Result()->SetError(1);
                    return true;
                }
                else if (lock_pair.second == LockOpStatus::Blocked)
                {
                    // Lock fail should stop the execution of current
                    // CC request since it's already in blocking queue.
                    // TODO(lzx): Add remote acknowlege when lock fail
                    return false;
                }

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
                                                   cc_proto);
                if (lock_pair.second == LockOpStatus::Failed)
                {
                    // lock confilct: back off and retry.
                    req.Result()->SetError(1);
                    return true;
                }
                else if (lock_pair.second == LockOpStatus::Blocked)
                {
                    // Lock fail should stop the execution of current
                    // CC request since it's already in blocking queue.
                    // TODO(lzx): Add remote acknowlege when lock fail
                    return false;
                }

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
            req.Result()->SetError(-1);
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

            // Lock has been acquired, UpsertLockHoldingTx
            LockHandleForResumedRequest(
                &req, tx_term, prior_cce, prior_cce->payload_status_);

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
                                                   cc_proto);
                if (lock_pair.second == LockOpStatus::Failed)
                {
                    // lock confilct: back off and retry.
                    req.Result()->SetError(1);
                    return true;
                }
                else if (lock_pair.second == LockOpStatus::Blocked)
                {
                    // Lock fail should stop the execution of current
                    // CC request since it's already in blocking queue.
                    return false;
                }

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
                                                       cc_proto);
                    if (lock_pair.second == LockOpStatus::Failed)
                    {
                        // lock confilct: back off and retry.
                        req.Result()->SetError(1);
                        return true;
                    }
                    else if (lock_pair.second == LockOpStatus::Blocked)
                    {
                        // Lock fail should stop the execution of current
                        // CC request since it's already in blocking queue.

                        // TODO(lzx): Add remote acknowlege when lock fail
                        return false;
                    }

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
                cce->KickOutArchiveRecords(recycle_ts);
                if (cce->commit_ts_ > req.ckpt_ts_)
                {
                    // Don't do checkpoint but flush undo
                    if (cce->ExportArchives(
                            req.archive_vec_, req.ckpt_ts_, Type()) > 0)
                    {
                        req.extra_vec_.push_back(cce);
                        if (cce->ckpt_ts_ == 1U &&
                            !cce->HasVisibleVersion(recycle_ts))
                        {
                            req.mv_base_vec_.push_back(cce);
                        }
                    }
                }
            }

            if (cce->commit_ts_ <= req.ckpt_ts_ &&
                cce->commit_ts_ > cce->ckpt_ts_.load(std::memory_order_acquire))
            {
                auto &ref = req.ckpt_vec_.emplace_back();
                ref.cce_ = cce;
                ref.payload_status_ = cce->payload_status_;
                ref.commit_ts_ = cce->commit_ts_;
                if (cce->payload_ != nullptr)
                {
                    if (shard_->EnableMvcc())
                    {
                        ref.SetPayload(cce->payload_.get());
                    }
                    else
                    {
                        ref.SetPayload(
                            std::make_unique<ValueT>(*cce->payload_));
                    }
                }
                if (shard_->EnableMvcc())
                {
                    // Also flush undo before truncating redo log.
                    cce->ExportArchives(req.archive_vec_, req.ckpt_ts_, Type());
                    if (cce->ckpt_ts_ == 1U &&
                        !cce->HasVisibleVersion(recycle_ts))
                    {
                        req.mv_base_vec_.push_back(cce);
                    }
                }

                cce->parent_map_->shard_->estimate_ccshard_log_size_ -=
                    cce->estimate_ccentry_log_size_;
                cce->estimate_ccentry_log_size_ = 0;
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

            uint32_t shard_code = Sharder::Instance().ShardCode(key.Hash());
            uint16_t core_id = (shard_code & 0x3FF) % shard_->core_cnt_;
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

            CcEntry<KeyT, ValueT> *cce = FindEmplace(key, req.CommitTs());

            if (cce == nullptr)
            {
                shard_->Enqueue(shard_->LocalCoreId(), &req);
                return false;
            }

            if (cce->commit_ts_ >= req.CommitTs())
            {
                // If the key exists in the cc map and its commit ts is
                // greater than that of the log record, and if (1) mvcc is
                // enabled, then install  the log record into archives; (2) mvcc
                // is not enabled, then skips installing the log record in the
                // cc map and moves to the next key in the log record.
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
                    cce->AddArchiveRecord(
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
                    cce->ArchiveBeforeUpdate(Type());
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

                if (cce->key_lock_.HasWriteLock())
                {
                    // If the record in the log has a commit ts greater than
                    // that of the cc entry and the cc entry has a write
                    // lock, the lock's owner must be the tx that commits
                    // the log record.
                    // TODO: it is safer if we ship the tx ID with the
                    // recovering message and match it against the lock holder.
                    TxNumber txn = cce->key_lock_.WriteLockTx();
                    ReleaseCceKeyLock(cce, txn);
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
                    auto &ref = tmp_ckpt_vec.emplace_back();
                    ref.cce_ = cce;
                    ref.payload_status_ = cce->payload_status_;
                    ref.commit_ts_ = cce->commit_ts_;
                    if (cce->payload_ != nullptr)
                    {
                        if (shard_->EnableMvcc())
                        {
                            ref.SetPayload(cce->payload_.get());
                        }
                        else
                        {
                            ref.SetPayload(
                                std::make_unique<ValueT>(*cce->payload_));
                        }
                    }

                    std::vector<FlushRecord> tmp_akvs;
                    cce->ExportArchives(tmp_akvs, cce->commit_ts_ - 1, Type());
                    bool res = shard_->FlushEntryForTest(
                        cce, tmp_ckpt_vec, tmp_akvs, only_archives);
                    assert(res == true);
                }
                if (only_archives)
                {
                    cce->archives_.clear();
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

    size_t size() const override
    {
        return ccm_.size();
    }

    std::unique_ptr<CcScanner> CreateScanner(
        ScanDirection direction) const override
    {
        return std::make_unique<TemplateCcScanner<KeyT, ValueT>>(
            direction,
            table_name_.Type() == TableType::Secondary
                ? ScanIndexType::Secondary
                : ScanIndexType::Primary,
            KeySchema());
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
        // Don't call DetachLru if entry is not in lru list(Emplaced by force)
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

        CcEntry<KeyT, ValueT> *cc_entry =
            static_cast<CcEntry<KeyT, ValueT> *>(remove_entry);

        CcEntry<KeyT, ValueT> *prior = cc_entry->map_prev_;
        CcEntry<KeyT, ValueT> *next = cc_entry->map_next_;

        prior->map_next_ = next;
        next->map_prev_ = prior;

        shard_->DecrementMemory(cc_entry->GetCcEntryMemUsage());

        ccm_.erase(*cc_entry->key_);
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

protected:
    CcEntry<KeyT, ValueT> *FindEmplace(const KeyT &key, uint64_t ts)
    {
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
        if (shard_->Full() && !is_catalog_cc_map_)
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
        auto em_it = ccm_.emplace_hint(lb_it, KeyT(key), this);
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

        return new_cce_ptr;
    }

    CcEntry<KeyT, ValueT> *Emplace(const KeyT &key, uint64_t ts)
    {
        // catalog ccmap bypass shard memory limit. since checkpointer may
        // emplace ccentry into ccmap.
        if (shard_->Full() && !is_catalog_cc_map_)
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
                    // The map is empty. The next entry of negative infinity is
                    // positive infinity.
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
                    // The map is empty. The prior entry of positive infinity is
                    // negative infinity.
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

                // If the current points to the beginning of the map, the prior
                // entry is negative infinity.
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
            // If the current points to negative infinity, keeps the iterator
            // unchanged.

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
            // The two iterators are equal, if they point to the same cc entry.
            // Note that when the iterator points to positive infinity, the
            // pointed cc entry is null.
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
     * starting from the start cc entry and whether the scan includes the start
     * cc entry's key or gap or both.
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
                 TemplateScanTuple<KeyT, ValueT> *tuple,
                 bool include_gap,
                 uint32_t ng_id,
                 int64_t ng_term,
                 uint64_t read_ts,
                 bool is_read_snapshot,
                 bool is_ckpt_delta = false) const
    {
        tuple->Key().Copy(*cce->key_);

        if (is_read_snapshot)
        {
            VersionResultRecord<ValueT> v_rec;
            bool res = cce->MvccGet(read_ts, v_rec, Type());
            if (!res)
            {
                // TODO(lzx): to handle this error.
                // return error.
            }
            if (v_rec.payload_status_ == RecordStatus::Normal ||
                (is_ckpt_delta &&
                 v_rec.payload_status_ == RecordStatus::Deleted))
            {
                tuple->Record() = *(v_rec.payload_ptr_);
            }
            tuple->key_ts_ = v_rec.commit_ts_;
            tuple->rec_status_ = v_rec.payload_status_;
        }
        else
        {
            if (cce->payload_status_ == RecordStatus::Normal ||
                (is_ckpt_delta &&
                 cce->payload_status_ == RecordStatus::Deleted))
            {
                tuple->Record() = *(cce->payload_);
            }
            tuple->rec_status_ = cce->payload_status_;
            tuple->key_ts_ = cce->commit_ts_;
        }

        tuple->gap_ts_ = include_gap ? cce->gap_commit_ts_ : 0;
        tuple->cce_addr_.SetCce(
            reinterpret_cast<uint64_t>(cce), ng_term, ng_id);
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
            bool res = cce->MvccGet(read_ts, v_rec, Type());
            if (!res)
            {
                // return error.
            }
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
    bool is_catalog_cc_map_;
};
}  // namespace txservice
