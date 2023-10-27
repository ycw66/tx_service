#pragma once

#include <algorithm>  // std::max
#include <cassert>
#include <chrono>
#include <cstddef>
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <unordered_set>
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
#include "meter.h"
#include "metrics.h"
#include "proto/cc_request.pb.h"
#include "remote/remote_cc_handler.h"  //RemoteCcHandler
#include "remote/remote_cc_request.h"
#include "remote/remote_type.h"
#include "sharder.h"
#include "store/data_store_handler.h"
#include "table_statistics.h"
#include "tx_execution.h"
#include "tx_id.h"
#include "tx_key.h"
#include "tx_trace.h"
#include "type.h"

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
          sample_pool_(nullptr)
    {
        pg_ng_inf_.prev_page_ = nullptr;
        pg_ng_inf_.next_page_ = &pg_ps_inf_;
        pg_ps_inf_.prev_page_ = &pg_ng_inf_;
        pg_ps_inf_.next_page_ = nullptr;

#ifndef ON_KEY_OBJECT
        if (table_schema && (table_name.Type() == TableType::Primary ||
                             table_name.Type() == TableType::Secondary ||
                             table_name.Type() == TableType::UniqueSecondary))
        {
            TableStatistics<KeyT> *statistics =
                static_cast<TableStatistics<KeyT> *>(
                    table_schema->StatisticsObject().get());
            assert(statistics != nullptr);
            if (Statistics::CoreDoSample(table_name) == shard->core_id_)
            {
                sample_pool_ = statistics->GetOrInitSamplePool(
                    table_name, cc_ng_id, shard);
                assert(sample_pool_);
            }
        }
#endif

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
                                            0,
                                            false);
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
#ifdef RANGE_PARTITION_ENABLED
                    req.Result()->SetError(CcErrorCode::OUT_OF_MEMORY);
                    return true;
#else
                    shard_->Enqueue(shard_->LocalCoreId(), &req);
                    return false;
#endif
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
            if (table_name_.Type() == TableType::Secondary ||
                table_name_.Type() == TableType::UniqueSecondary)
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
                                      0,
                                      false);
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
        bool is_upload = cce_addr == nullptr;

        CODE_FAULT_INJECTOR("term_TemplateCcMap_Execute_PostWriteCc", {
            if (table_name_.Type() == TableType::Primary)
            {
                LOG(INFO) << "FaultInject  "
                             "term_TemplateCcMap_Execute_PostWriteCc";
                req.Result()->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
                return true;
            }
        });

        if (!is_upload && !Sharder::Instance().CheckLeaderTerm(
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

        if (!is_upload && cce_addr->InsertPtr() != 0)
        {
            // DEAD BRANCH FOR NOW
            if (table_name_.Type() == TableType::Secondary ||
                table_name_.Type() == TableType::UniqueSecondary)
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
#ifdef RANGE_PARTITION_ENABLED
                    req.Result()->SetError(CcErrorCode::OUT_OF_MEMORY);
                    return true;
#else
                    shard_->Enqueue(shard_->LocalCoreId(), &req);
                    return false;
#endif
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
                if (ccm_has_full_entries_ || req.IsInitialInsert())
                {
                    new_cce->ckpt_ts_.store(1U);
                }

                prior_cce.gap_commit_ts_ = commit_ts;
                prior_cce.insert_intention_set_.erase(
                    --ite, prior_cce.insert_intention_set_.end());

                if (shard_->realtime_sampling_ && sample_pool_)
                {
                    sample_pool_->OnInsert(*key_ptr, table_schema_);
                }
                if (commit_ts > last_dirty_commit_ts_)
                {
                    last_dirty_commit_ts_ = commit_ts;
                }
                if (commit_ts > new_cce->parent_page_->last_dirty_commit_ts_)
                {
                    new_cce->parent_page_->last_dirty_commit_ts_ = commit_ts;
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
            if (is_upload)
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

                // Since the uploaded record might not be the latest version
                // of this record, we need to mark data_store_size_ of cce as
                // unknown. The operation type for the uploaded records should
                // be Upsert.
                Iterator it = FindEmplace(*key);
                cce = it->second;
                if (cce == nullptr)
                {
                    LOG(WARNING)
                        << "!!!WARNING!!! PostWriteCc have no"
                        << " enough memory. Txn: " << txn
                        << ", table name trace: " << this->table_name_.Trace();
                    // This cc shard has reached max memory limit. We
                    // didn't write data log for this post write req,
                    // but we have acquired range read lock for this
                    // key. If we do not return error and release the
                    // range read lock, it might block range split from
                    // finishing. We should return error here so that
                    // coordinator can release range read lock and retry
                    // later.
                    req.Result()->SetError(CcErrorCode::OUT_OF_MEMORY);
                    return true;
                }

                // Since this is a forward req, we assume this entry is not
                // visible on this ng yet so no need to check for lock.

                if (cce->ckpt_ts_ == 0U &&
                    (ccm_has_full_entries_ || req.IsInitialInsert()))
                {
                    uint64_t tmp_ts = 0U;
                    cce->ckpt_ts_.compare_exchange_strong(tmp_ts, 1U);
                }
            }
            else
            {
                cce = reinterpret_cast<CcEntry<KeyT, ValueT> *>(
                    cce_addr->CcePtr());

                if (cce->key_lock_ptr_ == nullptr ||
                    !cce->key_lock_ptr_->HasWriteLock() ||
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
                if (op_type == OperationType::Insert && cce->commit_ts_ == 1)
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

                if (commit_ts < cce->commit_ts_)
                {
                    // Concurrent upsert_tx has write the latest value, so
                    // discard the old value directly. For example, during add
                    // index transaction, we will write the packed sk data that
                    // generate from old pk records into the new sk ccmap, and
                    // before this post write request, we do not acquire the
                    // write lock on this TxKey, so this value has been updated
                    // by a concurrent transaction.
                    assert(is_upload);
                    req.Result()->SetFinished();
                    return true;
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
                // Now, all versions of non-unique SecondaryIndex key shared the
                // unpack info in current version's payload, though the unpack
                // info will not be used for deleted key, we must not change the
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
                        if (cce->payload_.use_count() == 1)
                        {
                            *(cce->payload_) = *commit_val;
                        }
                        else
                        {
                            cce->payload_ =
                                std::make_shared<ValueT>(*commit_val);
                        }
                    }
                    else
                    {
                        size_t offset = 0;
                        if (cce->payload_.use_count() != 1)
                        {
                            cce->payload_ = std::make_shared<ValueT>();
                        }
                        cce->payload_->Deserialize(payload_str->data(), offset);
                    }
                    shard_->mem_usage_ += cce->PayloadMemUsage();
                }

                RecordStatus cce_old_status = cce->payload_status_;
                cce->payload_status_ =
                    is_del ? RecordStatus::Deleted : RecordStatus::Normal;
                DLOG_IF(INFO, TRACE_OCC_ERR)
                    << "PostWriteCc, txn:" << txn << " ,cce: " << cce
                    << " ,commit_ts: " << commit_ts;

                if (commit_ts > last_dirty_commit_ts_)
                {
                    last_dirty_commit_ts_ = commit_ts;
                }
                if (commit_ts > cce->parent_page_->last_dirty_commit_ts_)
                {
                    cce->parent_page_->last_dirty_commit_ts_ = commit_ts;
                }
                if (shard_->realtime_sampling_ && sample_pool_)
                {
                    if (op_type == OperationType::Insert ||
                        op_type == OperationType::Upsert)
                    {
                        sample_pool_->OnInsert(
                            static_cast<const KeyT &>(*cce->Key()),
                            table_schema_);
                    }
                    else if (op_type == OperationType::Delete)
                    {
                        if (cce_old_status == RecordStatus::Normal)
                        {
                            sample_pool_->OnDelete(
                                static_cast<const KeyT &>(*cce->Key()),
                                table_schema_);
                        }
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

        if (table_name_.Type() == TableType::Secondary ||
            table_name_.Type() == TableType::UniqueSecondary)
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
                                            0,
                                            false);
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
                    req.SetTxKey(target_key);
                    break;
                case KeyType::PositiveInf:
                    target_key = PositiveInfinity<KeyT>::Instance();
                    req.SetTxKey(target_key);
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
#ifdef RANGE_PARTITION_ENABLED
                    hd_res->SetError(CcErrorCode::OUT_OF_MEMORY);
                    return true;
#else
                    shard_->Enqueue(shard_->LocalCoreId(), &req);
                    return false;
#endif
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
                                      0,
                                      false);
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

        if (table_name_.Type() == TableType::Secondary ||
            table_name_.Type() == TableType::UniqueSecondary)
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
                req.SetTxKey(target_key);
                break;
            case KeyType::PositiveInf:
                target_key = PositiveInfinity<KeyT>::Instance();
                req.SetTxKey(target_key);
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
#ifdef RANGE_PARTITION_ENABLED
            req.Result()->SetError(CcErrorCode::OUT_OF_MEMORY);
            return true;
#else
            shard_->Enqueue(shard_->LocalCoreId(), &req);
            return false;
#endif
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
#ifdef RANGE_PARTITION_ENABLED
                        req.Result()->SetError(CcErrorCode::OUT_OF_MEMORY);
                        return true;
#else
                        shard_->Enqueue(shard_->LocalCoreId(), &req);
                        return false;
#endif
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
                    if (req.CommitType() != PostWriteType::PrepareCommit)
                    {
                        cce_ptr->commit_ts_ = commit_ts;
                        cce_ptr->payload_status_ =
                            (req.OpType() == OperationType::Delete ||
                             req.OpType() == OperationType::DropTable)
                                ? RecordStatus::Deleted
                                : RecordStatus::Normal;
                    }
                }

                // When commit_ts = 0, the request removes the write lock
                // without installing a new value.

                if (req.CommitType() != PostWriteType::PrepareCommit)
                {
                    // For PostCommit or Commit, the post-write-all request
                    // releases the write lock/intent.
                    ReleaseCceKeyLock(cce_ptr, txn, req.NodeGroupId());
                }
                else
                {
                    // For PrepareCommit, the post-write-all request keeps write
                    // intent or downgrades the write lock to the write intent.
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

        CODE_FAULT_INJECTOR("before_post_read", {
            NodeGroupId ng = (req.Txn() >> 32L) >> 10;
            if (ng != cc_ng_id_)
            {
                LOG(INFO) << "FaultInject before_post_read: skip executing "
                             "PostReadCc to timeout transaction  tx: "
                          << req.Txn();
                return true;
            }
        });

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
        else if (table_name_.Type() == TableType::UniqueSecondary)
        {
            cc_op = req.IsForWrite() ? CcOperation::ReadForWrite
                                     : CcOperation::ReadSkIndex;
            is_read_snapshot =
                (iso_lvl == IsolationLevel::Snapshot && !req.IsForWrite());
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
                                                    req.ReadTimestamp(),
                                                    req.IsCoveringKeys());
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
                        RangeSliceId slice_id = shard_->PinRangeSlice(
                            table_name_,
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
                        else if (pin_status ==
                                 RangeSliceOpStatus::BlockedOnLoad)
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
                            hd_res->SetError(
                                CcErrorCode::PIN_RANGE_SLICE_FAILED);
                            return true;
                        }
                    }
                    else
                    {
                        assert(Type() == TableType::Catalog);
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
                                req.NodeGroupId(),
                                shard_->LocalCoreId());
                CODE_FAULT_INJECTOR("remote_read_msg_missed", {
                    LOG(INFO) << "FaultInject  remote_read_msg_missed"
                              << "txID: " << req.Txn();
                    if (!req.IsLocal())
                    {
                        remote::RemoteRead &remote_req =
                            static_cast<remote::RemoteRead &>(req);
                        remote_req.Acknowledge();
                    }

                    return false;
                });

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
                                      req.ReadTimestamp(),
                                      req.IsCoveringKeys());
            }

            // After acquiring lock
            switch (err_code)
            {
            case CcErrorCode::MVCC_READ_MUST_WAIT_WRITE:
            {
                req.SetIsWaitForPostWrite(true);
                // Put the request to top of key lock's blocking queue with
                // acquiring readlock. And then should release the readlock
                // before handling this request when PostWriteCc finished.
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

            // NOTE: Check if really need to wait for post write. There is no
            // need to wait for post write for the below case which also match
            // this condition `cce->commit_ts_ < req.ReadTimestamp()`:
            // The commit_ts of sk data generated from pk data during add index
            // txm is the commit_ts of add index txm, so those cce's commit_ts
            // also large than corresponding pk's commit_ts.
            bool wait_for_post_write =
                (cce->key_lock_ptr_ != nullptr &&
                 cce->key_lock_ptr_->HasWriteLock() &&
                 cce->key_lock_ptr_->WriteLockTx() != req.Txn());
            if (req.Isolation() == IsolationLevel::ReadCommitted &&
                cce->commit_ts_ > 0 && cce->commit_ts_ < req.ReadTimestamp() &&
                wait_for_post_write)
            {
                // When backtracking the content of primary key record according
                // to the secondary index key, if the commit_ts of this
                // primary key record is smaller than the commit_ts of the
                // secondary index key("req.ReadTimestamp()"), it means that the
                // current primary key has not been updated and there must be a
                // PostWriteCc request waiting to be executed. So, this read
                // should wait for the PostWriteCc completed.
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

        if (table_name_.Type() == TableType::Secondary ||
            table_name_.Type() == TableType::UniqueSecondary)
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
        if (table_name_.Type() == TableType::Secondary ||
            table_name_.Type() == TableType::UniqueSecondary)
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
                                                req.ReadTimestamp(),
                                                req.IsCoveringKeys());
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
                                                   req.ReadTimestamp(),
                                                   req.IsCoveringKeys());
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
                                                   req.ReadTimestamp(),
                                                   req.IsCoveringKeys());
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
                                                   req.ReadTimestamp(),
                                                   req.IsCoveringKeys());
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
        if (table_name_.Type() == TableType::Secondary ||
            table_name_.Type() == TableType::UniqueSecondary)
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
                                                req.ReadTimestamp(),
                                                req.IsCoveringKeys());
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
                                                   req.ReadTimestamp(),
                                                   req.IsCoveringKeys());
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
                                                       req.ReadTimestamp(),
                                                       req.IsCoveringKeys());
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

    void AddScanTupleMsg(const KeyT *key,
                         CcEntry<KeyT, ValueT> *cce,
                         RemoteScanSliceCache *remote_cache,
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
                ScanGap(key, cce, remote_cache, ng_term);
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
        if (table_name_.Type() == TableType::Secondary ||
            table_name_.Type() == TableType::UniqueSecondary)
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
                                                req.ReadTimestamp(),
                                                req.IsCoveringKeys());
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
                                                   req.ReadTimestamp(),
                                                   req.IsCoveringKeys());
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
                                                   req.ReadTimestamp(),
                                                   req.IsCoveringKeys());
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
                                                   req.ReadTimestamp(),
                                                   req.IsCoveringKeys());
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
        if (table_name_.Type() == TableType::Secondary ||
            table_name_.Type() == TableType::UniqueSecondary)
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
                                                req.ReadTimestamp(),
                                                req.IsCoveringKeys());

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
            prior_cce = reinterpret_cast<CcEntry<KeyT, ValueT> *>(
                req.PriorCceAddr().CcePtr());
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
                                                   req.ReadTimestamp(),
                                                   req.IsCoveringKeys());
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
                                                       req.ReadTimestamp(),
                                                       req.IsCoveringKeys());
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
        if (req.SendResponseIfFinished())
        {
            RangeSliceId slice_id = req.SliceId();
            slice_id.Unpin();
            return true;
        }
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
        if (table_name_.Type() == TableType::Secondary ||
            table_name_.Type() == TableType::UniqueSecondary)
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
        RemoteScanSliceCache *remote_scan_cache = nullptr;
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
                                             ng_term,
                                             KeySchema(),
                                             RecordSchema(),
                                             schema_ts_,
                                             table_schema_->GetKVCatalogInfo(),
                                             req.RangeId(),
                                             *req_start_key,
                                             req.StartInclusive(),
                                             &req,
                                             pin_status,
                                             false,
                                             req.PrefetchSize());

            if (pin_status == RangeSliceOpStatus::Retry)
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
            else if (pin_status == RangeSliceOpStatus::BlockedOnLoad)
            {
                return false;
            }
            else if (pin_status == RangeSliceOpStatus::Error)
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
            if (req.IsWaitForPostWrite(shard_->core_id_))
            {
                req.SetIsWaitForPostWrite(false, shard_->core_id_);
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
                                                req.ReadTimestamp(),
                                                req.IsCoveringKeys());

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
                                                   req.ReadTimestamp(),
                                                   req.IsCoveringKeys());
                switch (lock_pair.second)
                {
                case CcErrorCode::NO_ERROR:
                    break;
                case CcErrorCode::MVCC_READ_MUST_WAIT_WRITE:
                {
                    req.SetIsWaitForPostWrite(true, shard_->core_id_);
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
                                                   req.ReadTimestamp(),
                                                   req.IsCoveringKeys());
                switch (lock_pair.second)
                {
                case CcErrorCode::NO_ERROR:
                    break;
                case CcErrorCode::MVCC_READ_MUST_WAIT_WRITE:
                {
                    req.SetIsWaitForPostWrite(true, shard_->core_id_);
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
                                                   req.ReadTimestamp(),
                                                   req.IsCoveringKeys());
                switch (lock_pair.second)
                {
                case CcErrorCode::NO_ERROR:
                    break;
                case CcErrorCode::MVCC_READ_MUST_WAIT_WRITE:
                {
                    req.SetIsWaitForPostWrite(true, shard_->core_id_);
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
            // We only update result if req is local on SetFinish(). For
            // remote request we assign a dedicated response sender for each
            // req
            if (req.Result()->Value().is_local_)
            {
                slice_id.Unpin();
                return true;
            }
            else if (req.IsResponseSender(shard_->core_id_))
            {
                req.SendResponseIfFinished();
                slice_id.Unpin();
                return true;
            }
            else
            {
                // Renqueue the cc req to the sender req list.
                // We assign a dedicated core to be the response sender instead
                // of directly sending the response on the last finished core.
                // This is to avoid serialization of response message causing
                // one core to become significantly slower than others and would
                // end up being the sender of all scan slice response.
                shard_->local_shards_.EnqueueCcRequest(
                    shard_->core_id_, req.Txn(), &req);
                return false;
            }
        }
        else
        {
            return false;
        }
    }

    bool Execute(DataSyncScanCc &req) override
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

        const KeyT *start_key = static_cast<const KeyT *>(req.start_key_);
        const KeyT *end_key = static_cast<const KeyT *>(req.end_key_);
        Iterator it;
        Iterator end_it;
        if (req.pause_key_.at(shard_->core_id_).second)
        {
            // scan is already finished on this core
            std::pair<TxKey::Uptr, bool> ckpt_scan_result{nullptr, true};
            req.SetFinish(std::move(ckpt_scan_result), shard_->core_id_);
            return false;
        }
        if (req.pause_key_.at(shard_->core_id_).first == nullptr)
        {
            // If this is a new scan cc, start from the specified start key or
            // negative inf.
            if (start_key == nullptr ||
                start_key == NegativeInfinity<KeyT>::Instance())
            {
                it = Begin();
                it++;
            }
            else
            {
                it = LowerBound(*start_key);
                if (it->first == NegativeInfinity<KeyT>::Instance())
                {
                    it++;
                }
            }
        }
        else
        {
            const KeyT *pause_key = static_cast<const KeyT *>(
                req.pause_key_.at(shard_->core_id_).first.get());
            it = LowerBound(*pause_key);
        }

        if (end_key == nullptr || end_key->Type() == KeyType::PositiveInf)
        {
            end_it = End();
        }
        else
        {
            std::pair<Iterator, ScanType> end_pair =
                ForwardScanStart(*end_key, true);
            end_it = end_pair.first;
            if (end_pair.second == ScanType::ScanGap)
            {
                ++end_it;
            }
        }

        // Since we might skip the page that end_it is on if it's not updated
        // since last ckpt, it might skip end_it. If the last page is skipped it
        // will be set as the first entry on the next page. Also check if (it ==
        // end_it_next_page_it).
        Iterator end_it_next_page_it = end_it;
        if (end_it_next_page_it != End())
        {
            assert(end_it_next_page_it->second->parent_page_ != nullptr);
            if (end_it->second->parent_page_->next_page_ == PagePosInf())
            {
                end_it_next_page_it = End();
            }
            else
            {
                end_it_next_page_it = Iterator(
                    end_it->second->parent_page_->next_page_, 0, &neg_inf_);
            }
        }

        int64_t ng_term = Sharder::Instance().LeaderTerm(req.NodeGroupId());
        if (ng_term < 0)
        {
            req.SetError(CcErrorCode::TX_NODE_NOT_LEADER);
            return false;
        }

        uint64_t recycle_ts = 1U;
        if (shard_->EnableMvcc())
        {
            recycle_ts = shard_->GlobalMinSiTxStartTs();
        }

        std::vector<LruEntry *> remove_entries;

        // Only scan for updates after given from ts. previous_ckpt_ts_ is
        // used during regular ckpt, and previous_scan_ts_ is used during range
        // split explicitly.
        uint64_t from_ts =
            std::max(req.previous_ckpt_ts_, req.previous_scan_ts_);

        // DataSyncScanCc is running on TxProcessor thread. To avoid
        // blocking other transaction for a long time, we only process
        // CkptScanBatch number of pages in each round.

        for (size_t scan_cnt = 0;
             scan_cnt < DataSyncScanCc::DataSyncScanBatchSize &&
             req.accumulated_scan_cnt_.at(shard_->core_id_) <
                 req.scan_batch_size_ &&
             it != end_it && it != end_it_next_page_it;
             scan_cnt++)
        {
            const KeyT *key = it->first;
            CcEntry<KeyT, ValueT> *cce = it->second;
            assert(cce->parent_page_);
            if (cce->parent_page_->last_dirty_commit_ts_ <= from_ts)
            {
                // Skip the pages that have no updates since last data sync.
                if (cce->parent_page_->next_page_ == PagePosInf())
                {
                    it = End();
                }
                else
                {
                    it = Iterator(cce->parent_page_->next_page_, 0, &neg_inf_);
                }
                continue;
            }

            if (shard_->EnableMvcc())
            {
                shard_->DecrementMemory(cce->KickOutArchiveRecords(recycle_ts));
            }

            if (cce->NeedCkpt())
            {
#ifdef RANGE_PARTITION_ENABLED
                if (cce->data_store_size_.load(std::memory_order_acquire) ==
                    INT32_MAX)
                {
                    // Load data store size by pinning the slice. Data
                    // store size is required to decide slice & range
                    // update plan.
                    RangeSliceOpStatus pin_status;
                    RangeSliceId slice_id =
                        shard_->PinRangeSlice(table_name_,
                                              req.NodeGroupId(),
                                              ng_term,
                                              KeySchema(),
                                              RecordSchema(),
                                              table_schema_->Version(),
                                              table_schema_->GetKVCatalogInfo(),
                                              *key,
                                              true,
                                              &req,
                                              pin_status,
                                              true,
                                              UINT8_MAX);
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
                    else if (pin_status == RangeSliceOpStatus::Retry)
                    {
                        req.pause_key_.at(shard_->core_id_).first =
                            key->Clone();
                        shard_->Enqueue(shard_->LocalCoreId(), &req);
                        return false;
                    }
                    else if (pin_status == RangeSliceOpStatus::BlockedOnLoad)
                    {
                        req.pause_key_.at(shard_->core_id_).first =
                            key->Clone();
                        return false;
                    }
                    else if (pin_status == RangeSliceOpStatus::NotOwner)
                    {
                        // The recovered cc entry does not belong to this ng
                        // anymore. This will happen if ng failover after a
                        // range split just finished but before checkpointer is
                        // able to truncate the log. In this case the log
                        // records of the data that now falls on another ng will
                        // still be replayed on the old ng on recover. Skip the
                        // cc entry and remove it at the end.
                        remove_entries.push_back(cce);
                        it++;
                        continue;
                    }
                    else
                    {
                        // Checkpointing needs to load a slice only if one or
                        // more changed records are to be flushed. Range catalog
                        // must have been loaded when initial changes were made.
                        // So, pinning slice in checkpointing never returns
                        // BlockedOnCatalog. Moreover, since the force_load flag
                        // is set, pinning slice in checkpointing never returns
                        // Delay.
                        req.SetError(CcErrorCode::PIN_RANGE_SLICE_FAILED);
                        return true;
                    }
                }
#endif
                cce->ExportForCkpt(*key,
                                   req.DataSyncVec(shard_->core_id_),
                                   req.ArchiveVec(shard_->core_id_),
                                   req.MoveBaseIdxVec(shard_->core_id_),
                                   req.previous_scan_ts_,
                                   req.data_sync_ts_,
                                   recycle_ts,
                                   Type(),
                                   shard_->EnableMvcc(),
                                   req.accumulated_scan_cnt_[shard_->core_id_]);
            }
            it++;
        }

        TxKey::Uptr next_pause_key = nullptr;
        bool no_more_data = (it == end_it) || (it == end_it_next_page_it);
        if (!no_more_data)
        {
            next_pause_key = it->first->Clone();
        }

        for (LruEntry *cce : remove_entries)
        {
            Clean(cce);
        }

        if (no_more_data)
        {
            // scan data drained
            std::pair<TxKey::Uptr, bool> ckpt_scan_result{nullptr, true};
            req.SetFinish(std::move(ckpt_scan_result), shard_->core_id_);
            return false;
        }
        else
        {
            // set the pause_key_ to mark resume position and put the CkptScanCc
            // request into CcQueue again.
            if (req.accumulated_scan_cnt_.at(shard_->core_id_) <
                req.scan_batch_size_)
            {
                req.pause_key_.at(shard_->core_id_).first =
                    std::move(next_pause_key);
                shard_->Enqueue(&req);
            }
            else
            {
                // scan data is not drained
                std::pair<TxKey::Uptr, bool> ckpt_scan_result{
                    std::move(next_pause_key), false};
                req.SetFinish(std::move(ckpt_scan_result), shard_->core_id_);
                return false;
            }
        }

        return false;
    }

    bool Execute(BroadcastStatisticsCc &req) override
    {
        assert(false && "CatalogCcMap::Execute(BroadcastStatisticsCc &) only");
        return true;
    }

    bool Execute(AnalyzeTableAllCc &req) override
    {
        CcHandlerResult<Void> *hd_res = req.Result();
        int64_t ng_term = Sharder::Instance().LeaderTerm(req.NodeGroupId());
        if (ng_term < 0)
        {
            hd_res->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            return true;
        }

        assert(table_name_ == *req.GetTableName());
        assert(req.NodeGroupId() == cc_ng_id_);
        assert(Statistics::CoreDoSample(table_name_) == shard_->core_id_);

        using KeySamplePool = AnalyzeTableAllCc::SamplePool<
            AnalyzeTableAllCc::sample_pool_capacity_,
            KeyT,
            typename TemplateCcMapSamplePool<KeyT>::CopyKey>;
        using SliceSamplePool = AnalyzeTableAllCc::SamplePool<
            AnalyzeTableAllCc::sample_pool_capacity_,
            std::pair<int32_t, const StoreSlice *>,
            Copy<std::pair<int32_t, const StoreSlice *>>>;

        KeySamplePool *key_sample_pool = nullptr;
        SliceSamplePool *slice_sample_pool = nullptr;

        bool first_enter;
        if (req.key_sample_pool_ == nullptr)
        {
            assert(req.key_sample_pool_ == nullptr);

            key_sample_pool = new KeySamplePool();
            slice_sample_pool = new SliceSamplePool();

            req.key_sample_pool_.reset(key_sample_pool);
            req.slice_sample_pool_.reset(slice_sample_pool);

            first_enter = true;
        }
        else
        {
            assert(req.key_sample_pool_ != nullptr);

            key_sample_pool =
                static_cast<KeySamplePool *>(req.key_sample_pool_.get());
            slice_sample_pool =
                static_cast<SliceSamplePool *>(req.slice_sample_pool_.get());

            first_enter = false;
        }

        if (first_enter)
        {
            TableName range_table_name(table_name_.StringView(),
                                       TableType::RangePartition);

            // All ranges has been added read lock. It is safe to access them.
            const std::map<const TxKey *, TableRangeEntry, PtrLessThan<TxKey>>
                *range_map = shard_->GetTableRangesForATable(range_table_name,
                                                             cc_ng_id_);
            for (const auto &[range_start_key, range_entry] : *range_map)
            {
                if (shard_
                        ->GetRangeOwner(
                            range_entry.GetRangeInfo()->PartitionId(),
                            cc_ng_id_)
                        ->BucketOwner() == cc_ng_id_)
                {
                    const StoreRange *store_range = range_entry.RangeSlices();
                    assert(store_range != nullptr);
                    for (const std::unique_ptr<StoreSlice> &store_slice :
                         store_range->Slices())
                    {
                        slice_sample_pool->Insert(std::make_pair(
                            store_range->PartitionId(), store_slice.get()));
                    }
                }
            }

            assert(req.next_pin_slice_idx_ == 0);
        }

        // Pin-slice is necessary. On one hand, pin-slice would guarantee enough
        // samples, even if the amount of sampled slice is very few. On the
        // other hand, pin-slice would help estimating average bytes of records.
        //
        // Capacity of slice sample pool cannot be too small. Otherwise the
        // final sampled keys could not reflect original key distribution.
        // Capacity of slice sample pool cannot be too large. Otherwise too many
        // pin-slice calls can lead to too many accesses to storage.
        if (req.next_pin_slice_idx_ < slice_sample_pool->SampleKeys().size())
        {
            const auto [range_id, store_slice] =
                slice_sample_pool->SampleKeys().at(req.next_pin_slice_idx_);
            assert(store_slice->StartKey() != nullptr);

            const KeyT *slice_start_key =
                store_slice->StartKey()
                    ? static_cast<const KeyT *>(store_slice->StartKey())
                    : NegativeInfinity<KeyT>::Instance();
            const KeyT *slice_end_key =
                store_slice->EndKey()
                    ? static_cast<const KeyT *>(store_slice->EndKey())
                    : PositiveInfinity<KeyT>::Instance();

            RangeSliceOpStatus pin_status;

            // table_schema_
            const Schema *key_schema = nullptr;
            if (table_name_.Type() == TableType::Primary)
            {
                key_schema = table_schema_->KeySchema();
            }
            else
            {
                key_schema = table_schema_->IndexKeySchema(table_name_);
            }

            RangeSliceId slice_id =
                shard_->PinRangeSlice(table_name_,
                                      cc_ng_id_,
                                      ng_term,
                                      key_schema,
                                      table_schema_->RecordSchema(),
                                      table_schema_->Version(),
                                      table_schema_->GetKVCatalogInfo(),
                                      range_id,
                                      *slice_start_key,
                                      true,
                                      &req,
                                      pin_status,
                                      false,
                                      UINT8_MAX);
            if (pin_status == RangeSliceOpStatus::Successful)
            {
                slice_id.Unpin();

                auto [iter, scan_type] =
                    ForwardScanStart(*slice_start_key, true);
                while (iter != End() &&
                       *iter->first < static_cast<const KeyT &>(*slice_end_key))
                {
                    const KeyT &key = *iter->first;
                    const CcEntry<KeyT, ValueT> &cc_entry = *iter->second;

                    if (cc_entry.payload_status_ == RecordStatus::Normal)
                    {
                        if (key.Type() == KeyType::Normal)
                        {
                            key_sample_pool->Insert(key);

                            req.visit_keys_ += 1;
                        }
                    }

                    ++iter;
                }

                ++req.next_pin_slice_idx_;
                shard_->Enqueue(&req);
                return false;
            }
            else if (pin_status == RangeSliceOpStatus::BlockedOnLoad)
            {
                return false;
            }
            else
            {
                hd_res->SetError(CcErrorCode::DATA_STORE_ERR);
                return true;
            }
        }
        else
        {
            assert(sample_pool_ != nullptr);

            uint64_t node_group_records = 0;
            size_t visit_slices = slice_sample_pool->Size();
            if (visit_slices > 0)
            {
                uint64_t ng_slices =
                    shard_->CountSlices(table_name_, cc_ng_id_, cc_ng_id_);
                node_group_records =
                    (ng_slices * req.visit_keys_ * shard_->core_cnt_ +
                     visit_slices - 1) /
                    visit_slices;  // Ceiling divide
            }

            sample_pool_->Reset(std::move(key_sample_pool->random_pairing_),
                                node_group_records,
                                table_schema_);
            hd_res->SetFinished();
            return true;
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
            uint8_t op_val =
                *reinterpret_cast<const uint8_t *>(log_blob.data() + offset);
            OperationType op_type = static_cast<OperationType>(op_val);
            assert(op_type == OperationType::Insert ||
                   op_type == OperationType::Update ||
                   op_type == OperationType::Delete);

            offset += sizeof(uint8_t);

            uint16_t core_id = (key.Hash() & 0x3FF) % shard_->core_cnt_;
            if (core_id != shard_->core_id_)
            {
                // Skips the key in the log record that is not sharded to this
                // core.
                if (op_type == OperationType::Insert ||
                    op_type == OperationType::Update)
                {
                    rec.Deserialize(log_blob.data(), offset);
                }
                continue;
            }

            Iterator it = FindEmplace(key);
            CcEntry<KeyT, ValueT> *cce = it->second;

            if (cce == nullptr)
            {
#ifdef RANGE_PARTITION_ENABLED
                req.Result()->SetError(CcErrorCode::OUT_OF_MEMORY);
                return true;
#else
                shard_->Enqueue(shard_->LocalCoreId(), &req);
                return false;
#endif
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
                    if (op_type == OperationType::Insert ||
                        op_type == OperationType::Update)
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
                else if (op_type == OperationType::Insert ||
                         op_type == OperationType::Update)
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
                if (op_type == OperationType::Insert ||
                    op_type == OperationType::Update)
                {
                    shard_->DecrementMemory(cce->PayloadMemUsage());
                    if (cce->payload_.use_count() != 1)
                    {
                        cce->payload_ = std::make_shared<ValueT>();
                    }
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
                if (cce->commit_ts_ > last_dirty_commit_ts_)
                {
                    last_dirty_commit_ts_ = cce->commit_ts_;
                }
                if (cce->commit_ts_ > cce->parent_page_->last_dirty_commit_ts_)
                {
                    cce->parent_page_->last_dirty_commit_ts_ = cce->commit_ts_;
                }
                if (shard_->realtime_sampling_ && sample_pool_)
                {
                    if (op_type == OperationType::Insert)
                    {
                        sample_pool_->OnInsert(key, table_schema_);
                    }
                    else if (op_type == OperationType::Delete)
                    {
                        sample_pool_->OnDelete(key, table_schema_);
                    }
                }

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
                    std::vector<FlushRecord> tmp_ckpt_vec(1);
                    size_t tmp_ckpt_vec_size = 0;

                    std::vector<FlushRecord> tmp_akv_vec;
                    std::vector<size_t> tmp_mv_base_idx_vec;
                    std::vector<const TxKey *> tmp_mv_base_key_vec;

                    cce->ExportForCkpt(*cce_key,
                                       tmp_ckpt_vec,
                                       tmp_akv_vec,
                                       tmp_mv_base_idx_vec,
                                       0,
                                       cce->commit_ts_,
                                       1U,
                                       Type(),
                                       shard_->EnableMvcc(),
                                       tmp_ckpt_vec_size);

                    assert(tmp_ckpt_vec_size <= 1);
                    size_t offset = 0;
                    for (size_t i = 0; i < tmp_akv_vec.size(); ++i)
                    {
                        auto &rec = tmp_akv_vec[i];
                        rec.SetKey(
                            tmp_ckpt_vec[rec.GetKeyIndex() + offset].Key());
                    }

                    for (size_t i = 0; i < tmp_mv_base_idx_vec.size(); ++i)
                    {
                        size_t key_idx = tmp_mv_base_idx_vec[i];
                        const TxKey *key_raw_ptr = tmp_ckpt_vec[key_idx].Key();
                        tmp_mv_base_key_vec.emplace_back(key_raw_ptr);
                    }

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

            Iterator it = FindEmplace(*key, req.ForceLoad());
            const KeyT *cce_key = it->first;
            CcEntry<KeyT, ValueT> *cce = it->second;
            if (cce == nullptr)
            {
                // Memory reaches capacity while bringing a range slice into
                // memory.
                req.SetError(CcErrorCode::OUT_OF_MEMORY);
                return true;
            }

            uint32_t rec_store_size =
                data_item.is_deleted_ ? 0 : cce_key->Size() + record->Size();

            // If the in-memory version is from a upload request (i.e. generated
            // sk record from pk), the data store version might be newer. Only
            // overwrite if in memory version is newer.
            if (cce->commit_ts_ > 1 && data_item.version_ts_ <= cce->commit_ts_)
            {
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

            shard_->DecrementMemory(cce->PayloadMemUsage());
            if (cce->payload_ == nullptr)
            {
                cce->payload_ = std::make_shared<ValueT>(*record);
            }
            cce->commit_ts_ = data_item.version_ts_;
            cce->ckpt_ts_.store(data_item.version_ts_,
                                std::memory_order_relaxed);
            cce->payload_status_ = data_item.is_deleted_ ? RecordStatus::Deleted
                                                         : RecordStatus::Normal;
            cce->data_store_size_.store(rec_store_size,
                                        std::memory_order_relaxed);

            shard_->mem_usage_ += cce->PayloadMemUsage();
        }

        req.SetFinish();
        return false;
    }

    bool Execute(GetPostCkptSlice &req) override
    {
        RangeSliceId slice_id = req.SliceId();
        std::vector<SliceChangeInfo> &item_vec =
            req.SliceChangeInfoVec(shard_->core_id_);

        // Caller should have already pinned the slice.
        auto &pause_key_and_is_drained = req.PauseKey(shard_->core_id_);
        auto &pause_key_uptr = pause_key_and_is_drained.first;
        bool is_drained = pause_key_and_is_drained.second;

        if (is_drained)
        {
            assert(pause_key_uptr == nullptr);
            req.SetFinish();
            return false;
        }

        assert(!is_drained);

        Iterator map_it, map_end_it;

        if (pause_key_uptr != nullptr)
        {
            const KeyT *pause_key_raw_ptr =
                static_cast<const KeyT *>(pause_key_uptr.get());
            std::pair<Iterator, ScanType> start_pair =
                ForwardScanStart(*pause_key_raw_ptr, true);
            map_it = start_pair.first;
            if (start_pair.second == ScanType::ScanGap)
            {
                ++map_it;
            }
        }
        else
        {
            const KeyT *start_key =
                static_cast<const KeyT *>(slice_id.Slice()->StartKey());

            if (start_key == nullptr ||
                start_key->Type() == KeyType::NegativeInf)
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

        auto &ckpt_cce_raw_ptr_vec = req.CkptCceRawPtrVec(shard_->core_id_);
        size_t &next_vec_idx = req.item_vec_size_[shard_->core_id_];
        size_t ckpt_idx = req.SliceFirstIdx(shard_->core_id_);
        size_t end_idx = ckpt_cce_raw_ptr_vec.size();

        bool is_last_one = req.IsLastOne(shard_->core_id_);

        for (size_t scan_cnt = 0;
             scan_cnt < GetPostCkptSlice::ScanBatchSize && map_it != map_end_it;
             ++map_it, ++scan_cnt)
        {
            const KeyT *cce_key = map_it->first;
            CcEntry<KeyT, ValueT> *cce = map_it->second;

            if (cce->commit_ts_ <= 1)
            {
                // This is a new inserted key that the tx has not finished
                // post-processing.
                continue;
            }

            if (!is_last_one && ckpt_idx == end_idx)
            {
                // Need to aquire next batch flush vector
                break;
            }

            assert(is_last_one || ckpt_idx < end_idx);
            if (ckpt_idx < end_idx && reinterpret_cast<uintptr_t>(cce) ==
                                          ckpt_cce_raw_ptr_vec[ckpt_idx])
            {
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

                // This entry is not going to be flushed in this checkpoint,
                // so the data store size before and post ckpt are the same.

                item_vec[next_vec_idx++].Reset(
                    cce_key, data_store_size, data_store_size, true);
            }
        }

        if (map_it == map_end_it)
        {
            pause_key_and_is_drained = {nullptr, true};
            req.SetFinish();
        }
        else
        {
            assert(map_it != map_end_it);
            pause_key_and_is_drained = {map_it->first->Clone(), false};
            req.UpdateFirstIdx(shard_->core_id_, ckpt_idx);
            req.SetFinish();
        }

        return false;
    }

    bool Execute(KickoutCcEntryCc &req) override
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

        int64_t ng_term = Sharder::Instance().LeaderTerm(req.NodeGroupId());
        if (ng_term < 0)
        {
            req.Result()->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            return true;
        }

        // Iterate the cc map using the original page list.
        const KeyT *start_key = static_cast<const KeyT *>(req.StartKey());
        const KeyT *end_key = static_cast<const KeyT *>(req.EndKey());
        CleanType clean_type = req.CleanType();
        LruPage *lru_page;
        if (req.ResumeKey(shard_->core_id_) != nullptr)
        {
            // resume key is the first key we need to start with, find the
            // floor key of it in case resume key has already been kicked
            // out.
            const KeyT *resume_key =
                static_cast<const KeyT *>(req.ResumeKey(shard_->core_id_));
            Iterator it = Floor(*resume_key);
            lru_page = it->second->parent_page_;
        }
        else
        {
            if (req.StartKey() == nullptr)
            {
                lru_page = pg_ng_inf_.next_page_;
            }
            else
            {
                Iterator it = Floor(*start_key);
                if (it->first == NegativeInfinity<KeyT>::Instance())
                {
                    lru_page = pg_ng_inf_.next_page_;
                }
                else
                {
                    lru_page = it->second->parent_page_;
                }
            }
        }

        CcPage<KeyT, ValueT> *ccp =
            static_cast<CcPage<KeyT, ValueT> *>(lru_page);

        // To avoid occupy the TxProcessor thread for a long time, only process
        // KickoutPageBatchSize number of pages in each round.
        size_t scan_page_cnt = 0;
        bool is_success = true;
        while (scan_page_cnt < KickoutCcEntryCc::KickoutPageBatchSize &&
               (end_key == nullptr || ccp->FirstKey() < *end_key) &&
               ccp != &pg_ps_inf_)
        {
            auto [freed_cnt, next_page] =
                CleanPageAndReBalance(ccp, clean_type, &req, &is_success);
            ++scan_page_cnt;
            // Move to next page
            ccp = static_cast<CcPage<KeyT, ValueT> *>(next_page);
            if (!is_success)
            {
                // Clean failed, retry in the next round.
                LOG(ERROR) << "Failed to clean all target ccentries on core: "
                           << shard_->core_id_;
                break;
            }
        }

        if (ccp == &pg_ps_inf_ ||
            (end_key != nullptr && !(ccp->FirstKey() < *end_key)))
        {
            return req.SetFinish(shard_->core_id_);
        }
        else
        {
            // Set the resume key for next round
            req.SetResumeKey(&ccp->FirstKey(), shard_->core_id_);
            shard_->Enqueue(&req);
            return false;
        }
    }

    bool Execute(ApplyCc &req) override
    {
        return true;
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
            if (page->lru_next_ != nullptr)
            {
                shard_->DetachLru(page);
            }
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
     * @param clean_type
     * @param kickout_cc [optional]
     * @param is_success [optional]
     * @return result pair of which the first is free count and the scond is the
     * next page which will be cleaned. If clean type is CleanForFree, the next
     * page is the lru_next_ of the page. Otherwise, the value of the next page
     * is setted depending on clean status:
     * When clean successfully, return the current page's next page in the below
     * cases: page is empty; no borrow or merge; borrow from previous; merge
     * with previous. Return the current page in the below cases: borrow from
     * next; merge with next.
     * When clean failed, return the current page always.
     */
    std::pair<size_t, LruPage *> CleanPageAndReBalance(
        LruPage *lru_page,
        CleanType clean_type = CleanType::CleanForFree,
        KickoutCcEntryCc *kickout_cc = nullptr,
        bool *is_success = nullptr) override
    {
        size_t free_cnt = 0;
        LruPage *next_page = nullptr;
        if (kickout_cc)
        {
            // For target ccmap, go along with CcPage::next_page_
            CcPage<KeyT, ValueT> *ccpage =
                static_cast<CcPage<KeyT, ValueT> *>(lru_page);
            next_page = ccpage->next_page_;
        }
        else
        {
            // go along with the lru list.
            next_page = lru_page->lru_next_;
        }
        size_t mem_decreased = 0;

        // clean page
        CcPage<KeyT, ValueT> *page =
            static_cast<CcPage<KeyT, ValueT> *>(lru_page);
        const KeyT old_page_key(page->FirstKey());
        auto [success, last_read_ts] =
            CleanPage(page, mem_decreased, free_cnt, clean_type, kickout_cc);

        // Output the operation result if the caller care it.
        if (is_success != nullptr)
        {
            *is_success = success;
        }

        if (page->Empty())  // remove page if empty
        {
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

            if (kickout_cc != nullptr && !success)
            {
                // If the caller care the clean status, reset the value of
                // @@next_page depending on the clean result:
                // 1) When the current page has been cleaned successfully, there
                // is no need to reset the value of @@next_page.
                // 2) When this current page has not been cleaned successfully,
                // should set the current page as the next_page.
                next_page = page;
            }
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

                if (kickout_cc != nullptr &&
                    (success && page == &page1_it->second || !success))
                {
                    // If the caller care the clean status, reset the value of
                    // @@next_page depending on the clean result:
                    // 1) When the current page has been cleaned successfully,
                    // if borrow from the next(that's mean page == page1),
                    // should set the current page as the @@next_page.
                    // 2) When this current page has not been cleaned
                    // successfully, should return the current page as the
                    // @@next_page.
                    next_page = page;
                }
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

                bool real_merge_with_prev = false;
                if (can_merge_with_prev)
                {
                    real_merge_with_prev = true;
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

                CcPage<KeyT, ValueT> *merged_page = &page1_it->second;
                CcPage<KeyT, ValueT> *discarded_page = &page2_it->second;

                if (kickout_cc == nullptr && next_page == discarded_page)
                {
                    // For this case, the next page to be cleaned comes from the
                    // lru list.
                    // The next_page is current page's lru_next_, and if the
                    // next_page == discarded_page, the discarded_page must be
                    // the next page of the current page, that is to say, the
                    // current page will merge with the next. So the current
                    // page must equal to the merged page, and should set the
                    // @@next_page is merged page.
                    assert(page == merged_page);
                    next_page = merged_page;
                }

                // merge page1 and page2
                MergePages(page1_it,
                           page2_it,
                           page1_last_read_ts,
                           page2_last_read_ts,
                           page,
                           mem_decreased);

                if (kickout_cc != nullptr)
                {
                    // For this case, should set the value of @@next_page
                    // depending on the clean result:
                    // 1) When the current page has been cleaned successfully,
                    // if merged with previous page, set the value is the
                    // merged_page's next_page_; if merged with next page, set
                    // the value is the merged_page itself.
                    // 2) When the current page has not been cleaned
                    // successfully. Should set the value is the merged_page
                    // itself no matter merged with previous page or merged
                    // with next page.
                    next_page = (real_merge_with_prev && success)
                                    ? merged_page->next_page_
                                    : merged_page;
                }
            }
        }

        shard_->DecrementMemory(mem_decreased);
        size_ -= free_cnt;
        if (free_cnt > 0)
        {
            ccm_has_full_entries_ = false;
        }

        return {free_cnt, next_page};
    }

    void Clean() override
    {
        size_t mem_decreased = 0;
        for (auto it = ccmp_.begin(); it != ccmp_.end(); it++)
        {
            //            const CcPage<KeyT, ValueT> &page = it->second;
            CcPage<KeyT, ValueT> &page = it->second;
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

    const Schema *KeySchema() const override
    {
        if (table_schema_ != nullptr)
        {
            if (table_name_.Type() == TableType::Secondary ||
                table_name_.Type() == TableType::UniqueSecondary)
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

    bool BulkEmplaceForTest(std::vector<KeyT *> &keys)
    {
        std::random_device rd;
        std::default_random_engine generator(rd());
        std::uniform_int_distribution<uint64_t> distribution(0, 0xFFFFFFFF);
        for (auto key : keys)
        {
            bool emplace = false;
            auto it = FindEmplace(*key, emplace, false);
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
            cce->parent_page_->last_dirty_commit_ts_ = std::max(
                cce->commit_ts_, cce->parent_page_->last_dirty_commit_ts_);
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
            shard_->UpdateLruList(cce->parent_page_, false);
            return {lb_it->first, cce};
        }
        else
        {
            // The input key does not exist.
            return {nullptr, nullptr};
        }
    }

    Iterator FindEmplace(const KeyT &key, bool force_emplace = false)
    {
        bool emplace;
        return FindEmplace(key, emplace, force_emplace);
    }

    /**
     * Find or Emplace the CcEntry with key @param key.
     *
     * @param key
     * @return The Iterator pointing to the target CcEntry
     */
    Iterator FindEmplace(const KeyT &key,
                         bool &emplace,
                         bool force_emplace = false)
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

        // catalog and range ccmap bypass shard memory limit. since checkpointer
        // may emplace ccentry into ccmap.
        if (shard_->Full())
        {
            // The shard has reached the maximal capacity. Tries to clean cc
            // entries that have been checkpointed but are not being
            // accessed by active tx's.
            shard_->Clean();
            if (shard_->Full() && !table_name_.IsMeta() && !force_emplace)
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
            shard_->UpdateLruList(cce_ptr->parent_page_, false);
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
            uint64_t new_last_commit_ts = 0;
            target_page->Split(
                new_page_keys, new_page_entries, new_last_commit_ts);

            const KeyT &key_of_new_page = *new_page_keys.begin();
            auto new_page_it = ccmp_.try_emplace(target_it,
                                                 key_of_new_page,
                                                 this,
                                                 std::move(new_page_keys),
                                                 std::move(new_page_entries),
                                                 target_page,
                                                 target_page->next_page_);
            CcPage<KeyT, ValueT> *new_page = &new_page_it->second;
            new_page->last_dirty_commit_ts_ = new_last_commit_ts;
            mem_increased += new_page->MemUsage();

            // insert new page into lru list right after old
            // page
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
        shard_->UpdateLruList(target_page, true);
        shard_->mem_usage_ += mem_increased;
        size_++;

        return Iterator(target_page, idx_in_page, &neg_inf_);
    }

    Iterator Emplace(const KeyT &key)
    {
        return FindEmplace(key);
    }

    bool CheckCceKeyLock(const CcEntry<KeyT, ValueT> *cce_ptr,
                         const PostWriteAllCc &req) const
    {
        // For PrepareCommit, the post-write-all request keeps write
        // intent or downgrades the write lock to the write intent.
        // For PostCommit, the post-write-all request release write
        // intent or write lock.
        return cce_ptr->key_lock_ptr_ &&
               ((cce_ptr->key_lock_ptr_->HasWriteLock() &&
                 cce_ptr->key_lock_ptr_->WriteLockTx() == req.Txn()) ||
                (cce_ptr->key_lock_ptr_->HasWriteIntent() &&
                 cce_ptr->key_lock_ptr_->WriteIntentTx() == req.Txn()));
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
                    // We're only copying the shared_ptr here so we exclude the
                    // actual payload size.
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
                    // We're only copying the shared_ptr here so we exclude the
                    // actual payload size.
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
                 RemoteScanSliceCache *remote_cache,
                 bool include_gap,
                 int64_t ng_term,
                 uint64_t read_ts,
                 bool is_read_snapshot,
                 bool keep_deleted,
                 bool is_ckpt_delta = false) const
    {
        uint32_t tuple_size = 0;

        if (is_read_snapshot)
        {
            VersionResultRecord<ValueT> v_rec;
            cce->MvccGet(read_ts, Type(), v_rec);

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
            key->Serialize(remote_cache->keys_);
            tuple_size += key->Size();

            if (v_rec.payload_status_ == RecordStatus::Normal ||
                (is_ckpt_delta &&
                 v_rec.payload_status_ == RecordStatus::Deleted))
            {
                if (v_rec.payload_ptr_ != nullptr)
                {
                    v_rec.payload_ptr_->Serialize(remote_cache->records_);
                    tuple_size += v_rec.payload_ptr_->SerializedLength();
                }
            }
            remote_cache->rec_status_.push_back(
                remote::ToRemoteType::ConvertRecordStatus(
                    v_rec.payload_status_));
            remote_cache->key_ts_.push_back(v_rec.commit_ts_);
        }
        else
        {
            if (!(cce->payload_status_ == RecordStatus::Normal ||
                  cce->payload_status_ == RecordStatus::Deleted &&
                      keep_deleted))
            {
                return;
            }
            key->Serialize(remote_cache->keys_);
            tuple_size += key->SerializedLength();

            if (cce->payload_status_ == RecordStatus::Normal ||
                (is_ckpt_delta &&
                 cce->payload_status_ == RecordStatus::Deleted))
            {
                if (cce->payload_ != nullptr)
                {
                    cce->payload_->Serialize(remote_cache->records_);
                    tuple_size += cce->payload_->SerializedLength();
                }
            }
            remote_cache->rec_status_.push_back(
                remote::ToRemoteType::ConvertRecordStatus(
                    cce->payload_status_));
            remote_cache->key_ts_.push_back(cce->commit_ts_);
        }

        if (include_gap)
        {
            remote_cache->gap_ts_.push_back(cce->gap_commit_ts_);
        }
        else
        {
            remote_cache->gap_ts_.push_back(0);
        }

        remote_cache->cce_ptr_.push_back(reinterpret_cast<uint64_t>(cce));
        remote_cache->term_.push_back(ng_term);
        // For remote scans, the returned cc entries' node group ID is set
        // on the sender side when the sender receives the response.

        remote_cache->cache_mem_size_ += tuple_size;
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

    void ScanGap(const KeyT *key,
                 CcEntry<KeyT, ValueT> *cce,
                 RemoteScanSliceCache *cache,
                 int64_t ng_term) const
    {
        cache->key_ts_.push_back(0);
        cache->gap_ts_.push_back(cce->gap_commit_ts_);

        cache->cce_ptr_.push_back(reinterpret_cast<uint64_t>(cce));
        cache->term_.push_back(ng_term);

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
        if (start_key == nullptr || *start_key < *key || *start_key == *key)
        {
            if (end_key)
            {
                return *key < *end_key;
            }
            else
            {
                // end key is pos inf
                return true;
            }
        }

        return false;
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
     * @param clean_type
     * @return The bool value stand for the clean status, if return false, it
     * mean that the target ccentry can not be clean, the caller should retry
     * the kickout request. Currently, only when clean_type is
     * CleanForSplitRange and CleanForAlterTable care this status.
     */
    std::pair<bool, uint64_t> CleanPage(CcPage<KeyT, ValueT> *page,
                                        size_t &mem_decreased,
                                        size_t &free_cnt,
                                        CleanType clean_type,
                                        KickoutCcEntryCc *kickout_cc = nullptr)
    {
        uint64_t last_read_ts = 0;
        std::vector<KeyT> &keys = page->keys_;
        std::vector<std::unique_ptr<CcEntry<KeyT, ValueT>>> &entries =
            page->entries_;
        const KeyT *start_key, *end_key;
        if (kickout_cc)
        {
            start_key = static_cast<const KeyT *>(kickout_cc->StartKey());
            end_key = static_cast<const KeyT *>(kickout_cc->EndKey());
        }
        auto key_insert_it = keys.begin();
        auto entry_insert_it = entries.begin();

        uint64_t last_commit_ts = 0;

        // Whether all ccentries whose commit_ts < @ckpt_ts have been cleaned.
        bool clean_success = true;
        auto key_it = keys.begin();
        auto entry_it = entries.begin();
        for (; key_it != keys.end(); key_it++, entry_it++)
        {
            CcEntry<KeyT, ValueT> *cce = entry_it->get();
            last_read_ts = std::max(last_read_ts, cce->last_read_ts_);

            bool can_be_clean = false;
            switch (clean_type)
            {
            case CleanType::CleanForFree:
                can_be_clean = cce->IsFree();
                break;
            case CleanType::CleanForSplitRange:
            {
                assert(kickout_cc);
                can_be_clean = KeyInRange(&(*key_it), start_key, end_key);
                break;
            }
            case CleanType::CleanForAlterTable:
            {
                assert(kickout_cc);
                can_be_clean = cce->commit_ts_ <= kickout_cc->CkptTs() &&
                               cce->commit_ts_ > 1 && cce->IsFree();
                break;
            }
            default:
            {
                LOG(ERROR) << "Unknown clean type: " << (uint32_t) clean_type;
                assert(false);
            }
            }

            if (can_be_clean)
            {
#ifdef RANGE_PARTITION_ENABLED
                bool kick_ret = shard_->local_shards_.KickoutRangeSlice(
                    table_name_, cc_ng_id_, *key_it);
                if (!kick_ret)
                {
                    // If the slice is being loaded or pinned, do not clean
                    // it.
                    *key_insert_it = std::move(*key_it);
                    *entry_insert_it = std::move(*entry_it);
                    key_insert_it++;
                    entry_insert_it++;
                    // record the commit_ts if the entry cannot be cleaned.
                    last_commit_ts = std::max(last_commit_ts, cce->commit_ts_);
                    // The ccentry that expect to clean cannot be kick out.
                    // In this branch, only when clean_type is
                    // CleanForSplitRange or CleanForAlterTable care this clean
                    // status
                    if (clean_type == CleanType::CleanForSplitRange ||
                        clean_type == CleanType::CleanForAlterTable)
                    {
                        clean_success = false;
                    }
                }
                else
                {
                    // free entries will be erased
                    mem_decreased += cce->GetCcEntryMemUsage() +
                                     key_it->MemUsage() - sizeof(KeyT);
                    free_cnt++;
                }
#else
                // free entries will be erased
                mem_decreased += cce->GetCcEntryMemUsage() +
                                 key_it->MemUsage() - sizeof(KeyT);
                free_cnt++;
#endif
            }
            else
            {
                // The ccentry that expect to clean cannot be kick out.
                // In this branch, only when clean_type is CleanForAlterTable
                // care this clean status. For CleanForSplitRange, if
                // can_be_clean is false, it mean that this ccentry is not the
                // target one, so it do not care this clean status.
                if (clean_type == CleanType::CleanForAlterTable &&
                    cce->commit_ts_ <= kickout_cc->CkptTs() &&
                    cce->commit_ts_ > 1)
                {
                    assert(!cce->IsFree());
                    clean_success = false;
                }
                // keep the entries that are not free
                *key_insert_it = std::move(*key_it);
                *entry_insert_it = std::move(*entry_it);
                key_insert_it++;
                entry_insert_it++;

                // record the commit_ts if the entry cannot be cleaned.
                last_commit_ts = std::max(last_commit_ts, cce->commit_ts_);
            }
        }
        keys.erase(key_insert_it, keys.end());
        entries.erase(entry_insert_it, entries.end());
        // During range split kickout, we might clean cc entries that are still
        // dirty from page. So the max dirty ts might decrease.
        page->last_dirty_commit_ts_ =
            std::min(last_commit_ts, page->last_dirty_commit_ts_);

        return {clean_success, last_read_ts};
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
            page2.last_dirty_commit_ts_ = std::max(page1.last_dirty_commit_ts_,
                                                   page2.last_dirty_commit_ts_);
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
            page1.last_dirty_commit_ts_ = std::max(page1.last_dirty_commit_ts_,
                                                   page2.last_dirty_commit_ts_);
        }

        // update page key in the map
        TryUpdatePageKey(page1_it);
        TryUpdatePageKey(page2_it);

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
     * Merge page1 and page2. Update the map and lru list after the
     * merge.
     *
     * @param page1_it The iterator to the merged page
     * @param page2_it The iterator to the discarded page
     * @param page
     * @param page_key
     * @param mem_decreased
     */
    void MergePages(
        typename std::map<KeyT, CcPage<KeyT, ValueT>>::iterator &page1_it,
        typename std::map<KeyT, CcPage<KeyT, ValueT>>::iterator &page2_it,
        uint64_t page1_last_read_ts,
        uint64_t page2_last_read_ts,
        CcPage<KeyT, ValueT> *page,
        size_t &mem_decreased)
    {
        CcPage<KeyT, ValueT> *page1 = &page1_it->second;
        CcPage<KeyT, ValueT> *page2 = &page2_it->second;

        auto merged_page_it = page1_it;
        auto discarded_page_it = page2_it;
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

        // Update the LRU list.
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

        // last_dirty_commit_ts_ of merged page will inherit the larger one.
        merged_page->last_dirty_commit_ts_ =
            std::max(merged_page->last_dirty_commit_ts_,
                     discarded_page->last_dirty_commit_ts_);
        // remove discarded page from the map
        ccmp_.erase(discarded_page_it);
        // modify merged page's key in the map
        if (merged_page->FirstKey() != merged_page_it->first)
        {
            // merged page key has changed
            TryUpdatePageKey(merged_page_it);
        }
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

    // When sample_pool_ is not nullptr, sample_pool_ points to
    // TemplateCcMapSamplePool in TableSchema.
    //
    // Notice that, for each given node, sample on one core only.
    // For other cores, sample_pool_ is nullptr.
    TemplateCcMapSamplePool<KeyT> *sample_pool_;
};
}  // namespace txservice
