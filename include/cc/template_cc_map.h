#pragma once

#include <butil/time.h>

#include <algorithm>  // std::max
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <tuple>
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
#include "scan.h"
#include "sharder.h"
#include "store/data_store_handler.h"
#include "table_statistics.h"
#include "tx_id.h"
#include "tx_key.h"
#include "tx_record.h"
#include "tx_service_metrics.h"
#include "tx_trace.h"
#include "type.h"

#ifdef RANGE_PARTITION_ENABLED
#include "range_slice.h"
#endif

#ifdef ON_KEY_OBJECT
DECLARE_bool(skip_kv);
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
          neg_inf_page_(this),
          pos_inf_page_(this),
          sample_pool_(nullptr)
    {
        neg_inf_page_.prev_page_ = nullptr;
        neg_inf_page_.next_page_ = &pos_inf_page_;
        pos_inf_page_.prev_page_ = &neg_inf_page_;
        pos_inf_page_.next_page_ = nullptr;

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
        neg_inf_.ClearLocks(*shard_, cc_ng_id_);
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
        CcPage<KeyT, ValueT> *ccp = nullptr;
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
                                            cce_ptr->PayloadStatus(),
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
                ccp = it.GetPage();

                if (cce_ptr != &neg_inf_ && *key_ptr == *target_key)
                {
                    // The floor entry's key is equal to the insert key. If the
                    // key is deleted, the insert becomes an update. Or the
                    // insert is aborted due to the duplidate key conflict.
                    if (cce_ptr->PayloadStatus() == RecordStatus::Deleted)
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
                ccp = it.GetPage();

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
            assert("Unsupported insert for phantom reads.");
        }
        else
        {
            if (!resume)
            {
                std::tie(acquired_lock, err_code) =
                    AcquireCceKeyLock(&cc_entry,
                                      ccp,
                                      cc_entry.PayloadStatus(),
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
                cc_entry.GetKeyLock()->SetWLockTs(lock_ts);

                // Updates last_vali_ts after successfully acquiring the write
                // lock such that it is no smaller than the current time of
                // the shard. The net effect is that the tx acquiring the write
                // lock is forced not to commit at a time earlier than the
                // clock of this cc node, even if the clock of the tx's
                // coordinator node drifts and falls behind. Checkpointing
                // relies on this property to avoid picking a checkpoint ts in
                // this shard that may overlap with the ongoing tx.
                acquire_key_result.last_vali_ts_ =
                    std::max(shard_->LastReadTs(), lock_ts);

                uint64_t curr_version_ts = cc_entry.CommitTs();
                // If a write is an update, delete or insert w/o duplicate, the
                // write must be preceded by a read, which acquires the write
                // intent and sets the payload status. The fact that the commit
                // ts is 0 means that this write disregards the existing value,
                // if there is any, and overwrites it. In such a case, we return
                // the current time via acquire_key_result.commit_ts_ so that
                // the tx's commit timestamp is bigger than the prior version
                // (if there is any).
                curr_version_ts =
                    curr_version_ts > 0 ? curr_version_ts : shard_->Now();
                acquire_key_result.commit_ts_ = curr_version_ts;

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
            assert("Unsupported insert for phantom reads.");
            return true;
        }
        else
        {
            // upsert and delete branch.
            CcEntry<KeyT, ValueT> *cce;
            const KeyT *write_key = nullptr;
            CcPage<KeyT, ValueT> *cc_page = nullptr;

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
                cc_page = it.GetPage();
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
                write_key = it->first;

                // Since this is a forward req, we assume this entry is not
                // visible on this ng yet so no need to check for lock.
            }
            else
            {
                cce = reinterpret_cast<CcEntry<KeyT, ValueT> *>(
                    cce_addr->CcePtr());

                NonBlockingLock *lk = cce->GetKeyLock();
                if (lk == nullptr || !lk->HasWriteLock() ||
                    lk->WriteLockTx() != txn)
                {
                    req.Result()->SetFinished();
                    return true;
                }

                cc_page = static_cast<CcPage<KeyT, ValueT> *>(cce->GetCcPage());
                assert(cc_page != nullptr);
                write_key = cc_page->KeyOfEntry(cce);
            }

            if (commit_ts > 0)
            {
#ifdef RANGE_PARTITION_ENABLED
                if (op_type == OperationType::Insert && cce->CommitTs() == 1)
                {
                    // At post write we have already loaded the latest version
                    // of cce into memory. So if commit ts is 1 (entry does not
                    // exist and has no previous version), that means it does
                    // not exist in data store at all.
                    cce->data_store_size_.store(0, std::memory_order_relaxed);
                }
#endif

#ifndef ON_KEY_OBJECT
                // for mvcc
                if (shard_->EnableMvcc())
                {
                    // Archives whose version are bigger than ccentry's ckpt_ts_
                    // may be in use by checkpointer and must not be deleted
                    // here. These expired archives will be deleted at next
                    // checkpoint.
                    uint64_t recycle_ts = shard_->GlobalMinSiTxStartTs();
                    cce->KickOutArchiveRecords(recycle_ts);
                    cce->ArchiveBeforeUpdate(Type());
                }
#endif

                if (commit_ts < cce->CommitTs())
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
                    if (is_del)
                    {
                        cce->payload_ = nullptr;
                    }
                    else if (payload_str == nullptr)
                    {
#ifndef ON_KEY_OBJECT
                        if (cce->payload_.use_count() == 1)
                        {
                            *(cce->payload_) = *commit_val;
                        }
                        else
                        {
                            cce->payload_ =
                                std::make_shared<ValueT>(*commit_val);
                        }
#else
                        assert(false);
                        cce->payload_ = std::make_unique<ValueT>(*commit_val);
#endif
                    }
                    else
                    {
                        size_t offset = 0;
#ifndef ON_KEY_OBJECT
                        if (cce->payload_.use_count() != 1)
                        {
                            cce->payload_ = std::make_shared<ValueT>();
                        }
#else
                        assert(false);
                        if (cce->payload_ == nullptr)
                        {
                            cce->payload_ = std::make_unique<ValueT>();
                        }
#endif
                        cce->payload_->Deserialize(payload_str->data(), offset);
                    }
                }

                RecordStatus cce_old_status = cce->PayloadStatus();
                RecordStatus new_status =
                    is_del ? RecordStatus::Deleted : RecordStatus::Normal;
                cce->SetCommitTsPayloadStatus(commit_ts, new_status);

                if (is_upload && req.IsInitialInsert())
                {
                    // Updates the ckpt ts after commit ts is set.
                    cce->SetCkptTs(1U);
                }

                DLOG_IF(INFO, TRACE_OCC_ERR)
                    << "PostWriteCc, txn:" << txn << " ,cce: " << cce
                    << " ,commit_ts: " << commit_ts;

                if (commit_ts > last_dirty_commit_ts_)
                {
                    last_dirty_commit_ts_ = commit_ts;
                }
                if (commit_ts > cc_page->last_dirty_commit_ts_)
                {
                    cc_page->last_dirty_commit_ts_ = commit_ts;
                }
                if (shard_->realtime_sampling_ && sample_pool_)
                {
                    assert(write_key != nullptr);

                    if (op_type == OperationType::Insert ||
                        op_type == OperationType::Upsert)
                    {
                        sample_pool_->OnInsert(*write_key, table_schema_);
                    }
                    else if (op_type == OperationType::Delete)
                    {
                        if (cce_old_status == RecordStatus::Normal)
                        {
                            sample_pool_->OnDelete(*write_key, table_schema_);
                        }
                    }
                }
            }

            ReleaseCceLock(cce->GetKeyLock(),
                           cce,
                           txn,
                           req.NodeGroupId(),
                           LockType::WriteLock);
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
        CcPage<KeyT, ValueT> *ccp = nullptr;
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
                                            cce_ptr->PayloadStatus(),
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
                ccp = it.GetPage();

                if (cce_ptr != &neg_inf_ && *key_ptr == *target_key)
                {
                    // The floor entry's key is equal to the insert key. If the
                    // key is deleted, the insert becomes an update. Or the
                    // insert is aborted due to the duplidate key conflict.
                    if (cce_ptr->PayloadStatus() != RecordStatus::Deleted)
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
                ccp = it.GetPage();

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

        if (will_insert)
        {
            assert("Unsupported insert for phantom reads.");
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
                                      ccp,
                                      cc_entry.PayloadStatus(),
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
                if (cc_entry.PayloadStatus() != RecordStatus::Deleted)
                {
                    assert(acquired_lock == LockType::WriteIntent ||
                           acquired_lock == LockType::WriteLock);
                }

                // Updates last_vali_ts such that it is no smaller than (1) all
                // read transactions that have read the item in all shards, and
                // (2) the local time.
                if (shard_->core_id_ == 0)
                {
                    acquire_all_result.last_vali_ts_ = shard_->LastReadTs();
                }
                else
                {
                    acquire_all_result.last_vali_ts_ = std::max(
                        shard_->LastReadTs(), acquire_all_result.last_vali_ts_);
                }

                if (shard_->core_id_ == tx_core_id)
                {
                    acquire_all_result.local_cce_addr_.SetCce(
                        reinterpret_cast<uint64_t>(cce_ptr),
                        ng_term,
                        req.NodeGroupId(),
                        shard_->LocalCoreId());
                    acquire_all_result.commit_ts_ = cc_entry.CommitTs();
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
        if (req.Key() != nullptr)
        {
            target_key = static_cast<const KeyT *>(req.Key());
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
        if (req.Payload() != nullptr)
        {
            payload = static_cast<const ValueT *>(req.Payload());
        }
        // commit_ts = 0 means transaction failed (e.g. failed at prepare
        // phase), we have nothing to upload, only need to release write intent.
        else if (req.CommitTs() > 0 &&
                 req.CommitType() != PostWriteType::DowngradeLock)
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
            assert("Unsupported insert for phantom reads.");
        }
        else
        {
            LockType lk_type = LockType::NoLock;
            if (cce_ptr->GetKeyLock() != nullptr)
            {
                // AcquireAllCc only acquire WriteIntent or WriteLock
                auto [w_tx, w_type] = cce_ptr->GetKeyLock()->WriteTx();

                if (w_tx == txn)
                {
                    if (w_type == NonBlockingLock::WriteLockType::WriteLock)
                    {
                        lk_type = LockType::WriteLock;
                    }
                    else if (w_type ==
                             NonBlockingLock::WriteLockType::WriteIntent)
                    {
                        lk_type = LockType::WriteIntent;
                    }
                }
            }

            if (lk_type != LockType::NoLock)
            {
                if (commit_ts > 0 &&
                    req.CommitType() != PostWriteType::DowngradeLock)
                {
#ifndef ON_KEY_OBJECT
                    cce_ptr->payload_ = std::make_shared<ValueT>(*payload);
#else
                    cce_ptr->payload_ = std::make_unique<ValueT>(*payload);
#endif
                    // A prepare commit request only installs the dirty value,
                    // and does not change the record status and commit_ts.
                    if (req.CommitType() != PostWriteType::PrepareCommit)
                    {
#ifndef ON_KEY_OBJECT
                        RecordStatus status =
                            (req.OpType() == OperationType::Delete ||
                             req.OpType() == OperationType::DropTable)
                                ? RecordStatus::Deleted
                                : RecordStatus::Normal;
#else
                        // no need to delete catalog
                        RecordStatus status = RecordStatus::Normal;
#endif
                        cce_ptr->SetCommitTsPayloadStatus(commit_ts, status);
                    }
                }

                // When commit_ts = 0, the request removes the write lock
                // without installing a new value.

                if (req.CommitType() == PostWriteType::PrepareCommit ||
                    req.CommitType() == PostWriteType::DowngradeLock)
                {
                    // For PrepareCommit and DowngradeLock, the
                    // post-write-all request keeps write intent or downgrades
                    // the write lock to the write intent.
                    if (lk_type == LockType::WriteLock)
                    {
                        DowngradeCceKeyWriteLock(cce_ptr, txn);
                    }
                }
                else
                {
                    assert(req.CommitType() == PostWriteType::Commit ||
                           req.CommitType() == PostWriteType::PostCommit);

                    // For PostCommit or Commit, the post-write-all request
                    // releases the write lock.
                    ReleaseCceLock(cce_ptr->GetKeyLock(),
                                   cce_ptr,
                                   txn,
                                   req.NodeGroupId(),
                                   lk_type);
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
        if (cc_entry.PayloadStatus() != RecordStatus::Unknown && key_ts > 0 &&
            key_ts != cc_entry.CommitTs())
        {
            ReleaseCceLock(
                cc_entry.GetKeyLock(), &cc_entry, txn, req.NodeGroupId());
            // broken repeatable read, set error.
            hd_res->SetError(
                CcErrorCode::VALIDATION_FAILED_FOR_VERSION_MISMATCH);
            DLOG_IF(INFO, TRACE_OCC_ERR)
                << "PostReadCc, occ_err, txn:" << txn << " ,cce: " << &cc_entry
                << " ,payload_status: "
                << static_cast<int>(cc_entry.PayloadStatus())
                << " ,key_ts: " << key_ts
                << " ,cc_entry.commit_ts_: " << cc_entry.CommitTs();
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
                assert("Unsupported phantom reads.");
            }

            NonBlockingLock *key_lock = cc_entry.GetKeyLock();
            if (key_ts > 0)
            {
                shard_->UpdateLastReadTs(commit_ts);

                // Using locking protocol, this never happens.
                if (key_lock != nullptr && key_lock->HasWriteLock() &&
                    key_lock->WriteLockTx() != txn)
                {
                    int64_t ng_term =
                        Sharder::Instance().LeaderTerm(req.NodeGroupId());
                    shard_->CheckRecoverTx(
                        key_lock->WriteLockTx(), req.NodeGroupId(), ng_term);
                    conflicting_txs.AddConflictingTx(key_lock->WriteLockTx());

                    DLOG_IF(INFO, TRACE_OCC_ERR)
                        << "PostReadCc, occ_err, txn:" << txn
                        << " ,cce: " << &cc_entry
                        << " ,key conflict tx: " << key_lock->WriteLockTx();
                }
            }

            ReleaseCceLock(key_lock, &cc_entry, txn, req.NodeGroupId());

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
        CcPage<KeyT, ValueT> *ccp = nullptr;

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
                    cce->GetKeyLock()->ReleaseReadLock(req.Txn(), shard_);
                    acquired_lock = LockType::NoLock;
                    err_code = CcErrorCode::NO_ERROR;
                }
                else
                {
                    std::tie(acquired_lock, err_code) =
                        LockHandleForResumedRequest(cce,
                                                    cce->PayloadStatus(),
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
                Iterator it = Find(*look_key);
                cce = it->second;
                ccp = it.GetPage();

                // collect metrics: slice cache hits
                if (metrics::enable_cache_hit_rate)
                {
                    auto meter = shard_->GetMeter();
                    if (cce != nullptr)
                    {
                        meter->Collect(
                            metrics::NAME_CACHE_HIT_OR_MISS_TOTAL, 1, "hits");
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
                                ccp = it.GetPage();
                                if (cce == nullptr)
                                {
                                    hd_res->SetError(
                                        CcErrorCode::OUT_OF_MEMORY);
                                    return true;
                                }

                                if (cce->PayloadStatus() ==
                                    RecordStatus::Unknown)
                                {
                                    cce->SetCommitTsPayloadStatus(
                                        1U, RecordStatus::Deleted);
                                    cce->SetCkptTs(1U);
                                }
                                else
                                {
                                    assert(cce->CommitTs() > 1);
                                }
                            }
                            else
                            {
                                it = Find(*look_key);
                                cce = it->second;
                                ccp = it.GetPage();

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
                        ccp = it.GetPage();
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
                ccp = it.GetPage();

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
                    cce->PayloadStatus() == RecordStatus::Unknown)
                {
                    cce->SetCommitTsPayloadStatus(1U, RecordStatus::Deleted);
                    cce->SetCkptTs(1U);
                }

                if (metrics::enable_cache_hit_rate)
                {
                    auto meter = shard_->GetMeter();
                    if (cce->PayloadStatus() == RecordStatus::Unknown)
                    {
                        meter->Collect(
                            metrics::NAME_CACHE_HIT_OR_MISS_TOTAL, 1, "miss");
                    }
                    else
                    {
                        meter->Collect(
                            metrics::NAME_CACHE_HIT_OR_MISS_TOTAL, 1, "hits");
                    }
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
                                      ccp,
                                      cce->PayloadStatus(),
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

            if (cce->PayloadStatus() == RecordStatus::Unknown)
            {
                cce->payload_ = std::move(tmp_payload);
                cce->SetCommitTsPayloadStatus(req.ReadTimestamp(),
                                              tmp_payload_status);
            }
#ifndef ON_KEY_OBJECT
            else if (shard_->EnableMvcc() &&
                     cce->CommitTs() > req.ReadTimestamp())
            {
                // Trying to insert the record to backfill into archives is
                // needed, because the entry may be created when executing
                // "ReplayLogCc".
                cce->AddArchiveRecord(std::move(tmp_payload),
                                      tmp_payload_status,
                                      req.ReadTimestamp());
            }
            // Updates the ckpt timestamp such that it is no smaller than the
            // backfill version.
            cce->SetCkptTs(req.ReadTimestamp());

            // Refill mvcc archives
            if (shard_->EnableMvcc() &&
                (req.Type() == ReadType::OutsideNormal ||
                 req.Type() == ReadType::OutsideDeleted) &&
                req.ArchivesPtr() != nullptr && req.ArchivesPtr()->size() > 0)
            {
                cce->AddArchiveRecords(*req.ArchivesPtr());
            }
#endif
        }

        if (is_read_snapshot)
        {
#ifndef ON_KEY_OBJECT
            assert(req.Type() == ReadType::Inside);

            VersionResultRecord<ValueT> v_rec;
            cce->MvccGet(
                req.ReadTimestamp(), Type(), shard_->LastReadTs(), v_rec);
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

            if (metrics::enable_cache_hit_rate)
            {
                auto meter = shard_->GetMeter();
                if (v_rec.payload_status_ == RecordStatus::Unknown ||
                    v_rec.payload_status_ == RecordStatus::VersionUnknown ||
                    v_rec.payload_status_ == RecordStatus::BaseVersionMiss)
                {
                    meter->Collect(
                        metrics::NAME_CACHE_HIT_OR_MISS_TOTAL, 1, "miss");
                }
            }
            hd_res->Value().ts_ = v_rec.commit_ts_;
            hd_res->Value().rec_status_ = v_rec.payload_status_;
            hd_res->SetFinished();
            return true;
#endif
        }
        else if (cce->PayloadStatus() == RecordStatus::Normal &&
                 (req.Type() == ReadType::Inside || cce->CommitTs() > 1))
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
                cce->CommitTs() > 0 && cce->CommitTs() < req.ReadTimestamp())
            {
                // When backtracking the content of primary key record according
                // to the secondary index key, if the commit_ts of this
                // primary key record is smaller than the commit_ts of the
                // secondary index key("req.ReadTimestamp()"), it means that the
                // current primary key has not been updated and there must be a
                // PostWriteCc request waiting to be executed. So, this read
                // should wait for the PostWriteCc completed.
                req.SetIsWaitForPostWrite(true);
                NonBlockingLock *key_lock = cce->GetKeyLock();
                assert(key_lock != nullptr && key_lock->HasWriteLock() &&
                       key_lock->WriteLockTx() != req.Txn());
                // Put the request to top of key lock's blocking queue with
                // acquring readlock. And then should release the readlock
                // before handling this requst when PostWriteCc finished.
                key_lock->InsertBlockingQueue(&req, LockType::ReadLock);
                shard_->CheckRecoverTx(key_lock->WriteLockTx(), ng_id, ng_term);

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

        hd_res->Value().ts_ = cce->CommitTs();
        hd_res->Value().rec_status_ = cce->PayloadStatus();
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

        if (cce->PayloadStatus() == RecordStatus::Unknown)
        {
            assert(cce->CommitTs() == 1);
            if (req.RecordStatus() == RecordStatus::Normal)
            {
                size_t offset = 0;
#ifndef ON_KEY_OBJECT
                cce->payload_ = std::make_shared<ValueT>();
#else
                assert(false);
                cce->payload_ = std::make_unique<ValueT>();
#endif
                cce->payload_->Deserialize(req.rec_str_->data(), offset);
            }
            cce->SetCommitTsPayloadStatus(req.CommitTs(), req.RecordStatus());
        }
#ifndef ON_KEY_OBJECT
        else if (shard_->EnableMvcc() && cce->CommitTs() > req.CommitTs())
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
        }
        // Updates the ckpt timestamp such that it is no smaller than the
        // backfill version.
        cce->SetCkptTs(req.CommitTs());

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
#endif

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
                keep_deleted,
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
        CcEntry<KeyT, ValueT> *cce_last = nullptr;
        CcPage<KeyT, ValueT> *ccp_last = nullptr;

        if (req.CcePtr() != nullptr)
        {
            CcEntry<KeyT, ValueT> *cce =
                static_cast<CcEntry<KeyT, ValueT> *>(req.CcePtr());
            CcPage<KeyT, ValueT> *ccp =
                static_cast<CcPage<KeyT, ValueT> *>(cce->GetCcPage());
            assert(ccp != nullptr);
            scan_ccm_it = Iterator(cce, ccp, &neg_inf_);
            key_ptr = scan_ccm_it->first;
            ScanType scan_type = req.CcePtrScanType();

            req.SetCcePtr(nullptr);
            req.SetCcePtrScanType(ScanType::ScanUnknow);

            if (req.IsWaitForPostWrite())
            {
                req.SetIsWaitForPostWrite(false);
                cce->GetKeyLock()->ReleaseReadLock(req.Txn(), shard_);
            }
            else
            {
                // Lock has been acquired, UpsertLockHoldingTx
                auto lock_pair =
                    LockHandleForResumedRequest(cce,
                                                cce->PayloadStatus(),
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

            cce_last = cce;
            ccp_last = ccp;
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
            CcEntry<KeyT, ValueT> *cce = scan_ccm_it->second;
            CcPage<KeyT, ValueT> *ccp = scan_ccm_it.GetPage();
            ScanType scan_type = start_pair.second;

            req.SetCcePtr(cce);
            req.SetCcePtrScanType(scan_type);

            if (scan_type != ScanType::ScanGap)
            {
                auto lock_pair = AcquireCceKeyLock(cce,
                                                   ccp,
                                                   cce->PayloadStatus(),
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
            cce_last = cce;
            ccp_last = ccp;
        }

        if (req.direct_ == ScanDirection::Forward)
        {
            ++scan_ccm_it;

            Iterator pos_inf_it = End();
            for (; scan_ccm_it != pos_inf_it && !typed_cache->Full();
                 ++scan_ccm_it)
            {
                key_ptr = scan_ccm_it->first;
                CcEntry<KeyT, ValueT> *cce = scan_ccm_it->second;
                CcPage<KeyT, ValueT> *ccp = scan_ccm_it.GetPage();
#ifdef ON_KEY_OBJECT
                if (!FilterRecord(key_ptr,
                                  cce,
                                  req.GetRedisObjectType(),
                                  req.GetRedisScanPattern()))
                {
                    continue;
                }
#endif
                req.SetCcePtr(cce);
                req.SetCcePtrScanType(ScanType::ScanBoth);

                auto lock_pair = AcquireCceKeyLock(cce,
                                                   ccp,
                                                   cce->PayloadStatus(),
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

                cce_last = cce;
                ccp_last = ccp;
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
                CcEntry<KeyT, ValueT> *cce = scan_ccm_it->second;
                CcPage<KeyT, ValueT> *ccp = scan_ccm_it.GetPage();
#ifdef ON_KEY_OBJECT
                if (!FilterRecord(key_ptr,
                                  cce,
                                  req.GetRedisObjectType(),
                                  req.GetRedisScanPattern()))
                {
                    continue;
                }
#endif
                req.SetCcePtr(cce);
                req.SetCcePtrScanType(ScanType::ScanBoth);

                auto lock_pair = AcquireCceKeyLock(cce,
                                                   ccp,
                                                   cce->PayloadStatus(),
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

                cce_last = cce;
                ccp_last = ccp;
            }
        }

        if (cce_last != nullptr)
        {
            bool add_intent =
                cce_last->GetOrCreateKeyLock(shard_, this, ccp_last)
                    .AcquireReadIntent(req.Txn());

            if (add_intent)
            {
                shard_->UpsertLockHoldingTx(req.Txn(),
                                            tx_term,
                                            cce_last,
                                            false,
                                            ng_id,
                                            table_name_.Type());
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
        CcEntry<KeyT, ValueT> *prior_cce;
        CcEntry<KeyT, ValueT> *cce_last = nullptr;
        CcPage<KeyT, ValueT> *ccp_last = nullptr;

        if (req.CcePtr() != nullptr)
        {
            prior_cce = static_cast<CcEntry<KeyT, ValueT> *>(req.CcePtr());
            CcPage<KeyT, ValueT> *ccp =
                static_cast<CcPage<KeyT, ValueT> *>(prior_cce->GetCcPage());
            assert(ccp != nullptr);
            scan_ccm_it = Iterator(prior_cce, ccp, &neg_inf_);
            const KeyT *prior_cce_key = scan_ccm_it->first;
            ScanType scan_type = req.CcePtrScanType();

            req.SetCcePtr(nullptr);
            req.SetCcePtrScanType(ScanType::ScanUnknow);

            if (req.IsWaitForPostWrite())
            {
                req.SetIsWaitForPostWrite(false);
                prior_cce->GetKeyLock()->ReleaseReadLock(req.Txn(), shard_);
            }
            else
            {
                // Lock has been acquired, UpsertLockHoldingTx
                auto lock_pair =
                    LockHandleForResumedRequest(prior_cce,
                                                prior_cce->PayloadStatus(),
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

            cce_last = prior_cce;
            ccp_last = ccp;
        }
        else
        {
            prior_cce = reinterpret_cast<CcEntry<KeyT, ValueT> *>(
                typed_cache->Last()->cce_addr_.CcePtr());
            CcPage<KeyT, ValueT> *ccp =
                static_cast<CcPage<KeyT, ValueT> *>(prior_cce->GetCcPage());
            assert(ccp != nullptr);
            scan_ccm_it = Iterator(prior_cce, ccp, &neg_inf_);
            typed_cache->Reset();

            if (LockTypeUtil::DeduceLockType(cc_op,
                                             req.Isolation(),
                                             req.Protocol(),
                                             req.IsCoveringKeys()) ==
                LockType::NoLock)
            {
                ReleaseCceLock(prior_cce->GetKeyLock(),
                               prior_cce,
                               req.Txn(),
                               ng_id,
                               LockType::ReadIntent);
            }
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
                CcPage<KeyT, ValueT> *ccp = scan_ccm_it.GetPage();

                if (req.is_ckpt_delta_ && cce->IsPersistent())
                {
                    // If this is a scan for modified records since last
                    // checkpoint, skips those that have been checkpointed.
                    continue;
                }
#ifdef ON_KEY_OBJECT
                if (!FilterRecord(key,
                                  cce,
                                  req.GetRedisObjectType(),
                                  req.GetRedisScanPattern()))
                {
                    continue;
                }
#endif

                req.SetCcePtr(cce);
                req.SetCcePtrScanType(ScanType::ScanBoth);

                auto lock_pair = AcquireCceKeyLock(cce,
                                                   ccp,
                                                   cce->PayloadStatus(),
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

                cce_last = cce;
                ccp_last = ccp;
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
                CcPage<KeyT, ValueT> *ccp = scan_ccm_it.GetPage();
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
#ifdef ON_KEY_OBJECT
                    if (!FilterRecord(key,
                                      cce,
                                      req.GetRedisObjectType(),
                                      req.GetRedisScanPattern()))
                    {
                        continue;
                    }
#endif
                    req.SetCcePtr(cce);
                    req.SetCcePtrScanType(ScanType::ScanBoth);

                    auto lock_pair = AcquireCceKeyLock(cce,
                                                       ccp,
                                                       cce->PayloadStatus(),
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

                    cce_last = cce;
                    ccp_last = ccp;
                }
            }
        }

        if (cce_last != nullptr)
        {
            bool add_intent =
                cce_last->GetOrCreateKeyLock(shard_, this, ccp_last)
                    .AcquireReadIntent(req.Txn());

            if (add_intent)
            {
                shard_->UpsertLockHoldingTx(req.Txn(),
                                            tx_term,
                                            cce_last,
                                            false,
                                            ng_id,
                                            table_name_.Type());
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
                keep_deleted,
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
                keep_deleted,
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
        CcEntry<KeyT, ValueT> *cce_last = nullptr;
        CcPage<KeyT, ValueT> *ccp_last = nullptr;

        if (req.CcePtr(shard_->LocalCoreId()) != nullptr)
        {
            CcEntry<KeyT, ValueT> *cce = static_cast<CcEntry<KeyT, ValueT> *>(
                req.CcePtr(shard_->LocalCoreId()));
            CcPage<KeyT, ValueT> *ccp =
                static_cast<CcPage<KeyT, ValueT> *>(cce->GetCcPage());
            assert(ccp != nullptr);
            scan_ccm_it = Iterator(cce, ccp, &neg_inf_);
            key_ptr = scan_ccm_it->first;
            ScanType scan_type = req.CcePtrScanType(shard_->LocalCoreId());

            req.SetCcePtr(nullptr, shard_->LocalCoreId());
            req.SetCcePtrScanType(ScanType::ScanUnknow, shard_->LocalCoreId());

            if (req.IsWaitForPostWrite(shard_->LocalCoreId()))
            {
                req.SetIsWaitForPostWrite(false, shard_->LocalCoreId());
                cce->GetKeyLock()->ReleaseReadLock(req.Txn(), shard_);
            }
            else
            {
                // Lock has been acquired, UpsertLockHoldingTx
                auto lock_pair =
                    LockHandleForResumedRequest(cce,
                                                cce->PayloadStatus(),
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
            cce_last = cce;
            ccp_last = ccp;
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
            CcEntry<KeyT, ValueT> *cce = scan_ccm_it->second;
            CcPage<KeyT, ValueT> *ccp = scan_ccm_it.GetPage();

            req.SetCcePtr(cce, shard_->LocalCoreId());
            req.SetCcePtrScanType(scan_type, shard_->LocalCoreId());

            if (scan_type != ScanType::ScanGap)
            {
                auto lock_pair = AcquireCceKeyLock(cce,
                                                   ccp,
                                                   cce->PayloadStatus(),
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
            cce_last = cce;
            ccp_last = ccp;
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
                CcEntry<KeyT, ValueT> *cce = scan_ccm_it->second;
                CcPage<KeyT, ValueT> *ccp = scan_ccm_it.GetPage();

                if (req.is_ckpt_delta_ && cce->IsPersistent())
                {
                    continue;
                }
#ifdef ON_KEY_OBJECT
                if (!FilterRecord(key_ptr,
                                  cce,
                                  req.GetRedisObjectType(),
                                  req.GetRedisScanPattern()))
                {
                    continue;
                }
#endif
                req.SetCcePtr(cce, shard_->LocalCoreId());
                req.SetCcePtrScanType(ScanType::ScanBoth,
                                      shard_->LocalCoreId());

                auto lock_pair = AcquireCceKeyLock(cce,
                                                   ccp,
                                                   cce->PayloadStatus(),
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

                cce_last = cce;
                ccp_last = ccp;
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
                CcEntry<KeyT, ValueT> *cce = scan_ccm_it->second;
                CcPage<KeyT, ValueT> *ccp = scan_ccm_it.GetPage();

                if (req.is_ckpt_delta_ && cce->IsPersistent())
                {
                    continue;
                }
#ifdef ON_KEY_OBJECT
                if (!FilterRecord(key_ptr,
                                  cce,
                                  req.GetRedisObjectType(),
                                  req.GetRedisScanPattern()))
                {
                    continue;
                }
#endif
                req.SetCcePtr(cce, shard_->LocalCoreId());
                req.SetCcePtrScanType(ScanType::ScanBoth,
                                      shard_->LocalCoreId());

                auto lock_pair = AcquireCceKeyLock(cce,
                                                   ccp,
                                                   cce->PayloadStatus(),
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

                cce_last = cce;
                ccp_last = ccp;
            }
        }

        if (cce_last != nullptr)
        {
            bool add_intent =
                cce_last->GetOrCreateKeyLock(shard_, this, ccp_last)
                    .AcquireReadIntent(req.Txn());

            if (add_intent)
            {
                shard_->UpsertLockHoldingTx(req.Txn(),
                                            tx_term,
                                            cce_last,
                                            false,
                                            ng_id,
                                            table_name_.Type());
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
        CcEntry<KeyT, ValueT> *cce_last = nullptr;
        CcPage<KeyT, ValueT> *ccp_last = nullptr;

        if (req.CcePtr() != nullptr)
        {
            prior_cce = static_cast<CcEntry<KeyT, ValueT> *>(req.CcePtr());
            ScanType scan_type = req.CcePtrScanType();
            CcPage<KeyT, ValueT> *ccp =
                static_cast<CcPage<KeyT, ValueT> *>(prior_cce->GetCcPage());
            assert(ccp != nullptr);
            scan_ccm_it = Iterator(prior_cce, ccp, &neg_inf_);
            const KeyT *prior_cce_key = scan_ccm_it->first;

            req.SetCcePtr(nullptr);
            req.SetCcePtrScanType(ScanType::ScanUnknow);

            if (req.IsWaitForPostWrite())
            {
                req.SetIsWaitForPostWrite(false);
                prior_cce->GetKeyLock()->ReleaseReadLock(req.Txn(), shard_);
            }
            else
            {
                // Lock has been acquired, UpsertLockHoldingTx
                auto lock_pair =
                    LockHandleForResumedRequest(prior_cce,
                                                prior_cce->PayloadStatus(),
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
            cce_last = prior_cce;
            ccp_last = ccp;
        }
        else
        {
            prior_cce = reinterpret_cast<CcEntry<KeyT, ValueT> *>(
                req.PriorCceAddr().CcePtr());
            CcPage<KeyT, ValueT> *ccp =
                static_cast<CcPage<KeyT, ValueT> *>(prior_cce->GetCcPage());
            assert(ccp != nullptr);
            scan_ccm_it = Iterator(prior_cce, ccp, &neg_inf_);

            if (LockTypeUtil::DeduceLockType(cc_op,
                                             req.Isolation(),
                                             req.Protocol(),
                                             req.IsCoveringKeys()) ==
                LockType::NoLock)
            {
                ReleaseCceLock(prior_cce->GetKeyLock(),
                               prior_cce,
                               req.Txn(),
                               ng_id,
                               LockType::ReadIntent);
            }
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
                CcPage<KeyT, ValueT> *ccp = scan_ccm_it.GetPage();

                if (req.is_ckpt_delta_ && cce->IsPersistent())
                {
                    continue;
                }

#ifdef ON_KEY_OBJECT
                if (!FilterRecord(key,
                                  cce,
                                  req.GetRedisObjectType(),
                                  req.GetRedisScanPattern()))
                {
                    continue;
                }
#endif
                req.SetCcePtr(cce);
                req.SetCcePtrScanType(ScanType::ScanBoth);

                auto lock_pair = AcquireCceKeyLock(cce,
                                                   ccp,
                                                   cce->PayloadStatus(),
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

                cce_last = cce;
                ccp_last = ccp;
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
                CcPage<KeyT, ValueT> *ccp = scan_ccm_it.GetPage();

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
                    cce_last = cce;
                    break;
                }
                else
                {
#ifdef ON_KEY_OBJECT
                    if (!FilterRecord(key,
                                      cce,
                                      req.GetRedisObjectType(),
                                      req.GetRedisScanPattern()))
                    {
                        continue;
                    }
#endif
                    req.SetCcePtr(cce);
                    req.SetCcePtrScanType(ScanType::ScanBoth);

                    auto lock_pair = AcquireCceKeyLock(cce,
                                                       ccp,
                                                       cce->PayloadStatus(),
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

                    cce_last = cce;
                    ccp_last = ccp;
                }
            }
        }

        if (cce_last != nullptr)
        {
            bool add_intent =
                cce_last->GetOrCreateKeyLock(shard_, this, ccp_last)
                    .AcquireReadIntent(req.Txn());

            if (add_intent)
            {
                shard_->UpsertLockHoldingTx(req.Txn(),
                                            tx_term,
                                            cce_last,
                                            false,
                                            ng_id,
                                            table_name_.Type());
            }
        }

        req.Result()->SetFinished();
        return true;
    }

    bool Execute(ScanSliceCc &req) override
    {
        int64_t ng_term = Sharder::Instance().LeaderTerm(req.NodeGroupId());
        if (ng_term < 0 ||
            (req.RangeCcNgTerm() > 0 && req.RangeCcNgTerm() != ng_term))
        {
            return req.SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
        }
        if (req.SendResponseIfFinished())
        {
            req.UnpinSlices();
            return true;
        }

        CcOperation cc_op;
        bool is_read_snapshot =
            req.Isolation() == IsolationLevel::Snapshot && !req.IsForWrite();
        if (table_name_.Type() == TableType::Secondary ||
            table_name_.Type() == TableType::UniqueSecondary)
        {
            cc_op = CcOperation::ReadSkIndex;
        }
        else
        {
            cc_op = req.IsForWrite() ? CcOperation::ReadForWrite
                                     : CcOperation::Read;
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

        auto last_cce_of_cache =
            [&req, scan_cache, remote_scan_cache]() -> CcEntry<KeyT, ValueT> *
        {
            if (req.IsLocal())
            {
                if (scan_cache->Last())
                {
                    return reinterpret_cast<CcEntry<KeyT, ValueT> *>(
                        scan_cache->Last()->cce_addr_.CcePtr());
                }
                else
                {
                    return nullptr;
                }
            }
            else
            {
                if (remote_scan_cache->Size() > 0)
                {
                    return reinterpret_cast<CcEntry<KeyT, ValueT> *>(
                        remote_scan_cache->LastCce());
                }
                else
                {
                    return nullptr;
                }
            }
        };

        if (req.SliceId().Slice() == nullptr)
        {
            // The scan slice request is first dispatched to one core, which
            // pins the slice in memory. After the slice is pinned, the request
            // is dispatched to other cores to scan in parallel. The slice is
            // unpinned by the last core finishing the scan batch.
            RangeSliceOpStatus pin_status;
            const StoreSlice *last_pinned_slice;
            uint8_t max_pin_cnt = 1;
            if (req_end_key == nullptr &&
                req.PrefetchSize() < shard_->core_cnt_)
            {
                if (req.PrefetchSize() < UINT8_MAX)
                {
                    max_pin_cnt += req.PrefetchSize();
                }
                else
                {
                    max_pin_cnt = req.PrefetchSize();
                }
            }
            else
            {
                max_pin_cnt = shard_->core_cnt_;
            }
            RangeSliceId slice_id = shard_->PinRangeSlices(
                table_name_,
                req.NodeGroupId(),
                ng_term,
                KeySchema(),
                RecordSchema(),
                schema_ts_,
                table_schema_->GetKVCatalogInfo(),
                req.RangeId(),
                *req_start_key,
                req.StartInclusive(),
                req_end_key,
                req.EndInclusive(),
                &req,
                false,
                req.PrefetchSize(),
                max_pin_cnt,
                req.Direction() == ScanDirection::Forward,
                pin_status,
                last_pinned_slice);

            if (pin_status == RangeSliceOpStatus::Retry)
            {
                shard_->Enqueue(shard_->LocalCoreId(), &req);
                return false;
            }
            else if (pin_status == RangeSliceOpStatus::Delay)
            {
                if (slice_id.Range()->HasLock())
                {
                    return req.SetError(CcErrorCode::OUT_OF_MEMORY);
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
            else if (pin_status == RangeSliceOpStatus::Error ||
                     pin_status == RangeSliceOpStatus::NotOwner)
            {
                // If the pin operation returns an error, the data store
                // is inaccessible.
                return req.SetError(CcErrorCode::PIN_RANGE_SLICE_FAILED);
            }

            req.PinSlices(slice_id, last_pinned_slice);
            // Update unfinished cnt before dispatching to remaining cores.
            req.SetUnfinishedCoreCnt(req.GetShardCount());

            // Dispatches to remaining cores to scan pinned slice(s) in
            // parallel.
            for (uint16_t core_id = 0; core_id < shard_->local_shards_.Count();
                 ++core_id)
            {
                if (core_id == shard_->core_id_)
                {
                    continue;
                }

                shard_->local_shards_.EnqueueCcRequest(
                    shard_->core_id_, core_id, &req);
            }
        }

        Iterator scan_ccm_it;
        const KeyT *cce_key = nullptr;
        CcEntry<KeyT, ValueT> *cce = nullptr;
        uint32_t ng_id = req.NodeGroupId();
        int64_t tx_term = req.TxTerm();

        enum struct ScanReturnType
        {
            Success,
            Blocked,
            Yield,
            Error,
        };

        auto scan_tuple_func =
            [&, this](
                const KeyT *cce_key,
                CcEntry<KeyT, ValueT> *cce,
                CcPage<KeyT, ValueT> *ccp,
                ScanType scan_type) -> std::pair<ScanReturnType, CcErrorCode>
        {
            auto lock_pair = AcquireCceKeyLock(cce,
                                               ccp,
                                               cce->PayloadStatus(),
                                               &req,
                                               ng_id,
                                               ng_term,
                                               tx_term,
                                               cc_op,
                                               req.Isolation(),
                                               req.Protocol(),
                                               req.ReadTimestamp(),
                                               req.IsCoveringKeys());
            switch (lock_pair.second)
            {
            case CcErrorCode::NO_ERROR:
                break;
            case CcErrorCode::MVCC_READ_MUST_WAIT_WRITE:
            {
                req.SetBlockingInfo(
                    shard_->core_id_,
                    reinterpret_cast<uint64_t>(cce),
                    scan_type,
                    ScanSliceCc::ScanBlockingType::BlockOnFuture);
                return {ScanReturnType::Blocked, CcErrorCode::NO_ERROR};
            }
            case CcErrorCode::ACQUIRE_LOCK_BLOCKED:
            {
                req.SetBlockingInfo(shard_->core_id_,
                                    reinterpret_cast<uint64_t>(cce),
                                    scan_type,
                                    ScanSliceCc::ScanBlockingType::BlockOnLock);
                req.SetRangeCcNgTerm(ng_term);
                // Lock fail should stop the execution of current
                // CC request since it's already in blocking queue.
                return {ScanReturnType::Blocked, CcErrorCode::NO_ERROR};
            }
            default:
            {
                // lock confilct: back off and retry.
                return {ScanReturnType::Error, lock_pair.second};
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

            return {ScanReturnType::Success, CcErrorCode::NO_ERROR};
        };

        uint64_t addr = req.CceAddr(core_id);
        if (addr != 0)
        {
            cce = reinterpret_cast<CcEntry<KeyT, ValueT> *>(addr);
        }

        if (cce != nullptr)
        {
            auto [blocking_type, scan_type] = req.BlockingPair(core_id);
            CcPage<KeyT, ValueT> *ccp =
                static_cast<CcPage<KeyT, ValueT> *>(cce->GetCcPage());
            assert(ccp != nullptr);
            scan_ccm_it = Iterator(cce, ccp, &neg_inf_);
            cce_key = scan_ccm_it->first;

            if (blocking_type == ScanSliceCc::ScanBlockingType::NoBlocking)
            {
                // This is a resumed scan slice cc. If the scan itself won't
                // lock the cce, we will put a read intent on the last cce so
                // that it can't be kicked from memory and this cce addr is
                // valid. This lock should be released when the scan slicecc
                // resumes.
                if (LockTypeUtil::DeduceLockType(cc_op,
                                                 req.Isolation(),
                                                 req.Protocol(),
                                                 req.IsCoveringKeys()) ==
                    LockType::NoLock)
                {
                    ReleaseCceLock(cce->GetKeyLock(),
                                   cce,
                                   req.Txn(),
                                   ng_id,
                                   LockType::ReadIntent);
                }
            }
            else
            {
                bool is_locked = false;

                if (blocking_type ==
                    ScanSliceCc::ScanBlockingType::BlockOnFuture)
                {
                    // The scan was blocked because it intends to scan a key's
                    // version that has not been committed.
                    cce->GetKeyLock()->ReleaseReadLock(req.Txn(), shard_);
                }
                else
                {
                    // The scan was blocked because of read-write conflicts. The
                    // read lock/write intent has been acquired, updates the
                    // lock holding tx collection in this shard.
                    auto lock_pair =
                        LockHandleForResumedRequest(cce,
                                                    cce->PayloadStatus(),
                                                    &req,
                                                    ng_id,
                                                    ng_term,
                                                    tx_term,
                                                    cc_op,
                                                    req.Isolation(),
                                                    req.Protocol(),
                                                    req.ReadTimestamp(),
                                                    req.IsCoveringKeys());

                    if (lock_pair.second != CcErrorCode::NO_ERROR)
                    {
                        assert(lock_pair.second ==
                               CcErrorCode::MVCC_READ_FOR_WRITE_CONFLICT);
                        if (req.SetError(lock_pair.second))
                        {
                            req.UnpinSlices();
                            return true;
                        }
                        else
                        {
                            return false;
                        }
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
            if (req.Direction() == ScanDirection::Forward)
            {
                scan_ccm_it++;
            }
            else
            {
                scan_ccm_it--;
            }
        }
        else
        {
            std::pair<Iterator, ScanType> start_pair =
                req.Direction() == ScanDirection::Forward
                    ? ForwardScanStart(*req_start_key, req.StartInclusive())
                    : BackwardScanStart(*req_start_key, req.StartInclusive());

            scan_ccm_it = start_pair.first;
            cce_key = scan_ccm_it->first;
            cce = scan_ccm_it->second;
            if (start_pair.second == ScanType::ScanGap)
            {
                if (req.Direction() == ScanDirection::Forward)
                {
                    scan_ccm_it++;
                }
                else
                {
                    scan_ccm_it--;
                }
            }
        }

        RangeScanSliceResult &slice_result = hd_res->Value();
        auto [final_end_key, end_finalized] = slice_result.PeekLastKey();
        if (req.Direction() == ScanDirection::Forward)
        {
            const StoreSlice *last_slice = req.LastPinnedSlice();

            // The scan at core 0 sets the scan's end key. By default, the
            // scan's end is the exclusive end of the slice or the request's
            // specified end key, whichever is smaller. In case keys in the
            // slice are too many to fit into the scan cache, the key right
            // after the last scanned tuple at core 0 becomes the exclusive end
            // of scans at other cores. In such a case, it is mandatory that all
            // keys smaller than the end key at other cores are returned in this
            // batch. So, scans at other cores may slightly exceed the scan
            // cache's capacity.

            const KeyT *initial_end = nullptr;
            bool init_end_inclusive = false;

            // Given the scan batch's final end key, deduces the local scan's
            // end and inclusiveness.
            auto deduce_scan_end =
                [](const KeyT *batch_end_key,
                   const KeyT *req_end_key,
                   bool req_inclusive) -> std::pair<const KeyT *, bool>
            {
                const KeyT *end = nullptr;
                bool inclusive = false;

                // The scan batch's end key has been finalized. If the final
                // end key is null, it means that either the request specifies
                // the end key, which falls into the slice, or the scanned slice
                // is the last ending with positive infinity.
                if (batch_end_key == nullptr)
                {
                    if (req_end_key != nullptr)
                    {
                        end = req_end_key;
                        inclusive = req_inclusive;
                    }
                    else
                    {
                        end = PositiveInfinity<KeyT>::Instance();
                        inclusive = false;
                    }
                }
                else
                {
                    end = batch_end_key;
                    inclusive = false;
                }

                return {end, inclusive};
            };

            if (!end_finalized)
            {
                // This scan batch's end key has not been set. Takes the smaller
                // of the slice's last key and the request's end key as the
                // local scan's initial end. The initial end may be modified, if
                // another core finishes earlier and finalizes the batch's end
                // before this core. The final end may be smaller or greater
                // than the initial end.
                const KeyT *slice_end =
                    static_cast<const KeyT *>(last_slice->EndKey());
                if (slice_end == nullptr)
                {
                    slice_end = PositiveInfinity<KeyT>::Instance();
                }

                // If the request specifies the end key and it falls into the
                // slice, initializes the local scan's end to the request's end
                // key. Or, the scan end is the slice's end.
                if (req_end_key != nullptr &&
                    (*req_end_key < *slice_end ||
                     (*req_end_key == *slice_end && !req.EndInclusive())))
                {
                    initial_end = req_end_key;
                    init_end_inclusive = req.EndInclusive();
                }
                else
                {
                    initial_end = slice_end;
                    init_end_inclusive = false;
                }
            }
            else
            {
                // This scan batch's end key has been finalized by one of the
                // cores. Deduces the local scan's end and inclusiveness.
                std::tie(initial_end, init_end_inclusive) =
                    deduce_scan_end(static_cast<const KeyT *>(final_end_key),
                                    req_end_key,
                                    req.EndInclusive());
            }

            auto scan_loop_func = [&, this](const KeyT *end_key,
                                            bool inclusive,
                                            bool end_finalized)
                -> std::pair<ScanReturnType, CcErrorCode>
            {
                Iterator pos_inf_it = End();
                const KeyT *cce_key = scan_ccm_it->first;
                CcEntry<KeyT, ValueT> *cce = scan_ccm_it->second;
                CcPage<KeyT, ValueT> *ccp = scan_ccm_it.GetPage();

                auto is_cache_full =
                    [&req, scan_cache, remote_scan_cache]() -> bool {
                    return req.IsLocal() ? scan_cache->IsFull()
                                         : remote_scan_cache->IsFull();
                };

                while (scan_ccm_it != pos_inf_it &&
                       (end_finalized || !is_cache_full()) &&
                       (*cce_key < *end_key ||
                        (inclusive && *cce_key == *end_key)))
                {
                    auto [scan_ret, err_code] =
                        scan_tuple_func(cce_key, cce, ccp, ScanType::ScanBoth);

                    if (scan_ret != ScanReturnType::Success)
                    {
                        return {scan_ret, err_code};
                    }

                    ++scan_ccm_it;
                    cce_key = scan_ccm_it->first;
                    cce = scan_ccm_it->second;
                    ccp = scan_ccm_it.GetPage();
                }

                return {ScanReturnType::Success, CcErrorCode::NO_ERROR};
            };

            auto [scan_ret, err] =
                scan_loop_func(initial_end, init_end_inclusive, end_finalized);
            switch (scan_ret)
            {
            case ScanReturnType::Blocked:
                return false;
            case ScanReturnType::Error:
                if (req.SetError(err))
                {
                    req.UnpinSlices();
                    return true;
                }
                else
                {
                    return false;
                }
            case ScanReturnType::Yield:
                shard_->Enqueue(shard_->core_id_, &req);
                return false;
            default:
                break;
            }

            // If the end of this scan batch is not finalized when the local
            // scan at this core started, tries to set the batch's end using the
            // local end. If another core has finalized the batch's end, the
            // scan at this core may need to be adjusted: if the batch's final
            // end is less than the end at this core, keys after the final end
            // needs to be removed from the local scan cache; if the batch's
            // final end is greater than the end of this core, keys smaller than
            // the batch's final end but greater than the local end need to be
            // included in the local scan cache.
            if (!end_finalized)
            {
                const KeyT *local_end = nullptr;
                SlicePosition slice_position;

                // scan_ccm_it points to the entry after the last scanned tuple.
                // If the slice ends with positive infinity and has been fully
                // scanned, scan_ccm_it would point to positive infinity.
                auto pos_inf_it = End();
                if (scan_ccm_it != pos_inf_it &&
                    (*scan_ccm_it->first < *initial_end ||
                     (init_end_inclusive &&
                      *scan_ccm_it->first == *initial_end)))
                {
                    // The slice is too large. The scan has not fully scanned
                    // the slice, before reaching the cache's size limit.
                    // Pretends the slice's exclusive end to be the key after
                    // the last scanned tuple, from which the next scan batch
                    // resume.
                    local_end = scan_ccm_it->first;
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
                    if (initial_end == PositiveInfinity<KeyT>::Instance() ||
                        req_end_key == initial_end)
                    {
                        slice_position = SlicePosition::LastSlice;
                    }
                    else
                    {
                        // The local scan end must be the end of the slice.
                        local_end = initial_end;

                        const KeyT *range_end = static_cast<const KeyT *>(
                            req.SliceId().RangeEndKey());
                        if (range_end != nullptr && *initial_end == *range_end)
                        {
                            slice_position = SlicePosition::LastSliceInRange;
                        }
                        else
                        {
                            slice_position = SlicePosition::Middle;
                        }
                    }
                }

                auto [batch_end, set_success] =
                    slice_result.UpdateLastKey(local_end, slice_position);

                if (set_success)
                {
                    req.SetRangeCcNgTerm(ng_term);
                }
                else
                {
                    // The local scan tries to set the scan batch's end, but the
                    // scan at another core have set the batch's end. The scan
                    // results need to be adjusted, if the results include the
                    // keys greater than the batch's end, or the results miss
                    // some keys smaller than the batch's end.
                    auto [end_key, end_inclusive] =
                        deduce_scan_end(static_cast<const KeyT *>(batch_end),
                                        req_end_key,
                                        req.EndInclusive());
                    size_t trailing_cnt = 0;

                    // Excludes keys from the scan cache greater than the
                    // batch's end.
                    if (req.IsLocal())
                    {
                        while (scan_cache->Size() > 0)
                        {
                            const KeyT *last_key =
                                &scan_cache->Last()->KeyObj();
                            if (*end_key < *last_key ||
                                (*end_key == *last_key && !end_inclusive))
                            {
                                ++trailing_cnt;
                                scan_cache->RemoveLast();
                            }
                            else
                            {
                                break;
                            }
                        }
                    }
                    else
                    {
                        while (remote_scan_cache->Size() > 0)
                        {
                            // Cc entry pointers here are always valid since
                            // the slices are still pinned so the cce cannot
                            // be kicked from memory regardless of the lock
                            // type.
                            CcEntry<KeyT, ValueT> *last_remote_cce =
                                reinterpret_cast<CcEntry<KeyT, ValueT> *>(
                                    remote_scan_cache->LastCce());
                            while (scan_ccm_it->second != last_remote_cce)
                            {
                                // As long as remote scan cache is not empty,
                                // iterator should not reach neg inf.
                                --scan_ccm_it;
                                assert(scan_ccm_it != Begin());
                            }
                            const KeyT *last_key =
                                static_cast<const KeyT *>(scan_ccm_it->first);
                            if (*end_key < *last_key ||
                                (*end_key == *last_key && !end_inclusive))
                            {
                                trailing_cnt++;
                                remote_scan_cache->RemoveLast();
                            }
                            else
                            {
                                // Reset iterator to the key after the last
                                // scanned tuple since we might need to continue
                                // scanning if trailing_cnt == 0.
                                ++scan_ccm_it;
                                break;
                            }
                        }
                    }

                    // If no key is removed from the scan cache, it's possible
                    // that the local scan may miss keys smaller than the
                    // batch's end. Re-scans the cc map using the batch's end.
                    if (trailing_cnt == 0)
                    {
                        auto [scan_ret, err] =
                            scan_loop_func(end_key, end_inclusive, true);
                        switch (scan_ret)
                        {
                        case ScanReturnType::Blocked:
                            return false;
                        case ScanReturnType::Error:
                            if (req.SetError(err))
                            {
                                req.UnpinSlices();
                                return true;
                            }
                            else
                            {
                                return false;
                            }
                        case ScanReturnType::Yield:
                            shard_->Enqueue(shard_->core_id_, &req);
                            return false;
                        default:
                            break;
                        }
                    }
                }
            }

            // Sets the iterator to the last cce, which may need to be pinned to
            // resume the next scan batch.
            if (CcEntry<KeyT, ValueT> *last_cce = last_cce_of_cache(); last_cce)
            {
                while (scan_ccm_it->second != last_cce)
                {
                    --scan_ccm_it;
                }
            }
        }
        else
        {
            const StoreSlice *last_slice = req.LastPinnedSlice();

            const KeyT *initial_end = nullptr;
            bool init_end_inclusive = false;

            auto deduce_scan_end =
                [](const KeyT *batch_end_key,
                   const KeyT *req_end_key,
                   bool req_inclusive) -> std::pair<const KeyT *, bool>
            {
                const KeyT *end = nullptr;
                bool inclusive = false;

                // The scan batch's end key has been finalized. If the final
                // end key is null, it means that either the request specifies
                // the end key, which falls into the slice, or the scanned slice
                // is the first starting from negative infinity.
                if (batch_end_key == nullptr)
                {
                    if (req_end_key != nullptr)
                    {
                        end = req_end_key;
                        inclusive = req_inclusive;
                    }
                    else
                    {
                        end = NegativeInfinity<KeyT>::Instance();
                        inclusive = true;
                    }
                }
                else
                {
                    end = batch_end_key;
                    inclusive = true;
                }

                return {end, inclusive};
            };

            if (!end_finalized)
            {
                const KeyT *slice_begin =
                    static_cast<const KeyT *>(last_slice->StartKey());
                if (slice_begin == nullptr)
                {
                    slice_begin = NegativeInfinity<KeyT>::Instance();
                }

                if (req_end_key != nullptr && (*slice_begin < *req_end_key ||
                                               *slice_begin == *req_end_key))
                {
                    initial_end = req_end_key;
                    init_end_inclusive = req.EndInclusive();
                }
                else
                {
                    initial_end = slice_begin;
                    init_end_inclusive = true;
                }
            }
            else
            {
                // This scan batch's end key has been finalized by one of the
                // cores. Deduces the local scan's end and inclusiveness.
                std::tie(initial_end, init_end_inclusive) =
                    deduce_scan_end(static_cast<const KeyT *>(final_end_key),
                                    req_end_key,
                                    req.EndInclusive());
            }

            auto scan_loop_func = [&, this](const KeyT *end_key,
                                            bool inclusive,
                                            bool end_finalized)
                -> std::pair<ScanReturnType, CcErrorCode>
            {
                Iterator neg_inf_it = Begin();
                const KeyT *cce_key = scan_ccm_it->first;
                CcEntry<KeyT, ValueT> *cce = scan_ccm_it->second;
                CcPage<KeyT, ValueT> *ccp = scan_ccm_it.GetPage();

                auto is_cache_full =
                    [&req, scan_cache, remote_scan_cache]() -> bool {
                    return req.IsLocal() ? scan_cache->IsFull()
                                         : remote_scan_cache->IsFull();
                };

                while (scan_ccm_it != neg_inf_it &&
                       (end_finalized || !is_cache_full()) &&
                       (*end_key < *cce_key ||
                        (inclusive && *end_key == *cce_key)))
                {
                    auto [scan_ret, err_code] =
                        scan_tuple_func(cce_key, cce, ccp, ScanType::ScanBoth);

                    if (scan_ret != ScanReturnType::Success)
                    {
                        return {scan_ret, err_code};
                    }

                    --scan_ccm_it;
                    cce_key = scan_ccm_it->first;
                    cce = scan_ccm_it->second;
                    ccp = scan_ccm_it.GetPage();
                }

                return {ScanReturnType::Success, CcErrorCode::NO_ERROR};
            };

            auto [scan_ret, err] =
                scan_loop_func(initial_end, init_end_inclusive, end_finalized);
            switch (scan_ret)
            {
            case ScanReturnType::Blocked:
                return false;
            case ScanReturnType::Error:
                if (req.SetError(err))
                {
                    req.UnpinSlices();
                    return true;
                }
                else
                {
                    return false;
                }
            case ScanReturnType::Yield:
                shard_->Enqueue(shard_->core_id_, &req);
                return false;
            default:
                break;
            }

            // If the end of this scan batch is not finalized when the local
            // scan at this core started, tries to set the batch's end using the
            // local end. If another core has finalized the batch's end, the
            // scan at this core may need to be adjusted: if the batch's final
            // end is less than the end at this core, keys before the final end
            // needs to be removed from the local scan cache; if the batch's
            // final end is smaller than the end of this core, keys greater than
            // the batch's final end but less than the local end need to be
            // included in the local scan cache.

            if (!end_finalized)
            {
                const KeyT *local_end = nullptr;
                SlicePosition slice_position;

                // scan_ccm_it points to the entry before the last scanned
                // tuple.
                auto neg_inf_it = Begin();
                if (scan_ccm_it != neg_inf_it &&
                    (*initial_end < *scan_ccm_it->first ||
                     (init_end_inclusive &&
                      *scan_ccm_it->first == *initial_end)))
                {
                    // The slice is too large. The scan has not fully scanned
                    // the slice, before reaching the cache's size limit.
                    // Pretends the slice's inclusive start to be the last
                    // scanned key, from which the next scan batch resumes.
                    ++scan_ccm_it;
                    local_end = scan_ccm_it->first;
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
                    if (initial_end == NegativeInfinity<KeyT>::Instance() ||
                        req_end_key == initial_end)
                    {
                        slice_position = SlicePosition::FirstSlice;
                    }
                    else
                    {
                        // The local scan end must be the start of the slice.
                        local_end = initial_end;

                        const KeyT *range_start = static_cast<const KeyT *>(
                            req.SliceId().RangeStartKey());
                        if (range_start != nullptr &&
                            *initial_end == *range_start)
                        {
                            slice_position = SlicePosition::FirstSliceInRange;
                        }
                        else
                        {
                            slice_position = SlicePosition::Middle;
                        }
                    }
                }

                auto [batch_end, set_success] =
                    slice_result.UpdateLastKey(local_end, slice_position);

                if (set_success)
                {
                    req.SetRangeCcNgTerm(ng_term);
                }
                else
                {
                    // The local scan tries to set the scan batch's end, but the
                    // scan at another core have set the batch's end. The scan
                    // results need to be adjusted, if the results include the
                    // keys smaller than the batch's end, or the results miss
                    // some keys greater than the batch's end.
                    auto [end_key, end_inclusive] =
                        deduce_scan_end(static_cast<const KeyT *>(batch_end),
                                        req_end_key,
                                        req.EndInclusive());
                    size_t trailing_cnt = 0;

                    // Excludes keys from the scan cache smaller than the
                    // batch's end.
                    if (req.IsLocal())
                    {
                        while (scan_cache->Size() > 0)
                        {
                            const KeyT *last_key =
                                &scan_cache->Last()->KeyObj();
                            if (*last_key < *end_key ||
                                (*last_key == *end_key && !end_inclusive))
                            {
                                ++trailing_cnt;
                                scan_cache->RemoveLast();
                            }
                            else
                            {
                                break;
                            }
                        }
                    }
                    else
                    {
                        while (remote_scan_cache->Size() > 0)
                        {
                            // Cc entry pointers here are always valid since
                            // the slices are still pinned so the cce cannot
                            // be kicked from memory regardless of the lock
                            // type.
                            CcEntry<KeyT, ValueT> *last_remote_cce =
                                reinterpret_cast<CcEntry<KeyT, ValueT> *>(
                                    remote_scan_cache->LastCce());
                            while (scan_ccm_it->second != last_remote_cce)
                            {
                                // As long as remote scan cache is not empty,
                                // iterator should not reach pos inf.
                                ++scan_ccm_it;
                                assert(scan_ccm_it != End());
                            }
                            const KeyT *last_key =
                                static_cast<const KeyT *>(scan_ccm_it->first);
                            if (*last_key < *end_key ||
                                (*last_key == *end_key && !end_inclusive))
                            {
                                trailing_cnt++;
                                remote_scan_cache->RemoveLast();
                            }
                            else
                            {
                                // Reset iterator to the key after the last
                                // scanned tuple since we might need to continue
                                // scanning if trailing_cnt == 0.
                                --scan_ccm_it;
                                break;
                            }
                        }
                    }

                    // If no key is removed from the scan cache, it's possible
                    // that the local scan may miss keys greater than the
                    // batch's end. Re-scans the cc map using the batch's end.
                    if (trailing_cnt == 0)
                    {
                        auto [scan_ret, err] =
                            scan_loop_func(end_key, end_inclusive, true);
                        switch (scan_ret)
                        {
                        case ScanReturnType::Blocked:
                            return false;
                        case ScanReturnType::Error:
                            if (req.SetError(err))
                            {
                                req.UnpinSlices();
                                return true;
                            }
                            else
                            {
                                return false;
                            }
                        case ScanReturnType::Yield:
                            shard_->Enqueue(shard_->core_id_, &req);
                            return false;
                        default:
                            break;
                        }
                    }
                }
            }

            // Sets the iterator to the last cce, which may need to be pinned to
            // resume the next scan batch.
            if (CcEntry<KeyT, ValueT> *last_cce = last_cce_of_cache(); last_cce)
            {
                while (scan_ccm_it->second != last_cce)
                {
                    ++scan_ccm_it;
                }
            }
        }

        if (slice_result.slice_position_ == SlicePosition::Middle)
        {
            // When the scan batch stops in the middle of the range,
            // acquires the read intent on the last scanned key to prevent
            // if from kicking out. The next scan batch will resume from the
            // last key without searching the cc map.
            if (CcEntry<KeyT, ValueT> *last_cce = last_cce_of_cache(); last_cce)
            {
                CcPage<KeyT, ValueT> *last_ccp = scan_ccm_it.GetPage();
                bool add_intent =
                    last_cce->GetOrCreateKeyLock(shard_, this, last_ccp)
                        .AcquireReadIntent(req.Txn());
                if (add_intent)
                {
                    shard_->UpsertLockHoldingTx(req.Txn(),
                                                tx_term,
                                                last_cce,
                                                false,
                                                ng_id,
                                                table_name_.Type());
                }
            }
        }

        if (req.IsLocal())
        {
            req.GetLocalScanner()->CommitAtCore(core_id);
        }
        if (req.SetFinish())
        {
            if (req.Result()->Value().is_local_)
            {
                req.UnpinSlices();
                return true;
            }
            else if (req.IsResponseSender(shard_->core_id_))
            {
                req.SendResponseIfFinished();
                req.UnpinSlices();
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

#ifdef RANGE_PARTITION_ENABLED
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

        const KeyT *const req_start_key =
            req.start_key_ ? static_cast<const KeyT *>(req.start_key_)
                           : NegativeInfinity<KeyT>::Instance();
        const KeyT *const req_end_key =
            req.end_key_ ? static_cast<const KeyT *>(req.end_key_)
                         : PositiveInfinity<KeyT>::Instance();

        Iterator it;
        Iterator end_it;
        if (req.IsDrained(shard_->core_id_))
        {
            // scan is already finished on this core
            req.SetFinish(shard_->core_id_);
            return false;
        }

        auto &pause_key_and_is_drained = req.PausePos(shard_->core_id_);

        // Slice_id is not set, We need to pin slice.
        if (req.export_base_table_rec_if_need_ &&
            nullptr == req.slice_ids_[shard_->core_id_].Slice())
        {
            const KeyT *slice_start_key = nullptr;
            if (pause_key_and_is_drained.first != nullptr)
            {
                // Pin slice failed in the previous execution. Now, retry to pin
                slice_start_key = static_cast<const KeyT *>(
                    pause_key_and_is_drained.first.get());
            }
            else
            {
                // first enter
                slice_start_key = req_start_key;
            }

            bool pin_next_slice = true;

            // FIXME(lokax): Only loop X times to avoid blocking TxProcesser
            // when the range has many empty slices.
            while (pin_next_slice)
            {
                assert(req.slice_ids_[shard_->core_id_].Slice() == nullptr);

                RangeSliceOpStatus pin_status;
                RangeSliceId new_slice_id =
                    shard_->PinRangeSlice(table_name_,
                                          req.NodeGroupId(),
                                          req.NodeGroupTerm(),
                                          KeySchema(),
                                          RecordSchema(),
                                          schema_ts_,
                                          table_schema_->GetKVCatalogInfo(),
                                          *slice_start_key,
                                          true,
                                          &req,
                                          pin_status,
                                          true,
                                          32);

                switch (pin_status)
                {
                case RangeSliceOpStatus::Successful:
                {
                    break;
                }
                case RangeSliceOpStatus::BlockedOnLoad:
                {
                    pause_key_and_is_drained.first = slice_start_key->Clone();
                    return false;
                }
                case RangeSliceOpStatus::Retry:
                {
                    pause_key_and_is_drained.first = slice_start_key->Clone();
                    shard_->Enqueue(shard_->LocalCoreId(), &req);
                    return false;
                }
                default:
                {
                    assert(pin_status == RangeSliceOpStatus::Error);
                    req.SetError(CcErrorCode::PIN_RANGE_SLICE_FAILED);
                    return true;
                }
                }

                // The slice has been pinned.
                if (slice_start_key == NegativeInfinity<KeyT>::Instance())
                {
                    it = Begin();
                    it++;
                }
                else
                {
                    it = LowerBound(*slice_start_key);
                    if (it->first == NegativeInfinity<KeyT>::Instance())
                    {
                        it++;
                    }
                }

                const KeyT *slice_end_key =
                    new_slice_id.Slice()->EndKey()
                        ? static_cast<const KeyT *>(
                              new_slice_id.Slice()->EndKey())
                        : PositiveInfinity<KeyT>::Instance();

                if (slice_end_key == PositiveInfinity<KeyT>::Instance())
                {
                    end_it = End();
                }
                else
                {
                    std::pair<Iterator, ScanType> end_pair =
                        ForwardScanStart(*slice_end_key, true);
                    end_it = end_pair.first;
                    if (end_pair.second == ScanType::ScanGap)
                    {
                        ++end_it;
                    }
                }

                if (it == end_it && (!(*slice_end_key == *req_end_key)))
                {
                    // This slice is empty, pin next slice.
                    slice_start_key = slice_end_key;
                    new_slice_id.Unpin();
                    new_slice_id.Reset();
                }
                else
                {
                    // This slice is not empty or this empty slice is last slice
                    // of range. We stop to loop.
                    req.slice_ids_[shard_->core_id_] = new_slice_id;
                    pin_next_slice = false;
                }
            }
        }
        else
        {
            if (pause_key_and_is_drained.first == nullptr)
            {
                // If this is a new scan cc, start from the specified start
                // key or negative inf.
                if (req_start_key == NegativeInfinity<KeyT>::Instance())
                {
                    it = Begin();
                    it++;
                }
                else
                {
                    it = LowerBound(*req_start_key);
                    if (it->first == NegativeInfinity<KeyT>::Instance())
                    {
                        it++;
                    }
                }
            }
            else
            {
                const KeyT *pause_key = static_cast<const KeyT *>(
                    pause_key_and_is_drained.first.get());
                it = LowerBound(*pause_key);
            }

            const KeyT *search_end_key = req_end_key;

            if (req.export_base_table_rec_if_need_)
            {
                assert(req.slice_ids_[shard_->core_id_].Slice() != nullptr);

                search_end_key =
                    req.slice_ids_[shard_->core_id_].Slice()->EndKey()
                        ? static_cast<const KeyT *>(
                              req.slice_ids_[shard_->core_id_]
                                  .Slice()
                                  ->EndKey())
                        : PositiveInfinity<KeyT>::Instance();
            }

            if (search_end_key == PositiveInfinity<KeyT>::Instance())
            {
                end_it = End();
            }
            else
            {
                std::pair<Iterator, ScanType> end_pair =
                    ForwardScanStart(*search_end_key, true);
                end_it = end_pair.first;
                if (end_pair.second == ScanType::ScanGap)
                {
                    ++end_it;
                }
            }
        }

        // Since we might skip the page that end_it is on if it's not updated
        // since last ckpt, it might skip end_it. If the last page is skipped it
        // will be set as the first entry on the next page. Also check if (it ==
        // end_it_next_page_it).
        Iterator end_it_next_page_it = end_it;
        if (!req.export_base_table_rec_if_need_)
        {
            if (end_it_next_page_it != End())
            {
                CcPage<KeyT, ValueT> *ccp = end_it_next_page_it.GetPage();
                assert(ccp != nullptr);
                if (ccp->next_page_ == PagePosInf())
                {
                    end_it_next_page_it = End();
                }
                else
                {
                    end_it_next_page_it =
                        Iterator(ccp->next_page_, 0, &neg_inf_);
                }
            }
        }

        uint64_t recycle_ts = 1U;
        if (shard_->EnableMvcc() && !req.skip_archived_key_)
        {
            recycle_ts = shard_->GlobalMinSiTxStartTs();
        }

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
            CcPage<KeyT, ValueT> *ccp = it.GetPage();
            assert(ccp);

            if (!req.export_base_table_rec_if_need_)
            {
                if (ccp->last_dirty_commit_ts_ <= from_ts)
                {
                    // Skip the pages that have no updates since last data sync.
                    if (ccp->next_page_ == PagePosInf())
                    {
                        it = End();
                    }
                    else
                    {
                        it = Iterator(ccp->next_page_, 0, &neg_inf_);
                    }
                    continue;
                }
            }

            if (shard_->EnableMvcc() && !req.skip_archived_key_)
            {
                cce->KickOutArchiveRecords(recycle_ts);
            }

            if (!req.export_base_table_rec_if_need_)
            {
                if (cce->NeedCkpt())
                {
                    bool need_export = true;
                    if (cce->data_store_size_.load(std::memory_order_acquire) ==
                        INT32_MAX)
                    {
                        // Load data store size by pinning the slice. Data
                        // store size is required to decide slice & range
                        // update plan.
                        RangeSliceOpStatus pin_status;
                        RangeSliceId slice_id = shard_->PinRangeSlice(
                            table_name_,
                            req.NodeGroupId(),
                            req.NodeGroupTerm(),
                            KeySchema(),
                            RecordSchema(),
                            schema_ts_,
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
                            pause_key_and_is_drained.first = key->Clone();
                            shard_->Enqueue(shard_->LocalCoreId(), &req);
                            return false;
                        }
                        else if (pin_status ==
                                 RangeSliceOpStatus::BlockedOnLoad)
                        {
                            pause_key_and_is_drained.first = key->Clone();
                            return false;
                        }
                        else if (pin_status == RangeSliceOpStatus::NotOwner)
                        {
                            assert("Dead branch");
                            // The recovered cc entry does not belong to this ng
                            // anymore. This will happen if ng failover after a
                            // range split just finished but before checkpointer
                            // is able to truncate the log. In this case the log
                            // records of the data that now falls on another ng
                            // will still be replayed on the old ng on recover.
                            // Skip the cc entry and remove it at the end.
                            need_export = false;
                        }
                        else
                        {
                            // Checkpointing needs to load a slice only if one
                            // or more changed records are to be flushed. Range
                            // catalog must have been loaded when initial
                            // changes were made. So, pinning slice in
                            // checkpointing never returns BlockedOnCatalog.
                            // Moreover, since the force_load flag is set,
                            // pinning slice in checkpointing never returns
                            // Delay.
                            req.SetError(CcErrorCode::PIN_RANGE_SLICE_FAILED);
                            return false;
                        }
                    }

                    if (need_export)
                    {
                        cce->ExportForCkpt(
                            *key,
                            req.DataSyncVec(shard_->core_id_),
                            req.ArchiveVec(shard_->core_id_),
                            req.MoveBaseIdxVec(shard_->core_id_),
                            req.previous_scan_ts_,
                            req.data_sync_ts_,
                            recycle_ts,
                            Type(),
                            shard_->EnableMvcc(),
                            req.accumulated_scan_cnt_[shard_->core_id_],
                            false,
                            false);
                    }
                }
            }
            else
            {
                cce->ExportForCkpt(*key,
                                   req.DataSyncVec(shard_->core_id_),
                                   req.ArchiveVec(shard_->core_id_),
                                   req.MoveBaseIdxVec(shard_->core_id_),
                                   req.previous_scan_ts_,
                                   req.data_sync_ts_,
                                   recycle_ts,
                                   Type(),
                                   shard_->EnableMvcc(),
                                   req.accumulated_scan_cnt_[shard_->core_id_],
                                   true,
                                   req.skip_archived_key_);
            }

            // Forward iterator
            it++;

            if (req.export_base_table_rec_if_need_)
            {
                bool pin_next_slice =
                    it == end_it &&
                    req.slice_ids_[shard_->core_id_].Slice()->EndKey() !=
                        nullptr &&
                    (!(*req.slice_ids_[shard_->core_id_].Slice()->EndKey() ==
                       *req_end_key));

                // FIXME(lokax): Only loop X times to avoid blocking TxProcesser
                // when the range has many empty slices.
                while (pin_next_slice)
                {
                    const KeyT *slice_start_key = static_cast<const KeyT *>(
                        req.slice_ids_[shard_->core_id_].Slice()->EndKey());

                    // Unpin current slice
                    req.slice_ids_[shard_->core_id_].Unpin();
                    req.slice_ids_[shard_->core_id_].Reset();

                    // Pin next slice
                    RangeSliceOpStatus pin_status;
                    RangeSliceId new_slice_id =
                        shard_->PinRangeSlice(table_name_,
                                              req.NodeGroupId(),
                                              req.NodeGroupTerm(),
                                              KeySchema(),
                                              RecordSchema(),
                                              schema_ts_,
                                              table_schema_->GetKVCatalogInfo(),
                                              *slice_start_key,
                                              true,
                                              &req,
                                              pin_status,
                                              true,
                                              32);

                    switch (pin_status)
                    {
                    case RangeSliceOpStatus::Successful:
                    {
                        assert(*slice_start_key ==
                               *new_slice_id.Slice()->StartKey());
                        break;
                    }
                    case RangeSliceOpStatus::BlockedOnLoad:
                    {
                        pause_key_and_is_drained.first =
                            slice_start_key->Clone();
                        return false;
                    }
                    case RangeSliceOpStatus::Retry:
                    {
                        pause_key_and_is_drained.first =
                            slice_start_key->Clone();
                        shard_->Enqueue(shard_->LocalCoreId(), &req);
                        return false;
                    }
                    default:
                    {
                        assert(pin_status == RangeSliceOpStatus::Error);

                        req.SetError(CcErrorCode::PIN_RANGE_SLICE_FAILED);
                        return true;
                    }
                    }

                    const KeyT *slice_end_key =
                        new_slice_id.Slice()->EndKey()
                            ? static_cast<const KeyT *>(
                                  new_slice_id.Slice()->EndKey())
                            : PositiveInfinity<KeyT>::Instance();

                    it = LowerBound(*slice_start_key);
                    std::pair<Iterator, ScanType> end_pair =
                        ForwardScanStart(*slice_end_key, true);
                    end_it = end_pair.first;
                    if (end_pair.second == ScanType::ScanGap)
                    {
                        ++end_it;
                    }

                    req.slice_ids_[shard_->core_id_] = new_slice_id;

                    // This slice is not empty or this empty slice is last slice
                    // of range. We stop to loop.
                    if (it != end_it ||
                        req.slice_ids_[shard_->core_id_].Slice()->EndKey() ==
                            nullptr ||
                        (*req.slice_ids_[shard_->core_id_].Slice()->EndKey() ==
                         *req_end_key))
                    {
                        end_it_next_page_it = end_it;
                        pin_next_slice = false;
                    }
                }
            }
        }

        TxKey::Uptr next_pause_key = nullptr;
        bool no_more_data = (it == end_it) || (it == end_it_next_page_it);
        if (!no_more_data)
        {
            next_pause_key = it->first->Clone();
        }

        if (no_more_data)
        {
            // scan data drained
            if (req.export_base_table_rec_if_need_ &&
                req.slice_ids_[shard_->core_id_].Slice() != nullptr)
            {
                // Unpin slice
                req.slice_ids_[shard_->core_id_].Unpin();
                req.slice_ids_[shard_->core_id_].Reset();
            }

            pause_key_and_is_drained = {nullptr, true};
            req.SetFinish(shard_->core_id_);
        }
        else
        {
            pause_key_and_is_drained.first = std::move(next_pause_key);
            // set the pause_pos_ to mark resume position and put the
            // DataSyncScanCc request into CcQueue again.
            if (req.accumulated_scan_cnt_.at(shard_->core_id_) <
                req.scan_batch_size_)
            {
                shard_->Enqueue(&req);
            }
            else
            {
                // scan data is not drained
                req.SetFinish(shard_->core_id_);
            }
        }

        // Access DataSyncScanCc member variable is unsafe after
        // SetFinished(...).
        return false;
    }
#else

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

        const KeyT *const req_start_key =
            req.start_key_ ? static_cast<const KeyT *>(req.start_key_)
                           : NegativeInfinity<KeyT>::Instance();
        const KeyT *const req_end_key =
            req.end_key_ ? static_cast<const KeyT *>(req.end_key_)
                         : PositiveInfinity<KeyT>::Instance();

        Iterator it;
        Iterator end_it;
        if (req.IsDrained(shard_->core_id_))
        {
            // scan is already finished on this core
            req.SetFinish(shard_->core_id_);
            return false;
        }

        int64_t ng_term = Sharder::Instance().LeaderTerm(req.NodeGroupId());
        if (ng_term < 0)
        {
            req.SetError(CcErrorCode::TX_NODE_NOT_LEADER);
            return false;
        }

        auto &pause_pos_and_is_drained = req.PausePos(shard_->core_id_);

        if (pause_pos_and_is_drained.first == nullptr)
        {
            // If this is a new scan cc, start from the specified start
            // key or negative inf.
            if (req_start_key == NegativeInfinity<KeyT>::Instance())
            {
                it = Begin();
                it++;
            }
            else
            {
                it = LowerBound(*req_start_key);
                if (it->first == NegativeInfinity<KeyT>::Instance())
                {
                    it++;
                }
            }
        }
        else
        {
            CcEntry<KeyT, ValueT> *pause_entry =
                static_cast<CcEntry<KeyT, ValueT> *>(
                    pause_pos_and_is_drained.first);
            CcPage<KeyT, ValueT> *ccp =
                static_cast<CcPage<KeyT, ValueT> *>(pause_entry->GetCcPage());
            it = Iterator(pause_entry, ccp, &neg_inf_);
            ReleaseCceLock(
                pause_entry->GetKeyLock(), pause_entry, req.Txn(), cc_ng_id_);
        }

        const KeyT *search_end_key = req_end_key;

        if (search_end_key == PositiveInfinity<KeyT>::Instance())
        {
            end_it = End();
        }
        else
        {
            std::pair<Iterator, ScanType> end_pair =
                ForwardScanStart(*search_end_key, true);
            end_it = end_pair.first;
            if (end_pair.second == ScanType::ScanGap)
            {
                ++end_it;
            }
        }

        // Since we might skip the page that end_it is on if it's not
        // updated since last ckpt, it might skip end_it. If the last
        // page is skipped it will be set as the first entry on the next
        // page. Also check if (it == end_it_next_page_it).
        Iterator end_it_next_page_it = end_it;
        if (end_it_next_page_it != End())
        {
            CcPage<KeyT, ValueT> *ccp = end_it_next_page_it.GetPage();
            assert(ccp != nullptr);
            if (ccp->next_page_ == PagePosInf())
            {
                end_it_next_page_it = End();
            }
            else
            {
                end_it_next_page_it = Iterator(ccp->next_page_, 0, &neg_inf_);
            }
        }

        uint64_t recycle_ts = 1U;
        if (shard_->EnableMvcc())
        {
            recycle_ts = shard_->GlobalMinSiTxStartTs();
        }

        // Only scan for updates after given from ts. previous_ckpt_ts_
        // is used during regular ckpt, and previous_scan_ts_ is used
        // during range split explicitly.
        uint64_t from_ts = req.previous_ckpt_ts_;

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
            CcPage<KeyT, ValueT> *ccp = it.GetPage();
            assert(ccp);

            if (ccp->last_dirty_commit_ts_ <= from_ts)
            {
                // Skip the pages that have no updates since last data
                // sync.
                if (ccp->next_page_ == PagePosInf())
                {
                    it = End();
                }
                else
                {
                    it = Iterator(ccp->next_page_, 0, &neg_inf_);
                }
                continue;
            }

#ifndef ON_KEY_OBJECT
            if (shard_->EnableMvcc())
            {
                shard_->DecrementMemory(cce->KickOutArchiveRecords(recycle_ts));
            }
#endif

            if (cce->NeedCkpt())
            {
                cce->ExportForCkpt(*key,
                                   req.DataSyncVec(shard_->core_id_),
                                   req.ArchiveVec(shard_->core_id_),
                                   req.MoveBaseIdxVec(shard_->core_id_),
                                   req.previous_scan_ts_,
                                   req.data_sync_ts_,
                                   recycle_ts,
                                   Type(),
                                   shard_->EnableMvcc(),
                                   req.accumulated_scan_cnt_[shard_->core_id_],
                                   false,
                                   false);
            }

            // Forward iterator
            it++;
        }

        bool no_more_data = (it == end_it) || (it == end_it_next_page_it);

        if (no_more_data)
        {
            pause_pos_and_is_drained = {nullptr, true};
            // scan data drained
            req.SetFinish(shard_->core_id_);
            // Access DataSyncScanCc member variable is unsafe after
            // SetFinished(...).
            return false;
        }
        else
        {
            assert(pause_pos_and_is_drained.second == false);
            pause_pos_and_is_drained.first = it->second;
            bool add_intent =
                it->second->GetOrCreateKeyLock(shard_, this, it.GetPage())
                    .AcquireReadIntent(req.Txn());
            assert(add_intent);
            (void) add_intent;
            shard_->UpsertLockHoldingTx(req.Txn(),
                                        req.node_group_term_,
                                        it->second,
                                        false,
                                        cc_ng_id_,
                                        table_name_.Type());
            // set the pause_key_ to mark resume position and put the
            // DataSyncScanCc request into CcQueue again.
            if (req.accumulated_scan_cnt_.at(shard_->core_id_) <
                req.scan_batch_size_)
            {
                shard_->Enqueue(&req);
            }
            else
            {
                // scan data is not drained
                req.SetFinish(shard_->core_id_);
                return false;
            }
        }

        return false;
    }
#endif

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

        TableName range_table_name(table_name_.StringView(),
                                   TableType::RangePartition);
        if (!req.built_slice_sample_pool_)
        {
            // All ranges has been added read lock. It is safe to access
            // them. Also since all ranges are locked, the range map in
            // local cc shards will not change so we can trust the map
            // iterator after cc req resumes.
            std::map<const TxKey *, TableRangeEntry, PtrLessThan<TxKey>>
                *range_map = shard_->GetTableRangesForATable(range_table_name,
                                                             cc_ng_id_);
            if (first_enter)
            {
                req.range_it_ = range_map->begin();
            }
            for (; req.range_it_ != range_map->end(); req.range_it_++)
            {
                auto &range_entry = req.range_it_->second;
                if (shard_
                        ->GetRangeOwner(
                            range_entry.GetRangeInfo()->PartitionId(),
                            cc_ng_id_)
                        ->BucketOwner() == cc_ng_id_)
                {
                    // Pin store range so that it cannot be kicked out
                    // during analyze.
                    const StoreRange *store_range = range_entry.PinStoreRange();
                    if (!store_range)
                    {
                        range_entry.FetchRangeSlices(
                            range_table_name, &req, cc_ng_id_, ng_term, shard_);
                        return false;
                    }
                    assert(store_range != nullptr);
                    for (const std::unique_ptr<StoreSlice> &store_slice :
                         store_range->Slices())
                    {
                        slice_sample_pool->Insert(std::make_pair(
                            store_range->PartitionId(), store_slice.get()));
                    }
                }
            }

            req.built_slice_sample_pool_ = true;
            assert(req.next_pin_slice_idx_ == 0);
        }

        // Pin-slice is necessary. On one hand, pin-slice would
        // guarantee enough samples, even if the amount of sampled slice
        // is very few. On the other hand, pin-slice would help
        // estimating average bytes of records.
        //
        // Capacity of slice sample pool cannot be too small. Otherwise
        // the final sampled keys could not reflect original key
        // distribution. Capacity of slice sample pool cannot be too
        // large. Otherwise too many pin-slice calls can lead to too
        // many accesses to storage.
        if (req.next_pin_slice_idx_ < slice_sample_pool->SampleKeys().size())
        {
            const auto [range_id, store_slice] =
                slice_sample_pool->SampleKeys().at(req.next_pin_slice_idx_);
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

            const StoreSlice *last_pinned_slice;
            RangeSliceId slice_id =
                shard_->PinRangeSlices(table_name_,
                                       cc_ng_id_,
                                       ng_term,
                                       key_schema,
                                       table_schema_->RecordSchema(),
                                       schema_ts_,
                                       table_schema_->GetKVCatalogInfo(),
                                       range_id,
                                       *slice_start_key,
                                       true,
                                       nullptr,
                                       false,
                                       &req,
                                       false,
                                       UINT8_MAX,
                                       1,
                                       true,
                                       pin_status,
                                       last_pinned_slice);

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

                    if (cc_entry.PayloadStatus() == RecordStatus::Normal)
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
            else if (pin_status == RangeSliceOpStatus::Delay)
            {
                assert(!slice_id.Range()->HasLock());
                shard_->Enqueue(shard_->LocalCoreId(), &req);
                return false;
            }
            else
            {
                std::map<const TxKey *, TableRangeEntry, PtrLessThan<TxKey>>
                    *range_map = shard_->GetTableRangesForATable(
                        range_table_name, cc_ng_id_);
                for (auto range_it = range_map->begin();
                     range_it != range_map->end();
                     range_it++)
                {
                    auto &range_entry = range_it->second;
                    range_entry.UnPinStoreRange();
                }
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
            std::map<const TxKey *, TableRangeEntry, PtrLessThan<TxKey>>
                *range_map = shard_->GetTableRangesForATable(range_table_name,
                                                             cc_ng_id_);
            for (auto range_it = range_map->begin();
                 range_it != range_map->end();
                 range_it++)
            {
                auto &range_entry = range_it->second;
                range_entry.UnPinStoreRange();
            }
            hd_res->SetFinished();
            return true;
        }
    }

    bool Execute(ReloadCacheCc &req) override
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
        // A psuedo record that is used to deserialize and move forward
        // the record that is not sharded to the core.
        ValueT rec;
        size_t offset = 0;
        uint16_t next_core = UINT16_MAX;
        const std::string_view &log_blob = req.LogContentView();

        // If the log record's commit ts is smaller than that of the cc
        // map, this record is generated before the latest schema of the
        // table and hence should skip the replay process.
        if (req.CommitTs() < schema_ts_)
        {
            req.SetFinish();
            return true;
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
                // Skips the key in the log record that is not sharded
                // to this core.
                if (op_type == OperationType::Insert ||
                    op_type == OperationType::Update)
                {
                    rec.Deserialize(log_blob.data(), offset);
                }
                if (shard_->core_id_ == req.FirstCore() ||
                    (core_id != req.FirstCore() && core_id > shard_->core_id_))
                {
                    // Move to the smallest unvisited core id
                    next_core = std::min(core_id, next_core);
                }
                continue;
            }

            // Skip records that no longer belong to this ng.
            const TableRangeEntry *range_entry =
                shard_->GetTableRangeEntry(table_name_, cc_ng_id_, &key);

            const BucketInfo *bucket_info = shard_->GetBucketInfo(
                Sharder::MapRangeIdToBucketId(
                    range_entry->GetRangeInfo()->PartitionId()),
                cc_ng_id_);
            // Check if range bucket belongs to this ng or is migrating
            // to this ng.
            if (bucket_info->BucketOwner() != cc_ng_id_ &&
                bucket_info->DirtyBucketOwner() != cc_ng_id_)
            {
                int32_t new_range_id =
                    range_entry->GetRangeInfo()->GetKeyNewRangeId(&key);
                // If range is splitting, check if new range belongs to
                // this ng.
                if (new_range_id >= 0)
                {
                    const BucketInfo *new_bucket_info = shard_->GetBucketInfo(
                        Sharder::MapRangeIdToBucketId(
                            range_entry->GetRangeInfo()->PartitionId()),
                        cc_ng_id_);
                    if (new_bucket_info->BucketOwner() != cc_ng_id_ &&
                        new_bucket_info->DirtyBucketOwner() != cc_ng_id_)
                    {
                        if (op_type != OperationType::Delete)
                        {
                            rec.Deserialize(log_blob.data(), offset);
                        }
                        continue;
                    }
                }
            }

            Iterator it = FindEmplace(key);
            CcEntry<KeyT, ValueT> *cce = it->second;
            CcPage<KeyT, ValueT> *ccp = it.GetPage();

            if (cce == nullptr)
            {
                // Since we're not holding any range lock that would
                // block data sync during replay, just keep retrying
                // until we have free space in cc map.
                shard_->Enqueue(shard_->LocalCoreId(), &req);
                return false;
            }

            if (cce->CommitTs() >= req.CommitTs())
            {
                // If the key exists in the cc map and its commit ts is
                // greater than that of the log record, and if (1) mvcc
                // is enabled, then install  the log record into
                // archives; (2) mvcc is not enabled, then skips
                // installing the log record in the cc map and moves to
                // the next key in the log record.
                if (shard_->EnableMvcc())
                {
#ifndef ON_KEY_OBJECT
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
                    cce->AddArchiveRecord(
                        std::move(rec_ptr), rec_status, req.CommitTs());
#endif
                }
                else if (op_type == OperationType::Insert ||
                         op_type == OperationType::Update)
                {
                    rec.Deserialize(log_blob.data(), offset);
                }
            }
            else
            {
#ifndef ON_KEY_OBJECT
                if (shard_->EnableMvcc())
                {
                    cce->ArchiveBeforeUpdate(Type());
                }
#endif
                RecordStatus rec_status;
                if (op_type == OperationType::Insert ||
                    op_type == OperationType::Update)
                {
#ifndef ON_KEY_OBJECT
                    if (cce->payload_.use_count() != 1)
                    {
                        cce->payload_ = std::make_shared<ValueT>();
                    }
#else
                    assert(false);
                    cce->payload_ = std::make_unique<ValueT>();
#endif
                    cce->payload_->Deserialize(log_blob.data(), offset);
                    rec_status = RecordStatus::Normal;
                }
                else
                {
                    if (Type() != TableType::Secondary)
                    {
                        cce->payload_ = nullptr;
                    }
                    rec_status = RecordStatus::Deleted;
                }
                const uint64_t commit_ts = req.CommitTs();
                cce->SetCommitTsPayloadStatus(commit_ts, rec_status);

                if (commit_ts > last_dirty_commit_ts_)
                {
                    last_dirty_commit_ts_ = commit_ts;
                }
                if (commit_ts > ccp->last_dirty_commit_ts_)
                {
                    ccp->last_dirty_commit_ts_ = commit_ts;
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

                NonBlockingLock *key_lock = cce->GetKeyLock();
                if (key_lock != nullptr && key_lock->HasWriteLock())
                {
                    // If the record in the log has a commit ts greater
                    // than that of the cc entry and the cc entry has a
                    // write lock, the lock's owner must be the tx that
                    // commits the log record.
                    // TODO: it is safer if we ship the tx ID with the
                    // recovering message and match it against the lock
                    // holder.
                    TxNumber txn = key_lock->WriteLockTx();
                    ReleaseCceLock(key_lock,
                                   cce,
                                   txn,
                                   req.NodeGroupId(),
                                   LockType::WriteLock);
                }
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
            Iterator it = Find(key);
            const KeyT *cce_key = it->first;
            CcEntry<KeyT, ValueT> *cce = it->second;

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
                                       cce->CommitTs(),
                                       1U,
                                       Type(),
                                       shard_->EnableMvcc(),
                                       tmp_ckpt_vec_size,
                                       false,
                                       false);

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

                    bool res = shard_->FlushEntryForTest(table_name_,
                                                         table_schema_,
                                                         tmp_ckpt_vec,
                                                         tmp_akv_vec,
                                                         only_archives);
                    assert(res == true);
                    // This silences the -Wunused-but-set-variable
                    // warning without any runtime overhead.
                    (void) res;
                }
                if (only_archives)
                {
#ifndef ON_KEY_OBJECT
                    cce->ClearArchives();
#endif
                }
                else
                {
                    ccm_has_full_entries_ = false;
                    // Clean(cce);
                }
            }
        }
        req.Result()->SetValue(true);
        req.Result()->SetFinished();
        return true;
    }

    bool Execute(FillStoreSliceCc &req) override
    {
        std::vector<SliceDataItem> &slice_vec = req.SliceData(shard_->core_id_);

        size_t index = req.NextIndex(shard_->core_id_);
        size_t last_index = std::min(index + FillStoreSliceCc::MaxScanBatchSize,
                                     slice_vec.size());

        bool success =
            BatchFillSlice(slice_vec, req.ForceLoad(), index, last_index);

        if (!success)
        {
            req.SetError(CcErrorCode::OUT_OF_MEMORY);
            return true;
        }

        index = last_index;
        if (index == slice_vec.size())
        {
            req.SetFinish();
        }
        else
        {
            req.SetNextIndex(shard_->core_id_, index);
            shard_->Enqueue(shard_->LocalCoreId(), &req);
        }
        return false;
    }

    bool Execute(GetPostCkptSlice &req) override
    {
#ifdef RANGE_PARTITION_ENABLED
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
                static_cast<const KeyT *>(req.Slice()->StartKey());

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

        const KeyT *end_key = static_cast<const KeyT *>(req.Slice()->EndKey());
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

        for (size_t scan_cnt = 0;
             scan_cnt < GetPostCkptSlice::ScanBatchSize && map_it != map_end_it;
             ++map_it, ++scan_cnt)
        {
            const KeyT *cce_key = map_it->first;
            CcEntry<KeyT, ValueT> *cce = map_it->second;

            if (cce->CommitTs() <= 1)
            {
                // This is a new inserted key that the tx has not
                // finished post-processing.
                continue;
            }

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

                // This entry is not going to be flushed in this
                // checkpoint, so the data store size before and post
                // ckpt are the same.

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

#endif
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
            // resume key is the first key we need to start with, find
            // the floor key of it in case resume key has already been
            // kicked out.
            const KeyT *resume_key =
                static_cast<const KeyT *>(req.ResumeKey(shard_->core_id_));
            Iterator it = Floor(*resume_key);
            lru_page = it.GetPage();
        }
        else
        {
            if (req.StartKey() == nullptr)
            {
                lru_page = neg_inf_page_.next_page_;
            }
            else
            {
                Iterator it = Floor(*start_key);
                if (it->first == NegativeInfinity<KeyT>::Instance())
                {
                    lru_page = neg_inf_page_.next_page_;
                }
                else
                {
                    lru_page = it.GetPage();
                }
            }
        }

        CcPage<KeyT, ValueT> *ccp =
            static_cast<CcPage<KeyT, ValueT> *>(lru_page);

        // To avoid occupy the TxProcessor thread for a long time, only
        // process KickoutPageBatchSize number of pages in each round.
        size_t scan_page_cnt = 0;
        bool is_success = true;
        while (scan_page_cnt < KickoutCcEntryCc::KickoutPageBatchSize &&
               (end_key == nullptr || ccp->FirstKey() < *end_key) &&
               ccp != &pos_inf_page_)
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

        if (ccp == &pos_inf_page_ ||
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

        auto entry_vec = req.EntryVector();
        auto entry_tuples = req.EntryTuple();
        size_t batch_size = req.BatchSize();
        size_t start_key_index = req.StartKeyIndex();

        const TxKey *req_key = nullptr;
        const TxRecord *req_rec = nullptr;

        const KeyT *key = nullptr;
        KeyT decoded_key;
        const ValueT *commit_val = nullptr;
        ValueT decoded_rec;
        uint64_t commit_ts = 0;

        auto &resume_pos = req.GetPausedPosition(shard_->core_id_);
        size_t key_pos = std::get<0>(resume_pos);
        size_t key_offset = std::get<1>(resume_pos);
        size_t rec_offset = std::get<2>(resume_pos);
        size_t ts_offset = std::get<3>(resume_pos);
        size_t hash = 0;
#ifdef RANGE_PARTITION_ENABLED
        RangeSliceId current_slice_id;
        const KeyT *slice_end_key = nullptr;
#endif
        Iterator it;
        CcEntry<KeyT, ValueT> *cce;
        const KeyT *write_key = nullptr;
        CcPage<KeyT, ValueT> *cc_page = nullptr;
        size_t key_idx = 0;
        size_t next_key_offset = 0;
        size_t next_rec_offset = 0;
        size_t next_ts_offset = 0;
        for (; key_pos < batch_size; ++key_pos)
        {
            next_key_offset = key_offset;
            next_rec_offset = rec_offset;
            next_ts_offset = ts_offset;
            if (entry_vec != nullptr)
            {
                key_idx = start_key_index + key_pos;
                // get key
                req_key = entry_vec->at(key_idx)->key_.get();
                key = static_cast<const KeyT *>(req_key);
                // get record
                req_rec = entry_vec->at(key_idx)->rec_.get();
                commit_val = static_cast<const ValueT *>(req_rec);
                // get commit ts
                commit_ts = entry_vec->at(key_idx)->commit_ts_;
            }
            else
            {
                auto [key_str, rec_str, ts_str] = *entry_tuples;
                // deserialize key
                decoded_key.Deserialize(
                    key_str.data(), next_key_offset, KeySchema());
                key = &decoded_key;
                // deserialize rec
                decoded_rec.Deserialize(rec_str.data(), next_rec_offset);
                commit_val = &decoded_rec;
                // deserialize commit ts
                commit_ts = *((uint64_t *) (ts_str.data() + next_ts_offset));
                next_ts_offset += sizeof(uint64_t);
            }

            hash = key->Hash();
            size_t core_idx = (hash & 0x3FF) % shard_->core_cnt_;
            if (!(core_idx == shard_->core_id_) || commit_ts <= 1)
            {
                // Skip the key that does not belong to this core or
                // commit ts does not greater than 1. Move to next key.
                key_offset = next_key_offset;
                rec_offset = next_rec_offset;
                ts_offset = next_ts_offset;
                continue;
            }

#ifdef RANGE_PARTITION_ENABLED
            if (current_slice_id.Slice() != nullptr && *slice_end_key <= *key)
            {
                // Move to next slice.
                current_slice_id.Unpin();
                current_slice_id.Reset();
            }
            if (current_slice_id.Slice() == nullptr)
            {
                RangeSliceOpStatus pin_status;
                current_slice_id =
                    shard_->PinRangeSlice(table_name_,
                                          req.NodeGroupId(),
                                          req.CcNgTerm(),
                                          KeySchema(),
                                          RecordSchema(),
                                          schema_ts_,
                                          table_schema_->GetKVCatalogInfo(),
                                          *key,
                                          true,
                                          &req,
                                          pin_status,
                                          false,
                                          0);
                if (pin_status == RangeSliceOpStatus::Successful)
                {
                    slice_end_key =
                        current_slice_id.Slice()->EndKey() != nullptr
                            ? static_cast<const KeyT *>(
                                  current_slice_id.Slice()->EndKey())
                            : PositiveInfinity<KeyT>::Instance();
                }
                else if (pin_status == RangeSliceOpStatus::BlockedOnLoad)
                {
                    // set the paused key.
                    req.SetPausedPosition(shard_->core_id_,
                                          key_pos,
                                          key_offset,
                                          rec_offset,
                                          ts_offset);
                    return false;
                }
                else if (pin_status == RangeSliceOpStatus::Retry)
                {
                    // set the paused key
                    req.SetPausedPosition(shard_->core_id_,
                                          key_pos,
                                          key_offset,
                                          rec_offset,
                                          ts_offset);
                    shard_->Enqueue(shard_->LocalCoreId(), &req);
                    return false;
                }
                else if (pin_status == RangeSliceOpStatus::Delay)
                {
                    if (current_slice_id.Range()->HasLock())
                    {
                        return req.SetError(CcErrorCode::OUT_OF_MEMORY);
                    }
                    else
                    {
                        // set the paused key
                        req.SetPausedPosition(shard_->core_id_,
                                              key_pos,
                                              key_offset,
                                              rec_offset,
                                              ts_offset);
                        shard_->Enqueue(shard_->LocalCoreId(), &req);
                        return false;
                    }
                }
                else
                {
                    return req.SetError(CcErrorCode::PIN_RANGE_SLICE_FAILED);
                }
            }
#endif
            it = FindEmplace(*key);
            cce = it->second;
            cc_page = it.GetPage();
            if (cce == nullptr)
            {
#ifdef RANGE_PARTITION_ENABLED
                if (current_slice_id.Slice() != nullptr)
                {
                    current_slice_id.Unpin();
                    current_slice_id.Reset();
                }
#endif

                DLOG(WARNING) << "!!!WARNING!!! UploadBatchCc OOM on core: "
                              << shard_->core_id_ << ". Txn: " << req.Txn()
                              << ", table name: " << this->table_name_.Trace();
                // This cc shard has reached max memory limit. We didn't write
                // data log for this upload batch req, but we have acquired
                // range read lock for this key. If we do not return error and
                // release the range read lock, it might block range split from
                // finishing. We should return error here so that coordinator
                // can release range read lock and retry later.
                return req.SetError(CcErrorCode::OUT_OF_MEMORY);
            }
            write_key = it->first;

#ifdef RANGE_PARTITION_ENABLED
            if (cce->PayloadStatus() == RecordStatus::Unknown)
            {
                // After pin slice, the key with Unknown status must not
                // existed.
                RecordStatus new_status = RecordStatus::Deleted;
                cce->SetCommitTsPayloadStatus(1U, new_status);
                cce->SetCkptTs(1U);
                cce->data_store_size_.store(0, std::memory_order_relaxed);
            }
            else
            {
                // So far, UploadBatchCc is use exclusively for
                // secondary key encoded by primary key during add index
                // txm. If the target key already exists, it must be
                // newer than this encoded key, so the old value should
                // be discarded directly.
                assert(cce->CommitTs() > 1 && cce->CommitTs() >= commit_ts);
                key_offset = next_key_offset;
                rec_offset = next_rec_offset;
                ts_offset = next_ts_offset;
                continue;
            }
#endif

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
                continue;
            }

            // Now, all versions of non-unique SecondaryIndex key shared
            // the unpack info in current version's payload, though the
            // unpack info will not be used for deleted key, we must not
            // change the payload of secondary key ccentry if it is not
            // null.
            if (Type() != TableType::Secondary || cce->payload_ == nullptr)
            {
#ifndef ON_KEY_OBJECT
                if (cce->payload_.use_count() == 1)
                {
                    *(cce->payload_) = *commit_val;
                }
                else
                {
                    cce->payload_ = std::make_shared<ValueT>(*commit_val);
                }
#else
                assert(false);
                cce->payload_ = std::make_unique<ValueT>(*commit_val);
#endif
            }

            // Currently, this request is only used during add index
            // which will upload non-deleted records.
            cce->SetCommitTsPayloadStatus(commit_ts, RecordStatus::Normal);
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

            if (shard_->realtime_sampling_ && sample_pool_)
            {
                assert(write_key != nullptr);
                sample_pool_->OnInsert(*write_key, table_schema_);
            }

            // update the key offset
            key_offset = next_key_offset;
            rec_offset = next_rec_offset;
            ts_offset = next_ts_offset;
        }
#ifdef RANGE_PARTITION_ENABLED
        if (current_slice_id.Slice() != nullptr)
        {
            current_slice_id.Unpin();
        }
#endif
        return req.SetFinish();
    }

    bool Execute(ApplyCc &req) override
    {
        return true;
    }

    size_t size() const override
    {
        return size_;
    }

    /**
     * Clean erasable entries in lru_page, re-balance pages after clean.
     *
     * @param lru_page
     * @param clean_type
     * @param kickout_cc [optional]
     * @param is_success [optional]
     * @return result pair of which the first is free count and the
     * scond is the next page which will be cleaned. If clean type is
     * CleanForFree, the next page is the lru_next_ of the page.
     * Otherwise, the value of the next page is setted depending on
     * clean status: When clean successfully, return the current page's
     * next page in the below cases: page is empty; no borrow or merge;
     * borrow from previous; merge with previous. Return the current
     * page in the below cases: borrow from next; merge with next. When
     * clean failed, return the current page always.
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

        // clean page
        CcPage<KeyT, ValueT> *page =
            static_cast<CcPage<KeyT, ValueT> *>(lru_page);
        const KeyT old_page_key(page->FirstKey());
        bool success = CleanPage(page, free_cnt, clean_type, kickout_cc);

        // Output the operation result if the caller care it.
        if (is_success != nullptr)
        {
            *is_success = success;
        }

        if (page->Empty())  // remove page if empty
        {
            if (page->lru_next_ != nullptr)
            {
                shard_->DetachLru(page);
            }
            ccmp_.erase(old_page_key);
        }
        else if (page->Size() >= CcPage<KeyT, ValueT>::merge_threshold_)
        {
            // page is still half full, no redistribution or merge
            // needed
            auto page_it = ccmp_.find(old_page_key);
            assert(page_it != ccmp_.end());
            TryUpdatePageKey(page_it);

            if (kickout_cc != nullptr && !success)
            {
                // If the caller care the clean status, reset the value
                // of
                // @@next_page depending on the clean result:
                // 1) When the current page has been cleaned
                // successfully, there is no need to reset the value of
                // @@next_page. 2) When this current page has not been
                // cleaned successfully, should set the current page as
                // the next_page.
                next_page = page;
            }
        }
        else
        {
            // redistribute or merge page with its siblings
            CcPage<KeyT, ValueT> *prev = page->prev_page_;
            CcPage<KeyT, ValueT> *next = page->next_page_;
            bool can_borrow_from_prev =
                prev != &neg_inf_page_ &&
                page->Size() + prev->Size() >
                    CcPage<KeyT, ValueT>::split_threshold_;
            bool can_borrow_from_next =
                next != &pos_inf_page_ &&
                page->Size() + next->Size() >
                    CcPage<KeyT, ValueT>::split_threshold_;
            bool can_merge_with_prev =
                prev != &neg_inf_page_ &&
                page->Size() + prev->Size() <=
                    CcPage<KeyT, ValueT>::split_threshold_;
            bool can_merge_with_next =
                next != &pos_inf_page_ &&
                page->Size() + next->Size() <=
                    CcPage<KeyT, ValueT>::split_threshold_;
            if (can_borrow_from_prev || can_borrow_from_next)
            {
                // map needs to be updated through iterator
                auto page_it = ccmp_.find(old_page_key);
                // the two pages whose entries need to be redistributed
                // are identified by page1 and page2, page1 is the page
                // with smaller key
                auto page1_it = page_it;
                auto page2_it = page_it;
                if (can_borrow_from_prev)
                {
                    // borrow entries from previous page
                    page1_it--;
                }
                else if (can_borrow_from_next)
                {
                    // borrow entries from next page
                    page2_it++;
                }

                RedistributeBetweenPages(page1_it, page2_it);

                if (kickout_cc != nullptr &&
                    ((success && page == &page1_it->second) || !success))
                {
                    // If the caller care the clean status, reset the
                    // value of
                    // @@next_page depending on the clean result:
                    // 1) When the current page has been cleaned
                    // successfully, if borrow from the next(that's mean
                    // page == page1), should set the current page as
                    // the @@next_page. 2) When this current page has
                    // not been cleaned successfully, should return the
                    // current page as the
                    // @@next_page.
                    next_page = page;
                }
            }
            else if (can_merge_with_prev || can_merge_with_next)
            {
                // map needs to be updated through iterator
                auto page_it = ccmp_.find(old_page_key);
                // the two pages to be merged are identified by page1
                // and page2, page1 is the page with smaller key
                auto page1_it = page_it;
                auto page2_it = page_it;

                bool real_merge_with_prev = false;
                if (can_merge_with_prev)
                {
                    real_merge_with_prev = true;
                    // merge `page` with its previous page
                    page1_it--;
                }
                else if (can_merge_with_next)
                {
                    // merge `page` with its next page
                    page2_it++;
                }

                CcPage<KeyT, ValueT> *merged_page = &page1_it->second;
                CcPage<KeyT, ValueT> *discarded_page = &page2_it->second;

                if (kickout_cc == nullptr && next_page == discarded_page)
                {
                    // For this case, the next page to be cleaned comes
                    // from the lru list. The next_page is current
                    // page's lru_next_, and if the next_page ==
                    // discarded_page, the discarded_page must be the
                    // next page of the current page, that is to say,
                    // the current page will merge with the next. So the
                    // current page must equal to the merged page, and
                    // should set the
                    // @@next_page is merged page.
                    assert(page == merged_page);
                    next_page = merged_page;
                }

                // merge page1 and page2
                MergePages(page1_it, page2_it, page);

                if (kickout_cc != nullptr)
                {
                    // For this case, should set the value of
                    // @@next_page depending on the clean result: 1)
                    // When the current page has been cleaned
                    // successfully, if merged with previous page, set
                    // the value is the merged_page's next_page_; if
                    // merged with next page, set the value is the
                    // merged_page itself. 2) When the current page has
                    // not been cleaned successfully. Should set the
                    // value is the merged_page itself no matter merged
                    // with previous page or merged with next page.
                    next_page = (real_merge_with_prev && success)
                                    ? merged_page->next_page_
                                    : merged_page;
                }
            }
        }

        size_ -= free_cnt;
        if (free_cnt > 0)
        {
            ccm_has_full_entries_ = false;
        }

        return {free_cnt, next_page};
    }

    void Clean() override
    {
        for (auto it = ccmp_.begin(); it != ccmp_.end(); it++)
        {
            CcPage<KeyT, ValueT> &page = it->second;
            if (page.lru_next_ != nullptr)
            {
                shard_->DetachLru(&page);
            }

            for (auto &cce : page.entries_)
            {
                cce->ClearLocks(*shard_, cc_ng_id_);
            }
        }

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
     * Used for debug to verify the map_link is complete and keys are in
     * order.
     */
    size_t VerifyOrdering() override
    {
        // verify page order in map
        CcPage<KeyT, ValueT> *prev_page = &neg_inf_page_;
        for (auto it = ccmp_.begin(); it != ccmp_.end(); it++)
        {
            const KeyT &page_key = it->first;
            CcPage<KeyT, ValueT> *page = &it->second;
            assert(page_key == page->FirstKey());
            // This silences the -Wunused-but-set-variable warning
            // without any runtime overhead.
            (void) page_key;
            assert(page->prev_page_ == prev_page &&
                   prev_page->next_page_ == page);
            prev_page = page;
        }
        assert(prev_page->next_page_ == &pos_inf_page_ &&
               pos_inf_page_.prev_page_ == prev_page);
        // This silences the -Wunused-but-set-variable warning without
        // any runtime overhead.
        (void) prev_page;
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
            // This silences the -Wunused-but-set-variable warning
            // without any runtime overhead.
            (void) prev_key;
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
            CcPage<KeyT, ValueT> *ccp = it.GetPage();
            // randomly set ckpt_ts and commit_ts
            cce->SetCommitTsPayloadStatus(distribution(generator),
                                          RecordStatus::Normal);
            cce->SetCkptTs(distribution(generator));
            ccp->last_dirty_commit_ts_ =
                std::max(cce->CommitTs(), ccp->last_dirty_commit_ts_);
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
            if (!is_neg_inf && !is_pos_inf)
            {
                LOG(INFO) << ", cce parent page: " << current_page_;
                current_page_->DebugPrint();
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
                 CcPage<KeyT, ValueT> *cc_page,
                 CcEntry<KeyT, ValueT> *neg_inf_cce)
            : neg_inf_cce_(neg_inf_cce)
        {
            if (cc_page->IsNegInf())
            {
                current_.first = NegativeInfinity<KeyT>::Instance();
                current_.second = neg_inf_cce_;
                current_page_ = cc_page;
            }
            else if (cc_page->IsPosInf())
            {
                current_.first = PositiveInfinity<KeyT>::Instance();
                current_.second = nullptr;
                current_page_ = cc_page;
            }
            else
            {
                current_page_ = cc_page;
                idx_in_page_ = current_page_->FindEntry(cce);
                UpdateCurrent();
            }
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
                // The iterator points to negative infinity. Increments
                // the iterator to the first page in the map, if the map
                // is not empty.
                CcPage<KeyT, ValueT> *next_page = current_page_->next_page_;
                if (next_page->IsPosInf())
                {
                    // If the next page is the positive infinity page,
                    // the map is empty. The advanced iterator points to
                    // positive infinity.
                    current_.first = PositiveInfinity<KeyT>::Instance();
                    current_.second = nullptr;
                    current_page_ = next_page;
                }
                else
                {
                    current_page_ = next_page;
                    idx_in_page_ = 0;
                    UpdateCurrent();
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
            // If the iterator points to positive infinity, keeps the
            // iterator unchanged.

            return *this;
        }

        // Prefix decrement
        Iterator &operator--()
        {
            if (current_.first == PositiveInfinity<KeyT>::Instance())
            {
                // The iterator points to positive infinity. Decrements
                // the iterator to the last entry in the map, if the map
                // is not empty.
                CcPage<KeyT, ValueT> *prev_page = current_page_->prev_page_;
                if (prev_page->IsNegInf())
                {
                    // If the previous page is the negative infinity
                    // page, the map is empty. The advanced iterator
                    // points to negative infinity.
                    current_.first = NegativeInfinity<KeyT>::Instance();
                    current_.second = neg_inf_cce_;
                    current_page_ = prev_page;
                }
                else
                {
                    current_page_ = prev_page;
                    idx_in_page_ = current_page_->Size() - 1;
                    UpdateCurrent();
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

        CcPage<KeyT, ValueT> *GetPage() const
        {
            return current_page_;
        }

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
        return Iterator(&neg_inf_, &neg_inf_page_, &neg_inf_);
    }

    /**
     * @brief Returns an iterator that points to positive infinity.
     *
     * @return Iterator
     */
    Iterator End()
    {
        return Iterator(&pos_inf_, &pos_inf_page_, &neg_inf_);
    }

    Iterator Find(const KeyT &key)
    {
        if (&key == NegativeInfinity<KeyT>::Instance())
        {
            return Begin();
        }

        Iterator lb_it = LowerBound(key);
        if (lb_it != End() && *lb_it->first == key)
        {
            CcPage<KeyT, ValueT> *ccp = lb_it.GetPage();
            shard_->UpdateLruList(ccp, false);
            return lb_it;
        }
        else
        {
            // The input key does not exist.
            return End();
        }
    }

    Iterator FindEmplace(const KeyT &key, bool force_emplace = false)
    {
        bool emplace;
        return FindEmplace(key, emplace, force_emplace);
    }

    bool BatchFillSlice(std::vector<SliceDataItem> &slice_items,
                        bool force_emplace,
                        size_t first_index,
                        size_t end_idx)
    {
        if (slice_items.empty() || first_index >= end_idx)
        {
            return true;
        }

        // catalog and range ccmap bypass shard memory limit. since
        // checkpointer may emplace ccentry into ccmap.
        if (shard_->Full())
        {
            // The shard has reached the maximal capacity. Tries to
            // clean cc entries that have been checkpointed but are not
            // being accessed by active tx's.
            shard_->Clean();
            if (shard_->Full() && !shard_->TryHeapCollect() &&
                !table_name_.IsMeta() && !force_emplace)
            {
                return false;
            }
        }

        auto update_cc_entry = [shard = shard_](const SliceDataItem &data_item,
                                                CcEntry<KeyT, ValueT> *cce)
        {
            const ValueT *record =
                static_cast<const ValueT *>(data_item.record_.get());

#ifdef RANGE_PARTITION_ENABLED
            uint32_t rec_store_size =
                data_item.is_deleted_ ? 0
                                      : data_item.key_->Size() + record->Size();
#endif

            // If the in-memory version is from a upload request (i.e.
            // generated sk record from pk), the data store version
            // might be newer. Only overwrite if in memory version is
            // newer.
            const uint64_t cce_version = cce->CommitTs();
            if (cce_version > 1 && data_item.version_ts_ <= cce_version)
            {
#ifdef RANGE_PARTITION_ENABLED
                // Initialize the data store size if it is unspecified
                // before
                if (cce->data_store_size_.load(std::memory_order_acquire) ==
                    INT32_MAX)
                {
                    cce->data_store_size_.store(rec_store_size,
                                                std::memory_order_relaxed);
                }
#endif

#ifndef ON_KEY_OBJECT
                if (shard->EnableMvcc())
                {
                    cce->AddArchiveRecord(std::make_shared<ValueT>(*record),
                                          data_item.is_deleted_
                                              ? RecordStatus::Deleted
                                              : RecordStatus::Normal,
                                          data_item.version_ts_);
                }
#endif
                cce->SetCkptTs(data_item.version_ts_);

                // The cc entry's commit ts is 1 when it is initialized.
                // Commit ts greater than 1 means that the key is
                // already cached in memory.
                return;
            }

#ifndef ON_KEY_OBJECT
            if (cce->payload_.use_count() == 1)
            {
                *(cce->payload_) = *record;
            }
            else
            {
                cce->payload_ = std::make_shared<ValueT>(*record);
            }
#else
            assert(false);
            cce->payload_.reset(
                static_cast<ValueT *>(record->Clone().release()));
#endif

            RecordStatus status = data_item.is_deleted_ ? RecordStatus::Deleted
                                                        : RecordStatus::Normal;
            cce->SetCommitTsPayloadStatus(data_item.version_ts_, status);
            cce->SetCkptTs(data_item.version_ts_);

#ifdef RANGE_PARTITION_ENABLED
            cce->data_store_size_.store(rec_store_size,
                                        std::memory_order_relaxed);
#endif
        };

        typename decltype(ccmp_)::iterator target_iter;
        CcPage<KeyT, ValueT> *target_page = nullptr;

        if (ccmp_.begin() == ccmp_.end())
        {
            bool inserted;
            // ccmap is empty, insert a page
            std::tie(target_iter, inserted) = ccmp_.try_emplace(
                static_cast<const KeyT &>(*slice_items[first_index].key_),
                this,
                &neg_inf_page_,
                &pos_inf_page_);
            assert(inserted);
        }
        else
        {
            target_iter = ccmp_.upper_bound(
                static_cast<const KeyT &>(*slice_items[first_index].key_));

            if (target_iter != ccmp_.begin())
            {
                target_iter--;
            }
        }

        target_page = &target_iter->second;

        bool is_emplace = false;
        std::vector<KeyT> new_keys;
        std::vector<size_t> new_key_item_idxs;
        new_keys.reserve(CcPage<KeyT, ValueT>::split_threshold_);
        new_key_item_idxs.reserve(CcPage<KeyT, ValueT>::split_threshold_);
        std::vector<size_t> entry_indexs;

        for (size_t item_idx = first_index; item_idx < end_idx;)
        {
            const KeyT *target_key =
                static_cast<const KeyT *>(slice_items[item_idx].key_.get());

            size_t idx_in_page = target_page->Find(*target_key);

            if (idx_in_page != target_page->Size())
            {
                // found
                assert(idx_in_page < target_page->Size());

                is_emplace = false;
                update_cc_entry(slice_items[item_idx],
                                target_page->Entry(idx_in_page));
            }
            else
            {
                // Check whether the key is stored on the next page
                if (target_page->next_page_->FirstKey() <= *target_key)
                {
                    // Batch emplace new keys into this target page.
                    if (!new_keys.empty())
                    {
                        target_page->EmplaceKeys(new_keys, entry_indexs);

                        assert(new_keys.size() == entry_indexs.size());
                        assert(new_key_item_idxs.size() == entry_indexs.size());

                        for (size_t i = 0; i < entry_indexs.size(); ++i)
                        {
                            size_t slice_item_index = new_key_item_idxs[i];
                            update_cc_entry(
                                slice_items[slice_item_index],
                                target_page->Entry(entry_indexs[i]));
                        }

                        TryUpdatePageKey(target_iter);

                        new_keys.clear();
                        new_key_item_idxs.clear();
                    }

                    // Move to next page
                    target_iter++;

                    if (target_iter == ccmp_.end())
                    {
                        target_iter =
                            ccmp_.try_emplace(target_iter,
                                              *target_key,
                                              this,
                                              target_page,
                                              target_page->next_page_);
                    }
                    target_page = &target_iter->second;
                    continue;
                }

                // Page will be full soon.
                if (target_page->Size() + new_keys.size() ==
                    CcPage<KeyT, ValueT>::split_threshold_)
                {
                    // Batch emplace new keys into this target page.
                    if (!new_keys.empty())
                    {
                        target_page->EmplaceKeys(new_keys, entry_indexs);

                        assert(new_keys.size() == entry_indexs.size());
                        assert(new_key_item_idxs.size() == entry_indexs.size());

                        for (size_t i = 0; i < entry_indexs.size(); ++i)
                        {
                            size_t slice_item_index = new_key_item_idxs[i];
                            update_cc_entry(
                                slice_items[slice_item_index],
                                target_page->Entry(entry_indexs[i]));
                        }

                        TryUpdatePageKey(target_iter);

                        new_keys.clear();
                        new_key_item_idxs.clear();

                        assert(target_page->Full());
                    }
                }

                if (target_page->Full() && target_page->LastKey() < *target_key)
                {
                    assert(new_keys.empty());
                    assert(new_key_item_idxs.empty());

                    // target page is full, choose the next page if
                    // `key` can be inserted into next page
                    target_iter++;
                    if (target_iter == ccmp_.end())
                    {
                        // create a new page
                        target_iter =
                            ccmp_.try_emplace(target_iter,
                                              *target_key,
                                              this,
                                              target_page,
                                              target_page->next_page_);
                    }
                    target_page = &target_iter->second;
                }

                if (target_page->Full())
                {
                    assert(new_keys.empty());
                    assert(new_key_item_idxs.empty());

                    // split this page
                    std::vector<KeyT> new_page_keys;
                    std::vector<std::unique_ptr<CcEntry<KeyT, ValueT>>>
                        new_page_entries;
                    uint64_t new_last_commit_ts = 0;
                    target_page->Split(
                        new_page_keys, new_page_entries, new_last_commit_ts);

                    const KeyT &key_of_new_page = *new_page_keys.begin();
                    auto new_page_it =
                        ccmp_.try_emplace(target_iter,
                                          key_of_new_page,
                                          this,
                                          std::move(new_page_keys),
                                          std::move(new_page_entries),
                                          target_page,
                                          target_page->next_page_);
                    CcPage<KeyT, ValueT> *new_page = &new_page_it->second;
                    new_page->last_dirty_commit_ts_ = new_last_commit_ts;

                    for (auto &cce : new_page->entries_)
                    {
                        cce->UpdateCcPage(new_page);
                    }

                    // insert new page into lru list right after old
                    // page
                    if (target_page->lru_next_ != nullptr)
                    {
                        LruPage *next = target_page->lru_next_;
                        new_page->lru_next_ = next;
                        next->lru_prev_ = new_page;
                        target_page->lru_next_ = new_page;
                        new_page->lru_prev_ = target_page;
                        new_page->last_access_ts_ =
                            target_page->last_access_ts_;
                    }

                    if (new_page->FirstKey() <= *target_key)
                    {
                        target_iter = new_page_it;
                        target_page = new_page;
                    }
                }

                // We will insert these keys into the page later. This
                // is to avoid frequent moving of data during insertion
                new_keys.emplace_back(*target_key);
                new_key_item_idxs.emplace_back(item_idx);

                is_emplace = true;
            }

            shard_->UpdateLruList(target_page, is_emplace);
            ++item_idx;
        }

        if (!new_keys.empty())
        {
            target_page->EmplaceKeys(new_keys, entry_indexs);

            assert(new_keys.size() == entry_indexs.size());
            assert(new_key_item_idxs.size() == entry_indexs.size());

            for (size_t i = 0; i < entry_indexs.size(); ++i)
            {
                size_t slice_item_index = new_key_item_idxs[i];
                update_cc_entry(slice_items[slice_item_index],
                                target_page->Entry(entry_indexs[i]));
            }

            TryUpdatePageKey(target_iter);

            new_keys.clear();
            new_key_item_idxs.clear();
        }

        size_ += (end_idx - first_index);

        return true;
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

        // catalog and range ccmap bypass shard memory limit. since
        // checkpointer may emplace ccentry into ccmap.
        if (shard_->Full())
        {
            // The shard has reached the maximal capacity. Tries to
            // clean cc entries that have been checkpointed but are not
            // being accessed by active tx's.
            shard_->Clean();
            if (shard_->Full() && !shard_->TryHeapCollect() &&
                !table_name_.IsMeta() && !force_emplace)
            {
                return End();
            }
        }

        if (ccmp_.begin() == ccmp_.end())
        {
            // ccmap is empty, insert a page
            auto [it, inserted] =
                ccmp_.try_emplace(key, this, &neg_inf_page_, &pos_inf_page_);
            assert(inserted);
            // This silences the -Wunused-but-set-variable warning
            // without any runtime overhead.
            (void) inserted;
        }

        // First locate target page, then find or emplace `key` in the
        // page.
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
            shard_->UpdateLruList(target_page, false);
            Iterator iterator(target_page, idx_in_page, &neg_inf_);
            return iterator;
        }

        // not found, emplace key into target page, split the page if
        // it's full
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

            for (auto &cce : new_page->entries_)
            {
                cce->UpdateCcPage(new_page);
            }

            // insert new page into lru list right after old
            // page
            if (target_page->lru_next_ != nullptr)
            {
                LruPage *next = target_page->lru_next_;
                new_page->lru_next_ = next;
                next->lru_prev_ = new_page;
                target_page->lru_next_ = new_page;
                new_page->lru_prev_ = target_page;
                new_page->last_access_ts_ = target_page->last_access_ts_;
            }

            if (new_page->FirstKey() <= key)
            {
                target_it = new_page_it;
                target_page = new_page;
            }
        }

        idx_in_page = target_page->Emplace(key);
        emplace = true;
        // modify page key in the map if it changed
        TryUpdatePageKey(target_it);

        // update lru list
        shard_->UpdateLruList(target_page, true);
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
     * Whether ScanGap or ScanBoth depends on whether this is
     * range_cc_map scan. For template_cc_map, start from it's gap; for
     * range_cc_map, start from it's key and gap.
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
     * Find lower bound of @param key in map, i.e. the first entry whose
     * key is equal to or greater than @param key. Return an Iterator
     * pointing to this entry, if no such entry, return Begin() which
     * points to neg_inf_.
     * @param key
     * @return
     */
    Iterator LowerBound(const KeyT &key)
    {
        if (&key == NegativeInfinity<KeyT>::Instance())
        {
            return Begin();
        }

        // ccmp_ key is each page's smallest key, so the lower bound of
        // `key` might fall into either of two adjacent pages
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
            // page1->FirstKey < key <= page1->LastKey(), the lower
            // bound of `key` must locate in page1
            size_t idx_in_page = page1->LowerBound(key);
            return Iterator(page1, idx_in_page, &neg_inf_);
        }
        else
        {
            // page1->LastKey() < key <= page2->FirstKey(), the lower
            // bound of `key` must be the first key of page2
            return Iterator(page2, 0, &neg_inf_);
        }
    }

    /**
     * Find upper bound of @param key in map, i.e. the first entry whose
     * key is greater than @param key. Return an Iterator pointing to
     * this entry, if no such entry, return End() which points to
     * pos_inf_.
     * @param key
     * @return
     */
    Iterator UpperBound(const KeyT &key)
    {
        // ccmp_ key is each page's smallest key, so the upper bound of
        // `key` might fall into either of two adjacent pages
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
            // page1->FirstKey <= key < page1->LastKey(), the upper
            // bound of `key` must locate in page1
            size_t idx_in_page = page1->UpperBound(key);
            return Iterator(page1, idx_in_page, &neg_inf_);
        }
        else
        {
            // page1->LastKey() <= key < page2->FirstKey(), the upper
            // bound of `key` must be the first key of page2
            return Iterator(page2, 0, &neg_inf_);
        }
    }

    /**
     * @brief Finds the greatest cc entry whose key is less than or
     * equal to the input key. If the map is empty, the floor key is
     * negative infinity.
     *
     * @param key The input key
     * @return The Iterator pointing to the cc entry whose key is the
     * greatest key less than or equal to the input key. If `key` is
     * positive infinity, the Iterator points to the last key in the
     * map.
     */
    Iterator Floor(const KeyT &key)
    {
        Iterator it = LowerBound(key);
        if (*it->first != key ||
            it == End())  // special case for positive infinity
        {
            // lower bound of a non-negative infinity key should never
            // be Begin()
            assert(it != Begin());
            it--;
        }
        return it;
    }

    /**
     * @brief Searches the start cc entry of a forward scan.
     *
     * @param key Search key
     * @param inclusive Whether or not the start key is included in the
     * scan
     * @param is_include_floor_cce This param is used only by
     * range_cc_map scan, and is always true. Range scan searches for
     * the floor of the search key and returns both its key and gap.
     * @return std::pair<typename std::map<KeyT, CcEntry<KeyT,
     * ValueT>>::const_iterator, ScanType> A pair of a forward map
     * iterator starting from the start cc entry and whether the scan
     * includes the start cc entry's key or gap or both.
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
                // for range_cc_map, start from previous entry's key and
                // gap
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
                // lb_it's key is exactly equal to `key`, start from
                // lb_it and its gap, but not including previous key gap
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
     * @param inclusive Whether or not the start key is included in the
     * scan
     * @return std::pair<typename std::map<KeyT, CcEntry<KeyT,
     * ValueT>>::const_iterator, ScanType> A pair of a backward map
     * iterator starting from the start cc entry and whether the scan
     * includes the start cc entry's key or gap or both.
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
                // map empty or every key in map is greater than `key`,
                // return neg_inf_'s gap
                return std::make_pair(ub_it, ScanType::ScanGap);
            }
            else
            {
                ub_it--;
                // now, ub_it is the greatest entry equal to or less
                // than `key`

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
                // map empty or every key in ccm_ is greater than or
                // equal to `key`, return neg_inf_ gap
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
#ifndef ON_KEY_OBJECT
            VersionResultRecord<ValueT> v_rec;
            cce->MvccGet(read_ts, Type(), shard_->LastReadTs(), v_rec);

#ifdef RANGE_PARTITION_ENABLED
            // For snapshot reads, only if the visible version's record
            // status is deleted and no lock has been put on it, should
            // the record be skipped in the result set. Note that if the
            // visible version is mising in memory, the key still needs
            // to be returned. Runtime will use the key to retrieve the
            // visible version from the data store.
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
                    // We're only copying the shared_ptr here so we
                    // exclude the actual payload size.
                }
            }
            tuple->key_ts_ = v_rec.commit_ts_;
            tuple->rec_status_ = v_rec.payload_status_;
#endif
        }
        else
        {
            const RecordStatus rec_status = cce->PayloadStatus();
#ifdef RANGE_PARTITION_ENABLED
            if (rec_status == RecordStatus::Normal ||
                (rec_status == RecordStatus::Deleted && keep_deleted))
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

            if (rec_status == RecordStatus::Normal ||
                (is_ckpt_delta && rec_status == RecordStatus::Deleted))
            {
                if (cce->payload_ != nullptr)
                {
#ifndef ON_KEY_OBJECT
                    tuple->SetRecord(cce->payload_);
#else
                    // Redis KEYS command doesn't need value. But ObjectCcMap
                    // doesn't override ScanKey() on local ccmap. Thus,
                    // TemplateCcMap::ScanKey() on local ccmp may be called, and
                    // it need not set record.
#endif
                    // We're only copying the shared_ptr here so we
                    // exclude the actual payload size.
                }
            }
            tuple->rec_status_ = rec_status;
            tuple->key_ts_ = cce->CommitTs();
        }

        tuple->gap_ts_ = 0;
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
#ifndef ON_KEY_OBJECT
            VersionResultRecord<ValueT> v_rec;
            cce->MvccGet(read_ts, Type(), shard_->LastReadTs(), v_rec);

            // For snapshot reads, only if the visible version's record
            // status is deleted and no lock has been put on it, should
            // the record be skipped in the result set. Note that if the
            // visible version is mising in memory, the key still needs
            // to be returned. Runtime will use the key to retrieve the
            // visible version from the data store.
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
#endif
        }
        else
        {
            const RecordStatus rec_status = cce->PayloadStatus();
            if (!(rec_status == RecordStatus::Normal ||
                  (rec_status == RecordStatus::Deleted && keep_deleted)))
            {
                return;
            }
            key->Serialize(remote_cache->keys_);
            tuple_size += key->SerializedLength();

            if (rec_status == RecordStatus::Normal ||
                (is_ckpt_delta && rec_status == RecordStatus::Deleted))
            {
                if (cce->payload_ != nullptr)
                {
#ifndef ON_KEY_OBJECT
                    cce->payload_->Serialize(remote_cache->records_);
                    tuple_size += cce->payload_->SerializedLength();
#else
                    // Redis KEYS command doesn't need value. But ObjectCcMap
                    // doesn't override ScanKey() on local ccmap. Thus,
                    // TemplateCcMap::ScanKey() on local ccmp may be called, and
                    // it need not set record.
#endif
                }
            }
            remote_cache->rec_status_.push_back(
                remote::ToRemoteType::ConvertRecordStatus(rec_status));
            remote_cache->key_ts_.push_back(cce->CommitTs());
        }

        if (include_gap)
        {
            // Gap timestamps are not used at the moment.
            remote_cache->gap_ts_.push_back(0);
        }
        else
        {
            remote_cache->gap_ts_.push_back(0);
        }

        remote_cache->cce_ptr_.push_back(reinterpret_cast<uint64_t>(cce));
        remote_cache->term_.push_back(ng_term);
        // For remote scans, the returned cc entries' node group ID is
        // set on the sender side when the sender receives the response.

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
#ifndef ON_KEY_OBJECT
            VersionResultRecord<ValueT> v_rec;
            cce->MvccGet(read_ts, Type(), shard_->LastReadTs(), v_rec);

#ifdef RANGE_PARTITION_ENABLED
            // For snapshot reads, only if the visible version's record
            // status is deleted and no lock has been put on it, should
            // the record be skipped in the result set. Note that if the
            // visible version is mising in memory, the key still needs
            // to be returned. Runtime will use the key to retrieve the
            // visible version from the data store.
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
#endif
        }
        else
        {
            const RecordStatus rec_status = cce->PayloadStatus();
#ifdef RANGE_PARTITION_ENABLED
            if (rec_status == RecordStatus::Normal ||
                (rec_status == RecordStatus::Deleted && keep_deleted))
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

            if (rec_status == RecordStatus::Normal ||
                (is_ckpt_delta && rec_status == RecordStatus::Deleted))
            {
                tuple->clear_record();
                if (cce->payload_ != nullptr)
                {
#ifndef ON_KEY_OBJECT
                    cce->payload_->Serialize(*tuple->mutable_record());
                    tuple_size += cce->payload_->Size();
#else
                    // Redis KEYS command doesn't need value. But ObjectCcMap
                    // doesn't override ScanKey() on local ccmap. Thus,
                    // TemplateCcMap::ScanKey() on local ccmp may be called, and
                    // it need not set record.
#endif
                }
            }
            tuple->set_rec_status(
                remote::ToRemoteType::ConvertRecordStatus(rec_status));
            tuple->set_key_ts(cce->CommitTs());
        }

        if (include_gap)
        {
            // Gap timestamps are not used at the moment.
            tuple->set_gap_ts(0);
        }
        else
        {
            tuple->set_gap_ts(0);
        }

        remote::CceAddr_msg *cce_addr = tuple->mutable_cce_addr();
        cce_addr->set_cce_ptr(reinterpret_cast<uint64_t>(cce));
        cce_addr->set_term(ng_term);
        // For remote scans, the returned cc entries' node group ID is
        // set on the sender side when the sender receives the response.

        remote_cache->cache_mem_size_ += tuple_size;
    }

    void ScanGap(const KeyT *key,
                 CcEntry<KeyT, ValueT> *cce,
                 TemplateScanTuple<KeyT, ValueT> *tuple,
                 uint32_t ng_id,
                 int64_t ng_term) const
    {
        tuple->key_ts_ = 0;
        tuple->gap_ts_ = 0;
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
        tuple->set_gap_ts(0);

        remote::CceAddr_msg *cce_addr = tuple->mutable_cce_addr();
        cce_addr->set_cce_ptr(reinterpret_cast<uint64_t>(cce));
        cce_addr->set_term(ng_term);

        // For remote scans, the returned cc entries' node group ID is
        // set on the sender side when the sender receives the response.
    }

    void ScanGap(const KeyT *key,
                 CcEntry<KeyT, ValueT> *cce,
                 RemoteScanSliceCache *cache,
                 int64_t ng_term) const
    {
        cache->key_ts_.push_back(0);
        cache->gap_ts_.push_back(0);

        cache->cce_ptr_.push_back(reinterpret_cast<uint64_t>(cce));
        cache->term_.push_back(ng_term);

        // For remote scans, the returned cc entries' node group ID is
        // set on the sender side when the sender receives the response.
    }

    /**
     * @brief If key is in range of [start key, end key), left inclusive
     * right open.
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

    /**
     * If the a record is according to the conditions, return true, or
     * return false to neglect this record.
     */
    virtual bool FilterRecord(const KeyT *key,
                              const CcEntry<KeyT, ValueT> *cce,
                              int32_t obj_type,
                              const std::string_view &scan_pattern)
    {
        return true;
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
     * @param free_cnt
     * @param clean_type
     * @return The bool value stand for the clean status, if return
     * false, it mean that the target ccentry can not be clean, the
     * caller should retry the kickout request. Currently, only when
     * clean_type is CleanForSplitRange and CleanForAlterTable care this
     * status.
     */
    bool CleanPage(CcPage<KeyT, ValueT> *page,
                   size_t &free_cnt,
                   CleanType clean_type,
                   KickoutCcEntryCc *kickout_cc = nullptr)
    {
        std::vector<KeyT> &keys = page->keys_;
        std::vector<std::unique_ptr<CcEntry<KeyT, ValueT>>> &entries =
            page->entries_;
        const KeyT *start_key = nullptr;
        const KeyT *end_key = nullptr;
        if (kickout_cc)
        {
            start_key = static_cast<const KeyT *>(kickout_cc->StartKey());
            end_key = static_cast<const KeyT *>(kickout_cc->EndKey());
        }
        auto key_insert_it = keys.begin();
        auto entry_insert_it = entries.begin();

        uint64_t last_commit_ts = 0;

        // Whether all ccentries whose commit_ts < @ckpt_ts have been
        // cleaned.
        bool clean_success = true;
        auto key_it = keys.begin();
        auto entry_it = entries.begin();
        for (; key_it != keys.end(); key_it++, entry_it++)
        {
            CcEntry<KeyT, ValueT> *cce = entry_it->get();

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
                can_be_clean = cce->CommitTs() <= kickout_cc->CkptTs() &&
                               cce->CommitTs() > 1 && cce->IsFree();
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
                bool kick_ret = shard_->local_shards_.KickoutKeyInSlice(
                    table_name_, cc_ng_id_, *key_it);
                if (!kick_ret)
                {
                    // If the slice is being loaded or pinned, do not
                    // clean it.
                    *key_insert_it = std::move(*key_it);
                    *entry_insert_it = std::move(*entry_it);
                    key_insert_it++;
                    entry_insert_it++;
                    // record the commit_ts if the entry cannot be
                    // cleaned.
                    last_commit_ts = std::max(last_commit_ts, cce->CommitTs());
                    // The ccentry that expect to clean cannot be kick
                    // out. In this branch, only when clean_type is
                    // CleanForSplitRange or CleanForAlterTable care
                    // this clean status
                    if (clean_type == CleanType::CleanForSplitRange ||
                        clean_type == CleanType::CleanForAlterTable)
                    {
                        clean_success = false;
                    }
                }
                else
                {
                    // free entries will be erased
                    free_cnt++;
                }
#else
                // free entries will be erased
                free_cnt++;
#endif
            }
            else
            {
                // The ccentry that expect to clean cannot be kick out.
                // In this branch, only when clean_type is
                // CleanForAlterTable care this clean status. For
                // CleanForSplitRange, if can_be_clean is false, it mean
                // that this ccentry is not the target one, so it do not
                // care this clean status.
                if (clean_type == CleanType::CleanForAlterTable &&
                    cce->CommitTs() <= kickout_cc->CkptTs() &&
                    cce->CommitTs() > 1)
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
                last_commit_ts = std::max(last_commit_ts, cce->CommitTs());
            }
        }
        keys.erase(key_insert_it, keys.end());
        entries.erase(entry_insert_it, entries.end());
        // During range split kickout, we might clean cc entries that
        // are still dirty from page. So the max dirty ts might
        // decrease.
        page->last_dirty_commit_ts_ =
            std::min(last_commit_ts, page->last_dirty_commit_ts_);

        return clean_success;
    }

    /**
     * Redistribute entries between page1 and page2. This happens when
     * one page is cleaned and its size is below merge threshold and it
     * needs to borrow entries from its siblings to keep the tree
     * balanced.
     *
     * @param page1_it
     * @param page2_it
     * @param page1_last_read_ts
     * @param page2_last_read_ts
     * @return
     */
    void RedistributeBetweenPages(
        typename std::map<KeyT, CcPage<KeyT, ValueT>>::iterator &page1_it,
        typename std::map<KeyT, CcPage<KeyT, ValueT>>::iterator &page2_it)
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

            // Updates the parent page of the locks of the to-be-moved
            // entries.
            for (auto page1_it = page1.entries_.begin() + move_pos;
                 page1_it != page1.entries_.end();
                 ++page1_it)
            {
                (*page1_it)->UpdateCcPage(&page2);
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

            // Updates the parent page of the locks of the to-be-moved
            // entries.
            for (auto page2_it = page2.entries_.begin();
                 page2_it != page2.entries_.begin() + move_idx;
                 ++page2_it)
            {
                (*page2_it)->UpdateCcPage(&page1);
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

        // The locks of cc entries of a page should point to the same
        // page.
        assert(
            [&]()
            {
                for (const auto &cce : page1.entries_)
                {
                    if (cce->GetCcPage() != nullptr &&
                        cce->GetCcPage() != &page1)
                    {
                        return false;
                    }
                }

                return true;
            }());

        assert(
            [&]()
            {
                for (const auto &cce : page2.entries_)
                {
                    if (cce->GetCcPage() != nullptr &&
                        cce->GetCcPage() != &page2)
                    {
                        return false;
                    }
                }
                return true;
            }());

        // update page key in the map
        TryUpdatePageKey(page1_it);
        TryUpdatePageKey(page2_it);

        // update LRU list
        // after redistribution, the two pages should be seen as one in
        // the LRU list, insert the less recently used page after the
        // more recently used one
        LruPage *less_recently_used = nullptr, *more_recently_used = nullptr;
        if (page1.last_access_ts_ > page2.last_access_ts_)
        {
            more_recently_used = &page1;
            less_recently_used = &page2;
        }
        else
        {
            more_recently_used = &page2;
            less_recently_used = &page1;
        }

        if (less_recently_used->lru_next_ != nullptr)
        {
            shard_->DetachLru(less_recently_used);
        }
        LruPage *next = more_recently_used->lru_next_;
        less_recently_used->lru_next_ = next;
        next->lru_prev_ = less_recently_used;
        less_recently_used->lru_prev_ = more_recently_used;
        more_recently_used->lru_next_ = less_recently_used;
        less_recently_used->last_access_ts_ =
            more_recently_used->last_access_ts_;
    }

    /**
     * Merge page1 and page2. Update the map and lru list after the
     * merge.
     *
     * @param page1_it The iterator to the merged page
     * @param page2_it The iterator to the discarded page
     * @param page
     */
    void MergePages(
        typename std::map<KeyT, CcPage<KeyT, ValueT>>::iterator &page1_it,
        typename std::map<KeyT, CcPage<KeyT, ValueT>>::iterator &page2_it,
        CcPage<KeyT, ValueT> *page)
    {
        CcPage<KeyT, ValueT> *page1 = &page1_it->second;
        CcPage<KeyT, ValueT> *page2 = &page2_it->second;

        auto merged_page_it = page1_it;
        auto discarded_page_it = page2_it;
        CcPage<KeyT, ValueT> *merged_page = &merged_page_it->second;
        CcPage<KeyT, ValueT> *discarded_page = &discarded_page_it->second;

        // merge the key vector and entry vector
        std::vector<KeyT> merged_keys = std::move(page1->keys_);
        merged_keys.insert(merged_keys.end(),
                           std::make_move_iterator(page2->keys_.begin()),
                           std::make_move_iterator(page2->keys_.end()));
        std::vector<std::unique_ptr<CcEntry<KeyT, ValueT>>> merged_entries =
            std::move(page1->entries_);

        for (auto it = page2->entries_.begin(); it != page2->entries_.end();
             ++it)
        {
            (*it)->UpdateCcPage(merged_page);
        }

        merged_entries.insert(merged_entries.end(),
                              std::make_move_iterator(page2->entries_.begin()),
                              std::make_move_iterator(page2->entries_.end()));
        merged_page->keys_ = std::move(merged_keys);
        merged_page->entries_ = std::move(merged_entries);

        assert(
            [&]()
            {
                for (const auto &cce : merged_page->entries_)
                {
                    if (cce->GetCcPage() != nullptr &&
                        cce->GetCcPage() != merged_page)
                    {
                        return false;
                    }
                }
                return true;
            }());

        // Update the page order list.
        CcPage<KeyT, ValueT> *map_prev = page1->prev_page_;
        CcPage<KeyT, ValueT> *map_next = page2->next_page_;
        merged_page->prev_page_ = map_prev;
        merged_page->next_page_ = map_next;
        map_prev->next_page_ = merged_page;
        map_next->prev_page_ = merged_page;

        // Update the LRU list.
        // the merged page should take the more recently used page's
        // position in the LRU list
        if (page1->lru_next_ == page2 || page1->lru_prev_ == page2)
        {
            // corner case: the two pages are adjacent in LRU list, just
            // detach the discarded page
            if (discarded_page->lru_next_ != nullptr)
            {
                shard_->DetachLru(discarded_page);
            }
        }
        else
        {
            LruPage *lru_prev = nullptr, *lru_next = nullptr;
            uint64_t merge_last_access_ts;
            if (page1->last_access_ts_ > page2->last_access_ts_)
            {
                lru_prev = page1->lru_prev_;
                lru_next = page1->lru_next_;
                merge_last_access_ts = page1->last_access_ts_;
            }
            else
            {
                lru_prev = page2->lru_prev_;
                lru_next = page2->lru_next_;
                merge_last_access_ts = page2->last_access_ts_;
            }

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
            merged_page->last_access_ts_ = merge_last_access_ts;
        }

        // last_dirty_commit_ts_ of merged page will inherit the larger
        // one.
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
        return &neg_inf_page_;
    }

    CcPage<KeyT, ValueT> *PagePosInf()
    {
        return &pos_inf_page_;
    }

    std::map<KeyT, CcPage<KeyT, ValueT>> ccmp_;
    CcPage<KeyT, ValueT> neg_inf_page_, pos_inf_page_;
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
