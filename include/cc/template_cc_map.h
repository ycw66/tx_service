#pragma once

#include <algorithm>  // std::max
#include <unordered_set>

#include "cc_entry.h"
#include "cc_map.h"
#include "cc_protocol.h"
#include "cc_request.h"
#include "cc_shard.h"
#include "fault/fault_inject.h"
#include "proto/cc_request.pb.h"
#include "remote/remote_cc_request.h"
#include "sharder.h"
#include "tx_execution.h"
#include "tx_id.h"
#include "tx_key.h"
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
    virtual ~TemplateCcMap() = default;

    TemplateCcMap(CcShard *shard,
                  uint64_t schema_ts,
                  const Schema *key_schema = nullptr,
                  const Schema *rec_schema = nullptr)
        : CcMap(shard, schema_ts),
          ccm_(),
          neg_inf_(this),
          pos_inf_(this),
          key_schema_(key_schema),
          record_schema_(rec_schema)
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
    }

    bool Execute(AcquireCc &req) override
    {
        CcHandlerResult<AcquireKeyResult> *hd_res = req.Result();
        AcquireKeyResult &acquire_key_result = hd_res->Value();
        CcEntryAddr &cce_addr = acquire_key_result.cce_addr_;
        CcEntry<KeyT, ValueT> *cce_ptr = nullptr;
        bool resume = false;
        const KeyT *target_key = nullptr;
        KeyT decoded_key;

        int64_t ng_term = Sharder::Instance().LeaderTerm(req.NodeGroupId());
        if (ng_term < 0)
        {
            hd_res->SetError(-1);
            return true;
        }

        if (req.CcePtr() != nullptr)
        {
            // The request was blocked before and is now unblocked.
            resume = true;
            cce_ptr = static_cast<CcEntry<KeyT, ValueT> *>(req.CcePtr());
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
                decoded_key.Deserialize(key_str->data(), offset, key_schema_);
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
                    if (cce_ptr->payload_status_ != RecordStatus::Deleted)
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
        const TxId *txid = req.Txid();

        if (cce_addr.CcePtr() == 0)
        {
            // This is an insert. The new insert results in an insert entry in
            // the intention set of the preceding key's gap.

            auto ins_it = cc_entry.insert_intention_set_.find(target_key);
            if (ins_it != cc_entry.insert_intention_set_.end())
            {
                InsertEntry<KeyT, ValueT> &insert_entry = *ins_it->second;
                if (!(insert_entry.tx_id_ == *txid))
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
                    *target_key, *txid, cce_ptr);
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
            int64_t tx_term = req.TxTerm();

            // On execution resumption, the write lock has been acquired when
            // being unblocked.
            bool lock_success = resume ? true
                                       : cc_entry.key_lock_.AcquireWriteLock(
                                             &req, tx_term, req.Protocol());

            if (lock_success)
            {
                shard_->UpsertLockHoldingTx(req.Txn(), req.TxTerm(), cce_ptr);
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
            else
            {
                const std::unordered_set<TxNumber> &read_locks =
                    cc_entry.key_lock_.ReadLocks();
                if (read_locks.size() > 0)
                {
                    // If the request fails to acquire the write lock because of
                    // read locks, checks each read lock and recovers if needed.
                    for (const auto &read_tx : read_locks)
                    {
                        shard_->CheckRecoverTx(
                            read_tx, req.NodeGroupId(), ng_term);
                    }
                }
                else
                {
                    // The request fails because of write-write conflicts.
                    assert(cc_entry.key_lock_.HasWriteLock());
                    shard_->CheckRecoverTx(cc_entry.key_lock_.WriteLockTx(),
                                           req.NodeGroupId(),
                                           ng_term);
                }

                if (req.Protocol() == CcProtocol::OCC ||
                    req.Protocol() == CcProtocol::MVCC)
                {
                    // For OCC/MVCC, a conflict causes the tx to abort
                    // immediately.
                    hd_res->SetError(1);
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
        }

        return true;
    }

    bool Execute(PostWriteCc &req) override
    {
        const CcEntryAddr &cce_addr = *req.CceAddr();
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
        bool is_del = req.IsDeleted();

        if (cce_addr.InsertPtr() != 0)
        {
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
                assert(ite->second->tx_id_.TxNumber() == txn);

                shard_->mem_usage_ -= new_cce->payload_.MemUsage();
                if (payload_str == nullptr)
                {
                    new_cce->payload_ = *commit_val;
                }
                else
                {
                    size_t offset = 0;
                    new_cce->payload_.Deserialize(payload_str->data(), offset);
                }
                shard_->mem_usage_ += new_cce->payload_.MemUsage();
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
                size_t payload_size = new_cce->payload_.MemUsage();
                new_cce->parent_map_->shard_->UpdateEstimateLogSize(
                    new_cce, key_size, payload_size);
            }

            req.Result()->SetFinished();
            prior_cce.gap_lock_.ReleaseWriteLock(txn, shard_);
            // The insert places a write lock on the prior cc entry's gap.
            shard_->DeleteLockHolidngTx(txn, &prior_cce);
            return true;
        }
        else
        {
            // upsert and delete branch.
            assert(cce_addr.CcePtr() != 0);

            CcEntry<KeyT, ValueT> &cce =
                *reinterpret_cast<CcEntry<KeyT, ValueT> *>(cce_addr.CcePtr());

            if (cce.key_lock_.WriteLockTx() != txn)
            {
                req.Result()->SetFinished();
                return true;
            }

            if (commit_ts > 0)
            {
                // for mvcc
                if (req.Protocol() == CcProtocol::MVCC)
                {
                    // recycle before archive
                    // TODO(lzx): use scheduled tasks to perform recycling
                    shard_->mem_usage_ -= cce.KickOutArchiveRecords(commit_ts);

                    size_t added_mem_usage = cce.ArchiveBeforeUpdate();
                    shard_->mem_usage_ += added_mem_usage;
                }

                cce.commit_ts_ = commit_ts;

                shard_->mem_usage_ -= cce.payload_.MemUsage();
                if (payload_str == nullptr && !is_del)
                {
                    cce.payload_ = *commit_val;
                }
                else if (!is_del)
                {
                    size_t offset = 0;
                    cce.payload_.Deserialize(payload_str->data(), offset);
                }
                shard_->mem_usage_ += cce.payload_.MemUsage();
                cce.payload_status_ =
                    is_del ? RecordStatus::Deleted : RecordStatus::Normal;

                TryInsertCkptList(&cce);

                size_t key_size = cce.key_->MemUsage();
                size_t payload_size = cce.payload_.MemUsage();
                cce.parent_map_->shard_->UpdateEstimateLogSize(
                    &cce, key_size, payload_size);
            }

            req.Result()->SetFinished();
            cce.key_lock_.ReleaseWriteLock(txn, shard_);
            cce.wlock_ts_ = 0;
            shard_->DeleteLockHolidngTx(txn, &cce);
            return true;
        }
    }

    bool Execute(AcquireAllCc &req) override
    {
        CcHandlerResult<AcquireAllResult> *hd_res = req.Result();
        AcquireAllResult &acquire_all_result = hd_res->Value();
        CcEntry<KeyT, ValueT> *cce_ptr = nullptr;
        bool resume = false;
        const KeyT *target_key = nullptr;

        uint32_t ng_id = req.NodeGroupId();
        int64_t ng_term = Sharder::Instance().LeaderTerm(ng_id);
        if (ng_term < 0)
        {
            hd_res->SetError(-1);
            return true;
        }

        uint16_t tx_core_id = ((req.Txn() >> 32L) & 0x3FF) % shard_->core_cnt_;

        if (req.CcePtr() != nullptr)
        {
            // The request was blocked before and is now unblocked.
            resume = true;
            cce_ptr = static_cast<CcEntry<KeyT, ValueT> *>(req.CcePtr());
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
                decoded_key->Deserialize(key_str->data(), offset, key_schema_);
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
                if (!(insert_entry.tx_id_.TxNumber() == txn))
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

            // On execution resumption, the write lock/intent has been acquired.
            bool lock_success = false;
            if (!resume)
            {
                if (req.GetLockType() == LockType::WriteIntent)
                {
                    lock_success = cc_entry.key_lock_.AcquireWriteIntent(
                        &req, tx_term, req.Protocol());
                }
                else if (req.GetLockType() == LockType::WriteLock)
                {
                    lock_success = cc_entry.key_lock_.AcquireWriteLock(
                        &req, tx_term, req.Protocol());
                }
            }
            else
            {
                lock_success = true;
            }

            if (lock_success)
            {
                shard_->UpsertLockHoldingTx(req.Txn(), req.TxTerm(), cce_ptr);

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
            }
            else
            {
                if (req.GetLockType() == LockType::WriteIntent &&
                    cc_entry.key_lock_.HasWriteLock())
                {
                    shard_->CheckRecoverTx(cc_entry.key_lock_.WriteLockTx(),
                                           req.NodeGroupId(),
                                           ng_term);
                }
                else if (req.GetLockType() == LockType::WriteLock)
                {
                    const std::unordered_set<TxNumber> &read_locks =
                        cc_entry.key_lock_.ReadLocks();
                    if (!read_locks.empty())
                    {
                        // If the request fails to acquire the write lock
                        // because of read locks, checks each read lock and
                        // recovers if needed.
                        for (const auto &read_tx : read_locks)
                        {
                            shard_->CheckRecoverTx(
                                read_tx, req.NodeGroupId(), ng_term);
                        }
                    }
                    else if (cc_entry.key_lock_.HasWriteIntent())
                    {
                        // The request fails because of the write intent.
                        shard_->CheckRecoverTx(
                            cc_entry.key_lock_.WriteIntentTx(),
                            req.NodeGroupId(),
                            ng_term);
                    }
                    else if (cc_entry.key_lock_.HasWriteLock())
                    {
                        // The request fails because of the write lock.
                        shard_->CheckRecoverTx(cc_entry.key_lock_.WriteLockTx(),
                                               req.NodeGroupId(),
                                               ng_term);
                    }
                }

                if (req.Protocol() == CcProtocol::OCC ||
                    req.Protocol() == CcProtocol::MVCC)
                {
                    // For OCC/MVCC, a conflict causes the tx to abort
                    // immediately.
                    hd_res->SetError(1);
                    return true;
                }
                else
                {
                    // For 2PL, a conflict blocks the tx by putting it into the
                    // lock's blocking queue.

                    uint32_t tx_node = (req.Txn() >> 32L) >> 10;
                    if (tx_node != req.NodeGroupId())
                    {
                        req.Result()->Value().node_term_ = ng_term;
                        // If the request comes from a remote node, sends
                        // acknowledgement to the sender when the request is
                        // blocked.
                        remote::RemoteAcquireAll &remote_req =
                            static_cast<remote::RemoteAcquireAll &>(req);
                        remote_req.Acknowledge();
                    }

                    return false;
                }
            }
        }

        return true;
    }

    bool Execute(PostWriteAllCc &req) override
    {
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
            decoded_key->Deserialize(key_str->data(), offset, key_schema_);
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

                    new_cce->payload_ = *payload;
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
                cce_ptr->gap_lock_.ReleaseWriteLock(txn, shard_);
                // The insert places a write lock on the prior cc entry's gap.
                shard_->DeleteLockHolidngTx(txn, cce_ptr);
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
            LockType lk_type = LockType::NoLock;

            if (cce_ptr->key_lock_.HasWriteLock() &&
                cce_ptr->key_lock_.WriteLockTx() == txn)
            {
                lk_type = LockType::WriteLock;
            }
            else if (cce_ptr->key_lock_.HasWriteIntent() &&
                     cce_ptr->key_lock_.WriteIntentTx() == txn)
            {
                lk_type = LockType::WriteIntent;
            }

            if (lk_type == LockType::WriteLock ||
                lk_type == LockType::WriteIntent)
            {
                if (commit_ts > 0)
                {
                    if (req.CommitType() == PostWriteType::PrepareCommit)
                    {
                        cce_ptr->payload_ = *payload;
                    }
                    else
                    {
                        cce_ptr->commit_ts_ = commit_ts;
                        if (req.DmlOp() == DmlOperation::Delete)
                        {
                            cce_ptr->payload_status_ = RecordStatus::Deleted;
                        }
                        else
                        {
                            cce_ptr->payload_ = *payload;
                            cce_ptr->payload_status_ = RecordStatus::Normal;
                        }
                    }

                    TryInsertCkptList(cce_ptr);
                }

                // When commit_ts = 0, the request removes the write lock
                // without installing a new value.

                if (req.CommitType() != PostWriteType::PrepareCommit)
                {
                    // For prepare commit, the request installs the value,
                    // but does not release the write intent/lock.
                    if (lk_type == LockType::WriteLock)
                    {
                        cce_ptr->key_lock_.ReleaseWriteLock(txn, shard_);
                        shard_->DeleteLockHolidngTx(txn, cce_ptr);
                    }
                    else if (lk_type == LockType::WriteIntent)
                    {
                        cce_ptr->key_lock_.ReleaseWriteIntent(txn, shard_);
                        shard_->DeleteLockHolidngTx(txn, cce_ptr);
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
        ACTION_FAULT_INJECTOR("before_post_read");
        auto hd_res = req.Result();

        const CcEntryAddr &cce_addr = *req.CceAddr();
        if (!Sharder::Instance().CheckLeaderTerm(cce_addr.NodeGroupId(),
                                                 cce_addr.Term()))
        {
            hd_res->SetError(-1);
            return true;
        }

        // validate rset cce
        CcEntry<KeyT, ValueT> &cc_entry =
            *reinterpret_cast<CcEntry<KeyT, ValueT> *>(cce_addr.CcePtr());

        // read_entry.version_ts_: cc_entry.commit_ts_ before validation
        uint64_t key_ts = req.KeyTs();
        uint64_t gap_ts = req.GapTs();
        // the txm.commit_ts_
        uint64_t commit_ts = req.CommitTs();
        TxNumber txn = req.Txn();

        if ((key_ts > 0 && key_ts != cc_entry.commit_ts_) ||
            (gap_ts > 0 && gap_ts != cc_entry.gap_commit_ts_))
        {
            // 2PL is a blocking protocol. Once a read lock is acquired, no one
            // can possibly change the key. There is no validation step under
            // MVCC protocol.(MVCC using history versions to ensure repeatable
            // read.) So, this branch is only reachable for OCC protocol
            // validating version stability.
            assert(req.Protocol() == CcProtocol::OCC);

            // Releases intentions for OCC protocols.
            if (key_ts > 0)
            {
                cc_entry.key_lock_.ReleaseReadIntent(txn);
            }

            if (gap_ts > 0)
            {
                cc_entry.gap_lock_.ReleaseReadIntent(txn);
            }

            hd_res->SetError(1);  // broken repeatable read, set error.
        }
        else if (req.Protocol() == CcProtocol::OCC)
        {
            std::vector<TxId> &conflicting_txs = hd_res->Value();

            if (gap_ts > 0)
            {
                cc_entry.gap_last_read_ts_ =
                    std::max(cc_entry.gap_last_read_ts_, commit_ts);

                conflicting_txs.reserve(cc_entry.insert_intention_set_.size() +
                                        1);

                for (auto it = cc_entry.insert_intention_set_.begin();
                     it != cc_entry.insert_intention_set_.end();
                     ++it)
                {
                    conflicting_txs.emplace_back(it->second->tx_id_);
                }
            }
            cc_entry.gap_lock_.ReleaseReadIntent(txn);

            if (key_ts > 0)
            {
                cc_entry.last_read_ts_ =
                    std::max(cc_entry.last_read_ts_, commit_ts);

                if (cc_entry.key_lock_.HasWriteLock())
                {
                    conflicting_txs.emplace_back(
                        cc_entry.key_lock_.WriteLockTx());
                }
            }
            cc_entry.key_lock_.ReleaseReadIntent(txn);

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

            // ReadCc may use different lock type when acquiring the lock, for
            // example, select for update would acquire write intent. As a
            // result, we should also release the corresponding lock/intent as
            // well.
            cc_entry.key_lock_.ReleaseLock(txn, shard_, req.GetLockType());
            cc_entry.gap_lock_.ReleaseLock(txn, shard_, req.GetLockType());
        }

        shard_->DeleteLockHolidngTx(txn, &cc_entry);
        return true;
    }

    bool Execute(ReadCc &req) override
    {
        auto hd_res = req.Result();

        CcEntryAddr &cce_addr = hd_res->Value().cce_addr_;
        CcEntry<KeyT, ValueT> *cce = nullptr;

        int64_t ng_term = Sharder::Instance().LeaderTerm(req.NodeGroupId());
        if (ng_term < 0)
        {
            hd_res->SetError(-1);
            return true;
        }

        if (req.CcePtr() != nullptr)
        {
            // The request was blocked before. This is execution resumption
            // after the request is unblocked. The read lock/intention must have
            // been acquired.
            cce = static_cast<CcEntry<KeyT, ValueT> *>(req.CcePtr());
        }
        else if (cce_addr.CcePtr() == 0)
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
                    req.KeyBlob()->data(), offset, key_schema_);
                cce = FindEmplace(decoded_key, req.ReadTimestamp());
            }

            // The read request accesses a new key not in the cc map. But
            // the cc map is full and cannot allocates a new entry.
            if (cce == nullptr)
            {
                shard_->Enqueue(shard_->LocalCoreId(), &req);
                return false;
            }

            req.SetCcePtr(cce);
            cce_addr.SetCce(
                reinterpret_cast<uint64_t>(cce), ng_term, req.NodeGroupId());

            if (req.Isolation() >= IsolationLevel::RepeatableRead)
            {
                TxNumber tx_number = req.Txn();
                int64_t tx_term = req.TxTerm();
                uint32_t cce_node_group_id = req.NodeGroupId();

                if (req.GetLockType() == LockType::ReadLock)
                {
                    bool lock_success =
                        ReadLockCce(cce, req, tx_term, cce_node_group_id);

                    if (!lock_success)
                    {
                        uint32_t tx_node = (tx_number >> 32L) >> 10;
                        if (tx_node != cce_node_group_id)
                        {
                            // If the read request comes from a remote node,
                            // sends acknowledgement to the sender when the
                            // request is blocked.
                            remote::RemoteRead &remote_req =
                                static_cast<remote::RemoteRead &>(req);
                            remote_req.Acknowledge();
                        }
                        // ReadLock fail should stop the execution of current
                        // ReadCc request since it's already in blocking queue.

                        return false;
                    }
                }
                else
                {
                    // ReadIntention prevents ccentry being kicked out from
                    // cache, but will not block write lock.
                    cce->key_lock_.AcquireReadIntent(tx_number);
                }

                shard_->UpsertLockHoldingTx(tx_number, tx_term, cce);
            }
        }
        else
        {
            // For the read-outside request whose goal is to bring in a
            // record from the data store for caching, the cc entry's
            // address is known.
            assert(req.Type() != ReadType::Inside);
            assert(req.NodeGroupId() == cce_addr.NodeGroupId());
            cce = reinterpret_cast<CcEntry<KeyT, ValueT> *>(cce_addr.CcePtr());
        }

        // The request brings in the record to the cc entry for caching if
        // cce->payload_status_ is Unknown which means it doesn't override by
        // another transaction yet.
        if (cce->payload_status_ == RecordStatus::Unknown)
        {
            if (req.Type() == ReadType::OutsideNormal)
            {
                if (req.Record() != nullptr)
                {
                    ValueT *typed_rec = static_cast<ValueT *>(req.Record());
                    cce->payload_ = *typed_rec;
                }
                else
                {
                    assert(req.RecordBlob() != nullptr);

                    size_t offset = 0;
                    cce->payload_.Deserialize(req.RecordBlob()->data(), offset);
                }
                cce->payload_status_ = RecordStatus::Normal;
                cce->commit_ts_ = req.ReadTimestamp();
            }
            // set tomb ccentry to prevent access data store again.
            else if (req.Type() == ReadType::OutsideDeleted)
            {
                cce->payload_status_ = RecordStatus::Deleted;
                cce->commit_ts_ = req.ReadTimestamp();
            }
        }

        if (req.Isolation() == IsolationLevel::Snapshot)
        {
            assert(req.Protocol() == CcProtocol::MVCC);
            assert(req.Type() == ReadType::Inside);

            VersionRecord<ValueT> v_rec;
            bool res = cce->MvccGet(req.ReadTimestamp(), v_rec);
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

            if (req.Record() != nullptr)
            {
                ValueT *typed_rec = static_cast<ValueT *>(req.Record());
                *typed_rec = cce->payload_;
            }
            else
            {
                assert(req.RecordBlob() != nullptr);
                cce->payload_.Serialize(*req.RecordBlob());
            }
        }

        hd_res->Value().ts_ = cce->commit_ts_;
        hd_res->Value().rec_status_ = cce->payload_status_;

        hd_res->SetFinished();
        return true;
    }

    bool Execute(remote::RemoteReadOutside &req) override
    {
        const CcEntryAddr &cce_addr = req.cce_addr_;
        if (!Sharder::Instance().CheckLeaderTerm(cce_addr.NodeGroupId(),
                                                 cce_addr.Term()))
        {
            return true;
        }

        CcEntry<KeyT, ValueT> *cce =
            reinterpret_cast<CcEntry<KeyT, ValueT> *>(cce_addr.CcePtr());

        if (cce->payload_status_ == RecordStatus::Unknown)
        {
            assert(cce->commit_ts_ == 1);
            if (req.is_deleted_)
            {
                cce->payload_status_ = RecordStatus::Deleted;
            }
            else
            {
                size_t offset = 0;
                cce->payload_.Deserialize(req.rec_str_->data(), offset);
                cce->payload_status_ = RecordStatus::Normal;
            }
            cce->commit_ts_ = req.CommitTs();
        }

        req.Finish();
        return true;
    }

    bool Execute(ScanCloseCc &req) override
    {
        return true;
    }

    bool Execute(ScanOpenBatchCc &req) override
    {
        // Before the scan open request is enqueued, the local node's term is
        // obtained and kept in the cc request. This is to avoid getting the
        // node's terms repeatedly in each core, as the scan request is
        // dispatched to all cores.

        const KeyT *look_key = static_cast<const KeyT *>(req.start_key_);
        TemplateScanCache<KeyT, ValueT> *typed_cache =
            static_cast<TemplateScanCache<KeyT, ValueT> *>(req.scan_cache_);

        if (req.direct_ == ScanDirection::Forward)
        {
            CcEntry<KeyT, ValueT> *floor_cce = nullptr;
            if (req.CcePtr() != nullptr)
            {
                floor_cce = static_cast<CcEntry<KeyT, ValueT> *>(req.CcePtr());
                req.SetCcePtr(nullptr);
                // Lock has been acquired
            }
            else
            {
                floor_cce = look_key == NegativeInfinity<KeyT>::Instance()
                                ? &neg_inf_
                                : Floor(*look_key);
                assert(floor_cce != nullptr);

                TemplateScanTuple<KeyT, ValueT> *scan_tuple = nullptr;

                if (req.is_include_floor_cce_ ||
                    (floor_cce != &neg_inf_ && req.inclusive_ == true &&
                     *look_key == *floor_cce->key_))
                {
                    scan_tuple = typed_cache->AddScanTuple();
                    // The forward scan's starting point is inclusive and
                    // matches a cc entry's key. The scan starts from this cc
                    // entry, including the entry's key and the gap.
                    ScanKey(floor_cce,
                            scan_tuple,
                            true,
                            req.node_group_id_,
                            req.term_);
                    req.SetCcePtr(floor_cce);

                    if (!ConditionalReadLockCce(floor_cce,
                                                req,
                                                req.GetLockType(),
                                                req.TxTerm(),
                                                req.NodeGroupId()))
                    {
                        return false;
                    }
                }
                else if (!req.is_ckpt_delta_)
                {
                    scan_tuple = typed_cache->AddScanTuple();
                    // The forward scan's starting point is exclusive or falls
                    // into the gap of a cc entry. The scan starts from the cc
                    // entry and only includes the entry's gap.
                    ScanGap(
                        floor_cce, scan_tuple, req.node_group_id_, req.term_);
                    req.SetCcePtr(floor_cce);

                    if (!ConditionalReadLockCce(floor_cce,
                                                req,
                                                req.GetLockType(),
                                                req.TxTerm(),
                                                req.NodeGroupId(),
                                                true))
                    {
                        return false;
                    }
                }
            }

            CcEntry<KeyT, ValueT> *cce = floor_cce->map_next_;
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

                TemplateScanTuple<KeyT, ValueT> *scan_tuple = nullptr;
                scan_tuple = typed_cache->AddScanTuple();
                ScanKey(cce,
                        scan_tuple,
                        true,
                        req.node_group_id_,
                        req.term_,
                        req.is_ckpt_delta_);
                req.SetCcePtr(cce);

                if (!ConditionalReadLockCce(cce,
                                            req,
                                            req.GetLockType(),
                                            req.TxTerm(),
                                            req.NodeGroupId()))
                {
                    return false;
                }

                cce = cce->map_next_;
            }
        }
        else
        {
            CcEntry<KeyT, ValueT> *cce = nullptr;
            if (req.CcePtr() != nullptr)
            {
                cce = static_cast<CcEntry<KeyT, ValueT> *>(req.CcePtr());
                req.SetCcePtr(nullptr);
                // Lock has been acquired
            }
            else
            {
                cce = look_key == PositiveInfinity<KeyT>::Instance()
                          ? pos_inf_.map_prev_
                          : Floor(*look_key);
                assert(cce != nullptr);

                // The backward scan's starting point coincides with a cc
                // entry's key. If the starting point is inclusive, the scan
                // includes the entry's key. If the point is exclusive, the scan
                // starts from the prior entry, including its the key and the
                // gap.
                if (cce != &neg_inf_ && *look_key == *cce->key_)
                {
                    if (req.inclusive_)
                    {
                        TemplateScanTuple<KeyT, ValueT> *scan_tuple =
                            typed_cache->AddScanTuple();

                        ScanKey(cce,
                                scan_tuple,
                                false,
                                req.node_group_id_,
                                req.term_);
                        req.SetCcePtr(cce);

                        if (!ConditionalReadLockCce(cce,
                                                    req,
                                                    req.GetLockType(),
                                                    req.TxTerm(),
                                                    req.NodeGroupId()))
                        {
                            return false;
                        }
                    }
                }
            }

            cce = cce->map_prev_;
            while (cce != nullptr && !typed_cache->Full())
            {
                TemplateScanTuple<KeyT, ValueT> *scan_tuple =
                    typed_cache->AddScanTuple();

                if (cce == &neg_inf_)
                {
                    ScanGap(cce, scan_tuple, req.node_group_id_, req.term_);
                    req.SetCcePtr(cce);

                    if (!ConditionalReadLockCce(cce,
                                                req,
                                                req.GetLockType(),
                                                req.TxTerm(),
                                                req.NodeGroupId(),
                                                true))
                    {
                        return false;
                    }
                }
                else
                {
                    ScanKey(
                        cce, scan_tuple, true, req.node_group_id_, req.term_);
                    req.SetCcePtr(cce);

                    if (!ConditionalReadLockCce(cce,
                                                req,
                                                req.GetLockType(),
                                                req.TxTerm(),
                                                req.NodeGroupId()))
                    {
                        return false;
                    }
                }

                cce = cce->map_prev_;
            }
        }

        req.Result()->SetFinished();
        return true;
    }

    bool Execute(ScanNextBatchCc &req) override
    {
        int64_t term = Sharder::Instance().LeaderTerm(req.node_group_id_);
        if (term < 0)
        {
            req.Result()->SetError(-1);
            return false;
        }

        req.Result()->Value().term_ = term;
        TemplateScanCache<KeyT, ValueT> *typed_cache =
            static_cast<TemplateScanCache<KeyT, ValueT> *>(req.scan_cache_);
        assert(typed_cache->Full());

        ScanDirection direction = typed_cache->Scanner()->Direction();
        CcEntry<KeyT, ValueT> *prior_cce = nullptr;
        if (req.CcePtr() != nullptr)
        {
            prior_cce = static_cast<CcEntry<KeyT, ValueT> *>(req.CcePtr());
            req.SetCcePtr(nullptr);
            // Lock has been acquired
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

                // Return first available scan_tuple in scan_cache.
                TemplateScanTuple<KeyT, ValueT> *scan_tuple =
                    typed_cache->AddScanTuple();

                // Copy cce info to scan_tuple, which resides in scan_cache.
                ScanKey(cce,
                        scan_tuple,
                        true,
                        req.node_group_id_,
                        term,
                        req.is_ckpt_delta_);
                req.SetCcePtr(cce);

                if (!ConditionalReadLockCce(cce,
                                            req,
                                            req.GetLockType(),
                                            req.TxTerm(),
                                            req.NodeGroupId()))
                {
                    return false;
                }

                cce = cce->map_next_;
            }
        }
        else
        {
            CcEntry<KeyT, ValueT> *cce = prior_cce->map_prev_;
            while (cce != nullptr && !typed_cache->Full())
            {
                TemplateScanTuple<KeyT, ValueT> *scan_tuple =
                    typed_cache->AddScanTuple();

                if (cce == &neg_inf_)
                {
                    ScanGap(cce, scan_tuple, req.node_group_id_, term);
                    req.SetCcePtr(cce);

                    if (!ConditionalReadLockCce(cce,
                                                req,
                                                req.GetLockType(),
                                                req.TxTerm(),
                                                req.NodeGroupId(),
                                                true))
                    {
                        return false;
                    }
                }
                else
                {
                    ScanKey(cce, scan_tuple, true, req.node_group_id_, term);
                    req.SetCcePtr(cce);

                    if (!ConditionalReadLockCce(cce,
                                                req,
                                                req.GetLockType(),
                                                req.TxTerm(),
                                                req.NodeGroupId()))
                    {
                        return false;
                    }
                }

                cce = cce->map_prev_;
            }
        }

        req.Result()->SetFinished();
        return true;
    }

    bool Execute(remote::RemoteScanOpen &req) override
    {
        int64_t term = Sharder::Instance().LeaderTerm(req.node_group_id_);
        if (term < 0)
        {
            req.Result()->SetError(-1);
            return true;
        }

        const KeyT *look_key;
        KeyT key_obj;

        switch (req.key_type_)
        {
        case KeyType::NegativeInf:
            look_key = NegativeInfinity<KeyT>::Instance();
            break;
        case KeyType::PostiveInf:
            look_key = PositiveInfinity<KeyT>::Instance();
            break;
        default:
            size_t offset = 0;
            key_obj.Deserialize(
                req.start_key_str_->data(), offset, key_schema_);
            look_key = &key_obj;
            break;
        }

        std::vector<remote::ScanTuple_msg *> &cache =
            req.scan_caches_.at(shard_->LocalCoreId());

        if (req.direct_ == ScanDirection::Forward)
        {
            CcEntry<KeyT, ValueT> *floor_cce = nullptr;
            remote::ScanTuple_msg *tuple = nullptr;
            size_t tuple_idx = 0;

            if (req.CcePtr() != nullptr)
            {
                floor_cce = static_cast<CcEntry<KeyT, ValueT> *>(req.CcePtr());
                req.SetCcePtr(nullptr);
            }
            else
            {
                floor_cce = Floor(*look_key);
                assert(floor_cce != nullptr);

                remote::ScanTuple_msg *tuple = cache.at(0);

                if (floor_cce != &neg_inf_ && req.inclusive_ == true &&
                    *look_key == *floor_cce->key_)
                {
                    // The scan's starting point is inclusive and matches a cc
                    // entry's key. The scan results start from this cc entry,
                    // including the entry's key and the gap.
                    ScanKey(floor_cce, tuple, true, term);
                    ++tuple_idx;
                    req.SetCcePtr(floor_cce);

                    if (!ConditionalReadLockCce(floor_cce,
                                                req,
                                                req.GetLockType(),
                                                req.TxTerm(),
                                                req.NodeGroupId()))
                    {
                        return false;
                    }
                }
                else if (!req.is_ckpt_delta_)
                {
                    // The scan's starting point is exclusive or falls into the
                    // gap of a cc entry. The scan starts from the cc entry and
                    // only includes the entry's gap.
                    ScanGap(floor_cce, tuple, term);
                    ++tuple_idx;
                    req.SetCcePtr(floor_cce);

                    if (!ConditionalReadLockCce(floor_cce,
                                                req,
                                                req.GetLockType(),
                                                req.TxTerm(),
                                                req.NodeGroupId(),
                                                true))
                    {
                        return false;
                    }
                }
            }

            CcEntry<KeyT, ValueT> *cce = floor_cce->map_next_;
            while (cce != &pos_inf_ && tuple_idx < cache.size())
            {
                if (req.is_ckpt_delta_ &&
                    cce->commit_ts_ <=
                        cce->ckpt_ts_.load(std::memory_order_acquire))
                {
                    cce = cce->map_next_;
                    continue;
                }

                tuple = cache.at(tuple_idx);
                ScanKey(cce, tuple, true, term, req.is_ckpt_delta_);
                ++tuple_idx;
                req.SetCcePtr(cce);

                if (!ConditionalReadLockCce(cce,
                                            req,
                                            req.GetLockType(),
                                            req.TxTerm(),
                                            req.NodeGroupId()))
                {
                    return false;
                }

                cce = cce->map_next_;
            }

            cache.resize(tuple_idx);
        }
        else
        {
            CcEntry<KeyT, ValueT> *cce = nullptr;
            size_t idx = 0;

            if (req.CcePtr() != nullptr)
            {
                cce = static_cast<CcEntry<KeyT, ValueT> *>(req.CcePtr());
                req.SetCcePtr(nullptr);
                // Lock has been acquired
            }
            else
            {
                cce = Floor(*look_key);
                assert(cce != nullptr);

                // The backward scan's starting point coincides with a cc
                // entry's key. If the starting point is inclusive, the scan
                // includes the entry's key. If the point is exclusive, the scan
                // starts from the prior entry, including its both the key and
                // the gap.
                if (cce != &neg_inf_ && *look_key == *cce->key_)
                {
                    if (req.inclusive_)
                    {
                        remote::ScanTuple_msg *scan_tuple = cache.at(0);
                        ScanKey(cce, scan_tuple, false, term);
                        ++idx;
                        req.SetCcePtr(cce);

                        if (!ConditionalReadLockCce(cce,
                                                    req,
                                                    req.GetLockType(),
                                                    req.TxTerm(),
                                                    req.NodeGroupId()))
                        {
                            return false;
                        }
                    }
                }
            }

            cce = cce->map_prev_;
            while (cce != nullptr && idx < cache.size())
            {
                remote::ScanTuple_msg *scan_tuple = cache.at(idx);
                if (cce == &neg_inf_)
                {
                    ScanGap(cce, scan_tuple, term);
                    req.SetCcePtr(cce);

                    if (!ConditionalReadLockCce(cce,
                                                req,
                                                req.GetLockType(),
                                                req.TxTerm(),
                                                req.NodeGroupId(),
                                                true))
                    {
                        return false;
                    }
                }
                else
                {
                    ScanKey(cce, scan_tuple, true, term);
                    req.SetCcePtr(cce);

                    if (!ConditionalReadLockCce(cce,
                                                req,
                                                req.GetLockType(),
                                                req.TxTerm(),
                                                req.NodeGroupId()))
                    {
                        return false;
                    }
                }
                cce = cce->map_prev_;
                ++idx;
            }

            cache.resize(idx);
        }

        req.Result()->SetFinished();
        return true;
    }

    bool Execute(remote::RemoteScanNextBatch &req) override
    {
        int64_t term = Sharder::Instance().LeaderTerm(req.node_group_id_);
        if (term < 0)
        {
            req.Result()->SetError(-1);
            return true;
        }

        CcEntry<KeyT, ValueT> *prior_cce = nullptr;

        if (req.CcePtr() != nullptr)
        {
            prior_cce = static_cast<CcEntry<KeyT, ValueT> *>(req.CcePtr());
        }
        else
        {
            prior_cce =
                reinterpret_cast<CcEntry<KeyT, ValueT> *>(req.prior_cce_addr_);
        }

        ScanDirection direction = req.direct_;

        size_t idx = 0;
        if (direction == ScanDirection::Forward)
        {
            CcEntry<KeyT, ValueT> *cce = prior_cce->map_next_;
            while (cce != &pos_inf_ && idx < req.scan_cache_.size())
            {
                if (req.is_ckpt_delta_ &&
                    cce->commit_ts_ <=
                        cce->ckpt_ts_.load(std::memory_order_acquire))
                {
                    cce = cce->map_next_;
                    continue;
                }

                remote::ScanTuple_msg *scan_tuple = req.scan_cache_.at(idx);
                ScanKey(cce, scan_tuple, true, term, req.is_ckpt_delta_);
                ++idx;
                req.SetCcePtr(cce);

                if (!ConditionalReadLockCce(cce,
                                            req,
                                            req.GetLockType(),
                                            req.TxTerm(),
                                            req.NodeGroupId()))
                {
                    return false;
                }

                cce = cce->map_next_;
            }
        }
        else
        {
            CcEntry<KeyT, ValueT> *cce = prior_cce->map_prev_;
            while (cce != nullptr && idx < req.scan_cache_.size())
            {
                remote::ScanTuple_msg *scan_tuple = req.scan_cache_.at(idx);

                if (cce == &neg_inf_)
                {
                    ScanGap(cce, scan_tuple, term);
                    req.SetCcePtr(cce);

                    if (!ConditionalReadLockCce(cce,
                                                req,
                                                req.GetLockType(),
                                                req.TxTerm(),
                                                req.NodeGroupId(),
                                                true))
                    {
                        return false;
                    }
                }
                else
                {
                    ScanKey(cce, scan_tuple, true, term);
                    req.SetCcePtr(cce);

                    if (!ConditionalReadLockCce(cce,
                                                req,
                                                req.GetLockType(),
                                                req.TxTerm(),
                                                req.NodeGroupId()))
                    {
                        return false;
                    }
                }

                cce = cce->map_prev_;
                ++idx;
            }
        }
        req.scan_cache_.resize(idx);

        req.Result()->SetFinished();
        return true;
    }

    /// <summary>
    /// CommitSkCc only applies to the cc map of a secondary index.
    /// </summary>
    /// <param name="req"></param>
    bool Execute(CommitSkCc &req) override
    {
        return true;
    }

    bool Execute(CkptScanCc &req) override
    {
        LruEntry *lru_cce = req.start_entry_ == nullptr ? neg_inf_.ckpt_next_
                                                        : req.start_entry_;
        CcEntry<KeyT, ValueT> *cce =
            static_cast<CcEntry<KeyT, ValueT> *>(lru_cce);

        // CkptScanCc is running on TxProcessor thread. To avoid blocking
        // other transaction for a long time, we only process CkptScanBatch
        // number of entries in each round.
        size_t cnt = 0;
        while (cnt < CkptScanCc::CkptScanBatch && cce != &pos_inf_)
        {
            if (cce->commit_ts_ <= req.ckpt_ts_ &&
                cce->commit_ts_ > cce->ckpt_ts_.load(std::memory_order_acquire))
            {
                shard_->mem_usage_ -= cce->payload_ckpt_.first.MemUsage();
                cce->payload_ckpt_.first = cce->payload_;
                cce->payload_ckpt_.second =
                    cce->payload_status_ == RecordStatus::Deleted;

                cce->parent_map_->shard_->estimate_ccshard_log_size_ -=
                    cce->estimate_ccentry_log_size_;
                cce->estimate_ccentry_log_size_ = 0;

                req.ckpt_vec_.emplace_back(cce);
                shard_->mem_usage_ += cce->payload_ckpt_.first.MemUsage();
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
            return true;
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
        return true;
    }

    bool Execute(ReplayLogCc &req) override
    {
        KeyT key;
        // A psuedo record that is used to deserialize and move forward the
        // record that is not sharded to the core.
        ValueT rec;
        size_t offset = 0;
        const std::string_view &log_blob = req.LogContentView();

        // If the log record's commit ts is smaller than that of the cc map,
        // this record is generated before the latest schema of the table
        // and hence should skip the replay process.
        if (req.CommitTs() < commit_ts_)
        {
            req.SetFinish();
            return false;
        }

        while (offset < log_blob.size())
        {
            key.Deserialize(log_blob.data(), offset, key_schema_);
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
                // greater than that of the log record, skips installing the
                // log record in the cc map and moves to the next key in the
                // log record.
                if (delete_flag == 0)
                {
                    rec.Deserialize(log_blob.data(), offset);
                }
            }
            else
            {
                if (delete_flag == 0)
                {
                    cce->payload_.Deserialize(log_blob.data(), offset);
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
                    // the log record. TODO: it is safer if we ship the tx
                    // ID with the recovering message and match it against
                    // the lock holder.
                    TxNumber txn = cce->key_lock_.WriteLockTx();
                    cce->key_lock_.ReleaseWriteLock(txn, shard_);
                    shard_->DeleteLockHolidngTx(txn, cce);
                    // cce->key_lock_.ClearTx(txn);
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

    size_t size() const override
    {
        return ccm_.size();
    }

    std::unique_ptr<CcScanner> CreateScanner(
        ScanDirection direction) const override
    {
        return std::make_unique<TemplateCcScanner<KeyT, ValueT>>(
            direction, ScanIndexType::Primary, key_schema_);
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
        CcShard::DetachLru(remove_entry);

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

        shard_->mem_usage_ -= cc_entry->GetCcEntryMemUsage();

        ccm_.erase(*cc_entry->key_);
    }

    void GetCkptKeyRecord(const LruEntry *lru_entry,
                          const TxKey *&key,
                          const TxRecord *&rec,
                          bool &is_deleted) const override
    {
        const CcEntry<KeyT, ValueT> *cce =
            static_cast<const CcEntry<KeyT, ValueT> *>(lru_entry);

        key = cce->key_;
        rec = &cce->payload_ckpt_.first;
        is_deleted = cce->payload_ckpt_.second;
    }

    TableType Type() const override
    {
        return TableType::Primary;
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
        return key_schema_;
    }

    const Schema *RecordSchema() const override
    {
        return record_schema_;
    }

    std::unique_ptr<CcMap> Clone() const override
    {
        return std::make_unique<TemplateCcMap<KeyT, ValueT>>(
            shard_, commit_ts_, key_schema_, record_schema_);
    }

protected:
    CcEntry<KeyT, ValueT> *FindEmplace(const KeyT &key, uint64_t ts)
    {
        auto lb_it = ccm_.lower_bound(key);
        if (lb_it != ccm_.end() && lb_it->first == key)
        {
            shard_->UpdateLruList(&lb_it->second);
            return &lb_it->second;
        }

        if (shard_->Full())
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
        auto em_it = ccm_.emplace_hint(lb_it,
                                       std::piecewise_construct,
                                       std::forward_as_tuple(key),
                                       std::forward_as_tuple(this));
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

    CcEntry<KeyT, ValueT> *Emplace(const KeyT &key,
                                   uint64_t ts,
                                   bool force_to_emplace = false)
    {
        if (shard_->Full() && !force_to_emplace)
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
        auto em_it = ccm_.try_emplace(key, this);
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

        if (!force_to_emplace)
        {
            shard_->UpdateLruList(new_cce_ptr);
            shard_->mem_usage_ += new_cce_ptr->GetCcEntryMemUsage();
        }

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

    void ScanKey(CcEntry<KeyT, ValueT> *cce,
                 TemplateScanTuple<KeyT, ValueT> *tuple,
                 bool include_gap,
                 uint32_t ng_id,
                 int64_t term,
                 bool is_ckpt_delta = false) const
    {
        tuple->Key() = *cce->key_;
        if (cce->payload_status_ == RecordStatus::Normal ||
            (is_ckpt_delta && cce->payload_status_ != RecordStatus::Unknown))
        {
            tuple->Record() = cce->payload_;
        }
        tuple->rec_status_ = cce->payload_status_;
        tuple->key_ts_ = cce->commit_ts_;
        tuple->gap_ts_ = include_gap ? cce->gap_commit_ts_ : 0;
        tuple->cce_addr_.SetCce(reinterpret_cast<uint64_t>(cce), term, ng_id);
    }

    void ScanKey(CcEntry<KeyT, ValueT> *cce,
                 remote::ScanTuple_msg *tuple,
                 bool include_gap,
                 int64_t term,
                 bool is_ckpt_delta = false) const
    {
        tuple->clear_key();
        cce->key_->Serialize(*tuple->mutable_key());

        switch (cce->payload_status_)
        {
        case RecordStatus::Normal:
            tuple->clear_record();
            cce->payload_.Serialize(*tuple->mutable_record());
            tuple->set_rec_status(remote::ScanTuple_msg::RecordStatus::
                                      ScanTuple_msg_RecordStatus_NORMAL);
            break;
        case RecordStatus::Deleted:
            if (is_ckpt_delta)
            {
                tuple->clear_record();
                cce->payload_.Serialize(*tuple->mutable_record());
            }
            tuple->set_rec_status(remote::ScanTuple_msg::RecordStatus::
                                      ScanTuple_msg_RecordStatus_DELETED);
            break;
        case RecordStatus::Unknown:
            tuple->set_rec_status(remote::ScanTuple_msg::RecordStatus::
                                      ScanTuple_msg_RecordStatus_UNDEFINED);
            break;
        default:
            break;
        }

        tuple->set_key_ts(cce->commit_ts_);

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
        cce_addr->set_term(term);

        // For remote scans, the returned cc entries' node group ID is set
        // on the sender side when the sender receives the response.
    }

    void ScanGap(CcEntry<KeyT, ValueT> *cce,
                 TemplateScanTuple<KeyT, ValueT> *tuple,
                 uint32_t ng_id,
                 int64_t term) const
    {
        tuple->key_ts_ = 0;
        tuple->gap_ts_ = cce->gap_commit_ts_;
        tuple->cce_addr_.SetCce(reinterpret_cast<uint64_t>(cce), term, ng_id);
    }

    void ScanGap(CcEntry<KeyT, ValueT> *cce,
                 remote::ScanTuple_msg *tuple,
                 int64_t term) const
    {
        tuple->set_key_ts(0);
        tuple->set_gap_ts(cce->gap_commit_ts_);

        remote::CceAddr_msg *cce_addr = tuple->mutable_cce_addr();
        cce_addr->set_cce_ptr(reinterpret_cast<uint64_t>(cce));
        cce_addr->set_term(term);

        // For remote scans, the returned cc entries' node group ID is set
        // on the sender side when the sender receives the response.
    }

    std::map<KeyT, CcEntry<KeyT, ValueT>> ccm_;
    CcEntry<KeyT, ValueT> neg_inf_, pos_inf_;
    const Schema *key_schema_;
    const Schema *record_schema_;
};
}  // namespace txservice
