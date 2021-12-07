#pragma once

#include "cc_map.h"
#include "cc_request.h"
#include "cc_shard.h"
#include "fault/fault_inject.h"
#include "proto/cc_request.pb.h"
#include "remote/remote_cc_request.h"
#include "sharder.h"

namespace txservice
{
template <typename KeyT, typename ValueT>
class TemplateCcMap : public CcMap
{
public:
    TemplateCcMap() = delete;
    TemplateCcMap(const TemplateCcMap &rhs) = delete;
    TemplateCcMap(CcMap &&rhs) = delete;
    virtual ~TemplateCcMap() = default;

    TemplateCcMap(CcShard *shard,
                  const Schema *key_schema = nullptr,
                  const Schema *rec_schema = nullptr)
        : CcMap(shard),
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

    // function will not be executed
    bool Execute(AcquireTableWriteLockCC &req) override
    {
        return true;
    }
    // function will not be executed
    bool Execute(remote::RemoteAcquireTableWriteLockCC &req) override
    {
        return true;
    }

    // function will not be executed
    bool Execute(ReleaseTableWriteLockCC &req) override
    {
        return true;
    }

    // function will not be executed
    bool Execute(CommitCreateTableCC &req) override
    {
        return true;
    }

    // function will not be executed
    bool Execute(CommitDropTableCC &req) override
    {
        return true;
    }

    // function will not be executed
    bool Execute(FindCatalogCC &req) override
    {
        return true;
    }

    // function will not be executed
    bool Execute(CheckCatalogCC &req) override
    {
        return true;
    }

    bool Execute(AcquireCc &req) override
    {
        auto hd_res = req.Result();
        CcEntryAddr &cce_addr = hd_res->Value().cce_addr_;

        CcEntry<KeyT, ValueT> *cce_ptr = nullptr;
        const KeyT *target_key = nullptr;
        KeyT decoded_key;

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

        uint32_t ng_id = req.KeyShardCode() >> 10;
        int64_t ng_term = Sharder::Instance().LeaderTerm(ng_id);
        if (ng_term < 0)
        {
            hd_res->SetError(-1);
            return true;
        }

        if (req.IsInsert())
        {
            cce_ptr = Floor(*target_key);

            if (cce_ptr != &neg_inf_ && *cce_ptr->key_ == *target_key)
            {
                // The floor entry's key is equal to the insert key. If the key
                // is deleted, the insert becomes become an update. Or the
                // insert is aborted due to the duplidate key conflict.
                if (cce_ptr->payload_status_ == RecordStatus::Deleted)
                {
                    cce_addr.SetCce(
                        reinterpret_cast<uint64_t>(cce_ptr), ng_term, ng_id);
                }
                else
                {
                    // Inserts a duplicate key.
                    hd_res->SetError(1);
                    return true;
                }
            }
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
            cce_addr.SetCce(
                reinterpret_cast<uint64_t>(cce_ptr), ng_term, ng_id);
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
            cce_addr.SetInsert(
                reinterpret_cast<uint64_t>(insert_entry.get()), ng_term, ng_id);

            cc_entry.insert_intention_set_.emplace(&insert_entry->key_,
                                                   std::move(insert_entry));
            // Cc entry address has been updated. Only reset the result's last
            // validation ts.
            hd_res->Value().last_vali_ts_ = cc_entry.gap_last_vali_ts_;
            hd_res->SetFinished();
        }
        else
        {
            uint64_t tx_number = txid->TxNumber();
            int64_t tx_term = req.TxTerm();

            if (req.proto_ == CcProtocol::Locking &&
                (cc_entry.rlck_holders_.size() > 1 ||
                 (cc_entry.rlck_holders_.size() == 1 &&
                  cc_entry.rlck_holders_.find(tx_number) ==
                      cc_entry.rlck_holders_.end())))
            {
                // When there is at least one read locking not owned by the
                // calling tx, blocks the acquire request.
                cc_entry.blocking_queue_.Enqueue(&req);
                return false;
            }

            if (cc_entry.write_intention_.Empty())
            {
                cc_entry.write_intention_ = *txid;
                cc_entry.tx_term_ = tx_term;

                // If the tx previously read the key and now tries to update it,
                // replaces the read lock with the write intention.
                cc_entry.rlck_holders_.erase(tx_number);

                // Updates last_vali_ts when acquiring the write intention such
                // that it is no smaller than the current time of the shard.
                // The net effect is that the tx acquiring the write intention
                // is forced not to commit at a time earlier than the clock of
                // the participating node, even if the clock of the tx's
                // coordinator node drifts and falls behind. Checkpointing
                // relies on this property to avoid picking a checkpoint ts that
                // may overlap with an ongoing tx.
                uint64_t now_ts = shard_->Now();
                cc_entry.last_vali_ts_ =
                    std::max(cc_entry.last_vali_ts_, now_ts + 1);

                shard_->UpsertLockHoldingTx(
                    tx_number, cc_entry.last_vali_ts_, cce_ptr);

                hd_res->Value().last_vali_ts_ = cc_entry.last_vali_ts_;
                hd_res->SetFinished();
            }
            else
            {
                using namespace std::chrono_literals;
                uint64_t ts_gap =
                    std::chrono::duration_cast<std::chrono::seconds>(5s)
                        .count();

                TxNumber lk_holding_tx = cc_entry.write_intention_.TxNumber();
                TxLockInfo *lk_info =
                    shard_->GetActiveTxLockInfo(lk_holding_tx);
                uint64_t now_ts = shard_->Now();

                // If the write intention has been held by a conflicting tx
                // for a period of time (more than 5 seconds), tries to
                // recover the intention by inquiring the conflicting tx's
                // status. If the tx has failed or committed, recovers the
                // orphan intention. Or, keeps being blocked until the tx
                // makes further actions.
                if (lk_info == nullptr ||
                    (now_ts - lk_info->ts_ >= ts_gap &&
                     now_ts - lk_info->last_recover_ts_ >= ts_gap))
                {
                    // The intention's holding tx may not exist in the cc
                    // shard's active tx set. This is possible when the first
                    // attempt to recover the tx detects that the tx has
                    // committed and hence removes it from the active tx set,
                    // but the following log replay requests fail, leaving the
                    // intention unrecovered. Another possibility is that
                    // post-processing directs two requests to this shard, one
                    // succeeds and the other fails. The successful
                    // post-processing request removes the tx from the active tx
                    // set, so when the failed key's intention is recovered, the
                    // intention holding tx does not exist in the active set.
                    if (lk_info == nullptr)
                    {
                        lk_info = shard_->UpsertLockHoldingTx(
                            lk_holding_tx, now_ts, &cc_entry);
                    }

                    Sharder::Instance().RecoverTx(
                        lk_holding_tx, cc_entry.tx_term_, ng_id, ng_term);

                    // Updates the last_recover_ts field, so that following
                    // conflicting tx's will not try recovery immediately,
                    // avoiding a flood of recovery requests.
                    lk_info->last_recover_ts_ = now_ts;
                }

                if (req.proto_ == CcProtocol::OCC)
                {
                    // For OCC, a write-write conflict causes the tx to abort
                    // immediately.
                    hd_res->SetError(1);
                }
                else
                {
                    // For 2PL, a write-write conflict causes the acuquire
                    // request to block.
                    cc_entry.blocking_queue_.Enqueue(&req);
                }
            }
        }

        return true;
    }

    bool Resume(AcquireCc &req) override
    {
        return true;
    }

    bool Execute(PostDeleteCc &req) override
    {
        const CcEntryAddr &cce_addr = *req.CceAddr();

        if (cce_addr.InsertPtr() != 0)
        {
            InsertEntry<KeyT, ValueT> &insert_entry =
                *reinterpret_cast<InsertEntry<KeyT, ValueT> *>(
                    cce_addr.InsertPtr());

            CcEntry<KeyT, ValueT> &parent_entry = *insert_entry.parent_entry_;
            parent_entry.insert_intention_set_.erase(&insert_entry.key_);
            // The write lock/intention was released. Process blocking requests,
            // if there are any.
            parent_entry.UnblockRequests();
        }
        else
        {
            assert(cce_addr.CcePtr() != 0);

            CcEntry<KeyT, ValueT> &cc_entry =
                *reinterpret_cast<CcEntry<KeyT, ValueT> *>(cce_addr.CcePtr());

            // Clears the write intention if the intention is still held
            // by the calling txn.
            TxNumber txn = req.TxNumber();
            if (cc_entry.write_intention_ == txn)
            {
                cc_entry.write_intention_.Reset();

                shard_->DeleteLockHolidngTx(txn);

                // The write lock/intention was released. Process blocking
                // requests, if there are any.
                cc_entry.UnblockRequests();
            }
        }
        req.Result()->SetFinished();
        return true;
    }

    bool Execute(PostCommitCc &req) override
    {
        const CcEntryAddr &cce_addr = *req.CceAddr();
        if (!Sharder::Instance().CheckLeaderTerm(cce_addr.NodeGroupId(),
                                                 cce_addr.Term()))
        {
            req.Result()->SetError(-1);
            return true;
        }

        const ValueT *commit_val = static_cast<const ValueT *>(req.Payload());
        TxNumber txn = req.TxNumber();
        uint64_t commit_ts = req.CommitTs();
        const std::string *payload_str = req.PayloadStr();
        bool is_del = req.IsDeleted();

        if (cce_addr.InsertPtr() != 0)
        {
            assert(is_del == false);

            InsertEntry<KeyT, ValueT> &insert_entry =
                *reinterpret_cast<InsertEntry<KeyT, ValueT> *>(
                    cce_addr.InsertPtr());

            CcEntry<KeyT, ValueT> *new_cce =
                Emplace(insert_entry.key_, commit_ts);

            if (new_cce == nullptr)
            {
                // The cc map has reached the maximal capacity.
                shard_->Enqueue(shard_->LocalCoreId(), &req);

                // finally release table read lock
                shard_->ReleaseTableReadIntention(*req.GetTableName(), &req);

                return false;
            }

            CcEntry<KeyT, ValueT> &prior_cce = *insert_entry.parent_entry_;

            auto ite = prior_cce.insert_intention_set_.find(&insert_entry.key_);
            assert(ite != prior_cce.insert_intention_set_.end());
            assert(ite->second->tx_id_ == txn);

            if (payload_str == nullptr)
            {
                new_cce->payload_ = *commit_val;
            }
            else
            {
                size_t offset = 0;
                new_cce->payload_.Deserialize(payload_str->data(), offset);
            }
            new_cce->payload_status_ = RecordStatus::Normal;

            ++ite;
            for (auto it = ite; it != prior_cce.insert_intention_set_.end();
                 ++it)
            {
                InsertEntry<KeyT, ValueT> &insert_entry = *it->second.get();
                insert_entry.parent_entry_ = new_cce;
                new_cce->insert_intention_set_.emplace(it->first,
                                                       std::move(it->second));
            }

            new_cce->gap_commit_ts_ = commit_ts;
            new_cce->commit_ts_ = commit_ts;
            new_cce->gap_last_vali_ts_ = prior_cce.gap_last_vali_ts_;

            prior_cce.gap_commit_ts_ = commit_ts;
            prior_cce.insert_intention_set_.erase(
                --ite, prior_cce.insert_intention_set_.end());
            prior_cce.UnblockRequests();

            if (new_cce->ckpt_next_ == nullptr)
            {
                // If the new cc entry is not in the checkpoint list, enlists
                // the new entry.
                LruEntry *second_last = pos_inf_.ckpt_prev_;
                second_last->ckpt_next_ = new_cce;
                new_cce->ckpt_prev_ = second_last;
                new_cce->ckpt_next_ = &pos_inf_;
                pos_inf_.ckpt_prev_ = new_cce;
            }
        }
        else
        {
            assert(cce_addr.CcePtr() != 0);

            CcEntry<KeyT, ValueT> &cce =
                *reinterpret_cast<CcEntry<KeyT, ValueT> *>(cce_addr.CcePtr());

            if (cce.write_intention_ == txn)
            {
                cce.commit_ts_ = commit_ts;

                if (payload_str == nullptr && !is_del)
                {
                    cce.payload_ = *commit_val;
                }
                else if (!is_del)
                {
                    size_t offset = 0;
                    cce.payload_.Deserialize(payload_str->data(), offset);
                }
                cce.payload_status_ =
                    is_del ? RecordStatus::Deleted : RecordStatus::Normal;
                cce.write_intention_.Reset();

                shard_->DeleteLockHolidngTx(txn);

                cce.UnblockRequests();

                if (cce.ckpt_next_ == nullptr)
                {
                    // If the cc entry is not in the checkpoint list, enlists
                    // the entry.
                    LruEntry *second_last = pos_inf_.ckpt_prev_;
                    second_last->ckpt_next_ = &cce;
                    cce.ckpt_prev_ = second_last;
                    cce.ckpt_next_ = &pos_inf_;
                    pos_inf_.ckpt_prev_ = &cce;
                }
            }
        }

        req.Result()->SetFinished();
        return true;
    }

    bool Execute(ValidateCc &req) override
    {
        auto hd_res = req.Result();

        const CcEntryAddr &cce_addr = *req.CceAddr();
        if (!Sharder::Instance().CheckLeaderTerm(cce_addr.NodeGroupId(),
                                                 cce_addr.Term()))
        {
            hd_res->SetError(-1);
            return true;
        }

        CcEntry<KeyT, ValueT> &cc_entry =
            *reinterpret_cast<CcEntry<KeyT, ValueT> *>(cce_addr.CcePtr());

        cc_entry.rlck_holders_.erase(req.TxNumber());

        uint64_t key_ts = req.KeyTs();
        uint64_t gap_ts = req.GapTs();
        uint64_t commit_ts = req.CommitTs();

        if ((key_ts > 0 && key_ts != cc_entry.commit_ts_) ||
            (gap_ts > 0 && gap_ts != cc_entry.gap_commit_ts_))
        {
            hd_res->SetError(1);
        }
        else
        {
            std::vector<TxId> &conflicting_txs = hd_res->Value();
            conflicting_txs.clear();

            if (gap_ts > 0)
            {
                cc_entry.gap_last_vali_ts_ =
                    std::max(cc_entry.gap_last_vali_ts_, commit_ts);

                conflicting_txs.reserve(cc_entry.insert_intention_set_.size() +
                                        1);

                for (auto it = cc_entry.insert_intention_set_.begin();
                     it != cc_entry.insert_intention_set_.end();
                     ++it)
                {
                    conflicting_txs.emplace_back(it->second->tx_id_);
                }
            }

            if (key_ts > 0)
            {
                cc_entry.last_vali_ts_ =
                    std::max(cc_entry.last_vali_ts_, commit_ts);

                if (!cc_entry.write_intention_.Empty())
                {
                    conflicting_txs.emplace_back(cc_entry.write_intention_);
                }
            }

            hd_res->SetFinished();
        }

        return true;
    }

    bool Execute(PostReadCc &req) override
    {
        const CcEntryAddr &cce_addr = *req.CceAddr();
        if (!Sharder::Instance().CheckLeaderTerm(cce_addr.NodeGroupId(),
                                                 cce_addr.Term()))
        {
            req.Result()->SetError(-1);
            return true;
        }

        CcEntry<KeyT, ValueT> &cc_entry =
            *reinterpret_cast<CcEntry<KeyT, ValueT> *>(cce_addr.CcePtr());

        cc_entry.rlck_holders_.erase(req.TxNumber());

        if (req.proto_ == CcProtocol::Locking)
        {
            // Only reads under 2PL may block other tx's.
            cc_entry.UnblockRequests();
        }

        req.Result()->SetFinished();

        return true;
    }

    bool Execute(ReadCc &req) override
    {
        auto hd_res = req.Result();

        SIMPLE_FAULT_INJECTOR("monograph_read_panic_ccnode0");

        CcEntryAddr &cce_addr = hd_res->Value().cce_addr_;
        CcEntry<KeyT, ValueT> *cce = nullptr;

        uint32_t ng_id = req.KeyShardCode() >> 10;
        int64_t ng_term = Sharder::Instance().LeaderTerm(ng_id);
        if (ng_term < 0)
        {
            hd_res->SetError(-1);
            return true;
        }

        if (cce_addr.CcePtr() == 0)
        {
            if (req.key_ != nullptr)
            {
                const KeyT *look_key = static_cast<const KeyT *>(req.key_);
                cce = FindEmplace(*look_key, req.ts_);

                if (cce == nullptr)
                {
                    shard_->Enqueue(shard_->LocalCoreId(), &req);
                    return false;
                }
            }
            else
            {
                assert(req.key_str_ != nullptr);

                KeyT decoded_key;
                size_t offset = 0;
                decoded_key.Deserialize(
                    req.key_str_->data(), offset, key_schema_);

                cce = FindEmplace(decoded_key, req.ts_);

                if (cce == nullptr)
                {
                    shard_->Enqueue(shard_->LocalCoreId(), &req);
                    return false;
                }
            }
            cce_addr.SetCce(reinterpret_cast<uint64_t>(cce), ng_term, ng_id);

            if (req.proto_ == CcProtocol::Locking)
            {
                bool own_lock = cce->rlck_holders_.find(req.tx_number_) !=
                                cce->rlck_holders_.end();

                if (!cce->write_intention_.Empty() ||
                    (cce->blocking_queue_.Size() > 0 && !own_lock))
                {
                    // When the blocking queue is non-empty, it's either there
                    // is a write being blocked by concurrent reads or a read
                    // being blocked by the write.  For the former case, the
                    // current read request can  proceed, because it does not
                    // conflict with current read lock owners. However, this may
                    // result in starvation for the write, as reads continuously
                    // jump before the write. So, the current read request adds
                    // itself to the blocking queue, as long as the queue is
                    // non-empty.
                    cce->blocking_queue_.Enqueue(&req);
                    return false;
                }

                if (!own_lock)
                {
                    cce->rlck_holders_.emplace(req.tx_number_);
                }
            }
            else
            {
                // cce->rlck_holders_.emplace(req.tx_number_);
            }
        }
        else
        {
            assert(req.type_ != ReadType::Inside);
            assert(ng_id == cce_addr.NodeGroupId());

            if (ng_term != cce_addr.Term())
            {
                hd_res->SetError(-1);
                return true;
            }

            cce = reinterpret_cast<CcEntry<KeyT, ValueT> *>(cce_addr.CcePtr());
        }

        if (cce->payload_status_ == RecordStatus::Unknown)
        {
            // The request brings in the record to the cc entry for caching

            if (req.type_ == ReadType::OutsideNormal)
            {
                if (req.rec_ != nullptr)
                {
                    ValueT *typed_rec = static_cast<ValueT *>(req.rec_);
                    cce->payload_ = *typed_rec;
                }
                else
                {
                    assert(req.rec_str_ != nullptr);

                    size_t offset = 0;
                    cce->payload_.Deserialize(req.rec_str_->data(), offset);
                }
                cce->payload_status_ = RecordStatus::Normal;
            }
            else if (req.type_ == ReadType::OutsideDeleted)
            {
                cce->payload_status_ = RecordStatus::Deleted;
            }
        }

        if (cce->payload_status_ == RecordStatus::Normal &&
            (req.type_ == ReadType::Inside || cce->commit_ts_ > 1))
        {
            // Copies the newest committed payload to the read result, if (1)
            // this is a read request that starts concurrency control for
            // the input key (i.e., read inside), or (2) this is a read request
            // that brings in the record from the data store for caching, but
            // the key has been updated by another committed tx since the first
            // read request.
            if (req.rec_ != nullptr)
            {
                ValueT *typed_rec = static_cast<ValueT *>(req.rec_);
                *typed_rec = cce->payload_;
            }
            else
            {
                assert(req.rec_str_ != nullptr);
                cce->payload_.Serialize(*req.rec_str_);
            }
        }

        hd_res->Value().ts_ = cce->commit_ts_;
        hd_res->Value().rec_status_ = cce->payload_status_;

        hd_res->SetFinished();
        return true;
    }

    bool Resume(ReadCc &req) override
    {
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
            CcEntry<KeyT, ValueT> *floor_cce =
                look_key == NegativeInfinity<KeyT>::Instance()
                    ? &neg_inf_
                    : Floor(*look_key);

            assert(floor_cce != nullptr);

            TemplateScanTuple<KeyT, ValueT> *scan_tuple = nullptr;

            if (floor_cce != &neg_inf_ && req.inclusive_ == true &&
                *look_key == *floor_cce->key_)
            {
                scan_tuple = typed_cache->AddScanTuple();
                // The forward scan's starting point is inclusive and matches a
                // cc entry's key. The scan starts from this cc entry,
                // including the entry's key and the gap.
                ScanKey(
                    floor_cce, scan_tuple, true, req.node_group_id_, req.term_);
            }
            else if (!req.is_ckpt_delta_)
            {
                scan_tuple = typed_cache->AddScanTuple();
                // The forward scan's starting point is exclusive or falls into
                // the gap of a cc entry. The scan starts from the cc entry and
                // only includes the entry's gap.
                ScanGap(floor_cce, scan_tuple, req.node_group_id_, req.term_);
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

                scan_tuple = typed_cache->AddScanTuple();
                ScanKey(cce,
                        scan_tuple,
                        true,
                        req.node_group_id_,
                        req.term_,
                        req.is_ckpt_delta_);
                cce = cce->map_next_;
            }
        }
        else
        {
            CcEntry<KeyT, ValueT> *cce =
                look_key == PositiveInfinity<KeyT>::Instance()
                    ? pos_inf_.map_prev_
                    : Floor(*look_key);

            assert(cce != nullptr);

            // The backward scan's starting point coincides with a cc entry's
            // key. If the starting point is inclusive, the scan includes the
            // entry's key. If the point is exclusive, the scan starts from the
            // prior entry, including its the key and the gap.
            if (cce != &neg_inf_ && *look_key == *cce->key_)
            {
                if (req.inclusive_)
                {
                    TemplateScanTuple<KeyT, ValueT> *scan_tuple =
                        typed_cache->AddScanTuple();

                    ScanKey(
                        cce, scan_tuple, false, req.node_group_id_, req.term_);
                }
                cce = cce->map_prev_;
            }

            while (cce != nullptr && !typed_cache->Full())
            {
                TemplateScanTuple<KeyT, ValueT> *scan_tuple =
                    typed_cache->AddScanTuple();

                if (cce == &neg_inf_)
                {
                    ScanGap(cce, scan_tuple, req.node_group_id_, req.term_);
                }
                else
                {
                    ScanKey(
                        cce, scan_tuple, true, req.node_group_id_, req.term_);
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

        CcEntry<KeyT, ValueT> *prior_cce =
            reinterpret_cast<CcEntry<KeyT, ValueT> *>(
                typed_cache->Last()->cce_addr_.CcePtr());

        ScanDirection direction = typed_cache->Scanner()->Direction();
        typed_cache->Reset();

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

                TemplateScanTuple<KeyT, ValueT> *scan_tuple =
                    typed_cache->AddScanTuple();
                ScanKey(cce,
                        scan_tuple,
                        true,
                        req.node_group_id_,
                        term,
                        req.is_ckpt_delta_);
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
                }
                else
                {
                    ScanKey(cce, scan_tuple, true, req.node_group_id_, term);
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
            CcEntry<KeyT, ValueT> *floor_cce = Floor(*look_key);
            assert(floor_cce != nullptr);

            remote::ScanTuple_msg *tuple = cache.at(0);

            size_t tuple_idx = 0;

            if (floor_cce != &neg_inf_ && req.inclusive_ == true &&
                *look_key == *floor_cce->key_)
            {
                // The scan's starting point is inclusive and matches a cc
                // entry's key. The scan results start from this cc entry,
                // including the entry's key and the gap.
                ScanKey(floor_cce, tuple, true, term);
                ++tuple_idx;
            }
            else if (!req.is_ckpt_delta_)
            {
                // The scan's starting point is exclusive or falls into the gap
                // of a cc entry. The scan starts from the cc entry and only
                // includes the entry's gap.
                ScanGap(floor_cce, tuple, term);
                ++tuple_idx;
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
                cce = cce->map_next_;
                ++tuple_idx;
            }

            cache.resize(tuple_idx);
        }
        else
        {
            CcEntry<KeyT, ValueT> *cce = Floor(*look_key);
            assert(cce != nullptr);

            size_t idx = 0;
            // The backward scan's starting point coincides with a cc entry's
            // key. If the starting point is inclusive, the scan includes the
            // entry's key. If the point is exclusive, the scan starts from the
            // prior entry, including its both the key and the gap.
            if (cce != &neg_inf_ && *look_key == *cce->key_)
            {
                if (req.inclusive_)
                {
                    remote::ScanTuple_msg *scan_tuple = cache.at(0);
                    ScanKey(cce, scan_tuple, false, term);
                    ++idx;
                }
                cce = cce->map_prev_;
            }

            while (cce != nullptr && idx < cache.size())
            {
                remote::ScanTuple_msg *scan_tuple = cache.at(idx);
                if (cce == &neg_inf_)
                {
                    ScanGap(cce, scan_tuple, term);
                }
                else
                {
                    ScanKey(cce, scan_tuple, true, term);
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

        CcEntry<KeyT, ValueT> *prior_cce =
            reinterpret_cast<CcEntry<KeyT, ValueT> *>(req.prior_cce_addr_);

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
                cce = cce->map_next_;
                ++idx;
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
                }
                else
                {
                    ScanKey(cce, scan_tuple, true, term);
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
        // TODO: checkpoint also need table lock

        LruEntry *lru_cce = req.start_entry_ == nullptr ? neg_inf_.ckpt_next_
                                                        : req.start_entry_;
        CcEntry<KeyT, ValueT> *cce =
            static_cast<CcEntry<KeyT, ValueT> *>(lru_cce);

        size_t cnt = 0;
        while (cnt < CkptScanCc::CkptScanBatch && cce != &pos_inf_)
        {
            // The checkpoint ts should be smaller than the ts when an ongoing
            // tx acquired the write intention. Or, there is a possibility that
            // the tx commits prior to the checkpoint.
            assert(cce->write_intention_.Empty() ||
                   req.ckpt_ts_ <= cce->last_vali_ts_);

            if (cce->commit_ts_ <= req.ckpt_ts_ &&
                cce->commit_ts_ > cce->ckpt_ts_.load(std::memory_order_acquire))
            {
                cce->payload_ckpt_.first = cce->payload_;
                cce->payload_ckpt_.second =
                    cce->payload_status_ == RecordStatus::Deleted;

                req.ckpt_vec_.emplace_back(cce);
            }
            else if (cce->commit_ts_ <=
                     cce->ckpt_ts_.load(std::memory_order_acquire))
            {
                // If the key has been checkpointed, removes it from the
                // checkpoint list.
                ++cnt;
                LruEntry *next = cce->ckpt_next_;
                CcShard::DetachCkpt(cce);
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
            req.start_entry_ = cce;
            shard_->Enqueue(&req);
            return false;
        }
    }

    bool Execute(ReplayLogCc &req) override
    {
        KeyT key;
        // A psuedo record that is used to deserialize and move forward the
        // record that is not sharded to the core.
        ValueT rec;
        size_t offset = 0;
        const std::string_view &log_blob = req.log_blob_view_;

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
                // Skips the the key in the log record that is not sharded to
                // this core.
                if (delete_flag == 0)
                {
                    rec.Deserialize(log_blob.data(), offset);
                }
                continue;
            }

            CcEntry<KeyT, ValueT> *cce = FindEmplace(key, req.commit_ts_);
            assert(cce != nullptr);

            if (cce->commit_ts_ >= req.commit_ts_)
            {
                // If the key exists in the cc map and its commit ts is greater
                // than that of the log record, skips installing the log record
                // in the cc map and moves to the next key in the log record.
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
                cce->commit_ts_ = req.commit_ts_;
                if (!cce->write_intention_.Empty())
                {
                    // If the record in the log has a commit ts greater than
                    // that of the cc entry and the cc entry has a write
                    // intention, the intention's owner must be the tx that
                    // commits the log record. TODO: it is safer if we ship the
                    // tx ID with the recovering message and match it against
                    // the lock holder.
                    shard_->DeleteLockHolidngTx(
                        cce->write_intention_.TxNumber());
                    cce->write_intention_.Reset();
                }
            }
        }

        req.SetFinish();
        return true;
    }

    bool Execute(FaultInjectCC &req) override
    {
        return true;
    }

    size_t size() const override
    {
        return ccm_.size();
    }

    std::unique_ptr<CcScanner> CreateScanner(
        ScanDirection direction) const override
    {
        return std::make_unique<TemplateCcScanner<KeyT, ValueT>>(direction,
                                                                 key_schema_);
    }

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

        CcEntry<KeyT, ValueT> *prior = cc_entry->map_prev_;
        CcEntry<KeyT, ValueT> *next = cc_entry->map_next_;

        prior->map_next_ = next;
        next->map_prev_ = prior;

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
            shard_, key_schema_, record_schema_);
    }

private:
    CcEntry<KeyT, ValueT> *FindEmplace(const KeyT &key, uint64_t ts)
    {
        auto lb_it = ccm_.lower_bound(key);
        if (lb_it != ccm_.end() && lb_it->first == key)
        {
            return &lb_it->second;
        }

        if (shard_->Full())
        {
            // The shard has reached the maximal capacity. Tries to clean cc
            // entries that have been checkpointed but are not being accessed by
            // active tx's.
            size_t free_cnt = shard_->Clean();
            if (free_cnt == 0)
            {
                return nullptr;
            }
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
        return new_cce_ptr;
    }

    CcEntry<KeyT, ValueT> *Emplace(const KeyT &key, uint64_t ts)
    {
        if (shard_->Full())
        {
            // The shard has reached the maximal capacity. Try cleaning cc
            // entries that has been checkpointed and is not accessed by active
            // tx's.
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
            // If a new cc entry is inserted, updates the ordered double-linked
            // list of cc entries.

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
        return new_cce_ptr;
    }

    CcEntry<KeyT, ValueT> *Floor(const KeyT &key)
    {
        if (ccm_.size() == 0)
        {
            return &neg_inf_;
        }

        auto it = ccm_.lower_bound(key);

        if (it == ccm_.end())
        {
            return &ccm_.rbegin()->second;
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
                                      ScanTuple_msg_RecordStatus_UNKNOWN);
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

        // For remote scans, the returned cc entries' node group ID is set on
        // the sender side when the sender receives the response.
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

        // For remote scans, the returned cc entries' node group ID is set on
        // the sender side when the sender receives the response.
    }

    std::map<KeyT, CcEntry<KeyT, ValueT>> ccm_;
    CcEntry<KeyT, ValueT> neg_inf_, pos_inf_;
    const Schema *const key_schema_;
    const Schema *const record_schema_;
};
}  // namespace txservice
