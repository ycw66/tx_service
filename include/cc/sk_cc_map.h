#pragma once

#include <map>
#include <set>

#include "secondary_key.h"
#include "template_cc_map.h"

namespace txservice
{
template <typename SkT, typename PkT>
class SkCcMap : public CcMap
{
public:
    using KeyPair = std::pair<SkT, PkT>;
    using KeyPtrPair = std::pair<const SkT *, const PkT *>;

    SkCcMap() = delete;
    SkCcMap(const SkCcMap &rhs) = delete;
    SkCcMap(SkCcMap &&rhs) = delete;
    virtual ~SkCcMap() = default;

    SkCcMap(CcShard *shard,
            const Schema *sk_schema = nullptr,
            const Schema *pk_schema = nullptr)
        : CcMap(shard),
          neg_inf_(this),
          pos_inf_(this),
          compound_schema_(sk_schema, pk_schema)
    {
        neg_inf_.key_ = nullptr;
        neg_inf_.payload_.first = NegativeInfinity<SkT>::Instance();
        neg_inf_.payload_.second = NegativeInfinity<PkT>::Instance();
        pos_inf_.key_ = nullptr;
        pos_inf_.payload_.first = PositiveInfinity<SkT>::Instance();
        pos_inf_.payload_.second = PositiveInfinity<PkT>::Instance();

        neg_inf_.map_next_ = &pos_inf_;
        pos_inf_.map_prev_ = &neg_inf_;

        neg_inf_.ckpt_prev_ = nullptr;
        neg_inf_.ckpt_next_ = &pos_inf_;
        pos_inf_.ckpt_prev_ = &neg_inf_;
        pos_inf_.ckpt_next_ = nullptr;
    }

    bool Execute(AcquireCc &req) override
    {
        return true;
    }

    bool Execute(PostWriteCc &req) override
    {
        return true;
    }

    bool Execute(PostReadCc &req) override
    {
        return true;
    }

    bool Execute(ReadCc &req) override
    {
        auto hd_res = req.Result();

        uint32_t ng_id = req.KeyShardCode() >> 10;
        int64_t term = Sharder::Instance().LeaderTerm(ng_id);
        if (term < 0)
        {
            hd_res->SetError(-1);
            return true;
        }

        // The payload of the cc map of a secondary index is void. A tx never
        // issues a key-oriented read toward the cc map of a secondary index,
        // except for using the read request to bring an index entry (sk, pk)
        // into the cc map for concurrency control, i.e., read outside.
        assert(req.Type() == ReadType::OutsideNormal);

        CcEntryAddr &cce_addr = hd_res->Value().cce_addr_;
        CcEntry<KeyPair, KeyPtrPair> *cce_ptr = nullptr;

        if (req.Key() != nullptr)
        {
            const SecondaryKey<SkT, PkT> *look_key =
                static_cast<const SecondaryKey<SkT, PkT> *>(req.Key());

            cce_ptr = FindEmplace(
                look_key->SKey(), look_key->PKey(), req.ReadTimestamp());

            if (cce_ptr == nullptr)
            {
                shard_->Enqueue(shard_->LocalCoreId(), &req);
                return false;
            }
        }
        else
        {
            assert(req.KeyBlob() != nullptr);

            SecondaryKey<SkT, PkT> decoded_key;
            size_t offset = 0;
            decoded_key.Deserialize(
                req.KeyBlob()->data(), offset, &compound_schema_);

            cce_ptr = FindEmplace(
                decoded_key.SKey(), decoded_key.PKey(), req.ReadTimestamp());

            if (cce_ptr == nullptr)
            {
                shard_->Enqueue(shard_->LocalCoreId(), &req);
                return false;
            }
        }

        cce_ptr->payload_status_ = RecordStatus::Normal;
        cce_addr.SetCce(reinterpret_cast<uint64_t>(cce_ptr), term);

        hd_res->Value().ts_ = cce_ptr->commit_ts_;
        hd_res->Value().rec_status_ = cce_ptr->payload_status_;

        hd_res->SetFinished();
        return true;
    }

    bool Execute(remote::RemoteReadOutside &req) override
    {
        return true;
    }

    bool Execute(ScanCloseCc &req) override
    {
        return true;
    }

    bool Execute(ScanOpenBatchCc &req) override
    {
        int64_t term = Sharder::Instance().LeaderTerm(req.node_group_id_);
        if (term < 0)
        {
            req.Result()->SetError(-1);
            return true;
        }

        const SkT *look_sk = static_cast<const SkT *>(req.start_key_);
        TemplateScanCache<SecondaryKey<SkT, PkT>, VoidRecord> *typed_cache =
            static_cast<
                TemplateScanCache<SecondaryKey<SkT, PkT>, VoidRecord> *>(
                req.scan_cache_);

        if (req.direct_ == ScanDirection::Forward)
        {
            CcEntry<KeyPair, KeyPtrPair> *floor_cce =
                look_sk == NegativeInfinity<SkT>::Instance()
                    ? &neg_inf_
                    : Floor(*look_sk, req.direct_, req.inclusive_);
            assert(floor_cce != nullptr);

            TemplateScanTuple<SecondaryKey<SkT, PkT>, VoidRecord> *scan_tuple =
                typed_cache->AddScanTuple();

            if (floor_cce != &neg_inf_ && req.inclusive_ == true &&
                *look_sk == *floor_cce->payload_.first)
            {
                // The forward scan's starting point is inclusive and matches a
                // cc entry's key. The scan starts from this cc entry,
                // including the entry's key and the gap.
                ScanKey(floor_cce, scan_tuple, true, req.node_group_id_, term);
            }
            else
            {
                // The forward scan's starting point is exclusive or falls into
                // the gap of a cc entry. The scan starts from the cc entry and
                // only includes the entry's gap.
                ScanGap(floor_cce, scan_tuple, req.node_group_id_, term);
            }

            CcEntry<KeyPair, KeyPtrPair> *cce = floor_cce->map_next_;
            while (cce != &pos_inf_ && !typed_cache->Full())
            {
                scan_tuple = typed_cache->AddScanTuple();
                ScanKey(cce, scan_tuple, true, req.node_group_id_, term);
                cce = cce->map_next_;
            }
        }
        else
        {
            CcEntry<KeyPair, KeyPtrPair> *cce =
                look_sk == PositiveInfinity<SkT>::Instance()
                    ? pos_inf_.map_prev_
                    : Floor(*look_sk, req.direct_, req.inclusive_);

            assert(cce != nullptr);

            // The backward scan's starting point coincides with a cc entry's
            // key. If the starting point is inclusive, the scan includes the
            // entry's key. If the point is exclusive, the scan starts from the
            // prior entry, including both the key and the gap.
            if (cce != &neg_inf_ && *look_sk == *cce->payload_.first)
            {
                if (req.inclusive_)
                {
                    TemplateScanTuple<SecondaryKey<SkT, PkT>, VoidRecord>
                        *scan_tuple = typed_cache->AddScanTuple();

                    ScanKey(cce, scan_tuple, false, req.node_group_id_, term);
                }
                cce = cce->map_prev_;
            }

            while (cce != nullptr && !typed_cache->Full())
            {
                TemplateScanTuple<SecondaryKey<SkT, PkT>, VoidRecord>
                    *scan_tuple = typed_cache->AddScanTuple();

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

    bool Execute(ScanNextBatchCc &req) override
    {
        int64_t term = Sharder::Instance().LeaderTerm(req.node_group_id_);
        if (term < 0)
        {
            req.Result()->SetError(-1);
            return true;
        }

        TemplateScanCache<SecondaryKey<SkT, PkT>, VoidRecord> *typed_cache =
            static_cast<
                TemplateScanCache<SecondaryKey<SkT, PkT>, VoidRecord> *>(
                req.scan_cache_);

        assert(typed_cache->Full());

        CcEntry<KeyPair, KeyPtrPair> *prior_cce =
            reinterpret_cast<CcEntry<KeyPair, KeyPtrPair> *>(
                typed_cache->Last()->cce_addr_.CcePtr());

        ScanDirection direction = typed_cache->Scanner()->Direction();
        typed_cache->Reset();

        if (direction == ScanDirection::Forward)
        {
            CcEntry<KeyPair, KeyPtrPair> *cce = prior_cce->map_next_;
            while (cce != &pos_inf_ && !typed_cache->Full())
            {
                TemplateScanTuple<SecondaryKey<SkT, PkT>, VoidRecord>
                    *scan_tuple = typed_cache->AddScanTuple();
                ScanKey(cce, scan_tuple, true, req.node_group_id_, term);
                cce = cce->map_next_;
            }
        }
        else
        {
            CcEntry<KeyPair, KeyPtrPair> *cce = prior_cce->map_prev_;
            while (cce != nullptr && !typed_cache->Full())
            {
                TemplateScanTuple<SecondaryKey<SkT, PkT>, VoidRecord>
                    *scan_tuple = typed_cache->AddScanTuple();

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

        SkT look_sk;

        std::vector<remote::ScanTuple_msg *> &cache =
            req.scan_caches_.at(shard_->LocalCoreId());

        if (req.direct_ == ScanDirection::Forward)
        {
            CcEntry<KeyPair, KeyPtrPair> *floor_cce = nullptr;

            if (req.key_type_ == KeyType::NegativeInf)
            {
                floor_cce = &neg_inf_;
            }
            else
            {
                size_t offset = 0;
                look_sk.Deserialize(req.start_key_str_->data(),
                                    offset,
                                    compound_schema_.sk_schema_.get());
                floor_cce = Floor(look_sk, req.direct_, req.inclusive_);
            }

            assert(floor_cce != nullptr);

            remote::ScanTuple_msg *tuple = cache.at(0);

            if (floor_cce != &neg_inf_ && req.inclusive_ == true &&
                look_sk == *floor_cce->payload_.first)
            {
                // The scan's starting point is inclusive and matches a cc
                // entry's key. The scan start from this cc entry,
                // including the entry's key and the gap.
                ScanKey(floor_cce, tuple, true, term);
            }
            else
            {
                // The scan's starting point is exclusive or falls into the gap
                // of a cc entry. The scan starts from the cc entry and only
                // includes the entry's gap.
                ScanGap(floor_cce, tuple, term);
            }

            size_t idx = 1;
            CcEntry<KeyPair, KeyPtrPair> *cce = floor_cce->map_next_;
            while (cce != &pos_inf_ && idx < cache.size())
            {
                tuple = cache.at(idx);
                ScanKey(cce, tuple, true, term);
                cce = cce->map_next_;
                ++idx;
            }

            cache.resize(idx);
        }
        else
        {
            CcEntry<KeyPair, KeyPtrPair> *cce = nullptr;

            if (req.key_type_ == KeyType::PostiveInf)
            {
                cce = pos_inf_.map_prev_;
            }
            else
            {
                size_t offset = 0;
                look_sk.Deserialize(req.start_key_str_->data(),
                                    offset,
                                    compound_schema_.sk_schema_.get());
                cce = Floor(look_sk, req.direct_, req.inclusive_);
            }

            assert(cce != nullptr);

            size_t idx = 0;
            // The backward scan's starting point coincides with a cc entry's
            // key. If the starting point is inclusive, the scan includes the
            // entry's key. If the point is exclusive, the scan starts from the
            // prior entry, including its both the key and the gap.
            if (cce != &neg_inf_ && look_sk == *cce->payload_.first)
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

        CcEntry<KeyPair, KeyPtrPair> *prior_cce =
            reinterpret_cast<CcEntry<KeyPair, KeyPtrPair> *>(
                req.prior_cce_addr_);

        ScanDirection direction = req.direct_;

        size_t idx = 0;
        if (direction == ScanDirection::Forward)
        {
            CcEntry<KeyPair, KeyPtrPair> *cce = prior_cce->map_next_;
            while (cce != &pos_inf_ && idx < req.scan_cache_.size())
            {
                remote::ScanTuple_msg *scan_tuple = req.scan_cache_.at(idx);
                ScanKey(cce, scan_tuple, true, term);
                cce = cce->map_next_;
                ++idx;
            }
        }
        else
        {
            CcEntry<KeyPair, KeyPtrPair> *cce = prior_cce->map_prev_;
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

    bool Execute(CommitSkCc &req) override
    {
        uint32_t ng_id = req.key_shard_code_ >> 10;
        int64_t ng_term = Sharder::Instance().LeaderTerm(ng_id);
        if (ng_term < 0)
        {
            req.Result()->SetError(-1);
            return true;
        }

        CcEntry<KeyPair, KeyPtrPair> *cce = nullptr;

        if (req.skey_ != nullptr)
        {
            const SkT *sk = static_cast<const SkT *>(req.skey_);
            const PkT *pk = static_cast<const PkT *>(req.pkey_);

            cce = FindEmplace(*sk, *pk, req.ts_);
        }
        else
        {
            SkT sk_obj;
            size_t offset = 0;
            sk_obj.Deserialize(req.skey_str_->data(),
                               offset,
                               compound_schema_.sk_schema_.get());

            PkT pk_obj;
            offset = 0;
            pk_obj.Deserialize(req.pkey_str_->data(),
                               offset,
                               compound_schema_.pk_schema_.get());

            cce = FindEmplace(sk_obj, pk_obj, req.ts_);
        }

        if (cce == nullptr)
        {
            // The request needs a new cc entry but the cc map has reached the
            // maximal capacity. Blocks the request by putting it back to the cc
            // request queue.
            shard_->Enqueue(shard_->LocalCoreId(), &req);
            return false;
        }

        cce->payload_status_ =
            req.is_delete_ ? RecordStatus::Deleted : RecordStatus::Normal;
        cce->commit_ts_ = req.ts_;
        cce->gap_commit_ts_ = req.ts_;

        if (cce->ckpt_next_ == nullptr)
        {
            // If the cc entry is not in the checkpoint list, enlists
            // the entry.
            LruEntry *second_last = pos_inf_.ckpt_prev_;
            second_last->ckpt_next_ = cce;
            cce->ckpt_prev_ = second_last;
            cce->ckpt_next_ = &pos_inf_;
            pos_inf_.ckpt_prev_ = cce;
        }

        req.Result()->SetFinished();
        return true;
    }

    bool Execute(CkptScanCc &req) override
    {
        LruEntry *lru_cce = req.start_entry_ == nullptr ? neg_inf_.ckpt_next_
                                                        : req.start_entry_;
        CcEntry<KeyPair, KeyPtrPair> *cce =
            static_cast<CcEntry<KeyPair, KeyPtrPair> *>(lru_cce);

        size_t cnt = 0;
        while (cnt < CkptScanCc::CkptScanBatch && cce != &pos_inf_)
        {
            // The checkpoint ts should be smaller than the ts when an ongoing
            // tx acquired the write intention. Or, there is a possibility that
            // the tx commits prior to the checkpoint.
            assert(!cce->key_lock_.HasWriteLock() ||
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
                cce = static_cast<CcEntry<KeyPair, KeyPtrPair> *>(next);
                continue;
            }

            ++cnt;
            cce = static_cast<CcEntry<KeyPair, KeyPtrPair> *>(cce->ckpt_next_);
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
        return true;
    }

    // this function should not be executed.
    bool Execute(AcquireTableWriteLockCC &req) override
    {
        return true;
    }

    // this function should not be executed.
    bool Execute(remote::RemoteAcquireTableWriteLockCC &req) override
    {
        return true;
    }

    // this function should not be executed.
    bool Execute(ReleaseTableWriteLockCC &req) override
    {
        return true;
    }

    // this function should not be executed.
    bool Execute(CommitCreateTableCC &req) override
    {
        return true;
    }

    // this function should not be executed.
    bool Execute(CommitDropTableCC &req) override
    {
        return true;
    }

    // this function should not be executed.
    bool Execute(FindCatalogCC &req) override
    {
        return true;
    }

    // this function should not be executed.
    bool Execute(CheckCatalogCC &req) override
    {
        return true;
    }

    bool Execute(FaultInjectCC &req) override
    {
        return true;
    }

    std::unique_ptr<CcScanner> CreateScanner(
        ScanDirection direction) const override
    {
        return std::make_unique<
            TemplateCcScanner<SecondaryKey<SkT, PkT>, VoidRecord>>(
            direction, &compound_schema_);
    }

    size_t size() const override
    {
        size_t cnt = 0;
        for (auto sk_it = sk_index_.begin(); sk_it != sk_index_.end(); ++sk_it)
        {
            cnt += sk_it->second.size();
        }

        return cnt;
    }

    void Clean(LruEntry *remove_entry) override
    {
        CcEntry<KeyPair, KeyPtrPair> *cce =
            static_cast<CcEntry<KeyPair, KeyPtrPair> *>(remove_entry);

        CcEntry<KeyPair, KeyPtrPair> *prev_cce = cce->map_prev_;
        CcEntry<KeyPair, KeyPtrPair> *next_cce = cce->map_next_;

        if (*prev_cce->payload_.first == *cce->payload_.first ||
            *cce->payload_.first == *next_cce->payload_.first)
        {
            sk_index_.at(*cce->payload_.first).erase(*cce->payload_.second);
        }
        else
        {
            // The (sk,pk) pair is the last entry of this sk group. Removes the
            // sk from the index.
            sk_index_.erase(*cce->payload_.first);
        }
    }

    void GetCkptSk(const LruEntry *lru_entry,
                   const TxKey *&sk,
                   const TxKey *&pk,
                   bool &is_deleted) const override
    {
        const CcEntry<KeyPair, KeyPtrPair> *cce =
            static_cast<const CcEntry<KeyPair, KeyPtrPair> *>(lru_entry);

        const KeyPtrPair &key_pair = cce->payload_ckpt_.first;
        sk = key_pair.first;
        pk = key_pair.second;
        is_deleted = cce->payload_ckpt_.second;
    }

    TableType Type() const override
    {
        return TableType::Secondary;
    }

    const Schema *KeySchema() const override
    {
        return &compound_schema_;
    }

    const Schema *RecordSchema() const override
    {
        return nullptr;
    }

    /**
     * Used for debug to verify the map_link is complete.
     */
    size_t VerifyOrdering() override
    {
        CcEntry<KeyPair, KeyPtrPair> *cce_prev = nullptr;
        CcEntry<KeyPair, KeyPtrPair> *cce = neg_inf_.map_next_;
        assert(cce != nullptr);
        size_t cnt = 0;
        while (cce != &pos_inf_)
        {
            assert(cce_prev == nullptr ||
                   *cce_prev->payload_.first < *cce->payload_.first ||
                   *cce_prev->payload_.first == *cce->payload_.first &&
                       *cce_prev->payload_.second < *cce->payload_.second);
            cce_prev = cce;
            cce = cce->map_next_;
            ++cnt;
        }

        return cnt;
    }

    std::unique_ptr<CcMap> Clone() const override
    {
        return std::make_unique<SkCcMap<SkT, PkT>>(
            shard_,
            compound_schema_.sk_schema_.get(),
            compound_schema_.pk_schema_.get());
    }

private:
    void ScanKey(CcEntry<KeyPair, KeyPtrPair> *cce,
                 TemplateScanTuple<SecondaryKey<SkT, PkT>, VoidRecord> *tuple,
                 bool include_gap,
                 uint32_t ng_id,
                 int64_t term) const
    {
        SecondaryKey<SkT, PkT> &sk = tuple->Key();
        sk.SKey() = *cce->payload_.first;
        sk.PKey() = *cce->payload_.second;
        tuple->rec_status_ = cce->payload_status_;
        tuple->key_ts_ = cce->commit_ts_;
        tuple->gap_ts_ = include_gap ? cce->gap_commit_ts_ : 0;
        tuple->cce_addr_.SetCce(reinterpret_cast<uint64_t>(cce), term, ng_id);
    }

    void ScanKey(CcEntry<KeyPair, KeyPtrPair> *cce,
                 remote::ScanTuple_msg *tuple,
                 bool include_gap,
                 int64_t term) const
    {
        tuple->clear_key();
        std::string &key_blob = *tuple->mutable_key();

        // Serializes the secondary key
        cce->payload_.first->Serialize(key_blob);
        // Serializes the primary key
        cce->payload_.second->Serialize(key_blob);

        switch (cce->payload_status_)
        {
        case RecordStatus::Normal:
            tuple->set_rec_status(remote::ScanTuple_msg::RecordStatus::
                                      ScanTuple_msg_RecordStatus_NORMAL);
            break;
        case RecordStatus::Deleted:
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

    void ScanGap(CcEntry<KeyPair, KeyPtrPair> *cce,
                 TemplateScanTuple<SecondaryKey<SkT, PkT>, VoidRecord> *tuple,
                 uint32_t ng_id,
                 int64_t term) const
    {
        tuple->key_ts_ = 0;
        tuple->gap_ts_ = cce->gap_commit_ts_;
        tuple->cce_addr_.SetCce(reinterpret_cast<uint64_t>(cce), term, ng_id);
    }

    void ScanGap(CcEntry<KeyPair, KeyPtrPair> *cce,
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

    CcEntry<KeyPair, KeyPtrPair> *FindEmplace(const SkT &sk,
                                              const PkT &pk,
                                              uint64_t ts)
    {
        auto sk_it = sk_index_.lower_bound(sk);

        std::map<PkT, CcEntry<KeyPair, KeyPtrPair>> *pk_group = nullptr;
        typename std::map<PkT, CcEntry<KeyPair, KeyPtrPair>>::iterator pk_it;

        if (sk_it != sk_index_.end() && sk_it->first == sk)
        {
            pk_group = &sk_it->second;
            pk_it = pk_group->lower_bound(pk);

            if (pk_it != pk_group->end() && pk_it->first == pk)
            {
                return &pk_it->second;
            }
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
            sk_it = sk_index_.lower_bound(sk);
        }

        if (pk_group == nullptr)
        {
            // The input sk does not exist in the index. Creates a new pk group
            // for the input sk.
            sk_it = sk_index_.emplace_hint(sk_it,
                                           std::piecewise_construct,
                                           std::forward_as_tuple(sk),
                                           std::forward_as_tuple());
            pk_group = &sk_it->second;
            pk_it = pk_group->begin();
            assert(pk_it == pk_group->end());
        }

        pk_it = pk_group->emplace_hint(pk_it,
                                       std::piecewise_construct,
                                       std::forward_as_tuple(pk),
                                       std::forward_as_tuple(this));

        CcEntry<KeyPair, KeyPtrPair> *prev_cce = nullptr, *next_cce = nullptr,
                                     *new_cce = nullptr;

        new_cce = &pk_it->second;
        // The key of the cc entry of the secondary index is pseudo.
        new_cce->key_ = nullptr;
        new_cce->payload_.first = &sk_it->first;
        new_cce->payload_.second = &pk_it->first;

        if (pk_it == pk_group->begin())
        {
            // The newly inserted pk is the first element in the pk group. The
            // prior cc entry points to the last element of the prior sk's pk
            // group.

            if (sk_it == sk_index_.begin())
            {
                // There is no prior sk. The new (sk,pk) is the first entry in
                // the sk index. The prior cc entry points to negative infinity.
                prev_cce = &neg_inf_;
            }
            else
            {
                --sk_it;
                std::map<PkT, CcEntry<KeyPair, KeyPtrPair>> &prior_pk_group =
                    sk_it->second;
                prev_cce = &prior_pk_group.rbegin()->second;
                ++sk_it;
            }
        }
        else
        {
            --pk_it;
            prev_cce = &pk_it->second;
            ++pk_it;
        }

        ++pk_it;

        if (pk_it == pk_group->end())
        {
            // The newly inserted pk is the last element in the pk group. The
            // next cc entry points to the first element of the next sk's pk
            // group.

            ++sk_it;

            if (sk_it == sk_index_.end())
            {
                // There is no next sk. The new (sk,pk) is the last entry in
                // the sk index. The next cc entry points to positive infinity.
                next_cce = &pos_inf_;
            }
            else
            {
                std::map<PkT, CcEntry<KeyPair, KeyPtrPair>> &next_pk_group =
                    sk_it->second;
                next_cce = &next_pk_group.begin()->second;
            }
        }
        else
        {
            next_cce = &pk_it->second;
        }

        new_cce->map_prev_ = prev_cce;
        new_cce->map_next_ = next_cce;
        prev_cce->map_next_ = new_cce;
        next_cce->map_prev_ = new_cce;

        shard_->UpdateLruList(new_cce);
        return new_cce;
    }

    CcEntry<KeyPair, KeyPtrPair> *Floor(const SkT &sk,
                                        ScanDirection direction,
                                        bool inclusive)
    {
        if (sk_index_.size() == 0)
        {
            return &neg_inf_;
        }

        auto sk_it = sk_index_.lower_bound(sk);

        if (sk_it == sk_index_.end())
        {
            // When the search sk is greater than all the keys in the index, the
            // floor entry is the last index entry.
            std::map<PkT, CcEntry<KeyPair, KeyPtrPair>> &last_pk_group =
                sk_index_.rbegin()->second;

            return &last_pk_group.rbegin()->second;
        }

        CcEntry<KeyPair, KeyPtrPair> *floor_cce = nullptr;

        if (sk_it->first == sk)
        {
            std::map<PkT, CcEntry<KeyPair, KeyPtrPair>> &pk_group =
                sk_it->second;

            if (direction == ScanDirection::Forward && inclusive)
            {
                // >= sk. The floor entry is the first of the pk group.
                floor_cce = &pk_group.begin()->second;
            }
            else if (direction == ScanDirection::Backward && !inclusive)
            {
                // < sk. The floor entry is the entry preceding the pk group.
                floor_cce = &pk_group.begin()->second;
                floor_cce = floor_cce->map_prev_;
            }
            else
            {
                // > sk or <= sk. The floor entry is the last of the pk group.
                floor_cce = &pk_group.rbegin()->second;
            }
        }
        else
        {
            // When the search sk is between sk1 and sk2 (sk1 < sk2), the floor
            // entry is the last of the pk group of sk1.
            if (sk_it == sk_index_.begin())
            {
                return &neg_inf_;
            }

            --sk_it;

            std::map<PkT, CcEntry<KeyPair, KeyPtrPair>> &pk_group =
                sk_it->second;
            floor_cce = &pk_group.rbegin()->second;
        }

        return floor_cce;
    }

    std::map<SkT, std::map<PkT, CcEntry<KeyPair, KeyPtrPair>>> sk_index_;
    CcEntry<KeyPair, KeyPtrPair> neg_inf_, pos_inf_;
    const SkSchema compound_schema_;
};
}  // namespace txservice
