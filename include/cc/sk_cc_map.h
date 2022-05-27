#pragma once

#include <map>
#include <set>

#include "secondary_key.h"
#include "template_cc_map.h"

namespace txservice
{
struct VoidKey : public TxKey
{
    VoidKey()
    {
    }

    bool operator==(const TxKey &rhs) const override
    {
        if (const VoidKey *other_ptr = static_cast<const VoidKey *>(&rhs))
        {
            return *this == *other_ptr;
        }
        return false;
    }

    bool operator<(const TxKey &rhs) const override
    {
        return false;
    }

    size_t Hash() const override
    {
        return 0;
    }

    void Serialize(std::vector<char> &buf, size_t &offset) const override
    {
    }

    void Serialize(std::string &str) const override
    {
    }

    void Deserialize(const char *buf,
                     size_t &offset,
                     const txservice::Schema *key_schema) override
    {
    }

    TxKey::Uptr Clone() const override
    {
        return std::make_unique<VoidKey>(*this);
    }

    std::string ToString() const override
    {
        return std::string("");
    }

    size_t MemUsage() const override
    {
        return 0;
    }
};

template <typename SkT, typename PkT>
struct SkRecord : public TxRecord
{
    SkRecord()
    {
    }

    SkRecord(const SkT *sk, const PkT *pk) : sk_(sk), pk_(pk)
    {
    }

    void Serialize(std::vector<char> &buf, size_t &offset) const override
    {
    }

    void Serialize(std::string &str) const override
    {
    }

    void Deserialize(const char *buf, size_t &offset) override
    {
    }

    TxRecord::Uptr Clone() const override
    {
        return std::make_unique<SkRecord<SkT, PkT>>(sk_, pk_);
    }

    void Copy(const TxRecord &rhs) override
    {
        const SkRecord &typed_rhs = static_cast<const SkRecord &>(rhs);

        sk_ = typed_rhs.sk_;
        pk_ = typed_rhs.pk_;
    }

    std::string ToString() const override
    {
        return std::string("");
    }

    size_t MemUsage() const override
    {
        return 2 * sizeof(nullptr);
    }

    const SkT *sk_;
    const PkT *pk_;
};

template <typename SkT, typename PkT>
class SkCcMap : public CcMap
{
public:
    SkCcMap() = delete;
    SkCcMap(const SkCcMap &rhs) = delete;
    SkCcMap(SkCcMap &&rhs) = delete;

    SkCcMap(CcShard *shard,
            uint64_t schema_ts,
            const Schema *sk_schema = nullptr,
            const Schema *pk_schema = nullptr)
        : CcMap(shard, schema_ts),
          neg_inf_(this),
          pos_inf_(this),
          compound_schema_(sk_schema, pk_schema)
    {
        neg_inf_.key_ = nullptr;
        neg_inf_.payload_.sk_ = NegativeInfinity<SkT>::Instance();
        neg_inf_.payload_.pk_ = NegativeInfinity<PkT>::Instance();
        pos_inf_.key_ = nullptr;
        pos_inf_.payload_.sk_ = PositiveInfinity<SkT>::Instance();
        pos_inf_.payload_.pk_ = PositiveInfinity<PkT>::Instance();

        neg_inf_.map_next_ = &pos_inf_;
        pos_inf_.map_prev_ = &neg_inf_;

        neg_inf_.ckpt_prev_ = nullptr;
        neg_inf_.ckpt_next_ = &pos_inf_;
        pos_inf_.ckpt_prev_ = &neg_inf_;
        pos_inf_.ckpt_next_ = nullptr;
    }

    virtual ~SkCcMap()
    {
        Clean();
    }

    bool Execute(AcquireCc &req) override
    {
        return true;
    }

    bool Execute(PostWriteCc &req) override
    {
        return true;
    }

    bool Execute(AcquireAllCc &req) override
    {
        return false;
    }

    bool Execute(PostWriteAllCc &req) override
    {
        return false;
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
        CcEntry<VoidKey, SkRecord<SkT, PkT>> *cce_ptr = nullptr;

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
            CcEntry<VoidKey, SkRecord<SkT, PkT>> *floor_cce =
                look_sk == NegativeInfinity<SkT>::Instance()
                    ? &neg_inf_
                    : Floor(*look_sk, req.direct_, req.inclusive_);
            assert(floor_cce != nullptr);

            TemplateScanTuple<SecondaryKey<SkT, PkT>, VoidRecord> *scan_tuple =
                typed_cache->AddScanTuple();

            if (floor_cce != &neg_inf_ && req.inclusive_ == true &&
                *look_sk == *floor_cce->payload_.sk_)
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

            CcEntry<VoidKey, SkRecord<SkT, PkT>> *cce = floor_cce->map_next_;
            while (cce != &pos_inf_ && !typed_cache->Full())
            {
                scan_tuple = typed_cache->AddScanTuple();
                ScanKey(cce, scan_tuple, true, req.node_group_id_, term);
                cce = cce->map_next_;
            }
        }
        else
        {
            CcEntry<VoidKey, SkRecord<SkT, PkT>> *cce =
                look_sk == PositiveInfinity<SkT>::Instance()
                    ? pos_inf_.map_prev_
                    : Floor(*look_sk, req.direct_, req.inclusive_);

            assert(cce != nullptr);

            // The backward scan's starting point coincides with a cc entry's
            // key. If the starting point is inclusive, the scan includes the
            // entry's key. If the point is exclusive, the scan starts from the
            // prior entry, including both the key and the gap.
            if (cce != &neg_inf_ && *look_sk == *cce->payload_.sk_)
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

        CcEntry<VoidKey, SkRecord<SkT, PkT>> *prior_cce =
            reinterpret_cast<CcEntry<VoidKey, SkRecord<SkT, PkT>> *>(
                typed_cache->Last()->cce_addr_.CcePtr());

        ScanDirection direction = typed_cache->Scanner()->Direction();
        typed_cache->Reset();

        if (direction == ScanDirection::Forward)
        {
            CcEntry<VoidKey, SkRecord<SkT, PkT>> *cce = prior_cce->map_next_;
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
            CcEntry<VoidKey, SkRecord<SkT, PkT>> *cce = prior_cce->map_prev_;
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
            CcEntry<VoidKey, SkRecord<SkT, PkT>> *floor_cce = nullptr;

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
                look_sk == *floor_cce->payload_.sk_)
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
            CcEntry<VoidKey, SkRecord<SkT, PkT>> *cce = floor_cce->map_next_;
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
            CcEntry<VoidKey, SkRecord<SkT, PkT>> *cce = nullptr;

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
            if (cce != &neg_inf_ && look_sk == *cce->payload_.sk_)
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

        CcEntry<VoidKey, SkRecord<SkT, PkT>> *prior_cce =
            reinterpret_cast<CcEntry<VoidKey, SkRecord<SkT, PkT>> *>(
                req.prior_cce_addr_);

        ScanDirection direction = req.direct_;

        size_t idx = 0;
        if (direction == ScanDirection::Forward)
        {
            CcEntry<VoidKey, SkRecord<SkT, PkT>> *cce = prior_cce->map_next_;
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
            CcEntry<VoidKey, SkRecord<SkT, PkT>> *cce = prior_cce->map_prev_;
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

        CcEntry<VoidKey, SkRecord<SkT, PkT>> *cce = nullptr;

        if (req.secondary_key_ != nullptr)
        {
            const SecondaryKey<SkT, PkT> *secondary_key =
                static_cast<const SecondaryKey<SkT, PkT> *>(req.secondary_key_);

            cce = FindEmplace(
                secondary_key->SKey(), secondary_key->PKey(), req.ts_);
        }
        else
        {
            SecondaryKey<SkT, PkT> secondary_key_obj;
            size_t offset = 0;
            secondary_key_obj.Deserialize(
                req.secondary_key_str_->data(), offset, &compound_schema_);

            cce = FindEmplace(
                secondary_key_obj.SKey(), secondary_key_obj.PKey(), req.ts_);
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

        TryInsertCkptList(cce);

        req.Result()->SetFinished();
        return true;
    }

    bool Execute(CkptScanCc &req) override
    {
        LruEntry *lru_cce = req.start_entry_ == nullptr ? neg_inf_.ckpt_next_
                                                        : req.start_entry_;
        CcEntry<VoidKey, SkRecord<SkT, PkT>> *cce =
            static_cast<CcEntry<VoidKey, SkRecord<SkT, PkT>> *>(lru_cce);

        size_t cnt = 0;
        while (cnt < CkptScanCc::CkptScanBatch && cce != &pos_inf_)
        {
            // The checkpoint ts should be smaller than the ts when an ongoing
            // tx acquired the write intention. Or, there is a possibility that
            // the tx commits prior to the checkpoint.
            assert(!cce->key_lock_.HasWriteLock() ||
                   req.ckpt_ts_ <= cce->last_read_ts_);

            if (cce->commit_ts_ <= req.ckpt_ts_ &&
                cce->commit_ts_ > cce->ckpt_ts_.load(std::memory_order_acquire))
            {
                // no need to update memory usage since this payload_ckpt_ has a
                // fixed size
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
                cce->parent_map_->shard_->DetachCkpt(cce);
                cce = static_cast<CcEntry<VoidKey, SkRecord<SkT, PkT>> *>(next);
                continue;
            }

            ++cnt;
            cce = static_cast<CcEntry<VoidKey, SkRecord<SkT, PkT>> *>(
                cce->ckpt_next_);
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
        size_t offset = 0;
        const std::string_view &log_blob = req.LogContentView();

        // if replay record's commit_ts is smaller than ccmap's commit_ts,
        // this record is generated before the latest schema of the table
        // and hence should skip the replay process.
        if (req.CommitTs() < schema_ts_)
        {
            req.SetFinish();
            return false;
        }

        while (offset < log_blob.size())
        {
            SecondaryKey<SkT, PkT> decoded_key;
            decoded_key.Deserialize(log_blob.data(), offset, &compound_schema_);

            uint8_t delete_flag =
                *reinterpret_cast<const uint8_t *>(log_blob.data() + offset);
            offset += sizeof(uint8_t);

            uint32_t shard_code =
                Sharder::Instance().ShardCode(decoded_key.Hash());
            uint16_t core_id = (shard_code & 0x3FF) % shard_->core_cnt_;
            if (core_id != shard_->core_id_)
            {
                continue;
            }

            CcEntry<VoidKey, SkRecord<SkT, PkT>> *cce = FindEmplace(
                decoded_key.SKey(), decoded_key.PKey(), req.CommitTs());

            if (cce == nullptr)
            {
                shard_->Enqueue(shard_->LocalCoreId(), &req);
                return false;
            }

            // If the key exists in the cc map and its commit ts is
            // greater than that of the log record, skips installing the
            // log record in the cc map and moves to the next key in the
            // log record.
            if (cce->commit_ts_ < req.CommitTs())
            {
                if (delete_flag == 0)
                {
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

    bool Execute(FaultInjectCC &req) override
    {
        return true;
    }

    std::unique_ptr<CcScanner> CreateScanner(
        ScanDirection direction) const override
    {
        return std::make_unique<
            TemplateCcScanner<SecondaryKey<SkT, PkT>, VoidRecord>>(
            direction, ScanIndexType::Secondary, &compound_schema_);
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
        CcShard::DetachLru(remove_entry);

        if (remove_entry->ckpt_next_ != nullptr)
        {
            // If the cc entry is in the checkpoint list, removes it from
            // the checkpoint list.
            shard_->DetachCkpt(remove_entry);
        }

        CcEntry<VoidKey, SkRecord<SkT, PkT>> *cce =
            static_cast<CcEntry<VoidKey, SkRecord<SkT, PkT>> *>(remove_entry);

        CcEntry<VoidKey, SkRecord<SkT, PkT>> *prev_cce = cce->map_prev_;
        CcEntry<VoidKey, SkRecord<SkT, PkT>> *next_cce = cce->map_next_;

        if ((prev_cce != &neg_inf_ &&
             *prev_cce->payload_.sk_ == *cce->payload_.sk_) ||
            (next_cce != &pos_inf_ &&
             *cce->payload_.sk_ == *next_cce->payload_.sk_))
        {
            // delete secondary map of sk_index_
            shard_->mem_usage_ -= cce->GetCcEntryMemUsage();
            shard_->mem_usage_ -= cce->payload_.pk_->MemUsage();

            auto sk_it = sk_index_.find(*cce->payload_.sk_);
            assert(sk_it != sk_index_.end());
            sk_it->second.erase(*cce->payload_.pk_);
        }
        else
        {
            // delete sk_index_
            shard_->mem_usage_ -= cce->GetCcEntryMemUsage();
            shard_->mem_usage_ -= cce->payload_.pk_->MemUsage();
            shard_->mem_usage_ -= cce->payload_.sk_->MemUsage();

            // The (sk,pk) pair is the last entry of this sk group. Removes the
            // sk from the index.
            sk_index_.erase(*cce->payload_.sk_);
        }

        prev_cce->map_next_ = next_cce;
        next_cce->map_prev_ = prev_cce;
    }

    void Clean() override
    {
        while (neg_inf_.map_next_ != &pos_inf_)
        {
            Clean(neg_inf_.map_next_);
        }
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

    void GetCkptSk(const LruEntry *lru_entry,
                   const TxKey *&sk,
                   const TxKey *&pk,
                   bool &is_deleted) const override
    {
        const CcEntry<VoidKey, SkRecord<SkT, PkT>> *cce =
            static_cast<const CcEntry<VoidKey, SkRecord<SkT, PkT>> *>(
                lru_entry);

        const SkRecord<SkT, PkT> &sk_record = cce->payload_ckpt_.first;
        sk = sk_record.sk_;
        pk = sk_record.pk_;
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
        CcEntry<VoidKey, SkRecord<SkT, PkT>> *cce_prev = nullptr;
        CcEntry<VoidKey, SkRecord<SkT, PkT>> *cce = neg_inf_.map_next_;
        assert(cce != nullptr);
        size_t cnt = 0;
        while (cce != &pos_inf_)
        {
            assert(cce_prev == nullptr ||
                   *cce_prev->payload_.sk_ < *cce->payload_.sk_ ||
                   *cce_prev->payload_.sk_ == *cce->payload_.sk_ &&
                       *cce_prev->payload_.pk_ < *cce->payload_.pk_);
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
            schema_ts_,
            compound_schema_.sk_schema_.get(),
            compound_schema_.pk_schema_.get());
    }

private:
    void ScanKey(CcEntry<VoidKey, SkRecord<SkT, PkT>> *cce,
                 TemplateScanTuple<SecondaryKey<SkT, PkT>, VoidRecord> *tuple,
                 bool include_gap,
                 uint32_t ng_id,
                 int64_t term) const
    {
        SecondaryKey<SkT, PkT> &sk = tuple->Key();
        sk.SKey() = *cce->payload_.sk_;
        sk.PKey() = *cce->payload_.pk_;
        tuple->rec_status_ = cce->payload_status_;
        tuple->key_ts_ = cce->commit_ts_;
        tuple->gap_ts_ = include_gap ? cce->gap_commit_ts_ : 0;
        tuple->cce_addr_.SetCce(reinterpret_cast<uint64_t>(cce), term, ng_id);
    }

    void ScanKey(CcEntry<VoidKey, SkRecord<SkT, PkT>> *cce,
                 remote::ScanTuple_msg *tuple,
                 bool include_gap,
                 int64_t term) const
    {
        tuple->clear_key();
        std::string &key_blob = *tuple->mutable_key();

        // Serializes the secondary key
        cce->payload_.sk_->Serialize(key_blob);
        // Serializes the primary key
        cce->payload_.pk_->Serialize(key_blob);

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

        // For remote scans, the returned cc entries' node group ID is set on
        // the sender side when the sender receives the response.
    }

    void ScanGap(CcEntry<VoidKey, SkRecord<SkT, PkT>> *cce,
                 TemplateScanTuple<SecondaryKey<SkT, PkT>, VoidRecord> *tuple,
                 uint32_t ng_id,
                 int64_t term) const
    {
        tuple->key_ts_ = 0;
        tuple->gap_ts_ = cce->gap_commit_ts_;
        tuple->cce_addr_.SetCce(reinterpret_cast<uint64_t>(cce), term, ng_id);
    }

    void ScanGap(CcEntry<VoidKey, SkRecord<SkT, PkT>> *cce,
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

    CcEntry<VoidKey, SkRecord<SkT, PkT>> *FindEmplace(const SkT &sk,
                                                      const PkT &pk,
                                                      uint64_t ts)
    {
        auto sk_it = sk_index_.lower_bound(sk);

        std::map<PkT, CcEntry<VoidKey, SkRecord<SkT, PkT>>> *pk_group = nullptr;
        typename std::map<PkT, CcEntry<VoidKey, SkRecord<SkT, PkT>>>::iterator
            pk_it;

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

            // Recheck sk_it in case the iterator may be invalidated during
            // shard_->Clean(). The same for pk_it.
            sk_it = sk_index_.lower_bound(sk);
            if (sk_it->first == sk)
            {
                assert(!sk_it->second.empty());
                pk_group = &sk_it->second;
                pk_it = pk_group->lower_bound(pk);
            }
            else
            {
                pk_group = nullptr;
            }
        }

        if (pk_group == nullptr)
        {
            // The input sk does not exist in the index. Creates a new pk group
            // for the input sk.
            sk_it = sk_index_.emplace_hint(sk_it,
                                           std::piecewise_construct,
                                           std::forward_as_tuple(sk),
                                           std::forward_as_tuple());
            shard_->mem_usage_ += sk.MemUsage();
            pk_group = &sk_it->second;
            pk_it = pk_group->begin();
            assert(pk_it == pk_group->end());
        }

        pk_it = pk_group->emplace_hint(pk_it,
                                       std::piecewise_construct,
                                       std::forward_as_tuple(pk),
                                       std::forward_as_tuple(this));
        shard_->mem_usage_ += pk.MemUsage();
        CcEntry<VoidKey, SkRecord<SkT, PkT>> *prev_cce = nullptr,
                                             *next_cce = nullptr,
                                             *new_cce = nullptr;

        new_cce = &pk_it->second;
        new_cce->key_ = nullptr;
        new_cce->payload_.sk_ = &sk_it->first;
        new_cce->payload_.pk_ = &pk_it->first;

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
                std::map<PkT, CcEntry<VoidKey, SkRecord<SkT, PkT>>>
                    &prior_pk_group = sk_it->second;
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
                std::map<PkT, CcEntry<VoidKey, SkRecord<SkT, PkT>>>
                    &next_pk_group = sk_it->second;
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

        shard_->mem_usage_ += new_cce->GetCcEntryMemUsage();

        return new_cce;
    }

    CcEntry<VoidKey, SkRecord<SkT, PkT>> *Floor(const SkT &sk,
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
            std::map<PkT, CcEntry<VoidKey, SkRecord<SkT, PkT>>> &last_pk_group =
                sk_index_.rbegin()->second;

            return &last_pk_group.rbegin()->second;
        }

        CcEntry<VoidKey, SkRecord<SkT, PkT>> *floor_cce = nullptr;

        if (sk_it->first == sk)
        {
            std::map<PkT, CcEntry<VoidKey, SkRecord<SkT, PkT>>> &pk_group =
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

            std::map<PkT, CcEntry<VoidKey, SkRecord<SkT, PkT>>> &pk_group =
                sk_it->second;
            floor_cce = &pk_group.rbegin()->second;
        }

        return floor_cce;
    }

    std::map<SkT, std::map<PkT, CcEntry<VoidKey, SkRecord<SkT, PkT>>>>
        sk_index_;
    CcEntry<VoidKey, SkRecord<SkT, PkT>> neg_inf_, pos_inf_;
    const SkSchema compound_schema_;
};
}  // namespace txservice
