#pragma once

#include <map>
#include <memory>  // make_shared
#include <set>

#include "secondary_key.h"
#include "template_cc_map.h"
#include "tx_key.h"  // VoidKey
#include "type.h"    // TableName

namespace txservice
{

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

    const SkT *sk_{nullptr};
    const PkT *pk_{nullptr};
};

template <typename SkT, typename PkT>
class SkCcMap : public CcMap
{
public:
    SkCcMap() = delete;
    SkCcMap(const SkCcMap &rhs) = delete;
    SkCcMap(SkCcMap &&rhs) = delete;

    SkCcMap(CcShard *shard,
            const TableName &index_name,
            uint64_t schema_ts,
            const Schema *sk_schema = nullptr,
            const Schema *pk_schema = nullptr)
        : CcMap(shard, index_name, schema_ts),
          neg_inf_(this),
          pos_inf_(this),
          compound_schema_(sk_schema, pk_schema)
    {
        neg_inf_.key_ = nullptr;
        neg_inf_.payload_ = std::make_shared<SkRecord<SkT, PkT>>();
        neg_inf_.payload_->sk_ = NegativeInfinity<SkT>::Instance();
        neg_inf_.payload_->pk_ = NegativeInfinity<PkT>::Instance();
        pos_inf_.key_ = nullptr;
        pos_inf_.payload_ = std::make_shared<SkRecord<SkT, PkT>>();
        pos_inf_.payload_->sk_ = PositiveInfinity<SkT>::Instance();
        pos_inf_.payload_->pk_ = PositiveInfinity<PkT>::Instance();

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
        if (req.Isolation() == IsolationLevel::Snapshot)
        {
            // Notice(lzx): this case only for debug testing.
        }
        else
        {
            assert(req.Type() == ReadType::OutsideNormal);
        }

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

        if (req.Isolation() == IsolationLevel::Snapshot)
        {
            // Notice(lzx): this case only for debug testing.
            VersionRecord<SkRecord<SkT, PkT>> v_rec;
            bool res = cce_ptr->MvccGet(req.ReadTimestamp(), v_rec);
            if (!res)
            {
                // TODO(lzx): to handle this error.
                assert(res);
            }
            hd_res->Value().rec_status_ = v_rec.payload_status_;
            hd_res->Value().ts_ = v_rec.commit_ts_;
            hd_res->SetFinished();
            return true;
        }

        cce_ptr->payload_status_ = RecordStatus::Normal;
        cce_addr.SetCce(reinterpret_cast<uint64_t>(cce_ptr), term);

        hd_res->Value().ts_ = cce_ptr->commit_ts_;
        hd_res->Value().rec_status_ = cce_ptr->payload_status_;

        // Refill mvcc archives
        if ((req.Type() == ReadType::OutsideNormal ||
             req.Type() == ReadType::OutsideDeleted) &&
            req.ArchivesPtr() != nullptr && req.ArchivesPtr()->size() > 0)
        {
            cce_ptr->AddArchiveRecords(*req.ArchivesPtr());
        }

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
        TX_TRACE_ACTION_WITH_CONTEXT(
            (txservice::CcMap *) this,
            &req,
            [&req]() -> std::string
            {
                return std::string("\"cc_map_type\":\"sk_cc_map\"")
                    .append(",\"tx_number\":")
                    .append(std::to_string(req.Txn()))
                    .append(",\"term\":")
                    .append(std::to_string(req.TxTerm()));
            });
        TX_TRACE_DUMP(&req);

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

        Iterator scan_ccm_it;
        CcEntry<VoidKey, SkRecord<SkT, PkT>> *cce = nullptr;

        if (req.CcePtr() != nullptr)
        {
            cce = static_cast<CcEntry<VoidKey, SkRecord<SkT, PkT>> *>(
                req.CcePtr());
            req.SetCcePtr(nullptr);
            // Lock has been acquired
            scan_ccm_it = Iterator(cce, &neg_inf_, &pos_inf_);
        }
        else
        {
            std::pair<Iterator, ScanType> start_pair =
                req.direct_ == ScanDirection::Forward
                    ? FowardScanStart(*look_sk, req.inclusive_)
                    : BackwardScanStart(*look_sk, req.inclusive_);

            scan_ccm_it = start_pair.first;
            cce = std::get<2>(*scan_ccm_it);

            TemplateScanTuple<SecondaryKey<SkT, PkT>, VoidRecord> *scan_tuple =
                nullptr;
            scan_tuple = typed_cache->AddScanTuple();
            switch (start_pair.second)
            {
            case ScanType::ScanGap:
                ScanGap(cce, scan_tuple, req.node_group_id_, req.term_);
                break;
            case ScanType::ScanBoth:
                ScanKey(cce,
                        scan_tuple,
                        true,
                        req.node_group_id_,
                        req.term_,
                        req.ReadTimestamp(),
                        req.Isolation());
                break;
            case ScanType::ScanKey:
                ScanKey(cce,
                        scan_tuple,
                        false,
                        req.node_group_id_,
                        req.term_,
                        req.ReadTimestamp(),
                        req.Isolation());
                break;
            default:
                break;
            }
            req.SetCcePtr(cce);

            // sk only needs to acquire read intention
            if (!ConditionalReadLockCce(cce,
                                        req,
                                        LockType::ReadIntent,
                                        req.TxTerm(),
                                        req.NodeGroupId(),
                                        cce->payload_status_,
                                        term))
            {
                TX_TRACE_ACTION_WITH_CONTEXT(
                    &req,
                    "AcquireReadIntent.Fail",
                    reinterpret_cast<LruEntry *>(cce),
                    [&req]() -> std::string
                    {
                        return std::string(",\"tx_number\":")
                            .append(std::to_string(req.Txn()))
                            .append(",\"term\":")
                            .append(std::to_string(req.TxTerm()));
                    });
                return false;
            }
        }

        if (req.direct_ == ScanDirection::Forward)
        {
            ++scan_ccm_it;

            Iterator pos_inf_it = End();

            for (; scan_ccm_it != pos_inf_it && !typed_cache->Full();
                 ++scan_ccm_it)
            {
                cce = std::get<2>(*scan_ccm_it);
                TemplateScanTuple<SecondaryKey<SkT, PkT>, VoidRecord>
                    *scan_tuple = typed_cache->AddScanTuple();
                ScanKey(cce,
                        scan_tuple,
                        true,
                        req.node_group_id_,
                        req.term_,
                        req.ReadTimestamp(),
                        req.Isolation());
                req.SetCcePtr(cce);

                if (!ConditionalReadLockCce(cce,
                                            req,
                                            LockType::ReadIntent,
                                            req.TxTerm(),
                                            req.NodeGroupId(),
                                            cce->payload_status_,
                                            term))
                {
                    TX_TRACE_ACTION_WITH_CONTEXT(
                        &req,
                        "AcquireReadIntentOnKey.Fail",
                        reinterpret_cast<LruEntry *>(cce),
                        [&req]() -> std::string
                        {
                            return std::string(",\"tx_number\":")
                                .append(std::to_string(req.Txn()))
                                .append(",\"term\":")
                                .append(std::to_string(req.TxTerm()));
                        });
                    return false;
                }
            }
        }
        else
        {
            --scan_ccm_it;

            Iterator neg_inf_it = Begin();
            for (; scan_ccm_it != neg_inf_it && !typed_cache->Full();
                 --scan_ccm_it)
            {
                cce = std::get<2>(*scan_ccm_it);
                TemplateScanTuple<SecondaryKey<SkT, PkT>, VoidRecord>
                    *scan_tuple = typed_cache->AddScanTuple();
                ScanKey(cce,
                        scan_tuple,
                        true,
                        req.node_group_id_,
                        req.term_,
                        req.ReadTimestamp(),
                        req.Isolation());
                req.SetCcePtr(cce);

                if (!ConditionalReadLockCce(cce,
                                            req,
                                            LockType::ReadIntent,
                                            req.TxTerm(),
                                            req.NodeGroupId(),
                                            cce->payload_status_,
                                            term))
                {
                    TX_TRACE_ACTION_WITH_CONTEXT(
                        &req,
                        "AcquireReadIntentOnKey.Fail",
                        reinterpret_cast<LruEntry *>(cce),
                        [&req]() -> std::string
                        {
                            return std::string(",\"tx_number\":")
                                .append(std::to_string(req.Txn()))
                                .append(",\"term\":")
                                .append(std::to_string(req.TxTerm()));
                        });
                    return false;
                }
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
                return std::string("\"cc_map_type\":\"sk_cc_map\"")
                    .append(",\"tx_number\":")
                    .append(std::to_string(req.Txn()))
                    .append(",\"term\":")
                    .append(std::to_string(req.TxTerm()));
            });
        TX_TRACE_DUMP(&req);

        int64_t term = Sharder::Instance().LeaderTerm(req.node_group_id_);
        if (term < 0)
        {
            req.Result()->SetError(-1);
            return true;
        }
        req.Result()->Value().term_ = term;

        TemplateScanCache<SecondaryKey<SkT, PkT>, VoidRecord> *typed_cache =
            static_cast<
                TemplateScanCache<SecondaryKey<SkT, PkT>, VoidRecord> *>(
                req.scan_cache_);
        assert(typed_cache->Full());

        ScanDirection direction = typed_cache->Scanner()->Direction();
        CcEntry<VoidKey, SkRecord<SkT, PkT>> *prior_cce = nullptr;
        if (req.CcePtr() != nullptr)
        {
            prior_cce = static_cast<CcEntry<VoidKey, SkRecord<SkT, PkT>> *>(
                req.CcePtr());
            req.SetCcePtr(nullptr);
            // Lock has been acquired
        }
        else
        {
            prior_cce =
                reinterpret_cast<CcEntry<VoidKey, SkRecord<SkT, PkT>> *>(
                    typed_cache->Last()->cce_addr_.CcePtr());
            typed_cache->Reset();
        }

        if (direction == ScanDirection::Forward)
        {
            CcEntry<VoidKey, SkRecord<SkT, PkT>> *cce = prior_cce->map_next_;
            while (cce != &pos_inf_ && !typed_cache->Full())
            {
                if (req.is_ckpt_delta_ &&
                    cce->commit_ts_ <=
                        cce->ckpt_ts_.load(std::memory_order_acquire))
                {
                    cce = cce->map_next_;
                    continue;
                }

                TemplateScanTuple<SecondaryKey<SkT, PkT>, VoidRecord>
                    *scan_tuple = typed_cache->AddScanTuple();
                ScanKey(cce,
                        scan_tuple,
                        true,
                        req.node_group_id_,
                        term,
                        req.ReadTimestamp(),
                        req.Isolation());
                req.SetCcePtr(cce);

                if (!ConditionalReadLockCce(cce,
                                            req,
                                            LockType::ReadIntent,
                                            req.TxTerm(),
                                            req.NodeGroupId(),
                                            cce->payload_status_,
                                            term))
                {
                    TX_TRACE_ACTION_WITH_CONTEXT(
                        &req,
                        "AcquireReadIntent.Fail",
                        reinterpret_cast<LruEntry *>(cce),
                        [&req]() -> std::string
                        {
                            return std::string(",\"tx_number\":")
                                .append(std::to_string(req.Txn()))
                                .append(",\"term\":")
                                .append(std::to_string(req.TxTerm()));
                        });
                    return false;
                }

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
                    req.SetCcePtr(cce);

                    if (!ConditionalReadLockCce(cce,
                                                req,
                                                LockType::ReadIntent,
                                                req.TxTerm(),
                                                req.NodeGroupId(),
                                                cce->payload_status_,
                                                term,
                                                true))
                    {
                        TX_TRACE_ACTION_WITH_CONTEXT(
                            &req,
                            "AcquireReadIntentOnGap.Fail",
                            reinterpret_cast<LruEntry *>(cce),
                            [&req]() -> std::string
                            {
                                return std::string(",\"tx_number\":")
                                    .append(std::to_string(req.Txn()))
                                    .append(",\"term\":")
                                    .append(std::to_string(req.TxTerm()));
                            });
                        return false;
                    }
                }
                else
                {
                    ScanKey(cce,
                            scan_tuple,
                            true,
                            req.node_group_id_,
                            term,
                            req.ReadTimestamp(),
                            req.Isolation());
                    req.SetCcePtr(cce);

                    if (!ConditionalReadLockCce(cce,
                                                req,
                                                LockType::ReadIntent,
                                                req.TxTerm(),
                                                req.NodeGroupId(),
                                                cce->payload_status_,
                                                term,
                                                true))
                    {
                        TX_TRACE_ACTION_WITH_CONTEXT(
                            &req,
                            "AcquireReadIntentOnKey.Fail",
                            reinterpret_cast<LruEntry *>(cce),
                            [&req]() -> std::string
                            {
                                return std::string(",\"tx_number\":")
                                    .append(std::to_string(req.Txn()))
                                    .append(",\"term\":")
                                    .append(std::to_string(req.TxTerm()));
                            });
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
        TX_TRACE_ACTION_WITH_CONTEXT(
            (txservice::CcMap *) this,
            &req,
            [&req]() -> std::string
            {
                return std::string("\"cc_map_type\":\"sk_cc_map\"")
                    .append(",\"tx_number\":")
                    .append(std::to_string(req.Txn()))
                    .append(",\"term\":")
                    .append(std::to_string(req.TxTerm()));
            });
        TX_TRACE_DUMP(&req);

        int64_t term = Sharder::Instance().LeaderTerm(req.node_group_id_);
        if (term < 0)
        {
            req.Result()->SetError(-1);
            return true;
        }

        const SkT *look_sk;
        SkT sk_obj;

        switch (req.key_type_)
        {
        case KeyType::NegativeInf:
            look_sk = NegativeInfinity<SkT>::Instance();
            break;
        case KeyType::PostiveInf:
            look_sk = PositiveInfinity<SkT>::Instance();
            break;
        default:
            size_t offset = 0;
            sk_obj.Deserialize(req.start_key_str_->data(),
                               offset,
                               compound_schema_.sk_schema_.get());
            look_sk = &sk_obj;
            break;
        }

        std::vector<remote::ScanTuple_msg *> &cache =
            req.scan_caches_.at(shard_->LocalCoreId());

        Iterator scan_ccm_it;
        CcEntry<VoidKey, SkRecord<SkT, PkT>> *cce = nullptr;
        remote::ScanTuple_msg *tuple = nullptr;
        size_t tuple_idx = 0;

        if (req.CcePtr() != nullptr)
        {
            cce = static_cast<CcEntry<VoidKey, SkRecord<SkT, PkT>> *>(
                req.CcePtr());
            req.SetCcePtr(nullptr);
            // Lock has been acquired
            scan_ccm_it = Iterator(cce, &neg_inf_, &pos_inf_);
        }
        else
        {
            std::pair<Iterator, ScanType> start_pair =
                req.direct_ == ScanDirection::Forward
                    ? FowardScanStart(*look_sk, req.inclusive_)
                    : BackwardScanStart(*look_sk, req.inclusive_);

            scan_ccm_it = start_pair.first;
            cce = std::get<2>(*scan_ccm_it);

            remote::ScanTuple_msg *tuple = cache.at(0);
            switch (start_pair.second)
            {
            case ScanType::ScanGap:
                if (!req.is_ckpt_delta_)
                {
                    ScanGap(cce, tuple, term);
                }
                break;
            case ScanType::ScanBoth:
                ScanKey(cce,
                        tuple,
                        true,
                        term,
                        req.ReadTimestamp(),
                        req.Isolation());
                break;
            case ScanType::ScanKey:
                ScanKey(cce,
                        tuple,
                        false,
                        term,
                        req.ReadTimestamp(),
                        req.Isolation());
                break;
            default:
                break;
            }

            req.SetCcePtr(cce);
            if (!ConditionalReadLockCce(cce,
                                        req,
                                        LockType::ReadIntent,
                                        req.TxTerm(),
                                        req.NodeGroupId(),
                                        cce->payload_status_,
                                        term))
            {
                TX_TRACE_ACTION_WITH_CONTEXT(
                    &req,
                    "AcquireReadIntent.Fail",
                    reinterpret_cast<LruEntry *>(cce),
                    [&req]() -> std::string
                    {
                        return std::string(",\"tx_number\":")
                            .append(std::to_string(req.Txn()))
                            .append(",\"term\":")
                            .append(std::to_string(req.TxTerm()));
                    });
                return false;
            }
        }

        if (req.direct_ == ScanDirection::Forward)
        {
            ++scan_ccm_it;

            Iterator pos_inf_it = End();
            for (; scan_ccm_it != pos_inf_it && tuple_idx < cache.size();
                 ++scan_ccm_it)
            {
                cce = std::get<2>(*scan_ccm_it);
                if (req.is_ckpt_delta_ &&
                    cce->commit_ts_ <=
                        cce->ckpt_ts_.load(std::memory_order_acquire))
                {
                    ++scan_ccm_it;
                    continue;
                }
                tuple = cache.at(tuple_idx);
                ScanKey(cce,
                        tuple,
                        true,
                        term,
                        req.ReadTimestamp(),
                        req.Isolation());

                ++tuple_idx;
                req.SetCcePtr(cce);

                if (!ConditionalReadLockCce(cce,
                                            req,
                                            LockType::ReadIntent,
                                            req.TxTerm(),
                                            req.NodeGroupId(),
                                            cce->payload_status_,
                                            term))
                {
                    TX_TRACE_ACTION_WITH_CONTEXT(
                        &req,
                        "AcquireReadIntentOnKey.Fail",
                        reinterpret_cast<LruEntry *>(cce),
                        [&req]() -> std::string
                        {
                            return std::string(",\"tx_number\":")
                                .append(std::to_string(req.Txn()))
                                .append(",\"term\":")
                                .append(std::to_string(req.TxTerm()));
                        });
                    return false;
                }
            }
        }
        else
        {
            --scan_ccm_it;

            Iterator neg_inf_it = Begin();
            for (; scan_ccm_it != neg_inf_it && tuple_idx < cache.size();
                 --scan_ccm_it)
            {
                cce = std::get<2>(*scan_ccm_it);
                if (req.is_ckpt_delta_ &&
                    cce->commit_ts_ <=
                        cce->ckpt_ts_.load(std::memory_order_acquire))
                {
                    --scan_ccm_it;
                    continue;
                }
                tuple = cache.at(tuple_idx);
                ScanKey(cce,
                        tuple,
                        true,
                        term,
                        req.ReadTimestamp(),
                        req.Isolation());

                ++tuple_idx;
                req.SetCcePtr(cce);

                if (!ConditionalReadLockCce(cce,
                                            req,
                                            LockType::ReadIntent,
                                            req.TxTerm(),
                                            req.NodeGroupId(),
                                            cce->payload_status_,
                                            term))
                {
                    TX_TRACE_ACTION_WITH_CONTEXT(
                        &req,
                        "AcquireReadIntentOnKey.Fail",
                        reinterpret_cast<LruEntry *>(cce),
                        [&req]() -> std::string
                        {
                            return std::string(",\"tx_number\":")
                                .append(std::to_string(req.Txn()))
                                .append(",\"term\":")
                                .append(std::to_string(req.TxTerm()));
                        });
                    return false;
                }
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
                return std::string("\"cc_map_type\":\"sk_cc_map\"")
                    .append(",\"tx_number\":")
                    .append(std::to_string(req.Txn()))
                    .append(",\"term\":")
                    .append(std::to_string(req.TxTerm()));
            });
        TX_TRACE_DUMP(&req);

        int64_t term = Sharder::Instance().LeaderTerm(req.node_group_id_);
        if (term < 0)
        {
            req.Result()->SetError(-1);
            return true;
        }

        CcEntry<VoidKey, SkRecord<SkT, PkT>> *prior_cce = nullptr;
        if (req.CcePtr() != nullptr)
        {
            prior_cce = static_cast<CcEntry<VoidKey, SkRecord<SkT, PkT>> *>(
                req.CcePtr());
        }
        else
        {
            prior_cce =
                reinterpret_cast<CcEntry<VoidKey, SkRecord<SkT, PkT>> *>(
                    req.prior_cce_addr_);
        }

        ScanDirection direction = req.direct_;

        size_t idx = 0;
        if (direction == ScanDirection::Forward)
        {
            CcEntry<VoidKey, SkRecord<SkT, PkT>> *cce = prior_cce->map_next_;
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
                ScanKey(cce,
                        scan_tuple,
                        true,
                        term,
                        req.ReadTimestamp(),
                        req.Isolation());
                ++idx;
                req.SetCcePtr(cce);

                if (!ConditionalReadLockCce(cce,
                                            req,
                                            LockType::ReadIntent,
                                            req.TxTerm(),
                                            req.NodeGroupId(),
                                            cce->payload_status_,
                                            term))
                {
                    TX_TRACE_ACTION_WITH_CONTEXT(
                        &req,
                        "AcquireReadIntentOnKey.Fail",
                        reinterpret_cast<LruEntry *>(cce),
                        [&req]() -> std::string
                        {
                            return std::string(",\"tx_number\":")
                                .append(std::to_string(req.Txn()))
                                .append(",\"term\":")
                                .append(std::to_string(req.TxTerm()));
                        });
                    return false;
                }

                cce = cce->map_next_;
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
                    req.SetCcePtr(cce);

                    if (!ConditionalReadLockCce(cce,
                                                req,
                                                LockType::ReadIntent,
                                                req.TxTerm(),
                                                req.NodeGroupId(),
                                                cce->payload_status_,
                                                term,
                                                true))
                    {
                        TX_TRACE_ACTION_WITH_CONTEXT(
                            &req,
                            "AcquireReadIntentOnGap.Fail",
                            reinterpret_cast<LruEntry *>(cce),
                            [&req]() -> std::string
                            {
                                return std::string(",\"tx_number\":")
                                    .append(std::to_string(req.Txn()))
                                    .append(",\"term\":")
                                    .append(std::to_string(req.TxTerm()));
                            });
                        return false;
                    }
                }
                else
                {
                    ScanKey(cce,
                            scan_tuple,
                            true,
                            term,
                            req.ReadTimestamp(),
                            req.Isolation());
                    req.SetCcePtr(cce);

                    if (!ConditionalReadLockCce(cce,
                                                req,
                                                LockType::ReadIntent,
                                                req.TxTerm(),
                                                req.NodeGroupId(),
                                                cce->payload_status_,
                                                term,
                                                true))
                    {
                        TX_TRACE_ACTION_WITH_CONTEXT(
                            &req,
                            "AcquireReadIntentOnKey.Fail",
                            reinterpret_cast<LruEntry *>(cce),
                            [&req]() -> std::string
                            {
                                return std::string(",\"tx_number\":")
                                    .append(std::to_string(req.Txn()))
                                    .append(",\"term\":")
                                    .append(std::to_string(req.TxTerm()));
                            });
                        return false;
                    }
                }

                ++idx;
                cce = cce->map_prev_;
            }
        }
        req.scan_cache_.resize(idx);

        req.Result()->SetFinished();
        return true;
    }

    bool Execute(CommitSkCc &req) override
    {
        TX_TRACE_ACTION_WITH_CONTEXT(
            (txservice::CcMap *) this,
            &req,
            [&req]() -> std::string
            {
                return std::string("\"cc_map_type\":\"sk_cc_map\"")
                    .append(",\"tx_number\":")
                    .append(std::to_string(req.Txn()))
                    .append(",\"term\":")
                    .append("0");
            });
        TX_TRACE_DUMP(&req);

        uint32_t ng_id = req.key_shard_code_ >> 10;
        int64_t term = Sharder::Instance().LeaderTerm(ng_id);
        if (term < 0)
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

        // for mvcc
        if (req.Protocol() == CcProtocol::MVCC)
        {
            uint64_t recycle_ts = shard_->GlobalMinTxStartTs();
            cce->KickOutArchiveRecords(recycle_ts);
            size_t added_mem_usage = cce->ArchiveBeforeUpdate();
            shard_->mem_usage_ += added_mem_usage;
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
        TX_TRACE_ACTION_WITH_CONTEXT(
            (txservice::CcMap *) this,
            &req,
            [&req]() -> std::string
            {
                return std::string("\"cc_map_type\":\"sk_cc_map\"")
                    .append(",\"tx_number\":")
                    .append(std::to_string(req.Txn()))
                    .append(",\"term\":")
                    .append("0");
            });
        TX_TRACE_DUMP(&req);

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
                cce->payload_ckpt_.first = *(cce->payload_);
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
        TX_TRACE_ACTION_WITH_CONTEXT(
            (txservice::CcMap *) this,
            &req,
            [&req]() -> std::string
            {
                return std::string("\"cc_map_type\":\"sk_cc_map\"")
                    .append(",\"tx_number\":")
                    .append(std::to_string(req.Txn()))
                    .append(",\"term\":")
                    .append("0");
            });
        TX_TRACE_DUMP(&req);

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
                    shard_->DeleteLockHolidngTx(txn, cce, true);
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

    bool Execute(CleanCcEntryForTestCc &req) override
    {
        const TxKey *key_ptr = req.Key();
        bool only_archives = req.OnlyCleanArchives();
        CcEntry<VoidKey, SkRecord<SkT, PkT>> *cce_ptr = nullptr;

        if (key_ptr != nullptr)
        {
            // find cc entry
            const SecondaryKey<SkT, PkT> *look_key =
                static_cast<const SecondaryKey<SkT, PkT> *>(req.Key());
            const SkT &sk = look_key->SKey();
            const PkT &pk = look_key->PKey();

            auto sk_it = sk_index_.lower_bound(sk);

            std::map<PkT, CcEntry<VoidKey, SkRecord<SkT, PkT>>> *pk_group =
                nullptr;
            typename std::map<PkT,
                              CcEntry<VoidKey, SkRecord<SkT, PkT>>>::iterator
                pk_it;

            if (sk_it != sk_index_.end() && sk_it->first == sk)
            {
                pk_group = &sk_it->second;
                pk_it = pk_group->lower_bound(pk);

                if (pk_it != pk_group->end() && pk_it->first == pk)
                {
                    cce_ptr = &pk_it->second;
                }
            }

            if (cce_ptr != nullptr)
            {
                if (cce_ptr->payload_ != nullptr)
                {
                    cce_ptr->payload_ckpt_.first = *(cce_ptr->payload_);
                }
                cce_ptr->payload_ckpt_.second =
                    (cce_ptr->payload_status_ == RecordStatus::Deleted);
                bool res = shard_->FlushEntry(cce_ptr, only_archives);
                if (!res)
                {
                    req.Result()->SetValue(false);
                }
                else
                {
                    req.Result()->SetValue(true);
                    if (only_archives)
                    {
                        cce_ptr->archives_.clear();
                    }
                    else
                    {
                        ccm_has_full_entries_ = false;
                        Clean(cce_ptr);
                    }
                }
            }
        }
        req.Result()->SetFinished();
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
             *prev_cce->payload_->sk_ == *cce->payload_->sk_) ||
            (next_cce != &pos_inf_ &&
             *cce->payload_->sk_ == *next_cce->payload_->sk_))
        {
            // delete secondary map of sk_index_
            shard_->DecrementMemory(cce->GetCcEntryMemUsage() +
                                    cce->payload_->pk_->MemUsage());

            auto sk_it = sk_index_.find(*cce->payload_->sk_);
            assert(sk_it != sk_index_.end());
            sk_it->second.erase(*cce->payload_->pk_);

            if (sk_it->second.empty())
            {
                sk_index_.erase(*cce->payload_->sk_);
            }
        }
        else
        {
            // delete sk_index_
            shard_->DecrementMemory(cce->GetCcEntryMemUsage() +
                                    cce->payload_->pk_->MemUsage() +
                                    cce->payload_->sk_->MemUsage());

            // The (sk,pk) pair is the last entry of this sk group. Removes the
            // sk from the index.
            sk_index_.erase(*cce->payload_->sk_);
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
                   *cce_prev->payload_->sk_ < *cce->payload_->sk_ ||
                   *cce_prev->payload_->sk_ == *cce->payload_->sk_ &&
                       *cce_prev->payload_->pk_ < *cce->payload_->pk_);
            cce_prev = cce;
            cce = cce->map_next_;
            ++cnt;
        }

        return cnt;
    }

    TxKey::Uptr ExportSecondaryKey(LruEntry *entry) const override
    {
        CcEntry<VoidKey, SkRecord<SkT, PkT>> *cce =
            static_cast<CcEntry<VoidKey, SkRecord<SkT, PkT>> *>(entry);

        return std::make_unique<SecondaryKey<SkT, PkT>>(*cce->payload_->sk_,
                                                        *cce->payload_->pk_);
    }

private:
    void ScanKey(CcEntry<VoidKey, SkRecord<SkT, PkT>> *cce,
                 TemplateScanTuple<SecondaryKey<SkT, PkT>, VoidRecord> *tuple,
                 bool include_gap,
                 uint32_t ng_id,
                 int64_t term,
                 uint64_t read_ts,
                 IsolationLevel iso_level) const
    {
        SecondaryKey<SkT, PkT> &sk = tuple->Key();
        sk.SKey() = *cce->payload_->sk_;
        sk.PKey() = *cce->payload_->pk_;
        if (iso_level == IsolationLevel::Snapshot)
        {
            VersionRecord<SkRecord<SkT, PkT>> v_rec;
            bool res = cce->MvccGet(read_ts, v_rec);
            if (!res)
            {
                // TODO(lzx): to handle this error.
                assert(res);
            }
            tuple->rec_status_ = v_rec.payload_status_;
            tuple->key_ts_ = v_rec.commit_ts_;
        }
        else
        {
            tuple->rec_status_ = cce->payload_status_;
            tuple->key_ts_ = cce->commit_ts_;
        }
        tuple->gap_ts_ = include_gap ? cce->gap_commit_ts_ : 0;
        tuple->cce_addr_.SetCce(reinterpret_cast<uint64_t>(cce), term, ng_id);
    }

    void ScanKey(CcEntry<VoidKey, SkRecord<SkT, PkT>> *cce,
                 remote::ScanTuple_msg *tuple,
                 bool include_gap,
                 int64_t term,
                 uint64_t read_ts,
                 IsolationLevel iso_level) const
    {
        tuple->clear_key();
        std::string &key_blob = *tuple->mutable_key();

        // Serializes the secondary key
        cce->payload_->sk_->Serialize(key_blob);
        // Serializes the primary key
        cce->payload_->pk_->Serialize(key_blob);

        if (iso_level == IsolationLevel::Snapshot)
        {
            VersionRecord<SkRecord<SkT, PkT>> v_rec;
            bool res = cce->MvccGet(read_ts, v_rec);
            if (!res)
            {
                // TODO(lzx): to handle this error.
                assert(res);
            }
            tuple->set_rec_status(remote::ToRemoteType::ConvertRecordStatus(
                v_rec.payload_status_));
            tuple->set_key_ts(v_rec.commit_ts_);
        }
        else
        {
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
        new_cce->payload_ = std::make_shared<SkRecord<SkT, PkT>>();
        new_cce->payload_->sk_ = &sk_it->first;
        new_cce->payload_->pk_ = &pk_it->first;

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

    class Iterator
    {
        using iterator_category = std::bidirectional_iterator_tag;
        using difference_type = std::ptrdiff_t;
        using value_type =
            const std::tuple<const SkT *,
                             const PkT *,
                             CcEntry<VoidKey, SkRecord<SkT, PkT>> *>;
        using pointer = value_type *;    // or also value_type*
        using reference = value_type &;  // or also value_type&

    public:
        Iterator() = default;

        Iterator(
            typename std::map<
                SkT,
                std::map<PkT, CcEntry<VoidKey, SkRecord<SkT, PkT>>>>::iterator
                &sk_map_it,
            CcEntry<VoidKey, SkRecord<SkT, PkT>> *neg_inf_cce)
            : internal_sk_it_(sk_map_it), neg_inf_cce_(neg_inf_cce)
        {
            // When constructing Iterator using a sk_map_it, internal_pk_it_
            // should always point to the last entry of current pk map. Under no
            // circumstances should a scan starts from the beginning of a pk
            // map.
            internal_pk_it_ = std::prev(internal_sk_it_->second.end(), 1);
            UpdateCurrent();
        }

        Iterator(CcEntry<VoidKey, SkRecord<SkT, PkT>> *cce,
                 CcEntry<VoidKey, SkRecord<SkT, PkT>> *neg_inf_cce,
                 CcEntry<VoidKey, SkRecord<SkT, PkT>> *pos_inf_cce = nullptr)
            : neg_inf_cce_(neg_inf_cce)
        {
            // internal_map has at least 1 entry
            std::map<SkT, std::map<PkT, CcEntry<VoidKey, SkRecord<SkT, PkT>>>>
                &internal_map =
                    static_cast<SkCcMap<SkT, PkT> *>(cce->parent_map_)
                        ->sk_index_;

            if (cce == neg_inf_cce)
            {
                std::get<0>(current_) = NegativeInfinity<SkT>::Instance();
                std::get<1>(current_) = NegativeInfinity<PkT>::Instance();
                std::get<2>(current_) = neg_inf_cce_;
                internal_sk_it_ = internal_map.begin();
                internal_pk_it_ = internal_sk_it_->second.begin();
            }
            else if (cce == pos_inf_cce)
            {
                std::get<0>(current_) = PositiveInfinity<SkT>::Instance();
                std::get<1>(current_) = PositiveInfinity<PkT>::Instance();
                std::get<2>(current_) = nullptr;
                internal_sk_it_ = internal_map.end();
                if (!internal_map.empty())
                {
                    internal_pk_it_ =
                        std::prev(internal_map.end(), 1)->second.end();
                }
            }
            else
            {
                internal_sk_it_ = internal_map.find(*cce->payload_->sk_);
                internal_pk_it_ =
                    internal_sk_it_->second.find(*cce->payload_->pk_);
                assert(internal_sk_it_ != internal_map.end());
                assert(internal_pk_it_ != internal_sk_it_->second.end());

                UpdateCurrent();
            }
        }

        Iterator(Iterator &&rhs)
            : internal_sk_it_(rhs.internal_sk_it_),
              internal_pk_it_(rhs.internal_pk_it_),
              current_(rhs.current_),
              neg_inf_cce_(rhs.neg_inf_cce_)
        {
        }

        Iterator(const Iterator &rhs)
            : internal_sk_it_(rhs.internal_sk_it_),
              internal_pk_it_(rhs.internal_pk_it_),
              current_(rhs.current_),
              neg_inf_cce_(rhs.neg_inf_cce_)
        {
        }

        Iterator &operator=(const Iterator &rhs)
        {
            internal_sk_it_ = rhs.internal_sk_it_;
            internal_pk_it_ = rhs.internal_pk_it_;
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
            if (std::get<0>(current_) == NegativeInfinity<SkT>::Instance())
            {
                // The iterator points to negative infinity. Increments the
                // iterator to the first entry in the map, if the map is not
                // empty.

                std::map<SkT,
                         std::map<PkT, CcEntry<VoidKey, SkRecord<SkT, PkT>>>>
                    &internal_map = static_cast<SkCcMap<SkT, PkT> *>(
                                        neg_inf_cce_->parent_map_)
                                        ->sk_index_;

                // move from neg_inf_cce_ to internal_map.begin()
                internal_sk_it_ = internal_map.begin();

                if (internal_sk_it_ != internal_map.end())
                {
                    internal_pk_it_ = internal_sk_it_->second.begin();
                    UpdateCurrent();
                }
                else
                {
                    // The map is empty. The next entry of negative infinity is
                    // positive infinity.
                    std::get<0>(current_) = PositiveInfinity<SkT>::Instance();
                    std::get<1>(current_) = PositiveInfinity<PkT>::Instance();
                    std::get<2>(current_) = nullptr;
                }
            }
            else if (std::get<0>(current_) != PositiveInfinity<SkT>::Instance())
            {
                std::map<SkT,
                         std::map<PkT, CcEntry<VoidKey, SkRecord<SkT, PkT>>>>
                    &internal_map = static_cast<SkCcMap<SkT, PkT> *>(
                                        std::get<2>(current_)->parent_map_)
                                        ->sk_index_;

                ++internal_pk_it_;
                if (internal_pk_it_ != internal_sk_it_->second.end())
                {
                    UpdateCurrent();
                }
                else
                {
                    // The pk_it points to the end of current pk map.
                    ++internal_sk_it_;

                    if (internal_sk_it_ == internal_map.end())
                    {
                        std::get<0>(current_) =
                            PositiveInfinity<SkT>::Instance();
                        std::get<1>(current_) =
                            PositiveInfinity<PkT>::Instance();
                        std::get<2>(current_) = nullptr;
                    }
                    else
                    {
                        internal_pk_it_ = internal_sk_it_->second.begin();
                        UpdateCurrent();
                    }
                }
            }

            // If the current points to positive infinity, keeps the
            // iterator unchanged.
            return *this;
        }

        // Prefix decrement
        Iterator &operator--()
        {
            if (std::get<0>(current_) == PositiveInfinity<SkT>::Instance())
            {
                // The sk_it points to positive infinity. Decrements the
                // iterator to the last entry in the map, if the map is not
                // empty.

                std::map<SkT,
                         std::map<PkT, CcEntry<VoidKey, SkRecord<SkT, PkT>>>>
                    &internal_map = static_cast<SkCcMap<SkT, PkT> *>(
                                        neg_inf_cce_->parent_map_)
                                        ->sk_index_;

                internal_sk_it_ = internal_map.end();

                if (internal_sk_it_ != internal_map.begin())
                {
                    // Sk map is not empty
                    --internal_sk_it_;
                    internal_pk_it_ =
                        std::prev(internal_sk_it_->second.end(), 1);
                    UpdateCurrent();
                }
                else
                {
                    // Sk map is empty. The prior entry of positive infinity is
                    // negative infinity.
                    std::get<0>(current_) = NegativeInfinity<SkT>::Instance();
                    std::get<1>(current_) = NegativeInfinity<PkT>::Instance();
                    std::get<2>(current_) = neg_inf_cce_;
                }
            }
            else if (std::get<0>(current_) != NegativeInfinity<SkT>::Instance())
            {
                std::map<SkT,
                         std::map<PkT, CcEntry<VoidKey, SkRecord<SkT, PkT>>>>
                    &internal_map = static_cast<SkCcMap<SkT, PkT> *>(
                                        std::get<2>(current_)->parent_map_)
                                        ->sk_index_;

                if (internal_sk_it_ == internal_map.begin())
                {
                    if (internal_pk_it_ != internal_sk_it_->second.begin())
                    {
                        --internal_pk_it_;
                        UpdateCurrent();
                    }
                    else
                    {
                        // If the current sk_it points to the beginning of the
                        // map, the prior entry is negative infinity.
                        std::get<0>(current_) =
                            NegativeInfinity<SkT>::Instance();
                        std::get<1>(current_) =
                            NegativeInfinity<PkT>::Instance();
                        std::get<2>(current_) = neg_inf_cce_;
                    }
                }
                else
                {
                    if (internal_pk_it_ != internal_sk_it_->second.begin())
                    {
                        --internal_pk_it_;
                        UpdateCurrent();
                    }
                    else
                    {
                        // If the current pk_it points to the beginning of the
                        // pk map, decrement internal_sk_it_ and let pk_it
                        // points to the last item of current pk map.
                        --internal_sk_it_;
                        internal_pk_it_ =
                            std::prev(internal_sk_it_->second.end(), 1);
                        UpdateCurrent();
                    }
                }
            }

            // If the current sk_it points to negative infinity, keeps the
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
            // The two iterators are equal, if they point to the same cc entry.
            // Note that when the iterator points to positive infinity, the
            // pointed cc entry is null.
            return std::get<2>(lhs.current_) == std::get<2>(rhs.current_);
        };

        friend bool operator!=(const Iterator &lhs, const Iterator &rhs)
        {
            return std::get<2>(lhs.current_) != std::get<2>(rhs.current_);
        };

    private:
        void UpdateCurrent()
        {
            std::get<0>(current_) = &internal_sk_it_->first;
            std::get<1>(current_) = &internal_pk_it_->first;
            std::get<2>(current_) = &internal_pk_it_->second;
        }

        typename std::map<SkT,
                          std::map<PkT, CcEntry<VoidKey, SkRecord<SkT, PkT>>>>::
            iterator internal_sk_it_;
        typename std::map<PkT, CcEntry<VoidKey, SkRecord<SkT, PkT>>>::iterator
            internal_pk_it_;
        std::tuple<const SkT *,
                   const PkT *,
                   CcEntry<VoidKey, SkRecord<SkT, PkT>> *>
            current_{nullptr, nullptr, nullptr};
        CcEntry<VoidKey, SkRecord<SkT, PkT>> *neg_inf_cce_{nullptr};
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

    /**
     * @brief Searches the start cc entry of a forward scan.
     *
     * @param key Search key
     * @param inclusive Whether or not the start key is included in the scan
     * @return std::pair<typename std::map<KeyT, CcEntry<KeyT,
     * ValueT>>::const_iterator, ScanType> A pair of a forward map iterator
     * starting from the start cc entry and whether the scan includes the start
     * cc entry's key or gap or both.
     */
    std::pair<Iterator, ScanType> FowardScanStart(const SkT &key,
                                                  bool inclusive)
    {
        if (key.Type() == KeyType::NegativeInf)
        {
            return std::make_pair(Begin(), ScanType::ScanGap);
        }

        // The key equal to or greater than the search key.
        auto sk_lower_it = sk_index_.lower_bound(key);

        if (sk_lower_it == sk_index_.end())
        {
            if (sk_index_.empty())
            {
                return std::make_pair(Begin(), ScanType::ScanGap);
            }
            else
            {
                // sk_lower_it must be pointing to the end of the map. The start
                // entry is the last in the map, only including the gap.
                --sk_lower_it;
                return std::make_pair(Iterator(sk_lower_it, &neg_inf_),
                                      ScanType::ScanGap);
            }
        }

        if (sk_lower_it->first == key)
        {
            // The search key may match more than one cc entry. Even though
            // each cc map's key is unique, this is possible when the search
            // key is a prefix of a compound key. For example, the cc map's
            // keys are two-field keys (10, 'a'), (20, 'b'), (20, 'c'),
            // (30,'d'), and the search condition is 20: WEHRE pk >= 20 or WHERE
            // pk > 20. The search key is considered equal to both (20, 'b') and
            // (20, 'c').
            if (inclusive)
            {
                // WEHRE pk >= 20. The start entry is the entry before lower
                // bound, i.e., (10, 'a'), including the gap, which may contain
                // (20, 'a').

                if (sk_lower_it == sk_index_.begin())
                {
                    return std::make_pair(Begin(), ScanType::ScanGap);
                }
                else
                {
                    --sk_lower_it;
                    return std::make_pair(Iterator(sk_lower_it, &neg_inf_),
                                          ScanType::ScanGap);
                }
            }
            else
            {
                auto next_it = std::next(sk_lower_it, 1);
                if (next_it != sk_index_.end() && next_it->first == key)
                {
                    // The search key matches more than one entry, e.g., WEHRE
                    // pk > 20. The start entry is the end of the repeated
                    // entries, i.e., (20, 'c').

                    // The key greater than the search key, i.e., (30, 'd').
                    auto sk_upper_it = sk_index_.upper_bound(key);

                    // The start entry is the one prior to (30, 'd'), including
                    // the gap but not the key.
                    --sk_upper_it;
                    return std::make_pair(Iterator(sk_upper_it, &neg_inf_),
                                          ScanType::ScanGap);
                }
                else
                {
                    // The search key matches only one entry.
                    return std::make_pair(Iterator(sk_lower_it, &neg_inf_),
                                          ScanType::ScanGap);
                }
            }
        }
        else
        {
            // The search key falls into a gap between two existing keys. The
            // start entry precedes the lower bound, excluding the key.
            if (sk_lower_it == sk_index_.begin())
            {
                return std::make_pair(Begin(), ScanType::ScanGap);
            }
            else
            {
                --sk_lower_it;
                return std::make_pair(Iterator(sk_lower_it, &neg_inf_),
                                      ScanType::ScanGap);
            }
        }
    }

    std::pair<Iterator, ScanType> BackwardScanStart(const SkT &key,
                                                    bool inclusive)
    {
        if (key.Type() == KeyType::PostiveInf)
        {
            auto start_it = End();
            --start_it;
            return std::make_pair(start_it, ScanType::ScanBoth);
        }

        // The key equal to or greater than the search key.
        auto sk_lower_it = sk_index_.lower_bound(key);

        if (sk_lower_it == sk_index_.end())
        {
            if (sk_index_.empty())
            {
                return std::make_pair(Begin(), ScanType::ScanGap);
            }
            else
            {
                --sk_lower_it;
                return std::make_pair(Iterator(sk_lower_it, &neg_inf_),
                                      ScanType::ScanBoth);
            }
        }

        if (sk_lower_it->first == key)
        {
            // The search key may match more than one cc entry. Even though
            // each cc map's key is unique, this is possible when the search
            // key is a prefix of a compound key. For example, the cc map's
            // keys are two-field keys (10, 'a'), (20, 'b'), (20, 'c'),
            // (30,'d'), and the search condition is 20: WEHRE pk <= 20 or WHERE
            // pk < 20. The search key is considered equal to both (20, 'b') and
            // (20, 'c').

            if (inclusive)
            {
                auto next_it = std::next(sk_lower_it, 1);
                if (next_it != sk_index_.end() && next_it->first == key)
                {
                    // The search key matches more than one entry, e.g., WEHRE
                    // pk <= 20. The start entry is the end of the repeated
                    // entries, i.e., (20, 'c'), including the key and the gap
                    // (gap may have entry (20, 'd')).

                    auto sk_upper_it = sk_index_.upper_bound(key);
                    --sk_upper_it;
                    return std::make_pair(Iterator(sk_upper_it, &neg_inf_),
                                          ScanType::ScanBoth);
                }
                else
                {
                    // WHERE pk <= 10.
                    return std::make_pair(Iterator(sk_lower_it, &neg_inf_),
                                          ScanType::ScanBoth);
                }
            }
            else
            {
                // WHERE pk < 10. The start entry precedes the lower bound.
                if (sk_lower_it == sk_index_.begin())
                {
                    return std::make_pair(Begin(), ScanType::ScanGap);
                }
                else
                {
                    --sk_lower_it;
                    return std::make_pair(Iterator(sk_lower_it, &neg_inf_),
                                          ScanType::ScanBoth);
                }
            }
        }
        else
        {
            // The search key falls into a gap between two existing keys. The
            // start entry precedes the lower bound, including the key and the
            // gap.

            if (sk_lower_it == sk_index_.begin())
            {
                return std::make_pair(Begin(), ScanType::ScanGap);
            }
            else
            {
                --sk_lower_it;
                return std::make_pair(Iterator(sk_lower_it, &neg_inf_),
                                      ScanType::ScanBoth);
            }
        }
    }

    std::map<SkT, std::map<PkT, CcEntry<VoidKey, SkRecord<SkT, PkT>>>>
        sk_index_;
    CcEntry<VoidKey, SkRecord<SkT, PkT>> neg_inf_, pos_inf_;
    const SecondaryKeySchema compound_schema_;
};
}  // namespace txservice
