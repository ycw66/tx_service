#pragma once

#include "cc_request.h"
#include "range_bucket_key_record.h"
#include "template_cc_map.h"

namespace txservice
{
class RangeBucketCcMap : public TemplateCcMap<RangeBucketKey, RangeBucketRecord>
{
public:
    RangeBucketCcMap(const RangeBucketCcMap &rhs) = delete;
    ~RangeBucketCcMap() = default;

    using TemplateCcMap<RangeBucketKey, RangeBucketRecord>::Execute;
    using TemplateCcMap<RangeBucketKey, RangeBucketRecord>::FindEmplace;
    using TemplateCcMap<RangeBucketKey, RangeBucketRecord>::Emplace;
    using TemplateCcMap<RangeBucketKey, RangeBucketRecord>::Find;
    using TemplateCcMap<RangeBucketKey, RangeBucketRecord>::AcquireCceKeyLock;
    using TemplateCcMap<RangeBucketKey,
                        RangeBucketRecord>::LockHandleForResumedRequest;

    RangeBucketCcMap(CcShard *shard,
                     NodeGroupId cc_ng_id,
                     const TableName &table_name)
        : TemplateCcMap<RangeBucketKey, RangeBucketRecord>(
              shard, cc_ng_id, table_name, 1, nullptr, true)
    {
        auto bucket_map = shard->GetAllBucketInfos(cc_ng_id);
        assert(bucket_map != nullptr);
        for (auto &bucket : *bucket_map)
        {
            RangeBucketKey bucket_key(bucket.first);
            auto cce_it = FindEmplace(bucket_key);
            CcEntry<RangeBucketKey, RangeBucketRecord> *cce = cce_it->second;
            cce->commit_ts_ = bucket.second->Version();
            cce->payload_ =
                std::make_shared<RangeBucketRecord>(bucket.second.get());
            shard_->mem_usage_ += cce->PayloadMemUsage();
        }
    }

    bool Execute(ReadCc &req) override
    {
        TX_TRACE_ACTION_WITH_CONTEXT(
            (txservice::CcMap *) this,
            &req,
            [&req]() -> std::string
            {
                return std::string("\"cc_map_type\":\"range_bucket_cc_map\"")
                    .append(",\"tx_number\":")
                    .append(std::to_string(req.Txn()))
                    .append(",\"term\":")
                    .append(std::to_string(req.TxTerm()));
            });
        TX_TRACE_DUMP(&req);

        assert(req.IsLocal());
        uint32_t ng_id = req.NodeGroupId();
        int64_t ng_term = Sharder::Instance().LeaderTerm(ng_id);
        if (req.IsInRecovering())
        {
            ng_term = ng_term > 0
                          ? ng_term
                          : Sharder::Instance().CandidateLeaderTerm(ng_id);
        }

        if (ng_term < 0)
        {
            req.Result()->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            return true;
        }

        const RangeBucketKey *bucket_key =
            static_cast<const RangeBucketKey *>(req.Key());

        CcEntry<RangeBucketKey, RangeBucketRecord> *cce =
            Find(*bucket_key).second;
        assert(cce != nullptr);
        auto hd_result = req.Result();
        LockType acquired_lock;
        CcErrorCode err_code;
        if (req.CcePtr() != nullptr && req.CcePtr() == cce)
        {
            // The request was blocked before. This is execution resumption
            // after the request is unblocked. The read lock/intention must have
            // been acquired.
            CcOperation cc_op = req.IsForWrite() ? CcOperation::ReadForWrite
                                                 : CcOperation::Read;
            std::tie(acquired_lock, err_code) =
                LockHandleForResumedRequest(cce,
                                            cce->payload_status_,
                                            &req,
                                            req.NodeGroupId(),
                                            ng_term,
                                            req.TxTerm(),
                                            cc_op,
                                            req.Isolation(),
                                            req.Protocol(),
                                            req.ReadTimestamp(),
                                            false);
        }
        else
        {
            // try to acquire lock
            int64_t tx_term = req.TxTerm();
            uint32_t ng_id = req.NodeGroupId();
            IsolationLevel iso_lvl = req.Isolation();
            CcProtocol cc_proto = req.Protocol();
            CcOperation cc_op = req.IsForWrite() ? CcOperation::ReadForWrite
                                                 : CcOperation::Read;
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
                                  false);
        }

        // after acquiring lock
        switch (err_code)
        {
        case CcErrorCode::NO_ERROR:
        {
            CcEntryAddr &cce_addr = hd_result->Value().cce_addr_;
            cce_addr.SetCce(reinterpret_cast<uint64_t>(cce),
                            ng_term,
                            req.NodeGroupId(),
                            shard_->LocalCoreId());

            RangeBucketRecord *bucket_rec =
                static_cast<RangeBucketRecord *>(req.Record());
            *bucket_rec = *(cce->payload_);
            hd_result->Value().ts_ = cce->commit_ts_;
            hd_result->Value().rec_status_ = RecordStatus::Normal;
            hd_result->Value().lock_type_ = acquired_lock;
            hd_result->SetFinished();
            return true;
        }
        case CcErrorCode::ACQUIRE_LOCK_BLOCKED:
        {
            // You don't need a remote acknowledge here, since bucket owner read
            // is a local read anyway. Set CcePtr to indicate this is a resumed
            // req.
            req.SetCcePtr(cce);
            return false;
        }
        default:
        {
            return true;
        }
        }  //-- end: switch

        return true;
    }

    // Get bucket record of bucket id in cc map. This is used
    // when linking range record and range owner bucket record. It
    // should not be used for reading bucket record value.
    LruEntry *GetBucketRecord(uint16_t bucket_id)
    {
        RangeBucketKey bucket_key(bucket_id);
        return Find(bucket_key).second;
    }
};
}  // namespace txservice