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

    bool Execute(PostWriteAllCc &req) override
    {
        RangeBucketRecord *upload_bucket_rec = nullptr;
        const RangeBucketKey *target_key = nullptr;

        if (req.Key() != nullptr)
        {
            // Local request
            upload_bucket_rec = static_cast<RangeBucketRecord *>(req.Payload());
            target_key = static_cast<const RangeBucketKey *>(req.Key());
        }
        else
        {
            // Request comes from a remote node and is processed for the first
            // time. Deserialize the keys and payloads.
            assert(*req.KeyStrType() == KeyType::Normal);

            const std::string *key_str = req.KeyStr();
            assert(key_str != nullptr);
            std::unique_ptr<RangeBucketKey> decoded_key =
                std::make_unique<RangeBucketKey>();
            size_t offset = 0;
            decoded_key->Deserialize(key_str->data(), offset, KeySchema());
            target_key = decoded_key.get();
            req.SetDecodedKey(std::move(decoded_key));
            assert(req.PayloadStr() != nullptr);
            std::unique_ptr<RangeBucketRecord> decoded_rec =
                std::make_unique<RangeBucketRecord>();
            offset = 0;
            decoded_rec->Deserialize(req.PayloadStr()->data(), offset);
            upload_bucket_rec = decoded_rec.get();
            req.SetDecodedPayload(std::move(decoded_rec));
        }

        CcEntry<RangeBucketKey, RangeBucketRecord> *cce =
            Find(*target_key).second;
        assert(cce != nullptr);

        // Check whether cce key lock holder is the given tx of the
        // PostWriteAllCc before apply change.
        if (cce->key_lock_ptr_ == nullptr ||
            !cce->key_lock_ptr_->HasWrite(req.Txn()))
        {
            // Check if the tx still has lock on this cce. If this
            // is a duplicate post write all req or this ng has already
            // failed over and has released the lock during recovery, skip
            // processing this request.
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

        if (req.CommitType() == PostWriteType::PrepareCommit)
        {
            if (shard_->core_id_ == 0)
            {
                // Update bucket info in local cc shards. Upload dirty bucket
                // owner.
                upload_bucket_rec->SetBucketInfo(
                    shard_->local_shards_.UploadNewBucketInfo(
                        this->cc_ng_id_,
                        target_key->bucket_id_,
                        upload_bucket_rec->GetBucketInfo()->DirtyBucketOwner(),
                        upload_bucket_rec->GetBucketInfo()->DirtyVersion()));
            }
        }
        else if (req.CommitType() == PostWriteType::PostCommit ||
                 req.CommitType() == PostWriteType::Commit)
        {
            if (shard_->core_id_ == 0)
            {
                // Commit dirty bucket info in local cc shards.
                const BucketInfo *bucket_info = shard_->GetBucketInfo(
                    target_key->bucket_id_, this->cc_ng_id_);
                assert(bucket_info != nullptr);
                if (req.CommitType() == PostWriteType::PostCommit &&
                    bucket_info->DirtyVersion() > 0)
                {
                    // First time processing post write all. Commit the dirty
                    // version and drop store ranges if is old bucket owner.
                    NodeGroupId orig_owner = bucket_info->BucketOwner();
                    // Commit the dirty bucket info first so that store range
                    // can be loaded into memory on the dirty bucket owner ng.
                    bucket_info = shard_->local_shards_.CommitDirtyBucketInfo(
                        this->cc_ng_id_, target_key->bucket_id_);
                    if (this->cc_ng_id_ == orig_owner)
                    {
                        // If this ng is the original owner of the bucket,
                        // drop store ranges in this bucket since they are
                        // now migrated to other ng. Do this after dirty
                        // bucket info is committed to make sure all ranges
                        // are removed.
                        shard_->local_shards_.DropStoreRangesInBucket(
                            this->cc_ng_id_, target_key->bucket_id_);
                    }
                }
                else if (req.CommitType() == PostWriteType::Commit &&
                         bucket_info->Version() <
                             upload_bucket_rec->GetBucketInfo()->Version())
                {
                    // We skipped prepare commit, so there's no dirty bucket
                    // info. upload new bucket info directly.
                    bucket_info = shard_->local_shards_.UploadBucketInfo(
                        this->cc_ng_id_,
                        target_key->bucket_id_,
                        upload_bucket_rec->GetBucketInfo()->BucketOwner(),
                        upload_bucket_rec->GetBucketInfo()->Version());
                    if (this->cc_ng_id_ == bucket_info->BucketOwner())
                    {
                        // If this ng is the original owner of the bucket, drop
                        // store ranges in this bucket since they are now
                        // migrated to other ng.
                        shard_->local_shards_.DropStoreRangesInBucket(
                            this->cc_ng_id_, target_key->bucket_id_);
                    }
                }

                // The dirty version is already committed. If this ng is the
                // current owner of bucket, load store ranges into memory.
                if (this->cc_ng_id_ == bucket_info->BucketOwner())
                {
                    // Load store range if this ng is the new owner of the
                    // bucket.
                    int64_t term =
                        Sharder::Instance().LeaderTerm(this->cc_ng_id_);
                    if (!shard_->local_shards_.LoadStoreRangesInBucket(
                            this->cc_ng_id_,
                            target_key->bucket_id_,
                            shard_,
                            &req,
                            term))
                    {
                        return false;
                    }
                }
                upload_bucket_rec->SetBucketInfo(bucket_info);
            }
        }

        return TemplateCcMap::Execute(req);
    }

    bool Execute(ReplayLogCc &req) override
    {
        const std::string_view &content = req.LogContentView();
        ::txlog::ClusterScaleOpMessage scale_op_msg;
        scale_op_msg.ParseFromArray(content.data(), content.length());

        // Restore bucket info
        auto &migrate_process =
            scale_op_msg.node_group_bucket_migrate_process();
        auto bucket_map = shard_->GetAllBucketInfos(req.NodeGroupId());
        for (auto &[ng_id, ng_process] : migrate_process)
        {
            for (auto &[bucket_id, bucket_process] :
                 ng_process.bucket_migrate_process())
            {
                if (shard_->core_id_ == 0)
                {
                    BucketInfo *info = bucket_map->at(bucket_id).get();
                    // Update bucket owner on the first core
                    switch (bucket_process.stage())
                    {
                    case ::txlog::BucketMigrateMessage_Stage::
                        BucketMigrateMessage_Stage_NotStarted:
                    {
                        info->bucket_owner_ = bucket_process.old_owner();
                        break;
                    }
                    case ::txlog::BucketMigrateMessage_Stage::
                        BucketMigrateMessage_Stage_PrepareMigrate:
                    {
                        // TODO
                        assert(false);
                        info->bucket_owner_ = bucket_process.old_owner();
                        assert(bucket_process.migrate_ts() > info->Version());
                        info->SetDirty(bucket_process.new_owner(),
                                       bucket_process.migrate_ts());
                        break;
                    }
                    case ::txlog::BucketMigrateMessage_Stage::
                        BucketMigrateMessage_Stage_CommitMigrate:
                    {
                        // TODO
                        assert(false);
                        info->Set(bucket_process.new_owner(),
                                  bucket_process.migrate_ts());
                        break;
                    }
                    case ::txlog::BucketMigrateMessage_Stage::
                        BucketMigrateMessage_Stage_CleanMigrate:
                    {
                        // no op
                        assert(false);
                        break;
                    }
                    default:
                    {
                        assert(false);
                    }
                    }
                }
            }
        }

        req.SetFinish();
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