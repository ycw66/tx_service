#pragma once

#include <map>
#include <memory>
#include <string>
#include <utility>

#include "cc_entry.h"
#include "cc_handler_result.h"
#include "cc_request.h"
#include "error_messages.h"  //CcErrorCode
#include "range_bucket_cc_map.h"
#include "range_record.h"
#include "statistics.h"
#include "template_cc_map.h"
#include "tx_operation.h"
#include "tx_serialize.h"

namespace txservice
{
/**
 * @brief A range cc map is a special cc map that maps a table's sorted ranges
 * into their partition IDs. Each range is represented by the start key of the
 * range. When the table is newly created, the table has only one range, from
 * negative infinity to positive infinity, i.e., [neg_inf, pos_inf). The range
 * starting from negative infinity has a reserved partition ID: 0. Range
 * [pos_inf, pos_inf) is a special (non-existent) range, whose partition ID is
 * UINT32_MAX. So, the first range's partition ID is 0 and its next range's
 * partition ID is UINT32_MAX. When [neg_inf, pos_inf) is (evenly) split into
 * two sub-ranges [neg_inf, mid_key), [mid_key, pos_inf), the first sub-range
 * inherits the original range's partition ID, i.e., the partition ID of
 * [neg_inf, mid_key) is 0. The second sub-range's partition ID is designated as
 * the half of the original range's partition ID and that of the next range,
 * i.e., the partition ID of [mid_key, pos_inf) is (0 + UINT32_MAX) / 2.
 *
 * @tparam KeyT The type of the table's primary key.
 */
template <typename KeyT>
class RangeCcMap : public TemplateCcMap<KeyT, RangeRecord>
{
public:
    RangeCcMap(const RangeCcMap &rhs) = delete;

    using TemplateCcMap<KeyT, RangeRecord>::Execute;
    using TemplateCcMap<KeyT, RangeRecord>::FindEmplace;
    using TemplateCcMap<KeyT, RangeRecord>::Emplace;
    using TemplateCcMap<KeyT, RangeRecord>::AcquireCceKeyLock;
    using TemplateCcMap<KeyT, RangeRecord>::ReleaseCceKeyLock;
    using TemplateCcMap<KeyT, RangeRecord>::LockHandleForResumedRequest;
    using TemplateCcMap<KeyT, RangeRecord>::MoveRequest;
    using TemplateCcMap<KeyT, RangeRecord>::shard_;
    using TemplateCcMap<KeyT, RangeRecord>::Floor;
    using TemplateCcMap<KeyT, RangeRecord>::neg_inf_;
    using TemplateCcMap<KeyT, RangeRecord>::pos_inf_;
    using TemplateCcMap<KeyT, RangeRecord>::table_schema_;
    using TemplateCcMap<KeyT, RangeRecord>::KeySchema;
    using TemplateCcMap<KeyT, RangeRecord>::Find;
    using TemplateCcMap<KeyT, RangeRecord>::Begin;
    using TemplateCcMap<KeyT, RangeRecord>::End;

    /**
     * @brief Construct a new range cc map object. The range cc map has no
     * schema, so the schema's timestamp is set to 1 (the beginning of history).
     *
     * @param range_table_name
     * @param shard
     */
    RangeCcMap(const TableName &range_table_name,
               const txservice::TableSchema *table_schema,
               uint64_t schema_ts,
               CcShard *shard,
               NodeGroupId ng_id)
        : TemplateCcMap<KeyT, RangeRecord>(
              shard, ng_id, range_table_name, schema_ts, table_schema, false)
    {
        auto ranges =
            CcMap::shard_->GetTableRangesForATable(range_table_name, ng_id);
        assert(ranges != nullptr);
        neg_inf_.payload_ = std::make_shared<RangeRecord>();
        pos_inf_.payload_ = std::make_shared<RangeRecord>();
        auto bucket_map = static_cast<RangeBucketCcMap *>(
            shard->GetCcm(range_bucket_ccm_name, ng_id));

        for (auto &[key, table_range] : *ranges)
        {
            const RangeInfo *range_info = table_range.GetRangeInfo();
            const KeyT *start_key = static_cast<const KeyT *>(key);
            if (start_key->Type() == KeyType::NegativeInf)
            {
                neg_inf_.payload_->range_info_ = range_info;
                neg_inf_.commit_ts_ = range_info->version_ts_;
                neg_inf_.payload_status_ = RecordStatus::Normal;
                neg_inf_.payload_->range_owner_rec_ =
                    bucket_map->GetBucketRecord(Sharder::MapRangeIdToBucketId(
                        range_info->PartitionId()));
            }
            else
            {
                auto it =
                    TemplateCcMap<KeyT, RangeRecord>::FindEmplace(*start_key);
                CcEntry<KeyT, RangeRecord> *cce = it->second;
                cce->commit_ts_ = range_info->version_ts_;
                cce->payload_ = std::make_shared<RangeRecord>();
                cce->payload_->range_info_ = range_info;
                cce->payload_->range_owner_rec_ = bucket_map->GetBucketRecord(
                    Sharder::MapRangeIdToBucketId(range_info->PartitionId()));
                cce->payload_status_ = RecordStatus::Normal;
                shard_->mem_usage_ += cce->PayloadMemUsage();
                it--;
                it->second->payload_->end_key_ = range_info->start_key_.get();
            }
        }
    }

    ~RangeCcMap()
    {
        // Clean up bucket record locks if range record has read lock on it.
        // This happens with drop table, in which case we remove all read entry
        // related with the dropped table from readset and directly drop the
        // range cc map. In this case we need to manually clear the bucket locks
        // since postread will not be called.
        for (auto it = Begin(); it != End(); it++)
        {
            auto range_cce = it->second;
            if (range_cce->key_lock_ptr_ != nullptr &&
                !range_cce->key_lock_ptr_->ReadLocks().empty())
            {
                for (TxNumber txn : range_cce->key_lock_ptr_->ReadLocks())
                {
                    auto bucket_cce = static_cast<
                        CcEntry<RangeBucketKey, RangeBucketRecord> *>(
                        range_cce->payload_->range_owner_rec_);
                    ReleaseCceKeyLock(bucket_cce, txn, this->cc_ng_id_);
                }
            }
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
        req.is_include_floor_cce_ = true;
        return TemplateCcMap<KeyT, RangeRecord>::Execute(req);
    }

    /**
     * @brief A read request toward the range cc map searches a range containing
     * the input key, adds a read lock on the range and returns a pointer to the
     * table range entry in the returned record. The table range entry gives the
     * range's partition ID and the dirty range's partition ID, if the range is
     * being split or merged. It will also put a read lock on the range owner
     * bucket record, which will be released on range record post read. The
     * range owner bucket record is used to check which node group is holding
     * the data of this range.
     *
     * @param req The read request containing the input key and returned record.
     * @return true, if the request has been executed and is to be freed; false,
     * if the request blocked and should not be freed.
     */
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

        assert(req.IsLocal());
        assert(this->table_name_ == *req.GetTableName());

        CcHandlerResult<ReadKeyResult> *hd_result = req.Result();
        int64_t ng_term = Sharder::Instance().LeaderTerm(req.NodeGroupId());
        if (ng_term < 0)
        {
            hd_result->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            return true;
        }

        // For range cc maps, we assume that all of a table's ranges are loaded
        // into memory for caching when the range cc map is initialized. There
        // is never a read-outside request that brings an individual range into
        // memory for caching.
        assert(req.Type() != ReadType::OutsideNormal);

        CcEntry<KeyT, RangeRecord> *floor_cce = nullptr;

        LockType acquired_lock;
        CcErrorCode err_code;
        if (req.IsWaitForBucketRecordRead())
        {
            assert(req.CcePtr() != nullptr);
            // If we're waiting for bucket record, that means we must've alrady
            // acquired read lock on range record.
            floor_cce = static_cast<CcEntry<KeyT, RangeRecord> *>(req.CcePtr());
            CcEntry<RangeBucketKey, RangeBucketRecord> *bucket_cce =
                static_cast<CcEntry<RangeBucketKey, RangeBucketRecord> *>(
                    floor_cce->payload_->range_owner_rec_);
            std::tie(acquired_lock, err_code) =
                LockHandleForResumedRequest(bucket_cce,
                                            bucket_cce->payload_status_,
                                            &req,
                                            req.NodeGroupId(),
                                            ng_term,
                                            req.TxTerm(),
                                            CcOperation::Read,
                                            IsolationLevel::RepeatableRead,
                                            CcProtocol::Locking,
                                            req.ReadTimestamp(),
                                            false);
            // Lock handle should always succeed here. It will only fail if
            // the entry is deleted, but bucket record will never be
            // deleted.
            assert(err_code == CcErrorCode::NO_ERROR);
            CcEntryAddr &cce_addr = hd_result->Value().cce_addr_;
            cce_addr.SetCce(reinterpret_cast<uint64_t>(floor_cce),
                            ng_term,
                            req.NodeGroupId(),
                            shard_->LocalCoreId());
            RangeRecord *range_rec = static_cast<RangeRecord *>(req.Record());
            range_rec->CopyForReadResult(*(floor_cce->payload_));
            hd_result->Value().ts_ = floor_cce->commit_ts_;
            hd_result->Value().rec_status_ = RecordStatus::Normal;
            hd_result->Value().lock_type_ = acquired_lock;
            hd_result->SetFinished();
            return true;
        }
        // Rather than looking for an exact match, looks up the floor key
        // that represents the range containing the input key.
        const KeyT *look_key = static_cast<const KeyT *>(req.Key());

        auto it = Floor(*look_key);
        floor_cce = it->second;
        if (req.CcePtr() != nullptr && req.CcePtr() == floor_cce)
        {
            // The request was blocked before. This is execution resumption
            // after the request is unblocked. The read lock/intention must have
            // been acquired.

            // If the searching key is still in the same range, we don't
            // need to reacquire the key.
            CcOperation cc_op = req.IsForWrite() ? CcOperation::ReadForWrite
                                                 : CcOperation::Read;
            std::tie(acquired_lock, err_code) =
                LockHandleForResumedRequest(floor_cce,
                                            floor_cce->payload_status_,
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
            if (req.CcePtr() != nullptr)
            {
                // This is a resumed cc request but the searching key now falls
                // into a new range, release the lock on old range.
                CcEntry<KeyT, RangeRecord> *prev_cce =
                    static_cast<CcEntry<KeyT, RangeRecord> *>(req.CcePtr());
                prev_cce->key_lock_ptr_->ReleaseLock(
                    req.Txn(), shard_, LockType::ReadLock);
                // If we're waiting for bucekt lock that means we've alraedy
                // acquired read lock on range record, so the record cannot be
                // updated during this time.
                assert(!req.IsWaitForBucketRecordRead());
            }

            // Set CcePtr to indicate this is a resumed req.
            req.SetCcePtr(floor_cce);
            // try to acquire lock
            int64_t tx_term = req.TxTerm();
            uint32_t ng_id = req.NodeGroupId();
            IsolationLevel iso_lvl = req.Isolation();
            CcProtocol cc_proto = req.Protocol();
            CcOperation cc_op = req.IsForWrite() ? CcOperation::ReadForWrite
                                                 : CcOperation::Read;
            std::tie(acquired_lock, err_code) =
                AcquireCceKeyLock(floor_cce,
                                  floor_cce->payload_status_,
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
            // Lock range owner bucket
            auto bucket_cce =
                static_cast<CcEntry<RangeBucketKey, RangeBucketRecord> *>(
                    floor_cce->payload_->range_owner_rec_);
            assert(bucket_cce != nullptr);
            std::tie(acquired_lock, err_code) =
                AcquireCceKeyLock(bucket_cce,
                                  bucket_cce->payload_status_,
                                  &req,
                                  req.NodeGroupId(),
                                  ng_term,
                                  req.TxTerm(),
                                  CcOperation::Read,
                                  IsolationLevel::RepeatableRead,
                                  CcProtocol::Locking,
                                  req.ReadTimestamp(),
                                  true);
            if (err_code == CcErrorCode::ACQUIRE_LOCK_BLOCKED)
            {
                req.SetIsWaitForBucketRecordRead(true);
                return false;
            }
            CcEntryAddr &cce_addr = hd_result->Value().cce_addr_;
            cce_addr.SetCce(reinterpret_cast<uint64_t>(floor_cce),
                            ng_term,
                            req.NodeGroupId(),
                            shard_->LocalCoreId());
            RangeRecord *range_rec = static_cast<RangeRecord *>(req.Record());
            range_rec->CopyForReadResult(*(floor_cce->payload_));
            hd_result->Value().ts_ = floor_cce->commit_ts_;
            hd_result->Value().rec_status_ = RecordStatus::Normal;
            hd_result->Value().lock_type_ = acquired_lock;
            hd_result->SetFinished();
            return true;
        }
        case CcErrorCode::ACQUIRE_LOCK_BLOCKED:
        {
            // You don't need a remote acknowledge here, since range read is
            // a local read anyway
            return false;
        }
        default:
        {
            return true;
        }
        }  //-- end: switch

        return true;
    }

    bool Execute(PostReadCc &req) override
    {
        const CcEntryAddr &cce_addr = *req.CceAddr();
        CcEntry<KeyT, RangeRecord> &cc_entry =
            *reinterpret_cast<CcEntry<KeyT, RangeRecord> *>(cce_addr.CcePtr());

        // Release bucket record read lock. This lock was acquried in range
        // cc map read cc, and is not put into readset. So we need to be
        // releasing it here manually.
        auto bucket_cce =
            static_cast<CcEntry<RangeBucketKey, RangeBucketRecord> *>(
                cc_entry.payload_->range_owner_rec_);
        bucket_cce->last_read_ts_ =
            std::max(bucket_cce->last_read_ts_, req.CommitTs());
        ReleaseCceKeyLock(bucket_cce, req.Txn(), req.NodeGroupId());

        return TemplateCcMap<KeyT, RangeRecord>::Execute(req);
    }

    bool Execute(AcquireAllCc &req) override
    {
        if (shard_->core_id_ == 0 && req.Key() != nullptr)
        {
            // If this we are the owner of this range, mark the StoreRange as
            // locked. Note that req.Key() is always not null if we're the owner
            // since the owner if always the split range coordinator.
            const KeyT *range_key = static_cast<const KeyT *>(req.Key());
            TableRangeEntry *range_entry = shard_->GetTableRangeEntry(
                this->table_name_, req.NodeGroupId(), range_key);

            StoreRange *range = range_entry->RangeSlices();
            if (range)
            {
                range->Lock();
            }
        }

        return TemplateCcMap<KeyT, RangeRecord>::Execute(req);
    }

    /**
     * @brief
     */
    bool Execute(PostWriteAllCc &req) override
    {
        int64_t ng_term = Sharder::Instance().LeaderTerm(req.NodeGroupId());
        if (ng_term < 0)
        {
            req.Result()->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            return false;
        }

#ifndef ON_KEY_OBJECT
        // Initialize table statistics before split range.
        // If it is in recover range stage, the table statistics hasn't been
        // loaded yet.
        if (shard_->core_id_ == 0)
        {
            TableName base_table_name(this->table_name_.GetBaseTableNameSV(),
                                      TableType::Primary);
            const StatisticsEntry *statistics_entry =
                shard_->GetTableStatistics(base_table_name, this->cc_ng_id_);
            if (statistics_entry == nullptr ||
                statistics_entry->statistics_ == nullptr)
            {
                shard_->FetchTableStatistics(
                    base_table_name, this->cc_ng_id_, &req);
                return false;
            }
        }
#endif

        // When the commit ts is 0, the request commits nothing and only
        // removes the write intents/locks acquired earlier.
        if (req.CommitTs() == TransactionOperation::tx_op_failed_ts_)
        {
            return TemplateCcMap<KeyT, RangeRecord>::Execute(req);
        }

        // Prepare RangeRecord
        RangeRecord *upload_range_rec = nullptr;
        const TxKey *target_key = nullptr;
        std::vector<std::pair<TxKey::Uptr, uint32_t>> range_slices;
        // Place holder for decoded range info if req is remote
        if (req.Key() != nullptr)
        {
            upload_range_rec = static_cast<RangeRecord *>(req.Payload());
            target_key = req.Key();
        }
        else
        {
            // Request comes from a remote node and is processed for the first
            // time. Deserialize the keys and payloads.
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
            assert(req.PayloadStr() != nullptr);
            std::unique_ptr<RangeRecord> decoded_rec =
                std::make_unique<RangeRecord>();
            DeserializeRangeRecord(
                *req.PayloadStr(), decoded_rec.get(), range_slices);
            upload_range_rec = decoded_rec.get();
            req.SetDecodedPayload(std::move(decoded_rec));
        }

        if (req.CommitType() == PostWriteType::PrepareCommit)
        {
            // update the local shards' table range value on the first core.
            if (shard_->core_id_ == 0)
            {
                assert(
                    upload_range_rec->GetRangeInfo()->new_key_.size() ==
                    upload_range_rec->GetRangeInfo()->new_partition_id_.size());

                // upload the new range info to local cc shards,
                // point the req range rec to the local cc shard table_ranges_
                // entry. The req range rec will be used to update ccmap on each
                // core.
                upload_range_rec->SetRangeInfo(
                    shard_
                        ->UploadNewRangeInfo(
                            this->table_name_,
                            this->cc_ng_id_,
                            target_key,
                            upload_range_rec->GetRangeInfo()->new_key_,
                            upload_range_rec->GetRangeInfo()->new_partition_id_,
                            req.CommitTs())
                        ->GetRangeInfo());

                NodeGroupId range_owner =
                    shard_
                        ->GetRangeOwner(
                            upload_range_rec->GetRangeInfo()->PartitionId(),
                            this->cc_ng_id_)
                        ->BucketOwner();
                if (range_owner == this->cc_ng_id_)
                {
                    TableRangeEntry *range_entry = shard_->GetTableRangeEntry(
                        this->table_name_, req.NodeGroupId(), target_key);
                    assert(range_entry->RangeSlices());
                    range_entry->RangeSlices()->Unlock();
                }
                else
                {
                    ACTION_FAULT_INJECTOR(
                        "range_split_participant_prepare_post_all");
                }
            }
            // Register the range owner bucket for the new ranges
            auto bucket_map = static_cast<RangeBucketCcMap *>(
                shard_->GetCcm(range_bucket_ccm_name, this->cc_ng_id_));
            auto target_cce =
                Find(*static_cast<const KeyT *>(req.Key())).second;
            auto new_range_owner_rec =
                std::make_unique<std::vector<LruEntry *>>();
            for (int32_t new_id :
                 upload_range_rec->GetRangeInfo()->new_partition_id_)
            {
                // Link bucket owner record
                new_range_owner_rec->push_back(bucket_map->GetBucketRecord(
                    Sharder::MapRangeIdToBucketId(new_id)));
            }
            upload_range_rec->SetNewRangeOwnerRec(
                std::move(new_range_owner_rec));
            // Reuse range_owner_rec_ from old cce. range_owner_rec_ needs to be
            // reset on each core since they point to bucket records on
            // different cores.
            upload_range_rec->range_owner_rec_ =
                target_cce->payload_->range_owner_rec_;
        }
        else if (req.CommitType() == PostWriteType::PostCommit)
        {
            std::vector<const RangeInfo *> new_range_infos;
            TableRangeEntry *old_entry = shard_->GetTableRangeEntry(
                this->table_name_, req.NodeGroupId(), target_key);
            RangeInfo *old_info = old_entry->range_info_.get();

            if (shard_->core_id_ == 0)
            {
                // Check if the range entry is still dirty.
                // If not, that means the post write has already been executed
                // on this ng. We cannot execute the code below repeatedly,
                // since the code below will install the slice specs received
                // from the post write message. However once the write lock has
                // been removed, the slice specs could already be updated by
                // others(i.e. checkpointer). In this case we shold not
                // overwrite the udpated slice specs with the ones received from
                // the post write message.
                if (!old_info->IsDirty())
                {
                    assert(old_info->VersionTs() >= req.CommitTs());
                    req.Result()->SetFinished();
                    req.SetDecodedPayload(nullptr);
                    return true;
                }
                // Update table_ranges_ in local cc shard on the first core
                // Do not trust the stability of the end key pointer passed in,
                // it could be a pointer to the coordinator ng range key if
                // multiple ng lands on a single cc node. Find the next cce and
                // use the key ptr in range info.
                if (upload_range_rec->end_key_ !=
                    PositiveInfinity<KeyT>::Instance())
                {
                    CcEntry<KeyT, RangeRecord> *next_cce =
                        Find(*static_cast<const KeyT *>(
                                 upload_range_rec->end_key_))
                            .second;
                    assert(next_cce != nullptr);
                    upload_range_rec->end_key_ =
                        next_cce->payload_->range_info_->start_key_.get();
                }
                const TxKey *old_end_key = upload_range_rec->end_key_;

                // Split the StoreRange struct in old TableRangeEntry and get
                // the removed slice keys and sizes. These keys will be reused
                // as the slice keys in the new ranges.
                std::vector<std::tuple<TxKey::Uptr, uint32_t, SliceStatus>>
                    new_slice_keys;
                NodeGroupId range_owner =
                    shard_
                        ->GetRangeOwner(
                            upload_range_rec->GetRangeInfo()->PartitionId(),
                            this->cc_ng_id_)
                        ->BucketOwner();
                if (range_owner != this->cc_ng_id_)
                {
                    if (range_slices.empty())
                    {
                        // This could happen if a participant ng failover to the
                        // coordinator node. In this case, the participant will
                        // receive a local cc req.
                        for (auto &slice : *upload_range_rec->range_slices_)
                        {
                            range_slices.emplace_back(
                                slice.first ? slice.first->Clone() : nullptr,
                                slice.second);
                        }
                    }
                    std::unique_ptr<StoreRange> store_range =
                        std::make_unique<StoreRange>(
                            old_info->start_key_.get(),
                            old_end_key,
                            old_info->partition_id_,
                            range_owner,
                            *Sharder::Instance().GetLocalCcShards());
                    store_range->InitSlices(range_slices);
                    new_slice_keys = store_range->SplitRange(
                        old_info->new_key_.front().get());
                }
                else
                {
                    new_slice_keys = old_entry->RangeSlices()->SplitRange(
                        old_info->new_key_.front().get());
                }
                if (new_slice_keys.empty())
                {
                    // If split fails due to slice is pinned or is loading
                    // retry later.
                    shard_->Enqueue(shard_->LocalCoreId(), &req);
                    return false;
                }
                auto cur_slice = new_slice_keys.begin();

                // Create new range entries in local cc shard
                for (uint idx = 0; idx < old_info->new_partition_id_.size();
                     idx++)
                {
                    std::vector<std::tuple<TxKey::Uptr, uint32_t, SliceStatus>>
                        cur_range_slices;
                    // First slice start key will reuse the new range start key,
                    // so we can just pass in nullptr.
                    TxKey::Uptr range_start_key =
                        std::move(std::get<0>(*cur_slice));
                    std::get<0>(*cur_slice) = nullptr;
                    cur_range_slices.push_back(std::move(*cur_slice));
                    cur_slice++;

                    // Move the range slices that falls into the new range.
                    while (cur_slice != new_slice_keys.end() &&
                           (idx + 1 == old_info->new_key_.size() ||
                            *std::get<0>(*cur_slice) <
                                *old_info->new_key_.at(idx + 1)))
                    {
                        cur_range_slices.push_back(std::move(*cur_slice));
                        cur_slice++;
                    }
                    const TableRangeEntry *new_range = shard_->CreateTableRange(
                        this->table_name_,
                        this->cc_ng_id_,
                        old_info->new_partition_id_.at(idx),
                        std::move(range_start_key),
                        cur_slice == new_slice_keys.end()
                            ? old_end_key
                            : std::get<0>(*cur_slice).get(),
                        old_info->dirty_ts_,
                        &cur_range_slices);
                    new_range_infos.push_back(new_range->GetRangeInfo());
                }

                // Mark the range as commited, which makes the new keys
                // invisible to range read requests. The new keys are not
                // deleted yet because we need to pass them to other cores to
                // update range cc map.
                old_info->CommitDirty();
                upload_range_rec->end_key_ =
                    new_range_infos.front()->StartKey();
                upload_range_rec->SetRangeInfo(old_info);
                upload_range_rec->SetNewRangeOwnerRec(nullptr);

                if (range_owner == this->cc_ng_id_)
                {
                    ACTION_FAULT_INJECTOR("range_split_post_commit");
                    old_entry->RangeSlices()->SetRangeEndKey(
                        new_range_infos.front()->StartKey());
                    old_entry->RangeSlices()->Unlock();
                }
                else
                {
                    ACTION_FAULT_INJECTOR(
                        "range_split_post_commit_participant");
                }
            }
            else
            {
                for (auto &new_key : old_info->new_key_)
                {
                    new_range_infos.push_back(
                        shard_
                            ->GetTableRangeEntry(this->table_name_,
                                                 this->cc_ng_id_,
                                                 new_key.get())
                            ->GetRangeInfo());
                }
            }
            assert(new_range_infos.size());

            // add new range entry to range cc map
            auto target_cce =
                Find(*static_cast<const KeyT *>(req.Key())).second;
            auto &new_range_owner_rec =
                *target_cce->payload_->new_range_owner_rec_;
            for (uint idx = 0; idx < new_range_infos.size(); idx++)
            {
                auto new_range_info = new_range_infos.at(idx);
                const KeyT *start_key = (static_cast<const KeyT *>(
                    new_range_info->start_key_.get()));
                auto it =
                    TemplateCcMap<KeyT, RangeRecord>::FindEmplace(*start_key);
                CcEntry<KeyT, RangeRecord> *cce = it->second;

                if (cce->commit_ts_ >= new_range_info->version_ts_)
                {
                    // Skip if the new range entry is already committed.
                    continue;
                }
                cce->commit_ts_ = new_range_info->version_ts_;
                cce->payload_ = std::make_shared<RangeRecord>();
                cce->payload_->range_info_ = new_range_info;
                cce->payload_->range_owner_rec_ = new_range_owner_rec.at(idx);

                // update previous cce's end key
                it--;
                it->second->payload_->end_key_ = new_range_info->StartKey();
                it++;

                if (idx != new_range_infos.size() - 1)
                {
                    cce->payload_.get()->end_key_ =
                        new_range_infos.at(idx + 1)->start_key_.get();
                }
                else
                {
                    // Do not point end key to the map key (it->first) since
                    // it does not have pointer stability.
                    it++;
                    if (it->second)
                    {
                        cce->payload_.get()->end_key_ =
                            it->second->payload_->range_info_->start_key_.get();
                    }
                    else
                    {
                        // end key is pos inf
                        cce->payload_.get()->end_key_ = it->first;
                    }
                }
                cce->payload_status_ = RecordStatus::Normal;
                shard_->mem_usage_ += cce->PayloadMemUsage();
            }
            // range_owner_rec_ needs to be reset on each core since they point
            // to bucket records on different cores.
            upload_range_rec->range_owner_rec_ =
                target_cce->payload_->range_owner_rec_;

            if (shard_->realtime_sampling_ &&
                shard_->core_id_ == Statistics::CoreDoSample(this->table_name_))
            {
                SplitSamplePool(old_info);
            }

            // Now that every core has inserted new range entries into ccmap, we
            // don't need the new keys anymore. Remove new keys from the
            // original range.
            if (shard_->core_id_ == shard_->core_cnt_ - 1)
            {
                TableRangeEntry *range_entry = shard_->GetTableRangeEntry(
                    this->table_name_, this->cc_ng_id_, target_key);
                range_entry->range_info_->ClearDirty();
            }
        }

        return TemplateCcMap<KeyT, RangeRecord>::Execute(req);
    }

    bool Execute(ReplayLogCc &req) override
    {
        uint32_t group_id = req.NodeGroupId();
        int64_t ng_term = Sharder::Instance().CandidateLeaderTerm(group_id);
        if (ng_term < 0)
        {
            req.Result()->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            return false;
        }

        // restore the SplitRangeOpMessage
        const std::string_view &content = req.LogContentView();
        ::txlog::SplitRangeOpMessage ds_split_range_op_msg;
        ds_split_range_op_msg.ParseFromArray(content.data(), content.length());

        const TableSchema *table_schema = req.GetTableSchema();

        // Restore old range key
        std::unique_ptr<KeyT> old_range_key = std::make_unique<KeyT>();
        const KeyT *old_range_key_ptr;
        size_t offset = 0;

        if (ds_split_range_op_msg.range_key_case() ==
            txlog::SplitRangeOpMessage::RangeKeyCase::kRangeKeyValue)
        {
            old_range_key->Deserialize(
                const_cast<char *>(
                    ds_split_range_op_msg.range_key_value().c_str()),
                offset,
                KeySchema());
            old_range_key_ptr = old_range_key.get();
        }
        else
        {
            old_range_key_ptr = NegativeInfinity<KeyT>::Instance();
        }

        size_t new_range_cnt = ds_split_range_op_msg.new_partition_id_size();
        std::vector<std::unique_ptr<TxKey>> new_range_keys;
        std::vector<int32_t> new_range_ids;
        new_range_ids.reserve(new_range_cnt);
        new_range_keys.reserve(new_range_cnt);
        for (size_t i = 0; i < new_range_cnt; i++)
        {
            int32_t range_id = ds_split_range_op_msg.new_partition_id(i);
            new_range_ids.push_back(range_id);
            new_range_keys.push_back(std::make_unique<KeyT>());
            offset = 0;
            new_range_keys.back()->Deserialize(
                const_cast<char *>(
                    ds_split_range_op_msg.new_range_key(i).c_str()),
                offset,
                this->KeySchema());
        }

        // Restore partition partition id
        int32_t partition_id = ds_split_range_op_msg.partition_id();

        // Restore stage
        ::txlog::SplitRangeOpMessage_Stage stage =
            ds_split_range_op_msg.stage();

        uint32_t tx_node_id = (req.Txn() >> 32L) >> 10;
        int64_t tx_candidate_term =
            Sharder::Instance().CandidateLeaderTerm(tx_node_id);
        bool is_coordinator =
            tx_node_id == req.NodeGroupId() && tx_candidate_term >= 0;

        // Restore local_cc_shards state at core 0
        TableRangeEntry *old_table_range_entry = nullptr;
        const TxKey *old_end_key = nullptr;
        CcEntry<KeyT, RangeRecord> *old_range_cce = nullptr;

        if (ds_split_range_op_msg.range_key_case() ==
            txlog::SplitRangeOpMessage::RangeKeyCase::kRangeKeyNegInf)
        {
            old_range_cce = &neg_inf_;
        }
        else
        {
            auto it = Find(*old_range_key_ptr);
            assert(it.first);
            old_range_cce = it.second;
        }

        // Restore range end key
        if (stage == ::txlog::SplitRangeOpMessage_Stage_PrepareSplit)
        {
            // We can safely use the end key of the old range ccentry,
            // since we know for sure that the new range ccentries have
            // not been inserted into ccmap yet.
            old_end_key = old_range_cce->payload_->end_key_;
        }
        else if (stage == ::txlog::SplitRangeOpMessage_Stage_CommitSplit)
        {
            // Find the next cce of the last new range key, since we
            // don't know if the new range cce has been created or not.
            auto it = Floor(static_cast<const KeyT &>(*new_range_keys.back()));
            CcEntry<KeyT, RangeRecord> *prev_cce = it->second;
            old_end_key = prev_cce->payload_->end_key_;
        }
        else
        {
            assert(false);
        }

        std::vector<const RangeInfo *> new_range_infos;
        if (shard_->core_id_ == 0)
        {
            if (stage == ::txlog::SplitRangeOpMessage_Stage_PrepareSplit)
            {
                if (!is_coordinator)
                {
                    // Participants needs to be restored to state right before
                    // commit log is written, so we need to install the dirty
                    // range.
                    old_table_range_entry = const_cast<TableRangeEntry *>(
                        shard_->UploadNewRangeInfo(this->table_name_,
                                                   this->cc_ng_id_,
                                                   old_range_key_ptr,
                                                   new_range_keys,
                                                   new_range_ids,
                                                   req.CommitTs()));
                }
                else
                {
                    // For coordinator, the replay tx will install the dirty
                    // range for us.
                    old_table_range_entry = shard_->GetTableRangeEntry(
                        this->table_name_, this->cc_ng_id_, old_range_key_ptr);
                    old_table_range_entry->RangeSlices()->Lock();
                }
            }
            else if (stage == ::txlog::SplitRangeOpMessage_Stage_CommitSplit)
            {
                std::vector<std::pair<TxKey::Uptr, uint32_t>> range_slices;
                range_slices.emplace_back(nullptr,
                                          ds_split_range_op_msg.slice_sizes(0));
                for (int idx = 0; idx < ds_split_range_op_msg.slice_keys_size();
                     idx++)
                {
                    std::unique_ptr<KeyT> slice_key = std::make_unique<KeyT>();
                    offset = 0;
                    slice_key->Deserialize(
                        const_cast<char *>(
                            ds_split_range_op_msg.slice_keys(idx).c_str()),
                        offset,
                        this->KeySchema());
                    range_slices.emplace_back(
                        std::move(slice_key),
                        ds_split_range_op_msg.slice_sizes(idx + 1));
                }

                if (!is_coordinator)
                {
                    // For non coordinator, we need to assume that the
                    // postwriteall has already been executed. So install the
                    // dirty range info as the current range info and no lock is
                    // needed.
                    old_table_range_entry = shard_->GetTableRangeEntry(
                        this->table_name_, this->cc_ng_id_, old_range_key_ptr);
                    RangeInfo *old_info = const_cast<RangeInfo *>(
                        old_table_range_entry->GetRangeInfo());
                    // Initialize slice specs if any of the new ranges falls on
                    // this ng.
                    std::unique_ptr<StoreRange> store_range =
                        std::make_unique<StoreRange>(
                            old_info->start_key_.get(),
                            old_end_key,
                            old_info->partition_id_,
                            tx_node_id,
                            *Sharder::Instance().GetLocalCcShards());
                    store_range->InitSlices(range_slices);
                    std::vector<std::tuple<TxKey::Uptr, uint32_t, SliceStatus>>
                        new_slice_keys = store_range->SplitRange(
                            new_range_keys.front().get());

                    auto cur_slice = new_slice_keys.begin();
                    // Create new range entries in local cc shard
                    for (uint idx = 0; idx < new_range_ids.size(); idx++)
                    {
                        std::vector<
                            std::tuple<TxKey::Uptr, uint32_t, SliceStatus>>
                            cur_range_slices;
                        // First slice start key will reuse the new range start
                        // key, so we can just pass in nullptr.
                        TxKey::Uptr range_start_key =
                            std::move(std::get<0>(*cur_slice));
                        std::get<0>(*cur_slice) = nullptr;
                        cur_range_slices.push_back(std::move(*cur_slice));
                        cur_slice++;

                        // Move the range slices that falls into the new range.
                        while (cur_slice != new_slice_keys.end() &&
                               (idx + 1 == new_range_keys.size() ||
                                *std::get<0>(*cur_slice) <
                                    *new_range_keys.at(idx + 1)))
                        {
                            cur_range_slices.push_back(std::move(*cur_slice));
                            cur_slice++;
                        }

                        const TableRangeEntry *new_range =
                            shard_->CreateTableRange(
                                this->table_name_,
                                this->cc_ng_id_,
                                new_range_ids.at(idx),
                                std::move(range_start_key),
                                cur_slice == new_slice_keys.end()
                                    ? old_end_key
                                    : std::get<0>(*cur_slice).get(),
                                req.CommitTs(),
                                &cur_range_slices);
                        new_range_infos.push_back(new_range->GetRangeInfo());
                    }
                    old_info->ClearDirty(req.CommitTs());
                }
                else
                {
                    // Restore the current and dirty range info. Acquire write
                    // lock.
                    old_table_range_entry = const_cast<TableRangeEntry *>(
                        shard_->UploadNewRangeInfo(this->table_name_,
                                                   this->cc_ng_id_,
                                                   old_range_key_ptr,
                                                   new_range_keys,
                                                   new_range_ids,
                                                   req.CommitTs()));
                    // Restore range slice specs from log message. the range
                    // slice specs we read from data store is unreliable since
                    // it could've already been updated before the crash.
                    old_table_range_entry->RangeSlices()->InitSlices(
                        range_slices);
                    old_table_range_entry->RangeSlices()->Lock();
                }
            }
            else
            {
                // should not be here if log is in clean stage
                assert(false);
            }
        }
        else
        {
            old_table_range_entry = shard_->GetTableRangeEntry(
                this->table_name_, this->cc_ng_id_, old_range_key_ptr);
            if (stage == ::txlog::SplitRangeOpMessage_Stage_CommitSplit &&
                !is_coordinator)
            {
                for (auto &new_key : new_range_keys)
                {
                    new_range_infos.push_back(
                        shard_
                            ->GetTableRangeEntry(this->table_name_,
                                                 this->cc_ng_id_,
                                                 new_key.get())
                            ->GetRangeInfo());
                }
            }
        }
        assert(old_table_range_entry != nullptr);

        // Create new range records in ccmap if commit log and as participant.
        // In this case we should recover to the stage where post write all
        // has already been executed on this ng.
        if (stage == ::txlog::SplitRangeOpMessage_Stage_CommitSplit &&
            !is_coordinator)
        {
            // add new range entry to range cc map
            auto bucket_map = static_cast<RangeBucketCcMap *>(
                shard_->GetCcm(range_bucket_ccm_name, this->cc_ng_id_));
            for (uint idx = 0; idx < new_range_infos.size(); idx++)
            {
                auto new_range_info = new_range_infos.at(idx);
                const KeyT *start_key = (static_cast<const KeyT *>(
                    new_range_info->start_key_.get()));
                auto it =
                    TemplateCcMap<KeyT, RangeRecord>::FindEmplace(*start_key);
                CcEntry<KeyT, RangeRecord> *cce = it->second;

                if (cce->commit_ts_ >= new_range_info->version_ts_)
                {
                    continue;
                }
                cce->commit_ts_ = new_range_info->version_ts_;
                cce->payload_ = std::make_shared<RangeRecord>();
                cce->payload_->range_info_ = new_range_info;
                // Link bucket owner record
                cce->payload_->range_owner_rec_ =
                    bucket_map->GetBucketRecord(Sharder::MapRangeIdToBucketId(
                        new_range_info->PartitionId()));

                // update previous cce's end key
                it--;
                it->second->payload_->end_key_ = new_range_info->StartKey();
                it++;

                if (idx != new_range_infos.size() - 1)
                {
                    cce->payload_.get()->end_key_ =
                        new_range_infos.at(idx + 1)->start_key_.get();
                }
                else
                {
                    // Do not point end key to the map key (it->first)
                    // since it does not have pointer stability.
                    it++;
                    if (it->second)
                    {
                        cce->payload_.get()->end_key_ =
                            it->second->payload_->range_info_->start_key_.get();
                    }
                    else
                    {
                        // end key is pos inf
                        cce->payload_.get()->end_key_ = it->first;
                    }
                }
                cce->payload_status_ = RecordStatus::Normal;
                shard_->mem_usage_ += cce->PayloadMemUsage();
            }
        }

        if (ds_split_range_op_msg.stage() ==
                ::txlog::SplitRangeOpMessage_Stage_PrepareSplit ||
            (ds_split_range_op_msg.stage() ==
                 ::txlog::SplitRangeOpMessage_Stage_CommitSplit &&
             is_coordinator))
        {
            // For coordinator node, we need to restore to the state
            // right after the current log is written, for participant
            // node, we need to restore to the state right before the
            // next log is written. So if the log state is at prepare
            // stage, we need to acquire write lock on range no matter
            // what.
            auto lock_pair = AcquireCceKeyLock(old_range_cce,
                                               old_range_cce->payload_status_,
                                               &req,
                                               req.NodeGroupId(),
                                               ng_term,
                                               0,
                                               CcOperation::Write,
                                               IsolationLevel::RepeatableRead,
                                               CcProtocol::Locking,
                                               0,
                                               false);
            // When a cc node recovers, no one should be holding read
            // locks. So, the acquire operation should always succeed.
            assert(lock_pair.first == LockType::WriteLock &&
                   lock_pair.second == CcErrorCode::NO_ERROR);

            // Register the range owner bucket for the new ranges
            auto bucket_map = static_cast<RangeBucketCcMap *>(
                shard_->GetCcm(range_bucket_ccm_name, this->cc_ng_id_));
            auto new_range_owner_rec =
                std::make_unique<std::vector<LruEntry *>>();
            for (int32_t new_id : new_range_ids)
            {
                // Link bucket owner record
                new_range_owner_rec->push_back(bucket_map->GetBucketRecord(
                    Sharder::MapRangeIdToBucketId(new_id)));
            }
            old_range_cce->payload_->SetNewRangeOwnerRec(
                std::move(new_range_owner_rec));
        }

        // Move to next core
        if (shard_->core_id_ < shard_->core_cnt_ - 1)
        {
            req.ResetCcm();
            MoveRequest(&req, shard_->core_id_ + 1);
        }
        else
        {
            // Restore transaction and catalog read lock at last core if this
            // recovering node group is the tx coordinator
            if (tx_node_id == req.NodeGroupId() && tx_candidate_term >= 0)
            {
                const RangeInfo *old_range_info =
                    old_table_range_entry->GetRangeInfo();
                shard_->local_shards_.CreateSplitRangeRecoveryTx(
                    ds_split_range_op_msg,
                    table_schema,
                    partition_id,
                    old_range_info->StartKey()
                        ? old_range_info->StartKey()
                        : NegativeInfinity<KeyT>::Instance(),
                    old_end_key ? old_end_key
                                : PositiveInfinity<KeyT>::Instance(),
                    old_range_info,
                    std::move(new_range_keys),
                    std::move(new_range_ids),
                    tx_node_id,
                    req.Txn(),
                    tx_candidate_term,
                    req.CommitTs(),
                    std::move(req.GetCatalogCcEntry()),
                    req.RangeSplitStarted());
            }

            req.SetFinish();
        }

        return false;
    }

    TableType Type() const override
    {
        return TableType::RangePartition;
    }

private:
    void DeserializeRangeRecord(
        const std::string &payload,
        RangeRecord *range_record,
        std::vector<std::pair<TxKey::Uptr, uint32_t>> &range_slices)
    {
        const char *buf = payload.data();
        size_t offset = 0;
        std::unique_ptr<TxKey> start_key = nullptr;
        bool is_normal;
        DesrializeFrom(buf, offset, &is_normal);
        if (is_normal)
        {
            start_key = std::make_unique<KeyT>();
            start_key->Deserialize(buf, offset, nullptr);
        }

        uint64_t version_ts;
        uint32_t partition_id;
        DesrializeFrom(buf, offset, &partition_id);
        DesrializeFrom(buf, offset, &version_ts);
        std::unique_ptr<RangeInfo> range_info = std::make_unique<RangeInfo>(
            std::move(start_key), version_ts, partition_id);
        uint16_t new_part_size;
        DesrializeFrom(buf, offset, &new_part_size);
        if (new_part_size > 0)
        {
            range_info->is_dirty_ = true;
        }
        for (size_t idx = 0; idx < new_part_size; idx++)
        {
            TxKey::Uptr new_key = std::make_unique<KeyT>();
            new_key->Deserialize(buf, offset, nullptr);
            range_info->new_key_.push_back(std::move(new_key));
        }
        for (size_t idx = 0; idx < new_part_size; idx++)
        {
            int32_t new_part_id;
            DesrializeFrom(buf, offset, &new_part_id);
            range_info->new_partition_id_.push_back(new_part_id);
        }
        DesrializeFrom(buf, offset, &range_info->dirty_ts_);

        DesrializeFrom(buf, offset, &is_normal);
        if (is_normal)
        {
            KeyT end_key;
            end_key.Deserialize(buf, offset, nullptr);
            CcEntry<KeyT, RangeRecord> *cce = Find(end_key).second;
            assert(cce != nullptr);
            range_record->end_key_ =
                cce->payload_->range_info_->start_key_.get();
            assert(range_record->end_key_ != nullptr);
        }
        else
        {
            range_record->end_key_ = PositiveInfinity<KeyT>::Instance();
        }
        range_record->SetRangeInfo(std::move(range_info));

        uint16_t slice_cnt;
        DesrializeFrom(buf, offset, &slice_cnt);
        if (slice_cnt > 0)
        {
            uint32_t slice_size;
            DesrializeFrom(buf, offset, &slice_size);
            range_slices.emplace_back(nullptr, slice_size);
            for (size_t idx = 1; idx < slice_cnt; idx++)
            {
                DesrializeFrom(buf, offset, &slice_size);
                range_slices.emplace_back(nullptr, slice_size);
            }
            for (size_t idx = 1; idx < slice_cnt; idx++)
            {
                TxKey::Uptr slice_key = std::make_unique<KeyT>();
                slice_key->Deserialize(buf, offset, nullptr);
                range_slices.at(idx).first = std::move(slice_key);
            }
        }
    }

    void SplitSamplePool(const RangeInfo *old_info)
    {
        TableName table_or_index_name(this->table_name_.StringView(),
                                      this->table_name_.IsBase()
                                          ? TableType::Primary
                                          : TableType::Secondary);
        TableStatistics<KeyT> *statistics =
            static_cast<TableStatistics<KeyT> *>(
                table_schema_->StatisticsObject().get());

        statistics->OnSplitSamplePool(
            shard_, this->cc_ng_id_, table_or_index_name, old_info);
    }
};
}  // namespace txservice
