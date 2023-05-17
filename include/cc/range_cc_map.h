#pragma once

#include <map>
#include <memory>
#include <string>
#include <utility>

#include "cc_entry.h"
#include "cc_handler_result.h"
#include "cc_request.h"
#include "error_messages.h"  //CcErrorCode
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
    ~RangeCcMap() = default;

    using TemplateCcMap<KeyT, RangeRecord>::Execute;
    using TemplateCcMap<KeyT, RangeRecord>::FindEmplace;
    using TemplateCcMap<KeyT, RangeRecord>::Emplace;
    using TemplateCcMap<KeyT, RangeRecord>::AcquireCceKeyLock;
    using TemplateCcMap<KeyT, RangeRecord>::LockHandleForResumedRequest;
    using TemplateCcMap<KeyT, RangeRecord>::MoveRequest;
    using TemplateCcMap<KeyT, RangeRecord>::shard_;
    using TemplateCcMap<KeyT, RangeRecord>::Floor;
    using TemplateCcMap<KeyT, RangeRecord>::neg_inf_;
    using TemplateCcMap<KeyT, RangeRecord>::pos_inf_;
    using TemplateCcMap<KeyT, RangeRecord>::table_schema_;
    using TemplateCcMap<KeyT, RangeRecord>::KeySchema;
    using TemplateCcMap<KeyT, RangeRecord>::Find;

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

        for (auto &[key, table_range] : *ranges)
        {
            const RangeInfo *range_info = table_range.GetRangeInfo();
            const KeyT *start_key = static_cast<const KeyT *>(key);
            if (start_key->Type() == KeyType::NegativeInf)
            {
                neg_inf_.payload_->range_info_ = range_info;
                neg_inf_.commit_ts_ = range_info->version_ts_;
                neg_inf_.payload_status_ = RecordStatus::Normal;
            }
            else
            {
                auto it =
                    TemplateCcMap<KeyT, RangeRecord>::FindEmplace(*start_key);
                CcEntry<KeyT, RangeRecord> *cce = it->second;
                cce->commit_ts_ = range_info->version_ts_;
                cce->payload_ = std::make_shared<RangeRecord>();
                cce->payload_.get()->range_info_ = range_info;
                cce->payload_status_ = RecordStatus::Normal;
                shard_->mem_usage_ += cce->PayloadMemUsage();
                it--;
                it->second->payload_->end_key_ = range_info->start_key_.get();
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
     * being split or merged.
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
            }
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
            CcEntryAddr &cce_addr = hd_result->Value().cce_addr_;
            cce_addr.SetCce(reinterpret_cast<uint64_t>(floor_cce),
                            ng_term,
                            req.NodeGroupId(),
                            shard_->LocalCoreId());

            RangeRecord *range_rec = static_cast<RangeRecord *>(req.Record());
            *range_rec = *(floor_cce->payload_);
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
            // Set CcePtr to indicate this is a resumed req.
            req.SetCcePtr(floor_cce);
            return false;
        }
        default:
        {
            return true;
        }
        }  //-- end: switch

        return true;
    }

    bool Execute(AcquireAllCc &req) override
    {
        if (shard_->core_id_ == 0)
        {
            // If this we are the owner of this range, mark the StoreRange as
            // locked.
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
        }

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

                if (upload_range_rec->GetRangeInfo()->PartitionId() %
                        Sharder::Instance().NodeGroupCount() ==
                    this->cc_ng_id_)
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
        }
        else if (req.CommitType() == PostWriteType::PostCommit)
        {
            std::vector<const RangeInfo *> new_range_infos;
            TableRangeEntry *old_entry = shard_->GetTableRangeEntry(
                this->table_name_, req.NodeGroupId(), target_key);
            RangeInfo *old_info = old_entry->range_info_.get();

            if (shard_->core_id_ == 0)
            {
                // Update table_ranges_ in local cc shard on the first core
                const TxKey *old_end_key = upload_range_rec->end_key_;

                // Split the StoreRange struct in old TableRangeEntry and get
                // the removed slice keys and sizes. These keys will be reused
                // as the slice keys in the new ranges.
                std::vector<std::tuple<TxKey::Uptr, uint32_t, SliceStatus>>
                    new_slice_keys;
                if (upload_range_rec->GetRangeInfo()->PartitionId() %
                        Sharder::Instance().NodeGroupCount() !=
                    this->cc_ng_id_)
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
                    // Check which node group the new range falls on.
                    uint32_t cc_ng_id = old_info->new_partition_id_.at(idx) %
                                        Sharder::Instance().NodeGroupCount();

                    // If the new range falls on this ng, keep the range slices
                    // info, otherwise pass in nullptr
                    const TableRangeEntry *new_range = shard_->CreateTableRange(
                        this->table_name_,
                        this->cc_ng_id_,
                        old_info->new_partition_id_.at(idx),
                        std::move(range_start_key),
                        cur_slice == new_slice_keys.end()
                            ? old_end_key
                            : std::get<0>(*cur_slice).get(),
                        old_info->dirty_ts_,
                        cc_ng_id == this->cc_ng_id_ ? &cur_range_slices
                                                    : nullptr);
                    new_range_infos.push_back(new_range->GetRangeInfo());
                }

                // Mark the range as commited, which makes the new keys
                // invisible to range read requests. The new keys are not
                // deleted yet because we need to pass them to other cores to
                // update range cc map.
                old_info->CommitDirty();
                upload_range_rec->end_key_ =
                    new_range_infos.front()->start_key_.get();
                upload_range_rec->SetRangeInfo(old_info);

                if (upload_range_rec->GetRangeInfo()->PartitionId() %
                        Sharder::Instance().NodeGroupCount() ==
                    this->cc_ng_id_)
                {
                    old_entry->RangeSlices()->Unlock();
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
            for (uint idx = 0; idx < new_range_infos.size(); idx++)
            {
                auto new_range_info = new_range_infos.at(idx);
                const KeyT *start_key = (static_cast<const KeyT *>(
                    new_range_info->start_key_.get()));
                auto it =
                    TemplateCcMap<KeyT, RangeRecord>::FindEmplace(*start_key);
                CcEntry<KeyT, RangeRecord> *cce = it->second;

                cce->commit_ts_ = new_range_info->version_ts_;
                cce->payload_ = std::make_shared<RangeRecord>();
                cce->payload_.get()->range_info_ = new_range_info;
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

            if (shard_->realtime_sampling_)
            {
                if (shard_->core_id_ ==
                    Statistics::CoreDoSample(this->table_name_))
                {
                    SplitSamplePool(old_info);
                }
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
                // TODO{liunyl}
                assert(false);
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
        }
        assert(old_table_range_entry != nullptr);

        // Restore range cc map state
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

        if (ds_split_range_op_msg.stage() ==
            ::txlog::SplitRangeOpMessage_Stage_PrepareSplit)
        {
            // For coordinator node, we need to restore to the state right after
            // the current log is written, for participant node, we need to
            // restore to the state right before the next log is written. So if
            // the log state is at prepare stage, we need to acquire write lock
            // on range no matter what.
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
            // When a cc node recovers, no one should be holding read locks. So,
            // the acquire operation should always succeed.
            assert(lock_pair.first == LockType::WriteLock &&
                   lock_pair.second == CcErrorCode::NO_ERROR);
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
                const TxKey *old_end_key;
                if (stage == ::txlog::SplitRangeOpMessage_Stage_PrepareSplit)
                {
                    // We can safely use the end key of the old range ccentry,
                    // since we know for sure that the new range ccentries have
                    // not been inserted into ccmap yet.
                    old_end_key = old_range_cce->payload_->end_key_;
                }
                else
                {
                    // Find the next cce of the last new range key, since we
                    // don't know if the new range cce has been created or not.
                }
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
                table_schema_->StatisticsObject());

        statistics->OnSplitSamplePool(
            shard_, this->cc_ng_id_, table_or_index_name, old_info);
    }
};
}  // namespace txservice
