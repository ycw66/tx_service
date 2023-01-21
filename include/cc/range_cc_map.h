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

        for (auto &[key, table_range] : *ranges)
        {
            const RangeInfo *range_info = table_range.GetRangeInfo();
            const KeyT *start_key = static_cast<const KeyT *>(key);
            if (start_key->Type() == KeyType::NegativeInf)
            {
                neg_inf_.payload_.get()->range_info_ = range_info;
                neg_inf_.commit_ts_ = range_info->version_ts_;
                neg_inf_.payload_status_ = RecordStatus::Normal;
            }
            else
            {
                auto it =
                    TemplateCcMap<KeyT, RangeRecord>::FindEmplace(*start_key);
                CcEntry<KeyT, RangeRecord> *cce = it->second;
                cce->commit_ts_ = range_info->version_ts_;
                cce->payload_.get()->range_info_ = range_info;
                cce->payload_status_ = RecordStatus::Normal;
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
        if (req.CcePtr() != nullptr)
        {
            // The request was blocked before. This is execution resumption
            // after the request is unblocked. The read lock/intention must have
            // been acquired.
            floor_cce = static_cast<CcEntry<KeyT, RangeRecord> *>(req.CcePtr());

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
                                            req.ReadTimestamp());
        }
        else
        {
            // Rather than looking for an exact match, looks up the floor key
            // that represents the range containing the input key.
            const KeyT *look_key = static_cast<const KeyT *>(req.Key());

            auto it = Floor(*look_key);
            floor_cce = it->second;
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
                                  req.ReadTimestamp());
        }

        // after acquiring lock
        switch (err_code)
        {
        case CcErrorCode::NO_ERROR:
        {
            CcEntryAddr &cce_addr = hd_result->Value().cce_addr_;
            cce_addr.SetCce(reinterpret_cast<uint64_t>(floor_cce),
                            ng_term,
                            req.NodeGroupId());

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
            return false;
        }
        default:
        {
            return true;
        }
        }  //-- end: switch

        return true;
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
                break;
            case KeyType::PositiveInf:
                target_key = PositiveInfinity<KeyT>::Instance();
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
            // TODO{liunyl}: update record deserialize with multiple new keys
            assert(req.PayloadStr() != nullptr);
            std::unique_ptr<RangeRecord> decoded_rec =
                std::make_unique<RangeRecord>();
            size_t offset = 0;
            decoded_rec->Deserialize(req.PayloadStr()->data(), offset);
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
                upload_range_rec->range_info_ =
                    shard_
                        ->UploadNewRangeInfo(
                            this->table_name_,
                            this->cc_ng_id_,
                            target_key,
                            upload_range_rec->range_info_->new_key_,
                            upload_range_rec->range_info_->new_partition_id_,
                            req.CommitTs())
                        ->GetRangeInfo();
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
                const TxKey *old_end_key =
                    old_entry->RangeSlices()->RangeEndKey();

                // TODO{liunyl}: If node is not owner of data, store slice will
                // be nullptr in TableRangeEntry. Also we need to load
                // StoreRange and StoreSlice from data store if this ng is the
                // new owner of the splitted range.

                // Split the StoreRange struct in old TableRangeEntry and get
                // the removed slice keys and sizes. These keys will be reused
                // as the slice keys in the new ranges.
                std::vector<std::pair<TxKey::Uptr, uint32_t>> new_slice_keys =
                    old_entry->RangeSlices()->SplitRange(
                        old_info->new_key_.front().get());
                assert(!new_slice_keys.empty());
                auto cur_slice = new_slice_keys.begin();

                // Create new range entries in local cc shard
                for (uint idx = 0; idx < old_info->new_partition_id_.size();
                     idx++)
                {
                    std::vector<std::pair<TxKey::Uptr, uint32_t>>
                        cur_range_slices;
                    // First slice start key will reuse the new range start key,
                    // so we can just pass in nullptr.
                    TxKey::Uptr range_start_key = std::move(cur_slice->first);
                    cur_range_slices.emplace_back(nullptr, cur_slice->second);
                    cur_slice++;

                    // Move the range slices that falls into the new range.
                    while (
                        cur_slice != new_slice_keys.end() &&
                        (idx + 1 == old_info->new_key_.size() ||
                         *cur_slice->first < *old_info->new_key_.at(idx + 1)))
                    {
                        cur_range_slices.emplace_back(
                            std::move(cur_slice->first), cur_slice->second);
                        cur_slice++;
                    }

                    const TableRangeEntry *new_range = shard_->CreateTableRange(
                        this->table_name_,
                        this->cc_ng_id_,
                        old_info->new_partition_id_.at(idx),
                        std::move(range_start_key),
                        cur_slice == new_slice_keys.end()
                            ? old_end_key
                            : cur_slice->first.get(),
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
                    new_range_infos.front()->start_key_.get();
                upload_range_rec->range_info_ = old_info;
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
                // Make a copy of the start key in TableRangeEntry. FindEmplace
                // will std::move(start_key).
                KeyT start_key(*static_cast<const KeyT *>(
                    new_range_info->start_key_.get()));
                const TxKey *end_key = nullptr;
                if (idx != new_range_infos.size() - 1)
                {
                    end_key = new_range_infos.at(idx + 1)->start_key_.get();
                }

                auto it =
                    TemplateCcMap<KeyT, RangeRecord>::FindEmplace(start_key);
                CcEntry<KeyT, RangeRecord> *cce = it->second;

                cce->commit_ts_ = new_range_info->version_ts_;
                cce->payload_.get()->range_info_ = new_range_info;
                cce->payload_.get()->end_key_ = end_key;
                cce->payload_status_ = RecordStatus::Normal;
            }
            // Now that every core has inserted new range entries into ccmap, we
            // don't need the new keys anymore. Remove new keys from the
            // original range.
            if (shard_->core_id_ == shard_->core_cnt_ - 1)
            {
                shard_
                    ->GetTableRangeEntry(
                        this->table_name_, this->cc_ng_id_, target_key)
                    ->range_info_->ClearDirty();
            }
        }

        return TemplateCcMap<KeyT, RangeRecord>::Execute(req);
    }

    bool Execute(ReplayLogCc &req) override
    {
        // uint32_t group_id = req.NodeGroupId();
        // int64_t ng_term = Sharder::Instance().CandidateLeaderTerm(group_id);
        // if (ng_term < 0)
        //{
        //    req.Result()->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
        //    return false;
        //}

        //// restore the SplitRangeOpMessage
        // const std::string_view &content = req.LogContentView();
        //::txlog::SplitRangeOpMessage ds_split_range_op_msg;
        // ds_split_range_op_msg.ParseFromArray(content.data(),
        // content.length());

        // const TableSchema *table_schema = req.GetTableSchema();

        //// Restore new_range_key, which can't be neg or pos inf
        // size_t offset = 0;
        // std::unique_ptr<KeyT> new_range_key = std::make_unique<KeyT>();
        //  new_range_key->Deserialize(
        //     const_cast<char
        //     *>(ds_split_range_op_msg.new_range_key().c_str()), offset,
        //     this->KeySchema());

        // offset = 0;
        // std::unique_ptr<KeyT> new_range_key_for_recovery =
        //    std::make_unique<KeyT>();
        // new_range_key_for_recovery->Deserialize(
        //    const_cast<char *>(ds_split_range_op_msg.new_range_key().c_str()),
        //    offset,
        //    this->KeySchema());

        //// Restore partition partition id
        // int32_t partition_id = ds_split_range_op_msg.partition_id();
        // int32_t new_partition_id = ds_split_range_op_msg.new_partition_id();

        //// Restore stage
        //::txlog::SplitRangeOpMessage_Stage stage =
        //    ds_split_range_op_msg.stage();

        // uint32_t tx_node_id = (req.Txn() >> 32L) >> 10;
        // int64_t tx_candidate_term =
        //    Sharder::Instance().CandidateLeaderTerm(tx_node_id);

        //// Restore local_cc_shards state at core 0
        // TableRangeEntry *old_table_range_entry = nullptr;
        // if (shard_->core_id_ == 0)
        //{
        //    if (stage == ::txlog::SplitRangeOpMessage::PrepareDirtyOldRange)
        //    {
        //        const TableRangeEntryWithShade *range_entry_shade =
        //            shard_->GetTableRangeWithShade(
        //                CcMap::table_name_, partition_id, req.NodeGroupId());
        //        old_table_range_entry = range_entry_shade->shader_.get();
        //    }

        //    if (stage > ::txlog::SplitRangeOpMessage::PrepareDirtyOldRange &&
        //        stage < ::txlog::SplitRangeOpMessage::DeletingOldRangeData)
        //    {
        //        // upload the dirty range attributes to local cc shards
        //        const TableRangeEntryWithShade *range_entry_shade =
        //            shard_->CreateDirtyTableRange(CcMap::table_name_,
        //                                          partition_id,
        //                                          std::move(new_range_key),
        //                                          new_partition_id,
        //                                          req.CommitTs(),
        //                                          req.NodeGroupId());
        //        old_table_range_entry = range_entry_shade->shade_.get();
        //    }

        //    if (stage >= ::txlog::SplitRangeOpMessage::CommitOldRangeNewRange
        //    &&
        //        stage < ::txlog::SplitRangeOpMessage::DeletingOldRangeData)
        //    {
        //        // commit dirty range, old_range_entry switch back to shader
        //        std::pair<TableRangeEntry *, TableRangeEntry *> entries =
        //            shard_->CommitDirtyTableRange(CcMap::table_name_,
        //                                          partition_id,
        //                                          req.CommitTs(),
        //                                          group_id);
        //        old_table_range_entry = entries.first;
        //    }
        //}
        // else
        //{
        //    if (stage >= ::txlog::SplitRangeOpMessage::PrepareDirtyOldRange &&
        //        stage < ::txlog::SplitRangeOpMessage::DeletingOldRangeData)
        //    {
        //        const TableRangeEntryWithShade *table_range_entry_with_shard =
        //            shard_->GetTableRangeWithShade(
        //                CcMap::table_name_, partition_id, group_id);
        //        old_table_range_entry =
        //            table_range_entry_with_shard->shade_.get();
        //    }
        //    else if (stage ==
        //             ::txlog::SplitRangeOpMessage::CommitOldRangeNewRange)
        //    {
        //        const TableRangeEntryWithShade
        //            *old_table_range_entry_with_shard =
        //                shard_->GetTableRangeWithShade(
        //                    CcMap::table_name_, partition_id, group_id);
        //        old_table_range_entry =
        //            old_table_range_entry_with_shard->shader_.get();
        //    }
        //}

        //// Restore range cc map state
        // CcEntry<KeyT, RangeRecord> *old_range_cce = nullptr;

        // if (ds_split_range_op_msg.range_key_neg_inf() == true)
        //{
        //    old_range_cce = &neg_inf_;
        //}
        // else if (ds_split_range_op_msg.range_key_pos_inf() == true)
        //{
        //    old_range_cce = &pos_inf_;
        //}
        // else
        //{
        //    offset = 0;
        //    std::unique_ptr<KeyT> range_tx_key = std::make_unique<KeyT>();
        //    range_tx_key->Deserialize(
        //        const_cast<char *>(
        //            ds_split_range_op_msg.range_key_value().c_str()),
        //        offset,
        //        this->KeySchema());
        //    auto it = ccm_.find(*range_tx_key.get());
        //    assert(it != ccm_.end());
        //    old_range_cce = &it->second;
        //}

        //// Restore old range
        // if (stage <= ::txlog::SplitRangeOpMessage::PrepareDirtyOldRange)
        //{
        //    old_range_cce->payload_->range_entry_ = old_table_range_entry;
        //}

        //// Recover locks on range cce
        // if (stage == ::txlog::SplitRangeOpMessage::PrepareDirtyOldRange ||
        //    stage == ::txlog::SplitRangeOpMessage::CommitOldRangeNewRange)
        //{
        //    // Add write lock on old range cce
        //    bool success = old_range_cce->GetKeyLock().AcquireWriteLock(
        //        &req, CcProtocol::Locking);
        //    assert(success);
        //}
        // else if (stage == ::txlog::SplitRangeOpMessage::CopingOldRangeData)
        //{
        //    // Add write intention on old range cce
        //    bool success = old_range_cce->GetKeyLock().AcquireWriteIntent(
        //        &req, CcProtocol::Locking);
        //    assert(success);
        //}

        //// Move to next core
        // if (shard_->core_id_ < shard_->core_cnt_ - 1)
        //{
        //    req.ResetCcm();
        //    MoveRequest(&req, shard_->core_id_ + 1);
        //}
        // else
        //{
        //    std::unique_ptr<RangeRecord> old_range_record =
        //        std::make_unique<RangeRecord>(*old_range_cce->payload_.get());
        //    // Restore transaction and catalog read lock at last core if this
        //    is
        //    // the recovering node group is the tx coordinator
        //    if (tx_node_id == req.NodeGroupId() && tx_candidate_term >= 0)
        //    {
        //        shard_->local_shards_.CreateSplitRangeRecoveryTx(
        //            ds_split_range_op_msg,
        //            table_schema,
        //            old_range_cce->key_,
        //            std::move(old_range_record),
        //            partition_id,
        //            std::move(new_range_key_for_recovery),
        //            new_partition_id,
        //            tx_node_id,
        //            req.Txn(),
        //            tx_candidate_term,
        //            req.CommitTs(),
        //            std::move(req.GetCatalogCcEntry()));
        //    }

        //    req.SetFinish();
        //}

        return false;
    }

    TableType Type() const override
    {
        return TableType::RangePartition;
    }
};
}  // namespace txservice
