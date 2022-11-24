#pragma once

#include <map>
#include <memory>
#include <string>
#include <utility>

#include "cc_entry.h"
#include "cc_handler_result.h"
#include "cc_request.h"
#include "range_record.h"
#include "template_cc_map.h"

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
    using TemplateCcMap<KeyT, RangeRecord>::ccm_;
    using TemplateCcMap<KeyT, RangeRecord>::neg_inf_;
    using TemplateCcMap<KeyT, RangeRecord>::pos_inf_;

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
               CcShard *shard)
        : TemplateCcMap<KeyT, RangeRecord>(
              shard, range_table_name, schema_ts, table_schema, false)
    {
        std::map<int32_t, TableRangeEntryWithShade> *ranges =
            CcMap::shard_->GetAllTableRangesForATable(range_table_name);
        assert(ranges != nullptr);

        for (auto &[partition_id, table_range_with_shade] : *ranges)
        {
            // shader is prefered if it is not empty
            auto table_range = table_range_with_shade.shade_.get() != nullptr
                                   ? table_range_with_shade.shade_.get()
                                   : table_range_with_shade.shader_.get();

            const KeyT *start_key =
                static_cast<const KeyT *>(table_range->start_key_.get());

            // start_key_ nullptr stands for neg_inf
            if (start_key == nullptr)
            {
                RangeRecord *neg_inf_rec =
                    TemplateCcMap<KeyT, RangeRecord>::neg_inf_.payload_.get();
                neg_inf_rec->range_entry_ = table_range;
                TemplateCcMap<KeyT, RangeRecord>::neg_inf_.payload_status_ =
                    RecordStatus::Normal;
                TemplateCcMap<KeyT, RangeRecord>::neg_inf_.commit_ts_ = 1;
                continue;
            }

            CcEntry<KeyT, RangeRecord> *cce =
                TemplateCcMap<KeyT, RangeRecord>::FindEmplace(
                    *start_key, table_range->version_ts_);
            cce->commit_ts_ = table_range->version_ts_;
            cce->payload_.get()->range_entry_ = table_range;
            cce->payload_status_ = RecordStatus::Normal;
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
        CcHandlerResult<ReadKeyResult> *hd_result = req.Result();
        int64_t ng_term = Sharder::Instance().LeaderTerm(req.NodeGroupId());
        if (ng_term < 0)
        {
            hd_result->SetError(-1);
            return true;
        }

        // For range cc maps, we assume that all of a table's ranges are loaded
        // into memory for caching when the range cc map is initialized. There
        // is never a read-outside request that brings an individual range into
        // memory for caching.
        assert(req.Type() != ReadType::OutsideNormal);

        CcEntry<KeyT, RangeRecord> *floor_cce = nullptr;

        LockType acquired_lock;
        LockOpStatus lock_op_status;
        if (req.CcePtr() != nullptr)
        {
            // The request was blocked before. This is execution resumption
            // after the request is unblocked. The read lock/intention must have
            // been acquired.
            floor_cce = static_cast<CcEntry<KeyT, RangeRecord> *>(req.CcePtr());

            CcOperation cc_op = req.IsForWrite() ? CcOperation::ReadForWrite
                                                 : CcOperation::Read;
            acquired_lock =
                LockHandleForResumedRequest(&req,
                                            req.TxTerm(),
                                            floor_cce,
                                            floor_cce->payload_status_,
                                            cc_op,
                                            req.Isolation(),
                                            req.Protocol());
            lock_op_status = LockOpStatus::Successful;
        }
        else
        {
            // Rather than looking for an exact match, looks up the floor key
            // that represents the range containing the input key.
            const KeyT *look_key = static_cast<const KeyT *>(req.Key());
            floor_cce = Floor(*look_key);
            req.SetCcePtr(floor_cce);

            // try to acquire lock
            int64_t tx_term = req.TxTerm();
            uint32_t ng_id = req.NodeGroupId();
            IsolationLevel iso_lvl = req.Isolation();
            CcProtocol cc_proto = req.Protocol();
            CcOperation cc_op = req.IsForWrite() ? CcOperation::ReadForWrite
                                                 : CcOperation::Read;
            std::tie(acquired_lock, lock_op_status) =
                AcquireCceKeyLock(floor_cce,
                                  floor_cce->payload_status_,
                                  &req,
                                  ng_id,
                                  ng_term,
                                  tx_term,
                                  cc_op,
                                  iso_lvl,
                                  cc_proto);
        }

        // after acquiring lock
        switch (lock_op_status)
        {
        case LockOpStatus::Successful:
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
        case LockOpStatus::Failed:
        {
            return true;
        }
        case LockOpStatus::Blocked:
        {
            // You don't need a remote acknowledge here, since range read is
            // a local read anyway
            return false;
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
            req.Result()->SetError(-1);
        }

        // When the commit ts is 0, the request commits nothing and only
        // removes the write intents/locks acquired earlier.
        if (req.CommitTs() == 0)
        {
            return TemplateCcMap<KeyT, RangeRecord>::Execute(req);
        }

        // Prepare RangeRecord
        RangeRecord *upload_range_rec = nullptr;
        if (req.Payload() != nullptr)
        {
            // When the request comes from a tx in the same node, the
            // request references a schema record in the tx's space.
            upload_range_rec = static_cast<RangeRecord *>(req.Payload());
        }
        else
        {
            assert(req.PayloadStr() != nullptr);
            std::unique_ptr<RangeRecord> decoded_rec =
                std::make_unique<RangeRecord>();
            size_t offset = 0;
            decoded_rec->Deserialize(req.PayloadStr()->data(), offset);
            upload_range_rec = decoded_rec.get();
            req.SetDecodedPayload(std::move(decoded_rec));
        }

        const TableRangeEntry *range_entry = upload_range_rec->RangeEntry();
        int32_t partition_id = range_entry->partition_id_;
        int32_t new_partition_id = range_entry->new_partition_id_;

        if (req.CommitType() == PostWriteType::PrepareCommit)
        {
            // update the local shards' table range value when the current shard
            // is 0
            if (shard_->core_id_ == 0)
            {
                TableRangeEntry *range_entry_vo = const_cast<TableRangeEntry *>(
                    upload_range_rec->RangeEntry());
                // upload the dirty range attributes to local cc shards
                const TableRangeEntryWithShade *range_entry_shade =
                    shard_->CreateDirtyTableRange(
                        this->table_name_,
                        range_entry_vo->partition_id_,
                        std::move(range_entry_vo->new_key_),
                        range_entry_vo->new_partition_id_,
                        req.CommitTs());

                // point the range rec to the dirty range_entry_shade for range
                // cc map rec
                upload_range_rec->range_entry_ =
                    range_entry_shade->shade_.get();
            }
            else
            {
                // simply reset the binary_value_ to the local shards dirty
                // table range, and update the cc map
                upload_range_rec->range_entry_ =
                    shard_->GetTableEffectiveRangeEntry(this->table_name_,
                                                        partition_id);
            }
        }
        else if (req.CommitType() == PostWriteType::PostCommit)
        {
            const TableRangeEntry *old_range_entry = nullptr;
            const TableRangeEntry *new_range_entry = nullptr;
            if (shard_->core_id_ == 0)
            {
                // commit dirty range, old_range_entry switch back to shader
                std::pair<TableRangeEntry *, TableRangeEntry *> entries =
                    shard_->CommitDirtyTableRange(
                        this->table_name_, partition_id, req.CommitTs());
                old_range_entry = entries.first;
                new_range_entry = entries.second;
            }
            else
            {
                // switch old range rec pointer from shade_ to shader_
                old_range_entry = shard_->GetTableEffectiveRangeEntry(
                    this->table_name_, partition_id);
                // get the new range entry added by shard 0

                // TODO(XiaoJi):
                // upload_range_rec->RangeEntry()->new_partition_id_ has been
                // reset to -1 when shard#0 CommitDirtyTableRange
                new_range_entry = shard_->GetTableEffectiveRangeEntry(
                    this->table_name_, new_partition_id);
                assert(new_range_entry != nullptr);
            }

            // Reset old range entry pointer to range entry shader, executed in
            // the TemplateCcMap::Execute()
            upload_range_rec->range_entry_ = old_range_entry;

            // add new range entry to range cc map
            const KeyT *start_key =
                static_cast<const KeyT *>(new_range_entry->start_key_.get());

            CcEntry<KeyT, RangeRecord> *cce =
                TemplateCcMap<KeyT, RangeRecord>::FindEmplace(
                    *start_key, new_range_entry->version_ts_);

            cce->commit_ts_ = new_range_entry->version_ts_;
            cce->payload_.get()->range_entry_ = new_range_entry;
            cce->payload_status_ = RecordStatus::Normal;

            // Clean shade from the dirty old partition entry
            if (shard_->core_id_ == shard_->core_cnt_ - 1)
            {
                // clear shade_ on old partition
                shard_->PostCommitDirtyTableRange(this->table_name_,
                                                  partition_id);
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
            req.Result()->SetError(-1);
            return false;
        }

        // restore the SplitRangeOpMessage
        const std::string_view &content = req.LogContentView();
        ::txlog::SplitRangeOpMessage ds_split_range_op_msg;
        ds_split_range_op_msg.ParseFromArray(content.data(), content.length());

        const TableSchema *table_schema = req.GetTableSchema();

        // Restore new_range_key, which can't be neg or pos inf
        size_t offset = 0;
        std::unique_ptr<KeyT> new_range_key = std::make_unique<KeyT>();
        new_range_key->Deserialize(
            const_cast<char *>(ds_split_range_op_msg.new_range_key().c_str()),
            offset,
            this->KeySchema());

        offset = 0;
        std::unique_ptr<KeyT> new_range_key_for_recovery =
            std::make_unique<KeyT>();
        new_range_key_for_recovery->Deserialize(
            const_cast<char *>(ds_split_range_op_msg.new_range_key().c_str()),
            offset,
            this->KeySchema());

        // Restore partition partition id
        int32_t partition_id = ds_split_range_op_msg.partition_id();
        int32_t new_partition_id = ds_split_range_op_msg.new_partition_id();

        // Restore stage
        ::txlog::SplitRangeOpMessage_Stage stage =
            ds_split_range_op_msg.stage();

        uint32_t tx_node_id = (req.Txn() >> 32L) >> 10;
        int64_t tx_candidate_term =
            Sharder::Instance().CandidateLeaderTerm(tx_node_id);

        // Restore local_cc_shards state at core 0
        TableRangeEntry *old_table_range_entry = nullptr;
        if (shard_->core_id_ == 0)
        {
            if (stage == ::txlog::SplitRangeOpMessage::PrepareDirtyOldRange)
            {
                const TableRangeEntryWithShade *range_entry_shade =
                    shard_->GetTableRangeWithShade(CcMap::table_name_,
                                                   partition_id);
                old_table_range_entry = range_entry_shade->shader_.get();
            }

            if (stage > ::txlog::SplitRangeOpMessage::PrepareDirtyOldRange &&
                stage < ::txlog::SplitRangeOpMessage::DeletingOldRangeData)
            {
                // upload the dirty range attributes to local cc shards
                const TableRangeEntryWithShade *range_entry_shade =
                    shard_->CreateDirtyTableRange(CcMap::table_name_,
                                                  partition_id,
                                                  std::move(new_range_key),
                                                  new_partition_id,
                                                  req.CommitTs());
                old_table_range_entry = range_entry_shade->shade_.get();
            }

            if (stage >= ::txlog::SplitRangeOpMessage::CommitOldRangeNewRange &&
                stage < ::txlog::SplitRangeOpMessage::DeletingOldRangeData)
            {
                // commit dirty range, old_range_entry switch back to shader
                std::pair<TableRangeEntry *, TableRangeEntry *> entries =
                    shard_->CommitDirtyTableRange(
                        CcMap::table_name_, partition_id, req.CommitTs());
                old_table_range_entry = entries.first;
            }
        }
        else
        {
            if (stage >= ::txlog::SplitRangeOpMessage::PrepareDirtyOldRange &&
                stage < ::txlog::SplitRangeOpMessage::DeletingOldRangeData)
            {
                const TableRangeEntryWithShade *table_range_entry_with_shard =
                    shard_->GetTableRangeWithShade(CcMap::table_name_,
                                                   partition_id);
                old_table_range_entry =
                    table_range_entry_with_shard->shade_.get();
            }
            else if (stage ==
                     ::txlog::SplitRangeOpMessage::CommitOldRangeNewRange)
            {
                const TableRangeEntryWithShade
                    *old_table_range_entry_with_shard =
                        shard_->GetTableRangeWithShade(CcMap::table_name_,
                                                       partition_id);
                old_table_range_entry =
                    old_table_range_entry_with_shard->shader_.get();
            }
        }

        // Restore range cc map state
        CcEntry<KeyT, RangeRecord> *old_range_cce = nullptr;

        if (ds_split_range_op_msg.range_key_neg_inf() == true)
        {
            old_range_cce = &neg_inf_;
        }
        else if (ds_split_range_op_msg.range_key_pos_inf() == true)
        {
            old_range_cce = &pos_inf_;
        }
        else
        {
            std::unique_ptr<KeyT> range_tx_key = std::make_unique<KeyT>();
            range_tx_key->Deserialize(
                const_cast<char *>(
                    ds_split_range_op_msg.range_key_value().c_str()),
                offset,
                this->KeySchema());
            auto it = ccm_.find(*range_tx_key.get());
            assert(it != ccm_.end());
            old_range_cce = &it->second;
        }

        // Restore old range
        if (stage <= ::txlog::SplitRangeOpMessage::PrepareDirtyOldRange)
        {
            old_range_cce->payload_->range_entry_ = old_table_range_entry;
        }

        // Recover locks on range cce
        if (stage == ::txlog::SplitRangeOpMessage::PrepareDirtyOldRange ||
            stage == ::txlog::SplitRangeOpMessage::CommitOldRangeNewRange)
        {
            // Add write lock on old range cce
            bool success = old_range_cce->GetKeyLock().AcquireWriteLock(
                &req, CcProtocol::Locking);
            assert(success);
        }
        else if (stage == ::txlog::SplitRangeOpMessage::CopingOldRangeData)
        {
            // Add write intention on old range cce
            bool success = old_range_cce->GetKeyLock().AcquireWriteIntent(
                &req, CcProtocol::Locking);
            assert(success);
        }

        // Move to next core
        if (shard_->core_id_ < shard_->core_cnt_ - 1)
        {
            req.ResetCcm();
            MoveRequest(&req, shard_->core_id_ + 1);
        }
        else
        {
            std::unique_ptr<RangeRecord> old_range_record =
                std::make_unique<RangeRecord>(*old_range_cce->payload_.get());
            // Restore transaction and catalog read lock at last core if this is
            // the recovering node group is the tx coordinator
            if (tx_node_id == req.NodeGroupId() && tx_candidate_term >= 0)
            {
                shard_->local_shards_.CreateSplitRangeRecoveryTx(
                    ds_split_range_op_msg,
                    table_schema,
                    old_range_cce->key_,
                    std::move(old_range_record),
                    partition_id,
                    std::move(new_range_key_for_recovery),
                    new_partition_id,
                    tx_node_id,
                    req.Txn(),
                    tx_candidate_term,
                    req.CommitTs());
            }

            req.SetFinish();
        }

        return true;
    }

    TableType Type() const override
    {
        return TableType::RangePartition;
    }
};
}  // namespace txservice
