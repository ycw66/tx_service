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
    using TemplateCcMap<KeyT, RangeRecord>::ReadLockCce;
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
        : TemplateCcMap<KeyT, RangeRecord>(shard,
                                           range_table_name,
                                           schema_ts,
                                           table_schema->KeySchema(),
                                           nullptr),
          range_table_name_(range_table_name)
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
            if (partition_id == 0)
            {
                RangeRecord *neg_inf_rec =
                    TemplateCcMap<KeyT, RangeRecord>::neg_inf_.payload_.get();
                neg_inf_rec->range_entry_ = table_range;
                TemplateCcMap<KeyT, RangeRecord>::neg_inf_.payload_status_ =
                    RecordStatus::Normal;
                TemplateCcMap<KeyT, RangeRecord>::neg_inf_.commit_ts_ = 1;
                continue;
            }

            const KeyT *start_key =
                static_cast<const KeyT *>(table_range->start_key_.get());

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
        if (req.CcePtr() != nullptr)
        {
            // The request was blocked before. This is execution resumption
            // after the request is unblocked. The read lock/intention must have
            // been acquired.
            floor_cce = static_cast<CcEntry<KeyT, RangeRecord> *>(req.CcePtr());
        }
        else
        {
            // Rather than looking for an exact match, looks up the floor key
            // that represents the range containing the input key.
            const KeyT *look_key = static_cast<const KeyT *>(req.Key());
            floor_cce = Floor(*look_key);
            req.SetCcePtr(floor_cce);

            int64_t tx_term = req.TxTerm();
            uint32_t cce_node_group_id = req.NodeGroupId();

            bool lock_success =
                ReadLockCce(floor_cce, req, tx_term, cce_node_group_id);
            if (lock_success)
            {
                CcEntryAddr &cce_addr = hd_result->Value().cce_addr_;
                cce_addr.SetCce(reinterpret_cast<uint64_t>(floor_cce),
                                ng_term,
                                req.NodeGroupId());

                RangeRecord *range_rec =
                    static_cast<RangeRecord *>(req.Record());
                *range_rec = *(floor_cce->payload_);
                hd_result->Value().ts_ = floor_cce->commit_ts_;
                hd_result->Value().rec_status_ = RecordStatus::Normal;
                hd_result->SetFinished();
                return true;
            }
            else
            {
                TX_TRACE_ACTION_WITH_CONTEXT(
                    &req,
                    "AcquireReadLock.Fail",
                    reinterpret_cast<LruEntry *>(floor_cce),
                    [&req]() -> std::string
                    {
                        return std::string(",\"tx_number\":")
                            .append(std::to_string(req.Txn()))
                            .append(",\"term\":")
                            .append(std::to_string(req.TxTerm()));
                    });
                // You don't need a remote acknowledge here, since range read is
                // a local read anyway
                return false;
            }
        }
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

        // Prepare range key
        const KeyT *range_key = nullptr;
        if (req.Key() != nullptr)
        {
            range_key = static_cast<const KeyT *>(req.Key());
        }
        else
        {
            assert(req.KeyStr() != nullptr);
            std::unique_ptr<KeyT> decoded_key = std::make_unique<KeyT>();
            size_t offset = 0;
            decoded_key->Deserialize(
                req.KeyStr()->data(),
                offset,
                TemplateCcMap<KeyT, RangeRecord>::key_schema_);
            range_key = decoded_key.get();
            req.SetDecodedKey(std::move(decoded_key));
        }

        // When the commit ts is 0, the request commits nothing and only
        // removes the write intents/locks acquired earlier.
        if (req.CommitTs() == 0)
        {
            return TemplateCcMap<KeyT, RangeRecord>::Execute(req);
        }

        // Get the range record in this range cc map
        RangeRecord *cc_map_range_rec = nullptr;
        if (range_key == TemplateCcMap<KeyT, RangeRecord>::neg_inf_.key_)
        {
            cc_map_range_rec =
                TemplateCcMap<KeyT, RangeRecord>::neg_inf_.payload_.get();
        }
        else
        {
            auto cc_map_range_it = ccm_.find(*range_key);
            assert(cc_map_range_it != ccm_.end());
            cc_map_range_rec = cc_map_range_it->second.payload_.get();
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
        int32_t partition_id = upload_range_rec->RangeEntry()->partition_id_;
        int32_t new_partition_id =
            upload_range_rec->RangeEntry()->new_partition_id_;

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
                        range_table_name_,
                        range_entry_vo->partition_id_,
                        std::move(range_entry_vo->new_key_),
                        range_entry_vo->new_partition_id_,
                        req.CommitTs());

                // point the range rec to the dirty range_entry_shade for range
                // cc map rec
                cc_map_range_rec->range_entry_ =
                    range_entry_shade->shade_.get();
            }
            else
            {
                // simplely reset the binary_value_ to the local shards dirty
                // table range, and update the cc map
                const TableRangeEntryWithShade *range_entry_shade =
                    shard_->GetTableRangeWithShade(range_table_name_,
                                                   partition_id);
                cc_map_range_rec->range_entry_ =
                    range_entry_shade->shade_.get();
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
                        range_table_name_, partition_id, req.CommitTs());
                old_range_entry = entries.first;
                new_range_entry = entries.second;
            }
            else
            {
                // switch old range rec pointer from shade_ to shader_
                const TableRangeEntryWithShade *old_partition_shader =
                    shard_->GetTableRangeWithShade(range_table_name_,
                                                   partition_id);
                old_range_entry = old_partition_shader->shader_.get();
                // get the new range entry added by shard 0
                const TableRangeEntryWithShade *new_partition_shader =
                    shard_->GetTableRangeWithShade(range_table_name_,
                                                   new_partition_id);
                new_range_entry = new_partition_shader->shader_.get();
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
                shard_->PostCommitDirtyTableRange(range_table_name_,
                                                  partition_id);
            }
        }

        return TemplateCcMap<KeyT, RangeRecord>::Execute(req);
    }

private:
    TableName range_table_name_;
};
}  // namespace txservice
