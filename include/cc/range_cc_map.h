#pragma once

#include <map>
#include <string>

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

    /**
     * @brief Construct a new range cc map object. The range cc map has no
     * schema, so the schema's timestamp is set to 1 (the beginning of history).
     *
     * @param range_table_name
     * @param shard
     */
    RangeCcMap(const TableName &range_table_name, CcShard *shard)
        : TemplateCcMap<KeyT, RangeRecord>(shard, 1)
    {
        const std::map<uint32_t, TableRangeEntry> *ranges =
            CcMap::shard_->GetTableRanges(range_table_name);
        assert(ranges != nullptr);

        for (const auto &[partition_id, table_range] : *ranges)
        {
            if (partition_id == 0)
            {
                RangeRecord &neg_inf_rec =
                    TemplateCcMap<KeyT, RangeRecord>::neg_inf_.payload_;
                neg_inf_rec.binary_value_ = &table_range;
                TemplateCcMap<KeyT, RangeRecord>::neg_inf_.payload_status_ =
                    RecordStatus::Normal;
                continue;
            }

            const KeyT *start_key =
                static_cast<const KeyT *>(table_range.start_key_.get());

            CcEntry<KeyT, RangeRecord> *cce =
                TemplateCcMap<KeyT, RangeRecord>::Emplace(
                    *start_key, table_range.version_ts_, true);

            cce->commit_ts_ = table_range.version_ts_;
            cce->payload_.binary_value_ = &table_range;
            cce->payload_status_ = RecordStatus::Normal;
        }
    }

    using TemplateCcMap<KeyT, RangeRecord>::Execute;
    using TemplateCcMap<KeyT, RangeRecord>::ReadLockCce;

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
            floor_cce = TemplateCcMap<KeyT, RangeRecord>::Floor(*look_key);
            req.SetCcePtr(floor_cce);

            int64_t tx_term = req.TxTerm();
            uint32_t cce_node_group_id = req.NodeGroupId();

            bool lock_success = TemplateCcMap<KeyT, RangeRecord>::ReadLockCce(
                floor_cce, req, tx_term, cce_node_group_id);
            if (lock_success)
            {
                CcEntryAddr &cce_addr = hd_result->Value().cce_addr_;
                cce_addr.SetCce(reinterpret_cast<uint64_t>(floor_cce),
                                ng_term,
                                req.NodeGroupId());

                RangeRecord *range_rec =
                    static_cast<RangeRecord *>(req.Record());
                *range_rec = floor_cce->payload_;
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
};
}  // namespace txservice
