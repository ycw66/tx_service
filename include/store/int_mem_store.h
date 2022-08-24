#pragma once

#include <map>
#include <memory>  //unique_ptr
#include <string>
#include <utility>  //pair
#include <vector>

#include "store/data_store_handler.h"
#include "tx_key.h"     //CompositeKey
#include "tx_record.h"  //CompositeRecord,VersionTxRecord

namespace txservice::store
{
class IntMemoryStore : public DataStoreHandler
{
public:
    IntMemoryStore()
    {
    }

    bool Connect() override
    {
        return true;
    }

    bool PutAll(const TableName &table_name,
                std::vector<FlushRecord> &batch,
                const Schema *key_schema,
                const Schema *rec_schema,
                uint64_t schema_ts,
                uint32_t node_group,
                DsRangeEvaluateOperationService
                    *ds_range_evaluate_operation_service) override
    {
        for (const auto &ref : batch)
        {
            CcEntry<CompositeKey<int>, CompositeRecord<int>> *cce =
                static_cast<CcEntry<CompositeKey<int>, CompositeRecord<int>> *>(
                    ref.cce_);

            const CompositeKey<int> &key = *cce->key_;
            const CompositeRecord<int> &rec = *ref.Payload();

            int key_val = std::get<0>(key.Tuple());
            if (ref.payload_status_ == RecordStatus::Deleted)
            {
                int_store_.erase(key_val);
            }
            else
            {
                int rec_val = std::get<0>(rec.Tuple());
                int_store_.insert_or_assign(key_val, rec_val);
            }

            if (int_store_.size() > 1000)
            {
                int_store_.erase(int_store_.begin());
            }
        }

        return true;
    }

    bool PutSkAll(const TableName &table_name,
                  std::vector<FlushRecord> &batch,
                  const SecondaryKeySchema *sk_schema,
                  uint64_t schema_ts,
                  uint32_t node_group) override
    {
        assert(false);
        return false;
    }

    void UpsertTable(
        const txservice::TableName &ccm_table_name,
        const txservice::TableSchema *table_schema,
        const std::vector<txservice::TableName> *indexes,
        bool is_deleted,
        uint64_t commit_ts,
        txservice::CcHandlerResult<txservice::Void> *hd_res) override
    {
    }

    void FetchTableCatalog(const TableName &ccm_table_name,
                           void *fetch_req) override
    {
    }

    void FetchTableRanges(const TableName &range_table_name,
                          void *fetch_req) override
    {
    }

    bool Read(const txservice::TableName &table_name,
              const txservice::TxKey &key,
              txservice::TxRecord &rec,
              bool &found,
              uint64_t &version_ts,
              const txservice::Schema *key_schema,
              const txservice::Schema *rec_schema,
              uint64_t table_schema_ts) override
    {
        assert(false);
        return false;
    }

    bool FetchTable(const txservice::TableName &table_name,
                    std::string &schema_image,
                    bool &found,
                    uint64_t &version_ts) const override
    {
        assert(false);
        return false;
    }

    bool FetchTable(const txservice::TableName &table_name,
                    std::string &schema_image,
                    bool &found) const override
    {
        assert(false);
        return false;
    }

    bool DiscoverAllTableNames(
        std::vector<std::string> &norm_name_vec) const override
    {
        assert(false);
        return false;
    }

    //-- database
    bool UpsertDatabase(std::string_view db,
                        std::string_view definition) const override
    {
        assert(false);
        return false;
    }
    bool DropDatabase(std::string_view db) const override
    {
        assert(false);
        return false;
    }
    bool FetchDatabase(std::string_view db,
                       std::string &definition,
                       bool &found) const override
    {
        assert(false);
        return false;
    }
    bool FetchAllDatabase(std::vector<std::string> &dbnames) const override
    {
        assert(false);
        return false;
    }

    std::unique_ptr<DataStoreScanner> ScanForward(
        const txservice::TableName &table_name,
        const txservice::TxKey &start_key,
        bool inclusive,
        uint8_t key_parts,
        const std::string &search_cond,
        const txservice::Schema *key_schema,
        const txservice::Schema *rec_schema,
        bool scan_foward) override
    {
        assert(false);
        return nullptr;
    }

    bool GetRangeSize(const TableName &table_name,
                      int32_t partition_id,
                      int64_t *size)
    {
        return true;
    }

    bool FindRangeMedianKey(
        const TableName &table_name,
        int32_t partition_id,
        const txservice::Schema *key_schema,
        txservice::CcHandlerResult<RangeMedianKeyResult> *out_median_key_result)
    {
        return true;
    }

    bool CopyRangeData(const TableName &table_name,
                       int32_t old_partition_id,
                       int32_t new_partition_id,
                       const txservice::TxKey *start_key,
                       uint64_t tx_ts,
                       const txservice::Schema *key_schema,
                       const txservice::Schema *rec_schema)
    {
        return true;
    }

    bool DeleteOutOfRangeData(const TableName &table_name,
                              int32_t partition_id,
                              const TxKey *start_key,
                              const txservice::Schema *key_schema)
    {
        return true;
    }

    bool GetNextRangePartitionId(const txservice::TableName &tablename,
                                 int32_t *out_next_partition_id,
                                 int retry_count = 5)
    {
        return true;
    }

    bool UpsertRange(const TableName &range_table_name,
                     const Schema *key_schema,
                     TxKey *key,
                     int32_t partition_id,
                     int64_t ts)
    {
        return true;
    }

    /**
     * @brief Write batch historical versions into DataStore.
     *
     */
    bool PutArchivesAll(uint32_t node_group,
                        const txservice::TableName &table_name,
                        std::vector<txservice::FlushRecord> &batch) override
    {
        assert(false);
        return true;
    }

    /**
     * @brief  Get the latest visible(commit_ts <= upper_bound_ts) historical
     * version.
     */
    bool FetchVisibleArchive(const txservice::TableName &table_name,
                             const txservice::TxKey &key,
                             const uint64_t upper_bound_ts,
                             txservice::TxRecord &rec,
                             txservice::RecordStatus &rec_status,
                             uint64_t &commit_ts) override
    {
        auto &typed_key = dynamic_cast<const CompositeKey<int> &>(key);
        int int_key = std::get<0>(typed_key.Tuple());
        auto &ref =
            int_archives_[std::pair<TableName, int>(table_name, int_key)];
        for (size_t i = 0; i < ref.size(); i++)
        {
            if (ref[i].commit_ts_ <= upper_bound_ts)
            {
                rec = *ref[i].record_;
                rec_status = ref[i].record_status_;
                commit_ts = ref[i].commit_ts_;
                return true;
            }
        }

        return false;
    }

    /**
     * @brief  Fetch all archives whose commit_ts >= from_ts.
     */
    bool FetchArchives(const txservice::TableName &table_name,
                       const txservice::TxKey &key,
                       std::vector<txservice::VersionTxRecord> &archives,
                       uint64_t from_ts) override
    {
        auto &typed_key = dynamic_cast<const CompositeKey<int> &>(key);
        int int_key = std::get<0>(typed_key.Tuple());
        auto &ref =
            int_archives_[std::pair<TableName, int>(table_name, int_key)];
        for (size_t i = 0; i < ref.size(); i++)
        {
            if (ref[i].commit_ts_ >= from_ts)
            {
                auto &tmp = archives.emplace_back();
                tmp.commit_ts_ = ref[i].commit_ts_;
                tmp.record_status_ = ref[i].record_status_;
                tmp.record_ = ref[i].record_->Clone();
            }
        }

        return true;
    }

    size_t Size() const
    {
        return int_store_.size();
    }

    int MinKey() const
    {
        return int_store_.begin()->first;
    }

    int MaxKey() const
    {
        return int_store_.rbegin()->first;
    }

private:
    std::map<int, int> int_store_;
    std::map<std::pair<int, int>, Void> int_index_;
    std::map<std::pair<TableName, int>, std::vector<VersionTxRecord>>
        int_archives_;
};
}  // namespace txservice::store
