#pragma once

#include <string>
#include <utility>
#include <vector>  // std::vector

#include "catalog_factory.h"
#include "cc/cc_entry.h"
#include "metrics.h"
#include "range_slice.h"
#include "store/data_store_scanner.h"
#include "tx_key.h"
#include "tx_operation_result.h"
#include "tx_record.h"
#include "type.h"

namespace txservice
{
class TxService;
struct SliceDataItem;
class StoreSlice;
struct LoadRangeSliceRequest;

namespace store
{
enum class DataStoreDataType
{
    Blob,
    Numeric,
    String
};

struct DataStoreSearchCond
{
    DataStoreSearchCond(std::string field_name,
                        std::string op,
                        std::string val_str,
                        DataStoreDataType data_type)
        : field_name_(field_name),
          op_(op),
          val_str_(val_str),
          data_type_(data_type)
    {
    }

    std::string field_name_;
    std::string op_;
    std::string val_str_;
    DataStoreDataType data_type_;
};

class DataStoreHandler
{
public:
    virtual ~DataStoreHandler() = default;

    virtual bool Connect() = 0;

    /**
     * @brief flush entries in \@param batch to base table or skindex table in
     * data store, stop and return false if node_group is not longer leader.
     * @param batch
     * @param table_name base table name or sk index name
     * @param table_schema
     * @param node_group
     * @return whether all entries are written to data store successfully
     */
    virtual bool PutAll(std::vector<txservice::FlushRecord> &batch,
                        const txservice::TableName &table_name,
                        const txservice::TableSchema *table_schema,
                        uint32_t node_group) = 0;

    virtual void UpsertTable(
        const TableSchema *table_schema,
        OperationType op_type,
        uint64_t commit_ts,
        CcHandlerResult<Void> *hd_res,
        const txservice::AlterTableInfo *alter_table_info = nullptr) = 0;

    virtual void FetchTableCatalog(const TableName &ccm_table_name,
                                   void *fetch_req) = 0;

    virtual void FetchTableRanges(const KVCatalogInfo *kv_info,
                                  void *fetch_req) = 0;

    /**
     * @brief Read a row from base table or skindex table in datastore with
     * specified key. Caller should pass in complete primary key or skindex key.
     */
    virtual bool Read(const txservice::TableName &table_name,
                      const txservice::TxKey &key,
                      txservice::TxRecord &rec,
                      bool &found,
                      uint64_t &version_ts,
                      const txservice::TableSchema *table_schema) = 0;

    virtual bool FetchTable(const TableName &table_name,
                            std::string &schema_image,
                            bool &found,
                            uint64_t &version_ts) const = 0;

    bool FetchTable(const txservice::TableName &table_name,
                    std::string &schema_image,
                    bool &found) const
    {
        uint64_t version_ts;
        return FetchTable(table_name, schema_image, found, version_ts);
    }

    virtual void FetchCurrentTableStatistics(const TableName &ccm_table_name,
                                             void *fetch_req) = 0;

    virtual void FetchTableStatistics(const TableName &ccm_table_name,
                                      void *fetch_req) = 0;

    virtual bool UpsertTableStatistics(
        const TableName &ccm_table_name,
        const std::unordered_map<TableName,
                                 std::pair<uint64_t, std::vector<TxKey::Uptr>>>
            &sample_pool_map,
        uint64_t version) = 0;

    virtual bool LoadRangeSlice(const TableName &table_name,
                                const KVCatalogInfo *kv_info,
                                uint32_t partition_id,
                                LoadRangeSliceRequest *load_slice_req)
    {
        return false;
    }

    virtual bool UpdateRangeSlices(
        const TableName &table_name,
        uint64_t schema_ts,
        const TxKey *range_start_key,
        const std::vector<std::unique_ptr<StoreSlice>> &slices,
        bool update_slice_keys)
    {
        return false;
    }

    /**
     * @brief Upsert list of ranges into range table. This will also update
     * range slice sizes.
     */
    virtual bool UpsertRanges(
        const TableName &table_name,
        std::vector<
            std::tuple<const TxKey *, int32_t, std::vector<StoreSlice *>>>
            range_info,
        uint64_t version)
    {
        return false;
    }

    virtual bool DiscoverAllTableNames(
        std::vector<std::string> &norm_name_vec) const = 0;

    //-- database
    virtual bool UpsertDatabase(std::string_view db,
                                std::string_view definition) const = 0;
    virtual bool DropDatabase(std::string_view db) const = 0;
    virtual bool FetchDatabase(std::string_view db,
                               std::string &definition,
                               bool &found) const = 0;
    virtual bool FetchAllDatabase(std::vector<std::string> &dbnames) const = 0;

    virtual std::unique_ptr<DataStoreScanner> ScanForward(
        const TableName &table_name,
        const TxKey &start_key,
        bool inclusive,
        uint8_t key_parts,
        const std::vector<DataStoreSearchCond> &search_cond,
        const Schema *key_schema,
        const Schema *rec_schema,
        const KVCatalogInfo *kv_info,
        bool scan_foward) = 0;

    /**
     * @brief Write batch historical versions into DataStore.
     */
    virtual bool PutArchivesAll(uint32_t node_group,
                                const txservice::TableName &table_name,
                                const txservice::KVCatalogInfo *kv_info,
                                std::vector<txservice::FlushRecord> &batch) = 0;
    /**
     * @brief Copy record from base/sk table to mvcc_archives.
     */
    virtual bool CopyBaseToArchive(
        std::vector<const TxKey *> &batch,
        uint32_t node_group,
        const txservice::TableName &table_name,
        const txservice::TableSchema *table_schema) = 0;

    /**
     * @brief  Get the latest visible(commit_ts <= upper_bound_ts) historical
     * version.
     */
    virtual bool FetchVisibleArchive(const TableName &table_name,
                                     const txservice::KVCatalogInfo *kv_info,
                                     const TxKey &key,
                                     const uint64_t upper_bound_ts,
                                     TxRecord &rec,
                                     RecordStatus &rec_status,
                                     uint64_t &commit_ts) = 0;

    /**
     * @brief  Fetch all archives whose commit_ts >= from_ts.
     */
    virtual bool FetchArchives(
        const txservice::TableName &table_name,
        const txservice::KVCatalogInfo *kv_info,
        const txservice::TxKey &key,
        std::vector<txservice::VersionTxRecord> &archives,
        uint64_t from_ts) = 0;

    void SetTxService(TxService *tx_service)
    {
        tx_service_ = tx_service;
    }

    virtual bool CopyRangeData(
        const txservice::TableName &table_name,
        int32_t old_partition_id,
        const TxKey *old_end_key,
        std::vector<std::pair<TxKey::Uptr, int32_t>> &new_partition_info,
        uint64_t tx_ts,
        const TableSchema *table_schema) = 0;

    virtual bool DeleteOutOfRangeData(const txservice::TableName &table_name,
                                      int32_t partition_id,
                                      const TxKey *start_key,
                                      const TableSchema *table_schema) = 0;

    virtual bool GetNextRangePartitionId(const TableName &tablename,
                                         int32_t *out_next_partition_id,
                                         int retry_count = 5) = 0;

    virtual std::string CreateKVCatalogInfo(
        const TableSchema *table_schema) const = 0;

    virtual KVCatalogInfo::uptr DeserializeKVCatalogInfo(
        const std::string &kv_info_str, size_t &offset) const = 0;

    virtual std::string CreateNewKVCatalogInfo(
        const txservice::TableName &table_name,
        const txservice::TableSchema *current_table_schema,
        txservice::AlterTableInfo &alter_table_info) = 0;

    virtual void SetMetricsRegistry(metrics::MetricsRegistry *){};

protected:
    TxService *tx_service_;
};
}  // namespace store
}  // namespace txservice
