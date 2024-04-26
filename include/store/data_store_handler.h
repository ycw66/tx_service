#pragma once

#include <cstddef>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog_factory.h"
#include "cc/cc_entry.h"
#include "cc_handler_result.h"
#include "range_record.h"
// #include "range_slice.h"
#include "store/data_store_scanner.h"
#include "tx_key.h"
#include "tx_record.h"
#include "tx_service_metrics.h"
#include "type.h"

namespace txservice
{
class TxService;
struct FetchCatalogCc;
struct FetchTableStatisticsCc;
struct FetchTableRangesCc;
struct SliceDataItem;
class StoreSlice;
struct LoadRangeSliceRequest;
struct FetchRecordCc;

namespace store
{
enum class DataStoreDataType
{
    Blob,
    Numeric,
    String,
    Bool
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
    enum struct LoadRangeSliceStatus
    {
        Success = 0,
        Retry,
        Error
    };

    virtual ~DataStoreHandler() = default;

    virtual bool Connect() = 0;

    virtual void ScheduleTimerTasks(){};

    /**
     * Initialize cluster config based on the based in ips and ports. This
     * should only be called during bootstrap.
     */
    virtual bool InitializeClusterConfig(
        const std::vector<std::string> &ips,
        const std::vector<uint16_t> &ports,
        const uint16_t ng_rep_cnt,
        std::unordered_map<uint32_t, std::vector<NodeConfig>> &ng_configs,
        int32_t &seed) = 0;

    /**
     * Read cluster config from kv store cluster config table.
     */
    virtual bool ReadClusterConfig(
        std::unordered_map<uint32_t, std::vector<NodeConfig>> &ng_configs,
        uint64_t &version,
        int32_t &seed,
        bool &uninitialized) = 0;

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

    /**
     * @brief indicate end of flush entries in a single ckpt for \@param batch
     * to base table or skindex table in data store, stop and return false if
     * node_group is not longer leader.
     * @param batch
     * @param table_name base table name or sk index name
     * @param table_schema
     * @param node_group
     * @param version
     * @return whether all entries are written to data store successfully
     */
    virtual bool CkptEnd(const txservice::TableName &table_name,
                         const txservice::TableSchema *table_schema,
                         uint32_t node_group,
                         uint64_t version)
    {
        return true;
    }

    virtual void UpsertTable(
        const TableSchema *table_schema,
        OperationType op_type,
        uint64_t commit_ts,
        NodeGroupId ng_id,
        int64_t tx_term,
        CcHandlerResult<Void> *hd_res,
        const txservice::AlterTableInfo *alter_table_info = nullptr) = 0;

    virtual void FetchTableCatalog(const TableName &ccm_table_name,
                                   FetchCatalogCc *fetch_cc) = 0;

    virtual void FetchTableRanges(FetchTableRangesCc *fetch_cc) = 0;

    virtual void FetchRangeSlices(FetchRangeSlicesReq *fetch_cc) = 0;

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

    // Fetch record from datastore asynchronously.
    virtual void FetchRecord(const TableName &table_name,
                             const TxKey *key,
                             FetchRecordCc *fetch_cc)
    {
        assert(false);
    }

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

    virtual void FetchCurrentTableStatistics(
        const TableName &ccm_table_name, FetchTableStatisticsCc *fetch_cc) = 0;

    virtual void FetchTableStatistics(const TableName &ccm_table_name,
                                      FetchTableStatisticsCc *fetch_cc) = 0;

    virtual bool UpsertTableStatistics(
        const TableName &ccm_table_name,
        const std::unordered_map<TableName,
                                 std::pair<uint64_t, std::vector<TxKey>>>
            &sample_pool_map,
        uint64_t version) = 0;

    virtual LoadRangeSliceStatus LoadRangeSlice(
        const TableName &table_name,
        const KVCatalogInfo *kv_info,
        uint32_t partition_id,
        LoadRangeSliceRequest *load_slice_req)
    {
        return LoadRangeSliceStatus::Error;
    }

    virtual bool UpdateRangeSlices(const TableName &table_name,
                                   uint64_t schema_ts,
                                   TxKey range_start_key,
                                   std::vector<const StoreSlice *> slices,
                                   bool update_slice_keys)
    {
        return false;
    }

    /**
     * @brief Upsert list of ranges into range table. This will also update
     * range slice sizes.
     */
    virtual bool UpsertRanges(const TableName &table_name,
                              std::vector<SplitRangeInfo> range_info,
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

    virtual bool DropKvTable(const std::string &kv_table_name) const = 0;

    virtual void DropKvTableAsync(const std::string &kv_table_name) const = 0;

    virtual std::unique_ptr<DataStoreScanner> ScanForward(
        const TableName &table_name,
        uint32_t ng_id,
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
        std::vector<TxKey> &batch,
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

    void RegisterKvMetrics(metrics::MetricsRegistry *metrics_registry,
                           metrics::CommonLabels common_labels = {})
    {
        assert(metrics_registry);
        if (metrics::enable_kv_metrics)
        {
            metrics::kv_meter = std::make_unique<metrics::Meter>(
                metrics_registry, common_labels);
            metrics::kv_meter->Register(metrics::NAME_KV_FLUSH_ROWS_TOTAL,
                                        metrics::Type::Counter,
                                        {{"type", {"base", "archive"}}});
            metrics::kv_meter->Register(metrics::NAME_KV_LOAD_SLICE_TOTAL,
                                        metrics::Type::Counter);
            metrics::kv_meter->Register(metrics::NAME_KV_LOAD_SLICE_DURATION,
                                        metrics::Type::Histogram);
            metrics::kv_meter->Register(metrics::NAME_KV_READ_TOTAL,
                                        metrics::Type::Counter);
            metrics::kv_meter->Register(metrics::NAME_KV_READ_DURATION,
                                        metrics::Type::Histogram);
        }
    };

    virtual bool UpdateClusterConfig(
        const std::unordered_map<uint32_t, std::vector<NodeConfig>> &new_cnf,
        uint64_t version) = 0;

    virtual bool NeedCopyRange() const = 0;

protected:
    TxService *tx_service_{nullptr};
};
}  // namespace store
}  // namespace txservice
