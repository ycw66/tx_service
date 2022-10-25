#pragma once

#include <vector>  // std::vector

#include "catalog_factory.h"
#include "cc/cc_entry.h"
#include "store/data_store_scanner.h"
#include "tx_key.h"
#include "tx_operation_result.h"
#include "tx_record.h"
#include "type.h"

namespace txservice
{
class TxService;

namespace store
{
struct DataStoreSearchCond
{
    DataStoreSearchCond(std::string field_name,
                        std::string op,
                        std::string val_str,
                        bool is_numeric)
        : field_name_(field_name),
          op_(op),
          val_str_(val_str),
          is_numeric_(is_numeric)
    {
    }

    std::string field_name_;
    std::string op_;
    std::string val_str_;
    bool is_numeric_;
};

class DataStoreHandler
{
public:
    virtual ~DataStoreHandler() = default;

    virtual bool Connect() = 0;

    /**
     * flush entries in @param batch to data store, stop and return false if
     * node_group is no longer leader
     * @param batch
     * @param table_schema
     * @param schema_ts
     * @param node_group
     * @return whether all entries are written to data store successfully
     */
    virtual bool PutAll(std::vector<FlushRecord> &batch,
                        const TableSchema *table_schema,
                        uint64_t schema_ts,
                        uint32_t node_group) = 0;

    /**
     * flush entries in @param batch to data store, stop and return false if
     * node_group is no longer leader
     * @param index_name
     * @param batch
     * @param sk_schema
     * @param schema_ts
     * @param node_group
     * @return whether all entries are written to data store successfully
     */
    virtual bool PutSkAll(const TableName &index_name,
                          std::vector<FlushRecord> &batch,
                          const TableSchema *table_schema,
                          uint64_t schema_ts,
                          uint32_t node_group) = 0;

    virtual void UpsertTable(const TableSchema *table_schema,
                             bool is_deleted,
                             uint64_t commit_ts,
                             CcHandlerResult<Void> *hd_res) = 0;

    virtual void FetchTableCatalog(const TableName &ccm_table_name,
                                   void *fetch_req) = 0;

    virtual void FetchTableRanges(const KVCatalogInfo *kv_info,
                                  void *fetch_req) = 0;

    virtual bool Read(const TableName &table_name,
                      const TxKey &key,
                      TxRecord &rec,
                      bool &found,
                      uint64_t &version_ts,
                      const Schema *key_schema,
                      const Schema *rec_schema,
                      const KVCatalogInfo *kv_info,
                      uint64_t table_schema_ts) = 0;

    virtual bool ReadSk(const TableName &table_name,
                        const TxKey &key,
                        TxRecord &rec,
                        bool &found,
                        uint64_t &version_ts,
                        const Schema *key_schema,
                        const Schema *rec_schema,
                        const KVCatalogInfo *kv_info,
                        uint64_t table_schema_ts) = 0;

    virtual bool FetchTable(const TableName &table_name,
                            std::string &schema_image,
                            bool &found,
                            uint64_t &version_ts) const = 0;

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
                                std::vector<txservice::FlushRecord> &batch) = 0;
    /**
     * @brief Copy record from base/sk table to mvcc_archives.
     */
    virtual bool CopyBaseToArchive(std::vector<LruEntry *> &batch,
                                   uint32_t node_group,
                                   const txservice::TableName &table_name,
                                   const txservice::TableSchema *table_schema,
                                   uint64_t schema_ts,
                                   bool is_sk) = 0;

    /**
     * @brief  Get the latest visible(commit_ts <= upper_bound_ts) historical
     * version.
     */
    virtual bool FetchVisibleArchive(const TableName &table_name,
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
        const txservice::TxKey &key,
        std::vector<txservice::VersionTxRecord> &archives,
        uint64_t from_ts) = 0;

    void SetTxService(TxService *tx_service)
    {
        tx_service_ = tx_service;
    }

    virtual bool GetRangeSize(const txservice::TableName &table_name,
                              const TableSchema *table_schema,
                              int32_t partition_id,
                              int64_t *size) = 0;

    virtual bool FindRangeMedianKey(
        const txservice::TableName &table_name,
        int32_t partition_id,
        const TableSchema *table_schema,
        CcHandlerResult<RangeMedianKeyResult> *out_median_key_result) = 0;

    virtual bool CopyRangeData(const txservice::TableName &table_name,
                               int32_t old_partition_id,
                               int32_t new_partition_id,
                               const TxKey *start_key,
                               uint64_t tx_ts,
                               const TableSchema *table_schema) = 0;

    virtual bool DeleteOutOfRangeData(const txservice::TableName &table_name,
                                      int32_t partition_id,
                                      const TxKey *start_key,
                                      const TableSchema *table_schema) = 0;

    virtual bool GetNextRangePartitionId(const TableName &tablename,
                                         int32_t *out_next_partition_id,
                                         int retry_count = 5) = 0;

    virtual bool UpsertRange(const txservice::TableName &table_name,
                             const TableSchema *table_schema,
                             TxKey *key,
                             int32_t partition_id,
                             int64_t ts) = 0;

    virtual std::string CreateKVCatalogInfo(
        const TableSchema *table_schema) const = 0;

    virtual KVCatalogInfo::uptr DeserializeKVCatalogInfo(
        const std::string &kv_info_str, size_t &offset) const = 0;

protected:
    TxService *tx_service_;
};
}  // namespace store
}  // namespace txservice
