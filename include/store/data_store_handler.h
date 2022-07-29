#pragma once

#include "catalog_factory.h"
#include "cc/cc_entry.h"
#include "store/data_store_scanner.h"
#include "tx_key.h"
#include "tx_record.h"
#include "type.h"

namespace txservice
{
class TxService;

namespace store
{
class DataStoreHandler
{
public:
    virtual ~DataStoreHandler() = default;

    virtual bool Connect() = 0;

    /**
     * flush entries in @param batch to data store, stop and return false if
     * node_group is no longer leader
     * @param table_name
     * @param batch
     * @param key_schema
     * @param rec_schema
     * @param schema_ts
     * @param node_group
     * @return whether all entries are written to data store successfully
     */
    virtual bool PutAll(const txservice::TableName &table_name,
                        std::vector<txservice::LruEntry *> &batch,
                        const txservice::Schema *key_schema,
                        const txservice::Schema *rec_schema,
                        uint64_t schema_ts,
                        uint32_t node_group) = 0;

    /**
     * flush entries in @param batch to data store, stop and return false if
     * node_group is no longer leader
     * @param table_name
     * @param batch
     * @param sk_schema
     * @param schema_ts
     * @param node_group
     * @return whether all entries are written to data store successfully
     */
    virtual bool PutSkAll(const txservice::TableName &table_name,
                          std::vector<txservice::LruEntry *> &batch,
                          const txservice::SecondaryKeySchema *sk_schema,
                          uint64_t schema_ts,
                          uint32_t node_group) = 0;

    virtual void UpsertTable(
        const txservice::TableName &ccm_table_name,
        const txservice::TableSchema *table_schema,
        const std::vector<txservice::TableName> *indexes,
        bool is_deleted,
        uint64_t commit_ts,
        txservice::CcHandlerResult<txservice::Void> *hd_res) = 0;

    virtual void FetchTableCatalog(const txservice::TableName &ccm_table_name,
                                   void *fetch_req) = 0;

    virtual void FetchTableRanges(const txservice::TableName &range_table_name,
                                  void *fetch_req) = 0;

    virtual bool Read(const txservice::TableName &table_name,
                      const txservice::TxKey &key,
                      txservice::TxRecord &rec,
                      bool &found,
                      uint64_t &version_ts,
                      const txservice::Schema *key_schema,
                      const txservice::Schema *rec_schema,
                      uint64_t table_schema_ts) = 0;

    virtual bool FetchTable(const txservice::TableName &table_name,
                            std::string &schema_image,
                            bool &found,
                            uint64_t &version_ts) const = 0;

    virtual bool FetchTable(const txservice::TableName &table_name,
                            std::string &schema_image,
                            bool &found) const = 0;

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

    //-- view
    virtual bool UpsertView(std::string_view view,
                            std::string_view definition) const = 0;
    virtual bool DropView(std::string_view view) const = 0;
    virtual bool FetchView(std::string_view view,
                           std::string &definition,
                           bool &found) const = 0;
    virtual bool DiscoverAllViewNames(
        std::vector<std::string> &view_names) const = 0;

    virtual std::unique_ptr<DataStoreScanner> ScanForward(
        const txservice::TableName &table_name,
        const txservice::TxKey &start_key,
        bool inclusive,
        uint8_t key_parts,
        const txservice::Schema *key_schema,
        const txservice::Schema *rec_schema,
        bool scan_foward) = 0;

    virtual std::unique_ptr<DataStoreScanner> ScanForward(
        const txservice::TableName &table_name,
        const std::string &search_cond,
        const txservice::Schema *key_schema,
        const txservice::Schema *rec_schema,
        bool scan_foward) = 0;

    /**
     * @brief Write historical versions into DataStore.
     *
     */
    virtual bool PutArchives(
        const txservice::TableName &table_name,
        const txservice::TxKey &key,
        const std::vector<txservice::VersionedRecord> &archives) = 0;

    /**
     * @brief  Get the latest visible(commit_ts <= upper_bound_ts) historical
     * version.
     */
    virtual bool FetchVisibleArchive(const txservice::TableName &table_name,
                                     const txservice::TxKey &key,
                                     const uint64_t upper_bound_ts,
                                     txservice::TxRecord &rec,
                                     txservice::RecordStatus &rec_status,
                                     uint64_t &commit_ts) = 0;

    /**
     * @brief  Fetch all archives whose commit_ts >= from_ts.
     */
    virtual bool FetchArchives(
        const txservice::TableName &table_name,
        const txservice::TxKey &key,
        std::vector<txservice::VersionedRecord> &archives,
        uint64_t from_ts) = 0;

    void SetTxService(txservice::TxService *tx_service)
    {
        tx_service_ = tx_service;
    }

protected:
    txservice::TxService *tx_service_;
};
}  // namespace store
}  // namespace txservice