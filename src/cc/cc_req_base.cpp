#include "cc_req_base.h"

#include "catalog_key_record.h"
#include "cc/cc_shard.h"
#include "statistics.h"

namespace txservice
{
const CatalogEntry *CcRequestBase::InitCcm(const TableName &tbl_name,
                                           NodeGroupId cc_ng_id,
                                           CcShard &ccs)
{
    const TableName base_table_name{tbl_name.GetBaseTableNameSV(),
                                    TableType::Primary};

    const CatalogEntry *catalog_entry =
        ccs.GetCatalog(base_table_name, cc_ng_id);

    if (catalog_entry != nullptr)
    {
        const TableSchema *curr_schema = catalog_entry->schema_.get();
        if (curr_schema != nullptr && catalog_entry->Version() > 0)
        {
            {
                // Initialize table statistics
#ifdef RANGE_PARTITION_ENABLED
                // Initialize table ranges before create table
                // statistics.
                TableName base_range_table_name{tbl_name.GetBaseTableNameSV(),
                                                TableType::RangePartition};
                auto ranges = ccs.GetTableRangesForATable(base_range_table_name,
                                                          cc_ng_id);
                if (ranges == nullptr)
                {
                    ccs.FetchTableRanges(base_range_table_name,
                                         curr_schema->GetKVCatalogInfo(),
                                         this,
                                         cc_ng_id);
                    return nullptr;
                }
                for (const TableName &index_name : curr_schema->IndexNames())
                {
                    TableName index_range_table_name{index_name.StringView(),
                                                     TableType::RangePartition};
                    auto ranges = ccs.GetTableRangesForATable(
                        index_range_table_name, cc_ng_id);
                    if (ranges == nullptr)
                    {
                        ccs.FetchTableRanges(index_range_table_name,
                                             curr_schema->GetKVCatalogInfo(),
                                             this,
                                             cc_ng_id);
                        return nullptr;
                    }
                }
#endif
                // Initialize table statistics before create ccmap.
                const StatisticsEntry *statistics_entry =
                    ccs.GetTableStatistics(base_table_name, cc_ng_id);
                if (statistics_entry == nullptr ||
                    statistics_entry->statistics_ == nullptr)
                {
                    ccs.FetchTableStatistics(base_table_name, cc_ng_id, this);
                    return nullptr;
                }
            }

            ccs.CreateOrUpdatePkCcMap(base_table_name,
                                      curr_schema,
                                      cc_ng_id,
                                      catalog_entry->Version());

            std::vector<TableName> index_names = curr_schema->IndexNames();
            for (const TableName &index_name : index_names)
            {
                ccs.CreateOrUpdateSkCcMap(index_name,
                                          curr_schema,
                                          cc_ng_id,
                                          catalog_entry->Version());
            }
        }
    }
    else
    {
        // The local node does not contain the table's schema instance. The
        // FetchCatalog() method sends an async request toward the data
        // store to fetch the catalog. After fetching is finished, this cc
        // request is re-enqueued for re-execution.
        ccs.FetchCatalog(base_table_name, cc_ng_id, this);
    }

    return catalog_entry;
}
}  // namespace txservice
