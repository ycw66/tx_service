#include "cc_req_base.h"

#include "catalog_key_record.h"
#include "cc/cc_shard.h"

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
            ccs.CreateOrUpdatePkCcMap(base_table_name,
                                      curr_schema,
                                      cc_ng_id,
                                      catalog_entry->Version());

            const std::vector<TableName> &index_names =
                curr_schema->IndexNames();
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