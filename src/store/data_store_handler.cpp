#include "store/data_store_handler.h"

#include <chrono>

using namespace std::chrono_literals;

namespace txservice
{
namespace store
{
bool DataStoreHandler::FetchTable(const txservice::TableName &table_name,
                                  std::string &schema_image,
                                  bool &found) const
{
    uint64_t version_ts;
    return FetchTable(table_name, schema_image, found, version_ts);
}

void DataStoreHandler::CleanDefunctKvTable() const
{
    bool ok = false;
    do
    {
        std::set<std::string> tables_ctime_more_1d;
        std::set<std::string> tables_visible;
        std::set<std::string> tables_to_drop;

        ok = ListKvTableCTimeMore1d(tables_ctime_more_1d) &&
             ListVisibleKvTable(tables_visible);

        if (ok)
        {
            std::set_difference(
                tables_ctime_more_1d.begin(),
                tables_ctime_more_1d.end(),
                tables_visible.begin(),
                tables_visible.end(),
                std::inserter(tables_to_drop, tables_to_drop.end()));
            ok = std::all_of(tables_to_drop.begin(),
                             tables_to_drop.end(),
                             [this](const std::string &kv_table_name) -> bool
                             { return DropKvTable(kv_table_name); });
        }

        if (!ok)
        {
            std::this_thread::sleep_for(5s);
            LOG(ERROR) << "retry clean defunct kvtable";
        }
    } while (!ok);
}

}  // namespace store
}  // namespace txservice
