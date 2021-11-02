#pragma once

#include <memory>

#include "store/data_store_handler.h"

namespace txservice
{
/*
  Monograph Catalog
  Abstract interface of catalog in monograph storage engine. Each runtime registers
  it callback into monograph catalog. Take CREATE TABLE as an example, table is
  distributed on all the runtimes and monograph engines. Tlog is the ground truth of
  CREATE TABLE, but in postprocess of CREATE TABLE request, we need run callback
  to notify the runtime to refresh cache(monograph share) and create table in
  Cassandra
 */
class Catalog
{
public:
    Catalog()
    {
    }
    virtual ~Catalog() = default;

    // virtual void SetStore(store::DataStoreWriteHandler *store_hd) {}

    virtual bool CreateTable(const std::string &db_name,
                             const std::string &table_name,
                             const std::string &catalog_image,
                             uint32_t core_id,
                             bool create_cass_table)
    {
        return false;
    }

    virtual bool DropTable(const std::string &db_name,
                           const std::string &table_name,
                           uint32_t core_id,
                           bool create_cass_table)
    {
        return false;
    }
};
}  // namespace txservice
