#pragma once

#include "raft_log.pb.h"
#include "type.h"

namespace txlog
{
class ToRemoteType
{
public:
    static CcTableType ConvertTableType(txservice::TableType table_type)
    {
        switch (table_type)
        {
        case txservice::TableType::Primary:
            return CcTableType::Primary;
        case txservice::TableType::Secondary:
            return CcTableType::Secondary;
        case txservice::TableType::Catalog:
            return CcTableType::Catalog;
        case txservice::TableType::RangePartition:
            return CcTableType::RangePartition;
        default:
            assert(false);
            return CcTableType::Primary;
        }
    }
};

class ToLocalType
{
public:
    static txservice::TableType ConvertCcTableType(CcTableType table_type)
    {
        switch (table_type)
        {
        case CcTableType::Primary:
            return txservice::TableType::Primary;
        case CcTableType::Secondary:
            return txservice::TableType::Secondary;
        case CcTableType::Catalog:
            return txservice::TableType::Catalog;
        default:
            return txservice::TableType::RangePartition;
        }
    }
};

}  // namespace txlog
