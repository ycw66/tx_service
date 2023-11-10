#pragma once

#include "tx_record.h"

namespace txservice
{
struct TxObject : public TxRecord
{
public:
    TxObject() = default;

    TxObject(const TxObject &obj)
    {
    }

    TxObject &operator=(const TxObject &obj)
    {
        return *this;
    }
};
}  // namespace txservice
