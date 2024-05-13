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

    virtual TxRecord::Uptr DeserializeObject(const char *buf,
                                             size_t &offset) const
    {
        return nullptr;
    }
};
}  // namespace txservice
