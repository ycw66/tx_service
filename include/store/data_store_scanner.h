#pragma once

#include <stdint.h>

namespace txservice
{
class TxKey;
struct TxRecord;
namespace store
{
class DataStoreScanner
{
public:
    virtual ~DataStoreScanner() = default;
    virtual void Current(const txservice::TxKey *&key,
                         const txservice::TxRecord *&rec,
                         uint64_t &version_ts_,
                         bool &deleted_) = 0;
    virtual bool MoveNext() = 0;
    virtual void End() = 0;
};
}  // namespace store
}  // namespace txservice
