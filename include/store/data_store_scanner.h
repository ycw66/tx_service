#pragma once

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
                         const txservice::TxRecord *&rec) = 0;
    virtual bool MoveNext() = 0;
};
}  // namespace store
}  // namespace txservice
