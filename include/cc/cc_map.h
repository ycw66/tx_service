#pragma once

#include <map>

#include "ccm_scanner.h"
#include "tx_key.h"
#include "type.h"

namespace txservice
{
namespace remote
{
struct RemoteScanOpen;
struct RemoteScanNextBatch;
struct RemoteReadOutside;
struct RemoteAcquireTableWriteLockCC;
}  // namespace remote

struct AcquireCc;
struct PostDeleteCc;
struct PostCommitCc;
struct ValidateCc;
struct PostReadCc;
struct ReadCc;
struct ScanCloseCc;
struct ScanOpenBatchCc;
struct ScanNextBatchCc;
struct NegotiateCc;
struct CkptScanCc;
struct CommitSkCc;
struct CkptUpdateCc;
struct CkptTs;
struct ReplayLogCc;
struct AcquireTableWriteLockCC;
struct ReleaseTableWriteLockCC;
struct CommitCreateTableCC;
struct CommitDropTableCC;
struct FindCatalogCC;
struct CheckCatalogCC;
struct FaultInjectCC;

class CcShard;

class CcMap
{
public:
    using uptr = std::unique_ptr<CcMap>;

    CcMap(CcShard *shard) : shard_(shard)
    {
    }

    virtual ~CcMap() = default;

    virtual bool Execute(AcquireCc &req) = 0;
    virtual bool Resume(AcquireCc &req) = 0;
    virtual bool Execute(PostDeleteCc &req) = 0;
    virtual bool Execute(PostCommitCc &req) = 0;
    virtual bool Execute(ValidateCc &req) = 0;
    virtual bool Execute(PostReadCc &req) = 0;
    virtual bool Execute(ReadCc &req) = 0;
    virtual bool Resume(ReadCc &req) = 0;
    virtual bool Execute(ScanCloseCc &req) = 0;
    virtual bool Execute(ScanOpenBatchCc &req) = 0;
    virtual bool Execute(ScanNextBatchCc &req) = 0;
    virtual bool Execute(remote::RemoteScanOpen &req) = 0;
    virtual bool Execute(remote::RemoteScanNextBatch &req) = 0;
    virtual bool Execute(CommitSkCc &req) = 0;
    virtual bool Execute(CkptScanCc &req) = 0;
    virtual bool Execute(remote::RemoteReadOutside &req) = 0;
    virtual bool Execute(ReplayLogCc &req) = 0;
    virtual bool Execute(AcquireTableWriteLockCC &req) = 0;
    virtual bool Execute(remote::RemoteAcquireTableWriteLockCC &req) = 0;
    virtual bool Execute(CommitCreateTableCC &req) = 0;
    virtual bool Execute(ReleaseTableWriteLockCC &req) = 0;
    virtual bool Execute(CommitDropTableCC &req) = 0;
    virtual bool Execute(FindCatalogCC &req) = 0;
    virtual bool Execute(CheckCatalogCC &req) = 0;
    virtual bool Execute(FaultInjectCC &req) = 0;

    virtual std::unique_ptr<CcScanner> CreateScanner(
        ScanDirection direction) const = 0;

    virtual size_t size() const = 0;

    virtual void Clean(LruEntry *remove_entry) = 0;
    virtual size_t VerifyOrdering() = 0;

    virtual void GetCkptKeyRecord(const LruEntry *lru_entry,
                                  const TxKey *&key,
                                  const TxRecord *&rec,
                                  bool &is_deleted) const
    {
        key = nullptr;
        rec = nullptr;
        is_deleted = false;
    }

    virtual void GetCkptSk(const LruEntry *lru_entry,
                           const TxKey *&sk,
                           const TxKey *&pk,
                           bool &is_deleted) const
    {
        sk = nullptr;
        pk = nullptr;
        is_deleted = false;
    }

    virtual TableType Type() const = 0;
    virtual const Schema *KeySchema() const = 0;
    virtual const Schema *RecordSchema() const = 0;

    virtual std::unique_ptr<CcMap> Clone() const = 0;

    CcShard *const shard_;
};
}  // namespace txservice
