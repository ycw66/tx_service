#pragma once

#include <map>
#include <memory>

#include "cc/cc_req_base.h"
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
}  // namespace remote

struct AcquireCc;
struct AcquireAllCc;
struct PostWriteCc;
struct PostWriteAllCc;
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
struct FaultInjectCC;

class CcShard;

class CcMap
{
public:
    using uptr = std::unique_ptr<CcMap>;

    CcMap(CcShard *shard, uint64_t schema_ts)
        : shard_(shard), commit_ts_(schema_ts)
    {
    }

    virtual ~CcMap() = default;

    virtual bool Execute(AcquireCc &req) = 0;
    virtual bool Execute(AcquireAllCc &req) = 0;
    virtual bool Execute(PostWriteCc &req) = 0;
    virtual bool Execute(PostWriteAllCc &req) = 0;
    virtual bool Execute(PostReadCc &req) = 0;
    virtual bool Execute(ReadCc &req) = 0;
    virtual bool Execute(ScanCloseCc &req) = 0;
    virtual bool Execute(ScanOpenBatchCc &req) = 0;
    virtual bool Execute(ScanNextBatchCc &req) = 0;
    virtual bool Execute(remote::RemoteScanOpen &req) = 0;
    virtual bool Execute(remote::RemoteScanNextBatch &req) = 0;
    virtual bool Execute(CommitSkCc &req) = 0;
    virtual bool Execute(CkptScanCc &req) = 0;
    virtual bool Execute(remote::RemoteReadOutside &req) = 0;
    virtual bool Execute(ReplayLogCc &req) = 0;
    virtual bool Execute(FaultInjectCC &req) = 0;

    virtual std::unique_ptr<CcScanner> CreateScanner(
        ScanDirection direction) const = 0;

    virtual size_t size() const = 0;

    virtual void Clean(LruEntry *remove_entry) = 0;

    /**
     * @brief If the new cc_entry is not in the checkpoint list, enlists the new
     * entry.
     *
     * @param entry
     */
    virtual void TryInsertCkptList(LruEntry *entry) = 0;

    /**
     * Used for debug to verify the map_link is complete.
     */
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

    bool ConditionalReadLockCce(LruEntry *cce,
                                CcRequestBase &req,
                                LockType lock_type,
                                int64_t tx_term,
                                uint32_t cce_node_group_id,
                                bool gap_lock = false);

    bool ReadLockCce(LruEntry *cce,
                     CcRequestBase &req,
                     int64_t tx_term,
                     uint32_t cce_node_group_id,
                     bool gap_lock = false);

    void RecoverReadLocks(LruEntry &cce, uint32_t node_group_id);
    void RecoverWriteLock(const TxNumber &tx_number, uint32_t node_group_id);
    void RecoverWriteIntent(LruEntry &cce, uint32_t node_group_id);

    CcShard *const shard_;
    uint64_t commit_ts_;

protected:
    /**
     * @brief After the input request is executed at the current shard, moves
     * the request to another shard for execution.
     *
     * @param cc_req The cc request executed at the cc map.
     * @param target_core_id The destination shard/core ID to which the request
     * is moved.
     */
    void MoveRequest(CcRequestBase *cc_req, uint32_t target_core_id);
};
}  // namespace txservice
