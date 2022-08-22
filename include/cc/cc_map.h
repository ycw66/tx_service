#pragma once

#include <map>
#include <memory>

#include "cc/cc_req_base.h"
#include "ccm_scanner.h"
#include "tx_key.h"
#include "tx_operation_result.h"
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
struct CkptUpdateCc;
struct CkptTs;
struct ReplayLogCc;
struct FaultInjectCC;
struct CleanCcEntryForTestCc;

enum struct ScanType
{
    ScanKey = 0,
    ScanGap,
    ScanBoth
};

class CcShard;

class CcMap
{
public:
    using uptr = std::unique_ptr<CcMap>;

    CcMap(CcShard *shard,
          const TableName &table_name,
          uint64_t schema_ts,
          bool ccm_has_full_entries = false)
        : shard_(shard),
          table_name_(table_name),
          ccm_has_full_entries_(ccm_has_full_entries),
          schema_ts_(schema_ts)
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
    virtual bool Execute(CkptScanCc &req) = 0;
    virtual bool Execute(remote::RemoteReadOutside &req) = 0;
    virtual bool Execute(ReplayLogCc &req) = 0;
    virtual bool Execute(FaultInjectCC &req) = 0;
    virtual bool Execute(CleanCcEntryForTestCc &req) = 0;

    virtual std::unique_ptr<CcScanner> CreateScanner(
        ScanDirection direction) const = 0;

    virtual size_t size() const = 0;

    virtual void Clean(LruEntry *remove_entry) = 0;
    virtual void Clean() = 0;

    virtual TxKey::Uptr ExportSecondaryKey(LruEntry *entry) const
    {
        assert(false);
        return nullptr;
    }

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

    bool ConditionalReadLockCce(LruEntry *cce,
                                CcRequestBase &req,
                                LockType lock_type,
                                int64_t tx_term,
                                uint32_t cce_node_group_id,
                                RecordStatus payload_status,
                                int64_t ng_term,
                                ScanType scan_type,
                                bool is_sk = false);

    bool ReadLockCce(LruEntry *cce,
                     CcRequestBase &req,
                     int64_t tx_term,
                     uint32_t cce_node_group_id,
                     bool gap_lock = false);

    void RecoverReadLocks(LruEntry &cce, uint32_t node_group_id);
    void RecoverWriteLock(const TxNumber &tx_number, uint32_t node_group_id);
    void RecoverWriteIntent(LruEntry &cce, uint32_t node_group_id);

    uint64_t SchemaTs() const
    {
        return schema_ts_;
    }

    CcShard *const shard_;
    TableName table_name_;
    // Kv store can be skipped if we know ccm contains all the entries. This is
    // crucial for performance. This flag is true when the table is created and
    // no LRU kickout happens on this ccm. In future, we should make it at range
    // level: the kv access unit is range.
    bool ccm_has_full_entries_{false};

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

    bool AcquireWriteLockOnExistingCcEntry(
        AcquireCc &req,
        bool resume,
        CcHandlerResult<std::vector<AcquireKeyResult>> *hd_res,
        AcquireKeyResult &acquire_key_result,
        int64_t ng_term,
        LruEntry &cc_entry);

    uint64_t schema_ts_{1};
};
}  // namespace txservice
