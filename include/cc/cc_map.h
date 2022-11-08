#pragma once

#include <map>
#include <memory>
#include <utility>  // std::pair

#include "cc/cc_req_base.h"
#include "cc_protocol.h"
#include "ccm_scanner.h"
#include "tx_key.h"
#include "tx_operation_result.h"
#include "type.h"  // LockType, LockOpStatus

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
    ScanBoth,
    ScanUnknow
};

class CcShard;
struct TableSchema;

class CcMap
{
public:
    using uptr = std::unique_ptr<CcMap>;

    CcMap(CcShard *shard,
          const TableName &table_name,
          const TableSchema *table_schema,
          uint64_t schema_ts,
          bool ccm_has_full_entries = false)
        : shard_(shard),
          table_name_(table_name.StringView().data(),
                      table_name.StringView().size(),
                      table_name.Type()),
          ccm_has_full_entries_(ccm_has_full_entries),
          schema_ts_(schema_ts),
          table_schema_(table_schema)
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

    virtual TableType Type() const = 0;
    virtual const Schema *KeySchema() const = 0;
    virtual const Schema *RecordSchema() const = 0;

    uint64_t SchemaTs() const
    {
        return schema_ts_;
    }

    const TableSchema *GetTableSchema() const
    {
        return table_schema_;
    }

    CcShard *const shard_;
    TableName table_name_;  // string owner
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

    /**
     * @brief Acquire key lock of ccentry. If succes, this method will
     * 'UpsertLockHoldingTx', else, it recover the transaction holding lock.
     *
     * @param cce
     * @param req
     * @param ng_id
     * @param ng_term
     * @param tx_term
     * @param cc_op
     * @param iso_level
     * @param protocol
     * @param is_resume If true, it means that the request is restored from
     * lock blocking queue, that is, the request just acquired the lock.
     * @return std::pair<LockType, LockOpStatus>
     */
    std::pair<LockType, LockOpStatus> AcquireCceKeyLock(
        LruEntry *cce,
        RecordStatus cce_payload_status,
        CcRequestBase *req,
        uint32_t ng_id,
        int64_t ng_term,
        int64_t tx_term,
        CcOperation cc_op,
        IsolationLevel iso_level,
        CcProtocol protocol);

    LockType LockHandleForResumedRequest(CcRequestBase *req,
                                         int64_t tx_term,
                                         LruEntry *cce,
                                         RecordStatus cce_payload_status,
                                         bool is_wait_for_postwrite = false);

    // Insert the request into key_lock's blocking queue.
    void WaitForPostWriteDone(CcRequestBase *req, LruEntry *cce);

    void RecoverTxForLockConfilct(NonBlockingLock &lock,
                                  LockType lock_type,
                                  uint32_t ng_id,
                                  int64_t ng_term);

    void DowngradeCceKeyWriteLock(LruEntry *cce, TxNumber tx_number);

    /**
     * @brief ReleaseLock and DeleteLockHoldingTx
     *
     * @param cce
     * @param tx_number
     * @param lock_type
     */
    void ReleaseCceKeyLock(LruEntry *cce, TxNumber tx_number);
    void ReleaseCceGapLock(LruEntry *cce, TxNumber tx_number);
    /**
     * @brief The lock type of CcEntry's key_lock that held by some one
     * transaction.
     *
     * @param cce
     * @param tx_number
     * @return LockType
     */
    LockType CceKeyLockTypeHeldByTx(LruEntry *cce, TxNumber tx_number);

    uint64_t schema_ts_{1};
    const TableSchema *table_schema_;
};
}  // namespace txservice
