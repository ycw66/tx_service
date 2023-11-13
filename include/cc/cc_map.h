#pragma once

#include <map>
#include <memory>
#include <utility>  // std::pair

#include "cc/cc_req_base.h"
#include "cc_protocol.h"
#include "ccm_scanner.h"
#include "error_messages.h"  // CcErrorCode
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

struct LruEntry;

struct AcquireCc;
struct AcquireAllCc;
struct PostWriteCc;
struct PostWriteAllCc;
struct PostReadCc;
struct ReadCc;
struct ScanCloseCc;
struct ScanOpenBatchCc;
struct ScanNextBatchCc;
struct ScanSliceCc;
struct NegotiateCc;
struct DataSyncScanCc;
struct CkptUpdateCc;
struct CkptTs;
struct BroadcastStatisticsCc;
struct AnalyzeTableAllCc;
struct ReplayLogCc;
struct FaultInjectCC;
struct CleanCcEntryForTestCc;
struct FillStoreSliceCc;
struct GetPostCkptSlice;
struct KickoutCcEntryCc;
struct ApplyCc;

enum struct ScanType
{
    ScanKey = 0,
    ScanGap,
    ScanBoth,
    ScanUnknow
};

enum struct CleanType
{
    /**
     * This used to free the memory when the ccshard is full. In this case, the
     * `CleanPageAndReBalance()` is invoked during `CcShard::Clean()`, and the
     * CcPage come from the LRU list. Then will clean cc entries that is free,
     * that is to say, the ccentry has been checkpointed and have no lock on
     * this ccentry.
     */
    CleanForFree = 0,
    /**
     * This used to kickout the ccentries that donot belong to this node anymore
     * during split range operation. In this case, the `CleanPageAndReBalance()`
     * is invoked during execute `KickoutCcEntryCc` request, and the CcPage come
     * from the request's `start_key`. Then will clean cc entries that are in
     * the specific range.
     */
    CleanForSplitRange,
    /**
     * This used to kickout the ccentries during alter table operation. In this
     * case, the `CleanPageAndReBalance()` is invoked during execute
     * `KickoutCcEntryCc` request, and the CcPage come from the request's
     * `start_key`. Then will clean cc entries that match the below conditions:
     * 1) `commit_ts` less than the the timestamp specified by the request, 2)
     * large than 1, that is to say not the initial entry, 3) is free, that is
     * to say, the ccentry has been checkpointed and have no lock on it.
     */
    CleanForAlterTable
};

class CcShard;
struct TableSchema;

class CcMap
{
public:
    using uptr = std::unique_ptr<CcMap>;

    CcMap(CcShard *shard,
          NodeGroupId cc_ng_id,
          const TableName &table_name,
          const TableSchema *table_schema,
          uint64_t schema_ts,
          bool ccm_has_full_entries = false)
        : shard_(shard),
          cc_ng_id_(cc_ng_id),
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
    virtual bool Execute(ScanSliceCc &req) = 0;
    virtual bool Execute(DataSyncScanCc &req) = 0;
    virtual bool Execute(remote::RemoteReadOutside &req) = 0;
    virtual bool Execute(BroadcastStatisticsCc &req) = 0;
    virtual bool Execute(AnalyzeTableAllCc &req) = 0;
    virtual bool Execute(ReplayLogCc &req) = 0;
    virtual bool Execute(FaultInjectCC &req) = 0;
    virtual bool Execute(CleanCcEntryForTestCc &req) = 0;
    virtual bool Execute(FillStoreSliceCc &req) = 0;
    virtual bool Execute(GetPostCkptSlice &req) = 0;
    virtual bool Execute(KickoutCcEntryCc &req) = 0;
    virtual bool Execute(ApplyCc &req) = 0;

    virtual size_t size() const = 0;

    virtual void Clean(LruEntry *remove_entry) = 0;
    virtual std::pair<size_t, LruPage *> CleanPageAndReBalance(
        LruPage *page,
        CleanType clean_type = CleanType::CleanForFree,
        KickoutCcEntryCc *kickout_cc = nullptr,
        bool *is_success = nullptr) = 0;
    virtual void Clean() = 0;

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

    void SetTableSchema(const TableSchema *table_schema)
    {
        table_schema_ = table_schema;
    }

    void SetSchemaTs(uint64_t schema_ts)
    {
        schema_ts_ = schema_ts;
    }

    CcShard *const shard_;
    NodeGroupId cc_ng_id_;
    TableName table_name_;  // string owner
    // Kv store can be skipped if we know ccm contains all the entries. This is
    // crucial for performance. This flag is true when the table is created and
    // no LRU kickout happens on this ccm. In future, we should make it at range
    // level: the kv access unit is range.
    bool ccm_has_full_entries_{false};
    // The largest commit ts of dirty cc entries in this cc map. This value
    // might be larger than the actual max commit ts of cc entries. Currently
    // used to decide if this cc map has dirty data after a given ts.
    uint64_t last_dirty_commit_ts_{0};

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
     * @return std::pair<LockType,CcErrorCode> : the first arg of the pair is
     * the lock that the request will acquire, the second is the error code.
     */
    std::pair<LockType, CcErrorCode> AcquireCceKeyLock(
        LruEntry *cce,
        RecordStatus cce_payload_status,
        CcRequestBase *req,
        uint32_t ng_id,
        int64_t ng_term,
        int64_t tx_term,
        CcOperation cc_op,
        IsolationLevel iso_level,
        CcProtocol protocol,
        uint64_t read_ts,
        bool is_covering_keys);

    /**
     * @brief do check after request is resumed from lock blocking queue.
     * @return  std::pair<LockType,CcErrorCode> : the first arg of the pair is
     * the lock that the request will acquire, the second is the error code.
     */
    std::pair<LockType, CcErrorCode> LockHandleForResumedRequest(
        LruEntry *cce,
        RecordStatus cce_payload_status,
        CcRequestBase *req,
        uint32_t ng_id,
        int64_t ng_term,
        int64_t tx_term,
        CcOperation cc_op,
        IsolationLevel iso_level,
        CcProtocol protocol,
        uint64_t read_ts,
        bool is_covering_keys);

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
    void ReleaseCceLock(NonBlockingLock *lock,
                        LruEntry *cce,
                        TxNumber tx_number,
                        uint32_t ng_id,
                        LockType lk_type = LockType::NoLock);

    uint64_t schema_ts_{1};
    const TableSchema *table_schema_;
};
}  // namespace txservice
