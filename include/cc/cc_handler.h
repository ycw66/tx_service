#pragma once

#include <memory>
#include <unordered_set>
#include <vector>

#include "cc_handler_result.h"
#include "cc_protocol.h"
#include "ccm_scanner.h"
#include "read_write_entry.h"
#include "scan.h"
#include "tx_container.h"
#include "tx_key.h"
#include "tx_operation_result.h"
#include "tx_record.h"
#include "type.h"

namespace txservice
{
class CcHandler
{
public:
    using Pointer = std::unique_ptr<CcHandler>;

    virtual ~CcHandler() = default;

    virtual void AcquireWrite(const TableName &table_name,
                              const TxKey &key,
                              const TxId &txid,
                              int64_t tx_term,
                              uint64_t ts,
                              bool is_insert,
                              CcHandlerResult<AcquireKeyResult> &hres,
                              const CcProtocol proto = CcProtocol::OCC) = 0;

    /// <summary>
    /// Acquire table level write lock.
    /// </summary>
    /// <param name="table_name"></param>
    /// <param name="txid"></param>
    /// <param name="tx_number"></param>
    /// <param name="hres"></param>
    virtual void AcquireTableWriteLock(
        const TableName &table_name,
        const TxId &txid,
        int64_t tx_term,
        uint64_t tx_number,
        CcHandlerResult<std::unordered_map<uint32_t, int64_t>> &hres) = 0;

    /// <summary>
    /// Release table level write lock.
    /// </summary>
    /// <param name="table_name"></param>
    /// <param name="txid"></param>
    /// <param name="tx_number"></param>
    /// <param name="hres"></param>
    virtual void ReleaseTableWriteLock(const TableName &table_name,
                                       const TxId &txid,
                                       int64_t tx_term,
                                       uint64_t tx_number,
                                       CcHandlerResult<Void> &hres) = 0;

    /// <summary>
    /// Releases the write intention/lock for the input key after the tx aborts.
    /// The operation also unblocks the pending requests on the key.
    /// </summary>
    /// <param name="table_name"></param>
    /// <param name="key"></param>
    /// <param name="txid"></param>
    /// <param name="extension"></param>
    /// <param name=""></param>
    virtual void ReleaseWrite(uint64_t tx_number,
                              int64_t tx_term,
                              const CcEntryAddr &ccentry_addr,
                              CcHandlerResult<Void> &hres) = 0;

    /// <summary>
    /// Installs the committed write and releases the write intention/lock after
    /// the tx commits. The operation unblocks the pending requests, if there
    /// are any, on the key.
    /// </summary>
    /// <param name="table_name"></param>
    /// <param name="key"></param>
    /// <param name="txid"></param>
    /// <param name="commit_ts"></param>
    /// <param name="extension"></param>
    /// <param name=""></param>
    /// <param name="record"></param>
    /// <param name="is_deleted"></param>
    virtual void CommitWrite(uint64_t tx_number,
                             int64_t tx_term,
                             uint64_t commit_ts,
                             const CcEntryAddr &ccentry_addr,
                             const TxRecord &record,
                             bool is_deleted,
                             CcHandlerResult<Void> &hres) = 0;

    /// <summary>
    /// For OCC, validates whether or not the key has changed since the prior
    /// read.
    /// </summary>
    /// <param name="table_name"></param>
    /// <param name="key"></param>
    /// <param name="version_ts"></param>
    /// <param name="commit_ts"></param>
    /// <param name="extension"></param>
    /// <param name=""></param>
    virtual void ValidateRead(uint64_t tx_number,
                              int64_t tx_term,
                              uint64_t key_ts,
                              uint64_t gap_ts,
                              uint64_t commit_ts,
                              const CcEntryAddr &ccentry_addr,
                              CcHandlerResult<std::vector<TxId>> &hres) = 0;

    /// <summary>
    ///
    /// </summary>
    /// <param name="table_name"></param>
    /// <param name="key"></param>
    /// <param name="TxId"></param>
    /// <param name="extension"></param>
    /// <param name=""></param>
    virtual void PostprocessRead(uint64_t tx_number,
                                 int64_t tx_term,
                                 const CcEntryAddr &ccentry_addr,
                                 CcHandlerResult<Void> &,
                                 CcProtocol proto = CcProtocol::OCC) = 0;

    /// <summary>
    /// PostProcess for create table.
    /// </summary>
    /// <param name="table_name"></param>
    /// <param name="catalog_image_"></param>
    /// <param name="catalog_length_"></param>
    /// <param name="txid"></param>
    /// <param name="ts"></param>
    /// <param name="hresult"></param>
    virtual void CommitCreateTable(const TableName &table_name,
                                   const unsigned char *catalog_image_,
                                   size_t catalog_length_,
                                   int64_t tx_term,
                                   const TxId &txid,
                                   uint64_t ts,
                                   CcHandlerResult<Void> &hresult) = 0;

    /// <summary>
    /// PostProcess for drop table.
    /// </summary>
    /// <param name="table_name"></param>
    /// <param name="txid"></param>
    /// <param name="ts"></param>
    /// <param name="hresult"></param>
    virtual void CommitDropTable(const TableName &table_name,
                                 int64_t tx_term,
                                 const TxId &txid,
                                 uint64_t ts,
                                 CcHandlerResult<Void> &hresult) = 0;

    /// <summary>
    /// Starts concurrency control for the input key and returns the key's
    /// committed value, if there is any.
    /// </summary>
    /// <param name="table_name"></param>
    /// <param name="key"></param>
    /// <param name="TxId"></param>
    /// <param name="time"></param>
    /// <param name=""></param>
    virtual void Read(const TableName &table_name,
                      const TxKey &key,
                      TxRecord &rec,
                      ReadType read_type,
                      uint64_t tx_number,
                      int64_t tx_term,
                      const uint64_t ts,
                      CcHandlerResult<ReadKeyResult> &hres,
                      CcProtocol proto = CcProtocol::OCC) = 0;

    virtual void ReadOutside(TxRecord &rec,
                             bool is_deleted,
                             const CcEntryAddr &cce_addr,
                             CcHandlerResult<ReadKeyResult> &hres) = 0;

    virtual void ScanOpen(const TableName &table_name,
                          ScanIndexType index_type,
                          const TxKey &start_key,
                          bool inclusive,
                          uint64_t tx_number,
                          int64_t tx_term,
                          uint64_t start_ts,
                          CcHandlerResult<ScanOpenResult> &hd_res,
                          ScanDirection direction = ScanDirection::Forward,
                          CcProtocol proto = CcProtocol::OCC,
                          bool is_ckpt = false) = 0;

    virtual void ScanNextBatch(uint64_t tx_number,
                               int64_t tx_term,
                               uint64_t start_ts,
                               CcScanner &scanner,
                               CcHandlerResult<ScanNextResult> &hd_res,
                               CcProtocol proto = CcProtocol::OCC) = 0;

    virtual void ScanClose(size_t alias,
                           const TxKey &end_key,
                           bool inclusive,
                           CcProtocol proto = CcProtocol::OCC) = 0;

    virtual void UploadRecord(const TableName &table_name,
                              const TxKey &key,
                              TxRecord *record,
                              const CcEntryAddr &ccentry_addr,
                              CcHandlerResult<Void> &) = 0;

    virtual void UploadSecondaryKey(const TableName &table_name,
                                    const TxKey &sk,
                                    const TxKey &pk,
                                    CcHandlerResult<Void> &) = 0;

    virtual void CommitSecondaryKey(const TableName &table_name,
                                    const TxKey &sk,
                                    const TxKey &pk,
                                    bool is_delete,
                                    uint64_t ts,
                                    CcHandlerResult<Void> &) = 0;

    /// <summary>
    /// Starts a new tx and returns the tx ID.
    /// </summary>
    /// <param name="entry"></param>
    /// <param name="local_time"></param>
    /// <param name="max_txn_execution_time_ms"></param>
    /// <param name=""></param>
    virtual void NewTxn(CcHandlerResult<InitTxResult> &) = 0;

    /// <summary>
    /// Sets the commit timestamp of the input tx.
    /// </summary>
    /// <param name="txn_id"></param>
    /// <param name="commit_ts"></param>
    /// <param name="extension"></param>
    /// <param name=""></param>
    virtual void SetCommitTimestamp(const TxId &txid,
                                    uint64_t commit_ts,
                                    CcHandlerResult<uint64_t> &) = 0;

    /// <summary>
    /// Negotiates with the input tx such that it commits no later than the
    /// input timestamp.
    /// </summary>
    /// <param name="txn_id"></param>
    /// <param name="commit_ts_lower_bound"></param>
    /// <param name=""></param>
    virtual void UpdateCommitLowerBound(const TxId &txn_id,
                                        uint64_t commit_ts_lower_bound,
                                        CcHandlerResult<uint64_t> &) = 0;

    /// <summary>
    /// Sets the tx status to committed or aborted, after the fate of the tx is
    /// determined.
    /// </summary>
    /// <param name="txn_id"></param>
    /// <param name="status"></param>
    /// <param name="extension"></param>
    /// <param name=""></param>
    virtual void UpdateTxnStatus(const TxId &txn_id,
                                 TxnStatus status,
                                 CcHandlerResult<Void> &) = 0;

    virtual void FindCatalogInCCShard(const TableName &table_name,
                                      std::string *catalog_content,
                                      uint64_t tx_number,
                                      CcHandlerResult<bool> &hres) = 0;

    virtual void CheckCatalogVersionInCCShard(const TableName &table_name,
                                              std::string *source_version,
                                              uint64_t tx_number,
                                              CcHandlerResult<bool> &hres) = 0;

    virtual void FaultInject(const std::string &fault_name,
                             const std::string &fault_type,
                             int64_t tx_term,
                             const TxId &txid,
                             int node_id,
                             CcHandlerResult<bool> &hres) = 0;

    virtual void ReleaseAllTableLocks(
        std::unordered_set<std::string> opened_table_set,
        uint64_t tx_number,
        CcHandlerResult<bool> &hres) = 0;

    virtual uint32_t GetNodeId() const = 0;
};

}  // namespace txservice
