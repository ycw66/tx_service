#pragma once

#include <memory>
#include <unordered_set>
#include <vector>

#include "catalog_factory.h"
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

    /**
     * @brief Acquires a write lock for the input key in the concurrency control
     * (cc) map. When there is no conflict, the request puts a write lock on the
     * key's cc entry and returns the cc entry's version. When there is a
     * conflict, the request is blocked and put into a waiting queue, if the tx
     * is under 2PL. The request returns with an error upon conflicts, if the tx
     * is under OCC/MVCC, forcing the tx to abort immediately.
     *
     * @param table_name Table name of the input key
     * @param key The key to be locked
     * @param txid Tx ID
     * @param tx_term The term of the tx node
     * @param ts Start timestamp of the tx
     * @param is_insert Whether or not the write operation is an insert
     * @param hres Result handler of the request
     * @param proto Concurrency control protocol
     */
    virtual void AcquireWrite(const TableName &table_name,
                              const TxKey &key,
                              const TxId &txid,
                              int64_t tx_term,
                              uint64_t ts,
                              bool is_insert,
                              CcHandlerResult<AcquireKeyResult> &hres,
                              CcProtocol proto) = 0;

    /**
     * @brief Acquires write locks for the input key in all shards. This method
     * is used for replicated cc maps, where identical cc maps appear in all
     * nodes. Replicated cc maps are for data frequently accessed, rarely
     * modified and needs to be strongly consistent and performant, e.g., table
     * catalog and range partition function.
     *
     * @param table_name Table name of the input key
     * @param key The key to be locked
     * @param txid Tx ID
     * @param tx_term Term of the tx node
     * @param ts Start timestamp of the tx
     * @param is_insert Whether or not the write operation is an insert
     * @param hres Result handler of the request
     * @param proto Concurrency control protocol, 2PL or OCC/MVCC
     */
    virtual void AcquireWriteAll(const TableName &table_name,
                                 const TxKey &key,
                                 NodeGroupId ng_id,
                                 TxNumber txn,
                                 int64_t tx_term,
                                 bool is_insert,
                                 CcHandlerResult<AcquireAllResult> &hres,
                                 CcProtocol proto,
                                 LockType lk_type) = 0;

    virtual void PostWriteAll(const TableName &table_name,
                              const TxKey &key,
                              TxRecord &rec,
                              NodeGroupId ng_id,
                              uint64_t tx_number,
                              int64_t tx_term,
                              uint64_t ts,
                              CcHandlerResult<Void> &hres,
                              DmlOperation dml_op,
                              PostWriteType post_write_type) = 0;

    /**
     * @brief Post-processes a write key. Post-processing clears the write lock,
     * and if the tx commits, installs the committed record.
     *
     * @param tx_number Tx number
     * @param tx_term Term of the tx node
     * @param commit_ts Commit timestamp, if the tx commits. 0, if the tx aborts
     * and the sole purpose of the request is to release the write lock of the
     * key.
     * @param ccentry_addr Address of the cc entry, on which the write lock
     * is put.
     * @param record Pointer to the committed record. Null, if the tx aborts.
     * @param is_deleted Whether or not the write deletes a record
     * @param hres Result handler of the request
     * @param protocol Concurrency control protocol
     */
    virtual void PostWrite(uint64_t tx_number,
                           int64_t tx_term,
                           uint64_t commit_ts,
                           const CcEntryAddr &ccentry_addr,
                           const TxRecord *record,
                           bool is_deleted,
                           CcHandlerResult<Void> &hres,
                           CcProtocol protocol) = 0;

    /**
     * @brief Post-processes a read/scan key. Post-processing clears the read
     * lock or intention on the key's cc entry, matches the input key/gap
     * timestamps against those of the cc entry and updates the cc entry's
     * last_vali_ts field. The last_vali_ts field forces future transactions
     * writing the key to commit at timestamps later than the reading tx. For
     * OCC/MVCC, mismatches of the key/gap timestamps indicate that version
     * stability is violated. For isolation levels greater than or equal to
     * repeatable read, the reading tx needs to abort.
     *
     * @param tx_number Tx number
     * @param tx_term Term of the tx node
     * @param key_ts Version of the key. 0, if the tx does not read the key and
     * does not check version stability.
     * @param gap_ts Version of the gap. 0, if the tx does not read the gap and
     * does not check version stability.
     * @param commit_ts Commit timestamp of the tx. 0, if the tx aborts.
     * @param ccentry_addr Address of the cc entry
     * @param hres Result handler of the request
     * @param protocol Concurrency control protocol
     */
    virtual void PostRead(uint64_t tx_number,
                          int64_t tx_term,
                          uint64_t key_ts,
                          uint64_t gap_ts,
                          uint64_t commit_ts,
                          const CcEntryAddr &ccentry_addr,
                          CcHandlerResult<std::vector<TxId>> &hres,
                          CcProtocol protocol,
                          LockType lock_type) = 0;

    /**
     * @brief Reads the input key and returns the key's record. The request puts
     * a read lock (for 2PL) or intention (for OCC/MVCC) on the key's cc entry,
     * if the tx's isolation level is equal to or greater than repeatable read.
     * The request is blocked and put into a waiting queue, if (1) there is a
     * read-write conflict, (2) the tx is under 2PL, and (3) the isolation level
     * is greater than or equal to repeatable read.
     *
     * @param table_name Table name of the input key
     * @param key The key to be read
     * @param rec Key's record to be filled
     * @param read_type Read type
     * @param tx_number Tx number
     * @param tx_term Term of the tx node
     * @param ts Start timestamp of the tx
     * @param hres Result handler of the read request
     * @param iso_level Isolation level
     * @param proto Concurrency control (cc) protocol
     */
    virtual void Read(const TableName &table_name,
                      const TxKey &key,
                      TxRecord &rec,
                      ReadType read_type,
                      uint64_t tx_number,
                      int64_t tx_term,
                      const uint64_t ts,
                      CcHandlerResult<ReadKeyResult> &hres,
                      IsolationLevel iso_level = IsolationLevel::ReadCommitted,
                      CcProtocol proto = CcProtocol::OCC,
                      LockType lock_type = LockType::ReadLock) = 0;

    /**
     * @brief Brings the previously-read key's record into the cc map for
     * caching. The method is only called after Read(), which starts
     * concurrency control for a key not in the cc map and thus does not return
     * the key's record.
     *
     * @param tx_term Term of the tx node
     * @param rec Record to be cached
     * @param is_deleted Whether or not the record is deleted
     * @param cce_addr Address of the key's cc entry
     * @param hres Result handler of the request
     */
    virtual void ReadOutside(int64_t tx_term,
                             TxRecord &rec,
                             bool is_deleted,
                             const CcEntryAddr &cce_addr,
                             CcHandlerResult<ReadKeyResult> &hres) = 0;

    /**
     * @brief ReadLocal is used to read replicated cc maps, which contain a cc
     * map replica in every shard. An optimization for ReadLocal is to execute
     * the request directly without putting the request into the execution
     * queue.
     *
     * @param table_name Table name of the input key
     * @param key The key to be read
     * @param rec Key's record to be filled
     * @param read_type Read type
     * @param tx_number Tx number
     * @param tx_term Term of the tx node
     * @param ts Start timestamp of the tx
     * @param hres Result handler of the read request
     * @param iso_level Isolation level
     * @param proto Concurrency control (cc) protocol
     */
    virtual void ReadLocal(
        const TableName &table_name,
        const TxKey &key,
        TxRecord &record,
        ReadType read_type,
        uint64_t tx_number,
        int64_t tx_term,
        const uint64_t ts,
        CcHandlerResult<ReadKeyResult> &hres,
        IsolationLevel iso_level = IsolationLevel::RepeatableRead,
        CcProtocol proto = CcProtocol::Locking,
        LockType lock_type = LockType::ReadLock) = 0;

    virtual void ScanOpen(
        const TableName &table_name,
        ScanIndexType index_type,
        const TxKey &start_key,
        bool inclusive,
        uint64_t tx_number,
        int64_t tx_term,
        uint64_t start_ts,
        CcHandlerResult<ScanOpenResult> &hd_res,
        ScanDirection direction = ScanDirection::Forward,
        IsolationLevel iso_level = IsolationLevel::ReadCommitted,
        CcProtocol proto = CcProtocol::OCC,
        LockType lock_type = LockType::ReadLock,
        bool is_ckpt = false) = 0;

    virtual void ScanOpenLocal(
        const TableName &table_name,
        ScanIndexType index_type,
        const TxKey &start_key,
        bool inclusive,
        uint64_t tx_number,
        int64_t tx_term,
        uint64_t ts,
        CcHandlerResult<ScanOpenResult> &hd_res,
        ScanDirection direction = ScanDirection::Forward,
        IsolationLevel iso_level = IsolationLevel::ReadCommitted,
        CcProtocol proto = CcProtocol::OCC,
        LockType lock_type = LockType::ReadLock,
        bool is_ckpt_delta = false) = 0;

    virtual void ScanNextBatch(
        uint64_t tx_number,
        int64_t tx_term,
        uint64_t start_ts,
        CcScanner &scanner,
        CcHandlerResult<ScanNextResult> &hd_res,
        IsolationLevel iso_level = IsolationLevel::ReadCommitted,
        CcProtocol proto = CcProtocol::OCC,
        LockType lock_type = LockType::ReadLock) = 0;

    virtual void ScanNextBatchLocal(
        uint64_t tx_number,
        int64_t tx_term,
        uint64_t start_ts,
        CcScanner &scanner,
        CcHandlerResult<ScanNextResult> &hd_res,
        IsolationLevel iso_level = IsolationLevel::ReadCommitted,
        CcProtocol proto = CcProtocol::OCC) = 0;

    virtual void ScanClose(size_t alias,
                           const TxKey &end_key,
                           bool inclusive,
                           CcProtocol proto = CcProtocol::OCC,
                           LockType lock_type = LockType::ReadLock) = 0;

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

    virtual void FaultInject(const std::string &fault_name,
                             const std::string &fault_paras,
                             int64_t tx_term,
                             const TxId &txid,
                             std::vector<int> &vct_node_id,
                             CcHandlerResult<bool> &hres) = 0;

    virtual void DataStoreUpsertTable(const TableName &table_name,
                                      const TableSchema *schema,
                                      bool is_deleted,
                                      uint64_t commit_ts,
                                      CcHandlerResult<Void> &hres) = 0;

    virtual uint32_t GetNodeId() const = 0;
};

}  // namespace txservice
