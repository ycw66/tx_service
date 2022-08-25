#pragma once

#include <vector>

#include "cc_handler.h"
#include "cc_req_pool.h"
#include "cc_request.h"
#include "remote/remote_cc_handler.h"

namespace txservice
{
class LocalCcHandler : public CcHandler
{
public:
    LocalCcHandler() = delete;
    LocalCcHandler(const LocalCcHandler &rhs) = delete;

    LocalCcHandler(uint32_t thd_id, LocalCcShards &shards);

    void AcquireWrite(const TableName &table_name,
                      const TxKey &key,
                      TxNumber tx_number,
                      int64_t tx_term,
                      uint64_t ts,
                      bool is_insert,
                      CcHandlerResult<std::vector<AcquireKeyResult>> &hres,
                      uint32_t hd_res_idx,
                      const CcProtocol proto) override;

    void AcquireWriteAll(const TableName &table_name,
                         const TxKey &key,
                         NodeGroupId ng_id,
                         TxNumber txn,
                         int64_t tx_term,
                         bool is_insert,
                         CcHandlerResult<AcquireAllResult> &hres,
                         CcProtocol proto,
                         LockType lock_type) override;

    void PostWriteAll(const TableName &table_name,
                      const TxKey &key,
                      TxRecord &rec,
                      NodeGroupId ng_id,
                      uint64_t tx_number,
                      int64_t tx_term,
                      uint64_t commit_ts,
                      CcHandlerResult<PostProcessResult> &hres,
                      DmlOperation dml_op,
                      PostWriteType post_write_type) override;

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
    void PostWrite(uint64_t tx_number,
                   int64_t tx_term,
                   uint64_t commit_ts,
                   const CcEntryAddr &ccentry_addr,
                   const TxRecord *record,
                   bool is_deleted,
                   CcHandlerResult<PostProcessResult> &hres,
                   CcProtocol protocol) override;

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
    void PostRead(uint64_t tx_number,
                  int64_t tx_term,
                  uint64_t key_ts,
                  uint64_t gap_ts,
                  uint64_t commit_ts,
                  const CcEntryAddr &ccentry_addr,
                  CcHandlerResult<PostProcessResult> &hres,
                  CcProtocol protocol,
                  LockType lock_type) override;

    /// <summary>
    /// Starts concurrency control for the input key and returns the key's
    /// committed value, if there is any.
    /// </summary>
    /// <param name="table_name"></param>
    /// <param name="key"></param>
    /// <param name="record"></param>
    /// <param name="read_outside"></param>
    /// <param name="tx_number"></param>
    /// <param name="ts"></param>
    /// <param name="hres"></param>
    /// <param name="proto"></param>
    void Read(const TableName &table_name,
              const TxKey &key,
              TxRecord &record,
              ReadType read_type,
              uint64_t tx_number,
              int64_t tx_term,
              const uint64_t ts,
              CcHandlerResult<ReadKeyResult> &hres,
              IsolationLevel iso_level = IsolationLevel::ReadCommitted,
              CcProtocol proto = CcProtocol::OCC,
              LockType lock_type = LockType::ReadLock) override;

    void ReadOutside(int64_t tx_term,
                     TxRecord &rec,
                     bool is_deleted,
                     uint64_t commit_ts,
                     const CcEntryAddr &cce_addr,
                     CcHandlerResult<ReadKeyResult> &hres,
                     std::vector<VersionTxRecord> *archives = nullptr) override;

    void ReadLocal(const TableName &table_name,
                   const TxKey &key,
                   TxRecord &record,
                   ReadType read_type,
                   uint64_t tx_number,
                   int64_t tx_term,
                   const uint64_t ts,
                   CcHandlerResult<ReadKeyResult> &hres,
                   IsolationLevel iso_level = IsolationLevel::RepeatableRead,
                   CcProtocol proto = CcProtocol::Locking,
                   LockType lock_type = LockType::ReadLock) override;

    void ScanOpen(const TableName &table_name,
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
                  bool is_ckpt_delta = false) override;

    void ScanOpenLocal(const TableName &table_name,
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
                       bool is_ckpt_delta = false) override;

    void ScanNextBatch(uint64_t tx_number,
                       int64_t tx_term,
                       uint64_t start_ts,
                       CcScanner &scanner,
                       CcHandlerResult<ScanNextResult> &hd_res,
                       IsolationLevel iso_level = IsolationLevel::ReadCommitted,
                       CcProtocol proto = CcProtocol::OCC,
                       LockType lock_type = LockType::ReadLock) override;

    void ScanNextBatchLocal(
        uint64_t tx_number,
        int64_t tx_term,
        uint64_t start_ts,
        CcScanner &scanner,
        CcHandlerResult<ScanNextResult> &hd_res,
        IsolationLevel iso_level = IsolationLevel::ReadCommitted,
        CcProtocol proto = CcProtocol::OCC) override;

    void ScanClose(size_t alias,
                   const TxKey &end_key,
                   bool inclusive,
                   CcProtocol proto = CcProtocol::OCC,
                   LockType lock_type = LockType::ReadLock) override
    {
    }

    void UploadRecord(const TableName &table_name,
                      const TxKey &key,
                      TxRecord *record,
                      const CcEntryAddr &ccentry_addr,
                      CcHandlerResult<Void> &) override
    {
    }

    void UploadSecondaryKey(const TableName &table_name,
                            const TxKey &sk,
                            const TxKey &pk,
                            CcHandlerResult<Void> &) override
    {
    }

    /// <summary>
    /// Starts a new tx and returns the tx ID.
    /// </summary>
    /// <param name="entry"></param>
    /// <param name="local_time"></param>
    /// <param name="max_txn_execution_time_ms"></param>
    /// <param name=""></param>
    void NewTxn(CcHandlerResult<InitTxResult> &hres,
                IsolationLevel iso_level) override;

    /// <summary>
    /// Sets the commit timestamp of the input tx.
    /// </summary>
    /// <param name="txn_id"></param>
    /// <param name="commit_ts"></param>
    /// <param name="extension"></param>
    /// <param name=""></param>
    void SetCommitTimestamp(const TxId &txid,
                            uint64_t commit_ts,
                            CcHandlerResult<uint64_t> &hres) override;

    /// <summary>
    /// Negotiates with the input tx such that it commits no later than the
    /// input timestamp.
    /// </summary>
    /// <param name="txn_id"></param>
    /// <param name="commit_ts_lower_bound"></param>
    /// <param name=""></param>
    void UpdateCommitLowerBound(const TxId &txid,
                                uint64_t commit_ts_lower_bound,
                                CcHandlerResult<uint64_t> &) override;

    /// <summary>
    /// Sets the tx status to committed or aborted, after the fate of the tx is
    /// determined.
    /// </summary>
    /// <param name="txn_id"></param>
    /// <param name="status"></param>
    /// <param name="extension"></param>
    /// <param name=""></param>
    void UpdateTxnStatus(const TxId &txid,
                         IsolationLevel iso_level,
                         TxnStatus status,
                         CcHandlerResult<Void> &hres) override;

    void FaultInject(const std::string &fault_name,
                     const std::string &fault_paras,
                     int64_t tx_term,
                     const TxId &txid,
                     std::vector<int> &vct_node_id,
                     CcHandlerResult<bool> &hres) override;

    void DataStoreUpsertTable(const TableSchema *schema,
                              bool is_deleted,
                              uint64_t commit_ts,
                              CcHandlerResult<Void> &hres) override;

    void CleanCcEntryForTest(const TableName &table_name,
                             const TxKey &key,
                             bool only_archives,
                             bool flush,
                             uint64_t tx_number,
                             int64_t tx_term,
                             CcHandlerResult<bool> &hres) override;

    void DataStoreFindRangeMedianKey(
        int32_t partition,
        const TableSchema *table_schema,
        CcHandlerResult<RangeMedianKeyResult> &hd_res) override;

    void DataStoreCopyRangeData(int32_t old_partition_id,
                                int32_t new_partition_id,
                                const TxKey *start_key,
                                uint64_t tx_ts,
                                const TableSchema *table_schema,
                                CcHandlerResult<Void> &hd_res) override;

    void DataStoreUpsertRange(const TableSchema *table_schema,
                              txservice::TxKey *key,
                              int32_t partition_id,
                              int64_t ts,
                              CcHandlerResult<Void> &hd_res) override;

    void DataStoreDeleteOutOfRangeData(int32_t partition_id,
                                       const TxKey *start_key,
                                       const TableSchema *table_schema,
                                       CcHandlerResult<Void> &hd_res) override;
    /*
     * Get the node id which runs the current transaction.
     */
    uint32_t GetNodeId() const override;

    /*
     * Get the current ts_base value.
     */
    uint64_t GetTsBaseValue() const;

private:
    /// <summary>
    /// Thread Id is the local offset of the core to which the handler is
    /// pinned.
    /// </summary>
    uint32_t thd_id_;
    LocalCcShards &cc_shards_;
    remote::RemoteCcHandler remote_hd_;

    size_t scan_alias_cnt_;

    CcRequestPool<AcquireCc> acquire_pool;
    CcRequestPool<AcquireAllCc> acquire_all_pool_;
    CcRequestPool<PostWriteCc> postwrite_pool;
    CcRequestPool<PostWriteAllCc> postwrite_all_pool_;
    CcRequestPool<PostReadCc> postread_pool_;
    CcRequestPool<ReadCc> read_pool;
    CcRequestPool<NegotiateCc> negoti_pool;
    CcRequestPool<ScanOpenBatchCc> scan_open_pool;
    CcRequestPool<ScanNextBatchCc> scan_next_pool;
    CcRequestPool<FaultInjectCC> fault_inject_pool;
    CcRequestPool<CleanCcEntryForTestCc> clean_cc_entry_pool;

    friend class remote::RemoteCcHandler;
};
}  // namespace txservice
