#pragma once

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
                      const TxId &txid,
                      int64_t tx_term,
                      uint64_t ts,
                      bool is_insert,
                      CcHandlerResult<AcquireKeyResult> &hres,
                      const CcProtocol proto = CcProtocol::OCC) override;

    void AcquireTableWriteLock(
        const TableName &table_name,
        const TxId &txid,
        int64_t tx_term,
        uint64_t tx_number,
        CcHandlerResult<std::unordered_map<uint32_t, int64_t>> &hres) override;

    void ReleaseTableWriteLock(const TableName &table_name,
                               const TxId &txid,
                               int64_t tx_term,
                               uint64_t tx_number,
                               CcHandlerResult<Void> &hres) override;

    /// <summary>
    /// Releases the write intention/lock for the input key after the tx aborts.
    /// The operation also unblocks the pending requests on the same key.
    /// </summary>
    /// <param name="table_name"></param>
    /// <param name="key"></param>
    /// <param name="txid"></param>
    /// <param name="extension"></param>
    /// <param name=""></param>
    void ReleaseWrite(uint64_t tx_number,
                      int64_t tx_term,
                      const CcEntryAddr &ccentry_addr,
                      CcHandlerResult<Void> &) override;

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
    void CommitWrite(uint64_t tx_number,
                     int64_t tx_term,
                     uint64_t commit_ts,
                     const CcEntryAddr &ccentry_addr,
                     const TxRecord &record,
                     bool is_deleted,
                     CcHandlerResult<Void> &hres) override;

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
    void ValidateRead(uint64_t tx_number,
                      int64_t tx_term,
                      uint64_t key_ts,
                      uint64_t gap_ts,
                      uint64_t commit_ts,
                      const CcEntryAddr &ccentry_addr,
                      CcHandlerResult<std::vector<TxId>> &hres) override;

    void PostprocessRead(uint64_t tx_number,
                         int64_t tx_term,
                         const CcEntryAddr &ccentry_addr,
                         CcHandlerResult<Void> &hres,
                         CcProtocol proto = CcProtocol::OCC) override;

    void CommitCreateTable(const TableName &table_name,
                           const unsigned char *catalog_image_,
                           size_t catalog_length_,
                           int64_t tx_term,
                           const TxId &txid,
                           uint64_t ts,
                           CcHandlerResult<Void> &hresult) override;

    void CommitDropTable(const TableName &table_name,
                         int64_t tx_term,
                         const TxId &txid,
                         uint64_t ts,
                         CcHandlerResult<Void> &hresult) override;

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
              CcProtocol proto = CcProtocol::OCC) override;

    void ReadOutside(TxRecord &rec,
                     bool is_deleted,
                     const CcEntryAddr &cce_addr,
                     CcHandlerResult<ReadKeyResult> &hres) override;

    void ScanOpen(const TableName &table_name,
                  ScanIndexType index_type,
                  const TxKey &start_key,
                  bool inclusive,
                  uint64_t tx_number,
                  int64_t tx_term,
                  uint64_t ts,
                  CcHandlerResult<ScanOpenResult> &hd_res,
                  ScanDirection direction = ScanDirection::Forward,
                  CcProtocol proto = CcProtocol::OCC,
                  bool is_ckpt_delta = false) override;

    void ScanNextBatch(uint64_t tx_number,
                       int64_t tx_term,
                       uint64_t start_ts,
                       CcScanner &scanner,
                       CcHandlerResult<ScanNextResult> &hd_res,
                       CcProtocol proto = CcProtocol::OCC) override;

    void ScanClose(size_t alias,
                   const TxKey &end_key,
                   bool inclusive,
                   CcProtocol proto = CcProtocol::OCC) override
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

    void CommitSecondaryKey(const TableName &table_name,
                            const TxKey &sk,
                            const TxKey &pk,
                            bool is_delete,
                            uint64_t ts,
                            CcHandlerResult<Void> &) override;

    /// <summary>
    /// Starts a new tx and returns the tx ID.
    /// </summary>
    /// <param name="entry"></param>
    /// <param name="local_time"></param>
    /// <param name="max_txn_execution_time_ms"></param>
    /// <param name=""></param>
    void NewTxn(CcHandlerResult<InitTxResult> &hres) override;

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
                         TxnStatus status,
                         CcHandlerResult<Void> &hres) override;

    void FindCatalogInCCShard(const TableName &table_name,
                              std::string *catalog_content,
                              uint64_t tx_number,
                              CcHandlerResult<bool> &hres) override;

    void CheckCatalogVersionInCCShard(const TableName &table_name,
                                      std::string *source_version,
                                      uint64_t tx_number,
                                      CcHandlerResult<bool> &hres) override;

    void FaultInject(const std::string &fault_name,
                     const std::string &fault_type,
                     int64_t tx_term,
                     const TxId &txid,
                     int node_id,
                     CcHandlerResult<bool> &hres) override;

    void ReleaseAllTableLocks(std::unordered_set<std::string> opened_table_set,
                              uint64_t tx_number,
                              CcHandlerResult<bool> &hres) override;

    /*
     * Get the node id which runs the current transaction.
     */
    uint32_t GetNodeId() const override;

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
    CcRequestPool<AcquireTableWriteLockCC> table_write_lock_pool;
    CcRequestPool<ReleaseTableWriteLockCC> release_table_write_lock_pool;
    CcRequestPool<PostDeleteCc> postdel_pool;
    CcRequestPool<PostCommitCc> postcommit_pool;
    CcRequestPool<ValidateCc> reread_pool;
    CcRequestPool<PostReadCc> postread_pool;
    CcRequestPool<ReadCc> read_pool;
    CcRequestPool<NegotiateCc> negoti_pool;
    CcRequestPool<CommitSkCc> commitsk_pool;
    CcRequestPool<ScanOpenBatchCc> scan_open_pool;
    CcRequestPool<ScanNextBatchCc> scan_next_pool;
    CcRequestPool<CommitCreateTableCC> commit_create_table_pool;
    CcRequestPool<CommitDropTableCC> commit_drop_table_pool;
    CcRequestPool<FindCatalogCC> commit_find_catalog_pool;
    CcRequestPool<CheckCatalogCC> commit_check_catalog_pool;
    CcRequestPool<FaultInjectCC> fault_inject_pool;

    friend class remote::RemoteCcHandler;
};
}  // namespace txservice
