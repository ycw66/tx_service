#pragma once

#include <memory>  // std::unique_ptr
#include <utility>
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
                      uint32_t key_shard_code,
                      TxNumber tx_number,
                      int64_t tx_term,
                      uint16_t command_id,
                      uint64_t ts,
                      bool is_insert,
                      CcHandlerResult<std::vector<AcquireKeyResult>> &hres,
                      uint32_t hd_res_idx,
                      CcProtocol proto,
                      IsolationLevel iso_level) override;

    void AcquireWriteAll(const TableName &table_name,
                         const TxKey &key,
                         NodeGroupId ng_id,
                         TxNumber txn,
                         int64_t tx_term,
                         uint16_t command_id,
                         bool is_insert,
                         CcHandlerResult<AcquireAllResult> &hres,
                         CcProtocol proto,
                         CcOperation cc_op) override;

    void PostWriteAll(const TableName &table_name,
                      const TxKey &key,
                      TxRecord &rec,
                      NodeGroupId ng_id,
                      uint64_t tx_number,
                      int64_t tx_term,
                      uint16_t command_id,
                      uint64_t commit_ts,
                      CcHandlerResult<PostProcessResult> &hres,
                      OperationType op_type,
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
                   uint16_t command_id,
                   uint64_t commit_ts,
                   const CcEntryAddr &ccentry_addr,
                   const TxRecord *record,
                   OperationType operation_type,
                   uint32_t key_shard_code,
                   CcHandlerResult<PostProcessResult> &hres) override;

    /// <summary>
    /// Installs the committed write and releases the write intention/lock after
    /// the tx commits. The operation unblocks the pending requests, if there
    /// are any, on the key.
    /// </summary>
    /// <param name="tx_number"></param>
    /// <param name="tx_term"></param>
    /// <param name="command_id"></param>
    /// <param name="commit_ts"></param>
    /// <param name="table_name"></param>
    /// <param name="key"></param>
    /// <param name="record"></param>
    /// <param name="operation_type"></param>
    /// <param name="expected_term">If this value is SKIP_CHECK_TERM, it means
    /// that the caller does not care the term, and there is no need to check
    /// the term</param>
    void UploadRecord(TxNumber tx_number,
                      int64_t tx_term,
                      uint16_t command_id,
                      uint64_t commit_ts,
                      const TableName &table_name,
                      const TxKey *key,
                      const TxRecord *record,
                      OperationType operation_type,
                      uint32_t key_shard_code,
                      CcHandlerResult<PostProcessResult> &hres,
                      int64_t expected_term = SKIP_CHECK_TERM) override;

    /// <summary>
    /// For OCC, validates whether or not the key has changed since the
    /// prior read.
    /// </summary>
    /// <param name="table_name"></param>
    /// <param name="key"></param>
    /// <param name="version_ts"></param>
    /// <param name="commit_ts"></param>
    /// <param name="extension"></param>
    /// <param name=""></param>
    void PostRead(uint64_t tx_number,
                  int64_t tx_term,
                  uint16_t command_id,
                  uint64_t key_ts,
                  uint64_t gap_ts,
                  uint64_t commit_ts,
                  const CcEntryAddr &ccentry_addr,
                  CcHandlerResult<PostProcessResult> &hres) override;

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
              uint32_t key_shard_code,
              TxRecord &record,
              ReadType read_type,
              uint64_t tx_number,
              int64_t tx_term,
              uint16_t command_id,
              const uint64_t ts,
              CcHandlerResult<ReadKeyResult> &hres,
              IsolationLevel iso_level = IsolationLevel::ReadCommitted,
              CcProtocol proto = CcProtocol::OCC,
              bool is_for_write = false,
              bool is_covering_keys = false) override;

    void ReadOutside(int64_t tx_term,
                     uint16_t command_id,
                     TxRecord &rec,
                     bool is_deleted,
                     uint64_t commit_ts,
                     const CcEntryAddr &cce_addr,
                     CcHandlerResult<ReadKeyResult> &hres,
                     std::vector<VersionTxRecord> *archives = nullptr) override;

    bool ReadLocal(const TableName &table_name,
                   const TxKey &key,
                   TxRecord &record,
                   ReadType read_type,
                   uint64_t tx_number,
                   int64_t tx_term,
                   uint16_t command_id,
                   const uint64_t ts,
                   CcHandlerResult<ReadKeyResult> &hres,
                   IsolationLevel iso_level = IsolationLevel::RepeatableRead,
                   CcProtocol proto = CcProtocol::Locking,
                   bool is_for_write = false,
                   bool is_recovering = false,
                   bool execute_immediately = true) override;

    void ScanOpen(const TableName &table_name,
                  ScanIndexType index_type,
                  const TxKey &start_key,
                  bool inclusive,
                  uint64_t tx_number,
                  int64_t tx_term,
                  uint16_t command_id,
                  uint64_t ts,
                  CcHandlerResult<ScanOpenResult> &hd_res,
                  ScanDirection direction = ScanDirection::Forward,
                  IsolationLevel iso_level = IsolationLevel::ReadCommitted,
                  CcProtocol proto = CcProtocol::OCC,
                  bool is_for_write = false,
                  bool is_ckpt_delta = false,
                  bool is_covering_keys = false) override;

    void ScanOpenLocal(const TableName &table_name,
                       ScanIndexType index_type,
                       const TxKey &start_key,
                       bool inclusive,
                       uint64_t tx_number,
                       int64_t tx_term,
                       uint16_t command_id,
                       uint64_t ts,
                       CcHandlerResult<ScanOpenResult> &hd_res,
                       ScanDirection direction = ScanDirection::Forward,
                       IsolationLevel iso_level = IsolationLevel::ReadCommitted,
                       CcProtocol proto = CcProtocol::OCC,
                       bool is_for_write = false,
                       bool is_ckpt_delta = false) override;

    void ScanNextBatch(uint64_t tx_number,
                       int64_t tx_term,
                       uint16_t command_id,
                       uint64_t start_ts,
                       CcScanner &scanner,
                       CcHandlerResult<ScanNextResult> &hd_res) override;

    void ScanNextBatch(const TableName &tbl_name,
                       uint32_t range_id,
                       NodeGroupId range_owner,
                       int64_t cc_ng_term,
                       const TxKey *start_key,
                       bool start_inclusive,
                       const TxKey *end_key,
                       bool end_inclusive,
                       uint8_t prefetch_size,
                       uint64_t read_ts,
                       uint64_t tx_number,
                       int64_t tx_term,
                       uint16_t command_id,
                       CcHandlerResult<RangeScanSliceResult> &hd_res,
                       IsolationLevel iso_level = IsolationLevel::ReadCommitted,
                       CcProtocol proto = CcProtocol::OCC) override;

    void ScanNextBatchLocal(uint64_t tx_number,
                            int64_t tx_term,
                            uint16_t command_id,
                            uint64_t start_ts,
                            CcScanner &scanner,
                            CcHandlerResult<ScanNextResult> &hd_res) override;

    void ScanClose(const TableName &table_name,
                   ScanDirection direction,
                   std::unique_ptr<CcScanner> scanner) override;

    /// <summary>
    /// Starts a new tx and returns the tx ID.
    /// </summary>
    /// <param name="entry"></param>
    /// <param name="local_time"></param>
    /// <param name="max_txn_execution_time_ms"></param>
    /// <param name=""></param>
    void NewTxn(CcHandlerResult<InitTxResult> &hres,
                IsolationLevel iso_level,
                NodeGroupId tx_ng_id,
                uint32_t log_group_id) override;

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

    void ReloadCache(NodeGroupId ng_id,
                     TxNumber tx_number,
                     int64_t tx_term,
                     uint16_t command_id,
                     CcHandlerResult<Void> &hres) override;

    void FaultInject(const std::string &fault_name,
                     const std::string &fault_paras,
                     int64_t tx_term,
                     uint16_t command_id,
                     const TxId &txid,
                     std::vector<int> &vct_node_id,
                     CcHandlerResult<bool> &hres) override;

    void DataStoreUpsertTable(
        const TableSchema *schema,
        OperationType op_type,
        uint64_t commit_ts,
        CcHandlerResult<Void> &hres,
        const txservice::AlterTableInfo *alter_table_info = nullptr) override;

    void AnalyzeTableAll(const TableName &table_name,
                         NodeGroupId ng_id,
                         TxNumber tx_number,
                         int64_t tx_term,
                         uint16_t command_id,
                         CcHandlerResult<Void> &hres) override;

    void ObjectCommand(const TableName &table_name,
                       const TxKey &key,
                       uint32_t key_shard_code,
                       const TxCommand &obj_cmd,
                       TxCommandResult &obj_cmd_result,
                       TxNumber txn,
                       int64_t tx_term,
                       uint64_t tx_ts,
                       CcHandlerResult<ObjectCommandResult> &hres,
                       const CcProtocol proto,
                       bool commit) override;

    void CleanCcEntryForTest(const TableName &table_name,
                             const TxKey &key,
                             bool only_archives,
                             bool flush,
                             uint64_t tx_number,
                             int64_t tx_term,
                             uint16_t command_id,
                             CcHandlerResult<bool> &hres) override;

    /*
     * Get the node id which runs the current transaction.
     */
    uint32_t GetNodeId() const override;

    /*
     * Get the current ts_base value.
     */
    uint64_t GetTsBaseValue() const;

    void BlockCcReqCheck(uint64_t tx_number,
                         int64_t tx_term,
                         uint16_t command_id,
                         const CcEntryAddr &cce_addr,
                         CcHandlerResultBase *hres,
                         ResultTemplateType type) override;

    /// <summary>
    /// Kickout the ccentrise whose commit_ts less than @@ckpt_ts and betwen
    /// start_key and end_key from ccmap.
    /// </summary>
    /// <param name="table_name"></param>
    /// <param name="ng_id">Id of the node group that to exec. the req.</param>
    /// <param name="tx_number">Tx number</param>
    /// <param name="tx_term">Term of the tx node</param>
    /// <param name="command_id"></param>
    /// <param name="commit_ts"></param>
    /// <param name="hres">hres</param>
    void KickoutData(const TableName &table_name,
                     uint32_t ng_id,
                     TxNumber tx_number,
                     int64_t tx_term,
                     uint64_t command_id,
                     uint64_t commit_ts,
                     CcHandlerResult<Void> &hres,
                     CleanType clean_type,
                     const TxKey *start_key = nullptr,
                     const TxKey *end_key = nullptr) override;

private:
    /// <summary>
    /// Thread Id is the local offset of the core to which the handler is
    /// pinned.
    /// </summary>
    uint32_t thd_id_;
    LocalCcShards &cc_shards_;
    remote::RemoteCcHandler remote_hd_;

    CcRequestPool<AcquireCc> acquire_pool;
    CcRequestPool<AcquireAllCc> acquire_all_pool_;
    CcRequestPool<PostWriteCc> postwrite_pool;
    CcRequestPool<PostWriteAllCc> postwrite_all_pool_;
    CcRequestPool<PostReadCc> postread_pool_;
    CcRequestPool<ReadCc> read_pool;
    CcRequestPool<NegotiateCc> negoti_pool;
    CcRequestPool<ScanOpenBatchCc> scan_open_pool;
    CcRequestPool<ScanNextBatchCc> scan_next_pool;
    CcRequestPool<ScanSliceCc> scan_slice_pool;
    CcRequestPool<AnalyzeTableAllCc> analyze_table_all_pool;
    CcRequestPool<FaultInjectCC> fault_inject_pool;
    CcRequestPool<CleanCcEntryForTestCc> clean_cc_entry_pool;
    CcRequestPool<KickoutCcEntryCc> kickout_ccentry_pool_;
    CcRequestPool<ApplyCc> apply_pool;

    CircularQueue<std::unique_ptr<CcScanner>> pk_forward_scanner_{64};
    CircularQueue<std::unique_ptr<CcScanner>> pk_backward_scanner_{64};
    CircularQueue<std::unique_ptr<CcScanner>> sk_forward_scanner_{64};
    CircularQueue<std::unique_ptr<CcScanner>> sk_backward_scanner_{64};

    friend class remote::RemoteCcHandler;
};
}  // namespace txservice
