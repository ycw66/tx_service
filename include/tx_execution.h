#pragma once

#include <memory>
#include <stack>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "catalog_key_record.h"
#include "cc/cc_handler.h"
#include "cc/ccm_scanner.h"
#include "cc_protocol.h"
#include "log_closure.h"
#include "read_write_set.h"
#include "tx_operation.h"
#include "tx_req_result.h"
#include "txlog.h"

namespace txservice
{
struct TxRequest;
struct InitTxRequest;
struct ReadTxRequest;
struct ReadOutsideTxRequest;
struct ScanOpenTxRequest;
struct ScanNextTxRequest;
struct ScanCloseTxRequest;
struct UpsertTxRequest;
struct CommitTxRequest;
struct AbortTxRequest;
struct UpsertTableTxRequest;
struct FaultInjectTxRequest;
struct CleanCcEntryForTestTxRequest;
struct CleanArchivesTxRequest;
struct SplitRangeTxRequest;

class TxProcessor;

class TransactionExecution
{
public:
    using uptr = std::unique_ptr<TransactionExecution>;

    // The number of read/write/scan keys when the tx is considered to be
    // "large"
    static const uint32_t LargeTxKeySize = 1000;

    TransactionExecution(CcHandler *handler,
                         TxLog *tx_log,
                         TxProcessor *tx_processor,
                         CcProtocol proto = CcProtocol::OCC);

    TransactionExecution(const TransactionExecution &) = delete;

    /**
     * Interface for TxProcessor.
     */
    /**
     * @brief Resets the internal states of the tx state machine. Called when
     * the tx finishes.
     *
     * @param proto The concurrency control protocol.
     */
    void Reset(CcProtocol proto = CcProtocol::OCC);

    /**
     * @brief Restarts the tx state machine when it is reused for a new
     * user-level tx, allowing it to receive tx requests.
     *
     */
    void Restart();

    void Recycle()
    {
        assert(tx_status_.load(std::memory_order_relaxed) ==
               TxnStatus::Finished);
        tx_status_.store(TxnStatus::Recycled, std::memory_order_relaxed);
    }

    /**
     * @brief Check whether transction is idle and waiting for new TxRequest
     * from runtime.
     */
    bool Idle() const;

    /**
     * @brief Process different kinds of TxRequests based on the request state
     * machine by TxProcessor.
     */

    /**
     * @brief BeginRequest specifies the isolation level and cc protocol.
     */
    void ProcessTxRequest(InitTxRequest &begin_req);
    void ProcessTxRequest(ReadTxRequest &read_req);
    void ProcessTxRequest(ReadOutsideTxRequest &read_outside_req);
    void ProcessTxRequest(ScanOpenTxRequest &scan_open_req);
    void ProcessTxRequest(ScanNextTxRequest &scan_next_req);
    void ProcessTxRequest(ScanCloseTxRequest &scan_close_req);
    void ProcessTxRequest(UpsertTxRequest &upsert_req);
    void ProcessTxRequest(CommitTxRequest &commit_req);
    void ProcessTxRequest(AbortTxRequest &abort_req);
    void ProcessTxRequest(UpsertTableTxRequest &req);
    void ProcessTxRequest(FaultInjectTxRequest &fi_req);
    void ProcessTxRequest(CleanCcEntryForTestTxRequest &clean_req);
    void ProcessTxRequest(CleanArchivesTxRequest &clean_req);
    void ProcessTxRequest(SplitRangeTxRequest &range_split_req);

    /**
     * Interface for storage engine runtime.
     */
    /**
     * Put request into next_req_ and wait to be processed.
     * The hypothesis is that a client can only execute one request at a time,
     * and needs to call request.Wait() to wait for finish signal.
     */
    int Execute(TxRequest *tx_req);

    /**
     * General Interface
     */
    uint64_t TxNumber() const;

    int64_t TxTerm() const;

    uint16_t CommandId() const;

    uint32_t TxCcNodeId() const;

    TxnStatus TxStatus() const;

    void RecoverSchemaTx(const ::txlog::SchemaOpMessage &schema_op,
                         uint64_t txn,
                         int64_t tx_term,
                         uint64_t commit_ts);

    void RecoverSplitRangeTx(
        const ::txlog::SplitRangeOpMessage &ds_split_range_op_msg,
        const TableSchema *table_schema,
        const TxKey *range_key,
        std::unique_ptr<RangeRecord> splitting_range_record,
        uint32_t partition_id,
        std::unique_ptr<TxKey> new_range_key,
        uint32_t new_partition_id,
        uint64_t txn,
        int64_t tx_term,
        uint64_t commit_ts);

    std::string GetErrorMessage() const;

    void SetErrorMessage(const std::string &err_msg);

    CcProtocol GetCcProtocol() const
    {
        return protocol_;
    }

    IsolationLevel GetIsolationLevel() const
    {
        return iso_level_;
    }

    void SetIsolationLevel(IsolationLevel iso_level)
    {
        iso_level_ = iso_level;
    }

    uint64_t GetStartTs() const
    {
        return start_ts_;
    }

    void SetStartTs(uint64_t ts)
    {
        start_ts_ = ts;
    }

    /**
     * @brief A tx enlists itself in the binding tx processor for execution,
     * when (1) one of its local/remote cc requests returns a response, or (2)
     * it receives a tx request from the external user.
     *
     * @param remote_response Whether or not enlisting is the result of a remote
     * response.
     */
    void EnlistToExecute(bool remote_response, bool skip_remote_cnt);

    /**
     * @brief Before a tx sends a remote cc request, enlists itself into the
     * waiting queue of the binding tx processor. The tx processor periodically
     * checks the tx's in the waiting queue and forces them to retry or abort,
     * if remote requests time out because of remote failures.
     *
     */
    void EnlistToWait();

    /**
     * @brief The tx has timed out and is about to move foward to retry or
     * abort. Removes itself from the waiting queue of the binding tx processor.
     *
     */
    void ForceToForward();

private:
    /**
     * @brief Moves forward the tx state machine and transitions the machine to
     * next state if not blocked on the current state. A user's tx request
     * causes the tx state machine to transition. A single tx requests may lead
     * to a chain of transitions to different states. For example, a commit
     * request consists of acquiring write intentions, setting the commit
     * timestamp, writing the tx log and installing committed values. Some
     * transitions may not return immediately, causing the tx state machine to
     * be blocked on the current state. The tx state machine employs async
     * programming. Forward() checks if the current transition finishes, and if
     * so, moves to the next state, until reaching the final state of the
     * current tx request and notifying the user the tx request's result. When
     * being blocked, Forward() returns the control to the processing thread,
     * allowing it to switch to another tx state machine or process concurrency
     * control requests directed to the binding shard.
     *
     */
    void Forward();

    void PushOperation(TransactionOperation *op, int retry_num = RETRY_NUM);

    /**
     * @brief Move forward the tx's ts, either as the commit_ts_bound_ +1 or
     * candidate_ts +1, depends on which one is max
     */
    void ForwardTs(uint64_t candidate_ts = 0);

    /**
     * @brief Mark transaction as failed, and forward to clean up step
     */
    void MarkFailed();

    /**
     * Process Operations.
     * The TxRequest is responsible for putting the corresponding operations
     * into state_stack. The first Forward call will start to process these
     * operations.
     */

    void Process(InitTxnOperation &init_txn);
    void PostProcess(InitTxnOperation &init_txn);
    void Process(ReadOperation &read);
    void PostProcess(ReadOperation &read);
    void Process(ScanOpenOperation &scan_open);
    void PostProcess(ScanOpenOperation &scan_open);
    void Process(ScanNextOperation &scan_next);
    void PostProcess(ScanNextOperation &scan_next);
    void Process(AcquireWriteOperation &acquire_write);
    void PostProcess(AcquireWriteOperation &acquire_write);
    void Process(SetCommitTsOperation &set_ts);
    void PostProcess(SetCommitTsOperation &set_ts);
    void Process(ValidateOperation &validate);
    void PostProcess(ValidateOperation &validate);
    void Process(UpdateTxnStatus &update_txn);
    void PostProcess(UpdateTxnStatus &update_txn);
    void Process(PostProcessOp &post_process);
    void PostProcess(PostProcessOp &post_process);
    void Process(WriteToLogOp &write_log);
    void PostProcess(WriteToLogOp &write_log);
    void Process(FaultInjectOp &fault_inject_op);
    void PostProcess(FaultInjectOp &fault_inject_op);
    void Process(CleanCcEntryForTestOp &clean_entry_op);
    void PostProcess(CleanCcEntryForTestOp &clean_entry_op);

    void Process(AcquireAllOp &acq_all_op);
    void PostProcess(AcquireAllOp &acq_all_op);
    void Process(PostWriteAllOp &post_write_all_op);
    void PostProcess(PostWriteAllOp &post_write_all_op);

    void Process(DsUpsertTableOp &ds_upsert_table_op);
    void PostProcess(DsUpsertTableOp &ds_upsert_table_op);

    void Process(DsSplitRangeOp &ds_split_range_op);
    void PostProcess(DsSplitRangeOp &ds_split_range_op);

    void Process(NoOp &no_op);
    void PostProcess(NoOp &no_op);

    template <typename ResultType>
    void Process(DsOp<ResultType> &ds_op);
    template <typename ResultType>
    void PostProcess(DsOp<ResultType> &ds_op);

    // Process TxRequests without Operations. These TxRequests can be executed
    // immediately without using CcRequests.
    void ScanClose(size_t alias,
                   const TxKey &end_key,
                   const TableName &table_name);

    // drain out scan cache and move ScanTuple into readset
    void DrainOutScanCache(const TableName &table_name, CcScanner &scanner);

    void Update(const TableName &table_name,
                TxKey::Uptr key,
                TxRecord::Uptr rec);

    void Delete(const TableName &table_name, TxKey::Uptr key);

    void Upsert(const TableName &table_name,
                TxKey::Uptr key,
                TxRecord::Uptr rec,
                DmlOperation op);

    void Insert(const TableName &table_name,
                TxKey::Uptr key,
                TxRecord::Uptr rec);

    void Commit();
    void Abort();

    void FillDataLogRequest(WriteToLogOp &write_log);

    bool IsTimeOut(int wait_secs = 10);
    void StartTiming();

    enum struct TxType
    {
        Data = 0,
        Schema,
        PartitionFunction
    };

    CcHandler *handler;
    TxLog *txlog_;
    TxProcessor *const tx_processor_;

    TxId txid_;
    // The tx number is a global identifier of the tx in the cluster. It differs
    // from TxId in that TxId includes additional information to physically
    // locate the tx entry without additional lookups. The tx number is wrapped
    // by std::atomic so as to allow a remote cc request's response to match
    // against the tx number before setting the cc handler result. Matching tx
    // number is necessary because the tx may abort proactively, after not
    // receiving the response of the cc request for an extended period of time.
    std::atomic<uint64_t> tx_number_;
    int64_t tx_term_;
    uint64_t start_ts_;
    uint64_t commit_ts_;
    uint64_t commit_ts_bound_;
    std::atomic<TxnStatus> tx_status_;

    // The command id is a identifier to distinguish whether the cc request's
    // response is expired. For remote cc request's responses do not
    // guarantee the sequence and we re-run tx operations after timeout, that
    // is, same one operation's cc requests maybe sent repeatedly even if the
    // previous one is being handled, the result is chaos once some one of
    // the expired cc request's responses reaches before transaction finishing.
    // The {command_id_} increases when TxExectuion handle a new TxRequest or
    // send remote cc requests.
    std::atomic<uint16_t> command_id_;

    // The local time when the tx machine first moves to its current state.
    uint64_t state_clock_;

    std::vector<TransactionOperation *> state_stack_;
    TransactionOperation *prev_op_;
    size_t idle_rep_;

    // local cache of read/write entries.
    ReadWriteSet rw_set_;
    // when read an entry, it may not exist in ccmap. In this case, we create a
    // empty record in ccmap and add read intention for it. Then we read the
    // entry from data store and backfill the ccmap. cache_miss_read_cce_addr_
    // can help us to locate the previous empty cc entry quickly.
    CcEntryAddr cache_miss_read_cce_addr_;

    std::unique_ptr<SchemaOp> schema_op_;

    std::unique_ptr<DsSplitRangeOp> ds_split_range_op_;

    std::unordered_map<
        size_t,
        std::pair<TableWriteSet::const_iterator, TableWriteSet::const_iterator>>
        wset_iters_;
    std::unordered_map<size_t,
                       std::pair<TableWriteSet::const_reverse_iterator,
                                 TableWriteSet::const_reverse_iterator>>
        wset_reverse_iters_;

    /**
     * @brief A collection of open primary/secondary index scans. Each scan is
     * identified by a scan alias (of type size_t) generated when the scan is
     * opened.
     *
     */
    std::unordered_map<size_t, std::unique_ptr<CcScanner>> scans_;

    // Response whose returned result is void
    TxResult<Void> *void_resp_;
    // Response whose returned result is record
    TxResult<RecordStatus> *rec_resp_;
    // Response whose returned result is bool
    TxResult<bool> *bool_resp_;
    // Scan result
    TxResult<
        std::tuple<const TxKey *, const TxRecord *, RecordStatus, uint64_t>>
        *kvp_resp_;
    // Scan open result
    TxResult<size_t> *uint64_resp_;

    // detailed error message which indicates why does the transaction failed.
    // For example, during write log phase or validation phase.
    std::string detailed_error_msg_;

    // next_req_ is used to exchange request between runtime and TxProcessor.
    std::atomic<TxRequest *> next_req_;

    IsolationLevel iso_level_{IsolationLevel::ReadCommitted};
    CcProtocol protocol_{CcProtocol::OCC};

    // Initialization phase.
    InitTxnOperation init_txn_;

    // Execution phase.
    ReadOperation read_;
    ScanOpenOperation scan_open_;
    ScanNextOperation scan_next_;

    // Committing phase.
    AcquireWriteOperation acquire_write_;
    SetCommitTsOperation set_ts_;
    ValidateOperation validate_;
    UpdateTxnStatus update_txn_;
    PostProcessOp post_process_;
    WriteToLogOp write_log_;
    SleepOperation sleep_op_;

    // fault inject
    FaultInjectOp fault_inject_op_;

    // clean archives
    CleanCcEntryForTestOp clean_entry_op_;

    friend struct TransactionOperation;
    friend struct CompositeTransactionOperation;
    friend struct ReadOperation;
    friend struct ReadOutsideOperation;
    friend struct AcquireWriteOperation;
    friend struct SetCommitTsOperation;
    friend struct WriteToLogOp;
    friend struct UpdateTxnStatus;
    friend struct ValidateOperation;
    friend struct InitTxnOperation;
    friend struct PostProcessOp;
    friend struct ScanOpenOperation;
    friend struct ScanNextOperation;
    friend struct FaultInjectOp;
    friend struct AcquireAllOp;
    friend struct PostWriteAllOp;
    friend struct UpsertTableOp;
    friend struct DsUpsertTableOp;
    friend struct SleepOperation;
    friend struct CleanCcEntryForTestOp;
    friend struct CleanArchivesOp;
    friend struct DsSplitRangeOp;
    friend struct DsCopyRangeDataOp;
    friend struct DsFindRangeMedianKeyOp;
    friend struct DsDeleteOutOfRangeDataOp;
    friend struct DsUpsertRangeOp;
    friend struct NoOp;
    template <typename ResultType>
    friend struct DsOp;
    friend class TxProcessor;
};
}  // namespace txservice
