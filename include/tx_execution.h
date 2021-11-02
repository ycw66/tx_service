#pragma once

#include <unordered_set>

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
struct BeginRequest;
struct ReadRequest;
struct ReadOutsideRequest;
struct ScanOpenRequest;
struct ScanNextRequest;
struct ScanCloseRequest;
struct UpsertRequest;
struct CommitRequest;
struct AbortRequest;
struct CreateTableRequest;
struct DropTableRequest;
struct FetchCatalogRequest;
struct CheckCatalogVersionRequest;
struct FaultInjectRequest;

class TransactionExecution
{
public:
    using uptr = std::unique_ptr<TransactionExecution>;

    // The number of read/write/scan keys when the tx is considered to be
    // "large"
    static const size_t LargeTxKeySize = 1000;

    TransactionExecution(CcHandler *handler,
                         TxLog *tx_log,
                         CcProtocol proto = CcProtocol::OCC);

    TransactionExecution(const TransactionExecution &) = delete;

    void Reset(CcProtocol proto = CcProtocol::OCC);

    TxResult<Void> *Begin(uint64_t start_ts = 0);

    TxResult<RecordStatus> *Read(const TableName &table_name,
                                 const TxKey &key,
                                 TxRecord &record,
                                 ReadType read_type = ReadType::Inside);

    TxResult<RecordStatus> *ReadOutside(TxRecord &record, bool is_deleted);

    TxResult<size_t> *ScanOpen(const TableName &table_name,
                               ScanIndexType indx_type,
                               const TxKey &start_key,
                               bool inclusive = true,
                               ScanDirection direction = ScanDirection::Forward,
                               bool is_ckpt_delta = false);

    TxResult<std::tuple<const TxKey *, const TxRecord *, bool>> *ScanNext(
        size_t alias);

    void ScanClose(size_t alias, const TxKey &end_key);

    TxResult<Void> *Update(const TableName &table_name,
                           TxKeyContainer &key,
                           TxRecordContainer &rec,
                           SecondaryKeys *skeys = nullptr);

    TxResult<Void> *Delete(const TableName &table_name,
                           TxKeyContainer &key,
                           SecondaryKeys *skeys = nullptr);

    TxResult<Void> *Upsert(const TableName &table_name,
                           TxKeyContainer &key,
                           TxRecordContainer &rec,
                           SecondaryKeys *skeys = nullptr,
                           Operation op = Operation::Upsert);

    TxResult<Void> *Insert(const TableName &table_name,
                           TxKeyContainer &key,
                           TxRecordContainer &rec,
                           SecondaryKeys *skeys = nullptr);

    TxResult<bool> *Commit();

    TxResult<bool> *Abort();

    TxResult<bool> *CreateTable();

    TxResult<bool> *DropTable();

    TxResult<bool> *FetchCatalog();

    TxResult<bool> *CheckCatalogVersion();

    void FindCatalogFinish(bool succeed);

    void RequestFinish(bool succeed);

    void WriteDDLLog();

    bool Idle() const
    {
        return current_op_ == nullptr;
    }

    // Forward the operation state machine.
    // A request may consist of several steps, e.g. for commit request,
    // it includes operations like setts, writelog, postprocess etc.
    // Forward is used to whether the previous operation is finished,
    // and move forward to the next operation if finished.
    // when current_op_ is null, the state machine ends.
    void Forward()
    {
        if (current_op_ == nullptr)
            return;

        prev_op_ = current_op_;
        current_op_->Forward(this);
    }

    // Put request into next_req_ and wait to be processed.
    // The hypothesis is that client can only execute one request at a time,
    // and needs to call request.Wait() to wait for finish signal.
    int Execute(TxRequest *tx_req);
    uint64_t TxNumber() const;

    void Process(BeginRequest &begin_req);
    void Process(ReadRequest &read_req);
    void Process(ReadOutsideRequest &read_outside_req);
    void Process(ScanOpenRequest &scan_open_req);
    void Process(ScanNextRequest &scan_next_req);
    void Process(ScanCloseRequest &scan_close_req);
    void Process(UpsertRequest &upsert_req);
    void Process(CommitRequest &commit_req);
    void Process(AbortRequest &abort_req);
    void Process(CreateTableRequest &ct_req);
    void Process(DropTableRequest &dt_req);
    void Process(FetchCatalogRequest &fc_req);
    void Process(CheckCatalogVersionRequest &ccv_req);
    void Process(FaultInjectRequest &fi_req);

private:
    void PostBegin();
    void PostRead();
    void PostScanOpen();
    void PostScanNext();
    void PostScanClose();
    void Upload();
    void PostUpload();
    void SetTs();
    void PostSetTs();
    void Vali();
    void PostVali();
    void WriteLog();
    void PostWriteLog();
    void SetTxStatus();
    void PostSetTxStatus();
    void PostProcess();
    // release all the table level lock for this transaction.
    void ReleaseAllTableLocks();
    void PostPostProcess();
    void PostProcessCreateTable();
    void ReleaseTableWriteLock();
    void AcquireTableWriteLock();
    void PostProcessDropTable();
    void FindCatalogInCCShard();
    void CheckCatalogInCCShard();
    TxResult<bool> *FaultInject(const std::string &fault_name,
                                const std::string &fault_type,
                                int node_id);

    enum struct DDLType
    {
        UNKNOWN,
        CREATE_TABLE,
        DROP_TABLE
    };

    CcHandler *handler;
    TxLog *txlog_;

    TxId txid_;
    // The tx number is a global identifier of the tx in the cluster. It differs
    // from TxId in that TxId includes additional information to physically
    // locate the tx entry without additional lookups. The tx number is wrapped
    // by std::atomic so as to allow a remote cc request's response to match
    // against the tx number before setting the cc handler result. Matching tx
    // number is necessary because the tx may abort proactively, after not
    // receiving the response of the cc request for an extended period of time,
    // and the tx state machine is re-used for a new tx.
    std::atomic<uint64_t> tx_number_;
    int64_t tx_term_;
    uint64_t commit_ts_;
    uint64_t commit_ts_bound_;
    std::atomic<TxnStatus> tx_status_;
    bool finish_;

    TransactionOperation *current_op_, *prev_op_;
    size_t idle_rep_;
    CcEntryAddr read_cce_addr_;

    // Initialization phase.
    InitTxnOperation init_txn_;
    // Execution phase.
    ReadOperation read_;
    ScanOpenOperation scan_open_;
    ScanNextOperation scan_next_;
    // Committing phase.
    UploadOperation upload_;
    SetCommitTsOperation set_ts_;
    ValidateOperation validate_;
    UpdateTxnStatus update_txn_;
    PostProcessOp post_process_;
    WriteToLog write_log_;
    AcquireTableWriteLockOp acquire_table_write_lock_op;
    WriteDDLLogOp write_ddl_log_op;
    PostProcessDDLOp post_process_ddl_op;
    ReleaseTableWriteLockOp release_table_write_lock_op;
    FindCatalogInCCShardOp find_catalog_in_ccshard_op;
    CheckCatalogInCCShardOp check_catalog_in_ccshard_op;
    FaultInjectOp fault_inject_op;
    ReleaseAllTableLocksOp release_table_locks_op;

    // size_t rset_post_cnt_;
    size_t wset_post_cnt_;
    ReadWriteSet rw_set_;

    // create table statement
    DDLType ddl_type_;
    const std::string *mysql_table_name_;
    const unsigned char *catalog_image_;
    size_t catalog_length_;

    // record term information on each ccnode for table lock
    // key is node id, value is term
    std::unordered_map<uint32_t, int64_t> table_lock_term_map_;

    // fetch catalog
    std::string *catalog_content_;

    // check catalog version
    std::string *source_version_;

    // track all the opened tables. ReleaseAllTableLocks is responsible
    // for releasing the lock for opened tables.
    std::unordered_set<std::string> opened_table_set_;

    std::unordered_map<
        size_t,
        std::pair<TableWriteSet::const_iterator, TableWriteSet::const_iterator>>
        wset_iters_;
    std::unordered_map<size_t,
                       std::pair<TableWriteSet::const_reverse_iterator,
                                 TableWriteSet::const_reverse_iterator>>
        wset_reverse_iters_;
    // std::unordered_map<size_t, const ScanTuple *> scan_tuples_;
    std::unordered_map<size_t, std::unique_ptr<CcScanner>> scans_;

    // Response whose returned result is void
    TxResult<Void> void_res_;
    TxResult<Void> *void_resp_;
    // Response whose returned result is record
    TxResult<RecordStatus> rec_res_;
    TxResult<RecordStatus> *rec_resp_;
    // Response whose returned result is bool
    TxResult<bool> bool_res_;
    TxResult<bool> *bool_resp_;
    // Scan result
    TxResult<std::tuple<const TxKey *, const TxRecord *, bool>> kvp_res_;
    TxResult<std::tuple<const TxKey *, const TxRecord *, bool>> *kvp_resp_;
    //// Scan open result
    TxResult<size_t> uint64_res_;
    TxResult<size_t> *uint64_resp_;

    std::atomic<TxRequest *> next_req_;

    CcProtocol protocol_;

    friend struct ReadOperation;
    friend struct ReadOutsideOperation;
    friend struct UploadOperation;
    friend struct SetCommitTsOperation;
    friend struct WriteToLog;
    friend struct UpdateTxnStatus;
    friend struct ValidateOperation;
    friend struct InitTxnOperation;
    friend struct PostProcessOp;
    friend struct ScanOpenOperation;
    friend struct ScanNextOperation;
    friend struct AcquireTableWriteLockOp;
    friend struct ReleaseTableWriteLockOp;
    friend struct WriteDDLLogOp;
    friend struct PostProcessDDLOp;
    friend struct FindCatalogInCCShardOp;
    friend struct CheckCatalogInCCShardOp;
    friend struct FaultInjectOp;
    friend struct ReleaseAllTableLocksOp;
    friend class TxProcessor;
};
}  // namespace txservice
