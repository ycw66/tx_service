#include "tx_execution.h"

#include <cassert>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

#include "cc_protocol.h"
#include "error_messages.h"  //CcErrorCode
#include "local_cc_shards.h"
#include "scan.h"
#include "sharder.h"
#include "statistics.h"
#include "tx_operation.h"
#include "tx_operation_result.h"
#include "tx_request.h"
#include "tx_service.h"
#include "tx_trace.h"
#include "tx_util.h"
#include "type.h"

namespace txservice
{
// whether skip write redo log to log_service.
bool txservice_skip_redo_log = false;

TransactionExecution::TransactionExecution(CcHandler *handler,
                                           TxLog *txlog,
                                           TxProcessor *tx_processor,
                                           bool bind_to_ext_proc)
    : cc_handler_(handler),
      txlog_(txlog),
      tx_processor_(tx_processor),
      txid_(UINT32_MAX),
      tx_number_((uint64_t) UINT32_MAX << 32L),
      tx_term_(-1),
      commit_ts_(UINT64_MAX),
      commit_ts_bound_(0),
      tx_status_(TxnStatus::Ongoing),
      command_id_{0},
      rw_set_(),
      cache_miss_read_cce_addr_(),
      scans_(),
      void_resp_(nullptr),
      rec_resp_(nullptr),
      vct_rec_resp_(nullptr),
      rtp_resp_(nullptr),
      bool_resp_(nullptr),
      kvp_resp_(nullptr),
      uint64_resp_(nullptr),
      bind_to_ext_proc_(bind_to_ext_proc),
      init_txn_(this),
#ifdef RANGE_PARTITION_ENABLED
      lock_range_result_(this),
      read_(this, &lock_range_result_),
#else
      read_(this),
#endif
      scan_open_(this),
      scan_next_(this),
#ifdef RANGE_PARTITION_ENABLED
      obj_cmd_(this, &lock_range_result_),
      multi_obj_cmd_(this, &lock_range_result_),
#else
      obj_cmd_(this),
      multi_obj_cmd_(this),
#endif
#ifdef RANGE_PARTITION_ENABLED
      lock_write_ranges_(&lock_range_result_),
#endif
      acquire_write_(this),
      set_ts_(this),
      validate_(this),
      update_txn_(this),
      post_process_(this),
      write_log_(this),
      sleep_op_(this),
      analyze_table_all_op_(this),
      reload_cache_op_(this),
      fault_inject_op_(this),
      clean_entry_op_(this),
      abundant_lock_op_(this),
#ifdef RANGE_PARTITION_ENABLED
      batch_read_op_(this, &lock_range_result_)
#else
      batch_read_op_(this)
#endif
{
    TX_TRACE_ASSOCIATE(this, cc_handler_);

    init_tx_req_ = std::make_unique<InitTxRequest>();
    commit_tx_req_ = std::make_unique<CommitTxRequest>();
    scan_close_req_pool_ =
        std::make_unique<CircularQueue<std::unique_ptr<ScanCloseTxRequest>>>(8);
}

void TransactionExecution::Reset()
{
    cache_miss_read_cce_addr_.SetCce(0, -1, 0, 0);
    state_stack_.clear();
    txid_.Reset();
    tx_term_ = -1;
    commit_ts_ = UINT64_MAX;
    commit_ts_bound_ = 0;
    rw_set_.Reset();
    wset_iters_.clear();
    wset_reverse_iters_.clear();
    scans_.clear();
    tx_number_.store(UINT32_MAX, std::memory_order_release);
    command_id_.store(0, std::memory_order_release);
    void_resp_ = nullptr;
    rec_resp_ = nullptr;
    vct_rec_resp_ = nullptr;
    rtp_resp_ = nullptr;
    bool_resp_ = nullptr;
    kvp_resp_ = nullptr;
    uint64_resp_ = nullptr;
    schema_op_ = nullptr;
    split_flush_op_ = nullptr;
    index_op_ = nullptr;

    if (drain_batch_.capacity() > 32)
    {
        drain_batch_.resize(32);
        drain_batch_.shrink_to_fit();
    }
    drain_batch_.clear();

    scan_alias_cnt_ = 0;

    // drain out tx_req_queue_ (if any request left)
    if (bind_to_ext_proc_)
    {
        tx_req_queue_.Reset();
        bind_to_ext_proc_ = false;
    }
    else
    {
        req_queue_lock_.Lock();
        tx_req_queue_.Reset();
        req_queue_lock_.Unlock();
    }
}

void TransactionExecution::Restart(CcHandler *handler,
                                   TxLog *txlog,
                                   TxProcessor *tx_processor,
                                   bool bind_to_ext_proc)
{
    cc_handler_ = handler;
    txlog_ = txlog;
    tx_processor_ = tx_processor;
    bind_to_ext_proc_ = bind_to_ext_proc;
    tx_status_.store(TxnStatus::Ongoing, std::memory_order_relaxed);
}

bool TransactionExecution::IsIdle()
{
    return state_stack_.empty() && TxRequestCount() == 0;
}

uint64_t TransactionExecution::TxNumber() const
{
    return tx_number_.load(std::memory_order_relaxed);
}

int64_t TransactionExecution::TxTerm() const
{
    return tx_term_;
}

uint16_t TransactionExecution::CommandId() const
{
    return command_id_.load(std::memory_order_relaxed);
}

uint64_t TransactionExecution::CommitTs() const
{
    return commit_ts_;
}

uint32_t TransactionExecution::TxCcNodeId() const
{
    return (tx_number_.load(std::memory_order_relaxed) >> 32L) >> 10;
}

TxnStatus TransactionExecution::TxStatus() const
{
    return tx_status_.load(std::memory_order_relaxed);
}

void TransactionExecution::SetRecoverTxState(uint64_t txn,
                                             int64_t tx_term,
                                             uint64_t commit_ts)
{
    tx_number_.store(txn, std::memory_order_relaxed);
    tx_term_ = tx_term;
    commit_ts_ = commit_ts;
    tx_status_.store(TxnStatus::Recovering, std::memory_order_relaxed);
}

#ifdef EXT_TX_PROC_ENABLED
void TransactionExecution::Enlist()
{
    if (bind_to_ext_proc_)
    {
        tx_processor_->EnlistTx(this);
    }
}

void TransactionExecution::ExternalForward()
{
    if (bind_to_ext_proc_)
    {
        bool success = tx_processor_->ForwardTx(this);
        if (!success)
        {
            tx_processor_->EnlistTx(this);
        }
    }
}
#endif

TxErrorCode TransactionExecution::ConvertCcError(CcErrorCode error)
{
    switch (error)
    {
    case CcErrorCode::NO_ERROR:
        return TxErrorCode::NO_ERROR;

    case CcErrorCode::FORCE_FAIL:
        return TxErrorCode::INTERNAL_ERR_TIMEOUT;

    case CcErrorCode::REQUESTED_NODE_NOT_LEADER:
        return TxErrorCode::CC_REQ_FOLLOWER;

    case CcErrorCode::VALIDATION_FAILED_FOR_VERSION_MISMATCH:
    case CcErrorCode::VALIDATION_FAILED_FOR_CONFILICTED_TXS:
        return TxErrorCode::OCC_BREAK_REPEATABLE_READ;

    case CcErrorCode::MVCC_READ_FOR_WRITE_CONFLICT:
        return TxErrorCode::SI_R4W_ERR_KEY_WAS_UPDATED;

    case CcErrorCode::DEAD_LOCK_ABORT:
        return TxErrorCode::DEAD_LOCK_ABORT;

    case CcErrorCode::ACQUIRE_KEY_LOCK_FAILED_FOR_RW_CONFLICT:
        return TxErrorCode::READ_WRITE_CONFLICT;

    case CcErrorCode::ACQUIRE_KEY_LOCK_FAILED_FOR_WW_CONFLICT:
    case CcErrorCode::ACQUIRE_GAP_LOCK_FAILED:
        return TxErrorCode::WRITE_WRITE_CONFLICT;

    case CcErrorCode::DUPLICATE_INSERT_ERR:
        return TxErrorCode::DUPLICATE_KEY;

    case CcErrorCode::NG_TERM_CHANGED:
        return TxErrorCode::NG_TERM_CHANGED;

    case CcErrorCode::REQUEST_LOST:
        return TxErrorCode::REQUEST_LOST;

    case CcErrorCode::PIN_RANGE_SLICE_FAILED:
        return TxErrorCode::CKPT_PIN_RANGE_SLICE_FAIL;

    case CcErrorCode::DATA_STORE_ERR:
        return TxErrorCode::DATA_STORE_ERROR;

    case CcErrorCode::OUT_OF_MEMORY:
        return TxErrorCode::OUT_OF_MEMORY;

    case CcErrorCode::GET_RANGE_ID_ERR:
        return TxErrorCode::GET_RANGE_ID_ERROR;

    case CcErrorCode::ACQUIRE_LEADER_TERM_ERR:
        return TxErrorCode::ACQUIRE_LEADER_TERM_FAIL;

    case CcErrorCode::UNDEFINED_ERR:
    default:
        return TxErrorCode::UNDEFINED_ERR;
    }
}

TxmStatus TransactionExecution::Forward()
{
    TransactionOperation *prev_op = nullptr;
    uint16_t cmd_id = 0;

    while (true)
    {
        if (!state_stack_.empty())
        {
            TransactionOperation *curr_op = state_stack_.back();
            if (curr_op == prev_op && cmd_id == CommandId())
            {
                break;
            }

            prev_op = curr_op;
            cmd_id = CommandId();
            curr_op->Forward(this);
        }
        else
        {
            prev_op = nullptr;
            cmd_id = 0;

            TxRequest *req = DequeueTxRequest();
            if (req == nullptr)
            {
                break;
            }

            req->Process(this);
        }
    }

    TxnStatus status = TxStatus();
    if (status == TxnStatus::Finished)
    {
        return TxmStatus::Finished;
    }
    else if (state_stack_.empty() && TxRequestCount() == 0)
    {
        return TxmStatus::Idle;
    }
    else
    {
        return TxmStatus::Busy;
    }
}

void TransactionExecution::ForwardTs(uint64_t candidate_ts)
{
    commit_ts_ = commit_ts_bound_ + 1;
    if (candidate_ts != 0)
    {
        commit_ts_ = std::max(commit_ts_, candidate_ts + 1);
    }
}

void TransactionExecution::MarkFailed()
{
    commit_ts_ = 0;
}

int TransactionExecution::Execute(TxRequest *tx_req)
{
    TxnStatus status = tx_status_.load(std::memory_order_relaxed);

    if (status == TxnStatus::Ongoing || status == TxnStatus::Recovering)
    {
        if (bind_to_ext_proc_)
        {
            tx_req_queue_.Enqueue(tx_req);
        }
        else
        {
            req_queue_lock_.Lock();
            tx_req_queue_.Enqueue(tx_req);
            req_queue_lock_.Unlock();
        }
        return 0;
    }
    else
    {
        // The tx has started committing/aborting or has committed/aborted. Does
        // not accept new requests.
        return 1;
    }
}

void TransactionExecution::InitTx(IsolationLevel iso_level,
                                  CcProtocol protocol,
                                  NodeGroupId tx_ng_id,
                                  bool start_now)
{
    init_tx_req_->Reset();
    init_tx_req_->iso_level_ = iso_level;
    init_tx_req_->protocol_ = protocol;
    init_tx_req_->tx_ng_id_ = tx_ng_id;
    init_tx_req_->txm_ = this;
    Execute(init_tx_req_.get());
    if (start_now)
    {
        init_tx_req_->Wait();
    }
}

bool TransactionExecution::CommitTx(CommitTxRequest &commit_req)
{
    if (rw_set_.WriteSetSize() == 0 && rw_set_.ReadSetSize() == 0)
    {
        commit_tx_req_->Reset();
        commit_tx_req_->to_commit_ = commit_req.to_commit_;
        Execute(commit_tx_req_.get());
#ifdef EXT_TX_PROC_ENABLED
        Enlist();
#endif
        return true;
    }
    else
    {
        Execute(&commit_req);
        commit_req.Wait();
        bool success = commit_req.Result();
        return success;
    }
}

size_t TransactionExecution::OpenTxScan(ScanOpenTxRequest &scan_open_tx_req)
{
    scan_open_tx_req.scan_alias_ = scan_alias_cnt_;
    scan_alias_cnt_++;
    Execute(&scan_open_tx_req);
    return scan_open_tx_req.scan_alias_;
}

void TransactionExecution::CloseTxScan(uint64_t alias,
                                       const TableName *table_name,
                                       std::vector<UnlockTuple> &unlock_vec)
{
    ScanCloseTxRequest *scan_close_req = NextScanCloseTxReq(alias, table_name);
    assert(scan_close_req->unlock_batch_.empty());

    if (!unlock_vec.empty())
    {
        scan_close_req->unlock_batch_.swap(unlock_vec);
    }

    Execute(scan_close_req);
#ifdef EXT_TX_PROC_ENABLED
    // Note that for scan open, we don't enlist the tx for execution, and only
    // enlist for scan close. This is because scan open is always followed by
    // scan next or scan close, which will enlist the tx and executes scan open.
    // ExternalForward();
#endif
}

TxErrorCode TransactionExecution::TxUpsert(const TableName &table_name,
                                           TxKey::Uptr key,
                                           TxRecord::Uptr rec,
                                           OperationType op)
{
    if (!rw_set_.AddWrite(table_name, std::move(key), std::move(rec), op))
    {
        return TxErrorCode::WRITE_SET_BYTES_COUNT_EXCEED_ERR;
    }
    else
    {
        return TxErrorCode::NO_ERROR;
    }
}

void TransactionExecution::TxRevert(const TableName &table_name,
                                    const TxKey &key)
{
    rw_set_.DeleteWrite(table_name, key);
}

bool TransactionExecution::IsTimeOut(int wait_secs)
{
    ++state_forward_cnt_;
    uint32_t step = bind_to_ext_proc_ ? 1 : LoopCnt;
    if (state_forward_cnt_ == step)
    {
        state_forward_cnt_ = 0;
        uint64_t now_ts = LocalCcShards::ClockTs();
        using namespace std::chrono_literals;
        // TODO remove this hard code 10 seconds
        uint64_t duration =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::seconds(wait_secs))
                .count();
        if (now_ts - state_clock_ > duration)
        {
            // The local clock is advanced in roughly 2 seconds. So, if the
            // current time is greater than the prior one by at least 4
            // seconds(local clock advances at least two times), then we can
            // confirm the tx machine has been stuck in this state for at least
            // 2 seconds.
            //
            // local clock(s):      0          2          4
            //                |----------|----------|----------|
            //                          ^            ^
            // current time:          prior         now
            //
            state_clock_ = now_ts;
            return true;
        }
    }

    return false;
}

void TransactionExecution::StartTiming()
{
    state_forward_cnt_ = 0;
    state_clock_ = LocalCcShards::ClockTs();

#ifdef EXT_TX_PROC_ENABLED
    if (bind_to_ext_proc_)
    {
        tx_processor_->EnlistWaitingTx(this);
    }
#endif
}

void TransactionExecution::PushOperation(TransactionOperation *op,
                                         int retry_num)
{
    command_id_.fetch_add(1, std::memory_order_relaxed);
    state_stack_.push_back(op);
    op->retry_num_ = retry_num;
    op->is_running_ = false;
}

void TransactionExecution::ProcessTxRequest(InitTxRequest &init_txn_req)
{
    TX_TRACE_ACTION(this, &init_txn_req);
    uint64_resp_ = &init_txn_req.tx_result_;
    iso_level_ = init_txn_req.iso_level_;
    protocol_ = init_txn_req.protocol_;
    init_txn_.tx_ng_id_ = init_txn_req.tx_ng_id_ == UINT32_MAX
                              ? Sharder::Instance().NodeId()
                              : init_txn_req.tx_ng_id_;

    init_txn_.log_group_id_ = init_txn_req.log_group_id_;
    PushOperation(&init_txn_);
    Process(init_txn_);
}

void TransactionExecution::ProcessTxRequest(ReadTxRequest &read_req)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &read_req,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });

    if (tx_term_ < 0)
    {
        read_req.SetError(TxErrorCode::TX_INIT_FAIL);
        return;
    }

    rtp_resp_ = &read_req.tx_result_;

    read_.read_type_ = ReadType::Inside;
    read_.read_tx_req_ = &read_req;
    read_.Reset();

    PushOperation(&read_);
    Process(read_);
}

void TransactionExecution::ProcessTxRequest(
    ReadOutsideTxRequest &read_outside_req)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &read_outside_req,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    rec_resp_ = &read_outside_req.tx_result_;

    read_.read_type_ = read_outside_req.is_deleted_ ? ReadType::OutsideDeleted
                                                    : ReadType::OutsideNormal;
    read_.read_outside_tx_req_ = &read_outside_req;
    read_.Reset();
    PushOperation(&read_);
    Process(read_);
}

void TransactionExecution::ProcessTxRequest(ScanOpenTxRequest &scan_open_req)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &scan_open_req,
        [&]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_))
                .append("\"table_name:\":")
                .append(scan_open_req.tab_name_->String());
        });

    if (scan_open_req.scan_alias_ == UINT64_MAX)
    {
        scan_open_req.scan_alias_ = scan_alias_cnt_;
        ++scan_alias_cnt_;
    }
    uint64_resp_ = &scan_open_req.tx_result_;

    scan_open_.tx_req_ = &scan_open_req;
    PushOperation(&scan_open_);
    Process(scan_open_);
}

void TransactionExecution::ProcessTxRequest(ScanBatchTxRequest &scan_batch_req)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &scan_batch_req,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    bool_resp_ = &scan_batch_req.tx_result_;

    scan_next_.Reset();
    scan_next_.tx_req_ = &scan_batch_req;
    PushOperation(&scan_next_);
    Process(scan_next_);
}

void TransactionExecution::ProcessTxRequest(ScanCloseTxRequest &scan_close_req)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &scan_close_req,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });

    void_resp_ = nullptr;

    ScanClose(scan_close_req.unlock_batch_,
              scan_close_req.alias_,
              *scan_close_req.table_name_);

    scan_close_req.unlock_batch_.clear();
    scan_close_req.in_use_.store(false, std::memory_order_relaxed);
    scan_close_req.tx_result_.Finish(void_);
}

void TransactionExecution::ProcessTxRequest(UpsertTxRequest &upsert_req)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &upsert_req,
        [&]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_))
                .append("\"table_name:\":")
                .append(upsert_req.tab_name_->String());
        });
    void_resp_ = &upsert_req.tx_result_;
    Upsert(*upsert_req.tab_name_,
           std::move(upsert_req.key_),
           std::move(upsert_req.rec_),
           upsert_req.operation_type_);
}

void TransactionExecution::ProcessTxRequest(CommitTxRequest &commit_req)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &commit_req,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });

    bool_resp_ = &commit_req.tx_result_;
    if (commit_req.to_commit_)
    {
        Commit();
    }
    else
    {
        // When the tx is aborted/rolled back by the user, write locks must have
        // not acquired. Clear the write set before entering post-processing.
        rw_set_.ClearWriteSet();
        Abort();
    }
}

void TransactionExecution::ProcessTxRequest(AbortTxRequest &abort_req)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &abort_req,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });

    bool_resp_ = &abort_req.tx_result_;
    // When the tx is aborted/rolled back by the user, write locks must have not
    // acquired. Clear the write set before entering post-processing.
    rw_set_.ClearWriteSet();
    Abort();
}

void TransactionExecution::ProcessTxRequest(UpsertTableTxRequest &req)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &req,
        [&]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_))
                .append("\"table_name\":")
                .append(req.table_name_->String());
        });
    upsert_resp_ = &req.tx_result_;

    LocalCcShards *local_shards = Sharder::Instance().GetLocalCcShards();
    if (req.op_type_ == OperationType::CreateTable ||
        req.op_type_ == OperationType::Update ||
        req.op_type_ == OperationType::DropTable)
    {
        std::unique_lock<std::mutex> lk(
            local_shards->table_schema_op_pool_mux_);
        if (local_shards->table_schema_op_pool_.empty())
        {
            std::unique_ptr<UpsertTableOp> table_op = nullptr;
            table_op =
                std::make_unique<UpsertTableOp>(req.table_name_->StringView(),
                                                *req.curr_image_,
                                                req.curr_schema_ts_,
                                                *req.dirty_image_,
                                                req.op_type_,
                                                this);
            schema_op_ = std::move(table_op);
        }
        else
        {
            assert(local_shards->table_schema_op_pool_.back() != nullptr);
            schema_op_ = std::move(local_shards->table_schema_op_pool_.back());
            local_shards->table_schema_op_pool_.pop_back();

            schema_op_->Reset(req.table_name_->StringView(),
                              *req.curr_image_,
                              req.curr_schema_ts_,
                              *req.dirty_image_,
                              req.op_type_,
                              this);
        }
        lk.unlock();

        PushOperation(schema_op_.get());
    }
    else if (req.op_type_ == OperationType::AddIndex ||
             req.op_type_ == OperationType::DropIndex)
    {
        std::unique_lock<std::mutex> lk(local_shards->table_index_op_pool_mux_);
        if (local_shards->table_index_op_pool_.empty())
        {
            std::unique_ptr<UpsertTableIndexOp> index_op =
                std::make_unique<UpsertTableIndexOp>(
                    req.table_name_->StringView(),
                    *req.curr_image_,
                    req.curr_schema_ts_,
                    *req.dirty_image_,
                    *req.alter_table_info_image_,
                    req.op_type_,
                    this);

            index_op_ = std::move(index_op);
        }
        else
        {
            assert(local_shards->table_index_op_pool_.back() != nullptr);
            index_op_ = std::move(local_shards->table_index_op_pool_.back());
            local_shards->table_index_op_pool_.pop_back();

            index_op_->Reset(req.table_name_->StringView(),
                             *req.curr_image_,
                             req.curr_schema_ts_,
                             *req.dirty_image_,
                             *req.alter_table_info_image_,
                             req.op_type_,
                             this);
        }
        lk.unlock();

        PushOperation(index_op_.get());
    }
    else
    {
        // Currently, no implementation for other table schema operation, such
        // as add/drop columns.
        assert(false);
    }
}

void TransactionExecution::ProcessTxRequest(ObjectCommandTxRequest &req)
{
    rec_resp_ = &req.tx_result_;
    TxCommand *command = req.Command();
    const TxKey *key = req.Key();
    obj_cmd_.Reset(req.table_name_, key, command, &req, req.auto_commit_);

    PushOperation(&obj_cmd_);
    Process(obj_cmd_);
}

void TransactionExecution::ProcessTxRequest(MultiObjectCommandTxRequest &req)
{
    vct_rec_resp_ = &req.tx_result_;
    multi_obj_cmd_.Reset(req.table_name_,
                         req.VctKey(),
                         req.VctCommand(),
                         &req,
                         req.auto_commit_);

    PushOperation(&multi_obj_cmd_);
    Process(multi_obj_cmd_);
}

void TransactionExecution::ProcessTxRequest(ReloadCacheTxRequest &req)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &fi_req,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });

    void_resp_ = &req.tx_result_;

    uint32_t hres_ref_cnt = Sharder::Instance().NodeGroupCount();
    reload_cache_op_.Reset(hres_ref_cnt);

    PushOperation(&reload_cache_op_);
    Process(reload_cache_op_);
}

void TransactionExecution::ProcessTxRequest(FaultInjectTxRequest &fi_req)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &fi_req,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    bool_resp_ = &fi_req.tx_result_;

    fault_inject_op_.Set(
        fi_req.fault_name_, fi_req.fault_paras_, fi_req.vct_node_id_);
    PushOperation(&fault_inject_op_);
    Process(fault_inject_op_);
}

void TransactionExecution::ProcessTxRequest(SplitFlushTxRequest &req)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &req,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });

    bool_resp_ = &req.tx_result_;

    LocalCcShards *local_shards = Sharder::Instance().GetLocalCcShards();
    std::unique_lock<std::mutex> lk(
        local_shards->split_flush_range_op_pool_mux_);
    if (local_shards->split_flush_range_op_pool_.empty())
    {
        split_flush_op_ = std::make_unique<SplitFlushRangeOp>(
            *req.table_name_,
            req.schema_,
            req.old_start_key_,
            req.old_end_key_,
            req.store_range_,
            req.old_range_info_,
            std::move(req.new_range_info_),
            req.previous_scan_ts_,
            std::move(req.previous_data_sync_vec_),
            std::move(req.previous_archive_vec_),
            std::move(req.previous_mv_base_vec_),
            this);
    }
    else
    {
        split_flush_op_ =
            std::move(local_shards->split_flush_range_op_pool_.back());
        local_shards->split_flush_range_op_pool_.pop_back();
        assert(split_flush_op_ != nullptr);
        split_flush_op_->Reset(*req.table_name_,
                               req.schema_,
                               req.old_start_key_,
                               req.old_end_key_,
                               req.store_range_,
                               req.old_range_info_,
                               std::move(req.new_range_info_),
                               req.previous_scan_ts_,
                               std::move(req.previous_data_sync_vec_),
                               std::move(req.previous_archive_vec_),
                               std::move(req.previous_mv_base_vec_),
                               this);
    }
    lk.unlock();

    PushOperation(split_flush_op_.get());
}

void TransactionExecution::ProcessTxRequest(
    DataMigrationTxRequest &data_migration_req)
{
    void_resp_ = &data_migration_req.tx_result_;
    LocalCcShards *local_shards = Sharder::Instance().GetLocalCcShards();

    std::lock_guard<std::mutex> lk(local_shards->data_migration_op_pool_mux_);

    if (local_shards->migration_op_pool_.empty())
    {
        migration_op_ =
            std::make_unique<DataMigrationOp>(this, data_migration_req.status_);
    }
    else
    {
        migration_op_ = std::move(local_shards->migration_op_pool_.back());
        local_shards->migration_op_pool_.pop_back();
        migration_op_->Reset(this, data_migration_req.status_);
    }

    if (data_migration_req.status_->next_bucket_idx_ != 0)
    {
        // If this is not the first worker, we can destruct the
        // tx req after migrate status is passed into migration op.
        void_resp_->Finish(void_);
    }

    PushOperation(migration_op_.get());
    Forward();
}

void TransactionExecution::ProcessTxRequest(AnalyzeTableTxRequest &analyze_req)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &analyze_req,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_))
                .append("\"table_name\":")
                .append(analyze_req.table_name_->String());
        });

    void_resp_ = &analyze_req.tx_result_;
    analyze_table_all_op_.analyze_tx_req_ = &analyze_req;

    uint32_t hres_ref_cnt = Sharder::Instance().NodeGroupCount();
    analyze_table_all_op_.Reset(hres_ref_cnt);

    PushOperation(&analyze_table_all_op_);
    Process(analyze_table_all_op_);
}

void TransactionExecution::ProcessTxRequest(ClusterScaleTxRequest &req)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &req,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });

    void_resp_ = &req.tx_result_;
    LocalCcShards *local_shards = Sharder::Instance().GetLocalCcShards();
    std::unordered_map<NodeGroupId, std::vector<NodeConfig>> new_ng_config;
    if (req.scale_type_ == ClusterScaleOpType::AddNode)
    {
        new_ng_config = Sharder::Instance().AddNodeToCluster(*req.new_nodes_);
    }
    else if (req.scale_type_ == ClusterScaleOpType::RemoveNode)
    {
        new_ng_config =
            Sharder::Instance().RemoveNodeFromCluster(*req.remove_node_count_);
    }
    else
    {
        assert(false);
    }
    std::unique_lock<std::mutex> lk(local_shards->cluster_scale_op_mux_);
    if (local_shards->cluster_scale_op_)
    {
        local_shards->cluster_scale_op_->Reset(
            req.scale_type_, std::move(new_ng_config), this);
    }
    else
    {
        local_shards->cluster_scale_op_ = std::make_unique<ClusterScaleOp>(
            req.scale_type_, std::move(new_ng_config), this);
    }
    lk.unlock();

    PushOperation(local_shards->cluster_scale_op_.get());
}

void TransactionExecution::ProcessTxRequest(
    SchemaRecoveryTxRequest &recover_req)
{
    tx_status_.store(TxnStatus::Recovering, std::memory_order_relaxed);
    upsert_resp_ = &recover_req.tx_result_;
    auto &schema_op = recover_req.schema_op_msg_;
    switch (schema_op.schema_op_case())
    {
    case ::txlog::SchemaOpMessage::kTableOp:
    {
        const ::txlog::UpsertTableMessage &table_msg = schema_op.table_op();
        OperationType operation_type =
            static_cast<OperationType>(table_msg.op_type());

        LocalCcShards *local_shards = Sharder::Instance().GetLocalCcShards();
        switch (operation_type)
        {
        case OperationType::CreateTable:
        case OperationType::DropTable:
        case OperationType::Update:
        {
            std::unique_lock<std::mutex> lk(
                local_shards->table_schema_op_pool_mux_);
            if (Sharder::Instance()
                    .GetLocalCcShards()
                    ->table_schema_op_pool_.empty())
            {
                std::unique_ptr<UpsertTableOp> table_op = nullptr;
                table_op = std::make_unique<UpsertTableOp>(
                    schema_op.table_name_str(),
                    schema_op.old_catalog_blob(),
                    schema_op.catalog_ts(),
                    schema_op.new_catalog_blob(),
                    operation_type,
                    this);
                schema_op_ = std::move(table_op);
            }
            else
            {
                assert(Sharder::Instance()
                           .GetLocalCcShards()
                           ->table_schema_op_pool_.back() != nullptr);
                schema_op_ = std::move(Sharder::Instance()
                                           .GetLocalCcShards()
                                           ->table_schema_op_pool_.back());
                Sharder::Instance()
                    .GetLocalCcShards()
                    ->table_schema_op_pool_.pop_back();

                schema_op_->Reset(schema_op.table_name_str(),
                                  schema_op.old_catalog_blob(),
                                  schema_op.catalog_ts(),
                                  schema_op.new_catalog_blob(),
                                  operation_type,
                                  this);
            }
            lk.unlock();

            if (schema_op.stage() == ::txlog::SchemaOpMessage::Stage::
                                         SchemaOpMessage_Stage_PrepareSchema)
            {
                schema_op_->prepare_log_op_.hd_result_.SetFinished();
                schema_op_->op_ = &schema_op_->prepare_log_op_;
            }
            else
            {
                assert(schema_op.stage() ==
                       ::txlog::SchemaOpMessage::Stage::
                           SchemaOpMessage_Stage_CommitSchema);
                schema_op_->commit_log_op_.hd_result_.SetFinished();
                schema_op_->op_ = &schema_op_->commit_log_op_;
            }

            PushOperation(schema_op_.get());
            break;
        }
        case OperationType::AddIndex:
        case OperationType::DropIndex:
        {
            std::unique_lock<std::mutex> lk(
                local_shards->table_index_op_pool_mux_);

            if (local_shards->table_index_op_pool_.empty())
            {
                std::unique_ptr<UpsertTableIndexOp> index_op =
                    std::make_unique<UpsertTableIndexOp>(
                        schema_op.table_name_str(),
                        schema_op.old_catalog_blob(),
                        schema_op.catalog_ts(),
                        schema_op.new_catalog_blob(),
                        schema_op.alter_table_info_blob(),
                        operation_type,
                        this);

                index_op_ = std::move(index_op);
            }
            else
            {
                assert(local_shards->table_index_op_pool_.back() != nullptr);
                index_op_ =
                    std::move(local_shards->table_index_op_pool_.back());
                local_shards->table_index_op_pool_.pop_back();

                index_op_->Reset(schema_op.table_name_str(),
                                 schema_op.old_catalog_blob(),
                                 schema_op.catalog_ts(),
                                 schema_op.new_catalog_blob(),
                                 schema_op.alter_table_info_blob(),
                                 operation_type,
                                 this);
            }
            lk.unlock();

            if (schema_op.stage() == ::txlog::SchemaOpMessage::Stage::
                                         SchemaOpMessage_Stage_PrepareSchema)
            {
                index_op_->prepare_log_op_.hd_result_.SetFinished();
                index_op_->op_ = &index_op_->prepare_log_op_;
            }
            else if (schema_op.stage() ==
                     ::txlog::SchemaOpMessage::Stage::
                         SchemaOpMessage_Stage_PrepareIndexTable)
            {
                index_op_->prepare_log_for_sk_op_.hd_result_.SetFinished();
                index_op_->op_ = &index_op_->prepare_log_for_sk_op_;
            }
            else
            {
                assert(schema_op.stage() ==
                       ::txlog::SchemaOpMessage::Stage::
                           SchemaOpMessage_Stage_CommitSchema);
                index_op_->commit_log_op_.hd_result_.SetFinished();
                index_op_->op_ = &index_op_->commit_log_op_;
            }

            PushOperation(index_op_.get());
            break;
        }
        default:
            assert(false);
            break;
        }

        break;
    }
    default:
        tx_status_.store(TxnStatus::Finished, std::memory_order_relaxed);
        break;
    }
}

void TransactionExecution::ProcessTxRequest(
    RangeSplitRecoveryTxRequest &recover_req)
{
    tx_status_.store(TxnStatus::Recovering, std::memory_order_relaxed);
    bool_resp_ = &recover_req.tx_result_;

    const TableName range_table_name =
        TableName{recover_req.ds_split_range_op_msg_.table_name(),
                  TableType::RangePartition};
    const TableName table_name =
        TableName{range_table_name.StringView(),
                  TableName::Type(range_table_name.StringView())};

    std::vector<std::pair<TxKey::Uptr, int32_t>> new_range_info;
    for (size_t i = 0; i < recover_req.new_range_keys_.size(); i++)
    {
        new_range_info.emplace_back(std::move(recover_req.new_range_keys_[i]),
                                    recover_req.new_partition_ids_[i]);
    }

    uint64_t previous_scan_ts = 0;
    std::vector<FlushRecord> previous_data_sync_vec;
    std::vector<FlushRecord> previous_archive_vec;
    std::vector<const TxKey *> previous_mv_base_vec;

    LocalCcShards *local_shards = Sharder::Instance().GetLocalCcShards();
    std::unique_ptr<SplitFlushRangeOp> split_range_op = nullptr;
    std::unique_lock<std::mutex> lk(
        local_shards->split_flush_range_op_pool_mux_);
    if (local_shards->split_flush_range_op_pool_.empty())
    {
        split_range_op = std::make_unique<SplitFlushRangeOp>(
            table_name,
            recover_req.table_schema_,
            recover_req.start_key_,
            recover_req.end_key_,
            recover_req.store_range_,
            recover_req.range_info_,
            std::move(new_range_info),
            previous_scan_ts,
            std::move(previous_data_sync_vec),
            std::move(previous_archive_vec),
            std::move(previous_mv_base_vec),
            this);
    }
    else
    {
        split_range_op =
            std::move(local_shards->split_flush_range_op_pool_.back());
        local_shards->split_flush_range_op_pool_.pop_back();
        assert(split_range_op != nullptr);
        split_range_op->Reset(table_name,
                              recover_req.table_schema_,
                              recover_req.start_key_,
                              recover_req.end_key_,
                              recover_req.store_range_,
                              recover_req.range_info_,
                              std::move(new_range_info),
                              previous_scan_ts,
                              std::move(previous_data_sync_vec),
                              std::move(previous_archive_vec),
                              std::move(previous_mv_base_vec),
                              this);
    }
    lk.unlock();
    assert(split_range_op != nullptr);

    const ::txlog::SplitRangeOpMessage::Stage stage =
        recover_req.ds_split_range_op_msg_.stage();

    if (stage == ::txlog::SplitRangeOpMessage_Stage_PrepareSplit)
    {
        split_range_op->prepare_log_op_.hd_result_.SetFinished();
        split_range_op->op_ = &split_range_op->prepare_log_op_;
    }
    else
    {
        split_range_op->commit_log_op_.hd_result_.SetFinished();
        split_range_op->op_ = &split_range_op->commit_log_op_;
    }

    LOG(INFO) << "Recovering split flush tx " << TxNumber() << " on table "
              << table_name.StringView() << ", range id "
              << recover_req.range_info_->PartitionId();
    split_flush_op_ = std::move(split_range_op);
    PushOperation(split_flush_op_.get());
}

void TransactionExecution::Process(InitTxnOperation &init_txn)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &init_txn,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });

    if (metrics::enable_transactions)
    {
        tx_duration_start_ = metrics::Clock::now();
    }

    init_txn.is_running_ = true;
    commit_ts_ = 0;
    commit_ts_bound_ = 0;

    init_txn.Reset();

    cc_handler_->NewTxn(init_txn.hd_result_,
                        iso_level_,
                        init_txn.tx_ng_id_,
                        init_txn.log_group_id_);
}

void TransactionExecution::PostProcess(InitTxnOperation &init_txn)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &init_txn,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    if (init_txn.hd_result_.IsError())
    {
        DLOG(ERROR) << "InitTxnOperation failed for cc error:"
                    << init_txn.hd_result_.ErrorMsg() << ", tx owner "
                    << init_txn.tx_ng_id_;
        state_stack_.clear();

        if (uint64_resp_ != &init_tx_req_->tx_result_)
        {
            uint64_resp_->FinishError(TxErrorCode::TX_INIT_FAIL);
            // transaction can be recycled and put into free list.
            tx_status_.store(TxnStatus::Finished, std::memory_order_release);
            Reset();
        }
        else
        {
            TxRequest *req = nullptr;
            while (TxRequestCount() > 0)
            {
                req = DequeueTxRequest();
                req->SetError(TxErrorCode::TX_INIT_FAIL);
            }
        }
        return;
    }

    const InitTxResult &init_result = init_txn.hd_result_.Value();
    txid_ = init_result.txid_;
    uint64_t tx_number = txid_.TxNumber();
    tx_number_.store(tx_number, std::memory_order_release);
    start_ts_ = init_result.start_ts_;
    commit_ts_bound_ = init_result.start_ts_ + 1;
    tx_term_ = init_result.term_;
    state_stack_.pop_back();

#ifdef ON_KEY_OBJECT
    if (uint64_resp_ != &init_tx_req_->tx_result_)
    {
        uint64_resp_->Finish(tx_number);
    }
#else
    uint64_resp_->Finish(tx_number);
#endif
}

/**
 * @brief Process the ReadOperation and generate ReadCcRequest using handler.
 *
 * @param read read is a reference to TransactionExecution::read_
 */
void TransactionExecution::Process(ReadOperation &read)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &read,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });

    read.is_running_ = true;
    if (read.read_type_ == ReadType::Inside)
    {
        const TableName &table_name = *read.read_tx_req_->tab_name_;
        const TxKey &key = *read.read_tx_req_->key_;
        TxRecord &rec = *read.read_tx_req_->rec_;
        const uint64_t ts = read.read_tx_req_->ts_;
        bool is_covering_keys = read.read_tx_req_->is_covering_keys_;

        // Reads the specified key from the local cc map to which this tx is
        // bound. This API is used for reading cc maps replicated in all shards.
        // A typical use case of ReadLocal is to read and start concurrency
        // control of a table catalog.
        if (read.read_tx_req_->read_local_)
        {
            // So far ReadLocal() is use exclusively for reading catalogs.
            // Reading catalogs needs to put read locks, regardless of the tx's
            // concurrency control protocol. So for now, a read's isolation
            // level and cc protocol is fixed.
            if (iso_level_ < IsolationLevel::RepeatableRead)
            {
                read.iso_level_ = IsolationLevel::RepeatableRead;
            }
            else
            {
                read.iso_level_ = iso_level_;
            }
            read.protocol_ = CcProtocol::Locking;

            bool finished = cc_handler_->ReadLocal(
                table_name,
                key,
                rec,
                read.read_type_,
                tx_number_.load(std::memory_order_relaxed),
                tx_term_,
                command_id_.load(std::memory_order_relaxed),
                start_ts_,
                read.hd_result_,
                read.iso_level_,
                read.protocol_,
                read.read_tx_req_->is_for_write_,
                read.read_tx_req_->is_recovering_);

            if (finished)
            {
                command_id_.fetch_add(1, std::memory_order_relaxed);
            }
        }
        else
        {
            if (!read.local_cache_miss_)
            {
                // Step 1: fast path if key is update by the same tx.
                const WriteSetEntry *write = rw_set_.FindWrite(table_name, key);
                if (write != nullptr)
                {
                    if (write->op_ == OperationType::Delete)
                    {
                        state_stack_.pop_back();
                        assert(state_stack_.empty());
                        rtp_resp_->Finish(std::pair<RecordStatus, uint64_t>(
                            RecordStatus::Deleted, 0));
                    }
                    else
                    {
                        rec.Copy(*write->rec_.get());
                        state_stack_.pop_back();
                        assert(state_stack_.empty());
                        rtp_resp_->Finish(std::pair<RecordStatus, uint64_t>(
                            RecordStatus::Normal, 0));
                    }
                    return;
                }

                // Step 2: fast path if key is the same as last read key.
                const TxRecord *cache_rec =
                    rw_set_.FindCacheRead(table_name, key);
                // read_cache_ deos not have commit ts info that unique
                // secondary index read needs. So it is incorrect to use
                // this fast path when table type is UniqueSecondary.
                if (cache_rec != nullptr &&
                    table_name.Type() != TableType::UniqueSecondary)
                {
                    rec.Copy(*cache_rec);
                    state_stack_.pop_back();
                    assert(state_stack_.empty());
                    rtp_resp_->Finish(std::pair<RecordStatus, uint64_t>(
                        RecordStatus::Normal, 0));
                    return;
                }
            }

            read.local_cache_miss_ = true;
            read.protocol_ = protocol_;
            read.iso_level_ = iso_level_;

            uint32_t key_shard_code = 0;
#ifdef RANGE_PARTITION_ENABLED
            if (!lock_range_result_.IsFinished())
            {
                read.is_running_ = false;
                // First read and lock the range the key located in through
                // lock_range_op_.
                lock_range_result_.Value().Reset();
                lock_range_result_.Reset();

                lock_range_op_.Reset(TableName(table_name.StringView(),
                                               TableType::RangePartition),
                                     &key,
                                     &range_rec_,
                                     &lock_range_result_);

                // Control flow jumps to lock_range_op_, do not execute further
                // after `Process(lock_range_op_)` returns.
                PushOperation(&lock_range_op_);
                Process(lock_range_op_);
                return;
            }
            else  // lock range finished and succeeded
            {
                // If there is an error when getting the key's range ID, the
                // error would be caught when forwarding the read operation,
                // which forces the tx state machine moves to post-processing of
                // the read operation and returns an error to the tx read
                // request.
                assert(!lock_range_result_.IsError());

                // Uses the lower 10 bits of the key's hash code to shard the
                // key across CPU cores in a cc node.
                uint32_t residual = key.Hash() & 0x3FF;
                NodeGroupId range_ng =
                    range_rec_.GetRangeOwnerNg()->BucketOwner();
                key_shard_code = range_ng << 10 | residual;
            }
#else
            key_shard_code = Sharder::Instance().ShardCode(key.Hash());
#endif

            // Step 3: do read.
            read.protocol_ = protocol_;
            read.iso_level_ = iso_level_;
            if (read.read_tx_req_->is_for_share_ &&
                iso_level_ < IsolationLevel::RepeatableRead)
            {
                read.iso_level_ = IsolationLevel::RepeatableRead;
            }
            uint64_t read_ts = 0;
            if (read.iso_level_ == IsolationLevel::Snapshot)
            {
                read_ts = start_ts_;
            }
            else if (ts != 0)
            {
                read_ts = ts;
            }

            cc_handler_->Read(table_name,
                              key,
                              key_shard_code,
                              rec,
                              read.read_type_,
                              tx_number_.load(std::memory_order_relaxed),
                              tx_term_,
                              command_id_.load(std::memory_order_relaxed),
                              read_ts,
                              read.hd_result_,
                              read.iso_level_,
                              read.protocol_,
                              read.read_tx_req_->is_for_write_,
                              is_covering_keys);

            if (!read.hd_result_.Value().is_local_)
            {
                if (metrics::enable_transactions)
                {
                    auto meter = tx_processor_->meter_.get();
                    meter->Collect(
                        tx_processor_->REMOTE_REQUEST_ON_FLY_COUNT_NAME_,
                        metrics::Value::IncDecValue::Increment,
                        "read");
                    read.op_start_ = metrics::Clock::now();
                }

                StartTiming();
            }
            else
            {
                // If the read is local and puts no lock, the operation returns
                // instantly and will not timeout.
                LockType lk_type =
                    DeduceReadLockType(table_name.Type(),
                                       read.read_tx_req_->is_for_write_,
                                       read.iso_level_,
                                       is_covering_keys);

                if (lk_type != LockType::NoLock)
                {
                    StartTiming();
                }
            }
        }
    }
    else
    {
        TxRecord &record = read.read_outside_tx_req_->rec_;
        bool is_deleted = read.read_outside_tx_req_->is_deleted_;

        rw_set_.UpdateRead(cache_miss_read_cce_addr_,
                           read.read_outside_tx_req_->commit_ts_);
        cc_handler_->ReadOutside(tx_term_,
                                 command_id_.load(std::memory_order_relaxed),
                                 record,
                                 is_deleted,
                                 read.read_outside_tx_req_->commit_ts_,
                                 cache_miss_read_cce_addr_,
                                 read.hd_result_);

        DLOG_IF(INFO, TRACE_OCC_ERR)
            << "ReadOutside ,txn: " << tx_number_ << " ,cce:" << std::hex
            << cache_miss_read_cce_addr_.CcePtr() << " ,ts: " << std::dec
            << read.read_outside_tx_req_->commit_ts_
            << " ,is_deleted: " << static_cast<int>(is_deleted);

        return;
    }
}

void TransactionExecution::PostProcess(ReadOperation &read)
{
    // collect metrics: remote read duration
    if (metrics::enable_transactions && !read.hd_result_.Value().is_local_)
    {
        metrics::Meter *meter;
        meter = tx_processor_->meter_.get();
        meter->CollectDuration(tx_processor_->REMOTE_REQUEST_DURATION_NAME_,
                               read.op_start_,
                               "read");
        meter->Collect(tx_processor_->REMOTE_REQUEST_ON_FLY_COUNT_NAME_,
                       metrics::Value::IncDecValue::Decrement,
                       "read");
    }

    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &read,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    state_stack_.pop_back();
    assert(state_stack_.empty());

    if (read_.hd_result_.IsError())
    {
        DLOG(ERROR) << "ReadOperation failed for cc error:"
                    << read_.hd_result_.ErrorMsg() << "; txn: " << TxNumber();
        rtp_resp_->FinishError(ConvertCcError(read_.hd_result_.ErrorCode()));
    }
    else
    {
        const ReadKeyResult &read_res = read_.hd_result_.Value();
        const ReadTxRequest *read_tx_req = read.read_tx_req_;

        if (read.read_type_ == ReadType::OutsideDeleted ||
            read.read_type_ == ReadType::OutsideNormal)
        {
            rec_resp_->Finish(read_res.rec_status_);
            return;
        }

        // optimization for case that we read the same key continuously
        // especially speed up remote read. e.g. Read A, Write B, Read A.
        if (!read.read_tx_req_->read_local_ &&
            read_res.rec_status_ == RecordStatus::Normal)
        {
            // rw_set_.AddCacheRead(
            //     *read_req->tab_name_, *read_req->key_, *read_req->rec_);
        }

        if (read_.read_type_ == ReadType::Inside)
        {
            const TableName *table_name = read_tx_req->tab_name_;
            LockType lock_type = read_res.lock_type_;
            if (lock_type != LockType::NoLock)
            {
                DLOG_IF(INFO, TRACE_OCC_ERR)
                    << "Before AddRead, txn: " << tx_number_
                    << " ,cce:" << std::hex << read_res.cce_addr_.CcePtr()
                    << " ,ts: " << std::dec << read_res.ts_ << " ,rec_status: "
                    << static_cast<int>(read_res.rec_status_)
                    << " ,lock: " << static_cast<int>(read_res.lock_type_)
                    << " ,table: " << table_name->String();
                bool add_res;
                if (read_res.rec_status_ == RecordStatus::Unknown)
                {
                    // Only used to release lock.
                    add_res =
                        rw_set_.AddRead(read_res.cce_addr_, 0, table_name);
                }
                else
                {
                    add_res = rw_set_.AddRead(
                        read_res.cce_addr_, read_res.ts_, table_name);
                }
                if (!add_res)
                {
                    DLOG_IF(INFO, TRACE_OCC_ERR)
                        << "AddRead, occ_err: " << tx_number_
                        << " ,cce:" << std::hex << read_res.cce_addr_.CcePtr()
                        << " ,ts: " << read_res.ts_ << " ,rec_status: "
                        << static_cast<int>(read_res.rec_status_)
                        << " ,lock: " << static_cast<int>(read_res.lock_type_)
                        << " ,table: " << table_name->String();
                    rtp_resp_->FinishError(
                        TxErrorCode::OCC_BREAK_REPEATABLE_READ);

                    return;
                }
            }

            // Read lock early release logic:
            // If it is skread and succeeds and need to trace back pk entry,
            // add into drain_batch_
            if (read_tx_req->tab_name_->Type() == TableType::UniqueSecondary &&
                lock_type == LockType::ReadLock &&
                !read.read_tx_req_->is_covering_keys_)
            {
                assert(TxStatus() != TxnStatus::Recovering);

                uint16_t read_cnt = rw_set_.RemoveReadEntry(
                    *read_tx_req->tab_name_, read_res.cce_addr_);
                if (read_cnt == 0)
                {
                    drain_batch_.emplace_back(read_res.cce_addr_, read_res.ts_);
                }
            }
            else if (read_tx_req->tab_name_->Type() == TableType::Primary &&
                     !drain_batch_.empty())
            {
                assert(TxStatus() != TxnStatus::Recovering);
                assert(drain_batch_.size() == 1);

                abundant_lock_op_.Reset();
                PushOperation(&abundant_lock_op_);
                Process(abundant_lock_op_);
            }
        }

        if (read_.read_type_ == ReadType::Inside &&
            (read_res.rec_status_ == RecordStatus::Unknown ||
             read_res.rec_status_ == RecordStatus::VersionUnknown))
        {
            // If the read does not retrieve the value, the tx user is likely to
            // read the data store and brings in the value for caching. Cache
            // the cc entry's address in the tx's local variable.
            cache_miss_read_cce_addr_ = read_res.cce_addr_;
        }
        else
        {
            cache_miss_read_cce_addr_.SetCce(0, -1, 0, 0);
        }

        rtp_resp_->Finish(std::pair<RecordStatus, uint64_t>(
            read_res.rec_status_, read_res.ts_));
    }
}

void TransactionExecution::Process(ReadLocalOperation &lock_local)
{
    lock_local.hd_result_->Reset();
    bool finished =
        cc_handler_->ReadLocal(lock_local.table_name_,
                               *lock_local.key_,
                               *lock_local.rec_,
                               ReadType::Inside,
                               tx_number_.load(std::memory_order_relaxed),
                               tx_term_,
                               CommandId(),
                               start_ts_,
                               *lock_local.hd_result_,
                               IsolationLevel::RepeatableRead,
                               CcProtocol::Locking,
                               false,
                               false,
                               lock_local.execute_immediately_);
    if (finished)
    {
        command_id_.fetch_add(1, std::memory_order_relaxed);
    }
}

void TransactionExecution::PostProcess(ReadLocalOperation &lock_local)
{
    if (lock_local.hd_result_->IsError())
    {
        DLOG(ERROR) << "ReadLocalOperation failed for cc error:"
                    << lock_local.hd_result_->ErrorMsg() << ", txn "
                    << TxNumber();
    }
    else if (lock_local.hd_result_->Value().rec_status_ == RecordStatus::Normal)
    {
        // The read lock on the range is added, put the range cce into read
        // set for later release read lock.
        // The range cannot be changed before this tx finishes
        // post-processing, so the read lock on the range is kept until
        // then.
        const ReadKeyResult &read_res = lock_local.hd_result_->Value();
        rw_set_.AddRead(
            read_res.cce_addr_, read_res.ts_, &lock_local.table_name_);
    }
    state_stack_.pop_back();
}

void TransactionExecution::Process(ScanOpenOperation &scan_open)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &scan_open,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    const TableName &table_name = *scan_open.tx_req_->tab_name_;
    ScanIndexType index_type = scan_open.tx_req_->indx_type_;
    const TxKey &start_key = *scan_open.tx_req_->StartKey();
    bool inclusive = scan_open.tx_req_->start_inclusive_;
    ScanDirection direction = scan_open.tx_req_->direct_;
    bool is_ckpt_delta = scan_open.tx_req_->is_ckpt_delta_;
    bool is_for_write = scan_open.tx_req_->is_for_write_;
    bool is_for_share = scan_open.tx_req_->is_for_share_;
    bool is_covering_keys = scan_open.tx_req_->is_covering_keys_;

    scan_open.Reset();
    scan_open.is_running_ = true;
    scan_open.Set(&table_name,
                  index_type,
                  &start_key,
                  inclusive,
                  direction,
                  is_ckpt_delta);

    scan_open.hd_result_.Value().scan_alias_ = scan_open.tx_req_->scan_alias_;

    if (scan_open.tx_req_->read_local_)
    {
        cc_handler_->ScanOpenLocal(table_name,
                                   index_type,
                                   start_key,
                                   inclusive,
                                   tx_number_.load(std::memory_order_relaxed),
                                   tx_term_,
                                   command_id_.load(std::memory_order_relaxed),
                                   commit_ts_bound_,
                                   scan_open.hd_result_,
                                   direction,
                                   IsolationLevel::RepeatableRead,
                                   CcProtocol::Locking,
                                   is_for_write,
                                   is_ckpt_delta);
    }
    else
    {
        IsolationLevel iso_lvl = iso_level_;
        if (is_for_share && iso_level_ < IsolationLevel::RepeatableRead)
        {
            iso_lvl = IsolationLevel::RepeatableRead;
        }

        cc_handler_->ScanOpen(table_name,
                              index_type,
                              start_key,
                              inclusive,
                              tx_number_.load(std::memory_order_relaxed),
                              tx_term_,
                              command_id_.load(std::memory_order_relaxed),
                              start_ts_,
                              scan_open.hd_result_,
                              direction,
                              iso_lvl,
                              protocol_,
                              is_for_write,
                              is_ckpt_delta,
                              is_covering_keys);
    }

#ifndef RANGE_PARTITION_ENABLED
    StartTiming();
#endif
}

void TransactionExecution::PostProcess(ScanOpenOperation &scan_open)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &scan_open,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    state_stack_.pop_back();
    assert(state_stack_.empty());

    ScanOpenResult &open_result = scan_open.hd_result_.Value();

    if (scan_open.hd_result_.IsError())
    {
        DLOG(ERROR) << "ScanOpenOperation failed for cc error:"
                    << scan_open_.hd_result_.ErrorMsg();

        uint64_resp_->FinishError(
            ConvertCcError(scan_open.hd_result_.ErrorCode()));

        if (open_result.scanner_ != nullptr)
        {
            DrainScanner(open_result.scanner_.get(), *scan_open.table_name_);

            abundant_lock_op_.Reset();
            PushOperation(&abundant_lock_op_);
            Process(abundant_lock_op_);
        }

        return;
    }

    auto table_iter = rw_set_.WriteSet().find(*scan_open.table_name_);
    if (table_iter != rw_set_.WriteSet().end())
    {
        if (scan_open.direction_ == ScanDirection::Forward)
        {
            auto wset_it = rw_set_.InitIter(
                table_iter->second, scan_open.start_key_, scan_open.inclusive_);
            if (wset_it.first != wset_it.second)
            {
                wset_iters_.emplace(open_result.scan_alias_, wset_it);
            }
        }
        else
        {
            auto wset_rit = rw_set_.InitReverseIter(
                table_iter->second, scan_open.start_key_, scan_open.inclusive_);
            if (wset_rit.first != wset_rit.second)
            {
                wset_reverse_iters_.emplace(open_result.scan_alias_, wset_rit);
            }
        }
    }

    assert(scans_.find(open_result.scan_alias_) == scans_.end());

#ifdef RANGE_PARTITION_ENABLED
    // Constructs a pseudo slice prior to the first slice of the scan. And sets
    // the status of the scanner "Blocked".
    open_result.scanner_->SetStatus(ScannerStatus::Blocked);
    if (scan_open.tx_req_->StartKey()->Type() == KeyType::Normal)
    {
        scans_.try_emplace(open_result.scan_alias_,
                           std::move(open_result.scanner_),
                           scan_open.tx_req_->EndKey(),
                           scan_open.tx_req_->end_inclusive_,
                           UINT32_MAX,
                           UINT32_MAX,
                           scan_open.tx_req_->StartKey()->Clone(),
                           !scan_open.tx_req_->start_inclusive_,
                           scan_open.direction_ == ScanDirection::Forward
                               ? SlicePosition::LastSliceInRange
                               : SlicePosition::FirstSliceInRange);
    }
    else
    {
        scans_.try_emplace(open_result.scan_alias_,
                           std::move(open_result.scanner_),
                           scan_open.tx_req_->EndKey(),
                           scan_open.tx_req_->end_inclusive_,
                           UINT32_MAX,
                           UINT32_MAX,
                           scan_open.tx_req_->StartKey(),
                           !scan_open.tx_req_->start_inclusive_,
                           scan_open.direction_ == ScanDirection::Forward
                               ? SlicePosition::LastSliceInRange
                               : SlicePosition::FirstSliceInRange);
    }
#else
    scans_.try_emplace(open_result.scan_alias_,
                       std::move(open_result.scanner_),
                       scan_open.tx_req_->EndKey(),
                       scan_open.tx_req_->end_inclusive_);
#endif

    if (uint64_resp_ != nullptr)
    {
        uint64_resp_->Finish(open_result.scan_alias_);
    }
}

void TransactionExecution::Process(ScanNextOperation &scan_next)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &scan_next,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    uint64_t alias = scan_next.tx_req_->alias_;

    if (scan_next.scan_state_ == nullptr)
    {
        auto scan_it = scans_.find(alias);
        if (scan_it == scans_.end())
        {
            bool_resp_->FinishError(TxErrorCode::NG_TERM_CHANGED);
            state_stack_.pop_back();
            return;
        }

        scan_next.UpdateScanState(&scan_it->second);
    }
    scan_next.alias_ = alias;

    CcScanner &scanner = *scan_next.scan_state_->scanner_;
    scan_next.is_running_ = true;

    bool to_scan_next = scanner.Current() == nullptr &&
                        scanner.Status() == ScannerStatus::Blocked;

    bool is_local = true;
    if (to_scan_next && scanner.Type() == CcmScannerType::HashPartition)
    {
        if (scanner.read_local_)
        {
            cc_handler_->ScanNextBatchLocal(
                tx_number_.load(std::memory_order_relaxed),
                tx_term_,
                command_id_.load(std::memory_order_relaxed),
                start_ts_,
                scanner,
                scan_next.hd_result_);
        }
        else
        {
            cc_handler_->ScanNextBatch(
                tx_number_.load(std::memory_order_relaxed),
                tx_term_,
                command_id_.load(std::memory_order_relaxed),
                start_ts_,
                scanner,
                scan_next.hd_result_);
        }

        is_local = scan_next.hd_result_.Value().is_local_;
    }
#ifdef RANGE_PARTITION_ENABLED
    else if (to_scan_next && scanner.Type() == CcmScannerType::RangePartition)
    {
        ScanState &scan_state = *scan_next.scan_state_;

        if ((scanner.Direction() == ScanDirection::Forward &&
             scan_state.slice_position_ == SlicePosition::LastSlice) ||
            (scanner.Direction() == ScanDirection::Backward &&
             scan_state.slice_position_ == SlicePosition::FirstSlice))
        {
            // The current slice is the last (or first). There is no more slice
            // to scan.
            scanner.SetStatus(ScannerStatus::Closed);
            scan_next.slice_hd_result_.SetFinished();
            scan_next.unlock_range_result_.SetFinished();
            return;
        }
        else if (scan_state.slice_position_ == SlicePosition::Middle)
        {
            // When the current slice is in the middle, there is no need to lock
            // the range, as the range has been locked when scanning the range's
            // first slice. Sets the lock result to be finished, so that in case
            // the to-be-scanned slice is the last (first) of the range, the
            // range lock is released when the scan request returns.
            scan_next.lock_range_result_.SetFinished();

            cc_handler_->ScanNextBatch(
                scan_next.tx_req_->table_name_,
                scan_state.range_id_,
                scan_state.range_ng_,
                scan_next.RangeNgTerm(),
                scan_state.SliceLastKey(),
                !scan_state.inclusive_,
                scan_state.scan_end_key_,
                scan_state.scan_end_inclusive_,
                scan_next.tx_req_->prefetch_slice_cnt_,
                start_ts_,
                tx_number_.load(std::memory_order_relaxed),
                tx_term_,
                CommandId(),
                scan_next.slice_hd_result_,
                iso_level_,
                protocol_);

            is_local = scan_next.slice_hd_result_.Value().is_local_;
            if (metrics::enable_transactions && !is_local)
            {
                auto meter = tx_processor_->meter_.get();
                meter->Collect(tx_processor_->REMOTE_REQUEST_ON_FLY_COUNT_NAME_,
                               metrics::Value::IncDecValue::Increment,
                               "scan_next");
                scan_next.op_start_ = metrics::Clock::now();
            }
        }
        else if ((scanner.Direction() == ScanDirection::Forward &&
                  scan_state.slice_position_ ==
                      SlicePosition::LastSliceInRange) ||
                 (scanner.Direction() == ScanDirection::Backward &&
                  scan_state.slice_position_ ==
                      SlicePosition::FirstSliceInRange))
        {
            // The last scan reaches the end of the current range. The next scan
            // moves on to the next range, which starts from the last scan's end
            // key. Before reading the first slice of the next range, the scan
            // first locks the next range.

            if (scan_next.lock_range_result_.IsFinished())
            {
                // If there is an error when getting the key's range ID, the
                // error would be caught when forwarding the scan operation,
                // which forces the tx state machine moves to
                // post-processing of the scan operation and returns an
                // error to the tx scan request.
                assert(!scan_next.lock_range_result_.IsError());

                // The term of the cc node group hosting the to-be-scanned range
                // is unknown. Sets the term to -1, indicating it is not matched
                // against that of the cc node when the first slice of the
                // range. The first scan of the range will return the cc node's
                // term and subsequence scans of the remaining slices in the
                // range will match the term.
                cc_handler_->ScanNextBatch(
                    scan_next.tx_req_->table_name_,
                    scan_state.range_id_,
                    scan_state.range_ng_,
                    -1,
                    scan_state.SliceLastKey(),
                    !scan_state.inclusive_,
                    scan_state.scan_end_key_,
                    scan_state.scan_end_inclusive_,
                    scan_next.tx_req_->prefetch_slice_cnt_,
                    start_ts_,
                    tx_number_.load(std::memory_order_relaxed),
                    tx_term_,
                    CommandId(),
                    scan_next.slice_hd_result_,
                    iso_level_,
                    protocol_);

                is_local = scan_next.slice_hd_result_.Value().is_local_;
                if (metrics::enable_transactions && !is_local)
                {
                    auto meter = tx_processor_->meter_.get();
                    meter->Collect(
                        tx_processor_->REMOTE_REQUEST_ON_FLY_COUNT_NAME_,
                        metrics::Value::IncDecValue::Increment,
                        "scan_next");
                    scan_next.op_start_ = metrics::Clock::now();
                }
            }
            else
            {
                scan_next.is_running_ = false;

                ReadType lock_range_read_type =
                    scanner.Direction() == ScanDirection::Forward
                        ? ReadType::RangeLeftInclusive
                        : ReadType::RangeRightExclusive;
                scan_next.range_table_name_ =
                    TableName(scan_next.tx_req_->table_name_.StringView(),
                              TableType::RangePartition);

                bool finished = cc_handler_->ReadLocal(
                    scan_next.range_table_name_,
                    *scan_state.SliceLastKey(),
                    scan_next.range_rec_,
                    lock_range_read_type,
                    tx_number_.load(std::memory_order_relaxed),
                    tx_term_,
                    CommandId(),
                    start_ts_,
                    scan_next.lock_range_result_,
                    IsolationLevel::RepeatableRead,
                    CcProtocol::Locking);

                if (finished)
                {
                    command_id_.fetch_add(1, std::memory_order_relaxed);
                }
                return;
            }
        }
    }
#endif
    else if (scanner.Type() == CcmScannerType::HashPartition)
    {
        scan_next.hd_result_.SetFinished();
        return;
    }
#ifdef RANGE_PARTITION_ENABLED
    else if (scanner.Type() == CcmScannerType::RangePartition)
    {
        scan_next.unlock_range_result_.SetFinished();
        scan_next.slice_hd_result_.SetFinished();
        return;
    }
#endif

    if (!is_local ||
        DeduceReadLockType(scanner.IndexType() == ScanIndexType::Primary
                               ? TableType::Primary
                               : TableType::Secondary,
                           scanner.IsReadForWrite(),
                           scanner.Isolation(),
                           scanner.IsCoveringKey()) != LockType::NoLock)
    {
        StartTiming();
    }
}

void TransactionExecution::PostProcess(ScanNextOperation &scan_next)
{
    // collect metrics: remote scan next duration
#ifdef RANGE_PARTITION_ENABLED
    if (metrics::enable_transactions &&
        !scan_next.slice_hd_result_.Value().is_local_)
    {
        metrics::Meter *meter;
        meter = tx_processor_->meter_.get();
        meter->CollectDuration(tx_processor_->REMOTE_REQUEST_DURATION_NAME_,
                               scan_next.op_start_,
                               "scan_next");
        meter = tx_processor_->meter_.get();
        meter->Collect(tx_processor_->REMOTE_REQUEST_ON_FLY_COUNT_NAME_,
                       metrics::Value::IncDecValue::Decrement,
                       "scan_next");
    }
#endif

    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &scan_next,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    state_stack_.pop_back();
    assert(state_stack_.empty());

    const TableName &table_name = scan_next.tx_req_->table_name_;
    CcScanner &scanner = *scan_next.scan_state_->scanner_;

    if (scanner.Type() == CcmScannerType::HashPartition &&
        scan_next.hd_result_.IsError())
    {
        DrainScanner(&scanner, table_name);

        DLOG(ERROR) << "ScanNextOperation failed for cc error: "
                    << scan_next.hd_result_.ErrorMsg();
        bool_resp_->FinishError(
            ConvertCcError(scan_next.hd_result_.ErrorCode()));
        return;
    }
#ifdef RANGE_PARTITION_ENABLED
    else if (scanner.Type() == CcmScannerType::RangePartition &&
             scan_next.slice_hd_result_.IsError())
    {
        DrainScanner(&scanner, table_name);

        DLOG(ERROR) << "ScanNextOperation failed for cc error: "
                    << scan_next.slice_hd_result_.ErrorMsg() << ", table: "
                    << scan_next.tx_req_->table_name_.StringView();
        bool_resp_->FinishError(
            ConvertCcError(scan_next.slice_hd_result_.ErrorCode()));
        return;
    }
#endif

    enum struct AdvanceType
    {
        Ccm,
        WriteSet,
        Both
    };
    AdvanceType advance_type;

    const ScanTuple *cc_scan_tuple = nullptr;
    std::vector<ScanBatchTuple> &scan_batch = *scan_next.tx_req_->batch_;
    assert(scan_batch.empty());

    if (scanner.Direction() == ScanDirection::Forward)
    {
        auto it = wset_iters_.find(scan_next.alias_);

        while (scanner.Status() == ScannerStatus::Open)
        {
            cc_scan_tuple = scanner.Current();
            if (cc_scan_tuple == nullptr)
            {
                scanner.MoveNext();
                assert(scanner.Status() != ScannerStatus::Open);
                break;
            }

            if (it == wset_iters_.end() ||
                it->second.first == it->second.second ||
                cc_scan_tuple->key_ts_ == 0)
            {
                advance_type = AdvanceType::Ccm;
            }
            else
            {
                auto &wset_it = it->second.first;
                const WriteSetEntry &local_write = wset_it->second;
                if (*local_write.key_ < *cc_scan_tuple->Key())
                {
                    advance_type = AdvanceType::WriteSet;
                }
                else if (*cc_scan_tuple->Key() < *local_write.key_.get())
                {
                    advance_type = AdvanceType::Ccm;
                }
                else
                {
                    advance_type = AdvanceType::Both;
                }
            }

            if (advance_type == AdvanceType::WriteSet)
            {
                auto &wset_it = it->second.first;
                const WriteSetEntry &local_write = wset_it->second;
                if (local_write.op_ == OperationType::Delete)
                {
                    scan_batch.emplace_back(local_write.key_.get(),
                                            nullptr,
                                            RecordStatus::Deleted,
                                            1);
                }
                else
                {
                    scan_batch.emplace_back(local_write.key_.get(),
                                            local_write.rec_.get(),
                                            RecordStatus::Normal,
                                            1);
                }

                ++wset_it;
            }
            else
            {
                // Deduces the lock type. If a lock is put on the scanned
                // entry, adds the entry into the read set, so that the tx
                // releases the lock in the commit phase.
                LockType scan_tuple_lock_type =
                    scanner.DeduceScanTupleLockType(cc_scan_tuple->rec_status_);
                // "key_ts_ == 0", means the lock is added on gap. Now, gap
                // lock is not used when do scan operation.
                if (scan_tuple_lock_type != LockType::NoLock &&
                    cc_scan_tuple->key_ts_ != 0)
                {
                    TX_TRACE_ACTION_WITH_CONTEXT(
                        this,
                        "PostProcess.ScanOperation.AddReadSet.cce_ptr",
                        &scan_next,
                        (
                            [this, cc_scan_tuple]() -> std::string
                            {
                                return std::string("\"tx_number\":")
                                    .append(std::to_string(this->TxNumber()))
                                    .append(",\"tx_term\":")
                                    .append(std::to_string(this->tx_term_))
                                    .append(",\"cce_ptr\":")
                                    .append(std::to_string(
                                        cc_scan_tuple->cce_addr_.CcePtr()));
                            }));

                    // When the record status is unknown, the read ts is set
                    // to 0 to release the lock without validation.
                    uint64_t read_ts =
                        cc_scan_tuple->rec_status_ != RecordStatus::Unknown
                            ? cc_scan_tuple->key_ts_
                            : 0;
                    bool add_res = rw_set_.AddRead(
                        cc_scan_tuple->cce_addr_, read_ts, &table_name);
                    if (!add_res)
                    {
                        bool_resp_->FinishError(
                            TxErrorCode::OCC_BREAK_REPEATABLE_READ);
                        return;
                    }
                }

                if (advance_type == AdvanceType::Ccm)
                {
                    if (cc_scan_tuple->key_ts_ > 0)
                    {
#ifndef RANGE_PARTITION_ENABLED
                        if (cc_scan_tuple->rec_status_ == RecordStatus::Normal)
                        {
                            scan_batch.emplace_back(
                                cc_scan_tuple->Key(),
                                const_cast<TxRecord *>(cc_scan_tuple->Record()),
                                RecordStatus::Normal,
                                cc_scan_tuple->key_ts_,
                                cc_scan_tuple->cce_addr_);
                        }
                        else if (cc_scan_tuple->rec_status_ ==
                                 RecordStatus::Deleted)
                        {
                            // When the record status is not Normal, the record
                            // is set to null in the returned result.
                            scan_batch.emplace_back(cc_scan_tuple->Key(),
                                                    nullptr,
                                                    cc_scan_tuple->rec_status_,
                                                    cc_scan_tuple->key_ts_,
                                                    cc_scan_tuple->cce_addr_);
                        }
#else
                        // When the record status is not Normal, the record
                        // is set to null in the scan result.
                        const TxRecord *rec =
                            cc_scan_tuple->rec_status_ == RecordStatus::Normal
                                ? cc_scan_tuple->Record()
                                : nullptr;

                        scan_batch.emplace_back(cc_scan_tuple->Key(),
                                                const_cast<TxRecord *>(rec),
                                                cc_scan_tuple->rec_status_,
                                                cc_scan_tuple->key_ts_,
                                                cc_scan_tuple->cce_addr_);
#endif
                    }

                    scanner.MoveNext();
                }
                else
                {
                    auto &wset_it = it->second.first;
                    const WriteSetEntry &local_write = wset_it->second;
                    // Returns the key-value pair in the local write set.
                    if (local_write.op_ == OperationType::Delete)
                    {
                        scan_batch.emplace_back(local_write.key_.get(),
                                                nullptr,
                                                RecordStatus::Deleted,
                                                cc_scan_tuple->key_ts_);
                    }
                    else
                    {
                        scan_batch.emplace_back(local_write.key_.get(),
                                                local_write.rec_.get(),
                                                RecordStatus::Normal,
                                                cc_scan_tuple->key_ts_);
                    }

                    scanner.MoveNext();
                    ++wset_it;
                }
            }
        }

        if (it != wset_iters_.end())
        {
            auto &wset_it = it->second.first;
            auto &wset_end = it->second.second;
            const TxKey *batch_end_key = nullptr;
            if (scanner.Status() == ScannerStatus::Blocked)
            {
#ifdef RANGE_PARTITION_ENABLED
                batch_end_key = scan_next.scan_state_->SliceLastKey();
#else
                batch_end_key = scan_batch.empty()
                                    ? nullptr
                                    : scan_batch[scan_batch.size() - 1].key_;
#endif
            }

            while (wset_it != wset_end && (batch_end_key == nullptr ||
                                           *wset_it->first < *batch_end_key))
            {
                const WriteSetEntry &local_write = wset_it->second;
                // Returns the key-value pair in the local write set.
                if (local_write.op_ != OperationType::Delete)
                {
                    scan_batch.emplace_back(local_write.key_.get(),
                                            local_write.rec_.get(),
                                            RecordStatus::Normal,
                                            1);
                }
                else
                {
                    scan_batch.emplace_back(local_write.key_.get(),
                                            nullptr,
                                            RecordStatus::Deleted,
                                            1);
                }

                ++wset_it;
            }
        }
    }
    // backward scan
    else
    {
        auto rit = wset_reverse_iters_.find(scan_next.alias_);

        while (scanner.Status() == ScannerStatus::Open)
        {
            cc_scan_tuple = scanner.Current();
            if (cc_scan_tuple == nullptr)
            {
                scanner.MoveNext();
                assert(scanner.Status() != ScannerStatus::Open);
                break;
            }

            if (rit == wset_reverse_iters_.end() ||
                rit->second.first == rit->second.second ||
                cc_scan_tuple->key_ts_ == 0)
            {
                advance_type = AdvanceType::Ccm;
            }
            else
            {
                auto &wset_it = rit->second.first;
                const WriteSetEntry &local_write = wset_it->second;
                if (*cc_scan_tuple->Key() < *local_write.key_)
                {
                    advance_type = AdvanceType::WriteSet;
                }
                else if (*local_write.key_.get() < *cc_scan_tuple->Key())
                {
                    advance_type = AdvanceType::Ccm;
                }
                else
                {
                    advance_type = AdvanceType::Both;
                }
            }

            if (advance_type == AdvanceType::WriteSet)
            {
                auto &wset_it = rit->second.first;
                const WriteSetEntry &local_write = wset_it->second;
                if (local_write.op_ != OperationType::Delete)
                {
                    scan_batch.emplace_back(local_write.key_.get(),
                                            local_write.rec_.get(),
                                            RecordStatus::Normal,
                                            1);
                }
                else
                {
                    scan_batch.emplace_back(local_write.key_.get(),
                                            nullptr,
                                            RecordStatus::Deleted,
                                            1);
                }

                ++wset_it;
            }
            else
            {
                // Deduces the lock type. If a lock is put on the scanned
                // entry, adds the entry into the read set, so that the tx
                // releases the lock in the commit phase.
                LockType scan_tuple_lock_type =
                    scanner.DeduceScanTupleLockType(cc_scan_tuple->rec_status_);
                // "key_ts_ == 0", means the lock is added on gap. Now, gap
                // lock is not used when do scan operation.
                if (scan_tuple_lock_type != LockType::NoLock &&
                    cc_scan_tuple->key_ts_ != 0)
                {
                    TX_TRACE_ACTION_WITH_CONTEXT(
                        this,
                        "PostProcess.ScanOperation.AddReadSet.cce_ptr",
                        &scan_next,
                        (
                            [this, cc_scan_tuple]() -> std::string
                            {
                                return std::string("\"tx_number\":")
                                    .append(std::to_string(this->TxNumber()))
                                    .append(",\"tx_term\":")
                                    .append(std::to_string(this->tx_term_))
                                    .append(",\"cce_ptr\":")
                                    .append(std::to_string(
                                        cc_scan_tuple->cce_addr_.CcePtr()));
                            }));

                    uint64_t read_ts =
                        cc_scan_tuple->rec_status_ != RecordStatus::Unknown
                            ? cc_scan_tuple->key_ts_
                            : 0;
                    bool add_res = rw_set_.AddRead(
                        cc_scan_tuple->cce_addr_, read_ts, &table_name);
                    if (!add_res)
                    {
                        bool_resp_->FinishError(
                            TxErrorCode::OCC_BREAK_REPEATABLE_READ);
                        return;
                    }
                }

                if (advance_type == AdvanceType::Ccm)
                {
                    if (cc_scan_tuple->key_ts_ > 0)
                    {
#ifndef RANGE_PARTITION_ENABLED
                        if (cc_scan_tuple->rec_status_ == RecordStatus::Normal)
                        {
                            scan_batch.emplace_back(
                                cc_scan_tuple->Key(),
                                const_cast<TxRecord *>(cc_scan_tuple->Record()),
                                RecordStatus::Normal,
                                cc_scan_tuple->key_ts_,
                                cc_scan_tuple->cce_addr_);
                        }
                        else if (cc_scan_tuple->rec_status_ ==
                                 RecordStatus::Deleted)
                        {
                            // When the record status is not Normal, the record
                            // is set to null in the returned result.
                            scan_batch.emplace_back(cc_scan_tuple->Key(),
                                                    nullptr,
                                                    cc_scan_tuple->rec_status_,
                                                    cc_scan_tuple->key_ts_,
                                                    cc_scan_tuple->cce_addr_);
                        }
#else
                        // When the record status is not Normal, the record
                        // is set to null in the scan result.
                        const TxRecord *rec =
                            cc_scan_tuple->rec_status_ == RecordStatus::Normal
                                ? cc_scan_tuple->Record()
                                : nullptr;

                        scan_batch.emplace_back(cc_scan_tuple->Key(),
                                                const_cast<TxRecord *>(rec),
                                                cc_scan_tuple->rec_status_,
                                                cc_scan_tuple->key_ts_,
                                                cc_scan_tuple->cce_addr_);
#endif
                    }

                    scanner.MoveNext();
                }
                else
                {
                    auto &wset_it = rit->second.first;
                    const WriteSetEntry &local_write = wset_it->second;
                    // Returns the key-value pair in the local write set.
                    if (local_write.op_ == OperationType::Delete)
                    {
                        scan_batch.emplace_back(local_write.key_.get(),
                                                nullptr,
                                                RecordStatus::Deleted,
                                                cc_scan_tuple->key_ts_);
                    }
                    else
                    {
                        scan_batch.emplace_back(local_write.key_.get(),
                                                local_write.rec_.get(),
                                                RecordStatus::Normal,
                                                cc_scan_tuple->key_ts_);
                    }

                    scanner.MoveNext();
                    ++wset_it;
                }
            }
        }

        if (rit != wset_reverse_iters_.end())
        {
            auto &wset_it = rit->second.first;
            auto &wset_end = rit->second.second;
            const TxKey *batch_start_key = nullptr;
            if (scanner.Status() == ScannerStatus::Blocked)
            {
#ifdef RANGE_PARTITION_ENABLED
                batch_start_key = scan_next.scan_state_->SliceLastKey();
#else
                batch_start_key =
                    scan_batch.empty() ? nullptr : scan_batch[0].key_;
#endif
            }

            while (wset_it != wset_end && (batch_start_key == nullptr ||
                                           *batch_start_key < *wset_it->first ||
                                           *batch_start_key == *wset_it->first))
            {
                const WriteSetEntry &local_write = wset_it->second;
                // Returns the key-value pair in the local write set.
                if (local_write.op_ != OperationType::Delete)
                {
                    scan_batch.emplace_back(local_write.key_.get(),
                                            local_write.rec_.get(),
                                            RecordStatus::Normal,
                                            1);
                }
                else
                {
                    scan_batch.emplace_back(local_write.key_.get(),
                                            nullptr,
                                            RecordStatus::Deleted,
                                            1);
                }

                ++wset_it;
            }
        }
    }

#ifdef RANGE_PARTITION_ENABLED
    ScanDirection dir = scan_next.Direction();
    SlicePosition slice_pos = scan_next.scan_state_->slice_position_;
    bool scan_finished = (dir == ScanDirection::Forward &&
                          slice_pos == SlicePosition::LastSlice) ||
                         (dir == ScanDirection::Backward &&
                          slice_pos == SlicePosition::FirstSlice);

    if (scanner.Type() == CcmScannerType::RangePartition && scan_batch.empty())
    {
        // Scan next batch in range partition scans a slice at a time.
        // Keep scanning until we reach the last slice in last range or
        // we get something from the last slice scanned.
        if (!scan_finished)
        {
            scan_next.ResetResult();
            PushOperation(&scan_next);
            Process(scan_next);
            return;
        }
    }

    bool_resp_->Finish(scan_finished);
#else
    bool_resp_->Finish(false);
#endif
}

void TransactionExecution::ScanClose(
    const std::vector<UnlockTuple> &unlock_batch,
    uint64_t alias,
    const TableName &table_name)
{
    CcScanner *scanner = nullptr;
    auto scan_it = scans_.find(alias);
    if (scan_it == scans_.end())
    {
        return;
    }
    scanner = scan_it->second.scanner_.get();

    if (!unlock_batch.empty())
    {
        drain_batch_.reserve(unlock_batch.size());

        for (const UnlockTuple &tpl : unlock_batch)
        {
            // Newly-inserted records in the write set have empty cc entry
            // address.
            if (tpl.cce_addr_.Empty())
            {
                continue;
            }

            LockType lk_type = scanner->DeduceScanTupleLockType(tpl.status_);
            if (lk_type == LockType::NoLock)
            {
                continue;
            }

            uint16_t read_cnt =
                rw_set_.RemoveReadEntry(table_name, tpl.cce_addr_);
            if (read_cnt == 0)
            {
                drain_batch_.emplace_back(tpl.cce_addr_, tpl.version_ts_);
            }
        }
    }

#ifdef RANGE_PARTITION_ENABLED
    if (scan_it->second.slice_position_ == SlicePosition::Middle)
    {
        // Append last tuple of each ScanCache which has acquired ReadIntent to
        // the drain_batch_.
        //
        // 1) If last_tuple.lk_type is NoLock, then drain_batch_ doesn't include
        // them and should append them into itself. 2) If last_tuple.lk_type is
        // not NoLock, then drain_batch_ has include them, and should skip them.
        // Non-repetition and non-omission.
        std::vector<const ScanTuple *> last_tuples;
        last_tuples.reserve(scanner->CacheCount());
        scanner->ShardCacheLastTuples(&last_tuples);
        for (const ScanTuple *last_tuple : last_tuples)
        {
            if (last_tuple)
            {
                LockType lk_type =
                    scanner->DeduceScanTupleLockType(last_tuple->rec_status_);
                if (lk_type == LockType::NoLock)
                {
                    drain_batch_.emplace_back(last_tuple->cce_addr_,
                                              last_tuple->key_ts_);
                }
            }
        }
    }
#endif

    // Release trailing tuple locks acquired during scan. These tuples are
    // tuples scanned beyond scan end key and are not intended to be locked.
    // They were not added into read set. Check if they were put into read set
    // by other operations before, if not, release these locks.
    std::vector<const ScanTuple *> trailing_tuples;
    scanner->ShardCacheTrailingTuples(&trailing_tuples);
    for (auto tuple : trailing_tuples)
    {
        LockType lk_type = scanner->DeduceScanTupleLockType(tuple->rec_status_);
        if (lk_type != LockType::NoLock &&
            rw_set_.GetReadCnt(table_name, tuple->cce_addr_) == 0)
        {
            drain_batch_.emplace_back(tuple->cce_addr_, tuple->key_ts_);
        }
    }

#ifndef RANGE_PARTITION_ENABLED
    if (scanner != nullptr)
    {
        DrainScanner(scanner, table_name);
    }
#endif

    cc_handler_->ScanClose(
        table_name, scanner->Direction(), std::move(scan_it->second.scanner_));

    abundant_lock_op_.Reset();
    PushOperation(&abundant_lock_op_);
    Process(abundant_lock_op_);
    scans_.erase(scan_it);
}

void TransactionExecution::Update(const TableName &table_name,
                                  TxKey::Uptr key,
                                  TxRecord::Uptr rec)
{
    Upsert(table_name, std::move(key), std::move(rec), OperationType::Update);
}

TxErrorCode TransactionExecution::Insert(const TableName &table_name,
                                         TxKey::Uptr key,
                                         TxRecord::Uptr rec)
{
    TxResult<Void> tx_result(nullptr, nullptr);
    void_resp_ = &tx_result;
    Upsert(table_name, std::move(key), std::move(rec), OperationType::Insert);
    assert(tx_result.Status() != TxResultStatus::Unknown);

    return tx_result.ErrorCode();
}

void TransactionExecution::Delete(const TableName &table_name, TxKey::Uptr key)
{
    TxRecord::Uptr rec{nullptr};
    Upsert(table_name, std::move(key), std::move(rec), OperationType::Delete);
}

// Upsert modify tuple without locking in OCC protocol.
void TransactionExecution::Upsert(const TableName &table_name,
                                  TxKey::Uptr key,
                                  TxRecord::Uptr rec,
                                  OperationType op)
{
    if (!rw_set_.AddWrite(table_name, std::move(key), std::move(rec), op))
    {
        void_resp_->FinishError(TxErrorCode::WRITE_SET_BYTES_COUNT_EXCEED_ERR);
        return;
    }
    void_resp_->Finish(void_);
}

void TransactionExecution::Commit()
{
    if (tx_term_ < 0)
    {
        bool_resp_->Finish(false);
        // transaction can be recycled and put into free list.
        tx_status_.store(TxnStatus::Finished, std::memory_order_release);
        Reset();
        return;
    }

    bool is_recovering = TxStatus() == TxnStatus::Recovering;
    if (!is_recovering)
    {
        tx_status_.store(TxnStatus::Committing, std::memory_order_relaxed);
    }
    if (rw_set_.WriteSetSize() > 0)
    {
        assert(!is_recovering);
#ifdef RANGE_PARTITION_ENABLED
        lock_write_ranges_.Reset();
        PushOperation(&lock_write_ranges_);
        Process(lock_write_ranges_);
#else
        PushOperation(&acquire_write_);
        Process(acquire_write_);
#endif
    }
    else
    {
        if (is_recovering)
        {
            // For recover tx commit, commit_ts is already determined and
            // we don't need to update TEntry since it is not assigned for
            // a recovering tx. Just release all the locks in readset and
            // recycle txm.
            PushOperation(&validate_);
            Process(validate_);
        }
        else
        {
            PushOperation(&set_ts_);
            Process(set_ts_);
        }
    }
}

void TransactionExecution::Abort()
{
    if (tx_term_ < 0 || !CheckLeaderTerm())
    {
        if (bool_resp_ != nullptr)
        {
            bool_resp_->Finish(false);
        }

        // transaction can be recycled and put into free list.
        tx_status_.store(TxnStatus::Finished, std::memory_order_release);
        Reset();
        return;
    }

    bool is_recovering = TxStatus() == TxnStatus::Recovering;
    tx_status_.store(TxnStatus::Aborted, std::memory_order_relaxed);

    if (!is_recovering)
    {
        PushOperation(&update_txn_);
        Process(update_txn_);
    }
    else
    {
        // No need to update txn status since we did not assign
        // TEntry for recovering tx.
        uint32_t acquire_write_cnt =
            rw_set_.WriteSetSize() + rw_set_.ForwardWriteCnt();
        if (acquire_write_cnt > 0 && acquire_write_.hd_result_.IsError())
        {
            std::vector<AcquireKeyResult> &acquire_key_vec =
                acquire_write_.hd_result_.Value();
            size_t error_cnt = 0;
            for (const AcquireKeyResult &acq_key : acquire_key_vec)
            {
                if (acq_key.cce_addr_.Term() < 0)
                {
                    ++error_cnt;
                }
            }
            acquire_write_cnt -= error_cnt;
        }
#ifdef RANGE_PARTITION_ENABLED
        else if (lock_write_ranges_.lock_range_result_->IsError())
        {
            acquire_write_cnt = 0;
        }
#endif

#ifdef ON_KEY_OBJECT
        acquire_write_cnt += rw_set_.ObjectCommandSize();
#endif
        post_process_.Reset(acquire_write_cnt,
                            rw_set_.ReadSetSize(),
                            rw_set_.CatalogRangeSetSize());
        PushOperation(&post_process_);
        Process(post_process_);
    }
}

void TransactionExecution::Process(LockWriteRangesOp &lock_write_ranges)
{
#ifdef RANGE_PARTITION_ENABLED
    if (!lock_write_ranges.init_)
    {
        std::unordered_map<TableName, TableWriteSet> &wset = rw_set_.WriteSet();
        lock_write_ranges.table_it_ = wset.begin();
        lock_write_ranges.table_end_ = wset.end();

        lock_write_ranges.write_key_it_ =
            lock_write_ranges.table_it_->second.begin();
        lock_write_ranges.write_key_end_ =
            lock_write_ranges.table_it_->second.end();

        lock_write_ranges.init_ = true;
    }

    assert(lock_write_ranges.table_it_ != lock_write_ranges.table_end_);
    assert(lock_write_ranges.write_key_it_ != lock_write_ranges.write_key_end_);

    const TxKey *write_key = lock_write_ranges.write_key_it_->first;

    lock_write_ranges.lock_range_result_->Value().Reset();
    lock_write_ranges.lock_range_result_->Reset();
    lock_write_ranges.is_running_ = true;

    const TableName &tbl_name = lock_write_ranges.table_it_->first;
    lock_write_ranges.range_table_name_ =
        TableName(tbl_name.StringView(), TableType::RangePartition);

    bool finished =
        cc_handler_->ReadLocal(lock_write_ranges.range_table_name_,
                               *write_key,
                               range_rec_,
                               ReadType::Inside,
                               tx_number_.load(std::memory_order_relaxed),
                               tx_term_,
                               command_id_.load(std::memory_order_relaxed),
                               start_ts_,
                               lock_range_result_,
                               IsolationLevel::RepeatableRead,
                               CcProtocol::Locking,
                               false,
                               false,
                               lock_write_ranges.execute_immediately_);

    if (finished)
    {
        command_id_.fetch_add(1, std::memory_order_relaxed);
    }
#endif
}

void TransactionExecution::PostProcess(LockWriteRangesOp &lock_write_ranges)
{
#ifdef RANGE_PARTITION_ENABLED
    if (lock_write_ranges.lock_range_result_->IsError())
    {
        DLOG(ERROR) << "LockWriteRangesOp failed for cc error:"
                    << lock_write_ranges.lock_range_result_->ErrorMsg()
                    << ", tx " << TxNumber();
        state_stack_.pop_back();
        assert(state_stack_.empty());
        Abort();
        return;
    }

    const TxKey *range_start_key = range_rec_.GetRangeInfo()->StartKey();
    const TxKey *range_end_key = range_rec_.GetRangeInfo()->EndKey();

    const ReadKeyResult &read_res =
        lock_write_ranges.lock_range_result_->Value();
    const TableName &tbl_name = lock_write_ranges.table_it_->first;
    TableName range_tbl_name(tbl_name.StringView(), TableType::RangePartition);
    rw_set_.AddRead(read_res.cce_addr_, read_res.ts_, &range_tbl_name);

    const TxKey *write_key = lock_write_ranges.write_key_it_->first;
    assert(range_start_key == nullptr || !(*write_key < *range_start_key));
    assert(range_end_key == nullptr || *write_key < *range_end_key);

    lock_write_ranges.Advance(this);

    if (lock_write_ranges.table_it_ == lock_write_ranges.table_end_)
    {
        state_stack_.pop_back();
        assert(state_stack_.empty());

        PushOperation(&acquire_write_);
        Process(acquire_write_);
    }
    else
    {
        // There are more ranges to acquire read locks. Returns now and waits
        // for the next round of execution to acquire read locks on the
        // remaining ranges. Note that we do not call Forward() here. This is
        // because doing so leads to recursive calls of Forward(), each
        // acquiring a read lock on one range. This may result in stack overflow
        // when there are many ranges for write-set keys.
        lock_write_ranges.is_running_ = false;
        lock_write_ranges.execute_immediately_ = true;
        command_id_.fetch_add(1, std::memory_order_relaxed);
    }
#endif
}

void TransactionExecution::Process(AcquireWriteOperation &acquire_write)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &acquire_write,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    acquire_write.Reset(rw_set_.WriteSetSize() + rw_set_.ForwardWriteCnt(),
                        rw_set_.WriteSetSize());
    acquire_write.is_running_ = true;

    uint64_t current_ts =
        static_cast<LocalCcHandler *>(cc_handler_)->GetTsBaseValue();

    size_t res_idx = 0, entry_idx = 0;
    std::unordered_map<TableName, TableWriteSet> &wset = rw_set_.WriteSet();
    for (auto &[table_name, table_write_set] : wset)
    {
        for (auto &[key_ptr, write_entry] : table_write_set)
        {
#ifndef RANGE_PARTITION_ENABLED
            size_t hash = write_entry.key_->Hash();
            write_entry.key_shard_code_ = Sharder::Instance().ShardCode(hash);
#endif
            acquire_write.acquire_write_entries_[entry_idx++] = &write_entry;

            // TODO: enable is_insert after Serializable Isolation is
            // supported.
            cc_handler_->AcquireWrite(
                table_name,
                *write_entry.key_,
                write_entry.key_shard_code_,
                TxNumber(),
                tx_term_,
                command_id_.load(std::memory_order_relaxed),
                current_ts,
                false,
                acquire_write.hd_result_,
                res_idx++,
                protocol_,
                iso_level_);
            for (auto &[forward_shard_code, cce_addr] :
                 write_entry.forward_addr_)
            {
                cc_handler_->AcquireWrite(
                    table_name,
                    *write_entry.key_,
                    forward_shard_code,
                    TxNumber(),
                    tx_term_,
                    command_id_.load(std::memory_order_relaxed),
                    current_ts,
                    false,
                    acquire_write.hd_result_,
                    res_idx++,
                    protocol_,
                    iso_level_);
            }
        }
    }

    if (metrics::enable_transactions &&
        acquire_write.hd_result_.Value().at(0).remote_ack_cnt_->load(
            std::memory_order_relaxed) > 0)
    {
        auto meter = tx_processor_->meter_.get();
        meter->Collect(tx_processor_->REMOTE_REQUEST_ON_FLY_COUNT_NAME_,
                       metrics::Value::IncDecValue::Increment,
                       "acquire_write");
        acquire_write.op_start_ = metrics::Clock::now();
    }

    StartTiming();
}

void TransactionExecution::PostProcess(AcquireWriteOperation &acquire_write)
{
    // collect metrics: remote acquire write duration
    if (metrics::enable_transactions &&
        acquire_write.op_start_ < metrics::TimePoint::max())
    {
        metrics::Meter *meter;
        meter = tx_processor_->meter_.get();
        meter->CollectDuration(tx_processor_->REMOTE_REQUEST_DURATION_NAME_,
                               acquire_write.op_start_,
                               "acquire_write");
        meter = tx_processor_->meter_.get();
        meter->Collect(tx_processor_->REMOTE_REQUEST_ON_FLY_COUNT_NAME_,
                       metrics::Value::IncDecValue::Decrement,
                       "acquire_write");
    }

    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &acquire_write,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    state_stack_.pop_back();
    assert(state_stack_.empty());

    if (acquire_write.rset_has_expired_)
    {
        DLOG(ERROR) << "AcquireWriteOperation failed for rset has expired."
                    << "; txn: " << TxNumber();
        bool_resp_->SetErrorCode(TxErrorCode::WRITE_WRITE_CONFLICT);
        Abort();
    }
    else if (acquire_write.hd_result_.IsError())
    {
        DLOG(ERROR) << "AcquireWriteOperation failed for cc error:"
                    << acquire_write.hd_result_.ErrorMsg() << "  "
                    << static_cast<int>(acquire_write.hd_result_.ErrorCode())
                    << "; txn: " << TxNumber();
        bool_resp_->SetErrorCode(
            ConvertCcError(acquire_write.hd_result_.ErrorCode()));
        Abort();
    }
    else
    {
        PushOperation(&set_ts_);
        Process(set_ts_);
    }
}

void TransactionExecution::Process(SetCommitTsOperation &set_ts)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &set_ts,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    uint64_t candidate = commit_ts_bound_;

    set_ts.hd_result_.Reset();
    set_ts.is_running_ = true;

    for (const AcquireKeyResult &acquire_key :
         acquire_write_.hd_result_.Value())
    {
        candidate = std::max(candidate, acquire_key.last_vali_ts_ + 1);
        candidate = std::max(candidate, acquire_key.commit_ts_ + 1);
    }

    // TODO(zkl): update candidate with ObjectCommandOp's result's last_vali_ts_
    //  and commit_ts_

    const std::unordered_map<TableName,
                             std::unordered_map<CcEntryAddr, ReadSetEntry>>
        &rset = rw_set_.ReadSet();
    for (const auto &table_entry_it : rset)
    {
        for (auto read_it = table_entry_it.second.begin();
             read_it != table_entry_it.second.end();
             ++read_it)
        {
            candidate = std::max(candidate, read_it->second.version_ts_ + 1);
        }
    }

    set_ts.Reset();
    cc_handler_->SetCommitTimestamp(txid_, candidate, set_ts.hd_result_);
}

void TransactionExecution::PostProcess(SetCommitTsOperation &set_ts)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &set_ts,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    state_stack_.pop_back();
    assert(state_stack_.empty());

    if (set_ts.hd_result_.IsError())
    {
        DLOG(ERROR) << "SetCommitTsOperation failed for cc error:"
                    << set_ts.hd_result_.ErrorMsg();
        Abort();
    }
    else
    {
        commit_ts_ = set_ts.hd_result_.Value();
        if (rw_set_.ReadSetSize() > 0)
        {
            PushOperation(&validate_);
            Process(validate_);
        }
        else
        {
            bool needs_write_log =
                !txservice_skip_redo_log && rw_set_.NeedsWriteLog();
            if (txlog_ != nullptr && needs_write_log)
            {
#ifdef ON_KEY_OBJECT
                FillCommandLogRequest(write_log_);
#else
                FillDataLogRequest(write_log_);
#endif
                PushOperation(&write_log_);
                Process(write_log_);
            }
            else
            {
                bool is_recovering = TxStatus() == TxnStatus::Recovering;
                tx_status_.store(TxnStatus::Committed,
                                 std::memory_order_relaxed);

                if (!is_recovering)
                {
                    PushOperation(&update_txn_);
                    Process(update_txn_);
                }
                else
                {
                    // No need to update txn status since we did not assign
                    // TEntry for recovering tx.
                    uint32_t acquire_write_cnt =
                        rw_set_.WriteSetSize() + rw_set_.ForwardWriteCnt();
#ifdef ON_KEY_OBJECT
                    acquire_write_cnt += rw_set_.ObjectCommandSize();
#endif
                    post_process_.Reset(
                        acquire_write_cnt, 0, rw_set_.CatalogRangeSetSize());
                    PushOperation(&post_process_);
                    Process(post_process_);
                }
            }
        }
    }
}

void TransactionExecution::Process(ValidateOperation &validate)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &validate,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    const std::unordered_map<TableName,
                             std::unordered_map<CcEntryAddr, ReadSetEntry>>
        &rset = rw_set_.ReadSet();

    size_t read_data_cnt = rw_set_.ReadSetSize();
    validate.Reset(read_data_cnt);
    validate.is_running_ = true;
    bool empty_rset = true;

    for (const auto &[tbl_name, tbl_read_set] : rset)
    {
        if (tbl_name == catalog_ccm_name ||
            tbl_name.Type() == TableType::RangePartition)
        {
            continue;
        }

        for (const auto &[cce_addr, read_entry] : tbl_read_set)
        {
            empty_rset = false;
            cc_handler_->PostRead(tx_number_.load(std::memory_order_relaxed),
                                  tx_term_,
                                  command_id_.load(std::memory_order_relaxed),
                                  read_entry.version_ts_,
                                  0,
                                  commit_ts_,
                                  cce_addr,
                                  validate.hd_result_);
        }
    }

    if (metrics::enable_transactions && !validate.hd_result_.Value().is_local_)
    {
        auto meter = tx_processor_->meter_.get();
        meter->Collect(tx_processor_->REMOTE_REQUEST_ON_FLY_COUNT_NAME_,
                       metrics::Value::IncDecValue::Increment,
                       "validate");
        validate.op_start_ = metrics::Clock::now();
    }

    if (empty_rset)
    {
        validate.hd_result_.SetFinished();
        Forward();
    }
    else
    {
        StartTiming();
    }
}

void TransactionExecution::PostProcess(ValidateOperation &validate)
{
    // collect metrics: remote validate duration
    if (metrics::enable_transactions && !validate.hd_result_.Value().is_local_)
    {
        metrics::Meter *meter;
        meter = tx_processor_->meter_.get();
        meter->CollectDuration(tx_processor_->REMOTE_REQUEST_DURATION_NAME_,
                               validate.op_start_,
                               "validate");
        meter->Collect(tx_processor_->REMOTE_REQUEST_ON_FLY_COUNT_NAME_,
                       metrics::Value::IncDecValue::Decrement,
                       "validate");
    }

    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &validate,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    // The validation step is optional. Only pops the stack if the last step
    // is the validation step.
    if (!state_stack_.empty())
    {
        assert(state_stack_.back() == &validate_);
        state_stack_.pop_back();
    }

    if (validate.IsError())
    {
        DLOG_IF(INFO, TRACE_OCC_ERR)
            << "Validate, occ_err, txn: " << tx_number_
            << " ,hd_result_.IsError():"
            << static_cast<int>(validate.hd_result_.ErrorCode())
            << " ,conflict_tx size:" << validate.hd_result_.Value().Size();

        DLOG(ERROR) << "ValidateOperation failed for cc error:"
                    << validate.hd_result_.ErrorMsg() << ", txn " << TxNumber();

        if (bool_resp_ != nullptr)
        {
            bool_resp_->SetErrorCode(
                ConvertCcError(validate.hd_result_.ErrorCode()));
        }
#ifdef ON_KEY_OBJECT
        else if (rec_resp_ != nullptr)
        {
            // auto committed ObjectCommandTxRequest
            rec_resp_->FinishError(
                ConvertCcError(validate.hd_result_.ErrorCode()));
            rec_resp_ = nullptr;
        }
        else if (vct_rec_resp_ != nullptr)
        {
            // auto committed MultiObjectCommandTxRequest
            vct_rec_resp_->FinishError(
                ConvertCcError(validate.hd_result_.ErrorCode()));
            vct_rec_resp_ = nullptr;
        }
#endif

        Abort();
    }
    else
    {
        bool needs_write_log =
            !txservice_skip_redo_log && rw_set_.NeedsWriteLog();
        if (txlog_ != nullptr && needs_write_log)
        {
#ifdef ON_KEY_OBJECT
            FillCommandLogRequest(write_log_);
#else
            FillDataLogRequest(write_log_);
#endif
            PushOperation(&write_log_);
            Process(write_log_);
        }
        else
        {
            bool is_recovering = TxStatus() == TxnStatus::Recovering;
            tx_status_.store(TxnStatus::Committed, std::memory_order_relaxed);

            // This is a read-only tx. Notifies early before post-processing.
            if (bool_resp_ != nullptr)
            {
                bool_resp_->Finish(true);
                bool_resp_ = nullptr;
            }

            if (!is_recovering)
            {
                PushOperation(&update_txn_);
                Process(update_txn_);
            }
            else
            {
                // No need to update txn status since we did not assign TEntry
                // for recovering tx.
                post_process_.Reset(rw_set_.WriteSetSize() +
                                        rw_set_.ForwardWriteCnt() +
                                        rw_set_.ObjectCommandSize(),
                                    0,
                                    rw_set_.CatalogRangeSetSize());
                PushOperation(&post_process_);
                Process(post_process_);
            }
        }
    }
}

void TransactionExecution::FillDataLogRequest(WriteToLogOp &write_log)
{
    write_log.log_type_ = TxLogType::DATA;
    ACTION_FAULT_INJECTOR("before_write_log");

    write_log.log_closure_.LogRequest().Clear();

    ::txlog::LogRequest &log_req = write_log.log_closure_.LogRequest();
    ::txlog::WriteLogRequest *log_rec = log_req.mutable_write_log_request();

    log_rec->set_tx_term(tx_term_);
    log_rec->set_txn_number(TxNumber());
    log_rec->set_commit_timestamp(commit_ts_);
    log_rec->set_retry(false);

    auto shard_terms = log_rec->mutable_node_terms();
    shard_terms->clear();

    auto data_log_msg = log_rec->mutable_log_content()->mutable_data_log();
    auto shard_logs = data_log_msg->mutable_node_txn_logs();
    shard_logs->clear();

    assert(log_rec->node_terms_size() == 0);

    // old structure
    const std::unordered_map<TableName, TableWriteSet> &wset =
        rw_set_.WriteSet();
    // new structure
    std::unordered_map<
        NodeGroupId,
        std::unordered_map<TableName, std::vector<const WriteSetEntry *>>>
        ng_table_rec_set;

    // reorganize all WriteSetEntries from old structure to new structure
    for (const auto &[table_name, table_write_set] : wset)
    {
        for (const auto &[key_ptr, wset_entry] : table_write_set)
        {
            const CcEntryAddr &addr = wset_entry.cce_addr_;
            uint32_t cc_node_id = addr.NodeGroupId();

            // Only fills WriteLogRequest::node_terms for base table.
            auto shard_term_it = shard_terms->find(cc_node_id);
            if (shard_term_it == shard_terms->end())
            {
                (*shard_terms)[cc_node_id] = addr.Term();
            }
            else if (shard_term_it->second != addr.Term())
            {
                // Two keys in the tx's write set refer to the same cc node
                // group, but have different terms. It means that the cc node
                // must have failed over at least once and the tx have obtained
                // a write intention before the failure. The tx must abort
                // because the write intention obtained  the failure have been
                // invalidated.
                write_log.hd_result_.SetError(CcErrorCode::NG_TERM_CHANGED);
                return;
            }

            auto table_rec_it = ng_table_rec_set.try_emplace(cc_node_id);
            std::unordered_map<TableName, std::vector<const WriteSetEntry *>>
                &table_rec_set = table_rec_it.first->second;

            auto rec_vec_it = table_rec_set.emplace(
                std::piecewise_construct,
                std::forward_as_tuple(table_name.StringView(),
                                      table_name.Type()),
                std::forward_as_tuple());

            rec_vec_it.first->second.emplace_back(&wset_entry);

            for (const auto &[forward_shard_code, addr] :
                 wset_entry.forward_addr_)
            {
                // If the wset entry needs to be double written into different
                // ngs, write log for both ngs.
                uint32_t forward_ng_id =
                    Sharder::Instance().ShardToCcNodeGroup(forward_shard_code);
                auto table_rec_it = ng_table_rec_set.try_emplace(forward_ng_id);
                std::unordered_map<TableName,
                                   std::vector<const WriteSetEntry *>>
                    &table_rec_set = table_rec_it.first->second;

                auto rec_vec_it = table_rec_set.emplace(
                    std::piecewise_construct,
                    std::forward_as_tuple(table_name.StringView(),
                                          table_name.Type()),
                    std::forward_as_tuple(
                        std::vector<const WriteSetEntry *>()));
                rec_vec_it.first->second.emplace_back(&wset_entry);
            }
        }
    }

    // construct one log_ng_blob per ng_id
    for (const auto &[ng_id, table_rec_set] : ng_table_rec_set)
    {
        std::string *log_ng_blob = nullptr;
        auto shard_it = shard_logs->find(ng_id);
        if (shard_it == shard_logs->end())
        {
            std::string blob;
            (*shard_logs)[ng_id] = blob;
            log_ng_blob = &shard_logs->at(ng_id);
        }
        else
        {
            log_ng_blob = &shard_it->second;
        }

        // The log blob of a table in a node group is in the following
        // format: (1) A 1-byte integer for the length of the table name,
        // followed by (2) The string of the table name. (3) A 1-byte
        // integer for the type of table. (4) A 4-byte integer for the total
        // length of serialized key-record pairs modified by the tx in the
        // node group. (5) A sequence of modified records. Each record is
        // encoded as follows:
        //   (a) The serialized key
        //   (b) A 1-byte flag to indicate if the record is normal, deleted
        //   or void. (c) The serialized record if the record is normal.
        for (const auto &[table_name, wset_entry_vec] : table_rec_set)
        {
            uint8_t tabname_len = table_name.StringView().size();
            const char *ptr = reinterpret_cast<const char *>(&tabname_len);
            log_ng_blob->append(ptr, sizeof(uint8_t));
            log_ng_blob->append(table_name.StringView().data(), tabname_len);
            // 1 byte integer for table type
            ptr = reinterpret_cast<const char *>(&table_name.Type());
            log_ng_blob->append(ptr, sizeof(uint8_t));

            // The start position of the 4-byte integer for the length of
            // serialized k-v pairs.
            size_t kv_len_start = log_ng_blob->size();
            uint32_t kv_len = 0;
            ptr = reinterpret_cast<const char *>(&kv_len);
            // Reserves 4 bytes in the blob for the k-v length before
            // committed records are serialized and the length of the
            // serialized records are known.
            log_ng_blob->append(ptr, sizeof(uint32_t));

            for (const auto &wset_entry : wset_entry_vec)
            {
                wset_entry->key_->Serialize(*log_ng_blob);

                uint8_t operation = static_cast<uint8_t>(wset_entry->op_);
                log_ng_blob->append(reinterpret_cast<const char *>(&operation),
                                    1);

                if (wset_entry->op_ != OperationType::Delete &&
                    wset_entry->rec_ != nullptr)
                {
                    wset_entry->rec_->Serialize(*log_ng_blob);
                }
            }

            kv_len = log_ng_blob->size() - kv_len_start - sizeof(uint32_t);

            // Refills the reserved 4 bytes after knowing the length of
            // serialized records.
            log_ng_blob->replace(
                kv_len_start, sizeof(uint32_t), ptr, sizeof(uint32_t));
        }
    }
}

void TransactionExecution::FillCommandLogRequest(WriteToLogOp &write_log)
{
#ifdef ON_KEY_OBJECT
    write_log.log_type_ = TxLogType::DATA;

    write_log.log_closure_.LogRequest().Clear();

    ::txlog::LogRequest &log_req = write_log.log_closure_.LogRequest();
    ::txlog::WriteLogRequest *log_rec = log_req.mutable_write_log_request();

    log_rec->set_tx_term(tx_term_);
    log_rec->set_txn_number(txid_.TxNumber());
    log_rec->set_commit_timestamp(commit_ts_);
    log_rec->set_retry(false);

    auto shard_terms = log_rec->mutable_node_terms();
    shard_terms->clear();

    auto cmd_log_msg = log_rec->mutable_log_content()->mutable_data_log();
    auto shard_logs = cmd_log_msg->mutable_node_txn_logs();
    shard_logs->clear();

    assert(log_rec->node_terms_size() == 0);

    const std::unordered_map<TableName,
                             std::unordered_map<CcEntryAddr, CmdSetEntry>>
        &tx_cmd_set = *rw_set_.ObjectCommandCce();

    // organize by node group
    std::unordered_map<NodeGroupId, std::vector<const CmdSetEntry *>>
        ng_obj_cmds;
    for (const auto &[table_name, obj_cmd_set] : tx_cmd_set)
    {
        for (const auto &[cce_addr, obj_cmd_entry] : obj_cmd_set)
        {
            // skip those CmdSetEntry that have no successful commands
            if (!obj_cmd_entry.HasSuccessfulCommand())
            {
                continue;
            }
            uint32_t ng_id = cce_addr.NodeGroupId();
            auto shard_term_it = shard_terms->find(ng_id);
            if (shard_term_it == shard_terms->end())
            {
                (*shard_terms)[ng_id] = cce_addr.Term();
            }
            else if (shard_term_it->second != cce_addr.Term())
            {
                // Two keys in the tx's write set refer to the same cc node
                // group, but have different terms.
                // TODO(zkl): remote data
            }

            auto &obj_cmds_vector =
                ng_obj_cmds.try_emplace(ng_id).first->second;
            // insert cce into cmd_set
            obj_cmds_vector.emplace_back(&obj_cmd_entry);
        }
    }

    // construct one log_ng_blob per ng_id
    for (const auto &[ng_id, cmd_entry_vec] : ng_obj_cmds)
    {
        (*shard_logs)[ng_id] = std::string{};
        std::string &log_ng_blob = shard_logs->at(ng_id);

        for (auto cmd_entry : cmd_entry_vec)
        {
            const std::string &key_str = cmd_entry->obj_key_str_;
            uint64_t obj_version = cmd_entry->object_version_;
            const std::vector<std::string> cmd_str_list =
                cmd_entry->cmd_str_list_;

            // The start position of the 4-byte integer for the length of
            // serialized key and object commands.
            size_t key_cmd_len_start = log_ng_blob.size();
            uint32_t key_cmd_len = 0;
            const char *ptr = reinterpret_cast<const char *>(&key_cmd_len);
            // Reserve 4 bytes in the blob for the length of serialized key and
            // commands before it is known.
            log_ng_blob.append(ptr, sizeof(uint32_t));

            // write object key, object version, and commands to log blob
            log_ng_blob.append(key_str);
            log_ng_blob.append(reinterpret_cast<const char *>(&obj_version),
                               sizeof(obj_version));

            size_t cmds_len_start = log_ng_blob.size();
            uint32_t cmds_len = 0;
            log_ng_blob.append(reinterpret_cast<const char *>(&cmds_len),
                               sizeof(cmds_len));

            uint8_t has_del = cmd_entry->has_del_;
            log_ng_blob.append(reinterpret_cast<const char *>(&has_del),
                               sizeof(has_del));
            // number of commands
            uint16_t cmd_cnt = cmd_str_list.size();
            log_ng_blob.append(reinterpret_cast<const char *>(&cmd_cnt),
                               sizeof(cmd_cnt));

            for (const auto &cmd_str : cmd_str_list)
            {
                uint32_t cmd_len = cmd_str.size();
                log_ng_blob.append(reinterpret_cast<const char *>(&cmd_len),
                                   sizeof(cmd_len));
                log_ng_blob.append(cmd_str);
            }

            cmds_len = log_ng_blob.size() - cmds_len_start - sizeof(uint32_t);
            log_ng_blob.replace(cmds_len_start,
                                sizeof(cmds_len),
                                reinterpret_cast<const char *>(&cmds_len),
                                sizeof(cmds_len));

            key_cmd_len =
                log_ng_blob.size() - key_cmd_len_start - sizeof(uint32_t);

            // Refills the reserved 4 bytes after knowing the length of
            // serialized key and commands.
            log_ng_blob.replace(
                key_cmd_len_start, sizeof(uint32_t), ptr, sizeof(uint32_t));
        }
    }
#endif
}

void TransactionExecution::Process(WriteToLogOp &write_log)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &write_log,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });

    write_log.Reset();
    write_log.is_running_ = true;

    if (metrics::enable_transactions)
    {
        auto meter = tx_processor_->meter_.get();
        meter->Collect(tx_processor_->REMOTE_REQUEST_ON_FLY_COUNT_NAME_,
                       metrics::Value::IncDecValue::Increment,
                       "write_log");
        write_log.op_start_ = metrics::Clock::now();
    }

    assert(txlog_ != nullptr);
    // Note that node_id calculated from global core ID should always be
    // equal to the actual ccshard node id. But from txservice layer's view,
    // only txid is available. Txservice get txid from the bottom layer
    // (ccshard).
    write_log.log_group_id_ = txlog_->GetLogGroupId(tx_number_);
    ::txlog::WriteLogRequest *wlog_req =
        write_log.log_closure_.LogRequest().mutable_write_log_request();
    wlog_req->set_log_group_id(write_log.log_group_id_);
#ifdef EXT_TX_PROC_ENABLED
    write_log.hd_result_.SetToBlock();
    std::atomic_thread_fence(std::memory_order_release);
#endif

    txlog_->WriteLog(write_log.log_group_id_,
                     write_log.log_closure_.Controller(),
                     write_log.log_closure_.LogRequest(),
                     write_log.log_closure_.LogResponse(),
                     write_log.log_closure_);
    ACTION_FAULT_INJECTOR("after_write_log");
}

void TransactionExecution::PostProcess(WriteToLogOp &write_log)
{
    // collect metrics: write log duration
    if (metrics::enable_transactions)
    {
        metrics::Meter *meter;
        meter = tx_processor_->meter_.get();
        meter->CollectDuration(tx_processor_->REMOTE_REQUEST_DURATION_NAME_,
                               write_log.op_start_,
                               "write_log");
        meter = tx_processor_->meter_.get();
        meter->Collect(tx_processor_->REMOTE_REQUEST_ON_FLY_COUNT_NAME_,
                       metrics::Value::IncDecValue::Decrement,
                       "write_log");
    }
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &write_log,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    WriteToLogOp *log_op = static_cast<WriteToLogOp *>(state_stack_.back());
    state_stack_.pop_back();

    if (state_stack_.empty())
    {
        if (!log_op->hd_result_.IsError())
        {
            tx_status_.store(TxnStatus::Committed, std::memory_order_relaxed);
            // TODO(zkl): finish resp.
        }
        else
        {
            if (log_op->hd_result_.ErrorCode() ==
                CcErrorCode::LOG_CLOSURE_RESULT_UNKNOWN_ERR)
            {
                if (bool_resp_ != nullptr)
                {
                    bool_resp_->SetErrorCode(
                        TxErrorCode::LOG_SERVICE_UNREACHABLE);
                    bool_resp_->Finish(false);
                    bool_resp_ = nullptr;
                }
#ifdef ON_KEY_OBJECT
                else if (rec_resp_ != nullptr)
                {
                    // auto committed ObjectCommandTxRequest
                    rec_resp_->FinishError(
                        TxErrorCode::LOG_SERVICE_UNREACHABLE);
                    rec_resp_ = nullptr;
                }
                else if (vct_rec_resp_ != nullptr)
                {
                    // auto committed MultiObjectCommandTxRequest
                    vct_rec_resp_->FinishError(
                        TxErrorCode::LOG_SERVICE_UNREACHABLE);
                    vct_rec_resp_ = nullptr;
                }
#endif
                tx_status_.store(TxnStatus::Unknown, std::memory_order_release);
            }
            else
            {
                DLOG(ERROR) << "WriteToLogOp failed for cc error:"
                            << log_op->hd_result_.ErrorMsg();
                if (bool_resp_ != nullptr)
                {
                    bool_resp_->SetErrorCode(TxErrorCode::WRITE_LOG_FAIL);
                    bool_resp_->Finish(false);
                    bool_resp_ = nullptr;
                }
#ifdef ON_KEY_OBJECT
                else if (rec_resp_ != nullptr)
                {
                    // auto committed ObjectCommandTxRequest
                    rec_resp_->SetErrorCode(TxErrorCode::WRITE_LOG_FAIL);
                    rec_resp_ = nullptr;
                }
                else if (vct_rec_resp_ != nullptr)
                {
                    // auto committed MultiObjectCommandTxRequest
                    vct_rec_resp_->SetErrorCode(TxErrorCode::WRITE_LOG_FAIL);
                    vct_rec_resp_ = nullptr;
                }
#endif

                tx_status_.store(TxnStatus::Aborted, std::memory_order_release);
            }
        }
        PushOperation(&update_txn_);
        Process(update_txn_);
    }
    else
    {
        // The tx is committing a multi-stage operation, e.g., schema
        // changes.
    }
}

void TransactionExecution::Process(UpdateTxnStatus &update_txn)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &update_txn,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    update_txn.Reset();
    update_txn.is_running_ = true;
    cc_handler_->UpdateTxnStatus(txid_,
                                 iso_level_,
                                 tx_status_.load(std::memory_order_relaxed),
                                 update_txn.hd_result_);
}

void TransactionExecution::PostProcess(UpdateTxnStatus &update_txn)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &update_txn,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    state_stack_.pop_back();

    uint32_t acquire_write_cnt =
        rw_set_.WriteSetSize() + rw_set_.ForwardWriteCnt();
    if (acquire_write_cnt > 0 && acquire_write_.hd_result_.IsError())
    {
        std::vector<AcquireKeyResult> &acquire_key_vec =
            acquire_write_.hd_result_.Value();
        size_t error_cnt = 0;
        for (const AcquireKeyResult &acq_key : acquire_key_vec)
        {
            if (acq_key.cce_addr_.Term() < 0)
            {
                ++error_cnt;
            }
        }
        acquire_write_cnt -= error_cnt;
    }
#ifdef RANGE_PARTITION_ENABLED
    else if (lock_write_ranges_.lock_range_result_->IsError())
    {
        acquire_write_cnt = 0;
    }
#endif

    TxnStatus status = TxStatus();
    if (status == TxnStatus::Committed)
    {
        // The tx is committed. The tx must have finished validation.
        // Post-processing includes both primary keys that have locks and
        // secondary keys without locks.
        post_process_.Reset(acquire_write_cnt + rw_set_.ObjectCommandSize(),
                            0,
                            rw_set_.CatalogRangeSetSize());
    }
    else if (status == TxnStatus::Aborted)
    {
        post_process_.Reset(acquire_write_cnt + rw_set_.ObjectCommandSize(),
                            rw_set_.ReadSetSize(),
                            rw_set_.CatalogRangeSetSize());
    }
    else if (status == TxnStatus::Unknown)
    {
        post_process_.Reset(
            0, rw_set_.ReadSetSize(), rw_set_.CatalogRangeSetSize());
    }
    PushOperation(&post_process_);
    Process(post_process_);
}

void TransactionExecution::Process(PostProcessOp &post_process)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &post_process,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });

    post_process.is_running_ = false;

    uint64_t tx_number = TxNumber();
    uint16_t command_id = command_id_.load(std::memory_order_relaxed);

    if (tx_status_.load(std::memory_order_relaxed) == TxnStatus::Committed)
    {
        // If the tx has finished validation, the read intentions/locks of
        // the read-set keys have been cleared after validation.
        // Post-processing only clears the write locks of the write-set
        // keys.

        size_t idx = 0;
        const std::unordered_map<TableName, TableWriteSet> &wset =
            rw_set_.WriteSet();
        for (const auto &[table_name, table_write_set] : wset)
        {
            for (const auto &[key, write_entry] : table_write_set)
            {
                cc_handler_->PostWrite(tx_number,
                                       tx_term_,
                                       command_id,
                                       commit_ts_,
                                       write_entry.cce_addr_,
                                       write_entry.rec_.get(),
                                       write_entry.op_,
                                       write_entry.key_shard_code_,
                                       post_process.hd_result_);
                ++idx;
                for (auto &[forward_shard_code, cce_addr] :
                     write_entry.forward_addr_)
                {
                    cc_handler_->PostWrite(tx_number,
                                           tx_term_,
                                           command_id,
                                           commit_ts_,
                                           cce_addr,
                                           write_entry.rec_.get(),
                                           write_entry.op_,
                                           forward_shard_code,
                                           post_process.hd_result_);
                    ++idx;
                }
            }
        }

#ifdef ON_KEY_OBJECT
        const std::unordered_map<TableName,
                                 std::unordered_map<CcEntryAddr, CmdSetEntry>>
            *cmd_cce_set = rw_set_.ObjectCommandCce();
        assert(cmd_cce_set != nullptr);

        for (const auto &[table_name, cce_set] : *cmd_cce_set)
        {
            for (const auto &[cce_addr, cmd_set_entry] : cce_set)
            {
                cc_handler_->PostWrite(tx_number,
                                       tx_term_,
                                       command_id,
                                       commit_ts_,
                                       cce_addr,
                                       nullptr,
                                       OperationType::CommitCommands,
                                       0,
                                       post_process.hd_result_);
                ++idx;
            }
        }
#endif

        if (idx == 0)
        {
            // post_process.Forward(this);
        }
    }
    else
    {
        // If the tx failed during the acquire phase or was aborted before
        // entering the commit phase, post-processing removes write intents
        // of write-set keys and clears read intents/locks of read-set keys.

        size_t idx = 0;

        if (TxStatus() != TxnStatus::Unknown)
        {
            const std::unordered_map<TableName, TableWriteSet> &wset =
                rw_set_.WriteSet();

            for (const auto &[table_name, table_write_set] : wset)
            {
                for (const auto &[key, write_entry] : table_write_set)
                {
                    if (write_entry.cce_addr_.Term() >= 0)
                    {
                        assert(!write_entry.cce_addr_.Empty());

                        // Abort doesn't care the OperationType, since PostWrite
                        // is just used to release the lock.
                        cc_handler_->PostWrite(
                            tx_number_.load(std::memory_order_relaxed),
                            tx_term_,
                            command_id_.load(std::memory_order_relaxed),
                            0,
                            write_entry.cce_addr_,
                            nullptr,
                            write_entry.op_,
                            write_entry.key_shard_code_,
                            post_process.hd_result_);
                        ++idx;
                    }
                    // Keys that were not successfully locked in the cc
                    // map do not need post-processing.

                    for (const auto &[forward_shard_code, cce_addr] :
                         write_entry.forward_addr_)
                    {
                        if (cce_addr.Term() >= 0)
                        {
                            assert(!cce_addr.Empty());
                            cc_handler_->PostWrite(tx_number,
                                                   tx_term_,
                                                   command_id,
                                                   0,
                                                   cce_addr,
                                                   nullptr,
                                                   write_entry.op_,
                                                   forward_shard_code,
                                                   post_process.hd_result_);
                            ++idx;
                        }
                    }
                }
            }

#ifdef ON_KEY_OBJECT
            const std::unordered_map<
                TableName,
                std::unordered_map<CcEntryAddr, CmdSetEntry>> *cmd_cce_set =
                rw_set_.ObjectCommandCce();
            assert(cmd_cce_set != nullptr);

            for (const auto &[table_name, cce_set] : *cmd_cce_set)
            {
                for (const auto &[cce_addr, cmd_set_entry] : cce_set)
                {
                    cc_handler_->PostWrite(tx_number,
                                           tx_term_,
                                           command_id,
                                           0,
                                           cce_addr,
                                           nullptr,
                                           OperationType::CommitCommands,
                                           0,
                                           post_process.hd_result_);
                    ++idx;
                }
            }
#endif
        }

        const std::unordered_map<TableName,
                                 std::unordered_map<CcEntryAddr, ReadSetEntry>>
            &rset = rw_set_.ReadSet();

        for (const auto &[tbl_name, data_read_set] : rset)
        {
            if (tbl_name == catalog_ccm_name ||
                tbl_name.Type() == TableType::RangePartition)
            {
                continue;
            }

            for (const auto &[cce_addr, read_entry] : data_read_set)
            {
                cc_handler_->PostRead(
                    tx_number_.load(std::memory_order_relaxed),
                    tx_term_,
                    command_id_.load(std::memory_order_relaxed),
                    0,
                    0,
                    0,
                    cce_addr,
                    post_process.hd_result_);
                ++idx;
            }
        }

        if (idx == 0)
        {
            // post_process.Forward(this);
        }
    }

    if (metrics::enable_transactions &&
        !post_process.hd_result_.Value().is_local_)
    {
        auto meter = tx_processor_->meter_.get();
        meter->Collect(tx_processor_->REMOTE_REQUEST_ON_FLY_COUNT_NAME_,
                       metrics::Value::IncDecValue::Increment,
                       "post_process");
        post_process.op_start_ = metrics::Clock::now();
    }

    StartTiming();
}

void TransactionExecution::PostProcess(PostProcessOp &post_process)
{
    // collect metrics: remote post process duration
    // collect metrics: tx duration, and tx processed total
    if (metrics::enable_transactions)
    {
        auto meter = tx_processor_->meter_.get();
        if (!post_process.hd_result_.Value().is_local_)
        {
            meter->CollectDuration(tx_processor_->REMOTE_REQUEST_DURATION_NAME_,
                                   post_process.op_start_,
                                   "post_process");
            meter->Collect(tx_processor_->REMOTE_REQUEST_ON_FLY_COUNT_NAME_,
                           metrics::Value::IncDecValue::Decrement,
                           "post_process");
        }
        meter->CollectDuration(tx_processor_->TX_DURATION_NAME_,
                               tx_duration_start_);
        meter->Collect(tx_processor_->TX_PROCESSED_TOTAL_NAME_, 1);
    }

    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &post_process,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    if (!state_stack_.empty())
    {
        assert(state_stack_.back() == &post_process);
        state_stack_.pop_back();
    }
    assert(state_stack_.empty());

    if (bool_resp_ != nullptr && bool_resp_ != &commit_tx_req_->tx_result_)
    {
        if (tx_status_.load(std::memory_order_relaxed) == TxnStatus::Committed)
        {
            bool_resp_->Finish(true);
        }
        else
        {
            bool_resp_->Finish(false);
        }
    }
#ifdef ON_KEY_OBJECT
    else if (rec_resp_ != nullptr)
    {
        // auto committed ObjectCommandTxRequest
        rec_resp_->Finish(obj_cmd_.hd_result_.Value().rec_status_);
        rec_resp_ = nullptr;
    }
    else if (vct_rec_resp_ != nullptr)
    {
        // auto committed MultiObjectCommandTxRequest
        std::vector<RecordStatus> vct_rec;
        vct_rec.reserve(multi_obj_cmd_.vct_hd_result_.size());
        for (const auto &hresult : multi_obj_cmd_.vct_hd_result_)
        {
            vct_rec.push_back(hresult.Value().rec_status_);
        }

        vct_rec_resp_->Finish(std::move(vct_rec));
        vct_rec_resp_ = nullptr;
    }
#endif
    // transaction can be recycled and put into free list.
    tx_status_.store(TxnStatus::Finished, std::memory_order_release);

    Reset();
}

void TransactionExecution::Process(AcquireAllOp &acq_all_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &acq_all_op,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    uint32_t node_group_cnt = Sharder::Instance().NodeGroupCount();
    acq_all_op.Reset(node_group_cnt);
    acq_all_op.is_running_ = true;

    for (uint32_t nid = 0; nid < node_group_cnt; ++nid)
    {
        CcHandlerResult<AcquireAllResult> &hres = acq_all_op.hd_results_[nid];
        hres.Reset();
        hres.Value().remote_ack_cnt_ = &acq_all_op.remote_ack_cnt_;
        cc_handler_->AcquireWriteAll(
            *acq_all_op.table_name_,
            *acq_all_op.key_,
            nid,
            tx_number_.load(std::memory_order_relaxed),
            tx_term_,
            command_id_.load(std::memory_order_relaxed),
            false,
            hres,
            acq_all_op.protocol_,
            acq_all_op.cc_op_);
    }
    StartTiming();
}

void TransactionExecution::PostProcess(AcquireAllOp &acq_all_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &acq_all_op,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    state_stack_.pop_back();
}

void TransactionExecution::Process(PostWriteAllOp &post_write_all_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &post_write_all_op,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    uint32_t node_group_cnt = Sharder::Instance().NodeGroupCount();
    post_write_all_op.Reset(node_group_cnt);
    post_write_all_op.is_running_ = true;

    for (uint32_t ngid = 0; ngid < node_group_cnt; ++ngid)
    {
        if (TxCcNodeId() == ngid)
        {
            // Send out local request at last to prevent it from
            // modifying rec_ while the handler is still using it.
            continue;
        }
        cc_handler_->PostWriteAll(*post_write_all_op.table_name_,
                                  *post_write_all_op.key_,
                                  *post_write_all_op.rec_,
                                  ngid,
                                  tx_number_.load(std::memory_order_relaxed),
                                  tx_term_,
                                  command_id_.load(std::memory_order_relaxed),
                                  commit_ts_,
                                  post_write_all_op.hd_result_,
                                  post_write_all_op.op_type_,
                                  post_write_all_op.write_type_);
    }

    cc_handler_->PostWriteAll(*post_write_all_op.table_name_,
                              *post_write_all_op.key_,
                              *post_write_all_op.rec_,
                              TxCcNodeId(),
                              tx_number_.load(std::memory_order_relaxed),
                              tx_term_,
                              command_id_.load(std::memory_order_relaxed),
                              commit_ts_,
                              post_write_all_op.hd_result_,
                              post_write_all_op.op_type_,
                              post_write_all_op.write_type_);
    StartTiming();
}

void TransactionExecution::PostProcess(PostWriteAllOp &post_write_all_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &post_write_all_op,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    state_stack_.pop_back();

    // So far, post-write-all is only used for schema evolution operations.
    assert(!state_stack_.empty());
}

void TransactionExecution::ReleaseCatalogRangeLock(
    CcHandlerResult<PostProcessResult> &catalog_range_hd_result)
{
    const std::unordered_map<TableName,
                             std::unordered_map<CcEntryAddr, ReadSetEntry>>
        &rset = rw_set_.ReadSet();
    size_t ref_cnt = catalog_range_hd_result.RefCnt();
    assert(ref_cnt != 0);

    for (const auto &[tbl_name, tbl_set] : rset)
    {
        if (tbl_name.Type() != TableType::Catalog &&
            tbl_name.Type() != TableType::RangePartition)
        {
            continue;
        }

        for (const auto &[cce_addr, read_entry] : tbl_set)
        {
            --ref_cnt;
            cc_handler_->PostRead(TxNumber(),
                                  TxTerm(),
                                  CommandId(),
                                  read_entry.version_ts_,
                                  0,
                                  commit_ts_,
                                  cce_addr,
                                  catalog_range_hd_result,
                                  true);
        }
    }
    assert(ref_cnt == 0);
}

void TransactionExecution::DrainScanner(CcScanner *scanner,
                                        const TableName &table_name)
{
    assert(scanner != nullptr);

    // drain out the scan tuple in the scan cache
    scanner->SetDrainCacheMode(true);
    const ScanTuple *cc_scan_tuple = scanner->Current();
    // In case the scan status is blocked before
    if (cc_scan_tuple == nullptr && scanner->Status() == ScannerStatus::Blocked)
    {
        scanner->MoveNext();
        cc_scan_tuple = scanner->Current();
    }

    while (cc_scan_tuple != nullptr)
    {
        TX_TRACE_ACTION_WITH_CONTEXT(
            this,
            "PostProcess.ScanOperation.AddReadSet.cce_ptr",
            &rw_set_,
            (
                [this, cc_scan_tuple]() -> std::string
                {
                    return std::string("\"tx_number\":")
                        .append(std::to_string(this->TxNumber()))
                        .append(",\"tx_term\":")
                        .append(std::to_string(this->tx_term_))
                        .append(",\"cce_ptr\":")
                        .append(
                            std::to_string(cc_scan_tuple->cce_addr_.CcePtr()));
                }));

        LockType scan_tuple_lock_type =
            scanner->DeduceScanTupleLockType(cc_scan_tuple->rec_status_);
        // "key_ts_ == 0", means the lock is added on gap. Now, gap lock is
        // not used when do scan operation.
        if (scan_tuple_lock_type != LockType::NoLock &&
            cc_scan_tuple->key_ts_ != 0 &&
            rw_set_.RemoveReadEntry(table_name, cc_scan_tuple->cce_addr_) == 0)
        {
            if (cc_scan_tuple->rec_status_ == RecordStatus::Unknown)
            {
                // Only used to release lock.
                drain_batch_.emplace_back(cc_scan_tuple->cce_addr_, 0);
            }
            else
            {
                drain_batch_.emplace_back(cc_scan_tuple->cce_addr_,
                                          cc_scan_tuple->key_ts_);
            }
        }
        scanner->MoveNext();
        cc_scan_tuple = scanner->Current();
    }
}

void TransactionExecution::Process(DsUpsertTableOp &ds_upsert_table_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &ds_upsert_table_op,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    ds_upsert_table_op.Reset();
    ds_upsert_table_op.is_running_ = true;
#ifdef EXT_TX_PROC_ENABLED
    ds_upsert_table_op.hd_result_.SetToBlock();
#endif
    cc_handler_->DataStoreUpsertTable(ds_upsert_table_op.table_schema_,
                                      ds_upsert_table_op.op_type_,
                                      commit_ts_,
                                      ds_upsert_table_op.hd_result_,
                                      ds_upsert_table_op.alter_table_info_);
}

void TransactionExecution::PostProcess(DsUpsertTableOp &ds_upsert_table_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &ds_upsert_table_op,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    state_stack_.pop_back();
    assert(!state_stack_.empty());
}

void TransactionExecution::Process(ReloadCacheOperation &reload_cache_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &reload_cache_op_,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });

    reload_cache_op_.is_running_ = true;

    uint32_t ng_cnt = Sharder::Instance().NodeGroupCount();

    for (NodeGroupId ng_id = 0; ng_id < ng_cnt; ++ng_id)
    {
        cc_handler_->ReloadCache(ng_id,
                                 TxNumber(),
                                 TxTerm(),
                                 CommandId(),
                                 reload_cache_op.hd_result_);
    }

    StartTiming();
}

void TransactionExecution::PostProcess(ReloadCacheOperation &reload_cache_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &reload_cache_op_,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    state_stack_.pop_back();
    assert(state_stack_.empty());

    if (reload_cache_op.hd_result_.IsError())
    {
        DLOG(INFO) << "ReloadCacheOperation FinishError for cc error: "
                   << reload_cache_op.hd_result_.ErrorMsg();
        void_resp_->FinishError(
            ConvertCcError(reload_cache_op.hd_result_.ErrorCode()));
    }
    else
    {
        void_resp_->Finish(void_);
    }
}

void TransactionExecution::Process(FaultInjectOp &fault_inject_op_)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &fault_inject_op_,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    fault_inject_op_.Reset();
    fault_inject_op_.is_running_ = true;

    cc_handler_->FaultInject(fault_inject_op_.fault_name_,
                             fault_inject_op_.fault_paras_,
                             tx_term_,
                             command_id_.load(std::memory_order_relaxed),
                             txid_,
                             fault_inject_op_.vct_node_id_,
                             fault_inject_op_.hd_result_);
    StartTiming();
}

void TransactionExecution::PostProcess(FaultInjectOp &fault_inject_op_)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &fault_inject_op_,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    state_stack_.pop_back();
    assert(state_stack_.empty());

    bool_resp_->Finish(fault_inject_op_.succeed_);
}

void TransactionExecution::ProcessTxRequest(
    CleanCcEntryForTestTxRequest &clean_req)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &clean_req,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    bool_resp_ = &clean_req.tx_result_;

    clean_entry_op_.Set(clean_req.tab_name_,
                        clean_req.key_,
                        clean_req.only_archives_,
                        clean_req.flush_);
    PushOperation(&clean_entry_op_);
    Process(clean_entry_op_);
}

void TransactionExecution::Process(CleanCcEntryForTestOp &clean_entry_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &clean_entry_op,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    clean_entry_op_.Reset();
    clean_entry_op_.is_running_ = true;

    cc_handler_->CleanCcEntryForTest(
        *clean_entry_op_.tab_name_,
        *clean_entry_op_.key_,
        clean_entry_op_.only_archives_,
        clean_entry_op_.flush_,
        tx_number_.load(std::memory_order_relaxed),
        tx_term_,
        command_id_.load(std::memory_order_relaxed),
        clean_entry_op_.hd_result_);
    return;
}

void TransactionExecution::PostProcess(CleanCcEntryForTestOp &clean_entry_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &clean_entry_op,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    state_stack_.pop_back();
    assert(state_stack_.empty());

    bool_resp_->Finish(clean_entry_op.succeed_);
}

void TransactionExecution::Process(AnalyzeTableAllOp &analyze_table_all_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &analyze_table_all_op,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });

    analyze_table_all_op.is_running_ = true;

    uint32_t ng_cnt = Sharder::Instance().NodeGroupCount();

    for (NodeGroupId ng_id = 0; ng_id < ng_cnt; ++ng_id)
    {
        cc_handler_->AnalyzeTableAll(
            *analyze_table_all_op.analyze_tx_req_->table_name_,
            ng_id,
            TxNumber(),
            TxTerm(),
            CommandId(),
            analyze_table_all_op.hd_result_);
    }

    StartTiming();
}

void TransactionExecution::PostProcess(AnalyzeTableAllOp &analyze_table_all_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &analyze_table_all_op,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    state_stack_.pop_back();
    assert(state_stack_.empty());

    if (analyze_table_all_op.hd_result_.IsError())
    {
        DLOG(INFO) << "AnalyzeTableAllOp FinishError for cc error: "
                   << analyze_table_all_op.hd_result_.ErrorMsg();
        void_resp_->FinishError(
            ConvertCcError(analyze_table_all_op.hd_result_.ErrorCode()));
    }
    else
    {
        DLOG(INFO) << "txm notifies analyze tx request ";
        void_resp_->Finish(void_);
    }
}

template <typename ResultType>
void TransactionExecution::Process(AsyncOp<ResultType> &ds_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &ds_op,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    ds_op.hd_result_.Reset();
    ds_op.is_running_ = true;

    if (ds_op.op_func_ != nullptr)
    {
        ds_op.op_func_();
    }
    StartTiming();
}

template void TransactionExecution::Process(AsyncOp<Void> &ds_op);
template void TransactionExecution::Process(AsyncOp<PostProcessResult> &ds_op);

template <typename ResultType>
void TransactionExecution::PostProcess(AsyncOp<ResultType> &ds_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &ds_op,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    state_stack_.pop_back();
}

template void TransactionExecution::PostProcess(AsyncOp<Void> &ds_op);
template void TransactionExecution::PostProcess(
    AsyncOp<PostProcessResult> &ds_op);

void TransactionExecution::Process(FlushDataOp &flush_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &flush_op,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    flush_op.hd_result_.Reset();
    flush_op.is_running_ = true;

    Sharder::Instance().GetLocalCcShards()->FlushData(
        *flush_op.tab_name_,
        flush_op.schema_,
        flush_op.data_sync_ts_,
        tx_term_,
        flush_op.node_group_,
        flush_op.data_sync_vec_,
        flush_op.archive_vec_,
        flush_op.mv_vec_,
        flush_op.hd_result_,
        flush_op.delay_update_ckpt_ts_);
}

void TransactionExecution::PostProcess(FlushDataOp &flush_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &ckpt_op,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });

    state_stack_.pop_back();
}

void TransactionExecution::Process(NoOp &no_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &no_op,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    no_op.is_running_ = true;
    no_op.hd_result_.SetFinished();
}

void TransactionExecution::PostProcess(NoOp &no_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &no_op,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    state_stack_.pop_back();
}

void TransactionExecution::Process(PostReadOperation &post_read_operation)
{
    post_read_operation.is_running_ = true;
    cc_handler_->PostRead(tx_number_.load(std::memory_order_relaxed),
                          this->tx_term_,
                          command_id_.load(std::memory_order_relaxed),
                          post_read_operation.read_set_entry_->version_ts_,
                          0,
                          commit_ts_,
                          *post_read_operation.cce_addr_,
                          post_read_operation.hd_result_);
}

void TransactionExecution::PostProcess(PostReadOperation &post_read_operation)
{
    rw_set_.DedupRead(*post_read_operation.cce_addr_);
    state_stack_.pop_back();
}

void TransactionExecution::Process(ReleaseScanExtraLockOp &unlock_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &unlock_op,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });

    unlock_op.is_running_ = true;

    if (drain_batch_.size() == 0)
    {
        unlock_op.hd_result_.SetFinished();
    }
    else
    {
        unlock_op.hd_result_.SetRefCnt((uint32_t) drain_batch_.size());
    }

    for (const auto &addr_pair : drain_batch_)
    {
        cc_handler_->PostRead(TxNumber(),
                              TxTerm(),
                              CommandId(),
                              addr_pair.second,
                              0,
                              commit_ts_,
                              addr_pair.first,
                              unlock_op.hd_result_);
    }

    StartTiming();
}

void TransactionExecution::PostProcess(ReleaseScanExtraLockOp &lock_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &lock_op,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });

    drain_batch_.clear();
    // The lock_op step is optional. Only pops the stack if the last step
    // is the validation step.
    if (!state_stack_.empty())
    {
        assert(state_stack_.back() == &lock_op);
        state_stack_.pop_back();
    }
}

void TransactionExecution::Process(KickoutDataOp &kickout_data_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &kickout_data_all_op,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    kickout_data_op.is_running_ = true;
    kickout_data_op.hd_result_.Reset();
    cc_handler_->KickoutData(*kickout_data_op.table_name_,
                             kickout_data_op.node_group_,
                             tx_number_.load(std::memory_order_relaxed),
                             tx_term_,
                             command_id_.load(std::memory_order_relaxed),
                             kickout_data_op.commit_ts_,
                             kickout_data_op.hd_result_,
                             CleanType::CleanForSplitRange,
                             kickout_data_op.start_key_,
                             kickout_data_op.end_key_);
}

void TransactionExecution::PostProcess(KickoutDataOp &kickout_data_all_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &kickout_data_all_op,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_))
        });
    state_stack_.pop_back();
}

ScanCloseTxRequest *TransactionExecution::NextScanCloseTxReq(
    uint64_t alias, const TableName *table_name)
{
    size_t pool_size = scan_close_req_pool_->Size();
    for (size_t idx = 0; idx < pool_size; ++idx)
    {
        std::unique_ptr<ScanCloseTxRequest> scan_close_req =
            std::move(scan_close_req_pool_->Peek());
        scan_close_req_pool_->Dequeue();

        ScanCloseTxRequest *req = scan_close_req.get();
        scan_close_req_pool_->Enqueue(std::move(scan_close_req));

        if (!req->in_use_.load(std::memory_order_relaxed))
        {
            req->Reset(alias, table_name);
            return req;
        }
    }

    std::unique_ptr<ScanCloseTxRequest> scan_close_req =
        std::make_unique<ScanCloseTxRequest>(alias, table_name);
    ScanCloseTxRequest *req = scan_close_req.get();
    scan_close_req_pool_->Enqueue(std::move(scan_close_req));

    return req;
}

void TransactionExecution::Process(ObjectCommandOp &obj_cmd_op)
{
    obj_cmd_op.is_running_ = true;
    const TxKey &key = *obj_cmd_op.key_;
    uint32_t key_shard_code = 0;

#ifdef RANGE_PARTITION_ENABLED
    if (lock_range_result_.IsFinished())
    {
        // If there is an error when getting the key's range ID, the error would
        // be caught when forwarding the operation, which forces the tx state
        // machine to move to post-processing of the operation and returns an
        // error to the ObjectCommandTxRequest.
        assert(!lock_range_result_.IsError());

        // Uses the lower 10 bits of the key's hash code to shard the
        // key across CPU cores in a cc node.
        uint32_t residual = key.Hash() & 0x3FF;
        NodeGroupId range_ng = range_rec_.GetRangeOwnerNg()->BucketOwner();
        key_shard_code = range_ng << 10 | residual;
    }
    else
    {
        obj_cmd_op.is_running_ = false;
        // First read and lock the range the key located in through
        // lock_range_op_.
        lock_range_op_.Reset();
        lock_range_result_.Reset();

        lock_range_op_.key_ = &key;
        lock_range_op_.table_name_ = TableName(
            obj_cmd_op.table_name_->StringView(), TableType::RangePartition);
        lock_range_op_.rec_ = &range_rec_;
        lock_range_op_.hd_result_ = &lock_range_result_;

        // Control flow jumps to lock_range_op_, do not execute further
        // after `Process(lock_range_op_)` returns.
        PushOperation(&lock_range_op_);
        Process(lock_range_op_);
        return;
    }
#else
    key_shard_code = Sharder::Instance().ShardCode(key.Hash());
#endif

    uint64_t current_ts =
        dynamic_cast<LocalCcHandler *>(cc_handler_)->GetTsBaseValue();

    CcHandlerResult<ObjectCommandResult> &hd_res = obj_cmd_op.hd_result_;
    hd_res.Reset();

    // Directly commit the new value to the object if autocommit and skip_wal
    // are both set, on contrary to acquiring lock and committing the command in
    // postprocess.
    bool commit = obj_cmd_op.auto_commit_ && txservice_skip_redo_log;

    if (obj_cmd_op.cmd_tx_req_->read_type_ != ReadType::Inside)
    {
        assert(cache_miss_read_cce_addr_.CcePtr() != 0);
        assert(obj_cmd_op.cmd_tx_req_->read_type_ == ReadType::OutsideDeleted ||
               obj_cmd_op.cmd_tx_req_->read_type_ == ReadType::OutsideNormal);
        // backfill
        cc_handler_->ObjectCommandOutside(
            cache_miss_read_cce_addr_,
            *obj_cmd_op.command_,
            TxNumber(),
            tx_term_,
            command_id_.load(std::memory_order_relaxed),
            current_ts,
            hd_res,
            iso_level_,
            protocol_,
            commit,
            obj_cmd_op.cmd_tx_req_->rec_,
            obj_cmd_op.cmd_tx_req_->version_,
            obj_cmd_op.cmd_tx_req_->read_type_);
    }
    else
    {
        cache_miss_read_cce_addr_.SetCce(0, -1, 0, 0);

        cc_handler_->ObjectCommand(*obj_cmd_op.table_name_,
                                   *obj_cmd_op.key_,
                                   key_shard_code,
                                   *obj_cmd_op.command_,
                                   TxNumber(),
                                   tx_term_,
                                   command_id_.load(std::memory_order_relaxed),
                                   current_ts,
                                   hd_res,
                                   iso_level_,
                                   protocol_,
                                   commit);
    }
    StartTiming();
}

void TransactionExecution::PostProcess(ObjectCommandOp &obj_cmd_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &obj_cmd_op,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    state_stack_.pop_back();
    assert(state_stack_.empty());

    const CcHandlerResult<ObjectCommandResult> &hd_result =
        obj_cmd_op.hd_result_;
    if (hd_result.IsError())
    {
        rec_resp_->FinishError(ConvertCcError(hd_result.ErrorCode()));
        rec_resp_ = nullptr;
        if (obj_cmd_op.auto_commit_)
        {
            Abort();
        }
    }
    else
    {
        const ObjectCommandResult &cmd_result = hd_result.Value();
        RecordStatus obj_status = cmd_result.rec_status_;
        LockType lock_acquired = cmd_result.lock_acquired_;
        bool cmd_success = cmd_result.cmd_success_;
        const TxCommand *cmd = obj_cmd_op.command_;
        const TableName *table_name = obj_cmd_op.table_name_;
        const CcEntryAddr &cce_addr = cmd_result.cce_addr_;
        uint64_t commit_ts = cmd_result.commit_ts_;

        // The command is directly executed and committed on the object if
        // autocommit and skip wal are both set. In such case, there is no need
        // to write log and do post write, and no need to add command into write
        // set.
        bool directly_commit =
            obj_cmd_op.auto_commit_ && txservice_skip_redo_log;

        // For autocommit read-modify-write commands, the ObjectCommandTxRequest
        // sender will be notified after auto commit succeeds, i.e. after
        // PostProcess or WritLog.

        if (lock_acquired == LockType::WriteLock)
        {
            DLOG(INFO) << "txm acquired writelock";
            // The command modifies the object. Put it into the command set
            // for writing log and post-processing. If the command fails, only
            // to release the write lock.
            rw_set_.AddObjectCommand(
                *table_name,
                cce_addr,
                commit_ts,
                obj_cmd_op.key_,
                cmd_success ? obj_cmd_op.command_ : nullptr);
        }
        else if (lock_acquired != LockType::NoLock)
        {
            // Read lock is acquired under locking protocol. Add the cce to
            // read set for later PostRead.
            DLOG(INFO) << "txm acquired readlock/intent";
            bool add_res;
            if (obj_status == RecordStatus::Unknown)
            {
                // Only used to release lock.
                add_res = rw_set_.AddRead(cce_addr, 0, table_name);
            }
            else
            {
                add_res = rw_set_.AddRead(cce_addr, commit_ts, table_name);
            }
            if (!add_res)
            {
                // Add read set fail, there is at least two unmatched read. This
                // can't be autocommit request.
                assert(!obj_cmd_op.auto_commit_);
                rec_resp_->FinishError(TxErrorCode::OCC_BREAK_REPEATABLE_READ);
                rec_resp_ = nullptr;
                return;
            }
        }

        if (obj_status == RecordStatus::Unknown)
        {
            // If obj_status == RecordStatus::Unknown, means the object is not
            // in memory, it will finish this request and rerun it soon. It will
            // be reload after refill the data read from cassandra.
            cache_miss_read_cce_addr_ = cmd_result.cce_addr_;
            rec_resp_->Finish(obj_status);
            rec_resp_ = nullptr;
            return;
        }

        // Whether we should notify the request sender.
        if (!obj_cmd_op.auto_commit_ || directly_commit || cmd->IsReadOnly())
        {
            // Not autocommit, or autocommit and skip wal, or autocommit and
            // this is a read only command. Notify the ObjectCommandTxRequest
            // sender once the command finishes.
            rec_resp_->Finish(obj_status);
            rec_resp_ = nullptr;
        }

        // Whether we should auto commit the txn. For autocommit commands that
        // need to write log, the request sender will be notified after WriteLog
        // and PostProcess.
        if (obj_cmd_op.auto_commit_)
        {
            Commit();
        }
    }
}

void TransactionExecution::Process(MultiObjectCommandOp &obj_cmd_op)
{
#ifdef RANGE_PARTITION_ENABLED
    while (obj_cmd_op.range_lock_cur_ < obj_cmd_op.vct_key_->size())
    {
        obj_cmd_op.is_running_ = false;
        lock_range_result_.Value().Reset();
        lock_range_result_.Reset();

        lock_range_op_.Reset(
            TableName(obj_cmd_op.table_name_->StringView(),
                      TableType::RangePartition),
            obj_cmd_op.vct_key_->at(obj_cmd_op.range_lock_cur_),
            &range_rec_,
            &lock_range_result_);
        PushOperation(&lock_range_op_);
        Process(lock_range_op_);
        return;
    }
#endif

    obj_cmd_op.is_running_ = true;
    uint64_t current_ts =
        dynamic_cast<LocalCcHandler *>(cc_handler_)->GetTsBaseValue();
    bool commit = obj_cmd_op.auto_commit_ && txservice_skip_redo_log;
    auto &vct_backfill = obj_cmd_op.tx_req_->vct_backfill_;

    if (!vct_backfill.empty())
    {
        for (size_t i = 0; i < vct_backfill.size(); i++)
        {
            BackfillRec &refill_rec = vct_backfill[i];
            CcHandlerResult<ObjectCommandResult> &hd_res =
                obj_cmd_op.vct_hd_result_[i];

            const TxKey &key = *obj_cmd_op.vct_key_->at(refill_rec.pos_);
            uint32_t key_shard_code = 0;

#ifdef RANGE_PARTITION_ENABLED
            uint32_t residual = key.Hash() & 0x3FF;
            key_shard_code = obj_cmd_op.vct_key_shard_code_[refill_rec.pos_]
                                 << 10 |
                             residual;
#else
            key_shard_code = Sharder::Instance().ShardCode(key.Hash());
#endif
            hd_res.Reset();

            cc_handler_->ObjectCommandOutside(
                refill_rec.ety_addr_,
                *obj_cmd_op.vct_cmd_->at(refill_rec.pos_),
                TxNumber(),
                tx_term_,
                command_id_.load(std::memory_order_relaxed),
                current_ts,
                hd_res,
                iso_level_,
                protocol_,
                commit,
                &refill_rec.rec_,
                refill_rec.version_,
                refill_rec.read_type_);
        }
    }
    else
    {
        for (size_t i = 0; i < obj_cmd_op.vct_key_->size(); i++)
        {
            auto &hd_res = obj_cmd_op.vct_hd_result_[i];

            const TxKey &key = *obj_cmd_op.vct_key_->at(i);
            uint32_t key_shard_code = 0;

#ifdef RANGE_PARTITION_ENABLED
            uint32_t residual = key.Hash() & 0x3FF;
            key_shard_code = obj_cmd_op.vct_key_shard_code_[i] << 10 | residual;
#else
            key_shard_code = Sharder::Instance().ShardCode(key.Hash());
#endif

            hd_res.Reset();
            cc_handler_->ObjectCommand(
                *obj_cmd_op.table_name_,
                key,
                key_shard_code,
                *obj_cmd_op.vct_cmd_->at(i),
                TxNumber(),
                tx_term_,
                command_id_.load(std::memory_order_relaxed),
                current_ts,
                hd_res,
                iso_level_,
                protocol_,
                commit);
        }
    }

    StartTiming();
}

void TransactionExecution::PostProcess(MultiObjectCommandOp &obj_cmd_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &obj_cmd_op,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    state_stack_.pop_back();
    assert(state_stack_.empty());

#ifdef RANGE_PARTITION_ENABLED
    if (lock_range_result_.IsError())
    {
        DLOG(ERROR) << "MultiObjectCommandOp failed when acquire range locks. "
                       "Error code: "
                    << static_cast<int>(lock_range_result_.ErrorCode());
        vct_rec_resp_->FinishError(
            ConvertCcError(lock_range_result_.ErrorCode()));
        vct_rec_resp_ = nullptr;
        return;
    }
#endif

    CcErrorCode err = obj_cmd_op.atm_err_code_.load(std::memory_order_relaxed);
    if (err != CcErrorCode::NO_ERROR)
    {
        vct_rec_resp_->FinishError(ConvertCcError(err));
        vct_rec_resp_ = nullptr;
        if (obj_cmd_op.auto_commit_)
        {
            Abort();
        }
    }
    else
    {
        // The command is directly executed and committed on the object if
        // autocommit and skip wal are both set. In such case, there is no
        // need to write log and do post write, and no need to add command
        // into write set.
        bool directly_commit =
            obj_cmd_op.auto_commit_ && txservice_skip_redo_log;
        bool readonly = obj_cmd_op.vct_cmd_->at(0)->IsReadOnly();
        std::vector<RecordStatus> vct_rec;
        auto &vct_backfill = obj_cmd_op.tx_req_->vct_backfill_;
        bool need_backfill = false;

        if (!vct_backfill.empty())
        {
            vct_rec = obj_cmd_op.tx_req_->Result();
            for (size_t i = 0; i < vct_backfill.size(); i++)
            {
                BackfillRec &refill_rec = vct_backfill[i];
                const auto &cmd_res = obj_cmd_op.vct_hd_result_[i].Value();
                vct_rec[refill_rec.pos_] = cmd_res.rec_status_;
            }
        }
        else
        {
            vct_rec.reserve(obj_cmd_op.vct_hd_result_.size());
            vct_backfill.reserve(obj_cmd_op.vct_hd_result_.size());

            for (size_t i = 0; i < obj_cmd_op.vct_hd_result_.size(); i++)
            {
                const auto &cmd_res = obj_cmd_op.vct_hd_result_[i].Value();
                RecordStatus obj_status = cmd_res.rec_status_;
                vct_rec.push_back(obj_status);
                LockType lock_acquired = cmd_res.lock_acquired_;
                bool cmd_success = cmd_res.cmd_success_;
                const TxKey *key = obj_cmd_op.vct_key_->at(i);
                const TxCommand *cmd = obj_cmd_op.vct_cmd_->at(i);

                // For autocommit read-modify-write commands, the
                // ObjectCommandTxRequest sender will be notified after auto
                // commit succeeds, i.e. after PostProcess or WritLog.

                if (lock_acquired == LockType::WriteLock)
                {
                    LOG(INFO) << "txm acquired writelock";
                    // The command modifies the object. Put it into the command
                    // set for writing log and post-processing. If the command
                    // fails, only to release the write lock.
                    rw_set_.AddObjectCommand(*obj_cmd_op.table_name_,
                                             cmd_res.cce_addr_,
                                             cmd_res.commit_ts_,
                                             key,
                                             cmd_success ? cmd : nullptr);
                }
                else if (lock_acquired != LockType::NoLock)
                {
                    LOG(INFO)
                        << "txm acquired readlock, ReadIntent or WriteIntent";
                    // Read lock is acquired under locking protocol. Add the cce
                    // to read set for later PostRead.
                    rw_set_.AddRead(cmd_res.cce_addr_,
                                    cmd_res.commit_ts_,
                                    obj_cmd_op.table_name_);
                }

                if (obj_status == RecordStatus::Unknown)
                {
                    vct_backfill.emplace_back(i, cmd_res.cce_addr_);
                    need_backfill = true;
                }
            }
        }

        if (need_backfill)
        {
            // If this request has the objects that are not in memory, it will
            // finish soon, then it will be refill data read from cassandra and
            // reload again.
            vct_rec_resp_->Finish(std::move(vct_rec));
            vct_rec_resp_ = nullptr;
            return;
        }

        if (!obj_cmd_op.auto_commit_ || directly_commit || readonly)
        {
            // Not autocommit, or autocommit and skip wal, or autocommit and
            // this is a read only command. Notify the
            // ObjectCommandTxRequest sender once the command finishes.
            vct_rec_resp_->Finish(std::move(vct_rec));
            vct_rec_resp_ = nullptr;
        }

        if (obj_cmd_op.auto_commit_)
        {
            Commit();
        }
    }
}

void TransactionExecution::Process(KickoutDataAllOp &kickout_data_all_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &kickout_data_all_op,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    uint32_t ng_cnt = Sharder::Instance().NodeGroupCount();
    size_t table_cnt = kickout_data_all_op.table_names_.size();
    kickout_data_all_op.Reset(ng_cnt, table_cnt);
    kickout_data_all_op.is_running_ = true;

    for (uint32_t ng_id = 0; ng_id < ng_cnt; ++ng_id)
    {
        for (size_t table_idx = 0; table_idx < table_cnt; ++table_idx)
        {
            cc_handler_->KickoutData(
                *kickout_data_all_op.table_names_.at(table_idx),
                ng_id,
                tx_number_.load(std::memory_order_relaxed),
                tx_term_,
                command_id_.load(std::memory_order_relaxed),
                kickout_data_all_op.commit_ts_,
                kickout_data_all_op.hd_result_,
                CleanType::CleanForAlterTable);
        }
    }

    StartTiming();
}

void TransactionExecution::PostProcess(KickoutDataAllOp &kickout_data_all_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &kickout_data_all_op,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_))
        });
    state_stack_.pop_back();
}

void TransactionExecution::ProcessTxRequest(BatchReadTxRequest &batch_read_req)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &batch_read_req,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });

    if (tx_term_ < 0)
    {
        batch_read_req.SetError(TxErrorCode::TX_INIT_FAIL);
        return;
    }

    void_resp_ = &batch_read_req.tx_result_;

    batch_read_op_.batch_read_tx_req_ = &batch_read_req;
    batch_read_op_.Reset();

    PushOperation(&batch_read_op_);
    Process(batch_read_op_);
}

void TransactionExecution::Process(BatchReadOperation &batch_read_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &batch_read_op,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });

    batch_read_op.is_running_ = true;
    const TableName &table_name = *batch_read_op.batch_read_tx_req_->tab_name_;
    std::vector<txservice::ScanBatchTuple> &read_batch =
        batch_read_op.batch_read_tx_req_->read_batch_;

    assert(batch_read_op.hd_result_vec_.size() == read_batch.size());
    if (!batch_read_op.local_cache_checked_)
    {
        for (size_t idx = 0; idx < read_batch.size(); ++idx)
        {
            const TxKey &key = *read_batch[idx].key_;
            TxRecord &rec = *read_batch[idx].record_;
            RecordStatus &rec_status = read_batch[idx].status_;

            // Step 1: fast path if key is update by the same tx.
            const WriteSetEntry *write_entry =
                rw_set_.FindWrite(table_name, key);
            if (write_entry != nullptr)
            {
                if (write_entry->op_ == OperationType::Delete)
                {
                    rec_status = RecordStatus::Deleted;
                }
                else
                {
                    rec.Copy(*write_entry->rec_.get());
                    rec_status = RecordStatus::Normal;
                }

                batch_read_op.hd_result_vec_[idx].SetFinished();
            }
            else
            {
                rec_status = RecordStatus::Unknown;
            }
        }

        batch_read_op.local_cache_checked_ = true;
        if (batch_read_op.IsFinished())
        {
            return;
        }
    }

#ifdef RANGE_PARTITION_ENABLED
    while (batch_read_op.lock_it_ < read_batch.end())
    {
        // The key is found in the write set. No need to lock its range.
        if (batch_read_op.lock_it_->status_ != RecordStatus::Unknown)
        {
            ++batch_read_op.lock_it_;
            continue;
        }

        // Lock the range of the to-be-read key.
        batch_read_op.is_running_ = false;
        lock_range_result_.Value().Reset();
        lock_range_result_.Reset();

        lock_range_op_.Reset(
            TableName(table_name.StringView(), TableType::RangePartition),
            batch_read_op.lock_it_->key_,
            &range_rec_,
            &lock_range_result_);
        PushOperation(&lock_range_op_);
        Process(lock_range_op_);
        return;
    }
#endif

    const uint64_t corresponding_sk_commit_ts =
        batch_read_op.batch_read_tx_req_->corresponding_sk_commit_ts_;
    IsolationLevel iso_level = iso_level_;
    if (batch_read_op.batch_read_tx_req_->is_for_share_ &&
        iso_level_ < IsolationLevel::RepeatableRead)
    {
        iso_level = IsolationLevel::RepeatableRead;
    }
    uint64_t read_ts = 0;
    if (iso_level == IsolationLevel::Snapshot)
    {
        read_ts = start_ts_;
    }
    else if (corresponding_sk_commit_ts != 0)
    {
        read_ts = corresponding_sk_commit_ts;
    }

    for (size_t idx = 0; idx < read_batch.size(); ++idx)
    {
        if (read_batch[idx].status_ != RecordStatus::Unknown)
        {
            continue;
        }

        const TxKey &key = *read_batch[idx].key_;
        TxRecord &rec = *read_batch[idx].record_;

        uint32_t sharding_code = 0;
        size_t key_hash = key.Hash();
#ifdef RANGE_PARTITION_ENABLED
        sharding_code =
            read_batch[idx].cce_addr_.NodeGroupId() << 10 | (key_hash & 0x3FF);
#else
        sharding_code = Sharder::Instance().ShardCode(key_hash);
#endif
        cc_handler_->Read(table_name,
                          key,
                          sharding_code,
                          rec,
                          ReadType::Inside,
                          TxNumber(),
                          tx_term_,
                          CommandId(),
                          read_ts,
                          batch_read_op.hd_result_vec_[idx],
                          iso_level,
                          protocol_,
                          batch_read_op.batch_read_tx_req_->is_for_write_);
    }

    StartTiming();
}

void TransactionExecution::PostProcess(BatchReadOperation &batch_read_op)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &read,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    state_stack_.pop_back();
    assert(state_stack_.empty());

#ifdef RANGE_PARTITION_ENABLED
    if (lock_range_result_.IsError())
    {
        DLOG(ERROR) << "BatchReadOperation failed when acquire range locks. "
                       "Error code: "
                    << (int) lock_range_result_.ErrorCode();
        void_resp_->FinishError(ConvertCcError(lock_range_result_.ErrorCode()));
        return;
    }
#endif

    const BatchReadTxRequest *read_req = batch_read_op.batch_read_tx_req_;
    const TableName *table_name = read_req->tab_name_;
    std::vector<ScanBatchTuple> &read_batch = read_req->read_batch_;
    CcErrorCode err = CcErrorCode::NO_ERROR;

    for (size_t idx = 0; idx < read_batch.size(); ++idx)
    {
        ScanBatchTuple &tuple = read_batch[idx];
        if (tuple.status_ != RecordStatus::Unknown)
        {
            // The key appears in the write set. The record has been filled.
            tuple.version_ts_ = start_ts_;
            continue;
        }

        CcHandlerResult<ReadKeyResult> &hd_res =
            batch_read_op.hd_result_vec_[idx];
        assert(hd_res.IsFinished());

        if (hd_res.IsError())
        {
            // Only returns the first error code.
            if (err == CcErrorCode::NO_ERROR)
            {
                err = hd_res.ErrorCode();
            }
        }
        else
        {
            const ReadKeyResult &read_res = hd_res.Value();
            // The record has been filled when performing the read. Only sets
            // the record status and timestamp.
            tuple.status_ = read_res.rec_status_;
            tuple.version_ts_ = read_res.ts_;

            LockType lock_type = read_res.lock_type_;
            if (lock_type != LockType::NoLock)
            {
                bool success = false;
                if (read_res.rec_status_ == RecordStatus::Unknown)
                {
                    // Only used to release lock.
                    success =
                        rw_set_.AddRead(read_res.cce_addr_, 0, table_name);
                }
                else
                {
                    success = rw_set_.AddRead(
                        read_res.cce_addr_, read_res.ts_, table_name);
                }

                if (!success && err != CcErrorCode::NO_ERROR)
                {
                    err = CcErrorCode::VALIDATION_FAILED_FOR_VERSION_MISMATCH;
                }
            }
        }
    }

    if (err == CcErrorCode::NO_ERROR)
    {
        void_resp_->Finish(void_);
    }
    else
    {
        void_resp_->FinishError(ConvertCcError(err));
    }
}

void TransactionExecution::Process(NotifyStartMigrateOp &notify_migration_op)
{
    uint32_t ng_count = Sharder::Instance().NodeGroupCount();

    notify_migration_op.Reset(ng_count);
    notify_migration_op.is_running_ = true;

    uint64_t cluster_scale_txn = TxNumber();

    assert(notify_migration_op.unfinished_req_cnt_.load(
               std::memory_order_relaxed) == 0);

    for (const auto &migrate_plan : notify_migration_op.migrate_plans_)
    {
        auto nid = migrate_plan.first;
        auto &migrate_info = migrate_plan.second;
        if (!migrate_info.has_migration_tx_)
        {
            notify_migration_op.unfinished_req_cnt_.fetch_add(
                1, std::memory_order_release);
            notify_migration_op.InitDataMigration(cluster_scale_txn, nid);
        }
    }
}

void TransactionExecution::PostProcess(
    NotifyStartMigrateOp &notify_migration_op)
{
    state_stack_.pop_back();
}

void TransactionExecution::Process(
    CheckMigrationIsFinishedOp &check_migration_is_finished_op)
{
    check_migration_is_finished_op.Reset();
    check_migration_is_finished_op.is_running_ = true;

    auto cluster_scale_txn = tx_number_.load(std::memory_order_relaxed);
    uint32_t log_group_id = txlog_->GetLogGroupId(cluster_scale_txn);

    auto &closure = check_migration_is_finished_op.closure_;
    closure.Request().Clear();
    closure.Request().set_log_group_id(log_group_id);
    closure.Request().set_cluster_scale_txn(cluster_scale_txn);
    txlog_->CheckMigrationIsFinished(log_group_id,
                                     closure.Controller(),
                                     closure.Request(),
                                     closure.Response(),
                                     closure);
}

void TransactionExecution::PostProcess(
    CheckMigrationIsFinishedOp &notify_migration_finished_op)
{
    state_stack_.pop_back();
}

LockType TransactionExecution::DeduceReadLockType(TableType tbl_type,
                                                  bool read_for_write,
                                                  IsolationLevel iso_level,
                                                  bool is_covering_key,
                                                  RecordStatus rec_status)
{
    if (rec_status == RecordStatus::Deleted && !read_for_write)
    {
        return LockType::NoLock;
    }

    CcOperation cc_op = CcOperation::Read;
    if (read_for_write)
    {
        cc_op = CcOperation::ReadForWrite;
    }
    else if (tbl_type == TableType::Secondary ||
             tbl_type == TableType::UniqueSecondary)
    {
        cc_op = CcOperation::ReadSkIndex;
    }

    return LockTypeUtil::DeduceLockType(
        cc_op, iso_level, protocol_, is_covering_key);
}

void TransactionExecution::RecoverDataMigration(
    const ::txlog::BucketMigrateMessage *migrate_msg,
    size_t cur_idx,
    std::shared_ptr<DataMigrationStatus> status)
{
    LocalCcShards *local_shards = Sharder::Instance().GetLocalCcShards();

    std::lock_guard<std::mutex> lk(local_shards->data_migration_op_pool_mux_);

    if (local_shards->migration_op_pool_.empty())
    {
        migration_op_ = std::make_unique<DataMigrationOp>(this, status);
    }
    else
    {
        migration_op_ = std::move(local_shards->migration_op_pool_.back());
        local_shards->migration_op_pool_.pop_back();
        migration_op_->Reset(this, status);
    }
    if (migrate_msg)
    {
        migration_op_->migrate_bucket_idx_ = cur_idx;
        migration_op_->bucket_key_ =
            RangeBucketKey(status->bucket_ids_[cur_idx]);

        switch (migrate_msg->stage())
        {
        case ::txlog::BucketMigrateMessage_Stage::
            BucketMigrateMessage_Stage_BeforeLocking:
        {
            migration_op_->op_ = &migration_op_->write_before_locking_log_op_;
            migration_op_->write_before_locking_log_op_.hd_result_
                .SetFinished();
            break;
        }
        case ::txlog::BucketMigrateMessage_Stage::
            BucketMigrateMessage_Stage_PrepareMigrate:
        {
            migration_op_->op_ = &migration_op_->prepare_log_op_;
            migration_op_->prepare_log_op_.hd_result_.SetFinished();
            break;
        }
        case ::txlog::BucketMigrateMessage_Stage::
            BucketMigrateMessage_Stage_CommitMigrate:
        {
            migration_op_->op_ = &migration_op_->commit_log_op_;
            migration_op_->commit_log_op_.hd_result_.SetFinished();
            break;
        }
        default:
        {
            assert(false);
        }
        }
    }

    PushOperation(migration_op_.get());
}

void TransactionExecution::RecoverClusterScale(
    const ::txlog::ClusterScaleOpMessage &scale_op_msg,
    bool dm_started,
    bool dm_finished)
{
    // Read new cluster config from log
    int ng_cnt = scale_op_msg.new_ng_configs_size();
    std::unordered_map<uint32_t, std::vector<NodeConfig>> new_ng_configs;
    std::unordered_map<uint32_t, NodeConfig> node_configs;
    for (int idx = 0; idx < scale_op_msg.node_configs_size(); idx++)
    {
        auto &node_config = scale_op_msg.node_configs(idx);
        node_configs.try_emplace(node_config.node_id(),
                                 node_config.node_id(),
                                 node_config.host_name(),
                                 node_config.port());
    }
    for (int ng_idx = 0; ng_idx < ng_cnt; ng_idx++)
    {
        int node_cnt = scale_op_msg.new_ng_configs(ng_idx).member_nodes_size();
        int ng_id = scale_op_msg.new_ng_configs(ng_idx).ng_id();
        std::vector<NodeConfig> ng_nodes;
        for (int nidx = 0; nidx < node_cnt; nidx++)
        {
            int member_nid =
                scale_op_msg.new_ng_configs(ng_idx).member_nodes(nidx);
            auto &member_node_msg = node_configs[member_nid];
            ng_nodes.emplace_back(member_node_msg.node_id_,
                                  member_node_msg.host_name_,
                                  member_node_msg.port_);
        }
        new_ng_configs.try_emplace(ng_id, std::move(ng_nodes));
    }
    LocalCcShards *local_shards = Sharder::Instance().GetLocalCcShards();
    std::unique_lock<std::mutex> lk(local_shards->cluster_scale_op_mux_);
    ClusterScaleOpType op_type =
        scale_op_msg.event_type() ==
                ::txlog::ClusterScaleOpMessage_ScaleOpType_AddNode
            ? ClusterScaleOpType::AddNode
            : ClusterScaleOpType::RemoveNode;
    if (local_shards->cluster_scale_op_)
    {
        local_shards->cluster_scale_op_->Reset(
            op_type, std::move(new_ng_configs), this);
    }
    else
    {
        local_shards->cluster_scale_op_ = std::make_unique<ClusterScaleOp>(
            op_type, std::move(new_ng_configs), this);
    }
    ClusterScaleOp *op = local_shards->cluster_scale_op_.get();

    if (op_type == ClusterScaleOpType::AddNode)
    {
        if (scale_op_msg.stage() ==
            ::txlog::ClusterScaleOpMessage_Stage_PrepareScale)
        {
            op->op_ = &op->prepare_log_op_;
            op->prepare_log_op_.hd_result_.SetFinished();
        }
        else if (scale_op_msg.stage() ==
                 ::txlog::ClusterScaleOpMessage_Stage_ConfigUpdate)
        {
            if (dm_finished)
            {
                op->op_ = &op->check_migration_is_finished_op_;
                op->check_migration_is_finished_op_.migration_is_finished_ =
                    true;
                op->check_migration_is_finished_op_.rpc_is_finished_.store(
                    true);
                op->SetStatus(
                    TxNumber(),
                    remote::ClusterScaleStatus::CLUSTER_CONFIG_UPDATE);
            }
            else if (dm_started)
            {
                op->op_ = &op->install_cluster_config_op_;
                op->install_cluster_config_op_.Reset(1);
                op->install_cluster_config_op_.hd_result_.SetFinished();
                op->SetStatus(
                    TxNumber(),
                    remote::ClusterScaleStatus::CLUSTER_CONFIG_UPDATE);
            }
            else
            {
                op->op_ = &op->update_cluster_config_log_op_;
                op->update_cluster_config_log_op_.hd_result_.SetFinished();
            }
        }
        else
        {
            assert(false);
        }
    }
    else
    {
        if (scale_op_msg.stage() ==
            ::txlog::ClusterScaleOpMessage_Stage_PrepareScale)
        {
            if (dm_finished)
            {
                op->op_ = &op->check_migration_is_finished_op_;
                op->check_migration_is_finished_op_.migration_is_finished_ =
                    true;
                op->check_migration_is_finished_op_.rpc_is_finished_.store(
                    true);
            }
            else
            {
                op->op_ = &op->prepare_log_op_;
                op->prepare_log_op_.hd_result_.SetFinished();
            }
        }
        else if (scale_op_msg.stage() ==
                 ::txlog::ClusterScaleOpMessage_Stage_ConfigUpdate)
        {
            op->op_ = &op->update_cluster_config_log_op_;
            op->update_cluster_config_log_op_.hd_result_.SetFinished();
        }
        else
        {
            assert(false);
        }
    }
    // Read migration plan from log. This is only needed if data migrate
    // is not finished.
    if (!dm_finished)
    {
        std::unordered_map<NodeGroupId, BucketMigrateInfo> migrate_plan;
        for (auto &[ng_id, ng_process] :
             scale_op_msg.node_group_bucket_migrate_process())
        {
            BucketMigrateInfo ng_plan;

            ng_plan.has_migration_tx_ =
                ng_process.stage() !=
                ::txlog::NodeGroupMigrateMessage_Stage_NotStarted;
            for (auto &[bucket, bucket_msg] :
                 ng_process.bucket_migrate_process())
            {
                ng_plan.bucket_ids_.push_back(bucket);
                ng_plan.new_owner_ngs_.push_back(bucket_msg.new_owner());
                assert(bucket_msg.bucket_id() == bucket);
                assert(bucket_msg.old_owner() == ng_id);
            }
            migrate_plan.try_emplace(ng_id, std::move(ng_plan));
        }
        op->bucket_migrate_infos_ = std::move(migrate_plan);
    }
    PushOperation(op);
}

}  // namespace txservice
