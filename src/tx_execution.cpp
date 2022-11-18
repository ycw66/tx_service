#include "tx_execution.h"

#include <bitset>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <string>

#include "cc_protocol.h"
#include "local_cc_shards.h"
#include "scan.h"
#include "sharder.h"
#include "tx_operation_result.h"
#include "tx_request.h"
#include "tx_service.h"
#include "tx_trace.h"
#include "type.h"
#include "util.h"

namespace txservice
{
TransactionExecution::TransactionExecution(CcHandler *_handler,
                                           TxLog *txlog,
                                           TxProcessor *tx_processor,
                                           CcProtocol proto)
    : handler(_handler),
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
      bool_resp_(nullptr),
      kvp_resp_(nullptr),
      uint64_resp_(nullptr),
      detailed_error_msg_(""),
      next_req_(nullptr),
      protocol_(proto),
      init_txn_(this),
      read_(this),
      scan_open_(this),
      scan_next_(this),
      acquire_write_(this),
      set_ts_(this),
      validate_(this),
      update_txn_(this),
      post_process_(this),
      write_log_(this),
      sleep_op_(this),
      fault_inject_op_(this),
      clean_entry_op_(this)
{
    TX_TRACE_ASSOCIATE(this, handler);
}

void TransactionExecution::Reset(CcProtocol proto)
{
    cache_miss_read_cce_addr_.SetCce(0, -1, 0);
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
    bool_resp_ = nullptr;
    kvp_resp_ = nullptr;
    uint64_resp_ = nullptr;
    detailed_error_msg_ = "";
    next_req_.store(nullptr);
    protocol_ = proto;
    schema_op_ = nullptr;
    ds_split_range_op_ = nullptr;
}

void TransactionExecution::Restart()
{
    tx_status_.store(TxnStatus::Ongoing, std::memory_order_relaxed);
}

bool TransactionExecution::Idle() const
{
    return state_stack_.empty();
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

std::string TransactionExecution::GetErrorMessage() const
{
    return detailed_error_msg_;
}
void TransactionExecution::SetErrorMessage(const std::string &err_msg)
{
    detailed_error_msg_ = err_msg;
}

uint32_t TransactionExecution::TxCcNodeId() const
{
    return (tx_number_.load(std::memory_order_relaxed) >> 32L) >> 10;
}

TxnStatus TransactionExecution::TxStatus() const
{
    return tx_status_.load(std::memory_order_relaxed);
}

void TransactionExecution::RecoverSchemaTx(
    const ::txlog::SchemaOpMessage &schema_op,
    uint64_t txn,
    int64_t tx_term,
    uint64_t commit_ts)
{
    tx_status_.store(TxnStatus::Recovering, std::memory_order_relaxed);
    tx_number_.store(txn, std::memory_order_relaxed);
    tx_term_ = tx_term;
    commit_ts_ = commit_ts;

    switch (schema_op.schema_op_case())
    {
    case ::txlog::SchemaOpMessage::kTableOp:
    {
        const ::txlog::UpsertTableMessage &table_msg = schema_op.table_op();

        std::unique_ptr<UpsertTableOp> table_op =
            std::make_unique<UpsertTableOp>(
                schema_op.table_name_str(),
                schema_op.old_catalog_blob(),
                schema_op.catalog_ts(),
                schema_op.new_catalog_blob(),
                static_cast<OperationType>(table_msg.op_type()),
                this,
                schema_op.alter_table_info_blob());

        if (schema_op.stage() == ::txlog::SchemaOpMessage::Stage::
                                     SchemaOpMessage_Stage_PrepareSchema)
        {
            table_op->prepare_log_op_.hd_result_.SetFinished();
            table_op->op_ = &table_op->prepare_log_op_;
        }
        else
        {
            assert(schema_op.stage() == ::txlog::SchemaOpMessage::Stage::
                                            SchemaOpMessage_Stage_CommitSchema);
            table_op->commit_log_op_.hd_result_.SetFinished();
            table_op->op_ = &table_op->commit_log_op_;
        }

        schema_op_ = std::move(table_op);
        state_stack_.push_back(schema_op_.get());
        break;
    }
    default:
        tx_status_.store(TxnStatus::Finished);
        break;
    }
}

void TransactionExecution::RecoverSplitRangeTx(
    const ::txlog::SplitRangeOpMessage &ds_split_range_op_msg,
    const TableSchema *table_schema,
    const TxKey *range_key,
    std::unique_ptr<RangeRecord> splitting_range_record,
    uint32_t partition_id,
    std::unique_ptr<TxKey> new_range_key,
    uint32_t new_partition_id,
    uint64_t txn,
    int64_t tx_term,
    uint64_t commit_ts)
{
    tx_status_.store(TxnStatus::Recovering, std::memory_order_relaxed);
    tx_number_.store(txn, std::memory_order_relaxed);
    tx_term_ = tx_term;
    commit_ts_ = commit_ts;

    const TableName range_table_name = TableName{
        ds_split_range_op_msg.table_name(), TableType::RangePartition};
    const TableName base_table_name =
        TableName{range_table_name.StringView(), TableType::Primary};

    std::unique_ptr<DsSplitRangeOp> split_range_op =
        std::make_unique<DsSplitRangeOp>(base_table_name,
                                         table_schema,
                                         range_key,
                                         std::move(splitting_range_record),
                                         this);

    const ::txlog::SplitRangeOpMessage::Stage stage =
        ds_split_range_op_msg.stage();

    if (stage != ::txlog::SplitRangeOpMessage::PrepareDirtyOldRange &&
        stage != ::txlog::SplitRangeOpMessage::CopingOldRangeData &&
        stage != ::txlog::SplitRangeOpMessage::CommitOldRangeNewRange &&
        stage != ::txlog::SplitRangeOpMessage::DeletingOldRangeData &&
        stage != ::txlog::SplitRangeOpMessage::CleanLog)
    {
        tx_status_.store(TxnStatus::Finished);
        return;
    }

    split_range_op->new_range_key_ = std::move(new_range_key);
    split_range_op->new_partition_id_ = new_partition_id;

    if (stage >= ::txlog::SplitRangeOpMessage::CopingOldRangeData)
    {
        split_range_op->upload_range_entry_->new_key_ =
            split_range_op->new_range_key_->Clone();
        split_range_op->upload_range_entry_->new_partition_id_ =
            split_range_op->new_partition_id_;
        split_range_op->upload_range_record_->range_entry_ =
            split_range_op->upload_range_entry_.get();
    }

    switch (stage)
    {
    case ::txlog::SplitRangeOpMessage::PrepareDirtyOldRange:
        split_range_op->prepare_log_for_update_old_range_op_.hd_result_
            .SetFinished();
        split_range_op->op_ =
            &split_range_op->prepare_log_for_update_old_range_op_;
        break;
    case ::txlog::SplitRangeOpMessage::CopingOldRangeData:
        split_range_op->ds_copy_old_range_data_finished_log_op_.hd_result_
            .SetFinished();
        split_range_op->op_ =
            &split_range_op->ds_copy_old_range_data_finished_log_op_;
        break;
    case ::txlog::SplitRangeOpMessage::CommitOldRangeNewRange:
        split_range_op->commit_log_for_dirty_old_range_op_.hd_result_
            .SetFinished();
        split_range_op->op_ =
            &split_range_op->commit_log_for_dirty_old_range_op_;
        break;
    case ::txlog::SplitRangeOpMessage::DeletingOldRangeData:
        split_range_op->commit_log_for_dirty_old_range_op_.hd_result_
            .SetFinished();
        split_range_op->op_ =
            &split_range_op->delete_out_of_old_range_data_log_op_;
        break;
    case ::txlog::SplitRangeOpMessage::CleanLog:
    default:
        break;
    }

    ds_split_range_op_ = std::move(split_range_op);
    state_stack_.push_back(ds_split_range_op_.get());
}

void TransactionExecution::Forward()
{
    if (state_stack_.empty())
    {
        return;
    }
    prev_op_ = state_stack_.back();
    prev_op_->Forward(this);
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
    TxnStatus status = tx_status_.load(std::memory_order_acquire);

    if (status == TxnStatus::Ongoing)
    {
        assert(next_req_.load(std::memory_order_acquire) == nullptr);
        next_req_.store(tx_req, std::memory_order_release);
        return 0;
    }
    else
    {
        // The tx has started committing/aborting or has committed/aborted. Does
        // not accept new requests.
        return 1;
    }
}

bool TransactionExecution::IsTimeOut(int wait_secs)
{
    ++state_forward_cnt_;
    if (state_forward_cnt_ == LoopCnt)
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
}

void TransactionExecution::PushOperation(TransactionOperation *op,
                                         int retry_num)
{
    command_id_++;
    state_stack_.push_back(op);
    op->retry_num_ = retry_num;
    op->is_running_ = false;
}

void TransactionExecution::ProcessTxRequest(InitTxRequest &init_txn_req)
{
    TX_TRACE_ACTION(this, &init_txn_req);
    uint64_resp_ = &init_txn_req.tx_result_;
    uint64_resp_->Reset();
    iso_level_ = init_txn_req.iso_level_;
    protocol_ = init_txn_req.protocol_;

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
    rec_resp_ = &read_req.tx_result_;
    rec_resp_->Reset();

    read_.read_type_ = ReadType::Inside;
    read_.read_tx_req_ = &read_req;
    read_.hd_result_.Value().Reset();
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
    rec_resp_->Reset();

    read_.read_type_ = read_outside_req.is_deleted_ ? ReadType::OutsideDeleted
                                                    : ReadType::OutsideNormal;
    read_.read_outside_tx_req_ = &read_outside_req;
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

    uint64_resp_ = &scan_open_req.tx_result_;
    uint64_resp_->Reset();
    scan_open_.tx_req_ = &scan_open_req;
    PushOperation(&scan_open_);
    Process(scan_open_);
}

void TransactionExecution::ProcessTxRequest(ScanNextTxRequest &scan_next_req)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &scan_next_req,
        [this]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(this->TxNumber()))
                .append("\"tx_term\":")
                .append(std::to_string(this->tx_term_));
        });
    kvp_resp_ = &scan_next_req.tx_result_;
    kvp_resp_->Reset();

    scan_next_.tx_req_ = &scan_next_req;
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
    void_resp_ = &scan_close_req.tx_result_;
    void_resp_->Reset();

    ScanClose(scan_close_req.alias_,
              *scan_close_req.end_key_,
              scan_close_req.table_name_);
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
    void_resp_->Reset();
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
    bool_resp_->Reset();
    Commit();
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
    bool_resp_->Reset();
    // When the tx is aborted/rolled back by the user, write intentions must
    // have not acquired. Clear the write set before entering post-processing.
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
    bool_resp_ = &req.tx_result_;

    schema_op_ = std::make_unique<UpsertTableOp>(req.table_name_->StringView(),
                                                 *req.curr_image_,
                                                 req.curr_schema_ts_,
                                                 *req.dirty_image_,
                                                 req.op_type_,
                                                 this,
                                                 req.alter_table_info_image_);
    PushOperation(schema_op_.get());
    Forward();
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
    bool_resp_->Reset();

    fault_inject_op_.Set(
        fi_req.fault_name_, fi_req.fault_paras_, fi_req.vct_node_id_);
    PushOperation(&fault_inject_op_);
    Process(fault_inject_op_);
}

void TransactionExecution::ProcessTxRequest(SplitRangeTxRequest &req)
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
    bool_resp_->Reset();

    DLOG(INFO) << "SplitRangeTxRequest table_name_: "
               << req.table_name_.String();
    ds_split_range_op_ =
        std::make_unique<DsSplitRangeOp>(req.table_name_,
                                         req.table_schema_,
                                         req.range_key_,
                                         std::move(req.range_record_),
                                         this);
    // Fire and forget the SplitRangeTxRequest
    bool_resp_->Finish(true);
    bool_resp_ = nullptr;

    PushOperation(ds_split_range_op_.get());
    Forward();
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
    init_txn.is_running_ = true;
    commit_ts_ = 0;
    commit_ts_bound_ = 0;

    init_txn.Reset();

    handler->NewTxn(init_txn.hd_result_, iso_level_);
    init_txn.Forward(this);
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
        state_stack_.clear();
        uint64_resp_->FinishError();
        // transaction can be recycled and put into free list.
        tx_status_.store(TxnStatus::Finished, std::memory_order_release);
        Reset();
        return;
    }

    const InitTxResult &init_result = init_txn.hd_result_.Value();
    txid_ = init_result.txid_;
    tx_number_.store(txid_.TxNumber(), std::memory_order_release);
    start_ts_ = init_result.start_ts_;
    commit_ts_bound_ = init_result.start_ts_ + 1;
    tx_term_ = init_result.term_;
    state_stack_.pop_back();
    uint64_resp_->Finish(tx_number_.load(std::memory_order_acquire));
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
    read.Reset();
    read.is_running_ = true;
    if (read.read_type_ == ReadType::Inside)
    {
        const TableName &table_name = *read.read_tx_req_->tab_name_;
        const TxKey &key = *read.read_tx_req_->key_;
        TxRecord &rec = *read.read_tx_req_->rec_;
        const uint64_t corresponding_sk_commit_ts =
            read.read_tx_req_->corresponding_sk_commit_ts_;

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

            handler->ReadLocal(table_name,
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
                               read.read_tx_req_->is_for_write_);
        }
        else
        {
            // Step 1: fast path if key is update by the same tx.
            const WriteSetEntry *write = rw_set_.FindWrite(table_name, key);
            if (write != nullptr)
            {
                if (write->op_ == OperationType::Delete)
                {
                    state_stack_.pop_back();
                    assert(state_stack_.empty());
                    rec_resp_->Finish(RecordStatus::Deleted);
                }
                else
                {
                    rec.Copy(*write->rec_.get());
                    state_stack_.pop_back();
                    assert(state_stack_.empty());
                    rec_resp_->Finish(RecordStatus::Normal);
                }
                return;
            }

            // Step 2: fast path if key is the same as last read key.
            const TxRecord *cache_rec = rw_set_.FindCacheRead(table_name, key);
            if (cache_rec != nullptr)
            {
                rec.Copy(*cache_rec);
                state_stack_.pop_back();
                assert(state_stack_.empty());
                rec_resp_->Finish(RecordStatus::Normal);
                return;
            }

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
            else if (corresponding_sk_commit_ts != 0)
            {
                read_ts = corresponding_sk_commit_ts;
            }

            handler->Read(table_name,
                          key,
                          rec,
                          read.read_type_,
                          tx_number_.load(std::memory_order_relaxed),
                          tx_term_,
                          command_id_.load(std::memory_order_relaxed),
                          read_ts,
                          read.hd_result_,
                          read.iso_level_,
                          read.protocol_,
                          read.read_tx_req_->is_for_write_);

            StartTiming();

            return;
        }
    }
    else
    {
        TxRecord &record = read.read_outside_tx_req_->rec_;
        bool is_deleted = read.read_outside_tx_req_->is_deleted_;

        rw_set_.UpdateRead(cache_miss_read_cce_addr_,
                           read.read_outside_tx_req_->commit_ts_);
        handler->ReadOutside(tx_term_,
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
        rec_resp_->FinishError();
    }
    else
    {
        const ReadKeyResult &read_res = read_.hd_result_.Value();
        const ReadTxRequest *read_req = read.read_tx_req_;

        // optimization for case that we read the same key continuously
        // especially speed up remote read. e.g. Read A, Write B, Read A.
        if (read_res.rec_status_ == RecordStatus::Normal)
        {
            rw_set_.AddCacheRead(
                *read_req->tab_name_, *read_req->key_, *read_req->rec_);
        }

        if (read_.read_type_ == ReadType::Inside)
        {
            const TableName *table_name = read_req->tab_name_;
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
                    add_res = rw_set_.AddRead(read_res.cce_addr_,
                                              0,
                                              read_.protocol_,
                                              read_res.lock_type_,
                                              table_name);
                }
                else
                {
                    add_res = rw_set_.AddRead(read_res.cce_addr_,
                                              read_res.ts_,
                                              read_.protocol_,
                                              read_res.lock_type_,
                                              table_name);
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
                    rec_resp_->FinishError(
                        TxErrorCode::OCC_BREAK_REPEATABLE_READ);
                    return;
                }
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
            cache_miss_read_cce_addr_.SetCce(0, -1, 0);
        }

        rec_resp_->Finish(read_res.rec_status_);
    }
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
    const TxKey &start_key = *scan_open.tx_req_->start_key_;
    bool inclusive = scan_open.tx_req_->inclusive_;
    ScanDirection direction = scan_open.tx_req_->direct_;
    bool is_ckpt_delta = scan_open.tx_req_->is_ckpt_delta_;
    bool is_for_write = scan_open.tx_req_->is_for_write_;
    bool is_for_share = scan_open.tx_req_->is_for_share_;

    scan_open.Reset();
    scan_open.is_running_ = true;
    scan_open.Set(&table_name,
                  index_type,
                  &start_key,
                  inclusive,
                  direction,
                  is_ckpt_delta);

    if (scan_open.tx_req_->read_local_)
    {
        handler->ScanOpenLocal(table_name,
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

        handler->ScanOpen(table_name,
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
                          is_ckpt_delta);
    }

    StartTiming();
    return;
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

    if (scan_open_.hd_result_.IsError())
    {
        if (scan_open_.hd_result_.Value().scanner_ != nullptr)
        {
            const TableName &table_name = *scan_open_.tx_req_->tab_name_;
            CcScanner &scanner = *(scan_open_.hd_result_.Value().scanner_);

            // Add remaining ScanTuple into rset, so their lock can be released
            // when transaction been committed
            DrainOutScanCache(table_name, scanner);
        }
        uint64_resp_->FinishError();
        return;
    }

    ScanOpenResult &open_result = scan_open_.hd_result_.Value();

    auto table_iter = rw_set_.WriteSet().find(*scan_open_.table_name_);
    if (table_iter != rw_set_.WriteSet().end())
    {
        if (scan_open_.direction_ == ScanDirection::Forward)
        {
            auto wset_it = rw_set_.InitIter(table_iter->second,
                                            scan_open_.start_key_,
                                            scan_open_.inclusive_);
            if (wset_it.first != wset_it.second)
            {
                wset_iters_.emplace(open_result.scan_alias_, wset_it);
            }
        }
        else
        {
            auto wset_rit = rw_set_.InitReverseIter(table_iter->second,
                                                    scan_open_.start_key_,
                                                    scan_open_.inclusive_);
            if (wset_rit.first != wset_rit.second)
            {
                wset_reverse_iters_.emplace(open_result.scan_alias_, wset_rit);
            }
        }
    }

    auto em_it = scans_.emplace(open_result.scan_alias_,
                                std::move(open_result.scanner_));
    assert(em_it.second == true);

    uint64_resp_->Finish(open_result.scan_alias_);
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
    size_t alias = scan_next.tx_req_->alias_;

    auto it = scans_.find(alias);
    assert(it != scans_.end());
    CcScanner &scanner = *it->second;

    scan_next.Reset();
    scan_next.is_running_ = true;
    scan_next.Set(alias, &scanner);
    const ScanTuple *scan_tuple = scanner.Current();

    if (scan_tuple == nullptr && scanner.Status() == ScannerStatus::Blocked)
    {
        if (scanner.read_local_)
        {
            handler->ScanNextBatchLocal(
                tx_number_.load(std::memory_order_relaxed),
                tx_term_,
                command_id_.load(std::memory_order_relaxed),
                start_ts_,
                scanner,
                scan_next.hd_result_);
        }
        else
        {
            handler->ScanNextBatch(tx_number_.load(std::memory_order_relaxed),
                                   tx_term_,
                                   command_id_.load(std::memory_order_relaxed),
                                   start_ts_,
                                   scanner,
                                   scan_next.hd_result_);
        }
    }
    else
    {
        scan_next.hd_result_.SetFinished();
    }

    StartTiming();

    // Scanning next is only blocked when one of the shards' cache is drained.
    // Invokes Forward() to move forward the tx machine.
    scan_next.Forward(this);

    return;
}

void TransactionExecution::PostProcess(ScanNextOperation &scan_next)
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
    prev_op_ = state_stack_.back();
    state_stack_.pop_back();
    assert(state_stack_.empty());

    if (scan_next.hd_result_.IsError())
    {
        kvp_resp_->FinishError();
        return;
    }

    const TableName &table_name = scan_next.tx_req_->table_name_;
    const ScanTuple *cc_scan_tuple = scan_next.scanner_->Current();
    // (cc_scan_tuple->key_ts_ == 0) means it is a boundary key but outside the
    // query scope.
    // (cc_scan_tuple->rec_status_ == RecordStatus::Unknown) means
    // it is a backfilling entry and thus data store already contains this
    // entry. Since the final scan result is the merge of memory entries with
    // data store entries, as a result it's safe to skip these backfill entries.
    while (cc_scan_tuple != nullptr &&
           (cc_scan_tuple->key_ts_ == 0 ||
            cc_scan_tuple->rec_status_ == RecordStatus::Unknown))
    {
        // Lock need to be released when transaction be committed.
        LockType scan_tuple_lock_type =
            scan_next.scanner_->DeduceScanTupleLockType(cc_scan_tuple);
        // "key_ts_ == 0", means the lock is added on gap. Now, gap lock is not
        // used when do scan operation.
        if (scan_tuple_lock_type != LockType::NoLock &&
            cc_scan_tuple->key_ts_ != 0)
        {
            DLOG_IF(INFO, TRACE_OCC_ERR)
                << "Before AddRead, txn: " << tx_number_ << " ,cce:" << std::hex
                << cc_scan_tuple->cce_addr_.CcePtr() << " ,ts: " << std::dec
                << cc_scan_tuple->key_ts_ << " ,rec_status: "
                << static_cast<int>(cc_scan_tuple->rec_status_)
                << " ,lock: " << static_cast<int>(scan_tuple_lock_type)
                << " ,table: " << table_name.String();

            bool add_res;
            if (cc_scan_tuple->rec_status_ == RecordStatus::Unknown)
            {
                // Only used to release lock.
                add_res = rw_set_.AddRead(cc_scan_tuple->cce_addr_,
                                          0,
                                          scan_next.scanner_->protocol_,
                                          scan_tuple_lock_type,
                                          &table_name);
            }
            else
            {
                add_res = rw_set_.AddRead(cc_scan_tuple->cce_addr_,
                                          cc_scan_tuple->key_ts_,
                                          scan_next.scanner_->protocol_,
                                          scan_tuple_lock_type,
                                          &table_name);
            }
            if (!add_res)
            {
                DLOG_IF(INFO, TRACE_OCC_ERR)
                    << "AddRead, occ_err ,txn: " << tx_number_
                    << " ,cce:" << std::hex << cc_scan_tuple->cce_addr_.CcePtr()
                    << " ,ts: " << std::dec << cc_scan_tuple->key_ts_
                    << " ,rec_status: "
                    << static_cast<int>(cc_scan_tuple->rec_status_)
                    << " ,lock: " << static_cast<int>(scan_tuple_lock_type)
                    << " ,table: " << table_name.String();
                kvp_resp_->FinishError(TxErrorCode::OCC_BREAK_REPEATABLE_READ);
                return;
            }
        }
        scan_next.scanner_->MoveNext();
        cc_scan_tuple = scan_next.scanner_->Current();

        if (cc_scan_tuple == nullptr &&
            scan_next.scanner_->Status() == ScannerStatus::Blocked)
        {
            scan_next.hd_result_.Reset();
            handler->ScanNextBatch(tx_number_.load(std::memory_order_relaxed),
                                   tx_term_,
                                   command_id_.load(std::memory_order_relaxed),
                                   start_ts_,
                                   *scan_next.scanner_,
                                   scan_next.hd_result_);
            // put scannext_op into state stack since we need to scan the ccmap
            // again. Note that we should not call PushOperation() since we have
            // already triggerred the ScanNextBatch.
            state_stack_.push_back(prev_op_);
            return;
        }
    }

    assert(cc_scan_tuple != nullptr ||
           scan_next.scanner_->Status() == ScannerStatus::Closed);

    // Lock need to be released when transaction be committed, so add scan
    // result into transaction read set
    LockType scan_tuple_lock_type =
        scan_next.scanner_->DeduceScanTupleLockType(cc_scan_tuple);
    if (cc_scan_tuple != nullptr && scan_tuple_lock_type != LockType::NoLock)
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
                        .append(
                            std::to_string(cc_scan_tuple->cce_addr_.CcePtr()));
                }));

        DLOG_IF(INFO, TRACE_OCC_ERR)
            << "Before AddRead ,txn: " << tx_number_ << " ,cce:" << std::hex
            << cc_scan_tuple->cce_addr_.CcePtr() << " ,ts: " << std::dec
            << cc_scan_tuple->key_ts_
            << " ,rec_status: " << static_cast<int>(cc_scan_tuple->rec_status_)
            << " ,lock: " << static_cast<int>(scan_tuple_lock_type)
            << " ,table: " << table_name.String();

        bool add_res;
        if (cc_scan_tuple->rec_status_ == RecordStatus::Unknown)
        {
            // Only used to release lock.
            add_res = rw_set_.AddRead(cc_scan_tuple->cce_addr_,
                                      0,
                                      scan_next.scanner_->protocol_,
                                      scan_tuple_lock_type,
                                      &table_name);
        }
        else
        {
            add_res = rw_set_.AddRead(cc_scan_tuple->cce_addr_,
                                      cc_scan_tuple->key_ts_,
                                      scan_next.scanner_->protocol_,
                                      scan_tuple_lock_type,
                                      &table_name);
        }
        if (!add_res)
        {
            DLOG_IF(INFO, TRACE_OCC_ERR)
                << "AddRead, occ_err ,txn: " << tx_number_
                << " ,cce:" << std::hex << cc_scan_tuple->cce_addr_.CcePtr()
                << " ,ts: " << std::dec << cc_scan_tuple->key_ts_
                << " ,rec_status: "
                << static_cast<int>(cc_scan_tuple->rec_status_)
                << " ,lock: " << static_cast<int>(scan_tuple_lock_type)
                << " ,table: " << table_name.String();
            kvp_resp_->FinishError(TxErrorCode::OCC_BREAK_REPEATABLE_READ);
            return;
        }
    }

    if (scan_next.scanner_->Direction() == ScanDirection::Forward)
    {
        auto it = wset_iters_.find(scan_next.alias_);

        // Case that all the entries in the local write set have been scanned.
        if (it == wset_iters_.end() || it->second.first == it->second.second)
        {
            if (cc_scan_tuple != nullptr)
            {
                if (cc_scan_tuple->rec_status_ == RecordStatus::Normal)
                {
                    kvp_resp_->Finish(std::make_tuple(cc_scan_tuple->Key(),
                                                      cc_scan_tuple->Record(),
                                                      RecordStatus::Normal,
                                                      cc_scan_tuple->key_ts_));
                }
                else
                {
                    assert(cc_scan_tuple->rec_status_ ==
                               RecordStatus::Deleted ||
                           cc_scan_tuple->rec_status_ ==
                               RecordStatus::VersionUnknown);
                    if (scan_next.scanner_->is_ckpt_delta_)
                    {
                        kvp_resp_->Finish(
                            std::make_tuple(cc_scan_tuple->Key(),
                                            cc_scan_tuple->Record(),
                                            cc_scan_tuple->rec_status_,
                                            cc_scan_tuple->key_ts_));
                    }
                    else
                    {
                        kvp_resp_->Finish(
                            std::make_tuple(cc_scan_tuple->Key(),
                                            nullptr,
                                            cc_scan_tuple->rec_status_,
                                            cc_scan_tuple->key_ts_));
                    }
                }

                scan_next.scanner_->MoveNext();
            }
            else
            {
                kvp_resp_->Finish(std::make_tuple(
                    nullptr, nullptr, RecordStatus::Deleted, 0));
            }
            return;
        }

        const WriteSetEntry &local_write = it->second.first->second;

        // Case that needs to merge ccm entries with local write set entries.
        if (cc_scan_tuple == nullptr ||
            *local_write.key_ < *cc_scan_tuple->Key())
        {
            // Returns the key-value pair in the local write set.
            if (local_write.op_ == OperationType::Delete)
            {
                kvp_resp_->Finish(std::make_tuple(
                    local_write.key_.get(), nullptr, RecordStatus::Deleted, 0));
            }
            else
            {
                kvp_resp_->Finish(std::make_tuple(local_write.key_.get(),
                                                  local_write.rec_.get(),
                                                  RecordStatus::Normal,
                                                  0));
            }

            ++it->second.first;
        }
        else if (*cc_scan_tuple->Key() < *local_write.key_.get())
        {
            /*ScanSetEntry &scan_entry = rw_set_.NewScanEntry(
                    *scan_next_.table_name_, scan_result->key_);

            scan_entry.gap_ts_ = scan_result->gap_ts_;
            scan_entry.cce_addr_ = scan_result->cce_addr_;
            ++sset_post_cnt_;*/

            if (cc_scan_tuple->rec_status_ == RecordStatus::Normal)
            {
                kvp_resp_->Finish(std::make_tuple(cc_scan_tuple->Key(),
                                                  cc_scan_tuple->Record(),
                                                  RecordStatus::Normal,
                                                  cc_scan_tuple->key_ts_));
            }
            else
            {
                assert(cc_scan_tuple->rec_status_ == RecordStatus::Deleted);
                kvp_resp_->Finish(std::make_tuple(cc_scan_tuple->Key(),
                                                  nullptr,
                                                  cc_scan_tuple->rec_status_,
                                                  cc_scan_tuple->key_ts_));
            }
            scan_next.scanner_->MoveNext();
        }
        else if (*cc_scan_tuple->Key() == *local_write.key_.get())
        {
            // Returns the key-value pair in the local write set.
            if (local_write.op_ == OperationType::Delete)
            {
                kvp_resp_->Finish(std::make_tuple(
                    local_write.key_.get(), nullptr, RecordStatus::Deleted, 0));
            }
            else
            {
                kvp_resp_->Finish(std::make_tuple(local_write.key_.get(),
                                                  local_write.rec_.get(),
                                                  RecordStatus::Normal,
                                                  0));
            }

            ++it->second.first;
            scan_next.scanner_->MoveNext();
        }
    }
    // backward scan
    else
    {
        auto rit = wset_reverse_iters_.find(scan_next.alias_);

        if (rit == wset_reverse_iters_.end() ||
            rit->second.first == rit->second.second)
        {
            if (cc_scan_tuple != nullptr)
            {
                if (cc_scan_tuple->rec_status_ == RecordStatus::Normal)
                {
                    kvp_resp_->Finish(std::make_tuple(cc_scan_tuple->Key(),
                                                      cc_scan_tuple->Record(),
                                                      RecordStatus::Normal,
                                                      cc_scan_tuple->key_ts_));
                }
                else
                {
                    assert(cc_scan_tuple->rec_status_ == RecordStatus::Deleted);
                    kvp_resp_->Finish(
                        std::make_tuple(cc_scan_tuple->Key(),
                                        nullptr,
                                        cc_scan_tuple->rec_status_,
                                        cc_scan_tuple->key_ts_));
                }

                scan_next.scanner_->MoveNext();
            }
            else
            {
                kvp_resp_->Finish(std::make_tuple(
                    nullptr, nullptr, RecordStatus::Deleted, 0));
            }
            return;
        }

        const WriteSetEntry &local_write = rit->second.first->second;

        if (cc_scan_tuple == nullptr ||
            *cc_scan_tuple->Key() < *local_write.key_.get())
        {
            // Returns the key-value pair in the local write set.
            if (local_write.op_ == OperationType::Delete)
            {
                kvp_resp_->Finish(std::make_tuple(
                    local_write.key_.get(), nullptr, RecordStatus::Deleted, 0));
            }
            else
            {
                kvp_resp_->Finish(std::make_tuple(local_write.key_.get(),
                                                  local_write.rec_.get(),
                                                  RecordStatus::Normal,
                                                  0));
            }

            ++rit->second.first;
        }
        else if (*local_write.key_.get() < *cc_scan_tuple->Key())
        {
            /*ScanSetEntry &scan_entry = rw_set_.NewScanEntry(
                    *scan_next_.table_name_, scan_result->key_);

            scan_entry.gap_ts_ = scan_result->gap_ts_;
            scan_entry.cce_addr_ = scan_result->cce_addr_;
            ++sset_post_cnt_;*/

            if (cc_scan_tuple->rec_status_ == RecordStatus::Normal)
            {
                kvp_resp_->Finish(std::make_tuple(cc_scan_tuple->Key(),
                                                  cc_scan_tuple->Record(),
                                                  RecordStatus::Normal,
                                                  cc_scan_tuple->key_ts_));
            }
            else
            {
                assert(cc_scan_tuple->rec_status_ == RecordStatus::Deleted);
                kvp_resp_->Finish(std::make_tuple(cc_scan_tuple->Key(),
                                                  nullptr,
                                                  cc_scan_tuple->rec_status_,
                                                  cc_scan_tuple->key_ts_));
            }

            scan_next.scanner_->MoveNext();
        }
        else if (*cc_scan_tuple->Key() == *local_write.key_.get())
        {
            // Returns the key-value pair in the local write set.
            if (local_write.op_ == OperationType::Delete)
            {
                kvp_resp_->Finish(std::make_tuple(
                    local_write.key_.get(), nullptr, RecordStatus::Deleted, 0));
            }
            else
            {
                kvp_resp_->Finish(std::make_tuple(local_write.key_.get(),
                                                  local_write.rec_.get(),
                                                  RecordStatus::Normal,
                                                  0));
            }

            ++rit->second.first;
            scan_next.scanner_->MoveNext();
        }
    }
}

void TransactionExecution::ScanClose(size_t alias,
                                     const TxKey &end_key,
                                     const TableName &table_name)
{
    auto scan_it = scans_.find(alias);
    if (scan_it == scans_.end())
    {
        void_resp_->Finish(void_);
        return;
    }

    assert(scan_it != scans_.end());
    CcScanner &scanner = *scan_it->second;

    // Add remaining ScanTuple into rset, so their lock can be released when
    // transaction been committed
    DrainOutScanCache(table_name, scanner);

    handler->ScanClose(alias, end_key, false);
    scans_.erase(scan_it);
    void_resp_->Finish(void_);
}

// drain out scan cache and move ScanTuple into readset
void TransactionExecution::DrainOutScanCache(const TableName &table_name,
                                             CcScanner &scanner)
{
    // Add remaining ScanTuple into rset, so their lock can be released when
    // transaction been committed

    // drain out the scan tuple in the scan cache
    scanner.SetDrainCacheMode(true);
    const ScanTuple *cc_scan_tuple = scanner.Current();
    // In case the scan status is blocked before
    if (cc_scan_tuple == nullptr && scanner.Status() == ScannerStatus::Blocked)
    {
        scanner.MoveNext();
        cc_scan_tuple = scanner.Current();
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
            scanner.DeduceScanTupleLockType(cc_scan_tuple);
        // "key_ts_ == 0", means the lock is added on gap. Now, gap lock is not
        // used when do scan operation.
        if (scan_tuple_lock_type != LockType::NoLock &&
            cc_scan_tuple->key_ts_ != 0)
        {
            bool add_res;
            if (cc_scan_tuple->rec_status_ == RecordStatus::Unknown)
            {
                // Only used to release lock.
                add_res = rw_set_.AddRead(cc_scan_tuple->cce_addr_,
                                          0,
                                          scanner.protocol_,
                                          scan_tuple_lock_type,
                                          &table_name);
            }
            else
            {
                add_res = rw_set_.AddRead(cc_scan_tuple->cce_addr_,
                                          cc_scan_tuple->key_ts_,
                                          scanner.protocol_,
                                          scan_tuple_lock_type,
                                          &table_name);
            }
            if (!add_res)
            {
                continue;
            }
        }
        scanner.MoveNext();
        cc_scan_tuple = scanner.Current();
        scan_tuple_lock_type = scanner.DeduceScanTupleLockType(cc_scan_tuple);
    }
}

void TransactionExecution::Update(const TableName &table_name,
                                  TxKey::Uptr key,
                                  TxRecord::Uptr rec)
{
    Upsert(table_name, std::move(key), std::move(rec), OperationType::Update);
}

void TransactionExecution::Insert(const TableName &table_name,
                                  TxKey::Uptr key,
                                  TxRecord::Uptr rec)
{
    Upsert(table_name, std::move(key), std::move(rec), OperationType::Insert);
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
    tx_status_.store(TxnStatus::Committing, std::memory_order_release);

    if (rw_set_.WriteSetSize() > 0)
    {
        PushOperation(&acquire_write_);
        Process(acquire_write_);
    }
    else
    {
        PushOperation(&set_ts_);
        Process(set_ts_);
    }

    return;
}

void TransactionExecution::Abort()
{
    tx_status_.store(TxnStatus::Aborted, std::memory_order_release);
    PushOperation(&update_txn_);
    Process(update_txn_);
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
    size_t wset_size = rw_set_.WriteSetSize();
    acquire_write.Reset(wset_size);
    acquire_write.is_running_ = true;

    uint64_t current_ts =
        static_cast<LocalCcHandler *>(handler)->GetTsBaseValue();

    size_t idx = 0;
    std::unordered_map<TableName, TableWriteSet> &wset = rw_set_.WriteSet();
    for (auto &[table_name, table_write_set] : wset)
    {
        for (auto &[key_ptr, write_entry] : table_write_set)
        {
            acquire_write.acquire_write_entries_[idx] = &write_entry;

            // TODO: enable is_insert after Serializable Isolation is supported.
            handler->AcquireWrite(table_name,
                                  *write_entry.key_,
                                  TxNumber(),
                                  tx_term_,
                                  command_id_.load(std::memory_order_relaxed),
                                  current_ts,
                                  false,
                                  acquire_write.hd_result_,
                                  idx,
                                  protocol_,
                                  iso_level_);
            ++idx;
        }
    }

    StartTiming();
}

void TransactionExecution::PostProcess(AcquireWriteOperation &acquire_write)
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
    state_stack_.pop_back();
    assert(state_stack_.empty());
    if (acquire_write.hd_result_.IsError() || acquire_write.rset_has_expired_)
    {
        bool_resp_->SetErrorCode(TxErrorCode::WRITE_WRITE_CONFLICT);
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

    set_ts.is_running_ = true;

    for (const AcquireKeyResult &acquire_key :
         acquire_write_.hd_result_.Value())
    {
        candidate = std::max(candidate, acquire_key.last_vali_ts_ + 1);
        candidate = std::max(candidate, acquire_key.commit_ts_ + 1);
    }

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
    handler->SetCommitTimestamp(txid_, candidate, set_ts.hd_result_);
    set_ts.Forward(this);
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
        SetErrorMessage("Transaction abort: failed to set commit timestamp.");
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
            if (txlog_ != nullptr && rw_set_.WriteSetSize() > 0)
            {
                FillDataLogRequest(write_log_);
                PushOperation(&write_log_);
                Process(write_log_);
            }
            else
            {
                tx_status_.store(TxnStatus::Committed,
                                 std::memory_order_release);
                PushOperation(&update_txn_);
                Process(update_txn_);
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

    validate.Reset(rw_set_.ReadSetSize());
    validate.is_running_ = true;

    for (const auto &table_entry_it : rset)
    {
        for (const auto &[cce_addr, read_entry] : table_entry_it.second)
        {
            handler->PostRead(tx_number_.load(std::memory_order_relaxed),
                              tx_term_,
                              command_id_.load(std::memory_order_relaxed),
                              read_entry.version_ts_,
                              0,
                              commit_ts_,
                              cce_addr,
                              validate.hd_result_,
                              read_entry.protocol_,
                              read_entry.lock_type_);
        }
    }

    StartTiming();
}

void TransactionExecution::PostProcess(ValidateOperation &validate)
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
    // The validation step is optional. Only pops the stack if the last step is
    // the validation step.
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
        bool_resp_->SetErrorCode(TxErrorCode::OCC_BREAK_REPEATABLE_READ);
        Abort();
    }
    else if (txlog_ != nullptr && rw_set_.WriteSetSize() > 0)
    {
        FillDataLogRequest(write_log_);
        PushOperation(&write_log_);
        Process(write_log_);
    }
    else
    {
        tx_status_.store(TxnStatus::Committed, std::memory_order_release);
        PushOperation(&update_txn_);
        Process(update_txn_);
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
    log_rec->set_txn_number(txid_.TxNumber());
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
            uint32_t cc_node_id;

            if (table_name.Type() == TableType::Secondary)
            {
                uint32_t shard_code =
                    Sharder::Instance().ShardCode(key_ptr->Hash());
                cc_node_id = Sharder::Instance().LeaderNodeId(shard_code >> 10);
            }
            else
            {
                const CcEntryAddr &addr = wset_entry.cce_addr_;
                cc_node_id = addr.NodeGroupId();

                // Only fills WriteLogRequest::node_terms for base table.
                auto shard_term_it = shard_terms->find(cc_node_id);
                if (shard_term_it == shard_terms->end())
                {
                    (*shard_terms)[cc_node_id] = addr.Term();
                }
                else if (shard_term_it->second != addr.Term())
                {
                    // Two keys in the tx's write set refer to the same cc node
                    // group, but have different terms. It means that the cc
                    // node must have failed over at least once and the tx have
                    // obtained a write intention before the failure. The tx
                    // must abort because the write intention obtained before
                    // the failure have been invalidated.
                    write_log.hd_result_.SetError(1);
                    return;
                }
            }

            auto table_rec_it = ng_table_rec_set.try_emplace(cc_node_id);
            std::unordered_map<TableName, std::vector<const WriteSetEntry *>>
                &table_rec_set = table_rec_it.first->second;

            auto rec_vec_it = table_rec_set.emplace(
                std::piecewise_construct,
                std::forward_as_tuple(table_name.StringView(),
                                      table_name.Type()),
                std::forward_as_tuple(std::vector<const WriteSetEntry *>()));

            rec_vec_it.first->second.emplace_back(&wset_entry);
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

        // The log blob of a table in a node group is in the following format:
        // (1) A 1-byte integer for the length of the table name, followed by
        // (2) The string of the table name.
        // (3) A 1-byte integer for the type of table.
        // (4) A 4-byte integer for the total length of serialized key-record
        // pairs modified by the tx in the node group.
        // (5) A sequence of modified records. Each record is encoded as
        // follows:
        //   (a) The serialized key
        //   (b) A 1-byte flag to indicate if the record is normal, deleted or
        //   void.
        //   (c) The serialized record if the record is normal.
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
            // Reserves 4 bytes in the blob for the k-v length before committed
            // records are serialized and the length of the serialized records
            // are known.
            log_ng_blob->append(ptr, sizeof(uint32_t));

            for (const auto &wset_entry : wset_entry_vec)
            {
                wset_entry->key_->Serialize(*log_ng_blob);

                uint8_t delete_flag =
                    wset_entry->op_ == OperationType::Delete ? 1 : 0;
                log_ng_blob->append(
                    reinterpret_cast<const char *>(&delete_flag), 1);

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

    assert(txlog_ != nullptr);
    // Note that node_id calculated from global core ID should always be equal
    // to the actual ccshard node id. But from txservice layer's view, only txid
    // is available. Txservice get txid from the bottom layer (ccshard).
    uint32_t tx_cc_node_id = TxCcNodeId();
    write_log.log_group_id_ = txlog_->GetLogGroupId(tx_cc_node_id);
    txlog_->WriteLog(write_log.log_group_id_,
                     write_log.log_closure_.Controller(),
                     write_log.log_closure_.LogRequest(),
                     write_log.log_closure_.LogResponse(),
                     write_log.log_closure_);
    ACTION_FAULT_INJECTOR("after_write_log");
}

void TransactionExecution::PostProcess(WriteToLogOp &write_log)
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
    WriteToLogOp *log_op = static_cast<WriteToLogOp *>(state_stack_.back());
    state_stack_.pop_back();

    if (state_stack_.empty())
    {
        if (!log_op->hd_result_.IsError())
        {
            tx_status_.store(TxnStatus::Committed, std::memory_order_release);
            PushOperation(&update_txn_);
            Process(update_txn_);
        }
        else
        {
            if (log_op->hd_result_.ErrorCode() ==
                (int8_t) HandlerResultErrorType::Unknown)
            {
                bool_resp_->SetErrorCode(TxErrorCode::LOG_SERVICE_UNREACHABLE);
                tx_status_.store(TxnStatus::Unknown, std::memory_order_release);
            }
            else
            {
                bool_resp_->SetErrorCode(TxErrorCode::WRITE_LOG_FAIL);
                tx_status_.store(TxnStatus::Aborted, std::memory_order_release);
            }
            PushOperation(&update_txn_);
            Process(update_txn_);
        }
    }
    else
    {
        // The tx is committing a multi-stage operation, e.g., schema changes.
        Forward();
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
    handler->UpdateTxnStatus(txid_,
                             iso_level_,
                             tx_status_.load(std::memory_order_relaxed),
                             update_txn.hd_result_);
    update_txn.Forward(this);
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

    uint32_t acquire_write_cnt = rw_set_.WriteSetSize();
    if (rw_set_.WriteSetSize() > 0 && acquire_write_.hd_result_.IsError())
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

    if (TxStatus() != TxnStatus::Unknown &&
        (acquire_write_cnt > 0 || rw_set_.ReadSetSize() > 0))
    {
        if (TxStatus() == TxnStatus::Committed)
        {
            // The tx is committed. Post-processing includes both primary keys
            // that have locks and secondary keys without locks.
            post_process_.Reset(rw_set_.WriteSetSize(), 0);
        }
        else
        {
            post_process_.Reset(acquire_write_cnt, rw_set_.ReadSetSize());
        }
        PushOperation(&post_process_);
        Process(post_process_);
    }
    else
    {
        // For tx's that are in unknown result, or have finished validation but
        // do not upload anything, skips post-processing.

        if (bool_resp_ == nullptr)
        {
            // Do not notify mysql because the tx is initiated by range split
        }
        else if (tx_status_.load(std::memory_order_relaxed) ==
                 TxnStatus::Committed)
        {
            bool_resp_->Finish(true);
        }
        else if (tx_status_.load(std::memory_order_relaxed) ==
                     TxnStatus::Aborted ||
                 tx_status_.load(std::memory_order_relaxed) ==
                     TxnStatus::Unknown)
        {
            bool_resp_->Finish(false);
        }

        // transaction can be recycled and put into free list.
        tx_status_.store(TxnStatus::Finished, std::memory_order_release);
        Reset();
    }
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
    post_process.is_running_ = true;

    if (tx_status_.load(std::memory_order_relaxed) == TxnStatus::Committed)
    {
        // If the tx has finished validation, the read intentions/locks of the
        // read-set keys have been cleared after validation. Post-processing
        // only clears the write locks of the write-set keys.

        post_process.Reset(rw_set_.WriteSetSize(), 0);

        size_t idx = 0;
        const std::unordered_map<TableName, TableWriteSet> &wset =
            rw_set_.WriteSet();
        for (const auto &[table_name, table_write_set] : wset)
        {
            for (const auto &[key, write_entry] : table_write_set)
            {
                handler->PostWrite(tx_number_.load(std::memory_order_relaxed),
                                   tx_term_,
                                   command_id_.load(std::memory_order_relaxed),
                                   commit_ts_,
                                   write_entry.cce_addr_,
                                   write_entry.rec_.get(),
                                   write_entry.op_,
                                   post_process.hd_result_,
                                   protocol_);
                ++idx;
            }
        }
    }
    else
    {
        // If the tx failed during the acquire phase or was aborted before
        // entering the commit phase, post-processing removes write intents of
        // write-set keys and clears read intents/locks of read-set keys.

        size_t offset = 0;
        size_t idx = 0;
        const std::unordered_map<TableName, TableWriteSet> &wset =
            rw_set_.WriteSet();

        for (const auto &[table_name, table_write_set] : wset)
        {
            for (const auto &[key, write_entry] : table_write_set)
            {
                if (write_entry.cce_addr_.Term() < 0)
                {
                    // Keys that were not successfully locked in the cc map do
                    // not need post-processing.
                    ++idx;
                    continue;
                }
                assert(!write_entry.cce_addr_.Empty());

                // Abort doesn't care the OperationType, since PostWrite is just
                // used to release the lock.
                handler->PostWrite(tx_number_.load(std::memory_order_relaxed),
                                   tx_term_,
                                   command_id_.load(std::memory_order_relaxed),
                                   0,
                                   write_entry.cce_addr_,
                                   nullptr,
                                   write_entry.op_,
                                   post_process.hd_result_,
                                   protocol_);

                ++offset;
                ++idx;
            }
        }

        idx = 0;
        const std::unordered_map<TableName,
                                 std::unordered_map<CcEntryAddr, ReadSetEntry>>
            &rset = rw_set_.ReadSet();

        for (const auto &table_entry_it : rset)
        {
            for (const auto &[cce_addr, read_entry] : table_entry_it.second)
            {
                handler->PostRead(tx_number_.load(std::memory_order_relaxed),
                                  tx_term_,
                                  command_id_.load(std::memory_order_relaxed),
                                  0,
                                  0,
                                  0,
                                  cce_addr,
                                  post_process.hd_result_,
                                  read_entry.protocol_,
                                  read_entry.lock_type_);
                ++idx;
            }
        }
    }

    StartTiming();
}

void TransactionExecution::PostProcess(PostProcessOp &post_process)
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
    if (!state_stack_.empty())
    {
        assert(state_stack_.back() == &post_process);
        state_stack_.pop_back();
    }
    assert(state_stack_.empty());

    if (tx_status_.load(std::memory_order_relaxed) == TxnStatus::Committed)
    {
        if (bool_resp_ != nullptr)
        {
            bool_resp_->Finish(true);
        }
    }
    else if (tx_status_.load(std::memory_order_relaxed) == TxnStatus::Aborted)
    {
        if (bool_resp_ != nullptr)
        {
            bool_resp_->Finish(false);
        }
    }

    // transaction can be recycled and put into free list.
    tx_status_.store(TxnStatus::Finished, std::memory_order_release);

#ifdef METRICS_COLLECTOR_ENABLE
    tx_processor_->MetricCollect(metrics::Value::IncDecValue::Increment,
                                 post_process_total,
                                 std::nullopt);
#endif
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
        handler->AcquireWriteAll(*acq_all_op.table_name_,
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
    Forward();
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

    for (uint32_t nid = 0; nid < node_group_cnt; ++nid)
    {
        handler->PostWriteAll(*post_write_all_op.table_name_,
                              *post_write_all_op.key_,
                              *post_write_all_op.rec_,
                              nid,
                              tx_number_.load(std::memory_order_relaxed),
                              tx_term_,
                              command_id_.load(std::memory_order_relaxed),
                              commit_ts_,
                              post_write_all_op.hd_result_,
                              post_write_all_op.op_type_,
                              post_write_all_op.write_type_);
    }

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
    Forward();
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
    handler->DataStoreUpsertTable(ds_upsert_table_op.table_schema_,
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
    Forward();
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

    handler->FaultInject(fault_inject_op_.fault_name_,
                         fault_inject_op_.fault_paras_,
                         tx_term_,
                         command_id_.load(std::memory_order_relaxed),
                         txid_,
                         fault_inject_op_.vct_node_id_,
                         fault_inject_op_.hd_result_);
    return;
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
    bool_resp_->Reset();

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

    handler->CleanCcEntryForTest(*clean_entry_op_.tab_name_,
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
    Forward();
}

template <typename ResultType>
void TransactionExecution::Process(DsOp<ResultType> &ds_op)
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
}

template void TransactionExecution::Process(DsOp<RangeMedianKeyResult> &ds_op);
template void TransactionExecution::Process(DsOp<Void> &ds_op);

template <typename ResultType>
void TransactionExecution::PostProcess(DsOp<ResultType> &ds_op)
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
    Forward();
}

template void TransactionExecution::PostProcess(
    DsOp<RangeMedianKeyResult> &ds_op);
template void TransactionExecution::PostProcess(DsOp<Void> &ds_op);

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
    Forward();
}
}  // namespace txservice
