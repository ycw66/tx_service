#include "tx_execution.h"

#include <bitset>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <string>

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
#include "util.h"

namespace txservice
{
// whether skip write redo log to log_service.
bool txservice_skip_redo_log = false;

TransactionExecution::TransactionExecution(CcHandler *handler,
                                           TxLog *txlog,
                                           TxProcessor *tx_processor,
                                           CcProtocol proto)
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
      bool_resp_(nullptr),
      kvp_resp_(nullptr),
      uint64_resp_(nullptr),
      detailed_error_msg_(""),
      protocol_(proto),
      init_txn_(this),
#ifdef RANGE_PARTITION_ENABLED
      unlock_range_op_(this),
#endif
      read_(this),
      scan_open_(this),
      scan_next_(this),
      obj_cmd_(this),
#ifdef RANGE_PARTITION_ENABLED
      lock_write_ranges_(this),
#endif
      acquire_write_(this),
      set_ts_(this),
      validate_(this),
      update_txn_(this),
      post_process_(this),
      write_log_(this),
      sleep_op_(this),
      analyze_table_all_op_(this),
      fault_inject_op_(this),
      clean_entry_op_(this),
      abundant_lock_op_(this)
{
    TX_TRACE_ASSOCIATE(this, cc_handler_);

    init_tx_req_ = std::make_unique<InitTxRequest>();
    commit_tx_req_ = std::make_unique<CommitTxRequest>();
    scan_close_req_pool_ =
        std::make_unique<CircularQueue<std::unique_ptr<ScanCloseTxRequest>>>(8);
}

void TransactionExecution::Reset(CcProtocol proto)
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
    bool_resp_ = nullptr;
    kvp_resp_ = nullptr;
    uint64_resp_ = nullptr;
    detailed_error_msg_ = "";
    protocol_ = proto;
    schema_op_ = nullptr;
    split_flush_op_ = nullptr;
    drain_batch_.clear();
    scan_alias_cnt_ = 0;

    // drain out tx_req_queue_ (if any request left)
    TxRequest *req = nullptr;
    while (tx_req_queue_.try_dequeue(req))
    {
    }
}

void TransactionExecution::Restart(CcHandler *handler,
                                   TxLog *txlog,
                                   TxProcessor *tx_processor)
{
    cc_handler_ = handler;
    txlog_ = txlog;
    tx_processor_ = tx_processor;
    tx_status_.store(TxnStatus::Ongoing, std::memory_order_relaxed);
}

bool TransactionExecution::IsIdle()
{
    return state_stack_.empty() && tx_req_queue_.peek() == nullptr;
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

    case CcErrorCode::UNDEFINED_ERR:
    default:
        return TxErrorCode::UNDEFINED_ERR;
    }
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

        if (cc_handler_->table_schema_op_pool_.empty())
        {
            std::unique_ptr<UpsertTableOp> table_op = nullptr;
            table_op = std::make_unique<UpsertTableOp>(
                schema_op.table_name_str(),
                schema_op.old_catalog_blob(),
                schema_op.catalog_ts(),
                schema_op.new_catalog_blob(),
                static_cast<OperationType>(table_msg.op_type()),
                this,
                &(schema_op.alter_table_info_blob()));
            schema_op_ = std::move(table_op);
        }
        else
        {
            assert(cc_handler_->table_schema_op_pool_.back() != nullptr);
            schema_op_ = std::move(cc_handler_->table_schema_op_pool_.back());
            cc_handler_->table_schema_op_pool_.pop_back();

            schema_op_->Reset(schema_op.table_name_str(),
                              schema_op.old_catalog_blob(),
                              schema_op.catalog_ts(),
                              schema_op.new_catalog_blob(),
                              static_cast<OperationType>(table_msg.op_type()),
                              this,
                              &(schema_op.alter_table_info_blob()));
        }

        if (schema_op.stage() == ::txlog::SchemaOpMessage::Stage::
                                     SchemaOpMessage_Stage_PrepareSchema)
        {
            schema_op_->prepare_log_op_.hd_result_.SetFinished();
            schema_op_->op_ = &schema_op_->prepare_log_op_;
        }
        else
        {
            assert(schema_op.stage() == ::txlog::SchemaOpMessage::Stage::
                                            SchemaOpMessage_Stage_CommitSchema);
            schema_op_->commit_log_op_.hd_result_.SetFinished();
            schema_op_->op_ = &schema_op_->commit_log_op_;
        }

        state_stack_.push_back(schema_op_.get());
        break;
    }
    default:
        tx_status_.store(TxnStatus::Finished);
        break;
    }
}

void TransactionExecution::RemoteStatisticsTx(
    const TableName &table_or_index_name,
    uint64_t schema_version,
    const remote::NodeGroupSamplePool &remote_sample_pool)
{
    TableName base_table_name(table_or_index_name.GetBaseTableNameSV(),
                              TableType::Primary);
    CatalogKey catalog_key(base_table_name);
    CatalogRecord catalog_rec;
    ReadTxRequest read_req(&txservice::catalog_ccm_name,
                           &catalog_key,
                           &catalog_rec,
                           false,
                           false,
                           true);

    bool exists = false;
    TxErrorCode err = TxReadCatalog(this, read_req, exists);
    if (err == TxErrorCode::NO_ERROR && exists)
    {
        const TableSchema *table_schema = catalog_rec.Schema();
        if (table_schema->Version() == schema_version)
        {
            Statistics *statistics = table_schema->StatisticsObject();
            statistics->OnRemoteStatisticsMessage(
                table_or_index_name, table_schema, remote_sample_pool);
        }
    }
}

void TransactionExecution::RecoverSplitRangeTx(
    const ::txlog::SplitRangeOpMessage &ds_split_range_op_msg,
    const TableSchema *table_schema,
    int32_t partition_id,
    const TxKey *range_start_key,
    const TxKey *range_end_key,
    const RangeInfo *range_info,
    std::vector<std::unique_ptr<TxKey>> &&new_range_keys,
    std::vector<int32_t> &&new_partition_ids,
    NodeGroupId node_group,
    uint64_t txn,
    int64_t tx_term,
    uint64_t commit_ts,
    std::optional<std::pair<CcEntryAddr, ReadSetEntry>> catalog_cc_entry,
    std::shared_ptr<std::atomic_uint32_t> split_tx_started)
{
    tx_status_.store(TxnStatus::Recovering, std::memory_order_relaxed);
    tx_number_.store(txn, std::memory_order_relaxed);
    tx_term_ = tx_term;
    commit_ts_ = commit_ts;

    const TableName range_table_name = TableName{
        ds_split_range_op_msg.table_name(), TableType::RangePartition};
    const TableName table_name =
        TableName{range_table_name.StringView(),
                  TableName::Type(range_table_name.StringView())};

    std::vector<std::pair<TxKey::Uptr, int32_t>> new_range_info;
    for (size_t i = 0; i < new_range_keys.size(); i++)
    {
        new_range_info.emplace_back(std::move(new_range_keys[i]),
                                    new_partition_ids[i]);
    }
    std::unique_ptr<SplitFlushRangeOp> split_range_op =
        std::make_unique<SplitFlushRangeOp>(table_name,
                                            table_schema,
                                            node_group,
                                            range_start_key,
                                            range_end_key,
                                            range_info,
                                            std::move(new_range_info),
                                            this);

    split_range_op->catalog_cc_entry_ = std::move(catalog_cc_entry);
    split_range_op->recover_split_started_ = split_tx_started;
    split_range_op->pending_pin_data_ = true;

    const ::txlog::SplitRangeOpMessage::Stage stage =
        ds_split_range_op_msg.stage();

    if (stage == ::txlog::SplitRangeOpMessage_Stage_PrepareSplit)
    {
        split_range_op->prepare_log_op_.hd_result_.SetFinished();
        split_range_op->op_ = &split_range_op->prepare_log_op_;
    }
    else
    {
        LocalCcShards *shards = Sharder::Instance().GetLocalCcShards();
        const StoreRange *range =
            shards->FindRange(table_name, node_group, *range_start_key);
        const auto &slices = range->Slices();
        for (auto slice_it = slices.cbegin(); slice_it != slices.cend();
             ++slice_it)
        {
            if (slice_it == slices.cbegin())
            {
                split_range_op->slice_info_.emplace_back(nullptr,
                                                         (*slice_it)->Size());
            }
            else
            {
                split_range_op->slice_info_.emplace_back(
                    (*slice_it)->StartKey()->Clone(), (*slice_it)->Size());
            }
        }
        split_range_op->commit_log_op_.hd_result_.SetFinished();
        split_range_op->op_ = &split_range_op->commit_log_op_;
    }

    LOG(INFO) << "Recovering split flush tx " << TxNumber() << " on table "
              << table_name.StringView() << ", range id "
              << range_info->PartitionId();
    split_flush_op_ = std::move(split_range_op);
    state_stack_.push_back(split_flush_op_.get());
}

TxmStatus TransactionExecution::Forward()
{
    bool has_more_req = false;
    if (state_stack_.empty())
    {
        TxRequest *req = nullptr;
        bool success = tx_req_queue_.try_dequeue(req);
        assert(success == (req != nullptr));

        has_more_req = tx_req_queue_.peek() != nullptr;

        if (req != nullptr)
        {
            req->Process(this);
        }
    }
    else
    {
        TransactionOperation *curr_op = state_stack_.back();
        curr_op->Forward(this);
    }

    TxnStatus status = TxStatus();
    if (status == TxnStatus::Finished)
    {
        return TxmStatus::Finished;
    }
    else if (state_stack_.empty() && !has_more_req)
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
    TxnStatus status = tx_status_.load(std::memory_order_acquire);

    if (status == TxnStatus::Ongoing)
    {
        bool success = tx_req_queue_.enqueue(tx_req);
        assert(success);

        return 0;
    }
    else
    {
        // The tx has started committing/aborting or has committed/aborted. Does
        // not accept new requests.
        return 1;
    }
}

void TransactionExecution::InitTx(IsolationLevel iso_level, CcProtocol protocol)
{
    init_tx_req_->Reset();
    init_tx_req_->iso_level_ = iso_level;
    init_tx_req_->protocol_ = protocol;
    Execute(init_tx_req_.get());
}

bool TransactionExecution::CommitTx(CommitTxRequest &commit_req)
{
    if (rw_set_.WriteSetSize() == 0 && rw_set_.ReadSetSize() == 0)
    {
        commit_tx_req_->Reset();
        Execute(commit_tx_req_.get());
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

    if (tx_term_ < 0)
    {
        read_req.SetError(TxErrorCode::TX_INIT_FAIL);
        return;
    }

    rec_resp_ = &read_req.tx_result_;

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

    abort_req.tx_result_.Finish(false);
    bool_resp_ = nullptr;
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

    if (cc_handler_->table_schema_op_pool_.empty())
    {
        std::unique_ptr<UpsertTableOp> table_op = nullptr;
        table_op =
            std::make_unique<UpsertTableOp>(req.table_name_->StringView(),
                                            *req.curr_image_,
                                            req.curr_schema_ts_,
                                            *req.dirty_image_,
                                            req.op_type_,
                                            this,
                                            req.alter_table_info_image_);
        schema_op_ = std::move(table_op);
    }
    else
    {
        assert(cc_handler_->table_schema_op_pool_.back() != nullptr);
        schema_op_ = std::move(cc_handler_->table_schema_op_pool_.back());
        cc_handler_->table_schema_op_pool_.pop_back();

        schema_op_->Reset(req.table_name_->StringView(),
                          *req.curr_image_,
                          req.curr_schema_ts_,
                          *req.dirty_image_,
                          req.op_type_,
                          this,
                          req.alter_table_info_image_);
    }

    PushOperation(schema_op_.get());
    Forward();
}

void TransactionExecution::ProcessTxRequest(ObjectCommandTxRequest &req)
{
    rec_resp_ = &req.tx_result_;
    obj_cmd_.Reset(req.table_name_,
                   req.key_,
                   req.command_,
                   req.cmd_result_,
                   req.auto_commit_);
    PushOperation(&obj_cmd_);
    Process(obj_cmd_);
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

    split_flush_op_ =
        std::make_unique<SplitFlushRangeOp>(*req.table_name_,
                                            req.schema_,
                                            req.node_group_,
                                            req.old_start_key_,
                                            req.old_end_key_,
                                            req.old_range_info_,
                                            std::move(req.new_range_id_),
                                            this);

    PushOperation(split_flush_op_.get());
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

    cc_handler_->NewTxn(init_txn.hd_result_, iso_level_);
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
        DLOG(ERROR) << "InitTxnOperation failed for cc error:"
                    << init_txn.hd_result_.ErrorMsg();
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
            while (tx_req_queue_.try_dequeue(req))
            {
                req->SetError(TxErrorCode::TX_INIT_FAIL);
            }
        }
        return;
    }

    const InitTxResult &init_result = init_txn.hd_result_.Value();
    txid_ = init_result.txid_;
    tx_number_.store(txid_.TxNumber(), std::memory_order_release);
    start_ts_ = init_result.start_ts_;
    commit_ts_bound_ = init_result.start_ts_ + 1;
    tx_term_ = init_result.term_;
    state_stack_.pop_back();

    if (uint64_resp_ != &init_tx_req_->tx_result_)
    {
        uint64_resp_->Finish(tx_number_.load(std::memory_order_acquire));
    }

    Forward();
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

            cc_handler_->ReadLocal(table_name,
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
                const TxRecord *cache_rec =
                    rw_set_.FindCacheRead(table_name, key);
                if (cache_rec != nullptr)
                {
                    rec.Copy(*cache_rec);
                    state_stack_.pop_back();
                    assert(state_stack_.empty());
                    rec_resp_->Finish(RecordStatus::Normal);
                    return;
                }
            }

            read.local_cache_miss_ = true;
            read.protocol_ = protocol_;
            read.iso_level_ = iso_level_;

            uint32_t key_shard_code = 0;
#ifdef RANGE_PARTITION_ENABLED
            if (!read.lock_range_result_.IsFinished())
            {
                read.is_running_ = false;
                // First read and lock the range the key located in through
                // lock_range_op_.
                lock_range_op_.Reset();
                read.lock_range_result_.Reset();

                lock_range_op_.key_ = &key;
                lock_range_op_.range_table_name_ =
                    TableName(read.read_tx_req_->tab_name_->StringView(),
                              TableType::RangePartition);
                lock_range_op_.range_rec_ = &read.range_rec_;
                lock_range_op_.lock_range_result_ = &read.lock_range_result_;

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
                assert(!read.lock_range_result_.IsError());

                // Uses the lower 10 bits of the key's hash code to shard the
                // key across CPU cores in a cc node.
                uint32_t residual = key.Hash() & 0x3FF;
                key_shard_code = read.range_rec_.GetRangeInfo()->PartitionId()
                                     << 10 |
                                 residual;
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
            else if (corresponding_sk_commit_ts != 0)
            {
                read_ts = corresponding_sk_commit_ts;
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
                              read.read_tx_req_->is_for_write_);

            if (metrics::enable_transactions &&
                !read.hd_result_.Value().is_local_)
            {
                auto meter = tx_processor_->meter_.get();
                meter->Collect(tx_processor_->REMOTE_REQUEST_ON_FLY_COUNT_NAME_,
                               metrics::Value::IncDecValue::Increment,
                               "read");
                read.op_start_ = metrics::Clock::now();
            }

            StartTiming();
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
                    << read_.hd_result_.ErrorMsg();
        rec_resp_->FinishError(ConvertCcError(read_.hd_result_.ErrorCode()));
    }
    else
    {
        const ReadKeyResult &read_res = read_.hd_result_.Value();
        const ReadTxRequest *read_req = read.read_tx_req_;

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
            cache_miss_read_cce_addr_.SetCce(0, -1, 0, 0);
        }

        rec_resp_->Finish(read_res.rec_status_);
    }
}

#ifdef RANGE_PARTITION_ENABLED
void TransactionExecution::Process(LockReadRangeOperation &lock_range)
{
    cc_handler_->ReadLocal(lock_range.range_table_name_,
                           *lock_range.key_,
                           *lock_range.range_rec_,
                           ReadType::Inside,
                           tx_number_.load(std::memory_order_relaxed),
                           tx_term_,
                           CommandId(),
                           start_ts_,
                           *lock_range.lock_range_result_,
                           IsolationLevel::RepeatableRead,
                           CcProtocol::Locking);

    lock_range.Forward(this);
}

void TransactionExecution::PostProcess(LockReadRangeOperation &lock_range)
{
    if (lock_range.lock_range_result_->IsError())
    {
        DLOG(ERROR) << "LockReadRangeOperation failed for cc error:"
                    << lock_range.lock_range_result_->ErrorMsg();
        rec_resp_->FinishError(
            ConvertCcError(lock_range.lock_range_result_->ErrorCode()));
    }
    else if (lock_range.lock_range_result_->Value().rec_status_ ==
             RecordStatus::Normal)
    {
        // The read lock on the range is added, put the range cce into read
        // set for later release read lock.
        // The range cannot be changed before this tx finishes
        // post-processing, so the read lock on the range is kept until
        // then.
        const ReadKeyResult &read_res = lock_range.lock_range_result_->Value();
        rw_set_.AddRead(
            read_res.cce_addr_, read_res.ts_, &lock_range.range_table_name_);
    }
    state_stack_.pop_back();
    Forward();
}

void TransactionExecution::Process(UnlockReadRangeOperation &unlock_range)
{
    // remove range entry from read set and do PostRead
    rw_set_.DedupRead(*unlock_range.cce_addr_);

    // just send post read cc request and return
    cc_handler_->PostRead(TxNumber(),
                          TxTerm(),
                          CommandId(),
                          0,
                          0,
                          0,
                          *unlock_range.cce_addr_,
                          unlock_range.unlock_range_result_);
}

void TransactionExecution::PostProcess(UnlockReadRangeOperation &unlock_range)
{
    state_stack_.pop_back();
    Forward();
}

void TransactionExecution::ReleaseReadRangeLock(txservice::ReadOperation &read)
{
    unlock_range_op_.Reset();
    unlock_range_op_.cce_addr_ = &read.lock_range_result_.Value().cce_addr_;

    PushOperation(&unlock_range_op_);
    Process(unlock_range_op_);
}
#endif

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

#ifdef RANGE_PARTITION_ENABLED
    scan_open.Forward(this);
#else
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
            open_result.scanner_ = nullptr;

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
        assert(scan_it != scans_.end());
        scan_next.UpdateScanState(&scan_it->second);
    }
    scan_next.alias_ = alias;

    CcScanner &scanner = *scan_next.scan_state_->scanner_;
    scan_next.is_running_ = true;

    bool to_scan_next = scanner.Current() == nullptr &&
                        scanner.Status() == ScannerStatus::Blocked;

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
    }
#ifdef RANGE_PARTITION_ENABLED
    else if (to_scan_next && scanner.Type() == CcmScannerType::RangePartition)
    {
        ScanState &scan_state = *scan_next.scan_state_;

        if (scanner.Direction() == ScanDirection::Forward &&
                scan_state.slice_position_ == SlicePosition::LastSlice ||
            scanner.Direction() == ScanDirection::Backward &&
                scan_state.slice_position_ == SlicePosition::FirstSlice)
        {
            // The current slice is the last (or first). There is no more slice
            // to scan.
            scanner.SetStatus(ScannerStatus::Closed);
            scan_next.slice_hd_result_.SetFinished();
            scan_next.unlock_range_result_.SetFinished();
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
                scan_next.RangeNgTerm(),
                scan_state.SliceLastKey(),
                !scan_state.inclusive_,
                scan_state.scan_end_key_,
                scan_state.scan_end_inclusive_,
                start_ts_,
                tx_number_.load(std::memory_order_relaxed),
                tx_term_,
                CommandId(),
                scan_next.slice_hd_result_,
                iso_level_,
                protocol_);
            if (metrics::enable_transactions &&
                !scan_next.slice_hd_result_.Value().is_local_)
            {
                auto meter = tx_processor_->meter_.get();
                meter->Collect(tx_processor_->REMOTE_REQUEST_ON_FLY_COUNT_NAME_,
                               metrics::Value::IncDecValue::Increment,
                               "scan_next");
                scan_next.op_start_ = metrics::Clock::now();
            }
        }
        else if (scanner.Direction() == ScanDirection::Forward &&
                     scan_state.slice_position_ ==
                         SlicePosition::LastSliceInRange ||
                 scanner.Direction() == ScanDirection::Backward &&
                     scan_state.slice_position_ ==
                         SlicePosition::FirstSliceInRange)
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
                    -1,
                    scan_state.SliceLastKey(),
                    !scan_state.inclusive_,
                    scan_state.scan_end_key_,
                    scan_state.scan_end_inclusive_,
                    start_ts_,
                    tx_number_.load(std::memory_order_relaxed),
                    tx_term_,
                    CommandId(),
                    scan_next.slice_hd_result_,
                    iso_level_,
                    protocol_);
                if (metrics::enable_transactions &&
                    !scan_next.slice_hd_result_.Value().is_local_)
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

                cc_handler_->ReadLocal(
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
            }
        }
    }
#endif
    else if (scanner.Type() == CcmScannerType::HashPartition)
    {
        scan_next.hd_result_.SetFinished();
    }
#ifdef RANGE_PARTITION_ENABLED
    else if (scanner.Type() == CcmScannerType::RangePartition)
    {
        scan_next.unlock_range_result_.SetFinished();
        scan_next.slice_hd_result_.SetFinished();
    }
#endif

    StartTiming();
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

    CcScanner &scanner = *scan_next.scan_state_->scanner_;

    if (scanner.Type() == CcmScannerType::HashPartition &&
        scan_next.hd_result_.IsError())
    {
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
        DLOG(ERROR) << "ScanNextOperation failed for cc error: "
                    << scan_next.slice_hd_result_.ErrorMsg();
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

    const TableName &table_name = scan_next.tx_req_->table_name_;
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
                            scan_batch.emplace_back(cc_scan_tuple->Key(),
                                                    cc_scan_tuple->Record(),
                                                    RecordStatus::Normal,
                                                    cc_scan_tuple->key_ts_,
                                                    cc_scan_tuple->cce_addr_,
                                                    scan_tuple_lock_type);
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
                                                    cc_scan_tuple->cce_addr_,
                                                    scan_tuple_lock_type);
                        }
#else
                        // When the record status is not Normal, the record
                        // is set to null in the scan result.
                        const TxRecord *rec =
                            cc_scan_tuple->rec_status_ == RecordStatus::Normal
                                ? cc_scan_tuple->Record()
                                : nullptr;

                        scan_batch.emplace_back(cc_scan_tuple->Key(),
                                                rec,
                                                cc_scan_tuple->rec_status_,
                                                cc_scan_tuple->key_ts_,
                                                cc_scan_tuple->cce_addr_,
                                                scan_tuple_lock_type);
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
                            scan_batch.emplace_back(cc_scan_tuple->Key(),
                                                    cc_scan_tuple->Record(),
                                                    RecordStatus::Normal,
                                                    cc_scan_tuple->key_ts_,
                                                    cc_scan_tuple->cce_addr_,
                                                    scan_tuple_lock_type);
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
                                                    cc_scan_tuple->cce_addr_,
                                                    scan_tuple_lock_type);
                        }
#else
                        // When the record status is not Normal, the record
                        // is set to null in the scan result.
                        const TxRecord *rec =
                            cc_scan_tuple->rec_status_ == RecordStatus::Normal
                                ? cc_scan_tuple->Record()
                                : nullptr;

                        scan_batch.emplace_back(cc_scan_tuple->Key(),
                                                rec,
                                                cc_scan_tuple->rec_status_,
                                                cc_scan_tuple->key_ts_,
                                                cc_scan_tuple->cce_addr_,
                                                scan_tuple_lock_type);
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
    bool scan_finished = dir == ScanDirection::Forward &&
                             slice_pos == SlicePosition::LastSlice ||
                         dir == ScanDirection::Backward &&
                             slice_pos == SlicePosition::FirstSlice;

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
    assert(scan_it != scans_.end());
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

#ifdef EXT_TX_PROC_ENABLED
void TransactionExecution::ExternalForward()
{
    tx_processor_->ExternalForward(this);
}

std::atomic<uint16_t> *TransactionExecution::ExternalProcessorCnt()
{
    return &tx_processor_->external_processor_num_;
}

std::function<void()> *TransactionExecution::ExternalProcessorFunctor()
{
    return tx_processor_->ExtProcessorFunctor();
}
#endif

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

    tx_status_.store(TxnStatus::Committing, std::memory_order_release);

#ifndef ON_KEY_OBJECT
    if (rw_set_.WriteSetSize() > 0)
    {
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
#endif
    {
        PushOperation(&set_ts_);
        Process(set_ts_);
    }
}

void TransactionExecution::Abort()
{
    if (tx_term_ < 0)
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

    tx_status_.store(TxnStatus::Aborted, std::memory_order_release);
    PushOperation(&update_txn_);
    Process(update_txn_);
}

void TransactionExecution::Process(LockWriteRangesOp &lock_write_ranges)
{
    if (!lock_write_ranges.init_)
    {
        std::unordered_map<TableName, TableWriteSet> &wset = rw_set_.WriteSet();
        lock_write_ranges.table_it_ = wset.begin();
        lock_write_ranges.table_end_ = wset.end();

        const TableName &tbl_name = lock_write_ranges.table_it_->first;
        lock_write_ranges.range_table_name_ =
            TableName(tbl_name.StringView(), TableType::RangePartition);

        lock_write_ranges.write_key_it_ =
            lock_write_ranges.table_it_->second.begin();
        lock_write_ranges.write_key_end_ =
            lock_write_ranges.table_it_->second.end();

        lock_write_ranges.init_ = true;
    }

    // TODO{liunyl}: find in readset? no need to read the range again if
    // already in rset.
    assert(lock_write_ranges.table_it_ != lock_write_ranges.table_end_);
    assert(lock_write_ranges.write_key_it_ != lock_write_ranges.write_key_end_);

    const TxKey *write_key = lock_write_ranges.write_key_it_->first;

    lock_write_ranges.lock_range_result_.Reset();
    lock_write_ranges.is_running_ = true;
    cc_handler_->ReadLocal(lock_write_ranges.range_table_name_,
                           *write_key,
                           lock_write_ranges.range_rec_,
                           ReadType::Inside,
                           tx_number_.load(std::memory_order_relaxed),
                           tx_term_,
                           command_id_.load(std::memory_order_relaxed),
                           start_ts_,
                           lock_write_ranges.lock_range_result_,
                           IsolationLevel::RepeatableRead,
                           CcProtocol::Locking);

    lock_write_ranges.Forward(this);
}

void TransactionExecution::PostProcess(LockWriteRangesOp &lock_write_ranges)
{
    if (lock_write_ranges.lock_range_result_.IsError())
    {
        DLOG(ERROR) << "LockWriteRangesOp failed for cc error:"
                    << lock_write_ranges.lock_range_result_.ErrorMsg();
        Abort();
        return;
    }

    const TxKey *range_start_key =
        lock_write_ranges.range_rec_.GetRangeInfo()->StartKey();
    const TxKey *range_end_key = lock_write_ranges.range_rec_.end_key_;

    const ReadKeyResult &read_res =
        lock_write_ranges.lock_range_result_.Value();
    rw_set_.AddRead(
        read_res.cce_addr_, read_res.ts_, &lock_write_ranges.range_table_name_);

    const TxKey *write_key = lock_write_ranges.write_key_it_->first;
    assert(range_start_key == nullptr || !(*write_key < *range_start_key));
    assert(range_end_key == nullptr || *write_key < *range_end_key);

    lock_write_ranges.Advance();

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
    }
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
        static_cast<LocalCcHandler *>(cc_handler_)->GetTsBaseValue();

    size_t idx = 0;
    std::unordered_map<TableName, TableWriteSet> &wset = rw_set_.WriteSet();
    for (auto &[table_name, table_write_set] : wset)
    {
        for (auto &[key_ptr, write_entry] : table_write_set)
        {
#ifndef RANGE_PARTITION_ENABLED
            size_t hash = write_entry.key_->Hash();
            write_entry.key_shard_code_ = Sharder::Instance().ShardCode(hash);
#else
            if (write_entry.forward_key_shard_code_ != 0)
            {
                rw_set_.IncreaseFowardWriteCnt();
            }
#endif
            acquire_write.acquire_write_entries_[idx] = &write_entry;

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
                idx,
                protocol_,
                iso_level_);
            ++idx;
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

    if (acquire_write.hd_result_.IsError() || acquire_write.rset_has_expired_)
    {
        if (acquire_write.rset_has_expired_)
        {
            bool_resp_->SetErrorCode(TxErrorCode::WRITE_WRITE_CONFLICT);
            bool_resp_->Finish(false);
        }
        else
        {
            DLOG(ERROR) << "AcquireWriteOperation failed for cc error:"
                        << acquire_write.hd_result_.ErrorMsg() << "  "
                        << (int) acquire_write.hd_result_.ErrorCode();
            bool_resp_->SetErrorCode(
                ConvertCcError(acquire_write.hd_result_.ErrorCode()));
            bool_resp_->Finish(false);
        }

        bool_resp_ = nullptr;
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
        DLOG(ERROR) << "SetCommitTsOperation failed for cc error:"
                    << set_ts.hd_result_.ErrorMsg();
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
#ifdef ON_KEY_OBJECT
            bool needs_write_log =
                !txservice_skip_redo_log && rw_set_.ObjectCommandSize() > 0;
#else
            bool needs_write_log =
                !txservice_skip_redo_log && rw_set_.WriteSetSize() > 0;
#endif
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

    size_t read_data_cnt = rw_set_.ReadSetSize();
    validate.Reset(read_data_cnt);
    validate.is_running_ = true;

    for (const auto &[tbl_name, tbl_read_set] : rset)
    {
        if (tbl_name == catalog_ccm_name ||
            tbl_name.Type() == TableType::RangePartition)
        {
            continue;
        }

        for (const auto &[cce_addr, read_entry] : tbl_read_set)
        {
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

    StartTiming();
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
                    << validate.hd_result_.ErrorMsg();

        if (bool_resp_ != nullptr)
        {
            bool_resp_->SetErrorCode(
                ConvertCcError(validate.hd_result_.ErrorCode()));
            bool_resp_->Finish(false);
            bool_resp_ = nullptr;
        }
#ifdef ON_KEY_OBJECT
        else if (rec_resp_ != nullptr)
        {
            // auto committed ObjectCommandTxRequest
            rec_resp_->FinishError(
                ConvertCcError(validate.hd_result_.ErrorCode()));
            rec_resp_ = nullptr;
        }
#endif

        Abort();
    }
    else
    {
#ifdef ON_KEY_OBJECT
        bool needs_write_log =
            !txservice_skip_redo_log && rw_set_.ObjectCommandSize() > 0;
#else
        bool needs_write_log =
            !txservice_skip_redo_log && rw_set_.WriteSetSize() > 0;
#endif
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
            tx_status_.store(TxnStatus::Committed, std::memory_order_release);

            // This is a read-only tx. Notifies early before post-processing.
            if (bool_resp_ != nullptr)
            {
                bool_resp_->Finish(true);
                bool_resp_ = nullptr;
            }

            PushOperation(&update_txn_);
            Process(update_txn_);
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
                std::forward_as_tuple(std::vector<const WriteSetEntry *>()));

            rec_vec_it.first->second.emplace_back(&wset_entry);

            if (wset_entry.forward_key_shard_code_ != 0)
            {
                // If the wset entry needs to be double written into different
                // ngs, write log for both ngs.
                uint32_t forward_ng_id = Sharder::Instance().ShardToCcNodeGroup(
                    wset_entry.forward_key_shard_code_);
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

void TransactionExecution::FillCommandLogRequest(WriteToLogOp &write_log)
{
#ifdef ON_KEY_OBJECT
    //    write_log.log_type_ = TxLogType::COMMAND;
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
            LOG(INFO) << "writing log, key str: " << key_str
                      << ", object version: " << obj_version;

            size_t cmds_len_start = log_ng_blob.size();
            uint32_t cmds_len = 0;
            log_ng_blob.append(reinterpret_cast<const char *>(&cmds_len),
                               sizeof(cmds_len));

            // number of commands
            uint16_t cmd_cnt = cmd_str_list.size();
            log_ng_blob.append(reinterpret_cast<const char *>(&cmd_cnt),
                               sizeof(cmd_cnt));

            LOG(INFO) << "cmd cnt: " << cmd_cnt;

            for (const auto &cmd_str : cmd_str_list)
            {
                uint32_t cmd_len = cmd_str.size();
                LOG(INFO) << "cmd len: " << cmd_len;
                log_ng_blob.append(reinterpret_cast<const char *>(&cmd_len),
                                   sizeof(cmd_len));
                log_ng_blob.append(cmd_str);
            }

            cmds_len = log_ng_blob.size() - cmds_len_start - sizeof(uint32_t);
            log_ng_blob.replace(cmds_len_start,
                                sizeof(cmds_len),
                                reinterpret_cast<const char *>(&cmds_len),
                                sizeof(cmds_len));
            LOG(INFO) << "cmds len: " << cmds_len;

            key_cmd_len =
                log_ng_blob.size() - key_cmd_len_start - sizeof(uint32_t);
            LOG(INFO) << "key cmd len: " << key_cmd_len;

            // Refills the reserved 4 bytes after knowing the length of
            // serialized key and commands.
            log_ng_blob.replace(
                key_cmd_len_start, sizeof(uint32_t), ptr, sizeof(uint32_t));
            LOG(INFO) << "log ng blob length: " << log_ng_blob.size();
            LOG(INFO) << "kv len stored in log blob: "
                      << *(reinterpret_cast<const uint32_t *>(ptr));
            const std::string &str = log_ng_blob;
            LOG(INFO) << "log_ng_blob: " << str;
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

    if (metrics::enable_log_metrics)
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
    if (metrics::enable_log_metrics)
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
            tx_status_.store(TxnStatus::Committed, std::memory_order_release);
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
    cc_handler_->UpdateTxnStatus(txid_,
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

    TxnStatus status = TxStatus();
    if (status == TxnStatus::Committed)
    {
        // The tx is committed. The tx must have finished validation.
        // Post-processing includes both primary keys that have locks and
        // secondary keys without locks.
        post_process_.Reset(rw_set_.WriteSetSize() + rw_set_.ForwardWriteCnt() +
                                rw_set_.ObjectCommandSize(),
                            0,
                            rw_set_.CatalogRangeSetSize());
    }
    else if (status == TxnStatus::Aborted)
    {
        post_process_.Reset(acquire_write_cnt,
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

    if (tx_status_.load(std::memory_order_relaxed) == TxnStatus::Committed)
    {
        // If the tx has finished validation, the read intentions/locks of
        // the read-set keys have been cleared after validation.
        // Post-processing only clears the write locks of the write-set
        // keys.

        size_t idx = 0;
        uint64_t tx_number = TxNumber();
        uint16_t command_id = command_id_.load(std::memory_order_relaxed);
        const std::unordered_map<TableName, TableWriteSet> &wset =
            rw_set_.WriteSet();
        for (const auto &[table_name, table_write_set] : wset)
        {
            for (const auto &[key, write_entry] : table_write_set)
            {
                cc_handler_->PostWrite(
                    tx_number_.load(std::memory_order_relaxed),
                    tx_term_,
                    command_id,
                    commit_ts_,
                    write_entry.cce_addr_,
                    write_entry.rec_.get(),
                    write_entry.op_,
                    write_entry.key_shard_code_,
                    post_process.hd_result_);
                if (write_entry.forward_key_shard_code_ != 0)
                {
                    cc_handler_->ForwardPostWrite(
                        tx_number_.load(std::memory_order_relaxed),
                        tx_term_,
                        command_id,
                        commit_ts_,
                        table_name,
                        key,
                        write_entry.rec_.get(),
                        write_entry.op_,
                        write_entry.forward_key_shard_code_,
                        post_process.hd_result_);
                }
                ++idx;
            }
        }

#ifdef ON_KEY_OBJECT
        const std::unordered_map<TableName,
                                 std::unordered_map<CcEntryAddr, CmdSetEntry>>
            *cmd_cce_set = rw_set_.ObjectCommandCce();
        assert(cmd_cce_set != nullptr);

        for (const auto &[table_name, cce_set] : *cmd_cce_set)
        {
            for (const auto &[cce, key] : cce_set)
            {
                cc_handler_->PostWrite(tx_number,
                                       tx_term_,
                                       command_id,
                                       commit_ts_,
                                       cce,
                                       nullptr,
                                       OperationType::RedisCommand,
                                       0,
                                       post_process.hd_result_);
            }
            ++idx;
        }
#endif

        if (idx == 0)
        {
            Forward();
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
                    if (write_entry.cce_addr_.Term() < 0)
                    {
                        // Keys that were not successfully locked in the cc
                        // map do not need post-processing.
                        ++idx;
                        continue;
                    }
                    assert(!write_entry.cce_addr_.Empty());

                    // Abort doesn't care the OperationType, since PostWrite is
                    // just used to release the lock.
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
            }
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
            Forward();
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
        LOG(INFO) << "finish auto committed ObjectCommandTxRequest";
        rec_resp_->Finish(obj_cmd_.hd_result_.Value().rec_status_);
        rec_resp_ = nullptr;
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
        if (Sharder::Instance().NodeId() == nid)
        {
            // Send out local request at last to prevent it from
            // modifying rec_ while the handler is still using it.
            continue;
        }
        cc_handler_->PostWriteAll(*post_write_all_op.table_name_,
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

    cc_handler_->PostWriteAll(*post_write_all_op.table_name_,
                              *post_write_all_op.key_,
                              *post_write_all_op.rec_,
                              Sharder::Instance().NodeId(),
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
    Forward();
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
                                  catalog_range_hd_result);
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

    cc_handler_->FaultInject(fault_inject_op_.fault_name_,
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
    Forward();
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
}

template void TransactionExecution::Process(AsyncOp<Void> &ds_op);

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
    Forward();
}

template void TransactionExecution::PostProcess(AsyncOp<Void> &ds_op);

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
    Sharder::Instance().GetLocalCcShards()->FlushData(*flush_op.tab_name_,
                                                      flush_op.schema_,
                                                      flush_op.data_sync_ts_,
                                                      tx_term_,
                                                      flush_op.node_group_,
                                                      flush_op.data_sync_vec_,
                                                      flush_op.archive_vec_,
                                                      flush_op.mv_vec_,
                                                      flush_op.hd_result_);
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
    Forward();
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
    Forward();
}

void TransactionExecution::Process(PostReadOperation &post_read_operation)
{
    post_read_operation.is_running_ = true;
    CcEntryAddr *cce_addr = post_read_operation.cce_entry_.first;
    ReadSetEntry *read_set_entry = post_read_operation.cce_entry_.second;
    cc_handler_->PostRead(tx_number_.load(std::memory_order_relaxed),
                          this->tx_term_,
                          command_id_.load(std::memory_order_relaxed),
                          read_set_entry->version_ts_,
                          0,
                          commit_ts_,
                          *cce_addr,
                          post_read_operation.hd_result_);
}

void TransactionExecution::PostProcess(PostReadOperation &post_read_operation)
{
    state_stack_.pop_back();
    Forward();
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
    StartTiming();

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
    Forward();
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
    const TableName &table_name = *obj_cmd_op.table_name_;
    const TxKey &key = *obj_cmd_op.key_;
    uint32_t key_shard_code = 0;

#ifdef RANGE_PARTITION_ENABLED
    if (obj_cmd_op.lock_range_result_.IsFinished())
    {
        // If there is an error when getting the key's range ID, the error would
        // be caught when forwarding the operation, which forces the tx state
        // machine to move to post-processing of the operation and returns an
        // error to the ObjectCommandTxRequest.
        assert(!obj_cmd_op.lock_range_result_.IsError());

        // Uses the lower 10 bits of the key's hash code to shard the
        // key across CPU cores in a cc node.
        uint32_t residual = key.Hash() & 0x3FF;
        key_shard_code = obj_cmd_op.range_rec_.GetRangeInfo()->PartitionId()
                             << 10 |
                         residual;
    }
    else
    {
        obj_cmd_op.is_running_ = false;
        // First read and lock the range the key located in through
        // lock_range_op_.
        lock_range_op_.Reset();
        obj_cmd_op.lock_range_result_.Reset();

        lock_range_op_.key_ = &key;
        lock_range_op_.range_table_name_ =
            TableName(table_name.StringView(), TableType::RangePartition);
        lock_range_op_.range_rec_ = &obj_cmd_op.range_rec_;
        lock_range_op_.lock_range_result_ = &obj_cmd_op.lock_range_result_;

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

    cc_handler_->ObjectCommand(*obj_cmd_op.table_name_,
                               *obj_cmd_op.key_,
                               key_shard_code,
                               *obj_cmd_op.command_,
                               *obj_cmd_op.cmd_result_,
                               TxNumber(),
                               tx_term_,
                               current_ts,
                               hd_res,
                               protocol_,
                               commit);
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
    }
    else
    {
        const ObjectCommandResult &cmd_result = hd_result.Value();
        RecordStatus obj_status = cmd_result.rec_status_;
        const TxCommand *cmd = obj_cmd_op.command_;

        // The command is directly executed and committed on the object if
        // autocommit and skip wal are both set. In such case, there is no need
        // to write log and do post write, and no need to add command into write
        // set.
        bool directly_commit =
            obj_cmd_op.auto_commit_ && txservice_skip_redo_log;

        if (!obj_cmd_op.auto_commit_ || directly_commit || cmd->IsReadOnly())
        {
            // Not autocommit, or autocommit and skip wal, or autocommit and
            // this is a read only command. Notify the ObjectCommandTxRequest
            // sender once the command finishes.
            rec_resp_->Finish(obj_status);
            rec_resp_ = nullptr;
        }

        // For autocommit read-modify-write commands, the ObjectCommandTxRequest
        // sender will be notified after auto commit succeeds, i.e. after
        // PostProcess or WritLog.

        bool add_to_write_set = !directly_commit && !cmd->IsReadOnly() &&
                                obj_status == RecordStatus::Normal;

        if (add_to_write_set)
        {
            // The command modifies the object. Put it into the command set
            // for writing log and post-processing.
            rw_set_.AddObjectCommand(*obj_cmd_op.table_name_,
                                     cmd_result.cce_addr_,
                                     cmd_result.commit_ts_,
                                     obj_cmd_op.key_,
                                     obj_cmd_op.command_);
        }

        if (obj_cmd_op.auto_commit_)
        {
            Commit();
        }
    }
}

}  // namespace txservice
