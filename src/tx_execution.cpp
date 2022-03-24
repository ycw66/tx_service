#include "tx_execution.h"

#include <stdint.h>

#include <cassert>
#include <chrono>
#include <iostream>

#include "local_cc_shards.h"
#include "sharder.h"
#include "tx_operation_result.h"
#include "tx_request.h"

namespace txservice
{
TransactionExecution::TransactionExecution(CcHandler *_handler,
                                           TxLog *txlog,
                                           CcProtocol proto)
    : handler(_handler),
      txlog_(txlog),
      txid_(UINT32_MAX),
      tx_number_((uint64_t) UINT32_MAX << 32L),
      tx_term_(-1),
      commit_ts_(UINT64_MAX),
      commit_ts_bound_(0),
      tx_status_(TxnStatus::Ongoing),
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
      fault_inject_op_(this)
{
}

void TransactionExecution::Reset(CcProtocol proto)
{
    cache_miss_read_cce_addr_.SetCce(0, -1, 0);
    state_stack_.clear();
    txid_.Reset();
    tx_number_.store(UINT32_MAX, std::memory_order_release);
    tx_term_ = -1;
    commit_ts_ = UINT64_MAX;
    commit_ts_bound_ = 0;
    rw_set_.Reset();
    wset_iters_.clear();
    wset_reverse_iters_.clear();
    scans_.clear();
    void_resp_ = nullptr;
    rec_resp_ = nullptr;
    bool_resp_ = nullptr;
    kvp_resp_ = nullptr;
    uint64_resp_ = nullptr;
    detailed_error_msg_ = "";
    next_req_.store(nullptr);
    protocol_ = proto;
    schema_op_ = nullptr;
}

void TransactionExecution::Restart()
{
    tx_status_.store(TxnStatus::Ongoing, std::memory_order_release);
}

bool TransactionExecution::Idle() const
{
    return state_stack_.empty();
}

uint64_t TransactionExecution::TxNumber() const
{
    return tx_number_.load(std::memory_order_acquire);
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
            std::make_unique<UpsertTableOp>(schema_op.table_name(),
                                            schema_op.catalog_blob().data(),
                                            schema_op.catalog_blob().length(),
                                            table_msg.is_deleted(),
                                            this);

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

void TransactionExecution::Forward()
{
    if (state_stack_.empty())
    {
        return;
    }

    prev_op_ = state_stack_.back();
    prev_op_->Forward(this);
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
    state_stack_.push_back(op);
    op->retry_num_ = retry_num;
    op->is_running_ = false;
}

void TransactionExecution::ProcessTxRequest(InitTxRequest &init_txn_req)
{
    uint64_resp_ = &init_txn_req.tx_result_;
    uint64_resp_->Reset();
    iso_level_ = init_txn_req.iso_level_;
    protocol_ = init_txn_req.protocol_;

    PushOperation(&init_txn_);
    Process(init_txn_);
}

void TransactionExecution::ProcessTxRequest(ReadTxRequest &read_req)
{
    rec_resp_ = &read_req.tx_result_;
    rec_resp_->Reset();

    read_.read_type_ = ReadType::Inside;
    read_.read_tx_req_ = &read_req;
    PushOperation(&read_);
    Process(read_);
}

void TransactionExecution::ProcessTxRequest(
    ReadOutsideTxRequest &read_outside_req)
{
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
    uint64_resp_ = &scan_open_req.tx_result_;
    uint64_resp_->Reset();
    scan_open_.tx_req_ = &scan_open_req;
    PushOperation(&scan_open_);
    Process(scan_open_);
}

void TransactionExecution::ProcessTxRequest(ScanNextTxRequest &scan_next_req)
{
    kvp_resp_ = &scan_next_req.tx_result_;
    kvp_resp_->Reset();

    scan_next_.tx_req_ = &scan_next_req;
    PushOperation(&scan_next_);
    Process(scan_next_);
}

void TransactionExecution::ProcessTxRequest(ScanCloseTxRequest &scan_close_req)
{
    void_resp_ = &scan_close_req.tx_result_;
    void_resp_->Reset();

    ScanClose(scan_close_req.alias_, *scan_close_req.end_key_.get());
}

void TransactionExecution::ProcessTxRequest(UpsertTxRequest &upsert_req)
{
    void_resp_ = &upsert_req.tx_result_;
    void_resp_->Reset();
    Upsert(*upsert_req.tab_name_,
           upsert_req.key_,
           upsert_req.rec_,
           upsert_req.skeys_,
           upsert_req.is_delete_ ? DmlOperation::Delete : DmlOperation::Upsert);
}

void TransactionExecution::ProcessTxRequest(CommitTxRequest &commit_req)
{
    bool_resp_ = &commit_req.tx_result_;
    bool_resp_->Reset();
    Commit();
}

void TransactionExecution::ProcessTxRequest(AbortTxRequest &abort_req)
{
    bool_resp_ = &abort_req.tx_result_;
    bool_resp_->Reset();
    // When the tx is aborted/rolled back by the user, write intentions must
    // have not acquired. Clear the write set before entering post-processing.
    rw_set_.ClearWriteSet();
    Abort();
}

void TransactionExecution::ProcessTxRequest(UpsertTableTxRequest &req)
{
    bool_resp_ = &req.tx_result_;

    schema_op_ = std::make_unique<UpsertTableOp>(*req.table_name_,
                                                 req.catalog_image_,
                                                 req.catalog_length_,
                                                 req.is_deleted_,
                                                 this);

    PushOperation(schema_op_.get());
    Forward();
}

void TransactionExecution::ProcessTxRequest(FaultInjectTxRequest &fi_req)
{
    bool_resp_ = &fi_req.tx_result_;
    bool_resp_->Reset();

    fault_inject_op_.Set(
        fi_req.fault_name_, fi_req.fault_paras_, fi_req.vct_node_id_);
    PushOperation(&fault_inject_op_);
    Process(fault_inject_op_);
}

void TransactionExecution::Process(InitTxnOperation &init_txn)
{
    init_txn.is_running_ = true;
    commit_ts_ = 0;
    commit_ts_bound_ = 0;

    init_txn.Reset();

    handler->NewTxn(init_txn.hd_result_);
    init_txn.Forward(this);
    return;
}

void TransactionExecution::PostProcess(InitTxnOperation &init_txn)
{
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
    commit_ts_bound_ = init_result.start_ts_;
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
    read.Reset();
    read.is_running_ = true;
    if (read.read_type_ == ReadType::Inside)
    {
        const TableName &table_name = *read.read_tx_req_->tab_name_;
        const TxKey &key = *read.read_tx_req_->key_;
        TxRecord &rec = *read.read_tx_req_->rec_;
        ReadType read_type = read.read_tx_req_->type_;
        read.lock_type_ = read.read_tx_req_->lock_type_;

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
            read.read_type_ = read_type;
            read.iso_level_ = IsolationLevel::RepeatableRead;
            read.protocol_ = CcProtocol::Locking;

            handler->ReadLocal(table_name,
                               key,
                               rec,
                               read_type,
                               tx_number_.load(std::memory_order_relaxed),
                               tx_term_,
                               commit_ts_,
                               read.hd_result_,
                               IsolationLevel::RepeatableRead,
                               CcProtocol::Locking,
                               read.lock_type_);
        }
        else
        {
            // Step 1: fast path if key is update by the same tx.
            const WriteSetEntry *write = rw_set_.FindWrite(table_name, key);
            if (write != nullptr)
            {
                if (write->op_ == DmlOperation::Delete)
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
            if (rw_set_.cache_table_ == table_name &&
                rw_set_.cache_key_ != nullptr && *rw_set_.cache_key_ == key &&
                rw_set_.cache_rec_ != nullptr)
            {
                rec.Copy(*rw_set_.cache_rec_);
                state_stack_.pop_back();
                assert(state_stack_.empty());
                rec_resp_->Finish(RecordStatus::Normal);
                return;
            }

            read.read_type_ = read_type;
            read.protocol_ = protocol_;
            read.iso_level_ = iso_level_;

            rw_set_.cache_table_.clear();
            rw_set_.cache_table_ = table_name;
            rw_set_.cache_key_ = key.Clone();

            handler->Read(table_name,
                          key,
                          rec,
                          read_type,
                          tx_number_.load(std::memory_order_relaxed),
                          tx_term_,
                          commit_ts_,
                          read.hd_result_,
                          iso_level_,
                          protocol_,
                          read.lock_type_);

            StartTiming();

            return;
        }
    }
    else
    {
        TxRecord &record = read.read_outside_tx_req_->rec_;
        bool is_deleted = read.read_outside_tx_req_->is_deleted_;

        rw_set_.cache_rec_ = record.Clone();

        handler->ReadOutside(tx_term_,
                             record,
                             is_deleted,
                             cache_miss_read_cce_addr_,
                             read.hd_result_);

        return;
    }
}

void TransactionExecution::PostProcess(ReadOperation &read)
{
    state_stack_.pop_back();
    assert(state_stack_.empty());

    if (read_.hd_result_.IsError())
    {
        rw_set_.cache_table_.clear();
        rw_set_.cache_key_ = nullptr;
        rec_resp_->FinishError();
    }
    else
    {
        const ReadKeyResult &read_res = read_.hd_result_.Value();

        // optimization for case that we read the same key continuously
        // especially speed up remote read. e.g. Read A, Write B, Read A.
        if (read_res.rec_status_ == RecordStatus::Normal)
        {
            rw_set_.cache_rec_ = read_res.rec_->Clone();
        }

        if (read_res.rec_status_ != RecordStatus::RemoteUnknown &&
            read_.iso_level_ >= IsolationLevel::RepeatableRead)
        {
            rw_set_.AddRead(read_res.cce_addr_,
                            read_res.ts_,
                            read_.protocol_,
                            read_.read_type_,
                            read_.lock_type_);
        }

        if (read_.read_type_ == ReadType::Inside &&
            read_res.rec_status_ == RecordStatus::Unknown)
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
    const TableName &table_name = *scan_open.tx_req_->tab_name_;
    ScanIndexType index_type = scan_open.tx_req_->indx_type_;
    const TxKey &start_key = *scan_open.tx_req_->start_key_;
    bool inclusive = scan_open.tx_req_->inclusive_;
    ScanDirection direction = scan_open.tx_req_->direct_;
    bool is_ckpt_delta = scan_open.tx_req_->is_ckpt_delta_;

    scan_open.Reset();
    scan_open.is_running_ = true;
    scan_open.Set(&table_name,
                  index_type,
                  &start_key,
                  inclusive,
                  direction,
                  is_ckpt_delta);

    handler->ScanOpen(table_name,
                      index_type,
                      start_key,
                      inclusive,
                      tx_number_.load(std::memory_order_relaxed),
                      tx_term_,
                      commit_ts_bound_,
                      scan_open.hd_result_,
                      direction,
                      iso_level_,
                      protocol_,
                      LockType::ReadLock,
                      is_ckpt_delta);

    StartTiming();

    return;
}

void TransactionExecution::PostProcess(ScanOpenOperation &scan_open)
{
    state_stack_.pop_back();
    assert(state_stack_.empty());

    if (scan_open_.hd_result_.IsError())
    {
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
        handler->ScanNextBatch(tx_number_.load(std::memory_order_relaxed),
                               tx_term_,
                               commit_ts_bound_,
                               scanner,
                               scan_next.hd_result_,
                               iso_level_,
                               protocol_,
                               LockType::ReadLock);
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
    prev_op_ = state_stack_.back();
    state_stack_.pop_back();
    assert(state_stack_.empty());

    if (scan_next.hd_result_.IsError())
    {
        kvp_resp_->FinishError();
        return;
    }

    const ScanTuple *cc_scan_tuple = scan_next.scanner_->Current();

    //  cc_scan_tuple->key_ts_ == 0 means it is backfill entry and thus data
    //  store already contains this entry. Since the final scan result is the
    //  merge of memory entries with data store entries, as a result it's safe
    //  to skip these backfill entries.
    while (cc_scan_tuple != nullptr &&
           (cc_scan_tuple->key_ts_ == 0 ||
            cc_scan_tuple->rec_status_ == RecordStatus::Unknown))
    {
        scan_next.scanner_->MoveNext();
        cc_scan_tuple = scan_next.scanner_->Current();

        if (cc_scan_tuple == nullptr &&
            scan_next.scanner_->Status() == ScannerStatus::Blocked)
        {
            scan_next.hd_result_.Reset();
            handler->ScanNextBatch(tx_number_.load(std::memory_order_relaxed),
                                   tx_term_,
                                   commit_ts_bound_,
                                   *scan_next.scanner_,
                                   scan_next.hd_result_,
                                   iso_level_,
                                   protocol_);
            // put scannext_op into state stack since we need to scan the ccmap
            // again. Note that we should not call PushOperation() since we have
            // already triggerred the ScanNextBatch.
            state_stack_.push_back(prev_op_);
            return;
        }
    }

    assert(cc_scan_tuple != nullptr ||
           scan_next.scanner_->Status() == ScannerStatus::Closed);

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
                    kvp_resp_->Finish(std::make_tuple(
                        cc_scan_tuple->Key(), cc_scan_tuple->Record(), false));
                }
                else
                {
                    assert(cc_scan_tuple->rec_status_ == RecordStatus::Deleted);
                    if (scan_next.scanner_->is_ckpt_delta_)
                    {
                        kvp_resp_->Finish(
                            std::make_tuple(cc_scan_tuple->Key(),
                                            cc_scan_tuple->Record(),
                                            true));
                    }
                    else
                    {
                        kvp_resp_->Finish(std::make_tuple(
                            cc_scan_tuple->Key(), nullptr, true));
                    }
                }

                scan_next.scanner_->MoveNext();
            }
            else
            {
                kvp_resp_->Finish(std::make_tuple(nullptr, nullptr, true));
            }
            return;
        }

        const WriteSetEntry &local_write = it->second.first->second;

        // Case that needs to merge ccm entries with local write set entries.
        if (cc_scan_tuple == nullptr ||
            *local_write.key_.get() < *cc_scan_tuple->Key())
        {
            // Returns the key-value pair in the local write set.
            if (local_write.op_ == DmlOperation::Delete)
            {
                kvp_resp_->Finish(
                    std::make_tuple(local_write.key_.get(), nullptr, true));
            }
            else
            {
                kvp_resp_->Finish(std::make_tuple(
                    local_write.key_.get(), local_write.rec_.get(), false));
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
                kvp_resp_->Finish(std::make_tuple(
                    cc_scan_tuple->Key(), cc_scan_tuple->Record(), false));
            }
            else
            {
                assert(cc_scan_tuple->rec_status_ == RecordStatus::Deleted);
                kvp_resp_->Finish(
                    std::make_tuple(cc_scan_tuple->Key(), nullptr, true));
            }
            scan_next.scanner_->MoveNext();
        }
        else if (*cc_scan_tuple->Key() == *local_write.key_.get())
        {
            // Returns the key-value pair in the local write set.
            if (local_write.op_ == DmlOperation::Delete)
            {
                kvp_resp_->Finish(
                    std::make_tuple(local_write.key_.get(), nullptr, true));
            }
            else
            {
                kvp_resp_->Finish(std::make_tuple(
                    local_write.key_.get(), local_write.rec_.get(), false));
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
                    kvp_resp_->Finish(std::make_tuple(
                        cc_scan_tuple->Key(), cc_scan_tuple->Record(), false));
                }
                else
                {
                    assert(cc_scan_tuple->rec_status_ == RecordStatus::Deleted);
                    kvp_resp_->Finish(
                        std::make_tuple(cc_scan_tuple->Key(), nullptr, true));
                }

                scan_next.scanner_->MoveNext();
            }
            else
            {
                kvp_resp_->Finish(std::make_tuple(nullptr, nullptr, true));
            }
            return;
        }

        const WriteSetEntry &local_write = rit->second.first->second;

        if (cc_scan_tuple == nullptr ||
            *cc_scan_tuple->Key() < *local_write.key_.get())
        {
            // Returns the key-value pair in the local write set.
            if (local_write.op_ == DmlOperation::Delete)
            {
                kvp_resp_->Finish(
                    std::make_tuple(local_write.key_.get(), nullptr, true));
            }
            else
            {
                kvp_resp_->Finish(std::make_tuple(
                    local_write.key_.get(), local_write.rec_.get(), false));
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
                kvp_resp_->Finish(std::make_tuple(
                    cc_scan_tuple->Key(), cc_scan_tuple->Record(), false));
            }
            else
            {
                assert(cc_scan_tuple->rec_status_ == RecordStatus::Deleted);
                kvp_resp_->Finish(
                    std::make_tuple(cc_scan_tuple->Key(), nullptr, true));
            }

            scan_next.scanner_->MoveNext();
        }
        else if (*cc_scan_tuple->Key() == *local_write.key_.get())
        {
            // Returns the key-value pair in the local write set.
            if (local_write.op_ == DmlOperation::Delete)
            {
                kvp_resp_->Finish(
                    std::make_tuple(local_write.key_.get(), nullptr, true));
            }
            else
            {
                kvp_resp_->Finish(std::make_tuple(
                    local_write.key_.get(), local_write.rec_.get(), false));
            }

            ++rit->second.first;
            scan_next.scanner_->MoveNext();
        }
    }
}

void TransactionExecution::ScanClose(size_t alias, const TxKey &end_key)
{
    handler->ScanClose(alias, end_key, false);
    scans_.erase(alias);
    void_resp_->Finish(void_);
}

void TransactionExecution::Update(const TableName &table_name,
                                  TxKeyContainer &key,
                                  TxRecordContainer &rec,
                                  std::vector<SecondaryKeyInfo> *skeys)
{
    Upsert(table_name, key, rec, skeys, DmlOperation::Update);
}

void TransactionExecution::Insert(const TableName &table_name,
                                  TxKeyContainer &key,
                                  TxRecordContainer &rec,
                                  std::vector<SecondaryKeyInfo> *skeys)
{
    Upsert(table_name, key, rec, skeys, DmlOperation::Insert);
}

void TransactionExecution::Delete(const TableName &table_name,
                                  TxKeyContainer &key,
                                  std::vector<SecondaryKeyInfo> *skeys)
{
    TxRecordContainer rcon(nullptr);
    Upsert(table_name, key, rcon, skeys, DmlOperation::Delete);
}

// Upsert modify tuple without locking in OCC protocol.
void TransactionExecution::Upsert(const TableName &table_name,
                                  TxKeyContainer &key,
                                  TxRecordContainer &rec,
                                  std::vector<SecondaryKeyInfo> *skeys,
                                  DmlOperation op)
{
    rw_set_.AddWrite(table_name, key, rec, op, skeys);
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
    size_t wset_size = rw_set_.WriteSetSize();
    acquire_write.Reset(wset_size);
    acquire_write.is_running_ = true;

    size_t idx = 0;
    std::unordered_map<TableName, TableWriteSet> &wset = rw_set_.WriteSet();
    for (auto table_it = wset.begin(); table_it != wset.end(); ++table_it)
    {
        for (auto key_it = table_it->second.begin();
             key_it != table_it->second.end();
             ++key_it)
        {
            CcHandlerResult<AcquireKeyResult> &hres =
                acquire_write.results_[idx];
            hres.Reset();
            hres.Value().remote_ack_cnt_ = &acquire_write.remote_ack_cnt_;
            WriteSetEntry &write_entry = key_it->second;
            acquire_write.acquire_write_entries_.at(idx) = &write_entry;
            handler->AcquireWrite(table_it->first,
                                  *write_entry.key_.get(),
                                  txid_,
                                  tx_term_,
                                  commit_ts_bound_,
                                  write_entry.op_ == DmlOperation::Insert,
                                  hres,
                                  protocol_);
            ++idx;
        }
    }

    StartTiming();
}

void TransactionExecution::PostProcess(AcquireWriteOperation &acquire_write)
{
    state_stack_.pop_back();
    assert(state_stack_.empty());

    if (acquire_write.fail_cnt_.load(std::memory_order_acquire) > 0)
    {
        SetErrorMessage("Transaction abort: failed to acquire write lock.");
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
    uint64_t candidate = commit_ts_bound_;

    set_ts.is_running_ = true;
    for (size_t idx = 0; idx < acquire_write_.acquire_write_cnt_; ++idx)
    {
        uint64_t acquire_write_ts =
            acquire_write_.results_[idx].Value().last_vali_ts_;
        candidate = std::max(candidate, acquire_write_ts + 1);
    }

    const std::unordered_map<CcEntryAddr, ReadSetEntry> &rset =
        rw_set_.ReadSet();
    for (auto read_it = rset.begin(); read_it != rset.end(); ++read_it)
    {
        candidate = std::max(candidate, read_it->second.version_ts_ + 1);
    }

    handler->SetCommitTimestamp(txid_, candidate, set_ts.hd_result_);
    set_ts.Forward(this);
}

void TransactionExecution::PostProcess(SetCommitTsOperation &set_ts)
{
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
    size_t offset = 0;
    const std::unordered_map<CcEntryAddr, ReadSetEntry> &rset =
        rw_set_.ReadSet();

    validate.Reset(rw_set_.ReadSetSize());
    validate.vali_cce_addr_.clear();
    validate.is_running_ = true;

    for (const auto &[cce_addr, read_entry] : rset)
    {
        validate_.vali_cce_addr_.emplace_back(&cce_addr);

        CcHandlerResult<std::vector<TxId>> &hres = validate.results_[offset];
        hres.Reset();

        handler->PostRead(tx_number_.load(std::memory_order_relaxed),
                          tx_term_,
                          read_entry.version_ts_,
                          0,
                          commit_ts_,
                          cce_addr,
                          hres,
                          read_entry.protocol_,
                          read_entry.lock_type_);

        ++offset;
    }
}

void TransactionExecution::PostProcess(ValidateOperation &validate)
{
    // The validation step is optional. Only pops the stack if the last step is
    // the validation step.
    if (!state_stack_.empty())
    {
        assert(state_stack_.back() == &validate_);
        state_stack_.pop_back();
    }

    if (validate.error_.load(std::memory_order_acquire))
    {
        SetErrorMessage("Transaction abort: validation failed.");
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

    log_rec->set_txn_number(txid_.TxNumber());
    log_rec->set_commit_timestamp(commit_ts_);

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
        std::unordered_map<TableName,
                           std::vector<std::variant<const WriteSetEntry *,
                                                    const SecondaryKeyInfo *>>>>
        ng_table_rec_set;

    // reorganize all WriteSetEntries from old structure to new structure
    for (const auto &[table_name, table_write_set] : wset)
    {
        for (const auto &[key_ptr, write_set_entry] : table_write_set)
        {
            const CcEntryAddr &addr = write_set_entry.cce_addr_;

            auto shard_term_it = shard_terms->find(addr.NodeGroupId());
            if (shard_term_it == shard_terms->end())
            {
                (*shard_terms)[addr.NodeGroupId()] = addr.Term();
            }
            else if (shard_term_it->second != addr.Term())
            {
                // Two keys in the tx's write set refer to the same cc node
                // group, but have different terms. It means that the cc node
                // must have failed over at least once and the tx have obtained
                // a write intention before the failure. The tx must abort
                // because the write intention obtained before the failure have
                // been invalidated.
                write_log.hd_result_.SetError(1);
                return;
            }

            auto table_rec_it =
                ng_table_rec_set.try_emplace(addr.NodeGroupId());
            std::unordered_map<
                TableName,
                std::vector<std::variant<const WriteSetEntry *,
                                         const SecondaryKeyInfo *>>>
                &table_rec_set = table_rec_it.first->second;

            auto rec_vec_it = table_rec_set.try_emplace(table_name);
            rec_vec_it.first->second.emplace_back(
                std::in_place_type<const WriteSetEntry *>, &write_set_entry);

            if (!write_set_entry.sindx_.empty())
            {
                for (auto &sk_info : write_set_entry.sindx_)
                {
                    // use sk and pk to get a HashCode
                    const TxKey *sk = sk_info.sk_key_.get();
                    const TxKey *pk = sk_info.parent_entry_->key_.get();

                    // get cc_node group id for secondary index entry
                    uint32_t shard_code = Sharder::Instance().ShardCode(
                        TxKey::HashCode(*sk, *pk));
                    uint32_t sk_ng_id = shard_code >> 10;

                    auto table_rec_it = ng_table_rec_set.try_emplace(sk_ng_id);
                    std::unordered_map<
                        TableName,
                        std::vector<std::variant<const WriteSetEntry *,
                                                 const SecondaryKeyInfo *>>>
                        &table_rec_set = table_rec_it.first->second;

                    auto rec_vec_it =
                        table_rec_set.try_emplace(*sk_info.sk_index_name_);
                    rec_vec_it.first->second.emplace_back(
                        std::in_place_type<const SecondaryKeyInfo *>, &sk_info);
                }
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

        // The log blob of a table in a node group is in the following format:
        // (1) A 1-byte integer for the length of the table name, followed by
        // (2) The string of the table name.
        // (3) A 4-byte integer for the total length of serialized key-record
        // pairs modified by the tx in the node group.
        // (4) A sequence of modified records. Each record is encoded as
        // follows:
        //   (a) The serialized key
        //   (b) A 1-byte flag to indicate if the record is normal, deleted or
        //   void.
        //   (c) The serialized record if the record is normal.
        for (const auto &[table_name, variant_entry_vec] : table_rec_set)
        {
            uint8_t tabname_len = table_name.length();
            const char *ptr = reinterpret_cast<const char *>(&tabname_len);
            log_ng_blob->append(ptr, sizeof(uint8_t));
            log_ng_blob->append(table_name.data(), tabname_len);

            // The start position of the 4-byte integer for the length of
            // serialized k-v pairs.
            size_t kv_len_start = log_ng_blob->size();
            uint32_t kv_len = 0;
            ptr = reinterpret_cast<const char *>(&kv_len);
            // Reserves 4 bytes in the blob for the k-v length before committed
            // records are serialized and the length of the serialized records
            // are known.
            log_ng_blob->append(ptr, sizeof(uint32_t));

            for (auto variant_entry : variant_entry_vec)
            {
                if (variant_entry.index() == 0)
                {
                    const WriteSetEntry *wset_entry =
                        std::get<const WriteSetEntry *>(variant_entry);

                    wset_entry->key_.get()->Serialize(*log_ng_blob);

                    uint8_t delete_flag =
                        wset_entry->op_ == DmlOperation::Delete ? 1 : 0;
                    log_ng_blob->append(
                        reinterpret_cast<const char *>(&delete_flag), 1);

                    if (wset_entry->op_ != DmlOperation::Delete &&
                        wset_entry->rec_.get() != nullptr)
                    {
                        wset_entry->rec_.get()->Serialize(*log_ng_blob);
                    }
                }
                else
                {
                    const SecondaryKeyInfo *sk_info =
                        std::get<const SecondaryKeyInfo *>(variant_entry);

                    // Serialize sk and pk into log_ng_blob.
                    sk_info->sk_key_.get()->Serialize(*log_ng_blob);
                    sk_info->parent_entry_->key_.get()->Serialize(*log_ng_blob);

                    uint8_t delete_flag = sk_info->is_deleted_ == true ? 1 : 0;
                    log_ng_blob->append(
                        reinterpret_cast<const char *>(&delete_flag), 1);

                    // A secondary index entry has no payload.
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
    WriteToLogOp *log_op = static_cast<WriteToLogOp *>(state_stack_.back());
    state_stack_.pop_back();

    if (state_stack_.empty())
    {
        if (log_op->hd_result_.IsError())
        {
            SetErrorMessage("Transaction abort: failed to write log.");
            tx_status_.store(TxnStatus::Aborted, std::memory_order_release);
        }
        else
        {
            tx_status_.store(TxnStatus::Committed, std::memory_order_release);
        }
        PushOperation(&update_txn_);
        Process(update_txn_);
    }
    else
    {
        // The tx is committing a multi-stage operation, e.g., schema changes.
        Forward();
    }
}

void TransactionExecution::Process(UpdateTxnStatus &update_txn)
{
    update_txn.Reset();
    update_txn.is_running_ = true;
    handler->UpdateTxnStatus(txid_,
                             tx_status_.load(std::memory_order_relaxed),
                             update_txn.hd_result_);
    update_txn.Forward(this);
}

void TransactionExecution::PostProcess(UpdateTxnStatus &update_txn)
{
    state_stack_.pop_back();

    int wset_intention_cnt =
        rw_set_.WriteSetSize() > 0
            ? acquire_write_.acquire_write_cnt_ -
                  acquire_write_.fail_cnt_.load(std::memory_order_acquire)
            : 0;
    if (wset_intention_cnt != 0 || rw_set_.ReadSetSize() != 0)
    {
        post_process_.read_intention_size_ = rw_set_.ReadSetSize();
        post_process_.write_intention_size_ = wset_intention_cnt;
        PushOperation(&post_process_);
        Process(post_process_);
    }
    else
    {
        // For tx's that have finished validation and have not uploaded
        // anything, skips post-processing.

        if (tx_status_.load(std::memory_order_relaxed) == TxnStatus::Committed)
        {
            bool_resp_->Finish(true);
        }
        else if (tx_status_.load(std::memory_order_relaxed) ==
                 TxnStatus::Aborted)
        {
            bool_resp_->Finish(false);
        }

        // transaction can be recycled and put into free list.
        tx_status_.store(TxnStatus::Finished, std::memory_order_release);

        CcHandlerResult<Void> fake_result(this);
        handler->UpdateTxnStatus(
            txid_, tx_status_.load(std::memory_order_relaxed), fake_result);

        Reset();
    }
}

void TransactionExecution::Process(PostProcessOp &post_process)
{
    size_t read_intention_size = post_process.read_intention_size_;
    size_t write_intention_size = post_process.write_intention_size_;
    post_process.is_running_ = true;

    if (tx_status_.load(std::memory_order_relaxed) == TxnStatus::Committed)
    {
        post_process.Reset(0, rw_set_.WriteSetSize());

        size_t idx = 0;
        std::unordered_map<TableName, TableWriteSet> &wset = rw_set_.WriteSet();
        for (auto table_it = wset.begin(); table_it != wset.end(); ++table_it)
        {
            for (auto key_it = table_it->second.begin();
                 key_it != table_it->second.end();
                 ++key_it, ++idx)
            {
                WriteSetEntry &write_entry = key_it->second;
                CcHandlerResult<Void> &hres = post_process.write_results_[idx];
                hres.Reset();
                if (write_entry.sindx_.size() > 0)
                {
                    hres.SetRefCnt((uint32_t) write_entry.sindx_.size() + 1);
                }

                handler->PostWrite(tx_number_.load(std::memory_order_relaxed),
                                   tx_term_,
                                   commit_ts_,
                                   write_entry.cce_addr_,
                                   write_entry.rec_.get(),
                                   write_entry.op_ == DmlOperation::Delete,
                                   hres);

                for (auto sk_iter = write_entry.sindx_.begin();
                     sk_iter != write_entry.sindx_.end();
                     ++sk_iter)
                {
                    const TableName *tn = sk_iter->sk_index_name_;
                    const TxKey *sk = sk_iter->sk_key_.get();
                    bool is_delete = sk_iter->is_deleted_;

                    handler->CommitSecondaryKey(*tn,
                                                *sk,
                                                *write_entry.key_.get(),
                                                is_delete,
                                                commit_ts_,
                                                hres);
                }
            }
        }
    }
    else
    {
        // If the tx has finished validation, the read intentions/locks of the
        // read-set keys have been cleared after validation. Post-processing
        // only clears the write locks of the write-set keys. If the tx failed
        // during the acquire phase or was aborted before entering the commit
        // phase, post-processing removes write intentions of write-set keys and
        // clears read intentions/locks of read-set keys.

        post_process.Reset(read_intention_size, write_intention_size);

        size_t offset = 0;
        size_t idx = 0;
        const std::unordered_map<TableName, TableWriteSet> &wset =
            rw_set_.WriteSet();
        for (const auto &[table_name, table_write_set] : wset)
        {
            for (const auto &[key, write_entry] : table_write_set)
            {
                if (acquire_write_.results_[idx].IsError())
                {
                    // Keys that were not successfully acquire write lock in the
                    // cc map do not need post-processing.
                    ++idx;
                    continue;
                }
                assert(!write_entry.cce_addr_.Empty());

                CcHandlerResult<Void> &hres =
                    post_process.write_results_[offset];
                hres.Reset();

                handler->PostWrite(tx_number_.load(std::memory_order_relaxed),
                                   tx_term_,
                                   0,
                                   write_entry.cce_addr_,
                                   nullptr,
                                   false,
                                   hres);

                ++offset;
                ++idx;
            }
        }
        assert(offset == write_intention_size);

        idx = 0;
        const std::unordered_map<CcEntryAddr, ReadSetEntry> &rset =
            rw_set_.ReadSet();
        for (auto read_it = rset.begin(); read_it != rset.end();
             ++read_it, ++idx)
        {
            CcHandlerResult<std::vector<TxId>> &hres =
                post_process.read_results_[idx];
            hres.Reset();

            handler->PostRead(tx_number_.load(std::memory_order_relaxed),
                              tx_term_,
                              0,
                              0,
                              0,
                              read_it->first,
                              hres,
                              read_it->second.protocol_,
                              read_it->second.lock_type_);
        }
    }

    StartTiming();
}

void TransactionExecution::PostProcess(PostProcessOp &post_process)
{
    if (!state_stack_.empty())
    {
        assert(state_stack_.back() == &post_process);
        state_stack_.pop_back();
    }
    assert(state_stack_.empty());

    if (tx_status_.load(std::memory_order_relaxed) == TxnStatus::Committed)
    {
        bool_resp_->Finish(true);
    }
    else if (tx_status_.load(std::memory_order_relaxed) == TxnStatus::Aborted)
    {
        bool_resp_->Finish(false);
    }

    // transaction can be recycled and put into free list.
    tx_status_.store(TxnStatus::Finished, std::memory_order_release);

    CcHandlerResult<Void> fake_result(this);
    handler->UpdateTxnStatus(
        txid_, tx_status_.load(std::memory_order_relaxed), fake_result);

    Reset();
}

void TransactionExecution::Process(AcquireAllOp &acq_all_op)
{
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
                                 false,
                                 hres,
                                 acq_all_op.protocol_,
                                 acq_all_op.lk_type_);
    }

    StartTiming();
}

void TransactionExecution::PostProcess(AcquireAllOp &acq_all_op)
{
    state_stack_.pop_back();
    Forward();
}

void TransactionExecution::Process(PostWriteAllOp &post_write_all_op)
{
    uint32_t node_group_cnt = Sharder::Instance().NodeGroupCount();
    post_write_all_op.Reset(node_group_cnt);
    post_write_all_op.is_running_ = true;

    for (uint32_t nid = 0; nid < node_group_cnt; ++nid)
    {
        CcHandlerResult<Void> &hres = post_write_all_op.hd_results_[nid];
        hres.Reset();
        handler->PostWriteAll(*post_write_all_op.table_name_,
                              *post_write_all_op.key_,
                              *post_write_all_op.rec_,
                              nid,
                              tx_number_.load(std::memory_order_relaxed),
                              tx_term_,
                              commit_ts_,
                              hres,
                              post_write_all_op.dml_op_,
                              post_write_all_op.write_type_);
    }

    StartTiming();
}

void TransactionExecution::PostProcess(PostWriteAllOp &post_write_all_op)
{
    state_stack_.pop_back();
    // So far, post-write-all is only used for schema evolution operations.
    assert(!state_stack_.empty());
    Forward();
}

void TransactionExecution::Process(DsUpsertTableOp &ds_upsert_table_op)
{
    ds_upsert_table_op.Reset();
    ds_upsert_table_op.is_running_ = true;
    handler->DataStoreUpsertTable(*ds_upsert_table_op.table_name_,
                                  ds_upsert_table_op.table_schema_,
                                  ds_upsert_table_op.is_deleted_,
                                  commit_ts_,
                                  ds_upsert_table_op.hd_result_);
}

void TransactionExecution::PostProcess(DsUpsertTableOp &ds_upsert_table_op)
{
    state_stack_.pop_back();
    assert(!state_stack_.empty());
    Forward();
}

void TransactionExecution::Process(FaultInjectOp &fault_inject_op_)
{
    fault_inject_op_.Reset();
    fault_inject_op_.is_running_ = true;

    handler->FaultInject(fault_inject_op_.fault_name_,
                         fault_inject_op_.fault_paras_,
                         tx_term_,
                         txid_,
                         fault_inject_op_.vct_node_id_,
                         fault_inject_op_.hd_result_);
    return;
}

void TransactionExecution::PostProcess(FaultInjectOp &fault_inject_op_)
{
    state_stack_.pop_back();
    assert(state_stack_.empty());

    bool_resp_->Finish(fault_inject_op_.succeed_);
}
}  // namespace txservice
