#include "tx_execution.h"

#include <stdint.h>

#include <cassert>
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
      fault_inject_op(this)
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

void TransactionExecution::Read(const TableName &table_name,
                                const TxKey &key,
                                TxRecord &rec,
                                ReadType read_type)
{
    // Step 1: fast path if key is update by the same tx.
    const WriteSetEntry *write = rw_set_.FindWrite(table_name, key);
    if (write != nullptr)
    {
        if (write->op_ == DmlOperation::Delete)
        {
            rec_resp_->Finish(RecordStatus::Deleted);
        }
        else
        {
            rec.Copy(*write->rec_.get());
            rec_resp_->Finish(RecordStatus::Normal);
        }
        return;
    }

    // Step 2: fast path if key is the same as last read key.
    if (rw_set_.cache_table_ == table_name && rw_set_.cache_key_ != nullptr &&
        *rw_set_.cache_key_ == key && rw_set_.cache_rec_ != nullptr)
    {
        rec.Copy(*rw_set_.cache_rec_);
        rec_resp_->Finish(RecordStatus::Normal);
        return;
    }

    state_stack_.push_back(&read_);
    read_.Reset();
    read_.read_type_ = read_type;
    read_.protocol_ = protocol_;
    read_.iso_level_ = iso_level_;

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
                  read_.cc_result_,
                  iso_level_,
                  protocol_);

    StartTiming();

    return;
}

void TransactionExecution::PostRead()
{
    state_stack_.pop_back();
    assert(state_stack_.empty());

    if (read_.cc_result_.IsError())
    {
        rw_set_.cache_table_.clear();
        rw_set_.cache_key_ = nullptr;
        rec_resp_->FinishError();
    }
    else
    {
        const ReadKeyResult &read_res = read_.cc_result_.Value();

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
                            read_.read_type_);
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

void TransactionExecution::ReadOutside(TxRecord &record, bool is_deleted)
{
    rw_set_.cache_rec_ = record.Clone();

    state_stack_.push_back(&read_);
    read_.Reset();
    read_.read_type_ =
        is_deleted ? ReadType::OutsideDeleted : ReadType::OutsideNormal;

    handler->ReadOutside(tx_term_,
                         record,
                         is_deleted,
                         cache_miss_read_cce_addr_,
                         read_.cc_result_);

    return;
}

void TransactionExecution::ReadLocal(const TableName &table_name,
                                     const TxKey &key,
                                     TxRecord &record,
                                     ReadType read_type)
{
    state_stack_.push_back(&read_);
    read_.Reset();

    // So far ReadLocal() is use exclusively for reading catalogs. Reading
    // catalogs needs to put read locks, regardless of the tx's concurrency
    // control protocol. So for now, a read's isolation level and cc protocol is
    // fixed.
    read_.read_type_ = read_type;
    read_.iso_level_ = IsolationLevel::RepeatableRead;
    read_.protocol_ = CcProtocol::Locking;

    handler->ReadLocal(table_name,
                       key,
                       record,
                       read_type,
                       tx_number_.load(std::memory_order_relaxed),
                       tx_term_,
                       commit_ts_,
                       read_.cc_result_,
                       IsolationLevel::RepeatableRead,
                       CcProtocol::Locking);
}

void TransactionExecution::PostScanClose()
{
}

void TransactionExecution::ScanOpen(const TableName &table_name,
                                    ScanIndexType index_type,
                                    const TxKey &start_key,
                                    bool inclusive,
                                    ScanDirection direction,
                                    bool is_ckpt_delta)
{
    state_stack_.push_back(&scan_open_);
    scan_open_.cc_result_.Reset();

    scan_open_.Set(&table_name,
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
                      scan_open_.cc_result_,
                      direction,
                      iso_level_,
                      protocol_,
                      is_ckpt_delta);

    StartTiming();

    return;
}

void TransactionExecution::PostScanOpen()
{
    state_stack_.pop_back();
    assert(state_stack_.empty());

    if (scan_open_.cc_result_.IsError())
    {
        uint64_resp_->FinishError();
        return;
    }

    ScanOpenResult &open_result = scan_open_.cc_result_.Value();

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

void TransactionExecution::ScanNext(size_t alias)
{
    auto it = scans_.find(alias);
    assert(it != scans_.end());
    CcScanner &scanner = *it->second;

    scan_next_.Reset();
    state_stack_.push_back(&scan_next_);
    scan_next_.Set(alias, &scanner);

    const ScanTuple *scan_tuple = scanner.Current();

    if (scan_tuple == nullptr && scanner.Status() == ScannerStatus::Blocked)
    {
        scan_next_.cc_result_.Reset();
        handler->ScanNextBatch(tx_number_.load(std::memory_order_relaxed),
                               tx_term_,
                               commit_ts_bound_,
                               scanner,
                               scan_next_.cc_result_,
                               iso_level_,
                               protocol_);
    }
    else
    {
        scan_next_.cc_result_.SetFinished();
    }

    StartTiming();

    // Scanning next is only blocked when one of the shards' cache is drained.
    // Invokes Forward() to move forward the tx machine.
    scan_next_.Forward(this);

    return;
}

void TransactionExecution::ScanClose(size_t alias, const TxKey &end_key)
{
    handler->ScanClose(alias, end_key, false);
    scans_.erase(alias);
    void_resp_->Finish(void_);
}

void TransactionExecution::PostScanNext()
{
    prev_op_ = state_stack_.back();
    state_stack_.pop_back();
    assert(state_stack_.empty());

    if (scan_next_.cc_result_.IsError())
    {
        kvp_resp_->FinishError();
        return;
    }

    const ScanTuple *cc_scan_tuple = scan_next_.scanner_->Current();

    //  cc_scan_tuple->key_ts_ == 0 means it is backfill entry and thus data
    //  store already contains this entry. Since the final scan result is the
    //  merge of memory entries with data store entries, as a result it's safe
    //  to skip these backfill entries.
    while (cc_scan_tuple != nullptr &&
           (cc_scan_tuple->key_ts_ == 0 ||
            cc_scan_tuple->rec_status_ == RecordStatus::Unknown))
    {
        scan_next_.scanner_->MoveNext();
        cc_scan_tuple = scan_next_.scanner_->Current();

        if (cc_scan_tuple == nullptr &&
            scan_next_.scanner_->Status() == ScannerStatus::Blocked)
        {
            scan_next_.cc_result_.Reset();
            handler->ScanNextBatch(tx_number_.load(std::memory_order_relaxed),
                                   tx_term_,
                                   commit_ts_bound_,
                                   *scan_next_.scanner_,
                                   scan_next_.cc_result_,
                                   iso_level_,
                                   protocol_);
            state_stack_.push_back(prev_op_);
            return;
        }
    }

    assert(cc_scan_tuple != nullptr ||
           scan_next_.scanner_->Status() == ScannerStatus::Closed);

    if (scan_next_.scanner_->Direction() == ScanDirection::Forward)
    {
        auto it = wset_iters_.find(scan_next_.alias_);

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
                    if (scan_next_.scanner_->is_ckpt_delta_)
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

                scan_next_.scanner_->MoveNext();
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
            scan_next_.scanner_->MoveNext();
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
            scan_next_.scanner_->MoveNext();
        }
    }
    // backward scan
    else
    {
        auto rit = wset_reverse_iters_.find(scan_next_.alias_);

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

                scan_next_.scanner_->MoveNext();
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

            scan_next_.scanner_->MoveNext();
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
            scan_next_.scanner_->MoveNext();
        }
    }
}

void TransactionExecution::Update(const TableName &table_name,
                                  TxKeyContainer &key,
                                  TxRecordContainer &rec,
                                  SecondaryKeys *skeys)
{
    Upsert(table_name, key, rec, skeys, DmlOperation::Update);
}

void TransactionExecution::Insert(const TableName &table_name,
                                  TxKeyContainer &key,
                                  TxRecordContainer &rec,
                                  SecondaryKeys *skeys)
{
    Upsert(table_name, key, rec, skeys, DmlOperation::Insert);
}

void TransactionExecution::Delete(const TableName &table_name,
                                  TxKeyContainer &key,
                                  SecondaryKeys *skeys)
{
    TxRecordContainer rcon(nullptr);
    Upsert(table_name, key, rcon, skeys, DmlOperation::Delete);
}

// Upsert modify tuple without locking in OCC protocol.
void TransactionExecution::Upsert(const TableName &table_name,
                                  TxKeyContainer &key,
                                  TxRecordContainer &rec,
                                  SecondaryKeys *skeys,
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
        AcquireWrite();
    }
    else
    {
        SetTs();
    }

    return;
}

void TransactionExecution::FaultInject(const std::string &fault_name,
                                       const std::string &fault_type,
                                       int node_id)
{
    state_stack_.push_back(&fault_inject_op);

    CcHandlerResult<bool> &hres = fault_inject_op.cc_result_;
    hres.Reset();

    handler->FaultInject(
        fault_name, fault_type, tx_term_, txid_, node_id, hres);

    return;
}

void TransactionExecution::AcquireWrite()
{
    state_stack_.push_back(&acquire_write_);

    size_t wset_size = rw_set_.WriteSetSize();
    acquire_write_.Reset(wset_size);

    size_t idx = 0;
    std::unordered_map<TableName, TableWriteSet> &wset = rw_set_.WriteSet();
    for (auto table_it = wset.begin(); table_it != wset.end(); ++table_it)
    {
        for (auto key_it = table_it->second.begin();
             key_it != table_it->second.end();
             ++key_it)
        {
            CcHandlerResult<AcquireKeyResult> &hres =
                acquire_write_.results_[idx];
            hres.Reset();
            hres.Value().remote_ack_cnt_ = &acquire_write_.remote_ack_cnt_;
            WriteSetEntry &write_entry = key_it->second;
            acquire_write_.acquire_write_entries_.at(idx) = &write_entry;
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

void TransactionExecution::RequestFinish(bool succeed)
{
    state_stack_.pop_back();
    assert(state_stack_.empty());

    bool_resp_->Finish(succeed);
}

void TransactionExecution::PostAcquireWrite()
{
    state_stack_.pop_back();
    assert(state_stack_.empty());

    if (acquire_write_.fail_cnt_.load(std::memory_order_acquire) > 0)
    {
        Abort();
    }
    else
    {
        SetTs();
    }
}

void TransactionExecution::SetTs()
{
    state_stack_.push_back(&set_ts_);

    set_ts_.result_of_set_commit_ts_.Reset();

    uint64_t candidate = commit_ts_bound_;
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

    /*const std::unordered_map<
            TableName,
            std::map<const TxKey *, ScanSetEntry, PtrLessThan<TxKey>>> &sset_map
    = rw_set_.ScanSet();

    for (auto it = sset_map.begin(); it != sset_map.end(); ++it)
    {
            const std::map<const TxKey *, ScanSetEntry, PtrLessThan<TxKey>>
    &sset = it->second; for (auto sset_it = sset.begin(); sset_it != sset.end();
    ++sset_it)
            {
                    candidate = std::max(candidate, sset_it->second.gap_ts_ +
    1); candidate = std::max(candidate, sset_it->second.key_ts_ + 1);
            }
    }*/

    handler->SetCommitTimestamp(
        txid_, candidate, set_ts_.result_of_set_commit_ts_);

    set_ts_.Forward(this);
}

void TransactionExecution::PostSetTs()
{
    state_stack_.pop_back();
    assert(state_stack_.empty());

    if (set_ts_.result_of_set_commit_ts_.IsError())
    {
        Abort();
    }
    else
    {
        commit_ts_ = set_ts_.result_of_set_commit_ts_.Value();
        if (rw_set_.ReadSetSize() > 0)
        {
            Vali();
        }
        else
        {
            PostVali();
        }
    }
}

void TransactionExecution::Vali()
{
    state_stack_.push_back(&validate_);

    validate_.Reset(rw_set_.ReadSetSize());
    validate_.vali_cce_addr_.clear();

    /*if (rw_set_.ReadSetSize() + sset_post_cnt_ > validate_.results_.size())
    {
            validate_.Resize(rw_set_.ReadSetSize() + sset_post_cnt_);
    }*/

    size_t offset = 0;
    const std::unordered_map<CcEntryAddr, ReadSetEntry> &rset =
        rw_set_.ReadSet();

    for (const auto &[cce_addr, read_entry] : rset)
    {
        validate_.vali_cce_addr_.emplace_back(&cce_addr);

        CcHandlerResult<std::vector<TxId>> &hres = validate_.results_[offset];
        hres.Reset();

        handler->PostRead(tx_number_.load(std::memory_order_relaxed),
                          tx_term_,
                          read_entry.version_ts_,
                          0,
                          commit_ts_,
                          cce_addr,
                          hres,
                          read_entry.protocol_);

        ++offset;
    }

    /*const std::unordered_map<
            TableName,
            std::map<const TxKey *, ScanSetEntry, PtrLessThan<TxKey>>> &sset_map
    = rw_set_.ScanSet();

    for (auto table_it = sset_map.begin(); table_it != sset_map.end();
             ++table_it)
    {
            const std::map<const TxKey *, ScanSetEntry, PtrLessThan<TxKey>>
    &sset = table_it->second; for (auto sset_it = sset.begin(); sset_it !=
    sset.end(); ++sset_it)
            {
                    CcHandlerResult<std::vector<TxId>> &hres =
                            validate_.results_[offset];
                    hres.Reset();

                    const ScanSetEntry &scanEntry = sset_it->second;
                    handler->PostRead(tx_number_,
                                                              scanEntry.key_ts_,
                                                              scanEntry.gap_ts_,
                                                              commit_ts_,
                                                              scanEntry.cce_addr_,
                                                              hres);
                    ++offset;
            }
    }*/
}

void TransactionExecution::PostVali()
{
    // The validation step is optional. Only pops the stack if the last step is
    // the validation step.
    if (!state_stack_.empty())
    {
        assert(state_stack_.back() == &validate_);
        state_stack_.pop_back();
    }

    if (validate_.error_.load(std::memory_order_acquire))
    {
        Abort();
    }
    else if (txlog_ != nullptr && rw_set_.WriteSetSize() > 0)
    {
        WriteLog();
    }
    else
    {
        tx_status_.store(TxnStatus::Committed, std::memory_order_release);
        SetTxStatus();
    }
}

void TransactionExecution::WriteLog()
{
    state_stack_.push_back(&write_log_);
    write_log_.Reset();

    write_log_.log_closure_.LogResponse()
        .mutable_write_log_response()
        ->clear_redirect();

    ::txlog::LogRequest &log_req = write_log_.log_closure_.LogRequest();
    ::txlog::WriteLogRequest *log_rec = log_req.mutable_write_log_request();

    log_rec->set_txn_number(txid_.TxNumber());
    log_rec->set_commit_timestamp(commit_ts_);

    auto shard_terms = log_rec->mutable_node_terms();
    shard_terms->clear();

    auto data_log_msg = log_rec->mutable_log_content()->mutable_data_log();
    auto shard_logs = data_log_msg->mutable_node_txn_logs();
    shard_logs->clear();

    assert(log_rec->node_terms_size() == 0);

    const std::unordered_map<TableName, TableWriteSet> &wset =
        rw_set_.WriteSet();
    std::unordered_map<
        NodeGroupId,
        std::unordered_map<TableName, std::vector<const WriteSetEntry *>>>
        ng_table_rec_set;

    for (auto table_it = wset.begin(); table_it != wset.end(); ++table_it)
    {
        const TableName &table_name = table_it->first;

        for (auto key_it = table_it->second.begin();
             key_it != table_it->second.end();
             ++key_it)
        {
            const WriteSetEntry &write_entry = key_it->second;
            const CcEntryAddr &addr = write_entry.cce_addr_;

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
                write_log_.hd_result_.SetError(1);
                return;
            }

            auto table_rec_it =
                ng_table_rec_set.try_emplace(addr.NodeGroupId());
            std::unordered_map<TableName, std::vector<const WriteSetEntry *>>
                &table_rec_set = table_rec_it.first->second;

            auto rec_vec_it = table_rec_set.try_emplace(table_name);
            rec_vec_it.first->second.emplace_back(&write_entry);
        }
    }

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
        // (1) A 1-byte integer for the type of log: LogType::RECORD
        // (2) A 1-byte integer for the length of the table name, followed by
        // (3) The string of the table name.
        // (4) A 4-byte integer for the total length of serialized key-record
        // pairs modified by the tx in the node group.
        // (5) A sequence of modified records. Each record is encoded as
        // follows:
        //   (a) The serialized key
        //   (b) A 1-byte flag to indicate if the record is normal, deleted or
        //   void.
        //   (c) The serialized record if the record is normal.
        uint8_t log_type = static_cast<uint8_t>(LogType::RECORD);
        const char *ptr = reinterpret_cast<const char *>(&log_type);
        log_ng_blob->append(ptr, sizeof(uint8_t));
        for (const auto &[table_name, rec_vec] : table_rec_set)
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

            for (const WriteSetEntry *wset_entry : rec_vec)
            {
                wset_entry->key_.get()->Serialize(*log_ng_blob);

                uint8_t rec_flag =
                    wset_entry->op_ == DmlOperation::Delete ? 1 : 0;
                log_ng_blob->append(reinterpret_cast<const char *>(&rec_flag),
                                    1);

                if (wset_entry->rec_.get() != nullptr)
                {
                    // A secondary index entry has no payload.
                    wset_entry->rec_.get()->Serialize(*log_ng_blob);
                }
            }

            kv_len = log_ng_blob->size() - kv_len_start - sizeof(uint32_t);

            // Refills the reserved 4 bytes after knowing the length of
            // serialized records.
            log_ng_blob->replace(
                kv_len_start, sizeof(uint32_t), ptr, sizeof(uint32_t));
        }
    }

    assert(txlog_ != nullptr);

    // Note that node_id calculated from global core ID should always be equal
    // to the actual ccshard node id. But from txservice layer's view, only txid
    // is available. Txservice get txid from the bottom layer (ccshard).
    uint32_t log_group_id = txlog_->GetLogGroupId(txid_.GetNodeId());
    txlog_->WriteLog(log_group_id,
                     write_log_.log_closure_.Controller(),
                     log_req,
                     write_log_.log_closure_.LogResponse(),
                     write_log_.log_closure_);
}

void TransactionExecution::PostWriteLog()
{
    WriteToLogOp *log_op = static_cast<WriteToLogOp *>(state_stack_.back());
    state_stack_.pop_back();

    if (state_stack_.empty())
    {
        if (log_op->hd_result_.IsError())
        {
            tx_status_.store(TxnStatus::Aborted, std::memory_order_release);
        }
        else
        {
            tx_status_.store(TxnStatus::Committed, std::memory_order_release);
        }
        SetTxStatus();
    }
    else
    {
        // The tx is committing a multi-stage operation, e.g., schema changes.
        Forward();
    }
}

void TransactionExecution::SetTxStatus()
{
    state_stack_.push_back(&update_txn_);
    update_txn_.Reset();

    handler->UpdateTxnStatus(
        txid_, tx_status_.load(std::memory_order_relaxed), update_txn_.res_);

    update_txn_.Forward(this);
}

void TransactionExecution::PostSetTxStatus()
{
    state_stack_.pop_back();

    int wset_intention_cnt =
        rw_set_.WriteSetSize() > 0
            ? acquire_write_.acquire_write_cnt_ -
                  acquire_write_.fail_cnt_.load(std::memory_order_acquire)
            : 0;
    if (wset_intention_cnt != 0 || rw_set_.ReadSetSize() != 0)
    {
        PostProcess(rw_set_.ReadSetSize(), wset_intention_cnt);
    }
    else
    {
        // For tx's that have finished validation and have not uploaded
        // anything, skips post-processing.
        PostPostProcess();
    }
}

void TransactionExecution::PostProcess(size_t read_intention_size,
                                       size_t write_intention_size)
{
    state_stack_.push_back(&post_process_);

    if (tx_status_.load(std::memory_order_relaxed) == TxnStatus::Committed)
    {
        post_process_.Reset(0, rw_set_.WriteSetSize());

        size_t idx = 0;
        std::unordered_map<TableName, TableWriteSet> &wset = rw_set_.WriteSet();
        for (auto table_it = wset.begin(); table_it != wset.end(); ++table_it)
        {
            for (auto key_it = table_it->second.begin();
                 key_it != table_it->second.end();
                 ++key_it, ++idx)
            {
                WriteSetEntry &write_entry = key_it->second;
                CcHandlerResult<Void> &hres = post_process_.write_results_[idx];
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
                    const TableName *tn = std::get<0>(*sk_iter);
                    const TxKey *sk = std::get<1>(*sk_iter).get();
                    bool is_delete = std::get<2>(*sk_iter);

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

        post_process_.Reset(read_intention_size, write_intention_size);

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
                    post_process_.write_results_[offset];
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
                post_process_.read_results_[idx];
            hres.Reset();

            handler->PostRead(tx_number_.load(std::memory_order_relaxed),
                              tx_term_,
                              0,
                              0,
                              0,
                              read_it->first,
                              hres,
                              read_it->second.protocol_);
        }
    }

    StartTiming();
}

void TransactionExecution::PostPostProcess()
{
    if (!state_stack_.empty())
    {
        assert(state_stack_.back() == &post_process_);
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

void TransactionExecution::Abort()
{
    tx_status_.store(TxnStatus::Aborted, std::memory_order_release);
    SetTxStatus();
}

void TransactionExecution::PostAcquireAll()
{
    state_stack_.pop_back();
    Forward();
}

void TransactionExecution::Process(AcquireAllOp &acq_all_op)
{
    state_stack_.push_back(&acq_all_op);

    uint32_t node_group_cnt = Sharder::Instance().NodeGroupCount();
    acq_all_op.Reset(node_group_cnt);

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

void TransactionExecution::Process(PostWriteAllOp &post_write_all_op)
{
    state_stack_.push_back(&post_write_all_op);

    uint32_t node_group_cnt = Sharder::Instance().NodeGroupCount();
    post_write_all_op.Reset(node_group_cnt);

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

void TransactionExecution::PostPostWriteAll()
{
    state_stack_.pop_back();
    // So far, post-write-all is only used for schema evolution operations.
    assert(!state_stack_.empty());
    Forward();
}

void TransactionExecution::Process(WriteToLogOp &flush_log)
{
    state_stack_.push_back(&flush_log);
    flush_log.Reset();

    assert(txlog_ != nullptr);

    // Note that node_id calculated from global core ID should always be equal
    // to the actual ccshard node id. But from txservice layer's view, only txid
    // is available. Txservice get txid from the bottom layer (ccshard).
    uint32_t log_group_id = txlog_->GetLogGroupId(txid_.GetNodeId());
    txlog_->WriteLog(log_group_id,
                     flush_log.log_closure_.Controller(),
                     flush_log.log_closure_.LogRequest(),
                     flush_log.log_closure_.LogResponse(),
                     flush_log.log_closure_);
}

void TransactionExecution::PostDataStoreOp()
{
    state_stack_.pop_back();
    assert(!state_stack_.empty());
    Forward();
}

void TransactionExecution::Process(DsUpsertTableOp &ds_upsert_table_op)
{
    state_stack_.push_back(&ds_upsert_table_op);

    ds_upsert_table_op.Reset();
    handler->DataStoreUpsertTable(*ds_upsert_table_op.table_name_,
                                  *ds_upsert_table_op.kv_table_name_,
                                  ds_upsert_table_op.table_schema_,
                                  ds_upsert_table_op.is_deleted_,
                                  commit_ts_,
                                  ds_upsert_table_op.result_);
}

void TransactionExecution::Process(BeginRequest &begin_req)
{
    void_resp_ = &begin_req.cc_result_;
    void_resp_->Reset();
    iso_level_ = begin_req.iso_level_;
    protocol_ = begin_req.protocol_;
    Begin();
}

void TransactionExecution::Process(ReadRequest &read_req)
{
    rec_resp_ = &read_req.cc_result_;
    rec_resp_->Reset();

    if (read_req.read_local_)
    {
        ReadLocal(*read_req.tab_name_,
                  *read_req.key_,
                  *read_req.rec_,
                  read_req.type_);
    }
    else
    {
        Read(*read_req.tab_name_,
             *read_req.key_,
             *read_req.rec_,
             read_req.type_);
    }
}

void TransactionExecution::Process(ReadOutsideRequest &read_outside_req)
{
    rec_resp_ = &read_outside_req.cc_result_;
    rec_resp_->Reset();
    ReadOutside(read_outside_req.rec_, read_outside_req.is_deleted_);
}

void TransactionExecution::Process(ScanOpenRequest &scan_open_req)
{
    uint64_resp_ = &scan_open_req.cc_result_;
    uint64_resp_->Reset();
    ScanOpen(*scan_open_req.tab_name_,
             scan_open_req.indx_type_,
             *scan_open_req.start_key_,
             scan_open_req.inclusive_,
             scan_open_req.direct_,
             scan_open_req.is_ckpt_delta_);
}

void TransactionExecution::Process(ScanNextRequest &scan_next_req)
{
    kvp_resp_ = &scan_next_req.cc_result_;
    kvp_resp_->Reset();
    ScanNext(scan_next_req.alias_);
}

void TransactionExecution::Process(ScanCloseRequest &scan_close_req)
{
    void_resp_ = &scan_close_req.cc_result_;
    void_resp_->Reset();
    ScanClose(scan_close_req.alias_, *scan_close_req.end_key_.get());
}

void TransactionExecution::Process(UpsertRequest &upsert_req)
{
    void_resp_ = &upsert_req.cc_result_;
    void_resp_->Reset();
    Upsert(*upsert_req.tab_name_,
           upsert_req.key_,
           upsert_req.rec_,
           upsert_req.skeys_,
           upsert_req.is_delete_ ? DmlOperation::Delete : DmlOperation::Upsert);
}

void TransactionExecution::Process(CommitRequest &commit_req)
{
    bool_resp_ = &commit_req.cc_result_;
    bool_resp_->Reset();
    Commit();
}

void TransactionExecution::Process(AbortRequest &abort_req)
{
    bool_resp_ = &abort_req.cc_result_;
    bool_resp_->Reset();
    // When the tx is aborted/rolled back by the user, write intentions must
    // have not acquired. Clear the write set before entering post-processing.
    rw_set_.ClearWriteSet();
    Abort();
}

void TransactionExecution::Process(UpsertTableRequest &req)
{
    bool_resp_ = &req.cc_result_;

    schema_op_ = std::make_unique<UpsertTableOp>(*req.table_name_,
                                                 *req.kv_table_name_,
                                                 req.catalog_image_,
                                                 req.catalog_length_,
                                                 req.is_deleted_,
                                                 this);

    state_stack_.push_back(schema_op_.get());
    Forward();
}

void TransactionExecution::Process(FaultInjectRequest &fi_req)
{
    bool_resp_ = &fi_req.cc_result_;
    bool_resp_->Reset();

    FaultInject(fi_req.fault_name_, fi_req.fault_type_, fi_req.node_id_);
}

void TransactionExecution::Begin(uint64_t start_ts)
{
    state_stack_.push_back(&init_txn_);

    init_txn_.Reset();

    commit_ts_ = 0;
    commit_ts_bound_ = start_ts;

    handler->NewTxn(init_txn_.result_);
    init_txn_.Forward(this);

    return;
}

void TransactionExecution::PostBegin()
{
    if (init_txn_.result_.IsError())
    {
        state_stack_.clear();
        void_resp_->FinishError();
        // transaction can be recycled and put into free list.
        tx_status_.store(TxnStatus::Finished, std::memory_order_release);
        Reset();
        return;
    }

    const InitTxResult &init_result = init_txn_.result_.Value();
    txid_ = init_result.txid_;
    tx_number_.store(txid_.TxNumber(), std::memory_order_release);
    commit_ts_bound_ = init_result.start_ts_;
    tx_term_ = init_result.term_;
    state_stack_.pop_back();
    void_resp_->Finish(void_);
}

uint64_t TransactionExecution::TxNumber() const
{
    return tx_number_.load(std::memory_order_acquire);
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

bool TransactionExecution::IsTimeOut()
{
    ++state_forward_cnt_;
    if (state_forward_cnt_ == LoopCnt)
    {
        state_forward_cnt_ = 0;
        uint64_t now_ts = LocalCcShards::ClockTs();
        using namespace std::chrono_literals;
        // TODO remove this hard code 4 seconds
        uint64_t duration =
            std::chrono::duration_cast<std::chrono::microseconds>(4s).count();
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

}  // namespace txservice
