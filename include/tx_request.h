#pragma once

#include <memory>
#include <tuple>
#include <utility>

#include "catalog_key_record.h"
#include "scan.h"
#include "tx_command.h"
#include "tx_execution.h"
#include "tx_key.h"
#include "tx_record.h"
#include "tx_req_result.h"
#include "type.h"

namespace txservice
{
struct TxRequest
{
public:
    using Uptr = std::unique_ptr<TxRequest>;

    virtual ~TxRequest() = default;
    virtual void Process(TransactionExecution *txm) = 0;
    // virtual bool Finish() const = 0;

    static std::string ErrorMessage(TxErrorCode err_code)
    {
        auto it = tx_error_messages.find(err_code);
        if (it != tx_error_messages.end())
        {
            return it->second;
        }
        return "";
    }

    virtual void SetError(
        TxErrorCode err_code = TxErrorCode::UNDEFINED_ERR) = 0;
};

template <typename Subtype, typename T>
struct TemplateTxRequest : TxRequest
{
    TemplateTxRequest(const std::function<void()> *yield_fptr,
                      const std::function<void()> *resume_fptr,
                      TransactionExecution *txm)
        : tx_result_(yield_fptr, resume_fptr), txm_(txm)
    {
    }

    virtual ~TemplateTxRequest() = default;

    void Process(TransactionExecution *txm) override
    {
        txm->ProcessTxRequest(static_cast<Subtype &>(*this));
        return;
    }

    bool IsFinished()
    {
        return tx_result_.Status() != TxResultStatus::Unknown;
    }

    bool IsError() const
    {
        return tx_result_.IsError();
    }

    TxErrorCode ErrorCode() const
    {
        return tx_result_.ErrorCode();
    }

    const std::string &ErrorMsg() const
    {
        auto it = tx_error_messages.find(ErrorCode());
        if (it != tx_error_messages.end())
        {
            return it->second;
        }

        static std::string empty_err_msg;
        return empty_err_msg;
    }

    void Wait()
    {
        TxResultStatus result_status = TxResultStatus::Unknown;
        do
        {
#ifdef EXT_TX_PROC_ENABLED
            if (txm_ != nullptr)
            {
                txm_->ExternalForward();
            }
#endif

            tx_result_.Wait();

            result_status = tx_result_.Status();
        } while (result_status == TxResultStatus::Unknown);
    }

    const T &Result() const
    {
        return tx_result_.Value();
    }

    void Reset()
    {
        tx_result_.Reset();
    }

    void SetError(TxErrorCode err_code = TxErrorCode::UNDEFINED_ERR) override
    {
        tx_result_.FinishError(err_code);
    }

    TxResult<T> tx_result_;
    TransactionExecution *txm_{nullptr};

protected:
    friend class TransactionExecution;
};

struct InitTxRequest : public TemplateTxRequest<InitTxRequest, size_t>
{
    InitTxRequest(IsolationLevel level = IsolationLevel::ReadCommitted,
                  CcProtocol proto = CcProtocol::OCC,
                  const std::function<void()> *yield_fptr = nullptr,
                  const std::function<void()> *resume_fptr = nullptr,
                  TransactionExecution *txm = nullptr)
        : TemplateTxRequest(yield_fptr, resume_fptr, txm),
          iso_level_(level),
          protocol_(proto)
    {
    }

    ~InitTxRequest() = default;

    IsolationLevel iso_level_{IsolationLevel::ReadCommitted};
    CcProtocol protocol_{CcProtocol::OCC};
};

struct ReadTxRequest : public TemplateTxRequest<ReadTxRequest, RecordStatus>
{
public:
    ReadTxRequest(const TableName *tab_name = nullptr,
                  const TxKey *key = nullptr,
                  TxRecord *rec = nullptr,
                  bool is_for_write = false,
                  bool is_for_share = false,
                  bool read_local = false,
                  uint64_t corresponding_sk_commit_ts = 0,
                  bool is_covering_keys = false,
                  uint64_t *unique_sk_commit_ts = nullptr,
                  const std::function<void()> *yield_fptr = nullptr,
                  const std::function<void()> *resume_fptr = nullptr,
                  TransactionExecution *txm = nullptr)
        : TemplateTxRequest(yield_fptr, resume_fptr, txm),
          tab_name_(tab_name),
          key_(key),
          rec_(rec),
          is_for_write_(is_for_write),
          is_for_share_(is_for_share),
          read_local_(read_local),
          corresponding_sk_commit_ts_(corresponding_sk_commit_ts),
          is_covering_keys_(is_covering_keys),
          unique_sk_commit_ts_(unique_sk_commit_ts)
    {
    }

    void Set(const TableName *tab_name,
             const TxKey *key,
             TxRecord *rec,
             bool is_for_write = false,
             bool is_for_share = false,
             bool read_local = false,
             uint64_t corresponding_sk_commit_ts = 0,
             bool is_covering_keys = false,
             uint64_t *unique_sk_commit_ts = nullptr)
    {
        tab_name_ = tab_name;
        key_ = key;
        rec_ = rec;
        is_for_write_ = is_for_write;
        is_for_share_ = is_for_share;
        read_local_ = read_local;
        corresponding_sk_commit_ts_ = corresponding_sk_commit_ts;
        is_covering_keys_ = is_covering_keys;
        unique_sk_commit_ts_ = unique_sk_commit_ts;
    }

    const TableName *tab_name_;
    const TxKey *key_;
    TxRecord *rec_;
    bool is_for_write_;  // used for "select ... for update".
    bool is_for_share_;  // used for "select ... lock in share mode".
    bool read_local_;
    uint64_t corresponding_sk_commit_ts_;

    // For unique_sk point query
    bool is_covering_keys_;
    uint64_t *unique_sk_commit_ts_;
};

struct ReadOutsideTxRequest
    : public TemplateTxRequest<ReadOutsideTxRequest, RecordStatus>
{
public:
    ReadOutsideTxRequest(TxRecord &rec,
                         bool is_deleted,
                         uint64_t commit_ts,
                         const std::function<void()> *yield_fptr = nullptr,
                         const std::function<void()> *resume_fptr = nullptr,
                         TransactionExecution *txm = nullptr,
                         std::vector<VersionTxRecord> *archives = nullptr)
        : TemplateTxRequest(yield_fptr, resume_fptr, txm),
          rec_(rec),
          is_deleted_(is_deleted),
          commit_ts_(commit_ts),
          archives_(archives)
    {
    }

    TxRecord &rec_;
    bool is_deleted_;
    uint64_t commit_ts_;
    std::vector<VersionTxRecord> *archives_;
};

struct UpsertTxRequest : public TemplateTxRequest<UpsertTxRequest, Void>
{
    UpsertTxRequest(const TableName *tab_name,
                    TxKey::Uptr key,
                    TxRecord::Uptr rec,
                    OperationType operation_type,
                    const std::function<void()> *yield_fptr = nullptr,
                    const std::function<void()> *resume_fptr = nullptr,
                    TransactionExecution *txm = nullptr)
        : TemplateTxRequest(yield_fptr, resume_fptr, txm),
          tab_name_(tab_name),
          key_(std::move(key)),
          rec_(std::move(rec)),
          operation_type_(operation_type)
    {
    }

    const TableName *tab_name_;
    TxKey::Uptr key_;
    TxRecord::Uptr rec_;
    OperationType operation_type_;
};

struct ScanOpenTxRequest : public TemplateTxRequest<ScanOpenTxRequest, size_t>
{
    ScanOpenTxRequest() : TemplateTxRequest(nullptr, nullptr, nullptr)
    {
    }

    ScanOpenTxRequest(const TableName *tabname,
                      ScanIndexType index_type,
                      const TxKey *start_key,
                      bool start_inclusive = true,
                      const TxKey *end_key = nullptr,
                      bool end_inclusive = true,
                      ScanDirection direction = ScanDirection::Forward,
                      bool is_ckpt = false,
                      bool is_for_write = false,
                      bool is_for_share = false,
                      bool is_covering_keys = false,
                      bool is_read_local = false,
                      const std::function<void()> *yield_fptr = nullptr,
                      const std::function<void()> *resume_fptr = nullptr,
                      TransactionExecution *txm = nullptr)
        : TemplateTxRequest(yield_fptr, resume_fptr, txm),
          tab_name_(tabname),
          indx_type_(index_type),
          start_key_(start_key),
          start_inclusive_(start_inclusive),
          end_key_(end_key),
          end_inclusive_(end_inclusive),
          direct_(direction),
          is_ckpt_delta_(is_ckpt),
          is_for_write_(is_for_write),
          is_for_share_(is_for_share),
          is_covering_keys_(is_covering_keys),
          read_local_(is_read_local),
          scan_alias_(UINT64_MAX)
    {
    }

    void Reset(const TableName *tabname,
               ScanIndexType index_type,
               const TxKey *start_key,
               bool start_inclusive = true,
               const TxKey *end_key = nullptr,
               bool end_inclusive = true,
               ScanDirection direction = ScanDirection::Forward,
               bool is_ckpt = false,
               bool is_for_write = false,
               bool is_for_share = false,
               bool is_covering_keys = false,
               bool is_read_local = false,
               const std::function<void()> *yield_fptr = nullptr,
               const std::function<void()> *resume_fptr = nullptr,
               TransactionExecution *txm = nullptr)
    {
        tx_result_.Reset(yield_fptr, resume_fptr);
        tab_name_ = tabname;
        indx_type_ = index_type;
        start_key_ = start_key;
        start_inclusive_ = start_inclusive;
        end_key_ = end_key;
        end_inclusive_ = end_inclusive;
        direct_ = direction;
        is_ckpt_delta_ = is_ckpt;
        is_for_write_ = is_for_write;
        is_for_share_ = is_for_share;
        is_covering_keys_ = is_covering_keys;
        read_local_ = is_read_local;
        scan_alias_ = UINT64_MAX;
    }

    const TxKey *StartKey() const
    {
        return start_key_;
    }

    const TxKey *EndKey() const
    {
        return end_key_;
    }

    const TableName *tab_name_{nullptr};
    ScanIndexType indx_type_{ScanIndexType::Primary};
    const TxKey *start_key_{nullptr};
    bool start_inclusive_{false};
    const TxKey *end_key_{nullptr};
    bool end_inclusive_{false};
    ScanDirection direct_{ScanDirection::Forward};
    bool is_ckpt_delta_{false};
    bool is_for_write_{false};
    bool is_for_share_{false};
    bool is_covering_keys_{true};
    bool read_local_{false};
    uint64_t scan_alias_{UINT64_MAX};
};

struct ScanBatchTuple
{
    ScanBatchTuple() = default;
    ScanBatchTuple(const TxKey *key,
                   const TxRecord *rec,
                   RecordStatus status,
                   uint64_t version)
        : key_(key), record_(rec), status_(status), version_ts_(version)
    {
    }

    ScanBatchTuple(const TxKey *key,
                   const TxRecord *rec,
                   RecordStatus status,
                   uint64_t version,
                   const CcEntryAddr &cce_addr,
                   LockType lock_type)
        : key_(key),
          record_(rec),
          status_(status),
          version_ts_(version),
          cce_addr_(cce_addr)
    {
    }

    ScanBatchTuple(const ScanBatchTuple &rhs)
        : key_(rhs.key_),
          record_(rhs.record_),
          status_(rhs.status_),
          version_ts_(rhs.version_ts_),
          cce_addr_(rhs.cce_addr_)
    {
    }

    const TxKey *key_{nullptr};
    const TxRecord *record_{nullptr};
    RecordStatus status_{RecordStatus::Unknown};
    uint64_t version_ts_{0};
    const CcEntryAddr cce_addr_;
};

struct ScanBatchTxRequest : public TemplateTxRequest<ScanBatchTxRequest, bool>
{
    ScanBatchTxRequest() = delete;

    ScanBatchTxRequest(uint64_t alias,
                       const TableName &table_name,
                       std::vector<ScanBatchTuple> *batch_vec,
                       const std::function<void()> *yield_fptr = nullptr,
                       const std::function<void()> *resume_fptr = nullptr,
                       TransactionExecution *txm = nullptr)
        : TemplateTxRequest(yield_fptr, resume_fptr, txm),
          alias_(alias),
          table_name_(table_name),
          batch_(batch_vec)
    {
        batch_->clear();
    }

    uint64_t alias_;
    const TableName &table_name_;
    std::vector<ScanBatchTuple> *batch_;
};

struct UnlockTuple
{
    UnlockTuple(const CcEntryAddr &cce_addr,
                uint64_t version_ts,
                RecordStatus status)
        : cce_addr_(cce_addr), version_ts_(version_ts), status_(status)
    {
    }

    CcEntryAddr cce_addr_;
    uint64_t version_ts_;
    RecordStatus status_;
};

struct ScanCloseTxRequest : public TemplateTxRequest<ScanCloseTxRequest, Void>
{
    ScanCloseTxRequest() = delete;

    ScanCloseTxRequest(uint64_t alias,
                       const TableName *table_name,
                       const std::function<void()> *yield_fptr = nullptr,
                       const std::function<void()> *resume_fptr = nullptr,
                       TransactionExecution *txm = nullptr)
        : TemplateTxRequest(yield_fptr, resume_fptr, txm),
          alias_(alias),
          table_name_(table_name),
          in_use_(true)
    {
    }

    ScanCloseTxRequest(const std::vector<ScanBatchTuple> &scan_batch,
                       size_t scan_batch_idx,
                       uint64_t alias,
                       const TableName *table_name,
                       const std::function<void()> *yield_fptr = nullptr,
                       const std::function<void()> *resume_fptr = nullptr,
                       TransactionExecution *txm = nullptr)
        : TemplateTxRequest(yield_fptr, resume_fptr, txm),
          alias_(alias),
          table_name_(table_name),
          in_use_(true)
    {
        for (size_t idx = scan_batch_idx; idx < scan_batch.size(); ++idx)
        {
            const ScanBatchTuple &tuple = scan_batch[idx];
            unlock_batch_.emplace_back(
                tuple.cce_addr_, tuple.version_ts_, tuple.status_);
        }
    }

    void Reset(uint64_t alias, const TableName *table_name)
    {
        assert(!in_use_.load(std::memory_order_relaxed));

        tx_result_.Reset();
        alias_ = alias;
        table_name_ = table_name;
        in_use_.store(true, std::memory_order_relaxed);
    }

    std::vector<UnlockTuple> unlock_batch_;
    uint64_t alias_{UINT64_MAX};
    const TableName *table_name_{nullptr};
    std::atomic<bool> in_use_{false};
};

struct AbortTxRequest : public TemplateTxRequest<AbortTxRequest, bool>
{
    AbortTxRequest(const std::function<void()> *yield_fptr = nullptr,
                   const std::function<void()> *resume_fptr = nullptr,
                   TransactionExecution *txm = nullptr)
        : TemplateTxRequest(yield_fptr, resume_fptr, txm)
    {
    }
};

struct CommitTxRequest : public TemplateTxRequest<CommitTxRequest, bool>
{
    CommitTxRequest(const std::function<void()> *yield_fptr = nullptr,
                    const std::function<void()> *resume_fptr = nullptr,
                    TransactionExecution *txm = nullptr)
        : TemplateTxRequest(yield_fptr, resume_fptr, txm)
    {
    }
};

struct UpsertTableTxRequest
    : public TemplateTxRequest<UpsertTableTxRequest, bool>
{
    UpsertTableTxRequest(const TableName *table_name,
                         const std::string *curr_image,
                         uint64_t schema_ts,
                         const std::string *dirty_image,
                         txservice::OperationType op_type,
                         const std::string *alter_table_info_image = nullptr,
                         const std::function<void()> *yield_fptr = nullptr,
                         const std::function<void()> *resume_fptr = nullptr,
                         TransactionExecution *txm = nullptr)
        : TemplateTxRequest(yield_fptr, resume_fptr, txm),
          table_name_(table_name),
          curr_image_(curr_image),
          curr_schema_ts_(schema_ts),
          dirty_image_(dirty_image),
          op_type_(op_type),
          alter_table_info_image_(alter_table_info_image)
    {
    }

    const TableName *table_name_;
    const std::string *curr_image_;
    uint64_t curr_schema_ts_;
    const std::string *dirty_image_;
    txservice::OperationType op_type_;
    const std::string *alter_table_info_image_;
};

struct SplitFlushTxRequest : public TemplateTxRequest<SplitFlushTxRequest, bool>
{
    SplitFlushTxRequest(
        const TableName &table_name,
        const TableSchema *schema,
        NodeGroupId node_group,
        const TxKey *old_start_key,
        const TxKey *old_end_key,
        const RangeInfo *old_info,
        std::vector<std::pair<TxKey::Uptr, int32_t>> &&new_range_id)
        : TemplateTxRequest(nullptr, nullptr, nullptr),
          table_name_(&table_name),
          schema_(schema),
          node_group_(node_group),
          old_start_key_(old_start_key),
          old_end_key_(old_end_key),
          old_range_info_(old_info),
          new_range_id_(std::move(new_range_id))
    {
    }
    const TableName *table_name_{nullptr};
    const TableSchema *schema_{nullptr};
    NodeGroupId node_group_;
    const TxKey *old_start_key_{nullptr};
    const TxKey *old_end_key_{nullptr};
    const RangeInfo *old_range_info_{nullptr};
    std::vector<std::pair<TxKey::Uptr, int32_t>> new_range_id_;
};

struct AnalyzeTableTxRequest
    : public TemplateTxRequest<AnalyzeTableTxRequest, Void>
{
    AnalyzeTableTxRequest(const TableName *table_name = nullptr,
                          const std::function<void()> *yield_fptr = nullptr,
                          const std::function<void()> *resume_fptr = nullptr,
                          TransactionExecution *txm = nullptr)
        : TemplateTxRequest(yield_fptr, resume_fptr, txm),
          table_name_(table_name)
    {
    }

    const TableName *table_name_{nullptr};
};

struct ObjectCommandTxRequest
    : public TemplateTxRequest<ObjectCommandTxRequest, RecordStatus>
{
    ObjectCommandTxRequest(const TableName *table_name,
                           const TxKey *key,
                           const TxCommand *command,
                           TxCommandResult *cmd_result,
                           bool auto_commit = true)
        : TemplateTxRequest(nullptr, nullptr, nullptr),
          table_name_(table_name),
          key_(key),
          command_(command),
          cmd_result_(cmd_result),
          auto_commit_(auto_commit)
    {
    }

    const TableName *table_name_;
    const TxKey *key_;
    const TxCommand *command_;
    TxCommandResult *cmd_result_;
    bool auto_commit_{};
};

struct FaultInjectTxRequest
    : public TemplateTxRequest<FaultInjectTxRequest, bool>
{
    FaultInjectTxRequest(const std::string &fault_name,
                         const std::string &fault_paras,
                         std::vector<int> &vct_node_id)
        : TemplateTxRequest(nullptr, nullptr, nullptr),
          fault_name_(fault_name),
          fault_paras_(fault_paras)
    {
        vct_node_id_.swap(vct_node_id);
    }

    const std::string fault_name_;
    const std::string fault_paras_;
    std::vector<int> vct_node_id_;
};

// for test
struct CleanCcEntryForTestTxRequest
    : public TemplateTxRequest<CleanCcEntryForTestTxRequest, bool>
{
    CleanCcEntryForTestTxRequest(const TableName *tab_name = nullptr,
                                 const TxKey *key = nullptr,
                                 bool only_archives = false,
                                 bool flush = true)
        : TemplateTxRequest(nullptr, nullptr, nullptr),
          tab_name_(tab_name),
          key_(key),
          only_archives_{only_archives},
          flush_{flush}
    {
    }

    const TableName *tab_name_;
    const TxKey *key_;
    bool only_archives_;
    bool flush_;
};

}  // namespace txservice
