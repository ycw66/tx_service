#pragma once

#include <memory>
#include <tuple>
#include <utility>
#include <vector>

#include "catalog_key_record.h"
#include "range_slice.h"
#include "scan.h"
#include "tx_command.h"
#include "tx_execution.h"
#include "tx_key.h"
#include "tx_record.h"
#include "tx_req_result.h"
#include "type.h"

namespace txservice
{
struct DataMigrationStatus;
struct TxRequest
{
public:
    using Uptr = std::unique_ptr<TxRequest>;

    virtual ~TxRequest() = default;
    virtual void Process(TransactionExecution *txm) = 0;
    // virtual bool Finish() const = 0;

    static const std::string &ErrorMessage(TxErrorCode err_code)
    {
        if (err_code != TxErrorCode::NO_ERROR)
        {
            auto it = tx_error_messages.find(err_code);
            if (it != tx_error_messages.end())
            {
                return it->second;
            }
        }

        static std::string empty_err_msg;
        return empty_err_msg;
    }

    virtual void SetError(
        TxErrorCode err_code = TxErrorCode::UNDEFINED_ERR) = 0;
};

template <typename Subtype, typename T>
struct TemplateTxRequest : TxRequest
{
    TemplateTxRequest(const std::function<void()> *yield_fptr,
                      const std::function<void()> *resume_fptr,
                      TransactionExecution *txm = nullptr)
        : tx_result_(yield_fptr, resume_fptr), txm_(txm)
    {
    }

    virtual ~TemplateTxRequest() = default;

    void Process(TransactionExecution *txm) override
    {
        txm->ProcessTxRequest(static_cast<Subtype &>(*this));
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
        return TxRequest::ErrorMessage(ErrorCode());
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

protected:
    TransactionExecution *txm_{nullptr};
    friend class TransactionExecution;
};

struct InitTxRequest : public TemplateTxRequest<InitTxRequest, size_t>
{
    InitTxRequest(IsolationLevel level = IsolationLevel::ReadCommitted,
                  CcProtocol proto = CcProtocol::OCC,
                  const std::function<void()> *yield_fptr = nullptr,
                  const std::function<void()> *resume_fptr = nullptr,
                  TransactionExecution *txm = nullptr,
                  uint32_t tx_ng_id = UINT32_MAX,
                  uint32_t log_group_id = UINT32_MAX)
        : TemplateTxRequest(yield_fptr, resume_fptr, txm),
          iso_level_(level),
          protocol_(proto),
          tx_ng_id_(tx_ng_id),
          log_group_id_(log_group_id)
    {
    }

    ~InitTxRequest() = default;

    IsolationLevel iso_level_{IsolationLevel::ReadCommitted};
    CcProtocol protocol_{CcProtocol::OCC};
    uint32_t tx_ng_id_{UINT32_MAX};
    uint32_t log_group_id_{UINT32_MAX};
};

struct ReadTxRequest
    : public TemplateTxRequest<ReadTxRequest, std::pair<RecordStatus, uint64_t>>
{
public:
    ReadTxRequest(const TableName *tab_name = nullptr,
                  const TxKey *key = nullptr,
                  TxRecord *rec = nullptr,
                  bool is_for_write = false,
                  bool is_for_share = false,
                  bool read_local = false,
                  uint64_t ts = 0,
                  bool is_covering_keys = false,
                  bool is_recovering = false,
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
          ts_(ts),
          is_covering_keys_(is_covering_keys),
          is_recovering_(is_recovering)
    {
    }

    void Set(const TableName *tab_name,
             const TxKey *key,
             TxRecord *rec,
             bool is_for_write = false,
             bool is_for_share = false,
             bool read_local = false,
             uint64_t ts = 0,
             bool is_covering_keys = false,
             bool is_recovering = false)
    {
        tab_name_ = tab_name;
        key_ = key;
        rec_ = rec;
        is_for_write_ = is_for_write;
        is_for_share_ = is_for_share;
        read_local_ = read_local;
        ts_ = ts;
        is_covering_keys_ = is_covering_keys;
        is_recovering_ = is_recovering;
    }

    const TableName *tab_name_;
    const TxKey *key_;
    TxRecord *rec_;
    bool is_for_write_;  // used for "select ... for update".
    bool is_for_share_;  // used for "select ... lock in share mode".
    bool read_local_;

    /*

    Here, the timestamp serves two roles:

    1. When mvcc is enabled, this ts represents the start timestamp of the
    transaction.

    2. When mvcc is disabled, this ts represents the secondary key commit
    timestamp when performing a PkRead preceded by a sk read/scan. This ts is
    required in PkRead() to check whether the pk row is valid to read.

    The pk row is invalid to read if it is being modified concurrently. For
    example, txn#1 is updating a row (1,a,1) into (1,b,1) in table t1(i INT, j
    CHAR, k INT, PRIMARY KEY(i), UNIQUE(j)), while txn#2 wants to read a row
    where j=b. If ,in txn#1, (b,1) has been inserted into sk table(t1*~~j) while
    (1,b,1) has not been inserted into base table(t1), txn#2 will see (b,1) and
    use i=1 to do a pk read, but then find (1,a,1) instead, which is incorrect.

    */
    uint64_t ts_;

    // For unique_sk point query
    bool is_covering_keys_;

    // If this is a read request for recovering. If true we should
    // rely on candidate leader term instead of current term to decide
    // if node is valid leader.
    bool is_recovering_;
};

struct ReadOutsideTxRequest
    : public TemplateTxRequest<ReadOutsideTxRequest, RecordStatus>
{
public:
    ReadOutsideTxRequest(TxRecord &rec,
                         bool is_deleted,
                         uint64_t commit_ts,
                         std::vector<VersionTxRecord> *archives,
                         const std::function<void()> *yield_fptr = nullptr,
                         const std::function<void()> *resume_fptr = nullptr,
                         TransactionExecution *txm = nullptr)
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
                    const std::function<void()> *resume_fptr = nullptr)
        : TemplateTxRequest(yield_fptr, resume_fptr),
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
    ScanOpenTxRequest() : TemplateTxRequest(nullptr, nullptr)
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
        txm_ = txm;
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
    ScanBatchTuple(const TxKey *key, TxRecord *rec) : key_(key), record_(rec)
    {
    }
    ScanBatchTuple(const TxKey *key,
                   TxRecord *rec,
                   RecordStatus status,
                   uint64_t version)
        : key_(key), record_(rec), status_(status), version_ts_(version)
    {
    }

    ScanBatchTuple(const TxKey *key,
                   TxRecord *rec,
                   RecordStatus status,
                   uint64_t version,
                   const CcEntryAddr &cce_addr)
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
    TxRecord *record_{nullptr};
    RecordStatus status_{RecordStatus::Unknown};
    uint64_t version_ts_{0};
    CcEntryAddr cce_addr_;
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

#ifdef RANGE_PARTITION_ENABLED
    uint8_t prefetch_slice_cnt_{0};
#endif
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
                   const std::function<void()> *resume_fptr = nullptr)
        : TemplateTxRequest(yield_fptr, resume_fptr)
    {
    }
};

struct CommitTxRequest : public TemplateTxRequest<CommitTxRequest, bool>
{
    CommitTxRequest(bool to_commit = true,
                    const std::function<void()> *yield_fptr = nullptr,
                    const std::function<void()> *resume_fptr = nullptr,
                    TransactionExecution *txm = nullptr)
        : TemplateTxRequest(yield_fptr, resume_fptr, txm), to_commit_(to_commit)
    {
    }

    bool to_commit_{true};
};

struct UpsertTableTxRequest
    : public TemplateTxRequest<UpsertTableTxRequest, UpsertResult>
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
        const TxKey *old_start_key,
        const TxKey *old_end_key,
        StoreRange *store_range,
        const RangeInfo *old_info,
        std::vector<std::pair<TxKey::Uptr, int32_t>> &&new_range_info,
        uint64_t previous_scan_ts,
        std::vector<FlushRecord> &&previous_data_sync_vec,
        std::vector<FlushRecord> &&previous_archive_vec,
        std::vector<const TxKey *> &&previous_mv_base_vec)
        : TemplateTxRequest(nullptr, nullptr, nullptr),
          table_name_(&table_name),
          schema_(schema),
          old_start_key_(old_start_key),
          old_end_key_(old_end_key),
          store_range_(store_range),
          old_range_info_(old_info),
          new_range_info_(std::move(new_range_info)),
          previous_scan_ts_(previous_scan_ts),
          previous_data_sync_vec_(std::move(previous_data_sync_vec)),
          previous_archive_vec_(std::move(previous_archive_vec)),
          previous_mv_base_vec_(std::move(previous_mv_base_vec))
    {
    }
    const TableName *table_name_{nullptr};
    const TableSchema *schema_{nullptr};
    const TxKey *old_start_key_{nullptr};
    const TxKey *old_end_key_{nullptr};
    StoreRange *store_range_{nullptr};
    const RangeInfo *old_range_info_{nullptr};
    std::vector<std::pair<TxKey::Uptr, int32_t>> new_range_info_;
    uint64_t previous_scan_ts_;
    std::vector<FlushRecord> previous_data_sync_vec_;
    std::vector<FlushRecord> previous_archive_vec_;
    std::vector<const TxKey *> previous_mv_base_vec_;
};

struct DataMigrationTxRequest
    : public TemplateTxRequest<DataMigrationTxRequest, Void>
{
    DataMigrationTxRequest(std::shared_ptr<DataMigrationStatus> status)
        : TemplateTxRequest(nullptr, nullptr, nullptr), status_(status)
    {
    }

    std::shared_ptr<DataMigrationStatus> status_;
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
        : TemplateTxRequest(nullptr, nullptr),
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

struct ClusterScaleTxRequest
    : public TemplateTxRequest<ClusterScaleTxRequest, Void>
{
    ClusterScaleTxRequest(
        ClusterScaleOpType scale_type,
        std::vector<std::pair<std::string, uint16_t>> *new_nodes,
        uint16_t *remove_node_count)
        : TemplateTxRequest(nullptr, nullptr),
          scale_type_(scale_type),
          new_nodes_(new_nodes),
          remove_node_count_(remove_node_count)
    {
    }

    ClusterScaleOpType scale_type_;
    // Used when adding node, to indicate added node info
    std::vector<std::pair<std::string, uint16_t>> *new_nodes_;
    // Used when removing node, to indicate how many nodes to be removed
    uint16_t *remove_node_count_;
};

struct SchemaRecoveryTxRequest
    : public TemplateTxRequest<SchemaRecoveryTxRequest, UpsertResult>
{
    SchemaRecoveryTxRequest(const ::txlog::SchemaOpMessage &schema_op_msg)
        : TemplateTxRequest(nullptr, nullptr), schema_op_msg_(schema_op_msg)
    {
    }

    const ::txlog::SchemaOpMessage &schema_op_msg_;
};

struct RangeSplitRecoveryTxRequest
    : public TemplateTxRequest<RangeSplitRecoveryTxRequest, bool>
{
    RangeSplitRecoveryTxRequest(
        const ::txlog::SplitRangeOpMessage &ds_split_range_op_msg,
        const TableSchema *table_schema,
        int32_t partition_id,
        const TxKey *start_key,
        const TxKey *end_key,
        StoreRange *store_range,
        const RangeInfo *range_info,
        std::vector<std::unique_ptr<TxKey>> &&new_range_keys,
        std::vector<int32_t> &&new_partition_ids,
        uint32_t node_group_id)
        : TemplateTxRequest(nullptr, nullptr),
          ds_split_range_op_msg_(ds_split_range_op_msg),
          table_schema_(table_schema),
          partition_id_(partition_id),
          start_key_(start_key),
          end_key_(end_key),
          store_range_(store_range),
          range_info_(range_info),
          new_range_keys_(std::move(new_range_keys)),
          new_partition_ids_(std::move(new_partition_ids)),
          node_group_id_(node_group_id)
    {
    }

    const ::txlog::SplitRangeOpMessage &ds_split_range_op_msg_;
    const TableSchema *table_schema_;
    int32_t partition_id_;
    const TxKey *start_key_;
    const TxKey *end_key_;
    StoreRange *store_range_;
    const RangeInfo *range_info_;
    std::vector<std::unique_ptr<TxKey>> new_range_keys_;
    std::vector<int32_t> new_partition_ids_;
    uint32_t node_group_id_;
};

struct ReloadCacheTxRequest
    : public TemplateTxRequest<ReloadCacheTxRequest, Void>
{
    ReloadCacheTxRequest(const std::function<void()> *yield_fptr = nullptr,
                         const std::function<void()> *resume_fptr = nullptr,
                         TransactionExecution *txm = nullptr)
        : TemplateTxRequest(yield_fptr, resume_fptr, txm)
    {
    }
};

struct FaultInjectTxRequest
    : public TemplateTxRequest<FaultInjectTxRequest, bool>
{
    FaultInjectTxRequest(const std::string &fault_name,
                         const std::string &fault_paras,
                         std::vector<int> &vct_node_id,
                         const std::function<void()> *yield_fptr = nullptr,
                         const std::function<void()> *resume_fptr = nullptr,
                         TransactionExecution *txm = nullptr)
        : TemplateTxRequest(yield_fptr, resume_fptr, txm),
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
        : TemplateTxRequest(nullptr, nullptr),
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

// Batch read records from pk
struct BatchReadTxRequest : public TemplateTxRequest<BatchReadTxRequest, Void>
{
public:
    BatchReadTxRequest(const TableName *tab_name,
                       std::vector<ScanBatchTuple> &tuple_batch,
                       bool is_for_write = false,
                       bool is_for_share = false,
                       bool read_local = false,
                       const std::function<void()> *yield_fptr = nullptr,
                       const std::function<void()> *resume_fptr = nullptr,
                       TransactionExecution *txm = nullptr,
                       uint64_t corresponding_sk_commit_ts = 0)
        : TemplateTxRequest(yield_fptr, resume_fptr, txm),
          tab_name_(tab_name),
          read_batch_(tuple_batch),
          is_for_write_(is_for_write),
          is_for_share_(is_for_share),
          read_local_(read_local),
          corresponding_sk_commit_ts_(corresponding_sk_commit_ts)
    {
    }

    void Set(const TableName *tab_name,
             std::vector<ScanBatchTuple> &batch_read_pri,
             bool is_for_write = false,
             bool is_for_share = false,
             bool read_local = false,
             uint64_t corresponding_sk_commit_ts = 0)
    {
        tab_name_ = tab_name;
        read_batch_ = std::move(batch_read_pri);
        is_for_write_ = is_for_write;
        is_for_share_ = is_for_share;
        read_local_ = read_local;
        corresponding_sk_commit_ts_ = corresponding_sk_commit_ts;
    }

    const TableName *tab_name_;
    std::vector<ScanBatchTuple> &read_batch_;
    bool is_for_write_;  // used for "select ... for update".
    bool is_for_share_;  // used for "select ... lock in share mode".
    bool read_local_;
    uint64_t corresponding_sk_commit_ts_;
};

}  // namespace txservice
