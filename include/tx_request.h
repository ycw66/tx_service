#pragma once

#include <memory>
#include <tuple>
#include <utility>

#include "catalog_key_record.h"
#include "scan.h"
#include "tx_container.h"
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
};

template <typename Subtype, typename T>
struct TemplateTxRequest : TxRequest
{
    TemplateTxRequest() : tx_result_()
    {
    }

    virtual ~TemplateTxRequest() = default;

    void Process(TransactionExecution *txm) override
    {
        txm->ProcessTxRequest(static_cast<Subtype &>(*this));
        return;
    }

    bool Finish()
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

    std::string ErrorMsg() const
    {
        auto it = error_messages.find(ErrorCode());
        if (it != error_messages.end())
        {
            return it->second;
        }
        return "";
    }

    void Wait()
    {
        tx_result_.Wait();
    }

    const T &Result() const
    {
        return tx_result_.Value();
    }

    void Reset()
    {
        tx_result_.Reset();
    }

protected:
    TxResult<T> tx_result_;

    friend class TransactionExecution;
};

struct InitTxRequest : public TemplateTxRequest<InitTxRequest, size_t>
{
    InitTxRequest(IsolationLevel level = IsolationLevel::ReadCommitted,
                  CcProtocol proto = CcProtocol::OCC)
        : iso_level_(level), protocol_(proto)
    {
    }

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
                  uint64_t corresponding_sk_commit_ts = 0)
        : tab_name_(tab_name),
          key_(key),
          rec_(rec),
          is_for_write_(is_for_write),
          is_for_share_(is_for_share),
          read_local_(read_local),
          corresponding_sk_commit_ts_(corresponding_sk_commit_ts)
    {
    }

    void Set(const TableName *tab_name,
             const TxKey *key,
             TxRecord *rec,
             bool is_for_write = false,
             bool is_for_share = false,
             bool read_local = false,
             uint64_t corresponding_sk_commit_ts = 0)
    {
        tab_name_ = tab_name;
        key_ = key;
        rec_ = rec;
        is_for_write_ = is_for_write;
        is_for_share_ = is_for_share;
        read_local_ = read_local;
        corresponding_sk_commit_ts_ = corresponding_sk_commit_ts;
    }

    const TableName *tab_name_;
    const TxKey *key_;
    TxRecord *rec_;
    bool is_for_write_;  // used for "select ... for update".
    bool is_for_share_;  // used for "select ... lock in share mode".
    bool read_local_;
    uint64_t corresponding_sk_commit_ts_;
};

struct ReadOutsideTxRequest
    : public TemplateTxRequest<ReadOutsideTxRequest, RecordStatus>
{
public:
    ReadOutsideTxRequest(TxRecord &rec,
                         bool is_deleted,
                         uint64_t commit_ts,
                         std::vector<VersionTxRecord> *archives = nullptr)
        : rec_(rec),
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
                    TxKey *key,
                    TxRecord *rec,
                    OperationType operation_type)
        : tab_name_(tab_name),
          key_(key),
          rec_(rec),
          operation_type_(operation_type)
    {
    }

    UpsertTxRequest(const TableName *tab_name,
                    TxKey::Uptr key,
                    TxRecord::Uptr rec,
                    OperationType operation_type)
        : tab_name_(tab_name),
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
    ScanOpenTxRequest() = delete;

    ScanOpenTxRequest(const TableName *tabname,
                      ScanIndexType index_type,
                      const TxKey *start_key,
                      bool inclusive = true,
                      ScanDirection direction = ScanDirection::Forward,
                      bool is_ckpt = false,
                      bool is_for_write = false,
                      bool is_for_share = false,
                      bool is_read_local = false)
        : tab_name_(tabname),
          indx_type_(index_type),
          start_key_(start_key),
          inclusive_(inclusive),
          direct_(direction),
          is_ckpt_delta_(is_ckpt),
          is_for_write_(is_for_write),
          is_for_share_(is_for_share),
          read_local_(is_read_local)
    {
    }

    const TxKey *StartKey() const
    {
        return start_key_;
    }

    const TableName *tab_name_;
    ScanIndexType indx_type_;
    const TxKey *start_key_;
    bool inclusive_;
    ScanDirection direct_;
    bool is_ckpt_delta_;
    bool is_for_write_;
    bool is_for_share_;
    bool read_local_;
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
                   const CcEntryAddr cce_addr,
                   LockType lock_type)
        : key_(key),
          record_(rec),
          status_(status),
          version_ts_(version),
          cce_addr_(cce_addr),
          lock_type_(lock_type)
    {
    }

    ScanBatchTuple(const ScanBatchTuple &rhs)
        : key_(rhs.key_),
          record_(rhs.record_),
          status_(rhs.status_),
          version_ts_(rhs.version_ts_),
          cce_addr_(rhs.cce_addr_),
          lock_type_(rhs.lock_type_)
    {
    }

    const TxKey *key_{nullptr};
    const TxRecord *record_{nullptr};
    RecordStatus status_{RecordStatus::Unknown};
    uint64_t version_ts_{0};
    const CcEntryAddr cce_addr_;
    LockType lock_type_{LockType::NoLock};
};

struct ScanBatchTxRequest : public TemplateTxRequest<ScanBatchTxRequest, Void>
{
    ScanBatchTxRequest() = delete;

    ScanBatchTxRequest(size_t alias,
                       const TableName &table_name,
                       std::vector<ScanBatchTuple> *batch_vec)
        : alias_(alias), table_name_(table_name), batch_(batch_vec)
    {
        batch_->clear();
    }

    size_t alias_;
    const TableName &table_name_;
    std::vector<ScanBatchTuple> *batch_;
};

struct ScanCloseTxRequest : public TemplateTxRequest<ScanCloseTxRequest, Void>
{
    ScanCloseTxRequest(std::vector<txservice::ScanBatchTuple> *scan_batch,
                       size_t scan_batch_idx,
                       size_t alias,
                       TxKey *end_key,
                       const TableName &table_name)
        : scan_batch_(scan_batch),
          scan_batch_idx_(scan_batch_idx),
          alias_(alias),
          end_key_(end_key),
          //   lock_type_(lock_type),
          table_name_(table_name)
    {
    }

    std::vector<txservice::ScanBatchTuple> *scan_batch_;
    size_t scan_batch_idx_;

    size_t alias_;
    TxKey *end_key_;
    const TableName &table_name_;
};

struct AbortTxRequest : public TemplateTxRequest<AbortTxRequest, bool>
{
    AbortTxRequest() = default;
};

struct CommitTxRequest : public TemplateTxRequest<CommitTxRequest, bool>
{
    CommitTxRequest() = default;
};

struct UpsertTableTxRequest
    : public TemplateTxRequest<UpsertTableTxRequest, bool>
{
    UpsertTableTxRequest(const TableName *table_name,
                         const std::string *curr_image,
                         uint64_t schema_ts,
                         const std::string *dirty_image,
                         txservice::OperationType op_type,
                         const std::string *alter_table_info_image = nullptr)
        : table_name_(table_name),
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

struct SplitRangeTxRequest : public TemplateTxRequest<SplitRangeTxRequest, bool>
{
    SplitRangeTxRequest(const TableName table_name,
                        const TableSchema *table_schema,
                        const TxKey *range_key,
                        std::unique_ptr<RangeRecord> range_record)
        : table_name_(std::move(table_name)),
          table_schema_(table_schema),
          range_key_(range_key),
          range_record_(std::move(range_record))
    {
    }

    const TableName table_name_;
    const TableSchema *table_schema_{nullptr};
    const TxKey *range_key_{nullptr};
    std::unique_ptr<RangeRecord> range_record_{nullptr};
};

struct FaultInjectTxRequest
    : public TemplateTxRequest<FaultInjectTxRequest, bool>
{
    FaultInjectTxRequest(const std::string &fault_name,
                         const std::string &fault_paras,
                         std::vector<int> &vct_node_id)
        : fault_name_(fault_name), fault_paras_(fault_paras)
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
        : tab_name_(tab_name),
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
