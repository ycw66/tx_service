#pragma once

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
                  LockType lock_type = LockType::ReadLock,
                  bool read_local = false)
        : tab_name_(tab_name),
          key_(key),
          rec_(rec),
          lock_type_(lock_type),
          read_local_(read_local)
    {
    }

    void Set(const TableName *tab_name,
             const TxKey *key,
             TxRecord *rec,
             LockType lock_type,
             bool read_local = false)
    {
        tab_name_ = tab_name;
        key_ = key;
        rec_ = rec;
        lock_type_ = lock_type;
        read_local_ = read_local;
    }

    const TableName *tab_name_;
    const TxKey *key_;
    TxRecord *rec_;
    LockType lock_type_;
    bool read_local_;
};

struct ReadOutsideTxRequest
    : public TemplateTxRequest<ReadOutsideTxRequest, RecordStatus>
{
public:
    ReadOutsideTxRequest(TxRecord &rec, bool is_deleted, uint64_t commit_ts)
        : rec_(rec), is_deleted_(is_deleted), commit_ts_(commit_ts)
    {
    }

    TxRecord &rec_;
    bool is_deleted_;
    uint64_t commit_ts_;
};

struct UpsertTxRequest : public TemplateTxRequest<UpsertTxRequest, Void>
{
    UpsertTxRequest(const TableName *tab_name,
                    TxKey *key,
                    TxRecord *rec,
                    bool is_del = false)
        : tab_name_(tab_name), key_(key), rec_(rec), is_delete_(is_del)
    {
    }

    UpsertTxRequest(const TableName *tab_name,
                    TxKey::Uptr key,
                    TxRecord::Uptr rec,
                    bool is_del = false)
        : tab_name_(tab_name),
          key_(std::move(key)),
          rec_(std::move(rec)),
          is_delete_(is_del)
    {
    }

    const TableName *tab_name_;
    TxKey::Uptr key_;
    TxRecord::Uptr rec_;
    bool is_delete_;
};

struct ScanOpenTxRequest : public TemplateTxRequest<ScanOpenTxRequest, size_t>
{
    ScanOpenTxRequest(const TableName *tabname,
                      ScanIndexType index_type,
                      const TxKey *start_key,
                      LockType lock_type,
                      bool inclusive = true,
                      ScanDirection direction = ScanDirection::Forward,
                      bool is_ckpt = false,
                      bool is_read_local = false)
        : tab_name_(tabname),
          indx_type_(index_type),
          start_key_(start_key),
          lock_type_(lock_type),
          inclusive_(inclusive),
          direct_(direction),
          is_ckpt_delta_(is_ckpt),
          read_local_(is_read_local)
    {
    }

    const TableName *tab_name_;
    ScanIndexType indx_type_;
    const TxKey *start_key_;
    LockType lock_type_;
    bool inclusive_;
    ScanDirection direct_;
    bool is_ckpt_delta_;
    bool read_local_;
};

struct ScanNextTxRequest
    : public TemplateTxRequest<
          ScanNextTxRequest,
          std::tuple<const TxKey *, const TxRecord *, bool>>
{
    ScanNextTxRequest(size_t alias, LockType lock_type)
        : alias_(alias), lock_type_(lock_type)
    {
    }

    size_t alias_;
    LockType lock_type_;
};

struct ScanCloseTxRequest : public TemplateTxRequest<ScanCloseTxRequest, Void>
{
    ScanCloseTxRequest(size_t alias, TxKey *end_key)
        : alias_(alias), end_key_(end_key)
    {
    }

    size_t alias_;
    TxKeyContainer end_key_;
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
                         const char *catalog_image,
                         size_t catalog_len,
                         bool is_deleted)
        : table_name_(table_name),
          catalog_image_(catalog_image),
          catalog_length_(catalog_len),
          is_deleted_(is_deleted)
    {
    }

    const TableName *table_name_;
    const char *catalog_image_;
    size_t catalog_length_;
    bool is_deleted_;
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
}  // namespace txservice
