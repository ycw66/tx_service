#pragma once

#include "scan.h"
#include "tx_container.h"
#include "tx_execution.h"
#include "tx_key.h"
#include "tx_record.h"
#include "tx_req_result.h"

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
    TemplateTxRequest() : cc_result_()
    {
    }

    virtual ~TemplateTxRequest() = default;

    void Process(TransactionExecution *txm) override
    {
        txm->Process(static_cast<Subtype &>(*this));
        return;
    }

    bool Finish()
    {
        return cc_result_.Status() != TxResultStatus::Unknown;
    }

    bool IsError() const
    {
        return cc_result_.IsError();
    }

    void Wait()
    {
        cc_result_.Wait();
    }

    const T &Result() const
    {
        return cc_result_.Value();
    }

    void Reset()
    {
        cc_result_.Reset();
    }

protected:
    TxResult<T> cc_result_;

    friend class TransactionExecution;
};

struct BeginRequest : public TemplateTxRequest<BeginRequest, Void>
{
    BeginRequest() = default;
};

struct ReadRequest : public TemplateTxRequest<ReadRequest, RecordStatus>
{
public:
    ReadRequest(const TableName *tab_name = nullptr,
                const TxKey *key = nullptr,
                TxRecord *rec = nullptr,
                ReadType type = ReadType::Inside)
        : tab_name_(tab_name), key_(key), rec_(rec), type_(type)
    {
    }

    void Set(const TableName *tab_name,
             const TxKey *key,
             TxRecord *rec,
             ReadType type)
    {
        tab_name_ = tab_name;
        key_ = key;
        rec_ = rec;
        type_ = type;
    }

    const TableName *tab_name_;
    const TxKey *key_;
    TxRecord *rec_;
    ReadType type_;
};

struct ReadOutsideRequest
    : public TemplateTxRequest<ReadOutsideRequest, RecordStatus>
{
public:
    ReadOutsideRequest(TxRecord &rec, bool is_deleted)
        : rec_(rec), is_deleted_(is_deleted)
    {
    }

    TxRecord &rec_;
    bool is_deleted_;
};

struct UpsertRequest : public TemplateTxRequest<UpsertRequest, Void>
{
    UpsertRequest(const TableName *tab_name,
                  TxKey *key,
                  TxRecord *rec,
                  SecondaryKeys *sks = nullptr,
                  bool is_del = false)
        : tab_name_(tab_name),
          key_(key),
          rec_(rec),
          skeys_(sks),
          is_delete_(is_del)
    {
    }

    UpsertRequest(const TableName *tab_name,
                  TxKey::Uptr key,
                  TxRecord::Uptr rec,
                  SecondaryKeys *sks = nullptr,
                  bool is_del = false)
        : tab_name_(tab_name),
          key_(std::move(key)),
          rec_(std::move(rec)),
          skeys_(sks),
          is_delete_(is_del)
    {
    }

    const TableName *tab_name_;
    TxKeyContainer key_;
    TxRecordContainer rec_;
    SecondaryKeys *skeys_;
    bool is_delete_;
};

struct ScanOpenRequest : public TemplateTxRequest<ScanOpenRequest, size_t>
{
    ScanOpenRequest(const TableName *tabname,
                    ScanIndexType index_type,
                    const TxKey *start_key,
                    bool inclusive = true,
                    ScanDirection direction = ScanDirection::Forward,
                    bool is_ckpt = false)
        : tab_name_(tabname),
          indx_type_(index_type),
          start_key_(start_key),
          inclusive_(inclusive),
          direct_(direction),
          is_ckpt_delta_(is_ckpt)
    {
    }

    const TableName *tab_name_;
    ScanIndexType indx_type_;
    const TxKey *start_key_;
    bool inclusive_;
    ScanDirection direct_;
    bool is_ckpt_delta_;
};

struct ScanNextRequest : public TemplateTxRequest<
                             ScanNextRequest,
                             std::tuple<const TxKey *, const TxRecord *, bool>>
{
    ScanNextRequest(size_t alias) : alias_(alias)
    {
    }

    size_t alias_;
};

struct ScanCloseRequest : public TemplateTxRequest<ScanCloseRequest, Void>
{
    ScanCloseRequest(size_t alias, TxKey *end_key)
        : alias_(alias), end_key_(end_key)
    {
    }

    size_t alias_;
    TxKeyContainer end_key_;
};

struct AbortRequest : public TemplateTxRequest<AbortRequest, bool>
{
    AbortRequest() = default;
};

struct CommitRequest : public TemplateTxRequest<CommitRequest, bool>
{
    CommitRequest() = default;
};

struct CreateTableRequest : public TemplateTxRequest<CreateTableRequest, bool>
{
    CreateTableRequest(const std::string &mysql_table_name,
                       const unsigned char *catalog_image,
                       size_t catalog_length)
        : mysql_table_name_(mysql_table_name),
          catalog_image_(catalog_image),
          catalog_length_(catalog_length)
    {
    }

    const std::string mysql_table_name_;
    const unsigned char *catalog_image_;
    size_t catalog_length_;
};

struct DropTableRequest : public TemplateTxRequest<DropTableRequest, bool>
{
    DropTableRequest(const std::string &mysql_table_name)
        : mysql_table_name_(mysql_table_name)
    {
    }

    const std::string mysql_table_name_;
};

struct FetchCatalogRequest : public TemplateTxRequest<FetchCatalogRequest, bool>
{
    FetchCatalogRequest(const std::string &mysql_table_name,
                        std::string *catalog_content)
        : mysql_table_name_(mysql_table_name), catalog_content_(catalog_content)
    {
    }

    const std::string mysql_table_name_;
    std::string *catalog_content_;
};

struct CheckCatalogVersionRequest
    : public TemplateTxRequest<CheckCatalogVersionRequest, bool>
{
    CheckCatalogVersionRequest(const std::string &mysql_table_name,
                               std::string &source_version)
        : mysql_table_name_(mysql_table_name), source_version_(source_version)
    {
    }

    const std::string mysql_table_name_;
    std::string source_version_;
};

struct FaultInjectRequest : public TemplateTxRequest<FaultInjectRequest, bool>
{
    FaultInjectRequest(const std::string &fault_name,
                       const std::string &fault_type,
                       int node_id)
        : fault_name_(fault_name), fault_type_(fault_type), node_id_(node_id)
    {
    }

    const std::string fault_name_;
    const std::string fault_type_;
    int node_id_;
};
}  // namespace txservice
