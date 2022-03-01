#pragma once

#include <condition_variable>
#include <fstream>
#include <functional>
#include <iostream>
#include <map>
#include <mutex>
#include <string_view>
#include <vector>

#include "cc/cc_map.h"
#include "cc/cc_shard.h"
#include "cc/ccm_scanner.h"
#include "cc_entry.h"
#include "cc_handler_result.h"
#include "cc_req_base.h"
#include "fault/fault_inject.h"
#include "log_closure.h"
#include "scan.h"
#include "tx_operation_result.h"
#include "type.h"

namespace txservice
{
template <typename KeyT, typename ValueT>
class TemplateCcMap;

template <typename SkT, typename PkT>
class SkCcMap;

template <typename RequestT, typename ResultType>
struct TemplatedCcRequest : public CcRequestBase
{
public:
    TemplatedCcRequest() : res_(), table_name_(nullptr), ccm_(nullptr)
    {
    }

    virtual ~TemplatedCcRequest() = default;

    bool Execute(CcShard &ccs) override
    {
        int8_t error_code = 0;

        if (ccm_ == nullptr)
        {
            assert(table_name_ != nullptr);
            ccm_ = ccs.GetCcm(*table_name_, node_group_id_, error_code);

            if (ccm_ == nullptr)
            {
                const TableSchemaView *schema_view = InitCcm(ccs);

                if (schema_view != nullptr)
                {
                    if (schema_view->version_ts_ == 0)
                    {
                        // The schema view is initialized but the current schema
                        // is unset (version_ts is 0). This means that there is
                        // an error when read the catalog from the data store.
                        // Returns the request with an error.
                        res_->SetError(100);
                        return true;
                    }
                    else if (schema_view->schema_ != nullptr)
                    {
                        ccm_ = ccs.GetCcm(
                            *table_name_, node_group_id_, error_code);
                    }
                    else
                    {
                        // The local node (LocalCcShards) contains a schema
                        // instance, which indicates that the table has been
                        // dropped (the schema pointer is null). Other than
                        // replay log requests, cc requests should never reach
                        // here, because before cc requests are sent, query
                        // compilation reads the table schema and acquires a
                        // read lock on it. If the table does not exist, the
                        // query never enters the execution phase. If the table
                        // exists during compilation, the table cannot be
                        // dropped since then.
                        res_->SetError(100);
                        return true;
                    }
                }
                else
                {
                    // The table's schema is not available yet. Cannot
                    // initialize the cc map. The request will be re-executed
                    // after the schema is fetched from the data store.
                    return false;
                }
            }
        }

        assert(ccm_ != nullptr);
        RequestT *typed_req = static_cast<RequestT *>(this);
        return ccm_->Execute(*typed_req);
    }

    CcHandlerResult<ResultType> *Result()
    {
        return res_;
    }

    CcMap *Ccm()
    {
        return ccm_;
    }

    virtual const TableName *GetTableName()
    {
        return table_name_;
    }

    uint32_t NodeGroupId() const
    {
        return node_group_id_;
    }

protected:
    /**
     * @brief Initializes the request's target cc map, if the table
     * schema is available and indicates that the table exists. Sends an async
     * request to fetch the schema from the data store, if the schema is not
     * cached locally.
     *
     * @return const TableSchemaView* The pointer to the schema view of the
     * request's target cc map. Null, if the schema is not cached at the node
     * level.
     */
    const TableSchemaView *InitCcm(CcShard &ccs)
    {
        const TableSchemaView *schema_view = ccs.GetCatalog(*table_name_);

        if (schema_view != nullptr)
        {
            const TableSchema *curr_schema = schema_view->schema_;
            if (curr_schema != nullptr && schema_view->version_ts_ > 0)
            {
                CcMap *pk_ccm = ccs.CreatePkCcMap(
                    *table_name_, curr_schema, node_group_id_);
                pk_ccm->commit_ts_ = schema_view->version_ts_;

                std::vector<TableName> index_names = curr_schema->IndexNames();
                for (const TableName &index_name : index_names)
                {
                    CcMap *sk_ccm = ccs.CreateSkCcMap(
                        index_name, curr_schema, node_group_id_);
                    sk_ccm->commit_ts_ = schema_view->version_ts_;
                }
            }
        }
        else
        {
            // The local node does not contain the table's schema instance. The
            // FetchCatalog() method sends an async request toward the data
            // store to fetch the catalog. After fetching is finished, this cc
            // request is re-enqueued for re-execution.
            ccs.FetchCatalog(*table_name_, this);
        }

        return schema_view;
    }

    CcHandlerResult<ResultType> *res_{nullptr};
    const TableName *table_name_{nullptr};
    CcMap *ccm_{nullptr};
    uint32_t node_group_id_{0};
};

struct AcquireCc : public TemplatedCcRequest<AcquireCc, AcquireKeyResult>
{
public:
    AcquireCc()
        : key_(nullptr),
          key_str_(nullptr),
          key_shard_code_(0),
          txid_(nullptr),
          tx_term_(-1),
          ts_(0),
          is_insert_(false)
    {
    }

    virtual ~AcquireCc() = default;

    AcquireCc(const AcquireCc &rhs) = delete;
    AcquireCc(AcquireCc &&rhs) = delete;

    void Set(const TableName *tname,
             const TxKey *key,
             const uint32_t key_shard_code,
             const TxId *txid,
             int64_t tx_term,
             uint64_t ts,
             bool is_insert,
             CcHandlerResult<AcquireKeyResult> *res,
             CcProtocol proto)
    {
        table_name_ = tname;
        key_ = key;
        key_str_ = nullptr;
        key_shard_code_ = key_shard_code;
        node_group_id_ = key_shard_code >> 10;
        txid_ = txid;
        tx_number_ = txid->TxNumber();
        tx_term_ = tx_term;
        ts_ = ts;
        is_insert_ = is_insert;
        res_ = res;
        ccm_ = nullptr;
        proto_ = proto;
        cce_ptr_ = nullptr;
    }

    void Set(const TableName *tname,
             const std::string *key_str,
             const uint32_t key_shard_code,
             const TxId *txid,
             int64_t tx_term,
             uint64_t ts,
             bool is_insert,
             CcHandlerResult<AcquireKeyResult> *res,
             CcProtocol proto)
    {
        table_name_ = tname;
        key_ = nullptr;
        key_str_ = key_str;
        key_shard_code_ = key_shard_code;
        node_group_id_ = key_shard_code >> 10;
        txid_ = txid;
        tx_number_ = txid->TxNumber();
        tx_term_ = tx_term;
        ts_ = ts;
        is_insert_ = is_insert;
        res_ = res;
        ccm_ = nullptr;
        proto_ = proto;
        cce_ptr_ = nullptr;
    }

    const TxKey *Key() const
    {
        return key_;
    }

    const std::string *KeyStr() const
    {
        return key_str_;
    }

    const TxId *Txid() const
    {
        return txid_;
    }

    int64_t TxTerm() const
    {
        return tx_term_;
    }

    uint64_t Ts() const
    {
        return ts_;
    }

    bool IsInsert() const
    {
        return is_insert_;
    }

    uint32_t KeyShardCode() const
    {
        return key_shard_code_;
    }

    void SetCcePtr(LruEntry *ptr)
    {
        cce_ptr_ = ptr;
    }

    LruEntry *CcePtr() const
    {
        return cce_ptr_;
    }

private:
    const TxKey *key_;
    const std::string *key_str_;
    uint32_t key_shard_code_;
    const TxId *txid_;
    int64_t tx_term_;
    uint64_t ts_;
    bool is_insert_;
    // The pointer of the cc entry to which this request is directed. The
    // pointer is set, when the request locates the cc entry but is
    // blocked due to read-write conflicts in 2PL. After the request is
    // unblocked and acquires the lock, the request's execution resumes without
    // further lookup of the cc entry.
    LruEntry *cce_ptr_;
};

struct AcquireAllCc : public TemplatedCcRequest<AcquireAllCc, AcquireAllResult>
{
public:
    AcquireAllCc() = default;
    virtual ~AcquireAllCc() = default;

    AcquireAllCc(const AcquireAllCc &rhs) = delete;
    AcquireAllCc(AcquireAllCc &&rhs) = delete;

    void Set(const TableName *tname,
             const TxKey *key,
             uint32_t node_group_id,
             TxNumber tx_num,
             int64_t tx_term,
             bool is_insert,
             CcHandlerResult<AcquireAllResult> *res,
             CcProtocol proto,
             LockType lk_type)
    {
        table_name_ = tname;
        key_ = key;
        key_str_ = nullptr;
        node_group_id_ = node_group_id;
        tx_number_ = tx_num;
        tx_term_ = tx_term;
        is_insert_ = is_insert;
        res_ = res;
        ccm_ = nullptr;
        proto_ = proto;
        cce_ptr_ = nullptr;
        lock_type_ = lk_type;
    }

    void Set(const TableName *tname,
             const std::string *key_str,
             uint32_t node_group_id,
             TxNumber tx_num,
             int64_t tx_term,
             bool is_insert,
             CcHandlerResult<AcquireAllResult> *res,
             CcProtocol proto,
             LockType lk_type)
    {
        table_name_ = tname;
        key_ = nullptr;
        key_str_ = key_str;
        node_group_id_ = node_group_id;
        tx_number_ = tx_num;
        tx_term_ = tx_term;
        is_insert_ = is_insert;
        res_ = res;
        ccm_ = nullptr;
        proto_ = proto;
        cce_ptr_ = nullptr;
        lock_type_ = lk_type;
    }

    const TxKey *Key() const
    {
        return key_;
    }

    const std::string *KeyStr() const
    {
        return key_str_;
    }

    int64_t TxTerm() const
    {
        return tx_term_;
    }

    bool IsInsert() const
    {
        return is_insert_;
    }

    void SetCcePtr(LruEntry *ptr)
    {
        cce_ptr_ = ptr;
        ccm_ = nullptr;
    }

    LruEntry *CcePtr() const
    {
        return cce_ptr_;
    }

    LockType LkType() const
    {
        return lock_type_;
    }

    TxKey *DecodedKey() const
    {
        return decoded_key_ == nullptr ? nullptr : decoded_key_.get();
    }

    void SetDecodedKey(std::unique_ptr<TxKey> decoded_key)
    {
        decoded_key_ = std::move(decoded_key);
        key_ = decoded_key_.get();
    }

private:
    const TxKey *key_{nullptr};
    const std::string *key_str_{nullptr};
    std::unique_ptr<TxKey> decoded_key_{nullptr};
    int64_t tx_term_{-1};
    bool is_insert_{false};
    /**
     * @brief The pointer of the cc entry to which this request is directed. The
     * pointer is set, when the request locates the cc entry but is blocked due
     * to read-write conflicts in 2PL. After the request is unblocked and
     * acquires the lock, the request's execution resumes without further lookup
     * of the cc entry.
     *
     */
    LruEntry *cce_ptr_{nullptr};
    LockType lock_type_{LockType::WriteIntent};
};

struct PostWriteCc : public TemplatedCcRequest<PostWriteCc, Void>
{
public:
    PostWriteCc()
        : cce_addr_(nullptr),
          commit_ts_(0),
          payload_(nullptr),
          payload_str_(nullptr),
          is_deleted_(false)
    {
    }

    PostWriteCc(const PostWriteCc &rhs) = delete;
    PostWriteCc(PostWriteCc &&rhs) = delete;

    void Set(const CcEntryAddr *addr,
             uint64_t tx_number,
             uint64_t ts,
             const TxRecord *rec,
             bool is_deleted,
             CcHandlerResult<Void> *res)
    {
        cce_addr_ = addr;
        tx_number_ = tx_number;
        commit_ts_ = ts;
        payload_ = rec;
        payload_str_ = nullptr;
        is_deleted_ = is_deleted;
        res_ = res;

        if (addr->InsertPtr() != 0)
        {
            const UntypedInsertEntry *ins_ptr =
                reinterpret_cast<const UntypedInsertEntry *>(addr->InsertPtr());
            ccm_ = ins_ptr->Parent().parent_map_;
        }
        else
        {
            const LruEntry *lru_entry =
                reinterpret_cast<const LruEntry *>(addr->CcePtr());
            ccm_ = lru_entry->parent_map_;
        }

        node_group_id_ = cce_addr_->NodeGroupId();
    }

    void Set(const CcEntryAddr *addr,
             uint64_t tx_number,
             uint64_t ts,
             const std::string *rec,
             bool is_deleted,
             CcHandlerResult<Void> *res)
    {
        cce_addr_ = addr;
        tx_number_ = tx_number;
        commit_ts_ = ts;
        payload_ = nullptr;
        payload_str_ = rec;
        is_deleted_ = is_deleted;
        res_ = res;

        if (addr->InsertPtr() != 0)
        {
            const UntypedInsertEntry *ins_ptr =
                reinterpret_cast<const UntypedInsertEntry *>(addr->InsertPtr());
            ccm_ = ins_ptr->Parent().parent_map_;
        }
        else
        {
            const LruEntry *lru_entry =
                reinterpret_cast<const LruEntry *>(addr->CcePtr());
            ccm_ = lru_entry->parent_map_;
        }
    }

    const CcEntryAddr *CceAddr() const
    {
        return cce_addr_;
    }

    uint64_t CommitTs() const
    {
        return commit_ts_;
    }

    const TxRecord *Payload() const
    {
        return payload_;
    }

    const std::string *PayloadStr() const
    {
        return payload_str_;
    }

    bool IsDeleted() const
    {
        return is_deleted_;
    }

private:
    const CcEntryAddr *cce_addr_;
    uint64_t commit_ts_;
    const TxRecord *payload_;
    const std::string *payload_str_;
    bool is_deleted_;
};

struct PostWriteAllCc : public TemplatedCcRequest<PostWriteAllCc, Void>
{
public:
    PostWriteAllCc() = default;
    PostWriteAllCc(const PostWriteAllCc &rhs) = delete;
    PostWriteAllCc(PostWriteAllCc &&rhs) = delete;

    void Set(const TableName *tname,
             const TxKey *key,
             uint32_t node_group_id,
             uint64_t tx_number,
             uint64_t ts,
             TxRecord *rec,
             DmlOperation dml_op,
             CcHandlerResult<Void> *res,
             PostWriteType commit_type)
    {
        table_name_ = tname;
        key_ = key;
        tx_number_ = tx_number;
        node_group_id_ = node_group_id;
        commit_ts_ = ts;
        payload_ = rec;
        payload_str_ = nullptr;
        dml_op_ = dml_op;
        res_ = res;
        ccm_ = nullptr;
        commit_type_ = commit_type;
    }

    void Set(const TableName *tname,
             const std::string *key_str,
             uint32_t node_group_id,
             uint64_t tx_number,
             uint64_t ts,
             const std::string *rec,
             DmlOperation dml_op,
             CcHandlerResult<Void> *res,
             PostWriteType commit_type)
    {
        table_name_ = tname;
        key_str_ = key_str;
        tx_number_ = tx_number;
        node_group_id_ = node_group_id;
        commit_ts_ = ts;
        payload_ = nullptr;
        payload_str_ = rec;
        dml_op_ = dml_op;
        res_ = res;
        ccm_ = nullptr;
        commit_type_ = commit_type;
    }

    uint64_t CommitTs() const
    {
        return commit_ts_;
    }

    TxRecord *Payload() const
    {
        return payload_;
    }

    const std::string *PayloadStr() const
    {
        return payload_str_;
    }

    DmlOperation DmlOp() const
    {
        return dml_op_;
    }

    const TxKey *Key() const
    {
        return key_;
    }

    const std::string *KeyStr() const
    {
        return key_str_;
    }

    const TxKey *DecodedKey() const
    {
        return decoded_key_ == nullptr ? nullptr : decoded_key_.get();
    }

    void SetDecodedKey(std::unique_ptr<TxKey> decoded_key)
    {
        decoded_key_ = std::move(decoded_key);
        key_ = decoded_key_.get();
    }

    const TxRecord *DecodedPayload() const
    {
        return decoded_payload_ == nullptr ? nullptr : decoded_payload_.get();
    }

    void SetDecodedPayload(std::unique_ptr<TxRecord> decoded_rec)
    {
        decoded_payload_ = std::move(decoded_rec);
        payload_ = decoded_payload_.get();
    }

    PostWriteType CommitType() const
    {
        return commit_type_;
    }

    void ResetCcm()
    {
        ccm_ = nullptr;
    }

private:
    const TxKey *key_{nullptr};
    const std::string *key_str_{nullptr};
    std::unique_ptr<TxKey> decoded_key_{nullptr};
    uint64_t commit_ts_{0};
    TxRecord *payload_{nullptr};
    const std::string *payload_str_{nullptr};
    std::unique_ptr<TxRecord> decoded_payload_{nullptr};
    DmlOperation dml_op_{DmlOperation::Update};
    PostWriteType commit_type_;
};

struct PostReadCc : public TemplatedCcRequest<PostReadCc, std::vector<TxId>>
{
public:
    PostReadCc() : cce_addr_(nullptr), commit_ts_(0), key_ts_(0), gap_ts_(0)
    {
    }

    PostReadCc(const PostReadCc &rhs) = delete;
    PostReadCc(PostReadCc &&rhs) = delete;

    void Set(const CcEntryAddr *addr,
             uint64_t tx_number,
             uint64_t commit_ts,
             uint64_t key_ts,
             uint64_t gap_ts,
             CcHandlerResult<std::vector<TxId>> *res,
             CcProtocol protocol)
    {
        cce_addr_ = addr;
        tx_number_ = tx_number;
        commit_ts_ = commit_ts;
        key_ts_ = key_ts;
        gap_ts_ = gap_ts;
        res_ = res;
        proto_ = protocol;
        res->Value().clear();

        const LruEntry *lru_entry =
            reinterpret_cast<const LruEntry *>(addr->CcePtr());
        ccm_ = lru_entry->parent_map_;

        node_group_id_ = cce_addr_->NodeGroupId();
    }

    const CcEntryAddr *CceAddr() const
    {
        return cce_addr_;
    }

    uint64_t CommitTs() const
    {
        return commit_ts_;
    }

    uint64_t KeyTs() const
    {
        return key_ts_;
    }

    uint64_t GapTs() const
    {
        return gap_ts_;
    }

private:
    const CcEntryAddr *cce_addr_;
    uint64_t commit_ts_;
    uint64_t key_ts_;
    uint64_t gap_ts_;
};

struct ReadCc : public TemplatedCcRequest<ReadCc, ReadKeyResult>
{
public:
    ReadCc()
        : key_(nullptr),
          key_str_(nullptr),
          rec_(nullptr),
          rec_str_(nullptr),
          ts_(0),
          type_(ReadType::Inside)
    {
    }

    ReadCc(const ReadCc &rhs) = delete;
    ReadCc(ReadCc &&rhs) = delete;

    void Set(const TableName *tn,
             const TxKey *key,
             uint32_t key_shard_code,
             TxRecord *rec,
             ReadType read_type,
             uint64_t tx_number,
             int64_t tx_term,
             uint64_t ts,
             CcHandlerResult<ReadKeyResult> *res,
             IsolationLevel iso_level,
             CcProtocol proto)
    {
        key_ = key;
        key_str_ = nullptr;
        key_shard_code_ = key_shard_code;
        rec_ = rec;
        rec_str_ = nullptr;
        type_ = read_type;
        res_ = res;
        tx_number_ = tx_number;
        tx_term_ = tx_term;
        ts_ = ts;
        proto_ = proto;
        isolation_level_ = iso_level;

        const CcEntryAddr &cce_addr = res->Value().cce_addr_;
        if (cce_addr.CcePtr() != 0)
        {
            const LruEntry *entry =
                reinterpret_cast<const LruEntry *>(cce_addr.CcePtr());
            ccm_ = entry->parent_map_;
            table_name_ = nullptr;
        }
        else
        {
            table_name_ = tn;
            ccm_ = nullptr;
        }

        node_group_id_ = key_shard_code >> 10;
        cce_ptr_ = nullptr;
    }

    void Set(const TableName *tn,
             const std::string *key_str,
             uint32_t key_shard_code,
             std::string *rec_str,
             ReadType read_type,
             uint64_t tx_number,
             int64_t tx_term,
             uint64_t ts,
             CcHandlerResult<ReadKeyResult> *res,
             IsolationLevel iso_level,
             CcProtocol proto)
    {
        key_ = nullptr;
        key_str_ = key_str;
        key_shard_code_ = key_shard_code;
        rec_ = nullptr;
        rec_str_ = rec_str;
        type_ = read_type;
        res_ = res;
        tx_number_ = tx_number;
        tx_term_ = tx_term;
        ts_ = ts;
        proto_ = proto;
        isolation_level_ = iso_level;

        const CcEntryAddr &cce_addr = res->Value().cce_addr_;
        if (cce_addr.CcePtr() != 0)
        {
            const LruEntry *entry =
                reinterpret_cast<const LruEntry *>(cce_addr.CcePtr());
            ccm_ = entry->parent_map_;
            table_name_ = nullptr;
        }
        else
        {
            table_name_ = tn;
            ccm_ = nullptr;
        }

        node_group_id_ = key_shard_code >> 10;
        cce_ptr_ = nullptr;
    }

    uint32_t KeyShardCode() const
    {
        return key_shard_code_;
    }

    const TxKey *Key() const
    {
        return key_;
    }

    const std::string *KeyBlob() const
    {
        return key_str_;
    }

    TxRecord *Record()
    {
        return rec_;
    }

    std::string *RecordBlob()
    {
        return rec_str_;
    }

    int64_t TxTerm() const
    {
        return tx_term_;
    }

    uint64_t ReadTimestamp() const
    {
        return ts_;
    }

    ReadType Type() const
    {
        return type_;
    }

    void SetReadType(ReadType type)
    {
        type_ = type;
    }

    void SetCcePtr(LruEntry *ptr)
    {
        cce_ptr_ = ptr;
    }

    LruEntry *CcePtr() const
    {
        return cce_ptr_;
    }

private:
    const TxKey *key_;
    const std::string *key_str_;
    uint32_t key_shard_code_;
    TxRecord *rec_;
    std::string *rec_str_;
    int64_t tx_term_;
    uint64_t ts_;
    ReadType type_;
    // The pointer of the cc entry to which this request is directed. The
    // pointer is set, when the request locates the cc entry but is
    // blocked due to conflicts in 2PL. After the request is unblocked and
    // acquires the lock, the request's execution resumes without further lookup
    // of the cc entry.
    LruEntry *cce_ptr_{nullptr};
};

struct ScanOpenBatchCc
    : public TemplatedCcRequest<ScanOpenBatchCc, ScanOpenResult>
{
public:
    ScanOpenBatchCc() = default;

    void Set(const TableName *tn,
             ScanIndexType type,
             uint32_t ng_id,
             const TxKey *start_key,
             bool inclusive,
             ScanDirection direction,
             uint64_t tx_number,
             const uint64_t &ts,
             ScanCache *cache,
             int64_t term,
             CcHandlerResult<ScanOpenResult> *open_res,
             IsolationLevel iso_level,
             CcProtocol proto,
             bool is_delta)
    {
        table_name_ = tn;
        index_type_ = type;
        node_group_id_ = ng_id;
        start_key_ = start_key;
        inclusive_ = inclusive;
        direct_ = direction;
        tx_number_ = tx_number;
        ts_ = ts;
        scan_cache_ = cache;
        term_ = term;
        res_ = open_res;
        iso_level_ = iso_level;
        proto_ = proto;
        ccm_ = nullptr;
        is_ckpt_delta_ = is_delta;
    }

private:
    ScanIndexType index_type_{ScanIndexType::Primary};
    const TxKey *start_key_{nullptr};
    bool inclusive_{false};
    ScanDirection direct_{ScanDirection::Forward};
    uint64_t ts_{0};
    ScanCache *scan_cache_{nullptr};
    int64_t term_{-1};
    IsolationLevel iso_level_{IsolationLevel::ReadCommitted};
    bool is_ckpt_delta_{false};

    template <typename KeyT, typename ValueT>
    friend class TemplateCcMap;

    template <typename SkT, typename PkT>
    friend class SkCcMap;
};

struct ScanNextBatchCc
    : public TemplatedCcRequest<ScanNextBatchCc, ScanNextResult>
{
public:
    ScanNextBatchCc() = default;

    void Set(const uint32_t &ng_id,
             const uint64_t &ts,
             ScanCache *cache,
             CcHandlerResult<ScanNextResult> *next_res,
             IsolationLevel iso_level,
             CcProtocol proto,
             bool is_delta)
    {
        node_group_id_ = ng_id;
        ts_ = ts;
        scan_cache_ = cache;
        const ScanTuple *last_tuple = cache->LastTuple();
        const LruEntry *lru_entry =
            reinterpret_cast<const LruEntry *>(last_tuple->cce_addr_.CcePtr());
        ccm_ = lru_entry->parent_map_;
        res_ = next_res;
        iso_level_ = iso_level;
        proto_ = proto;
        is_ckpt_delta_ = is_delta;
    }

private:
    uint64_t ts_{0};
    ScanCache *scan_cache_{nullptr};
    IsolationLevel iso_level_{IsolationLevel::ReadCommitted};
    bool is_ckpt_delta_{false};

    template <typename KeyT, typename ValueT>
    friend class TemplateCcMap;

    template <typename SkT, typename PkT>
    friend class SkCcMap;
};

struct ScanCloseCc : public TemplatedCcRequest<ScanCloseCc, Void>
{
public:
    ScanCloseCc() : alias_(0), key_(nullptr), inclusive_(false)
    {
    }

    ~ScanCloseCc() = default;

    ScanCloseCc(const ScanCloseCc &rhs) = delete;
    ScanCloseCc(ScanCloseCc &&rhs) = delete;

    size_t alias_;
    const TxKey *key_;
    bool inclusive_;
};

struct CkptTsCc : public CcRequestBase
{
public:
    CkptTsCc(size_t shard_cnt)
        : ckpt_ts_(UINT64_MAX),
          mux_(),
          cv_(),
          finish_cnt_(0),
          shard_cnt_(shard_cnt)
    {
    }

    CkptTsCc() = delete;
    CkptTsCc(const CkptTsCc &) = delete;
    CkptTsCc(CkptTsCc &&) = delete;

    bool Execute(CcShard &ccs) override
    {
        std::unique_lock<std::mutex> lk(mux_);
        ckpt_ts_ = std::min(ckpt_ts_, ccs.ActiveTxMinTs());

        assert(finish_cnt_ < shard_cnt_);
        ++finish_cnt_;
        if (finish_cnt_ == shard_cnt_)
        {
            cv_.notify_one();
        }

        // return false since CkptTsCc is not reused and does not need to call
        // CcRequestBase::Free
        return false;
    }

    void Wait()
    {
        std::unique_lock<std::mutex> lk(mux_);
        cv_.wait(lk, [this] { return finish_cnt_ == shard_cnt_; });
    }

    uint64_t GetCkptTs() const
    {
        return ckpt_ts_;
    }

private:
    uint64_t ckpt_ts_;
    std::mutex mux_;
    std::condition_variable cv_;
    size_t finish_cnt_;
    size_t shard_cnt_;
};

struct CkptScanCc : public CcRequestBase
{
public:
    enum struct CkptScanStatus
    {
        Ongoing,
        Finish,
        Error
    };

    static constexpr size_t CkptScanBatch = 1000;

    CkptScanCc() = delete;

    CkptScanCc(const TableName &table_name,
               const uint64_t ckpt_ts,
               std::vector<LruEntry *> &vec)
        : table_name_(table_name),
          ckpt_ts_(ckpt_ts),
          ckpt_vec_(vec),
          start_entry_(nullptr),
          status_(CkptScanStatus::Ongoing),
          mux_(),
          cv_(),
          ccm_(nullptr)
    {
    }

    bool Execute(CcShard &ccs) override
    {
        int8_t error_code = 0;
        if (ccm_ == nullptr)
        {
            ccm_ = ccs.GetCcm(table_name_, node_group_, error_code);
        }

        if (ccm_ != nullptr)
        {
            ccm_->Execute(*this);
        }
        else
        {
            Notify();
        }
        // return false since CkptScanCc is not re-used and does not need to
        // call CcRequestBase::Free
        return false;
    }

    void Wait()
    {
        using namespace std::chrono_literals;

        std::unique_lock<std::mutex> lk(mux_);
        if (status_ != CkptScanStatus::Finish)
        {
            cv_.wait(lk, [this] { return status_ == CkptScanStatus::Finish; });
        }
    }

    void Reset(uint32_t node_group)
    {
        std::lock_guard<std::mutex> lk(mux_);
        ccm_ = nullptr;
        start_entry_ = nullptr;
        status_ = CkptScanStatus::Ongoing;
        node_group_ = node_group;
    }

    void Notify()
    {
        std::unique_lock<std::mutex> lk(mux_);
        status_ = CkptScanCc::CkptScanStatus::Finish;
        cv_.notify_one();
    }

private:
    const TableName &table_name_;
    const uint64_t ckpt_ts_;
    std::vector<LruEntry *> &ckpt_vec_;
    LruEntry *start_entry_;
    CkptScanStatus status_;
    std::mutex mux_;
    std::condition_variable cv_;
    CcMap *ccm_;
    uint32_t node_group_;

    template <typename KeyT, typename ValueT>
    friend class TemplateCcMap;

    template <typename SkT, typename PkT>
    friend class SkCcMap;

    friend class Checkpointer;
};

/**
 * @brief The post-processing request that commits a write to a secondary index.
 * Contrary to the primary index where a write consists of the acquiring phase
 * and the post-processing phase, isolation levels other than serializability
 * allows a write to the secondary index to skip the acquiring phase. This is
 * because concurrency control of the secondary index always traces back to the
 * primary index, which resolves all read-write and write-write conflicts. If a
 * tx has no conflicts on the primary index, it is allowed to commit and will
 * commit the change to the secondary index in post-processing. The only
 * exception is serializability which avoids phantom reads. To detect and
 * resolve phantom reads, a secondary index write needs the acquiring phase to
 * resolve with index scans.
 *
 */
struct CommitSkCc : public TemplatedCcRequest<CommitSkCc, Void>
{
public:
    CommitSkCc()
        : skey_(nullptr),
          skey_str_(nullptr),
          pkey_(nullptr),
          pkey_str_(nullptr),
          ts_(0),
          is_delete_(false)
    {
    }

    CommitSkCc(const CommitSkCc &rhs) = delete;
    CommitSkCc(CommitSkCc &&rhs) = delete;

    void Set(const TableName *tn,
             const TxKey *sk,
             const TxKey *pk,
             uint32_t key_shard_code,
             uint64_t ts,
             bool is_delete,
             CcHandlerResult<Void> *res)
    {
        table_name_ = tn;
        ccm_ = nullptr;
        skey_ = sk;
        skey_str_ = nullptr;
        pkey_ = pk;
        pkey_str_ = nullptr;
        key_shard_code_ = key_shard_code;
        ts_ = ts;
        is_delete_ = is_delete;
        res_ = res;
        node_group_id_ = key_shard_code >> 10;
    }

    void Set(const TableName *tn,
             const std::string *sk,
             uint32_t key_shard_code,
             const std::string *pk,
             uint64_t ts,
             bool is_delete,
             CcHandlerResult<Void> *res)
    {
        table_name_ = tn;
        ccm_ = nullptr;
        skey_ = nullptr;
        skey_str_ = sk;
        key_shard_code_ = key_shard_code;
        pkey_ = nullptr;
        pkey_str_ = pk;
        ts_ = ts;
        is_delete_ = is_delete;
        res_ = res;
        node_group_id_ = key_shard_code >> 10;
    }

    uint32_t KeyShardCode() const
    {
        return key_shard_code_;
    }

private:
    const TxKey *skey_;
    const std::string *skey_str_;
    uint32_t key_shard_code_;
    const TxKey *pkey_;
    const std::string *pkey_str_;
    uint64_t ts_;
    bool is_delete_;

    template <typename SkT, typename PkT>
    friend class SkCcMap;
};

struct NegotiateCc : public CcRequestBase
{
public:
    NegotiateCc() : txid_(nullptr), tx_ts_(0), res_(nullptr)
    {
    }

    bool Execute(CcShard &ccs) override
    {
        TEntry *conflict_tx = ccs.LocateTx(*txid_);
        if (conflict_tx == nullptr)
        {
            // The conflicting tx has finished and the tx entry has been
            // recycled. The status of the conflicting tx is unknown and
            // negotiation is impossible.
            res_->SetError(1);
            return true;
        }

        if (conflict_tx->commit_ts_ == 0)
        {
            conflict_tx->lower_bound_ =
                std::max(conflict_tx->lower_bound_, tx_ts_);

            // The conflicting tx has not set its commit ts. Negotiation
            // succeeds. Returns the maximal ts, as if the conflicting tx will
            // commit in infinity and does not conflict with the read tx.
            res_->SetValue(UINT64_MAX);
            res_->SetFinished();
        }
        else if (conflict_tx->commit_ts_ > tx_ts_)
        {
            // The conflicting tx has set the commit ts, which is larger than
            // the read tx. Read stability is satisfied.
            res_->SetValue(conflict_tx->commit_ts_);
            res_->SetFinished();
        }
        else
        {
            res_->SetError(1);
        }

        return true;
    }

    void Set(const TxId *txid, uint64_t tx_ts, CcHandlerResult<uint64_t> *res)
    {
        txid_ = txid;
        tx_ts_ = tx_ts;
        res_ = res;
    }

private:
    const TxId *txid_;
    uint64_t tx_ts_;
    CcHandlerResult<uint64_t> *res_;
};

struct CheckTxStatusCc : public CcRequestBase
{
public:
    CheckTxStatusCc(const TxNumber &tx_number)
        : tx_status_(TxnStatus::Ongoing),
          exists_(false),
          finish_(false),
          mux_(),
          cv_()
    {
    }

    bool Execute(CcShard &ccs) override
    {
        std::unique_lock<std::mutex> lk(mux_);
        TEntry *tx_entry = ccs.LocateTx(tx_number_);
        if (tx_entry == nullptr)
        {
            exists_ = false;
        }
        else
        {
            exists_ = true;
            tx_status_ = tx_entry->status_;
        }

        finish_ = true;
        // This notify_one() must be within the lock scope, because the owner of
        // this cc request is the RPC thread, which blocks on the finish flag
        // and will exit and return immediately after the flag is set to true,
        // de-allocating the cc request.
        cv_.notify_one();

        // This request is a stack object of the calling RPC thread. Returns
        // false to prevent the tx processor from invoking Free() to recycle.
        return false;
    }

    void Wait()
    {
        std::unique_lock<std::mutex> lk(mux_);
        cv_.wait(lk, [this]() { return finish_; });
    }

    TxnStatus TxStatus() const
    {
        return tx_status_;
    }

    bool Exists() const
    {
        return exists_;
    }

private:
    TxnStatus tx_status_;
    bool exists_;
    bool finish_;
    std::mutex mux_;
    std::condition_variable cv_;
};

struct ClearTxCc : public CcRequestBase
{
public:
    ClearTxCc(uint32_t core_cnt)
        : mux_(), wait_cv_(), finish_cnt_(0), core_cnt_(core_cnt)
    {
    }

    ~ClearTxCc() = default;

    void Set(uint64_t tx_number)
    {
        tx_number_ = tx_number;
        std::unique_lock<std::mutex> lk(mux_);
        finish_cnt_ = 0;
    }

    bool Execute(CcShard &ccs) override
    {
        ccs.ClearTx(tx_number_);

        std::unique_lock<std::mutex> lk(mux_);
        ++finish_cnt_;
        wait_cv_.notify_one();

        // return false since ClearTxCc is not reused and does not need to call
        // CcRequestBase::Free
        return false;
    }

    void Wait()
    {
        std::unique_lock<std::mutex> lk(mux_);
        wait_cv_.wait(lk, [this]() { return finish_cnt_ == core_cnt_; });
    }

private:
    std::mutex mux_;
    std::condition_variable wait_cv_;
    uint32_t finish_cnt_;
    const uint32_t core_cnt_;
};

struct ReplayLogCc : public TemplatedCcRequest<ReplayLogCc, Void>
{
public:
    ReplayLogCc(uint32_t ng_id,
                const std::string_view &table_name_view,
                std::string_view &&blob,
                uint64_t commit_ts,
                uint64_t txn,
                std::mutex &mux,
                std::condition_variable &cv,
                uint32_t &finish_cnt)
        : table_name_str_(table_name_view),
          log_blob_view_(blob),
          commit_ts_(commit_ts),
          result_(nullptr),
          external_mux_(mux),
          external_cv_(cv),
          finish_cnt_(finish_cnt)
    {
        table_name_ = &table_name_str_;
        node_group_id_ = ng_id;
        tx_number_ = txn;
        res_ = &result_;

        result_.post_lambda_ = [this](CcHandlerResult<Void> *res)
        {
            // Notifies the external caller--the log replay handler--that the
            // specified log record has been replayed in all cores of this node.
            std::lock_guard<std::mutex> lk(external_mux_);
            ++finish_cnt_;
            external_cv_.notify_all();
        };
    }

    ReplayLogCc(const ReplayLogCc &rhs) = delete;
    ReplayLogCc(ReplayLogCc &&rhs) = delete;

    bool Execute(CcShard &ccs) override
    {
        int8_t error_code = 0;

        if (ccm_ == nullptr)
        {
            assert(table_name_ != nullptr);
            ccm_ = ccs.GetCcm(*table_name_, node_group_id_, error_code);

            if (ccm_ == nullptr)
            {
                const TableSchemaView *schema_view = InitCcm(ccs);

                if (schema_view != nullptr)
                {
                    if (schema_view->version_ts_ == 0)
                    {
                        // The schema view is initialized but the current schema
                        // is unset (version_ts is 0). This means that there is
                        // an error when reading the catalog from the data
                        // store. Returns the request with an error.
                        res_->SetError(100);
                        return false;
                    }
                    else if (schema_view->schema_ != nullptr)
                    {
                        ccm_ = ccs.GetCcm(
                            *table_name_, node_group_id_, error_code);
                    }
                    else
                    {
                        // The table is dropped. Skips replaying the log for
                        // this cc map.
                        res_->SetFinished();
                        return false;
                    }
                }
                else
                {
                    // The table's schema is not available yet. Cannot
                    // initialize the cc map. The request will be re-executed
                    // after the schema is fetched from the data store.
                    return false;
                }
            }
        }

        assert(ccm_ != nullptr);
        return ccm_->Execute(*this);
    }

    void SetFinish()
    {
        result_.SetFinished();
    }

    const std::string_view &LogContentView() const
    {
        return log_blob_view_;
    }

    uint64_t CommitTs() const
    {
        return commit_ts_;
    }

    uint64_t Txn() const
    {
        return tx_number_;
    }

    void ResetCcm()
    {
        ccm_ = nullptr;
    }

private:
    std::string table_name_str_;
    std::string_view log_blob_view_;
    uint64_t commit_ts_;
    CcHandlerResult<Void> result_;
    std::mutex &external_mux_;
    std::condition_variable &external_cv_;
    uint32_t &finish_cnt_;
};

struct FaultInjectCC : public TemplatedCcRequest<FaultInjectCC, bool>
{
public:
    FaultInjectCC() : fault_name_(nullptr), fault_paras_(nullptr)
    {
    }

    virtual ~FaultInjectCC() = default;

    FaultInjectCC(const FaultInjectCC &rhs) = delete;
    FaultInjectCC(FaultInjectCC &&rhs) = delete;

    virtual bool Execute(CcShard &ccs) override
    {
        FaultInject::Instance().InjectFault(
            *fault_name_, *fault_paras_);
        res_->SetFinished();
        return true;
    }

    void Set(const std::string *fault_name,
             const std::string *fault_paras,
             CcHandlerResult<bool> *res)
    {
        fault_name_ = fault_name;
        fault_paras_ = fault_paras;
        res_ = res;
    }

    const std::string *FaultName() const
    {
        return fault_name_;
    }

    const std::string *FaultParas() const
    {
        return fault_paras_;
    }

private:
    const std::string *fault_name_;
    const std::string *fault_paras_;
};
}  // namespace txservice
