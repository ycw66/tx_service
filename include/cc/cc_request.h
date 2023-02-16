#pragma once

#include <algorithm>  // std::min
#include <condition_variable>
#include <fstream>
#include <functional>
#include <iostream>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "../../log_service/include/fault_inject.h"
#include "cc/cc_map.h"
#include "cc/cc_shard.h"
#include "cc/ccm_scanner.h"
#include "cc_handler_result.h"
#include "cc_req_base.h"
#include "constants.h"
#include "dead_lock_check.h"
#include "error_messages.h"  // CcErrorCode
#include "fault/fault_inject.h"
#include "log_closure.h"
#include "proto/cc_request.pb.h"
#include "read_write_set.h"
#include "scan.h"
#include "sharder.h"
#include "tx_operation_result.h"
#include "type.h"
#include "util.h"

namespace txservice
{
template <typename KeyT, typename ValueT>
class TemplateCcMap;

template <typename SkT, typename PkT>
class SkCcMap;

class CcMap;

struct LruPage;

template <typename RequestT, typename ResultType>
struct TemplatedCcRequest : public CcRequestBase
{
public:
    TemplatedCcRequest() : res_(), table_name_(nullptr), ccm_(nullptr)
    {
    }

    virtual ~TemplatedCcRequest() = default;

    virtual bool ValidTermCheck()
    {
        int64_t cc_ng_term = Sharder::Instance().LeaderTerm(node_group_id_);
        if (cc_ng_term < 0)
        {
            return false;
        }
        else
        {
            return true;
        }
    }

    bool Execute(CcShard &ccs) override
    {
        if (!ValidTermCheck())
        {
            res_->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            return true;
        }

        CcMap *ccm = nullptr;
        RequestT *typed_req = static_cast<RequestT *>(this);

        if (parallel_req_ || ccm_ == nullptr)
        {
            // assert(table_name_ != nullptr);
            assert(table_name_->StringView() != empty_sv);
            ccm = ccs.GetCcm(*table_name_, node_group_id_);

            if (ccm == nullptr)
            {
                if (table_name_->Type() == TableType::RangePartition)
                {
                    // Get original table name for the range table name
                    const TableName base_table_name{
                        table_name_->GetBaseTableNameSV(), TableType::Primary};
                    const CatalogEntry *catalog_entry =
                        ccs.GetCatalog(base_table_name, node_group_id_);
                    if (catalog_entry == nullptr ||
                        catalog_entry->schema_ == nullptr)
                    {
                        ccs.FetchCatalog(base_table_name, node_group_id_, this);
                        return false;
                    }
                    TableSchema *table_schema = catalog_entry->schema_.get();

                    // The request is toward a special cc map that contains a
                    // table's range meta data.
                    std::map<const TxKey *, TableRangeEntry, PtrLessThan<TxKey>>
                        *ranges = ccs.GetTableRangesForATable(*table_name_,
                                                              node_group_id_);
                    if (ranges != nullptr)
                    {
                        ccs.CreateOrUpdateRangeCcMap(*table_name_,
                                                     table_schema,
                                                     node_group_id_,
                                                     table_schema->Version());
                        ccm = ccs.GetCcm(*table_name_, node_group_id_);
                    }
                    else
                    {
                        // The local node does not contain the table's ranges.
                        // The FetchTableRanges() method will send an async
                        // request toward the data store to fetch the table's
                        // ranges and initializes the table's range cc map.
                        // After fetching is finished, this cc request is
                        // re-enqueued for re-execution.
                        ccs.FetchTableRanges(*table_name_,
                                             table_schema->GetKVCatalogInfo(),
                                             this,
                                             node_group_id_);
                        return false;
                    }
                }
                else
                {
                    // Find base table name for index table.
                    // Fecth/Get Catalog is based on base table name, but Get
                    // ccmap is based on the real table name, for example, index
                    // should get the correspond sk_ccmap.
                    assert(table_name_->Type() == TableType::Primary ||
                           table_name_->Type() == TableType::Secondary);
                    const TableName base_table_name{
                        table_name_->GetBaseTableNameSV(), TableType::Primary};
                    const CatalogEntry *catalog_entry =
                        ccs.GetCatalog(base_table_name, node_group_id_);

                    if (catalog_entry != nullptr)
                    {
                        const TableSchema *curr_schema =
                            catalog_entry->schema_.get();
                        if (curr_schema != nullptr)
                        {
                            ccs.CreateOrUpdatePkCcMap(base_table_name,
                                                      curr_schema,
                                                      node_group_id_,
                                                      catalog_entry->Version());

                            std::vector<TableName> index_names =
                                curr_schema->IndexNames();
                            for (const TableName &index_name : index_names)
                            {
                                ccs.CreateOrUpdateSkCcMap(
                                    index_name,
                                    curr_schema,
                                    node_group_id_,
                                    catalog_entry->Version());
                            }

                            ccm = ccs.GetCcm(*table_name_, node_group_id_);
                        }
                        else
                        {
                            // The local node (LocalCcShards) contains a schema
                            // instance, which indicates that the table has been
                            // dropped. Returns the request with an error.
                            res_->SetError(
                                CcErrorCode::REQUESTED_TABLE_NOT_EXISTS);
                            return true;
                        }
                    }
                    else
                    {
                        // The local node does not contain the table's schema
                        // instance. The FetchCatalog() method will send an
                        // async request toward the data store to fetch the
                        // catalog. After fetching is finished, this cc request
                        // is re-enqueued for re-execution.
                        ccs.FetchCatalog(base_table_name, node_group_id_, this);
                        return false;
                    }
                }
            }

            if (!parallel_req_)
            {
                ccm_ = ccm;
            }
            assert(ccm != nullptr);
            return ccm->Execute(*typed_req);
        }
        else
        {
            // non parallel request which is executed again, e.g. initial
            // execution blocked by lock.
            assert(ccm_ != nullptr);
            return ccm_->Execute(*typed_req);
        }
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

    void Reset(const TableName *tname,
               CcHandlerResult<ResultType> *res,
               uint32_t node_group_id,
               uint64_t tx_number,
               CcProtocol proto = CcProtocol::OCC,
               IsolationLevel iso_level = IsolationLevel::ReadCommitted)
    {
        res_ = res;
        table_name_ = tname;
        ccm_ = nullptr;
        node_group_id_ = node_group_id;

        tx_number_ = tx_number;
        proto_ = proto;
        isolation_level_ = iso_level;
    }

    void AbortCcRequest() override
    {
        res_->SetError(CcErrorCode::DEAD_LOCK_ABORT);
    }

protected:
    CcHandlerResult<ResultType> *res_{nullptr};
    const TableName *table_name_{nullptr};
    // track the ccmap for ccrequest, it has two usages: a. ccreq is blocked by
    // lock and need to be re-execute. b. ccreq records the ccentry address, and
    // need to use ccentry to find the corresponding ccmap and ccshard.
    CcMap *ccm_{nullptr};
    uint32_t node_group_id_{0};
    // whether request is running on multi threads in parallel. e.g.
    // RemoteScanOpen.
    bool parallel_req_{false};
};

struct AcquireCc
    : public TemplatedCcRequest<AcquireCc, std::vector<AcquireKeyResult>>
{
public:
    AcquireCc()
        : key_(nullptr),
          key_str_(nullptr),
          key_shard_code_(0),
          tx_term_(-1),
          ts_(0),
          is_insert_(false)
    {
    }

    virtual ~AcquireCc() = default;

    AcquireCc(const AcquireCc &rhs) = delete;
    AcquireCc(AcquireCc &&rhs) = delete;

    void Reset(const TableName *tname,
               const TxKey *key,
               const uint32_t key_shard_code,
               TxNumber txn,
               int64_t tx_term,
               uint64_t ts,
               bool is_insert,
               CcHandlerResult<std::vector<AcquireKeyResult>> *res,
               uint32_t hd_res_idx,
               CcProtocol proto,
               IsolationLevel iso_level)
    {
        uint32_t ng_id = Sharder::Instance().ShardToCcNodeGroup(key_shard_code);
        TemplatedCcRequest<AcquireCc, std::vector<AcquireKeyResult>>::Reset(
            tname, res, ng_id, txn, proto, iso_level);

        key_ = key;
        key_str_ = nullptr;
        key_shard_code_ = key_shard_code;
        tx_term_ = tx_term;
        ts_ = ts;
        is_insert_ = is_insert;
        cce_ptr_ = nullptr;
        hd_result_idx_ = hd_res_idx;
        is_local_ = true;
    }

    void Reset(const TableName *tname,
               const std::string *key_str,
               const uint32_t key_shard_code,
               TxNumber txn,
               int64_t tx_term,
               uint64_t ts,
               bool is_insert,
               CcHandlerResult<std::vector<AcquireKeyResult>> *res,
               uint32_t hd_res_idx,
               CcProtocol proto,
               IsolationLevel iso_level)
    {
        uint32_t ng_id = Sharder::Instance().ShardToCcNodeGroup(key_shard_code);
        TemplatedCcRequest<AcquireCc, std::vector<AcquireKeyResult>>::Reset(
            tname, res, ng_id, txn, proto);

        key_ = nullptr;
        key_str_ = key_str;
        key_shard_code_ = key_shard_code;
        tx_term_ = tx_term;
        ts_ = ts;
        is_insert_ = is_insert;
        cce_ptr_ = nullptr;
        hd_result_idx_ = hd_res_idx;
        is_local_ = false;
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

    uint32_t HandlerResultIndex() const
    {
        return hd_result_idx_;
    }

    bool IsLocal() const
    {
        return is_local_;
    }

private:
    const TxKey *key_;
    const std::string *key_str_;
    uint32_t key_shard_code_;
    int64_t tx_term_;
    uint64_t ts_;
    bool is_insert_;
    // The pointer of the cc entry to which this request is directed. The
    // pointer is set, when the request locates the cc entry but is
    // blocked due to conflicts in 2PL. After the request is unblocked and
    // acquires the lock, the request's execution resumes without further lookup
    // of the cc entry.
    LruEntry *cce_ptr_{nullptr};
    uint32_t hd_result_idx_{0};
    bool is_local_{true};
};

struct AcquireAllCc : public TemplatedCcRequest<AcquireAllCc, AcquireAllResult>
{
public:
    AcquireAllCc() = default;
    virtual ~AcquireAllCc() = default;

    AcquireAllCc(const AcquireAllCc &rhs) = delete;
    AcquireAllCc(AcquireAllCc &&rhs) = delete;

    void Reset(const TableName *tname,
               const TxKey *key,
               uint32_t node_group_id,
               TxNumber tx_number,
               int64_t tx_term,
               bool is_insert,
               CcHandlerResult<AcquireAllResult> *res,
               CcProtocol proto,
               CcOperation cc_op,
               IsolationLevel iso_level = IsolationLevel::ReadCommitted)
    {
        TemplatedCcRequest<AcquireAllCc, AcquireAllResult>::Reset(
            tname, res, node_group_id, tx_number, proto, iso_level);

        key_ = key;
        key_str_ = nullptr;
        key_str_type_ = nullptr;
        tx_term_ = tx_term;
        is_insert_ = is_insert;
        decoded_key_ = nullptr;
        cc_op_ = cc_op;
        cce_ptr_ = nullptr;
        is_local_ = true;
    }

    void Reset(const TableName *tname,
               const std::string *key_str,
               const KeyType *key_str_type,
               uint32_t node_group_id,
               TxNumber tx_number,
               int64_t tx_term,
               bool is_insert,
               CcHandlerResult<AcquireAllResult> *res,
               CcProtocol proto,
               CcOperation cc_op,
               IsolationLevel iso_level = IsolationLevel::ReadCommitted)
    {
        TemplatedCcRequest<AcquireAllCc, AcquireAllResult>::Reset(
            tname, res, node_group_id, tx_number, proto, iso_level);

        key_ = nullptr;
        key_str_ = key_str;
        key_str_type_ = key_str_type;
        tx_term_ = tx_term;
        is_insert_ = is_insert;
        decoded_key_ = nullptr;
        cc_op_ = cc_op;
        cce_ptr_ = nullptr;
        is_local_ = false;
    }

    const TxKey *Key() const
    {
        return key_;
    }

    const std::string *KeyStr() const
    {
        return key_str_;
    }

    const KeyType *KeyStrType() const
    {
        return key_str_type_;
    }

    int64_t TxTerm() const
    {
        return tx_term_;
    }

    bool IsInsert() const
    {
        return is_insert_;
    }

    CcOperation CcOp() const
    {
        return cc_op_;
    }

    // LockType GetLockType() const
    // {
    //     return lock_type_;
    // }

    TxKey *DecodedKey() const
    {
        return decoded_key_ == nullptr ? nullptr : decoded_key_.get();
    }

    void SetDecodedKey(std::unique_ptr<TxKey> decoded_key)
    {
        decoded_key_ = std::move(decoded_key);
        key_ = decoded_key_.get();
    }

    void SetCcePtr(LruEntry *ptr)
    {
        cce_ptr_ = ptr;
    }

    LruEntry *CcePtr() const
    {
        return cce_ptr_;
    }

    void ResetCcm()
    {
        ccm_ = nullptr;
    }

    bool IsLocal() const
    {
        return is_local_;
    }

private:
    const TxKey *key_{nullptr};
    const std::string *key_str_{nullptr};
    const KeyType *key_str_type_{nullptr};
    std::unique_ptr<TxKey> decoded_key_{nullptr};
    int64_t tx_term_{-1};
    bool is_insert_{false};
    CcOperation cc_op_{CcOperation::Write};
    // The pointer of the cc entry to which this request is directed. The
    // pointer is set, when the request locates the cc entry but is
    // blocked due to conflicts in 2PL. After the request is unblocked and
    // acquires the lock, the request's execution resumes without further lookup
    // of the cc entry.
    LruEntry *cce_ptr_{nullptr};
    bool is_local_{true};
    KeyType key_type_{KeyType::Normal};
};

struct PostWriteCc : public TemplatedCcRequest<PostWriteCc, PostProcessResult>
{
public:
    PostWriteCc()
        : cce_addr_(nullptr),
          commit_ts_(0),
          payload_(nullptr),
          payload_str_(nullptr)
    {
    }

    PostWriteCc(const PostWriteCc &rhs) = delete;
    PostWriteCc(PostWriteCc &&rhs) = delete;

    void Reset(const CcEntryAddr *addr,
               uint64_t tx_number,
               uint64_t ts,
               const TxRecord *rec,
               OperationType operation_type,
               uint32_t key_shard_code,
               CcHandlerResult<PostProcessResult> *res,
               CcProtocol proto)
    {
        TemplatedCcRequest<PostWriteCc, PostProcessResult>::Reset(
            nullptr, res, addr->NodeGroupId(), tx_number, proto);

        cce_addr_ = addr;
        commit_ts_ = ts;
        payload_ = rec;
        payload_str_ = nullptr;
        operation_type_ = operation_type;
        key_shard_code_ = key_shard_code;

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

    void Reset(const CcEntryAddr *addr,
               uint64_t tx_number,
               uint64_t ts,
               const std::string *rec,
               OperationType operation_type,
               uint32_t key_shard_code,
               CcHandlerResult<PostProcessResult> *res,
               CcProtocol proto)
    {
        TemplatedCcRequest<PostWriteCc, PostProcessResult>::Reset(
            nullptr, res, addr->NodeGroupId(), tx_number, proto);

        cce_addr_ = addr;
        commit_ts_ = ts;
        payload_ = nullptr;
        payload_str_ = rec;
        operation_type_ = operation_type;
        key_shard_code_ = key_shard_code;

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

    OperationType GetOperationType() const
    {
        return operation_type_;
    }

    uint32_t KeyShardCode() const
    {
        return key_shard_code_;
    }

private:
    const CcEntryAddr *cce_addr_;
    uint64_t commit_ts_;
    const TxRecord *payload_;
    const std::string *payload_str_;
    OperationType operation_type_;
    uint32_t key_shard_code_;
};

struct PostWriteAllCc
    : public TemplatedCcRequest<PostWriteAllCc, PostProcessResult>
{
public:
    PostWriteAllCc() = default;
    PostWriteAllCc(const PostWriteAllCc &rhs) = delete;
    PostWriteAllCc(PostWriteAllCc &&rhs) = delete;

    void Reset(const TableName *tname,
               const TxKey *key,
               uint32_t node_group_id,
               uint64_t tx_number,
               uint64_t ts,
               TxRecord *rec,
               OperationType op_type,
               CcHandlerResult<PostProcessResult> *res,
               PostWriteType commit_type,
               int64_t tx_term)
    {
        TemplatedCcRequest<PostWriteAllCc, PostProcessResult>::Reset(
            tname, res, node_group_id, tx_number, CcProtocol::OCC);

        key_ = key;
        key_str_ = nullptr;
        key_str_type_ = nullptr;
        decoded_key_ = nullptr;
        commit_ts_ = ts;
        payload_ = rec;
        payload_str_ = nullptr;
        decoded_payload_ = nullptr;
        op_type_ = op_type;
        commit_type_ = commit_type;
        tx_term_ = tx_term;
    }

    void Reset(const TableName *tname,
               const TxKey *key,
               uint32_t node_group_id,
               uint64_t tx_number,
               uint64_t ts,
               std::unique_ptr<TxRecord> rec,
               OperationType op_type,
               CcHandlerResult<PostProcessResult> *res,
               PostWriteType commit_type,
               int64_t tx_term)
    {
        TemplatedCcRequest<PostWriteAllCc, PostProcessResult>::Reset(
            tname, res, node_group_id, tx_number, CcProtocol::OCC);

        key_ = key;
        key_str_type_ = nullptr;
        key_str_ = nullptr;
        decoded_key_ = nullptr;
        commit_ts_ = ts;
        payload_ = rec.get();
        payload_str_ = nullptr;
        decoded_payload_ = std::move(rec);
        op_type_ = op_type;
        commit_type_ = commit_type;
        tx_term_ = tx_term;
    }

    void Reset(const TableName *tname,
               const std::string *key_str,
               KeyType *key_str_type,
               uint32_t node_group_id,
               uint64_t tx_number,
               uint64_t ts,
               const std::string *rec,
               OperationType op_type,
               CcHandlerResult<PostProcessResult> *res,
               PostWriteType commit_type,
               int64_t tx_term)
    {
        TemplatedCcRequest<PostWriteAllCc, PostProcessResult>::Reset(
            tname, res, node_group_id, tx_number, CcProtocol::OCC);

        key_ = nullptr;
        key_str_ = key_str;
        key_str_type_ = key_str_type;
        decoded_key_ = nullptr;
        commit_ts_ = ts;
        payload_ = nullptr;
        payload_str_ = rec;
        decoded_payload_ = nullptr;
        op_type_ = op_type;
        commit_type_ = commit_type;
        tx_term_ = tx_term;
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

    OperationType OpType() const
    {
        return op_type_;
    }

    void SetTxKey(const TxKey *key)
    {
        key_ = key;
    }

    const TxKey *Key() const
    {
        return key_;
    }

    const std::string *KeyStr() const
    {
        return key_str_;
    }

    const KeyType *KeyStrType() const
    {
        return key_str_type_;
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

    // int64_t TxTerm()
    // {
    //     return tx_term_;
    // }

private:
    const TxKey *key_{nullptr};
    const std::string *key_str_{nullptr};
    const KeyType *key_str_type_{nullptr};
    std::unique_ptr<TxKey> decoded_key_{nullptr};
    uint64_t commit_ts_{0};
    TxRecord *payload_{nullptr};
    const std::string *payload_str_{nullptr};
    /**
     * @brief When the PostWriteAllCc request is a remote request or is a local
     * request but dispatched to a non-native cc node group, decoded_payload_
     * owns a record on which the request is executed.
     *
     * Need to set to nullptr after PostWriteAll is finished, in order to
     * decrease the use count of TableSchema shared pointer inside
     * CatalogRecord.
     *
     */
    std::unique_ptr<TxRecord> decoded_payload_{nullptr};
    OperationType op_type_{OperationType::Update};
    PostWriteType commit_type_;
    int64_t tx_term_{0};
};

struct PostReadCc : public TemplatedCcRequest<PostReadCc, PostProcessResult>
{
public:
    PostReadCc() : cce_addr_(nullptr), commit_ts_(0), key_ts_(0), gap_ts_(0)
    {
    }

    PostReadCc(const PostReadCc &rhs) = delete;
    PostReadCc(PostReadCc &&rhs) = delete;

    void Reset(const CcEntryAddr *addr,
               uint64_t tx_number,
               uint64_t commit_ts,
               uint64_t key_ts,
               uint64_t gap_ts,
               CcHandlerResult<PostProcessResult> *res,
               CcProtocol protocol,
               LockType lock_type)
    {
        TemplatedCcRequest<PostReadCc, PostProcessResult>::Reset(
            nullptr, res, addr->NodeGroupId(), tx_number, protocol);

        cce_addr_ = addr;
        commit_ts_ = commit_ts;
        key_ts_ = key_ts;
        gap_ts_ = gap_ts;
        lock_type_ = lock_type;
        res->Value().Clear();

        const LruEntry *lru_entry =
            reinterpret_cast<const LruEntry *>(addr->CcePtr());
        ccm_ = lru_entry->parent_map_;
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

    LockType GetLockType() const
    {
        return lock_type_;
    }

    void SetLockType(LockType lock_type)
    {
        lock_type_ = lock_type;
    }

private:
    const CcEntryAddr *cce_addr_;
    uint64_t commit_ts_;
    uint64_t key_ts_;
    uint64_t gap_ts_;
    LockType lock_type_;
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
          type_(ReadType::Inside),
          is_for_write_(false)
    {
    }

    ReadCc(const ReadCc &rhs) = delete;
    ReadCc(ReadCc &&rhs) = delete;

    bool ValidTermCheck() override
    {
        if (!is_in_recovering_)
        {
            return TemplatedCcRequest<ReadCc, ReadKeyResult>::ValidTermCheck();
        }
        else
        {
            int64_t cterm =
                Sharder::Instance().CandidateLeaderTerm(node_group_id_);
            if (cterm < 0)
            {
                return false;
            }
            else
            {
                return true;
            }
        }
    }

    void Reset(const TableName *tn,
               const TxKey *key,
               uint32_t key_shard_code,
               TxRecord *rec,
               ReadType read_type,
               uint64_t tx_number,
               int64_t tx_term,
               uint64_t ts,
               CcHandlerResult<ReadKeyResult> *res,
               IsolationLevel iso_level,
               CcProtocol protocol,
               bool is_for_write = false,
               std::vector<VersionTxRecord> *archives = nullptr,
               bool is_in_recovering = false)
    {
        uint32_t ng_id = Sharder::Instance().ShardToCcNodeGroup(key_shard_code);
        TemplatedCcRequest<ReadCc, ReadKeyResult>::Reset(
            nullptr, res, ng_id, tx_number, protocol, iso_level);

        key_ = key;
        key_str_ = nullptr;
        key_shard_code_ = key_shard_code;
        rec_ = rec;
        rec_str_ = nullptr;
        tx_term_ = tx_term;
        ts_ = ts;
        type_ = read_type;
        is_for_write_ = is_for_write;
        cce_ptr_ = nullptr;
        archives_ = archives;
        is_local_ = true;
        is_wait_for_post_write_ = false;
        is_in_recovering_ = is_in_recovering;

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
    }

    void Reset(const TableName *tn,
               const std::string *key_str,
               uint32_t key_shard_code,
               std::string *rec_str,
               ReadType read_type,
               uint64_t tx_number,
               int64_t tx_term,
               uint64_t ts,
               CcHandlerResult<ReadKeyResult> *res,
               IsolationLevel iso_level,
               CcProtocol protocol,
               bool is_for_write = false,
               std::vector<VersionTxRecord> *archives = nullptr)
    {
        uint32_t ng_id = Sharder::Instance().ShardToCcNodeGroup(key_shard_code);
        TemplatedCcRequest<ReadCc, ReadKeyResult>::Reset(
            nullptr, res, ng_id, tx_number, protocol, iso_level);

        key_ = nullptr;
        key_str_ = key_str;
        key_shard_code_ = key_shard_code;
        rec_ = nullptr;
        rec_str_ = rec_str;
        tx_term_ = tx_term;
        ts_ = ts;
        type_ = read_type;
        is_for_write_ = is_for_write;
        cce_ptr_ = nullptr;
        archives_ = archives;
        is_local_ = false;
        is_wait_for_post_write_ = false;
        is_in_recovering_ = false;

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

    void SetReadTimestamp(uint64_t ts)
    {
        ts_ = ts;
    }

    ReadType Type() const
    {
        return type_;
    }

    bool IsForWrite() const
    {
        return is_for_write_;
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

    void SetArchivesPtr(std::vector<VersionTxRecord> *ptr)
    {
        archives_ = ptr;
    }

    std::vector<VersionTxRecord> *ArchivesPtr() const
    {
        return archives_;
    }

    bool IsLocal() const
    {
        return is_local_;
    }

    void SetIsWaitForPostWrite(bool is_wait)
    {
        is_wait_for_post_write_ = is_wait;
    }

    bool IsWaitForPostWrite() const
    {
        return is_wait_for_post_write_;
    }

    enum struct BlockingType
    {
        None = 0,
        OnLock,
        OnLoading
    };

    BlockingType BlockType() const
    {
        return blocking_type_;
    }

    void SetBlockType(BlockingType type)
    {
        blocking_type_ = type;
    }

    bool IsInRecovering() const
    {
        return is_in_recovering_;
    }

private:
    const TxKey *key_;
    const std::string *key_str_;
    /**
     * @brief The key sharding code shards a key into one of the cores in the tx
     * service. The lower 10 bits of the sharding code are drawn from the lower
     * 10 bits of the hash code of the key. They shard the key into one core
     * given a node. The remaining 22 bits of the sharding code shard the key
     * into one of the nodes of the tx service. When the tx service is hash
     * partitioned, the higher 22 bits represent the cc node group ID. When the
     * tx service is range partitioned, the higher 22 bits represent the range
     * ID.
     *
     */
    uint32_t key_shard_code_;
    TxRecord *rec_;
    std::string *rec_str_;
    int64_t tx_term_;
    uint64_t ts_;
    ReadType type_;
    bool is_for_write_;
    // The pointer of the cc entry to which this request is directed. The
    // pointer is set, when the request locates the cc entry but is
    // blocked due to conflicts in 2PL. After the request is unblocked and
    // acquires the lock, the request's execution resumes without further lookup
    // of the cc entry.
    LruEntry *cce_ptr_{nullptr};
    bool is_local_{true};
    // If CcEntry's CommitTs is less than read_ts when do
    // "PkReadCorrespondingSk" or "SnapshotRead", there must be a PostWriteCc
    // request has not done, then, this read should wait until it is completed.
    bool is_wait_for_post_write_{false};
    // Is issued in a recovering process
    bool is_in_recovering_{false};

    BlockingType blocking_type_;

    std::vector<VersionTxRecord> *archives_{nullptr};
};

struct ScanOpenBatchCc
    : public TemplatedCcRequest<ScanOpenBatchCc, ScanOpenResult>
{
public:
    ScanOpenBatchCc() = default;

    void Reset(const TableName *tn,
               ScanIndexType type,
               uint32_t ng_id,
               const TxKey *start_key,
               bool inclusive,
               ScanDirection direction,
               uint64_t tx_number,
               const uint64_t &ts,
               ScanCache *cache,
               int64_t term,
               CcHandlerResult<ScanOpenResult> *res,
               IsolationLevel iso_level,
               CcProtocol protocol,
               bool is_for_write,
               bool is_delta,
               bool is_include_floor_cce = false)
    {
        TemplatedCcRequest<ScanOpenBatchCc, ScanOpenResult>::Reset(
            tn, res, ng_id, tx_number, protocol, iso_level);

        index_type_ = type;
        start_key_ = start_key;
        inclusive_ = inclusive;
        direct_ = direction;
        ts_ = ts;
        scan_cache_ = cache;
        term_ = term;
        is_for_write_ = is_for_write;
        is_ckpt_delta_ = is_delta;
        is_include_floor_cce_ = is_include_floor_cce;
        cce_ptr_ = nullptr;
        cce_ptr_scan_type_ = ScanType::ScanUnknow;
    }

    int64_t TxTerm()
    {
        return term_;
    }

    bool IsForWrite() const
    {
        return is_for_write_;
    }

    uint64_t ReadTimestamp() const
    {
        return ts_;
    }

    void SetCcePtr(LruEntry *ptr)
    {
        cce_ptr_ = ptr;
    }

    LruEntry *CcePtr() const
    {
        return cce_ptr_;
    }

    ScanType CcePtrScanType()
    {
        return cce_ptr_scan_type_;
    }

    void SetCcePtrScanType(ScanType scan_type)
    {
        cce_ptr_scan_type_ = scan_type;
    }

    void SetIsWaitForPostWrite(bool is_wait)
    {
        is_wait_for_post_write_ = is_wait;
    }

    bool IsWaitForPostWrite() const
    {
        return is_wait_for_post_write_;
    }

private:
    ScanIndexType index_type_{ScanIndexType::Primary};
    const TxKey *start_key_{nullptr};
    bool inclusive_{false};
    ScanDirection direct_{ScanDirection::Forward};
    uint64_t ts_{0};
    ScanCache *scan_cache_{nullptr};
    int64_t term_{-1};
    bool is_for_write_{false};
    bool is_ckpt_delta_{false};
    // If always include floor_cce in scan result
    bool is_include_floor_cce_{false};
    // Record the scan type of the blocked cce
    ScanType cce_ptr_scan_type_{ScanType::ScanUnknow};

    // The pointer of the cc entry to which this request is directed. The
    // pointer is set, when the request locates the cc entry but is
    // blocked due to conflicts in 2PL. After the request is unblocked and
    // acquires the lock, the request's execution resumes without further lookup
    // of the cc entry.
    LruEntry *cce_ptr_{nullptr};

    bool is_wait_for_post_write_{false};

    template <typename KeyT, typename ValueT>
    friend class TemplateCcMap;

    template <typename SkT, typename PkT>
    friend class SkCcMap;

    friend class CcMap;

    template <typename KeyT>
    friend class RangeCcMap;

    friend std::ostream &operator<<(std::ostream &outs,
                                    txservice::ScanOpenBatchCc *r);
};

struct ScanNextBatchCc
    : public TemplatedCcRequest<ScanNextBatchCc, ScanNextResult>
{
public:
    ScanNextBatchCc() = default;

    void Reset(const uint32_t &ng_id,
               TxNumber tx_number,
               const uint64_t &ts,
               ScanCache *cache,
               int64_t tx_term,
               CcHandlerResult<ScanNextResult> *next_res,
               IsolationLevel iso_level,
               CcProtocol protocol,
               bool is_for_write,
               bool is_delta)
    {
        TemplatedCcRequest<ScanNextBatchCc, ScanNextResult>::Reset(
            nullptr, next_res, ng_id, tx_number, protocol, iso_level);

        ts_ = ts;
        scan_cache_ = cache;
        tx_term_ = tx_term;
        is_for_write_ = is_for_write;
        is_ckpt_delta_ = is_delta;
        cce_ptr_ = nullptr;
        cce_ptr_scan_type_ = ScanType::ScanUnknow;

        const ScanTuple *last_tuple = cache->LastTuple();
        const LruEntry *lru_entry =
            reinterpret_cast<const LruEntry *>(last_tuple->cce_addr_.CcePtr());
        ccm_ = lru_entry->parent_map_;
    }

    int64_t TxTerm()
    {
        return tx_term_;
    }

    bool IsForWrite() const
    {
        return is_for_write_;
    }

    uint64_t ReadTimestamp() const
    {
        return ts_;
    }

    void SetCcePtr(LruEntry *ptr)
    {
        cce_ptr_ = ptr;
    }

    LruEntry *CcePtr() const
    {
        return cce_ptr_;
    }

    ScanType CcePtrScanType()
    {
        return cce_ptr_scan_type_;
    }

    void SetCcePtrScanType(ScanType scan_type)
    {
        cce_ptr_scan_type_ = scan_type;
    }

    void SetIsWaitForPostWrite(bool is_wait)
    {
        is_wait_for_post_write_ = is_wait;
    }

    bool IsWaitForPostWrite() const
    {
        return is_wait_for_post_write_;
    }

private:
    uint64_t ts_{0};
    ScanCache *scan_cache_{nullptr};
    int64_t tx_term_{-1};

    bool is_for_write_{false};
    bool is_ckpt_delta_{false};
    // Record the scan type of the blocked cce
    ScanType cce_ptr_scan_type_{ScanType::ScanUnknow};

    // The pointer of the cc entry to which this request is directed. The
    // pointer is set, when the request locates the cc entry but is
    // blocked due to conflicts in 2PL. After the request is unblocked and
    // acquires the lock, the request's execution resumes without further lookup
    // of the cc entry.
    LruEntry *cce_ptr_{nullptr};

    bool is_wait_for_post_write_{false};

    template <typename KeyT, typename ValueT>
    friend class TemplateCcMap;

    template <typename SkT, typename PkT>
    friend class SkCcMap;

    template <typename KeyT>
    friend class RangeCcMap;

    friend std::ostream &operator<<(std::ostream &outs,
                                    txservice::ScanNextBatchCc *r);
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

struct ScanSliceCc
    : public TemplatedCcRequest<ScanSliceCc, RangeScanSliceResult>
{
public:
    ScanSliceCc() : start_key_(nullptr), start_key_type_(RangeKeyType::RawPtr)
    {
        parallel_req_ = true;
    }

    ~ScanSliceCc()
    {
        if (start_key_type_ == RangeKeyType::UniquePtr)
        {
            start_key_uptr_ = nullptr;
            end_key_uptr_ = nullptr;
        }
    }

    void Set(const TableName &tbl_name,
             uint32_t range_id,
             uint32_t ng_id,
             int64_t ng_term,
             const TxKey *start_key,
             bool start_inclusive,
             const TxKey *end_key,
             bool end_inclusive,
             uint64_t read_ts,
             TxNumber tx_number,
             int64_t tx_term,
             CcHandlerResult<RangeScanSliceResult> &hd_res,
             IsolationLevel iso_level,
             CcProtocol protocol,
             bool read_for_write)
    {
        assert(hd_res.Value().is_local_);

        TemplatedCcRequest<ScanSliceCc, RangeScanSliceResult>::Reset(
            &tbl_name, &hd_res, ng_id, tx_number, protocol, iso_level);

        range_id_ = range_id;

        if (start_key_type_ == RangeKeyType::UniquePtr)
        {
            start_key_uptr_ = nullptr;
        }
        start_key_ = start_key;
        start_key_type_ = RangeKeyType::RawPtr;
        start_inclusive_ = start_inclusive;

        if (end_key_type_ == RangeKeyType::UniquePtr)
        {
            end_key_uptr_ = nullptr;
        }
        end_key_ = end_key;
        end_key_type_ = RangeKeyType::RawPtr;
        end_inclusive_ = end_inclusive;

        direction_ = hd_res.Value().ccm_scanner_->Direction();
        ts_ = read_ts;
        tx_term_ = tx_term;
        cc_ng_term_ = ng_term;
        read_for_write_ = read_for_write;

        range_slice_id_.Reset();
    }

    void Set(const TableName &tbl_name,
             uint32_t range_id,
             uint32_t ng_id,
             int64_t ng_term,
             const std::string *start_key_str,
             bool start_inclusive,
             const std::string *end_key_str,
             bool end_inclusive,
             ScanDirection direction,
             uint64_t read_ts,
             TxNumber tx_number,
             int64_t tx_term,
             CcHandlerResult<RangeScanSliceResult> &hd_res,
             IsolationLevel iso_level,
             CcProtocol protocol,
             bool read_for_write)
    {
        assert(!hd_res.Value().is_local_);

        TemplatedCcRequest<ScanSliceCc, RangeScanSliceResult>::Reset(
            &tbl_name, &hd_res, ng_id, tx_number, protocol, iso_level);

        range_id_ = range_id;

        if (start_key_type_ == RangeKeyType::UniquePtr)
        {
            start_key_uptr_ = nullptr;
        }
        start_key_str_ = start_key_str;
        start_key_type_ = RangeKeyType::Binary;
        start_inclusive_ = start_inclusive;

        if (end_key_type_ == RangeKeyType::UniquePtr)
        {
            end_key_uptr_ = nullptr;
        }
        end_key_str_ = end_key_str;
        end_key_type_ = RangeKeyType::Binary;
        end_inclusive_ = end_inclusive;

        direction_ = direction;
        ts_ = read_ts;
        tx_term_ = tx_term;
        cc_ng_term_ = ng_term;
        read_for_write_ = read_for_write;

        range_slice_id_.Reset();
    }

    bool IsLocal() const
    {
        return start_key_type_ == RangeKeyType::RawPtr;
    }

    uint32_t RangeId() const
    {
        return range_id_;
    }

    int64_t RangeCcNgTerm() const
    {
        return cc_ng_term_;
    }

    /**
     * @brief Set the term of the cc node group where the range resides. The
     * method is used by the first scan of the range to notify the sender the
     * range's hosting cc ng's term. The remaining scans in the range are
     * expected to observe the same term, as they rely on pointer stability to
     * resume a scan where last scan batch stops.
     *
     * @param cc_ng_term
     */
    void SetRangeCcNgTerm(int64_t cc_ng_term)
    {
        assert(cc_ng_term_ < 0 || cc_ng_term_ == cc_ng_term);
        cc_ng_term_ = cc_ng_term;

        if (IsLocal())
        {
            res_->Value().ccm_scanner_->SetPartitionNgTerm(cc_ng_term);
        }
    }

    const TxKey *StartKey() const
    {
        switch (start_key_type_)
        {
        case RangeKeyType::RawPtr:
            return start_key_;
        case RangeKeyType::Binary:
            return nullptr;
        case RangeKeyType::UniquePtr:
            return start_key_uptr_.get();
        default:
            return nullptr;
        }
    }

    const TxKey *EndKey() const
    {
        switch (end_key_type_)
        {
        case RangeKeyType::RawPtr:
            return end_key_;
        case RangeKeyType::Binary:
            return nullptr;
        case RangeKeyType::UniquePtr:
            return end_key_uptr_.get();
        default:
            return nullptr;
        }
    }

    const std::string *StartKeyStr() const
    {
        return start_key_type_ == RangeKeyType::Binary ? start_key_str_
                                                       : nullptr;
    }

    const std::string *EndKeyStr() const
    {
        return end_key_type_ == RangeKeyType::Binary ? end_key_str_ : nullptr;
    }

    void SetStartKey(std::unique_ptr<TxKey> start_key)
    {
        if (start_key_type_ == RangeKeyType::UniquePtr)
        {
            start_key_uptr_ = std::move(start_key);
        }
        else
        {
            start_key_type_ = RangeKeyType::UniquePtr;
            start_key_uptr_.release();
            start_key_uptr_ = std::move(start_key);
        }
    }

    void SetEndKey(std::unique_ptr<TxKey> end_key)
    {
        if (end_key_type_ == RangeKeyType::UniquePtr)
        {
            end_key_uptr_ = std::move(end_key);
        }
        else
        {
            end_key_type_ = RangeKeyType::UniquePtr;
            end_key_uptr_.release();
            end_key_uptr_ = std::move(end_key);
        }
    }

    bool StartInclusive() const
    {
        return start_inclusive_;
    }

    bool EndInclusive() const
    {
        return end_inclusive_;
    }

    ScanDirection Direction() const
    {
        return direction_;
    }

    uint64_t ReadTimestamp() const
    {
        return ts_;
    }

    int64_t TxTerm() const
    {
        return tx_term_;
    }

    ScanCache *GetLocalScanCache(size_t shard_id)
    {
        return IsLocal() ? res_->Value().ccm_scanner_->Cache(shard_id)
                         : nullptr;
    }

    RemoteScanCache *GetRemoteScanCache(size_t shard_id)
    {
        if (IsLocal())
        {
            return nullptr;
        }

        RangeScanSliceResult &slice_result = res_->Value();
        assert(shard_id < slice_result.remote_scan_caches_->size());
        return &slice_result.remote_scan_caches_->at(shard_id);
    }

    uint64_t PriorCceAddr(uint16_t shard_id)
    {
        assert(shard_id < cce_addr_vec_.size());
        return cce_addr_vec_[shard_id];
    }

    void SetShardCount(uint16_t shard_cnt)
    {
        cce_addr_vec_.resize(shard_cnt);
        cce_ptr_vec_.resize(shard_cnt);
        blocked_scan_types_.resize(shard_cnt);
        unfinished_core_cnt_.store(shard_cnt, std::memory_order_release);
    }

    void SetPriorCceAddr(uint64_t addr, uint16_t shard_id)
    {
        assert(shard_id < cce_addr_vec_.size());
        cce_addr_vec_[shard_id] = addr;
        cce_ptr_vec_[shard_id] = nullptr;
    }

    uint64_t PriorCceAddr(uint16_t shard_id) const
    {
        assert(shard_id < cce_addr_vec_.size());
        return cce_addr_vec_[shard_id];
    }

    LruEntry *CcePtr(uint16_t shard_id)
    {
        assert(shard_id < cce_ptr_vec_.size());
        return cce_ptr_vec_[shard_id];
    }

    void SetCcePtr(LruEntry *block_on_cce, uint16_t shard_id)
    {
        cce_ptr_vec_[shard_id] = block_on_cce;
    }

    /**
     * @brief Notifies the scan slice request that the scan at the calling core
     * has finished.
     *
     * @return true, if all cores have finished the scan.
     * @return false, if the scan is not completed in all cores.
     */
    bool SetFinish()
    {
        uint16_t remaining_cnt =
            unfinished_core_cnt_.fetch_sub(1, std::memory_order_acq_rel);

        if (remaining_cnt == 1)
        {
            res_->SetFinished();
        }

        return remaining_cnt == 1;
    }

    bool IsForWrite() const
    {
        return read_for_write_;
    }

    const RangeSliceId &SliceId() const
    {
        return range_slice_id_;
    }

    void SetSliceId(const RangeSliceId &slice)
    {
        range_slice_id_ = slice;
    }

    void SetCceScanType(ScanType blocked_scan_type, uint16_t core_id)
    {
        blocked_scan_types_[core_id] = blocked_scan_type;
    }

    ScanType BlockedCceScanType(uint16_t core_id) const
    {
        return blocked_scan_types_[core_id];
    }

    void SetIsWaitForPostWrite(bool is_wait)
    {
        is_wait_for_post_write_ = is_wait;
    }

    bool IsWaitForPostWrite() const
    {
        return is_wait_for_post_write_;
    }

private:
    uint32_t range_id_{0};

    enum struct RangeKeyType
    {
        RawPtr,
        Binary,
        UniquePtr
    };

    union
    {
        const TxKey *start_key_;
        const std::string *start_key_str_;
        std::unique_ptr<TxKey> start_key_uptr_;
    };
    RangeKeyType start_key_type_;
    bool start_inclusive_{false};

    union
    {
        const TxKey *end_key_;
        const std::string *end_key_str_;
        std::unique_ptr<TxKey> end_key_uptr_;
    };
    RangeKeyType end_key_type_;
    bool end_inclusive_{false};

    ScanDirection direction_{ScanDirection::Forward};
    uint64_t ts_{0};
    int64_t tx_term_{-1};
    bool read_for_write_{false};
    bool is_wait_for_post_write_{false};
    int64_t cc_ng_term_{-1};

    std::vector<ScanType> blocked_scan_types_;

    std::vector<uint64_t> cce_addr_vec_;
    std::vector<LruEntry *> cce_ptr_vec_;
    std::atomic<uint16_t> unfinished_core_cnt_{0};
    RangeSliceId range_slice_id_;
};

struct CkptTsCc : public CcRequestBase
{
public:
    CkptTsCc(size_t shard_cnt, NodeGroupId ng_id)
        : ckpt_ts_(UINT64_MAX),
          mux_(),
          cv_(),
          finish_cnt_(0),
          shard_cnt_(shard_cnt),
          cc_ng_id_(ng_id)
    {
        for (size_t i = 0; i < shard_cnt_; i++)
        {
            memory_usage_kb_vec_.emplace_back(0);
            log_usage_kb_vec_.emplace_back(0);
        }
    }

    CkptTsCc() = delete;
    CkptTsCc(const CkptTsCc &) = delete;
    CkptTsCc(CkptTsCc &&) = delete;

    bool Execute(CcShard &ccs) override
    {
        std::unique_lock<std::mutex> lk(mux_);
        ckpt_ts_ = std::min(ckpt_ts_, ccs.ActiveTxMinTs(cc_ng_id_));
        memory_usage_kb_vec_[ccs.LocalCoreId()] = ccs.mem_usage_ / 1000;

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

    uint64_t GetMemUsage() const
    {
        uint64_t total_usage = 0;
        for (uint64_t shard_usage : memory_usage_kb_vec_)
        {
            total_usage += shard_usage;
        }
        return total_usage;
    }

    uint64_t GetLogUsage() const
    {
        uint64_t total_usage = 0;
        for (uint64_t shard_usage : log_usage_kb_vec_)
        {
            total_usage += shard_usage;
        }
        return total_usage;
    }

private:
    uint64_t ckpt_ts_;
    std::mutex mux_;
    std::condition_variable cv_;
    std::atomic<size_t> finish_cnt_;
    size_t shard_cnt_;
    std::vector<uint64_t> memory_usage_kb_vec_;
    std::vector<uint64_t> log_usage_kb_vec_;
    NodeGroupId cc_ng_id_;
};

struct CkptScanCc : public TemplatedCcRequest<CkptScanCc, Void>
{
public:
    // how many pages to scan one time
    // static constexpr size_t CkptScanBatch = 20;
    // todo: limit scan by scanned size
    static constexpr size_t CkptScanBatchSize = 32 * 1024;

    CkptScanCc() = default;

    CkptScanCc(const TableName &table_name,
               const uint64_t ckpt_ts,
               const uint64_t node_group,
               std::vector<FlushRecord> &ckpt_vec,
               std::vector<FlushRecord> &archive_vec,
               std::vector<const TxKey *> &mv_base_vec,
               CcHandlerResult<Void> *res,
               const TxKey *target_start_key = nullptr,
               const TxKey *target_end_key = nullptr)
        : ckpt_ts_(ckpt_ts),
          ckpt_vec_(&ckpt_vec),
          archive_vec_(&archive_vec),
          mv_base_vec_(&mv_base_vec),
          start_key_(target_start_key),
          end_key_(target_end_key),
          start_page_(nullptr)
    {
        this->table_name_ = &table_name;
        node_group_id_ = node_group;
        res_ = res;
    }

    void Reset(uint32_t node_group)
    {
        ccm_ = nullptr;
        start_page_ = nullptr;
        node_group_id_ = node_group;
    }

    void Reset(const TableName &table_name,
               uint64_t ckpt_ts,
               std::vector<FlushRecord> &ckpt_vec,
               std::vector<FlushRecord> &archive_vec,
               std::vector<const TxKey *> &mv_vec,
               uint32_t node_group,
               CcHandlerResult<Void> *res,
               const TxKey *target_start_key = nullptr,
               const TxKey *target_end_key = nullptr)
    {
        ccm_ = nullptr;
        ckpt_ts_ = ckpt_ts;
        node_group_id_ = node_group;
        this->table_name_ = &table_name;
        ckpt_vec_ = &ckpt_vec;
        archive_vec_ = &archive_vec;
        mv_base_vec_ = &mv_vec;
        start_key_ = target_start_key;
        end_key_ = target_end_key;
        start_page_ = nullptr;
        res_ = res;
    }

private:
    uint64_t ckpt_ts_;
    std::vector<FlushRecord> *ckpt_vec_;
    std::vector<FlushRecord> *archive_vec_;
    // Cache the entries to move record from "base" table to "archive" table
    std::vector<const TxKey *> *mv_base_vec_;
    // Start/end key of target range if the scan is on a range only, nullptr if
    // it's on entire table.
    const TxKey *start_key_{nullptr};
    const TxKey *end_key_{nullptr};
    LruPage *start_page_;

    template <typename KeyT, typename ValueT>
    friend class TemplateCcMap;

    friend std::ostream &operator<<(std::ostream &outs,
                                    txservice::CkptScanCc *r);
};

struct CkptStatisticsCc : public CcRequestBase
{
public:
    CkptStatisticsCc(const Statistics *statistics,
                     store::Statistics *store_statistics)
        : statistics_(statistics),
          store_statistics_(store_statistics),
          done_(false),
          mux_(),
          cv_()
    {
    }

    bool Execute(CcShard &ccs) override
    {
        std::unique_lock<std::mutex> lk(mux_);

        statistics_->ToSerializableObj(store_statistics_);

        done_ = true;
        cv_.notify_one();
        return false;
    }

    void Wait()
    {
        std::unique_lock<std::mutex> lk(mux_);
        cv_.wait(lk, [this] { return done_; });
    }

private:
    const Statistics *statistics_{nullptr};
    store::Statistics *store_statistics_{nullptr};

    bool done_{false};
    std::mutex mux_;
    std::condition_variable cv_;
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
            res_->SetError(CcErrorCode::NEGOTIATED_TX_UNKNOWN);
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
            res_->SetValue(std::move(conflict_tx->commit_ts_));
            res_->SetFinished();
        }
        else
        {
            res_->SetError(CcErrorCode::NEGOTIATE_TX_ERR);
        }

        return true;
    }

    void Reset(const TxId *txid, uint64_t tx_ts, CcHandlerResult<uint64_t> *res)
    {
        txid_ = txid;
        tx_ts_ = tx_ts;
        res_ = res;
    }

    CcHandlerResult<uint64_t> *Result()
    {
        return res_;
    }

private:
    const TxId *txid_;
    uint64_t tx_ts_;
    CcHandlerResult<uint64_t> *res_;
    friend std::ostream &operator<<(std::ostream &outs,
                                    txservice::NegotiateCc *r);
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
        tx_number_ = tx_number;
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

    friend std::ostream &operator<<(std::ostream &outs,
                                    txservice::CheckTxStatusCc *r);
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
                const TableType table_type,
                std::string_view &&blob,
                uint64_t commit_ts,
                uint64_t txn,
                std::mutex &mux,
                std::condition_variable &cv,
                uint32_t &finish_cnt,
                bool &recovery_error)
        : table_name_holder_(table_name_view, table_type),
          log_blob_view_(blob),
          commit_ts_(commit_ts),
          result_(nullptr),
          external_mux_(mux),
          external_cv_(cv),
          finish_cnt_(finish_cnt),
          recovery_error_(recovery_error)
    {
        table_name_ = &table_name_holder_;
        node_group_id_ = ng_id;
        tx_number_ = txn;
        res_ = &result_;
    }

    ReplayLogCc(const ReplayLogCc &rhs) = delete;
    ReplayLogCc(ReplayLogCc &&rhs) = delete;

    // ReplayLogCc is always stack object and won't be reused, worse, it might
    // be destructed before Execute returns, so always return false as caller
    // should never access this object after Execute returns
    bool Execute(CcShard &ccs) override
    {
        int64_t cc_ng_candid_term =
            Sharder::Instance().CandidateLeaderTerm(node_group_id_);
        int64_t cc_ng_term = Sharder::Instance().LeaderTerm(node_group_id_);
        if (cc_ng_candid_term < 0 && cc_ng_term < 0)
        {
            SetFinish();
            return false;
        }

        if (ccm_ == nullptr)
        {
            assert(table_name_ != nullptr);
            ccm_ = ccs.GetCcm(*table_name_, node_group_id_);

            if (ccm_ == nullptr)
            {
                if (table_name_->Type() == TableType::RangePartition)
                {
                    // Try to load the base table/index ccm
                    const txservice::TableName base_table_name{
                        table_name_->GetBaseTableNameSV(), TableType::Primary};
                    CcMap *base_table_ccm =
                        ccs.GetCcm(base_table_name, node_group_id_);

                    const CatalogEntry *catalog_entry = nullptr;
                    // Makse sure base table catalog and ccm already exists
                    if (base_table_ccm == nullptr)
                    {
                        catalog_entry =
                            InitCcm(*table_name_, node_group_id_, ccs);

                        // Wait for FetchCatalogCc to finish
                        if (catalog_entry == nullptr)
                        {
                            return false;
                        }
                    }
                    else
                    {
                        catalog_entry =
                            ccs.GetCatalog(base_table_name, node_group_id_);
                    }

                    if (catalog_entry == nullptr ||
                        catalog_entry->schema_ == nullptr)
                    {
                        // table has been dropped
                        SetFinish();
                        return false;
                    }
                    else if (catalog_entry->Version() == 0)
                    {
                        SetRecoveryError();
                        return false;
                    }

                    table_schema_ = catalog_entry->schema_.get();

                    // The request is toward a special cc map that contains a
                    // tabmode's ranges.
                    auto ranges = ccs.GetTableRangesForATable(*table_name_,
                                                              node_group_id_);
                    if (ranges != nullptr)
                    {
                        ccs.CreateOrUpdateRangeCcMap(*table_name_,
                                                     table_schema_,
                                                     node_group_id_,
                                                     table_schema_->Version());
                        ccm_ = ccs.GetCcm(*table_name_, node_group_id_);
                    }
                    else
                    {
                        // The local node does not contain the table's ranges.
                        // The FetchTableRanges() method will send an async
                        // request toward the data store to fetch the table's
                        // ranges and initializes the table's range cc map.
                        // After fetching is finished, this cc request is
                        // re-enqueued for re-execution.
                        ccs.FetchTableRanges(*table_name_,
                                             table_schema_->GetKVCatalogInfo(),
                                             this,
                                             node_group_id_);
                        return false;
                    }
                }
                else
                {
                    const CatalogEntry *catalog_entry =
                        InitCcm(*table_name_, node_group_id_, ccs);

                    if (catalog_entry != nullptr)
                    {
                        if (catalog_entry->Version() == 0)
                        {
                            // The schema view is initialized but the current
                            // schema is unset (version_ts is 0). This means
                            // that there is an error when reading the catalog
                            // from the data store. Returns the request with an
                            // error.
                            SetRecoveryError();
                            return false;
                        }
                        else if (catalog_entry->schema_ != nullptr &&
                                 commit_ts_ >= catalog_entry->Version())
                        {
                            ccm_ = ccs.GetCcm(*table_name_, node_group_id_);
                            assert(ccm_ != nullptr);
                        }
                        else
                        {
                            // The table is dropped. Skips replaying the log for
                            // this cc map.
                            SetFinish();
                            return false;
                        }
                    }
                    else
                    {
                        // The table's schema is not available yet. Cannot
                        // initialize the cc map. The request will be
                        // re-executed after the schema is fetched from the data
                        // store.
                        return false;
                    }
                }
            }
        }

        ccm_->Execute(*this);
        return false;
    }

    void SetFinish()
    {
        // Notifies the external caller--the log replay handler--that the
        // specified log record has been replayed in all cores of this node.
        // HandlerResult is not used by external caller, hence we don't need to
        // call HandlerResult.SetFinished().
        std::lock_guard<std::mutex> lk(external_mux_);
        ++finish_cnt_;
        external_cv_.notify_all();
    }

    void SetRecoveryError()
    {
        std::lock_guard<std::mutex> lk(external_mux_);
        ++finish_cnt_;
        recovery_error_ = true;
        external_cv_.notify_all();
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

    const TableSchema *GetTableSchema()
    {
        return table_schema_;
    }

    void SetCatalogCcEntry(CcEntryAddr catalog_cc_entry_addr,
                           ReadSetEntry read_set_entry)
    {
        catalog_cc_entry_ = std::optional<std::pair<CcEntryAddr, ReadSetEntry>>{
            std::make_pair(catalog_cc_entry_addr, read_set_entry)};
    }

    const std::optional<std::pair<CcEntryAddr, ReadSetEntry>>
    GetCatalogCcEntry()
    {
        if (catalog_cc_entry_ == std::nullopt)
        {
            return std::nullopt;
        }
        CcEntryAddr cce_addr = catalog_cc_entry_->first;
        ReadSetEntry read_set_entry = catalog_cc_entry_->second;

        return std::optional<std::pair<CcEntryAddr, ReadSetEntry>>{
            std::make_pair(cce_addr, read_set_entry)};
    }

private:
    TableName table_name_holder_;  //  not string owner, sv -> protobuf message.
    std::string_view log_blob_view_;
    uint64_t commit_ts_;
    CcHandlerResult<Void> result_;
    std::mutex &external_mux_;
    std::condition_variable &external_cv_;
    uint32_t &finish_cnt_;
    bool &recovery_error_;
    const struct TableSchema *table_schema_{nullptr};
    std::optional<std::pair<CcEntryAddr, ReadSetEntry>> catalog_cc_entry_{
        std::nullopt};

    friend std::ostream &operator<<(std::ostream &outs,
                                    txservice::ReplayLogCc *r);
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
        FaultInject::Instance().InjectFault(*fault_name_, *fault_paras_);
        txlog::FaultInject::Instance().InjectFault(*fault_name_, *fault_paras_);
        res_->SetFinished();
        return true;
    }

    void Reset(const std::string *fault_name,
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

struct CleanCcEntryForTestCc
    : public TemplatedCcRequest<CleanCcEntryForTestCc, bool>
{
public:
    CleanCcEntryForTestCc()
        : key_(nullptr),
          key_str_(nullptr),
          key_shard_code_(0U),
          only_archives_(false)
    {
    }

    CleanCcEntryForTestCc(const CleanCcEntryForTestCc &rhs) = delete;
    CleanCcEntryForTestCc(CleanCcEntryForTestCc &&rhs) = delete;

    void Reset(const TableName *tn,
               const TxKey *key,
               bool only_archives,
               bool flush,
               uint32_t key_shard_code,
               uint64_t tx_number,
               CcHandlerResult<bool> *res)
    {
        uint32_t ng_id = Sharder::Instance().ShardToCcNodeGroup(key_shard_code);
        TemplatedCcRequest<CleanCcEntryForTestCc, bool>::Reset(
            tn, res, ng_id, tx_number);
        key_ = key;
        key_str_ = nullptr;
        key_shard_code_ = key_shard_code;
        only_archives_ = only_archives;
        flush_ = flush;
        res_ = res;
    }

    void Reset(const TableName *tn,
               const std::string *key_str,
               bool only_archives,
               bool flush,
               uint32_t key_shard_code,
               uint64_t tx_number,
               CcHandlerResult<bool> *res)
    {
        uint32_t ng_id = Sharder::Instance().ShardToCcNodeGroup(key_shard_code);
        TemplatedCcRequest<CleanCcEntryForTestCc, bool>::Reset(
            tn, res, ng_id, tx_number);
        key_ = nullptr;
        key_str_ = key_str;
        key_shard_code_ = key_shard_code;
        only_archives_ = only_archives;
        flush_ = flush;
        res_ = res;
    }

    const TxKey *Key() const
    {
        return key_;
    }

    bool OnlyCleanArchives() const
    {
        return only_archives_;
    }

    bool WithFlush() const
    {
        return flush_;
    }

private:
    const TxKey *key_;
    const std::string *key_str_;
    uint32_t key_shard_code_;
    bool only_archives_;
    bool flush_;
};

struct CheckDeadLockResult
{
    /**
     * Record the transactions which are holding or waiting lock on this
     * ccentry.
     */
    struct EntryLockInfo
    {
        // The txids that locked the ccentry
        std::unordered_set<uint64_t> lock_txids;
        // The txids that waited the ccentry
        std::unordered_set<uint64_t> wait_txids;
    };

    CheckDeadLockResult() : unfinish_count_(0)
    {
    }

    void Reset()
    {
        entry_lock_info_vec_.resize(
            Sharder::Instance().GetLocalCcShardsCount());
        for (size_t i = 0; i < entry_lock_info_vec_.size(); i++)
        {
            entry_lock_info_vec_[i].clear();
        }

        txid_ety_lock_count_.resize(
            Sharder::Instance().GetLocalCcShardsCount());
        for (size_t i = 0; i < txid_ety_lock_count_.size(); i++)
        {
            txid_ety_lock_count_[i].clear();
        }

        unfinish_count_.store((int16_t) entry_lock_info_vec_.size(),
                              std::memory_order_relaxed);
    }

    // Every vector element corresponding to a core, the element is the map
    // between the ccentry's address and its locked and waited txids.
    std::vector<std::unordered_map<uint64_t, EntryLockInfo>>
        entry_lock_info_vec_;
    // Every vector element corresponding to a core, the element is the map
    // between txid and its locked entrys
    std::vector<std::unordered_map<uint64_t, uint32_t>> txid_ety_lock_count_;
    std::atomic_int16_t unfinish_count_;
};

struct CheckDeadLockCc : public CcRequestBase
{
public:
    CheckDeadLockCc()
    {
    }

    virtual ~CheckDeadLockCc() = default;
    CheckDeadLockCc(const CheckDeadLockCc &rhs) = delete;
    CheckDeadLockCc(CheckDeadLockCc &&rhs) = delete;

    bool Execute(CcShard &ccs) override
    {
        ccs.CollectLockWaitingInfo(dead_lock_result_);

        int16_t count = dead_lock_result_.unfinish_count_.fetch_sub(
            1, std::memory_order_acq_rel);
        if (count == 1)
        {
            DeadLockCheck::MergeLocalWaitingLockInfo(dead_lock_result_);
        }
        return true;
    }

    void Reset()
    {
        dead_lock_result_.Reset();
    }

    void Free() override
    {
        if (dead_lock_result_.unfinish_count_.load(std::memory_order_relaxed) ==
            0)
        {
            CcRequestBase::Free();
        }
    }

    CheckDeadLockResult &GetDeadLockResult()
    {
        return dead_lock_result_;
    }

protected:
    CheckDeadLockResult dead_lock_result_;
};

struct AbortTransactionCc : public CcRequestBase
{
public:
    AbortTransactionCc() : entry_addr_(0), tx_id_wait_(0)
    {
    }

    virtual ~AbortTransactionCc() = default;
    AbortTransactionCc(const AbortTransactionCc &rhs) = delete;
    AbortTransactionCc(AbortTransactionCc &&rhs) = delete;

    bool Execute(CcShard &ccs) override
    {
        LruEntry *lru_entry = reinterpret_cast<LruEntry *>(entry_addr_);
        std::unordered_map<NodeGroupId,
                           std::unordered_map<TxNumber, TxLockInfo>> &ltxs =
            ccs.GetLockHoldingTxs();

        for (TxNumber tx : tx_id_lock_vct_)
        {
            bool bfind = false;
            for (auto it_ng = ltxs.begin(); it_ng != ltxs.end(); it_ng++)
            {
                auto it_info = it_ng->second.find(tx);
                if (it_info->second.cce_list_.find(lru_entry) !=
                    it_info->second.cce_list_.end())
                {
                    bfind = true;
                    break;
                }
            }
            if (!bfind)
            {
                continue;
            }

            NonBlockingLock *key_lock = lru_entry->key_lock_ptr_;
            key_lock->AbortQueueRequest(tx_id_wait_);
            break;
        }

        return true;
    }

    void Reset(uint64_t entry_addr,
               std::vector<TxNumber> &tx_id_lock_vct,
               TxNumber tx_id_wait)
    {
        entry_addr_ = entry_addr;
        tx_id_lock_vct_.swap(tx_id_lock_vct);
        tx_id_wait_ = tx_id_wait;
    }

    uint64_t GetEntryAddr()
    {
        return entry_addr_;
    }
    TxNumber GetWaitTxId()
    {
        return tx_id_wait_;
    }
    const std::vector<TxNumber> &GetTxIdLock()
    {
        return tx_id_lock_vct_;
    }

protected:
    uint64_t entry_addr_;
    std::vector<TxNumber> tx_id_lock_vct_;
    TxNumber tx_id_wait_;
};
}  // namespace txservice
