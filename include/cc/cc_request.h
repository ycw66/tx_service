#pragma once

#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include <butil/iobuf.h>
#include <mimalloc-2.1/mimalloc.h>

#include <algorithm>  // std::min
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
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
#include "cc_protocol.h"
#include "cc_req_base.h"
#include "cc_req_misc.h"
#include "constants.h"
#include "dead_lock_check.h"
#include "error_messages.h"  // CcErrorCode
#include "fault/fault_inject.h"
#include "proto/cc_request.pb.h"
#include "raft_log.pb.h"
#include "random_pairing.h"
#include "range_slice.h"
#include "remote/cc_stream_receiver.h"
#include "remote/remote_type.h"
#include "scan.h"
#include "sharder.h"
#include "statistics.h"
#include "tx_command.h"
#include "tx_key.h"
#include "tx_operation_result.h"
#include "type.h"

namespace txservice
{
template <typename KeyT, typename ValueT>
class TemplateCcMap;

template <typename SkT, typename PkT>
class SkCcMap;

class CcMap;

struct LruPage;

class SamplePool;

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
        if (ng_term_ < 0)
        {
            ng_term_ = cc_ng_term;
        }

        if (cc_ng_term < 0 || cc_ng_term != ng_term_)
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
                        ccs.FetchCatalog(
                            base_table_name, node_group_id_, ng_term_, this);
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
                        ccs.FetchTableRanges(
                            *table_name_, this, node_group_id_, ng_term_);
                        return false;
                    }
                }
                else
                {
                    // Find base table name for index table.
                    // Fetch/Get Catalog is based on base table name, but Get
                    // ccmap is based on the real table name, for example, index
                    // should get the corresponding sk_ccmap.
                    assert(!table_name_->IsMeta());
                    const CatalogEntry *catalog_entry = ccs.InitCcm(
                        *table_name_, node_group_id_, ng_term_, this);
                    if (catalog_entry == nullptr)
                    {
                        // The local node does not contain the table's schema
                        // instance. The FetchCatalog() method will send an
                        // async request toward the data store to fetch the
                        // catalog. After fetching is finished, this cc request
                        // is re-enqueued for re-execution.
                        return false;
                    }
                    else
                    {
                        if (catalog_entry->schema_ == nullptr)
                        {
                            // The local node (LocalCcShards) contains a schema
                            // instance, which indicates that the table has been
                            // dropped. Returns the request with an error.
                            res_->SetError(
                                CcErrorCode::REQUESTED_TABLE_NOT_EXISTS);
                            return true;
                        }

                        ccm = ccs.GetCcm(*table_name_, node_group_id_);
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

    virtual const TableName *GetTableName() const
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
               IsolationLevel iso_level = IsolationLevel::ReadCommitted,
               int64_t ng_term = INIT_TERM)
    {
        res_ = res;
        table_name_ = tname;
        ccm_ = nullptr;
        node_group_id_ = node_group_id;
        ng_term_ = ng_term;

        tx_number_ = tx_number;
        proto_ = proto;
        isolation_level_ = iso_level;
    }

    // This method must be called on the same tx processor as the cc request.
    void AbortCcRequest(CcErrorCode err_code) override
    {
        assert(err_code != CcErrorCode::NO_ERROR);
        res_->SetError(err_code);
        Free();
    }

protected:
    CcHandlerResult<ResultType> *res_{nullptr};
    const TableName *table_name_{nullptr};
    // track the ccmap for ccrequest, it has two usages: a. ccreq is blocked by
    // lock and need to be re-execute. b. ccreq records the ccentry address, and
    // need to use ccentry to find the corresponding ccmap and ccshard.
    CcMap *ccm_{nullptr};

    // The term of the cc node group on which the request is first processed.
    // The term is matched, when the request is blocked and resumed on the same
    // cc node group. The request is terminated if the cc node group has failed
    // since first execution and the term changes.
    int64_t ng_term_{-1};

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

    TxKey *DecodedKey() const
    {
        return decoded_key_ == nullptr ? nullptr : decoded_key_.get();
    }

    void SetDecodedKey(std::unique_ptr<TxKey> decoded_key)
    {
        decoded_key_ = std::move(decoded_key);
        key_ = decoded_key_.get();
    }

    void SetTxKey(const TxKey *key)
    {
        key_ = key;
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
};

struct PostWriteCc : public TemplatedCcRequest<PostWriteCc, PostProcessResult>
{
public:
    PostWriteCc()
        : cce_addr_(nullptr),
          commit_ts_(0),
          is_remote_(false),
          is_initial_insert_(false),
          payload_(nullptr),
          key_(nullptr)
    {
    }

    PostWriteCc(const PostWriteCc &rhs) = delete;
    PostWriteCc(PostWriteCc &&rhs) = delete;

    bool ValidTermCheck() override
    {
        int64_t cc_ng_term = Sharder::Instance().LeaderTerm(node_group_id_);
        if (cce_addr_ != nullptr)
        {
            if (cce_addr_->Term() != cc_ng_term)
            {
                return false;
            }

            if (cce_addr_->InsertPtr() != 0)
            {
                const LruEntry *lru_entry =
                    reinterpret_cast<const LruEntry *>(cce_addr_->CcePtr());
                ccm_ = lru_entry->GetCcMap();
            }
            else if (cce_addr_->CcePtr() != 0)
            {
                const LruEntry *lru_entry =
                    reinterpret_cast<const LruEntry *>(cce_addr_->CcePtr());
                ccm_ = lru_entry->GetCcMap();
                assert(ccm_ != nullptr);
            }

            return true;
        }
        else
        {
            assert(table_name_ != nullptr);
            if (ng_term_ < 0)
            {
                ng_term_ = cc_ng_term;
            }

            if (cc_ng_term < 0 || cc_ng_term != ng_term_)
            {
                return false;
            }
            else
            {
                return true;
            }
        }
    }

    void Reset(const CcEntryAddr *addr,
               uint64_t tx_number,
               uint64_t ts,
               const TxRecord *rec,
               OperationType operation_type,
               uint32_t key_shard_code,
               CcHandlerResult<PostProcessResult> *res)
    {
        TemplatedCcRequest<PostWriteCc, PostProcessResult>::Reset(
            nullptr, res, addr->NodeGroupId(), tx_number);

        cce_addr_ = addr;
        commit_ts_ = ts;
        payload_ = rec;
        operation_type_ = operation_type;
        key_shard_code_ = key_shard_code;
        key_ = nullptr;
        is_remote_ = false;
        ccm_ = nullptr;
        is_initial_insert_ = false;
    }

    void Reset(const TxKey *key,
               const TableName &table_name,
               uint32_t ng_id,
               uint64_t tx_number,
               uint64_t ts,
               const TxRecord *rec,
               OperationType operation_type,
               uint32_t key_shard_code,
               CcHandlerResult<PostProcessResult> *res,
               bool initial_insertion = false,
               int64_t ng_term = INIT_TERM)
    {
        TemplatedCcRequest<PostWriteCc, PostProcessResult>::Reset(
            &table_name,
            res,
            ng_id,
            tx_number,
            CcProtocol::OCC,
            IsolationLevel::ReadCommitted,
            ng_term);

        cce_addr_ = nullptr;
        key_ = key;
        commit_ts_ = ts;
        payload_ = rec;
        operation_type_ = operation_type;
        key_shard_code_ = key_shard_code;
        is_remote_ = false;
        ccm_ = nullptr;
        is_initial_insert_ = initial_insertion;
    }

    void Reset(const CcEntryAddr *addr,
               uint64_t tx_number,
               uint64_t ts,
               const std::string *rec,
               OperationType operation_type,
               uint32_t key_shard_code,
               CcHandlerResult<PostProcessResult> *res)
    {
        TemplatedCcRequest<PostWriteCc, PostProcessResult>::Reset(
            nullptr, res, addr->NodeGroupId(), tx_number);

        cce_addr_ = addr;
        key_str_ = nullptr;
        commit_ts_ = ts;
        payload_str_ = rec;
        operation_type_ = operation_type;
        key_shard_code_ = key_shard_code;
        is_remote_ = true;
        ccm_ = nullptr;
        is_initial_insert_ = false;
    }

    void Reset(const TableName *table_name,
               const std::string *key_str,
               uint32_t node_group_id,
               uint64_t tx_number,
               uint64_t ts,
               const std::string *rec,
               OperationType operation_type,
               uint32_t key_shard_code,
               CcHandlerResult<PostProcessResult> *res,
               bool initial_insertion = false,
               int64_t ng_term = INIT_TERM)
    {
        TemplatedCcRequest<PostWriteCc, PostProcessResult>::Reset(
            table_name,
            res,
            node_group_id,
            tx_number,
            CcProtocol::OCC,
            IsolationLevel::ReadCommitted,
            ng_term);

        cce_addr_ = nullptr;
        key_str_ = key_str;
        commit_ts_ = ts;
        payload_str_ = rec;
        operation_type_ = operation_type;
        key_shard_code_ = key_shard_code;
        is_remote_ = true;
        ccm_ = nullptr;
        is_initial_insert_ = initial_insertion;
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
        return is_remote_ ? nullptr : payload_;
    }

    const std::string *PayloadStr() const
    {
        return is_remote_ ? payload_str_ : nullptr;
    }

    OperationType GetOperationType() const
    {
        return operation_type_;
    }

    uint32_t KeyShardCode() const
    {
        return key_shard_code_;
    }

    const TxKey *Key() const
    {
        return is_remote_ ? nullptr : key_;
    }

    const std::string *KeyStr() const
    {
        return is_remote_ ? key_str_ : nullptr;
    }

    bool IsInitialInsert() const
    {
        return is_initial_insert_;
    }

private:
    const CcEntryAddr *cce_addr_;
    uint64_t commit_ts_;
    bool is_remote_;
    bool is_initial_insert_;
    union
    {
        const TxRecord *payload_;
        const std::string *payload_str_;
    };
    OperationType operation_type_;
    uint32_t key_shard_code_;
    union
    {
        const TxKey *key_;
        const std::string *key_str_;
    };
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

    bool ValidTermCheck() override
    {
        int64_t cc_ng_term = Sharder::Instance().LeaderTerm(node_group_id_);
        assert(cce_addr_ != nullptr);
        if (cce_addr_->Term() != cc_ng_term)
        {
            return false;
        }

        const LruEntry *lru_entry =
            reinterpret_cast<const LruEntry *>(cce_addr_->CcePtr());
        ccm_ = lru_entry->GetCcMap();
        assert(ccm_ != nullptr);
        return true;
    }

    void Reset(const CcEntryAddr *addr,
               uint64_t tx_number,
               uint64_t commit_ts,
               uint64_t key_ts,
               uint64_t gap_ts,
               CcHandlerResult<PostProcessResult> *res)
    {
        TemplatedCcRequest<PostReadCc, PostProcessResult>::Reset(
            nullptr, res, addr->NodeGroupId(), tx_number);

        cce_addr_ = addr;
        commit_ts_ = commit_ts;
        key_ts_ = key_ts;
        gap_ts_ = gap_ts;
        res->Value().Clear();
        ccm_ = nullptr;
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
          type_(ReadType::Inside),
          is_for_write_(false)
    {
    }

    ReadCc(const ReadCc &rhs) = delete;
    ReadCc(ReadCc &&rhs) = delete;

    bool ValidTermCheck() override
    {
        int64_t cc_ng_term = -1;
        if (!is_in_recovering_)
        {
            cc_ng_term = Sharder::Instance().LeaderTerm(node_group_id_);
        }
        else
        {
            cc_ng_term =
                Sharder::Instance().CandidateLeaderTerm(node_group_id_);
        }

        auto &tmp_cce_addr = res_->Value().cce_addr_;
        if (tmp_cce_addr.CcePtr() != 0)
        {
            if (tmp_cce_addr.Term() != cc_ng_term)
            {
                return false;
            }

            const LruEntry *lru_entry =
                reinterpret_cast<const LruEntry *>(tmp_cce_addr.CcePtr());
            ccm_ = lru_entry->GetCcMap();
            assert(ccm_ != nullptr);
        }
        else
        {
            if (ng_term_ < 0)
            {
                ng_term_ = cc_ng_term;
            }

            if (cc_ng_term < 0 || cc_ng_term != ng_term_)
            {
                return false;
            }
            else
            {
                return true;
            }
        }

        return true;
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
               bool is_covering_keys = false,
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
        is_covering_keys_ = is_covering_keys;

        ccm_ = nullptr;
        if (res->Value().cce_addr_.CcePtr() != 0)
        {
            table_name_ = nullptr;
        }
        else
        {
            table_name_ = tn;
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
               bool is_covering_keys = false,
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
        is_covering_keys_ = is_covering_keys;

        ccm_ = nullptr;
        if (res->Value().cce_addr_.CcePtr() != 0)
        {
            table_name_ = nullptr;
        }
        else
        {
            table_name_ = tn;
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

    bool IsInRecovering() const
    {
        return is_in_recovering_;
    }

    bool IsCoveringKeys() const
    {
        return is_covering_keys_;
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
    // Reserved for unique sk read
    bool is_covering_keys_{false};

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
               bool is_covering_keys,
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
        is_covering_keys_ = is_covering_keys;
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

    bool IsCoveringKeys() const
    {
        return is_covering_keys_;
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
    bool is_covering_keys_{false};
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

    bool ValidTermCheck() override
    {
        int64_t cc_ng_term = Sharder::Instance().LeaderTerm(node_group_id_);
        if (cce_addr_->Term() != cc_ng_term)
        {
            return false;
        }

        const LruEntry *lru_entry =
            reinterpret_cast<const LruEntry *>(cce_addr_->CcePtr());
        ccm_ = lru_entry->GetCcMap();
        assert(ccm_ != nullptr);
        return true;
    }

    void Reset(const uint32_t &ng_id,
               TxNumber tx_number,
               const uint64_t &ts,
               ScanCache *cache,
               int64_t tx_term,
               CcHandlerResult<ScanNextResult> *next_res,
               IsolationLevel iso_level,
               CcProtocol protocol,
               bool is_for_write,
               bool is_delta,
               bool is_covering_keys)
    {
        TemplatedCcRequest<ScanNextBatchCc, ScanNextResult>::Reset(
            nullptr, next_res, ng_id, tx_number, protocol, iso_level);

        ts_ = ts;
        scan_cache_ = cache;
        tx_term_ = tx_term;
        is_for_write_ = is_for_write;
        is_ckpt_delta_ = is_delta;
        is_covering_keys_ = is_covering_keys;
        cce_ptr_ = nullptr;
        cce_ptr_scan_type_ = ScanType::ScanUnknow;

        const ScanTuple *last_tuple = cache->LastTuple();
        cce_addr_ = &last_tuple->cce_addr_;
        ccm_ = nullptr;
    }

    int64_t TxTerm()
    {
        return tx_term_;
    }

    bool IsForWrite() const
    {
        return is_for_write_;
    }

    bool IsCoveringKeys() const
    {
        return is_covering_keys_;
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
    const CcEntryAddr *cce_addr_;
    uint64_t ts_{0};
    ScanCache *scan_cache_{nullptr};
    int64_t tx_term_{-1};

    bool is_for_write_{false};
    bool is_covering_keys_{false};
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
    ScanSliceCc()
        : start_key_(nullptr),
          end_key_(nullptr),
          start_key_type_(RangeKeyType::RawPtr),
          end_key_type_(RangeKeyType::RawPtr)
    {
        parallel_req_ = true;
    }

    ~ScanSliceCc()
    {
        if (start_key_type_ == RangeKeyType::UniquePtr)
        {
            start_key_uptr_ = nullptr;
        }
        if (end_key_type_ == RangeKeyType::UniquePtr)
        {
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
             bool read_for_write,
             bool is_covering_keys,
             uint8_t prefetch_size)
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
        is_covering_keys_ = is_covering_keys;

        unfinished_core_cnt_.store(1, std::memory_order_relaxed);
        range_slice_id_.Reset();
        last_pinned_slice_ = nullptr;
        prefetch_size_ = prefetch_size;
        err_ = CcErrorCode::NO_ERROR;
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
             bool read_for_write,
             bool is_covering_keys,
             uint8_t prefetch_size)
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
        is_covering_keys_ = is_covering_keys;
        prefetch_size_ = prefetch_size;

        unfinished_core_cnt_.store(1, std::memory_order_relaxed);
        range_slice_id_.Reset();
        last_pinned_slice_ = nullptr;
        err_ = CcErrorCode::NO_ERROR;
    }

    void AbortCcRequest(CcErrorCode err_code) override
    {
        if (SetError(err_code))
        {
            // Last core finished. If the request has pinned any slice, unpin
            // it.
            if (range_slice_id_.Range() != nullptr)
            {
                UnpinSlices();
            }
            Free();
        }
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
            (void) start_key_uptr_.release();
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
            (void) end_key_uptr_.release();
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

    RemoteScanSliceCache *GetRemoteScanCache(size_t shard_id)
    {
        if (IsLocal())
        {
            return nullptr;
        }

        RangeScanSliceResult &slice_result = res_->Value();
        assert(shard_id < slice_result.remote_scan_caches_->size());
        return &slice_result.remote_scan_caches_->at(shard_id);
    }

    CcScanner *GetLocalScanner()
    {
        return IsLocal() ? res_->Value().ccm_scanner_ : nullptr;
    }

    enum struct ScanBlockingType
    {
        NoBlocking = 0,
        BlockOnLock,
        BlockOnFuture
    };

    uint64_t CceAddr(uint16_t core_id)
    {
        assert(core_id < blocking_vec_.size());
        return blocking_vec_[core_id].cce_addr_;
    }

    std::pair<ScanBlockingType, ScanType> BlockingPair(uint16_t core_id)
    {
        assert(core_id < blocking_vec_.size());
        return {blocking_vec_[core_id].type_,
                blocking_vec_[core_id].scan_type_};
    }

    void SetBlockingInfo(uint16_t core_id,
                         uint64_t cce_addr,
                         ScanType scan_type,
                         ScanBlockingType blocking_type)
    {
        assert(core_id < blocking_vec_.size());
        blocking_vec_[core_id] = {cce_addr, scan_type, blocking_type};
    }

    void SetShardCount(uint16_t shard_cnt)
    {
        blocking_vec_.resize(shard_cnt);
    }

    uint64_t GetShardCount() const
    {
        return blocking_vec_.size();
    }

    void SetUnfinishedCoreCnt(uint16_t core_cnt)
    {
        unfinished_core_cnt_.store(core_cnt, std::memory_order_release);
    }

    void SetPriorCceAddr(uint64_t addr, uint16_t shard_id)
    {
        assert(shard_id < blocking_vec_.size());
        blocking_vec_[shard_id] = {
            addr, ScanType::ScanUnknow, ScanBlockingType::NoBlocking};
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
            // Only update result if this is local request. Remote request
            // result will be updated by dedicated core.
            if (res_->Value().is_local_)
            {
                if (err_ == CcErrorCode::NO_ERROR)
                {
                    res_->SetFinished();
                }
                else
                {
                    res_->SetError(err_);
                }
            }
        }

        return remaining_cnt == 1;
    }

    bool SetError(CcErrorCode err)
    {
        err_ = err;
        uint16_t remaining_cnt =
            unfinished_core_cnt_.fetch_sub(1, std::memory_order_acq_rel);

        // remaining_cnt might be 0 if all cores have finished and the req is
        // put back into the result sending core's queue.
        if (remaining_cnt <= 1)
        {
            res_->SetError(err_);
        }

        return remaining_cnt <= 1;
    }

    /**
     * @brief Send response to src node if all cores have finished.
     * We use this method to send scan slice response if this request is
     * a remote request.
     * We assign a dedicated core to be the response sender instead of directly
     * sending the response on the last finished core. This is to avoid
     * serialization of response message causing one core to become
     * significantly slower than others and would end up being the sender of all
     * scan slice response.
     */
    bool SendResponseIfFinished()
    {
        if (unfinished_core_cnt_.load(std::memory_order_relaxed) == 0)
        {
            if (err_ == CcErrorCode::NO_ERROR)
            {
                res_->SetFinished();
            }
            else
            {
                res_->SetError(err_);
            }
            return true;
        }
        return false;
    }

    bool IsResponseSender(uint16_t core_id) const
    {
        return ((tx_number_ & 0x3FF) % blocking_vec_.size()) == core_id;
    }

    bool IsForWrite() const
    {
        return read_for_write_;
    }

    const RangeSliceId &SliceId() const
    {
        return range_slice_id_;
    }

    bool IsCoveringKeys() const
    {
        return is_covering_keys_;
    }

    /**
     * @brief Returns the number of slices to prefetch when loading a cache-miss
     * slice.
     *
     * @return uint8_t Number of slices to prefetch
     */
    uint8_t PrefetchSize() const
    {
        return prefetch_size_;
    }

    void PinSlices(const RangeSliceId &slice, const StoreSlice *last_slice)
    {
        range_slice_id_ = slice;
        last_pinned_slice_ = last_slice;
    }

    const StoreSlice *LastPinnedSlice() const
    {
        return last_pinned_slice_;
    }

    void UnpinSlices()
    {
        range_slice_id_.Range()->BatchUnpinSlices(
            range_slice_id_.Slice(),
            last_pinned_slice_,
            direction_ == ScanDirection::Forward);
    }

private:
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

    union
    {
        const TxKey *end_key_;
        const std::string *end_key_str_;
        std::unique_ptr<TxKey> end_key_uptr_;
    };

    RangeKeyType start_key_type_;
    RangeKeyType end_key_type_;

    uint32_t range_id_{0};
    ScanDirection direction_{ScanDirection::Forward};

    uint64_t ts_{0};
    int64_t tx_term_{-1};

    bool start_inclusive_{false};
    bool end_inclusive_{false};

    /**
     * @brief Number of slices to prefetch when a cache-miss slice is loaded.
     *
     */
    uint8_t prefetch_size_{0};
    bool read_for_write_{false};

    std::atomic<uint16_t> unfinished_core_cnt_{1};
    const StoreSlice *last_pinned_slice_{nullptr};
    bool is_covering_keys_{false};

    int64_t cc_ng_term_{-1};

    struct ScanBlockingInfo
    {
        uint64_t cce_addr_;
        ScanType scan_type_;
        ScanBlockingType type_;
    };
    std::vector<ScanBlockingInfo> blocking_vec_;

    RangeSliceId range_slice_id_;
    CcErrorCode err_{CcErrorCode::NO_ERROR};
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

private:
    uint64_t ckpt_ts_;
    std::mutex mux_;
    std::condition_variable cv_;
    std::atomic<size_t> finish_cnt_;
    size_t shard_cnt_;
    std::vector<uint64_t> memory_usage_kb_vec_;
    NodeGroupId cc_ng_id_;
};

struct ProcessRemoteScanRespCc : public CcRequestBase
{
public:
    static constexpr size_t SCAN_BATCH_SIZE = 256;

    ProcessRemoteScanRespCc() = default;

    void Reset(remote::CcStreamReceiver *receiver,
               std::unique_ptr<remote::ScanSliceResponse> resp_msg,
               std::vector<size_t> &&offset_tables,
               CcHandlerResult<RangeScanSliceResult> *hd_res,
               size_t worker_cnt)
    {
        receiver_ = receiver;
        resp_msg_ = std::move(resp_msg);
        offset_tables_ = std::move(offset_tables);
        hd_res_ = hd_res;

        unfinished_cnt_ = worker_cnt;
        next_remote_core_idx_ = worker_cnt;

        assert(offset_tables_.size() == RemoteCoreCnt());
        assert(worker_cnt <= RemoteCoreCnt());

        cur_idxs_.clear();
        key_offsets_.clear();
        rec_offsets_.clear();

        assert(cur_idxs_.empty());
        assert(key_offsets_.empty());
        assert(rec_offsets_.empty());

        for (size_t worker_idx = 0; worker_idx < worker_cnt; ++worker_idx)
        {
            // worker idx must be less or equal than remote core count
            cur_idxs_.push_back({worker_idx, 0});
            key_offsets_.push_back(KeyStartOffset(worker_idx));
            rec_offsets_.push_back(RecStartOffset(worker_idx));
        }
    }

    ProcessRemoteScanRespCc(const ProcessRemoteScanRespCc &) = delete;
    ProcessRemoteScanRespCc &operator=(const ProcessRemoteScanRespCc &) =
        delete;

    bool Execute(CcShard &ccs) override
    {
        size_t scan_cnt = 0;

        do
        {
            auto &[remote_core_idx, tuple_idx] = cur_idxs_.at(ccs.core_id_);

            const uint64_t *key_ts_ptr =
                (const uint64_t *) resp_msg_->key_ts().data();
            key_ts_ptr += MetaOffset(remote_core_idx);

            const uint64_t *gap_ts_ptr =
                (const uint64_t *) resp_msg_->gap_ts().data();
            gap_ts_ptr += MetaOffset(remote_core_idx);

            const uint64_t *term_ptr =
                (const uint64_t *) resp_msg_->term().data();
            term_ptr += MetaOffset(remote_core_idx);

            const uint64_t *cce_ptr_ptr =
                (const uint64_t *) resp_msg_->cce_ptr().data();
            cce_ptr_ptr += MetaOffset(remote_core_idx);

            const remote::RecordStatusType *rec_status_ptr =
                (const remote::RecordStatusType *) resp_msg_->rec_status()
                    .data();
            rec_status_ptr += MetaOffset(remote_core_idx);

            RangeScanSliceResult &scan_slice_result = hd_res_->Value();
            CcScanner &range_scanner = *scan_slice_result.ccm_scanner_;
            ScanCache *shard_cache = range_scanner.Cache(remote_core_idx);

            size_t &key_offset = key_offsets_[ccs.core_id_];
            size_t &rec_offset = rec_offsets_[ccs.core_id_];
            size_t tuple_cnt = TupleCnt(remote_core_idx);

            for (; tuple_idx < tuple_cnt && scan_cnt < SCAN_BATCH_SIZE;
                 ++tuple_idx, ++scan_cnt)
            {
                RecordStatus rec_status =
                    remote::ToLocalType::ConvertRecordStatusType(
                        rec_status_ptr[tuple_idx]);

                shard_cache->AddScanTuple(resp_msg_->keys(),
                                          key_offset,
                                          key_ts_ptr[tuple_idx],
                                          resp_msg_->records(),
                                          rec_offset,
                                          rec_status,
                                          gap_ts_ptr[tuple_idx],
                                          cce_ptr_ptr[tuple_idx],
                                          term_ptr[tuple_idx],
                                          remote_core_idx,
                                          scan_slice_result.cc_ng_id_);
            }

            if (tuple_idx == tuple_cnt)
            {
                auto [scan_end, is_set] = scan_slice_result.PeekLastKey();
                assert(is_set);

                // For remote scans, the scan result is a string representation
                // of scanned key-value pairs. It may include keys beyond the
                // scan's last key, due to parallel scans across multi cores at
                // the remote node. Removes the keys from the scan cache beyond
                // the scan's end.
                if (range_scanner.Direction() == ScanDirection::Forward)
                {
                    assert(scan_end == nullptr ||
                           scan_slice_result.slice_position_ ==
                               txservice::SlicePosition::Middle ||
                           scan_slice_result.slice_position_ ==
                               txservice::SlicePosition::LastSliceInRange);

                    while (scan_end != nullptr && shard_cache->Size() > 0 &&
                           *scan_end < *shard_cache->LastTuple()->Key())
                    {
                        shard_cache->RemoveLast();
                    }
                }
                else
                {
                    assert(scan_end == nullptr ||
                           scan_slice_result.slice_position_ ==
                               txservice::SlicePosition::Middle ||
                           scan_slice_result.slice_position_ ==
                               txservice::SlicePosition::FirstSliceInRange);

                    while (scan_end != nullptr && shard_cache->Size() > 0 &&
                           *shard_cache->LastTuple()->Key() < *scan_end)
                    {
                        shard_cache->RemoveLast();
                    }
                }

                range_scanner.CommitAtCore(remote_core_idx);

                if (!MoveForward(ccs.core_id_))
                {
                    // No more data
                    return SetFinished();
                }
            }

            //  To avoid blocking other request for a long time, we only process
            // ScanBatchSize number of data in each round.
        } while (scan_cnt < SCAN_BATCH_SIZE);

        // Put this request to CcQueue again.
        ccs.Enqueue(this);
        return false;
    }

    bool SetFinished()
    {
        // This core is last finished worker. We need to set handler result and
        // recycle message.
        if (unfinished_cnt_.fetch_sub(1, std::memory_order_release) == 1)
        {
            if (resp_msg_->error_code() != 0)
            {
                hd_res_->SetError(remote::ToLocalType::ConvertCcErrorCode(
                    resp_msg_->error_code()));
            }
            else
            {
                hd_res_->SetFinished();
            }

            // Recycle message
            receiver_->RecycleScanSliceResp(std::move(resp_msg_));

            // Return true to recycle this request
            return true;
        }

        return false;
    }

private:
    bool MoveForward(size_t worker_idx)
    {
        size_t new_remote_core_idx = next_remote_core_idx_.fetch_add(1);
        if (new_remote_core_idx < RemoteCoreCnt())
        {
            cur_idxs_.at(worker_idx) = {new_remote_core_idx, 0};
            key_offsets_.at(worker_idx) = KeyStartOffset(new_remote_core_idx);
            rec_offsets_.at(worker_idx) = RecStartOffset(new_remote_core_idx);

            return true;
        }

        // No more data
        return false;
    }

    size_t KeyStartOffset(size_t remote_core_idx) const
    {
        const size_t *ptr = reinterpret_cast<const size_t *>(
            resp_msg_->key_start_offsets().data());
        ptr += remote_core_idx;
        return *ptr;
    }

    size_t RecStartOffset(size_t remote_core_idx) const
    {
        const size_t *ptr = reinterpret_cast<const size_t *>(
            resp_msg_->record_start_offsets().data());
        ptr += remote_core_idx;
        return *ptr;
    }

    size_t MetaOffset(size_t remote_core_idx) const
    {
        return offset_tables_[remote_core_idx];
    }

    size_t TupleCnt(size_t remote_core_idx) const
    {
        const char *tuple_cnt_info = resp_msg_->tuple_cnt().data();
        // remote core count
        tuple_cnt_info += sizeof(uint16_t);
        // tuple count
        tuple_cnt_info += remote_core_idx * sizeof(size_t);
        return *(reinterpret_cast<const size_t *>(tuple_cnt_info));
    }

    uint16_t RemoteCoreCnt() const
    {
        const char *tuple_cnt_info = resp_msg_->tuple_cnt().data();
        return *reinterpret_cast<const uint16_t *>(tuple_cnt_info);
    }

    remote::CcStreamReceiver *receiver_{nullptr};
    std::unique_ptr<remote::ScanSliceResponse> resp_msg_{nullptr};
    // Store the start postition of meta data like `key_ts`.
    std::vector<size_t> offset_tables_;
    // The vector of {remote_core_idx, current_tuple_idx}.
    std::vector<std::pair<size_t, size_t>> cur_idxs_;

    // We need to store key/rec offset so that we could restart from pause
    // point.
    std::vector<size_t> key_offsets_;
    std::vector<size_t> rec_offsets_;

    // Unfinished worker count. std::min(this_node_core_count,
    // remote_core_count)
    std::atomic<size_t> unfinished_cnt_{0};
    // Next remote core idx we need to process.
    std::atomic<size_t> next_remote_core_idx_{0};
    CcHandlerResult<RangeScanSliceResult> *hd_res_{nullptr};
};

struct DataSyncScanCc : public CcRequestBase
{
public:
    // how many pages to scan one time
    // static constexpr size_t DataSyncScanBatch = 20;
    // todo: limit scan by scanned size
    static constexpr size_t DataSyncScanBatchSize = 128;

    DataSyncScanCc() = delete;

    DataSyncScanCc(const TableName &table_name,
                   uint64_t previous_scan_ts,
                   uint64_t previous_ckpt_ts,
                   uint64_t data_sync_ts,
                   uint64_t node_group_id,
                   int64_t node_group_term,
                   uint16_t core_cnt,
                   std::vector<std::pair<TxKey::Uptr, bool>> &&resume_pos,
                   size_t scan_batch_size,
                   const TxKey *target_start_key = nullptr,
                   const TxKey *target_end_key = nullptr,
                   bool include_flushed_rec = false)
        : table_name_(&table_name),
          node_group_id_(node_group_id),
          node_group_term_(node_group_term),
          core_cnt_(core_cnt),
          previous_scan_ts_(previous_scan_ts),
          previous_ckpt_ts_(previous_ckpt_ts),
          data_sync_ts_(data_sync_ts),
          start_key_(target_start_key),
          end_key_(target_end_key),
          pause_key_(std::move(resume_pos)),
          scan_batch_size_(scan_batch_size),
          err_(CcErrorCode::NO_ERROR),
          unfinished_cnt_(core_cnt_),
          mux_(),
          cv_(),
          include_flushed_rec_(include_flushed_rec)
    {
        assert(scan_batch_size_ > DataSyncScanBatchSize);
        for (size_t i = 0; i < core_cnt; i++)
        {
            data_sync_vec_.emplace_back();
            data_sync_vec_.back().resize(scan_batch_size);
            archive_vec_.emplace_back();
            archive_vec_.back().reserve(scan_batch_size);
            mv_base_idx_vec_.emplace_back();
            mv_base_idx_vec_.back().reserve(scan_batch_size);
            res_.emplace_back(nullptr, false);
            accumulated_scan_cnt_.emplace_back(0);
        }

        if (include_flushed_rec)
        {
#ifndef RANGE_PARTITION_ENABLED
            assert(false && "Only range partition");
            include_flushed_rec_ = false;
            return;
#endif
            slice_ids_.resize(core_cnt_);
        }
    }

    // DataSyncScanCc is always stack object and won't be reused, worse, it
    // might be destructed before Execute returns, so always return false as
    // callershould never access this object after Execute returns
    bool Execute(CcShard &ccs) override
    {
        CcMap *ccm = ccs.GetCcm(*table_name_, node_group_id_);

        if (ccm != nullptr)
        {
            ccm->Execute(*this);
        }
        else
        {
            // ccmap for this table does not exist on this shard, skip
            // scanning for this shard.
            std::pair<TxKey::Uptr, bool> res{nullptr, true};
            SetFinish(std::move(res), ccs.core_id_);
        }
        // return false since DataSyncScanCc is not re-used and does not need to
        // call CcRequestBase::Free
        return false;
    }

    void Wait()
    {
        std::unique_lock<std::mutex> lk(mux_);
        cv_.wait(lk, [this] { return unfinished_cnt_ == 0; });
    }

    void Reset(std::vector<std::pair<TxKey::Uptr, bool>> &&resume_pos)
    {
        std::lock_guard<std::mutex> lk(mux_);
        pause_key_ = std::move(resume_pos);
        unfinished_cnt_ = core_cnt_;
        res_.clear();
        for (size_t i = 0; i < core_cnt_; i++)
        {
            archive_vec_.at(i).clear();
            mv_base_idx_vec_.at(i).clear();
            res_.emplace_back(nullptr, false);
            accumulated_scan_cnt_.at(i) = 0;
        }
    }

    void SetError(CcErrorCode err)
    {
        std::lock_guard<std::mutex> lk(mux_);
        err_ = err;
        --unfinished_cnt_;
        if (unfinished_cnt_ == 0)
        {
            cv_.notify_one();
        }
    }

    void AbortCcRequest(CcErrorCode err_code) override
    {
        assert(err_code != CcErrorCode::NO_ERROR);
        std::lock_guard<std::mutex> lk(mux_);
        err_ = err_code;
        --unfinished_cnt_;
        if (unfinished_cnt_ == 0)
        {
            cv_.notify_one();
        }
    }

    bool IsError()
    {
        std::lock_guard<std::mutex> lk(mux_);
        return err_ != CcErrorCode::NO_ERROR;
    }

    CcErrorCode ErrorCode()
    {
        std::lock_guard<std::mutex> lk(mux_);
        return err_;
    }

    void SetFinish(std::pair<TxKey::Uptr, bool> &&res, size_t core_id)
    {
        std::unique_lock<std::mutex> lk(mux_);
        res_.at(core_id) = std::move(res);
        --unfinished_cnt_;
        if (unfinished_cnt_ == 0)
        {
            cv_.notify_one();
        }
    }

    uint32_t NodeGroupId()
    {
        return node_group_id_;
    }

    std::vector<std::pair<TxKey::Uptr, bool>> &Result()
    {
        return res_;
    }

    std::vector<FlushRecord> &DataSyncVec(uint16_t core_id)
    {
        return data_sync_vec_[core_id];
    }

    std::vector<FlushRecord> &ArchiveVec(uint16_t core_id)
    {
        return archive_vec_[core_id];
    }

    std::vector<size_t> &MoveBaseIdxVec(uint16_t core_id)
    {
        return mv_base_idx_vec_[core_id];
    }

    std::vector<size_t> accumulated_scan_cnt_;

private:
    const TableName *table_name_{nullptr};
    uint32_t node_group_id_;
    int64_t node_group_term_;
    uint16_t core_cnt_;
    // Used during range split. We only want new data changes after this given
    // ts, despite there might be older version that is still not synced into
    // data sotre yet (for mvcc only). It can be used as a hint to decide if a
    // page has dirty data that need to be put into data sync vec. However it is
    // not guaranteed that all entries committed before this ts are synced.
    uint64_t previous_scan_ts_;
    // Used during regular data sync scan. It is used as a hint to decide if a
    // page has dirty data since last round of checkpoint. It is guaranteed that
    // all entries committed before this ts are synced into data store.
    uint64_t previous_ckpt_ts_;
    // Target ts. Collect all data changes committed before this ts into data
    // sync vec.
    uint64_t data_sync_ts_;
    std::vector<std::vector<FlushRecord>> data_sync_vec_;
    std::vector<std::vector<FlushRecord>> archive_vec_;
    // Cache the entries to move record from "base" table to "archive" table
    std::vector<std::vector<size_t>> mv_base_idx_vec_;

    // Start/end key of target range if the scan is on a range only, nullptr if
    // it's on entire table.
    const TxKey *start_key_{nullptr};
    const TxKey *end_key_{nullptr};
    // Position that we left off during last round of ckpt scan. TxKey is the
    // key that we stopped at (has not been scanned though), bool is if this
    // core has finished scanning all keys already.
    std::vector<std::pair<TxKey::Uptr, bool>> pause_key_;
    size_t scan_batch_size_;

    CcErrorCode err_{CcErrorCode::NO_ERROR};
    uint32_t unfinished_cnt_;
    std::mutex mux_;
    std::condition_variable cv_;

    // scan result
    std::vector<std::pair<TxKey::Uptr, bool>> res_;

    // True means we also need to scan data which has been flushed to storage.
    // Note: This flag only used for RangePartition.
    bool include_flushed_rec_{false};
    std::vector<RangeSliceId> slice_ids_;

    template <typename KeyT, typename ValueT>
    friend class TemplateCcMap;

    friend std::ostream &operator<<(std::ostream &outs,
                                    txservice::DataSyncScanCc *r);
};

// This cc request is used to convert parallel access on ccmap/samplepool into
// serial access.
struct RunOnTxProcessorCc : public CcRequestBase
{
public:
    explicit RunOnTxProcessorCc(std::function<void(CcShard &ccs)> task)
        : task_(std::move(task)), is_finished_(false), mux_(), cv_()
    {
    }

    void Reset()
    {
        is_finished_ = false;
        error_code_ = CcErrorCode::NO_ERROR;
    }

    void Wait()
    {
        std::unique_lock<std::mutex> lk(mux_);
        cv_.wait(lk, [this]() { return is_finished_; });
    }

    bool IsError()
    {
        std::lock_guard<std::mutex> lk(mux_);
        return error_code_ != CcErrorCode::NO_ERROR;
    }

    CcErrorCode ErrorCode()
    {
        std::lock_guard<std::mutex> lk(mux_);
        return error_code_;
    }

    void AbortCcRequest(CcErrorCode error_code) override
    {
        std::unique_lock<std::mutex> lk(mux_);
        is_finished_ = true;
        error_code_ = error_code;
        cv_.notify_one();
    }

    bool Execute(CcShard &ccs) override
    {
        std::unique_lock<std::mutex> lk(mux_);

        task_(ccs);

        error_code_ = CcErrorCode::NO_ERROR;
        is_finished_ = true;
        cv_.notify_one();

        return false;
    }

private:
    std::function<void(CcShard &ccs)> task_;
    bool is_finished_{false};
    CcErrorCode error_code_{CcErrorCode::NO_ERROR};
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
        std::unique_lock lk(mux_);
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
        std::unique_lock lk(mux_);
        while (!finish_)
        {
            cv_.wait(lk);
        }
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
    bthread::Mutex mux_;
    bthread::ConditionVariable cv_;

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
    ReplayLogCc() = default;

    void Reset(
        uint32_t ng_id,
        std::string_view table_name_view,
        TableType table_type,
        std::string_view blob,
        uint64_t commit_ts,
        uint64_t txn,
        std::mutex &mux,
        std::condition_variable &cv,
        uint64_t &finish_cnt,
        bool &recovery_error,
        std::shared_ptr<std::vector<::txlog::ReplayMessage>> &msg_vec,
        std::shared_ptr<std::atomic_uint32_t> range_split_started = nullptr,
        std::unordered_set<TableName> *range_splitting = nullptr,
        uint16_t first_core = 0)
    {
        table_name_holder_ = TableName(table_name_view, table_type);
        TemplatedCcRequest<ReplayLogCc, Void>::Reset(
            &table_name_holder_, &result_, ng_id, txn);
        log_blob_view_ = blob;
        commit_ts_ = commit_ts;
        result_.Reset();
        external_mux_ = &mux;
        external_cv_ = &cv;
        finish_cnt_ = &finish_cnt;
        recovery_error_ = &recovery_error;
        msg_vec_ = msg_vec;
        next_core_ = UINT16_MAX;
        first_core_ = first_core;
        range_split_started_ = range_split_started;
        range_splitting_ = range_splitting;
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
            return true;
        }
        if (ccm_ == nullptr)
        {
            assert(table_name_ != nullptr);
            ccm_ = ccs.GetCcm(*table_name_, node_group_id_);

            if (ccm_ == nullptr)
            {
                if (table_name_->Type() == TableType::RangePartition)
                {
                    const txservice::TableName base_table_name{
                        table_name_->GetBaseTableNameSV(), TableType::Primary};
                    const CatalogEntry *catalog_entry =
                        ccs.GetCatalog(base_table_name, node_group_id_);
                    if (catalog_entry == nullptr)
                    {
                        ccs.FetchCatalog(
                            base_table_name,
                            node_group_id_,
                            std::max(cc_ng_candid_term, cc_ng_term),
                            this);
                        return false;
                    }

                    // If FetchCatalogCc failure due to storage fault,
                    // FetchCatalogCc::Execute() abort the ReplayLogCc
                    assert(catalog_entry->Version() > 0);
                    if (catalog_entry->schema_ == nullptr)
                    {
                        // table has been dropped
                        SetFinish();
                        return true;
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
                        ccs.FetchTableRanges(
                            *table_name_,
                            this,
                            node_group_id_,
                            std::max(cc_ng_candid_term, cc_ng_term));
                        return false;
                    }
                }
                else
                {
                    const CatalogEntry *catalog_entry =
                        ccs.InitCcm(*table_name_,
                                    node_group_id_,
                                    std::max(cc_ng_candid_term, cc_ng_term),
                                    this);

                    if (catalog_entry != nullptr)
                    {
                        // If FetchCatalogCc failure due to storage fault,
                        // FetchCatalogCc::Execute() abort the ReplayLogCc
                        assert(catalog_entry->Version() > 0);
                        if (catalog_entry->schema_ != nullptr &&
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
                            return true;
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
            else
            {
                table_schema_ = ccm_->GetTableSchema();
            }
        }
        return ccm_->Execute(*this);
    }

    void SetFinish()
    {
        // Notifies the external caller--the log replay handler--that the
        // specified log record has been replayed in all cores of this node.
        // HandlerResult is not used by external caller, hence we don't need to
        // call HandlerResult.SetFinished().
        msg_vec_ = nullptr;
        std::lock_guard<std::mutex> lk(*external_mux_);
        ++(*finish_cnt_);
        external_cv_->notify_all();
    }

    void AbortCcRequest(CcErrorCode err_code) override
    {
        assert(err_code != CcErrorCode::NO_ERROR);

        msg_vec_ = nullptr;
        std::lock_guard<std::mutex> lk(*external_mux_);
        ++(*finish_cnt_);
        *recovery_error_ = true;
        external_cv_->notify_all();
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

    void ResetTxn(uint64_t txn)
    {
        tx_number_ = txn;
    }

    void ResetCcm()
    {
        ccm_ = nullptr;
        offset_ = 0;
    }

    const TableSchema *GetTableSchema()
    {
        return table_schema_;
    }

    std::shared_ptr<std::atomic_uint32_t> RangeSplitStarted()
    {
        return range_split_started_;
    }

    bool RangeSplitting(const TableName &table_name) const
    {
        return range_splitting_ &&
               range_splitting_->find(table_name) != range_splitting_->end();
    }

    uint16_t FirstCore() const
    {
        return first_core_;
    }

    void SetOffset(size_t offset)
    {
        offset_ = offset;
    }
    size_t Offset() const
    {
        return offset_;
    }

    void SetNextCore(uint16_t next_core)
    {
        next_core_ = next_core;
    }

    uint16_t NextCore() const
    {
        return next_core_;
    }

private:
    TableName table_name_holder_{
        "",
        0,
        TableType::Primary};  //  not string owner, sv -> protobuf message.
    std::string_view log_blob_view_;
    // Temporarily store the currently parsed offset when fetch record from
    // kvstore asynchronously.
    size_t offset_{0};
    uint64_t commit_ts_;
    CcHandlerResult<Void> result_{nullptr};
    std::mutex *external_mux_;
    std::condition_variable *external_cv_;
    uint64_t *finish_cnt_;
    bool *recovery_error_;
    uint16_t first_core_;
    uint16_t next_core_{UINT16_MAX};
    const struct TableSchema *table_schema_{nullptr};
    // Reserved for range split log replay
    std::shared_ptr<std::atomic_uint32_t> range_split_started_{nullptr};
    // Keep a reference of replay msg until replay is finished since
    // log_blob_view_ and table_name_holder_ points to ReplayMessage.
    std::shared_ptr<std::vector<::txlog::ReplayMessage>> msg_vec_;

    // Reserved for schema op log replay
    const std::unordered_set<TableName> *range_splitting_;

    friend std::ostream &operator<<(std::ostream &outs,
                                    txservice::ReplayLogCc *r);
};

struct BroadcastStatisticsCc
    : public TemplatedCcRequest<BroadcastStatisticsCc, Void>
{
public:
    BroadcastStatisticsCc() = default;

    void Reset(uint32_t ng_id,
               const TableName *table_name,
               uint64_t schema_version,
               const remote::NodeGroupSamplePool &remote_sample_pool,
               TxNumber tx_number,
               CcHandlerResult<Void> *res)
    {
        TemplatedCcRequest<BroadcastStatisticsCc, Void>::Reset(
            &catalog_ccm_name, res, ng_id, tx_number);

        sampling_table_name_ = table_name;
        schema_version_ = schema_version;
        remote_sample_pool_ = &remote_sample_pool;
    }

    const TableName *SamplingTableName() const
    {
        return sampling_table_name_;
    }

    uint64_t SchemaVersion() const
    {
        return schema_version_;
    }

    const remote::NodeGroupSamplePool *SamplePool() const
    {
        return remote_sample_pool_;
    }

    void ResetCcm()
    {
        ccm_ = nullptr;
    }

protected:
    const TableName *sampling_table_name_{nullptr};
    uint64_t schema_version_{UINT64_MAX};
    const remote::NodeGroupSamplePool *remote_sample_pool_{nullptr};
};

struct AnalyzeTableAllCc : public TemplatedCcRequest<AnalyzeTableAllCc, Void>
{
public:
    struct SamplePoolBase
    {
        virtual ~SamplePoolBase() = default;
    };

    template <uint32_t CapacityN, typename KeyT, typename CopyKey>
    struct SamplePool : public SamplePoolBase
    {
    public:
        void Insert(const KeyT &key)
        {
            random_pairing_.Insert(key, ++counter_);
        }

        const std::vector<KeyT> &SampleKeys() const
        {
            return random_pairing_.SampleKeys();
        }

        uint32_t Size() const
        {
            return random_pairing_.Size();
        }

    public:
        RandomPairing<CapacityN, KeyT, CopyKey> random_pairing_;
        size_t counter_{0};
    };

public:
    AnalyzeTableAllCc() = default;

    void Reset(const TableName *table_name,
               uint32_t node_group_id,
               TxNumber tx_number,
               CcHandlerResult<Void> *res)
    {
        TemplatedCcRequest<AnalyzeTableAllCc, Void>::Reset(
            table_name, res, node_group_id, tx_number);

        Clear();
    }

    bool built_slice_sample_pool_{false};
    std::map<const TxKey *, TableRangeEntry, PtrLessThan<TxKey>>::iterator
        range_it_;

private:
    void Clear()
    {
        built_slice_sample_pool_ = false;
        key_sample_pool_.reset(nullptr);
        slice_sample_pool_.reset(nullptr);
        next_pin_slice_idx_ = 0;
        visit_keys_ = 0;
    }

public:
    constexpr static uint32_t sample_pool_capacity_{1024};
    std::unique_ptr<SamplePoolBase> key_sample_pool_{nullptr};
    std::unique_ptr<SamplePoolBase> slice_sample_pool_{nullptr};

    size_t next_pin_slice_idx_{0};

    uint32_t visit_keys_{0};
};

struct ReloadCacheCc : public TemplatedCcRequest<ReloadCacheCc, Void>
{
    ReloadCacheCc() = default;
    virtual ~ReloadCacheCc() = default;

    ReloadCacheCc(const ReloadCacheCc &) = delete;
    ReloadCacheCc(ReloadCacheCc &&) = delete;

    bool Execute(CcShard &ccs) override
    {
        if (!done_)
        {
            if (SystemHandler *sys_handler = ccs.GetSystemHandler();
                sys_handler)
            {
                sys_handler->ReloadCache(
                    [&ccs, this](bool ok)
                    {
                        done_ = true;
                        ok_ = ok;
                        ccs.Enqueue(this);
                    });
                return false;
            }
            else
            {
                done_ = true;
                ok_ = true;
            }
        }

        if (ok_)
        {
            res_->SetFinished();
        }
        else
        {
            res_->SetError(CcErrorCode::SYSTEM_HANDLER_ERR);
        }
        return true;
    }

    void Reset(CcHandlerResult<Void> *res)
    {
        res_ = res;
        done_ = false;
        ok_ = false;
    }

private:
    bool done_{false};
    bool ok_{false};
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

    bool Execute(CcShard &ccs) override
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
        // Can not sure the entry is in memory, so here verify by
        // ccs.GetLockHoldingTxs
        LruEntry *lru_entry = reinterpret_cast<LruEntry *>(entry_addr_);
        std::unordered_map<NodeGroupId,
                           std::unordered_map<TxNumber, TxLockInfo>> &ltxs =
            ccs.GetLockHoldingTxs();

        auto it_ng = ltxs.find(node_id_);
        // Maybe the ng leader has transfer to other node.
        if (it_ng != ltxs.end())
        {
            auto it_info = it_ng->second.find(tx_id_lock_);
            // Maybe the tx has taken part in more than dead lock cycles, and
            // it has been release in other cycle.
            if (it_info != it_ng->second.end() &&
                it_info->second.cce_list_.find(lru_entry) !=
                    it_info->second.cce_list_.end())
            {
                NonBlockingLock *key_lock = lru_entry->GetKeyLock();
                if (key_lock != nullptr)
                {
                    key_lock->AbortQueueRequest(tx_id_wait_);
                }
            }
        }

        return true;
    }

    void Reset(uint64_t entry_addr,
               TxNumber tx_id_lock,
               TxNumber tx_id_wait,
               uint32_t node_id)
    {
        entry_addr_ = entry_addr;
        tx_id_lock_ = tx_id_lock;
        tx_id_wait_ = tx_id_wait;
        node_id_ = node_id;
    }

    uint64_t GetEntryAddr()
    {
        return entry_addr_;
    }
    TxNumber GetWaitTxId()
    {
        return tx_id_wait_;
    }
    TxNumber GetTxIdLock()
    {
        return tx_id_lock_;
    }
    uint32_t GetNodeId()
    {
        return node_id_;
    }

protected:
    uint64_t entry_addr_;
    TxNumber tx_id_lock_;
    TxNumber tx_id_wait_;
    uint32_t node_id_;
};

/**
 * @brief Kickout the cc entries whose commit_ts less than @ckpt_ts.
 *
 * NOTE: Should ensure that all entries already be flushed into data store
 * before kickout them.
 *
 */
struct KickoutCcEntryCc : public TemplatedCcRequest<KickoutCcEntryCc, Void>
{
public:
    static constexpr size_t KickoutPageBatchSize = 32;

    enum struct KickoutStatus
    {
        Ongoing,
        Finished,
        Error
    };

    KickoutCcEntryCc() = default;

    KickoutCcEntryCc(const TableName &table_name,
                     const uint32_t ng_id,
                     const uint64_t ckpt_ts,
                     uint16_t core_cnt,
                     CcHandlerResult<Void> *res,
                     CleanType clean_type,
                     const TxKey *start_key = nullptr,
                     const TxKey *end_key = nullptr)
        : ckpt_ts_(ckpt_ts),
          clean_type_(clean_type),
          start_key_(start_key),
          end_key_(end_key),
          unfinished_cnt_(core_cnt)
    {
        table_name_ = &table_name;
        node_group_id_ = ng_id;
        res_ = res;
        for (uint16_t i = 0; i < core_cnt; ++i)
        {
            resume_key_.emplace_back(nullptr);
        }
    }

    KickoutCcEntryCc(const KickoutCcEntryCc &rhs) = delete;
    KickoutCcEntryCc(KickoutCcEntryCc &&rhs) = delete;

    void Reset(const TableName &table_name,
               const uint32_t ng_id,
               const uint64_t ckpt_ts,
               uint16_t core_cnt,
               CcHandlerResult<Void> *res,
               CleanType clean_type,
               const TxKey *start_key = nullptr,
               const TxKey *end_key = nullptr)
    {
        // Reset struct members with passed in args
        table_name_ = &table_name;
        node_group_id_ = ng_id;
        ckpt_ts_ = ckpt_ts;
        res_ = res;
        start_key_ = start_key;
        end_key_ = end_key;
        unfinished_cnt_ = core_cnt;
        clean_type_ = clean_type;
        resume_key_.clear();
        for (uint16_t i = 0; i < core_cnt; ++i)
        {
            resume_key_.emplace_back(nullptr);
        }
    }

    bool Execute(CcShard &ccs) override
    {
        CcMap *ccm = ccs.GetCcm(*table_name_, node_group_id_);

        if (ccm != nullptr)
        {
            return ccm->Execute(*this);
        }
        else
        {
            // If no ccmap for this table, nothing to kickout, notify finish
            // directly.
            return SetFinish(ccs.core_id_);
        }
    }

    uint64_t CkptTs() const
    {
        return ckpt_ts_;
    }

    TxKey *ResumeKey(uint16_t core_id) const
    {
        return resume_key_.at(core_id).get();
    }

    void SetResumeKey(const TxKey *key, uint16_t core_id)
    {
        resume_key_.at(core_id) = key->Clone();
    }

    const TxKey *StartKey() const
    {
        return start_key_;
    }

    const TxKey *EndKey() const
    {
        return end_key_;
    }

    bool SetFinish(size_t core_id)
    {
        if (unfinished_cnt_.fetch_sub(1, std::memory_order_acq_rel) == 1)
        {
            if (res_)
            {
                res_->SetFinished();
            }
            return true;
        }
        return false;
    }

    txservice::CleanType CleanType() const
    {
        return clean_type_;
    }

private:
    uint64_t ckpt_ts_{0};
    txservice::CleanType clean_type_{CleanType::CleanForSplitRange};
    const TxKey *start_key_{nullptr};
    const TxKey *end_key_{nullptr};
    std::vector<TxKey::Uptr> resume_key_;
    std::atomic_uint16_t unfinished_cnt_{0};
};

struct ResetCleanStartPageCc : public CcRequestBase
{
public:
    explicit ResetCleanStartPageCc(size_t core_cnt)
        : mux_(), cv_(), pending_shard_(core_cnt)
    {
    }
    bool Execute(CcShard &ccs) override
    {
        ccs.ResetCleanStart();
        {
            std::unique_lock<std::mutex> lk(mux_);
            if (--pending_shard_ == 0)
            {
                cv_.notify_one();
                // Reset waiting ckpt flag. Shards should be
                // able to request ckpt again if no cc entries
                // can be kicked out.
                ccs.SetWaitingCkpt(false);
            }
        }
        return false;
    }

    void Wait()
    {
        std::unique_lock<std::mutex> lk(mux_);
        cv_.wait(lk, [this] { return pending_shard_ == 0; });
    }

    std::mutex mux_;
    std::condition_variable cv_;
    size_t pending_shard_;
};

struct GetTableLastCommitTsCc : public CcRequestBase
{
    GetTableLastCommitTsCc() = delete;
    explicit GetTableLastCommitTsCc(const TableName &table_name,
                                    const uint32_t ng_id,
                                    uint16_t core_cnt)
        : table_name_(table_name),
          node_group_id_(ng_id),
          unfinished_cnt_(core_cnt),
          last_dirty_commit_ts_(0),
          mux_(),
          cv_()
    {
    }

    bool Execute(CcShard &ccs) override
    {
        CcMap *ccm = ccs.GetCcm(table_name_, node_group_id_);
        assert(!table_name_.IsMeta());
        uint64_t last_commit_ts = 0;

        if (ccm != nullptr)
        {
            last_commit_ts = ccm->last_dirty_commit_ts_;
        }

        std::unique_lock<std::mutex> lk(mux_);
        last_dirty_commit_ts_ = std::max(last_commit_ts, last_dirty_commit_ts_);
        if (--unfinished_cnt_ == 0)
        {
            cv_.notify_one();
        }
        return false;
    }

    // Should only be called when all cores finish.
    uint64_t LastCommitTs() const
    {
        assert(unfinished_cnt_ == 0);
        return last_dirty_commit_ts_;
    }

    void Wait()
    {
        std::unique_lock<std::mutex> lk(mux_);
        cv_.wait(lk, [this] { return unfinished_cnt_ == 0; });
    }

private:
    const TableName &table_name_;
    NodeGroupId node_group_id_;
    uint16_t unfinished_cnt_;
    uint64_t last_dirty_commit_ts_;
    std::mutex mux_;
    std::condition_variable cv_;
};

/**
 * Execute command to the object specified by key.
 */
struct ApplyCc : public TemplatedCcRequest<ApplyCc, ObjectCommandResult>
{
private:
    struct LocalTuple
    {
        const TxKey *key_{};
        TxCommand *cmd_{};
    };

    struct RemoteTuple
    {
        const std::string *key_str_{};
        const std::string *cmd_str_{};
        std::unique_ptr<TxCommand> cmd_uptr_;
    };

public:
    ApplyCc() : is_local_(true)
    {
        local_input_.key_ = nullptr;
        local_input_.cmd_ = nullptr;
    }

    ~ApplyCc() override
    {
        if (!is_local_)
        {
            remote_input_.cmd_uptr_ = nullptr;
        }
    };

    void Free() override
    {
        in_use_.store(false, std::memory_order_release);
        if (!is_local_)
        {
            //  release uptrs on ApplyCc finish, instead of reuse
            remote_input_.cmd_uptr_ = nullptr;
        }
    }

    void Reset(const TableName *table_name,
               const TxKey *key,
               const uint32_t key_shard_code,
               TxCommand *cmd,
               TxCommandResult *cmd_result,
               TxNumber txn,
               int64_t tx_term,
               uint64_t tx_ts,
               CcHandlerResult<ObjectCommandResult> *res,
               CcProtocol proto,
               IsolationLevel iso_level,
               bool commit)
    {
        TemplatedCcRequest<ApplyCc, ObjectCommandResult>::Reset(
            table_name,
            res,
            Sharder::Instance().ShardToCcNodeGroup(key_shard_code),
            txn,
            proto,
            iso_level);

        if (!is_local_)
        {
            remote_input_.cmd_uptr_ = nullptr;
        }

        is_local_ = true;
        local_input_.key_ = key;
        local_input_.cmd_ = cmd;

        key_shard_code_ = key_shard_code;
        tx_term_ = tx_term;
        tx_ts_ = tx_ts;
        cce_ptr_ = nullptr;
        apply_and_commit_ = commit;
    }

    // for remote
    void Reset(const TableName *table_name,
               const std::string *key_str,
               const uint32_t key_shard_code,
               const std::string *cmd_str,
               TxNumber txn,
               int64_t tx_term,
               uint64_t tx_ts,
               CcHandlerResult<ObjectCommandResult> *res,
               CcProtocol proto,
               IsolationLevel iso_level,
               bool commit)
    {
        TemplatedCcRequest<ApplyCc, ObjectCommandResult>::Reset(
            table_name,
            res,
            Sharder::Instance().ShardToCcNodeGroup(key_shard_code),
            txn,
            proto);

        if (!is_local_)
        {
            remote_input_.cmd_uptr_ = nullptr;
        }

        is_local_ = false;
        remote_input_.key_str_ = key_str;
        remote_input_.cmd_str_ = cmd_str;

        key_shard_code_ = key_shard_code;
        tx_term_ = tx_term;
        tx_ts_ = tx_ts;
        cce_ptr_ = nullptr;
    }

    bool IsLocal() const
    {
        return is_local_;
    }

    bool IsRemote() const
    {
        return !is_local_;
    }

    bool IsReadOnly() const
    {
        if (is_local_)
        {
            return local_input_.cmd_ == nullptr ||
                   local_input_.cmd_->IsReadOnly();
        }
        return remote_input_.cmd_uptr_ == nullptr ||
               remote_input_.cmd_uptr_->IsReadOnly();
    }

    const TxKey *Key() const
    {
        return is_local_ ? local_input_.key_ : nullptr;
    }

    const std::string *KeyImage() const
    {
        return is_local_ ? nullptr : remote_input_.key_str_;
    }

    TxCommand *CommandPtr() const
    {
        return is_local_ ? local_input_.cmd_ : nullptr;
    }

    const std::string *CommandImage() const
    {
        return is_local_ ? nullptr : remote_input_.cmd_str_;
    }

    bool OwnCommand() const
    {
        return !is_local_ && remote_input_.cmd_uptr_ != nullptr;
    }

    void SetCommand(std::unique_ptr<TxCommand> cmd)
    {
        assert(!is_local_);
        remote_input_.cmd_uptr_ = std::move(cmd);
    }

    std::unique_ptr<TxCommand> ReleaseCommand()
    {
        assert(!is_local_);
        return std::move(remote_input_.cmd_uptr_);
    }

    LruEntry *CcePtr() const
    {
        return cce_ptr_;
    }

    void SetCcePtr(LruEntry *cce)
    {
        cce_ptr_ = cce;
    }

    int64_t TxTerm() const
    {
        return tx_term_;
    }

    uint64_t TxTs() const
    {
        return tx_ts_;
    }

    union
    {
        LocalTuple local_input_;
        RemoteTuple remote_input_;
    };

    bool is_local_{};
    uint32_t key_shard_code_{};
    int64_t tx_term_{-1};
    uint64_t tx_ts_{1};

    // The pointer of the cc entry to which this request is directed. The
    // pointer is set, when the request locates the cc entry but is blocked
    // due to conflicts in 2PL. After the request is unblocked and acquires
    // the lock, the request's execution resumes without further lookup of
    // the cc entry.
    LruEntry *cce_ptr_{};

    // Execute the command and directly commit it on the object, skipping
    // acquiring lock and writing log. If false, just execute the command to
    // get the result.
    bool apply_and_commit_{};
};

struct RequestAborterCc : public CcRequestBase
{
    explicit RequestAborterCc(std::vector<CcRequestBase *> &&reqs,
                              CcErrorCode err_code)
        : reqs_(std::move(reqs)), err_code_(err_code)
    {
    }

    ~RequestAborterCc() = default;

    bool Execute(CcShard &ccs) override
    {
        for (CcRequestBase *req : reqs_)
        {
            req->AbortCcRequest(err_code_);
        }

        // This object ownership is owned by itself. We need to return true to
        // make sure Free() function will be called.
        return true;
    }

    void Free() override
    {
        // This object ownership is owned by itself. We need to delete this
        // object in CcShards::ProcessCcRequest(...)
        delete this;
    }

    std::vector<CcRequestBase *> reqs_;
    CcErrorCode err_code_;
};

struct CollectMemStatsCc : public CcRequestBase
{
    explicit CollectMemStatsCc(HeapMemStats *stats) : stats_(stats)
    {
    }

    ~CollectMemStatsCc() = default;

    bool Execute(CcShard &ccs) override
    {
        mi_thread_stats(&stats_->allocated_, &stats_->committed_);
        std::lock_guard<std::mutex> lk(mux_);
        finished_ = true;
        cv_.notify_one();
        return false;
    }

    void Wait()
    {
        std::unique_lock<std::mutex> lk(mux_);
        cv_.wait(lk, [this] { return finished_ == true; });
    }

private:
    HeapMemStats *stats_;
    std::mutex mux_;
    std::condition_variable cv_;
    bool finished_{false};
};
}  // namespace txservice
