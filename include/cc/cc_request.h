#pragma once

#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include <butil/iobuf.h>
#include <mimalloc-2.1/mimalloc.h>

#include <algorithm>  // std::min
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <iomanip>
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

#include "cc/cc_map.h"
#include "cc/cc_shard.h"
#include "cc/ccm_scanner.h"
#include "cc_entry.h"
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
#include "read_write_entry.h"
#include "remote/cc_stream_receiver.h"
#include "remote/remote_type.h"
#include "scan.h"
#include "sharder.h"
#include "statistics.h"
#include "tx_command.h"
#include "tx_id.h"
#include "tx_key.h"
#include "tx_operation_result.h"
#include "tx_record.h"
#include "type.h"

namespace txservice
{
thread_local inline CcRequestPool<ReplayLogCc> replay_cc_pool_;

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
        uint32_t src_ng_id = (tx_number_ >> 32L) >> 10;
        if (TxTerm() <= Sharder::Instance().InvalidLeaderTerm(src_ng_id))
        {
            return false;
        }
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
                    std::map<TxKey, TableRangeEntry::uptr> *ranges =
                        ccs.GetTableRangesForATable(*table_name_,
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
            assert(ccs.core_id_ == ccm->shard_->core_id_);
            return ccm->Execute(*typed_req);
        }
        else
        {
            // non parallel request which is executed again, e.g. initial
            // execution blocked by lock.
            assert(ccm_ != nullptr);
            assert(ccs.core_id_ == ccm_->shard_->core_id_);
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

    int64_t TxTerm() const
    {
        return tx_term_;
    }

    void Reset(const TableName *tname,
               CcHandlerResult<ResultType> *res,
               uint32_t node_group_id,
               uint64_t tx_number,
               int64_t tx_term,
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
        tx_term_ = tx_term;
        proto_ = proto;
        isolation_level_ = iso_level;
    }

    // This method must be called on the same tx processor as the cc request.
    void AbortCcRequest(CcErrorCode err_code) override
    {
        assert(err_code != CcErrorCode::NO_ERROR);
        bool finished = res_->SetError(err_code);

        if (finished)
        {
            Free();
        }
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

    // The term of which the request comes from. We have a cache of the largest
    // invalid term on each node group. If the term is samller than the invalid
    // term, we reject the request directly since the tx coordinate node is no
    // longer the leader of ng.
    int64_t tx_term_{-1};

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
        : key_ptr_(nullptr),
          key_str_(nullptr),
          key_shard_code_(0),
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
            tname, res, ng_id, txn, tx_term, proto, iso_level);

        key_ptr_ = key->KeyPtr();
        key_str_ = nullptr;
        key_shard_code_ = key_shard_code;
        ts_ = ts;
        is_insert_ = is_insert;
        cce_ptr_ = nullptr;
        hd_result_idx_ = hd_res_idx;
        is_local_ = true;
        block_by_lock_ = false;
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
            tname, res, ng_id, txn, tx_term, proto);

        key_ptr_ = nullptr;
        key_str_ = key_str;
        key_shard_code_ = key_shard_code;
        ts_ = ts;
        is_insert_ = is_insert;
        cce_ptr_ = nullptr;
        hd_result_idx_ = hd_res_idx;
        is_local_ = false;
        block_by_lock_ = false;
    }

    const void *Key() const
    {
        return key_ptr_;
    }

    const std::string *KeyStr() const
    {
        return key_str_;
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

    bool BlockedByLock() const
    {
        return block_by_lock_;
    }

    void SetBlockedByLock(bool val)
    {
        block_by_lock_ = val;
    }

private:
    const void *key_ptr_;
    const std::string *key_str_;
    uint32_t key_shard_code_;
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
    bool block_by_lock_{false};
};

struct AcquireAllCc : public TemplatedCcRequest<AcquireAllCc, AcquireAllResult>
{
public:
    AcquireAllCc()
    {
        parallel_req_ = true;
    }
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
               uint16_t core_cnt,
               CcProtocol proto,
               CcOperation cc_op,
               IsolationLevel iso_level = IsolationLevel::ReadCommitted)
    {
        TemplatedCcRequest<AcquireAllCc, AcquireAllResult>::Reset(
            tname, res, node_group_id, tx_number, tx_term, proto, iso_level);

        key_ptr_ = key->KeyPtr();
        key_str_ = nullptr;
        key_str_type_ = nullptr;
        is_insert_ = is_insert;
        decoded_key_ = TxKey();
        cc_op_ = cc_op;
        cce_ptr_.clear();
        cce_ptr_.resize(core_cnt, nullptr);
        is_local_ = true;
        res->Value().last_vali_ts_ = 0;
    }

    void Reset(const TableName *tname,
               const std::string *key_str,
               const KeyType *key_str_type,
               uint32_t node_group_id,
               TxNumber tx_number,
               int64_t tx_term,
               bool is_insert,
               CcHandlerResult<AcquireAllResult> *res,
               uint16_t core_cnt,
               CcProtocol proto,
               CcOperation cc_op,
               IsolationLevel iso_level = IsolationLevel::ReadCommitted)
    {
        TemplatedCcRequest<AcquireAllCc, AcquireAllResult>::Reset(
            tname, res, node_group_id, tx_number, tx_term, proto, iso_level);

        key_ptr_ = nullptr;
        key_str_ = key_str;
        key_str_type_ = key_str_type;
        is_insert_ = is_insert;
        decoded_key_ = TxKey();
        cc_op_ = cc_op;
        cce_ptr_.clear();
        cce_ptr_.resize(core_cnt, nullptr);
        is_local_ = false;
        res->Value().last_vali_ts_ = 0;
    }

    const void *Key() const
    {
        return key_ptr_;
    }

    const std::string *KeyStr() const
    {
        return key_str_;
    }

    const KeyType *KeyStrType() const
    {
        return key_str_type_;
    }

    bool IsInsert() const
    {
        return is_insert_;
    }

    CcOperation CcOp() const
    {
        return cc_op_;
    }

    void SetDecodedKey(TxKey decoded_key)
    {
        assert(decoded_key.IsOwner());
        decoded_key_ = std::move(decoded_key);
        key_ptr_ = decoded_key_.KeyPtr();
    }

    void SetTxKey(const void *key)
    {
        key_ptr_ = key;
    }

    void SetCcePtr(LruEntry *ptr, uint16_t idx)
    {
        cce_ptr_[idx] = ptr;
    }

    LruEntry *CcePtr(uint16_t idx) const
    {
        return cce_ptr_[idx];
    }

    bool IsLocal() const
    {
        return is_local_;
    }

    void SetLastValidTs(uint64_t ts)
    {
        // All cores will try to update last valid ts, so we need mutex
        // protection here.
        std::lock_guard<std::mutex> lk(mux_);
        res_->Value().last_vali_ts_ = std::max(res_->Value().last_vali_ts_, ts);
    }

protected:
    // protects acquire all res in concurrent update from different cores.
    std::mutex mux_;

private:
    const void *key_ptr_{nullptr};
    const std::string *key_str_{nullptr};
    const KeyType *key_str_type_{nullptr};
    TxKey decoded_key_{};
    bool is_insert_{false};
    CcOperation cc_op_{CcOperation::Write};
    // The pointer of the cc entry to which this request is directed. The
    // pointer is set, when the request locates the cc entry but is
    // blocked due to conflicts in 2PL. After the request is unblocked and
    // acquires the lock, the request's execution resumes without further lookup
    // of the cc entry.
    std::vector<LruEntry *> cce_ptr_;
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
        uint32_t src_ng_id = (tx_number_ >> 32L) >> 10;
        if (tx_term_ <= Sharder::Instance().InvalidLeaderTerm(src_ng_id))
        {
            return false;
        }
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
                if (lru_entry->PayloadStatus() == RecordStatus::Invalid)
                {
                    return false;
                }
                ccm_ = lru_entry->GetCcMap();
            }
            else if (cce_addr_->CcePtr() != 0)
            {
                const LruEntry *lru_entry =
                    reinterpret_cast<const LruEntry *>(cce_addr_->CcePtr());
                if (lru_entry->PayloadStatus() == RecordStatus::Invalid)
                {
                    return false;
                }
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
               int64_t tx_term,
               uint64_t ts,
               const TxRecord *rec,
               OperationType operation_type,
               uint32_t key_shard_code,
               CcHandlerResult<PostProcessResult> *res)
    {
        TemplatedCcRequest<PostWriteCc, PostProcessResult>::Reset(
            nullptr, res, addr->NodeGroupId(), tx_number, tx_term);

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
               int64_t tx_term,
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
            tx_term,
            CcProtocol::OCC,
            IsolationLevel::ReadCommitted,
            ng_term);

        cce_addr_ = nullptr;
        key_ = key != nullptr ? key->KeyPtr() : nullptr;
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
               int64_t tx_term,
               uint64_t ts,
               const std::string *rec,
               OperationType operation_type,
               uint32_t key_shard_code,
               CcHandlerResult<PostProcessResult> *res)
    {
        TemplatedCcRequest<PostWriteCc, PostProcessResult>::Reset(
            nullptr, res, addr->NodeGroupId(), tx_number, tx_term);

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
               int64_t tx_term,
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
            tx_term,
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

    const void *Key() const
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
        const void *key_;
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
            tname, res, node_group_id, tx_number, tx_term, CcProtocol::OCC);

        key_ = key->KeyPtr();
        key_str_ = nullptr;
        key_str_type_ = nullptr;
        decoded_key_ = TxKey();
        commit_ts_ = ts;
        payload_ = rec;
        payload_str_ = nullptr;
        decoded_payload_ = nullptr;
        op_type_ = op_type;
        commit_type_ = commit_type;
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
            tname, res, node_group_id, tx_number, tx_term, CcProtocol::OCC);

        key_ = key->KeyPtr();
        key_str_type_ = nullptr;
        key_str_ = nullptr;
        decoded_key_ = TxKey();
        commit_ts_ = ts;
        payload_ = rec.get();
        payload_str_ = nullptr;
        decoded_payload_ = std::move(rec);
        op_type_ = op_type;
        commit_type_ = commit_type;
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
            tname, res, node_group_id, tx_number, tx_term, CcProtocol::OCC);

        key_ = nullptr;
        key_str_ = key_str;
        key_str_type_ = key_str_type;
        decoded_key_ = TxKey();
        commit_ts_ = ts;
        payload_ = nullptr;
        payload_str_ = rec;
        decoded_payload_ = nullptr;
        op_type_ = op_type;
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

    OperationType OpType() const
    {
        return op_type_;
    }

    void SetTxKey(const void *key)
    {
        key_ = key;
    }

    const void *Key() const
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

    void SetDecodedKey(TxKey decoded_key)
    {
        decoded_key_ = std::move(decoded_key);
        key_ = decoded_key_.KeyPtr();
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
    const void *key_{nullptr};
    const std::string *key_str_{nullptr};
    const KeyType *key_str_type_{nullptr};
    TxKey decoded_key_;
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

        uint32_t src_ng_id = (tx_number_ >> 32L) >> 10;
        if (TxTerm() <= Sharder::Instance().InvalidLeaderTerm(src_ng_id))
        {
            return false;
        }

        const LruEntry *lru_entry =
            reinterpret_cast<const LruEntry *>(cce_addr_->CcePtr());
        if (lru_entry->PayloadStatus() == RecordStatus::Invalid)
        {
            return false;
        }
        ccm_ = lru_entry->GetCcMap();
        assert(ccm_ != nullptr);
        return true;
    }

    void Reset(const CcEntryAddr *addr,
               uint64_t tx_number,
               int64_t tx_term,
               uint64_t commit_ts,
               uint64_t key_ts,
               uint64_t gap_ts,
               CcHandlerResult<PostProcessResult> *res)
    {
        TemplatedCcRequest<PostReadCc, PostProcessResult>::Reset(
            nullptr, res, addr->NodeGroupId(), tx_number, tx_term);

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
    enum BlockType
    {
        NotBlocked,
        BlockByLock,
        // If CcEntry's CommitTs is less than read_ts when do
        // "PkReadCorrespondingSk" or "SnapshotRead", there must be a
        // PostWriteCc request has not done, then, this read should wait until
        // it is completed.
        BlockByPostWrite
    };
    ReadCc()
        : key_ptr_(nullptr),
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
        uint32_t src_ng_id = (tx_number_ >> 32L) >> 10;
        if (TxTerm() <= Sharder::Instance().InvalidLeaderTerm(src_ng_id))
        {
            return false;
        }
        int64_t cc_ng_term = -1;
        if (is_in_recovering_)
        {
            cc_ng_term =
                Sharder::Instance().CandidateLeaderTerm(node_group_id_);
        }
        if (cc_ng_term < 0)
        {
            cc_ng_term = Sharder::Instance().LeaderTerm(node_group_id_);
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
            if (lru_entry->PayloadStatus() == RecordStatus::Invalid)
            {
                return false;
            }
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
               bool is_in_recovering = false,
               bool point_read_on_miss = false)
    {
        uint32_t ng_id = Sharder::Instance().ShardToCcNodeGroup(key_shard_code);
        TemplatedCcRequest<ReadCc, ReadKeyResult>::Reset(
            nullptr, res, ng_id, tx_number, tx_term, protocol, iso_level);

        key_ptr_ = key->KeyPtr();
        key_str_ = nullptr;
        key_shard_code_ = key_shard_code;
        rec_ = rec;
        rec_str_ = nullptr;
        ts_ = ts;
        type_ = read_type;
        is_for_write_ = is_for_write;
        cce_ptr_ = nullptr;
        archives_ = archives;
        is_local_ = true;
        is_in_recovering_ = is_in_recovering;
        is_covering_keys_ = is_covering_keys;
        point_read_on_cache_miss_ = point_read_on_miss;
        blk_type_ = NotBlocked;

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
               std::vector<VersionTxRecord> *archives = nullptr,
               bool point_read_on_miss = false)
    {
        uint32_t ng_id = Sharder::Instance().ShardToCcNodeGroup(key_shard_code);
        TemplatedCcRequest<ReadCc, ReadKeyResult>::Reset(
            nullptr, res, ng_id, tx_number, tx_term, protocol, iso_level);

        key_ptr_ = nullptr;
        key_str_ = key_str;
        key_shard_code_ = key_shard_code;
        rec_ = nullptr;
        rec_str_ = rec_str;
        ts_ = ts;
        type_ = read_type;
        is_for_write_ = is_for_write;
        cce_ptr_ = nullptr;
        archives_ = archives;
        is_local_ = false;
        is_in_recovering_ = false;
        is_covering_keys_ = is_covering_keys;
        point_read_on_cache_miss_ = point_read_on_miss;
        blk_type_ = NotBlocked;

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
               const std::string &key_str,
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
               bool point_read_on_miss = false)
    {
        uint32_t ng_id = Sharder::Instance().ShardToCcNodeGroup(key_shard_code);
        TemplatedCcRequest<ReadCc, ReadKeyResult>::Reset(
            nullptr, res, ng_id, tx_number, tx_term, protocol, iso_level);

        key_ptr_ = nullptr;
        key_str_ = &key_str;
        key_shard_code_ = key_shard_code;
        rec_ = rec;
        rec_str_ = nullptr;
        ts_ = ts;
        type_ = read_type;
        is_for_write_ = is_for_write;
        cce_ptr_ = nullptr;
        archives_ = archives;
        is_local_ = true;
        is_in_recovering_ = false;
        is_covering_keys_ = is_covering_keys;
        point_read_on_cache_miss_ = point_read_on_miss;
        blk_type_ = NotBlocked;

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

    const void *Key() const
    {
        return key_ptr_;
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

    bool IsInRecovering() const
    {
        return is_in_recovering_;
    }

    bool IsCoveringKeys() const
    {
        return is_covering_keys_;
    }

    bool PointReadOnCacheMiss() const
    {
        return point_read_on_cache_miss_;
    }

    BlockType BlockedBy() const
    {
        return blk_type_;
    }

    void SetBlockType(BlockType blk)
    {
        blk_type_ = blk;
    }

private:
    const void *key_ptr_;
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

    // Is issued in a recovering process
    bool is_in_recovering_{false};
    // Reserved for unique sk read
    bool is_covering_keys_{false};
    bool point_read_on_cache_miss_{false};
    BlockType blk_type_{BlockType::NotBlocked};

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
               bool is_include_floor_cce = false
#ifdef ON_KEY_OBJECT
               ,
               bool is_skip_kv = false,
               int32_t obj_type = -1,
               const std::string_view &scan_pattern = {}
#endif
    )
    {
        TemplatedCcRequest<ScanOpenBatchCc, ScanOpenResult>::Reset(
            tn, res, ng_id, tx_number, term, protocol, iso_level);

        index_type_ = type;
        start_key_ = start_key->KeyPtr();
        inclusive_ = inclusive;
        direct_ = direction;
        ts_ = ts;
        scan_cache_ = cache;
        is_for_write_ = is_for_write;
        is_ckpt_delta_ = is_delta;
        is_covering_keys_ = is_covering_keys;
        is_include_floor_cce_ = is_include_floor_cce;
        cce_ptr_ = nullptr;
        cce_ptr_scan_type_ = ScanType::ScanUnknow;
#ifdef ON_KEY_OBJECT
        is_skip_kv_ = is_skip_kv;
        obj_type_ = obj_type;
        scan_pattern_ = scan_pattern;
#endif
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
#ifdef ON_KEY_OBJECT
    bool IsSkipKv() const
    {
        return is_skip_kv_;
    }

    int32_t GetRedisObjectType() const
    {
        return obj_type_;
    }
    const std::string_view &GetRedisScanPattern() const
    {
        return scan_pattern_;
    }
#endif

private:
    ScanIndexType index_type_{ScanIndexType::Primary};
    const void *start_key_{nullptr};
    bool inclusive_{false};
    ScanDirection direct_{ScanDirection::Forward};
    uint64_t ts_{0};
    ScanCache *scan_cache_{nullptr};
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
#ifdef ON_KEY_OBJECT
    bool is_skip_kv_{false};
    int32_t obj_type_{-1};
    std::string_view scan_pattern_;
#endif

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
        uint32_t src_ng_id = (tx_number_ >> 32L) >> 10;
        if (TxTerm() <= Sharder::Instance().InvalidLeaderTerm(src_ng_id))
        {
            return false;
        }
        int64_t cc_ng_term = Sharder::Instance().LeaderTerm(node_group_id_);
        if (cce_addr_->Term() != cc_ng_term)
        {
            return false;
        }

        const LruEntry *lru_entry =
            reinterpret_cast<const LruEntry *>(cce_addr_->CcePtr());
        if (lru_entry->PayloadStatus() == RecordStatus::Invalid)
        {
            return false;
        }
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
               bool is_covering_keys
#ifdef ON_KEY_OBJECT
               ,
               bool is_skip_kv = false,
               int32_t obj_type = -1,
               const std::string_view &scan_pattern = {}
#endif
    )
    {
        TemplatedCcRequest<ScanNextBatchCc, ScanNextResult>::Reset(
            nullptr, next_res, ng_id, tx_number, tx_term, protocol, iso_level);

        ts_ = ts;
        scan_cache_ = cache;
        is_for_write_ = is_for_write;
        is_ckpt_delta_ = is_delta;
        is_covering_keys_ = is_covering_keys;
        cce_ptr_ = nullptr;
        cce_ptr_scan_type_ = ScanType::ScanUnknow;

        const ScanTuple *last_tuple = cache->LastTuple();
        cce_addr_ = &last_tuple->cce_addr_;
        ccm_ = nullptr;
#ifdef ON_KEY_OBJECT
        is_skip_kv_ = is_skip_kv;
        obj_type_ = obj_type;
        scan_pattern_ = scan_pattern;
#endif
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

#ifdef ON_KEY_OBJECT
    bool IsSkipKv() const
    {
        return is_skip_kv_;
    }

    int32_t GetRedisObjectType() const
    {
        return obj_type_;
    }
    const std::string_view &GetRedisScanPattern() const
    {
        return scan_pattern_;
    }
#endif

private:
    const CcEntryAddr *cce_addr_;
    uint64_t ts_{0};
    ScanCache *scan_cache_{nullptr};

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

#ifdef ON_KEY_OBJECT
    bool is_skip_kv_{false};
    int32_t obj_type_{-1};
    std::string_view scan_pattern_;
#endif
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
            start_key_uptr_.~TxKey();
        }
        if (end_key_type_ == RangeKeyType::UniquePtr)
        {
            end_key_uptr_.~TxKey();
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
             bool is_require_keys,
             bool is_require_recs,
             bool is_require_sort,
             uint32_t prefetch_size)
    {
        assert(hd_res.Value().is_local_);

        TemplatedCcRequest<ScanSliceCc, RangeScanSliceResult>::Reset(
            &tbl_name, &hd_res, ng_id, tx_number, tx_term, protocol, iso_level);

        range_id_ = range_id;

        if (start_key_type_ == RangeKeyType::UniquePtr)
        {
            start_key_uptr_.~TxKey();
        }
        start_key_ = start_key != nullptr ? start_key->KeyPtr() : nullptr;
        start_key_type_ = RangeKeyType::RawPtr;
        start_inclusive_ = start_inclusive;

        if (end_key_type_ == RangeKeyType::UniquePtr)
        {
            end_key_uptr_.~TxKey();
        }
        end_key_ = end_key != nullptr ? end_key->KeyPtr() : nullptr;
        end_key_type_ = RangeKeyType::RawPtr;
        end_inclusive_ = end_inclusive;

        direction_ = hd_res.Value().ccm_scanner_->Direction();
        ts_ = read_ts;
        cc_ng_term_ = ng_term;
        read_for_write_ = read_for_write;
        is_covering_keys_ = is_covering_keys;
        is_require_keys_ = is_require_keys;
        is_require_recs_ = is_require_recs;
        is_require_sort_ = is_require_sort;

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
             bool is_require_keys,
             bool is_require_recs,
             bool is_require_sort,
             uint32_t prefetch_size)
    {
        assert(!hd_res.Value().is_local_);

        TemplatedCcRequest<ScanSliceCc, RangeScanSliceResult>::Reset(
            &tbl_name, &hd_res, ng_id, tx_number, tx_term, protocol, iso_level);

        range_id_ = range_id;

        if (start_key_type_ == RangeKeyType::UniquePtr)
        {
            start_key_uptr_.~TxKey();
        }
        start_key_str_ = start_key_str;
        start_key_type_ = RangeKeyType::Binary;
        start_inclusive_ = start_inclusive;

        if (end_key_type_ == RangeKeyType::UniquePtr)
        {
            end_key_uptr_.~TxKey();
        }
        end_key_str_ = end_key_str;
        end_key_type_ = RangeKeyType::Binary;
        end_inclusive_ = end_inclusive;

        direction_ = direction;
        ts_ = read_ts;
        cc_ng_term_ = ng_term;
        read_for_write_ = read_for_write;
        is_covering_keys_ = is_covering_keys;
        is_require_keys_ = is_require_keys;
        is_require_recs_ = is_require_recs;
        is_require_sort_ = is_require_sort;
        prefetch_size_ = prefetch_size;

        unfinished_core_cnt_.store(1, std::memory_order_relaxed);
        range_slice_id_.Reset();
        last_pinned_slice_ = nullptr;
        err_ = CcErrorCode::NO_ERROR;
    }

    bool Execute(CcShard &ccs) override
    {
        if (!ValidTermCheck())
        {
            // Do not modify res_ directly since there could be other cores
            // still working on this cc req.
            return SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
        }

        CcMap *ccm = nullptr;

        if (parallel_req_ || ccm_ == nullptr)
        {
            // assert(table_name_ != nullptr);
            assert(table_name_->StringView() != empty_sv);
            ccm = ccs.GetCcm(*table_name_, node_group_id_);

            if (ccm == nullptr)
            {
                // Find base table name for index table.
                // Fetch/Get Catalog is based on base table name, but Get
                // ccmap is based on the real table name, for example, index
                // should get the corresponding sk_ccmap.
                assert(!table_name_->IsMeta());
                const CatalogEntry *catalog_entry =
                    ccs.InitCcm(*table_name_, node_group_id_, ng_term_, this);
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
                        res_->SetError(CcErrorCode::REQUESTED_TABLE_NOT_EXISTS);
                        return true;
                    }

                    ccm = ccs.GetCcm(*table_name_, node_group_id_);
                }
            }
            if (!parallel_req_)
            {
                ccm_ = ccm;
            }
            assert(ccm != nullptr);
            return ccm->Execute(*this);
        }
        else
        {
            // non parallel request which is executed again, e.g. initial
            // execution blocked by lock.
            assert(ccm_ != nullptr);
            return ccm_->Execute(*this);
        }
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

    const void *StartKey() const
    {
        switch (start_key_type_)
        {
        case RangeKeyType::RawPtr:
            return start_key_;
        case RangeKeyType::Binary:
            return nullptr;
        case RangeKeyType::UniquePtr:
            return start_key_uptr_.KeyPtr();
        default:
            return nullptr;
        }
    }

    const void *EndKey() const
    {
        switch (end_key_type_)
        {
        case RangeKeyType::RawPtr:
            return end_key_;
        case RangeKeyType::Binary:
            return nullptr;
        case RangeKeyType::UniquePtr:
            return end_key_uptr_.KeyPtr();
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

    void SetStartKey(TxKey start_key)
    {
        if (start_key_type_ == RangeKeyType::UniquePtr)
        {
            start_key_uptr_ = std::move(start_key);
        }
        else
        {
            start_key_type_ = RangeKeyType::UniquePtr;
            // Clears the ownership bit, so that the following move assignment
            // does not accidentally trigger de-allocation.
            start_key_uptr_.Release();
            start_key_uptr_ = std::move(start_key);
        }
    }

    void SetEndKey(TxKey end_key)
    {
        if (end_key_type_ == RangeKeyType::UniquePtr)
        {
            end_key_uptr_ = std::move(end_key);
        }
        else
        {
            end_key_type_ = RangeKeyType::UniquePtr;
            end_key_uptr_.Release();
            // Clears the ownership bit, so that the following move assignment
            // does not accidentally trigger de-allocation.
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

    ScanCache *GetLocalScanCache(size_t shard_id)
    {
        assert(IsLocal());
        return res_->Value().ccm_scanner_->Cache(shard_id);
    }

    RemoteScanSliceCache *GetRemoteScanCache(size_t shard_id)
    {
        assert(!IsLocal());
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
                    res_->Value().ccm_scanner_->FinalizeCommit();

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

    bool IsRequireKeys() const
    {
        return is_require_keys_;
    }

    bool IsRequireRecords() const
    {
        return is_require_recs_;
    }

    bool IsRequireSort() const
    {
        return is_require_sort_;
    }

    /**
     * @brief Returns the number of slices to prefetch when loading a cache-miss
     * slice.
     *
     * @return uint32_t Number of slices to prefetch
     */
    uint32_t PrefetchSize() const
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
    enum struct RangeKeyType : uint8_t
    {
        RawPtr,
        Binary,
        UniquePtr
    };

    union
    {
        const void *start_key_;
        const std::string *start_key_str_;
        TxKey start_key_uptr_;
    };

    union
    {
        const void *end_key_;
        const std::string *end_key_str_;
        TxKey end_key_uptr_;
    };

    RangeKeyType start_key_type_;
    RangeKeyType end_key_type_;
    bool start_inclusive_{false};
    bool end_inclusive_{false};

    ScanDirection direction_{ScanDirection::Forward};
    /**
     * @brief Number of slices to prefetch when a cache-miss slice is loaded.
     *
     */
    uint32_t prefetch_size_{0};
    bool read_for_write_{false};
    bool is_covering_keys_{false};

    /**
     * For select count(*), we can skip copying keys and vals. We can
     * skip merge-sort.
     */
    bool is_require_keys_{true};
    bool is_require_recs_{true};
    bool is_require_sort_{true};

    uint32_t range_id_{0};

    std::atomic<uint16_t> unfinished_core_cnt_{1};
    CcErrorCode err_{CcErrorCode::NO_ERROR};

    uint64_t ts_{0};

    const StoreSlice *last_pinned_slice_{nullptr};

    int64_t cc_ng_term_{-1};

    struct ScanBlockingInfo
    {
        uint64_t cce_addr_;
        ScanType scan_type_;
        ScanBlockingType type_;
    };
    std::vector<ScanBlockingInfo> blocking_vec_;

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
            memory_allocated_vec_.emplace_back(0);
            memory_committed_vec_.emplace_back(0);
            heap_full_vec_.emplace_back(false);
        }
    }

    CkptTsCc() = delete;
    CkptTsCc(const CkptTsCc &) = delete;
    CkptTsCc(CkptTsCc &&) = delete;

    bool Execute(CcShard &ccs) override
    {
        uint64_t tx_min_ts = ccs.ActiveTxMinTs(cc_ng_id_);
        int64_t allocated, committed;
        bool full = ccs.GetShardHeap()->Full(&allocated, &committed);

        uint64_t old_val = ckpt_ts_.load(std::memory_order_relaxed);
        if (old_val > tx_min_ts)
        {
            while (!ckpt_ts_.compare_exchange_weak(
                       old_val, tx_min_ts, std::memory_order_acq_rel) &&
                   old_val > tx_min_ts)
                ;
        }
        memory_allocated_vec_[ccs.LocalCoreId()] = allocated;
        memory_committed_vec_[ccs.LocalCoreId()] = committed;
        heap_full_vec_[ccs.LocalCoreId()] = full;
        uint64_t finished = finish_cnt_.fetch_add(1, std::memory_order_acquire);
        if (finished == shard_cnt_ - 1)
        {
            std::unique_lock lk(mux_);
            cv_.notify_one();
        }

        // return false since CkptTsCc is not reused and does not need to call
        // CcRequestBase::Free
        return false;
    }

    void Wait()
    {
        std::unique_lock lk(mux_);
        while (finish_cnt_.load(std::memory_order_relaxed) != shard_cnt_)
        {
            cv_.wait(lk);
        }
    }

    uint64_t GetCkptTs() const
    {
        return ckpt_ts_.load(std::memory_order_relaxed);
    }

    uint64_t GetMemUsage() const
    {
        uint64_t total_usage = 0;
        for (uint64_t shard_usage : memory_allocated_vec_)
        {
            total_usage += shard_usage;
        }
        // return in kb
        return total_usage / 1024;
    }

    uint64_t GetMemCommited() const
    {
        uint64_t total_cmt = 0;
        for (uint64_t shard_cmt : memory_committed_vec_)
        {
            total_cmt += shard_cmt;
        }
        // return in kb
        return total_cmt / 1024;
    }

    void ShardMemoryUsageReport(LocalCcShards &local_shards)
    {
        for (uint16_t core_id = 0; core_id < memory_allocated_vec_.size();
             core_id++)
        {
            uint64_t &allocated = memory_allocated_vec_[core_id];
            uint64_t &committed = memory_committed_vec_[core_id];
            bool heap_full = heap_full_vec_[core_id];
            LOG(INFO) << "ccs " << core_id << " memory usage report, committed "
                      << committed << ", allocated " << allocated
                      << ", frag ratio " << std::setprecision(2)
                      << 100 * (static_cast<float>(committed - allocated) /
                                committed)
                      << " , heap full: " << heap_full;
        }
    }

private:
    std::atomic<uint64_t> ckpt_ts_;
    bthread::Mutex mux_;
    bthread::ConditionVariable cv_;
    std::atomic<size_t> finish_cnt_;
    size_t shard_cnt_;
    std::vector<uint64_t> memory_allocated_vec_;
    std::vector<uint64_t> memory_committed_vec_;
    std::vector<bool> heap_full_vec_;
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
                hd_res_->Value().ccm_scanner_->FinalizeCommit();

                hd_res_->SetFinished();
            }

            hd_res_->DecreaseCurrentHandlingResponse();

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

struct DefragShardHeapCc : public CcRequestBase
{
public:
    DefragShardHeapCc() = delete;
    ~DefragShardHeapCc() = default;

    explicit DefragShardHeapCc(size_t scan_batch_size)
        : scan_batch_size_(scan_batch_size), node_groups_(), tables_()
    {
    }

    DefragShardHeapCc(const DefragShardHeapCc &other) = delete;

    DefragShardHeapCc(DefragShardHeapCc &&other)
        : scan_batch_size_(other.scan_batch_size_),
          node_groups_(std::move(other.node_groups_)),
          tables_(std::move(other.tables_)),
          err_(other.err_),
          current_node_group_idx_(other.current_node_group_idx_),
          current_table_idx_(other.current_table_idx_),
          pause_pos_(std::move(other.pause_pos_)),
          defrag_cnt_(other.defrag_cnt_),
          lock_cnt_(other.lock_cnt_),
          kv_load_cnt_(other.kv_load_cnt_),
          ckpt_cnt_(other.ckpt_cnt_),
          non_frag_cnt_(other.non_frag_cnt_),
          total_cnt_(other.total_cnt_),
          ccmp_key_defraged_(other.ccmp_key_defraged_),
          run_count_(other.run_count_)
    {
    }

    void Reset(std::vector<std::pair<uint32_t, int64_t>> node_groups)
    {
        assert(node_groups.size() > 0);
        node_groups_ = std::move(node_groups);
        current_node_group_idx_ = 0;
        current_table_idx_ = -1;
        err_ = CcErrorCode::NO_ERROR;

        tables_.clear();
        pause_pos_ = {TxKey(), false};
        defrag_cnt_ = 0;
        lock_cnt_ = 0;
        kv_load_cnt_ = 0;
        ckpt_cnt_ = 0;
        non_frag_cnt_ = 0;
        total_cnt_ = 0;
        ccmp_key_defraged_ = false;
        run_count_ = 0;
    }

    bool Execute(CcShard &ccs) override
    {
        mi_heap_t *df = mi_heap_get_default();
        assert(df == ccs.GetShardHeap()->heap_);
        assert(ccs.GetShardHeapThreadId() == mi_thread_id());
        run_count_++;
        // dequeue wait list if heap is not full anymore every 20 scan batch
        if (run_count_ % 20 == 0 && !ccs.GetShardHeap()->Full())
        {
            ccs.DequeueWaitList();
        }

        if (static_cast<size_t>(current_node_group_idx_) < node_groups_.size())
        {
            auto &[current_node_group_id, current_node_group_term] =
                node_groups_[current_node_group_idx_];

            // check leader term of current node group
            int64_t cc_ng_term =
                Sharder::Instance().LeaderTerm(current_node_group_id);
            if (cc_ng_term < 0 || cc_ng_term != current_node_group_term)
            {
                // move to process next node group if term not matched
                tables_.clear();
                current_table_idx_ = -1;
                current_node_group_idx_++;
                ccs.Enqueue(this);
                return false;
            }

            // init tables for current node group
            if (current_table_idx_ == -1)
            {
                std::unordered_map<TableName, bool> tables =
                    ccs.GetCatalogTableNameSnapshot(current_node_group_id);
                for (auto &table : tables)
                {
                    if (table.first.IsMeta())
                    {
                        continue;
                    }
                    tables_.push_back(table.first);
                    // also need to defrag range cc map
                    TableName range_table_name{table.first.String(),
                                               TableType::RangePartition};
                    tables_.push_back(std::move(range_table_name));
                }
                current_table_idx_ = 0;
            }
        }
        else
        {
            // this is the end of the defrag heap cc scan
            ccs.SetDefragHeapCcOnFly(false);
            // deque cc request in wait list after
            // defragmentation
            ccs.DequeueWaitList();

            int64_t allocated, committed;
            mi_thread_stats(&allocated, &committed);
            LOG(INFO) << "Memory state after defragmentation in ccs "
                      << ccs.core_id_
                      << ", total comitted memory: " << committed
                      << ", actual used memory " << allocated << ", frag ratio "
                      << std::setprecision(2)
                      << 100 * (static_cast<float>(committed - allocated) /
                                committed);
            assert(df == ccs.GetShardHeap()->heap_);
            assert(ccs.GetShardHeapThreadId() == mi_thread_id());
            return false;
        }

        // if current table is drain, then move to next table
        if (IsCurrentTableDrained())
        {
            if (static_cast<size_t>(current_table_idx_) < tables_.size())
            {
                if (total_cnt_ > 0)
                {
                    float defrag_ratio =
                        static_cast<float>(defrag_cnt_) / total_cnt_;
                    DLOG(INFO) << "Defragmentation table "
                               << tables_.at(current_table_idx_).String()
                               << " table type: "
                               << static_cast<int>(
                                      tables_.at(current_table_idx_).Type())
                               << " ngid: "
                               << node_groups_[current_node_group_idx_].first
                               << " finished on core " << ccs.core_id_
                               << ", defrag count: " << defrag_cnt_
                               << ", lock count: " << lock_cnt_
                               << ", ckpt count: " << ckpt_cnt_
                               << ", kv load count: " << kv_load_cnt_
                               << ", non frag count: " << non_frag_cnt_
                               << ", total count: " << total_cnt_
                               << std::setprecision(2)
                               << ", defrag ratio: " << 100 * defrag_ratio;
                }
                defrag_cnt_ = 0;
                lock_cnt_ = 0;
                ckpt_cnt_ = 0;
                kv_load_cnt_ = 0;
                non_frag_cnt_ = 0;
                total_cnt_ = 0;
                current_table_idx_++;
            }

            // reset table state
            ccmp_key_defraged_ = false;
            pause_pos_ = {TxKey(), false};

            // if run out of tables
            if (static_cast<size_t>(current_table_idx_) == tables_.size())
            {
                tables_.clear();
                current_table_idx_ = -1;
                current_node_group_idx_++;
                ccs.Enqueue(this);
                return false;
            }
        }

        auto &table_name = tables_.at(current_table_idx_);

        CcMap *ccm =
            ccs.GetCcm(table_name, node_groups_[current_node_group_idx_].first);
        // if ccm is not exist anymore for some reason, just skip it
        if (ccm != nullptr)
        {
            ccm->Execute(*this);
            assert(df == ccs.GetShardHeap()->heap_);
            assert(ccs.GetShardHeapThreadId() == mi_thread_id());
        }
        else
        // if ccm if dropped, move to next table
        {
            pause_pos_ = {TxKey(), true};
            ccs.Enqueue(this);
        }
        (void) df;

        return false;
    }

    void SetError(CcErrorCode err)
    {
        err_ = err;
    }

    void AbortCcRequest(CcErrorCode err_code) override
    {
        assert(err_code != CcErrorCode::NO_ERROR);
        err_ = err_code;
    }

    bool IsError()
    {
        return err_ != CcErrorCode::NO_ERROR;
    }

    CcErrorCode ErrorCode()
    {
        return err_;
    }

    bool IsCurrentTableDrained() const
    {
        return pause_pos_.second;
    }

    uint32_t CurrentNodeGroupId()
    {
        return node_groups_[current_node_group_idx_].first;
    }

    std::pair<TxKey, bool> &PausePos()
    {
        return pause_pos_;
    }

    const size_t scan_batch_size_;

    std::vector<std::pair<uint32_t, int64_t>> node_groups_;
    std::vector<TableName> tables_;
    CcErrorCode err_{CcErrorCode::NO_ERROR};
    // the node groups
    int32_t current_node_group_idx_{0};
    // the table of current node groups being defragmented while this defrag cc
    // is in flying
    int32_t current_table_idx_{-1};

    std::pair<TxKey, bool> pause_pos_;
    size_t defrag_cnt_{0};
    size_t lock_cnt_{0};
    size_t kv_load_cnt_{0};
    size_t ckpt_cnt_{0};
    size_t non_frag_cnt_{0};
    size_t total_cnt_{0};
    bool ccmp_key_defraged_{0};
    // count the executed times
    size_t run_count_{0};
};

struct DataSyncScanCc : public CcRequestBase
{
public:
    // how many pages to scan one time
#ifdef ON_KEY_OBJECT
    // Yield more often on redis since any run one round
    // latency increase cause significant peformance impact.
    static constexpr size_t DataSyncScanBatchSize = 32;
#else
    static constexpr size_t DataSyncScanBatchSize = 128;
#endif

    DataSyncScanCc() = delete;
    ~DataSyncScanCc() = default;

    DataSyncScanCc(const TableName &table_name,
                   uint64_t previous_scan_ts,
                   uint64_t previous_ckpt_ts,
                   uint64_t data_sync_ts,
                   uint64_t node_group_id,
                   int64_t node_group_term,
                   uint16_t core_cnt,
                   size_t scan_batch_size,
                   uint64_t txn,
                   const TxKey *target_start_key,
                   const TxKey *target_end_key,
                   bool include_persisted_data,
#ifdef RANGE_PARTITION_ENABLED
                   bool export_base_table_rec_if_need = false,
                   bool skip_archived_key = false
#else
                   bool only_one_core,
                   std::function<bool(size_t hash_code)> filter
#endif
                   ,
                   uint64_t schema_version = 0)
        : force_flush_(false),
          table_name_(&table_name),
          node_group_id_(node_group_id),
          node_group_term_(node_group_term),
          core_cnt_(core_cnt),
          previous_scan_ts_(previous_scan_ts),
          previous_ckpt_ts_(previous_ckpt_ts),
          data_sync_ts_(data_sync_ts),
          start_key_(target_start_key),
          end_key_(target_end_key),
          scan_batch_size_(scan_batch_size),
          err_(CcErrorCode::NO_ERROR),
          unfinished_cnt_(core_cnt_),
          mux_(),
          cv_(),
          include_persisted_data_(include_persisted_data)
#ifdef RANGE_PARTITION_ENABLED
          ,
          export_base_table_rec_if_need_(export_base_table_rec_if_need),
          skip_archived_key_(skip_archived_key)
#else
          ,
          only_scan_one_core_(only_one_core),
          filter_lambda_(filter)
#endif
          ,
          schema_version_(schema_version)
    {
        tx_number_ = txn;
        assert(scan_batch_size_ > DataSyncScanBatchSize);
        for (size_t i = 0; i < core_cnt; i++)
        {
            data_sync_vec_.emplace_back();
            data_sync_vec_.back().resize(scan_batch_size);
#ifdef RANGE_PARTITION_ENABLED
            if (!skip_archived_key_)
#endif
            {
                archive_vec_.emplace_back();
                archive_vec_.back().reserve(scan_batch_size);
                mv_base_idx_vec_.emplace_back();
                mv_base_idx_vec_.back().reserve(scan_batch_size);
            }
#ifdef RANGE_PARTITION_ENABLED
            pause_pos_.emplace_back(TxKey(), false);
#else
            pause_pos_.emplace_back(nullptr, false);
#endif
            accumulated_scan_cnt_.emplace_back(0);
        }

#ifdef RANGE_PARTITION_ENABLED
        if (export_base_table_rec_if_need)
        {
            slice_ids_.resize(core_cnt_);
        }
#endif
    }

    bool ValidTermCheck()
    {
        int64_t cc_ng_term = Sharder::Instance().LeaderTerm(node_group_id_);
        if (node_group_term_ < 0)
        {
            node_group_term_ = cc_ng_term;
        }

        if (cc_ng_term < 0 || cc_ng_term != node_group_term_)
        {
            return false;
        }
        else
        {
            return true;
        }
    }

    // DataSyncScanCc is always stack object and won't be reused, worse, it
    // might be destructed before Execute returns, so always return false as
    // callershould never access this object after Execute returns
    bool Execute(CcShard &ccs) override
    {
        if (!ValidTermCheck())
        {
            SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            return false;
        }
        scan_count_++;
        CcMap *ccm = ccs.GetCcm(*table_name_, node_group_id_);
        if (ccm == nullptr)
        {
            assert(!table_name_->IsMeta());
            const CatalogEntry *catalog_entry = ccs.InitCcm(
                *table_name_, node_group_id_, node_group_term_, this);
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
                    SetError(CcErrorCode::REQUESTED_TABLE_NOT_EXISTS);
                    return false;
                }
                ccm = ccs.GetCcm(*table_name_, node_group_id_);
            }
        }
        assert(ccm != nullptr);
        ccm->Execute(*this);
        // return false since DataSyncScanCc is not re-used and does not need to
        // call CcRequestBase::Free
        return false;
    }

    bool IsDrained(size_t core_idx) const
    {
        return pause_pos_[core_idx].second;
    }

#ifdef RANGE_PARTITION_ENABLED
    std::pair<TxKey, bool> &PausePos(size_t core_idx)
#else
    std::pair<LruEntry *, bool> &PausePos(size_t core_idx)
#endif
    {
        return pause_pos_[core_idx];
    }

    void Wait()
    {
        std::unique_lock<std::mutex> lk(mux_);
        cv_.wait(lk, [this] { return unfinished_cnt_ == 0; });
    }

    void Reset()
    {
        std::lock_guard<std::mutex> lk(mux_);
        unfinished_cnt_ = core_cnt_;
        for (size_t i = 0; i < core_cnt_; i++)
        {
#ifdef RANGE_PARTITION_ENABLED
            if (!skip_archived_key_)
#endif
            {
                archive_vec_.at(i).clear();
                mv_base_idx_vec_.at(i).clear();
            }
            accumulated_scan_cnt_.at(i) = 0;
        }
        err_ = CcErrorCode::NO_ERROR;
        force_flush_ = false;
    }

    void SetError(CcErrorCode err)
    {
        std::lock_guard<std::mutex> lk(mux_);
        err_ = err;
        --unfinished_cnt_;
        if (unfinished_cnt_ == 0)
        {
#ifdef RANGE_PARTITION_ENABLED
            if (export_base_table_rec_if_need_)
            {
                UnpinSlices();
            }
#endif
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
#ifdef RANGE_PARTITION_ENABLED
            if (export_base_table_rec_if_need_)
            {
                UnpinSlices();
            }
#endif
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

    void SetFinish(size_t core_id)
    {
        std::unique_lock<std::mutex> lk(mux_);
        --unfinished_cnt_;
        if (unfinished_cnt_ == 0)
        {
#ifdef RANGE_PARTITION_ENABLED
            if (err_ != CcErrorCode::NO_ERROR && export_base_table_rec_if_need_)
            {
                UnpinSlices();
            }
#endif
            cv_.notify_one();
        }
    }

    uint32_t NodeGroupId()
    {
        return node_group_id_;
    }

    std::vector<FlushRecord> &DataSyncVec(uint16_t core_id)
    {
        return data_sync_vec_[core_id];
    }

    std::unique_ptr<std::vector<FlushRecord>> MoveOutDataSyncVec(
        uint16_t core_id)
    {
        auto moved = std::make_unique<std::vector<FlushRecord>>(
            std::move(data_sync_vec_[core_id]));
        data_sync_vec_[core_id] = std::vector<FlushRecord>();
        data_sync_vec_[core_id].resize(scan_batch_size_);
        return moved;
    }

    std::vector<FlushRecord> &ArchiveVec(uint16_t core_id)
    {
        return archive_vec_[core_id];
    }

    std::unique_ptr<std::vector<FlushRecord>> MoveOutArchiveVec(
        uint16_t core_id)
    {
        auto moved = std::make_unique<std::vector<FlushRecord>>(
            std::move(archive_vec_[core_id]));
        archive_vec_[core_id] = std::vector<FlushRecord>();
        archive_vec_[core_id].resize(scan_batch_size_);
        return moved;
    }

    std::vector<size_t> &MoveBaseIdxVec(uint16_t core_id)
    {
        return mv_base_idx_vec_[core_id];
    }

    int64_t NodeGroupTerm() const
    {
        return node_group_term_;
    }

    void UnpinSlices()
    {
#ifdef RANGE_PARTITION_ENABLED
        for (size_t i = 0; i < slice_ids_.size(); ++i)
        {
            if (slice_ids_[i].Slice() != nullptr)
            {
                slice_ids_[i].Unpin();
                slice_ids_[i].Reset();
            }
        }
#endif
    }

    void SetNotTruncateLog()
    {
        std::lock_guard<std::mutex> lk(mux_);
        err_ = CcErrorCode::LOG_NOT_TRUNCATABLE;
    }

    std::vector<size_t> accumulated_scan_cnt_;
    // force DataSync task flush out this batch of scaned data whatever other
    // criteria exists, e.g. scan mem is full
    bool force_flush_{false};

    size_t scan_count_{0};

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
    // Position that we left off during last round of ckpt scan.
    // pause_pos_.first is the key that we stopped at (has not been scanned
    // though), bool is if this core has finished scanning all keys already.
#ifdef RANGE_PARTITION_ENABLED
    std::vector<std::pair<TxKey, bool>> pause_pos_;
#else
    std::vector<std::pair<LruEntry *, bool>> pause_pos_;
#endif
    size_t scan_batch_size_;

    CcErrorCode err_{CcErrorCode::NO_ERROR};
    uint32_t unfinished_cnt_;
    std::mutex mux_;
    std::condition_variable cv_;
    // True means If no larger version exists, we need to export the data which
    // commit_ts same as ckpt_ts.
    bool include_persisted_data_{false};

#ifdef RANGE_PARTITION_ENABLED
    // True means we need to export the data in memory and in kv to ckpt vec.
    // Note: This is only used in range partition.
    bool export_base_table_rec_if_need_{false};
    std::vector<RangeSliceId> slice_ids_;

    // This is used for scan during add index txm.
    bool skip_archived_key_{false};

#else
    bool only_scan_one_core_{false};
    std::function<bool(size_t hash_code)> filter_lambda_;
#endif
    // keep schema vesion after acquire read lock on catalog, to prevent the
    // concurrency issue with Truncate Table, detail ref to tx issue #1130
    // If schema_version_ is 0, the check will be bypassed, since this data sync
    // scan is part of range split which block the schema change
    // TODO(xxx) general solution for #1130
    const uint64_t schema_version_{0};

    template <typename KeyT, typename ValueT>
    friend class TemplateCcMap;

    friend std::ostream &operator<<(std::ostream &outs,
                                    txservice::DataSyncScanCc *r);
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
        bthread::Mutex &mux,
        std::atomic<fault::ReplayService::WaitingStatus> &status,
        std::atomic<size_t> &on_fly_cnt,
        bool &recovery_error,
        std::shared_ptr<std::atomic_uint32_t> range_split_started = nullptr,
        std::unordered_set<TableName> *range_splitting = nullptr,
        uint16_t first_core = 0)
    {
        table_name_str_ = table_name_view;
        table_name_holder_ = TableName(table_name_str_, table_type);
        TemplatedCcRequest<ReplayLogCc, Void>::Reset(
            &table_name_holder_, &result_, ng_id, txn, -1);
        log_blob_str_ = blob;
        commit_ts_ = commit_ts;
        result_.Reset();
        external_mux_ = &mux;
        external_status_ = &status;
        external_on_fly_cnt_ = &on_fly_cnt;
        recovery_error_ = &recovery_error;
        next_core_ = UINT16_MAX;
        first_core_ = first_core;
        range_split_started_ = range_split_started;
        range_splitting_ = range_splitting;
        local_on_fly_cnt_ = nullptr;
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
        if (local_on_fly_cnt_ == nullptr ||
            local_on_fly_cnt_->fetch_sub(1, std::memory_order_relaxed) == 1)
        {
            // local_on_fly_cnt_ is nullptr means it's not a data log.
            // Otherwise, it's a data log generated by ParseDataLogCc,
            // only one of them can reach here.
            delete local_on_fly_cnt_;
            external_on_fly_cnt_->fetch_sub(1, std::memory_order_relaxed);
        }
    }

    void AbortCcRequest(CcErrorCode err_code) override
    {
        assert(err_code != CcErrorCode::NO_ERROR);

        {
            BAIDU_SCOPED_LOCK(*external_mux_);
            *recovery_error_ = true;
        }
        if (local_on_fly_cnt_ == nullptr ||
            local_on_fly_cnt_->fetch_sub(1, std::memory_order_relaxed) == 1)
        {
            // local_on_fly_cnt_ is nullptr means it's not a data log.
            // Otherwise, it's a data log generated by ParseDataLogCc,
            // only one of them can reach here.
            delete local_on_fly_cnt_;
            external_on_fly_cnt_->fetch_sub(1, std::memory_order_relaxed);
        }
    }

    const std::string_view LogContentView() const
    {
        return log_blob_str_;
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

    void SetLocalOnFlyCnt(std::atomic_uint32_t *local_on_fly_cnt)
    {
        local_on_fly_cnt_ = local_on_fly_cnt;
    }

private:
    TableName table_name_holder_{
        "",
        0,
        TableType::Primary};  //  not string owner, sv -> protobuf message.
    std::string table_name_str_;
    std::string log_blob_str_;
    // Temporarily store the currently parsed offset when fetch record from
    // kvstore asynchronously.
    size_t offset_{0};
    uint64_t commit_ts_;
    CcHandlerResult<Void> result_{nullptr};
    bthread::Mutex *external_mux_;
    std::atomic<fault::ReplayService::WaitingStatus> *external_status_;
    std::atomic<uint64_t> *external_on_fly_cnt_;
    // Reduces race condition by operating on ParseDataLogCc's local
    // on_fly_cnt. Only last ReplayLogCc decreases global on_fly_cnt.
    std::atomic_uint32_t *local_on_fly_cnt_{nullptr};
    bool *recovery_error_;
    uint16_t first_core_;
    uint16_t next_core_{UINT16_MAX};
    const struct TableSchema *table_schema_{nullptr};
    // Reserved for range split log replay
    std::shared_ptr<std::atomic_uint32_t> range_split_started_{nullptr};

    // Reserved for schema op log replay
    const std::unordered_set<TableName> *range_splitting_;

    friend std::ostream &operator<<(std::ostream &outs,
                                    txservice::ReplayLogCc *r);
};

struct ParseDataLogCc : public CcRequestBase
{
public:
    ParseDataLogCc() = default;

    void Reset(const std::string &log_records,
               uint32_t cc_ng_id,
               bthread::Mutex &mux,
               std::atomic<fault::ReplayService::WaitingStatus> &status,
               std::atomic<uint64_t> &on_fly_cnt,
               bool &recovery_error)
    {
        log_records_ = log_records;
        cc_ng_id_ = cc_ng_id;
        mux_ = &mux;
        status_ = &status;
        on_fly_cnt_ = &on_fly_cnt;
        recovery_error_ = &recovery_error;
    }

    bool Execute(CcShard &ccs) override
    {
        size_t offset = 0;
        // core of first key in log
        int dest_core = 0;
        std::vector<ReplayLogCc *> replay_cc_list;
        replay_cc_list.reserve(160);
        while (offset < log_records_.size())
        {
            // 8-byte for commit_ts
            uint64_t commit_ts = *reinterpret_cast<const uint64_t *>(
                log_records_.data() + offset);
            offset += sizeof(uint64_t);
            // 4-byte for log_blob length
            uint32_t blob_length = *reinterpret_cast<const uint32_t *>(
                log_records_.data() + offset);
            offset += sizeof(uint32_t);

            std::string_view blob(log_records_.data() + offset, blob_length);
            offset += blob_length;

            // parse log_blob
            size_t blob_offset = 0;
            while (blob_offset < blob.size())
            {
                // 1-byte integer for the length of the table name
                uint8_t table_name_len = *reinterpret_cast<const uint8_t *>(
                    blob.data() + blob_offset);
                blob_offset += sizeof(uint8_t);

                // Table name string
                std::string_view table_name_view(blob.data() + blob_offset,
                                                 table_name_len);
                blob_offset += table_name_len;
#ifdef ON_KEY_OBJECT
                TableType table_type = TableType::Primary;
                // 4-byte integer for the length of the serialized object keys
                // and commands of this tx.
                uint32_t kv_len = *reinterpret_cast<const uint32_t *>(
                    blob.data() + blob_offset);
                blob_offset += sizeof(uint32_t);
#else

                // 1-byte integer for the type of table
                uint8_t table_type_number = *reinterpret_cast<const uint8_t *>(
                    blob.data() + blob_offset);
                TableType table_type;
                switch (table_type_number)
                {
                case 0:
                    table_type = TableType::Primary;
                    break;
                case 1:
                    table_type = TableType::Secondary;
                    break;
                case 2:
                    table_type = TableType::UniqueSecondary;
                    break;
                default:
                    // Should not have meta table in data log.
                    assert(false);
                    break;
                }
                blob_offset += sizeof(uint8_t);

                // 4-byte integer for the length of the serialized
                // records from the table
                uint32_t kv_len = *reinterpret_cast<const uint32_t *>(
                    blob.data() + blob_offset);
                blob_offset += sizeof(uint32_t);
#endif
                size_t hash = ccs.GetCatalogFactory()->KeyHash(
                    blob.data(), blob_offset, nullptr);
                dest_core = hash ? (hash & 0x3FF) % ccs.core_cnt_
                                 : (dest_core + 1) % ccs.core_cnt_;
                ReplayLogCc *cc_req = replay_cc_pool_.NextRequest();
                replay_cc_list.push_back(cc_req);
                cc_req->Reset(
                    cc_ng_id_,
                    table_name_view,
                    table_type,
                    std::string_view(blob.data() + blob_offset, kv_len),
                    commit_ts,
                    0,
                    *mux_,
                    *status_,
                    *on_fly_cnt_,
                    *recovery_error_,
                    nullptr,
                    nullptr,
                    dest_core);

                blob_offset += kv_len;
            }
        }
        if (replay_cc_list.empty())
        {
            on_fly_cnt_->fetch_sub(1, std::memory_order_relaxed);
            return true;
        }
        std::atomic_uint32_t *local_on_fly_cnt =
            new std::atomic_uint32_t(replay_cc_list.size());

        for (auto cc_req : replay_cc_list)
        {
            cc_req->SetLocalOnFlyCnt(local_on_fly_cnt);
            ccs.Enqueue(ccs.core_id_, cc_req->FirstCore(), cc_req);
        }
        return true;
    }

private:
    std::string log_records_;
    uint32_t cc_ng_id_;
    bthread::Mutex *mux_;
    std::atomic<fault::ReplayService::WaitingStatus> *status_;
    std::atomic<uint64_t> *on_fly_cnt_;
    bool *recovery_error_;
    uint16_t next_core_ = 0;
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
               int64_t tx_term,
               CcHandlerResult<Void> *res)
    {
        TemplatedCcRequest<BroadcastStatisticsCc, Void>::Reset(
            &catalog_ccm_name, res, ng_id, tx_number, tx_term);

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
        virtual uint32_t Size() const = 0;
    };

    template <uint32_t CapacityN, typename KeyT, typename CopyKey>
    struct SamplePool : public SamplePoolBase
    {
    public:
        // Template method is not allowd to be virtual.
        void Insert(const KeyT &key)
        {
            random_pairing_.Insert(key, ++counter_);
        }

        // Template method is not allowd to be virtual.
        const std::vector<KeyT> &SampleKeys() const
        {
            return random_pairing_.SampleKeys();
        }

        uint32_t Size() const override
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
               int64_t tx_term,
               CcHandlerResult<Void> *res)
    {
        TemplatedCcRequest<AnalyzeTableAllCc, Void>::Reset(
            table_name, res, node_group_id, tx_number, tx_term);

        Clear();
    }

    void ResetCcm()
    {
        ccm_ = nullptr;
    }

    bool PinStoreRanges(CcShard *shard)
    {
        bool all_pinned = true;

        TableName range_table_name(table_name_->StringView(),
                                   TableType::RangePartition);
        std::map<TxKey, TableRangeEntry::uptr> *range_map =
            shard->GetTableRangesForATable(range_table_name, node_group_id_);

        for (auto &[range_key, range_entry] : *range_map)
        {
            uint32_t partition_id = range_entry->GetRangeInfo()->PartitionId();
            uint32_t bucket_owner =
                shard->GetRangeOwner(partition_id, node_group_id_)
                    ->BucketOwner();
            if (bucket_owner == node_group_id_)
            {
                bool pinned = pinned_store_ranges_.count(range_entry.get()) > 0;
                if (!pinned)
                {
                    // Pin store range so that it cannot be kicked out
                    // during analyze.
                    const StoreRange *store_range =
                        range_entry->PinStoreRange();
                    if (store_range)
                    {
                        pinned_store_ranges_.insert(range_entry.get());
                    }
                    else
                    {
                        range_entry->FetchRangeSlices(range_table_name,
                                                      this,
                                                      node_group_id_,
                                                      ng_term_,
                                                      shard);
                        all_pinned = false;
                        break;
                    }
                }
            }
        }

        return all_pinned;
    }

    void UnPinStoreRanges()
    {
        for (TableRangeEntry *range_entry : pinned_store_ranges_)
        {
            range_entry->UnPinStoreRange();
        }
    }

    const std::unordered_set<TableRangeEntry *> PinnedStoreRanges() const
    {
        return pinned_store_ranges_;
    }

private:
    void Clear()
    {
        key_sample_pool_.reset(nullptr);
        store_slice_sample_pool_.reset(nullptr);
        built_slice_sample_pool_ = false;
        pinned_store_ranges_.clear();
        pin_slice_idx_ = 0;
        pinned_slice_id_.Reset();
        continue_key_ = TxKey();
        visit_keys_ = 0;
        visit_slices_ = 0;
    }

public:
    constexpr static uint32_t sample_pool_capacity_{1024};
    std::unique_ptr<SamplePoolBase> key_sample_pool_{nullptr};
    std::unique_ptr<SamplePoolBase> store_slice_sample_pool_{nullptr};

    bool built_slice_sample_pool_{false};
    std::unordered_set<TableRangeEntry *> pinned_store_ranges_;

    size_t pin_slice_idx_{0};
    RangeSliceId pinned_slice_id_;

    TxKey continue_key_;

    uint32_t visit_keys_{0};
    uint32_t visit_slices_{0};
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
        txservice::FaultInject::Instance().InjectFault(*fault_name_,
                                                       *fault_paras_);
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
               int64_t tx_term,
               CcHandlerResult<bool> *res)
    {
        uint32_t ng_id = Sharder::Instance().ShardToCcNodeGroup(key_shard_code);
        TemplatedCcRequest<CleanCcEntryForTestCc, bool>::Reset(
            tn, res, ng_id, tx_number, tx_term);
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
               int64_t tx_term,
               CcHandlerResult<bool> *res)
    {
        uint32_t ng_id = Sharder::Instance().ShardToCcNodeGroup(key_shard_code);
        TemplatedCcRequest<CleanCcEntryForTestCc, bool>::Reset(
            tn, res, ng_id, tx_number, tx_term);
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
                     uint16_t core_cnt,
                     CcHandlerResult<Void> *res,
                     CleanType clean_type,
                     const TxKey *start_key = nullptr,
                     const TxKey *end_key = nullptr,
                     std::vector<uint16_t> *bucket_ids = nullptr,
                     uint64_t clean_ts = 0,
                     int32_t range_id = INT32_MAX,
                     uint64_t range_version = UINT64_MAX)
        : clean_type_(clean_type),
          bucket_ids_(bucket_ids),
          clean_ts_(clean_ts),
          range_id_(range_id),
          range_version_(range_version),
          start_key_(start_key),
          end_key_(end_key),
          unfinished_cnt_(core_cnt),
          err_code_(CcErrorCode::NO_ERROR)
    {
        table_name_ = &table_name;
        node_group_id_ = ng_id;
        res_ = res;
        for (uint16_t i = 0; i < core_cnt; ++i)
        {
            resume_key_.emplace_back(TxKey());
        }
    }

    KickoutCcEntryCc(const KickoutCcEntryCc &rhs) = delete;
    KickoutCcEntryCc(KickoutCcEntryCc &&rhs) = delete;

    void Reset(const TableName &table_name,
               const uint32_t ng_id,
               uint16_t core_cnt,
               CcHandlerResult<Void> *res,
               CleanType clean_type,
               const TxKey *start_key = nullptr,
               const TxKey *end_key = nullptr,
               std::vector<uint16_t> *bucket_ids = nullptr,
               uint64_t clean_ts = 0,
               int32_t range_id = INT32_MAX,
               uint64_t range_version = UINT64_MAX)
    {
        // Reset struct members with passed in args
        table_name_ = &table_name;
        node_group_id_ = ng_id;
        res_ = res;
        unfinished_cnt_ = core_cnt;
        err_code_ = CcErrorCode::NO_ERROR;
        bucket_ids_ = bucket_ids;
        clean_type_ = clean_type;
        clean_ts_ = clean_ts;
        range_id_ = range_id;
        range_version_ = range_version;
        resume_key_.clear();
        start_key_ = start_key ? start_key->KeyPtr() : nullptr;
        end_key_ = end_key ? end_key->KeyPtr() : nullptr;
        start_key_str_ = nullptr;
        end_key_str_ = nullptr;
        resume_key_.resize(core_cnt);
    }

    void Reset(const TableName &table_name,
               const uint32_t ng_id,
               uint16_t core_cnt,
               CcHandlerResult<Void> *res,
               CleanType clean_type,
               const std::string *start_key = nullptr,
               const std::string *end_key = nullptr,
               std::vector<uint16_t> *bucket_ids = nullptr,
               uint64_t clean_ts = 0,
               int32_t range_id = INT32_MAX,
               uint64_t range_version = UINT64_MAX)
    {
        start_key_str_ = start_key;
        end_key_str_ = end_key;
        start_key_ = nullptr;
        end_key_ = nullptr;
        table_name_ = &table_name;
        node_group_id_ = ng_id;
        res_ = res;
        unfinished_cnt_ = core_cnt;
        err_code_ = CcErrorCode::NO_ERROR;
        bucket_ids_ = bucket_ids;
        clean_type_ = clean_type;
        clean_ts_ = clean_ts;
        range_id_ = range_id;
        range_version_ = range_version;
        resume_key_.clear();
        resume_key_.resize(core_cnt);
    }

    bool Execute(CcShard &ccs) override
    {
        int64_t ng_term = Sharder::Instance().LeaderTerm(node_group_id_);
        if (ng_term < 0)
        {
            return SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
        }

        if (clean_type_ == CleanType::CleanCcm)
        {
            if (!CleanCcMap(ccs))
            {
                // Current ccmap has more page
                // Yield
                ccs.Enqueue(ccs.LocalCoreId(), this);
                return false;
            }

            return SetFinish();
        }

        CcMap *ccm = ccs.GetCcm(*table_name_, node_group_id_);

        if (ccm != nullptr)
        {
            // DataMigration adds a bucket write lock, so we can ensure that
            // range splits will not occur. There is no read lock on the
            // catalog, and there is a situation where a table is deleted and
            // then created. So the key range may be changed.
            if (clean_type_ == CleanType::CleanRangeDataForMigration)
            {
                // The version of the range has changed, which means that the
                // key range of the range has changed. `start_key_` and
                // `end_key_` no longer represent the correct key range.
                if (!ccs.CheckRangeVersion(*table_name_,
                                           node_group_id_,
                                           range_id_,
                                           range_version_))
                {
                    return SetFinish();
                }
            }

            return ccm->Execute(*this);
        }
        else
        {
            // If no ccmap for this table, nothing to kickout, notify finish
            // directly.
            return SetFinish();
        }
    }

    void SetUnfinishedCoreCnt(size_t core_cnt)
    {
        unfinished_cnt_ = core_cnt;
        size_t resume_key_vec_size = resume_key_.size();
        if (resume_key_vec_size < core_cnt)
        {
            for (size_t idx = resume_key_vec_size; idx < core_cnt; ++idx)
            {
                resume_key_.emplace_back(TxKey());
            }
        }
    }

    const TxKey *ResumeKey(uint16_t core_id) const
    {
        return &resume_key_.at(core_id);
    }

    void SetResumeKey(TxKey key, uint16_t core_id)
    {
        assert(key.IsOwner());
        resume_key_.at(core_id) = std::move(key);
    }

    const void *StartKey() const
    {
        return start_key_;
    }

    const void *EndKey() const
    {
        return end_key_;
    }

    void SetDecodedStartKey(TxKey key)
    {
        assert(key.IsOwner());
        decoded_start_key_ = std::move(key);
        start_key_ = decoded_start_key_.KeyPtr();
    }

    void SetDecodedEndKey(TxKey key)
    {
        assert(key.IsOwner());
        decoded_end_key_ = std::move(key);
        end_key_ = decoded_end_key_.KeyPtr();
    }

    const std::string *StartKeyStr() const
    {
        return start_key_str_;
    }

    const std::string *EndKeyStr() const
    {
        return end_key_str_;
    }

    void SetStartKey(const void *key)
    {
        start_key_ = key;
    }

    void SetEndKey(const void *key)
    {
        end_key_ = key;
    }

    bool SetFinish()
    {
        if (unfinished_cnt_.fetch_sub(1, std::memory_order_acq_rel) == 1)
        {
            if (res_)
            {
                CcErrorCode err_code =
                    err_code_.load(std::memory_order_relaxed);
                if (err_code == CcErrorCode::NO_ERROR)
                {
                    res_->SetFinished();
                }
                else
                {
                    res_->SetError(err_code);
                }
            }
            return true;
        }
        return false;
    }

    bool SetError(CcErrorCode err_code)
    {
        assert(err_code != CcErrorCode::NO_ERROR);
        err_code_.store(err_code, std::memory_order_release);
        return SetFinish();
    }

    template <typename KeyT>
    bool IsCleanTarget(const KeyT &key, const LruEntry *entry) const
    {
        switch (clean_type_)
        {
        case CleanType::CleanRangeData:
        case CleanType::CleanRangeDataForMigration:
        {
            assert(start_key_ && end_key_);
            const KeyT *start = static_cast<const KeyT *>(start_key_);
            const KeyT *end = static_cast<const KeyT *>(end_key_);

            if (*start < key || *start == key)
            {
                return key < *end;
            }

            return false;
        }
        case CleanType::CleanBucketData:
        {
            assert(bucket_ids_ && !bucket_ids_->empty());
            uint16_t bucket_id = key.Hash() & 0x3FFF;
            for (uint16_t id : *bucket_ids_)
            {
                if (bucket_id == id)
                {
                    return true;
                }
            }
            return false;
        }
        case CleanType::CleanForAlterTable:
        {
            return entry->CommitTs() <= clean_ts_ && entry->CommitTs() > 1;
        }
        default:
            assert(false);
            return false;
        }
    }

    bool CanBeCleaned(const LruEntry *entry) const
    {
        switch (clean_type_)
        {
        case CleanType::CleanRangeData:
        case CleanType::CleanRangeDataForMigration:
        case CleanType::CleanBucketData:
            // All data in the target range/bucket can be cleaned.
            return true;
        case CleanType::CleanForAlterTable:
            return entry->IsFree() && !entry->GetBeingCkpt();
        default:
            assert(false && "Unknown type");
            return false;
        }
    }

    bool CleanCcMap(CcShard &ccs)
    {
        assert(clean_type_ == CleanType::CleanCcm);

        return ccs.CleanCcmPages(*table_name_, node_group_id_, clean_ts_);
    }

    CleanType GetCleanType() const
    {
        return clean_type_;
    }

private:
    CleanType clean_type_;
    // Target buckets to be cleaned if clean type is CleanBucketData.
    std::vector<uint16_t> *bucket_ids_{nullptr};

    // 1. CleanForAlterTable: kickout all cce with commit ts <= clean_ts_
    // 2. CleanForTruncateTable: the commit ts of UpsertTableOp. We truncate ccm
    // only if catalog_entry->DirtyVersion() == clean_ts_.
    uint64_t clean_ts_{0};
    // Only used by migration
    int32_t range_id_{INT32_MAX};
    uint64_t range_version_{UINT64_MAX};
    const std::string *start_key_str_{nullptr};
    const std::string *end_key_str_{nullptr};
    const void *start_key_{nullptr};
    const void *end_key_{nullptr};
    TxKey decoded_start_key_{};
    TxKey decoded_end_key_{};
    std::vector<TxKey> resume_key_;
    std::atomic_uint16_t unfinished_cnt_{0};
    std::atomic<CcErrorCode> err_code_{CcErrorCode::NO_ERROR};
};

struct ReleaseDataSyncScanHeapCc : public CcRequestBase
{
public:
    static const size_t VEC_ERASE_BATCH_SIZE = 1000;

    explicit ReleaseDataSyncScanHeapCc(
        size_t core_cnt,
        std::vector<std::unique_ptr<std::vector<FlushRecord>>>
            data_sync_vec_per_core,
        std::vector<std::unique_ptr<std::vector<FlushRecord>>>
            archive_vec_per_core)
        : pending_shard_(core_cnt),
          data_sync_vec_per_core_(std::move(data_sync_vec_per_core)),
          archive_vec_per_core_(std::move(archive_vec_per_core))
    {
    }

    bool Execute(CcShard &ccs) override
    {
        // Release the data sync vec in same thread that allocated it, the
        // memory freed can be directly refelct to the mi stats allocated and
        // committed, otherwise the the stats updates will delayed to next
        // allocation
        if (data_sync_vec_per_core_.size() != 0)
        {
#ifdef RANGE_PARTITION_ENABLED
            auto &data_sync_vec = data_sync_vec_per_core_[ccs.core_id_];
#else
            auto &data_sync_vec = data_sync_vec_per_core_[0];
#endif
            if (data_sync_vec != nullptr)
            {
                // to avoid large jitter when releasing big memory chunck,
                // we release memory incremently in batch
                size_t vec_size = data_sync_vec->size();
                if (vec_size != 0)
                {
                    CcShardHeap *scan_heap = ccs.GetShardDataSyncScanHeap();
                    mi_heap_t *prev_heap = scan_heap->SetAsDefaultHeap();
                    if (vec_size > VEC_ERASE_BATCH_SIZE)
                    {
                        data_sync_vec->resize(vec_size - VEC_ERASE_BATCH_SIZE);
                    }
                    else
                    {
                        data_sync_vec->resize(0);
                    }
                    data_sync_vec->shrink_to_fit();
                    mi_heap_set_default(prev_heap);

                    if (data_sync_vec->size() != 0)
                    {
                        ccs.Enqueue(this);
                        return false;
                    }
                }
            }
        }

        if (archive_vec_per_core_.size() != 0)
        {
#ifdef RANGE_PARTITION_ENABLED
            auto &archive_vec = archive_vec_per_core_[ccs.core_id_];
#else
            auto &archive_vec = archive_vec_per_core_[0];
#endif
            if (archive_vec != nullptr)
            {
                size_t vec_size = archive_vec->size();
                if (vec_size != 0)
                {
                    CcShardHeap *scan_heap = ccs.GetShardDataSyncScanHeap();
                    mi_heap_t *prev_heap = scan_heap->SetAsDefaultHeap();
                    if (vec_size > VEC_ERASE_BATCH_SIZE)
                    {
                        archive_vec->resize(vec_size - VEC_ERASE_BATCH_SIZE);
                    }
                    else
                    {
                        archive_vec->resize(0);
                    }
                    mi_heap_set_default(prev_heap);

                    if (archive_vec->size() != 0)
                    {
                        ccs.Enqueue(this);
                        return false;
                    }
                }
            }
        }

        {
            std::lock_guard<std::mutex> lk(mux_);
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
    std::vector<std::unique_ptr<std::vector<FlushRecord>>>
        data_sync_vec_per_core_;
    std::vector<std::unique_ptr<std::vector<FlushRecord>>>
        archive_vec_per_core_;
};

struct PostFlushDataCc : public CcRequestBase
{
public:
    explicit PostFlushDataCc(size_t core_cnt) : pending_shard_(core_cnt)
    {
    }

    bool Execute(CcShard &ccs) override
    {
        ccs.ResetCleanStart();
        ccs.DequeueWaitList();

        {
            std::lock_guard<std::mutex> lk(mux_);
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
    std::vector<std::unique_ptr<std::vector<FlushRecord>>>
        data_sync_vec_per_core_;
    std::vector<std::unique_ptr<std::vector<FlushRecord>>>
        archive_vec_per_core_;
};

struct UpdateKeyCacheCc : public CcRequestBase
{
    static const size_t BatchSize = 256;
    UpdateKeyCacheCc(const TableName &tbl_name,
                     int64_t ng_term,
                     uint32_t ng_id,
                     std::vector<std::vector<FlushRecord *>> &&key_vecs,
                     StoreRange *range)
        : table_name_(tbl_name),
          ng_term_(ng_term),
          node_group_id_(ng_id),
          key_vecs_(std::move(key_vecs)),
          store_range_(range),
          unfinished_core_(key_vecs_.size())
    {
        assert(table_name_.Type() == TableType::Primary);
        pause_idx_.resize(key_vecs_.size(), 0);
    }

    bool Execute(CcShard &ccs) override
    {
        int64_t ng_term = Sharder::Instance().LeaderTerm(node_group_id_);
        if (ng_term < 0)
        {
            SetFinish();
            return false;
        }

        if (key_vecs_[ccs.core_id_].empty())
        {
            SetFinish();
            return false;
        }

        CcMap *ccm = ccs.GetCcm(table_name_, node_group_id_);

        if (ccm == nullptr)
        {
            // Fetch/Get Catalog is based on base table name, but Get
            // ccmap is based on the real table name, for example, index
            // should get the corresponding sk_ccmap.
            assert(!table_name_.IsMeta());
            const CatalogEntry *catalog_entry =
                ccs.InitCcm(table_name_, node_group_id_, ng_term_, this);
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
                    // dropped. No need to update the key cache.
                    SetFinish();
                    return false;
                }

                ccm = ccs.GetCcm(table_name_, node_group_id_);
            }
        }
        ccm->Execute(*this);

        return false;
    }

    void SetFinish()
    {
        std::unique_lock<std::mutex> lk(mux_);
        if (--unfinished_core_ == 0)
        {
            cv_.notify_one();
        }
    }

    void Wait()
    {
        std::unique_lock<std::mutex> lk(mux_);
        cv_.wait(lk, [this] { return unfinished_core_ == 0; });
    }

    const TableName &table_name_;
    int64_t ng_term_;
    uint32_t node_group_id_;
    std::vector<std::vector<FlushRecord *>> key_vecs_;
    StoreRange *store_range_;
    std::vector<size_t> pause_idx_;
    std::mutex mux_;
    std::condition_variable cv_;
    size_t unfinished_core_;
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
        TxCommand *cmd_{nullptr};
        // To say if ApplyCC owner this command, if TRUE, it should release it
        // manually, if FALSE, does not need to release here.
        bool is_owner_{false};
    };

public:
    explicit ApplyCc(bool is_local = true)
    {
        is_local_ = is_local;
        if (is_local_)
        {
            local_input_.key_ = nullptr;
            local_input_.cmd_ = nullptr;
        }
        else
        {
            remote_input_.key_str_ = nullptr;
            remote_input_.cmd_str_ = nullptr;
            remote_input_.cmd_ = nullptr;
            remote_input_.is_owner_ = false;
        }
    }

    ~ApplyCc() override
    {
        if (!is_local_)
        {
            if (remote_input_.is_owner_)
            {
                delete remote_input_.cmd_;
            }
            remote_input_.cmd_ = nullptr;
        }
    };

    void Free() override
    {
        if (!is_local_)
        {
            //  delete cmd_ after ApplyCc finish for reuse
            if (remote_input_.is_owner_)
            {
                delete remote_input_.cmd_;
            }
            remote_input_.cmd_ = nullptr;
            remote_input_.is_owner_ = false;
        }
        in_use_.store(false, std::memory_order_release);
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
               bool commit,
               bool skip_kv)
    {
        TemplatedCcRequest<ApplyCc, ObjectCommandResult>::Reset(
            table_name,
            res,
            Sharder::Instance().ShardToCcNodeGroup(key_shard_code),
            txn,
            tx_term,
            proto,
            iso_level);

        if (!is_local_)
        {
            if (remote_input_.is_owner_)
            {
                delete remote_input_.cmd_;
            }
            remote_input_.cmd_ = nullptr;
        }

        is_local_ = true;
        local_input_.key_ = key;
        local_input_.cmd_ = cmd;

        key_shard_code_ = key_shard_code;
        tx_ts_ = tx_ts;
        cce_ptr_ = nullptr;
        apply_and_commit_ = commit;
        skip_kv_ = skip_kv;
        block_type_ = ApplyBlockType::NoBlocking;
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
               bool commit,
               bool skip_kv)
    {
        TemplatedCcRequest<ApplyCc, ObjectCommandResult>::Reset(
            table_name,
            res,
            Sharder::Instance().ShardToCcNodeGroup(key_shard_code),
            txn,
            tx_term,
            proto,
            iso_level);

        if (!is_local_)
        {
            if (remote_input_.is_owner_)
            {
                delete remote_input_.cmd_;
            }
            remote_input_.cmd_ = nullptr;
        }

        is_local_ = false;
        remote_input_.key_str_ = key_str;
        remote_input_.cmd_str_ = cmd_str;
        remote_input_.cmd_ = nullptr;
        remote_input_.is_owner_ = false;

        key_shard_code_ = key_shard_code;
        tx_ts_ = tx_ts;
        cce_ptr_ = nullptr;
        block_type_ = ApplyBlockType::NoBlocking;
        apply_and_commit_ = commit;
        skip_kv_ = skip_kv;
    }

    bool IsLocal() const
    {
        return is_local_;
    }

    bool IsRemote() const
    {
        return !is_local_;
    }

    bool IsDelete() const
    {
        if (is_local_)
        {
            assert(local_input_.cmd_ != nullptr);
            return local_input_.cmd_ == nullptr ||
                   local_input_.cmd_->IsDelete();
        }
        assert(remote_input_.cmd_ != nullptr);
        return remote_input_.cmd_ == nullptr || remote_input_.cmd_->IsDelete();
    }

    bool IsReadOnly() const
    {
        if (is_local_)
        {
            return local_input_.cmd_ == nullptr ||
                   local_input_.cmd_->IsReadOnly();
        }
        return remote_input_.cmd_ == nullptr ||
               remote_input_.cmd_->IsReadOnly();
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

    bool HasCommand() const
    {
        return !is_local_ && remote_input_.cmd_ != nullptr;
    }

    void SetCommand(TxCommand *cmd)
    {
        assert(!is_local_ && remote_input_.cmd_ == nullptr);

        remote_input_.cmd_ = cmd;
        remote_input_.is_owner_ = true;
    }
    // For remote update command, if NOT apply_and_commit_, it will need move
    // the ownership into ccentry PendingCmd for executing in PostWriteCc.
    void RemoveOwnership()
    {
        remote_input_.is_owner_ = false;
    }

    LruEntry *CcePtr() const
    {
        return cce_ptr_;
    }

    void SetCcePtr(LruEntry *cce)
    {
        cce_ptr_ = cce;
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

    // Whether skip access backend storage when cache miss. If true, return key
    // not exists directly, otherwise try fetch the key from backend storage.
    bool skip_kv_{};

    enum struct ApplyBlockType
    {
        NoBlocking = 0,
        BlockOnRead,  // this could be ReadLock or WriteIntent
        BlockOnWriteLock,
        BlockOnFetch
    };
    ApplyBlockType block_type_{ApplyBlockType::NoBlocking};
};

struct UploadTxCommandsCc
    : public TemplatedCcRequest<UploadTxCommandsCc, PostProcessResult>
{
public:
    UploadTxCommandsCc()
        : cce_addr_(nullptr),
          object_version_(0),
          commit_ts_(0),
          cmd_str_list_(),
          has_overwrite_(false)
    //,is_remote_(false)
    {
    }

    UploadTxCommandsCc(const UploadTxCommandsCc &rhs) = delete;
    UploadTxCommandsCc(UploadTxCommandsCc &&rhs) = delete;

    bool ValidTermCheck() override
    {
        uint32_t src_ng_id = (tx_number_ >> 32L) >> 10;
        if (tx_term_ <= Sharder::Instance().InvalidLeaderTerm(src_ng_id))
        {
            return false;
        }

        assert(cce_addr_ != nullptr && cce_addr_->CcePtr() != 0 &&
               cce_addr_->Term() > 0);

        int64_t cc_ng_term = Sharder::Instance().LeaderTerm(node_group_id_);
        if (cce_addr_->Term() != cc_ng_term)
        {
            return false;
        }

        const LruEntry *lru_entry =
            reinterpret_cast<const LruEntry *>(cce_addr_->CcePtr());
        if (lru_entry->PayloadStatus() == RecordStatus::Invalid)
        {
            return false;
        }
        ccm_ = lru_entry->GetCcMap();
        assert(ccm_ != nullptr);
        return true;
    }

    void Reset(const CcEntryAddr *addr,
               uint64_t tx_number,
               int64_t tx_term,
               uint64_t object_version,
               uint64_t commit_ts,
               const std::vector<std::string> *cmd_list,
               bool has_overwrite,
               CcHandlerResult<PostProcessResult> *res)
    {
        TemplatedCcRequest<UploadTxCommandsCc, PostProcessResult>::Reset(
            nullptr, res, addr->NodeGroupId(), tx_number, tx_term);

        cce_addr_ = addr;
        object_version_ = object_version;
        commit_ts_ = commit_ts;
        cmd_str_list_ = cmd_list;
        has_overwrite_ = has_overwrite;

        // is_remote_ = false;
        ccm_ = nullptr;
    }

    const CcEntryAddr *CceAddr() const
    {
        return cce_addr_;
    }

    uint64_t ObjectVersion() const
    {
        return object_version_;
    }

    uint64_t CommitTs() const
    {
        return commit_ts_;
    }

    const std::vector<std::string> *CommandList()
    {
        return cmd_str_list_;
    }

    bool HasOverWrite()
    {
        return has_overwrite_;
    }

private:
    const CcEntryAddr *cce_addr_;
    // commit_ts of object before execute these tx commands
    uint64_t object_version_;
    // commit_ts of this transaction.
    uint64_t commit_ts_;
    const std::vector<std::string> *cmd_str_list_;
    bool has_overwrite_;

    // TODO(lzx): remote request, use std::vector<std::string_veiw>* cmds
    //  bool is_remote_{false};
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
        // this cc will only execute in context of shard heap, so the stats
        // collected are shard heap stats
        //
        assert(mi_heap_get_default() == ccs.GetShardHeap()->heap_);
        mi_thread_stats(&stats_->allocated_, &stats_->committed_);
        stats_->wait_list_size_ = ccs.WaitListSize();
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

struct UploadBatchCc : public CcRequestBase
{
    using WriteEntryTuple = std::tuple<const std::string &,
                                       const std::string &,
                                       const std::string &,
                                       const std::string &>;

    static constexpr size_t UploadBatchBatchSize = 128;

public:
    UploadBatchCc() = default;
    ~UploadBatchCc() = default;

    UploadBatchCc(const UploadBatchCc &rhs) = delete;
    UploadBatchCc(UploadBatchCc &&rhs) = delete;

    void Reset(const TableName &table_name,
               txservice::NodeGroupId ng_id,
               int64_t &ng_term,
               size_t core_cnt,
               size_t batch_size,
               size_t start_key_idx,
               const std::vector<WriteEntry *> &entry_vec,
               bthread::Mutex &req_mux,
               bthread::ConditionVariable &req_cv,
               size_t &finished_req_cnt,
               CcErrorCode &req_result,
               UploadBatchType data_type)
    {
        table_name_ = &table_name;
        node_group_id_ = ng_id;
        node_group_term_ = &ng_term;
        is_remote_ = false;
        batch_size_ = batch_size;
        start_key_idx_ = start_key_idx;
        entry_vector_ = &entry_vec;
        req_mux_ = &req_mux;
        req_cv_ = &req_cv;
        finished_req_cnt_ = &finished_req_cnt;
        req_result_ = &req_result;
        unfinished_cnt_.store(core_cnt, std::memory_order_relaxed);
        err_code_.store(CcErrorCode::NO_ERROR, std::memory_order_relaxed);
        paused_pos_.clear();
        paused_pos_.resize(core_cnt, {});
        data_type_ = data_type;
    }

    void Reset(const TableName &table_name,
               txservice::NodeGroupId ng_id,
               int64_t &ng_term,
               size_t core_cnt,
               uint32_t batch_size,
               const WriteEntryTuple &entry_tuple,
               bthread::Mutex &req_mux,
               bthread::ConditionVariable &req_cv,
               size_t &finished_req_cnt,
               UploadBatchType data_type)
    {
        table_name_ = &table_name;
        node_group_id_ = ng_id;
        node_group_term_ = &ng_term;
        is_remote_ = true;
        batch_size_ = batch_size;
        start_key_idx_ = 0;
        entry_tuples_ = &entry_tuple;
        req_mux_ = &req_mux;
        req_cv_ = &req_cv;
        finished_req_cnt_ = &finished_req_cnt;
        req_result_ = nullptr;
        unfinished_cnt_.store(core_cnt, std::memory_order_relaxed);
        err_code_.store(CcErrorCode::NO_ERROR, std::memory_order_relaxed);
        paused_pos_.clear();
        paused_pos_.resize(core_cnt, {});
        data_type_ = data_type;
    }

    bool ValidTermCheck()
    {
        std::lock_guard<bthread::Mutex> req_lk(*req_mux_);
        int64_t cc_ng_term = Sharder::Instance().LeaderTerm(node_group_id_);
        if (*node_group_term_ < 0)
        {
            *node_group_term_ = cc_ng_term;
        }

        if (cc_ng_term < 0 || cc_ng_term != *node_group_term_)
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
            return SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
        }

        CcMap *ccm = ccs.GetCcm(*table_name_, node_group_id_);
        if (ccm == nullptr)
        {
            assert(!table_name_->IsMeta());
            const CatalogEntry *catalog_entry = ccs.InitCcm(
                *table_name_, node_group_id_, *node_group_term_, this);
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
                    return SetError(CcErrorCode::REQUESTED_TABLE_NOT_EXISTS);
                }

                ccm = ccs.GetCcm(*table_name_, node_group_id_);
            }
        }

        assert(ccm != nullptr);
        return ccm->Execute(*this);
    }

    bool SetFinish()
    {
        if (unfinished_cnt_.fetch_sub(1, std::memory_order_acq_rel) == 1)
        {
            std::unique_lock<bthread::Mutex> req_lk(*req_mux_);
            ++(*finished_req_cnt_);
            auto res = err_code_.load(std::memory_order_relaxed);
            if (req_result_ && res != CcErrorCode::NO_ERROR)
            {
                *req_result_ = res;
            }
            req_cv_->notify_one();

            return true;
        }
        return false;
    }

    bool SetError(CcErrorCode err_code)
    {
        CcErrorCode no_error = CcErrorCode::NO_ERROR;
        err_code_.compare_exchange_strong(
            no_error, err_code, std::memory_order_acq_rel);
        if (unfinished_cnt_.fetch_sub(1, std::memory_order_acq_rel) == 1)
        {
            std::unique_lock<bthread::Mutex> req_lk(*req_mux_);
            ++(*finished_req_cnt_);
            if (req_result_)
            {
                *req_result_ = err_code_.load(std::memory_order_relaxed);
            }
            req_cv_->notify_one();

            return true;
        }
        return false;
    }

    void AbortCcRequest(CcErrorCode err_code) override
    {
        assert(err_code != CcErrorCode::NO_ERROR);
        DLOG(ERROR) << "Abort this uploadbatch request with error: "
                    << CcErrorMessage(err_code);
        if (SetError(err_code))
        {
            Free();
        }
    }

    int64_t CcNgTerm() const
    {
        return *node_group_term_;
    }

    uint32_t NodeGroupId() const
    {
        return node_group_id_;
    }

    CcErrorCode ErrorCode() const
    {
        return err_code_.load(std::memory_order_relaxed);
    }

    uint32_t BatchSize() const
    {
        return batch_size_;
    }

    const std::vector<WriteEntry *> *EntryVector() const
    {
        return is_remote_ ? nullptr : entry_vector_;
    }

    const WriteEntryTuple *EntryTuple() const
    {
        return is_remote_ ? entry_tuples_ : nullptr;
    }

    void SetPausedPosition(uint16_t core_id,
                           size_t key_index,
                           size_t key_off,
                           size_t rec_off,
                           size_t ts_off,
                           size_t status_off)
    {
        auto &key_pos = paused_pos_.at(core_id);
        std::get<0>(key_pos) = key_index;
        std::get<1>(key_pos) = key_off;
        std::get<2>(key_pos) = rec_off;
        std::get<3>(key_pos) = ts_off;
        std::get<4>(key_pos) = status_off;
    }

    const std::tuple<size_t, size_t, size_t, size_t, size_t> &GetPausedPosition(
        uint16_t core_id) const
    {
        return paused_pos_.at(core_id);
    }

    size_t StartKeyIndex() const
    {
        return start_key_idx_;
    }

    UploadBatchType Kind()
    {
        return data_type_;
    }

private:
    const TableName *table_name_{nullptr};
    uint32_t node_group_id_{0};
    int64_t *node_group_term_{nullptr};
    bool is_remote_{false};
    uint32_t batch_size_{0};
    size_t start_key_idx_{0};
    union
    {
        // for local request
        const std::vector<WriteEntry *> *entry_vector_;
        // for remote request
        const WriteEntryTuple *entry_tuples_;
    };

    bthread::Mutex *req_mux_{nullptr};
    bthread::ConditionVariable *req_cv_{nullptr};
    size_t *finished_req_cnt_{nullptr};
    CcErrorCode *req_result_{nullptr};
    // This two variables may be accessed by multi-cores.
    std::atomic<size_t> unfinished_cnt_{0};
    std::atomic<CcErrorCode> err_code_{CcErrorCode::NO_ERROR};
    // key index, key offset, record offset, ts offset, record status offset
    std::vector<std::tuple<size_t, size_t, size_t, size_t, size_t>> paused_pos_;

    UploadBatchType data_type_{UploadBatchType::SkIndexData};
};

struct UploadRangeSlicesCc : public CcRequestBase
{
    static constexpr uint16_t MaxParseBatchSize = 64;

public:
    UploadRangeSlicesCc() = default;
    ~UploadRangeSlicesCc() = default;

    UploadRangeSlicesCc(const UploadRangeSlicesCc &rhs) = delete;
    UploadRangeSlicesCc(UploadRangeSlicesCc &&rhs) = delete;
    void Reset(const TableName &table_name,
               NodeGroupId ng_id,
               int32_t partition_id,
               uint64_t version_ts,
               int32_t new_partition_id,
               const std::string *new_slices_keys,
               const std::string *new_slices_sizes,
               const std::string *new_slices_status,
               uint32_t slices_num,
               int64_t ng_term = INIT_TERM)
    {
        table_name_ = &table_name;
        node_group_id_ = ng_id;
        partition_id_ = partition_id;
        version_ts_ = version_ts;
        new_partition_id_ = new_partition_id;
        new_slices_keys_ = new_slices_keys;
        new_slices_sizes_ = new_slices_sizes;
        new_slices_status_ = new_slices_status;
        slices_cnt_ = slices_num;
        ng_term_ = ng_term;

        parse_offset_ = {0, 0, 0};
    }

    bool ValidTermCheck()
    {
        std::unique_lock<bthread::Mutex> lk(mutex_);
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
            SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            return true;
        }

        CcMap *ccm = ccs.GetCcm(*table_name_, node_group_id_);
        if (ccm == nullptr)
        {
            assert(table_name_->Type() == TableType::RangePartition);

            // Get original table name for the range table name
            const TableName base_table_name{table_name_->GetBaseTableNameSV(),
                                            TableType::Primary};
            const CatalogEntry *catalog_entry =
                ccs.GetCatalog(base_table_name, node_group_id_);
            if (catalog_entry == nullptr || catalog_entry->schema_ == nullptr)
            {
                ccs.FetchCatalog(
                    base_table_name, node_group_id_, ng_term_, this);
                return false;
            }
            TableSchema *table_schema = catalog_entry->schema_.get();

            // The request is toward a special cc map that contains a
            // table's range meta data.
            std::map<TxKey, TableRangeEntry::uptr> *ranges =
                ccs.GetTableRangesForATable(*table_name_, node_group_id_);
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

        assert(ccm != nullptr);
        return ccm->Execute(*this);
    }

    void SetError(CcErrorCode err_code)
    {
        std::unique_lock<bthread::Mutex> lk(mutex_);
        finish_ = true;
        err_code_ = err_code;
        cv_.notify_one();
    }

    void AbortCcRequest(CcErrorCode err_code) override
    {
        assert(err_code != CcErrorCode::NO_ERROR);
        DLOG(ERROR) << "Abort this upload range slices request with error: "
                    << CcErrorMessage(err_code);

        SetError(err_code);
    }

    void SetFinish()
    {
        std::unique_lock<bthread::Mutex> lk(mutex_);
        finish_ = true;
        cv_.notify_one();
    }

    void Wait()
    {
        std::unique_lock<bthread::Mutex> lk(mutex_);
        while (!finish_)
        {
            cv_.wait(lk);
        }
    }

    uint32_t NodeGroupId() const
    {
        return node_group_id_;
    }

    int64_t CcNgTerm() const
    {
        return ng_term_;
    }

    const TableName *GetTableName() const
    {
        return table_name_;
    }

    int32_t RangeId() const
    {
        return partition_id_;
    }

    int32_t NewRangeId() const
    {
        return new_partition_id_;
    }

    uint64_t VersionTs() const
    {
        return version_ts_;
    }

    const std::string &NewSlicesKeys() const
    {
        return *new_slices_keys_;
    }

    const std::string &NewSlicesSizes() const
    {
        return *new_slices_sizes_;
    }

    const std::string &NewSlicesStatus() const
    {
        return *new_slices_status_;
    }

    uint32_t NewSlicesCount() const
    {
        return slices_cnt_;
    }

    CcErrorCode ErrorCode() const
    {
        return err_code_;
    }

    void SetParseOffset(size_t keys_off, size_t sizes_off, size_t status_off)
    {
        std::get<0>(parse_offset_) = keys_off;
        std::get<1>(parse_offset_) = sizes_off;
        std::get<2>(parse_offset_) = status_off;
    }

    const std::tuple<size_t, size_t, size_t> &ParseOffsets() const
    {
        return parse_offset_;
    }

    std::vector<SliceInitInfo> &Slices()
    {
        return new_slices_;
    }

private:
    const TableName *table_name_{nullptr};
    uint32_t node_group_id_{0};
    int64_t ng_term_{INIT_TERM};

    int32_t partition_id_;
    uint64_t version_ts_;
    int32_t new_partition_id_;

    const std::string *new_slices_keys_{nullptr};
    // serialized type of size is uint32_t
    const std::string *new_slices_sizes_{nullptr};
    // serialized type of status is int8_t
    const std::string *new_slices_status_{nullptr};
    uint32_t slices_cnt_{0};

    std::tuple<size_t, size_t, size_t> parse_offset_{0, 0, 0};

    std::vector<SliceInitInfo> new_slices_{};

    bthread::Mutex mutex_;
    bthread::ConditionVariable cv_;
    bool finish_{false};
    CcErrorCode err_code_{CcErrorCode::NO_ERROR};
};

// upload multi slices data in one batch to future owner when splitting range.
struct UploadBatchSlicesCc : public CcRequestBase
{
    using WriteEntryTuple = std::tuple<const std::string &,
                                       const std::string &,
                                       const std::string &,
                                       const std::string &>;
    struct SliceUpdation
    {
        uint32_t range_{UINT32_MAX};
        uint32_t new_range_{UINT32_MAX};
        // Slices index in new range.
        std::vector<uint32_t> slice_idxs_{};
        uint64_t version_ts_{UINT64_MAX};
    };

    static constexpr size_t MaxParseBatchSize = 64;
    static constexpr size_t MaxEmplaceBatchSize = 64;

public:
    UploadBatchSlicesCc() = default;
    ~UploadBatchSlicesCc() = default;

    UploadBatchSlicesCc(const UploadBatchSlicesCc &rhs) = delete;
    UploadBatchSlicesCc(UploadBatchSlicesCc &&rhs) = delete;

    void Reset(const TableName &table_name,
               txservice::NodeGroupId ng_id,
               int64_t &ng_term,
               size_t core_cnt,
               const WriteEntryTuple &entry_tuple,
               std::shared_ptr<SliceUpdation> slice_info)
    {
        table_name_ = &table_name;
        node_group_id_ = ng_id;
        node_group_term_ = &ng_term;
        core_cnt_ = core_cnt;
        partitioned_slice_data_.resize(core_cnt);
        next_idxs_.resize(core_cnt);
        for (size_t i = 0; i < core_cnt; i++)
        {
            next_idxs_[i] = 0;
        }

        entry_tuples_ = &entry_tuple;
        slices_info_ = slice_info;

        unfinished_cnt_.store(core_cnt, std::memory_order_relaxed);
        err_code_.store(CcErrorCode::NO_ERROR, std::memory_order_relaxed);
    }

    bool ValidTermCheck()
    {
        std::lock_guard<bthread::Mutex> req_lk(req_mux_);
        int64_t cc_ng_term = Sharder::Instance().LeaderTerm(node_group_id_);
        if (*node_group_term_ < 0)
        {
            *node_group_term_ = cc_ng_term;
        }

        if (cc_ng_term < 0 || cc_ng_term != *node_group_term_)
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
            return SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
        }

        CcMap *ccm = ccs.GetCcm(*table_name_, node_group_id_);
        if (ccm == nullptr)
        {
            assert(!table_name_->IsMeta());
            const CatalogEntry *catalog_entry = ccs.InitCcm(
                *table_name_, node_group_id_, *node_group_term_, this);
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
                    return SetError(CcErrorCode::REQUESTED_TABLE_NOT_EXISTS);
                }

                ccm = ccs.GetCcm(*table_name_, node_group_id_);
            }
        }

        assert(ccm != nullptr);
        return ccm->Execute(*this);
    }

    std::pair<bool, std::shared_ptr<SliceUpdation>> SetFinish()
    {
        if (unfinished_cnt_.fetch_sub(1, std::memory_order_acq_rel) == 1)
        {
            std::unique_lock<bthread::Mutex> req_lk(req_mux_);

            req_cv_.notify_one();

            return {true, slices_info_};
        }
        return {false, nullptr};
    }

    bool SetError(CcErrorCode err_code)
    {
        CcErrorCode no_error = CcErrorCode::NO_ERROR;
        err_code_.compare_exchange_strong(
            no_error, err_code, std::memory_order_acq_rel);
        if (unfinished_cnt_.fetch_sub(1, std::memory_order_acq_rel) == 1)
        {
            std::unique_lock<bthread::Mutex> req_lk(req_mux_);

            req_cv_.notify_one();

            return true;
        }
        return false;
    }

    void AbortCcRequest(CcErrorCode err_code) override
    {
        assert(err_code != CcErrorCode::NO_ERROR);
        DLOG(ERROR) << "Abort this uploadbatch request with error: "
                    << CcErrorMessage(err_code);
        if (SetError(err_code))
        {
            Free();
        }
    }

    void Wait()
    {
        std::unique_lock<bthread::Mutex> lk(req_mux_);
        while (unfinished_cnt_ != 0)
        {
            req_cv_.wait(lk);
        }
    }

    int64_t CcNgTerm() const
    {
        return *node_group_term_;
    }

    uint32_t NodeGroupId() const
    {
        return node_group_id_;
    }

    CcErrorCode ErrorCode() const
    {
        return err_code_.load(std::memory_order_relaxed);
    }

    const WriteEntryTuple *EntryTuple() const
    {
        return entry_tuples_;
    }

    void SetParseOffset(size_t key_off,
                        size_t rec_off,
                        size_t ts_off,
                        size_t status_off)
    {
        std::get<0>(parse_offset_) = key_off;
        std::get<1>(parse_offset_) = rec_off;
        std::get<2>(parse_offset_) = ts_off;
        std::get<3>(parse_offset_) = status_off;
    }

    const std::tuple<size_t, size_t, size_t, size_t> &ParsePosition() const
    {
        return parse_offset_;
    }

    uint64_t DirtyVersion()
    {
        return slices_info_->version_ts_;
    }

    uint32_t RangeId() const
    {
        return slices_info_->range_;
    }

    uint32_t NewRangeId() const
    {
        return slices_info_->new_range_;
    }

    bool Parsed() const
    {
        return parsed_;
    }
    void SetParsed()
    {
        parsed_.store(true, std::memory_order_release);
    }

    void AddDataItem(TxKey key,
                     std::unique_ptr<txservice::TxRecord> &&record,
                     uint64_t version_ts,
                     bool is_deleted)
    {
        size_t hash = key.Hash();
        // Uses the lower 10 bits of the hash code to shard the key across
        // CPU cores at this node.
        uint16_t core_code = hash & 0x3FF;
        uint16_t core_id = core_code % core_cnt_;

        partitioned_slice_data_[core_id].emplace_back(
            std::move(key), std::move(record), version_ts, is_deleted);
    }

    size_t NextIndex(size_t core_idx) const
    {
        size_t next_idx = next_idxs_[core_idx];
        assert(next_idx <= partitioned_slice_data_[core_idx].size());
        return next_idx;
    }

    void SetNextIndex(size_t core_idx, size_t index)
    {
        assert(index <= partitioned_slice_data_[core_idx].size());
        next_idxs_[core_idx] = index;
    }

    // Notice: these data items belong to multi slices.
    std::deque<SliceDataItem> &SliceData(uint16_t core_id)
    {
        assert(core_id < partitioned_slice_data_.size());
        return partitioned_slice_data_[core_id];
    }

private:
    uint16_t core_cnt_;
    const TableName *table_name_{nullptr};
    uint32_t node_group_id_{0};
    int64_t *node_group_term_{nullptr};

    // std::vector<uint32_t> slice_sizes_;
    const WriteEntryTuple *entry_tuples_{nullptr};

    std::shared_ptr<SliceUpdation> slices_info_{nullptr};

    // key offset, record offset, ts offset, record status offset
    // when parse items
    std::tuple<size_t, size_t, size_t, size_t> parse_offset_{0, 0, 0, 0};
    // parse items on one core, then put the req to other cores.
    std::atomic_bool parsed_{false};

    std::vector<std::deque<SliceDataItem>> partitioned_slice_data_;
    // pause position when emplace keys into ccmap in batches
    std::vector<size_t> next_idxs_;

    bthread::Mutex req_mux_{};
    bthread::ConditionVariable req_cv_{};
    // size_t finished_req_cnt_{nullptr};
    // CcErrorCode req_result_{nullptr};
    // This two variables may be accessed by multi-cores.
    std::atomic<size_t> unfinished_cnt_{0};
    std::atomic<CcErrorCode> err_code_{CcErrorCode::NO_ERROR};
};

struct DbSizeCc : public CcRequestBase
{
public:
    DbSizeCc()
    {
    }

    void Reset(const TableName *table_name)
    {
        Clear();
        table_name_ = table_name;
        remote_shard_cnt_ = Sharder::Instance().NodeGroupCount() - 1;
        local_shard_cnt_ = Sharder::Instance().GetLocalCcShardsCount();
        total_obj_size_ = 0;
    }

    bool Execute(CcShard &ccs) override
    {
        assert(vct_ng_id_.size() >= 1);

        for (uint32_t ng_id : vct_ng_id_)
        {
            CcMap *map = ccs.GetCcm(*table_name_, ng_id);
            if (map != nullptr)
            {
                total_obj_size_.fetch_add(map->NormalObjectSize(),
                                          std::memory_order_relaxed);
            }
        }

        local_shard_cnt_.fetch_sub(1, std::memory_order_relaxed);
        if (local_shard_cnt_.load(std::memory_order_relaxed) == 0 &&
            remote_shard_cnt_.load(std::memory_order_relaxed) == 0)
        {
            std::unique_lock lk(mux_);
            cv_.notify_one();
        }

        return false;
    }

    int32_t GetLocalShardCnt()
    {
        return local_shard_cnt_.load(std::memory_order_relaxed);
    }

    int32_t GetRemoteShardCnt()
    {
        return remote_shard_cnt_.load(std::memory_order_relaxed);
    }

    int64_t GetTotalObjSize()
    {
        return total_obj_size_.load(std::memory_order_relaxed);
    }

    void AddLocalNodeGroupId(uint32_t ng_id)
    {
        vct_ng_id_.push_back(ng_id);
        if (vct_ng_id_.size() >= 2)
        {
            remote_shard_cnt_.fetch_sub(1, std::memory_order_relaxed);
        }
    }

    void AddRemoteObjSize(int32_t term, int64_t total_obj_size)
    {
        if (term != term_)
        {
            return;
        }

        total_obj_size_.fetch_add(total_obj_size, std::memory_order_relaxed);
        remote_shard_cnt_.fetch_sub(1, std::memory_order_relaxed);
        if (GetLocalShardCnt() == 0 && GetRemoteShardCnt() == 0)
        {
            std::unique_lock lk(mux_);
            cv_.notify_one();
        }
    }

    int32_t GetTerm()
    {
        return term_;
    }
    void IncTerm()
    {
        term_++;
    }

    void Clear()
    {
        total_obj_size_.store(0, std::memory_order_relaxed);
        local_shard_cnt_.store(0, std::memory_order_relaxed);
        remote_shard_cnt_.store(0, std::memory_order_relaxed);
        table_name_ = nullptr;
        vct_ng_id_.clear();
    }

    bthread::Mutex mux_;
    bthread::ConditionVariable cv_;

protected:
    std::atomic<int64_t> total_obj_size_;
    std::atomic<int32_t> local_shard_cnt_;
    std::atomic<int32_t> remote_shard_cnt_;
    int32_t term_{0};
    std::vector<uint32_t> vct_ng_id_;
    const TableName *table_name_;
};
}  // namespace txservice
