#pragma once

#include <algorithm>  // std::min
#include <condition_variable>
#include <fstream>
#include <functional>
#include <iostream>
#include <map>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

#include "../../log_service/include/fault_inject.h"
#include "cc/cc_map.h"
#include "cc/cc_shard.h"
#include "cc/ccm_scanner.h"
#include "cc_entry.h"
#include "cc_handler_result.h"
#include "cc_req_base.h"
#include "constants.h"
#include "fault/fault_inject.h"
#include "log_closure.h"
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
        int64_t cc_ng_term = Sharder::Instance().LeaderTerm(node_group_id_);
        if (cc_ng_term < 0)
        {
            res_->SetError(-1);
            return true;
        }

        CcMap *ccm = nullptr;
        RequestT *typed_req = static_cast<RequestT *>(this);

        if (parallel_req_ || ccm_ == nullptr)
        {
            assert(table_name_ != nullptr);
            ccm = ccs.GetCcm(*table_name_, node_group_id_);

            if (ccm == nullptr)
            {
                if (txservice::IsRangeTablename(*table_name_))
                {
                    // Get original table name for the range table name
                    const txservice::TableName base_table_name =
                        GetBaseTableNameFromRangeTableName(*table_name_);
                    const CatalogEntry *catalog_entry =
                        ccs.GetCatalog(base_table_name, node_group_id_);
                    // When a tx sends a request toward a table's range
                    // cc map, either to look up the range containing the
                    // input key or to lock a range for splitting/merging,
                    // this or prior tx's must have accessed the table's
                    // cc map at this node to read or write the table's
                    // data. Initialization of the table's cc map needs to
                    // instantiate the schema instance. So, the table's
                    // schema should never be null.
                    assert(catalog_entry != nullptr &&
                           catalog_entry->schema_ != nullptr);
                    TableSchema *table_schema = catalog_entry->schema_.get();

                    // The request is toward a special cc map that contains a
                    // tabmode's ranges.
                    auto ranges = ccs.GetAllTableRangesForATable(*table_name_);
                    if (ranges != nullptr)
                    {
                        ccs.CreateRangeCcMap(*table_name_,
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
                            *table_name_, table_schema->KeySchema(), this);
                        return false;
                    }
                }
                else
                {
                    // Find base table name for index table.
                    // Fecth/Get Catalog is based on base table name, but Get
                    // ccmap is based on the real table name, for example, index
                    // should get the correspond sk_ccmap.
                    TableName base_table_name = GetBaseTableName(*table_name_);
                    const CatalogEntry *catalog_entry =
                        ccs.GetCatalog(base_table_name, node_group_id_);

                    if (catalog_entry != nullptr)
                    {
                        const TableSchema *curr_schema =
                            catalog_entry->schema_.get();
                        if (curr_schema != nullptr)
                        {
                            ccs.CreatePkCcMap(base_table_name,
                                              curr_schema,
                                              node_group_id_,
                                              catalog_entry->Version());

                            std::vector<TableName> index_names =
                                curr_schema->IndexNames();
                            for (const TableName &index_name : index_names)
                            {
                                ccs.CreateSkCcMap(index_name,
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
                            res_->SetError(100);
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
    const CatalogEntry *InitCcm(CcShard &ccs)
    {
        TableName base_table_name = GetBaseTableName(*table_name_);

        const CatalogEntry *catalog_entry =
            ccs.GetCatalog(base_table_name, node_group_id_);

        if (catalog_entry != nullptr)
        {
            const TableSchema *curr_schema = catalog_entry->schema_.get();
            if (curr_schema != nullptr && catalog_entry->Version() > 0)
            {
                ccs.CreatePkCcMap(base_table_name,
                                  curr_schema,
                                  node_group_id_,
                                  catalog_entry->Version());

                std::vector<TableName> index_names = curr_schema->IndexNames();
                for (const TableName &index_name : index_names)
                {
                    ccs.CreateSkCcMap(index_name,
                                      curr_schema,
                                      node_group_id_,
                                      catalog_entry->Version());
                }
            }
        }
        else
        {
            // The local node does not contain the table's schema instance. The
            // FetchCatalog() method sends an async request toward the data
            // store to fetch the catalog. After fetching is finished, this cc
            // request is re-enqueued for re-execution.
            ccs.FetchCatalog(base_table_name, node_group_id_, this);
        }

        return catalog_entry;
    }

    /**
     * @brief Get the base table name from normal table or index table. For
     * index table, we need to remove the suffix.
     *
     * @param table_name: input table name, could be normal table or index
     * table.
     * @return base table name
     */
    std::string GetBaseTableName(const std::string &table_name)
    {
        std::string base_table_name;
        std::string::size_type pos = table_name.find(INDEX_NAME_PREFIX);
        if (pos != std::string::npos)
        {
            base_table_name = table_name.substr(0, pos);
        }
        else
        {
            base_table_name = table_name;
        }
        return base_table_name;
    }

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

    void Reset(const TableName *tname,
               const TxKey *key,
               const uint32_t key_shard_code,
               const TxId *txid,
               int64_t tx_term,
               uint64_t ts,
               bool is_insert,
               CcHandlerResult<AcquireKeyResult> *res,
               CcProtocol proto)
    {
        TemplatedCcRequest<AcquireCc, AcquireKeyResult>::Reset(
            tname, res, key_shard_code >> 10, txid->TxNumber(), proto);

        key_ = key;
        key_str_ = nullptr;
        key_shard_code_ = key_shard_code;
        txid_ = txid;
        tx_term_ = tx_term;
        ts_ = ts;
        is_insert_ = is_insert;
        cce_ptr_ = nullptr;
    }

    void Reset(const TableName *tname,
               const std::string *key_str,
               const uint32_t key_shard_code,
               const TxId *txid,
               int64_t tx_term,
               uint64_t ts,
               bool is_insert,
               CcHandlerResult<AcquireKeyResult> *res,
               CcProtocol proto)
    {
        TemplatedCcRequest<AcquireCc, AcquireKeyResult>::Reset(
            tname, res, key_shard_code >> 10, txid->TxNumber(), proto);

        key_ = nullptr;
        key_str_ = key_str;
        key_shard_code_ = key_shard_code;
        txid_ = txid;
        tx_term_ = tx_term;
        ts_ = ts;
        is_insert_ = is_insert;
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
    // blocked due to conflicts in 2PL. After the request is unblocked and
    // acquires the lock, the request's execution resumes without further lookup
    // of the cc entry.
    LruEntry *cce_ptr_{nullptr};
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
               LockType lk_type)
    {
        TemplatedCcRequest<AcquireAllCc, AcquireAllResult>::Reset(
            tname, res, node_group_id, tx_number, proto);

        key_ = key;
        key_str_ = nullptr;
        tx_term_ = tx_term;
        is_insert_ = is_insert;
        decoded_key_ = nullptr;
        lock_type_ = lk_type;
        cce_ptr_ = nullptr;
    }

    void Reset(const TableName *tname,
               const std::string *key_str,
               uint32_t node_group_id,
               TxNumber tx_number,
               int64_t tx_term,
               bool is_insert,
               CcHandlerResult<AcquireAllResult> *res,
               CcProtocol proto,
               LockType lk_type)
    {
        TemplatedCcRequest<AcquireAllCc, AcquireAllResult>::Reset(
            tname, res, node_group_id, tx_number, proto);

        key_ = nullptr;
        key_str_ = key_str;
        tx_term_ = tx_term;
        is_insert_ = is_insert;
        decoded_key_ = nullptr;
        lock_type_ = lk_type;
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

    int64_t TxTerm() const
    {
        return tx_term_;
    }

    bool IsInsert() const
    {
        return is_insert_;
    }

    LockType GetLockType() const
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

private:
    const TxKey *key_{nullptr};
    const std::string *key_str_{nullptr};
    std::unique_ptr<TxKey> decoded_key_{nullptr};
    int64_t tx_term_{-1};
    bool is_insert_{false};
    LockType lock_type_{LockType::WriteIntent};
    // The pointer of the cc entry to which this request is directed. The
    // pointer is set, when the request locates the cc entry but is
    // blocked due to conflicts in 2PL. After the request is unblocked and
    // acquires the lock, the request's execution resumes without further lookup
    // of the cc entry.
    LruEntry *cce_ptr_{nullptr};
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

    void Reset(const CcEntryAddr *addr,
               uint64_t tx_number,
               uint64_t ts,
               const TxRecord *rec,
               bool is_deleted,
               CcHandlerResult<Void> *res,
               CcProtocol proto)
    {
        TemplatedCcRequest<PostWriteCc, Void>::Reset(
            nullptr, res, addr->NodeGroupId(), tx_number, proto);

        cce_addr_ = addr;
        commit_ts_ = ts;
        payload_ = rec;
        payload_str_ = nullptr;
        is_deleted_ = is_deleted;

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
               bool is_deleted,
               CcHandlerResult<Void> *res,
               CcProtocol proto)
    {
        TemplatedCcRequest<PostWriteCc, Void>::Reset(
            nullptr, res, addr->NodeGroupId(), tx_number, proto);

        cce_addr_ = addr;
        commit_ts_ = ts;
        payload_ = nullptr;
        payload_str_ = rec;
        is_deleted_ = is_deleted;

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

    void Reset(const TableName *tname,
               const TxKey *key,
               uint32_t node_group_id,
               uint64_t tx_number,
               uint64_t ts,
               TxRecord *rec,
               DmlOperation dml_op,
               CcHandlerResult<Void> *res,
               PostWriteType commit_type,
               int64_t tx_term)
    {
        TemplatedCcRequest<PostWriteAllCc, Void>::Reset(
            tname, res, node_group_id, tx_number, CcProtocol::OCC);

        key_ = key;
        key_str_ = nullptr;
        decoded_key_ = nullptr;
        commit_ts_ = ts;
        payload_ = rec;
        payload_str_ = nullptr;
        decoded_payload_ = nullptr;
        dml_op_ = dml_op;
        commit_type_ = commit_type;
        tx_term_ = tx_term;
    }

    void Reset(const TableName *tname,
               const TxKey *key,
               uint32_t node_group_id,
               uint64_t tx_number,
               uint64_t ts,
               std::unique_ptr<TxRecord> rec,
               DmlOperation dml_op,
               CcHandlerResult<Void> *res,
               PostWriteType commit_type,
               int64_t tx_term)
    {
        TemplatedCcRequest<PostWriteAllCc, Void>::Reset(
            tname, res, node_group_id, tx_number, CcProtocol::OCC);

        key_ = key;
        key_str_ = nullptr;
        decoded_key_ = nullptr;
        commit_ts_ = ts;
        payload_ = rec.get();
        payload_str_ = nullptr;
        decoded_payload_ = std::move(rec);
        dml_op_ = dml_op;
        commit_type_ = commit_type;
        tx_term_ = tx_term;
    }

    void Reset(const TableName *tname,
               const std::string *key_str,
               uint32_t node_group_id,
               uint64_t tx_number,
               uint64_t ts,
               const std::string *rec,
               DmlOperation dml_op,
               CcHandlerResult<Void> *res,
               PostWriteType commit_type,
               int64_t tx_term)
    {
        TemplatedCcRequest<PostWriteAllCc, Void>::Reset(
            tname, res, node_group_id, tx_number, CcProtocol::OCC);

        key_ = nullptr;
        key_str_ = key_str;
        decoded_key_ = nullptr;
        commit_ts_ = ts;
        payload_ = nullptr;
        payload_str_ = rec;
        decoded_payload_ = nullptr;
        dml_op_ = dml_op;
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

    int64_t TxTerm()
    {
        return tx_term_;
    }

private:
    const TxKey *key_{nullptr};
    const std::string *key_str_{nullptr};
    std::unique_ptr<TxKey> decoded_key_{nullptr};
    uint64_t commit_ts_{0};
    TxRecord *payload_{nullptr};
    const std::string *payload_str_{nullptr};
    /**
     * @brief When the PostWriteAllCc request is a remote request or is a local
     * request but dispatched to a non-native cc node group, decoded_payload_
     * owns a record on which the request is executed.
     *
     */
    std::unique_ptr<TxRecord> decoded_payload_{nullptr};
    DmlOperation dml_op_{DmlOperation::Update};
    PostWriteType commit_type_;
    int64_t tx_term_{0};
};

struct PostReadCc : public TemplatedCcRequest<PostReadCc, std::vector<TxId>>
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
               CcHandlerResult<std::vector<TxId>> *res,
               CcProtocol protocol,
               LockType lock_type)
    {
        TemplatedCcRequest<PostReadCc, std::vector<TxId>>::Reset(
            nullptr, res, addr->NodeGroupId(), tx_number, protocol);

        cce_addr_ = addr;
        commit_ts_ = commit_ts;
        key_ts_ = key_ts;
        gap_ts_ = gap_ts;
        lock_type_ = lock_type;
        res->Value().clear();

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
          lock_type_(LockType::ReadLock)
    {
    }

    ReadCc(const ReadCc &rhs) = delete;
    ReadCc(ReadCc &&rhs) = delete;

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
               LockType lock_type,
               const std::vector<VersionedRecord> *archives = nullptr)
    {
        TemplatedCcRequest<ReadCc, ReadKeyResult>::Reset(
            nullptr, res, key_shard_code >> 10, tx_number, protocol, iso_level);

        key_ = key;
        key_str_ = nullptr;
        key_shard_code_ = key_shard_code;
        rec_ = rec;
        rec_str_ = nullptr;
        tx_term_ = tx_term;
        ts_ = ts;
        type_ = read_type;
        lock_type_ = lock_type;
        cce_ptr_ = nullptr;
        archives_ = archives;

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
               LockType lock_type,
               const std::vector<VersionedRecord> *archives = nullptr)
    {
        TemplatedCcRequest<ReadCc, ReadKeyResult>::Reset(
            nullptr, res, key_shard_code >> 10, tx_number, protocol, iso_level);

        key_ = nullptr;
        key_str_ = key_str;
        key_shard_code_ = key_shard_code;
        rec_ = nullptr;
        rec_str_ = rec_str;
        tx_term_ = tx_term;
        ts_ = ts;
        type_ = read_type;
        lock_type_ = lock_type;
        cce_ptr_ = nullptr;
        archives_ = archives;

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

    ReadType Type() const
    {
        return type_;
    }

    LockType GetLockType() const
    {
        return lock_type_;
    }

    void SetReadType(ReadType type)
    {
        type_ = type;
    }

    void SetLockType(LockType lock_type)
    {
        lock_type_ = lock_type;
    }

    void SetCcePtr(LruEntry *ptr)
    {
        cce_ptr_ = ptr;
    }

    LruEntry *CcePtr() const
    {
        return cce_ptr_;
    }

    void SetArchivesPtr(const std::vector<VersionedRecord> *ptr)
    {
        archives_ = ptr;
    }

    const std::vector<VersionedRecord> *ArchivesPtr() const
    {
        return archives_;
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
    LockType lock_type_;
    // The pointer of the cc entry to which this request is directed. The
    // pointer is set, when the request locates the cc entry but is
    // blocked due to conflicts in 2PL. After the request is unblocked and
    // acquires the lock, the request's execution resumes without further lookup
    // of the cc entry.
    LruEntry *cce_ptr_{nullptr};

    const std::vector<VersionedRecord> *archives_{nullptr};
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
               LockType lock_type,
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
        lock_type_ = lock_type;
        is_ckpt_delta_ = is_delta;
        is_include_floor_cce_ = is_include_floor_cce;
        cce_ptr_ = nullptr;
    }

    int64_t TxTerm()
    {
        return term_;
    }

    LockType GetLockType()
    {
        return lock_type_;
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

private:
    ScanIndexType index_type_{ScanIndexType::Primary};
    const TxKey *start_key_{nullptr};
    bool inclusive_{false};
    ScanDirection direct_{ScanDirection::Forward};
    uint64_t ts_{0};
    ScanCache *scan_cache_{nullptr};
    int64_t term_{-1};
    LockType lock_type_{LockType::ReadLock};
    bool is_ckpt_delta_{false};
    // If always include floor_cce in scan result
    bool is_include_floor_cce_{false};

    // The pointer of the cc entry to which this request is directed. The
    // pointer is set, when the request locates the cc entry but is
    // blocked due to conflicts in 2PL. After the request is unblocked and
    // acquires the lock, the request's execution resumes without further lookup
    // of the cc entry.
    LruEntry *cce_ptr_{nullptr};

    template <typename KeyT, typename ValueT>
    friend class TemplateCcMap;

    template <typename SkT, typename PkT>
    friend class SkCcMap;

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
               LockType lock_type,
               bool is_delta)
    {
        TemplatedCcRequest<ScanNextBatchCc, ScanNextResult>::Reset(
            nullptr, next_res, ng_id, tx_number, protocol, iso_level);

        ts_ = ts;
        scan_cache_ = cache;
        tx_term_ = tx_term;
        lock_type_ = lock_type;
        is_ckpt_delta_ = is_delta;
        cce_ptr_ = nullptr;

        const ScanTuple *last_tuple = cache->LastTuple();
        const LruEntry *lru_entry =
            reinterpret_cast<const LruEntry *>(last_tuple->cce_addr_.CcePtr());
        ccm_ = lru_entry->parent_map_;
    }

    int64_t TxTerm()
    {
        return tx_term_;
    }

    LockType GetLockType()
    {
        return lock_type_;
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

private:
    uint64_t ts_{0};
    ScanCache *scan_cache_{nullptr};
    int64_t tx_term_{-1};
    LockType lock_type_{LockType::ReadLock};

    bool is_ckpt_delta_{false};

    // The pointer of the cc entry to which this request is directed. The
    // pointer is set, when the request locates the cc entry but is
    // blocked due to conflicts in 2PL. After the request is unblocked and
    // acquires the lock, the request's execution resumes without further lookup
    // of the cc entry.
    LruEntry *cce_ptr_{nullptr};

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
        for (int i = 0; i < shard_cnt_; i++)
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
        ckpt_ts_ = std::min(ckpt_ts_, ccs.ActiveTxMinTs());
        memory_usage_kb_vec_[ccs.LocalCoreId()] = ccs.mem_usage_ / 1000;
        log_usage_kb_vec_[ccs.LocalCoreId()] =
            ccs.estimate_ccshard_log_size_ / 1000;

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
               std::vector<LruEntry *> &ckpt_vec)
        : table_name_(table_name),
          ckpt_ts_(ckpt_ts),
          ckpt_vec_(ckpt_vec),
          start_entry_(nullptr),
          status_(CkptScanStatus::Ongoing),
          mux_(),
          cv_(),
          ccm_(nullptr)
    {
    }

    // CkptScanCc is always stack object and won't be reused, worse, it might be
    // destructed before Execute returns, so always return false as caller
    // should never access this object after Execute returns
    bool Execute(CcShard &ccs) override
    {
        if (ccm_ == nullptr)
        {
            ccm_ = ccs.GetCcm(table_name_, node_group_);
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

    uint32_t GetNodeGroup()
    {
        return node_group_;
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
    friend std::ostream &operator<<(std::ostream &outs,
                                    txservice::CkptScanCc *r);
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
                std::string_view &&blob,
                uint64_t commit_ts,
                uint64_t txn,
                std::mutex &mux,
                std::condition_variable &cv,
                uint32_t &finish_cnt,
                bool &recovery_error)
        : table_name_str_(table_name_view),
          log_blob_view_(blob),
          commit_ts_(commit_ts),
          result_(nullptr),
          external_mux_(mux),
          external_cv_(cv),
          finish_cnt_(finish_cnt),
          recovery_error_(recovery_error)
    {
        table_name_ = &table_name_str_;
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
                const CatalogEntry *catalog_entry = InitCcm(ccs);

                if (catalog_entry != nullptr)
                {
                    if (catalog_entry->Version() == 0)
                    {
                        // The schema view is initialized but the current schema
                        // is unset (version_ts is 0). This means that there is
                        // an error when reading the catalog from the data
                        // store. Returns the request with an error.
                        SetRecoveryError();
                        return false;
                    }
                    else if (catalog_entry->schema_ != nullptr)
                    {
                        ccm_ = ccs.GetCcm(*table_name_, node_group_id_);

                        // Replaying records from a dropped table.
                        if (ccm_ == nullptr)
                        {
                            SetFinish();
                            return false;
                        }
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
                    // initialize the cc map. The request will be re-executed
                    // after the schema is fetched from the data store.
                    return false;
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

private:
    std::string table_name_str_;
    std::string_view log_blob_view_;
    uint64_t commit_ts_;
    CcHandlerResult<Void> result_;
    std::mutex &external_mux_;
    std::condition_variable &external_cv_;
    uint32_t &finish_cnt_;
    bool &recovery_error_;

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

struct StatMinTxStartTsCc : public CcRequestBase
{
public:
    explicit StatMinTxStartTsCc(size_t shard_cnt)
        : min_start_ts_(UINT64_MAX),
          mux_(),
          cv_(),
          finish_cnt_(0),
          shard_cnt_(shard_cnt)
    {
    }

    StatMinTxStartTsCc() = delete;
    StatMinTxStartTsCc(const StatMinTxStartTsCc &) = delete;
    StatMinTxStartTsCc(StatMinTxStartTsCc &&) = delete;

    bool Execute(CcShard &ccs) override
    {
        std::unique_lock<std::mutex> lk(mux_);
        min_start_ts_ = std::min(min_start_ts_, ccs.StatMinTxStartTs());
        assert(finish_cnt_ < shard_cnt_);
        ++finish_cnt_;
        if (finish_cnt_ == shard_cnt_)
        {
            cv_.notify_one();
        }

        // return false since StatMinTxStartTsCc is not reused and does not need
        // to call CcRequestBase::Free
        return false;
    }

    void Wait()
    {
        std::unique_lock<std::mutex> lk(mux_);
        cv_.wait(lk, [this] { return finish_cnt_ == shard_cnt_; });
    }

    uint64_t GetMinStartTs() const
    {
        return min_start_ts_;
    }

private:
    uint64_t min_start_ts_;
    std::mutex mux_;
    std::condition_variable cv_;
    std::atomic<size_t> finish_cnt_;
    size_t shard_cnt_;
};

struct KickoutArchivesCc : public CcRequestBase
{
public:
    KickoutArchivesCc()
    {
    }

    void Set(LruEntry *entry,
             uint32_t ng_id,
             int64_t term,
             uint64_t upper_bound_ts)
    {
        lru_entry_ = entry;
        node_grou_id_ = ng_id;
        term_ = term;
        upper_bound_ts_ = upper_bound_ts;
    }

    bool Execute(CcShard &ccs) override
    {
        if (Sharder::Instance().CheckLeaderTerm(node_grou_id_, term_))
        {
            ccs.DecrementMemory(
                lru_entry_->KickOutFlushedArchiveRecords(upper_bound_ts_));
        }

        delete this;
        return false;
    }

    LruEntry *lru_entry_;
    uint32_t node_grou_id_{0};
    int64_t term_{-1};
    uint64_t upper_bound_ts_;
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
               uint32_t key_shard_code,
               uint64_t tx_number,
               CcHandlerResult<bool> *res)
    {
        TemplatedCcRequest<CleanCcEntryForTestCc, bool>::Reset(
            tn, res, key_shard_code >> 10, tx_number);
        key_ = key;
        key_str_ = nullptr;
        key_shard_code_ = key_shard_code;
        only_archives_ = only_archives;
        res_ = res;
    }

    void Reset(const TableName *tn,
               const std::string *key_str,
               bool only_archives,
               uint32_t key_shard_code,
               uint64_t tx_number,
               CcHandlerResult<bool> *res)
    {
        TemplatedCcRequest<CleanCcEntryForTestCc, bool>::Reset(
            tn, res, key_shard_code >> 10, tx_number);
        key_ = nullptr;
        key_str_ = key_str;
        key_shard_code_ = key_shard_code;
        only_archives_ = only_archives;
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

private:
    const TxKey *key_;
    const std::string *key_str_;
    uint32_t key_shard_code_;
    bool only_archives_;
};

}  // namespace txservice
