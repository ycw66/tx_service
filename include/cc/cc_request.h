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
        }

        if (ccm_ == nullptr)
        {
            res_->SetError(error_code);
            return true;
        }
        else
        {
            RequestT *typed_req = static_cast<RequestT *>(this);
            return ccm_->Execute(*typed_req);
        }
    }

    /*bool Resume() override
    {
        return ccm_->Resume(*this);
    }*/

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

protected:
    CcHandlerResult<ResultType> *res_;
    const TableName *table_name_;
    CcMap *ccm_;
    uint32_t node_group_id_;
};

struct AcquireTableWriteLockCC
    : public TemplatedCcRequest<AcquireTableWriteLockCC,
                                std::unordered_map<uint32_t, int64_t>>
{
    AcquireTableWriteLockCC() : txid_(nullptr)
    {
    }

    virtual ~AcquireTableWriteLockCC() = default;

    virtual bool Execute(CcShard &ccs) override
    {
        bool success = ccs.AcquireTableWriteLock(*table_name_, this);

        if (success)
        {
            res_->SetFinished();
            return true;
        }
        else
        {
            // AcqureTableWriteLock is blocked
            return false;
        }
    }

    void Set(const TableName *tname,
             const TxId *txid,
             uint64_t tx_number,
             uint32_t node_group_id,
             CcHandlerResult<std::unordered_map<uint32_t, int64_t>> *res)
    {
        table_name_ = tname;
        txid_ = txid;
        tx_number_ = tx_number;
        node_group_id_ = node_group_id;
        res_ = res;
    }

    const TxId *Txid() const
    {
        return txid_;
    }

private:
    const TxId *txid_;
};

struct ReleaseTableWriteLockCC
    : public TemplatedCcRequest<ReleaseTableWriteLockCC, Void>
{
    ReleaseTableWriteLockCC() : txid_(nullptr)
    {
    }

    virtual ~ReleaseTableWriteLockCC() = default;

    virtual bool Execute(CcShard &ccs) override
    {
        bool success = ccs.ReleaseTableWriteLock(*table_name_, this);

        if (success)
        {
            res_->SetFinished();
            return true;
        }
        else
        {
            // RleaseTableWriteLock is blocked
            return false;
        }
    }

    void Set(const TableName *tname,
             const TxId *txid,
             uint64_t tx_number,
             uint32_t node_group_id,
             CcHandlerResult<Void> *res)
    {
        table_name_ = tname;
        txid_ = txid;
        tx_number_ = tx_number;
        node_group_id_ = node_group_id;
        res_ = res;
    }

    const TxId *Txid() const
    {
        return txid_;
    }

private:
    const TxId *txid_;
};

struct AcquireCc
    : public TemplatedCcRequest<AcquireCc, std::pair<uint64_t, CcEntryAddr>>,
      Resumable
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
             CcHandlerResult<std::pair<uint64_t, CcEntryAddr>> *res,
             CcProtocol proto = CcProtocol::OCC)
    {
        table_name_ = tname;
        key_ = key;
        key_str_ = nullptr;
        key_shard_code_ = key_shard_code;
        node_group_id_ = key_shard_code >> 10;
        txid_ = txid;
        tx_term_ = tx_term;
        ts_ = ts;
        is_insert_ = is_insert;
        res_ = res;
        ccm_ = nullptr;
        proto_ = proto;
    }

    void Set(const TableName *tname,
             const std::string *key_str,
             const uint32_t key_shard_code,
             const TxId *txid,
             int64_t tx_term,
             uint64_t ts,
             bool is_insert,
             CcHandlerResult<std::pair<uint64_t, CcEntryAddr>> *res,
             CcProtocol proto = CcProtocol::OCC)
    {
        table_name_ = tname;
        key_ = nullptr;
        key_str_ = key_str;
        key_shard_code_ = key_shard_code;
        node_group_id_ = key_shard_code >> 10;
        txid_ = txid;
        tx_term_ = tx_term;
        ts_ = ts;
        is_insert_ = is_insert;
        res_ = res;
        ccm_ = nullptr;
        proto_ = proto;
    }

    bool Resume() override
    {
        return true;
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

private:
    const TxKey *key_;
    const std::string *key_str_;
    uint32_t key_shard_code_;
    const TxId *txid_;
    int64_t tx_term_;
    uint64_t ts_;
    bool is_insert_;
};

struct CommitCreateTableCC
    : public TemplatedCcRequest<CommitCreateTableCC, Void>
{
public:
    CommitCreateTableCC() : node_group_id_(0), catalog_str_("")
    {
    }

    CommitCreateTableCC(const CommitCreateTableCC &rhs) = delete;
    CommitCreateTableCC(CommitCreateTableCC &&rhs) = delete;

    virtual ~CommitCreateTableCC() = default;

    virtual bool Execute(CcShard &ccs) override
    {
        std::vector<std::string> tokens;
        std::string token;
        // full table name's format is "./dbname/tablename"
        // token[1] is dbname, token[2] is table name given '/' as splitter
        // FIXME: fix this hardcode
        std::istringstream tokenStream(*table_name_);
        while (std::getline(tokenStream, token, '/'))
        {
            tokens.push_back(token);
        }

        // only the local request and the first TxProcessor needs to create
        // table on Cassandra
        bool create_cass_table = is_local_req_ && (ccs.core_id_ == 0);
        // FIXME: handle the case that post process is failed. For example,
        // Cassandra is down.
        ccs.GetCatalog()->CreateTable(tokens[1],
                                      tokens[2],
                                      catalog_str_,
                                      ccs.core_id_,
                                      create_cass_table);

        res_->SetFinished();
        return true;
    }

    void Set(const TableName *tname,
             const std::string &catalog_str,
             uint32_t node_group_id,
             CcHandlerResult<Void> *res,
             bool is_local_req)
    {
        table_name_ = tname;
        catalog_str_ = catalog_str;
        node_group_id_ = node_group_id;
        res_ = res;
        is_local_req_ = is_local_req;
    }

    std::string &GetFrm()
    {
        return catalog_str_;
    }

    const std::string *GetFullTableName()
    {
        return table_name_;
    }

private:
    uint32_t node_group_id_;
    std::string catalog_str_;
    bool is_local_req_;
};

struct CommitDropTableCC : public TemplatedCcRequest<CommitDropTableCC, Void>
{
public:
    CommitDropTableCC() : node_group_id_(0)
    {
    }

    CommitDropTableCC(const CommitDropTableCC &rhs) = delete;
    CommitDropTableCC(CommitDropTableCC &&rhs) = delete;

    virtual ~CommitDropTableCC() = default;

    virtual bool Execute(CcShard &ccs) override
    {
        std::vector<std::string> tokens;
        std::string token;
        // full table name's format is "./dbname/tablename"
        // token[1] is dbname, token[2] is table name given '/' as splitter
        // FIXME: fix this hardcode
        std::istringstream tokenStream(*table_name_);
        while (std::getline(tokenStream, token, '/'))
        {
            tokens.push_back(token);
        }

        // only the local request and the first TxProcessor needs to drop table
        // on Cassandra
        bool drop_cass_table = is_local_req_ && (ccs.core_id_ == 0);

        ccs.GetCatalog()->DropTable(
            tokens[1], tokens[2], ccs.core_id_, drop_cass_table);

        // It's OK to remove table in ccm at here, but to align with create
        // table, we put the logic of removing table in ccm into
        // catalog->DropTable.
        // ccs.RemoveCcm(*table_name_);

        res_->SetFinished();
        return true;
    }

    void Set(const TableName *tname,
             uint32_t node_group_id,
             CcHandlerResult<Void> *res,
             bool is_local_req)
    {
        table_name_ = tname;
        node_group_id_ = node_group_id;
        res_ = res;
        is_local_req_ = is_local_req;
    }

    const std::string *GetFullTableName()
    {
        return table_name_;
    }

private:
    uint32_t node_group_id_;
    bool is_local_req_;
};

struct PostDeleteCc : public TemplatedCcRequest<PostDeleteCc, Void>
{
public:
    PostDeleteCc() : cce_addr_(nullptr), tx_number_(0)
    {
    }

    PostDeleteCc(const PostDeleteCc &rhs) = delete;
    PostDeleteCc(PostDeleteCc &&rhs) = delete;

    void Set(const CcEntryAddr *addr,
             uint64_t tx_number,
             CcHandlerResult<Void> *res)
    {
        cce_addr_ = addr;
        tx_number_ = tx_number;
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

    const CcEntryAddr *CceAddr() const
    {
        return cce_addr_;
    }

    uint64_t TxNumber() const
    {
        return tx_number_;
    }

private:
    const CcEntryAddr *cce_addr_;
    uint64_t tx_number_;
};

struct PostCommitCc : public TemplatedCcRequest<PostCommitCc, Void>
{
public:
    PostCommitCc()
        : cce_addr_(nullptr),
          tx_number_(0),
          commit_ts_(0),
          payload_(nullptr),
          payload_str_(nullptr),
          is_deleted_(false)
    {
    }

    PostCommitCc(const PostCommitCc &rhs) = delete;
    PostCommitCc(PostCommitCc &&rhs) = delete;

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

    uint64_t TxNumber() const
    {
        return tx_number_;
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
    uint64_t tx_number_;
    uint64_t commit_ts_;
    const TxRecord *payload_;
    const std::string *payload_str_;
    bool is_deleted_;
};

struct ValidateCc : public TemplatedCcRequest<ValidateCc, std::vector<TxId>>
{
public:
    ValidateCc()
        : cce_addr_(nullptr),
          tx_number_(0),
          commit_ts_(0),
          key_ts_(0),
          gap_ts_(0)
    {
    }

    ValidateCc(const ValidateCc &rhs) = delete;
    ValidateCc(ValidateCc &&rhs) = delete;

    void Set(const CcEntryAddr *addr,
             uint64_t tx_number,
             uint64_t commit_ts,
             uint64_t key_ts,
             uint64_t gap_ts,
             CcHandlerResult<std::vector<TxId>> *res)
    {
        cce_addr_ = addr;
        tx_number_ = tx_number;
        commit_ts_ = commit_ts;
        key_ts_ = key_ts;
        gap_ts_ = gap_ts;
        res_ = res;

        const LruEntry *lru_entry =
            reinterpret_cast<const LruEntry *>(addr->CcePtr());
        ccm_ = lru_entry->parent_map_;

        node_group_id_ = cce_addr_->NodeGroupId();
    }

    const CcEntryAddr *CceAddr() const
    {
        return cce_addr_;
    }

    uint64_t TxNumber() const
    {
        return tx_number_;
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
    uint64_t tx_number_;
    uint64_t commit_ts_;
    uint64_t key_ts_;
    uint64_t gap_ts_;
};

struct PostReadCc : public TemplatedCcRequest<PostReadCc, Void>
{
public:
    PostReadCc() : cce_addr_(nullptr), tx_number_(0)
    {
    }

    PostReadCc(const PostReadCc &rhs) = delete;
    PostReadCc(PostReadCc &&rhs) = delete;

    void Set(const CcEntryAddr *addr,
             uint64_t tx_number,
             CcHandlerResult<Void> *res,
             CcProtocol proto = CcProtocol::OCC)
    {
        cce_addr_ = addr;
        tx_number_ = tx_number;
        res_ = res;
        const LruEntry *lru_entry =
            reinterpret_cast<const LruEntry *>(addr->CcePtr());
        ccm_ = lru_entry->parent_map_;
        node_group_id_ = addr->NodeGroupId();
        proto_ = proto;
    }

    const CcEntryAddr *CceAddr() const
    {
        return cce_addr_;
    }

    uint64_t TxNumber() const
    {
        return tx_number_;
    }

private:
    const CcEntryAddr *cce_addr_;
    uint64_t tx_number_;
};

struct ReadCc
    : public TemplatedCcRequest<
          ReadCc,
          std::tuple<TxRecord *, uint64_t, CcEntryAddr, RecordStatus>>,
      Resumable
{
public:
    ReadCc()
        : key_(nullptr),
          key_str_(nullptr),
          rec_(nullptr),
          rec_str_(nullptr),
          tx_number_(0),
          ts_(0),
          type_(ReadType::Inside)
    {
    }

    ReadCc(const ReadCc &rhs) = delete;
    ReadCc(ReadCc &&rhs) = delete;

    void Set(
        const TableName *tn,
        const TxKey *key,
        uint32_t key_shard_code,
        TxRecord *rec,
        ReadType read_type,
        uint64_t tx_number,
        uint64_t ts,
        CcHandlerResult<
            std::tuple<TxRecord *, uint64_t, CcEntryAddr, RecordStatus>> *res,
        CcProtocol proto = CcProtocol::OCC)
    {
        key_ = key;
        key_str_ = nullptr;
        key_shard_code_ = key_shard_code;
        rec_ = rec;
        rec_str_ = nullptr;
        type_ = read_type;
        res_ = res;
        tx_number_ = tx_number;
        ts_ = ts;
        proto_ = proto;

        const CcEntryAddr &cce_addr = std::get<2>(res->Value());
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
    }

    void Set(
        const TableName *tn,
        const std::string *key_str,
        uint32_t key_shard_code,
        std::string *rec_str,
        ReadType read_type,
        uint64_t tx_number,
        uint64_t ts,
        CcHandlerResult<
            std::tuple<TxRecord *, uint64_t, CcEntryAddr, RecordStatus>> *res,
        CcProtocol proto = CcProtocol::OCC)
    {
        key_ = nullptr;
        key_str_ = key_str;
        key_shard_code_ = key_shard_code;
        rec_ = nullptr;
        rec_str_ = rec_str;
        type_ = read_type;
        res_ = res;
        tx_number_ = tx_number;
        ts_ = ts;
        proto_ = proto;

        const CcEntryAddr &cce_addr = std::get<2>(res->Value());
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
    }

    bool Resume() override
    {
        return true;
    }

    uint32_t KeyShardCode() const
    {
        return key_shard_code_;
    }

private:
    const TxKey *key_;
    const std::string *key_str_;
    uint32_t key_shard_code_;
    TxRecord *rec_;
    std::string *rec_str_;
    uint64_t tx_number_;
    uint64_t ts_;
    ReadType type_;

    template <typename KeyT, typename ValueT>
    friend class TemplateCcMap;

    template <typename SkT, typename PkT>
    friend class SkCcMap;
};

struct ScanOpenBatchCc
    : public TemplatedCcRequest<ScanOpenBatchCc,
                                std::pair<size_t, std::unique_ptr<CcScanner>>>
{
public:
    ScanOpenBatchCc()
        : index_type_(ScanIndexType::Primary),
          start_key_(nullptr),
          inclusive_(true),
          direct_(ScanDirection::Forward),
          ts_(0),
          scan_cache_(nullptr),
          is_ckpt_delta_(false)
    {
    }

    void Set(const TableName *tn,
             ScanIndexType type,
             uint32_t ng_id,
             const TxKey *start_key,
             bool inclusive,
             ScanDirection direction,
             uint64_t tx_number,
             const uint64_t &ts,
             ScanCache *cache,
             CcHandlerResult<std::pair<size_t, std::unique_ptr<CcScanner>>>
                 *open_res,
             const CcProtocol &proto,
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
        res_ = open_res;
        proto_ = proto;
        ccm_ = nullptr;
        is_ckpt_delta_ = is_delta;
    }

private:
    ScanIndexType index_type_;
    const TxKey *start_key_;
    bool inclusive_;
    ScanDirection direct_;
    uint64_t ts_;
    ScanCache *scan_cache_;
    bool is_ckpt_delta_;

    template <typename KeyT, typename ValueT>
    friend class TemplateCcMap;

    template <typename SkT, typename PkT>
    friend class SkCcMap;
};

struct ScanNextBatchCc : public TemplatedCcRequest<ScanNextBatchCc, uint32_t>
{
public:
    ScanNextBatchCc() : ts_(0), scan_cache_(nullptr), is_ckpt_delta_(false)
    {
    }

    void Set(const uint32_t &ng_id,
             const uint64_t &ts,
             ScanCache *cache,
             CcHandlerResult<uint32_t> *next_res,
             const CcProtocol &proto,
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
        proto_ = proto;
        is_ckpt_delta_ = is_delta;
    }

private:
    uint64_t ts_;
    ScanCache *scan_cache_;
    bool is_ckpt_delta_;

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
    CkptTsCc(int id)
        : ckpt_ts_(UINT64_MAX), mux_(), cv_(), finish_(true), id_(id)
    {
    }

    bool Execute(CcShard &ccs) override
    {
        ckpt_ts_ = ccs.ActiveTxMinTs();

        std::unique_lock<std::mutex> lk(mux_);
        assert(finish_ == false);
        finish_ = true;
        cv_.notify_one();

        // return false since CkptTsCc is not reused and does not need to call CcRequestBase::Free
        return false;
    }

    void Wait()
    {
        std::unique_lock<std::mutex> lk(mux_);
        if (!finish_)
        {
            cv_.wait(lk, [this] { return finish_; });
        }
    }

    void Reset()
    {
        std::lock_guard<std::mutex> lk(mux_);
        assert(finish_ == true);
        finish_ = false;
        ckpt_ts_ = UINT64_MAX;
    }

    uint64_t GetCkptTs() const
    {
        return ckpt_ts_;
    }

private:
    uint64_t ckpt_ts_;
    std::mutex mux_;
    std::condition_variable cv_;
    bool finish_;
    std::atomic<int> id_;
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
            return ccm_->Execute(*this);
        }
        else
        {
            Notify();
            // return false since CkptScanCc is not reused and does not need to call CcRequestBase::Free
            return false;
        }
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

/// <summary>
/// The post-processing request that commits a write to a secondary index.
/// Contrary to the primary index where a write consists of the acquiring phase
/// and the post-processing phase, isolation levels other than serializability
/// allows a write to the secondary index to skip the acquiring phase. This is
/// because concurrency control of the secondary index always traces back to the
/// primary index, which resolves all read-write and write-write conflicts. If a
/// tx has no conflicts on the primary index, it is allowed to commit and will
/// commit the change to the secondary index in post-processing. The only
/// exception is serializability which avoids phantom reads. To detect and
/// resolve phantom reads, a secondary index write needs the acquiring phase to
/// negotiate with index scans.
/// </summary>
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

struct FindCatalogCC : public TemplatedCcRequest<FindCatalogCC, bool>
{
public:
    FindCatalogCC() : catalog_content_(nullptr)
    {
    }

    FindCatalogCC(const FindCatalogCC &rhs) = delete;
    FindCatalogCC(FindCatalogCC &&rhs) = delete;

    virtual ~FindCatalogCC() = default;

    virtual bool Execute(CcShard &ccs) override
    {
        if (!ccs.AcquireTableReadIntention(*table_name_, this))
        {
            return false;
        }

        bool ret = ccs.FindCatalog(*table_name_, catalog_content_);

        res_->SetValue(ret);
        res_->SetFinished();
        return true;
    }

    void Set(const TableName *tname,
             std::string *catalog_content,
             uint64_t tx_number,
             CcHandlerResult<bool> *res)
    {
        table_name_ = tname;
        catalog_content_ = catalog_content;
        tx_number_ = tx_number;
        res_ = res;
    }

    std::string *GetCatalogContent()
    {
        return catalog_content_;
    }

private:
    std::string *catalog_content_;
};

struct CheckCatalogCC : public TemplatedCcRequest<CheckCatalogCC, bool>
{
public:
    CheckCatalogCC() : source_version_(nullptr)
    {
    }

    CheckCatalogCC(const CheckCatalogCC &rhs) = delete;
    CheckCatalogCC(CheckCatalogCC &&rhs) = delete;

    virtual ~CheckCatalogCC() = default;

    virtual bool Execute(CcShard &ccs) override
    {
        if (!ccs.AcquireTableReadIntention(*table_name_, this))
        {
            return false;
        }

        bool ret = ccs.CheckCatalogVersion(*table_name_, *source_version_);

        res_->SetValue(ret);
        res_->SetFinished();
        return true;
    }

    void Set(const TableName *tname,
             std::string *source_version,
             uint64_t tx_number,
             CcHandlerResult<bool> *res)
    {
        table_name_ = tname;
        source_version_ = source_version;
        tx_number_ = tx_number;
        res_ = res;
    }

    std::string *GetSourceVersion()
    {
        return source_version_;
    }

private:
    std::string *source_version_;
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
        TxLockInfo *lk_info = ccs.GetActiveTxLockInfo(tx_number_);

        if (lk_info != nullptr)
        {
            for (LruEntry *&lru_ptr : lk_info->cce_list_)
            {
                if (!lru_ptr->write_intention_.Empty() &&
                    lru_ptr->write_intention_.TxNumber() == tx_number_)
                {
                    lru_ptr->write_intention_.Reset();
                }
            }
        }

        ccs.DeleteLockHolidngTx(tx_number_);

        std::unique_lock<std::mutex> lk(mux_);
        ++finish_cnt_;
        wait_cv_.notify_one();

        // return false since ClearTxCc is not reused and does not need to call CcRequestBase::Free
        return false;
    }

    void Wait()
    {
        std::unique_lock<std::mutex> lk(mux_);
        wait_cv_.wait(lk, [this]() { return finish_cnt_ == core_cnt_; });
    }

private:
    uint64_t tx_number_;
    std::mutex mux_;
    std::condition_variable wait_cv_;
    uint32_t finish_cnt_;
    const uint32_t core_cnt_;
};

struct ReplayLogCc : public CcRequestBase
{
public:
    ReplayLogCc(LogType log_type,
                NodeGroupId ng_id,
                std::string &&table_name,
                std::string_view &&blob,
                uint64_t commit_ts,
                uint32_t core_cnt,
                std::mutex &mux,
                std::condition_variable &cv,
                uint32_t &finish_cnt)
        : log_type_(log_type),
          ng_id_(ng_id),
          table_name_str_(table_name),
          log_blob_view_(blob),
          commit_ts_(commit_ts),
          result_(nullptr),
          external_mux_(mux),
          external_cv_(cv),
          finish_cnt_(finish_cnt)
    {
        result_.SetRefCnt(core_cnt);
        result_.post_lambda_ = [this](CcHandlerResult<int8_t> *res)
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
        if (log_type_ == LogType::RECORD)
        {
            int8_t error_code = 0;
            CcMap *ccm = ccs.GetCcm(table_name_str_, ng_id_, error_code);
            if (error_code == -1)
            {
                // The specified node group in this node is not the leader. This
                // is possible when the leader of the cc node group quickly
                // transfers to another node, and this node has not finished
                // replaying the log.
                result_.SetError(-1);
                return true;
            }

            assert(ccm != nullptr);
            return ccm->Execute(*this);
        }
        else if (log_type_ == LogType::CREATE_TABLE ||
                 log_type_ == LogType::DROP_TABLE)
        {
            std::vector<std::string> tokens;
            std::string token;
            // full table name's format is "./dbname/tablename"
            // token[1] is dbname, token[2] is table name given '/' as splitter
            std::istringstream tokenStream(table_name_str_);
            while (std::getline(tokenStream, token, '/'))
            {
                tokens.push_back(token);
            }

            // only the first TxProcessor needs to create or drop table on
            // Cassandra.
            bool manipulate_cass_table = ccs.core_id_ == 0;

            // create or drop table in Cassandra.
            if (log_type_ == LogType::CREATE_TABLE)
            {
                std::string catalog_str(log_blob_view_.data(),
                                        log_blob_view_.length());
                ccs.GetCatalog()->CreateTable(tokens[1],
                                              tokens[2],
                                              catalog_str,
                                              ccs.core_id_,
                                              manipulate_cass_table);
            }
            else
            {
                ccs.GetCatalog()->DropTable(
                    tokens[1], tokens[2], ccs.core_id_, manipulate_cass_table);
            }

            SetFinish();
            return true;
        }
        return false;
    }

    void SetFinish()
    {
        result_.SetValue(0);
        result_.SetFinished();
    }

private:
    LogType log_type_;
    NodeGroupId ng_id_;
    std::string table_name_str_;
    std::string_view log_blob_view_;
    uint64_t commit_ts_;
    CcHandlerResult<int8_t> result_;
    std::mutex &external_mux_;
    std::condition_variable &external_cv_;
    uint32_t &finish_cnt_;

    template <typename KeyT, typename ValueT>
    friend class TemplateCcMap;

    template <typename SkT, typename PkT>
    friend class SkCcMap;
};

struct FaultInjectCC : public TemplatedCcRequest<FaultInjectCC, bool>
{
public:
    FaultInjectCC() : fault_name_(nullptr), fault_type_(nullptr)
    {
    }

    virtual ~FaultInjectCC() = default;

    FaultInjectCC(const FaultInjectCC &rhs) = delete;
    FaultInjectCC(FaultInjectCC &&rhs) = delete;

    virtual bool Execute(CcShard &ccs) override
    {
        FaultInject::Instance().InjectFault(
            *fault_name_, *fault_type_, "", "", 0, 0);
        res_->SetFinished();
        return true;
    }

    void Set(const std::string *fault_name,
             const std::string *fault_type,
             CcHandlerResult<bool> *res)
    {
        fault_name_ = fault_name;
        fault_type_ = fault_type;
        res_ = res;
    }

    const std::string *FaultName() const
    {
        return fault_name_;
    }

    const std::string *FaultType() const
    {
        return fault_type_;
    }

private:
    const std::string *fault_name_;
    const std::string *fault_type_;
};
}  // namespace txservice
