#pragma once

#include <atomic>

#include "cc/cc_request.h"
#include "moodycamelqueue.h"
#include "proto/cc_request.pb.h"
#include "type.h"

namespace txservice
{
template <typename KeyT, typename ValueT>
class TemplateCcMap;

template <typename SkT, typename PkT>
class SkCcMap;

namespace remote
{
class RemoteCcHandler;

struct RemoteAcquire : public AcquireCc
{
public:
    RemoteAcquire();

    RemoteAcquire(const RemoteAcquire &rhs) = delete;
    RemoteAcquire(RemoteAcquire &&rhs) = delete;

    void Set(std::unique_ptr<CcMessage> input_msg, RemoteCcHandler *hd)
    {
        assert(input_msg->has_acquire_req());

        cc_res_.Reset();

        output_msg_.clear_tx_number();
        output_msg_.clear_handler_addr();
        output_msg_.clear_acquire_resp();

        const AcquireRequest &req = input_msg->acquire_req();
        txid_obj_.Reset((uint32_t) (input_msg->tx_number() >> 32L),
                        (uint32_t) (input_msg->tx_number() & 0xFFFFFFFFL),
                        req.vec_idx());

        AcquireCc::Set(&req.tablename(),
                       &req.key(),
                       req.key_shard_code(),
                       &txid_obj_,
                       input_msg->tx_term(),
                       req.ts(),
                       req.insert(),
                       &cc_res_);

        input_msg_ = std::move(input_msg);
        hd_ = hd;
    }

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_;
    RemoteCcHandler *hd_;

    TxId txid_obj_;
    CcHandlerResult<std::pair<uint64_t, CcEntryAddr>> cc_res_;

    friend class RemoteCcHandler;
};

struct RemoteValidate : public ValidateCc
{
public:
    RemoteValidate();

    void Set(std::unique_ptr<CcMessage> input_msg, RemoteCcHandler *hd)
    {
        assert(input_msg->has_validate_req());

        cc_res_.Reset();

        output_msg_.clear_tx_number();
        output_msg_.clear_handler_addr();
        output_msg_.clear_validate_resp();

        const ValidateRequest &req = input_msg->validate_req();
        const CceAddr_msg &cce_addr = req.cce_addr();

        cce_addr_.SetCce(
            cce_addr.cce_ptr(), cce_addr.term(), req.node_group_id());

        ValidateCc::Set(&cce_addr_,
                        input_msg->tx_number(),
                        req.commit_ts(),
                        req.key_ts(),
                        req.gap_ts(),
                        &cc_res_);

        input_msg_ = std::move(input_msg);
        hd_ = hd;
    }

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_;
    RemoteCcHandler *hd_;

    CcEntryAddr cce_addr_;
    CcHandlerResult<std::vector<TxId>> cc_res_;

    friend class RemoteCcHandler;
};

/*
   RemoteAcquireTableWriteLock request adds table write lock on every shard of
   remote node. Every table DDL needs to acquire table write lock firstly.
 */
struct RemoteAcquireTableWriteLockCC
    : public TemplatedCcRequest<RemoteAcquireTableWriteLockCC, Void>
{
public:
    RemoteAcquireTableWriteLockCC();

    RemoteAcquireTableWriteLockCC(const RemoteAcquireTableWriteLockCC &rhs) =
        delete;
    RemoteAcquireTableWriteLockCC(RemoteAcquireTableWriteLockCC &&rhs) = delete;

    void Free() override;

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

    void Set(std::unique_ptr<CcMessage> input_msg,
             RemoteCcHandler *hd,
             uint32_t core_cnt,
             int64_t node_term)
    {
        assert(input_msg->has_acquire_table_req());

        res_->Reset();
        res_->SetRefCnt(core_cnt);

        output_msg_.clear_tx_number();
        output_msg_.clear_handler_addr();
        output_msg_.clear_acquire_table_resp();

        const AcquireTableWriteLockRequest &req =
            input_msg->acquire_table_req();
        txid_obj_.Reset((uint32_t) (input_msg->tx_number() >> 32L),
                        (uint32_t) (input_msg->tx_number() & 0xFFFFFFFFL),
                        req.vec_idx());

        table_name_ = &req.tablename();
        tx_number_ = req.tx_number();
        node_group_id_ = req.node_group_id();

        unfinish_cnt_.store(core_cnt);

        input_msg_ = std::move(input_msg);
        hd_ = hd;
        node_term_ = node_term;
    }

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_;
    RemoteCcHandler *hd_;

    TxId txid_obj_;
    int64_t node_term_;
    CcHandlerResult<Void> cc_res_;
    std::atomic<uint32_t> unfinish_cnt_;

    friend class RemoteCcHandler;
};

/*
  RemoteReleaseTableWriteLock request release table write lock on every shard of
  remote node. Every table DDL needs to release table write lock at the end of
  postprocess.
*/
struct RemoteReleaseTableWriteLock : public ReleaseTableWriteLockCC
{
public:
    RemoteReleaseTableWriteLock();

    RemoteReleaseTableWriteLock(const RemoteReleaseTableWriteLock &rhs) =
        delete;
    RemoteReleaseTableWriteLock(RemoteReleaseTableWriteLock &&rhs) = delete;

    void Free() override;

    void Set(std::unique_ptr<CcMessage> input_msg,
             RemoteCcHandler *hd,
             uint32_t core_cnt)
    {
        assert(input_msg->has_release_table_req());

        cc_res_.Reset();
        cc_res_.SetRefCnt(core_cnt);

        output_msg_.clear_tx_number();
        output_msg_.clear_handler_addr();
        output_msg_.clear_release_table_resp();

        const ReleaseTableWriteLockRequest &req =
            input_msg->release_table_req();
        txid_obj_.Reset((uint32_t) (input_msg->tx_number() >> 32L),
                        (uint32_t) (input_msg->tx_number() & 0xFFFFFFFFL),
                        req.vec_idx());

        ReleaseTableWriteLockCC::Set(&req.tablename(),
                                     &txid_obj_,
                                     req.tx_number(),
                                     req.node_group_id(),
                                     &cc_res_);

        unfinish_cnt_.store(core_cnt);

        input_msg_ = std::move(input_msg);
        hd_ = hd;
    }

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_;
    RemoteCcHandler *hd_;

    TxId txid_obj_;
    uint32_t node_group_id_;
    CcHandlerResult<Void> cc_res_;
    std::atomic<uint32_t> unfinish_cnt_;

    friend class RemoteCcHandler;
};

/*
  Postprocess of create table.
 */
struct RemoteCommitCreateTable : public CommitCreateTableCC
{
public:
    RemoteCommitCreateTable();

    RemoteCommitCreateTable(const RemoteCommitCreateTable &rhs) = delete;
    RemoteCommitCreateTable(RemoteCommitCreateTable &&rhs) = delete;

    void Free() override;

    void Set(std::unique_ptr<CcMessage> input_msg,
             RemoteCcHandler *hd,
             uint32_t core_cnt)
    {
        assert(input_msg->has_commit_create_table_req());

        cc_res_.Reset();
        cc_res_.SetRefCnt(core_cnt);

        output_msg_.clear_tx_number();
        output_msg_.clear_handler_addr();

        const CommitCreateTableRequest &req =
            input_msg->commit_create_table_req();

        bool is_local_req = false;
        CommitCreateTableCC::Set(&req.tablename(),
                                 req.catalog_str(),
                                 req.node_group_id(),
                                 &cc_res_,
                                 is_local_req);

        unfinish_cnt_.store(core_cnt);

        input_msg_ = std::move(input_msg);
        hd_ = hd;
    }

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_;
    RemoteCcHandler *hd_;

    CcHandlerResult<Void> cc_res_;
    std::atomic<uint32_t> unfinish_cnt_;

    friend class RemoteCcHandler;
};

/*
  Postprocess of drop table.
  clear ccm entry, drop table in Cassandra on runtime.
 */
struct RemoteCommitDropTable : public CommitDropTableCC
{
public:
    RemoteCommitDropTable();

    RemoteCommitDropTable(const RemoteCommitDropTable &rhs) = delete;
    RemoteCommitDropTable(RemoteCommitDropTable &&rhs) = delete;

    void Free() override;

    void Set(std::unique_ptr<CcMessage> input_msg,
             RemoteCcHandler *hd,
             uint32_t core_cnt)
    {
        assert(input_msg->has_commit_drop_table_req());

        cc_res_.Reset();
        cc_res_.SetRefCnt(core_cnt);

        output_msg_.clear_tx_number();
        output_msg_.clear_handler_addr();

        const CommitDropTableRequest &req = input_msg->commit_drop_table_req();

        bool is_local_req = false;
        CommitDropTableCC::Set(
            &req.tablename(), req.node_group_id(), &cc_res_, is_local_req);

        unfinish_cnt_.store(core_cnt);

        input_msg_ = std::move(input_msg);
        hd_ = hd;
    }

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_;
    RemoteCcHandler *hd_;

    CcHandlerResult<Void> cc_res_;
    std::atomic<uint32_t> unfinish_cnt_;

    friend class RemoteCcHandler;
};

struct RemotePostRead : public PostReadCc
{
public:
    RemotePostRead();

    void Set(std::unique_ptr<CcMessage> input_msg, RemoteCcHandler *hd)
    {
        assert(input_msg->has_postread_req());

        cc_res_.Reset();

        output_msg_.clear_tx_number();
        output_msg_.clear_handler_addr();
        output_msg_.clear_post_resp();

        const PostReadRequest &req = input_msg->postread_req();
        const CceAddr_msg &cce_addr = req.cce_addr();
        cce_addr_.SetCce(
            cce_addr.cce_ptr(), cce_addr.term(), req.node_group_id());

        PostReadCc::Set(&cce_addr_, input_msg->tx_number(), &cc_res_);

        input_msg_ = std::move(input_msg);
        hd_ = hd;
    }

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_;
    RemoteCcHandler *hd_;

    CcEntryAddr cce_addr_;
    CcHandlerResult<Void> cc_res_;

    friend class RemoteCcHandler;
};

struct RemoteRead : public ReadCc
{
public:
    RemoteRead();

    void Set(std::unique_ptr<CcMessage> input_msg, RemoteCcHandler *hd)
    {
        assert(input_msg->has_read_req());

        cc_res_.Reset();

        output_msg_.clear_tx_number();
        output_msg_.clear_handler_addr();
        output_msg_.clear_read_resp();

        const ReadRequest &req = input_msg->read_req();
        ReadType read_type = ReadType::Inside;
        switch (req.read_type())
        {
        case ReadRequest_ReadType::ReadRequest_ReadType_INSIDE:
            read_type = ReadType::Inside;
            break;
        case ReadRequest_ReadType::ReadRequest_ReadType_OUTSIDE_NORMAL:
            read_type = ReadType::OutsideNormal;
            break;
        case ReadRequest_ReadType::ReadRequest_ReadType_OUTSIDE_DELETED:
            read_type = ReadType::OutsideDeleted;
            break;
        default:
            break;
        }

        CcEntryAddr &cce_addr = std::get<2>(cc_res_.Value());
        cce_addr.SetCce(0, 0, req.key_shard_code() >> 10);

        ReadResponse *resp = output_msg_.mutable_read_resp();
        resp->clear_record();
        if (read_type == ReadType::Inside)
        {
            ReadCc::Set(&req.tablename(),
                        &req.key(),
                        req.key_shard_code(),
                        resp->mutable_record(),
                        read_type,
                        input_msg->tx_number(),
                        req.ts(),
                        &cc_res_);
        }
        else
        {
            // The read brings in an external record (from the data store) for
            // concurrency control

            std::string *out_record = resp->mutable_record();
            *out_record = req.record();

            ReadCc::Set(&req.tablename(),
                        &req.key(),
                        req.key_shard_code(),
                        out_record,
                        read_type,
                        input_msg->tx_number(),
                        req.ts(),
                        &cc_res_);
        }

        input_msg_ = std::move(input_msg);
        hd_ = hd;
    }

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_;
    RemoteCcHandler *hd_;

    CcHandlerResult<std::tuple<TxRecord *, uint64_t, CcEntryAddr, RecordStatus>>
        cc_res_;

    friend class RemoteCcHandler;
};

struct RemoteReadOutside : public CcRequestBase
{
public:
    RemoteReadOutside();

    void Set(std::unique_ptr<CcMessage> input_msg, RemoteCcHandler *hd)
    {
        assert(input_msg->has_read_outside_req());

        const ReadOutsideRequest &req = input_msg->read_outside_req();

        assert(req.cce_addr().cce_ptr() != 0);
        cce_addr_.SetCce(req.cce_addr().cce_ptr(),
                         req.cce_addr().term(),
                         req.node_group_id());
        is_deleted_ = req.is_deleted();
        rec_str_ = &req.record();

        input_msg_ = std::move(input_msg);
        hd_ = hd;
    }

    CcMap *Ccm()
    {
        LruEntry *lru_entry_ = reinterpret_cast<LruEntry *>(cce_addr_.CcePtr());
        return lru_entry_->parent_map_;
    }

    bool Execute(CcShard &ccs) override
    {
        return Ccm()->Execute(*this);
    }

    void Finish();

private:
    std::unique_ptr<CcMessage> input_msg_;
    RemoteCcHandler *hd_;

    const std::string *rec_str_;
    bool is_deleted_;
    CcEntryAddr cce_addr_;

    friend class RemoteCcHandler;

    template <typename KeyT, typename ValueT>
    friend class ::txservice::TemplateCcMap;

    template <typename SkT, typename PkT>
    friend class ::txservice::SkCcMap;
};

struct RemotePostCommit : public PostCommitCc
{
public:
    RemotePostCommit();

    void Set(std::unique_ptr<CcMessage> input_msg, RemoteCcHandler *hd)
    {
        assert(input_msg->has_postcommit_req());

        cc_res_.Reset();

        output_msg_.clear_tx_number();
        output_msg_.clear_handler_addr();
        output_msg_.clear_post_resp();

        const PostCommitRequest &post_commit = input_msg->postcommit_req();
        const CceAddr_msg &cce_addr_msg = post_commit.cce_addr();

        if (cce_addr_msg.entry_ptr_case() ==
            CceAddr_msg::EntryPtrCase::kInsertPtr)
        {
            cce_addr_.SetInsert(cce_addr_msg.insert_ptr(),
                                cce_addr_msg.term(),
                                post_commit.node_group_id());
        }
        else
        {
            cce_addr_.SetCce(cce_addr_msg.cce_ptr(),
                             cce_addr_msg.term(),
                             post_commit.node_group_id());
        }

        PostCommitCc::Set(&cce_addr_,
                          input_msg->tx_number(),
                          post_commit.commit_ts(),
                          &post_commit.record(),
                          post_commit.is_deleted(),
                          &cc_res_);

        input_msg_ = std::move(input_msg);
        hd_ = hd;
    }

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_;
    RemoteCcHandler *hd_;

    CcEntryAddr cce_addr_;
    CcHandlerResult<Void> cc_res_;

    friend class RemoteCcHandler;
};

struct RemotePostDelete : public PostDeleteCc
{
public:
    RemotePostDelete();

    RemotePostDelete(const RemotePostDelete &rhs) = delete;
    RemotePostDelete(RemotePostDelete &&rhs) = delete;

    void Set(std::unique_ptr<CcMessage> input_msg, RemoteCcHandler *hd)
    {
        assert(input_msg->has_postdelete_req());

        cc_res_.Reset();

        output_msg_.clear_tx_number();
        output_msg_.clear_handler_addr();
        output_msg_.clear_post_resp();

        const PostDeleteRequest &post_delete = input_msg->postdelete_req();
        const CceAddr_msg &cce_addr_msg = post_delete.cce_addr();

        if (cce_addr_msg.entry_ptr_case() ==
            CceAddr_msg::EntryPtrCase::kInsertPtr)
        {
            cce_addr_.SetInsert(cce_addr_msg.insert_ptr(),
                                cce_addr_msg.term(),
                                post_delete.node_group_id());
        }
        else
        {
            cce_addr_.SetCce(cce_addr_msg.cce_ptr(),
                             cce_addr_msg.term(),
                             post_delete.node_group_id());
        }
        PostDeleteCc::Set(&cce_addr_, input_msg->tx_number(), &cc_res_);

        input_msg_ = std::move(input_msg);
        hd_ = hd;
    }

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_;
    RemoteCcHandler *hd_;

    CcEntryAddr cce_addr_;
    CcHandlerResult<Void> cc_res_;

    friend class RemoteCcHandler;
};

struct RemoteScanOpen : public TemplatedCcRequest<RemoteScanOpen, Void>
{
public:
    RemoteScanOpen();

    void Set(std::unique_ptr<CcMessage> input_msg,
             RemoteCcHandler *hd,
             uint32_t core_cnt);

    void Free() override;

    bool Execute(CcShard &ccs) override
    {
        int8_t err_code = 0;
        ccm_ = ccs.GetCcm(*table_name_, node_group_id_, err_code);

        if (ccm_ == nullptr)
        {
            cc_res_.SetError(err_code);
            return true;
        }
        else
        {
            return ccm_->Execute(*this);
        }
    }

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_;
    RemoteCcHandler *hd_;

    uint32_t node_group_id_;
    KeyType key_type_;
    const std::string *start_key_str_;
    bool inclusive_;
    ScanDirection direct_;
    std::vector<std::vector<ScanTuple_msg *>> scan_caches_;
    bool is_ckpt_delta_;
    CcHandlerResult<Void> cc_res_;
    std::atomic<uint32_t> unfinish_cnt_;

    template <typename KeyT, typename ValueT>
    friend class ::txservice::TemplateCcMap;

    template <typename SkT, typename PkT>
    friend class ::txservice::SkCcMap;
};

struct RemoteScanNextBatch
    : public TemplatedCcRequest<RemoteScanNextBatch, Void>
{
public:
    RemoteScanNextBatch();

    void Set(std::unique_ptr<CcMessage> input_msg, RemoteCcHandler *hd);

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_;
    RemoteCcHandler *hd_;

    uint32_t node_group_id_;
    uint64_t prior_cce_addr_;
    ScanDirection direct_;
    std::vector<ScanTuple_msg *> scan_cache_;
    bool is_ckpt_delta_;
    // The address of the CC map of the blocked core.
    CcHandlerResult<Void> cc_res_;

    template <typename KeyT, typename ValueT>
    friend class ::txservice::TemplateCcMap;

    template <typename SkT, typename PkT>
    friend class ::txservice::SkCcMap;

    friend class RemoteCcHandler;
};

struct RemoteCommitSk : public CommitSkCc
{
public:
    RemoteCommitSk();

    void Set(std::unique_ptr<CcMessage> input_msg, RemoteCcHandler *hd)
    {
        assert(input_msg->has_commit_sk_req());

        cc_res_.Reset();

        output_msg_.clear_tx_number();
        output_msg_.clear_handler_addr();
        output_msg_.clear_read_resp();

        const CommitSkRequest &req = input_msg->commit_sk_req();

        CommitSkCc::Set(&req.tablename(),
                        &req.sk(),
                        req.key_shard_code(),
                        &req.pk(),
                        req.ts(),
                        req.is_deleted(),
                        &cc_res_);

        input_msg_ = std::move(input_msg);
        hd_ = hd;
    }

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_;
    RemoteCcHandler *hd_;

    CcHandlerResult<Void> cc_res_;

    template <typename SkT, typename PkT>
    friend class ::txservice::SkCcMap;

    friend class RemoteCcHandler;
};

struct RemoteFaultInjectCC : public FaultInjectCC
{
public:
    RemoteFaultInjectCC();

    RemoteFaultInjectCC(const RemoteFaultInjectCC &rhs) = delete;
    RemoteFaultInjectCC(RemoteFaultInjectCC &&rhs) = delete;

    void Set(std::unique_ptr<CcMessage> input_msg, RemoteCcHandler *hd)
    {
        assert(input_msg->has_fault_inject_req());

        cc_res_.Reset();

        output_msg_.clear_tx_number();
        output_msg_.clear_handler_addr();
        output_msg_.clear_acquire_resp();

        const FaultInjectRequest &req = input_msg->fault_inject_req();

        FaultInjectCC::Set(&req.fault_name(), &req.fault_type(), &cc_res_);

        input_msg_ = std::move(input_msg);
        hd_ = hd;
    }

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_;
    RemoteCcHandler *hd_;

    CcHandlerResult<bool> cc_res_;

    friend class RemoteCcHandler;
};
}  // namespace remote
}  // namespace txservice
