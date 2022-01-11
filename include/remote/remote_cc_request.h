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
class CcStreamSender;

struct RemoteAcquire : public AcquireCc
{
public:
    RemoteAcquire();
    RemoteAcquire(const RemoteAcquire &rhs) = delete;
    RemoteAcquire(RemoteAcquire &&rhs) = delete;
    void Set(std::unique_ptr<CcMessage> input_msg);
    void Acknowledge();

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_{nullptr};
    CcStreamSender *hd_{nullptr};

    TxId txid_obj_;
    CcHandlerResult<AcquireKeyResult> cc_res_{nullptr};
};

struct RemotePostRead : public PostReadCc
{
public:
    RemotePostRead();
    void Set(std::unique_ptr<CcMessage> input_msg);

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_{nullptr};
    CcStreamSender *hd_{nullptr};

    CcEntryAddr cce_addr_;
    CcHandlerResult<std::vector<TxId>> cc_res_{nullptr};
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
             uint32_t core_cnt,
             int64_t node_term);

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_{nullptr};
    CcStreamSender *hd_{nullptr};

    TxId txid_obj_;
    int64_t node_term_{-1};
    CcHandlerResult<Void> cc_res_{nullptr};
    std::atomic<uint32_t> unfinish_cnt_{0};
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

    void Set(std::unique_ptr<CcMessage> input_msg, uint32_t core_cnt);

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_{nullptr};
    CcStreamSender *hd_{nullptr};

    TxId txid_obj_;
    uint32_t node_group_id_{0};
    CcHandlerResult<Void> cc_res_{nullptr};
    std::atomic<uint32_t> unfinish_cnt_{0};
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

    void Set(std::unique_ptr<CcMessage> input_msg, uint32_t core_cnt);

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_{nullptr};
    CcStreamSender *hd_{nullptr};

    CcHandlerResult<Void> cc_res_{nullptr};
    std::atomic<uint32_t> unfinish_cnt_{0};
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

    void Set(std::unique_ptr<CcMessage> input_msg, uint32_t core_cnt);

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_{nullptr};
    CcStreamSender *hd_{nullptr};

    CcHandlerResult<Void> cc_res_{nullptr};
    std::atomic<uint32_t> unfinish_cnt_{0};
};

struct RemoteRead : public ReadCc
{
public:
    RemoteRead();
    void Set(std::unique_ptr<CcMessage> input_msg);
    void Acknowledge();

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_{nullptr};
    CcStreamSender *hd_{nullptr};
    CcHandlerResult<ReadKeyResult> cc_res_{nullptr};
};

struct RemoteReadOutside : public CcRequestBase
{
public:
    RemoteReadOutside() = default;
    void Set(std::unique_ptr<CcMessage> input_msg);

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

    const CcEntryAddr &CceAddr() const
    {
        return cce_addr_;
    }

private:
    std::unique_ptr<CcMessage> input_msg_{nullptr};
    CcStreamSender *hd_{nullptr};

    const std::string *rec_str_{nullptr};
    bool is_deleted_{false};
    CcEntryAddr cce_addr_;

    template <typename KeyT, typename ValueT>
    friend class ::txservice::TemplateCcMap;

    template <typename SkT, typename PkT>
    friend class ::txservice::SkCcMap;
};

struct RemotePostWrite : public PostWriteCc
{
public:
    RemotePostWrite();
    void Set(std::unique_ptr<CcMessage> input_msg);

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_{nullptr};
    CcStreamSender *hd_{nullptr};

    CcEntryAddr cce_addr_;
    CcHandlerResult<Void> cc_res_{nullptr};
};

struct RemoteScanOpen : public TemplatedCcRequest<RemoteScanOpen, Void>
{
public:
    RemoteScanOpen();

    void Set(std::unique_ptr<CcMessage> input_msg, uint32_t core_cnt);

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
    std::unique_ptr<CcMessage> input_msg_{nullptr};
    CcStreamSender *hd_{nullptr};

    uint32_t node_group_id_{0};
    KeyType key_type_{KeyType::Normal};
    const std::string *start_key_str_{nullptr};
    bool inclusive_{true};
    ScanDirection direct_{ScanDirection::Forward};
    std::vector<std::vector<ScanTuple_msg *>> scan_caches_;
    bool is_ckpt_delta_{false};
    CcHandlerResult<Void> cc_res_{nullptr};
    std::atomic<uint32_t> unfinish_cnt_{0};
    IsolationLevel iso_level_{IsolationLevel::ReadCommitted};
    CcProtocol protocol_{CcProtocol::OCC};

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
    void Set(std::unique_ptr<CcMessage> input_msg);

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_{nullptr};
    CcStreamSender *hd_{nullptr};

    uint32_t node_group_id_{0};
    uint64_t prior_cce_addr_{0};
    ScanDirection direct_{ScanDirection::Forward};
    std::vector<ScanTuple_msg *> scan_cache_;
    bool is_ckpt_delta_{false};
    // The address of the CC map of the blocked core.
    CcHandlerResult<Void> cc_res_{nullptr};
    IsolationLevel iso_level_{IsolationLevel::ReadCommitted};
    CcProtocol protocol_{CcProtocol::OCC};

    template <typename KeyT, typename ValueT>
    friend class ::txservice::TemplateCcMap;

    template <typename SkT, typename PkT>
    friend class ::txservice::SkCcMap;
};

struct RemoteCommitSk : public CommitSkCc
{
public:
    RemoteCommitSk();
    void Set(std::unique_ptr<CcMessage> input_msg);

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_{nullptr};
    CcStreamSender *hd_{nullptr};
    CcHandlerResult<Void> cc_res_{nullptr};

    template <typename SkT, typename PkT>
    friend class ::txservice::SkCcMap;
};

struct RemoteFaultInjectCC : public FaultInjectCC
{
public:
    RemoteFaultInjectCC();

    RemoteFaultInjectCC(const RemoteFaultInjectCC &rhs) = delete;
    RemoteFaultInjectCC(RemoteFaultInjectCC &&rhs) = delete;

    void Set(std::unique_ptr<CcMessage> input_msg);

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_;
    CcStreamSender *hd_{nullptr};

    CcHandlerResult<bool> cc_res_{nullptr};

    friend class RemoteCcHandler;
};
}  // namespace remote
}  // namespace txservice
