#pragma once

#include <atomic>

#include "butil/logging.h"
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

struct RemoteAcquireAll : public AcquireAllCc
{
public:
    RemoteAcquireAll();
    RemoteAcquireAll(const RemoteAcquireAll &rhs) = delete;
    RemoteAcquireAll(RemoteAcquireAll &&rhs) = delete;
    void Set(std::unique_ptr<CcMessage> input_msg);
    void Acknowledge();

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_{nullptr};
    CcStreamSender *hd_{nullptr};

    CcHandlerResult<AcquireAllResult> cc_res_{nullptr};
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

struct RemotePostWriteAll : public PostWriteAllCc
{
public:
    RemotePostWriteAll();
    void Set(std::unique_ptr<CcMessage> input_msg);

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_{nullptr};
    CcStreamSender *hd_{nullptr};

    CcHandlerResult<Void> cc_res_{nullptr};
};

struct RemoteScanOpen : public TemplatedCcRequest<RemoteScanOpen, Void>
{
public:
    RemoteScanOpen();

    void Set(std::unique_ptr<CcMessage> input_msg, uint32_t core_cnt);
    void Free() override;

    int64_t TxTerm()
    {
        return tx_term_;
    }

    LockType GetLockType()
    {
        return lock_type_;
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
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_{nullptr};
    CcStreamSender *hd_{nullptr};

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
    int64_t tx_term_{0};
    enum LockType lock_type_
    {
        LockType::NoLock
    };

    // The pointer of the cc entry to which this request is directed. The
    // pointer is set, when the request locates the cc entry but is
    // blocked due to conflicts in 2PL. After the request is unblocked and
    // acquires the lock, the request's execution resumes without further lookup
    // of the cc entry.
    LruEntry *cce_ptr_{nullptr};

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

    int64_t TxTerm()
    {
        return tx_term_;
    }

    LockType GetLockType()
    {
        return lock_type_;
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
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_{nullptr};
    CcStreamSender *hd_{nullptr};

    uint64_t prior_cce_addr_{0};
    ScanDirection direct_{ScanDirection::Forward};
    std::vector<ScanTuple_msg *> scan_cache_;
    bool is_ckpt_delta_{false};
    // The address of the CC map of the blocked core.
    CcHandlerResult<Void> cc_res_{nullptr};
    IsolationLevel iso_level_{IsolationLevel::ReadCommitted};
    CcProtocol protocol_{CcProtocol::OCC};
    int64_t tx_term_{0};
    enum LockType lock_type_
    {
        LockType::NoLock
    };

    // The pointer of the cc entry to which this request is directed. The
    // pointer is set, when the request locates the cc entry but is
    // blocked due to conflicts in 2PL. After the request is unblocked and
    // acquires the lock, the request's execution resumes without further lookup
    // of the cc entry.
    LruEntry *cce_ptr_{nullptr};

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
