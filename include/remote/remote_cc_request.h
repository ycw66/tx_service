#pragma once

#include <atomic>
#include <memory>  // unique_ptr

#include "butil/logging.h"
#include "cc/cc_request.h"
#include "moodycamelqueue.h"
#include "proto/cc_request.pb.h"
#include "tx_record.h"  // RecordStatus
#include "type.h"

namespace txservice
{
template <typename KeyT, typename ValueT>
class TemplateCcMap;

template <typename SkT, typename PkT>
class SkCcMap;

class CcMap;

namespace remote
{
class CcStreamSender;

struct RemoteAcquire : public AcquireCc
{
public:
    RemoteAcquire();
    RemoteAcquire(const RemoteAcquire &rhs) = delete;
    RemoteAcquire(RemoteAcquire &&rhs) = delete;
    void Reset(std::unique_ptr<CcMessage> input_msg);
    void Acknowledge();
    uint64_t handler_addr()
    {
        if (input_msg_)
        {
            return input_msg_->handler_addr();
        }
        else
        {
            return 0;
        }
    }

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_{nullptr};
    CcStreamSender *hd_{nullptr};
    TableName remote_table_name_{empty_sv, TableType::Primary};

    CcHandlerResult<std::vector<AcquireKeyResult>> cc_res_{nullptr};
};

struct RemoteAcquireAll : public AcquireAllCc
{
public:
    RemoteAcquireAll();
    RemoteAcquireAll(const RemoteAcquireAll &rhs) = delete;
    RemoteAcquireAll(RemoteAcquireAll &&rhs) = delete;
    void Reset(std::unique_ptr<CcMessage> input_msg);
    void Acknowledge();
    uint64_t handler_addr()
    {
        if (input_msg_)
        {
            return input_msg_->handler_addr();
        }
        else
        {
            return 0;
        }
    }

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_{nullptr};
    CcStreamSender *hd_{nullptr};
    TableName remote_table_name_{empty_sv, TableType::Primary};

    CcHandlerResult<AcquireAllResult> cc_res_{nullptr};
};

struct RemotePostRead : public PostReadCc
{
public:
    RemotePostRead();
    void Reset(std::unique_ptr<CcMessage> input_msg);
    uint64_t handler_addr()
    {
        if (input_msg_)
        {
            return input_msg_->handler_addr();
        }
        else
        {
            return 0;
        }
    }

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_{nullptr};
    CcStreamSender *hd_{nullptr};
    TableName remote_table_name_{empty_sv, TableType::Primary};

    CcEntryAddr cce_addr_;
    CcHandlerResult<PostProcessResult> cc_res_{nullptr};
};

struct RemoteRead : public ReadCc
{
public:
    RemoteRead();
    void Reset(std::unique_ptr<CcMessage> input_msg);
    void Acknowledge();
    uint64_t handler_addr()
    {
        if (input_msg_)
        {
            return input_msg_->handler_addr();
        }
        else
        {
            return 0;
        }
    }

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_{nullptr};
    CcStreamSender *hd_{nullptr};
    TableName remote_table_name_{empty_sv, TableType::Primary};
    CcHandlerResult<ReadKeyResult> cc_res_{nullptr};
};

struct RemoteReadOutside : public CcRequestBase
{
public:
    RemoteReadOutside() = default;
    void Reset(std::unique_ptr<CcMessage> input_msg);
    uint64_t handler_addr()
    {
        if (input_msg_)
        {
            return input_msg_->handler_addr();
        }
        else
        {
            return 0;
        }
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

    const CcEntryAddr &CceAddr() const
    {
        return cce_addr_;
    }

    uint64_t CommitTs() const
    {
        return commit_ts_;
    }

    ::txservice::RecordStatus RecordStatus() const
    {
        return rec_status_;
    }

private:
    std::unique_ptr<CcMessage> input_msg_{nullptr};
    CcStreamSender *hd_{nullptr};

    const std::string *rec_str_{nullptr};
    ::txservice::RecordStatus rec_status_;

    uint64_t commit_ts_{0};
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
    void Reset(std::unique_ptr<CcMessage> input_msg);
    uint64_t handler_addr()
    {
        if (input_msg_)
        {
            return input_msg_->handler_addr();
        }
        else
        {
            return 0;
        }
    }

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_{nullptr};
    CcStreamSender *hd_{nullptr};
    TableName remote_table_name_{empty_sv, TableType::Primary};

    CcEntryAddr cce_addr_;
    CcHandlerResult<PostProcessResult> cc_res_{nullptr};
};

struct RemotePostWriteAll : public PostWriteAllCc
{
public:
    RemotePostWriteAll();
    void Reset(std::unique_ptr<CcMessage> input_msg);
    uint64_t handler_addr()
    {
        if (input_msg_)
        {
            return input_msg_->handler_addr();
        }
        else
        {
            return 0;
        }
    }

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_{nullptr};
    CcStreamSender *hd_{nullptr};
    TableName remote_table_name_{empty_sv, TableType::Primary};

    CcHandlerResult<PostProcessResult> cc_res_{nullptr};
};

struct RemoteScanOpen : public TemplatedCcRequest<RemoteScanOpen, Void>
{
public:
    RemoteScanOpen();

    void Reset(std::unique_ptr<CcMessage> input_msg, uint32_t core_cnt);
    void Free() override;
    uint64_t handler_addr()
    {
        if (input_msg_)
        {
            return input_msg_->handler_addr();
        }
        else
        {
            return 0;
        }
    }

    int64_t TxTerm()
    {
        return tx_term_;
    }

    uint16_t CommandId()
    {
        return input_msg_->command_id();
    }

    bool IsForWrite() const
    {
        return is_for_write_;
    }

    uint64_t ReadTimestamp() const
    {
        return snapshot_ts_;
    }

    void SetCcePtr(LruEntry *ptr, int core_id)
    {
        cce_ptr_.at(core_id) = ptr;
    }

    LruEntry *CcePtr(int core_id) const
    {
        return cce_ptr_[core_id];
    }

    ScanType CcePtrScanType(int core_id)
    {
        return cce_ptr_scan_type_[core_id];
    }

    void SetCcePtrScanType(ScanType scan_type, int core_id)
    {
        cce_ptr_scan_type_[core_id] = scan_type;
    }

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_{nullptr};
    CcStreamSender *hd_{nullptr};
    TableName remote_table_name_{empty_sv, TableType::Primary};

    KeyType key_type_{KeyType::Normal};
    const std::string *start_key_str_{nullptr};
    bool inclusive_{true};
    ScanDirection direct_{ScanDirection::Forward};
    std::vector<std::vector<ScanTuple_msg *>> scan_caches_;
    // tuple index of every core's scan_cache in {scan_caches_}
    std::vector<size_t> scan_caches_idxs_;
    bool is_ckpt_delta_{false};
    CcHandlerResult<Void> cc_res_{nullptr};
    std::atomic<uint32_t> unfinish_cnt_{0};
    int64_t tx_term_{0};
    bool is_for_write_{false};

    uint64_t snapshot_ts_{0};

    // The pointer of the cc entry to which this request is directed. The
    // pointer is set, when the request locates the cc entry but is
    // blocked due to conflicts in 2PL. After the request is unblocked and
    // acquires the lock, the request's execution resumes without further lookup
    // of the cc entry.
    std::vector<LruEntry *> cce_ptr_;
    // scan type for above cce_ptr_
    std::vector<ScanType> cce_ptr_scan_type_;

    template <typename KeyT, typename ValueT>
    friend class ::txservice::TemplateCcMap;

    template <typename SkT, typename PkT>
    friend class ::txservice::SkCcMap;

    friend class ::txservice::CcMap;
};

struct RemoteScanNextBatch
    : public TemplatedCcRequest<RemoteScanNextBatch, Void>
{
public:
    RemoteScanNextBatch();
    void Reset(std::unique_ptr<CcMessage> input_msg);

    uint64_t handler_addr()
    {
        if (input_msg_)
        {
            return input_msg_->handler_addr();
        }
        else
        {
            return 0;
        }
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
        return snapshot_ts_;
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

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_{nullptr};
    CcStreamSender *hd_{nullptr};

    uint64_t prior_cce_addr_{0};
    ScanDirection direct_{ScanDirection::Forward};
    std::vector<ScanTuple_msg *> scan_cache_;
    size_t scan_cache_idx_;
    bool is_ckpt_delta_{false};
    // The address of the CC map of the blocked core.
    CcHandlerResult<Void> cc_res_{nullptr};
    int64_t tx_term_{0};
    bool is_for_write_{false};
    uint64_t snapshot_ts_{0};

    // The pointer of the cc entry to which this request is directed. The
    // pointer is set, when the request locates the cc entry but is
    // blocked due to conflicts in 2PL. After the request is unblocked and
    // acquires the lock, the request's execution resumes without further lookup
    // of the cc entry.
    LruEntry *cce_ptr_{nullptr};
    // scan type for above cce_ptr_
    ScanType cce_ptr_scan_type_{ScanType::ScanUnknow};

    template <typename KeyT, typename ValueT>
    friend class ::txservice::TemplateCcMap;

    template <typename SkT, typename PkT>
    friend class ::txservice::SkCcMap;

    friend class ::txservice::CcMap;
};

struct RemoteFaultInjectCC : public FaultInjectCC
{
public:
    RemoteFaultInjectCC();

    RemoteFaultInjectCC(const RemoteFaultInjectCC &rhs) = delete;
    RemoteFaultInjectCC(RemoteFaultInjectCC &&rhs) = delete;

    void Reset(std::unique_ptr<CcMessage> input_msg);

    uint64_t handler_addr()
    {
        if (input_msg_)
        {
            return input_msg_->handler_addr();
        }
        else
        {
            return 0;
        }
    }

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_;
    CcStreamSender *hd_{nullptr};

    CcHandlerResult<bool> cc_res_{nullptr};

    friend class RemoteCcHandler;
};

struct RemoteCleanCcEntryForTestCc : public CleanCcEntryForTestCc
{
public:
    RemoteCleanCcEntryForTestCc();

    RemoteCleanCcEntryForTestCc(const RemoteCleanCcEntryForTestCc &rhs) =
        delete;
    RemoteCleanCcEntryForTestCc(RemoteCleanCcEntryForTestCc &&rhs) = delete;

    void Reset(std::unique_ptr<CcMessage> input_msg);

    uint64_t handler_addr()
    {
        if (input_msg_)
        {
            return input_msg_->handler_addr();
        }
        else
        {
            return 0;
        }
    }

private:
    CcMessage output_msg_;
    std::unique_ptr<CcMessage> input_msg_;
    CcStreamSender *hd_{nullptr};
    TableName remote_table_name_{empty_sv, TableType::Primary};

    CcHandlerResult<bool> cc_res_{nullptr};

    friend class RemoteCcHandler;
};

}  // namespace remote
}  // namespace txservice
