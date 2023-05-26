#pragma once

#include <atomic>
#include <memory>  //unique_ptr
#include <mutex>
#include <utility>

#include "cc/cc_entry.h"
#include "proto/cc_request.pb.h"
#include "type.h"

namespace txservice
{
class CcScanner;

enum class AckStatus : unsigned char
{
    Unknown = 0,  // Not set ack status;
    BlockQueue,   // The cc request is in block queue
    Finished,     // THe cc request has been executed.
    ErrorTerm     // cc node group term has changed
};

enum class ResultTemplateType
{
    AcquireKeyResult = 1,
    ReadKeyResult
};

struct AcquireKeyResult
{
    uint64_t last_vali_ts_{0};
    uint64_t commit_ts_{0};
    CcEntryAddr cce_addr_;
    // Number of remote acquire requests to be acknowledged in the transaction's
    // upload phase. For OCC protocol (optimistic write), an acquire request is
    // non-blocking, and the request's response is same as acknowledgement. For
    // OccRead/Locking protocols (pessimistic write), the request may be
    // blocked. An acknowledgement is a special response notifying the sender
    // the address and the term of the cc entry on which the request is blocked.
    std::atomic<int32_t> *remote_ack_cnt_{nullptr};
};

struct AcquireAllResult
{
    uint64_t last_vali_ts_{1};
    uint64_t commit_ts_{1};
    int64_t node_term_{-1};
    /**
     * @brief The address of the cc entry that co-locates with the sending tx in
     * the same core. The address is used to dedup the read intent/lock acquired
     * from prior reads of the local cc entry.
     *
     */
    CcEntryAddr local_cce_addr_;
    std::atomic<int32_t> *remote_ack_cnt_{nullptr};
};

struct ReadKeyResult
{
    void Reset()
    {
        rec_ = nullptr;
        ts_ = 0U;
        cce_addr_.SetTerm(-1);
        rec_status_ = RecordStatus::Unknown;
        lock_type_ = LockType::NoLock;
        is_local_ = true;
    }

    TxRecord *rec_;
    uint64_t ts_;
    CcEntryAddr cce_addr_;
    RecordStatus rec_status_;

    // Acquired key lock type by this read operation.
    LockType lock_type_{LockType::NoLock};

    bool is_local_{true};
};

struct ScanOpenResult
{
    ScanOpenResult()
    {
    }

    ScanOpenResult(ScanOpenResult &&other)
    {
        scan_alias_ = other.scan_alias_;
        cc_node_terms_ = std::move(other.cc_node_terms_);
        cc_node_returned_ = std::move(other.cc_node_returned_);
        scanner_ = std::move(other.scanner_);
    }

    ScanOpenResult &operator=(const ScanOpenResult &rhs) = delete;

    ScanOpenResult &operator=(ScanOpenResult &&rhs)
    {
        if (this == &rhs)
        {
            return *this;
        }

        scan_alias_ = rhs.scan_alias_;
        cc_node_terms_ = std::move(rhs.cc_node_terms_);
        cc_node_returned_ = std::move(rhs.cc_node_returned_);
        scanner_ = std::move(rhs.scanner_);

        return *this;
    }

    void Reset(size_t cc_node_cnt)
    {
        cc_node_terms_.resize(cc_node_cnt);
        cc_node_returned_.resize(cc_node_cnt);

        for (size_t nid = 0; nid < cc_node_cnt; ++nid)
        {
            cc_node_terms_[nid] = -1;
        }
    }

    std::unique_ptr<CcScanner> scanner_{nullptr};
    size_t scan_alias_{0};
    // The terms of all cc node groups. As cc node groups currently employ the
    // hash partition function, a scan is directed to all cc node groups. For
    // locking-based protocols, the scan request in a cc node group may be
    // blocked, which sends an acknowledgement to the request's issuer notifying
    // the cc node's term. This vector bookkeeps which cc node groups have sent
    // acknowledgement.
    std::vector<int64_t> cc_node_terms_;
    // std::vector<bool> is discouraged. We use one byte to denote if the scan
    // response toward a cc node has returned or not. We do not designate a
    // separate vector to bookkeep error codes of individual requests. This is
    // because if a scan request toward a cc node finishes with an error, the
    // error code is recorded in the cc handler result.
    std::vector<uint8_t> cc_node_returned_;
};

struct RemoteScanCache
{
    RemoteScanCache() : cache_msg_(nullptr), cache_mem_size_(0)
    {
    }

    RemoteScanCache(remote::ScanCache_msg *cache_msg, uint32_t mem_size)
        : cache_msg_(cache_msg), cache_mem_size_(mem_size)
    {
    }

    size_t Size() const
    {
        return cache_msg_->scan_tuple_size();
    }

    bool IsFull() const
    {
        return cache_mem_size_ >= 1024;
    }

    const std::string &LastScanKey() const
    {
        return cache_msg_->scan_tuple(cache_msg_->scan_tuple_size() - 1).key();
    }

    remote::ScanCache_msg *cache_msg_;
    uint32_t cache_mem_size_;
};

struct RangeScanSliceResult
{
    RangeScanSliceResult()
        : last_key_(nullptr),
          slice_position_(SlicePosition::FirstSlice),
          ccm_scanner_(nullptr),
          is_local_(true),
          cc_ng_id_(0)
    {
    }

    RangeScanSliceResult(TxKey::Uptr last_key, SlicePosition status)
        : last_key_(std::move(last_key)),
          slice_position_(status),
          ccm_scanner_(nullptr),
          is_local_(true),
          cc_ng_id_(0)

    {
    }

    RangeScanSliceResult(RangeScanSliceResult &&rhs)
        : last_key_(std::move(rhs.last_key_)),
          slice_position_(rhs.slice_position_),
          is_local_(rhs.is_local_),
          cc_ng_id_(rhs.cc_ng_id_)
    {
        if (rhs.is_local_)
        {
            ccm_scanner_ = rhs.ccm_scanner_;
        }
        else
        {
            remote_scan_caches_ = rhs.remote_scan_caches_;
        }
    }

    ~RangeScanSliceResult()
    {
        if (!is_local_)
        {
            last_key_ = nullptr;
        }
    }

    RangeScanSliceResult &operator=(RangeScanSliceResult &&rhs)
    {
        if (this == &rhs)
        {
            return *this;
        }

        last_key_ = std::move(rhs.last_key_);
        slice_position_ = rhs.slice_position_;
        is_local_ = rhs.is_local_;
        cc_ng_id_ = rhs.cc_ng_id_;

        if (rhs.is_local_)
        {
            ccm_scanner_ = rhs.ccm_scanner_;
        }
        else
        {
            remote_scan_caches_ = rhs.remote_scan_caches_;
        }

        return *this;
    }

    /**
     * @brief The last key of the current scan batch. For forward scans, the
     * last key is the exclusive end of the current slice, which is the
     * inclusive start key of the next scan batch. For backward scans, the last
     * key is the inclusive start of the current slice, which is the exclusive
     * start key of the next scan batch.
     *
     */
    TxKey::Uptr last_key_;
    SlicePosition slice_position_;

    union
    {
        CcScanner *ccm_scanner_;
        std::vector<RemoteScanCache> *remote_scan_caches_;
    };
    bool is_local_{true};

    NodeGroupId cc_ng_id_{0};
};

struct ScanNextResult
{
    bool is_local_;
    int64_t term_;
    uint32_t node_group_id_;
};

struct InitTxResult
{
    TxId txid_;
    uint64_t start_ts_;
    // The term of the cc node group to which the tx is bound.
    int64_t term_;
};

struct RangeMedianKeyResult
{
    RangeMedianKeyResult()
    {
    }

    RangeMedianKeyResult(const RangeMedianKeyResult &other)
    {
        median_key_ = other.median_key_->Clone();
        new_partition_id_ = other.new_partition_id_;
    }

    RangeMedianKeyResult &operator=(const RangeMedianKeyResult &rhs)
    {
        if (this == &rhs)
        {
            return *this;
        }
        median_key_ = rhs.median_key_->Clone();
        new_partition_id_ = rhs.new_partition_id_;

        return *this;
    }

    std::unique_ptr<TxKey> median_key_{nullptr};
    int32_t new_partition_id_{-1};
};

struct PostProcessResult
{
    PostProcessResult() = default;

    PostProcessResult(const PostProcessResult &rhs)
        : conflicting_txs_(rhs.conflicting_txs_)
    {
    }

    PostProcessResult(PostProcessResult &&rhs)
        : conflicting_txs_(std::move(rhs.conflicting_txs_))
    {
    }

    PostProcessResult &operator=(const PostProcessResult &rhs)
    {
        if (this == &rhs)
        {
            return *this;
        }

        conflicting_txs_ = rhs.conflicting_txs_;
        return *this;
    }

    void AddConflictingTx(TxNumber txn)
    {
        std::lock_guard<std::mutex> lk(mux_);
        conflicting_txs_.emplace_back(txn);
    }

    size_t Size()
    {
        std::lock_guard<std::mutex> lk(mux_);
        return conflicting_txs_.size();
    }

    void Clear()
    {
        std::lock_guard<std::mutex> lk(mux_);
        conflicting_txs_.clear();
    }

    std::vector<TxNumber> conflicting_txs_;
    std::mutex mux_;
    bool is_local_ = true;
};
}  // namespace txservice
