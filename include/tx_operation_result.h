#pragma once

#include <atomic>
#include <memory>  //unique_ptr
#include <mutex>

#include "cc/cc_entry.h"

namespace txservice
{
class CcScanner;

struct AcquireKeyResult
{
    uint64_t last_vali_ts_{0};
    uint64_t commit_ts_{0};
    CcEntryAddr cce_addr_;
    // Number of remote acquire requests to be acknowledged in the transaction's
    // upload phase. For OCC/MVCC protocols, an acquire request is non-blocking,
    // and the request's response is same as acknowledgement. For locking-based
    // protocols, the request may be blocked. An acknowledgement is a special
    // response notifying the sender the address and the term of the cc entry on
    // which the request is blocked.
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
    TxRecord *rec_;
    uint64_t ts_;
    CcEntryAddr cce_addr_;
    RecordStatus rec_status_;
};

struct ScanOpenResult
{
    ScanOpenResult()
    {
    }

    ScanOpenResult(const ScanOpenResult &other)
    {
        scan_alias_ = other.scan_alias_;
        cc_node_terms_ = other.cc_node_terms_;
        cc_node_returned_ = other.cc_node_returned_;
        scanner_ = other.scanner_->Clone();
    }
    ScanOpenResult &operator=(const ScanOpenResult &rhs)
    {
        if (this == &rhs)
        {
            return *this;
        }

        scan_alias_ = rhs.scan_alias_;
        cc_node_terms_ = rhs.cc_node_terms_;
        cc_node_returned_ = rhs.cc_node_returned_;
        scanner_ = rhs.scanner_->Clone();

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

struct ScanNextResult
{
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
};
}  // namespace txservice
