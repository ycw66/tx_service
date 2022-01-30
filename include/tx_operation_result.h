#pragma once

#include <atomic>

#include "cc/cc_entry.h"

namespace txservice
{
class CcScanner;

struct AcquireKeyResult
{
    uint64_t last_vali_ts_;
    uint64_t commit_ts_;
    CcEntryAddr cce_addr_;
    // Number of remote acquire requests to be acknowledged in the transaction's
    // upload phase. For OCC/MVCC protocols, an acquire request is non-blocking,
    // and the request's response is same as acknowledgement. For locking-based
    // protocols, the request may be blocked. An acknowledgement is a special
    // response notifying the sender the address and the term of the cc entry on
    // which the request is blocked.
    std::atomic<int32_t> *remote_ack_cnt_;
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
    void Reset(size_t cc_node_cnt)
    {
        cc_node_terms_.resize(cc_node_cnt);
        cc_node_returned_.resize(cc_node_cnt);

        for (size_t nid = 0; nid < cc_node_cnt; ++nid)
        {
            cc_node_terms_[nid] = -1;
        }
    }

    std::unique_ptr<CcScanner> scanner_;
    size_t scan_alias_;
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
}  // namespace txservice
