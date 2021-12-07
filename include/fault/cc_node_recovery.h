#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <thread>

#include "tx_id.h"
#include "type.h"

namespace txservice
{
class LocalCcShards;

namespace fault
{
struct RecoverTxInfo
{
    RecoverTxInfo() = default;

    RecoverTxInfo(uint64_t tx_number,
                  int64_t tx_term,
                  uint32_t cc_ng_id,
                  int64_t cc_ng_term)
        : tx_number_(tx_number),
          tx_term_(tx_term),
          cc_ng_id_(cc_ng_id),
          cc_ng_term_(cc_ng_term)
    {
    }

    // The number of tx who holds the intention/lock.
    uint64_t tx_number_;
    // The term of the cc node group in which the tx resides when the tx
    // acquires the intention/lock.
    int64_t tx_term_;
    // The ID of the cc node group in which the lock/intention resides.
    uint32_t cc_ng_id_;
    // The term of the cc node group in which the lock/intention resides.
    int64_t cc_ng_term_;
};

/**
 * @brief CcNodeRecoveryAgent is a background thread associated with each cc
 * node for recovering committed records in cc maps and orphan locks. When a cc
 * node is promoted to the leader, a CcNodeRecoveryAgent instance is
 * instantiated, notifying all log groups the term of the new leader. Each log
 * group in return ships unflushed, committed log records to the new leader,
 * which installs committed records to its cc maps. In normal execution, when a
 * lock in the cc node (leader) is held for an extended period of time, it is
 * passed to CcNodeRecoveryAgent for recovery: installs the committed value, if
 * the tx has committed; Or, clears the lock.
 *
 */
class CcNodeRecoveryAgent
{
public:
    CcNodeRecoveryAgent(NodeGroupId ng_id,
                        int64_t term,
                        const std::string &ip,
                        uint16_t port,
                        LocalCcShards &local_shards);

    ~CcNodeRecoveryAgent();

    /**
     * @brief Notifies the recovery agent the tx to be recovered.
     *
     * @param tx_number The tx number that uniquely identifies the tx.
     * @param tx_term The term of the tx node.
     * @param cc_ng_id The cc node group in which the lock is held.
     * @param cc_ng_term The term of the cc node group when the lock is
     * acquired, a.k.a. the lock's term.
     */
    void RecoverTx(uint64_t tx_number,
                   int64_t tx_term,
                   uint32_t cc_ng_id,
                   int64_t cc_ng_term);

private:
    void ClearTx(TxNumber tx_number);

    std::thread notify_thd_;
    std::atomic<bool> finish_;
    NodeGroupId ng_id_;
    int64_t term_;
    const std::string &ip_;
    uint16_t port_;
    std::deque<RecoverTxInfo> recover_tx_queue_;
    std::mutex queue_mux_;
    std::condition_variable queue_cv_;
    LocalCcShards &local_shards_;
};
}  // namespace fault
}  // namespace txservice