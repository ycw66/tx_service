#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <thread>

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

class LogNotifier
{
public:
    LogNotifier(NodeGroupId ng_id,
                int64_t term,
                const std::string &ip,
                uint16_t port,
                LocalCcShards &local_shards);

    ~LogNotifier();

    void RecoverTx(uint64_t tx_number,
                   int64_t tx_term,
                   uint32_t cc_ng_id,
                   int64_t cc_ng_term);

private:
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