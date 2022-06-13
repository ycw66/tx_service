#pragma once

#pragma once

#include <atomic>

#include "cc_protocol.h"
#include "tx_id.h"

namespace txservice
{
class CcShard;

struct CcRequestBase
{
public:
    virtual ~CcRequestBase() = default;

    /**
     * @brief Processes the cc request toward the input concurrency control (cc)
     * shard.
     *
     * @param ccs The cc shard on which the cc request is processed.
     * @return true, if the request needs to be freed and recycled; false, if
     * the request should not be freed and recycled.
     */
    virtual bool Execute(CcShard &ccs) = 0;

    bool InUse() const
    {
        return in_use_.load(std::memory_order_acquire);
    }

    virtual void Free()
    {
        in_use_.store(false, std::memory_order_release);
    }

    void Use()
    {
        in_use_.store(true, std::memory_order_release);
    }

    TxNumber Txn() const
    {
        return tx_number_;
    }

    CcProtocol Protocol() const
    {
        return proto_;
    }

    IsolationLevel Isolation() const
    {
        return isolation_level_;
    }

    // The previous time to run it. The interval time is const variable array.
    // It will be 16 times of previous interval.
    uint64_t prev_exec_ts_;

protected:
    CcRequestBase() = default;

    std::atomic<bool> in_use_{false};
    TxNumber tx_number_{0};
    CcProtocol proto_{CcProtocol::OCC};
    IsolationLevel isolation_level_{IsolationLevel::ReadCommitted};
};
}  // namespace txservice
