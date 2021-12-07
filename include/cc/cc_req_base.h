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

    TxNumber Tx() const
    {
        return tx_number_;
    }

protected:
    CcRequestBase()
        : in_use_(false),
          proto_(CcProtocol::OCC),
          isolation_level_(IsolationLevel::ReadCommitted)
    {
    }

    std::atomic<bool> in_use_;
    TxNumber tx_number_;

public:
    CcProtocol proto_;
    IsolationLevel isolation_level_;
};

/// <summary>
/// An interface for CC requests that may be blocked. Requests blocked on a key
/// are put into a queue and upon the key's state changes, the requests'
/// executions are resumed.
/// </summary>
struct Resumable
{
    virtual ~Resumable() = default;

    /// <summary>
    /// Resume the execution of a blocked cc request.
    /// </summary>
    /// <returns>True, if the execution finishes; false, if the request is still
    /// blocked.</returns>
    virtual bool Resume() = 0;
};
}  // namespace txservice
