#pragma once

#include <unordered_set>

#include "cc_protocol.h"
#include "cc_req_base.h"
#include "circular_queue.h"
#include "tx_id.h"

namespace txservice
{
template <typename KeyT, typename ValueT>
struct CcEntry;

/**
 * @brief Copied from TableLock.h
 *
 */
class NonBlockingLock
{
public:
    NonBlockingLock() = default;
    NonBlockingLock(const NonBlockingLock &rhs) = delete;

    /**
     * @brief Tries to acqurie the write lock. The operation succeeds, if no one
     * is holding the read and write lock. Note that the write lock does not
     * conflict with read intentions. The net effect of the failed operation
     * varies by concurrency control (cc) protocols: for 2PL, the request is put
     * into a waiting queue; for OCC/MVCC protocols, the request returns without
     * blocking.
     *
     * @param cc_req The cc request that tries to acquire the write lock.
     * @param tx_term The term of the cc node from which the tx comes.
     * @param protocol The cc protocol control the tx uses.
     * @return true, if the request acquires the write lock successfully; false,
     * if the request is blocked and put into the waiting queue.
     */
    bool AcquireWriteLock(CcRequestBase *cc_req,
                          int64_t tx_term,
                          CcProtocol protocol);

    void ReleaseWriteLock(TxNumber tx_number, CcShard *ccs);

    /**
     * @brief Tries to acquire the read lock. Only tx's under 2PL acquire read
     * locks. The operation succeeds, if no one is holding the write lock and no
     * write request is blocked. The operation is blocked and put into the
     * waiting queue, if the write lock is held by someone else or someone is
     * blocked and waiting for the write lock.
     *
     * @param cc_req The cc request that tries to acquire the write lock.
     * @param tx_term The term of the cc node from which the tx comes.
     * @return true, if the request acquires the read lock successfully; false,
     * if the request is blocked and put into the blocking queue.
     */
    bool AcquireReadLock(CcRequestBase *cc_req, int64_t tx_term);

    void ReleaseReadLock(TxNumber tx_number, CcShard *ccs);

    /**
     * @brief Acquires a read intention. Tx's under OCC/MVCC acquire read
     * intentions for read operations. Read intentions do not block writes.
     * Their goal is to prevent the cache replacement algorithm from kicking out
     * the cc entry from the cc map.
     *
     * @param tx_number The tx who acquires the read intention
     */
    void AcquireReadIntention(TxNumber tx_number);

    void ReleaseReadIntention(TxNumber tx_number);

    bool IsEmpty() const;

    TxNumber WriteTx() const;

    bool HasWriteLock() const;

    void ClearTx(TxNumber tx_number, CcShard *ccs);

    const std::unordered_set<TxNumber> &ReadLocks() const;

private:
    enum struct LockType
    {
        Read = 0,
        Write
    };

    struct LockQueueEntry
    {
        LockQueueEntry() = default;

        LockQueueEntry(CcRequestBase *req, LockType type, int64_t tx_term)
            : req_(req), lk_type_(type), tx_term_(tx_term)
        {
        }

        CcRequestBase *req_{nullptr};
        LockType lk_type_{LockType::Read};
        int64_t tx_term_;
    };

    // Read intentions do not block writes. They are used by a tx under OCC/MVCC
    // protocols to mark that the tx is accessing the data item and to prevent
    // the cache replacement algorithm from kicking out the item's concurrency
    // control (cc) entry from the cc map before the tx finishes.
    std::unordered_set<TxNumber> read_intentions_;
    // Tx's who have acquired read locks and their terms
    std::unordered_set<TxNumber> read_locks_;
    TxNumber write_tx_{0};
    bool is_write_lock_empty_{true};
    CircularQueue<LockQueueEntry> blocking_queue_;

    template <typename KeyT, typename ValueT>
    friend struct CcEntry;
};

}  // namespace txservice
