#pragma once

#include <unordered_set>

#include "cc_protocol.h"
#include "cc_req_base.h"
#include "circular_queue.h"
#include "tx_id.h"
#include "type.h"

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
     * is holding the read lock, the write lock or the write intent. Note that
     * the write lock does not conflict with read intentions. The net effect of
     * the failed operation varies by concurrency control (cc) protocols: for
     * 2PL, the request is put into a waiting queue; for OCC/MVCC protocols, the
     * request returns without blocking.
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

    bool AcquireWriteIntent(CcRequestBase *cc_req,
                            int64_t tx_term,
                            CcProtocol protocol);

    void ReleaseWriteIntent(TxNumber tx_number, CcShard *ccs);

    /**
     * @brief Tries to acquire the read lock. Only tx's under 2PL acquire read
     * locks. The operation succeeds, if no one is holding the write lock and no
     * write lock request is blocked. The operation is blocked and put into the
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
     * @brief Acquires a read intent. Tx's under OCC/MVCC acquire read intents
     * for read operations. Read intents do not block writes. Their goal is to
     * prevent the cache replacement algorithm from kicking out the cc entry
     * from the cc map.
     *
     * @param tx_number The tx who acquires the read intention
     */
    bool AcquireReadIntent(TxNumber tx_number);

    void ReleaseReadIntent(TxNumber tx_number);

    bool AcquireLock(CcRequestBase *cc_req,
                     int64_t tx_term,
                     CcProtocol protocol,
                     LockType lock_type);

    void ReleaseLock(TxNumber tx_number, CcShard *ccs, LockType lock_type);

    bool IsEmpty() const;

    TxNumber WriteLockTx() const;

    bool HasWriteLock() const;

    TxNumber WriteIntentTx() const;

    bool HasWriteIntent() const;

    void ClearTx(TxNumber tx_number, CcShard *ccs);

    const std::unordered_set<TxNumber> &ReadLocks() const;
    const std::unordered_set<TxNumber> &ReadIntents() const;

    size_t MemUsage() const
    {
        size_t mem_size_ = 0;

        mem_size_ += sizeof(read_intentions_) +
                     read_intentions_.size() * sizeof(TxNumber);
        mem_size_ +=
            sizeof(read_locks_) + read_locks_.size() * sizeof(TxNumber);
        mem_size_ += sizeof(write_lock_tx_);
        mem_size_ += sizeof(is_write_lock_empty_);
        mem_size_ += sizeof(write_intent_tx_);
        mem_size_ += sizeof(is_write_intent_empty_);
        mem_size_ += blocking_queue_.MemUsage() +
                     blocking_queue_.Capacity() * sizeof(LockQueueEntry);

        return mem_size_;
    }

private:
    struct LockQueueEntry
    {
        LockQueueEntry() = default;

        LockQueueEntry(CcRequestBase *req, LockType type, int64_t tx_term)
            : req_(req), lk_type_(type), tx_term_(tx_term)
        {
        }

        CcRequestBase *req_{nullptr};
        LockType lk_type_{LockType::ReadLock};
        int64_t tx_term_;
    };

    void ExecuteQueuedRequest(const LockQueueEntry &queue_head, CcShard *ccs);
    void UpgradeLock(TxNumber tx_number, LockType lock_type);
    void TryPopBlockingQueue(CcShard *ccs);

    bool NoReadLockConflict(TxNumber tx_number) const
    {
        return (read_locks_.empty() ||
                (read_locks_.size() == 1 && *read_locks_.begin() == tx_number));
    }

    bool NoWriteIntentConflict(TxNumber tx_number) const
    {
        return is_write_intent_empty_ || write_intent_tx_ == tx_number;
    }

    bool NoWriteLockConflict(TxNumber tx_number) const
    {
        return is_write_lock_empty_ || write_lock_tx_ == tx_number;
    }

    // Read intentions do not block writes. They are used by a tx under OCC/MVCC
    // protocols to mark that the tx is accessing the data item and to prevent
    // the cache replacement algorithm from kicking out the item's concurrency
    // control (cc) entry from the cc map before the tx finishes.
    std::unordered_set<TxNumber> read_intentions_;
    // Tx's who have acquired read locks and their terms
    std::unordered_set<TxNumber> read_locks_;
    TxNumber write_lock_tx_{0};
    bool is_write_lock_empty_{true};
    TxNumber write_intent_tx_{0};
    bool is_write_intent_empty_{true};
    // blocking_queue_ stores the requests which acquire lock/intent failed due
    // to conflict. There are three types of lock requests can be in blocking
    // queue: Write Lock(WL), Write Intent(WI) and Read Lock(RL). The conflict
    // map is that WL conflicts with WL/WI/RL, WI conflicts with WL/WI and RL
    // conflicts with WL.
    CircularQueue<LockQueueEntry> blocking_queue_;

    template <typename KeyT, typename ValueT>
    friend struct CcEntry;
};

}  // namespace txservice
