#include "cc/non_blocking_lock.h"

#include <butil/logging.h>

#include <cassert>

#include "cc/cc_shard.h"

namespace txservice
{
/**
 * @brief Upgrade write lock or write intent.
 * 1. release low level lock for write lock or write intent for the same
 * tx_number.
 * 2. acquire the desired write lock/intent.
 * 3. read lock and read intent have no upgrade logic since they don't have low
 * level locks.
 *
 */
void NonBlockingLock::UpgradeLock(TxNumber tx_number, LockType lock_type)
{
    // write lock needs to upgrade write intent as well.
    if (lock_type == LockType::WriteLock)
    {
        is_write_lock_empty_ = false;
        write_lock_tx_ = tx_number;
        if (!is_write_intent_empty_ && write_intent_tx_ == tx_number)
        {
            // there is at most one write intent, if the owner is current tx,
            // release the write intent.
            is_write_intent_empty_ = true;
            write_intent_tx_ = 0;
        }
    }
    else if (lock_type == LockType::WriteIntent)
    {
        is_write_intent_empty_ = false;
        write_intent_tx_ = tx_number;
    }

    // both write lock and write intent needs to upgrade read lock and read
    // intention.
    if (read_intentions_.size() > 0)
    {
        read_intentions_.erase(tx_number);
    }
    if (read_locks_.size() > 0)
    {
        read_locks_.erase(tx_number);
    }
}

/**
 * @brief Re-execute the queued request when some locks are released.
 */
void NonBlockingLock::ExecuteQueuedRequest(const LockQueueEntry &queue_head,
                                           CcShard *ccs)
{
    bool is_free = queue_head.req_->Execute(*ccs);
    if (is_free)
    {
        // Blocked cc requests are not in the cc processing queue and
        // hence needs to be freed here.
        queue_head.req_->Free();
    }
    blocking_queue_.Dequeue();
}

/**
 * @brief Try to pop the requests from queue and re-execute the requests if
 * there is no conflict.
 */
void NonBlockingLock::TryPopBlockingQueue(CcShard *ccs)
{
    while (blocking_queue_.Size() > 0)
    {
        const LockQueueEntry &queue_head = blocking_queue_.Peek();
        TxNumber queued_txn = queue_head.req_->Txn();
        if (queue_head.lk_type_ == LockType::WriteLock)
        {
            if (NoWriteLockConflict(queued_txn) &&
                NoWriteIntentConflict(queued_txn) &&
                NoReadLockConflict(queued_txn))
            {
                UpgradeLock(queued_txn, LockType::WriteLock);

                // re-execute the head request in the queue.
                ExecuteQueuedRequest(queue_head, ccs);
            }
            else
            {
                // stop poping request from queue when hitting conflict.
                return;
            }
        }
        else if (queue_head.lk_type_ == LockType::WriteIntent)
        {
            if (NoWriteLockConflict(queued_txn) &&
                NoWriteIntentConflict(queued_txn))
            {
                UpgradeLock(queued_txn, LockType::WriteIntent);

                // re-execute the head request in the queue.
                ExecuteQueuedRequest(queue_head, ccs);
            }
            else
            {
                // stop poping request from queue when hitting conflict.
                return;
            }
        }
        else if (queue_head.lk_type_ == LockType::ReadLock)
        {
            if (NoWriteLockConflict(queued_txn))
            {
                read_locks_.emplace(queue_head.req_->Txn());
                // re-execute the head request in the queue.
                ExecuteQueuedRequest(queue_head, ccs);
            }
            else
            {
                // stop poping request from queue when hitting conflict.
                return;
            }
        }
        else
        {
            return;
        }
    }
}

bool NonBlockingLock::AcquireLock(CcRequestBase *cc_req,
                                  int64_t tx_term,
                                  CcProtocol protocol,
                                  LockType lock_type)
{
    if (lock_type == LockType::ReadLock)
    {
        return AcquireReadLock(cc_req, tx_term);
    }
    else if (lock_type == LockType::WriteIntent)
    {
        return AcquireWriteIntent(cc_req, tx_term, protocol);
    }
    else if (lock_type == LockType::WriteLock)
    {
        return AcquireWriteLock(cc_req, tx_term, protocol);
    }
    else if (lock_type == LockType::ReadIntent)
    {
        return AcquireReadIntent(cc_req->Txn());
    }
}

void NonBlockingLock::ReleaseLock(TxNumber tx_number,
                                  CcShard *ccs,
                                  LockType lock_type)
{
    if (lock_type == LockType::ReadLock)
    {
        ReleaseReadLock(tx_number, ccs);
    }
    else if (lock_type == LockType::WriteIntent)
    {
        ReleaseWriteIntent(tx_number, ccs);
    }
    else if (lock_type == LockType::WriteLock)
    {
        ReleaseWriteLock(tx_number, ccs);
    }
    else if (lock_type == LockType::ReadIntent)
    {
        ReleaseReadIntent(tx_number);
    }
}

/**
 * @brief Acquire the write lock on this object (i.e. ccentry). The algorithm
 * is as follows:
 * 1. fast path if the lock is already held.
 * 1. list non conflict case: no write lock conflict, no write intent conflict
 * and no read lock conflict.
 * 2. upgrade low-level locks/intents if lock succeeds.
 * 3. put the request into blocking queue under LOCKING protocol.
 *
 * @param cc_req: lock request.
 * @param tx_term: term of ccnode group where the transaction resides.
 * @param protocol: OCC or LOCKING.
 * @return true: lock succeeds.
 * @return false: lock failed.
 */
bool NonBlockingLock::AcquireWriteLock(CcRequestBase *cc_req,
                                       int64_t tx_term,
                                       CcProtocol protocol)
{
    TxNumber tx_number = cc_req->Txn();

    // fast path for lock is already held.
    if (write_lock_tx_ == tx_number)
    {
        return true;
    }

    // lock succeeds if there is no conflict.
    if (NoWriteLockConflict(tx_number) && NoWriteIntentConflict(tx_number) &&
        NoReadLockConflict(tx_number))
    {
        UpgradeLock(tx_number, LockType::WriteLock);

        return true;
    }
    else
    {
        // lock fails.
        if (protocol == CcProtocol::Locking)
        {
            // block the request by putting it into the blocking queue.
            blocking_queue_.Enqueue(
                LockQueueEntry(cc_req, LockType::WriteLock, tx_term));
        }
        // OCC doesn't enqueue request.
        return false;
    }
}

/**
 * @brief Acquire the read lock on this object (i.e. ccentry). The algorithm
 * is as follows:
 * 1. fast path is that the lock is already held.
 * 2. acquire succeeds if write_lock is empty and (a. the blocking queue is
 * empty or b. the head of queue is write intent since write intent is not
 * conflict with read lock).
 * 3. put the request into blocking queue if lock fails.
 *
 * @param cc_req: lock request.
 * @param tx_term: term of ccnode group where the transaction resides.
 * @return true: lock succeeds.
 * @return false: lock failed, push request into blocking.
 */
bool NonBlockingLock::AcquireReadLock(CcRequestBase *cc_req, int64_t tx_term)
{
    TxNumber tx_number = cc_req->Txn();
    // fast path for lock is already held.
    if (read_locks_.find(tx_number) != read_locks_.end() ||
        (!is_write_intent_empty_ && write_intent_tx_ == tx_number) ||
        (!is_write_lock_empty_ && write_lock_tx_ == tx_number))
    {
        return true;
    }

    // read lock dones't conflict with write intent in blocking queue.
    bool no_blocking_queue_conflict =
        blocking_queue_.Size() == 0 ||
        blocking_queue_.Peek().lk_type_ == LockType::WriteIntent;

    if (NoWriteLockConflict(tx_number) && no_blocking_queue_conflict)
    {
        // acquire read lock succeeds
        read_locks_.emplace(tx_number);
        return true;
    }
    else
    {
        // protocol must be LOCKING, since tx under OCC never acquires read
        // locks.
        blocking_queue_.Enqueue(
            LockQueueEntry(cc_req, LockType::ReadLock, tx_term));
        return false;
    }
}

/**
 * @brief Release the read lock on this object (i.e. ccentry).
 *
 * @param tx_number
 * @param ccs
 */
void NonBlockingLock::ReleaseReadLock(TxNumber tx_number, CcShard *ccs)
{
    size_t removed_cnt = read_locks_.erase(tx_number);

    if (removed_cnt == 0)
    {
        return;
    }

    // If releasing the current read lock may unblock anything, it must be the
    // write lock who is the head of the blocking queue.
    if (removed_cnt > 0 && blocking_queue_.Size() > 0 &&
        blocking_queue_.Peek().lk_type_ == LockType::WriteLock)
    {
        const LockQueueEntry &queue_head = blocking_queue_.Peek();

        TxNumber txn = queue_head.req_->Txn();

        bool no_read_lk_conflict =
            read_locks_.empty() ||
            (read_locks_.size() == 1 && *read_locks_.begin() == txn);
        bool no_write_intent_conflict =
            is_write_intent_empty_ || write_intent_tx_ == txn;

        if (no_read_lk_conflict && no_write_intent_conflict)
        {
            is_write_lock_empty_ = false;
            write_lock_tx_ = txn;

            if (!read_locks_.empty())
            {
                read_locks_.erase(txn);
            }

            if (!is_write_intent_empty_)
            {
                is_write_intent_empty_ = true;
                write_intent_tx_ = 0;
            }

            bool is_free = queue_head.req_->Execute(*ccs);
            if (is_free)
            {
                // Blocked cc requests are not in the cc processing queue and
                // hence needs to be freed here.
                queue_head.req_->Free();
            }
            blocking_queue_.Dequeue();
        }
    }

    TryPopBlockingQueue(ccs);
}

/**
 * @brief Release the write lock on this object (i.e. ccentry).
 *
 * @param tx_number
 * @param ccs
 */
void NonBlockingLock::ReleaseWriteLock(TxNumber tx_number, CcShard *ccs)
{
    if (is_write_lock_empty_ || write_lock_tx_ != tx_number)
    {
        return;
    }

    // release the write lock.
    write_lock_tx_ = 0;
    is_write_lock_empty_ = true;

    TryPopBlockingQueue(ccs);
}

/**
 * @brief Acquire the write intent on this object (i.e. ccentry). The
 * algorithm is as follows:
 * 1. fast path if lock is already held.
 * 2. acquire succeeds if no conflict write intent or write lock on this
 * object and lock owner is not the current tx and blocking queue is empty.
 * 3. upgrade low-level locks/intents if succeeds.
 * 4. return true if acquire succeeds. return false if acquire fails. For
 * LOCKING protocol, put the request into blocking queue.
 *
 * @param cc_req: lock request.
 * @param tx_term: term of ccnode group where the transaction resides.
 * @param protocol: OCC or LOCKING.
 * @return true: lock succeeds.
 * @return false: lock failed, push request into blocking for LOCKING
 * protocol. return directly for OCC protocol.
 */
bool NonBlockingLock::AcquireWriteIntent(CcRequestBase *cc_req,
                                         int64_t tx_term,
                                         CcProtocol protocol)
{
    TxNumber tx_number = cc_req->Txn();

    // fast path for lock is already held.
    if ((!is_write_intent_empty_ && write_intent_tx_ == tx_number) ||
        (!is_write_lock_empty_ && write_lock_tx_ == tx_number))
    {
        return true;
    }
    // lock succeeds if:
    // 1. no conflict write intent or write locks
    // 2. blocking queue is empty which is used to avoid the starvation of
    // queued write lock.
    else if (NoWriteIntentConflict(tx_number) &&
             NoWriteLockConflict(tx_number) && blocking_queue_.Size() == 0)
    {
        UpgradeLock(tx_number, LockType::WriteIntent);

        return true;
    }
    // lock fails case.
    else
    {
        if (protocol == CcProtocol::Locking)
        {
            // block the request by putting it into the blocking queue.
            blocking_queue_.Enqueue(
                LockQueueEntry(cc_req, LockType::WriteIntent, tx_term));
        }
        // OCC doesn't enqueue request.
        return false;
    }
}

/**
 * @brief Release the write intent on this object (i.e. ccentry).
 *
 * @param tx_number
 * @param ccs
 */
void NonBlockingLock::ReleaseWriteIntent(TxNumber tx_number, CcShard *ccs)
{
    if (is_write_intent_empty_ || write_intent_tx_ != tx_number)
    {
        return;
    }

    // release the write intent.
    write_intent_tx_ = 0;
    is_write_intent_empty_ = true;

    TryPopBlockingQueue(ccs);
}

bool NonBlockingLock::AcquireReadIntent(TxNumber tx_number)
{
    read_intentions_.emplace(tx_number);
}

void NonBlockingLock::ReleaseReadIntent(TxNumber tx_number)
{
    if (read_intentions_.size() > 0)
    {
        read_intentions_.erase(tx_number);
    }
}

bool NonBlockingLock::IsEmpty() const
{
    return read_intentions_.empty() && read_locks_.empty() &&
           is_write_lock_empty_ && blocking_queue_.Size() == 0;
}

TxNumber NonBlockingLock::WriteLockTx() const
{
    return write_lock_tx_;
}

bool NonBlockingLock::HasWriteLock() const
{
    return !is_write_lock_empty_;
}

TxNumber NonBlockingLock::WriteIntentTx() const
{
    return write_intent_tx_;
}

bool NonBlockingLock::HasWriteIntent() const
{
    return !is_write_intent_empty_;
}

void NonBlockingLock::ClearTx(TxNumber tx_number, CcShard *ccs)
{
    if (!is_write_lock_empty_ && write_lock_tx_ == tx_number)
    {
        ReleaseWriteLock(tx_number, ccs);
    }
    else
    {
        ReleaseReadIntent(tx_number);
        ReleaseReadLock(tx_number, ccs);
    }
}

const std::unordered_set<TxNumber> &NonBlockingLock::ReadLocks() const
{
    return read_locks_;
}
}  // namespace txservice
