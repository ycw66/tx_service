#include "cc/non_blocking_lock.h"

#include <cassert>

#include "cc/cc_shard.h"

namespace txservice
{
bool NonBlockingLock::AcquireWriteLock(CcRequestBase *cc_req,
                                       int64_t tx_term,
                                       CcProtocol protocol)
{
    TxNumber tx_number = cc_req->Txn();

    if (is_write_lock_empty_)
    {
        if (read_locks_.size() == 0)
        {
            write_tx_ = tx_number;
            is_write_lock_empty_ = false;
            if (protocol == CcProtocol::OCC)
            {
                // Upgrades the read intention, if there is any, to the write
                // lock.
                read_intentions_.erase(tx_number);
            }
            return true;
        }
        else if (read_locks_.size() == 1 &&
                 read_locks_.find(tx_number) != read_locks_.end())
        {
            // Upgrades the read lock to the write lock.
            read_locks_.clear();
            write_tx_ = tx_number;
            is_write_lock_empty_ = false;
            return true;
        }
        else if (protocol == CcProtocol::OCC)
        {
            // OCC/MVCC protocols are non-blocking. So, if the write conflicts
            // with one or more read locks, gives up acquiring the write lock
            // without entering into the blocking queue.
            return false;
        }
    }
    else if (write_tx_ == tx_number)
    {
        // The tx has acquired the write lock and is trying to acquire the same
        // lock again.
        return true;
    }
    else if (protocol == CcProtocol::OCC)
    {
        // Conflicts with an existing write lock. For OCC/MVCC protocols, gives
        // up acquiring the write lock.
        return false;
    }

    // Read-write or write-write conflicts. Blocks the request by putting it
    // into the blocking queue.
    blocking_queue_.Enqueue(LockQueueEntry(cc_req, LockType::Write, tx_term));
    // Upgrades the read lock, if there is any.
    read_locks_.erase(tx_number);
    return false;
}

bool NonBlockingLock::AcquireReadLock(CcRequestBase *cc_req, int64_t tx_term)
{
    TxNumber txn = cc_req->Txn();

    if (is_write_lock_empty_ && blocking_queue_.Size() == 0)
    {
        read_locks_.emplace(txn);
        return true;
    }
    else if (read_locks_.find(txn) != read_locks_.end())
    {
        // The tx is already holding a read lock.
        return true;
    }
    else
    {
        // The acquire-read request is blocked when there is a write lock.
        // It is too blocked when where is no write lock, but the blocking
        // queue is not empty. When there is no write lock and the blocking
        // queue is not empty, there must be a write request being blocked. To
        // prevent starvation, the read request is enqueued after the write
        // request.
        blocking_queue_.Enqueue(
            LockQueueEntry(cc_req, LockType::Read, tx_term));
        return false;
    }
}

void NonBlockingLock::ReleaseReadLock(TxNumber tx_number, CcShard *ccs)
{
    size_t removed_cnt = read_locks_.erase(tx_number);

    if (removed_cnt > 0 && read_locks_.size() == 0 &&
        blocking_queue_.Size() > 0)
    {
        const LockQueueEntry &queue_head = blocking_queue_.Peek();
        assert(queue_head.lk_type_ == LockType::Write);
        // The write request in the blocking queue must be under the locking
        // protocol.
        bool success = AcquireWriteLock(
            queue_head.req_, queue_head.tx_term_, CcProtocol::Locking);
        assert(success);
        bool is_free = queue_head.req_->Execute(*ccs);
        if (is_free)
        {
            // Blocked cc requests are not in the cc processing queue and hence
            // needs to be freed here.
            queue_head.req_->Free();
        }
        blocking_queue_.Dequeue();
    }
}

void NonBlockingLock::ReleaseWriteLock(TxNumber tx_number, CcShard *ccs)
{
    write_tx_ = 0;
    is_write_lock_empty_ = true;
    // ccs->DeleteLockHolidngTx(tx_number);

    while (blocking_queue_.Size() > 0)
    {
        const LockQueueEntry &queue_head = blocking_queue_.Peek();
        if (queue_head.lk_type_ == LockType::Read)
        {
            bool success =
                AcquireReadLock(queue_head.req_, queue_head.tx_term_);
            assert(success);
            bool is_free = queue_head.req_->Execute(*ccs);
            if (is_free)
            {
                // Blocked cc requests are not in the cc processing queue and
                // hence needs to be freed here.
                queue_head.req_->Free();
            }
            blocking_queue_.Dequeue();
        }
        else
        {
            // The write request in the blocking queue must be under the locking
            // protocol.
            bool success = AcquireWriteLock(
                queue_head.req_, queue_head.tx_term_, CcProtocol::Locking);
            assert(success);
            bool is_free = queue_head.req_->Execute(*ccs);
            if (is_free)
            {
                // Blocked cc requests are not in the cc processing queue and
                // hence needs to be freed here.
                queue_head.req_->Free();
            }
            blocking_queue_.Dequeue();
            break;
        }
    }
}

void NonBlockingLock::AcquireReadIntention(TxNumber tx_number)
{
    read_intentions_.emplace(tx_number);
}

void NonBlockingLock::ReleaseReadIntention(TxNumber tx_number)
{
    read_intentions_.erase(tx_number);
}

bool NonBlockingLock::IsEmpty() const
{
    return read_intentions_.size() == 0 && read_locks_.size() == 0 &&
           is_write_lock_empty_ && blocking_queue_.Size() == 0;
}

TxNumber NonBlockingLock::WriteTx() const
{
    return write_tx_;
}

bool NonBlockingLock::HasWriteLock() const
{
    return !is_write_lock_empty_;
}

void NonBlockingLock::ClearTx(TxNumber tx_number, CcShard *ccs)
{
    if (!is_write_lock_empty_ && write_tx_ == tx_number)
    {
        ReleaseWriteLock(tx_number, ccs);
    }
    else
    {
        ReleaseReadIntention(tx_number);
        ReleaseReadLock(tx_number, ccs);
    }
}

const std::unordered_set<TxNumber> &NonBlockingLock::ReadLocks() const
{
    return read_locks_;
}
}  // namespace txservice