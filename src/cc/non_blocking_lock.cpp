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

    if (is_write_lock_empty_ && is_write_intent_empty_)
    {
        if (read_locks_.empty())
        {
            write_lock_tx_ = tx_number;
            is_write_lock_empty_ = false;
            // Upgrades the read intention, if there is any, to the write
            // lock.
            if (protocol == CcProtocol::OCC)
            {
                read_intentions_.erase(tx_number);
            }
            return true;
        }
        else if (read_locks_.size() == 1 && *read_locks_.begin() == tx_number)
        {
            // Upgrades the read lock to the write lock.
            read_locks_.erase(read_locks_.begin());
            write_lock_tx_ = tx_number;
            is_write_lock_empty_ = false;
            return true;
        }
        else if (protocol == CcProtocol::OCC)
        {
            // OCC/MVCC protocols are non-blocking. So, if the write
            // conflicts with one or more read locks, gives up acquiring the
            // write lock without entering into the blocking queue.
            return false;
        }
    }
    else if (!is_write_lock_empty_ && write_lock_tx_ == tx_number)
    {
        // The tx has acquired the write lock and tries to acquire the same
        // lock again.
        return true;
    }
    else if (!is_write_intent_empty_ && write_intent_tx_ == tx_number &&
             read_locks_.empty())
    {
        // The tx has acquired the write intent and tries to upgrade the intent
        // to a lock. Upgrade is successful if there is no read lock.
        write_intent_tx_ = 0;
        is_write_intent_empty_ = true;
        write_lock_tx_ = tx_number;
        is_write_lock_empty_ = false;

        return true;
    }
    else if (protocol == CcProtocol::OCC)
    {
        // Conflicts with an existing write lock or write intent. For OCC/MVCC
        // protocols, the tx gives up and aborts immediately.
        return false;
    }

    // Read-write or write-write conflicts. Blocks the request by putting it
    // into the blocking queue.
    blocking_queue_.Enqueue(
        LockQueueEntry(cc_req, LockType::WriteLock, tx_term));
    // Upgrades the read lock, if there is any.
    read_locks_.erase(tx_number);
    return false;
}

bool NonBlockingLock::AcquireReadLock(CcRequestBase *cc_req, int64_t tx_term)
{
    TxNumber txn = cc_req->Txn();

    // In theory, a tx should not acquire a read lock after acquiring a write
    // lock.

    if (is_write_lock_empty_)
    {
        if (read_locks_.find(txn) != read_locks_.end())
        {
            return true;
        }
        else if (!is_write_intent_empty_ && write_intent_tx_ == txn)
        {
            return true;
        }
        else if (blocking_queue_.Size() == 0 ||
                 blocking_queue_.Peek().lk_type_ == LockType::WriteIntent)
        {
            // A read lock request succeeds if there is no read-write conflict.
            // It is also blocked when there is a write lock request being
            // blocked, to prevent starvation. In theory, this calls for a scan
            // of the blocking queue. We employ a simple alternative approach:
            // if the head of the blocking queue is a write intent request,
            // which is blocked because of the conflict with the existing write
            // intent, the read lock request is allowed to proceed without
            // worrying starvation. Read lock requests will start getting
            // blocked once the write lock request "enters the scene".
            read_locks_.emplace(txn);
            return true;
        }
        else
        {
            assert(blocking_queue_.Peek().lk_type_ == LockType::WriteLock);
        }
    }
    else if (write_lock_tx_ == txn)
    {
        // The tx has acquired the write lock and tries to acquire the read lock
        // again. In theory, this should never happen.
        return true;
    }

    // The read lock request is blocked when there is a write lock. It is
    // too blocked when where is no conflict, but there is a write lock
    // request in the blocking queue. To prevent starvation, the read lock
    // request is enqueued after the write request.
    blocking_queue_.Enqueue(
        LockQueueEntry(cc_req, LockType::ReadLock, tx_term));
    return false;
}

void NonBlockingLock::ReleaseReadLock(TxNumber tx_number, CcShard *ccs)
{
    size_t removed_cnt = read_locks_.erase(tx_number);

    if (removed_cnt > 0 && read_locks_.empty() && blocking_queue_.Size() > 0 &&
        is_write_intent_empty_)
    {
        const LockQueueEntry &queue_head = blocking_queue_.Peek();
        // Read locks only block write locks. If someone is in the blocking
        // queue, it must be a write lock request.
        assert(queue_head.lk_type_ == LockType::WriteLock);
        write_lock_tx_ = queue_head.req_->Txn();
        is_write_lock_empty_ = false;
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
    if (is_write_lock_empty_ || write_lock_tx_ != tx_number)
    {
        return;
    }

    write_lock_tx_ = 0;
    is_write_lock_empty_ = true;

    while (blocking_queue_.Size() > 0)
    {
        const LockQueueEntry &queue_head = blocking_queue_.Peek();
        if (queue_head.lk_type_ == LockType::ReadLock)
        {
            assert(read_locks_.empty());

            read_locks_.emplace(queue_head.req_->Txn());
            bool is_free = queue_head.req_->Execute(*ccs);
            if (is_free)
            {
                // Blocked cc requests are not in the cc processing queue and
                // hence needs to be freed here.
                queue_head.req_->Free();
            }
            blocking_queue_.Dequeue();
        }
        else if (queue_head.lk_type_ == LockType::WriteIntent)
        {
            write_intent_tx_ = queue_head.req_->Txn();
            is_write_intent_empty_ = false;

            bool is_free = queue_head.req_->Execute(*ccs);
            if (is_free)
            {
                // Blocked cc requests are not in the cc processing queue and
                // hence needs to be freed here.
                queue_head.req_->Free();
            }
            blocking_queue_.Dequeue();
        }
        else if (queue_head.lk_type_ == LockType::WriteLock)
        {
            if (!is_write_intent_empty_ || !is_write_lock_empty_ ||
                !read_locks_.empty())
            {
                break;
            }

            write_lock_tx_ = queue_head.req_->Txn();
            is_write_lock_empty_ = false;

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

bool NonBlockingLock::AcquireWriteIntent(CcRequestBase *cc_req,
                                         int64_t tx_term,
                                         CcProtocol protocol)
{
    TxNumber tx_number = cc_req->Txn();

    if (is_write_lock_empty_ && is_write_intent_empty_)
    {
        if (read_locks_.erase(tx_number) > 0)
        {
            // A write intent is a special read lock in that it does not block
            // reads but only block write locks and intents. If the tx already
            // holds a read lock, "upgrades" it to the write intent.
            is_write_intent_empty_ = false;
            write_intent_tx_ = tx_number;
            return true;
        }
        else if (blocking_queue_.Size() == 0)
        {
            // A write intent does not conflict with read locks. But existing
            // read locks, if there are any, block write lock requests. To be
            // fair, if there is a write lock request in the blocking queue,
            // blocks this write intent request as well.
            is_write_intent_empty_ = false;
            write_intent_tx_ = tx_number;
            if (protocol == CcProtocol::OCC)
            {
                read_intentions_.erase(tx_number);
            }
            return true;
        }
        else
        {
            // Since read locks only block write lock requests, if the blocking
            // queue is not empty, the head of the blocking queue must be a
            // write lock request.
            assert(blocking_queue_.Peek().lk_type_ == LockType::WriteLock);
        }
    }
    else if (!is_write_lock_empty_ && write_lock_tx_ == tx_number)
    {
        return true;
    }
    else if (!is_write_intent_empty_ && write_intent_tx_ == tx_number)
    {
        // The tx has acquired the write intent and tries to acquire the same
        // intent again.
        return true;
    }
    else if (protocol == CcProtocol::OCC)
    {
        // Conflicts with an existing write lock or intent. For OCC/MVCC
        // protocols, gives up acquiring.
        return false;
    }

    blocking_queue_.Enqueue(
        LockQueueEntry(cc_req, LockType::WriteIntent, tx_term));
    read_locks_.erase(tx_number);
    return false;
}

void NonBlockingLock::ReleaseWriteIntent(TxNumber tx_number, CcShard *ccs)
{
    if (is_write_intent_empty_ || write_intent_tx_ != tx_number)
    {
        return;
    }

    write_intent_tx_ = 0;
    is_write_intent_empty_ = true;

    if (blocking_queue_.Size() > 0)
    {
        const LockQueueEntry &queue_head = blocking_queue_.Peek();
        // When someone is holding a write intent, since the write intent does
        // not block reads, the first request in the blocking queue must be a
        // write.
        assert(queue_head.lk_type_ == LockType::WriteIntent ||
               queue_head.lk_type_ == LockType::WriteLock);
        if (queue_head.lk_type_ == LockType::WriteIntent)
        {
            write_intent_tx_ = queue_head.req_->Txn();
            is_write_intent_empty_ = false;
            bool is_free = queue_head.req_->Execute(*ccs);
            if (is_free)
            {
                queue_head.req_->Free();
            }
            blocking_queue_.Dequeue();
        }
        else if (queue_head.lk_type_ == LockType::WriteLock &&
                 read_locks_.empty())
        {
            write_lock_tx_ = queue_head.req_->Txn();
            is_write_lock_empty_ = false;
            bool is_free = queue_head.req_->Execute(*ccs);
            if (is_free)
            {
                queue_head.req_->Free();
            }
            blocking_queue_.Dequeue();
        }
    }
}

void NonBlockingLock::AcquireReadIntent(TxNumber tx_number)
{
    read_intentions_.emplace(tx_number);
}

void NonBlockingLock::ReleaseReadIntent(TxNumber tx_number)
{
    read_intentions_.erase(tx_number);
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