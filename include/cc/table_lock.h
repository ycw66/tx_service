#pragma once

#include <unordered_set>

#include "cc_req_base.h"
#include "circular_queue.h"
#include "tx_id.h"

namespace txservice
{
/*
   Table Level Lock implementation
   There are two lock types: write lock and read intensions.
   Write lock is acquired by DDLs such as CREATE TABLE or DROP TABLE, it will
   block any DML request, e.g. select, insert, delete queries. Write lock needs
   to be acquired on all the shards. Read intension is acquired when table is
   opened. Read intension only needs to be acquired on one shard. Since write
   lock is distributed on all the shards, we can ensure the DML queries will be
   blocked by DDLs no matter which shards it is running.
   TODO: find-grained table level lock.
 */
class TableLock
{
public:
    TableLock()
        : read_intentions_(),
          write_lock_(0),
          is_write_lock_empty_(true),
          read_blocking_queue_(),
          write_blocking_queue_()
    {
    }

    TableLock(const TableLock &rhs) = delete;

    /**
       Acquire table write lock. It will block any table operation including
       table DDL, tabel read and table write etc. One example is to let DDL
       operation acquire table write lock which is used to to prevent concurrent
       DML operations.
     */
    bool AcquireWrite(CcRequestBase *cc_req)
    {
        if (is_write_lock_empty_ && read_intentions_.size() == 0)
        {
            write_lock_ = cc_req->Tx();
            is_write_lock_empty_ = false;
            return true;
        }
        else if (is_write_lock_empty_ && read_intentions_.size() == 1 &&
                 read_intentions_.find(cc_req->Tx()) != read_intentions_.end())
        {
            // upgrade the read intension to write lock
            read_intentions_.erase(cc_req->Tx());
            write_lock_ = cc_req->Tx();
            is_write_lock_empty_ = false;
            return true;
        }
        else
        {
            write_blocking_queue_.Enqueue(cc_req);
            return false;
        }
    }

    /**
       Isolation level greater than or equal to RepeatableRead needs to acquire
       table read intention during DML operation. This is used to protect
       concurrent DDL. Table read intention will be released at the end the each
       DML operation during commit stage.
     */
    bool AcquireReadIntention(CcRequestBase *cc_req)
    {
        if (is_write_lock_empty_ && write_blocking_queue_.Size() == 0)
        {
            auto table_iter = read_intentions_.find(cc_req->Tx());

            if (table_iter == read_intentions_.end())
            {
                read_intentions_.try_emplace(cc_req->Tx(), 1);
            }
            else
            {
                uint32_t &intention_count = table_iter->second;
                intention_count++;
            }
            return true;
        }
        else
        {
            read_blocking_queue_.Enqueue(cc_req);
            return false;
        }
    }

    void ReleaseReadIntention(TxNumber tx_number, CcShard *ccs)
    {
        auto table_iter = read_intentions_.find(tx_number);

        if (table_iter != read_intentions_.end())
        {
            uint32_t &intention_count = table_iter->second;
            intention_count--;

            if (intention_count == 0)
                read_intentions_.erase(table_iter);
        }

        if (read_intentions_.size() == 0 && write_blocking_queue_.Size() > 0)
        {
            CcRequestBase *cc_req = write_blocking_queue_.Peek();
            cc_req->Execute(*ccs);
            write_blocking_queue_.Dequeue();
        }
    }

    void ReleaseWrite(TxNumber tx_number, CcShard *ccs)
    {
        write_lock_ = 0;
        is_write_lock_empty_ = true;

        if (read_blocking_queue_.Size() > 0)
        {
            while (read_blocking_queue_.Size() > 0)
            {
                CcRequestBase *cc_req = read_blocking_queue_.Peek();
                cc_req->Execute(*ccs);
                read_blocking_queue_.Dequeue();
            }
        }
        else if (write_blocking_queue_.Size() > 0)
        {
            CcRequestBase *cc_req = write_blocking_queue_.Peek();
            cc_req->Execute(*ccs);
            write_blocking_queue_.Dequeue();
        }
    }

    void ReleaseAllTableLocks(TxNumber tx_number, CcShard *ccs)
    {
        write_lock_ = 0;
        is_write_lock_empty_ = true;
        read_intentions_.erase(tx_number);

        if (read_blocking_queue_.Size() > 0)
        {
            while (read_blocking_queue_.Size() > 0)
            {
                CcRequestBase *cc_req = read_blocking_queue_.Peek();
                cc_req->Execute(*ccs);
                read_blocking_queue_.Dequeue();
            }
        }
        else if (write_blocking_queue_.Size() > 0)
        {
            CcRequestBase *cc_req = write_blocking_queue_.Peek();
            cc_req->Execute(*ccs);
            write_blocking_queue_.Dequeue();
        }
    }

    // read intention is not the table read lock. It is only used to block
    // table write lock, but will not conflict with any tuple write lock.
    // each read intention is a pair of txnumber and count. uint32 is sufficient
    // since read intension will upgrade to table read lock when the number
    // tuple read lock exceeds a threshold
    std::unordered_map<TxNumber, uint32_t> read_intentions_;

    TxNumber write_lock_;
    bool is_write_lock_empty_;

    CircularQueue<CcRequestBase *> read_blocking_queue_;
    CircularQueue<CcRequestBase *> write_blocking_queue_;
};

}  // namespace txservice
   // namespace txservice
