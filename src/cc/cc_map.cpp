#include "cc/cc_map.h"

#include "cc/local_cc_shards.h"
#include "cc_entry.h"
#include "tx_trace.h"

namespace txservice
{
void CcMap::MoveRequest(CcRequestBase *cc_req, uint32_t target_core_id)
{
    shard_->local_shards_.EnqueueCcRequest(
        shard_->core_id_, target_core_id, cc_req);
}

void CcMap::RecoverReadLocks(LruEntry &cce, uint32_t node_group_id)
{
    int64_t ng_term = Sharder::Instance().LeaderTerm(node_group_id);
    const std::unordered_set<TxNumber> &read_locks = cce.key_lock_.ReadLocks();
    if (read_locks.empty())
    {
        return;
    }
    for (const auto &read_tx : read_locks)
    {
        shard_->CheckRecoverTx(read_tx, node_group_id, ng_term);
    }
}

void CcMap::RecoverWriteLock(const TxNumber &tx_number, uint32_t node_group_id)
{
    int64_t ng_term = Sharder::Instance().LeaderTerm(node_group_id);
    shard_->CheckRecoverTx(tx_number, node_group_id, ng_term);
}

void CcMap::RecoverWriteIntent(LruEntry &cce, uint32_t node_group_id)
{
    int64_t ng_term = Sharder::Instance().LeaderTerm(node_group_id);
    shard_->CheckRecoverTx(
        cce.key_lock_.WriteIntentTx(), node_group_id, ng_term);
}

bool CcMap::ConditionalReadLockCce(LruEntry *cce,
                                   CcRequestBase &req,
                                   LockType lock_type,
                                   int64_t tx_term,
                                   uint32_t cce_node_group_id,
                                   RecordStatus payload_status,
                                   int64_t ng_term,
                                   bool gap_lock)
{
    // cce payload_status is unknown means it is a cache miss read. Hence should
    // not acquire any lock.
    if (req.Isolation() >= IsolationLevel::RepeatableRead &&
        payload_status != RecordStatus::Unknown)
    {
        /*gap lock has not been implemented, just a place holder*/
        if (gap_lock)
        {
            return true;
        }

        TxNumber tx_number = req.Txn();
        if (lock_type == LockType::ReadLock)
        {
            bool lock_success =
                ReadLockCce(cce, req, tx_term, cce_node_group_id);

            if (!lock_success)
            {
                // TODO(Xiao Ji): Add remote acknowlege when lock fail
                return false;
            }
        }
        else
        {
            // ReadIntention prevents ccentry being kicked out from
            // cache, but will not block write lock.
            cce->key_lock_.AcquireReadIntent(tx_number);
        }

        shard_->UpsertLockHoldingTx(tx_number, tx_term, cce, false);
        return true;
    }
    else
    {
        // No locking is necessary for ReadCommitted isolation level
        return true;
    }
}

bool CcMap::ReadLockCce(LruEntry *cce,
                        CcRequestBase &req,
                        int64_t tx_term,
                        uint32_t cce_node_group_id,
                        bool gap_lock)
{
    /*gap lock has not been implemented, just a place holder*/
    if (gap_lock)
    {
        return true;
    }

    bool lock_success = false;

    if (!gap_lock)
    {
        lock_success = cce->key_lock_.AcquireLock(
            &req, tx_term, CcProtocol::Locking, LockType::ReadLock);
        if (!lock_success)
        {
            if (cce->key_lock_.HasWriteLock())
            {
                TX_TRACE_DUMP_WITH_CONTEXT(
                    cce->key_lock_.WriteLockTx(),
                    [cce]() -> std::string
                    {
                        return std::string("\"cce\":")
                            .append(FMT_POINTER_TO_UINT64T(cce))
                            .append(",\"associate\":\"key_lock_.write_lock\"");
                    });
                RecoverWriteLock(cce->key_lock_.WriteLockTx(),
                                 cce_node_group_id);
            }
            return false;
        }
    }
    else
    {
        lock_success = cce->gap_lock_.AcquireLock(
            &req, tx_term, CcProtocol::Locking, LockType::ReadLock);
        if (!lock_success)
        {
            if (cce->gap_lock_.HasWriteLock())
            {
                TX_TRACE_DUMP_WITH_CONTEXT(
                    cce->key_lock_.WriteLockTx(),
                    [cce]() -> std::string
                    {
                        return std::string("\"cce\":")
                            .append(FMT_POINTER_TO_UINT64T(cce))
                            .append(",\"associate\":\"key_lock_.write_lock\"");
                    });
                RecoverWriteLock(cce->gap_lock_.WriteLockTx(),
                                 cce_node_group_id);
            }
            return false;
        }
    }
    return true;
}
}  // namespace txservice
