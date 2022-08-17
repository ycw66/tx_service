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
                                   ScanType scan_type,
                                   bool is_sk)
{
    // cce payload_status is unknown means it is a cache miss read. Hence should
    // not acquire any lock.
    // sk also needs read lock, regardless of IsolationLevel.
    if ((req.Isolation() >= IsolationLevel::RepeatableRead || is_sk) &&
        payload_status != RecordStatus::Unknown &&
        req.Protocol() != CcProtocol::MVCC)
    {
        /*gap lock has not been implemented, just a place holder*/
        if (scan_type == ScanType::ScanGap)
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
        TX_TRACE_ACTION_WITH_CONTEXT(
            this,
            cce,
            (
                [&req, &ng_term, cce]() -> std::string
                {
                    return std::string("\"tx_number\":")
                        .append(std::to_string(req.Txn()))
                        .append(",\"tx_term\":")
                        .append(std::to_string(ng_term))
                        .append(",\"cce_ptr\":")
                        .append(
                            std::to_string(reinterpret_cast<uint64_t>(cce)));
                }));
        return true;
    }
    else
    {
        // No locking is necessary for ReadCommitted and Snapshot isolation
        // level.
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

bool CcMap::AcquireWriteLockOnExistingCcEntry(
    AcquireCc &req,
    bool resume,
    CcHandlerResult<AcquireKeyResult> *hd_res,
    AcquireKeyResult &acquire_key_result,
    int64_t ng_term,
    LruEntry &cc_entry)
{
    int64_t tx_term = req.TxTerm();

    // On execution resumption, the write lock has been acquired when
    // being unblocked.
    bool lock_success = resume ? true
                               : cc_entry.key_lock_.AcquireWriteLock(
                                     &req, tx_term, req.Protocol());

    if (lock_success)
    {
        shard_->UpsertLockHoldingTx(req.Txn(), req.TxTerm(), &cc_entry, true);
        // for mvcc
        uint64_t lock_ts = std::max(req.Ts(), shard_->Now());
        cc_entry.wlock_ts_ = lock_ts;

        // Updates last_vali_ts after successfully acquiring the write
        // lock such that it is no smaller than the current time of
        // the shard. The net effect is that the tx acquiring the write
        // lock is forced not to commit at a time earlier than the
        // clock of this cc node, even if the clock of the tx's
        // coordinator node drifts and falls behind. Checkpointing
        // relies on this property to avoid picking a checkpoint ts in
        // this shard that may overlap with the ongoing tx.
        acquire_key_result.last_vali_ts_ =
            std::max(cc_entry.last_read_ts_, lock_ts);
        acquire_key_result.commit_ts_ = cc_entry.commit_ts_;

        hd_res->SetFinished();
    }
    else
    {
        TX_TRACE_ACTION_WITH_CONTEXT(
            &req,
            "AcquireWriteLock.Fail",
            reinterpret_cast<LruEntry *>(&cc_entry),
            [&req]() -> std::string
            {
                return std::string(",\"tx_number\":")
                    .append(std::to_string(req.Txn()))
                    .append(",\"term\":")
                    .append(std::to_string(req.TxTerm()));
            });
        const std::unordered_set<TxNumber> &read_locks =
            cc_entry.key_lock_.ReadLocks();
        if (read_locks.size() > 0)
        {
            TX_TRACE_DUMP_WITH_CONTEXT(
                &read_locks,
                [&cc_entry]() -> std::string
                {
                    return std::string("\"CcEntry\":")
                        .append(FMT_POINTER_TO_UINT64T(&cc_entry))
                        .append(",\"associate\":\"key_lock_.read_locks\"");
                });
            // If the request fails to acquire the write lock because of
            // read locks, checks each read lock and recovers if needed.
            for (const auto &read_tx : read_locks)
            {
                // delete &read_tx;
                shard_->CheckRecoverTx(read_tx, req.NodeGroupId(), ng_term);
            }
        }
        else
        {
            TX_TRACE_DUMP_WITH_CONTEXT(
                cc_entry.key_lock_.WriteLockTx(),
                [&cc_entry]() -> std::string
                {
                    return std::string("\"CcEntry\":")
                        .append(FMT_POINTER_TO_UINT64T(&cc_entry))
                        .append(",\"associate\":\"key_lock_.write_lock\"");
                });

            // The request fails because of write-write conflicts.
            assert(cc_entry.key_lock_.HasWriteLock());
            shard_->CheckRecoverTx(
                cc_entry.key_lock_.WriteLockTx(), req.NodeGroupId(), ng_term);
        }

        if (req.Protocol() == CcProtocol::OCC ||
            req.Protocol() == CcProtocol::MVCC)
        {
            // For OCC/MVCC, a conflict causes the tx to abort
            // immediately.
            hd_res->SetError(1);
            return true;
        }
        else
        {
            // For 2PL, a conflict blocks the tx by putting it into the
            // lock's blocking queue.

            uint32_t tx_node = (req.Txn() >> 32L) >> 10;
            if (tx_node != req.NodeGroupId())
            {
                // If the acquire request comes from a remote node,
                // sends acknowledgement to the sender when the request
                // is blocked.
                remote::RemoteAcquire &remote_req =
                    static_cast<remote::RemoteAcquire &>(req);
                remote_req.Acknowledge();
            }

            return false;
        }
    }

    return true;
}

}  // namespace txservice
