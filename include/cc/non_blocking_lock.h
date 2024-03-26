#pragma once

#include <butil/logging.h>

#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "cc_protocol.h"
#include "cc_req_base.h"
#include "circular_queue.h"
#include "error_messages.h"
#include "tx_command.h"
#include "tx_id.h"
#include "tx_object.h"

namespace txservice
{
template <typename KeyT, typename ValueT>
struct CcEntry;

class CcMap;
struct LruPage;

class NonBlockingLock
{
public:
    using Uptr = std::unique_ptr<NonBlockingLock>;

    enum struct WriteLockType
    {
        NoWritelock = 0,
        WriteLock,
        WriteIntent
    };

    NonBlockingLock()
    {
    }

    ~NonBlockingLock()
    {
    }

    NonBlockingLock(const NonBlockingLock &rhs) = delete;
    NonBlockingLock(NonBlockingLock &&rhs) = delete;
    NonBlockingLock &operator=(NonBlockingLock &&rhs) = delete;

    void Reset()
    {
        read_intentions_.clear();
        read_locks_.clear();
        write_lk_type_ = WriteLockType::NoWritelock;
        write_txn_ = 0;
        blocking_queue_.Reset();
        wlock_ts_ = 0;
    }

    /**
     * @brief Tries to acqurie the write lock. The operation succeeds, if no one
     * is holding the read lock, the write lock or the write intent. Note that
     * the write lock does not conflict with read intentions. The net effect of
     * the failed operation varies by concurrency control (cc) protocols: for
     * 2PL, the request is put into a waiting queue; for OCC/MVCC protocols, the
     * request returns without blocking.
     *
     * @param cc_req The cc request that tries to acquire the write lock.
     * @param protocol The cc protocol control the tx uses.
     * @return true, if the request acquires the write lock successfully; false,
     * if the request is blocked and put into the waiting queue.
     */
    bool AcquireWriteLock(CcRequestBase *cc_req, CcProtocol protocol);

    bool ReleaseWriteLock(TxNumber tx_number, CcShard *ccs);

    /**
     *  @brief Release the write lock and add the write intent, take effect only
     * when write lock is owned by the tx_number
     */
    void DowngradeWriteLock(TxNumber tx_number, CcShard *ccs);

    bool AcquireWriteIntent(CcRequestBase *cc_req, CcProtocol protocol);

    bool ReleaseWriteIntent(TxNumber tx_number, CcShard *ccs);

    /**
     * @brief Tries to acquire the read lock. Only tx's under 2PL acquire read
     * locks. The operation succeeds, if no one is holding the write lock and no
     * write lock request is blocked. The operation is blocked and put into the
     * waiting queue, if the write lock is held by someone else or someone is
     * blocked and waiting for the write lock.
     *
     * @param cc_req The cc request that tries to acquire the write lock.
     * @return true, if the request acquires the read lock successfully; false,
     * if the request is blocked and put into the blocking queue.
     */
    bool AcquireReadLock(CcRequestBase *cc_req);

    bool ReleaseReadLock(TxNumber tx_number, CcShard *ccs);

    /**
     * @brief Acquires a read intent. Tx's under OCC/MVCC acquire read intents
     * for read operations. Read intents do not block writes. Their goal is to
     * prevent the cache replacement algorithm from kicking out the cc entry
     * from the cc map. Hence, acquiring read intent always succeeds.
     *
     * @param tx_number The tx who acquires the read intention
     * @return true, if the tx adds a read intent to the lock.
     * @return false, if the tx already holds a lock/intent higher than read
     * intent and no read intent is added.
     */
    bool AcquireReadIntent(TxNumber tx_number);

    bool ReleaseReadIntent(TxNumber tx_number);

    LockOpStatus AcquireLock(CcRequestBase *cc_req,
                             CcProtocol protocol,
                             LockType lock_type);

    void InsertBlockingQueue(CcRequestBase *cc_req, LockType lock_type);

    bool IsEmpty() const;

    TxNumber WriteLockTx() const;

    bool HasWriteLock() const;

    std::pair<TxNumber, WriteLockType> WriteTx() const
    {
        return {write_txn_, write_lk_type_};
    }

    bool HasWriteLockOrWriteIntent(TxNumber txn) const
    {
        return write_lk_type_ != WriteLockType::NoWritelock &&
               write_txn_ == txn;
    }

    bool HasWriteLock(TxNumber txn) const
    {
        return write_lk_type_ == WriteLockType::WriteLock && write_txn_ == txn;
    }

    LockType ClearTx(TxNumber tx_number, CcShard *ccs);

    const std::unordered_set<TxNumber> &ReadLocks() const;
    const std::unordered_set<TxNumber> &ReadIntents() const;

    uint64_t WLockTs() const
    {
        return wlock_ts_;
    }
    void SetWLockTs(uint64_t ts)
    {
        wlock_ts_ = ts;
    }

    std::string DebugInfo()
    {
        std::string debug_string = "read_intentions: ";
        for (auto it = read_intentions_.begin(); it != read_intentions_.end();
             it++)
        {
            debug_string.append(std::to_string(*it));
            debug_string.append(",");
        }

        debug_string.append(" ,read_locks: ");
        for (auto it = read_locks_.begin(); it != read_locks_.end(); it++)
        {
            debug_string.append(std::to_string(*it));
            debug_string.append(",");
        }

        debug_string.append(" ,write_lock: ");
        if (write_lk_type_ != WriteLockType::NoWritelock)
        {
            debug_string.append(std::to_string(write_txn_));
            debug_string.append(":");
            if (write_lk_type_ == WriteLockType::WriteLock)
            {
                debug_string.append("lock");
            }
            else
            {
                debug_string.append("intent");
            }
        }
        else
        {
            debug_string.append("empty");
        }

        return debug_string;
    }
    std::vector<TxNumber> GetBlockTxIds(TxNumber exclude_id);
    void AbortQueueRequest(TxNumber txid,
                           CcErrorCode err = CcErrorCode::DEAD_LOCK_ABORT);
    bool FindQueueRequest(TxNumber txid);

private:
    struct LockQueueEntry
    {
        LockQueueEntry() = default;

        LockQueueEntry(CcRequestBase *req, LockType type)
            : req_(req), lk_type_(type)
        {
        }

        LockQueueEntry(const LockQueueEntry &rhs)
        {
            req_ = rhs.req_;
            lk_type_ = rhs.lk_type_;
        }

        LockQueueEntry(LockQueueEntry &&rhs)
        {
            req_ = rhs.req_;
            lk_type_ = rhs.lk_type_;
        }

        LockQueueEntry &operator=(const LockQueueEntry &rhs)
        {
            if (this != &rhs)
            {
                req_ = rhs.req_;
                lk_type_ = rhs.lk_type_;
            }

            return *this;
        }

        LockQueueEntry &operator=(LockQueueEntry &&rhs)
        {
            if (this != &rhs)
            {
                req_ = rhs.req_;
                lk_type_ = rhs.lk_type_;
            }

            return *this;
        }

        CcRequestBase *req_{nullptr};
        LockType lk_type_{LockType::ReadLock};
    };

    void ExecuteQueuedRequest(const LockQueueEntry &queue_head, CcShard *ccs);
    void UpgradeLock(TxNumber tx_number, LockType lock_type);
    void TryPopBlockingQueue(CcShard *ccs);

    bool NoReadLockConflict(TxNumber tx_number) const
    {
        return read_locks_.empty() ||
               (read_locks_.size() == 1 && *read_locks_.begin() == tx_number);
    }

    bool NoWriteLockConflict(TxNumber tx_number) const
    {
        return write_lk_type_ != WriteLockType::WriteLock ||
               write_txn_ == tx_number;
    }

    bool NoWriteConflict(TxNumber txn) const
    {
        return write_lk_type_ == WriteLockType::NoWritelock ||
               write_txn_ == txn;
    }

    // Read intentions do not block writes. They are used by a tx under
    // OCC/MVCC protocols to mark that the tx is accessing the data item and
    // to prevent the cache replacement algorithm from kicking out the
    // item's concurrency control (cc) entry from the cc map before the tx
    // finishes.
    std::unordered_set<TxNumber> read_intentions_;
    // Tx's who have acquired read locks
    std::unordered_set<TxNumber> read_locks_;

    WriteLockType write_lk_type_;
    TxNumber write_txn_{0};

    // The time when a write tx acquires the write lock on this lock.
    uint64_t wlock_ts_;

    // blocking_queue_ stores the requests that 1) want to acquire
    // lock/intent but failed due to conflict, or 2) want to read a pk
    // record whose commit ts is less than the commit ts of the
    // corresponding secondary index the transaction just read, under which
    // circumstance the pk read should wait until the pk is updated to
    // maintain the consistency of pk-sk mapping. There are four types of
    // lock requests can be in blocking queue: Write Lock(WL), Write
    // Intent(WI), Read Lock(RL) and No Lock(NL). The conflict map is that
    // WL conflicts with WL/WI/RL, WI conflicts with WL/WI and RL conflicts
    // with WL. NL denotes a pk read request under read committed isolation
    // level. NL requests are always inserted to the beginning of
    // blocking_queue_ because it will not introduce any conflict with other
    // tansactions.
    CircularQueue<LockQueueEntry> blocking_queue_;

    template <typename KeyT, typename ValueT>
    friend struct CcEntry;
};

/**
 * The lock structure and extra data fields that are accessed with lock
 * acquired. This structure is assigned on-demand to reduce CcEntry's memory
 * overhead.
 */
class KeyGapLockAndExtraData
{
public:
    using uptr = std::unique_ptr<KeyGapLockAndExtraData>;

    KeyGapLockAndExtraData() = default;

    void Reset(CcMap *ccm, LruPage *page)
    {
        key_lock_.Reset();
        ccm_ = ccm;
        page_ = page;
    }

    void SetUsedStatus(bool is_used);

    bool GetUsedStatus() const
    {
        return in_use_;
    }

    NonBlockingLock *KeyLock()
    {
        return &key_lock_;
    }

    CcMap *GetCcMap() const
    {
        return ccm_;
    }

    LruPage *GetCcPage() const
    {
        return page_;
    }

    void UpdateCcPage(LruPage *new_page)
    {
        page_ = new_page;
    }

    bool IsEmpty()
    {
#ifdef ON_KEY_OBJECT
        if (key_lock_.IsEmpty())
        {
            // There must be no pending command and dirty payload if lock is
            // empty.
            assert((std::holds_alternative<TxCommand *>(pending_cmd_)
                        ? std::get<TxCommand *>(pending_cmd_) == nullptr
                        : std::get<std::unique_ptr<TxCommand>>(pending_cmd_) ==
                              nullptr) &&
                   dirty_payload_ == nullptr &&
                   dirty_payload_status_ == RecordStatus::NonExistent);
        }
#endif
        return key_lock_.IsEmpty();
    }

#ifdef ON_KEY_OBJECT
    std::variant<TxCommand *, std::unique_ptr<TxCommand>> PendingCmd()
    {
        return std::move(pending_cmd_);
    }

    void SetPendingCmd(
        std::variant<TxCommand *, std::unique_ptr<TxCommand>> cmd)
    {
        pending_cmd_ = std::move(cmd);
    }
    bool IsNullPendingCmd()
    {
        return std::holds_alternative<TxCommand *>(pending_cmd_)
                   ? std::get<TxCommand *>(pending_cmd_) == nullptr
                   : std::get<std::unique_ptr<TxCommand>>(pending_cmd_) ==
                         nullptr;
    }
    std::unique_ptr<TxObject> DirtyPayload()
    {
        return std::move(dirty_payload_);
    }

    void SetDirtyPayload(std::unique_ptr<TxObject> dirty_payload)
    {
        dirty_payload_ = std::move(dirty_payload);
    }

    RecordStatus DirtyPayloadStatus()
    {
        return dirty_payload_status_;
    }

    void SetDirtyPayloadStatus(RecordStatus status)
    {
        dirty_payload_status_ = status;
    }

    std::unique_ptr<ReplayTxnCmdList> ReplayCommandList()
    {
        return std::move(replay_cmd_list_);
    }

    void SetReplayCommandList(std::unique_ptr<ReplayTxnCmdList> replay_list)
    {
        replay_cmd_list_ = std::move(replay_list);
    }

    bool HasReplayCommandList()
    {
        return replay_cmd_list_ != nullptr;
    }

#endif

private:
    NonBlockingLock key_lock_;
    bool in_use_{false};
    CcMap *ccm_{nullptr};
    LruPage *page_{nullptr};

#ifdef ON_KEY_OBJECT
    std::variant<TxCommand *, std::unique_ptr<TxCommand>> pending_cmd_{nullptr};
    // temporary object to process subsequent commands in the same txn
    std::unique_ptr<TxObject> dirty_payload_;
    // status of temporary object
    RecordStatus dirty_payload_status_{RecordStatus::NonExistent};
    std::unique_ptr<ReplayTxnCmdList> replay_cmd_list_;
#endif
};

}  // namespace txservice
