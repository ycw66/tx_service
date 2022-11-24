#pragma once

#include <algorithm>  // std::max
#include <atomic>
#include <cassert>
#include <deque>
#include <map>
#include <memory>  // std::make_unique
#include <unordered_set>
#include <utility>  // std::move
#include <vector>

#include "cc_req_base.h"
#include "circular_queue.h"
#include "non_blocking_lock.h"
#include "tx_id.h"
#include "tx_key.h"
#include "tx_record.h"
#include "type.h"  // TableType

namespace txservice
{
class CcMap;

struct LruEntry;

template <typename KeyT, typename ValueT>
struct CcEntry;

struct UntypedInsertEntry
{
public:
    virtual ~UntypedInsertEntry() = default;
    virtual const LruEntry &Parent() const = 0;
};

template <typename KeyT, typename ValueT>
struct InsertEntry : public UntypedInsertEntry
{
public:
    InsertEntry(const KeyT &key,
                TxNumber txn,
                CcEntry<KeyT, ValueT> *parent_entry)
        : key_(key), txn_(txn), parent_entry_(parent_entry)
    {
    }

    const LruEntry &Parent() const override
    {
        return *parent_entry_;
    }

    const KeyT key_;  // owner of key_
    TxNumber txn_;
    CcEntry<KeyT, ValueT> *parent_entry_;
};

struct FlushRecord
{
    union PayloadPtr
    {
        const TxRecord *ptr_;
        std::unique_ptr<TxRecord> uptr_;
        ~PayloadPtr()
        {
        }
    };

    RecordStatus payload_status_{RecordStatus::Unknown};
    bool is_rec_owner_{false};
    PayloadPtr payload_{nullptr};
    uint64_t commit_ts_{1U};
    LruEntry *cce_;
    int32_t delta_size_{0};

    FlushRecord()
    {
    }
    ~FlushRecord()
    {
        if (is_rec_owner_)
        {
            payload_.uptr_.reset();
        }
    }

    FlushRecord(const FlushRecord &rhs) = delete;
    FlushRecord &operator=(const FlushRecord &rhs) = delete;

    FlushRecord &operator=(FlushRecord &&rhs)
    {
        if (this == &rhs)
        {
            return *this;
        }

        if (rhs.is_rec_owner_)
        {
            SetPayload(std::move(rhs.payload_.uptr_));
            is_rec_owner_ = true;
            rhs.is_rec_owner_ = false;
        }
        else
        {
            SetPayload(rhs.payload_.ptr_);
            is_rec_owner_ = false;
        }
        payload_status_ = rhs.payload_status_;
        commit_ts_ = rhs.commit_ts_;
        cce_ = rhs.cce_;
        return *this;
    }

    FlushRecord(FlushRecord &&rhs)
    {
        if (rhs.is_rec_owner_)
        {
            SetPayload(std::move(rhs.payload_.uptr_));
            is_rec_owner_ = rhs.is_rec_owner_;
            rhs.is_rec_owner_ = false;
        }
        else
        {
            SetPayload(rhs.payload_.ptr_);
            is_rec_owner_ = false;
        }
        payload_status_ = rhs.payload_status_;
        commit_ts_ = rhs.commit_ts_;
        cce_ = rhs.cce_;
    }

    void SetPayload(const TxRecord *ptr)
    {
        if (is_rec_owner_)
        {
            payload_.uptr_.reset();  // de-allocate the original record
        }
        payload_.ptr_ = ptr;
        is_rec_owner_ = false;
    }

    void SetPayload(std::unique_ptr<TxRecord> uptr)
    {
        if (is_rec_owner_)
        {
            // The move op will de-allocate the old record and obtain the
            // ownership of the input record.
            payload_.uptr_ = std::move(uptr);
        }
        else
        {
            // Need to call release() first, because the memory address stored
            // in payload_.uptr_ is not heap-allocated and payload_.uptr_ does
            // not own it.
            payload_.uptr_.release();
            payload_.uptr_ = std::move(uptr);
            is_rec_owner_ = true;
        }
    }

    const TxRecord *Payload() const
    {
        if (is_rec_owner_)
        {
            return payload_.uptr_.get();
        }
        return payload_.ptr_;
    }

    const TxKey *Key() const;
};

struct LruEntry
{
public:
    LruEntry() = delete;
    virtual ~LruEntry();

    LruEntry(CcMap *parent);

    virtual size_t GetCcEntryMemUsage() const = 0;

    virtual size_t ArchiveRecordsCount() const = 0;
    virtual size_t KickOutArchiveRecords(uint64_t oldest_active_tx_ts) = 0;
    virtual size_t ExportArchives(std::vector<FlushRecord> &akvs,
                                  uint64_t to_ts,
                                  TableType tbl_type) const = 0;

    /**
     * @brief Get key lock from lock array if it is null.
     *
     */
    NonBlockingLock &GetKeyLock();

    /**
     * @brief Get gap lock from lock array if it is null.
     *
     */
    NonBlockingLock &GetGapLock();

    /**
     * @brief When release a lock, ccentry should call TryResetKeyLock to try to
     * recycle the lock ptr to lock array if lock set is empty.
     *
     */
    void RecycleKeyLock();

    void RecycleGapLock();

    virtual const TxKey *Key() const = 0;

    /**
     * @brief check whether the entry can be kicked out from ccmap, iff no key
     * lock, no gap lock and not 'dirty' entry (entry which has been
     * checkpointed since the last change).
     *
     * @return true: entry can be kicked out.
     */
    bool IsFree();

    // Lru link which records the age of entries, when ccmap is full, kickout
    // the entries by the order of lru.
    LruEntry *lru_prev_{nullptr};
    LruEntry *lru_next_{nullptr};
    // Checkpoint link which is used by CkptScanCc to iterate and generate the
    // list of payload_ckpt_.
    LruEntry *ckpt_prev_{nullptr};
    LruEntry *ckpt_next_{nullptr};
    CcMap *const parent_map_;

    NonBlockingLock *key_lock_ptr_{nullptr};
    NonBlockingLock *gap_lock_ptr_{nullptr};

    uint64_t commit_ts_{1};
    // "last_read_ts_" is updated in tow cases:
    // (1) Read under MVCC+SnapshotIsolation: it will be updated to
    // max{read_ts, last_read_ts_} if latest version of ccentry less than
    // read timestamp, which pushes future transactions' commit
    // timestamps larger than the read timestamp of the current read
    // transaction;
    // (2) PostRead under OCC/LOCKING+RepeatableRead: it will be updated to
    // max{commit_ts,last_read_ts_} after releasing read intent/lock, which
    // pushes future transactions' commit timestamps larger than the largest
    // commit timestamp of all read transactions that have released the read
    // lock on the key;
    uint64_t last_read_ts_{1};

    uint64_t gap_commit_ts_{1};
    uint64_t gap_last_read_ts_{1};

    // Accumulated size of key-value pairs committed since last checkpoint.
    size_t estimate_ccentry_log_size_{0};

    // The timestamp when this record was last flushed to the data store. Unlike
    // other fields that are read/modified via a single thread, this field is
    // updated by a separate checkpointing thread, after it flushes changes to
    // the data store.
    std::atomic<uint64_t> ckpt_ts_{1};

    /**
     * @brief Accumulated size change since last checkpoint.
     *
     */
    std::atomic<int32_t> delta_size_{INT32_MAX};
};

/**
 * @brief Used as the result value type of read(scan/get) operation.
 *
 * @param payload_ptr_ Point to an payload saved in CcEntry or archives_.
 * Notice: "payload_ptr_" should not be deleted explicitly.
 * @param payload_status_
 * @param commit_ts_
 */
template <typename ValueT>
struct VersionResultRecord
{
public:
    const ValueT *payload_ptr_;
    RecordStatus payload_status_;
    uint64_t commit_ts_;

    VersionResultRecord()
        : payload_ptr_(nullptr),
          payload_status_(RecordStatus::Unknown),
          commit_ts_(1)
    {
    }
};

template <typename ValueT>
struct VersionRecord
{
public:
    std::unique_ptr<ValueT> payload_;
    uint64_t commit_ts_;
    RecordStatus payload_status_;

    VersionRecord()
        : payload_(nullptr),
          commit_ts_(1),
          payload_status_(RecordStatus::Unknown)
    {
    }

    VersionRecord(std::unique_ptr<ValueT> payload,
                  uint64_t commit_ts,
                  RecordStatus status)
        : payload_(std::move(payload)),
          commit_ts_(commit_ts),
          payload_status_(status)
    {
    }

    VersionRecord(const VersionRecord<ValueT> &rhs)
    {
        payload_ = std::make_unique<ValueT>(*rhs.payload_);
        payload_status_ = rhs.payload_status_;
        commit_ts_ = rhs.commit_ts_;
    }
    VersionRecord &operator=(const VersionRecord<ValueT> &rhs)
    {
        payload_ = std::make_unique<ValueT>(*rhs.payload_);
        payload_status_ = rhs.payload_status_;
        commit_ts_ = rhs.commit_ts_;
        return *this;
    }

    VersionRecord &operator=(VersionRecord<ValueT> &&rhs)
    {
        if (this != &rhs)
        {
            payload_ = std::move(rhs.payload_);
            payload_status_ = rhs.payload_status_;
            commit_ts_ = rhs.commit_ts_;
        }
        return *this;
    }

    size_t MemUsage() const
    {
        size_t mem_usage_ = sizeof(*this);
        if (payload_ != nullptr)
        {
            mem_usage_ += payload_->MemUsage();
        }
        return mem_usage_;
    }
};

/**
 * @brief A map entry in the concurrency control map. An entry governs
 * concurrency control of a data item and caches the data item's newest
 * committed value, as well as historical versions if needed.
 *
 * @tparam KeyT The key type of the data item
 * @tparam ValueT The value type of the data item
 */
template <typename KeyT, typename ValueT>
struct CcEntry : public LruEntry
{
public:
    /**
     * @brief A cc entry is initialized in the cc map with ts = 1, the beginning
     * of history. The record's status is initially unknown, until a tx reads
     * the key in the data store and brings the record into the cc entry, or a
     * tx updates the key with a new value. Ts = 0 is reserved to indicate
     * whether or not a key/gap is included in a scan's result.
     *
     * @param parent Pointer of the cc map to which the cc entry belongs
     */
    CcEntry(CcMap *parent)
        : LruEntry(parent),
          key_(nullptr),
          payload_status_(RecordStatus::Unknown),
          map_prev_(nullptr),
          map_next_(nullptr),
          archives_()
    {
        payload_ = std::make_unique<ValueT>();
    }

    ~CcEntry() = default;

    size_t GetCcEntryMemUsage() const override
    {
        size_t mem_usage_ = 0;
        size_t ptr_size = sizeof(nullptr);

        // LruEntry field members:
        // size of lru_prev_, lru_next_, ckpt_prev_, ckpt_next_, parent_map_
        mem_usage_ += 5 * ptr_size;
        // size of commit_ts_, last_read_ts_, gap_commit_ts_, gap_last_read_ts_,
        // ckpt_ts_
        mem_usage_ += 5 * sizeof(uint64_t);

        // size of key_lock_ptr_ and gap_lock_ptr_
        mem_usage_ += 2 * sizeof(uint64_t);

        // CcEntry field members:
        // size of pointer and KeyT
        mem_usage_ += ptr_size;
        if (key_ != nullptr)
        {
            mem_usage_ += key_->MemUsage();
        }
        // size of ValueT
        mem_usage_ += PayloadMemUsage();
        mem_usage_ += sizeof(RecordStatus);

        // TODO size of insert_intention_set_, not used yet
        // mem_usage_ += sizeof(insert_intention_set_) +
        //               insert_intention_set_.size() * 2 * ptr_size;

        // size of map_prev_, map_next_
        mem_usage_ += 2 * ptr_size;

        // size of archives_ and its content
        mem_usage_ += ptr_size;
        mem_usage_ += GetArchiveMemUsage();

        return mem_usage_;
    }

    size_t PayloadMemUsage() const
    {
        if (payload_ == nullptr)
        {
            return 0;
        }
        else
        {
            return payload_->MemUsage();
        }
    }

    size_t PayloadSize() const
    {
        return payload_ == nullptr ? 0 : payload_->Size();
    }

    const KeyT *key_;
    std::unique_ptr<ValueT> payload_;
    RecordStatus payload_status_;

    std::map<const KeyT *,
             std::unique_ptr<InsertEntry<KeyT, ValueT>>,
             PtrLessThan<KeyT>>
        insert_intention_set_;

    CcEntry<KeyT, ValueT> *map_prev_;
    CcEntry<KeyT, ValueT> *map_next_;

    // save versions exclude the current version.(descending order,eg.[4,3,2,1])
    std::unique_ptr<std::deque<VersionRecord<ValueT>>> archives_;

    /**
     * @brief Move(not copy) the current version (payload, payload_status,
     * commit_ts) to the archives_.
     *
     * @param move_payload true - move "payload", "payload_status" and
     * "commit_ts" of CcEntry to "archives_"; false - not move the payload ,
     * just copy "payload_status" and "commit_ts". For "PkIndex", we should set
     * it to "true", for "SkIndex", we should set it to "false".
     *
     * @return New memory usage caused by archive
     */
    size_t ArchiveBeforeUpdate(TableType tbl_type)
    {
        if (payload_status_ == RecordStatus::Unknown)
        {
            return 0;
        }

        size_t mem_usage = 0;
        if (archives_ == nullptr)
        {
            archives_ = std::make_unique<std::deque<VersionRecord<ValueT>>>();
            mem_usage += sizeof(*archives_);
        }

        if (archives_->size() > 0)
        {
            assert(commit_ts_ > archives_->front().commit_ts_);
        }

        if (tbl_type != TableType::Secondary)
        {
            archives_->emplace_front(
                std::move(payload_), commit_ts_, payload_status_);
        }
        else
        {
            // For SkIndex, all versions' payload is not changed.
            archives_->emplace_front(nullptr, commit_ts_, payload_status_);
        }
        mem_usage += sizeof(VersionResultRecord<ValueT>);

        if (ckpt_ts_ >= commit_ts_ && commit_ts_ != 1U)
        {
            // This version has not been flushed to archive table, adjust
            // 'ckpt_ts_' to let this version can be flushed at next checkpoint.
            ckpt_ts_.store(commit_ts_ - 1);
        }

        return mem_usage;
    }

    /**
     * @brief Add a batch of historical versions.
     *@param v_recs versions descending ordered by commit_ts
     * @return New memory usage caused by archive
     */
    size_t AddArchiveRecords(std::vector<VersionTxRecord> &v_recs)
    {
        if (v_recs.size() == 0)
        {
            return 0;
        }

        size_t mem_usage = 0U;
        if (archives_ == nullptr)
        {
            archives_ = std::make_unique<std::deque<VersionRecord<ValueT>>>();
            mem_usage += sizeof(*archives_);
        }

        auto it = archives_->begin();
        for (; it != archives_->end(); it++)
        {
            if (it->commit_ts_ <= v_recs[0].commit_ts_)
            {
                break;
            }
        }
        for (auto &vrec : v_recs)
        {
            if (it == archives_->end() || it->commit_ts_ != vrec.commit_ts_)
            {
                it = archives_->emplace(it);
                it->commit_ts_ = vrec.commit_ts_;
                it->payload_status_ = vrec.record_status_;
                it->payload_.reset(
                    static_cast<ValueT *>(vrec.record_.release()));

                mem_usage += it->MemUsage();
            }
            it++;
        }
        return mem_usage;
    }

    size_t AddArchiveRecord(std::unique_ptr<ValueT> payload_ptr,
                            RecordStatus payload_status,
                            uint64_t commit_ts)
    {
        size_t mem_usage = 0U;
        if (archives_ == nullptr)
        {
            archives_ = std::make_unique<std::deque<VersionRecord<ValueT>>>();
            mem_usage += sizeof(*archives_);
        }

        auto it = archives_->begin();
        for (; it != archives_->end(); it++)
        {
            if (it->commit_ts_ <= commit_ts)
            {
                break;
            }
        }
        if (it == archives_->end() || it->commit_ts_ < commit_ts)
        {
            it = archives_->emplace(it);
            it->commit_ts_ = commit_ts;
            it->payload_status_ = payload_status;
            it->payload_ = std::move(payload_ptr);
            mem_usage += it->MemUsage();
        }

        return mem_usage;
    }

    /**
     *
     * @brief Kick out historical versions that won't be used according to
     * 'oldest_active_tx_ts'.
     * eg:  achives[10,8,4,3,1], oldest_active_tx_ts= 5;
     * after kicking out, archives will be [10,8,4].
     *
     * @return mem usage of archive records kicked out
     */
    size_t KickOutArchiveRecords(uint64_t oldest_active_tx_ts) override
    {
        if (archives_ == nullptr)
        {
            return 0;
        }

        if (commit_ts_ <= oldest_active_tx_ts)
        {
            size_t mem_usage = GetArchiveMemUsage();
            // archives_->clear();
            archives_.reset(nullptr);
            return mem_usage;
        }

        if (archives_->size() <= 1)
        {
            return 0;
        }

        auto it = archives_->begin();
        for (; it != archives_->end(); it++)
        {
            if (it->commit_ts_ <= oldest_active_tx_ts)
            {
                break;
            }
        }
        if (it == archives_->end())
        {
            return 0;
        }
        it++;
        size_t mem_usage = 0U;
        for (auto it1 = it; it1 != archives_->end(); it1++)
        {
            mem_usage += it1->MemUsage();
        }
        archives_->erase(it, archives_->end());

        return mem_usage;
    }

    size_t GetArchiveMemUsage() const
    {
        if (archives_ == nullptr)
        {
            return 0;
        }
        size_t mem_usage = sizeof(*archives_);
        for (auto it = archives_->begin(); it != archives_->end(); ++it)
        {
            mem_usage += it->MemUsage();
        }
        return mem_usage;
    }

    /**
     * @brief Gets the visible version according to read timestamp.
     *
     * @return true : if find the record; false: not found or has write_lock
     */
    bool MvccGet(uint64_t ts,
                 VersionResultRecord<ValueT> &rec,
                 TableType tbl_type)
    {
        if (payload_status_ == RecordStatus::Unknown)
        {
            rec.payload_status_ = RecordStatus::Unknown;
            rec.commit_ts_ = commit_ts_;
            return true;
        }
        if (commit_ts_ <= ts)
        {
            if (key_lock_ptr_ != nullptr && key_lock_ptr_->HasWriteLock() &&
                key_lock_ptr_->WLockTs() < ts)
            {
                // Having write lock means the ccentry will be updated soon.
                // If wlock_ts_ < ts, the future 'commit_ts' is may also less
                // than the 'read timestamp', then should return the future
                // version.
                // There are two choice: (1)wait until the future version is
                // committed; (2) abort read transcation.

                // TODO(lzx): return error code and use retry mechanism instead
                // of abort immediately.
                return false;
            }

            // MVCC update last_read_ts_ of lastest ccentry to tell later
            // writer's commit_ts must be higher than MVCC reader's ts. Or it
            // will break the REPEATABLE READ since the next MVCC read in the
            // same transaction will read the new updated ccentry.
            last_read_ts_ = std::max(ts, last_read_ts_);
            if (payload_status_ == RecordStatus::Normal)
            {
                rec.payload_ptr_ = payload_.get();
            }
            rec.commit_ts_ = commit_ts_;
            rec.payload_status_ = payload_status_;
            return true;
        }

        if (archives_ != nullptr)
        {
            // if commit_ts_ > ts, find from archives_
            for (auto it = archives_->cbegin(); it != archives_->cend(); it++)
            {
                if (it->commit_ts_ <= ts)
                {
                    if (it->payload_status_ == RecordStatus::Normal)
                    {
                        if (tbl_type == TableType::Secondary)
                        {
                            rec.payload_ptr_ = payload_.get();
                        }
                        else
                        {
                            rec.payload_ptr_ = it->payload_.get();
                        }
                    }
                    rec.commit_ts_ = it->commit_ts_;
                    rec.payload_status_ = it->payload_status_;
                    return true;
                }
            }
        }

        rec.commit_ts_ = 1U;
        if (ckpt_ts_ == 1U)
        {
            // need fetch base table
            rec.payload_status_ = RecordStatus::Unknown;
        }
        else
        {
            rec.payload_status_ = RecordStatus::VersionUnknown;
        }
        return true;
    }

    bool HasVisibleVersion(uint64_t ts)
    {
        if (commit_ts_ <= ts)
        {
            return true;
        }
        if (archives_ != nullptr && !archives_->empty() &&
            archives_->back().commit_ts_ <= ts)
        {
            return true;
        }
        return false;
    }

    /**
     * @brief Export the historical versions when flushing(checkpoint).
     * From "last_ckpt_ts" to latest version (include current version).
     * @param akvs - the continer of exported records.
     * @param to_ts - the upper bound ts.
     * @return count of records exported
     */
    size_t ExportArchives(std::vector<FlushRecord> &akvs,
                          uint64_t to_ts,
                          TableType tbl_type) const override
    {
        if (archives_ == nullptr)
        {
            return 0;
        }
        size_t count = 0;
        if (archives_->size() > 0)
        {
            for (const VersionRecord<ValueT> &rec : *archives_)
            {
                if (rec.commit_ts_ > ckpt_ts_ && rec.commit_ts_ <= to_ts)
                {
                    auto &ref = akvs.emplace_back();
                    ref.cce_ = const_cast<LruEntry *>(
                        static_cast<const LruEntry *>(this));
                    if (tbl_type != TableType::Secondary)
                    {
                        ref.SetPayload(rec.payload_.get());  // pk
                    }
                    else
                    {
                        ref.SetPayload(payload_.get());  // sk
                    }
                    ref.payload_status_ = rec.payload_status_;
                    ref.commit_ts_ = rec.commit_ts_;
                    count++;
                }
            }
        }
        return count;
    }

    size_t ArchiveRecordsCount() const override
    {
        if (archives_ == nullptr)
        {
            return 0;
        }
        return archives_->size();
    }

    void ClearArchives()
    {
        if (archives_ != nullptr)
        {
            archives_.reset(nullptr);
        }
    }

    const TxKey *Key() const override
    {
        return key_;
    }
};

struct CcEntryAddr
{
public:
    CcEntryAddr() : cce_ptr_(0), insert_ptr_(0), node_group_id_(0), term_(-1)
    {
    }

    CcEntryAddr(const CcEntryAddr &rhs)
        : cce_ptr_(rhs.cce_ptr_),
          insert_ptr_(rhs.insert_ptr_),
          node_group_id_(rhs.node_group_id_),
          term_(rhs.term_.load(std::memory_order_acquire))
    {
    }

    bool operator==(const CcEntryAddr &rhs) const
    {
        return node_group_id_ == rhs.node_group_id_ && term_ == rhs.term_ &&
               ((cce_ptr_ != 0 && rhs.cce_ptr_ != 0 &&
                 cce_ptr_ == rhs.cce_ptr_) ||
                (insert_ptr_ != 0 && rhs.insert_ptr_ != 0 &&
                 insert_ptr_ == rhs.insert_ptr_));
    }

    CcEntryAddr &operator=(const CcEntryAddr &rhs)
    {
        if (this == &rhs)
        {
            return *this;
        }

        cce_ptr_ = rhs.cce_ptr_;
        insert_ptr_ = rhs.insert_ptr_;
        node_group_id_ = rhs.node_group_id_;
        term_.store(rhs.term_.load(std::memory_order_acquire),
                    std::memory_order_release);

        return *this;
    }

    bool Empty() const
    {
        return cce_ptr_ == 0 && insert_ptr_ == 0;
    }

    uint64_t CcePtr() const
    {
        return cce_ptr_;
    }

    uint64_t InsertPtr() const
    {
        return insert_ptr_;
    }

    uint32_t NodeGroupId() const
    {
        return node_group_id_;
    }

    int64_t Term() const
    {
        return term_.load(std::memory_order_acquire);
    }

    void SetCce(uint64_t addr, int64_t term)
    {
        cce_ptr_ = addr;
        insert_ptr_ = 0;
        term_.store(term, std::memory_order_release);
    }

    void SetCce(uint64_t addr, int64_t term, uint32_t ng)
    {
        cce_ptr_ = addr;
        insert_ptr_ = 0;
        node_group_id_ = ng;
        term_.store(term, std::memory_order_release);
    }

    void SetInsert(uint64_t addr, int64_t term)
    {
        insert_ptr_ = addr;
        cce_ptr_ = 0;
        term_.store(term, std::memory_order_release);
    }

    void SetInsert(uint64_t addr, int64_t term, uint32_t ng)
    {
        insert_ptr_ = addr;
        cce_ptr_ = 0;
        node_group_id_ = ng;
        term_.store(term, std::memory_order_release);
    }

    void SetNodeGroupId(uint32_t ng_id)
    {
        node_group_id_ = ng_id;
    }

    void SetTerm(int64_t term)
    {
        term_.store(term, std::memory_order_release);
    }

private:
    uint64_t cce_ptr_;
    uint64_t insert_ptr_;
    uint32_t node_group_id_;
    // The term of the cc node group to which the cc entry belongs. The variable
    // needs to be std::atomic, because for locking-based protocols the remote
    // node will send an acknowledge message to notify the tx when the
    // read/write request is blocked. The acknowledgement message, when arrives,
    // will set the term and the cc entry's address. We use std::atomic to sync
    // between the remote cc handler thread and the tx thread, which
    // periodically checks the term to determine if there is a timeout. Note
    // that for non-blocking protocols, we rely on the cc handler result to sync
    // between the remote handler thread and the tx thread, and the term does
    // not need to be std::atomic.
    std::atomic<int64_t> term_;
};

}  // namespace txservice

namespace std
{
template <>
struct hash<txservice::CcEntryAddr>
{
    std::size_t operator()(const txservice::CcEntryAddr &key) const
    {
        uint64_t ptr_hash = key.CcePtr() != 0 ? key.CcePtr() : key.InsertPtr();
        return (size_t) key.NodeGroupId() * 23 + ptr_hash;
    }
};
}  // namespace std
