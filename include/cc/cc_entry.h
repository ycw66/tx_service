#pragma once

#include <algorithm>  // std::max
#include <atomic>
#include <cassert>
#include <deque>
#include <map>
#include <memory>  // std::make_shared
#include <unordered_set>
#include <utility>  // std::move
#include <vector>

#include "cc_req_base.h"
#include "circular_queue.h"
#include "non_blocking_lock.h"
#include "tx_id.h"
#include "tx_key.h"
#include "tx_record.h"

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
                const TxId &tx_id,
                CcEntry<KeyT, ValueT> *parent_entry)
        : key_(key), tx_id_(tx_id), parent_entry_(parent_entry)
    {
    }

    InsertEntry(const KeyT &key,
                TxNumber &txn,
                CcEntry<KeyT, ValueT> *parent_entry)
        : key_(key), txn_(txn), parent_entry_(parent_entry)
    {
    }

    const LruEntry &Parent() const override
    {
        return *parent_entry_;
    }

    const KeyT key_;  // owner of key_
    TxId tx_id_;
    TxNumber txn_;
    CcEntry<KeyT, ValueT> *parent_entry_;
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
    virtual size_t KickOutFlushedArchiveRecords(uint64_t upper_bound_ts) = 0;
    virtual void ExportArchives(std::vector<VersionedRecord> &akvs) const = 0;
    virtual TxKey::Uptr ExportKey() const = 0;

    /**
     * @brief check whether the entry can be kicked out from ccmap, iff no key
     * lock, no gap lock and not 'dirty' entry (entry which has been
     * checkpointed since the last change).
     *
     * @return true: entry can be kicked out.
     */
    bool IsFree() const;

    // Lru link which records the age of entries, when ccmap is full, kickout
    // the entries by the order of lru.
    LruEntry *lru_prev_{nullptr};
    LruEntry *lru_next_{nullptr};
    // Checkpoint link which is used by CkptScanCc to iterate and generate the
    // list of payload_ckpt_.
    LruEntry *ckpt_prev_{nullptr};
    LruEntry *ckpt_next_{nullptr};
    CcMap *const parent_map_;

    NonBlockingLock key_lock_;
    NonBlockingLock gap_lock_;

    uint64_t commit_ts_{1};
    // "last_read_ts_" is updated in tow cases:
    // (1) Read under MVCC+SnapshotIsolation: it will be updated to
    // max{read_ts, last_read_ts_} if latest version of ccentry less than
    // read timestamp, which pushes future transactions' commit
    // timestamps larger than the read timestamp of the current read
    // transaction;
    //(2) PostRead under OCC/LOCKING+RepeatableRead: it will be updated to
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
struct VersionRecord
{
public:
    const ValueT *payload_ptr_;
    RecordStatus payload_status_;
    uint64_t commit_ts_;

    VersionRecord()
        : payload_ptr_(nullptr),
          payload_status_(RecordStatus::Unknown),
          commit_ts_(1)
    {
    }
};

// for mvcc
template <typename ValueT>
struct ArchiveRecord
{
public:
    std::shared_ptr<ValueT> payload_;
    uint64_t commit_ts_;
    RecordStatus payload_status_;

    ArchiveRecord()
        : payload_(nullptr),
          commit_ts_(1),
          payload_status_(RecordStatus::Unknown)
    {
    }

    ArchiveRecord(std::shared_ptr<ValueT> payload,
                  uint64_t commit_ts,
                  RecordStatus status)
        : payload_(payload), commit_ts_(commit_ts), payload_status_(status)
    {
    }

    ArchiveRecord(const ArchiveRecord<ValueT> &rhs)
    {
        payload_ = rhs.payload_;
        commit_ts_ = rhs.commit_ts_;
        payload_status_ = rhs.payload_status_;
    }

    ArchiveRecord &operator=(const ArchiveRecord<ValueT> &rhs)
    {
        if (this == &rhs)
        {
            return *this;
        }

        payload_ = rhs.payload_;
        commit_ts_ = rhs.commit_ts_;
        payload_status_ = rhs.payload_status_;

        return *this;
    }

    size_t MemUsage() const
    {
        size_t mem_usage_ = 0;
        if (payload_ != nullptr)
        {
            mem_usage_ += payload_->MemUsage();
        }
        mem_usage_ += sizeof(uint64_t);
        mem_usage_ += sizeof(RecordStatus);
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
          payload_(nullptr),
          payload_status_(RecordStatus::Unknown),
          payload_ckpt_(),
          map_prev_(nullptr),
          map_next_(nullptr),
          archives_()
    {
    }

    ~CcEntry() = default;

    size_t GetCcEntryMemUsage() const override
    {
        size_t mem_usage_ = 0;
        size_t ptr_size = sizeof(nullptr);

        // LruEntry field members:
        // size of lru_prev_, lru_next_, ckpt_prev_, ckpt_next_, parent_map_
        mem_usage_ += 5 * ptr_size;
        // two NonBlockingLocks
        mem_usage_ += key_lock_.MemUsage() + gap_lock_.MemUsage();
        // size of commit_ts_, last_read_ts_, gap_commit_ts_, gap_last_read_ts_
        // and ckpt_ts_
        mem_usage_ += 5 * sizeof(uint64_t);

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

        // size of payload_ckpt_
        mem_usage_ += payload_ckpt_.first.MemUsage() + sizeof(bool);
        // size of map_prev_, map_next_
        mem_usage_ += 2 * ptr_size;

        // for mvcc
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

    const KeyT *key_;
    std::shared_ptr<ValueT> payload_;
    RecordStatus payload_status_;

    std::map<const KeyT *,
             std::unique_ptr<InsertEntry<KeyT, ValueT>>,
             PtrLessThan<KeyT>>
        insert_intention_set_;

    // When checkpointing decides to flush this record, the payload is copied to
    // a standby object. This allows checkpointing to keep a raw pointer to the
    // standby payload, rather than making a copy of it.
    std::pair<ValueT, bool> payload_ckpt_;

    CcEntry<KeyT, ValueT> *map_prev_;
    CcEntry<KeyT, ValueT> *map_next_;

    // save versions exclude the current version.(descending order)
    std::deque<ArchiveRecord<ValueT>> archives_;
    // The time when a write tx acquires the write lock/intent on this cc entry.
    uint64_t wlock_ts_;

    /**
     * @brief Move(not copy) the current version (payload, payload_status,
     * commit_ts) to the archives_.
     *
     * @return New memory usage caused by archive
     */
    size_t ArchiveBeforeUpdate()
    {
        if (commit_ts_ <= 1)  // no history record
        {
            return 0;
        }

        if (archives_.size() > 0)
        {
            assert(commit_ts_ > archives_[0].commit_ts_);
        }

        // std::shared_ptr<ValueT> payload_ptr = payload_;
        if (parent_map_->Type() != TableType::Secondary)
        {
            archives_.emplace_front(payload_, commit_ts_, payload_status_);
        }
        else
        {
            archives_.emplace_front(nullptr, commit_ts_, payload_status_);
        }

        return sizeof(commit_ts_) + sizeof(payload_status_);
    }

    /**
     * @brief Add a batch of historical versions.
     *@param v_recs versions descending ordered by commit_ts
     * @return New memory usage caused by archive
     */
    size_t AddArchiveRecords(const std::vector<VersionedRecord> &v_recs)
    {
        if (v_recs.size() == 0)
        {
            return 0;
        }

        auto it = archives_.begin();
        for (; it != archives_.end(); it++)
        {
            if (it->commit_ts_ <= v_recs[0].commit_ts_)
            {
                break;
            }
        }
        size_t mem_usage = 0U;
        for (auto &vrec : v_recs)
        {
            if (it == archives_.end() || it->commit_ts_ != vrec.commit_ts_)
            {
                it = archives_.emplace(it);
                it->commit_ts_ = vrec.commit_ts_;
                it->payload_status_ = vrec.record_status_;
                if (vrec.record_status_ == RecordStatus::Normal)
                {
                    if (vrec.record_ != nullptr)
                    {
                        it->payload_ =
                            std::static_pointer_cast<ValueT>(vrec.record_);
                    }
                    else
                    {
                        size_t offset = 0;
                        ValueT *typed_rec = new ValueT();
                        typed_rec->Deserialize(vrec.record_blob_->data(),
                                               offset);
                    }
                }
                mem_usage += it->MemUsage();
            }
            it++;
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
        if (commit_ts_ <= oldest_active_tx_ts)
        {
            size_t mem_usage = GetArchiveMemUsage();
            archives_.clear();
            return mem_usage;
        }

        if (archives_.size() <= 1)
        {
            return 0;
        }

        auto it = archives_.begin();
        for (; it != archives_.end(); it++)
        {
            if (it->commit_ts_ <= oldest_active_tx_ts)
            {
                break;
            }
        }
        if (it == archives_.end())
        {
            return 0;
        }
        it++;
        size_t mem_usage = 0U;
        for (auto it1 = it; it1 != archives_.end(); it1++)
        {
            mem_usage += it1->MemUsage();
        }
        archives_.erase(it, archives_.end());

        return mem_usage;
    }

    /**
     *
     * @brief Kick out historical versions after being flushed to kvstore.
     * eg:  achives[10,8,4,3,1], upper_bound_ts= 4;
     * after kicking out, archives will be [10,8].
     *
     * @param upper_bound_ts the max verion has been flushed
     * @return mem usage of archive records kicked out
     */
    size_t KickOutFlushedArchiveRecords(uint64_t upper_bound_ts) override
    {
        auto it = archives_.begin();
        for (; it != archives_.end(); it++)
        {
            if (it->commit_ts_ <= upper_bound_ts)
            {
                break;
            }
        }

        size_t mem_usage = 0U;
        for (auto it1 = it; it1 != archives_.end(); it1++)
        {
            mem_usage += it1->MemUsage();
        }
        archives_.erase(it, archives_.end());

        return mem_usage;
    }

    size_t GetArchiveMemUsage() const
    {
        size_t mem_usage = 0;
        for (auto it = archives_.begin(); it != archives_.end(); ++it)
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
    bool MvccGet(uint64_t ts, VersionRecord<ValueT> &rec)
    {
        if (payload_status_ == RecordStatus::Unknown)
        {
            rec.payload_status_ = RecordStatus::Unknown;
            rec.commit_ts_ = commit_ts_;
            return true;
        }
        if (commit_ts_ <= ts)
        {
            if (key_lock_.HasWriteLock() && wlock_ts_ < ts)
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

            // MVCC update last_validation_ts_ of lastest ccentry to tell later
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
        for (auto it = archives_.cbegin(); it != archives_.cend(); it++)
        {
            if (it->commit_ts_ <= ts)
            {
                if (it->payload_status_ == RecordStatus::Normal)
                {
                    rec.payload_ptr_ = it->payload_.get();
                }
                rec.commit_ts_ = it->commit_ts_;
                rec.payload_status_ = it->payload_status_;
                return true;
            }
        }
        rec.commit_ts_ = 1;
        rec.payload_status_ = RecordStatus::VersionUnknown;
        return true;
    }

    void ExportArchives(std::vector<VersionedRecord> &akvs) const override
    {
        if (archives_.size() > 0)
        {
            akvs.reserve(archives_.size());
            for (auto &rec : archives_)
            {
                auto &ref = akvs.emplace_back();
                ref.record_ = rec.payload_;
                ref.record_status_ = rec.payload_status_;
                ref.commit_ts_ = rec.commit_ts_;
            }
        }
    }

    TxKey::Uptr ExportKey() const override
    {
        return key_->Clone();
    }

    size_t ArchiveRecordsCount() const override
    {
        return archives_.size();
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
