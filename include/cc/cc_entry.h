#pragma once

#include <butil/logging.h>

#include <algorithm>  // std::max
#include <atomic>
#include <cassert>
#include <list>
#include <map>
#include <memory>  // std::make_unique, make_shared, shared_ptr
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

#ifdef ON_KEY_OBJECT
#include "tx_command.h"
#endif

namespace txservice
{
class CcMap;

template <typename KeyT, typename ValueT>
class TemplateCcMap;

struct LruEntry;

template <typename KeyT, typename ValueT>
struct CcEntry;

template <typename KeyT, typename ValueT>
struct CcPage;

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
    union KeyPtr
    {
        const TxKey *ptr_;
        std::unique_ptr<TxKey> uptr_;
        // key_idx records where the TxKey raw ptr should be obtained.
        size_t key_idx_;
        ~KeyPtr()
        {
        }
    };

private:
    KeyPtr key_{nullptr};
    bool is_key_owner_{false};

    std::shared_ptr<TxRecord> payload_{nullptr};

public:
    RecordStatus payload_status_{RecordStatus::Unknown};
    uint64_t commit_ts_{1U};
    // todo: remove cce_
    LruEntry *cce_;
    int32_t delta_size_{INT32_MAX};

    FlushRecord()
    {
    }

    FlushRecord(std::unique_ptr<TxKey> uptr,
                std::shared_ptr<TxRecord> payload,
                RecordStatus payload_status,
                uint64_t commit_ts,
                LruEntry *cce,
                int32_t delta_size)
    {
        SetKey(std::move(uptr));
        payload_ = payload;
        payload_status_ = payload_status;
        commit_ts_ = commit_ts;
        cce_ = cce;
        delta_size_ = delta_size;
    }

    ~FlushRecord()
    {
        if (is_key_owner_)
        {
            key_.uptr_.reset();
        }
    }

    FlushRecord &operator=(FlushRecord &&rhs) noexcept
    {
        if (this == &rhs)
        {
            return *this;
        }

        if (rhs.is_key_owner_)
        {
            SetKey(std::move(rhs.key_.uptr_));
            rhs.is_key_owner_ = false;
        }
        else
        {
            SetKey(rhs.key_.ptr_);
        }

        payload_ = std::move(rhs.payload_);
        payload_status_ = rhs.payload_status_;
        commit_ts_ = rhs.commit_ts_;
        cce_ = rhs.cce_;
        delta_size_ = rhs.delta_size_;
        return *this;
    }

    FlushRecord(FlushRecord &&rhs) noexcept
    {
        if (rhs.is_key_owner_)
        {
            SetKey(std::move(rhs.key_.uptr_));
            rhs.is_key_owner_ = false;
        }
        else
        {
            SetKey(rhs.key_.ptr_);
        }

        payload_ = std::move(rhs.payload_);
        payload_status_ = rhs.payload_status_;
        commit_ts_ = rhs.commit_ts_;
        delta_size_ = rhs.delta_size_;
        cce_ = rhs.cce_;
    }

    void CloneOrCopyKey(const TxKey &key)
    {
        if (is_key_owner_)
        {
            assert(key_.uptr_ != nullptr);
            key_.uptr_->Copy(key);
        }
        else
        {
            (void) key_.uptr_.release();
            key_.uptr_ = key.Clone();
            is_key_owner_ = true;
        }
    }

    void SetKeyIndex(size_t offset)
    {
        if (is_key_owner_)
        {
            key_.uptr_.reset();
        }
        key_.key_idx_ = offset;
        is_key_owner_ = false;
    }

    size_t GetKeyIndex() const
    {
        assert(!is_key_owner_);
        return key_.key_idx_;
    }

    void SetKey(const TxKey *ptr)
    {
        if (is_key_owner_)
        {
            key_.uptr_.reset();
        }
        key_.ptr_ = ptr;
        is_key_owner_ = false;
    }

    void SetKey(std::unique_ptr<TxKey> uptr)
    {
        if (is_key_owner_)
        {
            // The move op will de-allocate the old record and obtain the
            // ownership of the input record.
            key_.uptr_ = std::move(uptr);
        }
        else
        {
            // key_ is treated as a unique_ptr, first release ownership,
            // otherwise ptr_ will be deleted
            (void) key_.uptr_.release();
            key_.uptr_ = std::move(uptr);
        }
        is_key_owner_ = true;
    }

#ifndef ON_KEY_OBJECT
    void SetPayload(std::shared_ptr<TxRecord> sptr)
    {
        payload_ = sptr;
    }
#else
    void SetPayload(const TxRecord *ptr)
    {
        auto rec = std::make_shared<BlobTxRecord>();
        ptr->Serialize(rec->value_);
        payload_ = rec;
    }
#endif

    const TxRecord *Payload() const
    {
        if (payload_ == nullptr)
        {
            return nullptr;
        }
        return payload_.get();
    }

    std::shared_ptr<TxRecord> GetPayload() const
    {
        return payload_;
    }

    size_t PayloadSize() const
    {
        return Payload() == nullptr ? 0 : Payload()->Size();
    }

    const TxKey *Key() const;

    /**
     * @brief Size of the FlushRecord. 0 if the record is in Deleted status.
     *
     * @return size_t
     */
    size_t Size() const
    {
        if (payload_status_ == RecordStatus::Deleted)
        {
            return 0;
        }
        else
        {
            return Key()->Size() + PayloadSize();
        }
    }
};

struct LruEntry
{
public:
    LruEntry() = delete;
    virtual ~LruEntry();

    LruEntry(CcMap *parent);

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

    // todo: remove parent_map_ of LruEntry
    CcMap *const parent_map_;

    NonBlockingLock *key_lock_ptr_{nullptr};
    NonBlockingLock *gap_lock_ptr_{nullptr};

    uint64_t commit_ts_{1};
    // "last_read_ts_" is updated in two cases:
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

    // The commit timestamp of the latest checkpoint version record. Unlike
    // other fields that are read/modified via a single thread, this field is
    // updated by a separate checkpointing thread, after it flushes changes to
    // the data store.
    std::atomic<uint64_t> ckpt_ts_{0};

    /**
     * @brief Size of this record in data store.
     * INT32_MAX is a special value that means unknown size.
     * Unkown size is used during log replay where the latest version
     * is directly written into ccmap and we don't know the record size
     * in KV storage.
     */
    std::atomic<int32_t> data_store_size_{INT32_MAX};
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
#ifdef ON_KEY_OBJECT
    const ValueT *payload_ptr_;
#else
    std::shared_ptr<ValueT> payload_ptr_;
#endif
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
#ifdef ON_KEY_OBJECT
    std::unique_ptr<ValueT> payload_;
#else
    std::shared_ptr<ValueT> payload_;
#endif
    uint64_t commit_ts_;
    RecordStatus payload_status_;

    VersionRecord()
        : payload_(nullptr),
          commit_ts_(1),
          payload_status_(RecordStatus::Unknown)
    {
    }

    VersionRecord(

#ifdef ON_KEY_OBJECT
        std::unique_ptr<ValueT> payload,
#else
        std::shared_ptr<ValueT> payload,
#endif
        uint64_t commit_ts,
        RecordStatus status)
        : payload_(std::move(payload)),
          commit_ts_(commit_ts),
          payload_status_(status)
    {
    }

    VersionRecord(const VersionRecord<ValueT> &rhs)
    {
        payload_ = std::move(rhs.payload_);
        payload_status_ = rhs.payload_status_;
        commit_ts_ = rhs.commit_ts_;
    }
    VersionRecord &operator=(const VersionRecord<ValueT> &rhs)
    {
        payload_ = std::move(rhs.payload_);
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
          payload_status_(RecordStatus::Unknown),
          payload_(nullptr),
          archives_()
    {
    }

    CcEntry(CcMap *parent_map, CcPage<KeyT, ValueT> *parent_page)
        : LruEntry(parent_map),
          payload_status_(RecordStatus::Unknown),
          payload_(nullptr),
          archives_(),
          parent_page_(parent_page)
    {
    }

    ~CcEntry() = default;

    size_t GetCcEntryMemUsage() const
    {
        size_t mem_usage = basic_mem_overhead_;
        mem_usage += PayloadMemUsage();
        mem_usage += GetArchiveMemUsage();
        // TODO size of insert_intention_set_, not used yet

        return mem_usage;
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

    size_t PayloadSerializedLength() const
    {
        return payload_ == nullptr ? 0 : payload_->SerializedLength();
    }

    RecordStatus payload_status_;
#ifndef ON_KEY_OBJECT
    std::shared_ptr<ValueT> payload_;
#else
    std::unique_ptr<ValueT> payload_;
    std::unique_ptr<TxCommand> pending_cmd_;
    std::unique_ptr<ReplayTxnCmdList> replay_cmd_list_;
    // temporary object to process subsequent commands in the same txn
    std::unique_ptr<ValueT> dirty_payload_;
    // status of temporary object
    RecordStatus dirty_payload_status_{RecordStatus::NonExistent};
#endif

    std::map<const KeyT *,
             std::unique_ptr<InsertEntry<KeyT, ValueT>>,
             PtrLessThan<KeyT>>
        insert_intention_set_;

    // save versions exclude the current version.(descending order,eg.[4,3,2,1])
    std::unique_ptr<std::list<VersionRecord<ValueT>>> archives_;

    // parent CcPage
    CcPage<KeyT, ValueT> *parent_page_{nullptr};

    inline static size_t basic_mem_overhead_ = sizeof(CcEntry<KeyT, ValueT>);

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
            archives_ = std::make_unique<std::list<VersionRecord<ValueT>>>();
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
            archives_->emplace_front(nullptr, commit_ts_, payload_status_);
        }
        mem_usage += sizeof(VersionResultRecord<ValueT>);
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
            archives_ = std::make_unique<std::list<VersionRecord<ValueT>>>();
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

    size_t AddArchiveRecord(
#ifdef ON_KEY_OBJECT
        std::unique_ptr<TxRecord> payload_uptr,
#else
        std::shared_ptr<ValueT> payload_ptr,
#endif
        RecordStatus payload_status,
        uint64_t commit_ts)
    {
        if (commit_ts == 1U && payload_status == RecordStatus::Deleted)
        {
            return 0;
        }
        size_t mem_usage = 0U;
        if (archives_ == nullptr)
        {
            archives_ = std::make_unique<std::list<VersionRecord<ValueT>>>();
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
#ifdef ON_KEY_OBJECT
            it->payload_.reset(static_cast<ValueT *>(payload_uptr.release()));
#else
            it->payload_ = payload_ptr;
#endif
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
    size_t KickOutArchiveRecords(uint64_t oldest_active_tx_ts)
    {
        if (archives_ == nullptr)
        {
            return 0;
        }

        if (commit_ts_ <= oldest_active_tx_ts)
        {
            size_t mem_usage = GetArchiveMemUsage();
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
     * @param ts - snapshot read timestamp
     * @param tbl_type - type of table
     * @param rec - variable to store result
     */
    void MvccGet(uint64_t ts,
                 TableType tbl_type,
                 VersionResultRecord<ValueT> &rec)
    {
        if (payload_status_ == RecordStatus::Unknown)
        {
            rec.payload_status_ = RecordStatus::Unknown;
            rec.commit_ts_ = commit_ts_;
            return;
        }
        if (commit_ts_ <= ts)
        {
            // MVCC update last_read_ts_ of lastest ccentry to tell later
            // writer's commit_ts must be higher than MVCC reader's ts. Or it
            // will break the REPEATABLE READ since the next MVCC read in the
            // same transaction will read the new updated ccentry.
            last_read_ts_ = std::max(ts, last_read_ts_);
            if (payload_status_ == RecordStatus::Normal)
            {
#ifdef ON_KEY_OBJECT
                rec.payload_ptr_ = payload_.get();
#else
                rec.payload_ptr_ = payload_;
#endif
            }
            rec.commit_ts_ = commit_ts_;
            rec.payload_status_ = payload_status_;
            return;
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
#ifdef ON_KEY_OBJECT
                            rec.payload_ptr_ = payload_.get();
#else
                            rec.payload_ptr_ = payload_;
#endif
                        }
                        else
                        {
#ifdef ON_KEY_OBJECT
                            rec.payload_ptr_ = it->payload_.get();
#else
                            rec.payload_ptr_ = it->payload_;
#endif
                        }
                    }
                    rec.commit_ts_ = it->commit_ts_;
                    rec.payload_status_ = it->payload_status_;
                    return;
                }
            }
        }

        rec.commit_ts_ = 1U;
        if (ckpt_ts_ == 0U)
        {
            // need fetch base table and archive table.
            rec.payload_status_ = RecordStatus::VersionUnknown;
        }
        else if (ckpt_ts_ <= ts)
        {
            // only need fetch base table.
            rec.payload_status_ = RecordStatus::BaseVersionMiss;
        }
        else
        {
            // only need fetch archive table.
            rec.payload_status_ = RecordStatus::ArchiveVersionMiss;
        }
    }

    /**
     * @brief For a read timestamp, is there a visible version in memory?
     */
    bool HasVisibleVersion(uint64_t read_ts) const
    {
        if (commit_ts_ <= read_ts)
        {
            return true;
        }
        if (archives_ != nullptr && !archives_->empty() &&
            archives_->back().commit_ts_ <= read_ts)
        {
            return true;
        }
        return false;
    }

    /**
     * @brief Export version records to flush into KvStore when mvcc is enabled.
     * eg. CcEntry's versions is [10,8,7,5,4], param ckpt_ts is "9",
     * then the version "8" is exported to ckpt_vec, versions [7,5,4] are
     * exported to akv_vec.
     * @param key - the key of this entry
     * @param ckpt_vec - store the version records to flush into "base table".
     * @param akv_vec - store the version records to flush into "archives
     * table".
     * @param from_ts - Previous round scan timestamp. We scan the data between
     * (from_ts, to_ts].
     * @param to_ts - Current round checkpoint timestamp.
     * @param include_flushed_rec - True means we also need to scan data which
     * has been flushed to storage. Note: This flag only used for
     * RangePartition.
     * @return the number of exported version records.
     */
    size_t ExportForCkpt(const KeyT &key,
                         std::vector<FlushRecord> &ckpt_vec,
                         std::vector<FlushRecord> &akv_vec,
                         std::vector<size_t> &mv_base_vec,
                         uint64_t from_ts,
                         uint64_t to_ts,
                         uint64_t oldest_active_tx_ts,
                         TableType tbl_type,
                         bool mvcc_enabled,
                         size_t &ckpt_vec_size,
                         bool include_flushed_rec) const
    {
        size_t exported_count = 0;
        if (commit_ts_ <= ckpt_ts_)
        {
            return exported_count;
        }

        size_t ckpt_idx = ckpt_vec_size;
        if (from_ts < commit_ts_ && commit_ts_ <= to_ts)
        {
            FlushRecord &ref = ckpt_vec[ckpt_vec_size++];
            ref.CloneOrCopyKey(key);
            ref.cce_ =
                const_cast<LruEntry *>(static_cast<const LruEntry *>(this));

            if (payload_status_ == RecordStatus::Normal)
            {
#ifndef ON_KEY_OBJECT
                ref.SetPayload(payload_);
#else
                ref.SetPayload(payload_.get());
#endif
            }

            ref.payload_status_ = payload_status_;
            ref.commit_ts_ = commit_ts_;

            int32_t data_store_size =
                data_store_size_.load(std::memory_order_acquire);
            if (data_store_size != INT32_MAX)
            {
                if (ref.payload_status_ != RecordStatus::Deleted)
                {
                    ref.delta_size_ =
                        key.Size() + ref.PayloadSize() - data_store_size;
                }
                else
                {
                    ref.delta_size_ = -data_store_size;
                }
            }
            else
            {
                // Mark the delta as unknwon
                ref.delta_size_ = INT32_MAX;
            }
            exported_count++;
        }

        if (!mvcc_enabled)
        {
            return exported_count;
        }

        if (archives_ != nullptr && archives_->size() > 0)
        {
            for (auto it = archives_->begin(); it != archives_->end(); it++)
            {
                if (from_ts < it->commit_ts_ && it->commit_ts_ <= to_ts)
                {
                    if (it->commit_ts_ < ckpt_ts_ || it->commit_ts_ == 1U)
                    {
                        break;
                    }
                    else
                    {
                        if (exported_count == 0 && it->commit_ts_ == ckpt_ts_)
                        {
                            if (include_flushed_rec)
                            {
                                FlushRecord &ref = ckpt_vec[ckpt_vec_size++];
                                ref.CloneOrCopyKey(key);

                                // This record was load from storage. We
                                // can't safely point to CcEntry of CcMap.
                                // Because the entry will be kickout after
                                // UnpinSlice. the pointer will become
                                // invalidation.
                                ref.cce_ = nullptr;

                                if (it->payload_status_ == RecordStatus::Normal)
                                {
                                    if (tbl_type != TableType::Secondary)
                                    {
#ifndef ON_KEY_OBJECT
                                        ref.SetPayload(
                                            it->payload_);  // pk, unique_sk
#else
                                        ref.SetPayload(it->payload_.get());
#endif
                                    }
                                    else
                                    {
#ifndef ON_KEY_OBJECT
                                        ref.SetPayload(payload_);  // sk
#else
                                        ref.SetPayload(payload_.get());
#endif
                                    }
                                }
                                ref.payload_status_ = it->payload_status_;
                                ref.commit_ts_ = it->commit_ts_;

                                // the size of record is not change.
                                ref.delta_size_ = 0;

                                exported_count++;
                            }

                            break;
                        }

                        if (exported_count == 0)
                        {
                            FlushRecord &ref = ckpt_vec[ckpt_vec_size++];
                            ref.CloneOrCopyKey(key);
                            ref.cce_ = const_cast<LruEntry *>(
                                static_cast<const LruEntry *>(this));
                            if (it->payload_status_ == RecordStatus::Normal)
                            {
                                if (tbl_type != TableType::Secondary)
                                {
#ifndef ON_KEY_OBJECT
                                    ref.SetPayload(
                                        it->payload_);  // pk, unique_sk
#else
                                    ref.SetPayload(it->payload_.get());
#endif
                                }
                                else
                                {
#ifndef ON_KEY_OBJECT
                                    ref.SetPayload(payload_);  // sk
#else
                                    ref.SetPayload(payload_.get());
#endif
                                }
                            }
                            ref.payload_status_ = it->payload_status_;
                            ref.commit_ts_ = it->commit_ts_;
                            int32_t data_store_size = data_store_size_.load(
                                std::memory_order_acquire);
                            if (data_store_size == INT32_MAX)
                            {
                                // Mark the delta as unknwon
                                ref.delta_size_ = INT32_MAX;
                            }
                            else
                            {
                                if (ref.payload_status_ ==
                                    RecordStatus::Deleted)
                                {
                                    ref.delta_size_ = -data_store_size;
                                }
                                else
                                {
                                    ref.delta_size_ = key.Size() +
                                                      ref.PayloadSize() -
                                                      data_store_size;
                                }
                            }
                        }
                        else
                        {
                            auto &ref = akv_vec.emplace_back();
                            ref.SetKeyIndex(ckpt_idx);
                            ref.cce_ = const_cast<LruEntry *>(
                                static_cast<const LruEntry *>(this));
                            if (it->payload_status_ == RecordStatus::Normal)
                            {
                                if (tbl_type != TableType::Secondary)
                                {
#ifndef ON_KEY_OBJECT
                                    ref.SetPayload(
                                        it->payload_);  // pk, unique_sk
#else
                                    ref.SetPayload(it->payload_.get());
#endif
                                }
                                else
                                {
#ifndef ON_KEY_OBJECT
                                    ref.SetPayload(payload_);  // sk
#else
                                    ref.SetPayload(payload_.get());
#endif
                                }
                            }
                            ref.payload_status_ = it->payload_status_;
                            ref.commit_ts_ = it->commit_ts_;
                        }

                        exported_count++;
                    }
                }
                else if (from_ts >= it->commit_ts_)
                {
                    if (include_flushed_rec && exported_count == 0)
                    {
                        if (it->commit_ts_ > ckpt_ts_)
                        {
                            continue;
                        }

                        if (it->commit_ts_ == ckpt_ts_ && it->commit_ts_ != 1)
                        {
                            FlushRecord &ref = ckpt_vec[ckpt_vec_size++];
                            ref.CloneOrCopyKey(key);

                            // This record was load from storage. We
                            // can't safely point to CcEntry of CcMap.
                            // Because the entry will be kickout after
                            // UnpinSlice. the pointer will become
                            // invalidation.
                            ref.cce_ = nullptr;

                            if (it->payload_status_ == RecordStatus::Normal)
                            {
                                if (tbl_type != TableType::Secondary)
                                {
#ifndef ON_KEY_OBJECT
                                    ref.SetPayload(
                                        it->payload_);  // pk, unique_sk
#else
                                    ref.SetPayload(it->payload_.get());
#endif
                                }
                                else
                                {
#ifndef ON_KEY_OBJECT
                                    ref.SetPayload(payload_);  // sk
#else
                                    ref.SetPayload(payload_.get());
#endif
                                }
                            }
                            ref.payload_status_ = it->payload_status_;
                            ref.commit_ts_ = it->commit_ts_;

                            // the size of record is not change.
                            ref.delta_size_ = 0;

                            exported_count++;
                        }
                    }

                    assert(!include_flushed_rec || exported_count != 0 ||
                           it->commit_ts_ < ckpt_ts_ || it->commit_ts_ == 1);
                    break;
                }
                // else: it->commit_ts_ > to_ts
            }
        }

        if (exported_count > 0 &&
            ckpt_ts_.load(std::memory_order_relaxed) == 0 &&
            !HasVisibleVersion(oldest_active_tx_ts))
        {
            // last ckpt version is needed but not in memory, and we're not sure
            // if an older version exists, need to copy record from "base table"
            // into "mvcc_archives table".
            mv_base_vec.push_back(ckpt_idx);
        }
        return exported_count;
    }

    size_t ArchiveRecordsCount() const
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

    bool NeedCkpt()
    {
        return commit_ts_ > ckpt_ts_.load(std::memory_order_acquire) &&
               (payload_status_ == RecordStatus::Normal ||
                payload_status_ == RecordStatus::Deleted);
    }

    const TxKey *Key() const override;
};

struct LruPage
{
    explicit LruPage(CcMap *parent) : parent_map_(parent)
    {
    }

    // Lru link which records the age of page, when ccshard is full, kickout
    // the entries by the order of lru.
    LruPage *lru_prev_{nullptr};
    LruPage *lru_next_{nullptr};

    CcMap *parent_map_{nullptr};
};

template <typename KeyT, typename ValueT>
struct CcPage : public LruPage
{
    /**
     * Construct page negative infinity and positive infinity.
     * @param parent
     */
    CcPage<KeyT, ValueT>(CcMap *parent) : LruPage(parent)
    {
    }

    /**
     * Construct a normal page.
     * @param parent
     * @param prev_page
     * @param next_page
     */
    CcPage<KeyT, ValueT>(CcMap *parent,
                         CcPage<KeyT, ValueT> *prev_page,
                         CcPage<KeyT, ValueT> *next_page)
        : LruPage(parent), prev_page_(prev_page), next_page_(next_page)
    {
        keys_.reserve(split_threshold_);
        entries_.reserve(split_threshold_);
        if (prev_page_ != nullptr)
        {
            prev_page_->next_page_ = this;
        }
        if (next_page_ != nullptr)
        {
            next_page_->prev_page_ = this;
        }
    }

    /**
     * Construct a page when splitting an existing page.
     * @param parent
     * @param keys
     * @param entries
     * @param prev_page
     * @param next_page
     */
    CcPage<KeyT, ValueT>(
        CcMap *parent,
        std::vector<KeyT> &&keys,
        std::vector<std::unique_ptr<CcEntry<KeyT, ValueT>>> &&entries,
        CcPage<KeyT, ValueT> *prev_page,
        CcPage<KeyT, ValueT> *next_page)
        : LruPage(parent),
          keys_(std::move(keys)),
          entries_(std::move(entries)),
          prev_page_(prev_page),
          next_page_(next_page)
    {
        for (auto &entry_ptr : entries_)
        {
            entry_ptr->parent_page_ = this;
        }
        if (prev_page_ != nullptr)
        {
            prev_page_->next_page_ = this;
        }
        if (next_page_ != nullptr)
        {
            next_page_->prev_page_ = this;
        }
    }

    ~CcPage<KeyT, ValueT>()
    {
        if (prev_page_ != nullptr)
        {
            prev_page_->next_page_ = next_page_;
        }
        if (next_page_ != nullptr)
        {
            next_page_->prev_page_ = prev_page_;
        }
    }

    CcPage<KeyT, ValueT>(const CcPage<KeyT, ValueT> &page) = delete;
    CcPage<KeyT, ValueT> operator=(const CcPage<KeyT, ValueT> &page) = delete;
    CcPage<KeyT, ValueT>(CcPage<KeyT, ValueT> &&page) = delete;
    CcPage<KeyT, ValueT> operator=(CcPage<KeyT, ValueT> &&page) = delete;

    /**
     * Memory usage of this CcPage, not including the keys and entries
     * @return
     */
    size_t MemUsage() const
    {
        return basic_mem_overhead_;
    }

    /**
     * Memory usage of this CcPage, including the keys and entries
     * @return
     */
    size_t TotalMemUsage() const
    {
        size_t mem_usage = MemUsage();
        auto key_it = keys_.begin();
        auto entry_ptr_it = entries_.begin();
        for (; key_it != keys_.end(); key_it++, entry_ptr_it++)
        {
            mem_usage += key_it->MemUsage() - sizeof(KeyT) +
                         (*entry_ptr_it)->GetCcEntryMemUsage();
        }
        return mem_usage;
    }

    /**
     *
     * @param key
     * @return
     */
    size_t Find(const KeyT &key) const
    {
        if (keys_.size() > 0 && keys_.back() < key)
        {
            // not found
            return keys_.size();
        }

        auto lb_it = std::lower_bound(keys_.begin(), keys_.end(), key);
        size_t idx_in_page = lb_it - keys_.begin();

        // check it equals key
        if (lb_it != keys_.end() && *lb_it == key)
        {
            return idx_in_page;
        }
        else
        {
            // not found
            return keys_.size();
        }
    }

    // Use two-way merge algrothim to bulk emplace keys to reduce data moving
    // overhead. Note: new_keys are not exist in keys_
    void EmplaceKeys(std::vector<KeyT> &new_keys,
                     size_t &mem_increased,
                     std::vector<size_t> &idxs_in_page)
    {
        if (new_keys.empty())
        {
            idxs_in_page.clear();
            return;
        }

        idxs_in_page.clear();
        idxs_in_page.resize(new_keys.size(), 0);
        assert(!idxs_in_page.empty());

        if (new_keys.size() == 1)
        {
            size_t idx_in_page = Emplace(new_keys.front(), mem_increased);
            idxs_in_page[0] = idx_in_page;
            return;
        }

        if (keys_.empty() || keys_.back() < new_keys.front())
        {
            for (size_t i = 0; i < new_keys.size(); ++i)
            {
                idxs_in_page[i] = keys_.size();

                keys_.push_back(std::move(new_keys[i]));
                entries_.push_back(
                    std::make_unique<CcEntry<KeyT, ValueT>>(parent_map_, this));

                size_t key_mem_increased =
                    keys_.back().MemUsage() - sizeof(KeyT);
                size_t entry_mem_increased =
                    entries_.back()->GetCcEntryMemUsage();
                mem_increased += key_mem_increased + entry_mem_increased;
            }

            return;
        }

        size_t total_size = new_keys.size() + keys_.size();
        size_t res_index = total_size;
        size_t old_index = keys_.size();
        size_t new_index = new_keys.size();

        keys_.resize(total_size);
        entries_.resize(total_size);

        while (old_index > 0 && new_index > 0)
        {
            if (keys_[old_index - 1] < new_keys[new_index - 1])
            {
                keys_[res_index - 1] = std::move(new_keys[new_index - 1]);
                entries_[res_index - 1] =
                    std::make_unique<CcEntry<KeyT, ValueT>>(parent_map_, this);
                idxs_in_page[new_index - 1] = res_index - 1;

                size_t key_mem_increased =
                    keys_[res_index - 1].MemUsage() - sizeof(KeyT);
                size_t entry_mem_increased =
                    entries_[res_index - 1]->GetCcEntryMemUsage();
                mem_increased += key_mem_increased + entry_mem_increased;

                new_index--;
            }
            else
            {
                assert(new_keys[new_index - 1] < keys_[old_index - 1]);
                keys_[res_index - 1] = std::move(keys_[old_index - 1]);
                entries_[res_index - 1] = std::move(entries_[old_index - 1]);
                old_index--;
            }

            res_index--;
        }

        for (; new_index > 0; --new_index, --res_index)
        {
            keys_[res_index - 1] = std::move(new_keys[new_index - 1]);
            entries_[res_index - 1] =
                std::make_unique<CcEntry<KeyT, ValueT>>(parent_map_, this);

            idxs_in_page[new_index - 1] = res_index - 1;

            size_t key_mem_increased =
                keys_[res_index - 1].MemUsage() - sizeof(KeyT);
            size_t entry_mem_increased =
                entries_[res_index - 1]->GetCcEntryMemUsage();
            mem_increased += key_mem_increased + entry_mem_increased;
        }
    }

    size_t Emplace(const KeyT &key, size_t &mem_increased)
    {
        // append check
        auto insert_it =
            keys_.size() > 0 && keys_.back() < key
                ? keys_.end()
                : std::lower_bound(keys_.begin(), keys_.end(), key);
        assert(insert_it == keys_.end() || *insert_it != key);

        size_t insert_pos = insert_it - keys_.begin();
        auto key_it = keys_.emplace(insert_it, key);
        auto entry_ptr_it = entries_.emplace(
            entries_.begin() + insert_pos,
            std::make_unique<CcEntry<KeyT, ValueT>>(parent_map_, this));

        size_t key_mem_increased = key_it->MemUsage() - sizeof(KeyT);
        size_t entry_mem_increased = (*entry_ptr_it)->GetCcEntryMemUsage();
        mem_increased += key_mem_increased + entry_mem_increased;
        return insert_pos;
    }

    /**
     * Find lower bound of key, requiring key is in the range of this page's
     * [front, back].
     * @param key
     * @return
     */
    size_t LowerBound(const KeyT &key) const
    {
        assert(!keys_.empty() && keys_.front() <= key && key <= keys_.back());
        size_t target_idx =
            std::lower_bound(keys_.begin(), keys_.end(), key) - keys_.begin();
        return target_idx;
    }

    void Split(
        std::vector<KeyT> &new_page_keys,
        std::vector<std::unique_ptr<CcEntry<KeyT, ValueT>>> &new_page_entries,
        uint64_t &new_last_commit_ts)
    {
        new_page_keys.reserve(split_threshold_);
        new_page_entries.reserve(split_threshold_);
        new_last_commit_ts = 0;
        size_t split_pos = keys_.size() / 2;
        new_page_keys.insert(new_page_keys.end(),
                             std::make_move_iterator(keys_.begin() + split_pos),
                             std::make_move_iterator(keys_.end()));
        for (size_t idx = split_pos; idx < entries_.size(); idx++)
        {
            new_last_commit_ts =
                std::max(new_last_commit_ts, entries_[idx]->commit_ts_);
            new_page_entries.push_back(std::move(entries_[idx]));
        }
        keys_.erase(keys_.begin() + split_pos, keys_.end());
        entries_.erase(entries_.begin() + split_pos, entries_.end());
    }

    /**
     * Find upper bound of key, requiring key is in the range of this page's
     * [front, back).
     * @param key
     * @return
     */
    size_t UpperBound(const KeyT &key) const
    {
        assert(!keys_.empty() && keys_.front() <= key && key < keys_.back());
        size_t target_idx =
            std::upper_bound(keys_.begin(), keys_.end(), key) - keys_.begin();
        return target_idx;
    }

    /**
     * Find cce in entries_, return its index.
     * @param cce
     * @return
     */
    size_t FindEntry(const CcEntry<KeyT, ValueT> *cce) const
    {
        assert(cce->parent_page_ == this);
        auto it = entries_.begin();
        while (it->get() != cce && it != entries_.end())
        {
            it++;
        }
        return it - entries_.begin();
    }

    bool Full() const
    {
        return keys_.size() == split_threshold_;
    }

    bool Empty() const
    {
        return keys_.size() == 0;
    }

    bool IsNegInf() const
    {
        return prev_page_ == nullptr;
    }

    bool IsPosInf() const
    {
        return next_page_ == nullptr;
    }

    const KeyT &FirstKey() const
    {
        if (IsNegInf())
        {
            return *NegativeInfinity<KeyT>::Instance();
        }
        if (IsPosInf())
        {
            return *PositiveInfinity<KeyT>::Instance();
        }
        assert(!keys_.empty());
        return keys_.front();
    }

    const KeyT &LastKey() const
    {
        if (IsNegInf())
        {
            return *NegativeInfinity<KeyT>::Instance();
        }
        if (IsPosInf())
        {
            return *PositiveInfinity<KeyT>::Instance();
        }
        assert(!keys_.empty());
        return keys_.back();
    }

    const KeyT *KeyOfEntry(const CcEntry<KeyT, ValueT> *cce) const
    {
        if (IsNegInf())
        {
            return NegativeInfinity<KeyT>::Instance();
        }
        if (IsPosInf())
        {
            return PositiveInfinity<KeyT>::Instance();
        }
        size_t idx_in_page = FindEntry(cce);
        assert(idx_in_page < keys_.size());
        return &keys_.at(idx_in_page);
    }

    CcEntry<KeyT, ValueT> *Entry(size_t idx_in_page)
    {
        assert(idx_in_page < entries_.size());
        return entries_[idx_in_page].get();
    }

    size_t Remove(size_t idx)
    {
        assert(idx < keys_.size());

        auto key_it = keys_.begin() + idx;
        auto entry_ptr_it = entries_.begin() + idx;
        size_t mem_decreased = key_it->MemUsage() - sizeof(KeyT) +
                               (*entry_ptr_it)->GetCcEntryMemUsage();

        keys_.erase(key_it);
        entries_.erase(entry_ptr_it);

        return mem_decreased;
    }

    size_t Remove(const KeyT &key)
    {
        size_t idx = Find(key);
        return Remove(idx);
    }

    size_t Remove(const CcEntry<KeyT, ValueT> *entry)
    {
        size_t idx = FindEntry(entry);
        return Remove(idx);
    }

    size_t Size() const
    {
        return keys_.size();
    }

    uint64_t LastReadTs() const
    {
        // todo: consider maintain last_read_ts_ of page
        uint64_t last_read_ts = 0;
        for (auto entry_it = entries_.begin(); entry_it != entries_.end();
             entry_it++)
        {
            last_read_ts = std::max(last_read_ts, (*entry_it)->last_read_ts_);
        }
        return last_read_ts;
    }

    void DebugPrint() const
    {
        LOG(INFO) << "page addr: " << this << ", keys_ size: " << keys_.size()
                  << ", entries_ size: " << entries_.size()
                  << ", prev_page_: " << prev_page_
                  << ", next_page_: " << next_page_
                  << ", IsNegInf: " << IsNegInf()
                  << ", IsPosInf: " << IsPosInf();
        //        for (const auto &up : entries_)
        //        {
        //            LOG(INFO) << "entry ptr: " << up.get();
        //        }
    }

    // threshold to trigger page split when inserting
    inline static constexpr size_t split_threshold_ = 64;
    // threshold to trigger page merge
    // needs thorough consideration to configure this as eager merging could
    // lead to thrashing, where a lot of successive delete and insert operations
    // lead to constant splits and merges
    inline static constexpr size_t merge_threshold_ = split_threshold_ / 2;

    std::vector<KeyT> keys_;
    std::vector<std::unique_ptr<CcEntry<KeyT, ValueT>>> entries_;

    CcPage<KeyT, ValueT> *prev_page_{nullptr};
    CcPage<KeyT, ValueT> *next_page_{nullptr};

    // The largest commit ts of dirty cc entries on this page. This value might
    // be larger than the actual max commit ts of cc entries. Currently used to
    // decide if this page has dirty data after a given ts.
    uint64_t last_dirty_commit_ts_{0};

    // CcPage is contained in std::_Rb_tree_node with node key and RBT node
    // pointers (32 bytes)
    inline static size_t basic_mem_overhead_ =
        sizeof(KeyT) + sizeof(CcPage<KeyT, ValueT>) + 32 +
        sizeof(KeyT) * split_threshold_ +
        sizeof(std::unique_ptr<CcEntry<KeyT, ValueT>>) * split_threshold_ +
        sizeof(uint64_t);
};

template <typename KeyT, typename ValueT>
const TxKey *CcEntry<KeyT, ValueT>::Key() const
{
    return parent_page_->KeyOfEntry(this);
}

struct CcEntryAddr
{
public:
    CcEntryAddr() : cce_ptr_(0), node_group_id_(0), core_id_(0), term_(-1)
    {
    }

    CcEntryAddr(const CcEntryAddr &rhs)
        : cce_ptr_(rhs.cce_ptr_),
          node_group_id_(rhs.node_group_id_),
          core_id_(rhs.core_id_),
          term_(rhs.term_.load(std::memory_order_acquire))
    {
    }

    bool operator==(const CcEntryAddr &rhs) const
    {
        return node_group_id_ == rhs.node_group_id_ && term_ == rhs.term_ &&
               cce_ptr_ != 0 && rhs.cce_ptr_ != 0 && cce_ptr_ == rhs.cce_ptr_;
    }

    CcEntryAddr &operator=(const CcEntryAddr &rhs)
    {
        if (this == &rhs)
        {
            return *this;
        }

        cce_ptr_ = rhs.cce_ptr_;
        node_group_id_ = rhs.node_group_id_;
        term_.store(rhs.term_.load(std::memory_order_acquire),
                    std::memory_order_release);
        core_id_ = rhs.core_id_;

        return *this;
    }

    bool Empty() const
    {
        return cce_ptr_ == 0 || cce_ptr_ == 1;
    }

    uint64_t CcePtr() const
    {
        return cce_ptr_;
    }

    uint64_t InsertPtr() const
    {
        if (cce_ptr_ & 1)
        {
            return cce_ptr_ & (UINT64_MAX - 1);
        }
        else
        {
            return 0;
        }
    }

    uint32_t NodeGroupId() const
    {
        return node_group_id_;
    }

    int64_t Term() const
    {
        return term_.load(std::memory_order_acquire);
    }

    uint32_t CoreId() const
    {
        return core_id_;
    }

    void SetCce(uint64_t addr, int64_t term, uint32_t core_id)
    {
        cce_ptr_ = addr;
        term_.store(term, std::memory_order_release);
        core_id_ = core_id;
    }

    void SetCce(uint64_t addr, int64_t term, uint32_t ng, uint32_t core_id)
    {
        cce_ptr_ = addr;
        node_group_id_ = ng;
        term_.store(term, std::memory_order_release);
        core_id_ = core_id;
    }

    void SetInsert(uint64_t addr, int64_t term, uint32_t core_id)
    {
        assert((addr & 1) == 0);
        cce_ptr_ = addr | 1;
        term_.store(term, std::memory_order_release);
        core_id_ = core_id;
    }

    void SetInsert(uint64_t addr, int64_t term, uint32_t ng, uint32_t core_id)
    {
        assert((addr & 1) == 0);
        cce_ptr_ = addr | 1;
        node_group_id_ = ng;
        term_.store(term, std::memory_order_release);
        core_id_ = core_id;
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
    /**
     * @brief The cc entry memory address. Given that memory addresses are even
     * numbers, we use the lowest bit to denote if this address points to an
     * insert entry.
     *
     */
    uint64_t cce_ptr_;
    uint32_t node_group_id_;
    uint32_t core_id_;
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
