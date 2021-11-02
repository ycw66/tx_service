#pragma once

#include <atomic>
#include <map>
#include <unordered_set>

#include "cc_req_base.h"
#include "circular_queue.h"
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

    const LruEntry &Parent() const override
    {
        return *parent_entry_;
    }

    const KeyT key_;  // owner of key_
    TxId tx_id_;
    CcEntry<KeyT, ValueT> *parent_entry_;
};

struct LruEntry
{
public:
    LruEntry() = delete;
    virtual ~LruEntry() = default;

    LruEntry(CcMap *parent)
        : lru_prev_(nullptr),
          lru_next_(nullptr),
          ckpt_prev_(nullptr),
          ckpt_next_(nullptr),
          parent_map_(parent),
          write_intention_(UINT32_MAX),
          ckpt_ts_(1)
    {
    }

    virtual bool IsFree() const
    {
        return false;
    }

    LruEntry *lru_prev_ = nullptr;
    LruEntry *lru_next_ = nullptr;
    LruEntry *ckpt_prev_ = nullptr;
    LruEntry *ckpt_next_ = nullptr;
    CcMap *const parent_map_;

    /// <summary>
    /// OCC and 2PL shares the same write intention/lock. A non-empty write
    /// intention blocks other 2PL requests and forces OCC acquire requests to
    /// abort. The non-empty write intention, however, does not block OCC read
    /// requests.
    /// </summary>
    TxId write_intention_;
    // The term of the cc node group in which the intention-holding tx resides.
    int64_t tx_term_;

    /// <summary>
    /// The timestamp when this record was last flushed to the data store.
    /// Unlike other fields that are read/modified via a single thread, this
    /// field is updated by a separate checkpointing thread, after it flushes
    /// changes to the data store.
    /// </summary>
    std::atomic<uint64_t> ckpt_ts_;
};

template <typename KeyT, typename ValueT>
struct CcEntry : public LruEntry
{
public:
    /// <summary>
    /// A cc entry is initialized in the cc map with ts = 1, the beginning of
    /// history. The record's status is initially unknown, until a tx reads the
    /// key in the data store and brings the record into the cc entry, or a tx
    /// updates the key with a new record. Ts = 0 is reserved to indicate
    /// whether or not a key/gap needs to be validated.
    /// </summary>
    /// <param name="parent">The pointer of the cc map to which the cc entry
    /// belongs</param>
    CcEntry(CcMap *parent)
        : LruEntry(parent),
          key_(nullptr),
          payload_(),
          payload_status_(RecordStatus::Unknown),
          commit_ts_(1),
          last_vali_ts_(1),
          gap_commit_ts_(1),
          gap_last_vali_ts_(1),
          payload_ckpt_(),
          map_prev_(nullptr),
          map_next_(nullptr)
    {
    }

    void UnblockRequests()
    {
        while (blocking_queue_.Size() > 0)
        {
            Resumable *const &req = blocking_queue_.Peek();
            if (req->Resume())
            {
                blocking_queue_.Dequeue();
            }
            else
            {
                break;
            }
        }
    }

    bool IsFree() const override
    {
        return write_intention_.Empty() && insert_intention_set_.empty() &&
               rlck_holders_.empty() &&
               commit_ts_ <= ckpt_ts_.load(std::memory_order_acquire);
    }

    const KeyT *key_;
    ValueT payload_;
    RecordStatus payload_status_;
    uint64_t commit_ts_;
    uint64_t last_vali_ts_;

    uint64_t gap_commit_ts_;
    uint64_t gap_last_vali_ts_;
    std::map<const KeyT *,
             std::unique_ptr<InsertEntry<KeyT, ValueT>>,
             PtrLessThan<KeyT>>
        insert_intention_set_;

    // A set of transactions having obtained the (shared) read lock. A non-empty
    // set blocks 2PL write requests as well as OCC acquire requests.
    std::unordered_set<uint64_t> rlck_holders_;
    CircularQueue<Resumable *> blocking_queue_;

    /// <summary>
    /// When checkpointing decides to flush this record, the payload is copied
    /// to a standby object. This allows checkpointing to keep a raw pointer to
    /// the payload, rather than making a copy of it.
    /// </summary>
    std::pair<ValueT, bool> payload_ckpt_;

    CcEntry<KeyT, ValueT> *map_prev_;
    CcEntry<KeyT, ValueT> *map_next_;
};

struct CcEntryAddr
{
public:
    CcEntryAddr() : cce_ptr_(0), insert_ptr_(0), node_group_id_(0), term_(-1)
    {
    }

    CcEntryAddr(const CcEntryAddr &) = default;

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
        term_ = rhs.term_;

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
        return term_;
    }

    void SetCce(uint64_t addr, int64_t term)
    {
        cce_ptr_ = addr;
        insert_ptr_ = 0;
        term_ = term;
    }

    void SetCce(uint64_t addr, int64_t term, uint32_t ng)
    {
        cce_ptr_ = addr;
        insert_ptr_ = 0;
        term_ = term;
        node_group_id_ = ng;
    }

    void SetInsert(uint64_t addr, int64_t term)
    {
        insert_ptr_ = addr;
        cce_ptr_ = 0;
        term_ = term;
    }

    void SetInsert(uint64_t addr, int64_t term, uint32_t ng)
    {
        insert_ptr_ = addr;
        cce_ptr_ = 0;
        term_ = term;
        node_group_id_ = ng;
    }

    void SetNodeGroupId(uint32_t ng_id)
    {
        node_group_id_ = ng_id;
    }

    /*void SetTerm(int64_t term)
    {
        term_ = term;
    }*/

private:
    uint64_t cce_ptr_;
    uint64_t insert_ptr_;
    uint32_t node_group_id_;
    int64_t term_;
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
