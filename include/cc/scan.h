#pragma once

#include <atomic>
#include <memory>  // shared_ptr

#include "cc_entry.h"
#include "tx_key.h"
#include "tx_record.h"

namespace txservice
{
enum class ScanDirection
{
    Forward,
    Backward
};

enum class ScanIndexType
{
    Primary,
    Secondary
};

enum class InclusiveType
{
    Open,
    Close
};

struct ScanTuple
{
public:
    ScanTuple()
        : key_ts_(0), gap_ts_(0), rec_status_(RecordStatus::Normal), cce_addr_()
    {
    }

    ScanTuple(uint64_t key_ts,
              uint64_t gap_ts,
              RecordStatus status,
              const CcEntryAddr &cce_addr)
        : key_ts_(key_ts),
          gap_ts_(gap_ts),
          rec_status_(status),
          cce_addr_(cce_addr)
    {
    }

    ScanTuple(ScanTuple &&tuple) = delete;
    ScanTuple(const ScanTuple &other) = delete;

    virtual ~ScanTuple() = default;

    virtual const TxKey *Key() const = 0;
    virtual const TxRecord *Record() const = 0;

    uint64_t key_ts_;
    uint64_t gap_ts_;
    RecordStatus rec_status_;
    CcEntryAddr cce_addr_;
};

template <typename KeyT, typename ValueT>
struct TemplateScanTuple : public ScanTuple
{
public:
    TemplateScanTuple()
        : ScanTuple(),
          key_obj_(),
          rec_ptr_(nullptr),
          rec_obj_(nullptr),
          is_ptr_(false)
    {
    }

    TemplateScanTuple(TemplateScanTuple<KeyT, ValueT> &&rhs)
        : ScanTuple(rhs.key_ts_, rhs.gap_ts_, rhs.rec_status_, rhs.cce_addr_),
          key_obj_(std::move(rhs.key_obj_)),
          rec_ptr_(std::move(rhs.rec_ptr_)),
          rec_obj_(std::move(rhs.rec_obj_)),
          is_ptr_(rhs.is_ptr_)
    {
    }

    ~TemplateScanTuple() = default;

    const TxKey *Key() const override
    {
        return &key_obj_;
    }

    const TxRecord *Record() const override
    {
        return is_ptr_ ? rec_ptr_.get() : rec_obj_.get();
    }

    KeyT &KeyObj()
    {
        return key_obj_;
    }

    const KeyT &KeyObj() const
    {
        return key_obj_;
    }

    const ValueT &RecordObj()
    {
        return is_ptr_ ? *rec_ptr_ : *rec_obj_;
    }

    void SetRecord(std::shared_ptr<ValueT> &ptr)
    {
        is_ptr_ = true;
        rec_ptr_ = ptr;
    }

    void SetRecord(const char *rec, size_t &offset)
    {
        is_ptr_ = false;
        if (rec_obj_ == nullptr)
        {
            rec_obj_ = std::make_unique<ValueT>();
        }
        rec_obj_->Deserialize(rec, offset);
    }
    friend bool operator<(const TemplateScanTuple<KeyT, ValueT> &lhs,
                          const TemplateScanTuple<KeyT, ValueT> &rhs)
    {
        return (lhs.key_ts_ != 0 && rhs.key_ts_ != 0 &&
                lhs.KeyObj() < rhs.KeyObj()) ||
               (lhs.key_ts_ == 0 && rhs.key_ts_ != 0);
    }

private:
    KeyT key_obj_;
    // Used when rec is pointing to a local cc map cc entry.
    std::shared_ptr<ValueT> rec_ptr_;
    // Used when rec is deserialized from a remote scan slice response,
    // in which case ScanTuple is the record owner,
    std::unique_ptr<ValueT> rec_obj_;
    bool is_ptr_{false};

    template <typename KT, typename VT>
    friend class TemplateCcScanner;

    template <typename KT, typename VT>
    friend class TemplateScanCache;
};
}  // namespace txservice
