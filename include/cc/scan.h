#pragma once

#include <atomic>

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
    TemplateScanTuple() : ScanTuple(), key_obj_(), rec_obj_()
    {
    }

    TemplateScanTuple(TemplateScanTuple<KeyT, ValueT> &&rhs)
        : ScanTuple(rhs.key_ts_, rhs.gap_ts_, rhs.rec_status_, rhs.cce_addr_),
          key_obj_(std::move(rhs.key_obj_)),
          rec_obj_(std::move(rhs.rec_obj_))
    {
    }

    ~TemplateScanTuple() = default;

    const TxKey *Key() const override
    {
        return &key_obj_;
    }

    const TxRecord *Record() const override
    {
        return &rec_obj_;
    }

    KeyT &KeyObj()
    {
        return key_obj_;
    }

    const KeyT &KeyObj() const
    {
        return key_obj_;
    }

    ValueT &RecordObj()
    {
        return rec_obj_;
    }

    friend bool operator<(const TemplateScanTuple<KeyT, ValueT> &lhs,
                          const TemplateScanTuple<KeyT, ValueT> &rhs)
    {
        return lhs.key_ts_ != 0 && rhs.key_ts_ != 0 &&
                   lhs.KeyObj() < rhs.KeyObj() ||
               lhs.key_ts_ == 0 && rhs.key_ts_ != 0;
    }

private:
    KeyT key_obj_;
    ValueT rec_obj_;

    template <typename KT, typename VT>
    friend class TemplateCcScanner;
};
}  // namespace txservice
