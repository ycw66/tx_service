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
    TemplateScanTuple() : key_obj_(), rec_obj_()
    {
    }

    TemplateScanTuple(TemplateScanTuple<KeyT, ValueT> &&rhs)
        : key_obj_(rhs.key_obj_),
          rec_obj_(rhs.rec_obj_),
          ScanTuple(std::move(rhs))
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

    KeyT &Key()
    {
        return key_obj_;
    }

    ValueT &Record()
    {
        return rec_obj_;
    }

private:
    KeyT key_obj_;
    ValueT rec_obj_;

    template <typename KT, typename VT>
    friend class TemplateCcScanner;
};
}  // namespace txservice
