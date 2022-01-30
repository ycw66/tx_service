#pragma once

#include <stdint.h>

#include "scan.h"
#include "tx_container.h"
#include "tx_key.h"
#include "tx_record.h"
#include "type.h"

namespace txservice
{
using SecondaryKeys =
    std::vector<std::tuple<const TableName *, TxKeyContainer, bool>>;

struct WriteSetEntry
{
    using Uptr = std::unique_ptr<WriteSetEntry>;

    WriteSetEntry()
        : key_(nullptr, ContainerType::rvalue),
          rec_(nullptr, ContainerType::rvalue),
          op_(DmlOperation::Upsert),
          cce_addr_(),
          sindx_()
    {
    }

    WriteSetEntry(const WriteSetEntry &other) = delete;

    WriteSetEntry(WriteSetEntry &&other) noexcept
        : key_(std::move(other.key_)),
          rec_(std::move(other.rec_)),
          op_(other.op_),
          cce_addr_(other.cce_addr_),
          sindx_(std::move(other.sindx_))
    {
    }

    TxKeyContainer key_;
    TxRecordContainer rec_;
    DmlOperation op_;
    CcEntryAddr cce_addr_;
    SecondaryKeys sindx_;
};

struct ReadSetEntry
{
    ReadSetEntry() = delete;
    ReadSetEntry(uint64_t ts, CcProtocol proto)
        : version_ts_(ts), protocol_(proto)
    {
    }

    uint64_t version_ts_;
    /**
     * @brief The concurrency control protocol used when this read is performed.
     * A tx reads two types of data: catalogs when the query is compiled, and
     * data items when the query is executed. Data items are read under the
     * concurrency control protocol specified by the tx. Catalogs are read under
     * a fixed protocol irrespective of the tx's. The tx relies on this
     * parameter to perform appropriate post-processing operations for read
     * data.
     *
     */
    CcProtocol protocol_;
};

struct ScanSetEntry
{
    using Uptr = std::unique_ptr<ScanSetEntry>;

    ScanSetEntry() : key_ts_(0), gap_ts_(0), cce_addr_()
    {
    }

    ScanSetEntry(const ScanSetEntry &rhs) = delete;

    ScanSetEntry(ScanSetEntry &&other) noexcept
        : key_ts_(other.key_ts_),
          gap_ts_(other.gap_ts_),
          cce_addr_(other.cce_addr_)
    {
    }

    void Reset(uint64_t begin_ts, const CcEntryAddr &addr)
    {
        key_ts_ = begin_ts;
        gap_ts_ = begin_ts;
        cce_addr_ = addr;
    }

    uint64_t key_ts_;
    uint64_t gap_ts_;
    CcEntryAddr cce_addr_;
};

}  // namespace txservice
