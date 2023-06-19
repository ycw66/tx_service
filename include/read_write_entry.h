#pragma once

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "scan.h"
#include "tx_container.h"
#include "tx_key.h"
#include "tx_record.h"
#include "type.h"

namespace txservice
{

struct WriteSetEntry;

struct WriteSetEntry
{
    using Uptr = std::unique_ptr<WriteSetEntry>;

    WriteSetEntry()
        : key_(nullptr), rec_(nullptr), op_(OperationType::Upsert), cce_addr_()
    {
    }

    WriteSetEntry(const WriteSetEntry &other) = delete;

    WriteSetEntry(WriteSetEntry &&other) noexcept
        : key_(std::move(other.key_)),
          rec_(std::move(other.rec_)),
          op_(other.op_),
          cce_addr_(other.cce_addr_),
          key_shard_code_(other.key_shard_code_),
          forward_key_shard_code_(other.forward_key_shard_code_)
    {
    }

    WriteSetEntry &operator=(WriteSetEntry &&other)
    {
        key_ = std::move(other.key_);
        rec_ = std::move(other.rec_);
        op_ = other.op_;
        cce_addr_ = other.cce_addr_;
        key_shard_code_ = other.key_shard_code_;
        forward_key_shard_code_ = other.forward_key_shard_code_;

        return *this;
    }

    TxKey::Uptr key_;
    TxRecord::Uptr rec_;
    OperationType op_;
    CcEntryAddr cce_addr_;
    uint32_t key_shard_code_{};
    // Used in double write scenarios during online DDL.
    uint32_t forward_key_shard_code_{0};
};

struct ReadSetEntry
{
    ReadSetEntry() = delete;
    explicit ReadSetEntry(uint64_t ts) : version_ts_(ts)
    {
    }

    uint64_t version_ts_;
    uint16_t read_cnt_{1};
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

struct CmdSetEntry
{
    CmdSetEntry(uint64_t object_version, std::string &&key, std::string &&cmd)
        : object_version_(object_version), obj_key_str_(std::move(key))
    {
        cmd_str_list_.emplace_back(std::move(cmd));
    }

    // commit_ts of the object cce when the commands apply to it, commands on
    // the same object must apply in commit_ts order
    uint64_t object_version_{};
    // serialized key, for writing log
    std::string obj_key_str_{};
    // serialized commands, for writing log
    std::vector<std::string> cmd_str_list_{};
};

}  // namespace txservice
