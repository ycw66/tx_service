#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "cc_entry.h"
#include "tx_command.h"
#include "tx_record.h"
#include "type.h"

namespace txservice
{

struct WriteSetEntry;

struct WriteSetEntry
{
    using Uptr = std::unique_ptr<WriteSetEntry>;

    WriteSetEntry() : rec_(nullptr), op_(OperationType::Upsert), cce_addr_()
    {
    }

    WriteSetEntry(const WriteSetEntry &other) = delete;

    WriteSetEntry(WriteSetEntry &&other) noexcept
        : rec_(std::move(other.rec_)),
          op_(other.op_),
          cce_addr_(other.cce_addr_),
          key_shard_code_(other.key_shard_code_),
          forward_addr_(std::move(other.forward_addr_))
    {
    }

    WriteSetEntry &operator=(WriteSetEntry &&other)
    {
        rec_ = std::move(other.rec_);
        op_ = other.op_;
        cce_addr_ = other.cce_addr_;
        key_shard_code_ = other.key_shard_code_;
        forward_addr_ = std::move(other.forward_addr_);

        return *this;
    }

    TxRecord::Uptr rec_;
    OperationType op_;
    CcEntryAddr cce_addr_;
    uint32_t key_shard_code_{};
    // Used in double write scenarios during online DDL.
    std::unordered_map<uint32_t, CcEntryAddr> forward_addr_;
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

struct CmdForwardEntry
{
    TxKey key_;
    uint32_t key_shard_code_;
    CcEntryAddr cce_addr_;

    CmdForwardEntry(TxKey &&key, uint32_t key_shard)
        : key_(std::move(key)), key_shard_code_(key_shard), cce_addr_()
    {
    }
};

/**
 * Txn commands on the same object.
 */
struct CmdSetEntry
{
    CmdSetEntry(uint64_t object_version,
                uint64_t last_vali_ts,
                std::string &&key)
        : object_version_(object_version),
          last_vali_ts_(last_vali_ts),
          obj_key_str_(std::move(key)),
          has_overwrite_(false)
    {
    }

    void AddCommand(const TxCommand *cmd)
    {
        assert(cmd != nullptr);
        if (cmd->IsOverwrite())
        {
            // clear all the commands since we don't need to write them into log
            cmd_str_list_.clear();
            has_overwrite_ = true;
        }

        std::string cmd_str;
        cmd->Serialize(cmd_str);
        cmd_str_list_.emplace_back(std::move(cmd_str));
    }

    // No need to write to the log if there is no successful command.
    bool HasSuccessfulCommand() const
    {
        return has_overwrite_ || object_modified_;
    }

    // commit_ts of the object cce when the commands apply to it, commands on
    // the same object must apply in commit_ts order
    uint64_t object_version_{};
    // The cce's last_validation ts, for setting commit ts of this txn.
    uint64_t last_vali_ts_{};
    bool object_modified_{};
    // serialized key, for writing log
    std::string obj_key_str_{};
    // serialized commands, for writing log
    std::vector<std::string> cmd_str_list_{};
    // Whether a overwrite command exists. If true, commands before this cmd are
    // discarded since there is no point writing them into the log.
    bool has_overwrite_{};
    // Store the forward write key_shard_code info If this key's bucket is in
    // migration
    std::unique_ptr<CmdForwardEntry> forward_entry_{nullptr};
};

struct WriteEntry
{
    WriteEntry() = delete;
    WriteEntry(TxKey key, TxRecord::Uptr rec, uint64_t commit_ts)
        : key_(std::move(key)), rec_(std::move(rec)), commit_ts_(commit_ts)
    {
    }

    WriteEntry(const WriteEntry &rhs) = delete;
    WriteEntry(WriteEntry &&rhs)
    {
        key_ = std::move(rhs.key_);
        rec_ = std::move(rhs.rec_);
        commit_ts_ = rhs.commit_ts_;
    }

    WriteEntry &operator=(WriteEntry &&rhs)
    {
        if (this == &rhs)
        {
            return *this;
        }

        key_ = std::move(rhs.key_);
        rec_ = std::move(rhs.rec_);
        commit_ts_ = rhs.commit_ts_;

        return *this;
    }

    TxKey key_;
    TxRecord::Uptr rec_;
    uint64_t commit_ts_;
};

}  // namespace txservice
