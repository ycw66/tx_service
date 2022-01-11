#pragma once

#include "tx_record.h"

namespace txservice
{
struct PartitionIdRecord : public TxRecord
{
public:
    PartitionIdRecord(uint32_t pid) : partition_id_(pid)
    {
    }

    PartitionIdRecord(const PartitionIdRecord &rhs)
        : partition_id_(rhs.partition_id_)
    {
    }

    ~PartitionIdRecord() = default;

    void Serialize(std::vector<char> &buf, size_t &offset) const override
    {
    }

    void Serialize(std::string &str) const override
    {
    }

    void Deserialize(const char *buf, size_t &offset) override
    {
    }

    TxRecord::Uptr Clone() const override
    {
        return std::make_unique<PartitionIdRecord>(*this);
    }

    void Copy(const TxRecord &rhs) override
    {
        const PartitionIdRecord &that =
            static_cast<const PartitionIdRecord &>(rhs);

        partition_id_ = that.partition_id_;
    }

    std::string ToString() const override
    {
        return std::to_string(partition_id_);
    }

private:
    uint32_t partition_id_;
};
}  // namespace txservice