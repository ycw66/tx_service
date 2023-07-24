#include "range_bucket_key_record.h"

namespace txservice
{
bool operator==(const RangeBucketKey &lhs, const RangeBucketKey &rhs)
{
    return lhs.bucket_id_ == rhs.bucket_id_;
}
bool operator!=(const RangeBucketKey &lhs, const RangeBucketKey &rhs)
{
    return lhs.bucket_id_ != rhs.bucket_id_;
}
bool operator<(const RangeBucketKey &lhs, const RangeBucketKey &rhs)
{
    return lhs.bucket_id_ < rhs.bucket_id_;
}
bool operator<=(const RangeBucketKey &lhs, const RangeBucketKey &rhs)
{
    return lhs.bucket_id_ <= rhs.bucket_id_;
}
bool RangeBucketKey::operator==(const TxKey &rhs) const
{
    return false;
}
bool RangeBucketKey::operator<(const TxKey &rhs) const
{
    return false;
}
size_t RangeBucketKey::Hash() const
{
    return std::hash<uint16_t>()(bucket_id_);
}

void RangeBucketKey::Serialize(std::vector<char> &buf, size_t &offset) const
{
    // Serialize bucket_id_ to buffer at offset
    const char *val_ptr =
        static_cast<const char *>(static_cast<const void *>(&bucket_id_));
    std::copy(val_ptr, val_ptr + sizeof(uint16_t), buf.begin() + offset);
    offset += sizeof(uint16_t);
}

void RangeBucketKey::Serialize(std::string &str) const
{
    // Serialize bucket_id_ to string
    const char *ptr = reinterpret_cast<const char *>(&bucket_id_);
    str.append(ptr, sizeof(uint16_t));
}

size_t RangeBucketKey::SerializedLength() const
{
    return sizeof(uint16_t);
}
void RangeBucketKey::Deserialize(const char *buf,
                                 size_t &offset,
                                 const Schema *schema)
{
}
TxKey::Uptr RangeBucketKey::Clone() const
{
    return std::make_unique<RangeBucketKey>(*this);
}
void RangeBucketKey::Copy(const TxKey &rhs)
{
}
std::string RangeBucketKey::ToString() const
{
    return "";
}
void RangeBucketRecord::Serialize(std::vector<char> &buf, size_t &offset) const
{
}
void RangeBucketRecord::Serialize(std::string &str) const
{
}
size_t RangeBucketRecord::SerializedLength() const
{
    return 0;
}
void RangeBucketRecord::Deserialize(const char *buf, size_t &offset)
{
}
TxRecord::Uptr RangeBucketRecord::Clone() const
{
    return std::make_unique<RangeBucketRecord>(*this);
}
void RangeBucketRecord::Copy(const TxRecord &rhs)
{
}
std::string RangeBucketRecord::ToString() const
{
    return "";
}
}  // namespace txservice