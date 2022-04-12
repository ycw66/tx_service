#include "catalog_key_record.h"

namespace txservice
{
CatalogKey::CatalogKey()
{
}

CatalogKey::CatalogKey(const TableName &name) : table_name_(name)
{
}

bool CatalogKey::operator==(const TxKey &rhs) const
{
    return false;
}

bool operator==(const CatalogKey &lhs, const CatalogKey &rhs)
{
    return lhs.table_name_ == rhs.table_name_;
}

bool CatalogKey::operator<(const TxKey &rhs) const
{
    return false;
}

bool operator<(const CatalogKey &lhs, const CatalogKey &rhs)
{
    return lhs.table_name_ < rhs.table_name_;
}

size_t CatalogKey::Hash() const
{
    return std::hash<std::string>{}(table_name_);
}

void CatalogKey::Serialize(std::vector<char> &buf, size_t &offset) const
{
    // A 2-byte integer represents lengths up to 65535, which is far more enough
    // for table names.
    uint16_t len_val = (uint16_t) table_name_.size();
    const char *val_ptr =
        static_cast<const char *>(static_cast<const void *>(&len_val));
    std::copy(val_ptr, val_ptr + sizeof(uint16_t), buf.begin() + offset);
    offset += sizeof(uint16_t);

    std::copy(table_name_.begin(), table_name_.end(), buf.begin() + offset);
    offset += len_val;
}

void CatalogKey::Serialize(std::string &str) const
{
    size_t len_sizeof = sizeof(uint16_t);
    // A 2-byte integer represents lengths up to 65535, which is far more enough
    // for table names.
    uint16_t len_val = (uint16_t) table_name_.size();
    const char *len_ptr = reinterpret_cast<const char *>(&len_val);

    str.append(len_ptr, len_sizeof);
    str.append(table_name_.data(), len_val);
}

void CatalogKey::Deserialize(const char *buf, size_t &offset, const Schema *)
{
    uint16_t *len_ptr = (uint16_t *) (buf + offset);
    uint16_t len_val = *len_ptr;
    offset += sizeof(uint16_t);

    table_name_.clear();
    table_name_.reserve(len_val);

    table_name_.append(buf + offset, len_val);
    offset += len_val;
}

TxKey::Uptr CatalogKey::Clone() const
{
    return std::make_unique<CatalogKey>(table_name_);
}

std::string CatalogKey::ToString() const
{
    return table_name_;
}

const TableName &CatalogKey::Name() const
{
    return table_name_;
}

TableName &CatalogKey::Name()
{
    return table_name_;
}

void CatalogRecord::Serialize(std::vector<char> &buf, size_t &offset) const
{
    const std::string *dirty_schema_image = std::get_if<1>(&binary_value_);
    assert(dirty_schema_image != nullptr);
    const std::string &schema_blob = *dirty_schema_image;

    uint32_t len_val = (uint32_t) schema_blob.size();
    const char *val_ptr =
        static_cast<const char *>(static_cast<const void *>(&len_val));
    std::copy(val_ptr, val_ptr + sizeof(uint32_t), buf.begin() + offset);
    offset += sizeof(uint32_t);

    std::copy(schema_blob.begin(), schema_blob.end(), buf.begin() + offset);
    offset += len_val;
}

void CatalogRecord::Serialize(std::string &str) const
{
    const std::string *dirty_schema_image = std::get_if<1>(&binary_value_);
    assert(dirty_schema_image != nullptr);
    const std::string &schema_blob = *dirty_schema_image;

    size_t len_sizeof = sizeof(uint32_t);
    uint32_t len_val = (uint32_t) schema_blob.size();
    const char *len_ptr = reinterpret_cast<const char *>(&len_val);

    str.append(len_ptr, len_sizeof);
    str.append(schema_blob.data(), len_val);
}

void CatalogRecord::Deserialize(const char *buf, size_t &offset)
{
    uint32_t *len_ptr = (uint32_t *) (buf + offset);
    uint32_t len_val = *len_ptr;
    offset += sizeof(uint32_t);

    binary_value_.emplace<1>(buf + offset, len_val);
    offset += len_val;
}

TxRecord::Uptr CatalogRecord::Clone() const
{
    return nullptr;
}

void CatalogRecord::Copy(const TxRecord &rhs)
{
}

std::string CatalogRecord::ToString() const
{
    return std::string();
}

const TableSchemaView *CatalogRecord::SchemaView() const
{
    const auto view = std::get_if<0>(&binary_value_);
    assert(view != nullptr);
    return *view;
}

void CatalogRecord::SetSchemaView(const TableSchemaView *view)
{
    binary_value_ = view;
}

const std::string &CatalogRecord::SchemaImage() const
{
    const std::string *schema_image = std::get_if<1>(&binary_value_);
    assert(schema_image != nullptr);
    return *schema_image;
}

void CatalogRecord::SetSchemaImage(std::string &&schema_image)
{
    binary_value_.emplace<1>(std::move(schema_image));
}

CatalogRecord &CatalogRecord::operator=(const CatalogRecord &rhs)
{
    if (this == &rhs)
    {
        return *this;
    }

    binary_value_ = rhs.binary_value_;

    return *this;
}
}  // namespace txservice