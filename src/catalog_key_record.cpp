#include "catalog_key_record.h"

namespace txservice
{
CatalogKey::CatalogKey()
{
}

CatalogKey::CatalogKey(const TableName &name) : table_name_(name)
{
}

CatalogKey::CatalogKey(const CatalogKey &rhs, const Schema *)
    : table_name_(rhs.table_name_)
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
    buf.resize(offset + sizeof(uint16_t) + len_val);
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

void CatalogKey::Copy(const TxKey &rhs)
{
    const CatalogKey &typed_rhs = static_cast<const CatalogKey &>(rhs);
    table_name_ = typed_rhs.table_name_;
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
    uint32_t len_val = (uint32_t) schema_image_.size();
    buf.reserve(offset + sizeof(uint32_t) + len_val);
    const char *val_ptr =
        static_cast<const char *>(static_cast<const void *>(&len_val));
    std::copy(val_ptr, val_ptr + sizeof(uint32_t), buf.begin() + offset);
    offset += sizeof(uint32_t);

    std::copy(schema_image_.begin(), schema_image_.end(), buf.begin() + offset);
    offset += len_val;
}

void CatalogRecord::Serialize(std::string &str) const
{
    size_t len_sizeof = sizeof(uint32_t);
    uint32_t len_val = (uint32_t) schema_image_.size();
    const char *len_ptr = reinterpret_cast<const char *>(&len_val);

    str.append(len_ptr, len_sizeof);
    str.append(schema_image_.data(), len_val);
}

void CatalogRecord::Deserialize(const char *buf, size_t &offset)
{
    uint32_t *len_ptr = (uint32_t *) (buf + offset);
    uint32_t len_val = *len_ptr;
    offset += sizeof(uint32_t);

    schema_image_.clear();
    schema_image_.reserve(len_val);
    schema_image_.append(buf + offset, len_val);
    offset += len_val;
}

TxRecord::Uptr CatalogRecord::Clone() const
{
    std::unique_ptr<CatalogRecord> rec = std::make_unique<CatalogRecord>();

    rec->schema_ = schema_;
    rec->dirty_schema_ = dirty_schema_;
    rec->schema_ts_ = schema_ts_;
    rec->schema_image_ = schema_image_;

    return rec;
}

void CatalogRecord::Copy(const TxRecord &rhs)
{
    const CatalogRecord &typed_rhs = static_cast<const CatalogRecord &>(rhs);
    *this = typed_rhs;
}

std::string CatalogRecord::ToString() const
{
    return std::string();
}

void CatalogRecord::SetSchemaView(const TableSchemaView *view)
{
    schema_ = view->schema_;
    dirty_schema_ = view->dirty_schema_;
    schema_ts_ = view->version_ts_;
}

const std::string &CatalogRecord::SchemaImage() const
{
    return schema_image_;
}

void CatalogRecord::SetSchemaImage(std::string &&schema_image)
{
    schema_image_ = std::move(schema_image);
}

void CatalogRecord::SetSchemaImage(std::string &schema_image)
{
    schema_image_ = schema_image;
}

const TableSchema *CatalogRecord::Schema() const
{
    return schema_;
}

uint64_t CatalogRecord::SchemaTs() const
{
    return schema_ts_;
}

const TableSchema *CatalogRecord::DirtySchema() const
{
    return dirty_schema_;
}

CatalogRecord &CatalogRecord::operator=(const CatalogRecord &rhs)
{
    if (this == &rhs)
    {
        return *this;
    }

    schema_ = rhs.schema_;
    dirty_schema_ = rhs.dirty_schema_;
    schema_ts_ = rhs.schema_ts_;
    schema_image_ = rhs.schema_image_;

    return *this;
}
}  // namespace txservice