#pragma once

#include <stdint.h>

#include <memory>
#include <string>

namespace txservice
{
class TxKey;
struct TxRecord;

struct Schema
{
public:
    using Uptr = std::unique_ptr<Schema>;

    virtual ~Schema() = default;

    virtual std::string GetColumnString(const TxKey &key, size_t col_idx) const
    {
        std::string s;
        return s;
    }

    virtual std::string GetColumnString(const TxRecord &rec,
                                        size_t col_idx) const
    {
        std::string s;
        return s;
    }

    virtual Schema::Uptr Clone() const = 0;
};

struct KeySchema : public Schema
{
    virtual bool CompareKeys(const TxKey &key1,
                             const TxKey &key2,
                             size_t *const column_index) const = 0;

    virtual uint16_t ExtendKeyParts() const = 0;
    virtual uint64_t SchemaTs() const = 0;
};

struct SecondaryKeySchema : public KeySchema
{
public:
    SecondaryKeySchema() = delete;

    SecondaryKeySchema(const Schema *sk_sch, const Schema *pk_sch)
        : SecondaryKeySchema(sk_sch->Clone(), pk_sch->Clone())
    {
    }

    SecondaryKeySchema(std::unique_ptr<const Schema> sk_sch,
                       std::unique_ptr<const Schema> pk_sch)
        : sk_schema_(static_cast<const KeySchema *>(sk_sch.release())),
          pk_schema_(static_cast<const KeySchema *>(pk_sch.release()))
    {
    }

    SecondaryKeySchema(const SecondaryKeySchema &sch)
        : SecondaryKeySchema(sch.sk_schema_->Clone(), sch.pk_schema_->Clone())
    {
    }

    Schema::Uptr Clone() const override
    {
        return std::make_unique<SecondaryKeySchema>(*this);
    }

    bool CompareKeys(const TxKey &key1,
                     const TxKey &key2,
                     size_t *const column_index) const override
    {
        return sk_schema_->CompareKeys(key1, key2, column_index);
    }

    // Number of key parts in the index (including "index extension")
    uint16_t ExtendKeyParts() const override
    {
        return sk_schema_->ExtendKeyParts();
    }

    uint64_t SchemaTs() const override
    {
        return sk_schema_->SchemaTs();
    }

    std::unique_ptr<const KeySchema> sk_schema_;
    std::unique_ptr<const KeySchema> pk_schema_;
};
}  // namespace txservice
