#pragma once

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

struct SecondaryKeySchema : public Schema
{
public:
    SecondaryKeySchema() = delete;

    SecondaryKeySchema(const Schema *sk_sch, const Schema *pk_sch)
        : sk_schema_(sk_sch->Clone()), pk_schema_(pk_sch->Clone())
    {
    }

    SecondaryKeySchema(std::unique_ptr<const Schema> sk_sch,
                       std::unique_ptr<const Schema> pk_sch)
        : sk_schema_(std::move(sk_sch)), pk_schema_(std::move(pk_sch))
    {
    }

    SecondaryKeySchema(const SecondaryKeySchema &sch)
        : sk_schema_(sch.sk_schema_->Clone()),
          pk_schema_(sch.pk_schema_->Clone())
    {
    }

    Schema::Uptr Clone() const override
    {
        return std::make_unique<SecondaryKeySchema>(*this);
    }

    std::unique_ptr<const Schema> sk_schema_;
    std::unique_ptr<const Schema> pk_schema_;
};
}  // namespace txservice