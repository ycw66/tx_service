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

struct SkSchema : public Schema
{
public:
    SkSchema() = delete;

    SkSchema(const Schema *sk_sch, const Schema *pk_sch)
    {
        sk_schema_.reset(sk_sch->Clone().release());
        pk_schema_.reset(pk_sch->Clone().release());
    }

    SkSchema(const SkSchema &sch)
    {
        sk_schema_.reset(sch.sk_schema_->Clone().release());
        pk_schema_.reset(sch.pk_schema_->Clone().release());
    }

    Schema::Uptr Clone() const override
    {
        return std::make_unique<SkSchema>(*this);
    }

    std::unique_ptr<const Schema> sk_schema_;
    std::unique_ptr<const Schema> pk_schema_;
};
}  // namespace txservice