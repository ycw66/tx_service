#pragma once

#include <memory>

namespace txservice
{
class TxKey;
struct TxRecord;

struct Schema
{
public:
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
};

struct SkSchema : public Schema
{
public:
    SkSchema() = delete;

    SkSchema(const Schema *sk_sch, const Schema *pk_sch)
        : sk_schema_(sk_sch), pk_schema_(pk_sch)
    {
    }

    const Schema *const sk_schema_;
    const Schema *const pk_schema_;
};
}  // namespace txservice