#pragma once

#include <memory>
#include <stdexcept>
#include <string>

#include "schema.h"
#include "tx_serialize.h"

namespace txservice
{
enum class KeyType
{
    NegativeInf,
    PositiveInf,
    Normal
};

class TxKey
{
public:
    using Uptr = std::unique_ptr<TxKey>;

    virtual ~TxKey() = default;
    /*
    virtual TxKey &operator=(const TxKey &that) = 0;*/
    virtual bool operator==(const TxKey &rhs) const = 0;
    virtual bool operator<(const TxKey &rhs) const = 0;
    virtual size_t Hash() const = 0;
    virtual void Serialize(std::vector<char> &buf, size_t &offset) const = 0;
    virtual void Serialize(std::string &str) const = 0;
    virtual void Deserialize(const char *buf,
                             size_t &offset,
                             const Schema *key_schema) = 0;
    virtual TxKey::Uptr Clone() const = 0;
    virtual std::string ToString() const = 0;
    virtual void Copy(const TxKey &rhs) = 0;

    /**
     * Whether a search key is prefix of a full key, to distinguish between
     * prefix equality and full equality. Returns true if *this is a prefix of
     * rhs, false if *this and rhs is exactly the same. Should be called only
     * when *this == rhs returns true.
     * @param rhs
     * @return
     */
    virtual bool IsPrefixOf(const TxKey &rhs) const
    {
        return false;
    }

    virtual KeyType Type() const
    {
        return KeyType::Normal;
    }

    virtual size_t MemUsage() const
    {
        return 0;
    }

    static size_t HashCode(const TxKey &sk, const TxKey &pk)
    {
        size_t hash = 17;
        hash = hash * 23 + sk.Hash();
        hash = hash * 23 + pk.Hash();

        return hash;
    }
};

struct TxKeyHash
{
    std::size_t operator()(const TxKey *key_ptr) const
    {
        return key_ptr->Hash();
    }
};

template <typename T>
struct PtrEqual
{
    bool operator()(const T *lhs, const T *rhs) const
    {
        return *lhs == *rhs;
    }
};

template <typename T>
struct PtrLessThan
{
    bool operator()(const T *lhs, const T *rhs) const
    {
        return *lhs < *rhs;
    }
};

template <typename KeyT>
struct NegativeInfinity : public KeyT
{
public:
    static const NegativeInfinity<KeyT> *Instance()
    {
        static NegativeInfinity<KeyT> instance_;
        return &instance_;
    }

    bool operator==(const KeyT &rhs) const
    {
        return &rhs == Instance();
    }

    bool operator==(const TxKey &rhs) const override
    {
        return &rhs == Instance();
    }

    bool operator<(const KeyT &rhs) const
    {
        // Negative infinity is smaller than any key, except itself.
        if (&rhs == Instance())
        {
            return false;
        }
        return true;
    }

    bool operator<(const TxKey &rhs) const override
    {
        // Negative infinity is smaller than any key, except itself.
        if (&rhs == Instance())
        {
            return false;
        }
        return true;
    }

    KeyType Type() const override
    {
        return KeyType::NegativeInf;
    }

private:
    NegativeInfinity()
    {
    }

    NegativeInfinity(const NegativeInfinity<KeyT> &rhs) = delete;
    NegativeInfinity(NegativeInfinity<KeyT> &&rhs) = delete;
};

template <typename KeyT>
struct PositiveInfinity : public KeyT
{
public:
    static const PositiveInfinity<KeyT> *Instance()
    {
        static PositiveInfinity<KeyT> instance_;
        return &instance_;
    }

    bool operator==(const KeyT &rhs) const
    {
        const TxKey *self = Instance();
        return self == &rhs;
    }

    bool operator==(const TxKey &rhs) const override
    {
        return Instance() == &rhs;
    }

    bool operator<(const KeyT &rhs) const
    {
        // Positive infinity is no less than any key, including itself.
        return false;
    }

    bool operator<(const TxKey &rhs) const override
    {
        // Positive infinity is no less than any key, including itself.
        return false;
    }

    KeyType Type() const override
    {
        return KeyType::PositiveInf;
    }

private:
    PositiveInfinity()
    {
    }

    PositiveInfinity(const PositiveInfinity<KeyT> &rhs) = delete;
    PositiveInfinity(PositiveInfinity<KeyT> &&rhs) = delete;
};

template <typename T>
struct CompositeHash
{
    static void Hash(size_t &hash, const T &val)
    {
        hash = hash * 23 + std::hash<T>()(val);
    }
};

template <typename... Types>
class CompositeKey : public TxKey
{
public:
    CompositeKey() : field_cnt_(0)
    {
    }

    CompositeKey(Types &&...val) : fields_(val...)
    {
        field_cnt_ = std::tuple_size<decltype(fields_)>::value;
    }

    CompositeKey(std::tuple<Types...> &&t) : fields_(t)
    {
        field_cnt_ = std::tuple_size<decltype(fields_)>::value;
    }

    CompositeKey(const CompositeKey &other)
        : fields_(other.fields_), field_cnt_(other.field_cnt_)
    {
    }

    CompositeKey(const CompositeKey &rhs, const txservice::Schema *schema)
        : fields_(rhs.fields_), field_cnt_(rhs.field_cnt_)
    {
    }

    CompositeKey(CompositeKey &&other)
        : fields_(other.fields_), field_cnt_(other.field_cnt_)
    {
    }

    void Reset(Types &&...vals)
    {
        TupleResetHelper(fields_, std::index_sequence_for<Types...>{}, vals...);
    }

    void Reset(const Types &...vals)
    {
        TupleResetHelper(fields_, std::index_sequence_for<Types...>{}, vals...);
    }

    bool operator==(const CompositeKey<Types...> &rhs) const
    {
        if (&rhs == NegativeInfinity<CompositeKey<Types...>>::Instance() ||
            &rhs == PositiveInfinity<CompositeKey<Types...>>::Instance())
        {
            return this == &rhs;
        }
        return fields_ == rhs.fields_;
    }

    bool operator==(const TxKey &rhs) const override
    {
        const CompositeKey<Types...> &rhs_key =
            static_cast<const CompositeKey<Types...> &>(rhs);

        return *this == rhs_key;
    }

    bool operator<(const CompositeKey<Types...> &rhs) const
    {
        if (this == PositiveInfinity<CompositeKey<Types...>>::Instance() ||
            &rhs == NegativeInfinity<CompositeKey<Types...>>::Instance())
        {
            return false;
        }
        else if (this == NegativeInfinity<CompositeKey<Types...>>::Instance() ||
                 &rhs == PositiveInfinity<CompositeKey<Types...>>::Instance())
        {
            return true;
        }

        return fields_ < rhs.fields_;
    }

    bool operator<(const TxKey &rhs) const override
    {
        const CompositeKey<Types...> &rhs_key =
            static_cast<const CompositeKey<Types...> &>(rhs);

        return *this < rhs_key;
    }

    CompositeKey &operator=(const CompositeKey<Types...> &rhs)
    {
        if (this == &rhs)
        {
            return *this;
        }

        fields_ = rhs.fields_;
        field_cnt_ = rhs.field_cnt_;

        return *this;
    }

    size_t Hash() const override
    {
        size_t hash = field_cnt_ <= 1 ? 0 : 17;

        std::apply([&hash](Types... field)
                   { (void(CompositeHash<Types>::Hash(hash, field)), ...); },
                   fields_);

        return hash;
    }

    std::string Print()
    {
        return std::apply([](Types... v)
                          { return ((Stringify<Types>::Get(v) + ",") + ...); },
                          fields_);
    }

    void Serialize(std::vector<char> &buf, size_t &offset) const override
    {
        size_t mem_size = std::apply(
            [](Types... field) { return (MemSize<Types>::Size(field) + ...); },
            fields_);

        if (buf.capacity() - offset < mem_size)
        {
            buf.resize(offset + mem_size);
        }

        std::apply(
            [&buf, &offset](Types... field)
            { (void(Serializer<Types>::Serialize(field, buf, offset)), ...); },
            fields_);
    }

    void Serialize(std::string &buf) const override
    {
        std::apply([&buf](Types... field)
                   { (void(Serializer<Types>::Serialize(field, buf)), ...); },
                   fields_);
    }

    void Deserialize(const char *buf,
                     size_t &offset,
                     const Schema *schema) override
    {
        TupleDeserializeHelper(
            fields_, std::index_sequence_for<Types...>{}, buf, offset);
    }

    TxKey::Uptr Clone() const override
    {
        return std::make_unique<CompositeKey<Types...>>(*this);
    }

    void Copy(const TxKey &rhs) override
    {
        const CompositeKey<Types...> &typed_rhs =
            static_cast<const CompositeKey<Types...> &>(rhs);
        fields_ = typed_rhs.fields_;
        field_cnt_ = typed_rhs.field_cnt_;
    }

    const std::tuple<Types...> &Tuple() const
    {
        return fields_;
    }

    std::tuple<Types...> &Tuple()
    {
        return fields_;
    }

    std::string ToString() const override
    {
        return std::apply(
            [](Types... v)
            { return "<" + ((Stringify<Types>::Get(v) + ",") + ...) + ">"; },
            fields_);
    }

private:
    std::tuple<Types...> fields_;
    size_t field_cnt_;
};

struct VoidKey : public TxKey
{
    VoidKey()
    {
    }

    bool operator==(const TxKey &rhs) const override
    {
        if (const VoidKey *other_ptr = static_cast<const VoidKey *>(&rhs))
        {
            return *this == *other_ptr;
        }
        return false;
    }

    bool operator<(const TxKey &rhs) const override
    {
        return false;
    }

    size_t Hash() const override
    {
        return 0;
    }

    void Serialize(std::vector<char> &buf, size_t &offset) const override
    {
    }

    void Serialize(std::string &str) const override
    {
    }

    void Deserialize(const char *buf,
                     size_t &offset,
                     const txservice::Schema *key_schema) override
    {
    }

    TxKey::Uptr Clone() const override
    {
        return std::make_unique<VoidKey>(*this);
    }

    void Copy(const TxKey &rhs) override
    {
    }

    std::string ToString() const override
    {
        return std::string("");
    }

    size_t MemUsage() const override
    {
        return 0;
    }
};

}  // namespace txservice

namespace std
{
template <>
struct hash<txservice::TxKey *>
{
    std::size_t operator()(const txservice::TxKey *key) const
    {
        return key->Hash();
    }
};
}  // namespace std
