#pragma once

#include "tx_key.h"

namespace txservice
{
template <typename SkT, typename PkT>
struct SecondaryKey : public TxKey
{
public:
    SecondaryKey() : sk_(), pk_()
    {
    }

    SecondaryKey(const SecondaryKey &rhs) : sk_(rhs.sk_), pk_(rhs.pk_)
    {
    }

    SecondaryKey(const SkT &sk, const PkT &pk) : sk_(sk), pk_(pk)
    {
    }

    SecondaryKey(SkT &&sk, PkT &&pk) : sk_(sk), pk_(pk)
    {
    }

    bool operator==(const TxKey &rhs) const override
    {
        if (const SecondaryKey *rhs_ptr =
                static_cast<const SecondaryKey *>(&rhs))
        {
            return sk_ == rhs_ptr->sk_ && pk_ == rhs_ptr->pk_;
        }

        return false;
    }

    bool operator==(const SecondaryKey &rhs) const
    {
        return sk_ == rhs.sk_ && pk_ == rhs.pk_;
    }

    bool operator<(const TxKey &rhs) const override
    {
        if (const SecondaryKey *rhs_ptr =
                static_cast<const SecondaryKey *>(&rhs))
        {
            return sk_ < rhs_ptr->sk_ ||
                   (sk_ == rhs_ptr->sk_ && pk_ < rhs_ptr->pk_);
        }

        return false;
    }

    size_t Hash() const override
    {
        return TxKey::HashCode(sk_, pk_);
    }

    void Serialize(std::vector<char> &buf, size_t &offset) const override
    {
        sk_.Serialize(buf, offset);
        pk_.Serialize(buf, offset);
    }

    void Serialize(std::string &str) const override
    {
        sk_.Serialize(str);
        pk_.Serialize(str);
    }

    void Deserialize(const char *buf,
                     size_t &offset,
                     const Schema *schema) override
    {
        const Schema *sk_schema = nullptr;
        const Schema *pk_schema = nullptr;

        if (schema != nullptr)
        {
            const SkSchema *sk_sch = static_cast<const SkSchema *>(schema);
            sk_schema = sk_sch->sk_schema_.get();
            pk_schema = sk_sch->pk_schema_.get();
        }

        sk_.Deserialize(buf, offset, sk_schema);
        pk_.Deserialize(buf, offset, pk_schema);
    }

    TxKey::Uptr Clone() const override
    {
        return std::make_unique<SecondaryKey<SkT, PkT>>(sk_, pk_);
    }

    std::string ToString() const override
    {
        return sk_.ToString() + "," + pk_.ToString();
    }

    SkT &SKey()
    {
        return sk_;
    }

    const SkT &SKey() const
    {
        return sk_;
    }

    PkT &PKey()
    {
        return pk_;
    }

    const PkT &PKey() const
    {
        return pk_;
    }

private:
    SkT sk_;
    PkT pk_;
};

/// <summary>
/// A secondary index key is a pair of the indexed value and the primary key.
/// </summary>
// struct SecondaryKey : public TxKey
//{
//    SecondaryKey() = delete;
//    SecondaryKey(const SecondaryKey &other) = delete;
//
//    SecondaryKey(TxKey::Uptr sk, TxKey::Uptr pk)
//        : sk_(std::move(sk)), pk_(std::move(pk))
//    {
//    }
//
//    virtual bool operator==(const TxKey &rhs) const override
//    {
//        if (&rhs == &(PosInfinityKey::instance) ||
//            &rhs == &(NegInfinityKey::instance))
//        {
//            return false;
//        }
//
//        if (const SecondaryKey *rhs_ptr =
//                static_cast<const SecondaryKey *>(&rhs))
//        {
//            return *sk_ == *rhs_ptr->sk_ && *pk_ == *rhs_ptr->pk_;
//        }
//
//        return false;
//    }
//    virtual bool operator<(const TxKey &rhs) const override
//    {
//        if (&rhs == &(PosInfinityKey::instance))
//        {
//            return true;
//        }
//        else if (&rhs == &(NegInfinityKey::instance))
//        {
//            return false;
//        }
//
//        if (const SecondaryKey *rhs_ptr =
//                static_cast<const SecondaryKey *>(&rhs))
//        {
//            return *sk_ < *rhs_ptr->sk_ ||
//                   *sk_ == *rhs_ptr->sk_ && *pk_ < *rhs_ptr->pk_;
//        }
//
//        return false;
//    }
//
//    virtual TxKey &operator=(const TxKey &rhs) override
//    {
//        if (const SecondaryKey *rhs_ptr =
//                static_cast<const SecondaryKey *>(&rhs))
//        {
//            if (sk_ == nullptr)
//            {
//                sk_ = rhs_ptr->sk_->Clone();
//            }
//            else
//            {
//                *sk_ = *rhs_ptr->sk_;
//            }
//
//            if (pk_ == nullptr)
//            {
//                pk_ = rhs_ptr->pk_->Clone();
//            }
//            else
//            {
//                *pk_ = *rhs_ptr->pk_;
//            }
//        }
//
//        return *this;
//    }
//
//    virtual size_t Hash() const override
//    {
//        return 0;
//    }
//    virtual void Serialize(std::vector<char> &buf,
//                           size_t &offset) const override
//    {
//        sk_->Serialize(buf, offset);
//        pk_->Serialize(buf, offset);
//    }
//
//    virtual void Deserialize(const char *buf, size_t &offset) override
//    {
//        if (sk_ == nullptr || pk_ == nullptr)
//        {
//            return;
//        }
//
//        sk_->Deserialize(buf, offset);
//        pk_->Deserialize(buf, offset);
//    }
//
//    virtual TxKey::Uptr Clone() const
//    {
//        return std::make_unique<SecondaryKey>(sk_->Clone(), pk_->Clone());
//    }
//
//    virtual std::string ToString() const
//    {
//        return sk_->ToString() + "," + pk_->ToString();
//    }
//
//    TxKey::Uptr sk_;
//    TxKey::Uptr pk_;
//};
}  // namespace txservice