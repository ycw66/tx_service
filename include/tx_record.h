#pragma once

#include <memory>
#include <string>
#include <variant>

#include "tx_key.h"
#include "tx_serialize.h"

namespace txservice
{
enum struct RecordStatus
{
    /// <summary>
    /// The record returned from the cc map is the newest committed value.
    /// </summary>
    Normal = 0,
    /// <summary>
    /// A tx starts concurrency control for the key, but the key's record is
    /// unknown and needs to be retrieved from the data store.
    /// </summary>
    Unknown,
    /// <summary>
    /// The record is deleted.
    /// </summary>
    Deleted,
    /// <summary>
    /// A tx sends a read request to bring the record into a remote cc map for
    /// caching. The read request does not wait for the response, so the remote
    /// record's newest status is unknown (which may have changed since the
    /// initial read that starts concurrency control).
    /// </summary>
    RemoteUnknown,
    /// <summary>
    /// Under MVCC-SnapshotIsolation, a tx read one of the historical
    /// versions of a key, but the historical version is unknown and needs
    /// to be retrieved from the data store.
    //  Also, the historical version may be not in data store if the historical
    //  versions are not flushed into data store before node crashing.
    /// </summary>
    VersionUnknown
};

struct TxRecord
{
    using Uptr = std::unique_ptr<TxRecord>;

    virtual ~TxRecord() = default;
    virtual void Serialize(std::vector<char> &buf, size_t &offset) const = 0;
    virtual void Serialize(std::string &str) const = 0;
    virtual void Deserialize(const char *buf, size_t &offset) = 0;
    virtual TxRecord::Uptr Clone() const = 0;
    virtual void Copy(const TxRecord &rhs) = 0;
    virtual std::string ToString() const = 0;

    virtual size_t MemUsage() const
    {
        return 0;
    }
};

template <typename... Types>
class CompositeRecord : public TxRecord
{
public:
    CompositeRecord() : field_cnt_(0)
    {
    }

    CompositeRecord(Types &&...val) : fields_(val...)
    {
        field_cnt_ = std::tuple_size<decltype(fields_)>::value;
    }

    CompositeRecord(std::tuple<Types...> &&t) : fields_(t)
    {
        field_cnt_ = std::tuple_size<decltype(fields_)>::value;
    }

    CompositeRecord(const CompositeRecord &other)
        : fields_(other.fields_), field_cnt_(other.field_cnt_)
    {
    }

    CompositeRecord(const TxRecord &other)
    {
        if (const CompositeRecord<Types...> *other_ptr =
                static_cast<const CompositeRecord<Types...> *>(&other))
        {
            fields_ = other_ptr->fields_;
            field_cnt_ = other_ptr->field_cnt_;
        }
    }

    CompositeRecord(CompositeRecord &&other)
        : fields_(other.fields_), field_cnt_(other.field_cnt_)
    {
    }

    ~CompositeRecord() = default;

    void Reset(Types &&...vals)
    {
        // fields_(val...);
        TupleResetHelper(fields_, std::index_sequence_for<Types...>{}, vals...);
    }

    void Reset(const Types &...vals)
    {
        TupleResetHelper(fields_, std::index_sequence_for<Types...>{}, vals...);
    }

    std::string ToString() const override
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

    void Deserialize(const char *buf, size_t &offset) override
    {
        TupleDeserializeHelper(
            fields_, std::index_sequence_for<Types...>{}, buf, offset);
    }

    TxRecord::Uptr Clone() const override
    {
        return std::make_unique<CompositeRecord<Types...>>(*this);
    }

    void Copy(const TxRecord &rhs) override
    {
        const CompositeRecord<Types...> &typed_rhs =
            static_cast<const CompositeRecord<Types...> &>(rhs);

        fields_ = typed_rhs.fields_;
        field_cnt_ = typed_rhs.field_cnt_;
    }

    size_t MemUsage() const override
    {
        return sizeof(field_cnt_) + sizeof(fields_);
    }

    CompositeRecord &operator=(const CompositeRecord &rhs)
    {
        if (this == &rhs)
        {
            return *this;
        }

        fields_ = rhs.fields_;
        field_cnt_ = rhs.field_cnt_;

        return *this;
    }

    const std::tuple<Types...> &Tuple() const
    {
        return fields_;
    }

    std::tuple<Types...> &Tuple()
    {
        return fields_;
    }

private:
    std::tuple<Types...> fields_;
    size_t field_cnt_;
};

struct VoidRecord : public TxRecord
{
public:
    VoidRecord()
    {
    }

    VoidRecord(VoidRecord &&rhs)
    {
    }

    ~VoidRecord() = default;

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
        return std::make_unique<VoidRecord>();
    }

    void Copy(const TxRecord &rhs) override
    {
        return;
    }

    VoidRecord &operator=(const VoidRecord &rhs)
    {
        return *this;
    }

    VoidRecord &operator=(VoidRecord &&other) noexcept
    {
        return *this;
    }

    std::string ToString() const override
    {
        return std::string("");
    }
};

/**
 * @brief A wrap type of TxRecord with version and status.
 * @param record_status_ : txservice::RecordStatus
 * @param commit_ts_ : uint64_t
 */
struct VersionTxRecord
{
public:
    VersionTxRecord()
        : record_(nullptr),
          record_status_(RecordStatus::Unknown),
          commit_ts_(1UL)
    {
    }

    std::unique_ptr<TxRecord> record_;
    RecordStatus record_status_;
    uint64_t commit_ts_;
};

}  // namespace txservice
