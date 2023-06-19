#pragma once

#include <memory>
#include <string>

#include "tx_record.h"

namespace txservice
{
struct TxObject;

struct TxCommandResult
{
public:
    virtual ~TxCommandResult() = default;
    virtual void Serialize(std::string &buf) const = 0;
    virtual void Deserialize(const char *buf, size_t &offset) = 0;
};

struct TxCommand
{
public:
    virtual ~TxCommand() = default;
    virtual bool IsReadOnly() const = 0;
    virtual std::unique_ptr<TxRecord> CreateObject(
        const std::string *image) const = 0;
    virtual std::unique_ptr<TxCommandResult> CreateCommandResult() const = 0;
    virtual bool ProceedOnNonExistentObject() const = 0;
    virtual void Apply(TxObject &object, TxCommandResult &cmd_result) const = 0;

    // read only command need not commit
    virtual void CommitOn(TxObject &object)
    {
    }

    // serialize command for remote request and writing log
    virtual void Serialize(std::string &str) const
    {
    }

    // deserialize a command from binary blob for processing remote request and
    // replaying log
    virtual void Deserialize(std::string_view cmd_img)
    {
    }
};
}  // namespace txservice
