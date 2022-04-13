#pragma once

namespace txservice
{
enum class CcProtocol
{
    OCC = 0,
    Locking,
    MVCC
};

enum class IsolationLevel
{
    ReadCommitted = 0,
    Snapshot,
    RepeatableRead,
    Serializable
};
}  // namespace txservice