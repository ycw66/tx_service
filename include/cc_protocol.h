#pragma once

namespace txservice
{
enum class CcProtocol
{
    OCC = 0,
    Locking
};

enum class IsolationLevel
{
    ReadCommitted = 0,
    Snapshot,
    RepeatableRead,
    Serializable
};
}  // namespace txservice