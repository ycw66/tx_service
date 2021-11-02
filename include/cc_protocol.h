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
    RepeatableRead,
    Snapshot,
    Serializable
};
}  // namespace txservice