#pragma once

#include <cassert>  // assert
#include <cstdint>

namespace txservice
{

/**
 * @brief
 * - "Optimistic Read"  : Read intent or no intent. No conflict with anyone.
 *
 * - "Pessimistic Read" : ReadLock. Conflict: block and wait.
 *
 * - "Optimistic Write" : WriteLock and WriteIntent. Confilict: back off and
 * retry.
 *
 * - "Pessimistic Write" : WriteLock and WriteIntent. Confilict: block and wait.
 *
 */
enum class CcProtocol
{
    OCC = 0,  // Optimistic Read + Optimistic Write
    OccRead,  // Optimistic Read + Pessimistic Write
    Locking,  // Pessimistic Read + Pessimistic Write
};

enum class CcOperation
{
    Read = 0,
    ReadForWrite,
    Write,
    ReadSkIndex,
};

/**
 * @brief
 * - "Snapshot isolation level" can be accomplished only using "OCC" or
 * "OccRead" CcProtocol.
 *
 * - "ReadCommitted"/"RepeatableRead"/"Serializable" can be accomplished using
 * all CcProtocol.
 *
 */
enum class IsolationLevel
{
    ReadCommitted = 0,
    Snapshot,
    RepeatableRead,
    Serializable
};

enum class LockType : uint8_t
{
    NoLock = 0,
    ReadIntent,
    ReadLock,
    WriteIntent,
    WriteLock,
};

enum class LockOpStatus
{
    Successful = 0,
    Failed,
    Blocked
};

class LockTypeUtil
{
public:
    static LockType DeduceLockType(CcOperation cc_op,
                                   IsolationLevel iso_level,
                                   CcProtocol cc_protocol,
                                   bool is_covering_keys)
    {
        if (cc_op == CcOperation::ReadSkIndex)
        {
            if (iso_level == IsolationLevel::Snapshot ||
                (iso_level == IsolationLevel::ReadCommitted &&
                 is_covering_keys))
            {
                return LockType::NoLock;
            }
            else
            {
                return LockType::ReadLock;
            }
        }
        else if (cc_op == CcOperation::ReadForWrite)
        {
            return LockType::WriteIntent;
        }
        else if (cc_op == CcOperation::Write)
        {
            return LockType::WriteLock;
        }
        else if (cc_op == CcOperation::Read)
        {
            switch (iso_level)
            {
            case IsolationLevel::ReadCommitted:
            case IsolationLevel::Snapshot:
                return LockType::NoLock;
            case IsolationLevel::RepeatableRead:
            case IsolationLevel::Serializable:
                if (cc_protocol == CcProtocol::Locking)
                {
                    return LockType::ReadLock;
                }
                else
                {
                    return LockType::ReadIntent;
                }
            default:
                assert(false);
                return LockType::NoLock;
            }
        }
        assert(false);
        return LockType::NoLock;
    }
};

}  // namespace txservice