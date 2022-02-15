#pragma once

#include <string>

namespace txservice
{
#define KB(x)   ((size_t) (x) << 10);
#define MB(x)   ((size_t) (x) << 20);
#define GB(x)   ((size_t) (x) << 30);

struct Void
{
};

constexpr Void void_ = Void();

#define void_return return void_;

enum class DmlOperation
{
    Update,
    Delete,
    Insert,
    Upsert
};

enum class TxnStatus
{
    Ongoing = 0,
    Committed,
    Aborted,
    // After the runtime sends the commit command, the tx starts committing.
    // From this point forward, the tx runs toward the end, either committed or
    // aborted, and cannot be interrupted by the upper runtime, e.g., the user
    // closes the connection to the runtime. This status marks the period
    // between when the tx starts committing and when the tx's fate is finalized
    // (committed or aborted).
    Committing,
    // transaction is finished and can be recycled.
    Finished
};

using TableName = std::string;
using NodeGroupId = uint32_t;

enum struct TableType
{
    Primary,
    Secondary,
    Catalog,
    RangePartition
};

enum struct ReadType
{
    // Starts concurrency control for the input the key and returns the key's
    // value.
    Inside,
    // Starts concurrency control for the input key-value pair retrieved from
    // the data store.
    OutsideNormal,
    // Starts concurrency control for the input key that does not exist in the
    // data store.
    OutsideDeleted
};

enum struct LockType
{
    NoLock = 0,
    ReadIntention,
    ReadLock,
    WriteIntent,
    WriteLock
};

enum struct PostWriteType
{
    // Single commit installs the committed value and removes the write
    // lock/intent.
    Commit,
    // PrepareCommit uploads a dirty value but does not release the write
    // intent/lock acquired previously. After the prepare commit log flushed,
    // the operation is guaranteed to succeed and can only roll forward upon
    // failures.
    PrepareCommit,
    // PostCommit releases the write lock/intent and turns the dirty value to
    // the committed value.
    PostCommit
};

inline static TableName catalog_ccm_name{"__catalog"};
}  // namespace txservice