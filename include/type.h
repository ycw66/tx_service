#pragma once

namespace txservice
{
struct Void
{
};

constexpr Void void_ = Void();

#define void_return return void_;

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
    /// <summary>
    /// Starts concurrency control for the input the key.
    /// </summary>
    Inside,
    /// <summary>
    /// Starts concurrency control for the input key-value pair retrieved from
    /// the data store.
    /// </summary>
    OutsideNormal,
    /// <summary>
    /// Starts concurrency control for the input key that does not exist in the
    /// data store.
    /// </summary>
    OutsideDeleted,
};
}  // namespace txservice