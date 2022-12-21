#pragma once

#include <map>
#include <string>
#include <vector>

namespace txservice
{
enum struct TxErrorCode
{
    NO_ERROR = 0,
    UNDEFINED_ERR,
    // Fails to acquire write locks due to write-Write conflicts.
    WRITE_WRITE_CONFLICT,
    // Acquairing write locks times out.
    ACQUIRE_WRITE_TIMEOUT,
    // Read validations fail.
    VALIDATION_FAIL,
    // Read validations time out.
    VALIDATION_TIMEOUT,
    // Flushing the data log is unsuccessful.
    DATA_LOG_FAIL,
    // Fail to flush the prepare commit log.
    PREPARE_LOG_FAIL,
    // Fail to flush the post commit log.
    POST_LOG_FAIL,
    // A cc request's target cc map does not exist.
    CCM_NOT_FOUND,
    // A cc request is directed to a follower of the target cc node group.
    CC_REQ_FOLLOWER,
    // The tx state machine is bound to a follower of a cc node group.
    TX_BOUND_FOLLOWER,
    DATA_STORE_READ_ERR,
    DATA_STORE_WRITE_ERR,
    DATA_STORE_CONNECT_ERR,
    OCC_BREAK_REPEATABLE_READ,
    LOG_SERVICE_UNREACHABLE,
    WRITE_LOG_FAIL,
    UPSERT_TABLE_PREPARE_FAIL,
    TRANSACTION_NODE_NOT_LEADER,
    UPSERT_TABLE_ACQUIRE_WRITE_INTENT_FAIL,
    SPLIT_RANGE_ACQUIRE_WRITE_INTENT_FAIL,
    SPLIT_RANGE_ACQUIRE_WRITE_LOCK_FAIL,
    SPLIT_RANGE_PREPARE_LOG_FOR_OLD_RANGE_FAIL,
    WRITE_SET_BYTES_COUNT_EXCEED_ERR,

    // Under MVCC protocol, if write transaction has acquired the write
    // lock, then it will generate its commit_ts without knowing the later
    // read. Hence the writer's commit_ts may be smaller than the reader's
    // snapshot_ts which will break the snapshot isolation level.
    CC_ERR_MVCC_READ_MUST_WAIT_WRITE,
    CC_ERR_MVCC_VERSION_PREMATURELY_KICKED,
    // Detect dead lock and abort the transaction
    DEAD_LOCK_ABORT
};

enum struct CcErrorCode
{
    NO_ERROR = 0,
    UNDEFINED_ERR,

    FORCE_FAIL,
    NG_TERM_CHANGED,
    REQUEST_NODE_NOT_LEADER,
    TX_NODE_NOT_LEADER,

    NEGOTIATED_TX_UNKNOWN,
    NEGOTIATE_TX_ERR,

    REQUESTED_TABLE_DROPPED,
    REQUESTED_TABLE_INDEX_DROPPED,
    CRATE_CCM_SCANNER_FAILED,

    DUPLICATE_INSERT_ERR,
    ACQUIRE_KEY_LOCK_FAILED,
    ACQUIRE_GAP_LOCK_FAILED,
    VALIDATION_FAILED_FOR_VERSION_MISMATCH,
    VALIDATION_FAILED_FOR_CONFILICTED_TXS,
    MVCC_READ_MUST_WAIT_WRITE,
    MVCC_READ_FOR_WRITE_NEED_LATEST,

    // range
    GET_RANGE_ID_ERR,
    PIN_RANGE_SLICE_FAILED,

    // data store handler
    DATA_STORE_UPSERT_TABLE_ERR,

    // log service
    LOG_CLOSURE_RESULT_UNKOWN_ERR,

    // Detect dead lock and abort the transaction
    DEAD_LOCK_ABORT,

    // NOTICE: please keep this variable at tail.
    LAST_ERROR_CODE,

};

static const std::map<TxErrorCode, std::string> error_messages{
    {TxErrorCode::UNDEFINED_ERR, "Undefined error."},
    {TxErrorCode::OCC_BREAK_REPEATABLE_READ,
     "OCC break repeatable read isolation level."},
    {TxErrorCode::LOG_SERVICE_UNREACHABLE,
     "Log service is unreachable, transaction status is unknown."},
    {TxErrorCode::WRITE_LOG_FAIL, "Write Log fails."},
    {TxErrorCode::UPSERT_TABLE_PREPARE_FAIL, "Failed at prepare phase."},
    {TxErrorCode::TRANSACTION_NODE_NOT_LEADER,
     "Transaction failed due to the transaction node is no longer the raft "
     "leader."},
    {TxErrorCode::UPSERT_TABLE_ACQUIRE_WRITE_INTENT_FAIL,
     "Failed at acquire write intent."},
    {TxErrorCode::WRITE_SET_BYTES_COUNT_EXCEED_ERR,
     "Transaction failed due to write set bytes count too large."},
    {TxErrorCode::WRITE_WRITE_CONFLICT,
     "Transaction failed due to write-write conflicts."},
    {TxErrorCode::DEAD_LOCK_ABORT, "Abort the transaction due to dead lock."}};

}  // namespace txservice
