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
    UPSERT_TABLE_ACQUIRE_WRITE_INTENT_FAIL
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
     "Failed at acquire write intent."}};

}  // namespace txservice