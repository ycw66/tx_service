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
    OCC_BREAK_REPEATABLE_READ
};

static const std::map<TxErrorCode, std::string> error_messages{
    {TxErrorCode::UNDEFINED_ERR, "Undefined error."},
    {TxErrorCode::OCC_BREAK_REPEATABLE_READ,
     "OCC break repeatable read isolation level."},
};

}  // namespace txservice