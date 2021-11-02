#pragma once

#ifdef __GNUC__

#include <brpc/controller.h>

#include "../log_service/proto/raft_log.pb.h"
#include "cc/cc_handler_result.h"

namespace txservice
{
enum struct LogType
{
    RECORD,
    CREATE_TABLE,
    DROP_TABLE
};

/*
 * LogClosure is the closure for txlog service
 *
 * LogClosure will be passed to txlog service along with log request. When log
 * request is finished, Function Run() will be called to notify the txservice.
 */
class LogClosure : public google::protobuf::Closure
{
public:
    LogClosure(CcHandlerResult<Void> *notify) : cntl_(), notify_tx_(notify)
    {
    }

    ~LogClosure() = default;

    // Run() will be called when log request is processed by txlog service.
    void Run() override
    {
        if (response_.success())
        {
            notify_tx_->SetFinished();
        }
        else
        {
            notify_tx_->SetError(1);
        }
    }

    const ::txlog::LogRequest &LogReq() const
    {
        return request_;
    }

    ::txlog::LogRequest &MutableLogRequest()
    {
        return request_;
    }

    ::txlog::LogResponse &MutableLogResponse()
    {
        return response_;
    }

    brpc::Controller *Controller()
    {
        return &cntl_;
    }

    void Reset()
    {
        cntl_.Reset();
        request_.clear_write_log_request();
        response_.clear_write_log_response();
    }

private:
    brpc::Controller cntl_;
    ::txlog::LogRequest request_;
    ::txlog::LogResponse response_;
    CcHandlerResult<Void> *notify_tx_;
};
}  // namespace txservice

#endif
