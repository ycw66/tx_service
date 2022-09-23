#pragma once

#ifdef __GNUC__

#include <brpc/controller.h>

#include "../log_service/proto/raft_log.pb.h"
#include "cc/cc_handler_result.h"
#include "fault_inject.h"
#include "type.h"

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
    explicit LogClosure(CcHandlerResult<Void> *hd_result)
        : cntl_(), hd_result_(hd_result)
    {
    }

    ~LogClosure() = default;

    // Run() will be called when log request is processed by txlog service.
    void Run() override
    {
        CODE_FAULT_INJECTOR("log_closure_result_unknown", {
            hd_result_->SetError((int8_t) HandlerResultErrorType::Unknown);
            return;
        });
        // rpc fails including timeout indicates the status of log request is
        // unknown.
        if (cntl_.Failed() || response_.response_status() ==
                                  ::txlog::LogResponse_ResponseStatus_Unknown)
        {
            hd_result_->SetError((int8_t) HandlerResultErrorType::Unknown);
        }
        else if (response_.response_status() ==
                 ::txlog::LogResponse_ResponseStatus_Success)
        {
            hd_result_->SetFinished();
        }
        else
        {
            hd_result_->SetError((int8_t) HandlerResultErrorType::Error);
        }
    }

    ::txlog::LogRequest &LogRequest()
    {
        return request_;
    }

    const ::txlog::LogRequest &LogRequest() const
    {
        return request_;
    }

    ::txlog::LogResponse &LogResponse()
    {
        return response_;
    }

    const ::txlog::LogResponse &LogResponse() const
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
        // request should not reset since the log request could be resend.
        response_.Clear();
    }

private:
    brpc::Controller cntl_;
    ::txlog::LogRequest request_;
    ::txlog::LogResponse response_;
    CcHandlerResult<Void> *hd_result_;
};
}  // namespace txservice

#endif
