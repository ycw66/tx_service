#pragma once

#include <brpc/channel.h>
#include <brpc/server.h>
#include <brpc/stream.h>

#include <mutex>
#include <unordered_set>

#include "proto/cc_request.pb.h"
#include "raft_log.pb.h"

namespace txservice
{
class LocalCcShards;

namespace fault
{
class CcNode;

class LogReplayHandler : public brpc::StreamInputHandler,
                         public ::txlog::LogReplayService
{
public:
    LogReplayHandler() = delete;

    LogReplayHandler(LocalCcShards &local_shards, uint16_t port);

    ~LogReplayHandler();

    void Connect(::google::protobuf::RpcController *controller,
                 const ::txlog::LogReplayConnectRequest *request,
                 ::txlog::LogReplayConnectResponse *response,
                 ::google::protobuf::Closure *done) override;

    int on_received_messages(brpc::StreamId stream_id,
                             butil::IOBuf *const messages[],
                             size_t size) override;

    void on_idle_timeout(brpc::StreamId id) override
    {
    }

    void on_closed(brpc::StreamId id) override
    {
    }

private:
    LocalCcShards &local_shards_;
    brpc::Server replay_server_;
    uint32_t log_group_cnt_;
    // Log groups that have finished log replay
    std::unordered_set<brpc::StreamId> inbound_streams_;
    std::mutex inbound_mux_;

    // CcNode &cc_node_;
};
}  // namespace fault
}  // namespace txservice
