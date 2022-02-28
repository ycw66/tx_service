#pragma once

#include <brpc/channel.h>
#include <brpc/server.h>
#include <brpc/stream.h>

#include <condition_variable>
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

class ReplayService : public brpc::StreamInputHandler,
                      public ::txlog::LogReplayService
{
public:
    ReplayService() = delete;
    ReplayService(LocalCcShards &local_shards);
    ~ReplayService();

    void Connect(::google::protobuf::RpcController *controller,
                 const ::txlog::LogReplayConnectRequest *request,
                 ::txlog::LogReplayConnectResponse *response,
                 ::google::protobuf::Closure *done) override;

    void UpdateLogGroupLeader(::google::protobuf::RpcController *controller,
                              const ::txlog::LogLeaderUpdateRequest *request,
                              ::txlog::LogLeaderUpdateResponse *response,
                              ::google::protobuf::Closure *done) override;

    int on_received_messages(brpc::StreamId stream_id,
                             butil::IOBuf *const messages[],
                             size_t size) override;

    void on_idle_timeout(brpc::StreamId id) override
    {
    }

    void on_closed(brpc::StreamId id) override;

private:
    LocalCcShards &local_shards_;
    std::unordered_set<brpc::StreamId> inbound_streams_;
    std::mutex inbound_mux_;
    std::condition_variable inbound_cv_;
};
}  // namespace fault
}  // namespace txservice
