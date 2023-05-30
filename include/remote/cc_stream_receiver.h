#pragma once

#include <brpc/stream.h>
#include <google/protobuf/arena.h>

#include <condition_variable>
#include <memory>  // std::unique_ptr
#include <mutex>
#include <unordered_set>

#include "cc_req_pool.h"
#include "moodycamelqueue.h"
#include "proto/cc_request.pb.h"
#include "tx_record.h"
#include "type.h"

namespace txservice
{
class LocalCcShards;

namespace remote
{
class CcStreamSender;

class CcStreamReceiver : public brpc::StreamInputHandler, public CcStreamService
{
public:
    explicit CcStreamReceiver(LocalCcShards &local_shards);
    ~CcStreamReceiver() = default;

    void Shutdown();

    void Connect(::google::protobuf::RpcController *controller,
                 const ConnectRequest *request,
                 ConnectResponse *response,
                 ::google::protobuf::Closure *done) override;

    int on_received_messages(brpc::StreamId stream_id,
                             butil::IOBuf *const messages[],
                             size_t size) override;

    void on_idle_timeout(brpc::StreamId stream) override
    {
    }

    void on_closed(brpc::StreamId stream) override;

private:
    std::unique_ptr<google::protobuf::Arena> GetArena();
    void RecycleArena(std::unique_ptr<google::protobuf::Arena> arena);
    void OnReceiveCcMsg(CcMessage *msg,
                        std::unique_ptr<google::protobuf::Arena> arena);

    std::mutex inbound_mux_;
    std::condition_variable inbound_cv_;
    std::unordered_set<brpc::StreamId> inbound_streams_;
    LocalCcShards &local_shards_;
};
}  // namespace remote
}  // namespace txservice