#pragma once

#include <brpc/channel.h>
#include <brpc/stream.h>

#include <condition_variable>
#include <deque>
#include <memory>  // std::unique_ptr
#include <mutex>
#include <unordered_map>

#include "cc/cc_handler_result.h"
#include "moodycamelqueue.h"
#include "proto/cc_request.pb.h"

namespace txservice
{
namespace remote
{
/**
 * @brief Messages need to be resend.
 * cc_stream_sender may fail to send a message, possibly due to the remote
 * node is restarted, and cc_stream_sender needs to reconnect the stream.
 */
struct ResendMessage
{
public:
    using Uptr = std::unique_ptr<ResendMessage>;
    ResendMessage();
    ResendMessage(const CcMessage &msg, CcHandlerResultBase *res)
        : msg_(msg), res_(res)
    {
    }

    CcMessage msg_;
    CcHandlerResultBase *res_;
};

class CcStreamSender
{
public:
    CcStreamSender();
    ~CcStreamSender();

    void RecycleArena(std::unique_ptr<google::protobuf::Arena> arena);
    bool SendMessageToNg(uint32_t node_group_id,
                         const CcMessage &msg,
                         CcHandlerResultBase *res = nullptr,
                         bool resend = false);
    bool SendMessageToNode(uint32_t dest_node_id,
                           const CcMessage &msg,
                           CcHandlerResultBase *res = nullptr,
                           bool resend = false);
    void AddRemoteNode(uint32_t node_id, const std::string &ip, uint16_t port);

    /**
     * @brief Used by cc_stream_receiver. Nofity to setup stream to peer when
     * receiving peer's connect request.
     */
    void NotifyConnectStream();

private:
    void ConnectStreams();
    int ConnectStream(uint32_t node_id, int64_t version);

    std::mutex outbound_mux_;
    std::condition_variable out_cv_;

    std::unordered_map<uint32_t, std::pair<brpc::Channel, std::string>>
        outbound_channels_;
    // A map mapping the destination node ID to the stream connecting to it.
    // Each stream is associated with a version number, to prevent two users
    // from re-connecting the stream simultaneously.
    std::unordered_map<uint32_t,
                       std::pair<brpc::StreamId, std::atomic<int64_t>>>
        outbound_streams_;
    std::unordered_map<uint32_t, int64_t> to_connect_nodes_;
    // <node_id, resend_queue_to_node_id>
    std::unordered_map<uint32_t,
                       moodycamel::ConcurrentQueue<ResendMessage::Uptr>>
        resend_message_list_;

    // The background thread that establishes cc streams to remote nodes.
    std::thread connect_thd_;
    bool terminate_;
};
}  // namespace remote
}  // namespace txservice