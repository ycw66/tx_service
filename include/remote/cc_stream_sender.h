#pragma once

#include <brpc/channel.h>
#include <brpc/stream.h>

#include <condition_variable>
#include <deque>
#include <mutex>
#include <unordered_map>

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
    ResendMessage(uint32_t node_group_id, CcMessage msg)
        : node_group_id_(node_group_id), msg_(msg)
    {
    }

    uint32_t node_group_id_;
    CcMessage msg_;
};

class CcStreamSender
{
public:
    CcStreamSender(
        moodycamel::ConcurrentQueue<std::unique_ptr<CcMessage>> &msg_pool);
    ~CcStreamSender();

    void RecycleCcMsg(std::unique_ptr<CcMessage> msg);
    bool SendMessage(uint32_t node_group_id,
                     const CcMessage &msg,
                     bool resend = false);
    void AddRemoteNode(uint32_t node_id, const std::string &ip, uint16_t port);

private:
    void ConnectStreams();
    int ConnectStream(uint32_t node_id, int64_t version);

    moodycamel::ConcurrentQueue<std::unique_ptr<CcMessage>> &msg_pool_;

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
    std::deque<ResendMessage::Uptr> resend_message_list;

    // The background thread that establishes cc streams to remote nodes.
    std::thread connect_thd_;
    bool terminate_;
};
}  // namespace remote
}  // namespace txservice