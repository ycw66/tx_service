#include "remote/cc_stream_sender.h"

#include <chrono>
#include <string>

#include "sharder.h"
#include "tx_trace.h"

namespace txservice
{
namespace remote
{
CcStreamSender::~CcStreamSender()
{
    {
        std::unique_lock<std::mutex> lk(outbound_mux_);
        terminate_ = true;
    }
    out_cv_.notify_one();
    connect_thd_.join();
}

CcStreamSender::CcStreamSender(
    moodycamel::ConcurrentQueue<std::unique_ptr<CcMessage>> &msg_pool)
    : msg_pool_(msg_pool), terminate_(false)
{
    connect_thd_ = std::thread([this] { ConnectStreams(); });
}

void CcStreamSender::RecycleCcMsg(std::unique_ptr<CcMessage> msg)
{
    msg_pool_.enqueue(std::move(msg));
}

/**
 * @brief Send message to a specific node. Failed message will
 * be put into a retry list once the stream to the node is
 * reconstructed.
 *
 * @param dest_node_id
 * @param msg
 * @param res
 * @param resend
 * @return true
 * @return false
 */
bool CcStreamSender::SendMessageToNode(uint32_t dest_node_id,
                                       const CcMessage &msg,
                                       CcHandlerResultBase *res,
                                       bool resend)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &msg,
        (
            [&dest_node_id]() -> std::string
            {
                return std::string("{\"dest_node_id\":")
                    .append(std::to_string(dest_node_id))
                    .append("}");
            }));
    TX_TRACE_DUMP(&msg);

    auto stream_it = outbound_streams_.find(dest_node_id);
    if (stream_it == outbound_streams_.end())
    {
        // SendMessage error return -1 to indicate the request needs retry.
        if (res != nullptr)
        {
            res->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
        }

        LOG(ERROR) << "Trying to connect to an unknown remote node. Node Id: "
                   << dest_node_id;
        return false;
    }

    std::atomic<int64_t> &stream_version = stream_it->second.second;
    int64_t stream_ver = stream_version.load(std::memory_order_acquire);
    if (stream_ver < 0)
    {
        DLOG(INFO) << "CC stream is connecting, buffer the message for resend";
        // resend the message if stream is connecting
        auto resend_message_list = resend_message_list_.try_emplace(
            dest_node_id, moodycamel::ConcurrentQueue<ResendMessage::Uptr>());
        resend_message_list.first->second.enqueue(
            std::make_unique<ResendMessage>(msg, res));

        // always wake up connector thread to either reconnect streams or
        // resend messages.
        out_cv_.notify_one();
        return true;
    }

    brpc::StreamId &stream_id = stream_it->second.first;

    butil::IOBuf iobuf;
    butil::IOBufAsZeroCopyOutputStream wrapper(&iobuf);
    msg.SerializeToZeroCopyStream(&wrapper);

    int error_code = brpc::StreamWrite(stream_id, iobuf);
    while (error_code != 0)
    {
        if (error_code == EAGAIN)
        {
            error_code = brpc::StreamWrite(stream_id, iobuf);
        }
        else
        {
            // for resend message, we have already reconnect the stream, if it
            // still failed to send the message, it possibly means that the
            // remote node is dead. We should skip resend the message again.
            if (resend)
            {
                // SendMessage error return -1 to indicate the request needs
                // retry.
                if (res != nullptr)
                {
                    res->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
                }
                break;
            }

            std::lock_guard<std::mutex> lk(outbound_mux_);

            // If the stream version is -1, a separate thread has notified
            // the connecting thread to reconnect the stream. If the stream
            // version is greater than the previously-read one (stream_ver),
            // a new stream has been connected. In either case, the current
            // thread does not initiated a reconnection.
            if (stream_ver == stream_version.load(std::memory_order_acquire))
            {
                to_connect_nodes_.try_emplace(dest_node_id, stream_ver + 1);
                stream_version.store(-1, std::memory_order_release);
            }

            // put the failed message into the resend_message_list.
            auto resend_message_list = resend_message_list_.try_emplace(
                dest_node_id,
                moodycamel::ConcurrentQueue<ResendMessage::Uptr>());
            resend_message_list.first->second.enqueue(
                std::make_unique<ResendMessage>(msg, res));

            // always wake up connector thread to either reconnect streams or
            // resend messages.
            out_cv_.notify_one();

            break;
        }
    }

    return error_code == 0;
}

bool CcStreamSender::SendScanRespToNode(uint32_t dest_node_id,
                                        const ScanSliceResponse &msg,
                                        CcHandlerResultBase *res,
                                        bool resend)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        &msg,
        (
            [&dest_node_id]() -> std::string
            {
                return std::string("{\"dest_node_id\":")
                    .append(std::to_string(dest_node_id))
                    .append("}");
            }));
    TX_TRACE_DUMP(&msg);

    auto stream_it = long_msg_outbound_streams_.find(dest_node_id);
    if (stream_it == long_msg_outbound_streams_.end())
    {
        // SendMessage error return -1 to indicate the request needs retry.
        if (res != nullptr)
        {
            res->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
        }

        LOG(ERROR) << "Trying to connect to an unknown remote node. Node Id: "
                   << dest_node_id;
        return false;
    }

    std::atomic<int64_t> &stream_version = stream_it->second.second;
    int64_t stream_ver = stream_version.load(std::memory_order_acquire);
    if (stream_ver < 0)
    {
        // The stream is invalid, when the stream version is less than 0.
        // SendMessage error return -1 to indicate the request needs retry.
        if (res != nullptr)
        {
            res->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
        }
        return false;
    }

    brpc::StreamId &stream_id = stream_it->second.first;

    butil::IOBuf iobuf;
    butil::IOBufAsZeroCopyOutputStream wrapper(&iobuf);
    msg.SerializeToZeroCopyStream(&wrapper);

    int error_code = brpc::StreamWrite(stream_id, iobuf);
    while (error_code != 0)
    {
        if (error_code == EAGAIN)
        {
            error_code = brpc::StreamWrite(stream_id, iobuf);
        }
        else
        {
            // for resend message, we have already reconnect the stream, if it
            // still failed to send the message, it possibly means that the
            // remote node is dead. We should skip resend the message again.
            if (resend)
            {
                // SendMessage error return -1 to indicate the request needs
                // retry.
                if (res != nullptr)
                {
                    res->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
                }
                break;
            }

            std::lock_guard<std::mutex> lk(outbound_mux_);

            // If the stream version is -1, a separate thread has notified
            // the connecting thread to reconnect the stream. If the stream
            // version is greater than the previously-read one (stream_ver),
            // a new stream has been connected. In either case, the current
            // thread does not initiated a reconnection.
            if (stream_ver == stream_version.load(std::memory_order_acquire))
            {
                to_connect_nodes_.try_emplace(dest_node_id, stream_ver + 1);
                stream_version.store(-1, std::memory_order_release);
            }

            // put the failed message into the resend_message_list.
            auto resend_message_list =
                long_msg_resend_message_list_.try_emplace(
                    dest_node_id,
                    moodycamel::ConcurrentQueue<ResendScanSliceResp::Uptr>());
            resend_message_list.first->second.enqueue(
                std::make_unique<ResendScanSliceResp>(msg, res));

            // always wake up connector thread to either reconnect streams or
            // resend messages.
            out_cv_.notify_one();

            break;
        }
    }

    return error_code == 0;
}

/**
 * @brief Send a message to a node group leader. Failed message will
 * be put into a retry list once the stream to the node group leader is
 * reconstructed.
 *
 * @param node_group_id
 * @param msg
 * @param res
 * @param resend
 * @return true
 * @return false
 */
bool CcStreamSender::SendMessageToNg(uint32_t node_group_id,
                                     const CcMessage &msg,
                                     CcHandlerResultBase *res,
                                     bool resend)
{
    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(node_group_id);
    return SendMessageToNode(dest_node_id, msg, res, resend);
}

void CcStreamSender::AddRemoteNode(uint32_t node_id,
                                   const std::string &ip,
                                   uint16_t port)
{
    auto channel_it = outbound_channels_.try_emplace(node_id);
    channel_it.first->second.second = ip + ":" + std::to_string(port);

    auto stream_it = outbound_streams_.try_emplace(node_id);
    std::atomic<int64_t> &stream_version = stream_it.first->second.second;
    stream_version.store(-1, std::memory_order_release);

    stream_it = long_msg_outbound_streams_.try_emplace(node_id);
    std::atomic<int64_t> &long_msg_stream_version =
        stream_it.first->second.second;
    long_msg_stream_version.store(-1, std::memory_order_release);

    std::lock_guard<std::mutex> lk(outbound_mux_);
    to_connect_nodes_.emplace(node_id, 0);
    LOG(INFO) << "Emplace node " << channel_it.first->second.second;
    out_cv_.notify_one();
}

void CcStreamSender::NotifyConnectStream()
{
    std::lock_guard<std::mutex> lk(outbound_mux_);
    out_cv_.notify_one();
}

void CcStreamSender::ConnectStreams()
{
    using namespace std::chrono_literals;
    std::unique_lock<std::mutex> lk(outbound_mux_);
    while (!terminate_)
    {
        out_cv_.wait_for(lk, 1s, [this] { return terminate_; });

        if (terminate_)
        {
            break;
        }

        if (to_connect_nodes_.size() == 0)
        {
            continue;
        }

        std::vector<std::pair<uint32_t, int64_t>> nodes(
            to_connect_nodes_.begin(), to_connect_nodes_.end());

        for (const auto &[nid, version] : nodes)
        {
            lk.unlock();
            int err = ConnectStream(nid, version);
            lk.lock();

            if (err == 0)
            {
                LOG(INFO) << "Establish the cc stream to node "
                          << outbound_channels_.at(nid).second;
                // Resend failed messages to the reconnected node.
                if (resend_message_list_.find(nid) !=
                    resend_message_list_.end())
                {
                    // release lock before resend queued messages.
                    lk.unlock();
                    moodycamel::ConcurrentQueue<ResendMessage::Uptr>
                        &resend_message_list = resend_message_list_.at(nid);
                    while (!resend_message_list.is_empty())
                    {
                        ResendMessage::Uptr messages[100];
                        size_t msg_cnt =
                            resend_message_list.try_dequeue_bulk(messages, 100);
                        for (size_t i = 0; i < msg_cnt; ++i)
                        {
                            SendMessageToNode(nid,
                                              messages[i]->msg_,
                                              messages[i]->res_,
                                              true);
                        }
                    }
                    lk.lock();
                }
                if (long_msg_resend_message_list_.find(nid) !=
                    long_msg_resend_message_list_.end())
                {
                    // release lock before resend queued messages.
                    lk.unlock();
                    moodycamel::ConcurrentQueue<ResendScanSliceResp::Uptr>
                        &resend_message_list =
                            long_msg_resend_message_list_.at(nid);
                    while (!resend_message_list.is_empty())
                    {
                        ResendScanSliceResp::Uptr messages[100];
                        size_t msg_cnt =
                            resend_message_list.try_dequeue_bulk(messages, 100);
                        for (size_t i = 0; i < msg_cnt; ++i)
                        {
                            SendScanRespToNode(nid,
                                               messages[i]->msg_,
                                               messages[i]->res_,
                                               true);
                        }
                    }
                    lk.lock();
                }
            }
            else
            {
                LOG(ERROR) << "Fail to connect the cc stream to node "
                           << outbound_channels_.at(nid).second;
            }
        }
    }
}

int CcStreamSender::ConnectStream(uint32_t node_id, int64_t version)
{
    auto channel_it = outbound_channels_.find(node_id);
    brpc::Channel &channel = channel_it->second.first;
    std::string ip_addr = channel_it->second.second;

    auto stream_it = outbound_streams_.find(node_id);
    brpc::StreamId &stream_id = stream_it->second.first;
    std::atomic<int64_t> &stream_version = stream_it->second.second;

    brpc::ChannelOptions options;
    options.protocol = brpc::PROTOCOL_BAIDU_STD;
    options.timeout_ms = 100;
    options.max_retry = 3;
    int err = channel.Init(ip_addr.c_str(), &options);
    if (err != 0)
    {
        return err;
    }

    brpc::Controller cntl;
    txservice::remote::CcStreamService_Stub stub(&channel);

    err = brpc::StreamCreate(&stream_id, cntl, nullptr);
    if (err != 0)
    {
        return err;
    }

    txservice::remote::ConnectRequest request;
    txservice::remote::ConnectResponse response;
    request.set_message("Connect");
    request.set_type(remote::StreamType::RegularCcStream);
    stub.Connect(&cntl, &request, &response, nullptr);
    if (cntl.Failed())
    {
        return cntl.ErrorCode();
    }

    stream_it = long_msg_outbound_streams_.find(node_id);
    brpc::StreamId &long_msg_stream_id = stream_it->second.first;
    std::atomic<int64_t> &long_msg_stream_version = stream_it->second.second;

    brpc::ChannelOptions options2;
    options2.protocol = brpc::PROTOCOL_BAIDU_STD;
    options2.timeout_ms = 100;
    options2.max_retry = 3;
    err = channel.Init(ip_addr.c_str(), &options2);
    if (err != 0)
    {
        return err;
    }

    brpc::Controller long_msg_cntl;
    txservice::remote::CcStreamService_Stub long_msg_stub(&channel);

    err = brpc::StreamCreate(&long_msg_stream_id, long_msg_cntl, nullptr);
    if (err != 0)
    {
        return err;
    }

    txservice::remote::ConnectRequest long_msg_request;
    txservice::remote::ConnectResponse long_msg_response;
    long_msg_request.set_message("Connect");
    long_msg_request.set_type(remote::StreamType::LongMsgCcStream);
    long_msg_stub.Connect(
        &long_msg_cntl, &long_msg_request, &long_msg_response, nullptr);
    if (long_msg_cntl.Failed())
    {
        return long_msg_cntl.ErrorCode();
    }

    std::lock_guard<std::mutex> lk(outbound_mux_);
    to_connect_nodes_.erase(node_id);

    // One of them must be marked as disconnected (-1).
    assert(stream_version.load(std::memory_order_acquire) == -1 ||
           long_msg_stream_version.load(std::memory_order_acquire) == -1);
    stream_version.store(version, std::memory_order_release);
    long_msg_stream_version.store(version, std::memory_order_release);

    return 0;
}
}  // namespace remote
}  // namespace txservice
