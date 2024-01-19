#include "remote/cc_stream_sender.h"

#include <arpa/inet.h>
#include <netdb.h>  // getaddrinfo

#include <chrono>
#include <string>
#include <unordered_set>

#include "braft/util.h"
#include "sharder.h"
#include "tx_trace.h"

namespace txservice
{
namespace remote
{
CcStreamSender::~CcStreamSender()
{
    {
        std::unique_lock<std::mutex> lk(to_connect_mux_);
        terminate_.store(true, std::memory_order_release);
    }
    to_connect_cv_.notify_one();
    connect_thd_.join();
}

CcStreamSender::CcStreamSender(
    moodycamel::ConcurrentQueue<std::unique_ptr<CcMessage>> &msg_pool)
    : msg_pool_(msg_pool), terminate_(false), to_connect_flag_(false)
{
    stream_write_options_.write_in_background = true;
    connect_thd_ = std::thread([this] { ConnectStreams(); });
}

void CcStreamSender::RecycleCcMsg(std::unique_ptr<CcMessage> msg)
{
    msg_pool_.enqueue(std::move(msg));
}

void CcStreamSender::ReConnectStream(uint32_t dest_node_id)
{
    std::shared_lock<std::shared_mutex> outbound_lk(outbound_mux_);
    auto stream_it = outbound_streams_.find(dest_node_id);
    if (stream_it == outbound_streams_.end())
    {
        return;
    }

    std::atomic<int64_t> &stream_version = std::get<1>(stream_it->second);
    int64_t stream_ver = stream_version.load(std::memory_order_acquire);

    std::lock_guard<std::mutex> lk(to_connect_mux_);
    // If the stream version is -1, a separate thread has notified
    // the connecting thread to reconnect the stream. If the stream
    // version is greater than the previously-read one (stream_ver),
    // a new stream has been connected. In either case, the current
    // thread does not initiated a reconnection.
    if (stream_version.compare_exchange_strong(
            stream_ver, -1, std::memory_order_release))
    {
        to_connect_regular_streams_.try_emplace(dest_node_id, stream_ver + 1);
    }
}

void CcStreamSender::ReConnectLongMsgStream(uint32_t dest_node_id)
{
    std::shared_lock<std::shared_mutex> outbound_lk(outbound_mux_);
    auto stream_it = long_msg_outbound_streams_.find(dest_node_id);
    if (stream_it == long_msg_outbound_streams_.end())
    {
        return;
    }

    std::atomic<int64_t> &stream_version = std::get<1>(stream_it->second);
    int64_t stream_ver = stream_version.load(std::memory_order_acquire);

    std::lock_guard<std::mutex> lk(to_connect_mux_);
    // If the stream version is -1, a separate thread has notified
    // the connecting thread to reconnect the stream. If the stream
    // version is greater than the previously-read one (stream_ver),
    // a new stream has been connected. In either case, the current
    // thread does not initiated a reconnection.
    if (stream_version.compare_exchange_strong(
            stream_ver, -1, std::memory_order_release))
    {
        to_connect_long_msg_streams_.try_emplace(dest_node_id, stream_ver + 1);
    }
}

bool CcStreamSender::UpdateStreamIP(uint32_t node_id,
                                    const std::string &new_connection_ip)
{
    std::shared_lock<std::shared_mutex> outbound_lk(outbound_mux_);
    auto stream_it = outbound_streams_.find(node_id);
    if (stream_it == outbound_streams_.end())
    {
        return false;
    }

    std::string &stream_ip = std::get<2>(stream_it->second);
    if (stream_ip.empty())
    {
        // set stream ip with the ip of the first connection.
        std::get<2>(stream_it->second) = new_connection_ip;
        return false;
    }
    else if (stream_ip != new_connection_ip)
    {
        // return true to indicate ip changed
        std::get<2>(stream_it->second) = new_connection_ip;
        return true;
    }

    return false;
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
                                       bool resend,
                                       bool log_verbose)
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
    if (log_verbose)
    {
        LOG(INFO) << "SendMessageToNode " << dest_node_id;
    }
    std::shared_lock<std::shared_mutex> outbound_lk(outbound_mux_);
    auto stream_it = outbound_streams_.find(dest_node_id);
    if (stream_it == outbound_streams_.end())
    {
        // SendMessage error return -1 to indicate the request needs retry.
        if (res != nullptr && res->SetResultByStreamThread())
        {
            res->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
        }

        LOG(ERROR) << "Trying to connect to an unknown remote node. Node Id: "
                   << dest_node_id;
        return false;
    }

    std::atomic<int64_t> &stream_version = std::get<1>(stream_it->second);
    int64_t stream_ver = stream_version.load(std::memory_order_acquire);
    if (stream_ver == -1)
    {
        // resend the message if stream is connecting
        std::lock_guard<std::mutex> lk(to_connect_mux_);
        if (log_verbose)
        {
            LOG(INFO) << "CC stream is connecting, buffer the message for "
                         "resend";
        }
        auto resend_message_list = resend_message_list_.try_emplace(
            dest_node_id, moodycamel::ConcurrentQueue<ResendMessage::Uptr>());
        resend_message_list.first->second.enqueue(
            std::make_unique<ResendMessage>(msg, res));

        // always wake up connector thread to either reconnect streams or
        // resend messages.
        to_connect_flag_.store(true, std::memory_order_release);
        to_connect_cv_.notify_one();
        return true;
    }

    brpc::StreamId stream_id = std::get<0>(stream_it->second);

    butil::IOBuf iobuf;
    butil::IOBufAsZeroCopyOutputStream wrapper(&iobuf);
    msg.SerializeToZeroCopyStream(&wrapper);

    int error_code =
        brpc::StreamWrite(stream_id, iobuf, &stream_write_options_);
    if (log_verbose)
    {
        LOG(INFO) << "cc_stream_sender: do stream write with stream id: "
                  << stream_id << " dest_node_id: " << dest_node_id
                  << " print response error code: " << error_code;
    }
    while (error_code != 0)
    {
        if (error_code == EAGAIN)
        {
            error_code =
                brpc::StreamWrite(stream_id, iobuf, &stream_write_options_);
            if (log_verbose)
            {
                LOG(INFO) << "cc_stream_sender: do stream write again "
                             "with stream id: "
                          << stream_id
                          << " print response error code: " << error_code;
            }
        }
        else
        {
            // for resend message, we have already reconnect the stream, if it
            // still failed to send the message, it possibly means that the
            // remote node is dead. We should skip resend the message again.
            if (resend)
            {
                if (log_verbose)
                {
                    LOG(INFO) << "cc_stream_sender: resend message and break";
                }
                // SendMessage error return -1 to indicate the request needs
                // retry.
                if (res != nullptr && res->SetResultByStreamThread())
                {
                    res->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
                }
                break;
            }
            if (log_verbose)
            {
                LOG(INFO) << "cc_stream_sender: check stream version: "
                          << stream_ver << " and need resend message";
            }
            std::lock_guard<std::mutex> lk(to_connect_mux_);
            // If the stream version is -1, a separate thread has notified
            // the connecting thread to reconnect the stream. If the stream
            // version is greater than the previously-read one (stream_ver),
            // a new stream has been connected. In either case, the current
            // thread does not initiated a reconnection.
            if (stream_version.compare_exchange_strong(
                    stream_ver, -1, std::memory_order_release))
            {
                to_connect_regular_streams_.try_emplace(dest_node_id,
                                                        stream_ver + 1);
            }
            // put the failed message into the resend_message_list.
            auto resend_message_list = resend_message_list_.try_emplace(
                dest_node_id,
                moodycamel::ConcurrentQueue<ResendMessage::Uptr>());
            resend_message_list.first->second.enqueue(
                std::make_unique<ResendMessage>(msg, res));

            // always wake up connector thread to either reconnect streams
            // or resend messages.
            to_connect_flag_.store(true, std::memory_order_release);
            to_connect_cv_.notify_one();
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

    std::shared_lock<std::shared_mutex> outbound_lk(outbound_mux_);
    auto stream_it = long_msg_outbound_streams_.find(dest_node_id);
    if (stream_it == long_msg_outbound_streams_.end())
    {
        // SendMessage error return -1 to indicate the request needs retry.
        if (res != nullptr && res->SetResultByStreamThread())
        {
            res->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
        }

        LOG(ERROR) << "Trying to connect to an unknown remote node. Node Id: "
                   << dest_node_id;
        return false;
    }

    std::atomic<int64_t> &stream_version = std::get<1>(stream_it->second);
    int64_t stream_ver = stream_version.load(std::memory_order_acquire);
    if (stream_ver == -1)
    {
        // resend the message if stream is connecting
        std::lock_guard<std::mutex> lk(to_connect_mux_);
        DLOG(INFO) << "CC stream is connecting, buffer the message for resend";

        auto resend_message_list = long_msg_resend_message_list_.try_emplace(
            dest_node_id,
            moodycamel::ConcurrentQueue<ResendScanSliceResp::Uptr>());
        resend_message_list.first->second.enqueue(
            std::make_unique<ResendScanSliceResp>(msg, res));

        // always wake up connector thread to either reconnect streams or
        // resend messages.
        to_connect_flag_.store(true, std::memory_order_release);
        to_connect_cv_.notify_one();
        return true;
    }

    brpc::StreamId &stream_id = std::get<0>(stream_it->second);

    butil::IOBuf iobuf;
    butil::IOBufAsZeroCopyOutputStream wrapper(&iobuf);
    msg.SerializeToZeroCopyStream(&wrapper);

    int error_code =
        brpc::StreamWrite(stream_id, iobuf, &stream_write_options_);
    while (error_code != 0)
    {
        if (error_code == EAGAIN)
        {
            error_code =
                brpc::StreamWrite(stream_id, iobuf, &stream_write_options_);
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
                if (res != nullptr && res->SetResultByStreamThread())
                {
                    res->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
                }
                break;
            }

            std::lock_guard<std::mutex> lk(to_connect_mux_);
            // If the stream version is -1, a separate thread has notified
            // the connecting thread to reconnect the stream. If the stream
            // version is greater than the previously-read one (stream_ver),
            // a new stream has been connected. In either case, the current
            // thread does not initiated a reconnection.
            if (stream_version.compare_exchange_strong(
                    stream_ver, -1, std::memory_order_release))
            {
                to_connect_long_msg_streams_.try_emplace(dest_node_id,
                                                         stream_ver + 1);
            }
            // put the failed message into the long_msg_resend_message_list.
            auto resend_message_list =
                long_msg_resend_message_list_.try_emplace(
                    dest_node_id,
                    moodycamel::ConcurrentQueue<ResendScanSliceResp::Uptr>());
            resend_message_list.first->second.enqueue(
                std::make_unique<ResendScanSliceResp>(msg, res));

            // wake up connector thread to reconnect streams
            to_connect_flag_.store(true, std::memory_order_release);
            to_connect_cv_.notify_one();

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

void CcStreamSender::UpdateRemoteNodes(
    const std::unordered_map<NodeGroupId, std::vector<NodeConfig>> &ng_config)
{
    std::unique_lock<std::shared_mutex> lk(outbound_mux_);

    for (auto &[node_id, config] : ng_config)
    {
        // Connect to new nodes in new cluster configs.
        if (outbound_channels_.find(node_id) == outbound_channels_.end())
        {
            auto channel_it = outbound_channels_.try_emplace(node_id);
            const std::string &ip = config.front().host_name_;
            uint16_t port = config.front().port_;
            channel_it.first->second = ip + ":" + std::to_string(port);
            auto stream_it = outbound_streams_.try_emplace(node_id);
            std::get<0>(stream_it.first->second) = brpc::INVALID_STREAM_ID;
            std::get<1>(stream_it.first->second)
                .store(-1, std::memory_order_release);

            stream_it = long_msg_outbound_streams_.try_emplace(node_id);
            std::get<0>(stream_it.first->second) = brpc::INVALID_STREAM_ID;
            std::get<1>(stream_it.first->second)
                .store(-1, std::memory_order_release);

            {
                // Add it to the reconnect lists, the connect_thd_ will connect
                // to these nodes later.
                std::unique_lock<std::mutex> to_connect_lk(to_connect_mux_);
                to_connect_regular_streams_.try_emplace(node_id, 0);
                to_connect_long_msg_streams_.try_emplace(node_id, 0);
            }
        }
    }

    std::unordered_set<uint32_t> removed_nodes;
    for (auto &[node_id, channel] : outbound_channels_)
    {
        // node group is no longer in the new cluster config.
        if (ng_config.find(node_id) == ng_config.end())
        {
            {
                std::unique_lock<std::mutex> to_connect_lk(to_connect_mux_);
                to_connect_regular_streams_.erase(node_id);
                to_connect_long_msg_streams_.erase(node_id);
                resend_message_list_.erase(node_id);
                long_msg_resend_message_list_.erase(node_id);
            }
            LOG(INFO) << "Closed cc stream to node " << node_id;
            brpc::StreamClose(std::get<0>(outbound_streams_.at(node_id)));
            outbound_streams_.erase(node_id);
            brpc::StreamClose(
                std::get<0>(long_msg_outbound_streams_.at(node_id)));
            long_msg_outbound_streams_.erase(node_id);
            removed_nodes.insert(node_id);
        }
    }
    for (auto nid : removed_nodes)
    {
        outbound_channels_.erase(nid);
    }
}

void CcStreamSender::NotifyConnectStream()
{
    to_connect_flag_.store(true, std::memory_order_release);
    to_connect_cv_.notify_one();
}

void CcStreamSender::ConnectStreams()
{
    using namespace std::chrono_literals;
    std::unique_lock<std::mutex> lk(to_connect_mux_);
    while (!terminate_.load(std::memory_order_acquire))
    {
        to_connect_cv_.wait_for(
            lk,
            1s,
            [this]
            {
                return terminate_.load(std::memory_order_acquire) ||
                       to_connect_flag_.load(std::memory_order_acquire);
            });

        if (terminate_.load(std::memory_order_acquire))
        {
            break;
        }

        to_connect_flag_.store(false, std::memory_order_release);

        if (to_connect_regular_streams_.size() == 0 &&
            to_connect_long_msg_streams_.size() == 0)
        {
            continue;
        }

        std::vector<std::pair<uint32_t, int64_t>> regular_streams(
            to_connect_regular_streams_.begin(),
            to_connect_regular_streams_.end());

        for (const auto &[nid, version] : regular_streams)
        {
            lk.unlock();
            int err = ConnectStream(nid, version);
            lk.lock();

            if (err == 0)
            {
                LOG(INFO) << "Establish the cc stream to node " << nid;

                // Resend failed messages to the reconnected node.
                auto message_list_it = resend_message_list_.find(nid);
                while (message_list_it != resend_message_list_.end() &&
                       !message_list_it->second.is_empty())
                {
                    ResendMessage::Uptr messages[100];
                    size_t msg_cnt =
                        message_list_it->second.try_dequeue_bulk(messages, 100);

                    // release lock before resend queued messages.
                    lk.unlock();
                    for (size_t i = 0; i < msg_cnt; ++i)
                    {
                        SendMessageToNode(
                            nid, messages[i]->msg_, messages[i]->res_, true);
                    }
                    lk.lock();
                    // Update the message list since the node might be
                    // removed when sending the mssages.
                    message_list_it = resend_message_list_.find(nid);
                }
            }
            else
            {
                LOG(ERROR) << "Fail to connect the cc stream to node " << nid;
            }
        }

        std::vector<std::pair<uint32_t, int64_t>> long_msg_streams(
            to_connect_long_msg_streams_.begin(),
            to_connect_long_msg_streams_.end());
        for (const auto &[nid, version] : long_msg_streams)
        {
            lk.unlock();
            int err = ConnectLongMsgStream(nid, version);
            lk.lock();

            if (err == 0)
            {
                LOG(INFO) << "Establish the long msg cc stream to node " << nid;

                auto message_list_it = long_msg_resend_message_list_.find(nid);
                while (message_list_it != long_msg_resend_message_list_.end() &&
                       !message_list_it->second.is_empty())
                {
                    ResendScanSliceResp::Uptr messages[100];
                    size_t msg_cnt =
                        message_list_it->second.try_dequeue_bulk(messages, 100);

                    // release lock before resend queued messages.
                    lk.unlock();
                    for (size_t i = 0; i < msg_cnt; ++i)
                    {
                        SendScanRespToNode(
                            nid, messages[i]->msg_, messages[i]->res_, true);
                    }
                    lk.lock();
                    // Update the message list since the node might be
                    // removed when sending the mssages.
                    message_list_it = long_msg_resend_message_list_.find(nid);
                }
            }
            else
            {
                LOG(ERROR) << "Fail to connect the long msg cc stream to node "
                           << nid;
            }
        }
    }
}

int CcStreamSender::ConnectStream(uint32_t node_id, int64_t version)
{
    std::shared_lock<std::shared_mutex> outbound_lk(outbound_mux_);
    auto channel_it = outbound_channels_.find(node_id);
    if (channel_it == outbound_channels_.end())
    {
        // Connecting to an unkown node.
        std::lock_guard<std::mutex> lk(to_connect_mux_);
        to_connect_regular_streams_.erase(node_id);
        return -1;
    }
    brpc::Channel channel;
    std::string ip_addr = channel_it->second;

    brpc::ChannelOptions options;
    options.protocol = brpc::PROTOCOL_BAIDU_STD;
    options.timeout_ms = 100;
    options.max_retry = 3;
    size_t comma_pos = ip_addr.find(':');
    assert(comma_pos != std::string::npos);
    std::string node_ip_str = ip_addr.substr(0, comma_pos);
    uint16_t node_port = std::stoi(ip_addr.substr(comma_pos + 1));
    butil::ip_t ip_t;
    int err;
    std::string node_ip;
    if (0 != butil::str2ip(node_ip_str.c_str(), &ip_t))
    {
        // for case `node_ip_str` is hostname format.
        std::string naming_service_url;
        braft::HostNameAddr hostname_addr(node_ip_str, node_port);
        braft::HostNameAddr2NSUrl(hostname_addr, naming_service_url);
        err = channel.Init(
            naming_service_url.c_str(), braft::LOAD_BALANCER_NAME, &options);
        if (err != 0)
        {
            return err;
        }
        // Get IP address.
        char ip_str[INET_ADDRSTRLEN];
        struct addrinfo hints, *addrs;
        memset(&hints, 0, sizeof(hints));
        hints.ai_family = AF_INET;
        hints.ai_socktype = SOCK_STREAM;
        hints.ai_protocol = IPPROTO_TCP;
        err = getaddrinfo(node_ip_str.c_str(), NULL, &hints, &addrs);
        if (err != 0)
        {
            LOG(ERROR) << "GetAddrInfo error: " << gai_strerror(err);
            return err;
        }
        for (struct addrinfo *item = addrs; item != NULL; item = item->ai_next)
        {
            void *addr;
            // get pointer to the address itself, different fields in IPv4 and
            // IPv6
            if (item->ai_family == AF_INET)
            {
                // address is IPv4
                struct sockaddr_in *ipv4 = (struct sockaddr_in *) item->ai_addr;
                addr = &(ipv4->sin_addr);
            }
            else
            {
                // address is IPv6
                struct sockaddr_in6 *ipv6 =
                    (struct sockaddr_in6 *) item->ai_addr;
                addr = &(ipv6->sin6_addr);
            }

            // convert IP to a string
            inet_ntop(item->ai_family, addr, ip_str, INET_ADDRSTRLEN);
            break;
        }
        freeaddrinfo(addrs);
        node_ip.append(ip_str);
    }
    else
    {
        err = channel.Init(ip_addr.c_str(), &options);
        if (err != 0)
        {
            return err;
        }
        node_ip.append(node_ip_str);
    }

    auto stream_it = outbound_streams_.find(node_id);
    brpc::StreamId &stream_id = std::get<0>(stream_it->second);
    if (stream_id != brpc::INVALID_STREAM_ID)
    {
        brpc::StreamClose(stream_id);
    }
    std::atomic<int64_t> &stream_version = std::get<1>(stream_it->second);
    assert(stream_version.load() == -1);

    txservice::remote::CcStreamService_Stub stub(&channel);
    brpc::Controller cntl;
    err = brpc::StreamCreate(&stream_id, cntl, nullptr);
    if (err != 0)
    {
        return err;
    }

    txservice::remote::ConnectRequest request;
    txservice::remote::ConnectResponse response;
    request.set_message("Connect");
    request.set_type(remote::StreamType::RegularCcStream);

    request.set_node_id(Sharder::Instance().NodeId());
    request.set_node_ip(node_ip);
    stub.Connect(&cntl, &request, &response, nullptr);
    if (cntl.Failed())
    {
        return cntl.ErrorCode();
    }
    stream_version.store(version, std::memory_order_release);

    std::lock_guard<std::mutex> lk(to_connect_mux_);
    to_connect_regular_streams_.erase(node_id);
    return 0;
}

int CcStreamSender::ConnectLongMsgStream(uint32_t node_id, int64_t version)
{
    std::shared_lock<std::shared_mutex> outbound_lk(outbound_mux_);
    auto channel_it = outbound_channels_.find(node_id);
    if (channel_it == outbound_channels_.end())
    {
        // Connecting to an unkown node.
        long_msg_resend_message_list_.erase(node_id);
        return -1;
    }

    brpc::Channel channel;
    std::string ip_addr = channel_it->second;
    auto stream_it = long_msg_outbound_streams_.find(node_id);
    brpc::StreamId &long_msg_stream_id = std::get<0>(stream_it->second);
    if (long_msg_stream_id != brpc::INVALID_STREAM_ID)
    {
        brpc::StreamClose(long_msg_stream_id);
    }
    std::atomic<int64_t> &long_msg_stream_version =
        std::get<1>(stream_it->second);
    assert(long_msg_stream_version.load() == -1);

    brpc::ChannelOptions options;
    options.protocol = brpc::PROTOCOL_BAIDU_STD;
    options.timeout_ms = 100;
    options.max_retry = 3;
    size_t comma_pos = ip_addr.find(':');
    assert(comma_pos != std::string::npos);
    std::string node_ip_str = ip_addr.substr(0, comma_pos);
    uint16_t node_port = std::stoi(ip_addr.substr(comma_pos + 1));
    butil::ip_t ip_t;
    int err;
    if (0 != butil::str2ip(node_ip_str.c_str(), &ip_t))
    {
        // for case `node_ip_str` is hostname format.
        std::string naming_service_url;
        braft::HostNameAddr hostname_addr(node_ip_str, node_port);
        braft::HostNameAddr2NSUrl(hostname_addr, naming_service_url);
        err = channel.Init(
            naming_service_url.c_str(), braft::LOAD_BALANCER_NAME, &options);
    }
    else
    {
        err = channel.Init(ip_addr.c_str(), &options);
    }
    if (err != 0)
    {
        return err;
    }

    txservice::remote::CcStreamService_Stub stub(&channel);
    brpc::Controller long_msg_cntl;

    err = brpc::StreamCreate(&long_msg_stream_id, long_msg_cntl, nullptr);
    if (err != 0)
    {
        return err;
    }

    txservice::remote::ConnectRequest long_msg_request;
    txservice::remote::ConnectResponse long_msg_response;
    long_msg_request.set_message("Connect");
    long_msg_request.set_type(remote::StreamType::LongMsgCcStream);
    stub.Connect(
        &long_msg_cntl, &long_msg_request, &long_msg_response, nullptr);
    if (long_msg_cntl.Failed())
    {
        return long_msg_cntl.ErrorCode();
    }
    long_msg_stream_version.store(version, std::memory_order_release);

    std::lock_guard<std::mutex> lk(to_connect_mux_);
    to_connect_long_msg_streams_.erase(node_id);
    return 0;
}
}  // namespace remote
}  // namespace txservice
