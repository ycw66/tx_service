#include "remote/cc_stream_sender.h"

#include "sharder.h"

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

bool CcStreamSender::SendMessage(uint32_t node_group_id, const CcMessage &msg)
{
    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(node_group_id);

    auto stream_it = outbound_streams_.find(dest_node_id);

    if (stream_it == outbound_streams_.end())
    {
        LOG(ERROR) << "Trying to connect to an unknown remote node. Node Id: "
                   << dest_node_id;
        return false;
    }

    std::atomic<int64_t> &stream_version = stream_it->second.second;
    int64_t stream_ver = stream_version.load(std::memory_order_acquire);
    if (stream_ver < 0)
    {
        // The stream is invalid, when the stream version is less than 0.
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
                out_cv_.notify_one();
            }

            break;
        }
    }

    return error_code == 0;
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

    std::lock_guard<std::mutex> lk(outbound_mux_);
    to_connect_nodes_.emplace(node_id, 0);
    LOG(INFO) << "Emplace node " << channel_it.first->second.second;
    out_cv_.notify_one();
}

void CcStreamSender::ConnectStreams()
{
    std::unique_lock<std::mutex> lk(outbound_mux_);
    while (!terminate_)
    {
        out_cv_.wait(
            lk, [this] { return to_connect_nodes_.size() != 0 || terminate_; });

        if (terminate_)
        {
            break;
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
            }
            else
            {
                LOG(ERROR) << "Fail to connect the cc stream to node "
                           << outbound_channels_.at(nid).second;
            }
        }

        if (to_connect_nodes_.size() > 0)
        {
            using namespace std::chrono_literals;
            lk.unlock();
            std::this_thread::sleep_for(5s);
            lk.lock();
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
    stub.Connect(&cntl, &request, &response, nullptr);
    if (cntl.Failed())
    {
        return cntl.ErrorCode();
    }

    std::lock_guard<std::mutex> lk(outbound_mux_);
    to_connect_nodes_.erase(node_id);
    assert(stream_version.load(std::memory_order_acquire) == -1);
    stream_version.store(version, std::memory_order_release);

    return 0;
}
}  // namespace remote
}  // namespace txservice