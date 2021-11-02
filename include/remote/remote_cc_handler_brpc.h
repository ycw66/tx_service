#pragma once

#include <brpc/channel.h>
#include <brpc/server.h>
#include <brpc/stream.h>
#include <bthread/bthread.h>

#include <condition_variable>
#include <mutex>
#include <queue>
#include <unordered_map>
#include <unordered_set>

#include "proto/cc_request.pb.h"
#include "remote_cc_handler.h"

namespace txservice
{
namespace remote
{
class RemoteCcHandler_Brpc : public RemoteCcHandler,
                             public brpc::StreamInputHandler,
                             public txservice::remote::CcService
{
public:
    RemoteCcHandler_Brpc() = delete;

    RemoteCcHandler_Brpc(LocalCcShards *local_shards, int port = 8000)
        : RemoteCcHandler(local_shards),
          server_(),
          inbound_mux_(),
          outbound_mux_(),
          out_cv_(),
          outbound_channels_(),
          outbound_streams_(),
          to_connect_nodes_(),
          inbound_streams_(),
          terminate_(false)
    {
        if (server_.AddService(this, brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
        {
            LOG(ERROR) << "Fail to add the remote cc service.";
        }

        brpc::ServerOptions options;
        options.idle_timeout_sec = -1;
        int error_code = server_.Start(port, &options);
        if (error_code != 0)
        {
            LOG(ERROR) << "Fail to start the remote cc service. Error code: "
                       << error_code;
        }

        connect_thd_ = std::thread([this] { ConnectStreams(); });
    }

    virtual ~RemoteCcHandler_Brpc()
    {
        {
            std::lock_guard<std::mutex> lk(outbound_mux_);
            terminate_ = true;
        }
        out_cv_.notify_one();
        connect_thd_.join();

        for (const auto &connect_pair : to_connect_nodes_)
        {
            outbound_channels_.erase(connect_pair.first);
            outbound_channels_.erase(connect_pair.first);
        }

        for (auto &[node_id, stream_pair] : outbound_streams_)
        {
            if (node_id == local_shards_->NodeId() ||
                stream_pair.second.load(std::memory_order_acquire) < 0)
            {
                continue;
            }

            std::unique_ptr<CcMessage> close_msg = GetCcMsg();
            close_msg->set_type(
                CcMessage_MessageType::CcMessage_MessageType_Shutdown);

            butil::IOBuf iobuf;
            butil::IOBufAsZeroCopyOutputStream wrapper(&iobuf);
            close_msg->SerializeToZeroCopyStream(&wrapper);

            int error_code = brpc::StreamWrite(stream_pair.first, iobuf);
            size_t retry = 0;
            while (error_code != 0 && retry <= 3)
            {
                if (error_code == EAGAIN)
                {
                    error_code = brpc::StreamWrite(stream_pair.first, iobuf);
                    ++retry;
                }
                else
                {
                    break;
                }
            }
        }

        {
            std::lock_guard<std::mutex> guard(inbound_mux_);
            for (auto &stream_id : inbound_streams_)
            {
                brpc::StreamClose(stream_id);
            }
        }

        server_.Stop(0);
        server_.Join();
    }

    void AddNode(uint32_t node_id, const std::string &ip, uint16_t port)
    {
        // std::unique_lock<std::mutex> lk(mux_);

        if (node_id == local_shards_->NodeId())
        {
            return;
        }

        auto channel_it = outbound_channels_.try_emplace(node_id);
        // brpc::Channel &channel = channel_it.first->second.first;
        channel_it.first->second.second = ip + ":" + std::to_string(port);

        auto stream_it = outbound_streams_.try_emplace(node_id);
        // brpc::StreamId &stream_id = stream_it.first->second;
        std::atomic<int64_t> &stream_version = stream_it.first->second.second;
        stream_version.store(-1, std::memory_order_release);

        std::lock_guard<std::mutex> lk(outbound_mux_);
        to_connect_nodes_.emplace(node_id, 0);
        out_cv_.notify_one();
    }

    bool SendRequest(uint32_t node_group_id, const CcMessage &msg) override
    {
        uint32_t dest_node_id = Sharder::Instance().NodeId(node_group_id);
        assert(dest_node_id != local_shards_->NodeId());

        return SendMessage(dest_node_id, msg);
    }

    bool SendResponse(uint32_t node_id, const CcMessage &msg) override
    {
        return SendMessage(node_id, msg);
    }

    bool SendMessage(uint32_t node_id, const remote::CcMessage &msg)
    {
        auto stream_it = outbound_streams_.find(node_id);

        if (stream_it == outbound_streams_.end())
        {
            LOG(ERROR)
                << "Trying to connect to an unknown remote node. Node Id: "
                << node_id;
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

                // If the stream version is -1, a separte thread has notified
                // the connecting thread to reconnect the stream. If the stream
                // version is greater than the previously-read one (stream_ver),
                // a new stream has been connected. In either case, the current
                // thread does not initiated a reconnection.
                if (stream_ver ==
                    stream_version.load(std::memory_order_acquire))
                {
                    to_connect_nodes_.try_emplace(node_id, stream_ver + 1);
                    stream_version.store(-1, std::memory_order_release);
                    out_cv_.notify_one();
                }

                break;
            }
        }

        return error_code == 0;
    }

    void Connect(::google::protobuf::RpcController *controller,
                 const ConnectRequest *request,
                 ConnectResponse *response,
                 ::google::protobuf::Closure *done) override
    {
        brpc::StreamId stream_socket;

        // This object helps you to call done->Run() in RAII style. If you need
        // to process the request asynchronously, pass done_guard.release().
        brpc::ClosureGuard done_guard(done);

        brpc::Controller *cntl = static_cast<brpc::Controller *>(controller);

        brpc::StreamOptions stream_options;
        stream_options.max_buf_size = 0;
        stream_options.handler = this;
        if (brpc::StreamAccept(&stream_socket, *cntl, &stream_options) != 0)
        {
            cntl->SetFailed("Fail to accept stream");
            return;
        }

        response->set_message("Accepted");

        std::lock_guard<std::mutex> guard(inbound_mux_);
        inbound_streams_.emplace(stream_socket);
    }

    void Transfer(::google::protobuf::RpcController *controller,
                  const TransferRequest *request,
                  TransferResponse *response,
                  ::google::protobuf::Closure *done) override
    {
        // This object helps you to call done->Run() in RAII style. If you need
        // to process the request asynchronously, pass done_guard.release().
        brpc::ClosureGuard done_guard(done);

        uint32_t ng_id = request->ng_id();
        int err = Sharder::Instance().TransferLeader(ng_id);

        response->set_error(err != 0);
    }

    int on_received_messages(brpc::StreamId stream_id,
                             butil::IOBuf *const messages[],
                             size_t size) override
    {
        for (size_t i = 0; i < size; ++i)
        {
            std::unique_ptr<CcMessage> cc_msg = GetCcMsg();

            butil::IOBufAsZeroCopyInputStream wrapper(*messages[i]);
            cc_msg->ParseFromZeroCopyStream(&wrapper);

            if (cc_msg->type() ==
                CcMessage_MessageType::CcMessage_MessageType_Shutdown)
            {
                {
                    std::lock_guard<std::mutex> guard(inbound_mux_);
                    inbound_streams_.erase(stream_id);
                }
                brpc::StreamClose(stream_id);
            }
            else
            {
                OnReceiveCcMsg(std::move(cc_msg));
            }
        }
        return 0;
    }

    void on_idle_timeout(brpc::StreamId stream) override
    {
    }

    void on_closed(brpc::StreamId stream) override
    {
    }

private:
    void ConnectStreams()
    {
        std::unique_lock<std::mutex> lk(outbound_mux_);
        while (!terminate_)
        {
            out_cv_.wait(
                lk,
                [this] { return to_connect_nodes_.size() != 0 || terminate_; });

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

    int ConnectStream(uint32_t node_id, int64_t version)
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
        txservice::remote::CcService_Stub stub(&channel);

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

    brpc::Server server_;
    std::mutex inbound_mux_;
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

    std::unordered_set<brpc::StreamId> inbound_streams_;

    /// <summary>
    /// The background thread that establishes cc streams to remote nodes.
    /// </summary>
    std::thread connect_thd_;
    bool terminate_;

    friend class RemoteHandler;
};

}  // namespace remote

}  // namespace txservice
