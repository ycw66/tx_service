#pragma once

#include <brpc/channel.h>
#include <brpc/server.h>
#include <brpc/stream.h>

#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "proto/cc_request.pb.h"
#include "raft_log.pb.h"
#include "txlog.h"

namespace txservice
{
class LocalCcShards;

struct ReplayLogCc;

namespace fault
{
class CcNode;

struct ReplayLogInfo
{
    ReplayLogInfo() = default;

    ReplayLogInfo(uint32_t cc_ng_id, int64_t cc_ng_term, int log_group)
        : cc_ng_id_(cc_ng_id), cc_ng_term_(cc_ng_term), log_group_(log_group)
    {
    }

    uint32_t cc_ng_id_;
    int64_t cc_ng_term_;
    int log_group_;
};

struct RecoverTxInfo
{
    RecoverTxInfo() = default;

    RecoverTxInfo(uint64_t tx_number,
                  int64_t tx_term,
                  uint32_t cc_ng_id,
                  int64_t cc_ng_term)
        : tx_number_(tx_number),
          tx_term_(tx_term),
          cc_ng_id_(cc_ng_id),
          cc_ng_term_(cc_ng_term)
    {
    }

    // The number of tx who holds the intention/lock.
    uint64_t tx_number_;
    // The term of the cc node group in which the tx resides when the tx
    // acquires the intention/lock.
    int64_t tx_term_;
    // The ID of the cc node group in which the lock/intention resides.
    uint32_t cc_ng_id_;
    // The term of the cc node group in which the lock/intention resides.
    int64_t cc_ng_term_;
};

class ReplayService : public brpc::StreamInputHandler,
                      public ::txlog::LogReplayService
{
public:
    ReplayService() = delete;
    ReplayService(LocalCcShards &local_shards,
                  TxLog *log_agent,
                  std::string ip,
                  uint16_t port);
    ~ReplayService() = default;

    void Shutdown();

    void Connect(::google::protobuf::RpcController *controller,
                 const ::txlog::LogReplayConnectRequest *request,
                 ::txlog::LogReplayConnectResponse *response,
                 ::google::protobuf::Closure *done) override;

    void UpdateLogGroupLeader(::google::protobuf::RpcController *controller,
                              const ::txlog::LogLeaderUpdateRequest *request,
                              ::txlog::LogLeaderUpdateResponse *response,
                              ::google::protobuf::Closure *done) override;

    /**
     * Send ReplayLogRequest to log groups. If log_group is negative, send to
     * all log groups, else just the specified log group.
     * @param cc_ng_id
     * @param cc_ng_term
     * @param log_group
     */
    void ReplayLog(uint32_t cc_ng_id, int64_t cc_ng_term, int log_group = -1);

    void RecoverTx(uint64_t tx_number,
                   int64_t tx_term,
                   uint32_t cc_ng_id,
                   int64_t cc_ng_term);

    int on_received_messages(brpc::StreamId stream_id,
                             butil::IOBuf *const messages[],
                             size_t size) override;

    void on_idle_timeout(brpc::StreamId id) override;

    void on_closed(brpc::StreamId id) override;

private:
    struct ConnectionInfo
    {
        ConnectionInfo() = default;
        ConnectionInfo(uint32_t lg_id,
                       uint32_t cc_ng_id,
                       int64_t cc_ng_term,
                       bool recovering)
            : log_group_id_(lg_id),
              cc_ng_id_(cc_ng_id),
              cc_ng_term_(cc_ng_term),
              recovering_(recovering)
        {
        }

        uint32_t log_group_id_;
        uint32_t cc_ng_id_;
        int64_t cc_ng_term_;
        bool recovering_;
    };

    LocalCcShards &local_shards_;
    // Each ConnectionInfo is uniquely identified by <cc_ng_id, log_group_id>
    // pair. For each <cc_ng_id, log_group_id> pair, even if a stream is closed,
    // the ConnectionInfo remains, until replaced by a new stream connection.
    std::unordered_map<brpc::StreamId, ConnectionInfo> inbound_connections_;
    int active_stream_cnt_ = 0;
    std::mutex inbound_mux_;
    std::condition_variable inbound_cv_;

    void WaitAndClearRequests(
        std::vector<std::unique_ptr<ReplayLogCc>> &cc_req_vec,
        std::mutex &mux,
        std::condition_variable &cv,
        uint32_t &finish_log_cnt);
    static const int timeout_ms_ = 2000;
    // to resend ReplayLogRequest on stream timeout
    TxLog *log_agent_;

    std::thread notify_thread_;
    std::deque<ReplayLogInfo> replay_log_queue_;
    std::deque<RecoverTxInfo> recover_tx_queue_;
    std::mutex queue_mux_;
    std::condition_variable queue_cv_;
    std::atomic<bool> finish_;

    // ip and port of log replay server of this node
    std::string ip_;
    uint16_t port_;

    void ClearTx(uint64_t tx_number);
};
}  // namespace fault
}  // namespace txservice
