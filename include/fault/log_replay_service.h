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

#include "txlog.h"

namespace txservice
{
class LocalCcShards;

struct ReplayLogCc;

namespace fault
{
class CcNode;

struct ReplayLogTask
{
    ReplayLogTask() = default;

    ReplayLogTask(uint32_t cc_ng_id,
                  int64_t cc_ng_term,
                  int log_group,
                  uint64_t queued_clock)
        : cc_ng_id_(cc_ng_id),
          cc_ng_term_(cc_ng_term),
          log_group_(log_group),
          queued_clock_(queued_clock)
    {
    }

    uint32_t cc_ng_id_;
    int64_t cc_ng_term_;
    int log_group_;
    // the clock when the request is put into the replay queue.
    // set queued_clock_ to 0, if it's not a delayed request.
    uint64_t queued_clock_;
};

struct RecoverTxTask
{
    RecoverTxTask() = default;

    RecoverTxTask(uint64_t tx_number,
                  int64_t tx_term,
                  uint32_t cc_ng_id,
                  int64_t cc_ng_term,
                  int32_t key_write_lock_count)
        : tx_number_(tx_number),
          tx_term_(tx_term),
          cc_ng_id_(cc_ng_id),
          cc_ng_term_(cc_ng_term),
          key_write_lock_count_(key_write_lock_count)
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
    // How many key write locks in this tx for current shard
    int32_t key_write_lock_count_;
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
     * @brief Send ReplayLogRequest to log groups. If log_group is negative,
     * send to all log groups, else just the specified log group.
     */
    void ReplayLog(uint32_t cc_ng_id,
                   int64_t cc_ng_term,
                   int log_group = -1,
                   bool delayed_request = false);

    void RecoverTx(uint64_t tx_number,
                   int64_t tx_term,
                   uint32_t cc_ng_id,
                   int64_t cc_ng_term,
                   int32_t write_lock_count);

    void NotifyLeaderTransfer();

    int on_received_messages(brpc::StreamId stream_id,
                             butil::IOBuf *const messages[],
                             size_t size) override;

    void on_idle_timeout(brpc::StreamId id) override;

    void on_closed(brpc::StreamId id) override;

private:
    /**
     * @brief ReplayNow() check whether the replay request can be processed
     * immediately. When replay error happens, we will put the replay request
     * into replay queue again, but with a delay (default 10 seconds).
     */
    bool ReplayNow(ReplayLogTask &task);

    void ProcessReplayLogTask(ReplayLogTask &task);

    int ProcessDelayedReplayLogTask();

    void ProcessRecoverTxTask(RecoverTxTask &task);

    void RequestLeaderTransfer();

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
        bool recovery_error_{false};
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
        brpc::StreamId stream_id,
        std::vector<std::unique_ptr<ReplayLogCc>> &cc_req_vec,
        std::mutex &mux,
        std::condition_variable &cv,
        uint32_t &finish_log_cnt,
        bool &recovery_error);
    static const int timeout_ms_ = 2000;
    // to resend ReplayLogRequest on stream timeout
    TxLog *log_agent_;

    std::thread notify_thread_;
    std::deque<ReplayLogTask> replay_log_queue_;
    std::deque<ReplayLogTask> delayed_replay_queue_;
    std::deque<RecoverTxTask> recover_tx_queue_;
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
