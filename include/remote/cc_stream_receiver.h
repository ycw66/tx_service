#pragma once

#include <brpc/stream.h>

#include <condition_variable>
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
    CcStreamReceiver(
        LocalCcShards &local_shards,
        moodycamel::ConcurrentQueue<std::unique_ptr<CcMessage>> &msg_pool);
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

    static IsolationLevel ConvertIsolation(IsolationType iso_level);
    static CcProtocol ConvertProtocol(CcProtocolType proto);
    static LockType ConvertLockType(CcLockType lock_type);
    static PostWriteType ConvertCommitType(CommitType commit_type);
    static RecordStatus ConvertRecordStatusType(RecordStatusType status_type);

private:
    std::unique_ptr<CcMessage> GetCcMsg();
    void OnReceiveCcMsg(std::unique_ptr<CcMessage> msg);

    std::mutex inbound_mux_;
    std::condition_variable inbound_cv_;
    std::unordered_set<brpc::StreamId> inbound_streams_;
    LocalCcShards &local_shards_;

    // A pool of protobuf messages for remote cc requests. The stream service
    // receives a message, de-serializes it and dispatches it to local shards
    // for processing. The message is put back into the pool after the cc
    // request is processed.
    moodycamel::ConcurrentQueue<std::unique_ptr<CcMessage>> &msg_pool_;
};
}  // namespace remote
}  // namespace txservice