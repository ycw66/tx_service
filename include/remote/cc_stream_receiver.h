#pragma once

#include <brpc/stream.h>

#include <condition_variable>
#include <mutex>
#include <unordered_set>

#include "cc_req_pool.h"
#include "moodycamelqueue.h"
#include "proto/cc_request.pb.h"
#include "remote/remote_cc_request.h"

namespace txservice
{
class LocalCcShards;

namespace remote
{
class CcStreamReceiver : public brpc::StreamInputHandler, public CcStreamService
{
public:
    CcStreamReceiver(
        LocalCcShards &local_shards,
        moodycamel::ConcurrentQueue<std::unique_ptr<CcMessage>> &msg_pool);
    ~CcStreamReceiver();

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

    // Cc requests received via the stream are first de-serialized as remote cc
    // requests and then enqueued into the local cc shards for processing.
    CcRequestPool<RemoteAcquire> acquire_pool_;
    CcRequestPool<RemotePostWrite> postwrite_pool_;
    CcRequestPool<RemotePostRead> postread_pool_;
    CcRequestPool<RemoteRead> read_pool_;
    CcRequestPool<RemoteReadOutside> read_outside_pool_;
    CcRequestPool<RemoteScanOpen> scan_open_pool_;
    CcRequestPool<RemoteScanNextBatch> scan_next_pool_;
    CcRequestPool<RemoteCommitSk> commit_sk_pool_;
    CcRequestPool<RemoteAcquireTableWriteLockCC> acquire_table_write_lock_pool;
    CcRequestPool<RemoteCommitCreateTable> commit_create_table_pool;
    CcRequestPool<RemoteReleaseTableWriteLock> release_table_write_lock_pool;
    CcRequestPool<RemoteCommitDropTable> commit_drop_table_pool;
    CcRequestPool<RemoteFaultInjectCC> fault_inject_pool_;
    // CcRequestPool<NegotiateCc> negoti_pool;

    friend class remote::CcStreamSender;
};
}  // namespace remote
}  // namespace txservice