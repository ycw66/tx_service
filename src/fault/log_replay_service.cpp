#include "log_replay_service.h"

#include "cc/cc_request.h"
#include "cc/local_cc_shards.h"
#include "fault/cc_node.h"
#include "sharder.h"
#include "type.h"

namespace txservice
{
namespace fault
{
ReplayService::ReplayService(LocalCcShards &local_shards)
    : local_shards_(local_shards)
{
}

ReplayService::~ReplayService()
{
    std::unique_lock<std::mutex> lk(inbound_mux_);
    for (auto &stream_id : inbound_streams_)
    {
        brpc::StreamClose(stream_id);
    }

    inbound_cv_.wait(lk, [this]() { return inbound_streams_.size() == 0; });
}

void ReplayService::Connect(::google::protobuf::RpcController *controller,
                            const ::txlog::LogReplayConnectRequest *request,
                            ::txlog::LogReplayConnectResponse *response,
                            ::google::protobuf::Closure *done)
{
    brpc::StreamId stream_socket;
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

    response->set_success(true);

    std::lock_guard<std::mutex> guard(inbound_mux_);
    inbound_streams_.emplace(stream_socket);
}

void ReplayService::UpdateLogGroupLeader(
    ::google::protobuf::RpcController *controller,
    const ::txlog::LogLeaderUpdateRequest *request,
    ::txlog::LogLeaderUpdateResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);
    uint32_t lg_id = request->lg_id();
    uint32_t idx = request->lg_idx();
    Sharder::Instance().UpdateLogGroupLeader(lg_id, idx);
    response->set_error(false);
    LOG(INFO) << "Update log group:" << lg_id << " leader to index:" << idx;
}

int ReplayService::on_received_messages(brpc::StreamId stream_id,
                                        butil::IOBuf *const messages[],
                                        size_t size)
{
    std::vector<::txlog::ReplayMessage> msg_vec(size);
    std::vector<std::unique_ptr<ReplayLogCc>> cc_req_vec;

    std::mutex mux;
    std::condition_variable cv;
    uint32_t finish_log_cnt = 0;

    for (size_t idx = 0; idx < size; ++idx)
    {
        ::txlog::ReplayMessage &msg = msg_vec.at(idx);
        butil::IOBufAsZeroCopyInputStream wrapper(*messages[idx]);
        msg.ParseFromZeroCopyStream(&wrapper);

        uint32_t cc_ng_id = msg.cc_node_group_id();
        int64_t cc_ng_term = msg.cc_node_group_term();

        // parse and process log records
        const std::string &log_records = msg.binary_log_records();
        size_t offset = 0;
        while (offset < log_records.size())
        {
            // 8-byte for commit_ts
            uint64_t commit_ts = *reinterpret_cast<const uint64_t *>(
                log_records.data() + offset);
            offset += sizeof(uint64_t);
            // 4-byte for log_blob length
            uint32_t blob_length = *reinterpret_cast<const uint32_t *>(
                log_records.data() + offset);
            offset += sizeof(uint32_t);

            std::string_view blob(log_records.data() + offset, blob_length);
            offset += blob_length;

            // parse log_blob
            size_t blob_offset = 0;
            while (blob_offset < blob.size())
            {
                // 1-byte integer for the length of the table name
                uint8_t table_name_len = *reinterpret_cast<const uint8_t *>(
                    blob.data() + blob_offset);
                blob_offset += sizeof(uint8_t);

                // Table name string
                std::string_view table_name_view(blob.data() + blob_offset,
                                                 table_name_len);
                blob_offset += table_name_len;

                // 4-byte integer for the length of the serialized
                // records from the table
                uint32_t kv_len = *reinterpret_cast<const uint32_t *>(
                    blob.data() + blob_offset);
                blob_offset += sizeof(uint32_t);

                std::unique_ptr<ReplayLogCc> &cc_req =
                    cc_req_vec.emplace_back(std::make_unique<ReplayLogCc>(
                        cc_ng_id,
                        table_name_view,
                        std::string_view(blob.data() + blob_offset, kv_len),
                        commit_ts,
                        0,
                        mux,
                        cv,
                        finish_log_cnt));

                // Enqueues the replay request to the first local shard. The
                // shard will deserialize the log record and only insert the
                // records belonging to its cc map. The replay request is then
                // moved to remaining shards one after another and is replayed
                // at individual shards separately.
                local_shards_.EnqueueCcRequest(0, cc_req.get());

                blob_offset += kv_len;
            }
        }

        // process schema_op_msgs
        for (const ::txlog::ReplaySchemaMsg &schema_op_msg :
             msg.schema_op_msgs())
        {
            const std::string &schema_op_blob = schema_op_msg.schema_op_blob();
            std::string_view catalog_table_name_view(catalog_ccm_name);

            std::unique_ptr<ReplayLogCc> &cc_req =
                cc_req_vec.emplace_back(std::make_unique<ReplayLogCc>(
                    cc_ng_id,
                    catalog_table_name_view,
                    std::string_view(schema_op_blob.data(),
                                     schema_op_blob.length()),
                    schema_op_msg.commit_ts(),
                    schema_op_msg.txn(),
                    mux,
                    cv,
                    finish_log_cnt));

            assert(finish_log_cnt == 0);
            local_shards_.EnqueueCcRequest(0, cc_req.get());

            // For every schema operation, waits for it to be recovered at all
            // shards before moving to the next replay request.
            std::unique_lock<std::mutex> lk(mux);
            cv.wait(lk,
                    [&finish_log_cnt, &cc_req_vec]
                    { return finish_log_cnt == cc_req_vec.size(); });
        }

        // process finish message
        if (msg.has_finish())
        {
            // mark log replay finish only when all preceding ReplayLogCc
            // requests finished.
            {
                std::unique_lock<std::mutex> lk(mux);
                cv.wait(lk,
                        [&finish_log_cnt, &cc_req_vec]
                        { return finish_log_cnt == cc_req_vec.size(); });
            }

            // receive finish message from one of log groups
            const ::txlog::ReplayFinishMsg &finish_msg = msg.finish();
            uint32_t lg_id = finish_msg.log_group_id();
            Sharder::Instance().FinishLogReplay(cc_ng_id, cc_ng_term, lg_id);
        }
    }

    {
        std::unique_lock<std::mutex> lk(mux);
        cv.wait(lk,
                [&finish_log_cnt, &cc_req_vec]
                { return finish_log_cnt == cc_req_vec.size(); });
    }

    return 0;
}

void ReplayService::on_closed(brpc::StreamId id)
{
    std::unique_lock<std::mutex> lk(inbound_mux_);
    inbound_streams_.erase(id);
    if (inbound_streams_.size() == 0)
    {
        inbound_cv_.notify_one();
    }
}
}  // namespace fault
}  // namespace txservice