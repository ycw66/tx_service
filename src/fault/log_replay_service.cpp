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

int ReplayService::on_received_messages(brpc::StreamId stream_id,
                                        butil::IOBuf *const messages[],
                                        size_t size)
{
    std::vector<::txlog::ReplayMessage> msg_vec(size);
    std::vector<std::unique_ptr<ReplayLogCc>> cc_req_vec;
    cc_req_vec.reserve(size);

    std::mutex mux;
    std::condition_variable cv;
    uint32_t finish_log_cnt = 0;

    for (size_t idx = 0; idx < size; ++idx)
    {
        ::txlog::ReplayMessage &msg = msg_vec.at(idx);
        butil::IOBufAsZeroCopyInputStream wrapper(*messages[idx]);
        msg.ParseFromZeroCopyStream(&wrapper);

        if (msg.has_log_record())
        {
            const ::txlog::ReplayRecordMsg &log_rec = msg.log_record();
            uint64_t commit_ts = log_rec.commit_ts();
            const std::string &blob = log_rec.log_blob();
            uint32_t cc_node_group_id = log_rec.cc_node_group_id();

            size_t offset = 0;

            while (offset < blob.size())
            {
                // 1-byte integer for the length of the table name
                uint8_t log_type =
                    *reinterpret_cast<const uint8_t *>(blob.data());
                offset += sizeof(uint8_t);

                if (log_type == static_cast<uint8_t>(LogType::CREATE_TABLE))
                {
                    // 1-byte integer for the length of the table name
                    uint8_t table_name_len = *reinterpret_cast<const uint8_t *>(
                        blob.data() + offset);
                    offset += sizeof(uint8_t);

                    // Table name string
                    std::string table_name(blob.data() + offset,
                                           table_name_len);
                    offset += table_name_len;

                    // 4-byte integer for the length of the catalog
                    uint32_t catalog_len = *reinterpret_cast<const uint32_t *>(
                        blob.data() + offset);
                    offset += sizeof(uint32_t);

                    std::unique_ptr<ReplayLogCc> &cc_req =
                        cc_req_vec.emplace_back(std::make_unique<ReplayLogCc>(
                            LogType::CREATE_TABLE,
                            cc_node_group_id,
                            std::move(table_name),
                            std::string_view(blob.data() + offset, catalog_len),
                            commit_ts,
                            local_shards_.Count(),
                            mux,
                            cv,
                            finish_log_cnt));

                    for (uint32_t core_id = 0; core_id < local_shards_.Count();
                         ++core_id)
                    {
                        local_shards_.EnqueueCcRequest(core_id, cc_req.get());
                    }

                    offset += catalog_len;
                }
                else if (log_type == static_cast<uint8_t>(LogType::DROP_TABLE))
                {
                    // 1-byte integer for the length of the table name
                    uint8_t table_name_len = *reinterpret_cast<const uint8_t *>(
                        blob.data() + offset);
                    offset += sizeof(uint8_t);

                    // Table name string
                    std::string table_name(blob.data() + offset,
                                           table_name_len);
                    offset += table_name_len;

                    std::unique_ptr<ReplayLogCc> &cc_req =
                        cc_req_vec.emplace_back(std::make_unique<ReplayLogCc>(
                            LogType::DROP_TABLE,
                            cc_node_group_id,
                            std::move(table_name),
                            std::string_view(blob.data(), 0),
                            commit_ts,
                            local_shards_.Count(),
                            mux,
                            cv,
                            finish_log_cnt));

                    for (uint32_t core_id = 0; core_id < local_shards_.Count();
                         ++core_id)
                    {
                        local_shards_.EnqueueCcRequest(core_id, cc_req.get());
                    }
                }
                else if (log_type == static_cast<uint8_t>(LogType::RECORD))
                {
                    // 1-byte integer for the length of the table name
                    uint8_t table_name_len = *reinterpret_cast<const uint8_t *>(
                        blob.data() + offset);
                    offset += sizeof(uint8_t);

                    // Table name string
                    std::string table_name(blob.data() + offset,
                                           table_name_len);
                    offset += table_name_len;

                    // 4-byte integer for the length of the serialized
                    // records from the table
                    uint32_t kv_len = *reinterpret_cast<const uint32_t *>(
                        blob.data() + offset);
                    offset += sizeof(uint32_t);

                    std::unique_ptr<ReplayLogCc> &cc_req =
                        cc_req_vec.emplace_back(std::make_unique<ReplayLogCc>(
                            LogType::RECORD,
                            cc_node_group_id,
                            std::move(table_name),
                            std::string_view(blob.data() + offset, kv_len),
                            commit_ts,
                            local_shards_.Count(),
                            mux,
                            cv,
                            finish_log_cnt));

                    // Enqueues the replay request to all local shards. Each
                    // local shard will deserialize the same log record
                    // independently and only inserts the records belonging
                    // to it to its cc map.
                    for (uint32_t core_id = 0; core_id < local_shards_.Count();
                         ++core_id)
                    {
                        local_shards_.EnqueueCcRequest(core_id, cc_req.get());
                    }

                    offset += kv_len;
                }
            }
        }
        else
        {
            // receive finish message from one of log groups
            const ::txlog::ReplayFinishMsg &finish_msg = msg.finish();
            uint32_t lg_id = finish_msg.log_group_id();
            uint32_t cc_ng_id = finish_msg.cc_node_group_id();

            Sharder::Instance().FinishLogReplay(cc_ng_id, lg_id);
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