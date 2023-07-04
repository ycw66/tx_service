#include "log_replay_service.h"

#include "cc/cc_request.h"
#include "cc/local_cc_shards.h"
#include "fault/cc_node.h"
#include "proto/cc_request.pb.h"
#include "raft_log.pb.h"
#include "sharder.h"
#include "type.h"

/**
 * LogReplayService serves three purposes:
 * replay log after a ccnode becomes leader; recover orphan lock's belonging
 * txn; check whether this node is preferred node group's leader periodically
 * and request leader transfer if not.
 *
 * Here is a brief replay protocol description:
 * 1. ccnode becomes a raft leader and begins to send ReplayLogRequest to all
 * the log groups. If log group's leader is not elected, the ccnode will call
 * braft API to wait for the log leader to be elected.
 * 2. Log group will connect to log_replay_service and send replay logs to
 * ccnode.
 * 3. ccnode receives and replays the logs from the stream.
 * 4. If ccnode fails to receive new logs after a timeout(2000ms), it will close
 * the stream and send a new ReplayLogRequest to log group leader. And replay
 * from the beginning.
 * 5. ccnode finishes the log replay and become the actual leader from candidate
 * leader. Then it will close the log replay stream.
 * 6. Later when an orphan lock is detected, will send RecoveryTx request to log
 * service.
 * 7. log service will send the orphan lock replay log to log_replay_service if
 * the orphan lock tx is committed. Since the stream is closed, will reconnect
 * the stream and send the message. Note that the new connection will not set
 * idle_timeout since orphan lock will not happens in normal case.
 * 8. ccnode replay the orphan lock record and release the orphan lock.
 */

namespace txservice
{
namespace fault
{
ReplayService::ReplayService(LocalCcShards &local_shards,
                             TxLog *log_agent,
                             std::string ip,
                             uint16_t port)
    : local_shards_(local_shards),
      log_agent_(log_agent),
      finish_(false),
      ip_(std::move(ip)),
      port_(port)
{
    notify_thread_ = std::thread(
        [this]
        {
            // LOG(INFO) << "replay service notify thread started";
            while (!finish_.load(std::memory_order_acquire))
            {
                std::unique_lock<std::mutex> lk(queue_mux_);
                queue_cv_.wait_for(
                    lk,
                    std::chrono::seconds(10),
                    [this]
                    {
                        return !replay_log_queue_.empty() ||
                               !recover_tx_queue_.empty() ||
                               finish_.load(std::memory_order_acquire);
                    });
                if (finish_.load(std::memory_order_acquire))
                {
                    LOG(INFO) << "replay service notify thread quits";
                    break;
                }
                if (!replay_log_queue_.empty())
                {
                    ReplayLogTask task = replay_log_queue_.front();
                    replay_log_queue_.pop_front();
                    lk.unlock();
                    LOG(INFO) << "replay service processes a ReplayLog task";
                    ProcessReplayLogTask(task);
                    continue;
                }
                if (!delayed_replay_queue_.empty())
                {
                    int ready_cnt = ProcessDelayedReplayLogTask();
                    if (ready_cnt > 0)
                    {
                        // ReplayLog task is of the highest priority, continue
                        // to next round and process the ready ReplayTask
                        continue;
                    }
                }
                if (!recover_tx_queue_.empty())
                {
                    RecoverTxTask task = recover_tx_queue_.front();
                    recover_tx_queue_.pop_front();
                    lk.unlock();
                    LOG(INFO) << "replay service processes a RecoverTx task";
                    ProcessRecoverTxTask(task);
                    continue;
                }
                if (!Sharder::Instance().IsPreferredGroupLeader())
                {
                    lk.unlock();
                    LOG(INFO)
                        << "this node is not preferred node group's leader, "
                           "request leader transfer";
                    RequestLeaderTransfer();
                }
            }
        });
}

bool ReplayService::ReplayNow(ReplayLogTask &task)
{
    // queued_clock_ == 0 means it's not a delayed request.
    if (task.queued_clock_ == 0)
    {
        return true;
    }

    // check whether the delayed request can be executed now.
    uint64_t now_ts = LocalCcShards::ClockTs();
    using namespace std::chrono_literals;
    uint64_t duration = std::chrono::duration_cast<std::chrono::microseconds>(
                            std::chrono::seconds(30))
                            .count();
    if (now_ts - task.queued_clock_ > duration)
    {
        return true;
    }
    else
    {
        return false;
    }
}

void ReplayService::Shutdown()
{
    // reap background thread
    {
        std::unique_lock lk(queue_mux_);
        finish_.store(true, std::memory_order_release);
        queue_cv_.notify_one();
    }
    notify_thread_.join();

    // close all streams
    std::unique_lock<std::mutex> lk(inbound_mux_);
    for (auto it = inbound_connections_.begin();
         it != inbound_connections_.end();
         it++)
    {
        brpc::StreamClose(it->first);
    }

    inbound_cv_.wait(lk, [this]() { return active_stream_cnt_ == 0; });
}

void ReplayService::Connect(::google::protobuf::RpcController *controller,
                            const ::txlog::LogReplayConnectRequest *request,
                            ::txlog::LogReplayConnectResponse *response,
                            ::google::protobuf::Closure *done)
{
    brpc::StreamId stream_socket;
    brpc::ClosureGuard done_guard(done);

    uint32_t cc_ng_id = request->cc_node_group_id();
    uint32_t log_group_id = request->log_group_id();
    int64_t cc_ng_term = request->cc_ng_term();
    std::unique_lock lk(inbound_mux_);

    // indicates whether this connection (<cc_ng_id, lg_id> pair) is still
    // recovering.
    // It will affect the stream option. If recovering is true, then
    // the stream will set idle_timeout and try to resend ReplayLogRequest if
    // timeout happens.
    bool recovering = true;

    for (const auto &[stream_id, info] : inbound_connections_)
    {
        if (info.cc_ng_id_ == cc_ng_id && info.log_group_id_ == log_group_id)
        {
            if (cc_ng_term > info.cc_ng_term_)
            {
                // cc_ng_term > info.cc_ng_term_, the cc_ng has failed over
                recovering = true;
            }
            else if (cc_ng_term == info.cc_ng_term_)
            {
                // cc_ng_term == info.cc_ng_term, there are two possibilities:
                // 1) the cc_ng is still recovering, and this request is a
                // response for ReplayService's stream timeout and resend
                // ReplayLogRequest; or
                // 2) the cc node has recovered, and LogShippingAgent reconnect
                // to send RecoverTx results.
                // In either case, the new stream to be created should reserve
                // the recover status of old stream, and whether idle_timeout
                // should be set for this new stream depends on it.
                recovering = info.recovering_;
            }
            else
            {
                // an outdated connect request, ignore
                return;
            }
            // close old stream and remove entry
            brpc::StreamClose(stream_id);
            inbound_connections_.erase(stream_id);
            break;
        }
    }
    // either no old connection found for <cc_ng_id, lg_id> pair, or old
    // connection has been removed. accept connect request and insert
    // ConnectionInfo.

    brpc::Controller *cntl = static_cast<brpc::Controller *>(controller);

    brpc::StreamOptions stream_options;
    stream_options.handler = this;
    // set idle_timeout_ms when this is a new term connection or a reconnect of
    // recovering connection
    stream_options.idle_timeout_ms = recovering ? timeout_ms_ : -1;
    if (brpc::StreamAccept(&stream_socket, *cntl, &stream_options) != 0)
    {
        cntl->SetFailed("Fail to accept stream");
        return;
    }

    response->set_success(true);
    LOG(INFO) << "replay service accepting new stream: " << stream_socket
              << " from log group: " << log_group_id
              << " to cc_ng: " << cc_ng_id << " at term: " << cc_ng_term;

    inbound_connections_.insert_or_assign(
        stream_socket,
        ConnectionInfo(log_group_id, cc_ng_id, cc_ng_term, recovering));
    active_stream_cnt_++;
}

void ReplayService::UpdateLogGroupLeader(
    ::google::protobuf::RpcController *controller,
    const ::txlog::LogLeaderUpdateRequest *request,
    ::txlog::LogLeaderUpdateResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);
    uint32_t lg_id = request->lg_id();
    uint32_t node_id = request->node_id();
    Sharder::Instance().UpdateLogGroupLeader(lg_id, node_id);
    response->set_error(false);
    LOG(INFO) << "Update log group:" << lg_id
              << " leader to node_id:" << node_id;
}

void ReplayService::ReplayLog(uint32_t cc_ng_id,
                              int64_t cc_ng_term,
                              int log_group,
                              bool delayed_request)
{
    std::unique_lock lk(queue_mux_);
    if (delayed_request)
    {
        uint64_t queued_clock = LocalCcShards::ClockTs();
        delayed_replay_queue_.emplace_back(
            cc_ng_id, cc_ng_term, log_group, queued_clock);
    }
    else
    {
        replay_log_queue_.emplace_back(cc_ng_id, cc_ng_term, log_group, 0);
    }
    queue_cv_.notify_one();
}

void ReplayService::RecoverTx(uint64_t tx_number,
                              int64_t tx_term,
                              uint32_t cc_ng_id,
                              int64_t cc_ng_term)
{
    std::unique_lock lk(queue_mux_);
    recover_tx_queue_.emplace_back(tx_number, tx_term, cc_ng_id, cc_ng_term);
    queue_cv_.notify_one();
}

void ReplayService::NotifyLeaderTransfer()
{
    queue_cv_.notify_one();
}

int ReplayService::on_received_messages(brpc::StreamId stream_id,
                                        butil::IOBuf *const messages[],
                                        size_t size)
{
    std::vector<::txlog::ReplayMessage> msg_vec(size);
    std::vector<std::unique_ptr<ReplayLogCc>> cc_req_vec;
    std::vector<std::unique_ptr<ReadCc>> catalog_read_cc_req_vec;
    std::unordered_map<TableName, std::shared_ptr<std::atomic_uint32_t>>
        table_range_split_cnt;
    std::unordered_set<TableName> range_split_tables;

    std::mutex mux;
    std::condition_variable cv;
    uint32_t finish_log_cnt = 0;
    bool recovery_error = false;

    for (size_t idx = 0; idx < size; ++idx)
    {
        ::txlog::ReplayMessage &msg = msg_vec.at(idx);
        butil::IOBufAsZeroCopyInputStream wrapper(*messages[idx]);
        msg.ParseFromZeroCopyStream(&wrapper);
        if (idx == 0)
        {
            // All of the shema and range split logs should be in the first msg.
            // Collect which tables are range splitting. For these table we only
            // need to recover write intent on catalog entry instead of write
            // lock when processing schema op msg.
            for (const ::txlog::ReplaySplitRangeMsg &split_range_msg :
                 msg.split_range_op_msgs())
            {
                const std::string &split_range_op_blob =
                    split_range_msg.split_range_op_blob();
                size_t blob_offset = 0;
                uint8_t table_name_len = *reinterpret_cast<const uint8_t *>(
                    split_range_op_blob.data() + blob_offset);
                blob_offset += sizeof(uint8_t);

                // Table name string
                std::string_view table_name_view(
                    split_range_op_blob.data() + blob_offset, table_name_len);

                // Add read lock on catalog
                TableName table_name{table_name_view,
                                     TableName::Type(table_name_view)};
                TableName base_table_name{table_name.GetBaseTableNameSV(),
                                          TableType::Primary};
                range_split_tables.insert(base_table_name);
            }
        }

        uint32_t cc_ng_id = msg.cc_node_group_id();
        int64_t cc_ng_term = msg.cc_node_group_term();

        // process schema ops before processing data ops
        for (const ::txlog::ReplaySchemaMsg &schema_op_msg :
             msg.schema_op_msgs())
        {
            const std::string &schema_op_blob = schema_op_msg.schema_op_blob();

            std::unique_ptr<ReplayLogCc> &cc_req =
                cc_req_vec.emplace_back(std::make_unique<ReplayLogCc>(
                    cc_ng_id,
                    catalog_ccm_name_sv,
                    TableType::Catalog,
                    std::string_view(schema_op_blob.data(),
                                     schema_op_blob.length()),
                    schema_op_msg.commit_ts(),
                    schema_op_msg.txn(),
                    mux,
                    cv,
                    finish_log_cnt,
                    recovery_error,
                    nullptr,
                    &range_split_tables));

            local_shards_.EnqueueCcRequest(0, cc_req.get());

            // wait for this schema operation to be recovered at all shards
            // before processing next
            WaitAndClearRequests(
                stream_id, cc_req_vec, mux, cv, finish_log_cnt, recovery_error);
        }

        // process range split ops
        for (const ::txlog::ReplaySplitRangeMsg &split_range_msg :
             msg.split_range_op_msgs())
        {
            const std::string &split_range_op_blob =
                split_range_msg.split_range_op_blob();
            size_t blob_offset = 0;
            uint8_t table_name_len = *reinterpret_cast<const uint8_t *>(
                split_range_op_blob.data() + blob_offset);
            blob_offset += sizeof(uint8_t);

            const uint64_t txn = split_range_msg.txn();
            const uint64_t ts = split_range_msg.commit_ts();
            const uint32_t shard_code = txn >> 32L;

            // Table name string
            std::string_view table_name_view(
                split_range_op_blob.data() + blob_offset, table_name_len);

            // Add read lock on catalog
            uint32_t tx_node_id = (txn >> 32L) >> 10;
            int64_t tx_candidate_term =
                Sharder::Instance().CandidateLeaderTerm(tx_node_id);
            TableName table_name{table_name_view,
                                 TableName::Type(table_name_view)};
            TableName base_table_name{table_name.GetBaseTableNameSV(),
                                      TableType::Primary};
            CatalogKey catalog_key(base_table_name);
            CatalogRecord catalog_rec;
            CcHandlerResult<ReadKeyResult> catalog_read_cc_result(nullptr);
            std::mutex read_mux;
            std::condition_variable read_cv;
            bool finished = false;
            catalog_read_cc_result.post_lambda_ =
                [&](CcHandlerResult<ReadKeyResult> *hd)
            {
                std::unique_lock<std::mutex> lk(read_mux);
                finished = true;
                read_cv.notify_one();
            };

            // Add read lock on catalog at the first
            // TODO{liunyl}: potential dead lock here? schema replay could've
            // acquired write lock on catalog and this read cc will be blocked.
            ReadCc read_cc;
            read_cc.Reset(&catalog_ccm_name,
                          &catalog_key,
                          shard_code,
                          &catalog_rec,
                          ReadType::Inside,
                          txn,
                          tx_candidate_term,
                          ts,
                          &catalog_read_cc_result,
                          IsolationLevel::RepeatableRead,
                          CcProtocol::Locking,
                          false,
                          false,
                          nullptr,
                          true);
            local_shards_.EnqueueCcRequest(0, &read_cc);

            {
                std::unique_lock<std::mutex> lk(read_mux);
                read_cv.wait(lk, [&] { return finished; });
            }

            CcEntryAddr catalog_cce_addr =
                catalog_read_cc_result.Value().cce_addr_;
            // LockType catalog_lock_type =
            //     catalog_read_cc_result.Value().lock_type_;
            uint64_t catalog_version_ts = catalog_read_cc_result.Value().ts_;
            ReadSetEntry catalog_read_set_entry =
                ReadSetEntry(catalog_version_ts);
            // ReadSetEntry catalog_read_set_entry = ReadSetEntry(
            //     catalog_version_ts, CcProtocol::Locking, catalog_lock_type);

            auto res_pair = table_range_split_cnt.try_emplace(
                base_table_name, std::make_shared<std::atomic_uint32_t>(0));

            // Replay Split
            blob_offset += table_name_len;
            std::unique_ptr<ReplayLogCc> &cc_req =
                cc_req_vec.emplace_back(std::make_unique<ReplayLogCc>(
                    cc_ng_id,
                    table_name_view,
                    TableType::RangePartition,
                    std::string_view(
                        split_range_op_blob.data() + blob_offset,
                        split_range_op_blob.length() - blob_offset),
                    ts,
                    txn,
                    mux,
                    cv,
                    finish_log_cnt,
                    recovery_error,
                    res_pair.first->second));
            cc_req->SetCatalogCcEntry(catalog_cce_addr, catalog_read_set_entry);

            local_shards_.EnqueueCcRequest(0, cc_req.get());
            // wait for this range split operation to be recovered at all shards
            // before processing next
            WaitAndClearRequests(
                stream_id, cc_req_vec, mux, cv, finish_log_cnt, recovery_error);
        }

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
#ifdef ON_KEY_OBJECT
                std::string_view table_name_view(redis_table_name_sv);
                TableType table_type = TableType::Primary;
                // 4-byte integer for the length of the serialized object key
                // and commands
                uint32_t kv_len = *reinterpret_cast<const uint32_t *>(
                    blob.data() + blob_offset);
                blob_offset += sizeof(uint32_t);
                LOG(INFO) << "kv len: " << kv_len;
#else
                // 1-byte integer for the length of the table name
                uint8_t table_name_len = *reinterpret_cast<const uint8_t *>(
                    blob.data() + blob_offset);
                blob_offset += sizeof(uint8_t);

                // Table name string
                std::string_view table_name_view(blob.data() + blob_offset,
                                                 table_name_len);
                blob_offset += table_name_len;

                // 1-byte integer for the type of table
                uint8_t table_type_number = *reinterpret_cast<const uint8_t *>(
                    blob.data() + blob_offset);
                TableType table_type;
                switch (table_type_number)
                {
                case 0:
                    table_type = TableType::Primary;
                    break;
                case 1:
                    table_type = TableType::Secondary;
                    break;
                case 2:
                    table_type = TableType::UniqueSecondary;
                    break;
                case 3:
                    table_type = TableType::Catalog;
                    break;
                case 4:
                    table_type = TableType::RangePartition;
                    break;
                }
                blob_offset += sizeof(uint8_t);

                // 4-byte integer for the length of the serialized
                // records from the table
                uint32_t kv_len = *reinterpret_cast<const uint32_t *>(
                    blob.data() + blob_offset);
                blob_offset += sizeof(uint32_t);
#endif

                std::unique_ptr<ReplayLogCc> &cc_req =
                    cc_req_vec.emplace_back(std::make_unique<ReplayLogCc>(
                        cc_ng_id,
                        table_name_view,
                        table_type,
                        std::string_view(blob.data() + blob_offset, kv_len),
                        commit_ts,
                        0,
                        mux,
                        cv,
                        finish_log_cnt,
                        recovery_error));

                // Enqueues the replay request to the first local shard. The
                // shard will deserialize the log record and only insert the
                // records belonging to its cc map. The replay request is then
                // moved to remaining shards one after another and is replayed
                // at individual shards separately.
                local_shards_.EnqueueCcRequest(0, cc_req.get());

                blob_offset += kv_len;
            }
        }

        if (msg.has_finish())
        {
            // finish log replay of this log group
            // wait for all preceding ReplayLogCc requests finish
            WaitAndClearRequests(
                stream_id, cc_req_vec, mux, cv, finish_log_cnt, recovery_error);

            // update recovering status and then close this stream,
            // log_shipping_agent has to create a new stream to send recoverTx
            // log records. when accepting that new stream, set no
            // idle_timeout_ms as that is a long-running connection.
            std::unique_lock lk(inbound_mux_);
            auto it = inbound_connections_.find(stream_id);
            if (it != inbound_connections_.end())
            {
                // ignore the old stream which is not in inbound_connections_.
                // if error happens during replay, the current ng's leader term
                // should not be updated.
                if (it->second.recovery_error_)
                {
                    LOG(ERROR)
                        << "monographdb failed to recovery on ccnode group:"
                        << cc_ng_id << " with term:" << cc_ng_term;
                }
                else
                {
                    // finish log replay of this log group
                    const ::txlog::ReplayFinishMsg &finish_msg = msg.finish();
                    uint32_t lg_id = finish_msg.log_group_id();
                    uint32_t latest_txn_no = finish_msg.latest_txn_no();
                    uint64_t last_ckpt_ts = finish_msg.last_ckpt_ts();
                    Sharder::Instance().FinishLogReplay(cc_ng_id,
                                                        cc_ng_term,
                                                        lg_id,
                                                        latest_txn_no,
                                                        last_ckpt_ts);
                }

                LOG(INFO) << "replay connection: cc node group: "
                          << it->second.cc_ng_id_
                          << ", term: " << it->second.cc_ng_term_
                          << ", log group: " << it->second.log_group_id_
                          << ", set recovering status to finished";
                it->second.recovering_ = false;
            }
            brpc::StreamClose(stream_id);
            // assumption: finish message must be the last message so return
            return 0;
        }
    }

    WaitAndClearRequests(
        stream_id, cc_req_vec, mux, cv, finish_log_cnt, recovery_error);
    return 0;
}

void ReplayService::on_idle_timeout(brpc::StreamId id)
{
    // if the cc_node is still recovering, resend replay request to
    // corresponding log group. on_idle_timeout will be triggered every
    // timeout_ms_ until ReplayLogRequest succeeds and a new stream on this
    // <cc_ng_id, log_group_id> pair is established, this stream will be closed
    // then.
    ConnectionInfo info{};
    {
        std::unique_lock lk(inbound_mux_);
        auto it = inbound_connections_.find(id);
        if (it == inbound_connections_.end())
        {
            // this stream has been replaced by a newer one
            return;
        }
        info = it->second;
    }
    if (info.recovering_)
    {
        // still recovering, resend replay request to log group
        uint32_t lg_id = info.log_group_id_;
        uint32_t cc_ng_id = info.cc_ng_id_;
        int64_t cc_ng_term = info.cc_ng_term_;
        LOG(INFO) << "replay service stream: " << id
                  << " timeouts, cc_node group: " << cc_ng_id
                  << ", log group: " << lg_id
                  << ", still recovering, resend ReplayLogRequest";
        ReplayLog(cc_ng_id, cc_ng_term, lg_id);
        brpc::StreamClose(id);
    }
}

void ReplayService::on_closed(brpc::StreamId id)
{
    // If remote log group crashes, the stream will be closed, should check cc
    // node's recovering status and resend ReplayLogRequest here?
    // Seems unnecessary as log group's new leader will try to reship records.
    // Besides, the stream might be closed by LogShippingAgent intentionally.
    // There is no way to tell the difference. So, just do nothing.

    std::unique_lock<std::mutex> lk(inbound_mux_);
    active_stream_cnt_--;
    LOG(INFO) << "replay service stream: " << id
              << ", is closed, active stream cnt: " << active_stream_cnt_;
    if (active_stream_cnt_ == 0)
    {
        inbound_cv_.notify_one();
    }
}

void ReplayService::WaitAndClearRequests(
    brpc::StreamId stream_id,
    std::vector<std::unique_ptr<ReplayLogCc>> &cc_req_vec,
    std::mutex &mux,
    std::condition_variable &cv,
    uint32_t &finish_log_cnt,
    bool &recovery_error)
{
    std::unique_lock<std::mutex> lk(mux);
    cv.wait(lk,
            [&finish_log_cnt, &cc_req_vec]
            { return (finish_log_cnt == cc_req_vec.size()); });
    if (finish_log_cnt == cc_req_vec.size())
    {
        cc_req_vec.clear();
        finish_log_cnt = 0;
    }
    if (recovery_error)
    {
        std::unique_lock lk(inbound_mux_);
        uint32_t error_node_group_id = 0;
        int64_t error_term = -1;

        // find the replay error node group and its term.
        auto it = inbound_connections_.find(stream_id);
        assert(it != inbound_connections_.end());
        if (it != inbound_connections_.end())
        {
            error_node_group_id = it->second.cc_ng_id_;
            error_term = it->second.cc_ng_term_;
            it->second.recovery_error_ = true;
        }

        // close all the streams belonging to the current node group and term.
        for (const auto &[stream_id, info] : inbound_connections_)
        {
            if (info.cc_ng_id_ == error_node_group_id &&
                info.cc_ng_term_ == error_term)
            {
                brpc::StreamClose(stream_id);
            }
        }
        // put the replay log request back to the replay queue, but the replay
        // request will be scheduled with 10 senconds delay. log_id = -1 means
        // replay from all the log groups.
        ReplayLog(error_node_group_id, error_term, -1, true);
    }
}

void ReplayService::ClearTx(uint64_t tx_number)
{
    ClearTxCc req(local_shards_.Count());
    req.Set(tx_number);

    for (uint32_t core_id = 0; core_id < local_shards_.Count(); ++core_id)
    {
        local_shards_.EnqueueCcRequest(core_id, &req);
    }

    req.Wait();
}

void ReplayService::ProcessReplayLogTask(ReplayLogTask &task)
{
    if (Sharder::Instance().CandidateLeaderTerm(task.cc_ng_id_) < 0)
    {
        // node group is not recovering, skip replay.
        return;
    }
    // call log agent replay log api
    log_agent_->ReplayLog(
        task.cc_ng_id_, task.cc_ng_term_, ip_, port_, task.log_group_, finish_);
}

int ReplayService::ProcessDelayedReplayLogTask()
{
    // move the delayed replay requests whose timer is fired to
    // replay_log_queue_
    int ready_cnt = 0;
    for (auto it = delayed_replay_queue_.begin();
         it != delayed_replay_queue_.end();)
    {
        if (ReplayNow(*it))
        {
            replay_log_queue_.emplace_back(*it);
            it = delayed_replay_queue_.erase(it);
            ready_cnt++;
        }
        else
        {
            it++;
        }
    }
    return ready_cnt;
}

void ReplayService::ProcessRecoverTxTask(RecoverTxTask &task)
{
    // process RecoverTx request

    // Recovering a tx's lock consists of two parts: (1)
    // inquires the tx status in the cc node in which the tx
    // resides, and (2) if the tx's status is committed or the
    // tx is not found, checks the tx status in the log group.

    // The tx node ID is represented by the higher 4 bytes, in
    // which the lower 10 bits represents the local core ID.
    uint32_t tx_ng = (task.tx_number_ >> 32L) >> 10;
    uint32_t tx_leader = Sharder::Instance().LeaderNodeId(tx_ng);
    remote::CheckTxStatusResponse_TxStatus tx_status;

    if (tx_leader == Sharder::Instance().NodeId())
    {
        CheckTxStatusCc check_tx_cc(task.tx_number_);
        local_shards_.EnqueueCcRequest(task.tx_number_ >> 32L, &check_tx_cc);
        check_tx_cc.Wait();

        if (check_tx_cc.Exists())
        {
            switch (check_tx_cc.TxStatus())
            {
            case TxnStatus::Committed:
                tx_status = remote::CheckTxStatusResponse_TxStatus_COMMITTED;
                break;
            case TxnStatus::Unknown:
                tx_status =
                    remote::CheckTxStatusResponse_TxStatus_RESULT_UNKNOWN;
                break;
            case TxnStatus::Aborted:
                tx_status = remote::CheckTxStatusResponse_TxStatus_ABORTED;
                break;
            default:
                tx_status = remote::CheckTxStatusResponse_TxStatus_ONGOING;
                break;
            }
        }
        else
        {
            tx_status = remote::CheckTxStatusResponse_TxStatus_NOT_FOUND;
        }
    }
    else
    {
        std::string tx_ip;
        uint16_t tx_port;
        Sharder::Instance().GetNodeAddress(tx_leader, tx_ip, tx_port);

        brpc::Channel channel;
        if (channel.Init(tx_ip.c_str(), tx_port + 1, nullptr) != 0)
        {
            // Fails to establish the channel to the tx node.
            // Silently returns. The tx will be recovered again
            // by next conflicting tx.
            LOG(ERROR) << "Fail to init the channel to the "
                          "leader of ng#"
                       << tx_ng << " for tx lock recovery.";
            return;
        }

        remote::CcRpcService_Stub stub(&channel);

        remote::CheckTxStatusRequest req;
        req.set_tx_number(task.tx_number_);
        req.set_tx_term(task.tx_term_);
        remote::CheckTxStatusResponse res;

        brpc::Controller cntl;
        stub.CheckTxStatus(&cntl, &req, &res, nullptr);

        if (cntl.Failed())
        {
            LOG(ERROR) << "Fail to check the tx status in ng#" << tx_ng
                       << ". Error code: " << cntl.ErrorCode()
                       << ". Msg: " << cntl.ErrorText();
            return;
        }

        tx_status = res.tx_status();
    }

    if (tx_status == remote::CheckTxStatusResponse_TxStatus_ONGOING)
    {
        LOG(INFO) << "The tx " << task.tx_number_
                  << " is ongoing. Does nothing for recovery.";
        return;
    }
    else if (tx_status == remote::CheckTxStatusResponse_TxStatus_ABORTED)
    {
        LOG(INFO) << "The tx" << task.tx_number_
                  << " has aborted. Clears the tx's lock.";
        ClearTx(task.tx_number_);
    }
    else
    {
        // The tx is either committed or result unknown or not
        // found in the tx's cc node, either because the tx node
        // fails or because the tx didn't finish post-processing
        // but decided to move on. In either case, asks the log
        // group: if the tx has committed, the log group ships
        // the tx's log record to the cc node to recover the
        // committed record. Or, the tx must have aborted.

        RecoverTxStatus status = log_agent_->RecoverTx(task.tx_number_,
                                                       task.tx_term_,
                                                       task.cc_ng_id_,
                                                       task.cc_ng_term_,
                                                       ip_,
                                                       port_);

        if (status == RecoverTxStatus::NotCommitted ||
            status == RecoverTxStatus::Alive)
        {
            LOG(INFO) << "The tx " << task.tx_number_
                      << " is to be cleared, after asking "
                         "the log group.";

            // If the tx is not committed, sends a cc request to
            // local cc shards to clear write intentions left by
            // the tx. If the tx node is still alive according
            // to the log group, and yet no log record is found,
            // given that the prior inquiry of the tx status is
            // inconclusive, the tx must have aborted
            // proactively. Clears the tx's locks.
            ClearTx(task.tx_number_);
        }
        else if (status == RecoverTxStatus::RecoverError)
        {
            LOG(INFO) << "There is a tx recovery error when asking "
                         "the log group. Tx number "
                      << task.tx_number_;
        }
        else
        {
            LOG(INFO) << "The tx " << task.tx_number_
                      << " to be recovered has committed.";
            // For DML transactions, if the tx has committed,
            // the log group will ship the tx's committed
            // records to the cc node. If there is an error,
            // does nothing. The next conflicting tx will try a
            // new recovery.
            // For multi-stage transactions, the tx has written
            // log and is guaranteed to succeed and release the
            // lock, do nothing and the lock will be released by
            // the coordinator.
        }
    }
}

void ReplayService::RequestLeaderTransfer()
{
    uint32_t node_id = Sharder::Instance().NodeId();
    Sharder::Instance().UpdateLeader(node_id);
    uint32_t leader_node_id = Sharder::Instance().LeaderNodeId(node_id);
    if (leader_node_id != node_id)
    {
        std::string leader_ip;
        uint16_t leader_port;
        Sharder::Instance().GetNodeAddress(
            leader_node_id, leader_ip, leader_port);
        brpc::Channel channel;
        if (channel.Init(leader_ip.c_str(), leader_port + 1, nullptr) != 0)
        {
            // Fails to establish the channel to the leader. Silently returns.
            // LeaderTransfer will be retried if this node is still not
            // preferred group leader.
            LOG(ERROR) << "Fail to init the channel to the "
                          "leader of ng#"
                       << node_id << " for leader transfer.";
            return;
        }

        remote::CcRpcService_Stub stub(&channel);

        remote::TransferRequest req;
        req.set_ng_id(node_id);
        remote::TransferResponse res;
        res.set_error(false);

        brpc::Controller cntl;
        cntl.set_timeout_ms(3000);
        stub.Transfer(&cntl, &req, &res, nullptr);

        if (cntl.Failed())
        {
            LOG(ERROR) << "Fail the Transfer RPC of ng#" << node_id
                       << ". Error code: " << cntl.ErrorCode()
                       << ". Msg: " << cntl.ErrorText();
        }
        else if (res.error())
        {
            LOG(ERROR) << "Fail to transfer the leader of ng#" << node_id
                       << " to this node";
        }
        else
        {
            LOG(INFO) << "Transfer rpc succeeds, this node should reclaim ng#"
                      << node_id << " leadership later";
        }
    }
}
}  // namespace fault
}  // namespace txservice
