#include "remote/cc_stream_receiver.h"

#include <brpc/controller.h>

#include "cc/local_cc_shards.h"
#include "error_messages.h"  //CcErrorCode
#include "remote/remote_type.h"
#include "sharder.h"
#include "tx_execution.h"
#include "tx_trace.h"

namespace txservice
{
namespace remote
{
// Cc requests received via the stream are first de-serialized as remote cc
// requests and then enqueued into the local cc shards for processing.
thread_local CcRequestPool<RemoteAcquire> acquire_pool_;
thread_local CcRequestPool<RemotePostWrite> postwrite_pool_;
thread_local CcRequestPool<RemoteAcquireAll> acquire_all_pool_;
thread_local CcRequestPool<RemotePostWriteAll> post_write_all_pool_;
thread_local CcRequestPool<RemotePostRead> postread_pool_;
thread_local CcRequestPool<RemoteRead> read_pool_;
thread_local CcRequestPool<RemoteReadOutside> read_outside_pool_;
thread_local CcRequestPool<RemoteScanOpen> scan_open_pool_;
thread_local CcRequestPool<RemoteScanNextBatch> scan_next_pool_;
thread_local CcRequestPool<RemoteFaultInjectCC> fault_inject_pool_;
thread_local CcRequestPool<RemoteCleanCcEntryForTestCc> clean_cc_entry_pool_;

CcStreamReceiver::CcStreamReceiver(
    LocalCcShards &local_shards,
    moodycamel::ConcurrentQueue<std::unique_ptr<CcMessage>> &msg_pool)
    : local_shards_(local_shards), msg_pool_(msg_pool)
{
}

void CcStreamReceiver::Shutdown()
{
    std::unique_lock<std::mutex> lk(inbound_mux_);
    for (auto &stream_id : inbound_streams_)
    {
        brpc::StreamClose(stream_id);
    }

    inbound_cv_.wait(lk, [this]() { return inbound_streams_.size() == 0; });
}

void CcStreamReceiver::Connect(::google::protobuf::RpcController *controller,
                               const ConnectRequest *request,
                               ConnectResponse *response,
                               ::google::protobuf::Closure *done)
{
    brpc::StreamId stream_socket;

    // This object helps you to call done->Run() in RAII style. If you need
    // to process the request asynchronously, pass done_guard.release().
    brpc::ClosureGuard done_guard(done);

    brpc::Controller *cntl = static_cast<brpc::Controller *>(controller);

    brpc::StreamOptions stream_options;
    stream_options.handler = this;
    if (brpc::StreamAccept(&stream_socket, *cntl, &stream_options) != 0)
    {
        cntl->SetFailed("Fail to accept stream");
        return;
    }

    Sharder::Instance().GetCcStreamSender()->NotifyConnectStream();

    response->set_message("Accepted");

    std::lock_guard<std::mutex> guard(inbound_mux_);
    inbound_streams_.emplace(stream_socket);
}

int CcStreamReceiver::on_received_messages(brpc::StreamId stream_id,
                                           butil::IOBuf *const messages[],
                                           size_t size)
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

void CcStreamReceiver::on_closed(brpc::StreamId stream)
{
    std::unique_lock<std::mutex> lk(inbound_mux_);
    inbound_streams_.erase(stream);
    if (inbound_streams_.size() == 0)
    {
        inbound_cv_.notify_one();
    }
}

std::unique_ptr<CcMessage> CcStreamReceiver::GetCcMsg()
{
    std::unique_ptr<CcMessage> msg;
    if (msg_pool_.try_dequeue(msg))
    {
        return msg;
    }
    else
    {
        return std::make_unique<CcMessage>();
    }
}

void CcStreamReceiver::OnReceiveCcMsg(std::unique_ptr<CcMessage> msg)
{
    TX_TRACE_ACTION_WITH_CONTEXT(
        this,
        msg.get(),
        [&msg]() -> std::string
        {
            return std::string("\"tx_number\":")
                .append(std::to_string(msg->tx_number()))
                .append(",\"tx_term\":")
                .append(std::to_string(msg->tx_term()));
        });
    TX_TRACE_DUMP(msg.get());

    switch (msg->type())
    {
    case CcMessage::MessageType::CcMessage_MessageType_AcquireRequest:
    {
        RemoteAcquire *acquire_req = acquire_pool_.NextRequest();
        TX_TRACE_ASSOCIATE(msg.get(), acquire_req);
        acquire_req->Reset(std::move(msg));
        local_shards_.EnqueueCcRequest(acquire_req->KeyShardCode(),
                                       acquire_req);

        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_AcquireResponse:
    {
        // message AcquireResponse
        //{
        //    bool error = 1;
        //    uint64 vali_ts = 2;
        //    CceAddr_msg cce_addr = 3;
        //}

        assert(msg->has_acquire_resp());

        CcHandlerResult<std::vector<AcquireKeyResult>> *hd_res = nullptr;

        uint32_t tx_node_id = (msg->tx_number() >> 32L) >> 10;
        int64_t tx_term = msg->tx_term();
        if (!Sharder::Instance().CheckLeaderTerm(tx_node_id, tx_term))
        {
            // The tx node has failed. Pointer stability does not hold anymore.
            msg_pool_.enqueue(std::move(msg));
            break;
        }
        else
        {
            hd_res = reinterpret_cast<
                CcHandlerResult<std::vector<AcquireKeyResult>> *>(
                msg->handler_addr());

            if (hd_res->Txm()->TxNumber() != msg->tx_number() ||
                hd_res->Txm()->CommandId() != msg->command_id())
            {
                // The original tx has terminated and the tx machine has been
                // recycled. The response message is directed to an obsolete tx.
                // Skips setting the cc handler result.
                msg_pool_.enqueue(std::move(msg));
                break;
            }
        }

        const AcquireResponse &cc_res = msg->acquire_resp();
        const CceAddr_msg &cce_addr_res = cc_res.cce_addr();
        AcquireKeyResult &acq_res = hd_res->Value()[cc_res.vec_idx()];

        if (cc_res.error_code() != 0)
        {
            if (acq_res.cce_addr_.Term() < 0)
            {
                acq_res.remote_ack_cnt_->fetch_sub(1);
            }

            hd_res->SetError(
                ToLocalType::ConvertCcErrorCode(cc_res.error_code()));
        }
        else
        {
            if (acq_res.cce_addr_.Term() < 0)
            {
                if (cce_addr_res.entry_ptr_case() ==
                    CceAddr_msg::EntryPtrCase::kInsertPtr)
                {
                    acq_res.cce_addr_.SetInsert(cce_addr_res.insert_ptr(),
                                                cce_addr_res.term());
                }
                else
                {
                    acq_res.cce_addr_.SetCce(cce_addr_res.cce_ptr(),
                                             cce_addr_res.term());
                }

                // Even though the role of remote_ack_cnt_ is to bookkeep how
                // many remote acknowledgements have been received, the cc entry
                // address is updated and will be read by the tx processor in a
                // separate thread. To ensure the updated address is visible to
                // the tx processor, the memory order must be
                // memory_order_release.
                acq_res.remote_ack_cnt_->fetch_sub(1,
                                                   std::memory_order_release);
            }

            if (!cc_res.is_ack())
            {
                // For locking-based protocols, when the acquire request is
                // blocked in a remote node, the remote node will send an
                // acknowledgement message to notify the sending tx the cce
                // address and the node's term. When the acquire request is
                // unblocked, the response will send back the last validation ts
                // of the key.
                acq_res.last_vali_ts_ = cc_res.vali_ts();
                acq_res.commit_ts_ = cc_res.commit_ts();
                hd_res->SetFinished();
            }
        }

        msg_pool_.enqueue(std::move(msg));
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_AcquireAllRequest:
    {
        RemoteAcquireAll *acquire_all_req = acquire_all_pool_.NextRequest();
        TX_TRACE_ASSOCIATE(msg.get(), acquire_all_req);
        acquire_all_req->Reset(std::move(msg));
        local_shards_.EnqueueCcRequest(0, acquire_all_req);
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_AcquireAllResponse:
    {
        assert(msg->has_acquire_all_resp());

        CcHandlerResult<AcquireAllResult> *hd_res = nullptr;

        uint32_t tx_node_id = (msg->tx_number() >> 32L) >> 10;
        int64_t tx_term = msg->tx_term();
        if (!Sharder::Instance().CheckLeaderTerm(tx_node_id, tx_term))
        {
            // The tx node has failed. Pointer stability does not hold anymore.
            msg_pool_.enqueue(std::move(msg));
            break;
        }
        else
        {
            hd_res = reinterpret_cast<CcHandlerResult<AcquireAllResult> *>(
                msg->handler_addr());

            if (hd_res->Txm()->TxNumber() != msg->tx_number() ||
                hd_res->Txm()->CommandId() != msg->command_id())
            {
                // The original tx has terminated and the tx machine has been
                // recycled. The response message is directed to an obsolete tx.
                // Skips setting the cc handler result.
                msg_pool_.enqueue(std::move(msg));
                break;
            }
        }

        const AcquireAllResponse &cc_res = msg->acquire_all_resp();
        AcquireAllResult &acq_all_res = hd_res->Value();

        if (cc_res.error_code() != 0)
        {
            hd_res->SetError(
                ToLocalType::ConvertCcErrorCode(cc_res.error_code()));
        }
        else
        {
            if (acq_all_res.node_term_ < 0)
            {
                acq_all_res.node_term_ = cc_res.node_term();
                // Uses memory_order_release to ensure the updated node term is
                // visible to the tx processor, which is in a separate thread.
                acq_all_res.remote_ack_cnt_->fetch_sub(
                    1, std::memory_order_release);
            }
            if (!cc_res.is_ack())
            {
                // For locking-based protocols, when the acquire request is
                // blocked in a remote node, the remote node will send an
                // acknowledgement message to notify the sending tx the cce
                // address and the node's term. When the acquire request is
                // unblocked, the response will send back the last validation ts
                // of the key.
                acq_all_res.last_vali_ts_ = cc_res.vali_ts();
                acq_all_res.commit_ts_ = cc_res.commit_ts();
                hd_res->SetFinished();
            }
        }

        msg_pool_.enqueue(std::move(msg));
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_ValidateRequest:
    {
        assert(msg->has_validate_req());

        const ValidateRequest &req = msg->validate_req();
        const CceAddr_msg &cce_addr = req.cce_addr();

        if (!Sharder::Instance().CheckLeaderTerm(req.node_group_id(),
                                                 cce_addr.term()))
        {
            CcMessage return_msg;
            return_msg.set_tx_number(msg->tx_number());
            return_msg.set_handler_addr(msg->handler_addr());
            return_msg.set_tx_term(msg->tx_term());

            ValidateResponse *resp = return_msg.mutable_validate_resp();
            resp->set_error_code(1);

            CcStreamSender *cc_stream_sender =
                Sharder::Instance().GetCcStreamSender();

            cc_stream_sender->SendMessageToNode(req.src_node_id(), return_msg);
            msg_pool_.enqueue(std::move(msg));
        }
        else
        {
            RemotePostRead *vali_req = postread_pool_.NextRequest();
            TX_TRACE_ASSOCIATE(msg.get(), vali_req);
            vali_req->Reset(std::move(msg));
            vali_req->Ccm()->shard_->Enqueue(vali_req);
        }

        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_ValidateResponse:
    {
        assert(msg->has_validate_resp());

        CcHandlerResult<PostProcessResult> *hd_res = nullptr;

        uint32_t tx_node_id = (msg->tx_number() >> 32L) >> 10;
        int64_t tx_term = msg->tx_term();
        if (!Sharder::Instance().CheckLeaderTerm(tx_node_id, tx_term))
        {
            // The tx node has failed. Pointer stability does not hold anymore.
            msg_pool_.enqueue(std::move(msg));
            break;
        }
        else
        {
            hd_res = reinterpret_cast<CcHandlerResult<PostProcessResult> *>(
                msg->handler_addr());

            if (hd_res->Txm()->TxNumber() != msg->tx_number() ||
                hd_res->Txm()->CommandId() != msg->command_id())
            {
                // The original tx has terminated and the tx machine has been
                // recycled. The response message is directed to an obsolete tx.
                // Skips setting the cc handler result.
                msg_pool_.enqueue(std::move(msg));
                break;
            }
        }

        const ValidateResponse &cc_res = msg->validate_resp();

        if (cc_res.error_code() != 0)
        {
            hd_res->SetError(
                ToLocalType::ConvertCcErrorCode(cc_res.error_code()));
        }
        else
        {
            if (cc_res.txs_size() == 0)
            {
                hd_res->SetFinished();
            }
            else
            {
                // Does not perform tx negotiations so far.
                hd_res->SetError(
                    CcErrorCode::VALIDATION_FAILED_FOR_CONFILICTED_TXS);
            }
        }
        msg_pool_.enqueue(std::move(msg));
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_PostprocessResponse:
    {
        assert(msg->has_post_resp());

        CcHandlerResult<PostProcessResult> *hd_res = nullptr;

        uint32_t tx_node_id = (msg->tx_number() >> 32L) >> 10;
        int64_t tx_term = msg->tx_term();
        if (!Sharder::Instance().CheckLeaderTerm(tx_node_id, tx_term))
        {
            // The tx node has failed. Pointer stability does not hold anymore.
            msg_pool_.enqueue(std::move(msg));
            break;
        }
        else
        {
            hd_res = reinterpret_cast<CcHandlerResult<PostProcessResult> *>(
                msg->handler_addr());

            if (hd_res->Txm()->TxNumber() != msg->tx_number() ||
                hd_res->Txm()->CommandId() != msg->command_id())
            {
                // The original tx has terminated and the tx machine has been
                // recycled. The response message is directed to an obsolete tx.
                // Skips setting the cc handler result.
                msg_pool_.enqueue(std::move(msg));
                break;
            }
        }

        const PostprocessResponse &cc_res = msg->post_resp();

        if (cc_res.error_code() != 0)
        {
            hd_res->SetError(
                ToLocalType::ConvertCcErrorCode(cc_res.error_code()));
        }
        else
        {
            hd_res->SetFinished();
        }

        msg_pool_.enqueue(std::move(msg));
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_ReadRequest:
    {
        RemoteRead *read = read_pool_.NextRequest();
        TX_TRACE_ASSOCIATE(msg.get(), read);
        read->Reset(std::move(msg));
        local_shards_.EnqueueCcRequest(read->KeyShardCode(), read);
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_ReadOutsideRequest:
    {
        RemoteReadOutside *read_outside = read_outside_pool_.NextRequest();

        TX_TRACE_ASSOCIATE(msg.get(), read_outside);
        read_outside->Reset(std::move(msg));

        const CcEntryAddr &cce_addr = read_outside->CceAddr();
        if (Sharder::Instance().CheckLeaderTerm(cce_addr.NodeGroupId(),
                                                cce_addr.Term()))
        {
            read_outside->Ccm()->shard_->Enqueue(read_outside);
        }
        else
        {
            read_outside->Finish();
            read_outside->Free();
        }

        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_ReadResponse:
    {
        assert(msg->has_read_resp());

        CcHandlerResult<ReadKeyResult> *hd_res = nullptr;

        uint32_t tx_node_id = (msg->tx_number() >> 32L) >> 10;
        int64_t tx_term = msg->tx_term();
        if (!Sharder::Instance().CheckLeaderTerm(tx_node_id, tx_term))
        {
            // The tx node has failed. Pointer stability does not hold anymore.
            msg_pool_.enqueue(std::move(msg));
            break;
        }
        else
        {
            hd_res = reinterpret_cast<CcHandlerResult<ReadKeyResult> *>(
                msg->handler_addr());

            if (hd_res->Txm()->TxNumber() != msg->tx_number() ||
                hd_res->Txm()->CommandId() != msg->command_id())
            {
                // The original tx has terminated and the tx machine has been
                // recycled. The response message is directed to an obsolete tx.
                // Skips setting the cc handler result.
                msg_pool_.enqueue(std::move(msg));
                break;
            }
        }

        const ReadResponse &read_res = msg->read_resp();

        if (read_res.error_code() != 0)
        {
            hd_res->SetError(
                ToLocalType::ConvertCcErrorCode(read_res.error_code()));
        }
        else
        {
            ReadKeyResult &read_result = hd_res->Value();

            if (read_result.cce_addr_.Term() < 0)
            {
                const CceAddr_msg &cce_addr_msg = read_res.cce_addr();
                read_result.cce_addr_.SetCce(cce_addr_msg.cce_ptr(),
                                             cce_addr_msg.term());
                // CC entry's shard Id has been set when the read request was
                // sent.
            }

            if (!read_res.is_ack())
            {
                switch (read_res.rec_status())
                {
                case RecordStatusType::NORMAL:
                {
                    read_result.rec_status_ = RecordStatus::Normal;

                    size_t offset = 0;
                    read_result.rec_->Deserialize(read_res.record().data(),
                                                  offset);

                    break;
                }
                case RecordStatusType::DELETED:
                {
                    read_result.rec_status_ = RecordStatus::Deleted;
                    break;
                }
                case RecordStatusType::UNDEFINED:
                {
                    read_result.rec_status_ = RecordStatus::Unknown;
                    break;
                }
                case RecordStatusType::VERSIONUNDEFIND:
                {
                    read_result.rec_status_ = RecordStatus::VersionUnknown;
                    break;
                }
                default:
                    break;
                }

                read_result.ts_ = read_res.ts();
                read_result.lock_type_ =
                    ToLocalType::ConvertLockType(read_res.lock_type());
                hd_res->SetFinished();
            }
        }
        msg_pool_.enqueue(std::move(msg));
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_PostCommitRequest:
    {
        assert(msg->has_postcommit_req());

        const PostCommitRequest &post_commit = msg->postcommit_req();
        const CceAddr_msg &cce_addr_msg = post_commit.cce_addr();

        if (!Sharder::Instance().CheckLeaderTerm(post_commit.node_group_id(),
                                                 cce_addr_msg.term()))
        {
            CcMessage return_msg;
            return_msg.set_tx_number(msg->tx_number());
            return_msg.set_handler_addr(msg->handler_addr());
            return_msg.set_tx_term(msg->tx_term());

            PostprocessResponse *resp = return_msg.mutable_post_resp();
            resp->set_error_code(1);

            CcStreamSender *cc_stream_sender =
                Sharder::Instance().GetCcStreamSender();
            cc_stream_sender->SendMessageToNode(post_commit.src_node_id(),
                                                return_msg);
            msg_pool_.enqueue(std::move(msg));
        }
        else
        {
            RemotePostWrite *post_commit = postwrite_pool_.NextRequest();
            TX_TRACE_ASSOCIATE(msg.get(), post_commit);
            post_commit->Reset(std::move(msg));
            post_commit->Ccm()->shard_->Enqueue(post_commit);
        }

        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_PostWriteAllRequest:
    {
        RemotePostWriteAll *post_write_all = post_write_all_pool_.NextRequest();
        TX_TRACE_ASSOCIATE(msg.get(), post_write_all);
        post_write_all->Reset(std::move(msg));
        local_shards_.EnqueueCcRequest(0, post_write_all);
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_ScanOpenRequest:
    {
        std::shared_ptr<CcHandlerResult<Void>> cc_res_{nullptr};
        RemoteScanOpen *scan_open_req = scan_open_pool_.NextRequest();
        uint32_t local_core_cnt = (uint32_t) local_shards_.Count();
        TX_TRACE_ASSOCIATE(msg.get(), scan_open_req);
        scan_open_req->Reset(std::move(msg), local_core_cnt);

        for (uint32_t core_id = 0; core_id < local_core_cnt; ++core_id)
        {
            // The scan open request is directed to all local shards. The
            // request pre-allocates scan caches, one for each shard. Each shard
            // fills its own designated cache, so there is no synchronization
            // across cores.
            local_shards_.EnqueueCcRequest(core_id, scan_open_req);
        }

        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_ScanOpenResponse:
    {
        /*message ScanOpenResponse
        {
            bool error = 1;
            uint32 shard_id = 2;
            repeated ScanCache_msg scan_cache = 3;
        }*/

        assert(msg->has_scan_open_resp());

        CcHandlerResult<ScanOpenResult> *hd_res = nullptr;
        uint32_t tx_node_id = (msg->tx_number() >> 32L) >> 10;
        int64_t tx_term = msg->tx_term();
        if (!Sharder::Instance().CheckLeaderTerm(tx_node_id, tx_term))
        {
            // The tx node has failed. Pointer stability does not hold anymore.
            msg_pool_.enqueue(std::move(msg));
            break;
        }
        else
        {
            hd_res = reinterpret_cast<CcHandlerResult<ScanOpenResult> *>(
                msg->handler_addr());

            if (hd_res->Txm()->TxNumber() != msg->tx_number() ||
                hd_res->Txm()->CommandId() != msg->command_id())
            {
                // The original tx has terminated and the tx machine has been
                // recycled. The response message is directed to an obsolete tx.
                // Skips setting the cc handler result.
                msg_pool_.enqueue(std::move(msg));
                break;
            }
        }

        const ScanOpenResponse &scan_open_res = msg->scan_open_resp();
        uint32_t ng_id = scan_open_res.node_group_id();
        hd_res->Value().cc_node_returned_[ng_id] = 1;

        // Even if ScanOpen operation fail, we should also move scan
        // result into read set to release acquired lock.
        {
            CcScanner &scanner = *hd_res->Value().scanner_;
            int64_t term = -1;

            for (int core_id = 0; core_id < scan_open_res.scan_cache_size();
                 ++core_id)
            {
                uint32_t shard_code = (ng_id << 10) + core_id;
                const ScanCache_msg &cache_msg =
                    scan_open_res.scan_cache(core_id);
                ScanCache *shard_cache = scanner.AddShard(shard_code);

                for (int idx = 0; idx < cache_msg.scan_tuple_size(); ++idx)
                {
                    const ScanTuple_msg &tuple_msg = cache_msg.scan_tuple(idx);
                    term = tuple_msg.cce_addr().term();

                    RecordStatus rec_status =
                        ToLocalType::ConvertRecordStatusType(
                            tuple_msg.rec_status());

                    shard_cache->AddScanTuple(tuple_msg.key(),
                                              tuple_msg.key_ts(),
                                              tuple_msg.record(),
                                              rec_status,
                                              tuple_msg.gap_ts(),
                                              tuple_msg.cce_addr().cce_ptr(),
                                              tuple_msg.cce_addr().term(),
                                              ng_id,
                                              scanner.is_ckpt_delta_);
                }
            }

            hd_res->Value().cc_node_terms_[ng_id] = term;
        }

        if (scan_open_res.error_code() != 0)
        {
            hd_res->SetError(
                ToLocalType::ConvertCcErrorCode(scan_open_res.error_code()));
        }
        else
        {
            hd_res->SetFinished();
        }

        msg_pool_.enqueue(std::move(msg));
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_ScanNextRequest:
    {
        RemoteScanNextBatch *scan_next_req = scan_next_pool_.NextRequest();
        TX_TRACE_ASSOCIATE(msg.get(), scan_next_req);
        scan_next_req->Reset(std::move(msg));
        scan_next_req->Ccm()->shard_->Enqueue(scan_next_req);
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_ScanNextResponse:
    {
        /*message ScanNextResponse
        {
            bool error = 1;
            repeated ScanTuple_msg scan_tuple = 2;
            uint64 ccm_ptr = 3;
            uint64 scan_cache_ptr = 4;
        }*/

        assert(msg->has_scan_next_resp());

        CcHandlerResult<ScanNextResult> *hd_res = nullptr;

        uint32_t tx_node_id = (msg->tx_number() >> 32L) >> 10;
        int64_t tx_term = msg->tx_term();
        if (!Sharder::Instance().CheckLeaderTerm(tx_node_id, tx_term))
        {
            // The tx node has failed. Pointer stability does not hold anymore.
            msg_pool_.enqueue(std::move(msg));
            break;
        }
        else
        {
            hd_res = reinterpret_cast<CcHandlerResult<ScanNextResult> *>(
                msg->handler_addr());

            if (hd_res->Txm()->TxNumber() != msg->tx_number() ||
                hd_res->Txm()->CommandId() != msg->command_id())
            {
                // The original tx has terminated and the tx machine has been
                // recycled. The response message is directed to an obsolete tx.
                // Skips setting the cc handler result.
                msg_pool_.enqueue(std::move(msg));
                break;
            }
        }

        const ScanNextResponse &scan_next_res = msg->scan_next_resp();

        // Even if ScanNext operation fail, we should also move scan
        // result into read set to release acquired lock.
        {
            ScanCache *shard_cache =
                reinterpret_cast<ScanCache *>(scan_next_res.scan_cache_ptr());

            uint32_t ng_id = shard_cache->LastTuple()->cce_addr_.NodeGroupId();
            shard_cache->Reset();

            for (int idx = 0; idx < scan_next_res.scan_tuple_size(); ++idx)
            {
                const ScanTuple_msg &tuple_msg = scan_next_res.scan_tuple(idx);
                hd_res->Value().term_ = tuple_msg.cce_addr().term();

                RecordStatus rec_status = ToLocalType::ConvertRecordStatusType(
                    tuple_msg.rec_status());

                shard_cache->AddScanTuple(tuple_msg.key(),
                                          tuple_msg.key_ts(),
                                          tuple_msg.record(),
                                          rec_status,
                                          tuple_msg.gap_ts(),
                                          tuple_msg.cce_addr().cce_ptr(),
                                          tuple_msg.cce_addr().term(),
                                          ng_id);
            }
        }

        if (scan_next_res.error_code() != 0)
        {
            hd_res->SetError(
                ToLocalType::ConvertCcErrorCode(scan_next_res.error_code()));
        }
        else
        {
            hd_res->SetFinished();
        }

        msg_pool_.enqueue(std::move(msg));
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_FaultInjectRequest:
    {
        RemoteFaultInjectCC *fault_inject_req =
            fault_inject_pool_.NextRequest();
        TX_TRACE_ASSOCIATE(msg.get(), fault_inject_req);
        fault_inject_req->Reset(std::move(msg));
        local_shards_.EnqueueCcRequest(0, fault_inject_req);

        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_FaultInjectResponse:
    {
        assert(msg->has_fault_inject_resp());

        uint32_t tx_node_id = (msg->tx_number() >> 32L) >> 10;

        int64_t tx_term = msg->tx_term();
        if (!Sharder::Instance().CheckLeaderTerm(tx_node_id, tx_term))
        {
            // The tx node has failed. Pointer stability does not hold anymore.
            msg_pool_.enqueue(std::move(msg));
            break;
        }
        CcHandlerResult<bool> *hd_res =
            reinterpret_cast<CcHandlerResult<bool> *>(msg->handler_addr());

        const FaultInjectResponse &fi_res = msg->fault_inject_resp();

        if (fi_res.error_code() != 0)
        {
            hd_res->SetError(
                ToLocalType::ConvertCcErrorCode(fi_res.error_code()));
        }
        else
        {
            hd_res->SetFinished();
        }

        msg_pool_.enqueue(std::move(msg));
        break;
    }
    case CcMessage::MessageType::
        CcMessage_MessageType_CleanCcEntryForTestRequest:
    {
        RemoteCleanCcEntryForTestCc *clean_req =
            clean_cc_entry_pool_.NextRequest();
        TX_TRACE_ASSOCIATE(msg.get(), clean_req);
        clean_req->Reset(std::move(msg));
        local_shards_.EnqueueCcRequest(0, clean_req);

        break;
    }
    case CcMessage::MessageType::
        CcMessage_MessageType_CleanCcEntryForTestResponse:
    {
        assert(msg->has_clean_cc_entry_resp());

        uint32_t tx_node_id = (msg->tx_number() >> 32L) >> 10;

        int64_t tx_term = msg->tx_term();
        if (!Sharder::Instance().CheckLeaderTerm(tx_node_id, tx_term))
        {
            // The tx node has failed. Pointer stability does not hold anymore.
            msg_pool_.enqueue(std::move(msg));
            break;
        }
        CcHandlerResult<bool> *hd_res =
            reinterpret_cast<CcHandlerResult<bool> *>(msg->handler_addr());

        const CleanCcEntryForTestResponse &clean_res =
            msg->clean_cc_entry_resp();

        if (clean_res.error_code() != 0)
        {
            hd_res->SetError(
                ToLocalType::ConvertCcErrorCode(clean_res.error_code()));
        }
        else
        {
            hd_res->SetFinished();
        }

        msg_pool_.enqueue(std::move(msg));
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_RecoverStateCheckRequest:
    {
        // this request is only used during cluster initialization.
        const RecoverStateCheckRequest &req = msg->recover_state_check_req();

        CcMessage send_msg;

        send_msg.set_type(CcMessage::MessageType::
                              CcMessage_MessageType_RecoverStateCheckResponse);

        RecoverStateCheckResponse *recover_resp =
            send_msg.mutable_recover_state_check_resp();

        // error_code is set to -1 if the log replay is not finished.
        if (Sharder::Instance().LeaderTerm(req.node_group_id()) > 0)
        {
            recover_resp->set_error_code(0);
        }
        else
        {
            recover_resp->set_error_code(-1);
        }

        recover_resp->set_node_group_id(req.node_group_id());

        Sharder::Instance().GetCcStreamSender()->SendMessageToNode(
            req.src_node_id(), send_msg);

        break;
    }
    case CcMessage::MessageType::
        CcMessage_MessageType_RecoverStateCheckResponse:
    {
        // this response is only used during cluster initialization.
        const RecoverStateCheckResponse &resp = msg->recover_state_check_resp();

        if (resp.error_code() == 0)
        {
            Sharder::Instance().RemoteNodeFinishRecovery(resp.node_group_id());
        }
        break;
    }
    case CcMessage::MessageType::
        CcMessage_MessageType_BroadcastStatisticsRequest:
    {
        const BroadcastStatisticsRequest &req = msg->broadcast_statistics_req();
        TableName table_name(req.table_name_str(), TableType::Primary);
        NodeGroupId node_group_id = req.node_group_id();
        const std::string &statistics_binary = req.statistics_binary();

        const CatalogEntry *catalog_entry =
            local_shards_.GetCatalog(table_name, node_group_id);
        if (catalog_entry)
        {
            catalog_entry->schema_->StatisticsObject()->Reset(statistics_binary,
                                                              true);
        }
        break;
    }
    default:
        break;
    }
}

}  // namespace remote
}  // namespace txservice
