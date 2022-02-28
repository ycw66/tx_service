#include "remote/cc_stream_receiver.h"

#include <brpc/controller.h>

#include "cc/local_cc_shards.h"
#include "sharder.h"
#include "tx_execution.h"

namespace txservice
{
namespace remote
{
CcStreamReceiver::CcStreamReceiver(
    LocalCcShards &local_shards,
    moodycamel::ConcurrentQueue<std::unique_ptr<CcMessage>> &msg_pool)
    : local_shards_(local_shards), msg_pool_(msg_pool)
{
}

CcStreamReceiver::~CcStreamReceiver()
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
    switch (msg->type())
    {
    case CcMessage::MessageType::CcMessage_MessageType_AcquireRequest:
    {
        RemoteAcquire *acquire_req = acquire_pool_.NextRequest();
        acquire_req->Set(std::move(msg));
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

        CcHandlerResult<AcquireKeyResult> *hd_res = nullptr;

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
            hd_res = reinterpret_cast<CcHandlerResult<AcquireKeyResult> *>(
                msg->handler_addr());

            if (hd_res->Txm()->TxNumber() != msg->tx_number())
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
        AcquireKeyResult &acq_res = hd_res->Value();

        if (cc_res.error_code() != 0)
        {
            if (acq_res.cce_addr_.Term() < 0)
            {
                acq_res.remote_ack_cnt_->fetch_sub(1);
            }

            hd_res->SetError(cc_res.error_code());
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

                acq_res.remote_ack_cnt_->fetch_sub(1);
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
        acquire_all_req->Set(std::move(msg));
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

            if (hd_res->Txm()->TxNumber() != msg->tx_number())
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
            hd_res->SetError(cc_res.error_code());
        }
        else
        {
            if (acq_all_res.node_term_ < 0)
            {
                acq_all_res.node_term_ = cc_res.node_term();
                acq_all_res.remote_ack_cnt_->fetch_sub(1);
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
        RemotePostRead *vali_req = postread_pool_.NextRequest();
        vali_req->Set(std::move(msg));
        vali_req->Ccm()->shard_->Enqueue(vali_req);
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_ValidateResponse:
    {
        assert(msg->has_validate_resp());

        CcHandlerResult<std::vector<TxId>> *hd_res = nullptr;

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
            hd_res = reinterpret_cast<CcHandlerResult<std::vector<TxId>> *>(
                msg->handler_addr());

            if (hd_res->Txm()->TxNumber() != msg->tx_number())
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
            hd_res->SetError(cc_res.error_code());
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
                hd_res->SetError(1);
            }
        }
        msg_pool_.enqueue(std::move(msg));
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_PostprocessResponse:
    {
        assert(msg->has_post_resp());

        CcHandlerResult<Void> *hd_res = nullptr;

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
            hd_res =
                reinterpret_cast<CcHandlerResult<Void> *>(msg->handler_addr());

            if (hd_res->Txm()->TxNumber() != msg->tx_number())
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
            hd_res->SetError(cc_res.error_code());
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
        read->Set(std::move(msg));
        local_shards_.EnqueueCcRequest(read->KeyShardCode(), read);
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_ReadOutsideRequest:
    {
        RemoteReadOutside *read_outside = read_outside_pool_.NextRequest();
        read_outside->Set(std::move(msg));

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

            if (hd_res->Txm()->TxNumber() != msg->tx_number())
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
            hd_res->SetError(read_res.error_code());
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
                case ReadResponse::RecordStatus::
                    ReadResponse_RecordStatus_NORMAL:
                {
                    read_result.rec_status_ = RecordStatus::Normal;

                    size_t offset = 0;
                    read_result.rec_->Deserialize(read_res.record().data(),
                                                  offset);

                    break;
                }
                case ReadResponse::RecordStatus::
                    ReadResponse_RecordStatus_DELETED:
                {
                    read_result.rec_status_ = RecordStatus::Deleted;
                    break;
                }
                case ReadResponse::RecordStatus::
                    ReadResponse_RecordStatus_UNKNOWN:
                {
                    read_result.rec_status_ = RecordStatus::Unknown;
                    break;
                }
                default:
                    break;
                }

                read_result.ts_ = read_res.ts();
                hd_res->SetFinished();
            }
        }
        msg_pool_.enqueue(std::move(msg));
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_PostCommitRequest:
    {
        RemotePostWrite *post_commit = postwrite_pool_.NextRequest();
        post_commit->Set(std::move(msg));
        post_commit->Ccm()->shard_->Enqueue(post_commit);
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_PostWriteAllRequest:
    {
        RemotePostWriteAll *post_write_all = post_write_all_pool_.NextRequest();
        post_write_all->Set(std::move(msg));
        local_shards_.EnqueueCcRequest(0, post_write_all);
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_ScanOpenRequest:
    {
        RemoteScanOpen *scan_open_req = scan_open_pool_.NextRequest();
        uint32_t local_core_cnt = (uint32_t) local_shards_.Count();
        scan_open_req->Set(std::move(msg), local_core_cnt);

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

            if (hd_res->Txm()->TxNumber() != msg->tx_number())
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

        if (scan_open_res.error_code() != 0)
        {
            hd_res->SetError(scan_open_res.error_code());
        }
        else
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

                    RecordStatus rec_status;
                    switch (tuple_msg.rec_status())
                    {
                    case ScanTuple_msg::RecordStatus::
                        ScanTuple_msg_RecordStatus_NORMAL:
                        rec_status = RecordStatus::Normal;
                        break;
                    case ScanTuple_msg::RecordStatus::
                        ScanTuple_msg_RecordStatus_DELETED:
                        rec_status = RecordStatus::Deleted;
                        break;
                    case ScanTuple_msg::RecordStatus::
                        ScanTuple_msg_RecordStatus_UNKNOWN:
                        rec_status = RecordStatus::Unknown;
                        break;
                    default:
                        rec_status = RecordStatus::Normal;
                        break;
                    }

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
            hd_res->SetFinished();
        }

        msg_pool_.enqueue(std::move(msg));
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_ScanNextRequest:
    {
        RemoteScanNextBatch *scan_next_req = scan_next_pool_.NextRequest();
        scan_next_req->Set(std::move(msg));
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

            if (hd_res->Txm()->TxNumber() != msg->tx_number())
            {
                // The original tx has terminated and the tx machine has been
                // recycled. The response message is directed to an obsolete tx.
                // Skips setting the cc handler result.
                msg_pool_.enqueue(std::move(msg));
                break;
            }
        }

        const ScanNextResponse &scan_next_res = msg->scan_next_resp();

        if (scan_next_res.error_code() != 0)
        {
            hd_res->SetError(scan_next_res.error_code());
        }
        else
        {
            ScanCache *shard_cache =
                reinterpret_cast<ScanCache *>(scan_next_res.scan_cache_ptr());

            uint32_t ng_id = shard_cache->LastTuple()->cce_addr_.NodeGroupId();
            shard_cache->Reset();

            for (int idx = 0; idx < scan_next_res.scan_tuple_size(); ++idx)
            {
                const ScanTuple_msg &tuple_msg = scan_next_res.scan_tuple(idx);
                hd_res->Value().term_ = tuple_msg.cce_addr().term();

                RecordStatus rec_status;
                switch (tuple_msg.rec_status())
                {
                case ScanTuple_msg::RecordStatus::
                    ScanTuple_msg_RecordStatus_NORMAL:
                    rec_status = RecordStatus::Normal;
                    break;
                case ScanTuple_msg::RecordStatus::
                    ScanTuple_msg_RecordStatus_DELETED:
                    rec_status = RecordStatus::Deleted;
                    break;
                case ScanTuple_msg::RecordStatus::
                    ScanTuple_msg_RecordStatus_UNKNOWN:
                    rec_status = RecordStatus::Unknown;
                    break;
                default:
                    rec_status = RecordStatus::Normal;
                    break;
                }

                shard_cache->AddScanTuple(tuple_msg.key(),
                                          tuple_msg.key_ts(),
                                          tuple_msg.record(),
                                          rec_status,
                                          tuple_msg.gap_ts(),
                                          tuple_msg.cce_addr().cce_ptr(),
                                          tuple_msg.cce_addr().term(),
                                          ng_id);
            }

            hd_res->SetFinished();
        }

        msg_pool_.enqueue(std::move(msg));
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_CommitSkRequest:
    {
        RemoteCommitSk *commit_sk_req = commit_sk_pool_.NextRequest();
        commit_sk_req->Set(std::move(msg));
        local_shards_.EnqueueCcRequest(commit_sk_req->KeyShardCode(),
                                       commit_sk_req);
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_FaultInjectRequest:
    {
        RemoteFaultInjectCC *fault_inject_req =
            fault_inject_pool_.NextRequest();
        fault_inject_req->Set(std::move(msg));
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
            hd_res->SetError(fi_res.error_code());
        }
        else
        {
            hd_res->SetFinished();
        }

        msg_pool_.enqueue(std::move(msg));
        break;
    }
    default:
        break;
    }
}

IsolationLevel CcStreamReceiver::ConvertIsolation(IsolationType iso_type)
{
    switch (iso_type)
    {
    case IsolationType::ReadCommitted:
        return IsolationLevel::ReadCommitted;
    case IsolationType::SnapshotIsolation:
        return IsolationLevel::Snapshot;
    case IsolationType::RepeatableRead:
        return IsolationLevel::RepeatableRead;
    case IsolationType::Serializable:
        return IsolationLevel::Serializable;
    default:
        return IsolationLevel::ReadCommitted;
    }
}

CcProtocol CcStreamReceiver::ConvertProtocol(CcProtocolType proto)
{
    if (proto == CcProtocolType::Locking)
    {
        return CcProtocol::Locking;
    }
    else
    {
        return CcProtocol::OCC;
    }
}

PostWriteType CcStreamReceiver::ConvertCommitType(CommitType commit_type)
{
    if (commit_type == CommitType::PrepareCommit)
    {
        return PostWriteType::PrepareCommit;
    }
    else
    {
        return PostWriteType::PostCommit;
    }
}
}  // namespace remote
}  // namespace txservice