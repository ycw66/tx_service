#include "remote_cc_handler.h"

#include <iostream>

#include "cc/local_cc_shards.h"
#include "tx_execution.h"

#ifdef _MSC_VER
#include "remote/remote_cc_handler_win.h"
#else
#include "remote/remote_cc_handler_brpc.h"
#endif

void txservice::remote::RemoteCcHandler::OnReceiveCcMsg(
    std::unique_ptr<CcMessage> msg)
{
    switch (msg->type())
    {
    case CcMessage::MessageType::CcMessage_MessageType_AcquireRequest:
    {
        RemoteAcquire *acquire_req = acquire_pool_.NextRequest();
        acquire_req->Set(std::move(msg), this);
        local_shards_->EnqueueCcRequest(acquire_req->KeyShardCode(),
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

        CcHandlerResult<std::pair<uint64_t, CcEntryAddr>> *hd_res = nullptr;

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
                CcHandlerResult<std::pair<uint64_t, CcEntryAddr>> *>(
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

        if (cc_res.error_code() != 0)
        {
            hd_res->SetError(cc_res.error_code());
        }
        else
        {
            std::get<0>(hd_res->Value()) = cc_res.vali_ts();

            CcEntryAddr &cce_addr = std::get<1>(hd_res->Value());
            if (cce_addr_res.entry_ptr_case() ==
                CceAddr_msg::EntryPtrCase::kInsertPtr)
            {
                cce_addr.SetInsert(cce_addr_res.insert_ptr(),
                                   cce_addr_res.term());
            }
            else
            {
                cce_addr.SetCce(cce_addr_res.cce_ptr(), cce_addr_res.term());
            }
            hd_res->SetFinished();
        }

        msg_pool_.enqueue(std::move(msg));
        break;
    }
    case CcMessage::MessageType::
        CcMessage_MessageType_AcquireTableWriteLockRequest:
    {
        RemoteAcquireTableWriteLockCC *acquire_table_req =
            acquire_table_write_lock_pool.NextRequest();

        // record the node term when acquiring the table write lock. For
        // ccnode with multiple ccshards, we only get the term once.
        // the node term is used by log service to indicate the state of node
        // which holds the table lock.
        int64_t ng_term =
            Sharder::Instance().LeaderTerm(local_shards_->NodeId());
        if (ng_term < 0)
        {
            acquire_table_req->Set(std::move(msg), this, 1, 0);
            acquire_table_req->Result()->SetError(-1);
        }
        else
        {
            uint32_t local_core_cnt = (uint32_t) local_shards_->Count();

            acquire_table_req->Set(
                std::move(msg), this, local_core_cnt, ng_term);

            for (uint32_t core_id = 0; core_id < local_core_cnt; ++core_id)
            {
                // The acquire table write lock request is directed to all local
                // shards.
                local_shards_->EnqueueCcRequest(core_id, acquire_table_req);
            }
        }
        break;
    }
    case CcMessage::MessageType::
        CcMessage_MessageType_AcquireTableWriteLockResponse:
    {
        assert(msg->has_acquire_table_resp());
        CcHandlerResult<std::unordered_map<uint32_t, int64_t>> *hd_res =
            nullptr;

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
                CcHandlerResult<std::unordered_map<uint32_t, int64_t>> *>(
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

        const AcquireTableWriteLockResponse &cc_res = msg->acquire_table_resp();

        std::unordered_map<uint32_t, int64_t> &term_map = hd_res->Value();
        term_map.try_emplace(cc_res.node_id(), cc_res.term());

        if (cc_res.error_code() == 0)
        {
            hd_res->SetFinished();
        }
        else
        {
            hd_res->SetError(cc_res.error_code());
        }

        msg_pool_.enqueue(std::move(msg));
        break;
    }
    case CcMessage::MessageType::
        CcMessage_MessageType_ReleaseTableWriteLockRequest:
    {
        RemoteReleaseTableWriteLock *release_table_req =
            release_table_write_lock_pool.NextRequest();
        uint32_t local_core_cnt = (uint32_t) local_shards_->Count();
        release_table_req->Set(std::move(msg), this, local_core_cnt);

        for (uint32_t core_id = 0; core_id < local_core_cnt; ++core_id)
        {
            // The release table write lock request is directed to all local
            // shards.
            local_shards_->EnqueueCcRequest(core_id, release_table_req);
        }
        break;
    }
    case CcMessage::MessageType::
        CcMessage_MessageType_ReleaseTableWriteLockResponse:
    {
        assert(msg->has_release_table_resp());
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

        const ReleaseTableWriteLockResponse &cc_res = msg->release_table_resp();

        if (cc_res.error_code() == 0)
        {
            hd_res->SetFinished();
        }
        else
        {
            hd_res->SetError(cc_res.error_code());
        }

        msg_pool_.enqueue(std::move(msg));
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_CommitCreateTableRequest:
    {
        RemoteCommitCreateTable *commit_create_table_req =
            commit_create_table_pool.NextRequest();
        uint32_t local_core_cnt = (uint32_t) local_shards_->Count();
        commit_create_table_req->Set(std::move(msg), this, local_core_cnt);

        for (uint32_t core_id = 0; core_id < local_core_cnt; ++core_id)
        {
            local_shards_->EnqueueCcRequest(core_id, commit_create_table_req);
        }

        break;
    }
    case CcMessage::MessageType::
        CcMessage_MessageType_CommitCreateTableResponse:
    {
        assert(msg->has_commit_create_table_resp());

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

        const CommitCreateTableResponse &cc_res =
            msg->commit_create_table_resp();

        if (cc_res.error_code() == 0)
        {
            hd_res->SetFinished();
        }
        else
        {
            hd_res->SetError(cc_res.error_code());
        }

        msg_pool_.enqueue(std::move(msg));
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_CommitDropTableRequest:
    {
        RemoteCommitDropTable *commit_drop_table_req =
            commit_drop_table_pool.NextRequest();
        uint32_t local_core_cnt = (uint32_t) local_shards_->Count();
        commit_drop_table_req->Set(std::move(msg), this, local_core_cnt);

        for (uint32_t core_id = 0; core_id < local_core_cnt; ++core_id)
        {
            local_shards_->EnqueueCcRequest(core_id, commit_drop_table_req);
        }
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_CommitDropTableResponse:
    {
        assert(msg->has_commit_drop_table_resp());

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

        const CommitDropTableResponse &cc_res = msg->commit_drop_table_resp();

        if (cc_res.error_code() == 0)
        {
            hd_res->SetFinished();
        }
        else
        {
            hd_res->SetError(cc_res.error_code());
        }

        msg_pool_.enqueue(std::move(msg));
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_ValidateRequest:
    {
        RemoteValidate *vali_req = vali_pool_.NextRequest();
        vali_req->Set(std::move(msg), this);
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
    case CcMessage::MessageType::CcMessage_MessageType_PostReadRequest:
    {
        RemotePostRead *post_read = postread_pool_.NextRequest();
        post_read->Set(std::move(msg), this);
        post_read->Ccm()->shard_->Enqueue(post_read);
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
        read->Set(std::move(msg), this);
        local_shards_->EnqueueCcRequest(read->KeyShardCode(), read);
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_ReadOutsideRequest:
    {
        RemoteReadOutside *read_outside = read_outside_pool_.NextRequest();
        read_outside->Set(std::move(msg), this);

        const CcEntryAddr &cce_addr = read_outside->cce_addr_;
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

        CcHandlerResult<
            std::tuple<TxRecord *, uint64_t, CcEntryAddr, RecordStatus>>
            *hd_res = nullptr;

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
            hd_res = reinterpret_cast<CcHandlerResult<
                std::tuple<TxRecord *, uint64_t, CcEntryAddr, RecordStatus>> *>(
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
            switch (read_res.rec_status())
            {
            case ReadResponse::RecordStatus::ReadResponse_RecordStatus_NORMAL:
            {
                std::get<3>(hd_res->Value()) = RecordStatus::Normal;

                TxRecord *&rec = std::get<0>(hd_res->Value());
                size_t offset = 0;
                rec->Deserialize(read_res.record().data(), offset);

                break;
            }
            case ReadResponse::RecordStatus::ReadResponse_RecordStatus_DELETED:
            {
                std::get<3>(hd_res->Value()) = RecordStatus::Deleted;
                break;
            }
            case ReadResponse::RecordStatus::ReadResponse_RecordStatus_UNKNOWN:
            {
                std::get<3>(hd_res->Value()) = RecordStatus::Unknown;
                break;
            }
            default:
                break;
            }

            std::get<1>(hd_res->Value()) = read_res.ts();

            CcEntryAddr &cce_addr = std::get<2>(hd_res->Value());
            const CceAddr_msg &cce_addr_msg = read_res.cce_addr();
            cce_addr.SetCce(cce_addr_msg.cce_ptr(), cce_addr_msg.term());
            // CC entry's shard Id has been set when the read request was sent.

            hd_res->SetFinished();
        }
        msg_pool_.enqueue(std::move(msg));
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_PostCommitRequest:
    {
        RemotePostCommit *post_commit = postcommit_pool_.NextRequest();
        post_commit->Set(std::move(msg), this);
        post_commit->Ccm()->shard_->Enqueue(post_commit);
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_PostDeleteRequest:
    {
        RemotePostDelete *post_del = postdel_pool_.NextRequest();
        post_del->Set(std::move(msg), this);
        post_del->Ccm()->shard_->Enqueue(post_del);
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_ScanOpenRequest:
    {
        RemoteScanOpen *scan_open_req = scan_open_pool_.NextRequest();
        uint32_t local_core_cnt = (uint32_t) local_shards_->Count();
        scan_open_req->Set(std::move(msg), this, local_core_cnt);

        for (uint32_t core_id = 0; core_id < local_core_cnt; ++core_id)
        {
            // The scan open request is directed to all local shards. The
            // request pre-allocates scan caches, one for each shard. Each shard
            // fills its own designated cache, so there is no synchronization
            // across cores.
            local_shards_->EnqueueCcRequest(core_id, scan_open_req);
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

        CcHandlerResult<std::pair<size_t, std::unique_ptr<CcScanner>>> *hd_res =
            nullptr;

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
            hd_res = reinterpret_cast<CcHandlerResult<
                std::pair<size_t, std::unique_ptr<CcScanner>>> *>(
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

        if (scan_open_res.error_code() != 0)
        {
            hd_res->SetError(scan_open_res.error_code());
        }
        else
        {
            CcScanner &scanner = *hd_res->Value().second;
            uint32_t ng_id = scan_open_res.node_group_id();

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

            hd_res->SetFinished();
        }

        msg_pool_.enqueue(std::move(msg));
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_ScanNextRequest:
    {
        RemoteScanNextBatch *scan_next_req = scan_next_pool_.NextRequest();
        scan_next_req->Set(std::move(msg), this);
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

        CcHandlerResult<uint32_t> *hd_res = nullptr;

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
            hd_res = reinterpret_cast<CcHandlerResult<uint32_t> *>(
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
        commit_sk_req->Set(std::move(msg), this);
        local_shards_->EnqueueCcRequest(commit_sk_req->KeyShardCode(),
                                        commit_sk_req);
        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_FaultInjectRequest:
    {
        RemoteFaultInjectCC *fault_inject_req =
            fault_inject_pool_.NextRequest();
        fault_inject_req->Set(std::move(msg), this);
        local_shards_->EnqueueCcRequest(0, fault_inject_req);

        break;
    }
    case CcMessage::MessageType::CcMessage_MessageType_FaultInjectResponse:
    {
        assert(msg->has_fault_inject_resp());

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

void txservice::remote::RemoteCcHandler::AcquireWrite(
    const TableName &table_name,
    const TxKey &key,
    uint32_t key_shard_code,
    const TxId &txid,
    int64_t tx_term,
    uint64_t ts,
    bool is_insert,
    CcHandlerResult<std::pair<uint64_t, CcEntryAddr>> &hres,
    const CcProtocol proto)
{
    /*message AcquireRequest
    {
        uint32 src_node_id = 1;
        string tablename = 3;
        string key = 4;
        uint32 key_shard_code = 5;
        uint32 vec_idx = 6;
        uint64 ts = 7;
        bool insert = 8;
    }*/

    std::unique_ptr<CcMessage> send_msg = GetCcMsg();

    send_msg->set_type(
        CcMessage::MessageType::CcMessage_MessageType_AcquireRequest);
    send_msg->set_tx_number(txid.TxNumber());
    send_msg->set_handler_addr(reinterpret_cast<uint64_t>(&hres));
    send_msg->set_tx_term(tx_term);

    AcquireRequest *acq = send_msg->mutable_acquire_req();
    acq->set_src_node_id(local_shards_->node_id_);
    acq->set_tablename(table_name);
    acq->clear_key();
    key.Serialize(*acq->mutable_key());

    acq->set_vec_idx(txid.VecIdx());
    acq->set_ts(ts);
    acq->set_insert(is_insert);
    acq->set_key_shard_code(key_shard_code);

    bool success = SendRequest(key_shard_code >> 10, *send_msg);

    send_msg->clear_type();
    send_msg->clear_tx_number();
    send_msg->clear_handler_addr();
    send_msg->clear_acquire_req();

    msg_pool_.enqueue(std::move(send_msg));

    if (!success)
    {
        hres.SetError(-1);
    }
}

void txservice::remote::RemoteCcHandler::AcquireTableWriteLock(
    const TableName &table_name,
    const TxId &txid,
    int64_t tx_term,
    uint64_t tx_number,
    uint32_t node_group_id,
    CcHandlerResult<std::unordered_map<uint32_t, int64_t>> &hres)
{
    std::unique_ptr<CcMessage> send_msg = GetCcMsg();

    send_msg->set_type(CcMessage::MessageType::
                           CcMessage_MessageType_AcquireTableWriteLockRequest);
    send_msg->set_tx_number(txid.TxNumber());
    send_msg->set_handler_addr(reinterpret_cast<uint64_t>(&hres));
    send_msg->set_tx_term(tx_term);

    AcquireTableWriteLockRequest *acq = send_msg->mutable_acquire_table_req();
    acq->set_src_node_id(local_shards_->node_id_);
    acq->set_tablename(table_name);

    acq->set_vec_idx(txid.VecIdx());
    acq->set_tx_number(tx_number);
    acq->set_node_group_id(node_group_id);

    bool success = SendRequest(node_group_id, *send_msg);

    send_msg->clear_type();
    send_msg->clear_tx_number();
    send_msg->clear_handler_addr();
    send_msg->clear_acquire_req();

    msg_pool_.enqueue(std::move(send_msg));

    if (!success)
    {
        hres.SetError(-1);
    }
}

void txservice::remote::RemoteCcHandler::ReleaseTableWriteLock(
    const TableName &table_name,
    const TxId &txid,
    int64_t tx_term,
    uint64_t tx_number,
    uint32_t node_group_id,
    CcHandlerResult<Void> &hres)
{
    std::unique_ptr<CcMessage> send_msg = GetCcMsg();

    send_msg->set_type(CcMessage::MessageType::
                           CcMessage_MessageType_ReleaseTableWriteLockRequest);
    send_msg->set_tx_number(txid.TxNumber());
    send_msg->set_handler_addr(reinterpret_cast<uint64_t>(&hres));
    send_msg->set_tx_term(tx_term);

    ReleaseTableWriteLockRequest *acq = send_msg->mutable_release_table_req();
    acq->set_src_node_id(local_shards_->node_id_);
    acq->set_tablename(table_name);

    acq->set_vec_idx(txid.VecIdx());
    acq->set_tx_number(tx_number);
    acq->set_node_group_id(node_group_id);

    bool success = SendRequest(node_group_id, *send_msg);

    send_msg->clear_type();
    send_msg->clear_tx_number();
    send_msg->clear_handler_addr();
    send_msg->clear_acquire_req();

    msg_pool_.enqueue(std::move(send_msg));

    if (!success)
    {
        hres.SetError(-1);
    }
}

void txservice::remote::RemoteCcHandler::ReleaseWrite(
    uint64_t tx_number,
    int64_t tx_term,
    const CcEntryAddr &cce_addr,
    CcHandlerResult<Void> &hd_res)
{
    std::unique_ptr<CcMessage> send_msg = GetCcMsg();

    send_msg->set_type(
        CcMessage::MessageType::CcMessage_MessageType_PostDeleteRequest);
    send_msg->set_tx_number(tx_number);
    send_msg->set_handler_addr(reinterpret_cast<uint64_t>(&hd_res));
    send_msg->set_tx_term(tx_term);

    PostDeleteRequest *post_del = send_msg->mutable_postdelete_req();
    post_del->set_src_node_id(local_shards_->node_id_);
    post_del->set_node_group_id(cce_addr.NodeGroupId());
    CceAddr_msg *cce_addr_msg = post_del->mutable_cce_addr();
    cce_addr_msg->set_cce_ptr(cce_addr.CcePtr());
    cce_addr_msg->set_term(cce_addr.Term());

    bool success = SendRequest(cce_addr.NodeGroupId(), *send_msg);

    send_msg->clear_type();
    send_msg->clear_tx_number();
    send_msg->clear_handler_addr();
    send_msg->clear_postdelete_req();

    msg_pool_.enqueue(std::move(send_msg));

    if (!success)
    {
        hd_res.SetError(-1);
    }
}

void txservice::remote::RemoteCcHandler::CommitWrite(
    uint64_t tx_number,
    int64_t tx_term,
    uint64_t commit_ts,
    const CcEntryAddr &cce_addr,
    const TxRecord &record,
    bool is_deleted,
    CcHandlerResult<Void> &hres)
{
    std::unique_ptr<CcMessage> send_msg = GetCcMsg();

    send_msg->set_type(
        CcMessage::MessageType::CcMessage_MessageType_PostCommitRequest);
    send_msg->set_tx_number(tx_number);
    send_msg->set_handler_addr(reinterpret_cast<uint64_t>(&hres));
    send_msg->set_tx_term(tx_term);

    PostCommitRequest *post_commit = send_msg->mutable_postcommit_req();
    post_commit->set_src_node_id(local_shards_->node_id_);
    post_commit->set_node_group_id(cce_addr.NodeGroupId());
    CceAddr_msg *cce_addr_msg = post_commit->mutable_cce_addr();
    if (cce_addr.CcePtr() != 0)
    {
        cce_addr_msg->set_cce_ptr(cce_addr.CcePtr());
        cce_addr_msg->set_term(cce_addr.Term());
    }
    else
    {
        cce_addr_msg->set_insert_ptr(cce_addr.InsertPtr());
        cce_addr_msg->set_term(cce_addr.Term());
    }

    post_commit->clear_record();
    if (!is_deleted)
    {
        record.Serialize(*post_commit->mutable_record());
    }

    post_commit->set_commit_ts(commit_ts);
    post_commit->set_is_deleted(is_deleted);

    bool success = SendRequest(cce_addr.NodeGroupId(), *send_msg);

    send_msg->clear_type();
    send_msg->clear_tx_number();
    send_msg->clear_handler_addr();
    send_msg->clear_postcommit_req();

    msg_pool_.enqueue(std::move(send_msg));

    if (!success)
    {
        hres.SetError(-1);
    }
}

void txservice::remote::RemoteCcHandler::ValidateRead(
    uint64_t tx_number,
    int64_t tx_term,
    uint64_t key_ts,
    uint64_t gap_ts,
    uint64_t commit_ts,
    const CcEntryAddr &cce_addr,
    CcHandlerResult<std::vector<TxId>> &hres)
{
    std::unique_ptr<CcMessage> send_msg = GetCcMsg();

    send_msg->set_type(
        CcMessage::MessageType::CcMessage_MessageType_ValidateRequest);
    send_msg->set_tx_number(tx_number);
    send_msg->set_handler_addr(reinterpret_cast<uint64_t>(&hres));
    send_msg->set_tx_term(tx_term);

    ValidateRequest *vali = send_msg->mutable_validate_req();
    vali->set_src_node_id(local_shards_->node_id_);
    vali->set_node_group_id(cce_addr.NodeGroupId());
    CceAddr_msg *cce_addr_msg = vali->mutable_cce_addr();
    cce_addr_msg->set_cce_ptr(cce_addr.CcePtr());
    cce_addr_msg->set_term(cce_addr.Term());
    vali->set_commit_ts(commit_ts);
    vali->set_key_ts(key_ts);
    vali->set_gap_ts(gap_ts);

    bool success = SendRequest(cce_addr.NodeGroupId(), *send_msg);

    send_msg->clear_type();
    send_msg->clear_tx_number();
    send_msg->clear_handler_addr();
    send_msg->clear_validate_req();

    msg_pool_.enqueue(std::move(send_msg));

    if (!success)
    {
        hres.SetError(-1);
    }
}

void txservice::remote::RemoteCcHandler::PostprocessRead(
    uint64_t tx_number,
    int64_t tx_term,
    const CcEntryAddr &cce_addr,
    CcHandlerResult<Void> &hres,
    CcProtocol proto)
{
    std::unique_ptr<CcMessage> send_msg = GetCcMsg();

    send_msg->set_type(
        CcMessage::MessageType::CcMessage_MessageType_PostReadRequest);
    send_msg->set_tx_number(tx_number);
    send_msg->set_handler_addr(reinterpret_cast<uint64_t>(&hres));
    send_msg->set_tx_term(tx_term);

    PostReadRequest *post_read = send_msg->mutable_postread_req();
    post_read->set_src_node_id(local_shards_->node_id_);
    post_read->set_node_group_id(cce_addr.NodeGroupId());
    CceAddr_msg *cce_addr_msg = post_read->mutable_cce_addr();
    cce_addr_msg->set_cce_ptr(cce_addr.CcePtr());
    cce_addr_msg->set_term(cce_addr.Term());

    bool success = SendRequest(cce_addr.NodeGroupId(), *send_msg);

    send_msg->clear_type();
    send_msg->clear_tx_number();
    send_msg->clear_handler_addr();
    send_msg->clear_postread_req();

    msg_pool_.enqueue(std::move(send_msg));

    if (!success)
    {
        hres.SetError(-1);
    }
}

void txservice::remote::RemoteCcHandler::CommitCreateTable(
    const TableName &table_name,
    std::string catalog_str,
    const TxId &txid,
    uint64_t ts,
    uint32_t node_group_id,
    CcHandlerResult<Void> &hres)
{
    std::unique_ptr<CcMessage> send_msg = GetCcMsg();

    send_msg->set_type(
        CcMessage::MessageType::CcMessage_MessageType_CommitCreateTableRequest);
    send_msg->set_tx_number(txid.TxNumber());
    send_msg->set_handler_addr(reinterpret_cast<uint64_t>(&hres));

    CommitCreateTableRequest *acq = send_msg->mutable_commit_create_table_req();
    acq->set_src_node_id(local_shards_->node_id_);
    acq->set_tablename(table_name);

    acq->set_catalog_str(catalog_str);
    acq->set_ts(ts);
    acq->set_node_group_id(node_group_id);

    bool success = SendRequest(node_group_id, *send_msg);

    send_msg->clear_type();
    send_msg->clear_tx_number();
    send_msg->clear_handler_addr();
    send_msg->clear_acquire_req();

    msg_pool_.enqueue(std::move(send_msg));

    if (!success)
    {
        hres.SetError(-1);
    }
}

void txservice::remote::RemoteCcHandler::CommitDropTable(
    const TableName &table_name,
    const TxId &txid,
    uint64_t ts,
    uint32_t node_group_id,
    CcHandlerResult<Void> &hres)
{
    std::unique_ptr<CcMessage> send_msg = GetCcMsg();

    send_msg->set_type(
        CcMessage::MessageType::CcMessage_MessageType_CommitDropTableRequest);
    send_msg->set_tx_number(txid.TxNumber());
    send_msg->set_handler_addr(reinterpret_cast<uint64_t>(&hres));

    CommitDropTableRequest *acq = send_msg->mutable_commit_drop_table_req();
    acq->set_src_node_id(local_shards_->node_id_);
    acq->set_tablename(table_name);

    acq->set_ts(ts);
    acq->set_node_group_id(node_group_id);

    bool success = SendRequest(node_group_id, *send_msg);

    send_msg->clear_type();
    send_msg->clear_tx_number();
    send_msg->clear_handler_addr();
    send_msg->clear_acquire_req();

    msg_pool_.enqueue(std::move(send_msg));

    if (!success)
    {
        hres.SetError(-1);
    }
}

void txservice::remote::RemoteCcHandler::Read(
    const TableName &table_name,
    const TxKey &key,
    uint32_t key_shard_code,
    const TxRecord &record,
    ReadType read_type,
    uint64_t tx_number,
    int64_t tx_term,
    const uint64_t ts,
    CcHandlerResult<std::tuple<TxRecord *, uint64_t, CcEntryAddr, RecordStatus>>
        &hres,
    CcProtocol proto)
{
    std::unique_ptr<CcMessage> send_msg = GetCcMsg();

    send_msg->set_type(
        CcMessage::MessageType::CcMessage_MessageType_ReadRequest);
    send_msg->set_tx_number(tx_number);
    send_msg->set_handler_addr(reinterpret_cast<uint64_t>(&hres));
    send_msg->set_tx_term(tx_term);

    ReadRequest *read = send_msg->mutable_read_req();
    read->set_src_node_id(local_shards_->node_id_);
    read->set_tablename(table_name);
    read->clear_key();
    key.Serialize(*read->mutable_key());
    read->set_key_shard_code(key_shard_code);

    read->clear_record();
    switch (read_type)
    {
    case ReadType::Inside:
        read->set_read_type(ReadRequest_ReadType::ReadRequest_ReadType_INSIDE);
        break;
    case ReadType::OutsideNormal:
    {
        read->set_read_type(
            ReadRequest_ReadType::ReadRequest_ReadType_OUTSIDE_NORMAL);
        std::string *rec_str = read->mutable_record();
        record.Serialize(*rec_str);
        break;
    }
    case ReadType::OutsideDeleted:
        read->set_read_type(
            ReadRequest_ReadType::ReadRequest_ReadType_OUTSIDE_DELETED);
        break;
    default:
        break;
    }

    read->set_ts(ts);

    bool success = SendRequest(key_shard_code >> 10, *send_msg);

    send_msg->clear_type();
    send_msg->clear_tx_number();
    send_msg->clear_handler_addr();
    send_msg->clear_read_req();

    msg_pool_.enqueue(std::move(send_msg));

    if (!success)
    {
        hres.SetError(-1);
    }
}

/*
 * ReadOutside fills the tuple read from KV into cache.
 */
void txservice::remote::RemoteCcHandler::ReadOutside(
    const TxRecord &record, bool is_deleted, const CcEntryAddr &cce_addr)
{
    std::unique_ptr<CcMessage> send_msg = GetCcMsg();

    send_msg->set_type(
        CcMessage::MessageType::CcMessage_MessageType_ReadOutsideRequest);

    ReadOutsideRequest *read_outside = send_msg->mutable_read_outside_req();
    assert(cce_addr.CcePtr() != 0);
    read_outside->set_node_group_id(cce_addr.NodeGroupId());

    CceAddr_msg *cce_msg = read_outside->mutable_cce_addr();
    cce_msg->set_cce_ptr(cce_addr.CcePtr());
    cce_msg->set_term(cce_addr.Term());

    read_outside->set_is_deleted(is_deleted);

    read_outside->clear_record();
    if (!is_deleted)
    {
        record.Serialize(*read_outside->mutable_record());
    }

    // ReadOutside doesn't care the execution of fill tuple succeeds or not.
    // Return value of SendRequest could be ignored.
    SendRequest(cce_addr.NodeGroupId(), *send_msg);

    send_msg->clear_type();
    send_msg->clear_read_outside_req();

    msg_pool_.enqueue(std::move(send_msg));
}

void txservice::remote::RemoteCcHandler::ScanOpen(
    const TableName &table_name,
    ScanIndexType index_type,
    uint32_t node_group_id,
    const TxKey &start_key,
    bool inclusive,
    uint64_t tx_number,
    int64_t tx_term,
    uint64_t ts,
    CcHandlerResult<std::pair<size_t, std::unique_ptr<CcScanner>>> &hd_res,
    ScanDirection direction,
    CcProtocol proto,
    bool is_ckpt)
{
    std::unique_ptr<CcMessage> send_msg = GetCcMsg();

    send_msg->set_type(
        CcMessage::MessageType::CcMessage_MessageType_ScanOpenRequest);
    send_msg->set_tx_number(tx_number);
    send_msg->set_handler_addr(reinterpret_cast<uint64_t>(&hd_res));
    send_msg->set_tx_term(tx_term);

    ScanOpenRequest *scan_open = send_msg->mutable_scan_open_req();
    scan_open->set_src_node_id(local_shards_->node_id_);
    scan_open->set_tablename(table_name);
    scan_open->set_shard_id(node_group_id);

    switch (start_key.Type())
    {
    case KeyType::NegativeInf:
        scan_open->set_neg_inf(true);
        break;
    case KeyType::PostiveInf:
        scan_open->set_pos_inf(true);
        break;
    default:
        scan_open->clear_key();
        start_key.Serialize(*scan_open->mutable_key());
        break;
    }

    scan_open->set_inclusive(inclusive);
    scan_open->set_direction(direction == ScanDirection::Forward);
    scan_open->set_ts(ts);
    scan_open->set_ckpt(is_ckpt);

    bool success = SendRequest(node_group_id, *send_msg);

    send_msg->clear_type();
    send_msg->clear_tx_number();
    send_msg->clear_handler_addr();
    send_msg->clear_scan_open_req();

    msg_pool_.enqueue(std::move(send_msg));

    if (!success)
    {
        hd_res.SetError(-1);
    }
}

void txservice::remote::RemoteCcHandler::ScanNext(
    uint32_t ng_id,
    uint64_t tx_number,
    int64_t tx_term,
    uint64_t start_ts,
    ScanCache *scan_cache,
    CcHandlerResult<uint32_t> &hd_res,
    CcProtocol proto,
    bool is_ckpt)
{
    std::unique_ptr<CcMessage> send_msg = GetCcMsg();

    send_msg->set_type(
        CcMessage::MessageType::CcMessage_MessageType_ScanNextRequest);
    send_msg->set_tx_number(tx_number);
    send_msg->set_handler_addr(reinterpret_cast<uint64_t>(&hd_res));
    send_msg->set_tx_term(tx_term);

    ScanNextRequest *scan_next = send_msg->mutable_scan_next_req();

    scan_next->set_src_node_id(local_shards_->node_id_);
    scan_next->set_node_group_id(ng_id);
    const CcEntryAddr &last_cce_addr = scan_cache->LastTuple()->cce_addr_;
    scan_next->set_prior_cce_ptr(last_cce_addr.CcePtr());
    scan_next->set_direction(scan_cache->Scanner()->Direction() ==
                             ScanDirection::Forward);
    scan_next->set_ts(start_ts);
    scan_next->set_scan_cache_ptr(reinterpret_cast<uint64_t>(scan_cache));
    scan_next->set_ckpt(is_ckpt);

    bool success = SendRequest(ng_id, *send_msg);

    send_msg->clear_type();
    send_msg->clear_tx_number();
    send_msg->clear_handler_addr();
    send_msg->clear_scan_next_req();

    msg_pool_.enqueue(std::move(send_msg));

    if (!success)
    {
        hd_res.SetError(-1);
    }
}

void txservice::remote::RemoteCcHandler::CommitSecondaryKey(
    const TableName &table_name,
    const TxKey &sk,
    const TxKey &pk,
    uint32_t key_shard_code,
    bool is_delete,
    uint64_t ts,
    CcHandlerResult<Void> &hd_res)
{
    /*message CommitSkRequest
    {
        uint32 src_node_id = 1;
        string tablename = 2;
        bytes sk = 3;
        bytes pk = 4;
        uint32 key_shard_code = 5;
        uint64 ts = 6;
        bool is_deleted = 7;
    }*/

    std::unique_ptr<CcMessage> send_msg = GetCcMsg();

    send_msg->set_type(
        CcMessage::MessageType::CcMessage_MessageType_CommitSkRequest);
    send_msg->set_handler_addr(reinterpret_cast<uint64_t>(&hd_res));

    CommitSkRequest *commit_sk = send_msg->mutable_commit_sk_req();
    commit_sk->set_src_node_id(local_shards_->node_id_);
    commit_sk->set_tablename(table_name);

    sk.Serialize(*commit_sk->mutable_sk());
    pk.Serialize(*commit_sk->mutable_pk());

    commit_sk->set_key_shard_code(key_shard_code);
    commit_sk->set_ts(ts);
    commit_sk->set_is_deleted(is_delete);

    bool success = SendRequest(key_shard_code >> 10, *send_msg);

    send_msg->clear_type();
    send_msg->clear_handler_addr();
    send_msg->clear_commit_sk_req();

    msg_pool_.enqueue(std::move(send_msg));

    if (!success)
    {
        hd_res.SetError(-1);
    }
}

void txservice::remote::RemoteCcHandler::FaultInject(
    const std::string &fault_name,
    const std::string &fault_type,
    int node_id,
    CcHandlerResult<bool> &hres)
{
    std::unique_ptr<CcMessage> send_msg = GetCcMsg();

    send_msg->set_type(
        CcMessage::MessageType::CcMessage_MessageType_FaultInjectRequest);
    send_msg->set_handler_addr(reinterpret_cast<uint64_t>(&hres));

    FaultInjectRequest *fi_req = send_msg->mutable_fault_inject_req();
    fi_req->set_src_node_id(local_shards_->node_id_);
    fi_req->set_fault_name(fault_name);
    fi_req->set_fault_type(fault_type);

    bool success = SendRequest(node_id, *send_msg);

    send_msg->clear_type();
    send_msg->clear_handler_addr();
    send_msg->clear_acquire_req();

    msg_pool_.enqueue(std::move(send_msg));

    if (!success)
    {
        hres.SetError(-1);
    }
}
