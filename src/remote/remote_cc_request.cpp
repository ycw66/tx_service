#include "remote/remote_cc_request.h"

#include <string_view>

#include "cc/ccm_scanner.h"
#include "remote/remote_cc_handler.h"
#include "remote/remote_type.h"
#include "sharder.h"

txservice::remote::RemoteAcquire::RemoteAcquire()
{
    res_ = &cc_res_;

    output_msg_.set_type(
        CcMessage::MessageType::CcMessage_MessageType_AcquireResponse);

    cc_res_.post_lambda_ =
        [this](CcHandlerResult<std::vector<AcquireKeyResult>> *res)
    {
        output_msg_.set_tx_number(input_msg_->tx_number());
        output_msg_.set_handler_addr(input_msg_->handler_addr());
        output_msg_.set_tx_term(input_msg_->tx_term());
        output_msg_.set_command_id(input_msg_->command_id());

        const AcquireRequest &acquire_req = input_msg_->acquire_req();
        AcquireResponse *resp = output_msg_.mutable_acquire_resp();
        resp->set_is_ack(false);
        resp->set_error_code(res->ErrorCode());
        resp->set_vec_idx(acquire_req.vec_idx());

        if (!cc_res_.IsError())
        {
            const AcquireKeyResult &acquire_key_res = cc_res_.Value()[0];

            resp->set_vali_ts(acquire_key_res.last_vali_ts_);
            resp->set_commit_ts(acquire_key_res.commit_ts_);

            const CcEntryAddr &addr = acquire_key_res.cce_addr_;
            CceAddr_msg *resp_addr = resp->mutable_cce_addr();
            if (addr.CcePtr() != 0)
            {
                resp_addr->set_cce_ptr(addr.CcePtr());
            }
            else
            {
                resp_addr->set_insert_ptr(addr.InsertPtr());
            }
            resp_addr->set_term(addr.Term());
        }

        ACTION_FAULT_INJECTOR("remote_acquire_before_sendmessage");
        const AcquireRequest &req = input_msg_->acquire_req();
        hd_->SendMessageToNode(req.src_node_id(), output_msg_);

        hd_->RecycleCcMsg(std::move(input_msg_));
    };
}

void txservice::remote::RemoteAcquire::Reset(
    std::unique_ptr<CcMessage> input_msg)
{
    assert(input_msg->has_acquire_req());

    cc_res_.Reset();
    cc_res_.Value().resize(1);
    cc_res_.Value()[0].cce_addr_.SetCce(0, -1);

    output_msg_.clear_tx_number();
    output_msg_.clear_handler_addr();
    output_msg_.clear_acquire_resp();

    const AcquireRequest &req = input_msg->acquire_req();

    std::string_view table_name_sv{req.table_name_str()};
    // Need to parse the string if not include table type in protobuf
    remote_table_name_ = TableName(
        table_name_sv, ToLocalType::ConvertCcTableType(req.table_type()));

    AcquireCc::Reset(&remote_table_name_,
                     &req.key(),
                     req.key_shard_code(),
                     input_msg->tx_number(),
                     input_msg->tx_term(),
                     req.ts(),
                     req.insert(),
                     &cc_res_,
                     req.vec_idx(),
                     ToLocalType::ConvertProtocol(req.protocol()),
                     ToLocalType::ConvertIsolation(req.iso_level()));

    input_msg_ = std::move(input_msg);

    if (hd_ == nullptr)
    {
        hd_ = Sharder::Instance().GetCcStreamSender();
    }
}

void txservice::remote::RemoteAcquire::Acknowledge()
{
    output_msg_.set_tx_number(input_msg_->tx_number());
    output_msg_.set_handler_addr(input_msg_->handler_addr());
    output_msg_.set_tx_term(input_msg_->tx_term());
    output_msg_.set_command_id(input_msg_->command_id());

    const AcquireRequest &acquire_req = input_msg_->acquire_req();
    AcquireResponse *acquire_resp = output_msg_.mutable_acquire_resp();
    acquire_resp->set_is_ack(true);
    acquire_resp->set_error_code(0);
    acquire_resp->set_vec_idx(acquire_req.vec_idx());

    CceAddr_msg *resp_addr = acquire_resp->mutable_cce_addr();
    const CcEntryAddr &addr = cc_res_.Value()[0].cce_addr_;
    assert(addr.CcePtr() != 0 || addr.InsertPtr() != 0);
    if (addr.CcePtr() != 0)
    {
        resp_addr->set_cce_ptr(addr.CcePtr());
    }
    else
    {
        resp_addr->set_insert_ptr(addr.InsertPtr());
    }
    resp_addr->set_term(addr.Term());

    const AcquireRequest &req = input_msg_->acquire_req();
    hd_->SendMessageToNode(req.src_node_id(), output_msg_);
}

txservice::remote::RemoteAcquireAll::RemoteAcquireAll()
{
    res_ = &cc_res_;

    output_msg_.set_type(
        CcMessage::MessageType::CcMessage_MessageType_AcquireAllResponse);

    cc_res_.post_lambda_ = [this](CcHandlerResult<AcquireAllResult> *res)
    {
        output_msg_.set_tx_number(input_msg_->tx_number());
        output_msg_.set_handler_addr(input_msg_->handler_addr());
        output_msg_.set_tx_term(input_msg_->tx_term());
        output_msg_.set_command_id(input_msg_->command_id());

        AcquireAllResponse *resp = output_msg_.mutable_acquire_all_resp();
        resp->set_error_code(res->ErrorCode());
        resp->set_is_ack(false);

        if (!cc_res_.IsError())
        {
            const AcquireAllResult &acquire_all_res = cc_res_.Value();

            resp->set_vali_ts(acquire_all_res.last_vali_ts_);
            resp->set_commit_ts(acquire_all_res.commit_ts_);
            resp->set_node_term(acquire_all_res.node_term_);
        }

        const AcquireAllRequest &req = input_msg_->acquire_all_req();
        hd_->SendMessageToNode(req.src_node_id(), output_msg_);
        hd_->RecycleCcMsg(std::move(input_msg_));
    };
}

void txservice::remote::RemoteAcquireAll::Reset(
    std::unique_ptr<CcMessage> input_msg)
{
    assert(input_msg->has_acquire_all_req());

    cc_res_.Reset();

    output_msg_.clear_tx_number();
    output_msg_.clear_handler_addr();
    output_msg_.clear_acquire_all_resp();

    const AcquireAllRequest &req = input_msg->acquire_all_req();
    std::string_view table_name_sv{req.table_name_str()};
    remote_table_name_ = TableName(
        table_name_sv, ToLocalType::ConvertCcTableType(req.table_type()));

    AcquireAllCc::Reset(&remote_table_name_,
                        &req.key(),
                        req.node_group_id(),
                        input_msg->tx_number(),
                        input_msg->tx_term(),
                        req.insert(),
                        &cc_res_,
                        ToLocalType::ConvertProtocol(req.protocol()),
                        ToLocalType::ConvertCcOperation(req.cc_op()));

    input_msg_ = std::move(input_msg);

    if (hd_ == nullptr)
    {
        hd_ = Sharder::Instance().GetCcStreamSender();
    }
}

void txservice::remote::RemoteAcquireAll::Acknowledge()
{
    output_msg_.set_tx_number(input_msg_->tx_number());
    output_msg_.set_handler_addr(input_msg_->handler_addr());
    output_msg_.set_tx_term(input_msg_->tx_term());
    output_msg_.set_command_id(input_msg_->command_id());

    AcquireAllResponse *acquire_all_resp =
        output_msg_.mutable_acquire_all_resp();
    acquire_all_resp->set_is_ack(true);
    acquire_all_resp->set_error_code(0);
    acquire_all_resp->set_node_term(cc_res_.Value().node_term_);

    const AcquireAllRequest &req = input_msg_->acquire_all_req();
    hd_->SendMessageToNode(req.src_node_id(), output_msg_);
}

txservice::remote::RemotePostRead::RemotePostRead()
{
    res_ = &cc_res_;

    output_msg_.set_type(
        CcMessage::MessageType::CcMessage_MessageType_ValidateResponse);

    cc_res_.post_lambda_ = [this](CcHandlerResult<PostProcessResult> *res)
    {
        output_msg_.set_tx_number(input_msg_->tx_number());
        output_msg_.set_handler_addr(input_msg_->handler_addr());
        output_msg_.set_tx_term(input_msg_->tx_term());
        output_msg_.set_command_id(input_msg_->command_id());

        ValidateResponse *resp = output_msg_.mutable_validate_resp();
        resp->set_error_code(res->ErrorCode());

        if (!res->IsError())
        {
            // RemotePostRead at the remote node accesses one key, which locates
            // in a single shard. Hence, there are no concurrent modifications
            // of PostReadResult. It is safe to access the result's array
            // without the mutex protection.
            for (TxNumber &txn : res->Value().conflicting_txs_)
            {
                resp->add_txs(txn);
            }
        }

        const ValidateRequest &req = input_msg_->validate_req();
        hd_->SendMessageToNode(req.src_node_id(), output_msg_);
        hd_->RecycleCcMsg(std::move(input_msg_));
    };
}

void txservice::remote::RemotePostRead::Reset(
    std::unique_ptr<CcMessage> input_msg)
{
    assert(input_msg->has_validate_req());

    cc_res_.Reset();

    output_msg_.clear_tx_number();
    output_msg_.clear_handler_addr();
    output_msg_.clear_validate_resp();

    const ValidateRequest &req = input_msg->validate_req();
    const CceAddr_msg &cce_addr = req.cce_addr();

    cce_addr_.SetCce(cce_addr.cce_ptr(), cce_addr.term(), req.node_group_id());

    PostReadCc::Reset(&cce_addr_,
                      input_msg->tx_number(),
                      req.commit_ts(),
                      req.key_ts(),
                      req.gap_ts(),
                      &cc_res_,
                      ToLocalType::ConvertProtocol(req.protocol()),
                      ToLocalType::ConvertLockType(req.lock_type()));

    input_msg_ = std::move(input_msg);

    if (hd_ == nullptr)
    {
        hd_ = Sharder::Instance().GetCcStreamSender();
    }
}

txservice::remote::RemoteRead::RemoteRead()
{
    res_ = &cc_res_;

    output_msg_.set_type(
        CcMessage::MessageType::CcMessage_MessageType_ReadResponse);

    cc_res_.post_lambda_ = [this](CcHandlerResult<ReadKeyResult> *res)
    {
        output_msg_.set_tx_number(input_msg_->tx_number());
        output_msg_.set_handler_addr(input_msg_->handler_addr());
        output_msg_.set_tx_term(input_msg_->tx_term());
        output_msg_.set_command_id(input_msg_->command_id());

        const ReadKeyResult &read_result = res->Value();
        ReadResponse *resp = output_msg_.mutable_read_resp();
        resp->set_is_ack(false);
        resp->set_error_code(res->ErrorCode());

        if (!res->IsError())
        {
            resp->set_rec_status(
                ToRemoteType::ConvertRecordStatus(read_result.rec_status_));
            resp->set_lock_type(
                ToRemoteType::ConvertLockType(read_result.lock_type_));
            resp->set_ts(read_result.ts_);

            CceAddr_msg *cce_addr_msg = resp->mutable_cce_addr();
            cce_addr_msg->set_cce_ptr(read_result.cce_addr_.CcePtr());
            cce_addr_msg->set_term(read_result.cce_addr_.Term());
        }

        const ReadRequest &req = input_msg_->read_req();
        hd_->SendMessageToNode(req.src_node_id(), output_msg_);
        hd_->RecycleCcMsg(std::move(input_msg_));
    };
}

void txservice::remote::RemoteRead::Reset(std::unique_ptr<CcMessage> input_msg)
{
    assert(input_msg->has_read_req());

    cc_res_.Reset();

    output_msg_.clear_tx_number();
    output_msg_.clear_handler_addr();
    output_msg_.clear_read_resp();

    const ReadRequest &req = input_msg->read_req();
    std::string_view table_name_sv{req.table_name_str()};
    remote_table_name_ = TableName(
        table_name_sv, ToLocalType::ConvertCcTableType(req.table_type()));

    ReadType read_type = ReadType::Inside;
    switch (req.read_type())
    {
    case ReadRequest_ReadType::ReadRequest_ReadType_INSIDE:
        read_type = ReadType::Inside;
        break;
    case ReadRequest_ReadType::ReadRequest_ReadType_OUTSIDE_NORMAL:
        read_type = ReadType::OutsideNormal;
        break;
    case ReadRequest_ReadType::ReadRequest_ReadType_OUTSIDE_DELETED:
        read_type = ReadType::OutsideDeleted;
        break;
    default:
        break;
    }

    cc_res_.Value().cce_addr_.SetCce(0, -1, req.key_shard_code() >> 10);

    ReadResponse *resp = output_msg_.mutable_read_resp();
    resp->clear_record();
    if (read_type == ReadType::Inside)
    {
        ReadCc::Reset(&remote_table_name_,
                      &req.key(),
                      req.key_shard_code(),
                      resp->mutable_record(),
                      read_type,
                      input_msg->tx_number(),
                      input_msg->tx_term(),
                      req.ts(),
                      &cc_res_,
                      ToLocalType::ConvertIsolation(req.iso_level()),
                      ToLocalType::ConvertProtocol(req.protocol()),
                      req.is_for_write());
    }
    else
    {
        // The read brings in an external record (from the data store) for
        // concurrency control

        std::string *out_record = resp->mutable_record();
        *out_record = req.record();

        ReadCc::Reset(&remote_table_name_,
                      &req.key(),
                      req.key_shard_code(),
                      out_record,
                      read_type,
                      input_msg->tx_number(),
                      input_msg->tx_term(),
                      req.ts(),
                      &cc_res_,
                      ToLocalType::ConvertIsolation(req.iso_level()),
                      ToLocalType::ConvertProtocol(req.protocol()),
                      req.is_for_write());
    }

    input_msg_ = std::move(input_msg);

    if (hd_ == nullptr)
    {
        hd_ = Sharder::Instance().GetCcStreamSender();
    }
}

void txservice::remote::RemoteRead::Acknowledge()
{
    output_msg_.set_tx_number(input_msg_->tx_number());
    output_msg_.set_handler_addr(input_msg_->handler_addr());
    output_msg_.set_tx_term(input_msg_->tx_term());
    output_msg_.set_command_id(input_msg_->command_id());

    ReadResponse *read_resp = output_msg_.mutable_read_resp();
    read_resp->set_is_ack(true);
    read_resp->set_error_code(0);

    CceAddr_msg *resp_addr = read_resp->mutable_cce_addr();
    const CcEntryAddr &addr = cc_res_.Value().cce_addr_;
    assert(addr.CcePtr() != 0 || addr.InsertPtr() != 0);
    if (addr.CcePtr() != 0)
    {
        resp_addr->set_cce_ptr(addr.CcePtr());
    }
    else
    {
        resp_addr->set_insert_ptr(addr.InsertPtr());
    }
    resp_addr->set_term(addr.Term());

    const ReadRequest &req = input_msg_->read_req();
    hd_->SendMessageToNode(req.src_node_id(), output_msg_);
}

txservice::remote::RemotePostWrite::RemotePostWrite()
{
    res_ = &cc_res_;

    output_msg_.set_type(
        CcMessage::MessageType::CcMessage_MessageType_PostprocessResponse);

    cc_res_.post_lambda_ = [this](CcHandlerResult<PostProcessResult> *res)
    {
        output_msg_.set_tx_number(input_msg_->tx_number());
        output_msg_.set_handler_addr(input_msg_->handler_addr());
        output_msg_.set_tx_term(input_msg_->tx_term());
        output_msg_.set_command_id(input_msg_->command_id());

        PostprocessResponse *resp = output_msg_.mutable_post_resp();
        resp->set_error_code(res->ErrorCode());

        const PostCommitRequest &req = input_msg_->postcommit_req();
        hd_->SendMessageToNode(req.src_node_id(), output_msg_);
        hd_->RecycleCcMsg(std::move(input_msg_));
    };
}

void txservice::remote::RemotePostWrite::Reset(
    std::unique_ptr<CcMessage> input_msg)
{
    assert(input_msg->has_postcommit_req());

    cc_res_.Reset();

    output_msg_.clear_tx_number();
    output_msg_.clear_handler_addr();
    output_msg_.clear_post_resp();

    const PostCommitRequest &post_commit = input_msg->postcommit_req();
    const CceAddr_msg &cce_addr_msg = post_commit.cce_addr();

    if (cce_addr_msg.entry_ptr_case() == CceAddr_msg::EntryPtrCase::kInsertPtr)
    {
        cce_addr_.SetInsert(cce_addr_msg.insert_ptr(),
                            cce_addr_msg.term(),
                            post_commit.node_group_id());
    }
    else
    {
        cce_addr_.SetCce(cce_addr_msg.cce_ptr(),
                         cce_addr_msg.term(),
                         post_commit.node_group_id());
    }

    uint64_t commit_ts = post_commit.commit_ts();
    const std::string *rec_str =
        commit_ts > 0 ? &post_commit.record() : nullptr;
    proto_ = ToLocalType::ConvertProtocol(post_commit.protocol());
    PostWriteCc::Reset(&cce_addr_,
                       input_msg->tx_number(),
                       commit_ts,
                       rec_str,
                       static_cast<DmlOperation>(post_commit.dml_operation()),
                       &cc_res_,
                       proto_);

    input_msg_ = std::move(input_msg);

    if (hd_ == nullptr)
    {
        hd_ = Sharder::Instance().GetCcStreamSender();
    }
}

txservice::remote::RemotePostWriteAll::RemotePostWriteAll()
{
    res_ = &cc_res_;

    output_msg_.set_type(
        CcMessage::MessageType::CcMessage_MessageType_PostprocessResponse);

    cc_res_.post_lambda_ = [this](CcHandlerResult<PostProcessResult> *res)
    {
        output_msg_.set_tx_number(input_msg_->tx_number());
        output_msg_.set_handler_addr(input_msg_->handler_addr());
        output_msg_.set_tx_term(input_msg_->tx_term());
        output_msg_.set_command_id(input_msg_->command_id());

        PostprocessResponse *resp = output_msg_.mutable_post_resp();
        resp->set_error_code(res->ErrorCode());

        const PostWriteAllRequest &req = input_msg_->post_write_all_req();
        hd_->SendMessageToNode(req.src_node_id(), output_msg_);
        hd_->RecycleCcMsg(std::move(input_msg_));
    };
}

void txservice::remote::RemotePostWriteAll::Reset(
    std::unique_ptr<CcMessage> input_msg)
{
    assert(input_msg->has_post_write_all_req());

    cc_res_.Reset();

    output_msg_.clear_tx_number();
    output_msg_.clear_handler_addr();
    output_msg_.clear_post_resp();

    const PostWriteAllRequest &post_write_all = input_msg->post_write_all_req();

    std::string_view table_name_sv{post_write_all.table_name_str()};
    remote_table_name_ =
        TableName(table_name_sv,
                  ToLocalType::ConvertCcTableType(post_write_all.table_type()));

    uint64_t commit_ts = post_write_all.commit_ts();
    const std::string *rec_str =
        commit_ts > 0 ? &post_write_all.record() : nullptr;
    DmlOperation dml_op = post_write_all.is_deleted() ? DmlOperation::Delete
                                                      : DmlOperation::Upsert;
    PostWriteType write_type =
        ToLocalType::ConvertCommitType(post_write_all.commit_type());

    int64_t tx_term = input_msg->tx_term();

    PostWriteAllCc::Reset(&remote_table_name_,
                          &post_write_all.key(),
                          post_write_all.node_group_id(),
                          input_msg->tx_number(),
                          commit_ts,
                          rec_str,
                          dml_op,
                          &cc_res_,
                          write_type,
                          tx_term);

    input_msg_ = std::move(input_msg);

    if (hd_ == nullptr)
    {
        hd_ = Sharder::Instance().GetCcStreamSender();
    }
}

txservice::remote::RemoteScanOpen::RemoteScanOpen()
{
    parallel_req_ = true;
    output_msg_.set_type(
        CcMessage::MessageType::CcMessage_MessageType_ScanOpenResponse);
    res_ = &cc_res_;

    cc_res_.post_lambda_ = [this](CcHandlerResult<Void> *res)
    {
        output_msg_.set_tx_number(input_msg_->tx_number());
        output_msg_.set_handler_addr(input_msg_->handler_addr());
        output_msg_.set_tx_term(input_msg_->tx_term());
        output_msg_.set_command_id(input_msg_->command_id());

        ScanOpenResponse *scan_open = output_msg_.mutable_scan_open_resp();

        scan_open->set_error_code(cc_res_.ErrorCode());

        if (!cc_res_.IsError())
        {
            for (int core_id = 0; core_id < scan_open->scan_cache_size();
                 ++core_id)
            {
                ScanCache_msg *cache = scan_open->mutable_scan_cache(core_id);

                // The response message allocates a fixed number (the batch
                // size) of scan tuples up front. If the batch is not full,
                // removes the trailing tuples.
                while (scan_caches_.at(core_id).size() <
                       (size_t) cache->scan_tuple_size())
                {
                    cache->mutable_scan_tuple()->RemoveLast();
                }
            }
        }
        else
        {
            CcOperation cc_op =
                IsForWrite() ? CcOperation::ReadForWrite : CcOperation::Read;
            if (remote_table_name_.Type() == TableType::Secondary)
            {
                cc_op = CcOperation::ReadSkIndex;
            }
            LockType lock_type =
                LockTypeUtil::DeduceLockType(cc_op, Isolation(), Protocol());

            if (lock_type == LockType::NoLock)
            {
                // Not acquire lock, just clear scan cache.
                for (int core_id = 0; core_id < scan_open->scan_cache_size();
                     ++core_id)
                {
                    ScanCache_msg *cache =
                        scan_open->mutable_scan_cache(core_id);
                    cache->Clear();
                }
            }
            else
            {
                // Acquired lock, should transfer scan cache back to release
                // the locks through PostRead.
                for (int core_id = 0; core_id < scan_open->scan_cache_size();
                     ++core_id)
                {
                    ScanCache_msg *cache =
                        scan_open->mutable_scan_cache(core_id);

                    // The response message allocates a fixed number (the
                    // batch size) of scan tuples up front. If the batch is not
                    // full, removes the trailing tuples.
                    while (scan_caches_.at(core_id).size() <
                           (size_t) cache->scan_tuple_size())
                    {
                        cache->mutable_scan_tuple()->RemoveLast();
                    }
                }
            }
        }

        scan_open->set_node_group_id(node_group_id_);
        const ScanOpenRequest &req = input_msg_->scan_open_req();
        hd_->SendMessageToNode(req.src_node_id(), output_msg_);
        hd_->RecycleCcMsg(std::move(input_msg_));
    };
}

void txservice::remote::RemoteScanOpen::Reset(
    std::unique_ptr<CcMessage> input_msg, uint32_t core_cnt)
{
    assert(input_msg->has_scan_open_req());

    cc_res_.Reset();
    cc_res_.SetRefCnt(core_cnt);
    cce_ptr_.clear();
    cce_ptr_.resize(core_cnt);
    cce_ptr_scan_type_.clear();
    cce_ptr_scan_type_.resize(core_cnt);

    const ScanOpenRequest &scan_open = input_msg->scan_open_req();

    std::string_view table_name_sv{scan_open.table_name_str()};
    remote_table_name_ = TableName(
        table_name_sv, ToLocalType::ConvertCcTableType(scan_open.table_type()));

    node_group_id_ = scan_open.shard_id();
    table_name_ = &remote_table_name_;
    tx_term_ = input_msg->tx_term();
    is_for_write_ = scan_open.is_for_write();
    isolation_level_ = ToLocalType::ConvertIsolation(scan_open.iso_level());
    proto_ = ToLocalType::ConvertProtocol(scan_open.protocol());
    tx_number_ = input_msg->tx_number();
    snapshot_ts_ = scan_open.ts();

    ccm_ = nullptr;

    if (scan_open.start_key_case() == ScanOpenRequest::StartKeyCase::kNegInf)
    {
        key_type_ = KeyType::NegativeInf;
        start_key_str_ = nullptr;
    }
    else if (scan_open.start_key_case() ==
             ScanOpenRequest::StartKeyCase::kPosInf)
    {
        key_type_ = KeyType::PositiveInf;
        start_key_str_ = nullptr;
    }
    else
    {
        key_type_ = KeyType::Normal;
        start_key_str_ = &scan_open.key();
    }

    inclusive_ = scan_open.inclusive();
    direct_ = scan_open.direction() ? ScanDirection::Forward
                                    : ScanDirection::Backward;

    is_ckpt_delta_ = scan_open.ckpt();

    output_msg_.clear_tx_number();
    output_msg_.clear_handler_addr();
    output_msg_.clear_scan_open_resp();

    ScanOpenResponse *resp = output_msg_.mutable_scan_open_resp();
    assert(resp->scan_cache_size() == 0);

    scan_caches_.resize(core_cnt);
    scan_caches_idxs_.resize(core_cnt);
    for (size_t cid = 0; cid < core_cnt; ++cid)
    {
        ScanCache_msg *cache_msg = resp->add_scan_cache();
        assert(cache_msg->scan_tuple_size() == 0);
        scan_caches_.at(cid).clear();
        scan_caches_idxs_.at(cid) = 0;

        for (size_t idx = 0; idx < ScanCache::ScanBatchSize; ++idx)
        {
            ScanTuple_msg *tuple = cache_msg->add_scan_tuple();
            scan_caches_.at(cid).emplace_back(tuple);
        }
    }

    unfinish_cnt_.store(core_cnt);

    input_msg_ = std::move(input_msg);

    if (hd_ == nullptr)
    {
        hd_ = Sharder::Instance().GetCcStreamSender();
    }
}

void txservice::remote::RemoteScanOpen::Free()
{
    uint32_t prior_val = unfinish_cnt_.fetch_sub(1);
    if (prior_val == 1)
    {
        CcRequestBase::Free();
    }
}

txservice::remote::RemoteScanNextBatch::RemoteScanNextBatch()
{
    scan_cache_.reserve(ScanCache::ScanBatchSize);
    output_msg_.set_type(
        CcMessage::MessageType::CcMessage_MessageType_ScanNextResponse);
    is_ckpt_delta_ = false;
    res_ = &cc_res_;
    cce_ptr_ = nullptr;

    /*message ScanNextResponse
    {
        bool error = 1;
        repeated ScanTuple_msg scan_tuple = 2;
        uint64 ccm_ptr = 3;
        uint64 scan_cache_ptr = 4;
    }*/

    cc_res_.post_lambda_ = [this](CcHandlerResult<Void> *res)
    {
        output_msg_.set_tx_number(input_msg_->tx_number());
        output_msg_.set_handler_addr(input_msg_->handler_addr());
        output_msg_.set_tx_term(input_msg_->tx_term());
        output_msg_.set_command_id(input_msg_->command_id());

        ScanNextResponse *scan_next = output_msg_.mutable_scan_next_resp();
        const ScanNextRequest &req = input_msg_->scan_next_req();
        scan_next->set_error_code(res->ErrorCode());

        if (!res->IsError())
        {
            while (scan_cache_.size() < (size_t) scan_next->scan_tuple_size())
            {
                scan_next->mutable_scan_tuple()->RemoveLast();
            }

            scan_next->set_scan_cache_ptr(req.scan_cache_ptr());
        }
        else
        {
            CcOperation cc_op =
                IsForWrite() ? CcOperation::ReadForWrite : CcOperation::Read;
            const LruEntry *prior_lru_entry =
                reinterpret_cast<const LruEntry *>(prior_cce_addr_);
            if (prior_lru_entry->parent_map_->Type() == TableType::Secondary)
            {
                cc_op = CcOperation::ReadSkIndex;
            }
            LockType lock_type =
                LockTypeUtil::DeduceLockType(cc_op, Isolation(), Protocol());

            if (lock_type == LockType::NoLock)
            {
                // Not acquire lock, just clear scan cache.
                scan_next->mutable_scan_tuple()->Clear();
            }
            else
            {
                // Acquired lock, should transfer scan cache back to release
                // the locks through PostRead.
                while (scan_cache_.size() <
                       (size_t) scan_next->scan_tuple_size())
                {
                    scan_next->mutable_scan_tuple()->RemoveLast();
                }

                scan_next->set_scan_cache_ptr(req.scan_cache_ptr());
            }
        }

        hd_->SendMessageToNode(req.src_node_id(), output_msg_);
        hd_->RecycleCcMsg(std::move(input_msg_));
    };
}

void txservice::remote::RemoteScanNextBatch::Reset(
    std::unique_ptr<CcMessage> input_msg)
{
    assert(input_msg->has_scan_next_req());

    cc_res_.Reset();

    const ScanNextRequest &scan_next = input_msg->scan_next_req();

    node_group_id_ = scan_next.node_group_id();
    prior_cce_addr_ = scan_next.prior_cce_ptr();
    direct_ = scan_next.direction() ? ScanDirection::Forward
                                    : ScanDirection::Backward;
    tx_term_ = input_msg->tx_term();
    is_for_write_ = scan_next.is_for_write();
    isolation_level_ = ToLocalType::ConvertIsolation(scan_next.iso_level());
    proto_ = ToLocalType::ConvertProtocol(scan_next.protocol());
    tx_number_ = input_msg->tx_number();
    cce_ptr_ = nullptr;
    snapshot_ts_ = scan_next.ts();

    const LruEntry *prior_lru_entry =
        reinterpret_cast<const LruEntry *>(prior_cce_addr_);
    ccm_ = prior_lru_entry->parent_map_;

    output_msg_.clear_tx_number();
    output_msg_.clear_handler_addr();
    output_msg_.clear_scan_next_resp();

    ScanNextResponse *resp = output_msg_.mutable_scan_next_resp();
    assert(resp->scan_tuple_size() == 0);

    scan_cache_.clear();
    scan_cache_idx_ = 0;
    for (size_t idx = 0; idx < ScanCache::ScanBatchSize; ++idx)
    {
        ScanTuple_msg *tuple = resp->add_scan_tuple();
        scan_cache_.emplace_back(tuple);
    }

    is_ckpt_delta_ = scan_next.ckpt();

    input_msg_ = std::move(input_msg);

    if (hd_ == nullptr)
    {
        hd_ = Sharder::Instance().GetCcStreamSender();
    }
}

void txservice::remote::RemoteReadOutside::Reset(
    std::unique_ptr<CcMessage> input_msg)
{
    assert(input_msg->has_read_outside_req());

    const ReadOutsideRequest &req = input_msg->read_outside_req();

    assert(req.cce_addr().cce_ptr() != 0);
    cce_addr_.SetCce(
        req.cce_addr().cce_ptr(), req.cce_addr().term(), req.node_group_id());
    rec_status_ = ToLocalType::ConvertRecordStatusType(req.rec_status());
    commit_ts_ = req.commit_ts();
    rec_str_ = &req.record();

    input_msg_ = std::move(input_msg);

    if (hd_ == nullptr)
    {
        hd_ = Sharder::Instance().GetCcStreamSender();
    }
}

void txservice::remote::RemoteReadOutside::Finish()
{
    hd_->RecycleCcMsg(std::move(input_msg_));
}

txservice::remote::RemoteFaultInjectCC::RemoteFaultInjectCC() : cc_res_(nullptr)
{
    res_ = &cc_res_;

    output_msg_.set_type(
        CcMessage::MessageType::CcMessage_MessageType_FaultInjectResponse);

    cc_res_.post_lambda_ = [this](CcHandlerResult<bool> *res)
    {
        output_msg_.set_tx_number(input_msg_->tx_number());
        output_msg_.set_tx_term(input_msg_->tx_term());
        output_msg_.set_command_id(input_msg_->command_id());
        output_msg_.set_handler_addr(input_msg_->handler_addr());

        FaultInjectResponse *resp = output_msg_.mutable_fault_inject_resp();
        resp->set_error_code(res->ErrorCode());

        const FaultInjectRequest &req = input_msg_->fault_inject_req();
        hd_->SendMessageToNode(req.src_node_id(), output_msg_);
        hd_->RecycleCcMsg(std::move(input_msg_));
    };
}

void txservice::remote::RemoteFaultInjectCC::Reset(
    std::unique_ptr<CcMessage> input_msg)
{
    assert(input_msg->has_fault_inject_req());

    cc_res_.Reset();

    output_msg_.clear_tx_number();
    output_msg_.clear_handler_addr();
    output_msg_.clear_acquire_resp();

    const FaultInjectRequest &req = input_msg->fault_inject_req();

    FaultInjectCC::Reset(&req.fault_name(), &req.fault_paras(), &cc_res_);

    input_msg_ = std::move(input_msg);

    if (hd_ == nullptr)
    {
        hd_ = Sharder::Instance().GetCcStreamSender();
    }
}

txservice::remote::RemoteCleanCcEntryForTestCc::RemoteCleanCcEntryForTestCc()
    : cc_res_(nullptr)
{
    res_ = &cc_res_;

    output_msg_.set_type(CcMessage::MessageType::
                             CcMessage_MessageType_CleanCcEntryForTestResponse);

    cc_res_.post_lambda_ = [this](CcHandlerResult<bool> *res)
    {
        output_msg_.set_tx_number(input_msg_->tx_number());
        output_msg_.set_tx_term(input_msg_->tx_term());
        output_msg_.set_command_id(input_msg_->command_id());
        output_msg_.set_handler_addr(input_msg_->handler_addr());

        CleanCcEntryForTestResponse *resp =
            output_msg_.mutable_clean_cc_entry_resp();
        resp->set_error_code(res->ErrorCode());

        const CleanCcEntryForTestRequest &req =
            input_msg_->clean_cc_entry_req();
        hd_->SendMessageToNode(req.src_node_id(), output_msg_);
        hd_->RecycleCcMsg(std::move(input_msg_));
    };
}

void txservice::remote::RemoteCleanCcEntryForTestCc::Reset(
    std::unique_ptr<CcMessage> input_msg)
{
    assert(input_msg->has_clean_cc_entry_req());

    cc_res_.Reset();

    output_msg_.clear_tx_number();
    output_msg_.clear_handler_addr();
    output_msg_.clear_acquire_resp();

    const CleanCcEntryForTestRequest &req = input_msg->clean_cc_entry_req();
    std::string_view table_name_sv{req.table_name_str()};
    remote_table_name_ = TableName(
        table_name_sv, ToLocalType::ConvertCcTableType(req.table_type()));

    CleanCcEntryForTestCc::Reset(&remote_table_name_,
                                 &req.key(),
                                 req.only_archives(),
                                 req.flush(),
                                 req.key_shard_code(),
                                 input_msg->tx_number(),
                                 &cc_res_);

    input_msg_ = std::move(input_msg);

    if (hd_ == nullptr)
    {
        hd_ = Sharder::Instance().GetCcStreamSender();
    }
}
