#include "remote/remote_cc_request.h"

#include "cc/ccm_scanner.h"
#include "remote/remote_cc_handler.h"

txservice::remote::RemoteAcquire::RemoteAcquire()
    : AcquireCc(),
      output_msg_(),
      input_msg_(nullptr),
      hd_(nullptr),
      txid_obj_(),
      cc_res_(nullptr)
{
    res_ = &cc_res_;

    output_msg_.set_type(
        CcMessage::MessageType::CcMessage_MessageType_AcquireResponse);

    cc_res_.post_lambda_ =
        [this](CcHandlerResult<std::pair<uint64_t, CcEntryAddr>> *res)
    {
        output_msg_.set_tx_number(txid_obj_.TxNumber());
        output_msg_.set_handler_addr(input_msg_->handler_addr());
        output_msg_.set_tx_term(input_msg_->tx_term());

        AcquireResponse *resp = output_msg_.mutable_acquire_resp();
        resp->set_error_code(res->ErrorCode());

        if (!res->IsError())
        {
            resp->set_vali_ts(res->Value().first);
            const CcEntryAddr &addr = res->Value().second;
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

        const AcquireRequest &req = input_msg_->acquire_req();
        hd_->SendResponse(req.src_node_id(), output_msg_);

        hd_->RecycleCcMsg(std::move(input_msg_));
    };
}

txservice::remote::RemoteAcquireTableWriteLockCC::
    RemoteAcquireTableWriteLockCC()
    : output_msg_(),
      input_msg_(nullptr),
      hd_(nullptr),
      txid_obj_(),
      cc_res_(nullptr),
      node_term_(0)
{
    res_ = &cc_res_;

    output_msg_.set_type(
        CcMessage::MessageType::
            CcMessage_MessageType_AcquireTableWriteLockResponse);

    cc_res_.post_lambda_ = [this](CcHandlerResult<Void> *res)
    {
        output_msg_.set_tx_number(input_msg_->tx_number());
        output_msg_.set_tx_term(input_msg_->tx_term());
        output_msg_.set_handler_addr(input_msg_->handler_addr());

        AcquireTableWriteLockResponse *resp =
            output_msg_.mutable_acquire_table_resp();
        resp->set_error_code(cc_res_.ErrorCode());

        // set table write lock ccnode_id and its term.
        resp->set_node_id(node_group_id_);
        resp->set_term(node_term_);

        const AcquireTableWriteLockRequest &req =
            input_msg_->acquire_table_req();
        hd_->SendResponse(req.src_node_id(), output_msg_);

        hd_->RecycleCcMsg(std::move(input_msg_));
    };
}

void txservice::remote::RemoteAcquireTableWriteLockCC::Free()
{
    uint32_t prior_val = unfinish_cnt_.fetch_sub(1);
    if (prior_val == 1)
    {
        CcRequestBase::Free();
    }
}

txservice::remote::RemoteReleaseTableWriteLock::RemoteReleaseTableWriteLock()
    : ReleaseTableWriteLockCC(),
      output_msg_(),
      input_msg_(nullptr),
      hd_(nullptr),
      txid_obj_(),
      cc_res_(nullptr)
{
    res_ = &cc_res_;

    output_msg_.set_type(
        CcMessage::MessageType::
            CcMessage_MessageType_ReleaseTableWriteLockResponse);

    cc_res_.post_lambda_ = [this](CcHandlerResult<Void> *res)
    {
        output_msg_.set_tx_number(input_msg_->tx_number());
        output_msg_.set_tx_term(input_msg_->tx_term());
        output_msg_.set_handler_addr(input_msg_->handler_addr());

        ReleaseTableWriteLockResponse *resp =
            output_msg_.mutable_release_table_resp();
        resp->set_error_code(cc_res_.ErrorCode());

        const ReleaseTableWriteLockRequest &req =
            input_msg_->release_table_req();
        hd_->SendResponse(req.src_node_id(), output_msg_);

        hd_->RecycleCcMsg(std::move(input_msg_));
    };
}

void txservice::remote::RemoteReleaseTableWriteLock::Free()
{
    uint32_t prior_val = unfinish_cnt_.fetch_sub(1);
    if (prior_val == 1)
    {
        CcRequestBase::Free();
    }
}

txservice::remote::RemoteCommitCreateTable::RemoteCommitCreateTable()
    : CommitCreateTableCC(),
      output_msg_(),
      input_msg_(nullptr),
      hd_(nullptr),
      cc_res_(nullptr)
{
    res_ = &cc_res_;

    output_msg_.set_type(CcMessage::MessageType::
                             CcMessage_MessageType_CommitCreateTableResponse);

    cc_res_.post_lambda_ = [this](CcHandlerResult<Void> *res)
    {
        output_msg_.set_tx_number(input_msg_->tx_number());
        output_msg_.set_tx_term(input_msg_->tx_term());
        output_msg_.set_handler_addr(input_msg_->handler_addr());

        CommitCreateTableResponse *resp =
            output_msg_.mutable_commit_create_table_resp();
        resp->set_error_code(cc_res_.ErrorCode());

        const CommitCreateTableRequest &req =
            input_msg_->commit_create_table_req();
        hd_->SendResponse(req.src_node_id(), output_msg_);

        hd_->RecycleCcMsg(std::move(input_msg_));
    };
}

void txservice::remote::RemoteCommitCreateTable::Free()
{
    uint32_t prior_val = unfinish_cnt_.fetch_sub(1);
    if (prior_val == 1)
    {
        CcRequestBase::Free();
    }
}

txservice::remote::RemoteCommitDropTable::RemoteCommitDropTable()
    : CommitDropTableCC(),
      output_msg_(),
      input_msg_(nullptr),
      hd_(nullptr),
      cc_res_(nullptr)
{
    res_ = &cc_res_;

    output_msg_.set_type(
        CcMessage::MessageType::CcMessage_MessageType_CommitDropTableResponse);

    cc_res_.post_lambda_ = [this](CcHandlerResult<Void> *res)
    {
        output_msg_.set_tx_number(input_msg_->tx_number());
        output_msg_.set_tx_term(input_msg_->tx_term());
        output_msg_.set_handler_addr(input_msg_->handler_addr());

        CommitDropTableResponse *resp =
            output_msg_.mutable_commit_drop_table_resp();
        resp->set_error_code(cc_res_.ErrorCode());

        const CommitDropTableRequest &req = input_msg_->commit_drop_table_req();
        hd_->SendResponse(req.src_node_id(), output_msg_);

        hd_->RecycleCcMsg(std::move(input_msg_));
    };
}

void txservice::remote::RemoteCommitDropTable::Free()
{
    uint32_t prior_val = unfinish_cnt_.fetch_sub(1);
    if (prior_val == 1)
    {
        CcRequestBase::Free();
    }
}

txservice::remote::RemoteValidate::RemoteValidate()
    : output_msg_(),
      input_msg_(nullptr),
      hd_(nullptr),
      cce_addr_(),
      cc_res_(nullptr)
{
    res_ = &cc_res_;

    output_msg_.set_type(
        CcMessage::MessageType::CcMessage_MessageType_ValidateResponse);

    cc_res_.post_lambda_ = [this](CcHandlerResult<std::vector<TxId>> *res)
    {
        output_msg_.set_tx_number(input_msg_->tx_number());
        output_msg_.set_handler_addr(input_msg_->handler_addr());
        output_msg_.set_tx_term(input_msg_->tx_term());

        ValidateResponse *resp = output_msg_.mutable_validate_resp();
        resp->set_error_code(res->ErrorCode());

        if (!res->IsError())
        {
            for (TxId &tx : res->Value())
            {
                TxId_msg *txid_msg = resp->add_txs();
                txid_msg->set_core_id(tx.GlobalCoreId());
                txid_msg->set_ident(tx.Identity());
                txid_msg->set_vec_idx(tx.VecIdx());
            }
        }

        const ValidateRequest &req = input_msg_->validate_req();
        hd_->SendResponse(req.src_node_id(), output_msg_);
        hd_->RecycleCcMsg(std::move(input_msg_));
    };
}

txservice::remote::RemotePostRead::RemotePostRead()
    : output_msg_(),
      input_msg_(nullptr),
      hd_(nullptr),
      cce_addr_(),
      cc_res_(nullptr)
{
    res_ = &cc_res_;

    output_msg_.set_type(
        CcMessage::MessageType::CcMessage_MessageType_PostprocessResponse);

    cc_res_.post_lambda_ = [this](CcHandlerResult<Void> *res)
    {
        output_msg_.set_tx_number(input_msg_->tx_number());
        output_msg_.set_handler_addr(input_msg_->handler_addr());
        output_msg_.set_tx_term(input_msg_->tx_term());

        PostprocessResponse *resp = output_msg_.mutable_post_resp();
        resp->set_error_code(res->ErrorCode());

        const PostReadRequest &req = input_msg_->postread_req();
        hd_->SendResponse(req.src_node_id(), output_msg_);
        hd_->RecycleCcMsg(std::move(input_msg_));
    };
}

txservice::remote::RemoteRead::RemoteRead()
    : ReadCc(),
      output_msg_(),
      input_msg_(nullptr),
      hd_(nullptr),
      cc_res_(nullptr)
{
    res_ = &cc_res_;

    output_msg_.set_type(
        CcMessage::MessageType::CcMessage_MessageType_ReadResponse);

    cc_res_.post_lambda_ =
        [this](CcHandlerResult<
               std::tuple<TxRecord *, uint64_t, CcEntryAddr, RecordStatus>>
                   *res)
    {
        output_msg_.set_tx_number(input_msg_->tx_number());
        output_msg_.set_handler_addr(input_msg_->handler_addr());
        output_msg_.set_tx_term(input_msg_->tx_term());

        auto &res_tuple = res->Value();
        ReadResponse *resp = output_msg_.mutable_read_resp();
        resp->set_error_code(res->ErrorCode());

        if (!res->IsError())
        {
            switch (std::get<3>(res_tuple))
            {
            case RecordStatus::Normal:
                resp->set_rec_status(ReadResponse::RecordStatus::
                                         ReadResponse_RecordStatus_NORMAL);
                break;
            case RecordStatus::Deleted:
                resp->set_rec_status(ReadResponse::RecordStatus::
                                         ReadResponse_RecordStatus_DELETED);
                break;
            case RecordStatus::Unknown:
                resp->set_rec_status(ReadResponse::RecordStatus::
                                         ReadResponse_RecordStatus_UNKNOWN);
                break;
            default:
                break;
            }

            resp->set_ts(std::get<1>(res_tuple));

            CcEntryAddr &cce_addr = std::get<2>(res_tuple);
            CceAddr_msg *cce_addr_msg = resp->mutable_cce_addr();
            cce_addr_msg->set_cce_ptr(cce_addr.CcePtr());
            cce_addr_msg->set_term(cce_addr.Term());
        }

        const ReadRequest &req = input_msg_->read_req();
        hd_->SendResponse(req.src_node_id(), output_msg_);
        hd_->RecycleCcMsg(std::move(input_msg_));
    };
}

txservice::remote::RemotePostCommit::RemotePostCommit()
    : output_msg_(),
      input_msg_(nullptr),
      hd_(nullptr),
      cce_addr_(),
      cc_res_(nullptr)
{
    res_ = &cc_res_;

    output_msg_.set_type(
        CcMessage::MessageType::CcMessage_MessageType_PostprocessResponse);

    cc_res_.post_lambda_ = [this](CcHandlerResult<Void> *res)
    {
        output_msg_.set_tx_number(input_msg_->tx_number());
        output_msg_.set_handler_addr(input_msg_->handler_addr());
        output_msg_.set_tx_term(input_msg_->tx_term());

        PostprocessResponse *resp = output_msg_.mutable_post_resp();
        resp->set_error_code(res->ErrorCode());

        const PostCommitRequest &req = input_msg_->postcommit_req();
        hd_->SendResponse(req.src_node_id(), output_msg_);
        hd_->RecycleCcMsg(std::move(input_msg_));
    };
}

txservice::remote::RemotePostDelete::RemotePostDelete() : cc_res_(nullptr)
{
    res_ = &cc_res_;

    output_msg_.set_type(
        CcMessage::MessageType::CcMessage_MessageType_PostprocessResponse);

    cc_res_.post_lambda_ = [this](CcHandlerResult<Void> *res)
    {
        output_msg_.set_tx_number(input_msg_->tx_number());
        output_msg_.set_handler_addr(input_msg_->handler_addr());
        output_msg_.set_tx_term(input_msg_->tx_term());

        PostprocessResponse *resp = output_msg_.mutable_post_resp();
        resp->set_error_code(res->ErrorCode());

        const PostDeleteRequest &req = input_msg_->postdelete_req();
        hd_->SendResponse(req.src_node_id(), output_msg_);
        hd_->RecycleCcMsg(std::move(input_msg_));
    };
}

txservice::remote::RemoteScanOpen::RemoteScanOpen()
    : output_msg_(),
      input_msg_(nullptr),
      hd_(nullptr),
      node_group_id_(0),
      key_type_(KeyType::Normal),
      start_key_str_(nullptr),
      inclusive_(true),
      direct_(ScanDirection::Forward),
      scan_caches_(),
      is_ckpt_delta_(false),
      cc_res_(nullptr),
      unfinish_cnt_(0)
{
    output_msg_.set_type(
        CcMessage::MessageType::CcMessage_MessageType_ScanOpenResponse);
    res_ = &cc_res_;

    cc_res_.post_lambda_ = [this](CcHandlerResult<Void> *res)
    {
        output_msg_.set_tx_number(input_msg_->tx_number());
        output_msg_.set_handler_addr(input_msg_->handler_addr());
        output_msg_.set_tx_term(input_msg_->tx_term());

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
            for (int core_id = 0; core_id < scan_open->scan_cache_size();
                 ++core_id)
            {
                ScanCache_msg *cache = scan_open->mutable_scan_cache(core_id);
                cache->Clear();
            }
        }

        scan_open->set_node_group_id(node_group_id_);
        const ScanOpenRequest &req = input_msg_->scan_open_req();
        hd_->SendResponse(req.src_node_id(), output_msg_);
        hd_->RecycleCcMsg(std::move(input_msg_));
    };
}

void txservice::remote::RemoteScanOpen::Set(
    std::unique_ptr<CcMessage> input_msg,
    RemoteCcHandler *hd,
    uint32_t core_cnt)
{
    assert(input_msg->has_scan_open_req());

    cc_res_.Reset();
    cc_res_.SetRefCnt(core_cnt);

    const ScanOpenRequest &scan_open = input_msg->scan_open_req();

    node_group_id_ = scan_open.shard_id();
    table_name_ = &scan_open.tablename();
    ccm_ = nullptr;

    if (scan_open.start_key_case() == ScanOpenRequest::StartKeyCase::kNegInf)
    {
        key_type_ = KeyType::NegativeInf;
    }
    else if (scan_open.start_key_case() ==
             ScanOpenRequest::StartKeyCase::kPosInf)
    {
        key_type_ = KeyType::PostiveInf;
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
    for (size_t cid = 0; cid < core_cnt; ++cid)
    {
        ScanCache_msg *cache_msg = resp->add_scan_cache();
        assert(cache_msg->scan_tuple_size() == 0);
        scan_caches_.at(cid).clear();

        for (size_t idx = 0; idx < ScanCache::ScanBatchSize; ++idx)
        {
            ScanTuple_msg *tuple = cache_msg->add_scan_tuple();
            scan_caches_.at(cid).emplace_back(tuple);
        }
    }

    unfinish_cnt_.store(core_cnt);

    hd_ = hd;
    input_msg_ = std::move(input_msg);
}

void txservice::remote::RemoteScanOpen::Free()
{
    uint32_t prior_val = unfinish_cnt_.fetch_sub(1);
    if (prior_val == 1)
    {
        CcRequestBase::Free();
    }
}

txservice::remote::RemoteScanNextBatch::RemoteScanNextBatch() : cc_res_(nullptr)
{
    scan_cache_.reserve(ScanCache::ScanBatchSize);
    output_msg_.set_type(
        CcMessage::MessageType::CcMessage_MessageType_ScanNextResponse);
    is_ckpt_delta_ = false;
    res_ = &cc_res_;

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
            scan_next->mutable_scan_tuple()->Clear();
        }

        hd_->SendResponse(req.src_node_id(), output_msg_);
        hd_->RecycleCcMsg(std::move(input_msg_));
    };
}

void txservice::remote::RemoteScanNextBatch::Set(
    std::unique_ptr<CcMessage> input_msg, RemoteCcHandler *hd)
{
    /*message ScanNextRequest
    {
        uint32 src_node_id = 1;
            uint32 node_group_id = 2;
            uint64 prior_cce_ptr = 3;
            bool direction = 4;
            uint64 ts = 5;
            uint64 scan_cache_ptr = 6;
            bool ckpt = 7;
    }*/

    assert(input_msg->has_scan_next_req());

    cc_res_.Reset();

    const ScanNextRequest &scan_next = input_msg->scan_next_req();

    node_group_id_ = scan_next.node_group_id();
    prior_cce_addr_ = scan_next.prior_cce_ptr();
    direct_ = scan_next.direction() ? ScanDirection::Forward
                                    : ScanDirection::Backward;

    const LruEntry *prior_lru_entry =
        reinterpret_cast<const LruEntry *>(prior_cce_addr_);
    ccm_ = prior_lru_entry->parent_map_;

    output_msg_.clear_tx_number();
    output_msg_.clear_handler_addr();
    output_msg_.clear_scan_next_resp();

    ScanNextResponse *resp = output_msg_.mutable_scan_next_resp();
    assert(resp->scan_tuple_size() == 0);

    scan_cache_.clear();
    for (size_t idx = 0; idx < ScanCache::ScanBatchSize; ++idx)
    {
        ScanTuple_msg *tuple = resp->add_scan_tuple();
        scan_cache_.emplace_back(tuple);
    }

    is_ckpt_delta_ = scan_next.ckpt();

    hd_ = hd;
    input_msg_ = std::move(input_msg);
}

txservice::remote::RemoteCommitSk::RemoteCommitSk()
    : output_msg_(), input_msg_(nullptr), hd_(nullptr), cc_res_(nullptr)
{
    res_ = &cc_res_;

    output_msg_.set_type(
        CcMessage::MessageType::CcMessage_MessageType_PostprocessResponse);

    cc_res_.post_lambda_ = [this](CcHandlerResult<Void> *res)
    {
        output_msg_.set_tx_number(input_msg_->tx_number());
        output_msg_.set_tx_term(input_msg_->tx_term());
        output_msg_.set_handler_addr(input_msg_->handler_addr());

        PostprocessResponse *resp = output_msg_.mutable_post_resp();
        resp->set_error_code(res->ErrorCode());

        const CommitSkRequest &req = input_msg_->commit_sk_req();
        hd_->SendResponse(req.src_node_id(), output_msg_);
        hd_->RecycleCcMsg(std::move(input_msg_));
    };
}

txservice::remote::RemoteReadOutside::RemoteReadOutside()
    : input_msg_(nullptr),
      hd_(nullptr),
      rec_str_(nullptr),
      is_deleted_(false),
      cce_addr_()
{
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
        output_msg_.set_handler_addr(input_msg_->handler_addr());

        FaultInjectResponse *resp = output_msg_.mutable_fault_inject_resp();
        resp->set_error_code(res->ErrorCode());

        const FaultInjectRequest &req = input_msg_->fault_inject_req();
        hd_->SendResponse(req.src_node_id(), output_msg_);
        hd_->RecycleCcMsg(std::move(input_msg_));
    };
}
