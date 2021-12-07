#include "local_cc_handler.h"

#include "local_cc_shards.h"
#include "remote/remote_cc_handler.h"
#include "sharder.h"

txservice::LocalCcHandler::LocalCcHandler(uint32_t thd_id,
                                          LocalCcShards &shards)
    : thd_id_(thd_id),
      cc_shards_(shards),
      remote_hd_(*Sharder::Instance().GetCcStreamSender()),
      scan_alias_cnt_(0)
{
}

void txservice::LocalCcHandler::AcquireWrite(
    const TableName &table_name,
    const TxKey &key,
    const TxId &txid,
    int64_t tx_term,
    uint64_t ts,
    bool is_insert,
    CcHandlerResult<AcquireKeyResult> &hres,
    const CcProtocol proto)
{
    uint32_t shard_code = Sharder::Instance().ShardCode(key.Hash());
    uint32_t ng_id = shard_code >> 10;
    hres.Value().cce_addr_.SetNodeGroupId(ng_id);
    hres.Value().cce_addr_.SetCce(0, -1);

    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(ng_id);
    if (dest_node_id == cc_shards_.node_id_)
    {
        hres.Value().remote_ack_cnt_ = nullptr;

        AcquireCc *req = acquire_pool.NextRequest();
        req->Set(&table_name,
                 &key,
                 shard_code,
                 &txid,
                 tx_term,
                 ts,
                 is_insert,
                 &hres,
                 proto);
        cc_shards_.EnqueueCcRequest(thd_id_, shard_code, req);
    }
    else
    {
        hres.Value().remote_ack_cnt_->fetch_add(1);
        remote_hd_.AcquireWrite(cc_shards_.node_id_,
                                table_name,
                                key,
                                shard_code,
                                txid,
                                tx_term,
                                ts,
                                is_insert,
                                hres,
                                proto);
    }
}

/*
  Acquire table write lock on all the shards.
  Queries like DDL statements need to acquire table level write lock
  firstly to prevent concurrent DML queries.
 */
void txservice::LocalCcHandler::AcquireTableWriteLock(
    const TableName &table_name,
    const TxId &txid,
    int64_t tx_term,
    uint64_t tx_number,
    CcHandlerResult<std::unordered_map<uint32_t, int64_t>> &hd_res)
{
    uint32_t ng_cnt = Sharder::Instance().NodeGroupCount();

    // sends requests to all the local shards and remote nodes.
    // expect to receive one response from each remote nodes and
    // one response from each local shards.
    uint32_t dependent_cnt = ng_cnt - 1 + cc_shards_.Count();
    hd_res.SetRefCnt(dependent_cnt);

    for (uint32_t ng_id = 0; ng_id < ng_cnt; ++ng_id)
    {
        uint32_t node_id = Sharder::Instance().LeaderNodeId(ng_id);
        if (node_id == cc_shards_.node_id_)
        {
            std::unordered_map<uint32_t, int64_t> &ng_term_map = hd_res.Value();

            // record the node term when acquiring the table write lock.
            // For ccnode with multiple ccshards, we only get the term
            // once.
            int64_t ng_term = Sharder::Instance().LeaderTerm(node_id);
            if (ng_term < 0)
            {
                hd_res.SetError(-1);
                continue;
            }
            ng_term_map.try_emplace(node_id, ng_term);

            for (uint32_t core_id = 0; core_id < cc_shards_.Count(); ++core_id)
            {
                AcquireTableWriteLockCC *req =
                    table_write_lock_pool.NextRequest();

                req->Set(&table_name,
                         &txid,
                         tx_number,
                         cc_shards_.node_id_,
                         &hd_res);

                cc_shards_.EnqueueCcRequest(thd_id_, core_id, req);
            }
        }
        else
        {
            remote_hd_.AcquireTableWriteLock(cc_shards_.node_id_,
                                             table_name,
                                             txid,
                                             tx_term,
                                             tx_number,
                                             ng_id,
                                             hd_res);
        }
    }
}

void txservice::LocalCcHandler::ReleaseTableWriteLock(
    const TableName &table_name,
    const TxId &txid,
    int64_t tx_term,
    uint64_t tx_number,
    CcHandlerResult<Void> &hd_res)
{
    uint32_t ng_cnt = Sharder::Instance().NodeGroupCount();

    // sends requests to all the local shards and remote nodes.
    // expect to receive one response from each remote nodes and
    // one response from each local shards.
    uint32_t dependent_cnt = ng_cnt - 1 + cc_shards_.Count();
    hd_res.SetRefCnt(dependent_cnt);

    for (uint32_t ng_id = 0; ng_id < ng_cnt; ++ng_id)
    {
        uint32_t node_id = Sharder::Instance().LeaderNodeId(ng_id);
        if (node_id == cc_shards_.node_id_)
        {
            for (uint32_t core_id = 0; core_id < cc_shards_.Count(); ++core_id)
            {
                ReleaseTableWriteLockCC *req =
                    release_table_write_lock_pool.NextRequest();

                req->Set(&table_name,
                         &txid,
                         tx_number,
                         cc_shards_.node_id_,
                         &hd_res);

                cc_shards_.EnqueueCcRequest(thd_id_, core_id, req);
            }
        }
        else
        {
            remote_hd_.ReleaseTableWriteLock(cc_shards_.node_id_,
                                             table_name,
                                             txid,
                                             tx_term,
                                             tx_number,
                                             ng_id,
                                             hd_res);
        }
    }
}

void txservice::LocalCcHandler::ReleaseWrite(uint64_t tx_number,
                                             int64_t tx_term,
                                             const CcEntryAddr &cce_addr,
                                             CcHandlerResult<Void> &hres)
{
    uint32_t ng_id = cce_addr.NodeGroupId();
    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(ng_id);

    if (dest_node_id == cc_shards_.node_id_)
    {
        PostDeleteCc *req = postdel_pool.NextRequest();
        req->Set(&cce_addr, tx_number, &hres);
        const LruEntry *lru_entry =
            reinterpret_cast<const LruEntry *>(cce_addr.CcePtr());
        CcMap *ccm = lru_entry->parent_map_;

        ccm->shard_->Enqueue(thd_id_, req);
    }
    else
    {
        remote_hd_.ReleaseWrite(
            cc_shards_.node_id_, tx_number, tx_term, cce_addr, hres);
    }
}

void txservice::LocalCcHandler::CommitWrite(uint64_t tx_number,
                                            int64_t tx_term,
                                            uint64_t commit_ts,
                                            const CcEntryAddr &cce_addr,
                                            const TxRecord &record,
                                            bool is_deleted,
                                            CcHandlerResult<Void> &hres)
{
    uint32_t ng_id = cce_addr.NodeGroupId();
    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(ng_id);

    if (dest_node_id == cc_shards_.node_id_)
    {
        PostCommitCc *req = postcommit_pool.NextRequest();
        req->Set(&cce_addr, tx_number, commit_ts, &record, is_deleted, &hres);
        const LruEntry *lru_entry =
            reinterpret_cast<const LruEntry *>(cce_addr.CcePtr());
        CcMap *ccm = lru_entry->parent_map_;

        ccm->shard_->Enqueue(thd_id_, req);
    }
    else
    {
        remote_hd_.CommitWrite(cc_shards_.node_id_,
                               tx_number,
                               tx_term,
                               commit_ts,
                               cce_addr,
                               record,
                               is_deleted,
                               hres);
    }
}

void txservice::LocalCcHandler::ValidateRead(
    uint64_t tx_number,
    int64_t tx_term,
    uint64_t key_ts,
    uint64_t gap_ts,
    uint64_t commit_ts,
    const CcEntryAddr &cce_addr,
    CcHandlerResult<std::vector<TxId>> &hres)
{
    uint32_t ng_id = cce_addr.NodeGroupId();
    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(ng_id);

    if (dest_node_id == cc_shards_.node_id_)
    {
        ValidateCc *req = reread_pool.NextRequest();
        req->Set(&cce_addr, tx_number, commit_ts, key_ts, gap_ts, &hres);
        const LruEntry *lru_entry =
            reinterpret_cast<const LruEntry *>(cce_addr.CcePtr());
        CcMap *ccm = lru_entry->parent_map_;

        ccm->shard_->Enqueue(thd_id_, req);
    }
    else
    {
        remote_hd_.ValidateRead(cc_shards_.node_id_,
                                tx_number,
                                tx_term,
                                key_ts,
                                gap_ts,
                                commit_ts,
                                cce_addr,
                                hres);
    }
}

void txservice::LocalCcHandler::PostprocessRead(uint64_t tx_number,
                                                int64_t tx_term,
                                                const CcEntryAddr &cce_addr,
                                                CcHandlerResult<Void> &hres,
                                                CcProtocol proto)
{
    uint32_t ng_id = cce_addr.NodeGroupId();
    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(ng_id);

    if (dest_node_id == cc_shards_.node_id_)
    {
        PostReadCc *req = postread_pool.NextRequest();
        req->Set(&cce_addr, tx_number, &hres, proto);
        const LruEntry *lru_entry =
            reinterpret_cast<const LruEntry *>(cce_addr.CcePtr());
        CcMap *ccm = lru_entry->parent_map_;

        ccm->shard_->Enqueue(thd_id_, req);
    }
    else
    {
        remote_hd_.PostprocessRead(
            cc_shards_.node_id_, tx_number, tx_term, cce_addr, hres, proto);
    }
}

/*
  Postprocess of CREATE TABLE statement.
  Persist table catalog and refresh monograph share on runtime
 */
void txservice::LocalCcHandler::CommitCreateTable(
    const TableName &table_name,
    const unsigned char *catalog_image_,
    size_t catalog_length_,
    const TxId &txid,
    uint64_t ts,
    CcHandlerResult<Void> &hresult)
{
    uint32_t ng_cnt = Sharder::Instance().NodeGroupCount();

    // sends requests to all the local shards and remote nodes.
    // expect to receive one response from each remote nodes and
    // one response from each local shards.
    uint32_t dependent_cnt = ng_cnt - 1 + cc_shards_.Count();
    hresult.SetRefCnt(dependent_cnt);
    std::string catalog_str(
        (char *) const_cast<unsigned char *>(catalog_image_), catalog_length_);

    for (uint32_t ng_id = 0; ng_id < ng_cnt; ++ng_id)
    {
        uint32_t node_id = Sharder::Instance().LeaderNodeId(ng_id);
        if (node_id == cc_shards_.node_id_)
        {
            for (uint32_t core_id = 0; core_id < cc_shards_.Count(); ++core_id)
            {
                CommitCreateTableCC *req =
                    commit_create_table_pool.NextRequest();
                // only local request is responsible for creating table
                // in Cassandra
                bool is_local_req = true;
                req->Set(
                    &table_name, catalog_str, node_id, &hresult, is_local_req);
                cc_shards_.EnqueueCcRequest(thd_id_, core_id, req);
            }
        }
        else
        {
            remote_hd_.CommitCreateTable(cc_shards_.node_id_,
                                         table_name,
                                         catalog_str,
                                         txid,
                                         ts,
                                         ng_id,
                                         hresult);
        }
    }
}

/*
  Postprocess of DROP TABLE statement.
  Delete table catalog and refresh monograph share on runtime
 */
void txservice::LocalCcHandler::CommitDropTable(const TableName &table_name,
                                                const TxId &txid,
                                                uint64_t ts,
                                                CcHandlerResult<Void> &hresult)
{
    uint32_t ng_cnt = Sharder::Instance().NodeGroupCount();

    // sends requests to all the local shards and remote nodes.
    // expect to receive one response from each remote nodes and
    // one response from each local shards.
    uint32_t dependent_cnt = ng_cnt - 1 + cc_shards_.Count();
    hresult.SetRefCnt(dependent_cnt);

    for (uint32_t ng_id = 0; ng_id < ng_cnt; ++ng_id)
    {
        uint32_t node_id = Sharder::Instance().LeaderNodeId(ng_id);
        if (node_id == cc_shards_.node_id_)
        {
            for (uint32_t core_id = 0; core_id < cc_shards_.Count(); ++core_id)
            {
                CommitDropTableCC *req = commit_drop_table_pool.NextRequest();
                // only local request is responsible for dropping table
                // in Cassandra
                bool is_local_req = true;
                req->Set(&table_name, node_id, &hresult, is_local_req);
                cc_shards_.EnqueueCcRequest(thd_id_, core_id, req);
            }
        }
        else
        {
            remote_hd_.CommitDropTable(
                cc_shards_.node_id_, table_name, txid, ts, ng_id, hresult);
        }
    }
}

void txservice::LocalCcHandler::Read(const TableName &table_name,
                                     const TxKey &key,
                                     TxRecord &record,
                                     ReadType read_type,
                                     uint64_t tx_number,
                                     int64_t tx_term,
                                     const uint64_t ts,
                                     CcHandlerResult<ReadKeyResult> &hres,
                                     CcProtocol proto)
{
    hres.Value().rec_ = &record;
    uint32_t shard_code = Sharder::Instance().ShardCode(key.Hash());
    uint32_t shard_id = shard_code >> 10;
    CcEntryAddr &cce_addr = hres.Value().cce_addr_;
    cce_addr.SetNodeGroupId(shard_id);
    cce_addr.SetCce(0, -1);

    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(shard_id);
    if (dest_node_id == cc_shards_.node_id_)
    {
        ReadCc *req = read_pool.NextRequest();
        req->Set(&table_name,
                 &key,
                 shard_code,
                 &record,
                 read_type,
                 tx_number,
                 ts,
                 &hres,
                 proto);
        cc_shards_.EnqueueCcRequest(thd_id_, shard_code, req);
    }
    else
    {
        remote_hd_.Read(cc_shards_.node_id_,
                        table_name,
                        key,
                        shard_code,
                        record,
                        read_type,
                        tx_number,
                        tx_term,
                        ts,
                        hres,
                        proto);
    }
}

/*
 * ReadOutside fills the tuple read from KV into cache.
 */
void txservice::LocalCcHandler::ReadOutside(
    TxRecord &rec,
    bool is_deleted,
    const CcEntryAddr &cce_addr,
    CcHandlerResult<ReadKeyResult> &hres)
{
    assert(cce_addr.CcePtr() != 0);

    uint32_t ng_id = cce_addr.NodeGroupId();
    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(ng_id);

    if (dest_node_id == cc_shards_.node_id_)
    {
        ReadType read_type =
            is_deleted ? ReadType::OutsideDeleted : ReadType::OutsideNormal;

        hres.Value().cce_addr_.SetCce(
            cce_addr.CcePtr(), cce_addr.Term(), cce_addr.NodeGroupId());

        ReadCc *req = read_pool.NextRequest();
        req->Set(nullptr, nullptr, ng_id << 10, &rec, read_type, 0, 0, &hres);
        const LruEntry *lru_entry =
            reinterpret_cast<const LruEntry *>(cce_addr.CcePtr());
        CcMap *ccm = lru_entry->parent_map_;

        ccm->shard_->Enqueue(thd_id_, req);
    }
    else
    {
        remote_hd_.ReadOutside(rec, is_deleted, cce_addr);
        // we don't care whether the remote request succeeds or not,
        // since it's just a fill of cache.
        hres.Value().rec_status_ = RecordStatus::RemoteUnknown;
        hres.SetFinished();
    }
}

void txservice::LocalCcHandler::ScanOpen(
    const TableName &table_name,
    ScanIndexType index_type,
    const TxKey &start_key,
    bool inclusive,
    uint64_t tx_number,
    int64_t tx_term,
    uint64_t ts,
    CcHandlerResult<ScanOpenResult> &hd_res,
    ScanDirection direction,
    CcProtocol proto,
    bool is_ckpt_delta)
{
    CcShard &local_shard = *cc_shards_.cc_shards_.at(thd_id_);
    int8_t err_code = 0;
    CcMap *ccm = local_shard.GetCcm(table_name, local_shard.node_id_, err_code);
    if (ccm == nullptr)
    {
        hd_res.SetError(1);
        return;
    }

    uint32_t ng_cnt = Sharder::Instance().NodeGroupCount();
    // A scan sends requests to local cores and remote cc nodes.
    uint32_t dependent_cnt = ng_cnt - 1 + cc_shards_.Count();
    hd_res.SetRefCnt(dependent_cnt);

    ScanOpenResult &open_result = hd_res.Value();
    open_result.Reset(ng_cnt);

    open_result.scanner_ = ccm->CreateScanner(direction);
    CcScanner *scanner_ptr = open_result.scanner_.get();
    open_result.scan_alias_ = scan_alias_cnt_;
    ++scan_alias_cnt_;

    scanner_ptr->is_ckpt_delta_ = is_ckpt_delta;

    for (uint32_t ng_id = 0; ng_id < ng_cnt; ++ng_id)
    {
        uint32_t node_id = Sharder::Instance().LeaderNodeId(ng_id);
        if (node_id == cc_shards_.node_id_)
        {
            int local_term = Sharder::Instance().LeaderTerm(ng_id);
            if (local_term < 0)
            {
                // The local node is not the leader of the corresponding
                // cc node group. Skips scanning the local shards.
                open_result.cc_node_returned_[ng_id] = 1;
                for (uint32_t core_id = 0; core_id < cc_shards_.Count();
                     ++core_id)
                {
                    // The cc handler is set to be errored multiple times (i.e.,
                    // #core-count times), because the cc handler result's
                    // reference count includes the local core count.
                    hd_res.SetError(-1);
                }
                continue;
            }

            for (uint32_t core_id = 0; core_id < cc_shards_.Count(); ++core_id)
            {
                uint32_t shard_code = (ng_id << 10) + core_id;
                ScanCache *shard_scan_cache = scanner_ptr->AddShard(shard_code);

                ScanOpenBatchCc *req = scan_open_pool.NextRequest();
                req->Set(&table_name,
                         index_type,
                         ng_id,
                         &start_key,
                         inclusive,
                         direction,
                         tx_number,
                         ts,
                         shard_scan_cache,
                         local_term,
                         &hd_res,
                         proto,
                         scanner_ptr->is_ckpt_delta_);

                cc_shards_.EnqueueCcRequest(thd_id_, core_id, req);
            }

            open_result.cc_node_terms_[ng_id] = local_term;
            // For the local node, we mark the flag in the cc_node_returned
            // vector to true, even though the scan requests toward local cores
            // may not all finish. This is because the cc_node_returned vector
            // is used to trace remote requests, not requests toward local
            // cores.
            open_result.cc_node_returned_[ng_id] = 1;
        }
        else
        {
            remote_hd_.ScanOpen(cc_shards_.node_id_,
                                table_name,
                                index_type,
                                ng_id,
                                start_key,
                                inclusive,
                                tx_number,
                                tx_term,
                                ts,
                                hd_res,
                                direction,
                                proto,
                                scanner_ptr->is_ckpt_delta_);
        }
    }
}

void txservice::LocalCcHandler::ScanNextBatch(
    uint64_t tx_number,
    int64_t tx_term,
    uint64_t start_ts,
    CcScanner &scanner,
    CcHandlerResult<ScanNextResult> &hd_res,
    CcProtocol proto)
{
    uint32_t shard_code = scanner.BlockedShard();
    ScanCache *blocked_cache = scanner.Cache(shard_code);
    uint32_t node_group_id = shard_code >> 10;
    hd_res.Value().node_group_id_ = node_group_id;

    uint32_t node_id = Sharder::Instance().LeaderNodeId(node_group_id);
    if (node_id == cc_shards_.node_id_)
    {
        ScanNextBatchCc *req = scan_next_pool.NextRequest();
        req->Set(node_group_id,
                 start_ts,
                 blocked_cache,
                 &hd_res,
                 proto,
                 scanner.is_ckpt_delta_);

        cc_shards_.EnqueueCcRequest(thd_id_, shard_code, req);
    }
    else
    {
        remote_hd_.ScanNext(cc_shards_.node_id_,
                            node_group_id,
                            tx_number,
                            tx_term,
                            start_ts,
                            blocked_cache,
                            hd_res,
                            proto,
                            scanner.is_ckpt_delta_);
    }
}

void txservice::LocalCcHandler::CommitSecondaryKey(const TableName &table_name,
                                                   const TxKey &sk,
                                                   const TxKey &pk,
                                                   bool is_delete,
                                                   uint64_t ts,
                                                   CcHandlerResult<Void> &hres)
{
    uint64_t hash = TxKey::HashCode(sk, pk);
    uint32_t shard_code = Sharder::Instance().ShardCode(hash);

    uint32_t node_id = Sharder::Instance().LeaderNodeId(shard_code >> 10);
    if (node_id == cc_shards_.node_id_)
    {
        CommitSkCc *req = commitsk_pool.NextRequest();
        req->Set(&table_name, &sk, &pk, shard_code, ts, is_delete, &hres);
        cc_shards_.EnqueueCcRequest(thd_id_, shard_code, req);
    }
    else
    {
        remote_hd_.CommitSecondaryKey(cc_shards_.node_id_,
                                      table_name,
                                      sk,
                                      pk,
                                      shard_code,
                                      is_delete,
                                      ts,
                                      hres);
    }
}

void txservice::LocalCcHandler::NewTxn(CcHandlerResult<InitTxResult> &hres)
{
    CcShard &ccs = *(cc_shards_.cc_shards_[thd_id_]);
    TEntry &tx = ccs.NewTx();

    int64_t term = Sharder::Instance().LeaderTerm(ccs.node_id_);
    if (term >= 0)
    {
        InitTxResult &init_tx_res = hres.Value();
        init_tx_res.txid_ = tx.GetTxId(ccs.GlobalCoreId());
        init_tx_res.start_ts_ = tx.lower_bound_;
        init_tx_res.term_ = term;
        hres.SetFinished();
    }
    else
    {
        // When the native cc node is not the leader, we could try to
        // find another cc node group whose leader is in this node and
        // bind the new tx to it. However, since this node is not the
        // preferred leader of that cc node group, it is very likely
        // that that leader will be transferred soon. For simplicity, we stop
        // creating new tx's if the native node is not the leader for now.
        hres.SetError(-1);
    }
}

void txservice::LocalCcHandler::SetCommitTimestamp(
    const TxId &txid, uint64_t commit_ts, CcHandlerResult<uint64_t> &hres)
{
    CcShard &ccs = *cc_shards_.cc_shards_[thd_id_];
    TEntry &tx = ccs.tx_vec_.at(txid.vec_idx_);
    assert(tx.ident_ == txid.ident_);
    uint64_t local_ts = ccs.ts_base_.load(std::memory_order_relaxed);
    tx.commit_ts_ = std::max(local_ts, std::max(tx.lower_bound_, commit_ts));

    // The thread-local timer is monotonically increasing. If the
    // comparison fails, the timer must have been advanced by the
    // machine clock and the newest time must be greater than
    // tx.commit_ts_ + 1.
    ccs.ts_base_.compare_exchange_strong(local_ts,
                                         std::max(local_ts, tx.commit_ts_ + 1));

    hres.SetValue(tx.commit_ts_);
    hres.SetFinished();
}

void txservice::LocalCcHandler::UpdateCommitLowerBound(
    const TxId &txid,
    uint64_t commit_ts_lower_bound,
    CcHandlerResult<uint64_t> &hres)
{
    NegotiateCc *req = negoti_pool.NextRequest();
    req->Set(&txid, commit_ts_lower_bound, &hres);
    // The lower 10 bits represent the local core Id. The remaining high
    // bits represent the node Id.
    uint16_t local_core_id = txid.global_core_id_ & 0x3FF;
    cc_shards_.EnqueueCcRequest(thd_id_, local_core_id, req);
}

void txservice::LocalCcHandler::UpdateTxnStatus(const TxId &txid,
                                                TxnStatus status,
                                                CcHandlerResult<Void> &hres)
{
    CcShard &ccs = *(cc_shards_.cc_shards_[thd_id_]);
    TEntry &te = ccs.tx_vec_.at(txid.vec_idx_);
    assert(te.ident_ == txid.ident_);
    te.status_ = status;
    hres.SetFinished();
}

void txservice::LocalCcHandler::FindCatalogInCCShard(
    const TableName &table_name,
    std::string *catalog_content,
    uint64_t tx_number,
    CcHandlerResult<bool> &hres)
{
    CcShard &ccs = *(cc_shards_.cc_shards_[thd_id_]);

    FindCatalogCC *req = commit_find_catalog_pool.NextRequest();
    req->Set(&table_name, catalog_content, tx_number, &hres);

    // Put the request in the queue if failed to get read intention.
    if (!ccs.AcquireTableReadIntention(table_name, req))
    {
        return;
    }

    // FindCatalog immediately from local ccshard.
    bool ret = ccs.FindCatalog(table_name, catalog_content);

    hres.SetValue(ret);
    hres.SetFinished();
}
void txservice::LocalCcHandler::CheckCatalogVersionInCCShard(
    const TableName &table_name,
    std::string *source_version,
    uint64_t tx_number,
    CcHandlerResult<bool> &hres)
{
    CcShard &ccs = *(cc_shards_.cc_shards_[thd_id_]);

    CheckCatalogCC *req = commit_check_catalog_pool.NextRequest();
    req->Set(&table_name, source_version, tx_number, &hres);

    // Put the request in the queue if failed to get read intention.
    if (!ccs.AcquireTableReadIntention(table_name, req))
    {
        return;
    }

    // Get CheckCatalogVersion immediately from local ccshard.
    bool ret = ccs.CheckCatalogVersion(table_name, *source_version);

    hres.SetValue(ret);
    hres.SetFinished();
}

void txservice::LocalCcHandler::FaultInject(const std::string &fault_name,
                                            const std::string &fault_type,
                                            int node_id,
                                            CcHandlerResult<bool> &hres)
{
    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(node_id);
    if (dest_node_id == cc_shards_.node_id_)
    {
        FaultInjectCC *req = fault_inject_pool.NextRequest();
        req->Set(&fault_name, &fault_type, &hres);
        cc_shards_.EnqueueCcRequest(0, req);
    }
    else
    {
        remote_hd_.FaultInject(
            cc_shards_.node_id_, fault_name, fault_type, node_id, hres);
    }
}

// release all the acquired table level locks for all the opened tables.
void txservice::LocalCcHandler::ReleaseAllTableLocks(
    std::unordered_set<std::string> opened_table_set,
    uint64_t tx_number,
    CcHandlerResult<bool> &hres)
{
    CcShard &ccs = *(cc_shards_.cc_shards_[thd_id_]);

    for (const auto &elem : opened_table_set)
    {
        ccs.ReleaseAllTableLocks(elem, tx_number);
    }

    hres.SetValue(true);
    hres.SetFinished();
}

/*
 * Get the node id which runs the current transaction.
 */
uint32_t txservice::LocalCcHandler::GetNodeId() const
{
    return cc_shards_.NodeId();
}
