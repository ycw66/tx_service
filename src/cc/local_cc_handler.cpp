#include "local_cc_handler.h"

#include <string>

#include "local_cc_shards.h"
#include "remote/remote_cc_handler.h"
#include "sharder.h"
#include "tx_record.h"
#include "tx_trace.h"
#include "type.h"

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
        TX_TRACE_ACTION(this, req);
        TX_TRACE_DUMP(req);
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

void txservice::LocalCcHandler::AcquireWriteAll(
    const TableName &table_name,
    const TxKey &key,
    NodeGroupId ng_id,
    TxNumber txn,
    int64_t tx_term,
    bool is_insert,
    CcHandlerResult<AcquireAllResult> &hres,
    CcProtocol proto,
    LockType lock_type)
{
    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(ng_id);
    if (dest_node_id == cc_shards_.node_id_)
    {
        hres.Value().remote_ack_cnt_ = nullptr;
        AcquireAllCc *req = acquire_all_pool_.NextRequest();
        req->Set(&table_name,
                 &key,
                 ng_id,
                 txn,
                 tx_term,
                 is_insert,
                 &hres,
                 proto,
                 lock_type);
        TX_TRACE_ACTION(this, req);
        TX_TRACE_DUMP(req);
        // The request is dispatched to the first core and then passed to
        // remaining cores consecutively.
        cc_shards_.EnqueueCcRequest(thd_id_, 0, req);
    }
    else
    {
        remote_hd_.AcquireWriteAll(cc_shards_.node_id_,
                                   table_name,
                                   key,
                                   ng_id,
                                   txn,
                                   tx_term,
                                   is_insert,
                                   hres,
                                   proto,
                                   lock_type);
    }
}

void txservice::LocalCcHandler::PostWriteAll(const TableName &table_name,
                                             const TxKey &key,
                                             TxRecord &rec,
                                             NodeGroupId ng_id,
                                             uint64_t tx_number,
                                             int64_t tx_term,
                                             uint64_t commit_ts,
                                             CcHandlerResult<Void> &hres,
                                             DmlOperation dml_op,
                                             PostWriteType post_write_type)
{
    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(ng_id);
    if (dest_node_id == cc_shards_.node_id_)
    {
        PostWriteAllCc *req = postwrite_all_pool_.NextRequest();
        req->Set(&table_name,
                 &key,
                 ng_id,
                 tx_number,
                 commit_ts,
                 &rec,
                 dml_op,
                 &hres,
                 post_write_type);
        TX_TRACE_ACTION(this, req);
        TX_TRACE_DUMP(req);
        // The request is dispatched to the first core and then passed to
        // remaining cores consecutively.
        cc_shards_.EnqueueCcRequest(thd_id_, 0, req);
    }
    else
    {
        remote_hd_.PostWriteAll(cc_shards_.node_id_,
                                table_name,
                                key,
                                rec,
                                ng_id,
                                tx_number,
                                tx_term,
                                commit_ts,
                                hres,
                                dml_op,
                                post_write_type);
    }
}

void txservice::LocalCcHandler::PostWrite(uint64_t tx_number,
                                          int64_t tx_term,
                                          uint64_t commit_ts,
                                          const CcEntryAddr &cce_addr,
                                          const TxRecord *record,
                                          bool is_deleted,
                                          CcHandlerResult<Void> &hres,
                                          CcProtocol protocol)
{
    uint32_t ng_id = cce_addr.NodeGroupId();
    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(ng_id);

    if (dest_node_id == cc_shards_.node_id_)
    {
        if (!Sharder::Instance().CheckLeaderTerm(ng_id, cce_addr.Term()))
        {
            // Term mismatch means this PostWrite is failovered to the current
            // node, and locks are already lost during failover hence no need to
            // release the lock again.
            hres.SetFinished();
            return;
        }

        PostWriteCc *req = postwrite_pool.NextRequest();
        req->Set(&cce_addr,
                 tx_number,
                 commit_ts,
                 record,
                 is_deleted,
                 &hres,
                 protocol);
        TX_TRACE_ACTION(this, req);
        TX_TRACE_DUMP(req);
        const LruEntry *lru_entry =
            reinterpret_cast<const LruEntry *>(cce_addr.CcePtr());
        CcMap *ccm = lru_entry->parent_map_;

        ccm->shard_->Enqueue(thd_id_, req);
    }
    else
    {
        remote_hd_.PostWrite(cc_shards_.node_id_,
                             tx_number,
                             tx_term,
                             commit_ts,
                             cce_addr,
                             record,
                             is_deleted,
                             hres,
                             protocol);
    }
}

void txservice::LocalCcHandler::PostRead(
    uint64_t tx_number,
    int64_t tx_term,
    uint64_t key_ts,
    uint64_t gap_ts,
    uint64_t commit_ts,
    const CcEntryAddr &cce_addr,
    CcHandlerResult<std::vector<TxId>> &hres,
    CcProtocol protocol,
    LockType lock_type)
{
    uint32_t ng_id = cce_addr.NodeGroupId();
    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(ng_id);

    if (dest_node_id == cc_shards_.node_id_)
    {
        if (!Sharder::Instance().CheckLeaderTerm(ng_id, cce_addr.Term()))
        {
            // Term mismatch means this PostRead is failovered to the current
            // node, and locks are already lost during failover hence validation
            // can only return error and transaction needs to be aborted.
            hres.SetError(-1);
            return;
        }

        PostReadCc *req = postread_pool_.NextRequest();
        req->Set(&cce_addr,
                 tx_number,
                 commit_ts,
                 key_ts,
                 gap_ts,
                 &hres,
                 protocol,
                 lock_type);
        TX_TRACE_ACTION(this, req);
        TX_TRACE_DUMP(req);
        const LruEntry *lru_entry =
            reinterpret_cast<const LruEntry *>(cce_addr.CcePtr());
        CcMap *ccm = lru_entry->parent_map_;

        ccm->shard_->Enqueue(thd_id_, req);
    }
    else
    {
        remote_hd_.PostRead(cc_shards_.node_id_,
                            tx_number,
                            tx_term,
                            key_ts,
                            gap_ts,
                            commit_ts,
                            cce_addr,
                            hres,
                            protocol,
                            lock_type);
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
                                     IsolationLevel iso_level,
                                     CcProtocol proto,
                                     LockType lock_type)
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
                 tx_term,
                 ts,
                 &hres,
                 iso_level,
                 proto,
                 lock_type);
        TX_TRACE_ACTION(this, req);
        TX_TRACE_DUMP(req);
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
                        iso_level,
                        proto,
                        lock_type);
    }
}

/*
 * ReadOutside fills the tuple read from KV into cache.
 */
void txservice::LocalCcHandler::ReadOutside(
    int64_t tx_term,
    TxRecord &rec,
    bool is_deleted,
    uint64_t commit_ts,
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
        // A read-outside request brings a record into the cc map for caching.
        // Its tx number is meaningless: it does not represent the tx committing
        // the record. Thus, it is set to 0. The commit timestamp is set to 1,
        // the beginning of history. Once a record is flushed to the data store
        // and kicked out from the cc map, the record history only exists in the
        // log. We pretend the record exists since the beginning of history,
        // which is good enough for the concurrency control purpose. The
        // isolation level is set to read committed, so that the request leaves
        // no read intention or lock on the cc entry.
        req->Set(nullptr,
                 nullptr,
                 ng_id << 10,
                 &rec,
                 read_type,
                 0,
                 tx_term,
                 commit_ts,
                 &hres,
                 IsolationLevel::ReadCommitted,
                 CcProtocol::OCC,
                 LockType::NoLock);
        TX_TRACE_ACTION(this, req);
        TX_TRACE_DUMP(req);
        const LruEntry *lru_entry =
            reinterpret_cast<const LruEntry *>(cce_addr.CcePtr());
        CcMap *ccm = lru_entry->parent_map_;

        ccm->shard_->Enqueue(thd_id_, req);
    }
    else
    {
        remote_hd_.ReadOutside(tx_term, rec, is_deleted, commit_ts, cce_addr);
        // we don't care whether the remote request succeeds or not,
        // since it's just a fill of cache.
        hres.Value().rec_status_ = RecordStatus::RemoteUnknown;
        hres.SetFinished();
    }
}

void txservice::LocalCcHandler::ReadLocal(const TableName &table_name,
                                          const TxKey &key,
                                          TxRecord &record,
                                          ReadType read_type,
                                          uint64_t tx_number,
                                          int64_t tx_term,
                                          const uint64_t ts,
                                          CcHandlerResult<ReadKeyResult> &hres,
                                          IsolationLevel iso_level,
                                          CcProtocol proto,
                                          LockType lock_type)
{
    ReadKeyResult &read_result = hres.Value();
    read_result.rec_ = &record;
    read_result.rec_status_ = RecordStatus::Unknown;
    read_result.ts_ = 0;
    CcEntryAddr &cce_addr = read_result.cce_addr_;

    CcShard &ccs = *(cc_shards_.cc_shards_[thd_id_]);
    int64_t term = Sharder::Instance().LeaderTerm(ccs.node_id_);
    uint32_t shard_code = tx_number >> 32L;
    uint32_t cc_ng_id = shard_code >> 10;
    cce_addr.SetNodeGroupId(cc_ng_id);
    cce_addr.SetCce(0, term);

    if (term < 0)
    {
        // When a tx starts, the tx can only be bound to a native cc node who is
        // the leader. Since a read local request is dispatched to the same
        // shard to which the tx is bound, if the native cc node is not the
        // leader now, returns an error.
        hres.SetError(-1);
        return;
    }

    ReadCc *read_req = read_pool.NextRequest();
    read_req->Set(&table_name,
                  &key,
                  shard_code,
                  &record,
                  read_type,
                  tx_number,
                  tx_term,
                  ts,
                  &hres,
                  iso_level,
                  proto,
                  lock_type);
    TX_TRACE_ACTION(this, read_req);
    TX_TRACE_DUMP(read_req);

    int8_t err_code = 0;
    CcMap *ccm = ccs.GetCcm(table_name, cc_ng_id, err_code);

    if (ccm != nullptr)
    {  //__catalog table will be preloaded when ccshard constructed
        ccm->Execute(*read_req);
    }
    else
    {  // otherwise, let the TemplateCcRequest load in the data
        ccs.Enqueue(read_req);
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
    IsolationLevel iso_level,
    CcProtocol proto,
    LockType lock_type,
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
    size_t core_cnt = cc_shards_.Count();
    // A scan sends requests to local cores and remote cc nodes.
    uint32_t dependent_cnt = ng_cnt - 1 + core_cnt;
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

            for (uint32_t core_id = 0; core_id < core_cnt; ++core_id)
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
                         iso_level,
                         proto,
                         lock_type,
                         scanner_ptr->is_ckpt_delta_);

                TX_TRACE_ACTION(this, req);
                TX_TRACE_DUMP(req);
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
                                iso_level,
                                proto,
                                lock_type,
                                scanner_ptr->is_ckpt_delta_);
        }
    }
}

void txservice::LocalCcHandler::ScanOpenLocal(
    const TableName &table_name,
    ScanIndexType index_type,
    const TxKey &start_key,
    bool inclusive,
    uint64_t tx_number,
    int64_t tx_term,
    uint64_t ts,
    CcHandlerResult<ScanOpenResult> &hd_res,
    ScanDirection direction,
    IsolationLevel iso_level,
    CcProtocol proto,
    LockType lock_type,
    bool is_ckpt_delta)
{
    // TODO: consolidate these kind term check in some common place
    if (tx_term < 0)
    {
        // When a tx starts, the tx can only be bound to a native cc node who is
        // the leader. Since a read local request is dispatched to the same
        // shard to which the tx is bound, if the native cc node is not the
        // leader now, returns an error.
        hd_res.SetError(-1);
        return;
    }

    // Check if the table exists
    CcShard &local_shard = *cc_shards_.cc_shards_.at(thd_id_);
    int8_t err_code = 0;
    CcMap *ccm = local_shard.GetCcm(table_name, local_shard.node_id_, err_code);

    if (ccm == nullptr)
    {
        hd_res.SetError(-1);
        return;
    }

    ScanOpenResult &open_result = hd_res.Value();
    open_result.Reset(1);

    open_result.scanner_ = ccm->CreateScanner(direction);
    CcScanner *scanner_ptr = open_result.scanner_.get();
    open_result.scan_alias_ = scan_alias_cnt_++;
    scanner_ptr->is_ckpt_delta_ = is_ckpt_delta;
    uint32_t ng_id = local_shard.node_id_;
    uint32_t shard_code = (ng_id << 10) + local_shard.core_id_;
    ScanCache *shard_scan_cache = scanner_ptr->AddShard(shard_code);

    ScanOpenBatchCc *scan_open_cc_req = scan_open_pool.NextRequest();
    scan_open_cc_req->Set(&table_name,
                          index_type,
                          ng_id,
                          &start_key,
                          inclusive,
                          direction,
                          tx_number,
                          ts,
                          shard_scan_cache,
                          tx_term,
                          &hd_res,
                          iso_level,
                          proto,
                          LockType::ReadLock,
                          scanner_ptr->is_ckpt_delta_);

    TX_TRACE_ACTION(this, scan_open_cc_req);
    TX_TRACE_DUMP(scan_open_cc_req);
    ccm->Execute(*scan_open_cc_req);
}

void txservice::LocalCcHandler::ScanNextBatch(
    uint64_t tx_number,
    int64_t tx_term,
    uint64_t start_ts,
    CcScanner &scanner,
    CcHandlerResult<ScanNextResult> &hd_res,
    IsolationLevel iso_level,
    CcProtocol proto,
    LockType lock_type)
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
                 tx_number,
                 start_ts,
                 blocked_cache,
                 tx_term,
                 &hd_res,
                 iso_level,
                 proto,
                 lock_type,
                 scanner.is_ckpt_delta_);

        TX_TRACE_ACTION(this, req);
        TX_TRACE_DUMP(req);
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
                            iso_level,
                            proto,
                            lock_type,
                            scanner.is_ckpt_delta_);
    }
}

void txservice::LocalCcHandler::ScanNextBatchLocal(
    uint64_t tx_number,
    int64_t tx_term,
    uint64_t start_ts,
    CcScanner &scanner,
    CcHandlerResult<ScanNextResult> &hd_res,
    IsolationLevel iso_level,
    CcProtocol proto)
{
    uint32_t shard_code = scanner.BlockedShard();
    ScanCache *blocked_cache = scanner.Cache(shard_code);
    uint32_t node_group_id = shard_code >> 10;
    hd_res.Value().node_group_id_ = node_group_id;

    CcShard &local_shard = *cc_shards_.cc_shards_.at(thd_id_);
    ScanNextBatchCc *req = scan_next_pool.NextRequest();
    req->Set(node_group_id,
             tx_number,
             start_ts,
             blocked_cache,
             tx_term,
             &hd_res,
             iso_level,
             proto,
             LockType::ReadLock,
             scanner.is_ckpt_delta_);
    TX_TRACE_ACTION(this, req);
    TX_TRACE_DUMP(req);
    local_shard.Enqueue(req);
}

void txservice::LocalCcHandler::CommitSecondaryKey(TxNumber txn,
                                                   int64_t tx_term,
                                                   const TableName &table_name,
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
        TX_TRACE_ACTION(this, req);
        TX_TRACE_DUMP(req);
        cc_shards_.EnqueueCcRequest(thd_id_, shard_code, req);
    }
    else
    {
        remote_hd_.CommitSecondaryKey(cc_shards_.node_id_,
                                      txn,
                                      tx_term,
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

    // Code injection for test InitTxRequest failure
    CODE_FAULT_INJECTOR("init_tx_error", {
        // This injection just run once
        term = -2;
    });

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
    TX_TRACE_ACTION(this, req);
    TX_TRACE_DUMP(req);
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

void txservice::LocalCcHandler::FaultInject(const std::string &fault_name,
                                            const std::string &fault_paras,
                                            int64_t tx_term,
                                            const TxId &txid,
                                            std::vector<int> &vct_node_id,
                                            CcHandlerResult<bool> &hres)
{
    if (vct_node_id.size() == 0)
        vct_node_id.push_back(cc_shards_.node_id_);
    else if (vct_node_id[0] == -1)
    {
        vct_node_id.clear();
        for (int i = 0; i < (int) Sharder::Instance().NodeGroupCount(); i++)
        {
            vct_node_id.push_back(i);
        }
    }

    hres.SetRefCnt(vct_node_id.size());
    for (int id : vct_node_id)
    {
        uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(id);
        if (dest_node_id == cc_shards_.node_id_)
        {
            FaultInjectCC *req = fault_inject_pool.NextRequest();
            req->Set(&fault_name, &fault_paras, &hres);
            TX_TRACE_ACTION(this, req);
            TX_TRACE_DUMP(req);
            cc_shards_.EnqueueCcRequest(0, req);
        }
        else
        {
            remote_hd_.FaultInject(cc_shards_.node_id_,
                                   fault_name,
                                   fault_paras,
                                   tx_term,
                                   txid,
                                   id,
                                   hres);
        }
    }
}

void txservice::LocalCcHandler::DataStoreUpsertTable(
    const TableName &table_name,
    const TableSchema *schema,
    bool is_deleted,
    uint64_t commit_ts,
    CcHandlerResult<Void> &hres)
{
    cc_shards_.store_hd_->UpsertTable(
        table_name, schema, is_deleted, commit_ts, &hres);
}

/*
 * Get the node id which runs the current transaction.
 */
uint32_t txservice::LocalCcHandler::GetNodeId() const
{
    return cc_shards_.NodeId();
}

/**
 * @brief Get value of "ts_base".
 */
uint64_t txservice::LocalCcHandler::GetTsBaseValue() const
{
    CcShard &ccs = *(cc_shards_.cc_shards_[thd_id_]);
    return ccs.Now();
}
