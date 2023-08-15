#include "local_cc_handler.h"

#include <chrono>
#include <string>

#include "error_messages.h"  //CcErrorCode
#include "local_cc_shards.h"
#include "remote/remote_cc_handler.h"
#include "remote/remote_type.h"
#include "sharder.h"
#include "statistics.h"
#include "tx_execution.h"
#include "tx_record.h"
#include "tx_trace.h"
#include "tx_worker_pool.h"
#include "type.h"

DECLARE_bool(skip_wal);

txservice::LocalCcHandler::LocalCcHandler(uint32_t thd_id,
                                          LocalCcShards &shards)
    : thd_id_(thd_id),
      cc_shards_(shards),
      remote_hd_(*Sharder::Instance().GetCcStreamSender())
{
}

void txservice::LocalCcHandler::AcquireWrite(
    const TableName &table_name,
    const TxKey &key,
    uint32_t key_shard_code,
    TxNumber tx_number,
    int64_t tx_term,
    uint16_t command_id,
    uint64_t ts,
    bool is_insert,
    CcHandlerResult<std::vector<AcquireKeyResult>> &hres,
    uint32_t hd_res_idx,
    CcProtocol proto,
    IsolationLevel iso_level)
{
    uint32_t ng_id = Sharder::Instance().ShardToCcNodeGroup(key_shard_code);
    AcquireKeyResult &acquire_result = hres.Value()[hd_res_idx];
    acquire_result.cce_addr_.SetNodeGroupId(ng_id);
    acquire_result.cce_addr_.SetCce(0, -1, 0);

    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(ng_id);
    if (dest_node_id == cc_shards_.node_id_)
    {
        AcquireCc *req = acquire_pool.NextRequest();
        req->Reset(&table_name,
                   &key,
                   key_shard_code,
                   tx_number,
                   tx_term,
                   ts,
                   is_insert,
                   &hres,
                   hd_res_idx,
                   proto,
                   iso_level);
        TX_TRACE_ACTION(this, req);
        TX_TRACE_DUMP(req);
        cc_shards_.EnqueueCcRequest(thd_id_, key_shard_code, req);
    }
    else
    {
        acquire_result.remote_ack_cnt_->fetch_add(1, std::memory_order_relaxed);
        remote_hd_.AcquireWrite(cc_shards_.node_id_,
                                ng_id,
                                table_name,
                                key,
                                key_shard_code,
                                tx_number,
                                tx_term,
                                command_id,
                                ts,
                                is_insert,
                                hres,
                                hd_res_idx,
                                proto,
                                iso_level);
    }
}

void txservice::LocalCcHandler::AcquireWriteAll(
    const TableName &table_name,
    const TxKey &key,
    NodeGroupId ng_id,
    TxNumber txn,
    int64_t tx_term,
    uint16_t command_id,
    bool is_insert,
    CcHandlerResult<AcquireAllResult> &hres,
    CcProtocol proto,
    CcOperation cc_op)
{
    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(ng_id);
    if (dest_node_id == cc_shards_.node_id_)
    {
        hres.Value().remote_ack_cnt_ = nullptr;
        AcquireAllCc *req = acquire_all_pool_.NextRequest();
        req->Reset(&table_name,
                   &key,
                   ng_id,
                   txn,
                   tx_term,
                   is_insert,
                   &hres,
                   proto,
                   cc_op);
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
                                   command_id,
                                   is_insert,
                                   hres,
                                   proto,
                                   cc_op);
        hres.Value().remote_ack_cnt_->fetch_add(1);
    }
}

void txservice::LocalCcHandler::PostWriteAll(
    const TableName &table_name,
    const TxKey &key,
    TxRecord &rec,
    NodeGroupId ng_id,
    uint64_t tx_number,
    int64_t tx_term,
    uint16_t command_id,
    uint64_t commit_ts,
    CcHandlerResult<PostProcessResult> &hres,
    OperationType op_type,
    PostWriteType post_write_type)
{
    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(ng_id);
    if (dest_node_id == cc_shards_.node_id_)
    {
        PostWriteAllCc *req = postwrite_all_pool_.NextRequest();

        // When PostWriteAll is directed to leaders of two cc node groups in
        // same physical node, the two cc requests should reference their own
        // records. The record of a PostWriteAllCc has two roles: (1) upload a
        // serialized schema image, (2) return a pointer to the schema object
        // cached in the tx service.
        if (ng_id == cc_shards_.node_id_)
        {
            req->Reset(&table_name,
                       &key,
                       ng_id,
                       tx_number,
                       commit_ts,
                       &rec,
                       op_type,
                       &hres,
                       post_write_type,
                       tx_term);
        }
        else
        {
            std::unique_ptr<TxRecord> dup_rec = rec.Clone();
            req->Reset(&table_name,
                       &key,
                       ng_id,
                       tx_number,
                       commit_ts,
                       std::move(dup_rec),
                       op_type,
                       &hres,
                       post_write_type,
                       tx_term);
        }

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
                                command_id,
                                commit_ts,
                                hres,
                                op_type,
                                post_write_type);
    }
}

void txservice::LocalCcHandler::PostWrite(
    uint64_t tx_number,
    int64_t tx_term,
    uint16_t command_id,
    uint64_t commit_ts,
    const CcEntryAddr &cce_addr,
    const TxRecord *record,
    OperationType operation_type,
    uint32_t key_shard_code,
    CcHandlerResult<PostProcessResult> &hres)
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
        req->Reset(&cce_addr,
                   tx_number,
                   commit_ts,
                   record,
                   operation_type,
                   key_shard_code,
                   &hres);
        TX_TRACE_ACTION(this, req);
        TX_TRACE_DUMP(req);
        cc_shards_.EnqueueCcRequest(thd_id_, cce_addr.CoreId(), req);
    }
    else
    {
        hres.Value().is_local_ = false;
        hres.IncrementRemoteRef();
        remote_hd_.PostWrite(cc_shards_.node_id_,
                             tx_number,
                             tx_term,
                             command_id,
                             commit_ts,
                             cce_addr,
                             record,
                             operation_type,
                             key_shard_code,
                             hres);
    }
}

void txservice::LocalCcHandler::ForwardPostWrite(
    TxNumber tx_number,
    int64_t tx_term,
    uint16_t command_id,
    uint64_t commit_ts,
    const TableName &table_name,
    const TxKey *key,
    const TxRecord *record,
    OperationType operation_type,
    uint32_t key_shard_code,
    CcHandlerResult<PostProcessResult> &hres,
    bool blocked,
    int64_t expected_term)
{
    uint32_t ng_id = Sharder::Instance().ShardToCcNodeGroup(key_shard_code);
    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(ng_id);

    if (dest_node_id == cc_shards_.node_id_)
    {
        if (!Sharder::Instance().CheckLeaderTerm(ng_id, tx_term))
        {
            // Term mismatch means this PostWrite is failovered to the current
            // node, and locks are already lost during failover hence no need to
            // release the lock again.
            hres.SetFinished();
            return;
        }

        if (expected_term != SKIP_CHECK_TERM)
        {
            // The transaction that performs this operation requires that the
            // node group leader cannot change during the entire transaction
            // process. Therefore, it is necessary to record the leader term of
            // each target node group when performing the operation for the
            // first time. At the same time, in subsequent operations, by
            // checking the node group's term to confirm whether changes have
            // occurred.
            int64_t ng_term = Sharder::Instance().LeaderTerm(ng_id);
            if (expected_term != ng_term)
            {
                assert(expected_term > 0);
                // Leader transferred. For example, a remote node group
                // transferred to local node.
                hres.SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
                LOG(ERROR) << "LocalCcHandler::ForwardPostWrite: The leader of "
                              "the destinate node group transferred for ng#"
                           << ng_id;
                return;
            }
        }

        PostWriteCc *req = postwrite_pool.NextRequest();

        req->Reset(key,
                   table_name,
                   ng_id,
                   tx_number,
                   commit_ts,
                   record,
                   operation_type,
                   key_shard_code,
                   &hres,
                   (operation_type == OperationType::Insert),
                   blocked);

        TX_TRACE_ACTION(this, req);
        TX_TRACE_DUMP(req);
        cc_shards_.EnqueueCcRequest(key_shard_code, req);
    }
    else
    {
        hres.Value().is_local_ = false;
        hres.IncrementRemoteRef();

        remote_hd_.ForwardPostWrite(cc_shards_.node_id_,
                                    tx_number,
                                    tx_term,
                                    command_id,
                                    commit_ts,
                                    expected_term,
                                    key,
                                    table_name,
                                    record,
                                    operation_type,
                                    key_shard_code,
                                    hres,
                                    blocked);
    }
}

void txservice::LocalCcHandler::PostRead(
    uint64_t tx_number,
    int64_t tx_term,
    uint16_t command_id,
    uint64_t key_ts,
    uint64_t gap_ts,
    uint64_t commit_ts,
    const CcEntryAddr &cce_addr,
    CcHandlerResult<PostProcessResult> &hres)
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
            hres.SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            return;
        }

        PostReadCc *req = postread_pool_.NextRequest();
        req->Reset(&cce_addr, tx_number, commit_ts, key_ts, gap_ts, &hres);
        TX_TRACE_ACTION(this, req);
        TX_TRACE_DUMP(req);
        cc_shards_.EnqueueCcRequest(thd_id_, cce_addr.CoreId(), req);
    }
    else
    {
        hres.Value().is_local_ = false;
        hres.IncrementRemoteRef();
        remote_hd_.PostRead(cc_shards_.node_id_,
                            tx_number,
                            tx_term,
                            command_id,
                            key_ts,
                            gap_ts,
                            commit_ts,
                            cce_addr,
                            hres);
    }
}

void txservice::LocalCcHandler::Read(const TableName &table_name,
                                     const TxKey &key,
                                     uint32_t key_shard_code,
                                     TxRecord &record,
                                     ReadType read_type,
                                     uint64_t tx_number,
                                     int64_t tx_term,
                                     uint16_t command_id,
                                     const uint64_t ts,
                                     CcHandlerResult<ReadKeyResult> &hres,
                                     IsolationLevel iso_level,
                                     CcProtocol proto,
                                     bool is_for_write,
                                     bool is_covering_keys)
{
    hres.Value().rec_ = &record;
    uint32_t cc_ng_id = Sharder::Instance().ShardToCcNodeGroup(key_shard_code);
    ReadKeyResult &read_result = hres.Value();
    CcEntryAddr &cce_addr = read_result.cce_addr_;
    cce_addr.SetNodeGroupId(cc_ng_id);
    cce_addr.SetCce(0, -1, 0);

    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(cc_ng_id);
    if (dest_node_id == cc_shards_.node_id_)
    {
        read_result.is_local_ = true;

        ReadCc *req = read_pool.NextRequest();
        req->Reset(&table_name,
                   &key,
                   key_shard_code,
                   &record,
                   read_type,
                   tx_number,
                   tx_term,
                   ts,
                   &hres,
                   iso_level,
                   proto,
                   is_for_write,
                   is_covering_keys);
        TX_TRACE_ACTION(this, req);
        TX_TRACE_DUMP(req);
        cc_shards_.EnqueueCcRequest(thd_id_, key_shard_code, req);
    }
    else
    {
        read_result.is_local_ = false;

        remote_hd_.Read(cc_shards_.node_id_,
                        cc_ng_id,
                        table_name,
                        key,
                        key_shard_code,
                        record,
                        read_type,
                        tx_number,
                        tx_term,
                        command_id,
                        ts,
                        hres,
                        iso_level,
                        proto,
                        is_for_write,
                        is_covering_keys);
    }
}

/*
 * ReadOutside fills the tuple read from KV into cache.
 */
void txservice::LocalCcHandler::ReadOutside(
    int64_t tx_term,
    uint16_t command_id,
    TxRecord &rec,
    bool is_deleted,
    uint64_t commit_ts,
    const CcEntryAddr &cce_addr,
    CcHandlerResult<ReadKeyResult> &hres,
    std::vector<VersionTxRecord> *archives)
{
    assert(cce_addr.CcePtr() != 0);

    uint32_t ng_id = cce_addr.NodeGroupId();
    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(ng_id);

    if (dest_node_id == cc_shards_.node_id_)
    {
        ReadType read_type =
            is_deleted ? ReadType::OutsideDeleted : ReadType::OutsideNormal;

        hres.Value().cce_addr_.SetCce(cce_addr.CcePtr(),
                                      cce_addr.Term(),
                                      cce_addr.NodeGroupId(),
                                      cce_addr.CoreId());

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
        req->Reset(nullptr,
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
                   false,
                   archives);

        TX_TRACE_ACTION(this, req);
        TX_TRACE_DUMP(req);
        cc_shards_.EnqueueCcRequest(thd_id_, cce_addr.CoreId(), req);
    }
    else
    {
        remote_hd_.ReadOutside(
            tx_term, command_id, rec, is_deleted, commit_ts, cce_addr);
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
                                          uint16_t command_id,
                                          const uint64_t ts,
                                          CcHandlerResult<ReadKeyResult> &hres,
                                          IsolationLevel iso_level,
                                          CcProtocol proto,
                                          bool is_for_write,
                                          bool is_recovering)
{
    ReadKeyResult &read_result = hres.Value();
    read_result.rec_ = &record;
    read_result.rec_status_ = RecordStatus::Unknown;
    read_result.ts_ = 0;
    read_result.is_local_ = true;
    CcEntryAddr &cce_addr = read_result.cce_addr_;

    CcShard *ccs;
    if (table_name == cluster_config_ccm_name)
    {
        // cluster config map is only initialized on core 0. If we're
        // visiting cluster config ccm, we need to send a regular read
        // req to another core.
        ccs = cc_shards_.cc_shards_[0].get();
    }
    else
    {
        ccs = cc_shards_.cc_shards_[thd_id_].get();
    }
    int64_t term;
    if (is_recovering)
    {
        term = Sharder::Instance().CandidateLeaderTerm(ccs->node_id_);
    }
    else
    {
        term = Sharder::Instance().LeaderTerm(ccs->node_id_);
    }
    uint32_t shard_code = tx_number >> 32L;
    uint32_t cc_ng_id = shard_code >> 10;
    cce_addr.SetNodeGroupId(cc_ng_id);
    cce_addr.SetCce(0, term, 0);

    if (term < 0)
    {
        // When a tx starts, the tx can only be bound to a native cc node who is
        // the leader. Since a read local request is dispatched to the same
        // shard to which the tx is bound, if the native cc node is not the
        // leader now, returns an error.
        hres.SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
        return;
    }

    ReadCc *read_req = read_pool.NextRequest();
    read_req->Reset(&table_name,
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
                    is_for_write,
                    false,
                    nullptr,
                    is_recovering);
    TX_TRACE_ACTION(this, read_req);
    TX_TRACE_DUMP(read_req);

    CcMap *ccm = ccs->GetCcm(table_name, cc_ng_id);

    if (ccm != nullptr && thd_id_ == ccs->core_id_)
    {  //__catalog table will be preloaded when ccshard constructed
        bool finished = ccm->Execute(*read_req);
        if (finished)
        {
            read_req->Free();
        }
    }
    else
    {  // otherwise, let the TemplateCcRequest load in the data
        ccs->Enqueue(read_req);
    }
}

void txservice::LocalCcHandler::ScanOpen(
    const TableName &table_name,
    ScanIndexType index_type,
    const TxKey &start_key,
    bool inclusive,
    uint64_t tx_number,
    int64_t tx_term,
    uint16_t command_id,
    uint64_t ts,
    CcHandlerResult<ScanOpenResult> &hd_res,
    ScanDirection direction,
    IsolationLevel iso_level,
    CcProtocol proto,
    bool is_for_write,
    bool is_ckpt_delta,
    bool is_covering_keys)
{
    CcShard &local_shard = *cc_shards_.cc_shards_[thd_id_];

    std::unique_ptr<CcScanner> ccm_scanner = nullptr;
    if (table_name.Type() == TableType::Secondary ||
        table_name.Type() == TableType::UniqueSecondary)
    {
        const TableName base_table_name{table_name.GetBaseTableNameSV(),
                                        TableType::Primary};
        const CatalogEntry *catalog_entry =
            local_shard.GetCatalog(base_table_name, local_shard.node_id_);

        if (catalog_entry == nullptr || catalog_entry->schema_ == nullptr)
        {
            hd_res.SetError(CcErrorCode::REQUESTED_TABLE_NOT_EXISTS);
            return;
        }

        const SecondaryKeySchema *index_key_schema =
            catalog_entry->schema_->IndexKeySchema(table_name);
        if (index_key_schema == nullptr)
        {
            hd_res.SetError(CcErrorCode::REQUESTED_INDEX_TABLE_NOT_EXISTS);
            return;
        }

        if (direction == ScanDirection::Forward &&
            sk_forward_scanner_.Size() > 0)
        {
            ccm_scanner = std::move(sk_forward_scanner_.Peek());
            sk_forward_scanner_.Dequeue();
            ccm_scanner->Reset(index_key_schema);
        }
        else if (direction == ScanDirection::Backward &&
                 sk_backward_scanner_.Size() > 0)
        {
            ccm_scanner = std::move(sk_backward_scanner_.Peek());
            sk_backward_scanner_.Dequeue();
            ccm_scanner->Reset(index_key_schema);
        }
        else
        {
            ccm_scanner = local_shard.catalog_factory_->CreateSkCcmScanner(
                direction, index_key_schema);
        }
    }
    else
    {
        const CatalogEntry *catalog_entry =
            local_shard.GetCatalog(table_name, local_shard.node_id_);

        if (catalog_entry == nullptr || catalog_entry->schema_ == nullptr)
        {
            hd_res.SetError(CcErrorCode::REQUESTED_TABLE_NOT_EXISTS);
            return;
        }

        const Schema *key_schema = catalog_entry->schema_->KeySchema();

        if (direction == ScanDirection::Forward &&
            pk_forward_scanner_.Size() > 0)
        {
            ccm_scanner = std::move(pk_forward_scanner_.Peek());
            pk_forward_scanner_.Dequeue();
            ccm_scanner->Reset(key_schema);
        }
        else if (direction == ScanDirection::Backward &&
                 pk_backward_scanner_.Size() > 0)
        {
            ccm_scanner = std::move(pk_backward_scanner_.Peek());
            pk_backward_scanner_.Dequeue();
            ccm_scanner->Reset(key_schema);
        }
        else
        {
            ccm_scanner = local_shard.catalog_factory_->CreatePkCcmScanner(
                direction, key_schema);
        }
    }

    ScanOpenResult &open_result = hd_res.Value();

    open_result.scanner_ = std::move(ccm_scanner);
    CcScanner *scanner_ptr = open_result.scanner_.get();
    assert(open_result.scan_alias_ < UINT64_MAX);
    scanner_ptr->SetStatus(ScannerStatus::Open);
    scanner_ptr->SetDrainCacheMode(false);
    scanner_ptr->is_ckpt_delta_ = is_ckpt_delta;
    scanner_ptr->is_for_write_ = is_for_write;
    scanner_ptr->is_covering_keys_ = is_covering_keys;
    scanner_ptr->iso_level_ = iso_level;
    scanner_ptr->protocol_ = proto;
    scanner_ptr->read_local_ = false;

#ifdef RANGE_PARTITION_ENABLED
    hd_res.SetFinished();
#else
    uint32_t ng_cnt = Sharder::Instance().NodeGroupCount();
    open_result.Reset(ng_cnt);
    size_t core_cnt = cc_shards_.Count();
    // A scan sends requests to local cores and remote cc nodes.
    uint32_t dependent_cnt = ng_cnt - 1 + core_cnt;
    hd_res.SetRefCnt(dependent_cnt);

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
                    hd_res.SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
                }
                continue;
            }

            for (uint32_t core_id = 0; core_id < core_cnt; ++core_id)
            {
                uint32_t shard_code = (ng_id << 10) + core_id;
                ScanCache *shard_scan_cache = scanner_ptr->AddShard(shard_code);
                ScanOpenBatchCc *req = scan_open_pool.NextRequest();
                req->Reset(&table_name,
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
                           is_for_write,
                           is_ckpt_delta,
                           is_covering_keys);

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
                                command_id,
                                ts,
                                hd_res,
                                direction,
                                iso_level,
                                proto,
                                is_for_write,
                                is_ckpt_delta,
                                is_covering_keys);
        }
    }
#endif
}

void txservice::LocalCcHandler::ScanOpenLocal(
    const TableName &table_name,
    ScanIndexType index_type,
    const TxKey &start_key,
    bool inclusive,
    uint64_t tx_number,
    int64_t tx_term,
    uint16_t command_id,
    uint64_t ts,
    CcHandlerResult<ScanOpenResult> &hd_res,
    ScanDirection direction,
    IsolationLevel iso_level,
    CcProtocol proto,
    bool is_for_write,
    bool is_ckpt_delta)
{
    // TODO: consolidate these kind term check in some common place
    if (tx_term < 0)
    {
        // When a tx starts, the tx can only be bound to a native cc node who is
        // the leader. Since a read local request is dispatched to the same
        // shard to which the tx is bound, if the native cc node is not the
        // leader now, returns an error.
        hd_res.SetError(CcErrorCode::TX_NODE_NOT_LEADER);
        return;
    }

    CcShard &local_shard = *cc_shards_.cc_shards_.at(thd_id_);

    const Schema *schema = nullptr;
    std::unique_ptr<CcScanner> ccm_scanner = nullptr;
    if (table_name.Type() == TableType::RangePartition)
    {
        const TableName base_table_name{table_name.StringView(),
                                        TableType::Primary};
        const CatalogEntry *catalog_entry =
            local_shard.GetCatalog(base_table_name, local_shard.node_id_);
        if (catalog_entry != nullptr && catalog_entry->schema_ != nullptr)
        {
            schema = catalog_entry->schema_.get()->KeySchema();
        }

        ccm_scanner = local_shard.catalog_factory_->CreateRangeCcmScanner(
            direction, schema, table_name);
    }
    else if (table_name.Type() == TableType::Secondary ||
             table_name.Type() == TableType::UniqueSecondary)
    {
        const TableName base_table_name{table_name.StringView(),
                                        TableType::Primary};
        const CatalogEntry *catalog_entry =
            local_shard.GetCatalog(base_table_name, local_shard.node_id_);
        if (catalog_entry != nullptr && catalog_entry->schema_ != nullptr)
        {
            schema = catalog_entry->schema_.get()->IndexKeySchema(table_name);
        }
        ccm_scanner =
            local_shard.catalog_factory_->CreateSkCcmScanner(direction, schema);
    }
    else
    {
        const CatalogEntry *catalog_entry =
            local_shard.GetCatalog(table_name, local_shard.node_id_);

        if (catalog_entry != nullptr && catalog_entry->schema_ != nullptr)
        {
            schema = catalog_entry->schema_.get()->KeySchema();
        }

        ccm_scanner =
            local_shard.catalog_factory_->CreatePkCcmScanner(direction, schema);
    }

    if (ccm_scanner == nullptr)
    {
        hd_res.SetError(CcErrorCode::CRATE_CCM_SCANNER_FAILED);
        return;
    }

    ScanOpenResult &open_result = hd_res.Value();
    open_result.Reset(1);

    open_result.scanner_ = std::move(ccm_scanner);
    CcScanner *scanner_ptr = open_result.scanner_.get();
    assert(open_result.scan_alias_ < UINT64_MAX);
    scanner_ptr->SetStatus(ScannerStatus::Open);
    scanner_ptr->SetDrainCacheMode(false);
    scanner_ptr->is_ckpt_delta_ = is_ckpt_delta;
    scanner_ptr->is_for_write_ = is_for_write;
    scanner_ptr->iso_level_ = iso_level;
    scanner_ptr->protocol_ = proto;
    scanner_ptr->read_local_ = true;

    uint32_t ng_id = local_shard.node_id_;
    uint32_t shard_code = (ng_id << 10) + local_shard.core_id_;
    ScanCache *shard_scan_cache = scanner_ptr->AddShard(shard_code);

    ScanOpenBatchCc *scan_open_cc_req = scan_open_pool.NextRequest();
    scan_open_cc_req->Reset(&table_name,
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
                            scanner_ptr->iso_level_,
                            scanner_ptr->protocol_,
                            scanner_ptr->is_for_write_,
                            scanner_ptr->is_ckpt_delta_,
                            scanner_ptr->is_covering_keys_);

    TX_TRACE_ACTION(this, scan_open_cc_req);
    TX_TRACE_DUMP(scan_open_cc_req);
    // Check if the table exists
    CcMap *ccm = local_shard.GetCcm(table_name, local_shard.node_id_);

    if (ccm != nullptr)
    {
        bool finished = ccm->Execute(*scan_open_cc_req);
        if (finished)
        {
            scan_open_cc_req->Free();
        }
    }
    else
    {
        local_shard.Enqueue(scan_open_cc_req);
    }
}

void txservice::LocalCcHandler::ScanNextBatch(
    uint64_t tx_number,
    int64_t tx_term,
    uint16_t command_id,
    uint64_t start_ts,
    CcScanner &scanner,
    CcHandlerResult<ScanNextResult> &hd_res)
{
    uint32_t shard_code = scanner.BlockedShard();
    ScanCache *blocked_cache = scanner.Cache(shard_code);
    uint32_t node_group_id = shard_code >> 10;
    hd_res.Value().node_group_id_ = node_group_id;

    uint32_t node_id = Sharder::Instance().LeaderNodeId(node_group_id);
    if (node_id == cc_shards_.node_id_)
    {
        hd_res.Value().is_local_ = true;
        ScanNextBatchCc *req = scan_next_pool.NextRequest();
        req->Reset(node_group_id,
                   tx_number,
                   start_ts,
                   blocked_cache,
                   tx_term,
                   &hd_res,
                   scanner.iso_level_,
                   scanner.protocol_,
                   scanner.is_for_write_,
                   scanner.is_ckpt_delta_,
                   scanner.is_covering_keys_);

        TX_TRACE_ACTION(this, req);
        TX_TRACE_DUMP(req);
        cc_shards_.EnqueueCcRequest(thd_id_, shard_code, req);
    }
    else
    {
        hd_res.Value().is_local_ = false;
        remote_hd_.ScanNext(cc_shards_.node_id_,
                            node_group_id,
                            tx_number,
                            tx_term,
                            command_id,
                            start_ts,
                            blocked_cache,
                            hd_res,
                            scanner.iso_level_,
                            scanner.protocol_,
                            scanner.is_for_write_,
                            scanner.is_ckpt_delta_);
    }
}

void txservice::LocalCcHandler::ScanNextBatch(
    const TableName &tbl_name,
    uint32_t range_id,
    NodeGroupId range_owner,
    int64_t cc_ng_term,
    const TxKey *start_key,
    bool start_inclusive,
    const TxKey *end_key,
    bool end_inclusive,
    uint8_t prefetch_size,
    uint64_t read_ts,
    uint64_t tx_number,
    int64_t tx_term,
    uint16_t command_id,
    CcHandlerResult<RangeScanSliceResult> &hd_res,
    IsolationLevel iso_level,
    CcProtocol proto)
{
    hd_res.Value().cc_ng_id_ = range_owner;
    uint32_t node_id = Sharder::Instance().LeaderNodeId(range_owner);
    if (node_id == cc_shards_.node_id_)
    {
        hd_res.Value().is_local_ = true;
        CcScanner &scanner = *hd_res.Value().ccm_scanner_;
        ScanSliceCc *req = scan_slice_pool.NextRequest();
        req->Set(tbl_name,
                 range_id,
                 range_owner,
                 cc_ng_term,
                 start_key,
                 start_inclusive,
                 end_key,
                 end_inclusive,
                 read_ts,
                 tx_number,
                 tx_term,
                 hd_res,
                 iso_level,
                 proto,
                 scanner.is_for_write_,
                 scanner.is_covering_keys_,
                 prefetch_size);

        uint32_t core_cnt = cc_shards_.Count();
        req->SetShardCount(core_cnt);

        // When the cc ng term is less than 0, this is the first scan of the
        // specified range.
        if (cc_ng_term < 0)
        {
            scanner.ResetShards(core_cnt);
        }

        for (uint32_t core_id = 0; core_id < core_cnt; ++core_id)
        {
            ScanCache *cache = scanner.Cache(core_id);
            const ScanTuple *last_tuple = cache->LastTuple();

            // The entry address is available only after lock has been acquired.
            // But Occ|ReadCommitted won't acquire any lock.
            LockType lock_type = LockType::NoLock;
            if (last_tuple != nullptr)
            {
                lock_type =
                    scanner.DeduceScanTupleLockType(last_tuple->rec_status_);
            }

            req->SetPriorCceAddr(lock_type == LockType::NoLock
                                     ? 0
                                     : last_tuple->cce_addr_.CcePtr(),
                                 core_id);
            req->SetCcePtr(nullptr, core_id);

            cache->Reset();
        }

        // The scan slice request is dispatched to the first core. The first
        // core tries to pin the slice in memory and if succeeds, further
        // dispatches the request to remaining cores for parallel scans.
        cc_shards_.EnqueueCcRequest(thd_id_, 0, req);
    }
    else
    {
        hd_res.Value().is_local_ = false;
        remote_hd_.ScanNext(cc_shards_.node_id_,
                            tbl_name,
                            range_id,
                            range_owner,
                            cc_ng_term,
                            start_key,
                            start_inclusive,
                            end_key,
                            end_inclusive,
                            prefetch_size,
                            read_ts,
                            tx_number,
                            tx_term,
                            command_id,
                            hd_res,
                            iso_level,
                            proto);
    }
}

void txservice::LocalCcHandler::ScanNextBatchLocal(
    uint64_t tx_number,
    int64_t tx_term,
    uint16_t command_id,
    uint64_t start_ts,
    CcScanner &scanner,
    CcHandlerResult<ScanNextResult> &hd_res)
{
    uint32_t shard_code = scanner.BlockedShard();
    ScanCache *blocked_cache = scanner.Cache(shard_code);
    uint32_t node_group_id = shard_code >> 10;
    hd_res.Value().node_group_id_ = node_group_id;

    CcShard &local_shard = *cc_shards_.cc_shards_.at(thd_id_);
    ScanNextBatchCc *req = scan_next_pool.NextRequest();
    req->Reset(node_group_id,
               tx_number,
               start_ts,
               blocked_cache,
               tx_term,
               &hd_res,
               scanner.iso_level_,
               scanner.protocol_,
               scanner.is_for_write_,
               scanner.is_ckpt_delta_,
               scanner.is_covering_keys_);
    TX_TRACE_ACTION(this, req);
    TX_TRACE_DUMP(req);
    local_shard.Enqueue(req);
}

void txservice::LocalCcHandler::ScanClose(const TableName &table_name,
                                          ScanDirection direction,
                                          std::unique_ptr<CcScanner> scanner)
{
    assert(scanner->Direction() == direction);

    if (table_name.Type() == TableType::Primary)
    {
        assert(scanner->IndexType() == ScanIndexType::Primary);

        if (direction == ScanDirection::Forward)
        {
            pk_forward_scanner_.Enqueue(std::move(scanner));
        }
        else
        {
            pk_backward_scanner_.Enqueue(std::move(scanner));
        }
    }
    else if (table_name.Type() == TableType::Secondary)
    {
        assert(scanner->IndexType() == ScanIndexType::Secondary);

        if (direction == ScanDirection::Forward)
        {
            sk_forward_scanner_.Enqueue(std::move(scanner));
        }
        else
        {
            sk_backward_scanner_.Enqueue(std::move(scanner));
        }
    }
}

void txservice::LocalCcHandler::NewTxn(CcHandlerResult<InitTxResult> &hres,
                                       IsolationLevel iso_level)
{
    CcShard &ccs = *(cc_shards_.cc_shards_[thd_id_]);

    int64_t term = Sharder::Instance().LeaderTerm(ccs.node_id_);

    // Code injection for test InitTxRequest failure
    CODE_FAULT_INJECTOR("init_tx_error", {
        // This injection just run once
        term = -2;
    });

    if (term >= 0)
    {
        // NewTx reads each ccshard's next_tx_ident_, which is set concurrently
        // by log replay thread when native cc node finishes log replay. The two
        // events are synchronized by leader_term_.
        TEntry &tx = ccs.NewTx();
        InitTxResult &init_tx_res = hres.Value();
        init_tx_res.txid_ = tx.GetTxId(ccs.GlobalCoreId());
        TxNumber txn = init_tx_res.txid_.TxNumber();
        init_tx_res.start_ts_ = tx.lower_bound_;
        init_tx_res.term_ = tx.term_;
        hres.SetFinished();

        // Update active tx info
        if (iso_level == IsolationLevel::Snapshot)
        {
            ccs.AddActiveSiTx(txn, tx.lower_bound_);
        }
    }
    else
    {
        // When the native cc node is not the leader, we could try to
        // find another cc node group whose leader is in this node and
        // bind the new tx to it. However, since this node is not the
        // preferred leader of that cc node group, it is very likely
        // that that leader will be transferred soon. For simplicity, we stop
        // creating new tx's if the native node is not the leader for now.
        hres.SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
    }
}

void txservice::LocalCcHandler::SetCommitTimestamp(
    const TxId &txid, uint64_t commit_ts, CcHandlerResult<uint64_t> &hres)
{
    CcShard &ccs = *cc_shards_.cc_shards_[thd_id_];
    TEntry &tx = ccs.tx_vec_.at(txid.vec_idx_);
    assert(tx.ident_ == txid.ident_);
    uint64_t local_ts = ccs.Now();
    tx.commit_ts_ = std::max(local_ts, std::max(tx.lower_bound_, commit_ts));

    // The thread-local timer is monotonically increasing. If the
    // comparison fails, the timer must have been advanced by the
    // machine clock and the newest time must be greater than
    // tx.commit_ts_ + 1.
    ccs.UpdateTsBase(std::max(local_ts, tx.commit_ts_ + 1));

    hres.SetValue(std::move(tx.commit_ts_));
    hres.SetFinished();
}

void txservice::LocalCcHandler::UpdateCommitLowerBound(
    const TxId &txid,
    uint64_t commit_ts_lower_bound,
    CcHandlerResult<uint64_t> &hres)
{
    NegotiateCc *req = negoti_pool.NextRequest();
    req->Reset(&txid, commit_ts_lower_bound, &hres);
    TX_TRACE_ACTION(this, req);
    TX_TRACE_DUMP(req);
    // The lower 10 bits represent the local core Id. The remaining high
    // bits represent the node Id.
    uint16_t local_core_id = txid.global_core_id_ & 0x3FF;
    cc_shards_.EnqueueCcRequest(thd_id_, local_core_id, req);
}

void txservice::LocalCcHandler::UpdateTxnStatus(const TxId &txid,
                                                IsolationLevel iso_level,
                                                TxnStatus status,
                                                CcHandlerResult<Void> &hres)
{
    CcShard &ccs = *(cc_shards_.cc_shards_[thd_id_]);
    TEntry &te = ccs.tx_vec_.at(txid.vec_idx_);
    assert(te.ident_ == txid.ident_);
    te.status_ = status;
    hres.SetFinished();

    // Update active tx info
    if (iso_level == IsolationLevel::Snapshot)
    {
        ccs.RemoveActiveSiTx(txid.TxNumber());
    }
}

void txservice::LocalCcHandler::FaultInject(const std::string &fault_name,
                                            const std::string &fault_paras,
                                            int64_t tx_term,
                                            uint16_t command_id,
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
            req->Reset(&fault_name, &fault_paras, &hres);
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
                                   command_id,
                                   txid,
                                   id,
                                   hres);
        }
    }
}

void txservice::LocalCcHandler::DataStoreUpsertTable(
    const TableSchema *schema,
    OperationType op_type,
    uint64_t commit_ts,
    CcHandlerResult<Void> &hres,
    const txservice::AlterTableInfo *alter_table_info)
{
    cc_shards_.store_hd_->UpsertTable(
        schema, op_type, commit_ts, &hres, alter_table_info);
}

void txservice::LocalCcHandler::AnalyzeTableAll(const TableName &table_name,
                                                NodeGroupId ng_id,
                                                TxNumber tx_number,
                                                int64_t tx_term,
                                                uint16_t command_id,
                                                CcHandlerResult<Void> &hres)
{
    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(ng_id);
    if (dest_node_id == cc_shards_.NodeId())
    {
        uint32_t shard_code =
            Statistics::ShardCode(table_name.GetBaseTableNameSV());
        AnalyzeTableAllCc *req = analyze_table_all_pool.NextRequest();
        req->Reset(&table_name, ng_id, tx_number, &hres);
        cc_shards_.EnqueueCcRequest(thd_id_, shard_code, req);
    }
    else
    {
        hres.IncrementRemoteRef();
        remote_hd_.AnalyzeTableAll(cc_shards_.node_id_,
                                   table_name,
                                   ng_id,
                                   tx_number,
                                   tx_term,
                                   command_id,
                                   hres);
    }
}

void txservice::LocalCcHandler::ObjectCommand(
    const txservice::TableName &table_name,
    const txservice::TxKey &key,
    uint32_t key_shard_code,
    const txservice::TxCommand &obj_cmd,
    txservice::TxCommandResult &obj_cmd_result,
    txservice::TxNumber txn,
    int64_t tx_term,
    uint64_t tx_ts,
    txservice::CcHandlerResult<txservice::ObjectCommandResult> &hres,
    const txservice::CcProtocol proto,
    bool commit)
{
    uint32_t ng_id = Sharder::Instance().ShardToCcNodeGroup(key_shard_code);
    hres.Value().cce_addr_.SetCce(0, -1, ng_id, 0);

    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(ng_id);
    if (dest_node_id == cc_shards_.node_id_)
    {
        ApplyCc *req = apply_pool.NextRequest();
        req->Reset(&table_name,
                   &key,
                   key_shard_code,
                   &obj_cmd,
                   &obj_cmd_result,
                   txn,
                   tx_term,
                   tx_ts,
                   &hres,
                   proto,
                   commit);
        cc_shards_.EnqueueCcRequest(thd_id_, key_shard_code, req);
    }
    else
    {
        // TODO(zkl): support remote requests.
    }
}

void txservice::LocalCcHandler::CleanCcEntryForTest(const TableName &table_name,
                                                    const TxKey &key,
                                                    bool only_archives,
                                                    bool flush,
                                                    uint64_t tx_number,
                                                    int64_t tx_term,
                                                    uint16_t command_id,
                                                    CcHandlerResult<bool> &hres)
{
    uint32_t shard_code = Sharder::Instance().ShardCode(key.Hash());
    uint32_t shard_id = shard_code >> 10;

    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(shard_id);
    if (dest_node_id == cc_shards_.node_id_)
    {
        CleanCcEntryForTestCc *req = clean_cc_entry_pool.NextRequest();
        req->Reset(&table_name,
                   &key,
                   only_archives,
                   flush,
                   shard_code,
                   tx_number,
                   &hres);
        TX_TRACE_ACTION(this, req);
        TX_TRACE_DUMP(req);
        cc_shards_.EnqueueCcRequest(thd_id_, shard_code, req);
    }
    else
    {
        remote_hd_.CleanCcEntryForTest(cc_shards_.node_id_,
                                       table_name,
                                       key,
                                       only_archives,
                                       flush,
                                       shard_code,
                                       tx_number,
                                       tx_term,
                                       command_id,
                                       hres);
    }
}

void txservice::LocalCcHandler::KickoutData(const TableName &table_name,
                                            uint32_t ng_id,
                                            TxNumber tx_number,
                                            int64_t tx_term,
                                            uint64_t command_id,
                                            uint64_t commit_ts,
                                            CcHandlerResult<Void> &hres,
                                            CleanType clean_type,
                                            const TxKey *start_key,
                                            const TxKey *end_key)
{
    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(ng_id);
    if (dest_node_id == cc_shards_.node_id_)
    {
        KickoutCcEntryCc *req = kickout_ccentry_pool_.NextRequest();
        req->Reset(table_name,
                   ng_id,
                   commit_ts,
                   Sharder::Instance().GetLocalCcShardsCount(),
                   &hres,
                   clean_type,
                   start_key,
                   end_key);

        TX_TRACE_ACTION(this, req);
        TX_TRACE_DUMP(req);
        // Dispatch the request to all cores and run in parallel
        for (uint16_t idx = 0;
             idx < Sharder::Instance().GetLocalCcShardsCount();
             idx++)
        {
            cc_shards_.EnqueueToCcShard(idx, req);
        }
    }
    else
    {
        remote_hd_.KickoutData(cc_shards_.node_id_,
                               tx_number,
                               tx_term,
                               command_id,
                               table_name,
                               ng_id,
                               commit_ts,
                               clean_type,
                               hres);
    }
}

/**
 * @param is_dirty If true, should use the dirty table schema.
 * @param expected_term Reject this operation if expected_term doesn't match the
 * current leader term of this Node Group if the value is not INIT_TERM(-1).
 */
void txservice::LocalCcHandler::FlushDataAll(const TableName &table_name,
                                             NodeGroupId ng_id,
                                             TxNumber tx_number,
                                             int64_t tx_term,
                                             uint16_t command_id,
                                             uint64_t data_sync_ts,
                                             bool is_dirty,
                                             int64_t &expected_term,
                                             CcHandlerResult<Void> &hres)
{
    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(ng_id);

    if (dest_node_id == cc_shards_.node_id_)
    {
        if (table_name.Type() == TableType::Primary)
        {
            ACTION_FAULT_INJECTOR("term_FlushDataAll_PK_crashed");
        }
        else if (table_name.Type() == TableType::Secondary)
        {
            ACTION_FAULT_INJECTOR("term_FlushDataAll_SK_crashed");
        }

        int64_t ng_term = Sharder::Instance().LeaderTerm(ng_id);
        if (ng_term < 0 || (expected_term > 0 && ng_term != expected_term))
        {
            // Leader transferred.
            hres.SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            LOG(ERROR) << "LocalCcHandler::FlushDataAll: the leader of the "
                          "destinate node group transferred for ng#"
                       << ng_id;
            return;
        }

        cc_shards_.EnqueueDataSyncTask(table_name,
                                       ng_id,
                                       ng_term,
                                       data_sync_ts,
                                       nullptr,
                                       nullptr,
                                       nullptr,
                                       nullptr,
                                       is_dirty,
                                       &hres);
    }
    else
    {
        // For remote node, use RPC service
        std::string node_ip;
        uint16_t node_port;
        Sharder::Instance().GetNodeAddress(dest_node_id, node_ip, node_port);

        brpc::Channel channel;
        if (channel.Init(
                node_ip.c_str(), GET_CCNODE_RPC_PORT(node_port), nullptr) != 0)
        {
            // Fail to establish the channel to the target node.
            LOG(ERROR) << "Flush data all: Fail to init the channel to the"
                          " leader of ng#"
                       << ng_id;
            hres.SetError(CcErrorCode::ESTABLISH_NODE_CHANNEL_FAILED);
            return;
        }

        remote::CcRpcService_Stub stub(&channel);
        remote::FlushDataAllRequest request;
        request.set_tx_number(tx_number);
        request.set_tx_term(tx_term);
        request.set_command_id(command_id);
        request.set_table_name_str(table_name.String());
        request.set_table_type(
            remote::ToRemoteType::ConvertTableType(table_name.Type()));
        request.set_node_group_id(ng_id);
        request.set_node_group_term(expected_term);
        request.set_handler_addr(reinterpret_cast<uint64_t>(&hres));
        request.set_data_sync_ts(data_sync_ts);
        request.set_is_dirty(is_dirty);
        // This will be deleted after the response been handled.
        remote::FlushDataAllResponse *response =
            new remote::FlushDataAllResponse();

        brpc::Controller *cntl = new brpc::Controller();
        cntl->set_timeout_ms(-1);
        // Asynchronous mode
        google::protobuf::Closure *done =
            brpc::NewCallback(&HandleFlushDataAllResponse, cntl, response);
        stub.FlushDataAll(cntl, &request, response, done);
        DLOG(INFO) << "Remote RPC FlushDataAll of ng#" << ng_id << ".";
    }
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
    return cc_shards_.TsBase();
}

void txservice::LocalCcHandler::BlockCcReqCheck(uint64_t tx_number,
                                                int64_t tx_term,
                                                uint16_t command_id,
                                                const CcEntryAddr &cce_addr,
                                                CcHandlerResultBase *hres,
                                                ResultTemplateType type)
{
    uint32_t ng_id = cce_addr.NodeGroupId();
    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(ng_id);

    // If the ccrequest is in same node, it is not need to check. We think one
    // node is stable and and it will always ok or the entire process crash.
    if (dest_node_id != cc_shards_.node_id_)
    {
        remote_hd_.BlockCcReqCheck(cc_shards_.node_id_,
                                   tx_number,
                                   tx_term,
                                   command_id,
                                   cce_addr,
                                   hres,
                                   type);
    }
}

int64_t txservice::LocalCcHandler::NodeGroupLeaderTerm(
    uint32_t ng_id,
    TxNumber tx_number,
    int64_t tx_term,
    uint16_t command_id,
    CcHandlerResult<std::vector<int64_t>> &hres)
{
    int64_t term = INIT_TERM;
    uint32_t leader_node_id = Sharder::Instance().LeaderNodeId(ng_id);
    if (leader_node_id == cc_shards_.node_id_)
    {
        // This node is the leader of the input node group.
        term = Sharder::Instance().LeaderTerm(ng_id);
        assert(term > 0);

        auto &terms = hres.Value();
        terms.at(ng_id) = term;

        hres.SetFinished();
    }
    else
    {
        std::string node_ip;
        uint16_t node_port;
        Sharder::Instance().GetNodeAddress(leader_node_id, node_ip, node_port);

        brpc::Channel channel;
        if (channel.Init(
                node_ip.c_str(), GET_CCNODE_RPC_PORT(node_port), nullptr) != 0)
        {
            // Fail to establish the channel to the tx node. Do not update the
            // leader term of input node group.
            LOG(ERROR) << "Update leader term: Fail to init the channel to the"
                          " leader of ng#"
                       << ng_id;
            hres.SetError(CcErrorCode::NG_TERM_CHANGED);
            return term;
        }

        remote::CcRpcService_Stub stub(&channel);
        remote::AcquireNodeGroupTermRequest request;
        request.set_node_group_id(ng_id);
        request.set_tx_number(tx_number);
        request.set_tx_term(tx_term);
        request.set_command_id(command_id);
        request.set_handler_addr(reinterpret_cast<uint64_t>(&hres));
        // This will be deleted after the response been handled.
        remote::AcquireNodeGroupTermResponse *response_ptr =
            new remote::AcquireNodeGroupTermResponse();

        brpc::Controller *cntl_ptr = new brpc::Controller();
        cntl_ptr->set_timeout_ms(100);
        // Asynchronous mode
        google::protobuf::Closure *done = brpc::NewCallback(
            &HandleAcquireNodeGroupTermResponse, cntl_ptr, response_ptr);
        stub.AcquireNodeGroupLeaderTerm(cntl_ptr, &request, response_ptr, done);

        term = UNKNOWN_TERM;
    }

    return term;
}

/**
 * Handle RPC response
 */
void txservice::LocalCcHandler::HandleAcquireNodeGroupTermResponse(
    brpc::Controller *cntl, remote::AcquireNodeGroupTermResponse *response)
{
    // std::unique_ptr make sure cntl/response will be deleted before
    // returning.
    std::unique_ptr<brpc::Controller> cntl_guard(cntl);
    std::unique_ptr<remote::AcquireNodeGroupTermResponse> response_guard(
        response);

    if (cntl->Failed())
    {
        // RPC failed, fields in response are undefined, cannot use.
        // Special case, cannot set HandlerResult.
        LOG(ERROR)
            << "Failed to process the AcquireNodeGroupTerm RPC. Error code: "
            << cntl->ErrorCode() << ". Error Msg: " << cntl->ErrorText();
        return;
    }

    // Handle response
    CcHandlerResult<std::vector<int64_t>> *hd_res = nullptr;

    uint32_t tx_node_id = (response->tx_number() >> 32L) >> 10L;
    int64_t tx_term = response->tx_term();
    if (!Sharder::Instance().CheckLeaderTerm(tx_node_id, tx_term))
    {
        LOG(WARNING)
            << "Acquire node group term response, but tx node has failed.";
        // The tx node has failed. Pointer stability does not hold anymore.
        return;
    }
    else
    {
        hd_res = reinterpret_cast<CcHandlerResult<std::vector<int64_t>> *>(
            response->handler_addr());

        if (hd_res->Txm()->TxNumber() != response->tx_number() ||
            hd_res->Txm()->CommandId() != response->command_id())
        {
            LOG(WARNING) << "Acquire node group term response, but original tx "
                            "has terminated.";
            // The original tx has terminated and the tx machine has been
            // recycled. The response is directed to an obsolete tx. Skips
            // setting the cc handler result.
            return;
        }
    }

    uint32_t ng_id = response->node_group_id();
    int64_t term = response->node_group_term();

    auto &terms = hd_res->Value();
    terms.at(ng_id) = term;

    hd_res->SetFinished();
    LOG(INFO) << "Handle acquire node group term response for ng#" << ng_id
              << ", and term: " << term;
    // Closure created by NewCallback deletes itself at the end of Run.
}

/**
 * Handle RPC response
 */
void txservice::LocalCcHandler::HandleFlushDataAllResponse(
    brpc::Controller *cntl, remote::FlushDataAllResponse *response)
{
    // std::unique_ptr make sure cntl/response will be deleted before
    // returning.
    std::unique_ptr<brpc::Controller> cntl_guard(cntl);
    std::unique_ptr<remote::FlushDataAllResponse> response_guard(response);

    if (cntl->Failed())
    {
        // RPC failed, fields in response are undefined, cannot use.
        // Special case, cannot set HandlerResult.
        LOG(ERROR) << "Failed to process the FlushDataAll RPC. Error code: "
                   << cntl->ErrorCode() << ". Error Msg: " << cntl->ErrorText();
        return;
    }

    // Handle response
    CcHandlerResult<Void> *hd_res = nullptr;

    uint32_t tx_node_id = (response->tx_number() >> 32L) >> 10L;
    int64_t tx_term = response->tx_term();
    if (!Sharder::Instance().CheckLeaderTerm(tx_node_id, tx_term))
    {
        LOG(WARNING) << "Flush data all response, but tx node has failed.";
        // The tx node has failed. Pointer stability does not hold anymore.
        return;
    }
    else
    {
        hd_res =
            reinterpret_cast<CcHandlerResult<Void> *>(response->handler_addr());

        if (hd_res->Txm()->TxNumber() != response->tx_number() ||
            hd_res->Txm()->CommandId() != response->command_id())
        {
            LOG(WARNING) << "Flush data all response, but original tx has"
                            " terminated.";
            // The original tx has terminated and the tx machine has been
            // recycled. The response is directed to an obsolete tx. Skips
            // setting the cc handler result.
            return;
        }
    }

    if (response->error_code())
    {
        CcErrorCode error_code =
            static_cast<CcErrorCode>(response->error_code());
        LOG(ERROR) << "Handle flush data all response: Failed with error"
                   << " message: " << cc_error_messages.at(error_code);
        hd_res->SetError(error_code);
    }
    else
    {
        DLOG(INFO) << "Handle flush data all response successfully.";
        hd_res->SetFinished();
    }
    // Closure created by NewCallback deletes itself at the end of Run.
}
