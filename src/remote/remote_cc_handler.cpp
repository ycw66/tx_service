#include "remote_cc_handler.h"

#include <iostream>

#include "cc/local_cc_shards.h"
#include "sharder.h"
#include "tx_execution.h"

txservice::remote::RemoteCcHandler::RemoteCcHandler(CcStreamSender &sender)
    : stream_sender_(sender)
{
}

void txservice::remote::RemoteCcHandler::AcquireWrite(
    uint32_t src_id,
    const TableName &table_name,
    const TxKey &key,
    uint32_t key_shard_code,
    const TxId &txid,
    int64_t tx_term,
    uint64_t ts,
    bool is_insert,
    CcHandlerResult<AcquireKeyResult> &hres,
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

    CcMessage send_msg;

    send_msg.set_type(
        CcMessage::MessageType::CcMessage_MessageType_AcquireRequest);
    send_msg.set_tx_number(txid.TxNumber());
    send_msg.set_handler_addr(reinterpret_cast<uint64_t>(&hres));
    send_msg.set_tx_term(tx_term);

    AcquireRequest *acq = send_msg.mutable_acquire_req();
    acq->set_src_node_id(src_id);
    acq->set_tablename(table_name);
    acq->clear_key();
    key.Serialize(*acq->mutable_key());

    acq->set_vec_idx(txid.VecIdx());
    acq->set_ts(ts);
    acq->set_insert(is_insert);
    acq->set_key_shard_code(key_shard_code);
    acq->set_protocol(ConvertProtocol(proto));

    bool success = stream_sender_.SendMessage(key_shard_code >> 10, send_msg);

    send_msg.clear_type();
    send_msg.clear_tx_number();
    send_msg.clear_handler_addr();
    send_msg.clear_acquire_req();

    if (!success)
    {
        hres.SetError(-1);
    }
}

void txservice::remote::RemoteCcHandler::AcquireTableWriteLock(
    uint32_t src_id,
    const TableName &table_name,
    const TxId &txid,
    int64_t tx_term,
    uint64_t tx_number,
    uint32_t node_group_id,
    CcHandlerResult<std::unordered_map<uint32_t, int64_t>> &hres)
{
    CcMessage send_msg;

    send_msg.set_type(CcMessage::MessageType::
                          CcMessage_MessageType_AcquireTableWriteLockRequest);
    send_msg.set_tx_number(txid.TxNumber());
    send_msg.set_tx_term(tx_term);
    send_msg.set_handler_addr(reinterpret_cast<uint64_t>(&hres));

    AcquireTableWriteLockRequest *acq = send_msg.mutable_acquire_table_req();
    acq->set_src_node_id(src_id);
    acq->set_tablename(table_name);

    acq->set_vec_idx(txid.VecIdx());
    acq->set_tx_number(tx_number);
    acq->set_node_group_id(node_group_id);

    bool success = stream_sender_.SendMessage(node_group_id, send_msg);

    send_msg.clear_type();
    send_msg.clear_tx_number();
    send_msg.clear_handler_addr();
    send_msg.clear_acquire_req();

    if (!success)
    {
        hres.SetError(-1);
    }
}

void txservice::remote::RemoteCcHandler::ReleaseTableWriteLock(
    uint32_t src_node_id,
    const TableName &table_name,
    const TxId &txid,
    int64_t tx_term,
    uint64_t tx_number,
    uint32_t node_group_id,
    CcHandlerResult<Void> &hres)
{
    CcMessage send_msg;

    send_msg.set_type(CcMessage::MessageType::
                          CcMessage_MessageType_ReleaseTableWriteLockRequest);
    send_msg.set_tx_number(txid.TxNumber());
    send_msg.set_tx_term(tx_term);
    send_msg.set_handler_addr(reinterpret_cast<uint64_t>(&hres));

    ReleaseTableWriteLockRequest *acq = send_msg.mutable_release_table_req();
    acq->set_src_node_id(src_node_id);
    acq->set_tablename(table_name);

    acq->set_vec_idx(txid.VecIdx());
    acq->set_tx_number(tx_number);
    acq->set_node_group_id(node_group_id);

    bool success = stream_sender_.SendMessage(node_group_id, send_msg);

    send_msg.clear_type();
    send_msg.clear_tx_number();
    send_msg.clear_handler_addr();
    send_msg.clear_acquire_req();

    if (!success)
    {
        hres.SetError(-1);
    }
}

void txservice::remote::RemoteCcHandler::PostWrite(
    uint32_t src_node_id,
    uint64_t tx_number,
    int64_t tx_term,
    uint64_t commit_ts,
    const CcEntryAddr &cce_addr,
    const TxRecord *record,
    bool is_deleted,
    CcHandlerResult<Void> &hres)
{
    CcMessage send_msg;

    send_msg.set_type(
        CcMessage::MessageType::CcMessage_MessageType_PostCommitRequest);
    send_msg.set_tx_number(tx_number);
    send_msg.set_handler_addr(reinterpret_cast<uint64_t>(&hres));
    send_msg.set_tx_term(tx_term);

    PostCommitRequest *post_commit = send_msg.mutable_postcommit_req();
    post_commit->set_src_node_id(src_node_id);
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
    if (commit_ts > 0 && !is_deleted)
    {
        // The commit ts is 0, if the post-write request is used to clear the
        // write lock when the tx aborts.
        assert(record != nullptr);
        record->Serialize(*post_commit->mutable_record());
    }

    post_commit->set_commit_ts(commit_ts);
    post_commit->set_is_deleted(is_deleted);

    bool success = stream_sender_.SendMessage(cce_addr.NodeGroupId(), send_msg);

    send_msg.clear_type();
    send_msg.clear_tx_number();
    send_msg.clear_handler_addr();
    send_msg.clear_postcommit_req();

    if (!success)
    {
        hres.SetError(-1);
    }
}

void txservice::remote::RemoteCcHandler::PostRead(
    uint32_t src_node_id,
    uint64_t tx_number,
    int64_t tx_term,
    uint64_t key_ts,
    uint64_t gap_ts,
    uint64_t commit_ts,
    const CcEntryAddr &cce_addr,
    CcHandlerResult<std::vector<TxId>> &hres,
    CcProtocol protocol)
{
    CcMessage send_msg;

    send_msg.set_type(
        CcMessage::MessageType::CcMessage_MessageType_ValidateRequest);
    send_msg.set_tx_number(tx_number);
    send_msg.set_handler_addr(reinterpret_cast<uint64_t>(&hres));
    send_msg.set_tx_term(tx_term);

    ValidateRequest *vali = send_msg.mutable_validate_req();
    vali->set_src_node_id(src_node_id);
    vali->set_node_group_id(cce_addr.NodeGroupId());
    CceAddr_msg *cce_addr_msg = vali->mutable_cce_addr();
    cce_addr_msg->set_cce_ptr(cce_addr.CcePtr());
    cce_addr_msg->set_term(cce_addr.Term());
    vali->set_commit_ts(commit_ts);
    vali->set_key_ts(key_ts);
    vali->set_gap_ts(gap_ts);
    vali->set_protocol(ConvertProtocol(protocol));

    bool success = stream_sender_.SendMessage(cce_addr.NodeGroupId(), send_msg);

    send_msg.clear_type();
    send_msg.clear_tx_number();
    send_msg.clear_handler_addr();
    send_msg.clear_validate_req();

    if (!success)
    {
        hres.SetError(-1);
    }
}

void txservice::remote::RemoteCcHandler::CommitCreateTable(
    uint32_t src_node_id,
    const TableName &table_name,
    std::string catalog_str,
    int64_t tx_term,
    const TxId &txid,
    uint64_t ts,
    uint32_t node_group_id,
    CcHandlerResult<Void> &hres)
{
    CcMessage send_msg;

    send_msg.set_type(
        CcMessage::MessageType::CcMessage_MessageType_CommitCreateTableRequest);
    send_msg.set_tx_number(txid.TxNumber());
    send_msg.set_handler_addr(reinterpret_cast<uint64_t>(&hres));
    send_msg.set_tx_term(tx_term);

    CommitCreateTableRequest *acq = send_msg.mutable_commit_create_table_req();
    acq->set_src_node_id(src_node_id);
    acq->set_tablename(table_name);

    acq->set_catalog_str(catalog_str);
    acq->set_ts(ts);
    acq->set_node_group_id(node_group_id);

    bool success = stream_sender_.SendMessage(node_group_id, send_msg);

    send_msg.clear_type();
    send_msg.clear_tx_number();
    send_msg.clear_handler_addr();
    send_msg.clear_acquire_req();

    if (!success)
    {
        hres.SetError(-1);
    }
}

void txservice::remote::RemoteCcHandler::CommitDropTable(
    uint32_t src_node_id,
    const TableName &table_name,
    int64_t tx_term,
    const TxId &txid,
    uint64_t ts,
    uint32_t node_group_id,
    CcHandlerResult<Void> &hres)
{
    CcMessage send_msg;

    send_msg.set_type(
        CcMessage::MessageType::CcMessage_MessageType_CommitDropTableRequest);
    send_msg.set_tx_number(txid.TxNumber());
    send_msg.set_handler_addr(reinterpret_cast<uint64_t>(&hres));
    send_msg.set_tx_term(tx_term);

    CommitDropTableRequest *acq = send_msg.mutable_commit_drop_table_req();
    acq->set_src_node_id(src_node_id);
    acq->set_tablename(table_name);

    acq->set_ts(ts);
    acq->set_node_group_id(node_group_id);

    bool success = stream_sender_.SendMessage(node_group_id, send_msg);

    send_msg.clear_type();
    send_msg.clear_tx_number();
    send_msg.clear_handler_addr();
    send_msg.clear_acquire_req();

    if (!success)
    {
        hres.SetError(-1);
    }
}

void txservice::remote::RemoteCcHandler::Read(
    uint32_t src_node_id,
    const TableName &table_name,
    const TxKey &key,
    uint32_t key_shard_code,
    const TxRecord &record,
    ReadType read_type,
    uint64_t tx_number,
    int64_t tx_term,
    const uint64_t ts,
    CcHandlerResult<ReadKeyResult> &hres,
    IsolationLevel iso_level,
    CcProtocol proto)
{
    CcMessage send_msg;

    send_msg.set_type(
        CcMessage::MessageType::CcMessage_MessageType_ReadRequest);
    send_msg.set_tx_number(tx_number);
    send_msg.set_handler_addr(reinterpret_cast<uint64_t>(&hres));
    send_msg.set_tx_term(tx_term);

    ReadRequest *read = send_msg.mutable_read_req();
    read->set_src_node_id(src_node_id);
    read->set_tablename(table_name);
    read->clear_key();
    key.Serialize(*read->mutable_key());
    read->set_key_shard_code(key_shard_code);
    read->set_iso_level(ConvertIsolation(iso_level));
    read->set_protocol(ConvertProtocol(proto));

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

    bool success = stream_sender_.SendMessage(key_shard_code >> 10, send_msg);

    send_msg.clear_type();
    send_msg.clear_tx_number();
    send_msg.clear_handler_addr();
    send_msg.clear_read_req();

    if (!success)
    {
        hres.SetError(-1);
    }
}

/*
 * ReadOutside fills the tuple read from KV into cache.
 */
void txservice::remote::RemoteCcHandler::ReadOutside(
    int64_t tx_term,
    const TxRecord &record,
    bool is_deleted,
    const CcEntryAddr &cce_addr)
{
    CcMessage send_msg;

    send_msg.set_type(
        CcMessage::MessageType::CcMessage_MessageType_ReadOutsideRequest);
    send_msg.set_tx_number(0);
    send_msg.set_tx_term(tx_term);

    ReadOutsideRequest *read_outside = send_msg.mutable_read_outside_req();
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
    stream_sender_.SendMessage(cce_addr.NodeGroupId(), send_msg);

    send_msg.clear_type();
    send_msg.clear_read_outside_req();
}

void txservice::remote::RemoteCcHandler::ScanOpen(
    uint32_t src_node_id,
    const TableName &table_name,
    ScanIndexType index_type,
    uint32_t node_group_id,
    const TxKey &start_key,
    bool inclusive,
    uint64_t tx_number,
    int64_t tx_term,
    uint64_t ts,
    CcHandlerResult<ScanOpenResult> &hd_res,
    ScanDirection direction,
    IsolationLevel iso_level,
    CcProtocol proto,
    bool is_ckpt)
{
    CcMessage send_msg;

    send_msg.set_type(
        CcMessage::MessageType::CcMessage_MessageType_ScanOpenRequest);
    send_msg.set_tx_number(tx_number);
    send_msg.set_handler_addr(reinterpret_cast<uint64_t>(&hd_res));
    send_msg.set_tx_term(tx_term);

    ScanOpenRequest *scan_open = send_msg.mutable_scan_open_req();
    scan_open->set_src_node_id(src_node_id);
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
    scan_open->set_iso_level(ConvertIsolation(iso_level));
    scan_open->set_protocol(ConvertProtocol(proto));
    scan_open->set_ckpt(is_ckpt);

    bool success = stream_sender_.SendMessage(node_group_id, send_msg);

    send_msg.clear_type();
    send_msg.clear_tx_number();
    send_msg.clear_handler_addr();
    send_msg.clear_scan_open_req();

    if (!success)
    {
        hd_res.SetError(-1);
    }
}

void txservice::remote::RemoteCcHandler::ScanNext(
    uint32_t src_node_id,
    uint32_t ng_id,
    uint64_t tx_number,
    int64_t tx_term,
    uint64_t start_ts,
    ScanCache *scan_cache,
    CcHandlerResult<ScanNextResult> &hd_res,
    IsolationLevel iso_level,
    CcProtocol proto,
    bool is_ckpt)
{
    CcMessage send_msg;

    send_msg.set_type(
        CcMessage::MessageType::CcMessage_MessageType_ScanNextRequest);
    send_msg.set_tx_number(tx_number);
    send_msg.set_handler_addr(reinterpret_cast<uint64_t>(&hd_res));
    send_msg.set_tx_term(tx_term);

    ScanNextRequest *scan_next = send_msg.mutable_scan_next_req();

    scan_next->set_src_node_id(src_node_id);
    scan_next->set_node_group_id(ng_id);
    const CcEntryAddr &last_cce_addr = scan_cache->LastTuple()->cce_addr_;
    scan_next->set_prior_cce_ptr(last_cce_addr.CcePtr());
    scan_next->set_direction(scan_cache->Scanner()->Direction() ==
                             ScanDirection::Forward);
    scan_next->set_ts(start_ts);
    scan_next->set_scan_cache_ptr(reinterpret_cast<uint64_t>(scan_cache));
    scan_next->set_iso_level(ConvertIsolation(iso_level));
    scan_next->set_protocol(ConvertProtocol(proto));
    scan_next->set_ckpt(is_ckpt);

    bool success = stream_sender_.SendMessage(ng_id, send_msg);

    send_msg.clear_type();
    send_msg.clear_tx_number();
    send_msg.clear_handler_addr();
    send_msg.clear_scan_next_req();

    if (!success)
    {
        hd_res.SetError(-1);
    }
}

void txservice::remote::RemoteCcHandler::CommitSecondaryKey(
    uint32_t src_node_id,
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

    CcMessage send_msg;

    send_msg.set_type(
        CcMessage::MessageType::CcMessage_MessageType_CommitSkRequest);
    send_msg.set_handler_addr(reinterpret_cast<uint64_t>(&hd_res));

    CommitSkRequest *commit_sk = send_msg.mutable_commit_sk_req();
    commit_sk->set_src_node_id(src_node_id);
    commit_sk->set_tablename(table_name);

    sk.Serialize(*commit_sk->mutable_sk());
    pk.Serialize(*commit_sk->mutable_pk());

    commit_sk->set_key_shard_code(key_shard_code);
    commit_sk->set_ts(ts);
    commit_sk->set_is_deleted(is_delete);

    bool success = stream_sender_.SendMessage(key_shard_code >> 10, send_msg);

    send_msg.clear_type();
    send_msg.clear_handler_addr();
    send_msg.clear_commit_sk_req();

    if (!success)
    {
        hd_res.SetError(-1);
    }
}

void txservice::remote::RemoteCcHandler::FaultInject(
    uint32_t src_node_id,
    const std::string &fault_name,
    const std::string &fault_type,
    int64_t tx_term,
    const TxId &txid,
    int node_id,
    CcHandlerResult<bool> &hres)
{
    CcMessage send_msg;

    send_msg.set_type(
        CcMessage::MessageType::CcMessage_MessageType_FaultInjectRequest);
    send_msg.set_handler_addr(reinterpret_cast<uint64_t>(&hres));
    send_msg.set_tx_term(tx_term);
    send_msg.set_tx_number(txid.TxNumber());

    FaultInjectRequest *fi_req = send_msg.mutable_fault_inject_req();
    fi_req->set_src_node_id(src_node_id);
    fi_req->set_fault_name(fault_name);
    fi_req->set_fault_type(fault_type);

    bool success = stream_sender_.SendMessage(node_id, send_msg);

    send_msg.clear_type();
    send_msg.clear_handler_addr();
    send_msg.clear_acquire_req();

    if (!success)
    {
        hres.SetError(-1);
    }
}

txservice::remote::IsolationType
txservice::remote::RemoteCcHandler::ConvertIsolation(IsolationLevel iso_level)
{
    switch (iso_level)
    {
    case IsolationLevel::ReadCommitted:
        return IsolationType::ReadCommitted;
    case IsolationLevel::Snapshot:
        return IsolationType::SnapshotIsolation;
    case IsolationLevel::RepeatableRead:
        return IsolationType::RepeatableRead;
    case IsolationLevel::Serializable:
        return IsolationType::Serializable;
    default:
        return IsolationType::ReadCommitted;
    }
}

txservice::remote::CcProtocolType
txservice::remote::RemoteCcHandler::ConvertProtocol(CcProtocol proto)
{
    if (proto == CcProtocol::Locking)
    {
        return CcProtocolType::Locking;
    }
    else
    {
        return CcProtocolType::Occ;
    }
}
