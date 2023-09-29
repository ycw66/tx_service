#include "remote/cc_node_service.h"

#include "cc/local_cc_shards.h"
#include "remote/remote_type.h"
#include "sharder.h"
#include "tx_service.h"

namespace txservice
{
namespace remote
{
CcNodeService::CcNodeService(LocalCcShards &local_shards)
    : local_shards_(local_shards)
{
}

void CcNodeService::Transfer(::google::protobuf::RpcController *controller,
                             const TransferRequest *request,
                             TransferResponse *response,
                             ::google::protobuf::Closure *done)
{
    // This object helps you to call done->Run() in RAII style. If you need
    // to process the request asynchronously, pass done_guard.release().
    brpc::ClosureGuard done_guard(done);

    uint32_t ng_id = request->ng_id();
    int err = Sharder::Instance().TransferLeader(ng_id);

    response->set_error(err != 0);
}

void CcNodeService::CheckTxStatus(::google::protobuf::RpcController *controller,
                                  const CheckTxStatusRequest *request,
                                  CheckTxStatusResponse *response,
                                  ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);

    TxNumber tx_number = request->tx_number();
    // The higher 4 bytes represent the global core ID, which is a
    // combination of node ID and local core ID.
    uint32_t global_core_id = tx_number >> 32L;
    uint32_t tx_ng = global_core_id >> 10;

    if (!Sharder::Instance().CheckLeaderTerm(tx_ng, request->tx_term()))
    {
        // The target tx's term does not match that of the leader. It means that
        // the leader of the cc node group must have failed over and not contain
        // the tx.
        response->set_tx_status(CheckTxStatusResponse_TxStatus::
                                    CheckTxStatusResponse_TxStatus_NOT_FOUND);
        return;
    }

    CheckTxStatusCc check_tx_cc(tx_number);
    local_shards_.EnqueueCcRequest(global_core_id, &check_tx_cc);
    check_tx_cc.Wait();

    if (check_tx_cc.Exists())
    {
        switch (check_tx_cc.TxStatus())
        {
        case TxnStatus::Committed:
            response->set_tx_status(
                CheckTxStatusResponse_TxStatus::
                    CheckTxStatusResponse_TxStatus_COMMITTED);
            break;
        case TxnStatus::Aborted:
            response->set_tx_status(CheckTxStatusResponse_TxStatus::
                                        CheckTxStatusResponse_TxStatus_ABORTED);
            break;
        case TxnStatus::Unknown:
            response->set_tx_status(
                CheckTxStatusResponse_TxStatus::
                    CheckTxStatusResponse_TxStatus_RESULT_UNKNOWN);
            break;
        default:
            response->set_tx_status(CheckTxStatusResponse_TxStatus::
                                        CheckTxStatusResponse_TxStatus_ONGOING);
            break;
        }
    }
    else
    {
        response->set_tx_status(CheckTxStatusResponse_TxStatus::
                                    CheckTxStatusResponse_TxStatus_NOT_FOUND);
    }
}

/**
 * @brief RPC NotifyNewLeaderStart update the leader cache on the Sharder
 * without referring to the braft service.
 */
void CcNodeService::NotifyNewLeaderStart(
    ::google::protobuf::RpcController *controller,
    const NotifyNewLeaderStartRequest *request,
    NotifyNewLeaderStartResponse *response,
    ::google::protobuf::Closure *done)
{
    // This object helps you to call done->Run() in RAII style. If you need
    // to process the request asynchronously, pass done_guard.release().
    brpc::ClosureGuard done_guard(done);

    uint32_t ng_id = request->ng_id();
    uint32_t node_id = request->node_id();

    // update the leader cache directly.
    Sharder::Instance().UpdateLeader(ng_id, node_id);

    response->set_error(false);
}

/**
 * @brief RPC service: get min start_ts of all active transactions on the node.
 */
void CcNodeService::GetMinTxStartTs(
    ::google::protobuf::RpcController *controller,
    const ::txservice::remote::GetMinTxStartTsRequest *request,
    ::txservice::remote::GetMinTxStartTsResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);

    uint32_t ng_id = request->ng_id();
    uint64_t min_ts = UINT64_MAX;
    if (Sharder::Instance().LeaderTerm(local_shards_.NodeId()) > 0)
    {
        auto term = Sharder::Instance().LeaderTerm(ng_id);
        min_ts = local_shards_.StatsLocalActiveSiTxs();

        response->set_term(term);
        response->set_ts(min_ts);
        response->set_error(false);
    }
    else
    {
        response->set_error(true);
    }
}

void CcNodeService::ClusterAddNode(
    ::google::protobuf::RpcController *controller,
    const ::txservice::remote::ClusterAddNodeRequest *request,
    ::txservice::remote::ClusterAddNodeResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);
    using namespace txservice;

    if (Sharder::Instance().LeaderTerm(local_shards_.NodeId()) <= 0)
    {
        // Node is not preferred leader of node group.
        response->set_result(
            ::txservice::remote::ClusterScaleWriteLogResult::FAIL);
        return;
    }

    std::vector<std::pair<std::string, uint16_t>> delta_nodes;
    for (int i = 0; i < request->host_list_size(); i++)
    {
        delta_nodes.emplace_back(request->host_list(i), request->port_list(i));
    }
    // Start cluster scale tx and wait for the log is written before
    // returning.
    TxService *tx_service =
        Sharder::Instance().GetLocalCcShards()->GetTxservice();
    TransactionExecution *txm = tx_service->NewTx();
    InitTxRequest init_req;
    // Set isolation level to RepeatableRead to ensure the readlock
    // will be set during the execution of the following
    // ReadTxRequest.
    init_req.iso_level_ = IsolationLevel::RepeatableRead;
    init_req.protocol_ = CcProtocol::Locking;
    init_req.Reset();
    txm->Execute(&init_req);
    init_req.Wait();

    if (init_req.IsError())
    {
        LOG(ERROR) << "Failed to init tx for cluster scale event.";
        response->set_result(
            ::txservice::remote::ClusterScaleWriteLogResult::FAIL);
        return;
    }

    ClusterScaleTxRequest scale_req(
        ClusterScaleOpType::AddNode, &delta_nodes, nullptr, nullptr);
    txm->Execute(&scale_req);
    scale_req.WaitForWriteLog();

    if (scale_req.GetErr() != CcErrorCode::NO_ERROR)
    {
        if (scale_req.GetErr() == CcErrorCode::LOG_CLOSURE_RESULT_UNKNOWN_ERR)
        {
            // write log result unkown, need to query new leader later.

            response->set_result(
                ::txservice::remote::ClusterScaleWriteLogResult::UNKOWN);
        }
        else
        {
            LOG(ERROR) << "Failed to start cluster scale event, txn: "
                       << txm->TxNumber();
            response->set_result(
                ::txservice::remote::ClusterScaleWriteLogResult::FAIL);
        }
        return;
    }
    response->set_result(
        ::txservice::remote::ClusterScaleWriteLogResult::SUCCESS);
    response->set_tx_number(txm->TxNumber());
}

void CcNodeService::ClusterRemoveNode(
    ::google::protobuf::RpcController *controller,
    const ::txservice::remote::ClusterRemoveNodeRequest *request,
    ::txservice::remote::ClusterRemoveNodeResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);
    using namespace txservice;

    if (Sharder::Instance().LeaderTerm(local_shards_.NodeId()) <= 0)
    {
        // Node is not preferred leader of node group.
        response->set_result(
            ::txservice::remote::ClusterScaleWriteLogResult::FAIL);
        return;
    }

    std::vector<std::pair<std::string, uint16_t>> delta_nodes;
    // Start cluster scale tx and wait for the log is written before
    // returning.
    TxService *tx_service =
        Sharder::Instance().GetLocalCcShards()->GetTxservice();
    TransactionExecution *txm = tx_service->NewTx();
    InitTxRequest init_req;
    // Set isolation level to RepeatableRead to ensure the readlock
    // will be set during the execution of the following
    // ReadTxRequest.
    init_req.iso_level_ = IsolationLevel::RepeatableRead;
    init_req.protocol_ = CcProtocol::Locking;
    init_req.Reset();
    txm->Execute(&init_req);
    init_req.Wait();

    if (init_req.IsError())
    {
        LOG(ERROR) << "Failed to init tx for cluster scale event.";
        response->set_result(
            ::txservice::remote::ClusterScaleWriteLogResult::FAIL);
        return;
    }

    uint16_t remove_node_count = request->remove_node_count();
    std::vector<std::pair<std::string, uint16_t>> removed_nodes;
    ClusterScaleTxRequest scale_req(ClusterScaleOpType::RemoveNode,
                                    nullptr,
                                    &removed_nodes,
                                    &remove_node_count);
    txm->Execute(&scale_req);
    scale_req.WaitForWriteLog();

    if (scale_req.GetErr() != CcErrorCode::NO_ERROR)
    {
        if (scale_req.GetErr() == CcErrorCode::LOG_CLOSURE_RESULT_UNKNOWN_ERR)
        {
            // write log result unkown, need to query new leader later.

            response->set_result(
                ::txservice::remote::ClusterScaleWriteLogResult::UNKOWN);
        }
        else
        {
            LOG(ERROR) << "Failed to start cluster scale event, txn: "
                       << txm->TxNumber();
            response->set_result(
                ::txservice::remote::ClusterScaleWriteLogResult::FAIL);
        }
        return;
    }
    response->set_result(
        ::txservice::remote::ClusterScaleWriteLogResult::SUCCESS);
    response->set_tx_number(txm->TxNumber());
    for (auto &node : removed_nodes)
    {
        response->add_host_list(node.first);
        response->add_port_list(node.second);
    }
}

/**
 * @brief RPC service: get the leader term of the specific node group.
 */
void CcNodeService::AcquireNodeGroupLeaderTerm(
    ::google::protobuf::RpcController *controller,
    const AcquireNodeGroupTermRequest *request,
    AcquireNodeGroupTermResponse *response,
    ::google::protobuf::Closure *done)
{
    // This object helps you to call done->Run() in RAII style. If you need
    // to process the request asynchronously, pass done_guard.release().
    brpc::ClosureGuard done_guard(done);

    // Set response
    response->set_tx_number(request->tx_number());
    response->set_tx_term(request->tx_term());
    response->set_command_id(request->command_id());
    response->set_handler_addr(request->handler_addr());

    uint32_t ng_id = request->node_group_id();
    int64_t term = Sharder::Instance().LeaderTerm(ng_id);

    if (term < 0)
    {
        LOG(WARNING)
            << "!!!WARNING!!! The non-leader node receives the request for ng#"
            << ng_id;
        response->set_node_group_term(INIT_TERM);
    }
    else
    {
        response->set_node_group_term(term);
    }
    response->set_node_group_id(ng_id);
}

/**
 * @brief RPC service: flush all tuples whose commit timestamp less than the
 *  @@request.ckpt_ts into data store.
 */
void CcNodeService::FlushDataAll(::google::protobuf::RpcController *controller,
                                 const FlushDataAllRequest *request,
                                 FlushDataAllResponse *response,
                                 ::google::protobuf::Closure *done)
{
    // This object helps to call done->Run() in RAII style. If you need to
    // process the request asynchronously, pass done_guard.release().
    brpc::ClosureGuard done_guard(done);

    // Set response
    response->set_tx_number(request->tx_number());
    response->set_tx_term(request->tx_term());
    response->set_command_id(request->command_id());
    response->set_handler_addr(request->handler_addr());

    uint32_t ng_id = request->node_group_id();
    int64_t expected_ng_term = request->node_group_term();
    // Check the term.
    int64_t current_ng_term = Sharder::Instance().LeaderTerm(ng_id);
    if (current_ng_term < 0 ||
        (expected_ng_term > 0 && expected_ng_term != current_ng_term))
    {
        // The destinate node group's term has changed, ignore this request.
        response->set_error_code(static_cast<google::protobuf::int32>(
            CcErrorCode::REQUESTED_NODE_NOT_LEADER));
        LOG(ERROR) << "CcNodeService FlushDataAll RPC: node not leader on ng#"
                   << ng_id;
        return;
    }

    std::string_view table_name_sv{request->table_name_str()};
    TableType table_type =
        ToLocalType::ConvertCcTableType(request->table_type());
    TableName table_name = TableName(table_name_sv, table_type);

    uint64_t data_sync_ts = request->data_sync_ts();
    bool is_dirty = request->is_dirty();

    if (table_type == TableType::Primary)
    {
        ACTION_FAULT_INJECTOR("term_FlushDataAllRPC_PK_crashed");
    }
    else if (table_type == TableType::Secondary)
    {
        ACTION_FAULT_INJECTOR("term_FlushDataAllRPC_SK_crashed");
    }
    DLOG(INFO) << "CcNodeService FlushDataAll RPC on #ng" << ng_id
               << ", and flush table:" << table_name.String();

    std::shared_ptr<DataSyncStatus> status = std::make_shared<DataSyncStatus>();

    local_shards_.EnqueueDataSyncTask(table_name,
                                      ng_id,
                                      current_ng_term,
                                      data_sync_ts,
                                      false,
                                      is_dirty,
                                      status);
    std::unique_lock<std::mutex> lk(status->mux_);
    status->all_task_started_ = true;
    status->cv_.wait(lk, [&status] { return status->unfinished_tasks_ == 0; });

    CcErrorCode error_code = status->err_code_;

    DLOG(INFO) << "CcNodeService FlushDataAll RPC on #ng" << ng_id
               << " finished with error: " << (int32_t) error_code;
    response->set_error_code(static_cast<google::protobuf::int32>(error_code));
}

void CcNodeService::NotifyNewNodeReady(
    ::google::protobuf::RpcController *controller,
    const ::txservice::remote::NotifyNewNodeReadyRequest *request,
    ::txservice::remote::NotifyNewNodeReadyResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);
    auto shards = Sharder::Instance().GetLocalCcShards();
    TxNumber txn = request->tx_number();
    std::unique_lock<std::mutex> lk(shards->cluster_scale_op_mux_);
    auto scale_op = shards->cluster_scale_op_.get();
    if (scale_op->GetStatus(txn) ==
        remote::ClusterScaleStatus::CLUSTER_CONFIG_UPDATE)
    {
        assert(scale_op->op_ == &scale_op->wait_for_new_node_ready_op_);
        scale_op->wait_for_new_node_ready_op_.hd_result_.SetFinished();
        // TODO{liunyl}: Wait until we write log to confirm new nodes are ready.
        response->set_error(false);
    }
    else
    {
        // Invalid state for notify new node ready
        response->set_error(true);
    }
}

void CcNodeService::CheckClusterScaleStatus(
    ::google::protobuf::RpcController *controller,
    const ::txservice::remote::ClusterScaleStatusRequest *request,
    ::txservice::remote::ClusterScaleStatusResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_gaurd(done);
    auto shards = Sharder::Instance().GetLocalCcShards();
    TxNumber txn = request->tx_number();
    std::unique_lock<std::mutex> lk(shards->cluster_scale_op_mux_);
    auto scale_op = shards->cluster_scale_op_.get();
    // TODO{liunyl}: redirect request to tx coordinator ng if current node
    // is not tx owner.
    response->set_status(scale_op->GetStatus(txn));
}
}  // namespace remote
}  // namespace txservice