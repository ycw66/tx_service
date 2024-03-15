#include "remote/cc_node_service.h"

#include <braft/util.h>  //braft::HostNameAddr2NSUrl

#include "cc/local_cc_shards.h"
#include "remote/remote_type.h"
#include "sharder.h"
#include "tx_request.h"
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
    // Write all cluster scale log to log group 0. Log group will check
    // if there's another cluster scale event in progress and reject
    // the prepare log request if so.
    init_req.log_group_id_ = 0;
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
        ClusterScaleOpType::AddNode, &delta_nodes, nullptr);
    txm->Execute(&scale_req);
    scale_req.Wait();

    if (scale_req.IsError())
    {
        if (scale_req.ErrorCode() == TxErrorCode::LOG_SERVICE_UNREACHABLE)
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
    // Write all cluster scale log to log group 0. Log group will check
    // if there's another cluster scale event in progress and reject
    // the prepare log request if so.
    init_req.log_group_id_ = 0;
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
    ClusterScaleTxRequest scale_req(
        ClusterScaleOpType::RemoveNode, nullptr, &remove_node_count);
    txm->Execute(&scale_req);
    scale_req.Wait();

    if (scale_req.IsError())
    {
        if (scale_req.ErrorCode() == TxErrorCode::LOG_SERVICE_UNREACHABLE)
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

    uint32_t ng_id = request->node_group_id();
    int64_t leader_term = INIT_TERM;

    bthread::Mutex b_thd_mu;
    bthread::ConditionVariable b_thd_cv;
    bool finished = false;
    std::thread worker_thd = std::thread(
        [ng_id, &leader_term, &b_thd_mu, &b_thd_cv, &finished]()
        {
            while ((leader_term = Sharder::Instance().LeaderTerm(ng_id)) < 0 &&
                   Sharder::Instance().CandidateLeaderTerm(ng_id) > 0)
            {
                // The RPC server can receive the remote request, but this
                // node has not finish log replay, so should wait until log
                // replay finished.
                LOG(INFO) << "CcNodeService AcquireLeaderTerm on ng#" << ng_id
                          << " waiting log replay finished.";
                std::this_thread::sleep_for(10s);
            }
            if (leader_term < 0)
            {
                LOG(ERROR) << "!!!ERROR!!! The non-leader node receives "
                              "the request for ng#"
                           << ng_id;
            }

            std::unique_lock b_thd_lk(b_thd_mu);
            finished = true;
            b_thd_cv.notify_one();
        });

    std::unique_lock lk(b_thd_mu);
    while (!finished)
    {
        b_thd_cv.wait(lk);
    }

    response->set_node_group_term(leader_term);
    response->set_node_group_id(ng_id);
    worker_thd.join();
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

    uint32_t ng_id = request->node_group_id();
    int64_t ng_term = request->node_group_term();

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

    bthread::Mutex b_thd_mu;
    bthread::ConditionVariable b_thd_cv;
    bool finished = false;
    CcErrorCode error_code = CcErrorCode::NO_ERROR;
    std::thread worker_thd = std::thread(
        [&table_name,
         ng_id,
         &ng_term,
         data_sync_ts,
         is_dirty,
         &b_thd_mu,
         &b_thd_cv,
         &error_code,
         &finished,
         &local_shards = this->local_shards_]()
        {
            int64_t leader_term = INIT_TERM;
            while (Sharder::Instance().LeaderTerm(ng_id) < 0 &&
                   Sharder::Instance().CandidateLeaderTerm(ng_id) > 0)
            {
                // The RPC server can receive the remote request, but this
                // node has not finish log replay, including data(.pk) log and
                // catalog(.table range info) log, so should wait until log
                // replay finished.
                LOG(INFO) << "CcNodeService FlushDataAll on ng#" << ng_id
                          << " waiting log replay finished.";
                std::this_thread::sleep_for(3s);
            }

            if ((leader_term = Sharder::Instance().LeaderTerm(ng_id)) < 0)
            {
                error_code = CcErrorCode::REQUESTED_NODE_NOT_LEADER;
                std::unique_lock b_thd_lk(b_thd_mu);
                finished = true;
                b_thd_cv.notify_one();
                return;
            }
            ng_term = ng_term < 0 ? leader_term : ng_term;
            DLOG(INFO) << "CcNodeService FlushDataAll RPC on #ng" << ng_id
                       << ", with node group term: " << ng_term
                       << ". And flush table:" << table_name.String();

            std::shared_ptr<DataSyncStatus> status =
                std::make_shared<DataSyncStatus>();

            local_shards.EnqueueDataSyncTaskForTable(table_name,
                                                     ng_id,
                                                     ng_term,
                                                     data_sync_ts,
                                                     false,
                                                     is_dirty,
                                                     status);

            std::unique_lock<std::mutex> lk(status->mux_);
            status->all_task_started_ = true;
            status->cv_.wait(
                lk, [&status] { return status->unfinished_tasks_ == 0; });

            error_code = status->err_code_;

            std::unique_lock b_thd_lk(b_thd_mu);
            finished = true;
            b_thd_cv.notify_one();
        });

    std::unique_lock lk(b_thd_mu);
    while (!finished)
    {
        b_thd_cv.wait(lk);
    }

    response->set_error_code(static_cast<google::protobuf::int32>(error_code));
    worker_thd.join();
    DLOG(INFO) << "CcNodeService FlushDataAll RPC on #ng" << ng_id
               << ", with node group term: " << ng_term
               << " finished with error: " << (int32_t) error_code;
}

void CcNodeService::InitDataMigration(
    ::google::protobuf::RpcController *controller,
    const ::txservice::remote::InitMigrationRequest *request,
    ::txservice::remote::InitMigrationResponse *response,
    ::google::protobuf::Closure *done)
{
    auto thd = std::thread(
        [request, response, done]()
        {
            brpc::ClosureGuard done_guard(done);

            // We don't know if this RPC request is stale or new.
            // We just create a new transaction to do data migration.
            // the write_first_prepare_log request of migration transaction will
            // be rejected if this RPC request is stale or if the
            // ClusterScaleTx is finished transaction.

            TxLog *tx_log = Sharder::Instance().GetLogAgent();
            auto cluster_scale_tx_log_ng_id =
                tx_log->GetLogGroupId(request->tx_number());

            TxService *tx_service =
                Sharder::Instance().GetLocalCcShards()->GetTxservice();
            std::vector<TransactionExecution *> txms;
            std::vector<TxNumber> txns;
            int worker_tx_cnt = request->migrate_infos_size() > 10
                                    ? 10
                                    : request->migrate_infos_size();
            for (int i = 0; i < worker_tx_cnt; i++)
            {
                TransactionExecution *txm = tx_service->NewTx();

                InitTxRequest init_req;
                // Set isolation level to RepeatableRead to ensure the readlock
                // will be set during the execution of the following
                // ReadTxRequest.
                init_req.iso_level_ = IsolationLevel::RepeatableRead;
                init_req.protocol_ = CcProtocol::Locking;
                // Set tx node group id
                init_req.tx_ng_id_ = request->orig_owner();
                // Set log node group id to ensure the log will be write to
                // special location.
                init_req.log_group_id_ = cluster_scale_tx_log_ng_id;

                init_req.Reset();
                txm->Execute(&init_req);
                init_req.Wait();

                if (init_req.IsError())
                {
                    response->set_success(false);
                    for (auto cur_txm : txms)
                    {
                        AbortTxRequest abort_req;
                        cur_txm->Execute(&abort_req);
                        abort_req.Wait();
                    }
                    return;
                }

                txms.push_back(txm);
                txns.push_back(txm->TxNumber());
            }

            // Fill in the migration plan that will be passed to workers
            std::vector<uint16_t> bucket_ids;
            std::vector<NodeGroupId> new_owner_ids;
            size_t migrate_infos_size =
                static_cast<size_t>(request->migrate_infos_size());
            bucket_ids.reserve(migrate_infos_size);
            new_owner_ids.reserve(migrate_infos_size);

            for (size_t idx = 0; idx < migrate_infos_size; ++idx)
            {
                const auto &migrate_info = request->migrate_infos(idx);
                bucket_ids.push_back(migrate_info.bucket_id());
                new_owner_ids.push_back(migrate_info.new_owner());
            }
            std::shared_ptr<DataMigrationStatus> status =
                std::make_shared<DataMigrationStatus>(request->tx_number(),
                                                      std::move(bucket_ids),
                                                      std::move(new_owner_ids),
                                                      std::move(txns));
            // All worker txs has been started, now write the first prepare log,
            // the first tx will write a prepare log that marks the node group
            // migration process has been started. The log contains all of the
            // worker txns, so once this log is written, the migration of this
            // node gorup is always going to succeed.
            DataMigrationTxRequest migrate_req(status);
            txms[0]->Execute(&migrate_req);
            migrate_req.Wait();

            if (migrate_req.IsError())
            {
                if (migrate_req.ErrorCode() ==
                    TxErrorCode::DUPLICATE_MIGRATION_TX_ERROR)
                {
                    // Migration on this node group is already in progress.
                    // We can mark the migration init as success.
                    response->set_success(true);
                }
                else
                {
                    response->set_success(false);
                }
                // Abort rest of the workers.
                for (size_t i = 1; i < txms.size(); i++)
                {
                    AbortTxRequest abort_req;
                    txms[i]->Execute(&abort_req);
                    abort_req.Wait();
                }
            }
            else
            {
                assert(!migrate_req.IsError());

                response->set_success(true);
                for (size_t i = 1; i < txms.size(); i++)
                {
                    // If the log is successfully written, start the rest of the
                    // workers.
                    DataMigrationTxRequest migrate_req(status);
                    txms[i]->Execute(&migrate_req);
                    // Wait for shared ptr is passed into txm before destructing
                    // tx req.
                    migrate_req.Wait();
                }
            }
        });

    thd.detach();
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
    NodeGroupId tx_ng_id = (txn >> 32L) >> 10;
    int64_t term = Sharder::Instance().LeaderTerm(tx_ng_id);
    if (term >= 0)
    {
        std::unique_lock<std::mutex> lk(shards->cluster_scale_op_mux_);
        auto scale_op = shards->cluster_scale_op_.get();
        auto status = scale_op->GetStatus(txn);
        // Once term changes, status will be set to invalid. So make sure
        // the term hasn't changed when we're reading the status.
        if (term == Sharder::Instance().LeaderTerm(tx_ng_id))
        {
            response->set_status(status);
        }
        else
        {
            response->set_status(remote::ClusterScaleStatus::UNKNOWN);
        }
    }
    else
    {
        // Redirect rpc to leader node of tx ng.
        int32_t node_id = Sharder::Instance().LeaderNodeId(tx_ng_id);
        auto channel = Sharder::Instance().GetCcNodeServiceChannel(node_id);
        if (channel == nullptr)
        {
            response->set_status(remote::ClusterScaleStatus::UNKNOWN);
            return;
        }

        remote::CcRpcService_Stub stub(channel.get());
        brpc::Controller cntl;
        stub.CheckClusterScaleStatus(&cntl, request, response, nullptr);
        if (cntl.Failed())
        {
            response->set_status(remote::ClusterScaleStatus::UNKNOWN);
            Sharder::Instance().UpdateCcNodeServiceChannel(node_id, channel);
        }
    }
}

void CcNodeService::GetClusterNodes(
    ::google::protobuf::RpcController *controller,
    const ::txservice::remote::GetClusterNodesRequest *request,
    ::txservice::remote::GetClusterNodesResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);
    // First make sure we're the preferred leader of ng since we'll need to
    // put read lock on cluster config cc map when reading cluster config.
    if (Sharder::Instance().LeaderTerm(Sharder::Instance().NodeId()) < 0)
    {
        response->set_error(true);
        return;
    }

    // Put a read lock on cluster config first before reading the node list.
    TxService *tx_service =
        Sharder::Instance().GetLocalCcShards()->GetTxservice();
    auto txm = tx_service->NewTx();
    InitTxRequest init_req;
    init_req.iso_level_ = IsolationLevel::RepeatableRead;
    init_req.protocol_ = CcProtocol::Locking;
    init_req.Reset();
    txm->Execute(&init_req);
    init_req.Wait();

    if (init_req.IsError())
    {
        response->set_error(true);
        return;
    }
    ReadTxRequest read_req;
    ClusterConfigRecord rec;
    read_req.Set(&cluster_config_ccm_name,
                 NegativeInfinity<VoidKey>::Instance(),
                 &rec,
                 false,
                 false,
                 true);
    txm->Execute(&read_req);
    read_req.Wait();
    RecordStatus rec_status = read_req.Result().first;

    if (read_req.IsError() || rec_status != RecordStatus::Normal)
    {
        CommitTxRequest commit_req;
        txm->CommitTx(commit_req);
        response->set_error(true);
        return;
    }

    // Now read node list from sharder
    std::string ip;
    uint16_t port;
    uint32_t ng_cnt = Sharder::Instance().NodeGroupCount();
    for (uint32_t i = 0; i < ng_cnt; i++)
    {
        Sharder::Instance().GetNodeAddress(i, ip, port);
        if (!ip.empty())
        {
            response->add_host_list(ip);
            response->add_port_list(port);
        }
    }
    CommitTxRequest commit_req;
    txm->Execute(&commit_req);
    commit_req.Wait();
    response->set_error(false);
}
}  // namespace remote
}  // namespace txservice
