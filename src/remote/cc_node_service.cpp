#include "remote/cc_node_service.h"

#include "cc/local_cc_shards.h"
#include "error_messages.h"
#include "remote/remote_type.h"
#include "sharder.h"
#include "sk_generator.h"
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

void CcNodeService::OnLeaderStart(::google::protobuf::RpcController *controller,
                                  const OnLeaderStartRequest *request,
                                  OnLeaderChangeResponse *response,
                                  ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);
    NodeGroupId ng_id = request->node_group_id();
    int64_t term = request->node_group_term();
    Sharder::Instance().OnLeaderStart(ng_id, term);
    response->set_error(false);
}

void CcNodeService::OnLeaderStop(::google::protobuf::RpcController *controller,
                                 const OnLeaderStopRequest *request,
                                 OnLeaderChangeResponse *response,
                                 ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);
    NodeGroupId ng_id = request->node_group_id();
    Sharder::Instance().OnLeaderStop(ng_id);
    response->set_error(false);
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
    LOG(INFO) << "received add node request";

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
        request->id(), ClusterScaleOpType::AddNode, &delta_nodes, nullptr);
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
        else if (scale_req.ErrorCode() ==
                 TxErrorCode::DUPLICATE_CLUSTER_SCALE_TX_ERROR)
        {
            response->set_result(
                ::txservice::remote::ClusterScaleWriteLogResult::SUCCESS);
            response->set_tx_number(scale_req.Result());
        }
        else
        {
            response->set_result(
                ::txservice::remote::ClusterScaleWriteLogResult::FAIL);
        }

        return;
    }

    response->set_result(
        ::txservice::remote::ClusterScaleWriteLogResult::SUCCESS);
    response->set_tx_number(scale_req.Result());
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
    LOG(INFO) << "received remove node request";

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
    ClusterScaleTxRequest scale_req(request->id(),
                                    ClusterScaleOpType::RemoveNode,
                                    nullptr,
                                    &remove_node_count);
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
        else if (scale_req.ErrorCode() ==
                 TxErrorCode::DUPLICATE_CLUSTER_SCALE_TX_ERROR)
        {
            response->set_result(
                ::txservice::remote::ClusterScaleWriteLogResult::SUCCESS);
            response->set_tx_number(scale_req.Result());
        }
        else
        {
            response->set_result(
                ::txservice::remote::ClusterScaleWriteLogResult::FAIL);
        }
        return;
    }

    response->set_result(
        ::txservice::remote::ClusterScaleWriteLogResult::SUCCESS);
    response->set_tx_number(scale_req.Result());
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
    if (table_type == TableType::Secondary)
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
                       << ". And flush table:" << table_name.Trace();

            uint64_t table_last_synced_ts = 0;
            std::shared_ptr<DataSyncStatus> status =
                std::make_shared<DataSyncStatus>(false);

            local_shards.EnqueueDataSyncTaskForTable(table_name,
                                                     ng_id,
                                                     ng_term,
                                                     data_sync_ts,
                                                     table_last_synced_ts,
                                                     is_dirty,
                                                     false,
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
               << " finished with error: " << CcErrorMessage(error_code);
}

void CcNodeService::InitDataMigration(
    ::google::protobuf::RpcController *controller,
    const ::txservice::remote::InitMigrationRequest *request,
    ::txservice::remote::InitMigrationResponse *response,
    ::google::protobuf::Closure *done)
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

    // Fill in the migration plan that will be passed to workers
    std::vector<uint16_t> bucket_ids;
    std::vector<NodeGroupId> new_owner_ngs;
    size_t migrate_infos_size =
        static_cast<size_t>(request->migrate_infos_size());
    bucket_ids.reserve(migrate_infos_size);
    new_owner_ngs.reserve(migrate_infos_size);

    for (size_t idx = 0; idx < migrate_infos_size; ++idx)
    {
        const auto &migrate_info = request->migrate_infos(idx);
        bucket_ids.push_back(migrate_info.bucket_id());
        new_owner_ngs.push_back(migrate_info.new_owner());
    }
#ifdef RANGE_PARTITION_ENABLED
    // Each worker processes 10 buckets at a time.
    int worker_tx_cnt =
        bucket_ids.size() > 100 ? 10 : (bucket_ids.size() / 10) + 1;
    // Each worker processes 10 buckets at a time.
    std::vector<std::vector<uint16_t>> bucket_ids_per_task(1);
    std::vector<std::vector<NodeGroupId>> new_owner_ngs_per_task(1);
    std::vector<uint16_t> *cur_task_bucekt_ids = &bucket_ids_per_task.back();
    std::vector<NodeGroupId> *cur_task_new_owner_ngs =
        &new_owner_ngs_per_task.back();
    for (size_t i = 0; i < bucket_ids.size(); i++)
    {
        cur_task_bucekt_ids->push_back(bucket_ids[i]);
        cur_task_new_owner_ngs->push_back(new_owner_ngs[i]);
        if (cur_task_bucekt_ids->size() == 10 && i != bucket_ids.size() - 1)
        {
            bucket_ids_per_task.push_back(std::vector<uint16_t>());
            new_owner_ngs_per_task.push_back(std::vector<NodeGroupId>());
            cur_task_bucekt_ids = &bucket_ids_per_task.back();
            cur_task_new_owner_ngs = &new_owner_ngs_per_task.back();
        }
    }

#else
    // Each worker processes all buckets that is on a specific core.
    int worker_tx_cnt = Sharder::Instance().GetLocalCcShards()->Count();
    // Put all buckets on the same core to the same task.
    std::vector<std::vector<uint16_t>> bucket_ids_per_task(worker_tx_cnt);
    std::vector<std::vector<NodeGroupId>> new_owner_ngs_per_task(worker_tx_cnt);
    for (size_t i = 0; i < bucket_ids.size(); i++)
    {
        uint16_t core_id =
            Sharder::Instance().ShardBucketIdToCoreIdx(bucket_ids[i]);
        bucket_ids_per_task[core_id].push_back(bucket_ids[i]);
        new_owner_ngs_per_task[core_id].push_back(new_owner_ngs[i]);
    }

    // Remove empty tasks.
    for (auto task_it = bucket_ids_per_task.begin();
         task_it != bucket_ids_per_task.end();)
    {
        if (task_it->empty())
        {
            worker_tx_cnt--;
            task_it = bucket_ids_per_task.erase(task_it);
        }
        else
        {
            task_it++;
        }
    }
#endif
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

    std::shared_ptr<DataMigrationStatus> status =
        std::make_shared<DataMigrationStatus>(request->tx_number(),
                                              std::move(bucket_ids_per_task),
                                              std::move(new_owner_ngs_per_task),
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
}

void CcNodeService::CheckClusterScaleStatus(
    ::google::protobuf::RpcController *controller,
    const ::txservice::remote::ClusterScaleStatusRequest *request,
    ::txservice::remote::ClusterScaleStatusResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);
    auto *txlog = Sharder::Instance().GetLogAgent();
    auto status = txlog->CheckClusterScaleStatus(0, request->id());

    switch (status)
    {
    case ::txlog::CheckClusterScaleStatusResponse::Status::
        CheckClusterScaleStatusResponse_Status_UNKNOWN:
    {
        response->set_status(ClusterScaleStatus::UNKNOWN);
        break;
    }
    case ::txlog::CheckClusterScaleStatusResponse::Status::
        CheckClusterScaleStatusResponse_Status_NO_STARTED:
    {
        response->set_status(ClusterScaleStatus::NOT_STARTED);
        break;
    }
    case ::txlog::CheckClusterScaleStatusResponse::Status::
        CheckClusterScaleStatusResponse_Status_STARTED:
    {
        response->set_status(ClusterScaleStatus::IN_PROGRESS);
        break;
    }
    case ::txlog::CheckClusterScaleStatusResponse::Status::
        CheckClusterScaleStatusResponse_Status_FINISHED:
    {
        response->set_status(ClusterScaleStatus::FINISHED);
        break;
    }
    default:
        assert(false && "Dead branch");
    }
}

void CcNodeService::CheckClusterConfigIsUpdated(
    ::google::protobuf::RpcController *controller,
    const ::txservice::remote::CheckClusterConfigIsUpdatedRequest *request,
    ::txservice::remote::CheckClusterConfigIsUpdatedResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);
    auto *store_hd = Sharder::Instance().GetLocalCcShards()->store_hd_;
    std::unordered_map<uint32_t, std::vector<NodeConfig>> ng_configs;
    uint64_t version;
    int32_t seed;
    bool uninitialized = false;
    if (!store_hd->ReadClusterConfig(ng_configs, version, seed, uninitialized))
    {
        assert(uninitialized == false);
        // cannot read cluster config. just set `finished` to false. cp will
        // retry.
        response->set_finished(false);

        LOG(ERROR)
            << "RPC#CheckClusterConfigIsUpdated cannot read cluster "
               "config. Please check whether the storage engine is working";
        return;
    }

    bool updated = ng_configs.size() == request->cluster_size();
    response->set_finished(updated);
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
                 VoidKey::NegInfTxKey(),
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

void CcNodeService::GenerateSkFromPk(
    ::google::protobuf::RpcController *controller,
    const ::txservice::remote::GenerateSkFromPkRequest *request,
    ::txservice::remote::GenerateSkFromPkResponse *response,
    ::google::protobuf::Closure *done)
{
    // This object helps to call done->Run() in RAII style. If you need to
    // process the request asynchronously, pass done_guard.release().
    brpc::ClosureGuard done_guard(done);

    NodeGroupId ng_id = request->node_group_id();
    uint64_t scan_ts = request->scan_ts();

    std::string_view table_name_sv{request->table_name_str()};
    TableName base_table_name = TableName(table_name_sv, TableType::Primary);

    int32_t partition_id = request->partition_id();
    const std::string &end_key_str = request->end_key();
    const std::string &start_key_str = request->start_key();

    std::vector<TableName> new_indexes_name;
    new_indexes_name.reserve(request->new_sk_name_str_size());
    for (int idx = 0; idx < request->new_sk_name_str_size(); ++idx)
    {
        std::string_view table_name_sv{request->new_sk_name_str(idx)};
        new_indexes_name.emplace_back(TableName(
            table_name_sv,
            ToLocalType::ConvertCcTableType(request->new_sk_type(idx))));
    }

    uint64_t tx_number = request->tx_number();
    int64_t tx_term = request->tx_term();

    bthread::Mutex bthd_mux;
    bthread::ConditionVariable bthd_cv;
    bool is_finished = false;
    size_t scanned_pk_items_count = 0;
    int res_code = 0;
    std::vector<int64_t> ng_terms_vec;
    std::thread worker_thd = std::thread(
        [&base_table_name,
         &ng_id,
         &scan_ts,
         &partition_id,
         &start_key_str,
         &end_key_str,
         &new_indexes_name,
         &bthd_mux,
         &bthd_cv,
         &is_finished,
         &scanned_pk_items_count,
         &res_code,
         &ng_terms_vec,
         tx_number,
         tx_term]()
        {
            while (Sharder::Instance().LeaderTerm(ng_id) < 0 &&
                   Sharder::Instance().CandidateLeaderTerm(ng_id) > 0)
            {
                // The RPC server can receive the remote request, but this
                // node has not finish log replay, including data(.pk) log and
                // catalog(.table range info) log, so should wait until log
                // replay finished.
                LOG(WARNING) << "CcNodeService GenerateSkFromPk of ng#" << ng_id
                             << " for partition id: " << partition_id
                             << " waiting log replay finished.";
                std::this_thread::sleep_for(3s);
            }

            if (Sharder::Instance().LeaderTerm(ng_id) < 0)
            {
                LOG(WARNING) << "CcNodeService GenerateSkFromPk non-leader "
                             << "node receive this task of ng#" << ng_id
                             << " for partition id: " << partition_id;
                std::unique_lock<bthread::Mutex> lk(bthd_mux);
                res_code =
                    static_cast<int>(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
                is_finished = true;
                bthd_cv.notify_all();
                return;
            }
            DLOG(INFO) << "CcNodeService GenerateSkFromPk RPC of ng#" << ng_id
                       << " for partition id: " << partition_id
                       << ". Base table:" << base_table_name.Trace();

            LocalCcShards *cc_shards = Sharder::Instance().GetLocalCcShards();
            std::unique_ptr<SkGenerator> sk_generator = nullptr;
            {
                std::lock_guard<std::mutex> lk(
                    cc_shards->table_index_op_pool_mux_);
                if (cc_shards->sk_generator_pool_.empty())
                {
                    sk_generator = std::make_unique<SkGenerator>();
                }
                else
                {
                    assert(cc_shards->sk_generator_pool_.back() != nullptr);
                    sk_generator =
                        std::move(cc_shards->sk_generator_pool_.back());
                    cc_shards->sk_generator_pool_.pop_back();
                }
            }

            sk_generator->Reset(start_key_str,
                                end_key_str,
                                scan_ts,
                                base_table_name,
                                ng_id,
                                partition_id,
                                tx_number,
                                tx_term,
                                new_indexes_name);
            sk_generator->ProcessTask();

            CcErrorCode task_res = sk_generator->TaskResult();
            if (task_res != CcErrorCode::NO_ERROR)
            {
                LOG(ERROR) << "Finish this generate index task of ng#" << ng_id
                           << " for partition id: " << partition_id
                           << " caused by error: " << CcErrorMessage(task_res);
            }
            else
            {
                scanned_pk_items_count = sk_generator->ScannedItemsCount();
                auto &terms = sk_generator->NodeGroupTerms();
                size_t ng_cnt = terms.size();
                ng_terms_vec.resize(ng_cnt, INIT_TERM);
                for (size_t idx = 0; idx < ng_cnt; ++idx)
                {
                    ng_terms_vec.at(idx) = terms.at(idx);
                }
            }

            std::unique_lock<bthread::Mutex> lk(bthd_mux);
            res_code = static_cast<int>(task_res);
            is_finished = true;
            bthd_cv.notify_all();

            // recycle skgenerator
            {
                std::lock_guard<std::mutex> lk(
                    cc_shards->table_index_op_pool_mux_);
                cc_shards->sk_generator_pool_.emplace_back(
                    std::move(sk_generator));
            }
        });

    std::unique_lock<bthread::Mutex> lk(bthd_mux);
    while (!is_finished)
    {
        bthd_cv.wait(lk);
    }

    response->set_error_code(res_code);
    response->set_pk_items_count(scanned_pk_items_count);
    for (size_t idx = 0; idx < ng_terms_vec.size(); ++idx)
    {
        response->add_ng_terms(ng_terms_vec.at(idx));
    }

    worker_thd.join();
    DLOG(INFO) << "CcNodeService GenerateSkFromPk RPC of ng#" << ng_id
               << " for partition id: " << partition_id
               << " finished with error: "
               << CcErrorMessage(static_cast<CcErrorCode>(res_code))
               << ". Scanned items count: " << scanned_pk_items_count;
}

void CcNodeService::UploadBatch(
    ::google::protobuf::RpcController *controller,
    const ::txservice::remote::UploadBatchRequest *request,
    ::txservice::remote::UploadBatchResponse *response,
    ::google::protobuf::Closure *done)
{
    // This object helps to call done->Run() in RAII style. If you need to
    // process the request asynchronously, pass done_guard.release().
    brpc::ClosureGuard done_guard(done);

    NodeGroupId ng_id = request->node_group_id();
    int64_t ng_term = request->node_group_term();

    std::string_view table_name_sv{request->table_name_str()};
    TableType table_type =
        ToLocalType::ConvertCcTableType(request->table_type());
    TableName table_name = TableName(table_name_sv, table_type);
    UploadBatchType data_type =
        ToLocalType::ConvertUploadBatchType(request->kind());

    CODE_FAULT_INJECTOR("term_UploadBatch_Timeout", {
        // The rpc timeout.
        FaultInject::Instance().InjectFault("term_UploadBatch_Timeout",
                                            "remove");
        bthread::Mutex b_mux_;
        bthread::ConditionVariable b_cv_;
        std::unique_lock<bthread::Mutex> lk(b_mux_);
        b_cv_.wait_for(lk, 2000000);
        DLOG(ERROR) << "UploadBatch service timeout for table: "
                    << table_name.Trace() << " of ng#" << ng_id;
        return;
    });

    DLOG(INFO) << "CcNodeService UploadBatch RPC of #ng" << ng_id
               << " for table:" << table_name.Trace();

    LocalCcShards *cc_shards = Sharder::Instance().GetLocalCcShards();
    size_t core_cnt = cc_shards->Count();
    uint32_t batch_size = request->batch_size();

    auto write_entry_tuple =
        UploadBatchCc::WriteEntryTuple(request->keys(),
                                       request->records(),
                                       request->commit_ts(),
                                       request->rec_status());

    size_t finished_req = 0;
    bthread::Mutex req_mux;
    bthread::ConditionVariable req_cv;

    UploadBatchCc req;
    req.Use();
    req.Reset(table_name,
              ng_id,
              ng_term,
              core_cnt,
              batch_size,
              write_entry_tuple,
              req_mux,
              req_cv,
              finished_req,
              data_type);
    for (size_t core = 0; core < core_cnt; ++core)
    {
        cc_shards->EnqueueToCcShard(core, &req);
    }

    {
        std::unique_lock<bthread::Mutex> req_lk(req_mux);
        while (finished_req != 1 || req.InUse())
        {
            req_cv.wait_for(req_lk, 1000000);
        }
    }

    response->set_error_code(ToRemoteType::ConvertCcErrorCode(req.ErrorCode()));
    response->set_ng_term(req.CcNgTerm());
    DLOG(INFO) << "CcNodeService UploadBatch RPC of #ng" << ng_id
               << " finished with error: "
               << static_cast<uint32_t>(req.ErrorCode());
}

void CcNodeService::PublishBucketsMigrating(
    ::google::protobuf::RpcController *controller,
    const ::txservice::remote::PubBucketsMigratingRequest *request,
    ::txservice::remote::PubBucketsMigratingResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);

    const NodeGroupId ng_id = request->node_group_id();
    DLOG(INFO) << "CcNodeService received PublishBucketsMigrating RPC of #ng"
               << ng_id << ", is_migrating:" << (int) request->is_migrating();

    if (Sharder::Instance().LeaderNodeId(ng_id) != Sharder::Instance().NodeId())
    {
        response->set_node_group_id(ng_id);
        response->set_success(false);
        LOG(INFO) << "CcNodeService finish PublishBucketsMigrating RPC of #ng"
                  << ng_id << ", current node is not leader !!!";
        return;
    }

    LocalCcShards *cc_shards = Sharder::Instance().GetLocalCcShards();
    bool is_migrating = request->is_migrating();
    cc_shards->SetBucketMigrating(is_migrating);

    if (is_migrating)
    {
        WaitNoNakedBucketRefCc req;
        cc_shards->EnqueueToCcShard(0, &req);
        req.Wait();
    }

    response->set_node_group_id(ng_id);
    response->set_success(true);

    LOG(INFO) << "CcNodeService finish PublishBucketsMigrating RPC of #ng"
              << ng_id;
}

void CcNodeService::UploadRangeSlices(
    ::google::protobuf::RpcController *controller,
    const ::txservice::remote::UploadRangeSlicesRequest *request,
    ::txservice::remote::UploadRangeSlicesResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);

    std::string_view table_name_sv{request->table_name_str()};
    TableName table_name = TableName(table_name_sv, TableType::RangePartition);
    NodeGroupId ng_id = request->node_group_id();
    int32_t old_partition_id = request->old_partition_id();
    uint64_t version_ts = request->version_ts();
    int32_t new_partition_id = request->new_partition_id();
    const std::string &keys = request->new_slices_keys();
    const std::string &sizes = request->new_slices_sizes();
    const std::string &status = request->new_slices_status();
    uint32_t keys_num = request->new_slices_num();

    LocalCcShards *cc_shards = Sharder::Instance().GetLocalCcShards();
    size_t core_cnt = cc_shards->Count();
    uint16_t rand_core = std::rand() % core_cnt;
    // upload range slices info
    UploadRangeSlicesCc req;
    req.Reset(table_name,
              ng_id,
              old_partition_id,
              version_ts,
              new_partition_id,
              &keys,
              &sizes,
              &status,
              keys_num);
    cc_shards->EnqueueToCcShard(rand_core, &req);
    req.Wait();

    response->set_ng_term(req.CcNgTerm());
    response->set_error_code(ToRemoteType::ConvertCcErrorCode(req.ErrorCode()));

    if (req.ErrorCode() != CcErrorCode::NO_ERROR)
    {
        LOG(INFO) << "CcNodeService finish UploadRangeSlices RPC of #ng"
                  << ng_id << ", ng_term:" << req.CcNgTerm() << ", Range#"
                  << old_partition_id << ", DirtyRange#" << new_partition_id
                  << ", error_code:" << (int) req.ErrorCode();
    }
    else
    {
        DLOG(INFO) << "CcNodeService finish UploadRangeSlices RPC of #ng"
                   << ng_id << ", ng_term:" << req.CcNgTerm() << ", Range#"
                   << old_partition_id << ", DirtyRange#" << new_partition_id
                   << ", error_code:" << (int) req.ErrorCode();
    }
}

void CcNodeService::UploadBatchSlices(
    ::google::protobuf::RpcController *controller,
    const ::txservice::remote::UploadBatchSlicesRequest *request,
    ::txservice::remote::UploadBatchResponse *response,
    ::google::protobuf::Closure *done)
{
    // This object helps to call done->Run() in RAII style. If you need to
    // process the request asynchronously, pass done_guard.release().
    brpc::ClosureGuard done_guard(done);

    NodeGroupId ng_id = request->node_group_id();
    int64_t ng_term = request->node_group_term();

    std::string_view table_name_sv{request->table_name_str()};
    TableType table_type =
        ToLocalType::ConvertCcTableType(request->table_type());
    TableName table_name = TableName(table_name_sv, table_type);

    LocalCcShards *cc_shards = Sharder::Instance().GetLocalCcShards();
    size_t core_cnt = cc_shards->Count();

    auto write_entry_tuple =
        UploadBatchSlicesCc::WriteEntryTuple(request->keys(),
                                             request->records(),
                                             request->commit_ts(),
                                             request->rec_status());

    auto slices_info = std::make_shared<UploadBatchSlicesCc::SliceUpdation>();
    slices_info->range_ = request->partition_id();
    slices_info->new_range_ = request->new_partition_id();
    slices_info->version_ts_ = request->version_ts();
    slices_info->slice_idxs_.resize(request->slices_idxs_size());
    for (uint32_t i = 0; i < slices_info->slice_idxs_.size(); i++)
    {
        slices_info->slice_idxs_[i] = request->slices_idxs(i);
    }

    UploadBatchSlicesCc req;
    req.Use();
    req.Reset(
        table_name, ng_id, ng_term, core_cnt, write_entry_tuple, slices_info);

    // Select a core randomly to parse items. After parsed, this core will push
    // the request to other cores to emplace keys.
    uint16_t rand_core = std::rand() % core_cnt;
    cc_shards->EnqueueToCcShard(rand_core, &req);
    req.Wait();

    CcErrorCode err = CcErrorCode::NO_ERROR;
    if (req.ErrorCode() != CcErrorCode::NO_ERROR)
    {
        err = req.ErrorCode();
    }

    response->set_error_code(ToRemoteType::ConvertCcErrorCode(err));
    response->set_ng_term(ng_term);
    DLOG(INFO) << "CcNodeService UploadBatch RPC of #ng" << ng_id
               << " finished with error: " << static_cast<uint32_t>(err);
}

}  // namespace remote
}  // namespace txservice
