#include "remote/cc_node_service.h"

#include "cc/local_cc_shards.h"
#include "sharder.h"

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

}  // namespace remote
}  // namespace txservice