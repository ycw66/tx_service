#pragma once

#include <brpc/channel.h>

#include "../proto/cc_request.pb.h"

namespace txservice
{
class LocalCcShards;

namespace remote
{
/**
 * @brief CcNodeService is a RPC service associated with each physical node,
 * providing sync RPCs invoked by remote nodes. It is on the same port as the
 * node's cc stream service, which accepts a stream of cc requests from remote
 * nodes.
 *
 */
class CcNodeService : public CcRpcService
{
public:
    CcNodeService(LocalCcShards &local_shards);

    void Transfer(::google::protobuf::RpcController *controller,
                  const TransferRequest *request,
                  TransferResponse *response,
                  ::google::protobuf::Closure *done) override;

    void CheckTxStatus(::google::protobuf::RpcController *controller,
                       const CheckTxStatusRequest *request,
                       CheckTxStatusResponse *response,
                       ::google::protobuf::Closure *done) override;

    void NotifyNewLeaderStart(::google::protobuf::RpcController *controller,
                              const NotifyNewLeaderStartRequest *request,
                              NotifyNewLeaderStartResponse *response,
                              ::google::protobuf::Closure *done) override;
    void GetMinTxStartTs(
        ::google::protobuf::RpcController *controller,
        const ::txservice::remote::GetMinTxStartTsRequest *request,
        ::txservice::remote::GetMinTxStartTsResponse *response,
        ::google::protobuf::Closure *done) override;

    void ClusterAddNode(
        ::google::protobuf::RpcController *controller,
        const ::txservice::remote::ClusterAddNodeRequest *request,
        ::txservice::remote::ClusterAddNodeResponse *response,
        ::google::protobuf::Closure *done) override;

    void ClusterRemoveNode(
        ::google::protobuf::RpcController *controller,
        const ::txservice::remote::ClusterRemoveNodeRequest *request,
        ::txservice::remote::ClusterRemoveNodeResponse *response,
        ::google::protobuf::Closure *done) override;

private:
    LocalCcShards &local_shards_;
};
}  // namespace remote
}  // namespace txservice