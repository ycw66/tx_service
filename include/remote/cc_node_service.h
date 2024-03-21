#pragma once

#include <brpc/channel.h>
#include <bthread/condition_variable.h>

#include "proto/cc_request.pb.h"

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

    void OnLeaderStart(::google::protobuf::RpcController *controller,
                       const OnLeaderStartRequest *request,
                       OnLeaderChangeResponse *response,
                       ::google::protobuf::Closure *done) override;

    void OnLeaderStop(::google::protobuf::RpcController *controller,
                      const OnLeaderStopRequest *request,
                      OnLeaderChangeResponse *response,
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

    void GetClusterNodes(
        ::google::protobuf::RpcController *controller,
        const ::txservice::remote::GetClusterNodesRequest *request,
        ::txservice::remote::GetClusterNodesResponse *response,
        ::google::protobuf::Closure *done) override;

    void CheckClusterScaleStatus(
        ::google::protobuf::RpcController *controller,
        const ::txservice::remote::ClusterScaleStatusRequest *request,
        ::txservice::remote::ClusterScaleStatusResponse *response,
        ::google::protobuf::Closure *done);

    void AcquireNodeGroupLeaderTerm(
        ::google::protobuf::RpcController *controller,
        const AcquireNodeGroupTermRequest *request,
        AcquireNodeGroupTermResponse *response,
        ::google::protobuf::Closure *done) override;

    void FlushDataAll(::google::protobuf::RpcController *controller,
                      const FlushDataAllRequest *request,
                      FlushDataAllResponse *response,
                      ::google::protobuf::Closure *done) override;

    void InitDataMigration(
        ::google::protobuf::RpcController *controller,
        const ::txservice::remote::InitMigrationRequest *request,
        ::txservice::remote::InitMigrationResponse *response,
        ::google::protobuf::Closure *done) override;

    void GenerateSkFromPk(
        ::google::protobuf::RpcController *controller,
        const ::txservice::remote::GenerateSkFromPkRequest *request,
        ::txservice::remote::GenerateSkFromPkResponse *response,
        ::google::protobuf::Closure *done) override;

    void UploadBatch(::google::protobuf::RpcController *controller,
                     const ::txservice::remote::UploadBatchRequest *request,
                     ::txservice::remote::UploadBatchResponse *response,
                     ::google::protobuf::Closure *done) override;

private:
    LocalCcShards &local_shards_;
};
}  // namespace remote
}  // namespace txservice
