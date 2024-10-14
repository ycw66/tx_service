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
                       OnLeaderStartResponse *response,
                       ::google::protobuf::Closure *done) override;

    void OnLeaderStop(::google::protobuf::RpcController *controller,
                      const OnLeaderStopRequest *request,
                      OnLeaderChangeResponse *response,
                      ::google::protobuf::Closure *done) override;

    void OnStartFollowing(::google::protobuf::RpcController *controller,
                          const OnStartFollowingRequest *request,
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
        ::google::protobuf::Closure *done) override;

    void CheckClusterConfigIsUpdated(
        ::google::protobuf::RpcController *controller,
        const ::txservice::remote::CheckClusterConfigIsUpdatedRequest *request,
        ::txservice::remote::CheckClusterConfigIsUpdatedResponse *response,
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

    void PublishBucketsMigrating(
        ::google::protobuf::RpcController *controller,
        const ::txservice::remote::PubBucketsMigratingRequest *request,
        ::txservice::remote::PubBucketsMigratingResponse *response,
        ::google::protobuf::Closure *done) override;

    void UploadRangeSlices(
        ::google::protobuf::RpcController *controller,
        const ::txservice::remote::UploadRangeSlicesRequest *request,
        ::txservice::remote::UploadRangeSlicesResponse *response,
        ::google::protobuf::Closure *done) override;

    void UploadBatchSlices(
        ::google::protobuf::RpcController *controller,
        const ::txservice::remote::UploadBatchSlicesRequest *request,
        ::txservice::remote::UploadBatchResponse *response,
        ::google::protobuf::Closure *done) override;

    void FetchPayload(::google::protobuf::RpcController *controller,
                      const ::txservice::remote::FetchPayloadRequest *request,
                      ::txservice::remote::FetchPayloadResponse *response,
                      ::google::protobuf::Closure *done) override;

    void FetchCatalog(::google::protobuf::RpcController *controller,
                      const ::txservice::remote::FetchPayloadRequest *request,
                      ::txservice::remote::FetchPayloadResponse *response,
                      ::google::protobuf::Closure *done) override;

    void StandbyStartFollowing(
        ::google::protobuf::RpcController *controller,
        const ::txservice::remote::StandbyStartFollowingRequest *request,
        ::txservice::remote::StandbyStartFollowingResponse *response,
        ::google::protobuf::Closure *done) override;

    void UpdateStandbyCkptTs(
        ::google::protobuf::RpcController *controller,
        const ::txservice::remote::UpdateStandbyCkptTsRequest *request,
        ::txservice::remote::UpdateStandbyCkptTsResponse *response,
        ::google::protobuf::Closure *done) override;

    void UpdateStandbyConsistentTs(
        ::google::protobuf::RpcController *controller,
        const ::txservice::remote::UpdateStandbyConsistentTsRequest *request,
        ::txservice::remote::UpdateStandbyConsistentTsResponse *response,
        ::google::protobuf::Closure *done) override;

    void RequestResendStandbyMessage(
        ::google::protobuf::RpcController *controller,
        const ::txservice::remote::RequestResendStandbyMessageRequest *request,
        ::txservice::remote::RequestResendStandbyMessageResponse *response,
        ::google::protobuf::Closure *done) override;

    void RequestStorageSnapshotSync(
        ::google::protobuf::RpcController *controller,
        const ::txservice::remote::StorageSnapshotSyncRequest *request,
        ::txservice::remote::StorageSnapshotSyncResponse *response,
        ::google::protobuf::Closure *done) override;

    void OnSnapshotSynced(
        ::google::protobuf::RpcController *controller,
        const ::txservice::remote::OnSnapshotSyncedRequest *request,
        ::txservice::remote::OnSnapshotSyncedResponse *response,
        ::google::protobuf::Closure *done) override;

    void FetchNodeInfo(::google::protobuf::RpcController *controller,
                       const ::txservice::remote::FetchNodeInfoRequest *request,
                       ::txservice::remote::FetchNodeInfoResponse *response,
                       ::google::protobuf::Closure *done) override;

private:
    LocalCcShards &local_shards_;
};
}  // namespace remote
}  // namespace txservice
