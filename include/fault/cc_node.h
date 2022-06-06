#pragma once

#include <braft/raft.h>  // braft::Node braft::StateMachine
#include <braft/util.h>  // braft::AsyncClosureGuard
#include <brpc/channel.h>

#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include "log_replay_service.h"
#include "proto/cc_request.pb.h"

namespace txservice::fault
{
namespace txservice
{
class LocalCcShards;
}

/**
 * @brief A cc node is a member of a Raft group in which the leader holds a part
 * of distributed cc maps and the followers are empty. When a cc node becomes
 * the leader, either because the Raft group starts or because of a failover,
 * the leader recovers the committed, unflushed writes from all log groups,
 * before starting serving cc requests. On failover, the cc entries in the old
 * leader are invalidated. By default, given the port number of the local node,
 * the Raft group is on port_number+1.
 *
 */
class CcNode : public braft::StateMachine
{
public:
    static const uint16_t rep_group_cnt = 3;

    explicit CcNode(const uint32_t ng_id,
                    const uint32_t node_id,
                    const std::string &ip,
                    const uint16_t port,
                    const std::vector<std::string> &ng_ips,
                    const std::vector<uint16_t> &ng_ports,
                    std::string storage_path,
                    LocalCcShards &local_shards,
                    fault::ReplayService *replay_service,
                    uint32_t log_group_cnt);

    ~CcNode()
    {
        if (node_ != nullptr)
        {
            delete node_;
        }
    }

    int Start();

    int TransferLeader();

    int64_t Term() const
    {
        return leader_term_.load(std::memory_order_acquire);
    }

    void FinishLogGroupReplay(uint32_t log_group_id,
                              int64_t ng_term,
                              uint32_t latest_committed_txn_no);

    int64_t CandidateTerm() const
    {
        return candidate_leader_term_;
    }

private:
    static braft::NodeOptions BaseNodeOptions()
    {
        braft::NodeOptions node_options;
        node_options.election_timeout_ms = 5000;
        node_options.node_owns_fsm = false;
        node_options.snapshot_interval_s = 0;
        node_options.disable_cli = false;
        return node_options;
    }

    void NotifyNewLeaderStart(uint32_t leader_ng_id, uint32_t leader_node_id);

    void on_apply(braft::Iterator &iter) override
    {
    }

    void on_leader_start(int64_t term) override;

    void on_leader_stop(const butil::Status &status) override;

    void on_shutdown() override
    {
    }

    void on_error(const ::braft::Error &err) override
    {
        LOG(ERROR) << "Raft error " << err;
    }

    using ::braft::StateMachine::on_configuration_committed;
    void on_configuration_committed(const ::braft::Configuration &) override
    {
    }

    void on_stop_following(const ::braft::LeaderChangeContext &ctx) override
    {
    }

    void on_start_following(const ::braft::LeaderChangeContext &ctx) override;

    // CcNode belongs to node group: ng_id_.
    const uint32_t ng_id_;
    // CcNode is located on node: node_id_.
    const uint32_t node_id_;
    const std::string ip_;
    const uint16_t port_;
    // The offset of this cc node in the cc node group. By default, the first
    // node in the cc node group acts as the leader.
    uint32_t node_idx_;
    // The addresses of the nodes in the cc node group.
    const std::vector<std::string> ng_ips_;
    const std::vector<uint16_t> ng_ports_;
    // The local path where the Raft configurations are stored.
    const std::string storage_path_;

    braft::Node *volatile node_;
    std::atomic<int64_t> leader_term_;
    int64_t candidate_leader_term_;

    LocalCcShards &local_cc_shards_;

    // for replay log and recover txn
    fault::ReplayService *replay_service_;

    // recovered_log_groups_ records the log groups which have finished the
    // recovery.
    std::unordered_set<uint32_t> recovered_log_groups_;
    // recovery_mux_ is used to protect recovered_log_groups_, since it will be
    // updated by replay thread and raft service thread concurrently.
    std::mutex recovery_mux_;

    uint32_t log_group_cnt_;
};
}  // namespace txservice::fault
