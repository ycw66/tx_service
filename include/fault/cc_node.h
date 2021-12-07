#pragma once

#include <braft/raft.h>  // braft::Node braft::StateMachine
#include <braft/util.h>  // braft::AsyncClosureGuard
#include <brpc/channel.h>

#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include "cc_node_recovery.h"
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
                    const std::string &ip,
                    const uint16_t port,
                    const std::vector<std::string> &ng_ips,
                    const std::vector<uint16_t> &ng_ports,
                    std::string storage_path,
                    LocalCcShards &local_shards,
                    uint32_t log_group_cnt);

    ~CcNode()
    {
        recovery_hd_ = nullptr;

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

    void FinishLogGroupReplay(uint32_t log_group_id);

    void RecoverTx(uint64_t tx_number,
                   int64_t tx_term,
                   uint32_t cc_ng_id,
                   int64_t cc_ng_term);

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

    void on_apply(braft::Iterator &iter) override
    {
    }

    void on_leader_start(int64_t term) override;

    void on_leader_stop(const butil::Status &status) override
    {
    }

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

    const uint32_t ng_id_;
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
    std::unique_ptr<CcNodeRecoveryAgent> recovery_hd_;
    std::unordered_set<uint32_t> recovered_log_groups_;
    uint32_t log_group_cnt_;
};
}  // namespace txservice::fault
