#pragma once

#include <braft/raft.h>  // braft::Node braft::StateMachine
#include <braft/util.h>  // braft::AsyncClosureGuard
#include <brpc/channel.h>

#include <filesystem>
#include <shared_mutex>
#include <string>
#include <unordered_set>
#include <vector>

#include "log_replay_service.h"
#include "sharder.h"

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
    CcNode(const uint32_t ng_id,
           const uint32_t node_id,
           const std::string &ip,
           const uint16_t port,
           const std::vector<std::string> &ng_ips,
           const std::vector<uint16_t> &ng_ports,
           std::string storage_path,
           LocalCcShards &local_shards,
           fault::ReplayService *replay_service,
           uint32_t log_group_cnt);

    ~CcNode();

    int Start();

    void Shutdown();

    // Blocking this thread until the node is eventually down.
    void Join();

    int TransferLeader();

    // This should only be called when ng is deleted from cluster. This will
    // shutdown cc node in this ng and delete cc ng log of this ng.
    void Remove()
    {
        Shutdown();
        Join();
        std::filesystem::remove_all(std::filesystem::path(storage_path_));
    }

    bool CheckLogGroupReplayFinished(uint32_t log_group_id, int64_t ng_term);

    void FinishLogGroupReplay(uint32_t log_group_id,
                              int64_t ng_term,
                              uint32_t latest_committed_txn_no,
                              uint64_t last_ckpt_ts);

    /**
     * Pin data of this node group if this ccnode is group leader.
     * Must be called in pair with UnpinData().
     *
     * @return leader term of this ccnode
     */
    int64_t PinData();

    /**
     * Unpin data of this node group so that ccmaps and catalogs can be cleared
     * if ccnode is no longer leader.
     * Must be called in pair with PinData().
     * waitToFinish: true - Wait until the UnpinData to finish
     */
    void UnpinData();

    bool UpdateCkptTs(uint64_t new_ckpt_ts);

    uint64_t GetCkptTs()
    {
        return last_ckpt_ts_.load(std::memory_order_relaxed);
    }

    /**
     * Update the node group config in cc_node. If the config changed and
     * current node is the preferred leader of the ng, update braft node group.
     * The braft ng update will be an async call, it will put cc_req back in
     * queue once it is finished.
     *
     * @return If async call to update braft group is made.
     */
    bool UpdateNodeGroupConfig(const std::vector<std::string> &ng_ips,
                               const std::vector<uint16_t> &ng_ports,
                               std::mutex &mux,
                               std::condition_variable &cv,
                               bool &finished,
                               bool &succ);

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

    // protects ng_ips_, ng_ports_ and node_idx_
    std::shared_mutex config_mux_;
    //  CcNode belongs to node group: ng_id_.
    const uint32_t ng_id_;
    // CcNode is located on node: node_id_.
    const uint32_t node_id_;
    const std::string ip_;
    const uint16_t port_;
    // The offset of this cc node in the cc node group. By default, the first
    // node in the cc node group acts as the leader.
    uint32_t node_idx_;
    // The addresses of the nodes in the cc node group.
    std::vector<std::string> ng_ips_;
    std::vector<uint16_t> ng_ports_;
    // The local path where the Raft configurations are stored.
    const std::string storage_path_;

    braft::Node *volatile node_;

    std::atomic<uint64_t> last_ckpt_ts_;

    // number of threads currently accessing data of this node group, this node
    // group's data cannot be cleared unless pinning_threads_ is 0
    int pinning_threads_;
    std::mutex pinning_threads_mux_;
    std::condition_variable pinning_threads_cv_;

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

class ChangePeerClosure : public braft::Closure
{
public:
    explicit ChangePeerClosure(std::mutex &mux,
                               std::condition_variable &cv,
                               bool &finished,
                               bool &succ,
                               braft::Configuration &config,
                               braft::Node *node)
        : mux_(mux),
          cv_(cv),
          finished_(finished),
          succ_(succ),
          new_config_(config),
          node_(node)
    {
    }
    ~ChangePeerClosure()
    {
    }

    void Run() override;

private:
    std::mutex &mux_;
    std::condition_variable &cv_;
    bool &finished_;
    bool &succ_;
    braft::Configuration new_config_;
    braft::Node *node_;
};
}  // namespace txservice::fault
