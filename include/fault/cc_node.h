#pragma once

#include <brpc/channel.h>

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
class CcNode
{
public:
    CcNode(const uint32_t ng_id,
           const uint32_t node_id,
           LocalCcShards &local_shards,
           fault::ReplayService *replay_service,
           uint32_t log_group_cnt);

    int64_t Term() const
    {
        return leader_term_.load(std::memory_order_acquire);
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

    void OnLeaderStart(int64_t term);
    void OnLeaderStop();

    /**
     * Update the node group config in cc_node. If the config changed and
     * current node is the preferred leader of the ng, update node group in host
     * manager. The host manager ng update will be an async call, it will put
     * cc_req back in queue once it is finished.
     *
     * @return If async call to update host manager node group is made.
     */
    bool UpdateNodeGroupConfig(const std::vector<std::string> &ng_ips,
                               const std::vector<uint16_t> &ng_ports,
                               std::mutex &mux,
                               std::condition_variable &cv,
                               bool &finished,
                               bool &succ);

private:
    void NotifyNewLeaderStart(uint32_t leader_ng_id, uint32_t leader_node_id);

    //  CcNode belongs to node group: ng_id_.
    const uint32_t ng_id_;
    // CcNode is located on node: node_id_.
    const uint32_t node_id_;
    std::atomic<int64_t> leader_term_;
    std::atomic<int64_t> candidate_leader_term_;

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

}  // namespace txservice::fault
