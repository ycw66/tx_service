#pragma once

#include <cstdint>
#include <unordered_map>

#include "braft/route_table.h"
#include "brpc/server.h"
#include "fault/cc_node.h"
#include "fault/log_replay_service.h"
#include "remote/cc_node_service.h"
#include "remote/cc_stream_receiver.h"
#include "remote/cc_stream_sender.h"
#include "txlog.h"

namespace txservice
{
#define GET_CCNODE_RPC_PORT(port) port + 1
#define GET_LOG_REPLAY_RPC_PORT(port) port + 3

class LocalCcShards;

/**
 * Sharder is the collection of services supplied by TxService, which includes:
 * 1. cc_stream rpc service, which transfers CcRequests between different
 * TxService nodes.
 * 2. cc_node rpc service, which is used by raft protocol to communicate CcNodes
 * among the raft group.
 * 3. log_replay rpc service, which receives and replay redo log from log
 * service by streaming.
 *
 * Sharder also specifies the hash function which shards the key to the
 * corresponding TxProcessor.
 */
class Sharder
{
public:
    static Sharder &Instance(uint32_t node_id = 0,
                             const std::vector<std::string> *ips = nullptr,
                             const std::vector<uint16_t> *ports = nullptr,
                             LocalCcShards *local_shards = nullptr,
                             std::unique_ptr<TxLog> log_agent = nullptr)
    {
        static Sharder instance_(
            node_id, ips, ports, *local_shards, std::move(log_agent));
        return instance_;
    }

    void Shutdown();

    /**
     * @brief Returns the ID of the leader node of the input cc node group.
     *
     * @param cc_ng_id The cc node group ID.
     * @return uint32_t The ID of the leader node.
     */
    uint32_t LeaderNodeId(uint32_t cc_ng_id) const
    {
        return ng_leader_cache_.at(cc_ng_id).load(std::memory_order_acquire);
    }

    uint32_t ShardCode(uint64_t hash_code) const
    {
        // Uses the lower 10 bits to shard the key across CPU cores in a node.
        uint32_t residual = hash_code & 0x3FF;
        // Uses the higher bits to shard across nodes.
        uint32_t node_group_id = (hash_code >> 10) % ng_leader_cache_.size();
        return (node_group_id << 10) | residual;
    }

    uint32_t NodeGroupCount() const
    {
        return (uint32_t) ng_leader_cache_.size();
    }

    void GetNodeAddress(uint32_t node_id, std::string &ip, uint16_t &port);

    /**
     * @brief Initializes cc nodes in this node and starts network services.
     * Given a node's port number local_port, the cc stream service is on
     * local_port. The raft and the cc rpc services are on local_port+1. The log
     * group is on local_port+2. The replay service is on local_port+3.
     *
     * @param path The local path where raft meta data is stored.
     * @return int Error code.
     */
    int Init(const std::string &path);

    /**
     * @brief Checks if the current leader of the input cc node group is on the
     * same term as the expected one.
     *
     * @param ng_id The cc node group ID.
     * @param term The expected leader term.
     * @return true, if the current leader is the expected one.
     * @return false, otherwise.
     */
    bool CheckLeaderTerm(uint32_t ng_id, int64_t term) const;

    /**
     * @brief The term of the leader of the input cc node group.
     *
     * @param ng_id The cc node group ID.
     * @return int64_t The leader's term.
     */
    int64_t LeaderTerm(uint32_t ng_id) const;

    int64_t CandidateLeaderTerm(uint32_t ng_id) const;

    /**
     * @brief Updates the leader cache of all cc node groups.
     *
     */
    void UpdateLeaders();

    /**
     * @brief When a remote cc node group fails over, this node is unaware of
     * the new leader. Its cc requests are directed to a non-leader node and
     * return with errors. The method updates the leader of the input cc node
     * group in the local cache.
     *
     * @param ng_id The cc node group ID.
     */
    void UpdateLeader(uint32_t ng_id);

    /**
     * @brief NotifyNewLeaderStart rpc will send the node_id of the new leader
     * to each node. Update the leader cache without referring to braft service.
     *
     * @param ng_id The cc node group ID.
     * @param node_id The node_id of leader.
     */
    void UpdateLeader(uint32_t ng_id, uint32_t node_id);

    /**
     * @brief Update the log group's leader node id when the log group leader
     * changed.
     *
     * @param lg_id The log group ID.
     * @param node_id The index of leader in log group.
     */
    void UpdateLogGroupLeader(uint32_t lg_id, uint32_t idx)
    {
        lg_leader_idx_vct_[lg_id]->store(idx, std::memory_order_relaxed);
    }

    /**
     * @brief Processes the RPC that transfers the leader of the specified cc
     * node group to the preferred cc node.
     *
     * @param ng_id The cc node group in which the leader is transferred.
     * @return int Error code.
     */
    int TransferLeader(uint32_t ng_id);

    std::unique_ptr<TxLog> GetLogAgent() const
    {
        return log_agent_->Clone();
    }

    /**
     * @brief Updates the specified cc node's status, when it is recovering as
     * the leader and has received all log records from the specifid log group.
     * The cc node starts serving as the leader, after receiving committed log
     * records from all log groups.
     *
     * @param cc_ng_id The cc node group ID.
     * @param log_group_id The ID of the log group from which committed log
     * records have been shipped.
     */
    void FinishLogReplay(uint32_t cc_ng_id,
                         int64_t cc_ng_term,
                         uint32_t log_group_id);

    /**
     * @brief Recovers the input orphan lock held for an extended period of
     * time.
     *
     * @param tx_number The number of the tx who holds the lock.
     * @param tx_term The term of the tx node when the lock was acquired.
     * @param cc_ng_id The cc node group in which the lock resides.
     * @param cc_ng_term The term of the cc node group of the lock.
     */
    void RecoverTx(uint64_t tx_number,
                   int64_t tx_term,
                   uint32_t cc_ng_id,
                   int64_t cc_ng_term);

    remote::CcStreamSender *GetCcStreamSender()
    {
        return cc_stream_sender_ != nullptr ? cc_stream_sender_.get() : nullptr;
    }

    uint32_t GetNodeCount()
    {
        return ips_.size();
    }

    uint32_t NodeId() const
    {
        return node_id_;
    }

private:
    Sharder(uint32_t node_id,
            const std::vector<std::string> *ips,
            const std::vector<uint16_t> *ports,
            LocalCcShards &local_shards,
            std::unique_ptr<TxLog> log_agent);

    ~Sharder() = default;

    /**
     * @brief Registers all cc node groups and their configurations in the braft
     * cache.
     *
     */
    void ConfigRouteTable();

    uint32_t node_id_;
    std::vector<std::string> ips_;
    std::vector<uint16_t> ports_;
    // save the newest log group leader index, it will be update by rpc call
    // LogReplayService or LogAgent::RefreshLeader when the leader was changed
    std::vector<std::unique_ptr<std::atomic_uint32_t>> lg_leader_idx_vct_;
    // we have one raft group for each logical shard(specified by ip & port)
    // each group's current leader is stored in ng_leader_cache_.
    std::unordered_map<uint32_t, std::atomic<uint32_t>> ng_leader_cache_;
    std::mutex mux_;
    std::unordered_map<uint32_t, std::unique_ptr<fault::CcNode>> cc_nodes_;

    moodycamel::ConcurrentQueue<std::unique_ptr<remote::CcMessage>> msg_pool_;

    // The cc stream sender establishes connections to remote nodes and sends cc
    // requests and responses to remote nodes via streams. It is initialized
    // before the cc stream receiver, given that the cc stream receiver
    // accepts and dispatches cc requests to tx processors, which process the
    // requests and send the responses back via the cc stream sender.
    std::unique_ptr<remote::CcStreamSender> cc_stream_sender_;

    // The RPC server that listens on the port of local_port and serves the cc
    // stream service.
    brpc::Server cc_stream_server_;
    // The stream service that accepts a stream of cc requests from remote
    // nodes.
    std::unique_ptr<remote::CcStreamReceiver> cc_stream_receiver_;

    // The RPC server that listens on the port of local_port+1. It provides sync
    // RPCs toward this node and serves Raft communications within cc node
    // groups.
    brpc::Server cc_node_server_;
    // The service that provides sync RPCs to remote nodes, i.e., leader
    // transfer and checking tx status.
    std::unique_ptr<remote::CcNodeService> cc_node_service_;

    // The RPC server that listens on the port of local_port+3 and serves the
    // log replay service.
    brpc::Server log_replay_server_;
    // The replay service that accepts a stream of log replay messages from all
    // log groups.
    std::unique_ptr<fault::ReplayService> log_replay_service_;

    LocalCcShards &local_shards_;
    std::unique_ptr<TxLog> log_agent_;
};
}  // namespace txservice
