#pragma once

#include <brpc/channel.h>
#include <bthread/moodycamelqueue.h>
#include <stdint.h>

#include <atomic>
#include <condition_variable>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "brpc/server.h"
#include "butil/third_party/murmurhash3/murmurhash3.h"
#include "proto/cc_request.pb.h"
#include "tx_serialize.h"
#include "txlog.h"
#include "type.h"

namespace txservice
{
#define GET_CCNODE_RPC_PORT(port) port + 1
#define GET_LOG_REPLAY_RPC_PORT(port) port + 3

class LocalCcShards;
class TxLog;
class TxWorkerPool;
struct TableName;
struct CcRequestBase;
class CcShard;

namespace fault
{
class CcNode;
class ReplayService;
}  // namespace fault

namespace remote
{
class CcNodeService;
class CcStreamSender;
class CcStreamReceiver;
}  // namespace remote
struct NodeConfig
{
public:
    NodeConfig() = default;
    NodeConfig(uint32_t node_id, const std::string &host_name, uint16_t port)
        : node_id_(node_id), host_name_(host_name), port_(port)
    {
    }

    NodeConfig(const NodeConfig &rhs)
        : node_id_(rhs.node_id_), host_name_(rhs.host_name_), port_(rhs.port_)
    {
    }

    void Serialize(std::string &buf) const
    {
        SerializeToStr(&node_id_, buf);
        Serializer<std::string>::Serialize(host_name_, buf);
        SerializeToStr(&port_, buf);
    }

    size_t SerializedLength() const
    {
        return sizeof(uint32_t) + sizeof(uint16_t) + host_name_.length() +
               sizeof(uint16_t);
    }

    void Deserialize(const char *buf, size_t &offset)
    {
        DesrializeFrom(buf, offset, &node_id_);
        host_name_ = Serializer<std::string>::Deserialize(buf, offset);
        DesrializeFrom(buf, offset, &port_);
    }

    uint32_t node_id_{UINT32_MAX};
    std::string host_name_{""};
    uint16_t port_{0};
};
struct ClusterConfig
{
    ClusterConfig() = default;
    ClusterConfig(const ClusterConfig &rhs) : version_(rhs.version_)
    {
        for (auto &pair : rhs.ng_configs_)
        {
            ng_configs_.emplace(pair.first, pair.second);
        }
        for (auto &pair : rhs.cc_nodes_)
        {
            cc_nodes_.emplace(pair.first, pair.second);
        }
    }

    ClusterConfig &operator&=(ClusterConfig &&rhs)
    {
        version_ = rhs.version_;
        ng_configs_ = std::move(rhs.ng_configs_);
        cc_nodes_ = std::move(rhs.cc_nodes_);

        return *this;
    }

    std::unordered_map<NodeGroupId, std::vector<NodeConfig>> ng_configs_;
    std::unordered_map<NodeGroupId, std::shared_ptr<fault::CcNode>> cc_nodes_;
    uint64_t version_{0};
};

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
    static Sharder &Instance(
        uint32_t node_id = 0,
        const std::unordered_map<NodeGroupId, std::vector<NodeConfig>>
            *ng_configs = nullptr,
        uint64_t config_version = 0,
        const std::vector<std::string> *txlog_ips = nullptr,
        const std::vector<uint16_t> *txlog_ports = nullptr,
        LocalCcShards *local_shards = nullptr,
        std::unique_ptr<TxLog> log_agent = nullptr,
        const std::string *local_path = nullptr)
    {
        static Sharder instance_;
        return instance_;
    }

    void Shutdown();
    void CloseStreamSender();

    /**
     * @brief Returns the ID of the leader node of the input cc node group.
     *
     * @param cc_ng_id The cc node group ID.
     * @return uint32_t The ID of the leader node.
     */
    uint32_t LeaderNodeId(uint32_t cc_ng_id)
    {
        return ng_leader_cache_[cc_ng_id].load(std::memory_order_relaxed);
    }

    uint32_t ShardCode(uint64_t hash_code)
    {
        // Uses the lower 10 bits to shard the key across CPU cores in a node.
        uint32_t residual = hash_code & 0x3FF;
        // Uses the higher bits to shard across nodes.

#ifdef ON_KEY_OBJECT
        // Redis use the slot id as shard code mapping to node group.
        uint16_t slot_id = hash_code & 0x3FFF;
        auto ng_count = NodeGroupCount();
        uint16_t slot_count_per_ng = (16384 + ng_count - 1) / ng_count;
        uint32_t node_group_id = slot_id / slot_count_per_ng;

#elif defined(RANGE_PARTITION_ENABLED)
        uint32_t node_group_id = hash_code >> 10;
#else
        uint32_t node_group_id = (hash_code >> 10) % NodeGroupCount();
#endif

        return (node_group_id << 10) | residual;
    }

    uint16_t ShardBucketIdToCoreIdx(uint16_t bucket_id);

    uint32_t ShardToCcNodeGroup(uint32_t sharding_code)
    {
#ifdef ON_KEY_OBJECT
        return sharding_code >> 10;
#else
#ifdef RANGE_PARTITION_ENABLED
        return sharding_code >> 10;
#else
        return (sharding_code >> 10) % NodeGroupCount();
#endif
#endif
    }

    static inline uint16_t MapRangeIdToBucketId(int32_t range_id)
    {
        uint32_t hash_val;
        butil::MurmurHash3_x86_32(&range_id, sizeof(range_id), 9001, &hash_val);
        return hash_val % total_range_buckets;
    }

    uint32_t NodeGroupCount()
    {
        std::shared_lock<std::shared_mutex> lk(cluster_cnf_mux_);
        return cluster_config_.ng_configs_.size();
    }

    /**
     * @brief Gets the ip and port of the input node. Result is saved in ip and
     * port. If the node id is not found in cluster, set ip as empty string.
     *
     */
    void GetNodeAddress(uint32_t node_id, std::string &ip, uint16_t &port);

    /**
     * @brief Initializes cc nodes in this node and starts network services.
     * Given a node's port number local_port, the cc stream service is on
     * local_port. The raft and the cc rpc services are on local_port+1. The log
     * group is on local_port+2. The replay service is on local_port+3.
     *
     * @return int Error code.
     */
    int Init(uint32_t node_id,
             const std::unordered_map<NodeGroupId, std::vector<NodeConfig>>
                 *ng_configs,
             uint64_t config_version,
             const std::vector<std::string> *txlog_ips,
             const std::vector<uint16_t> *txlog_ports,
             const std::string *hm_ip,
             const uint16_t *hm_port,
             const std::string *hm_bin_path,
             LocalCcShards *local_shards,
             std::unique_ptr<TxLog> log_agent,
             const std::string &local_path,
             const uint16_t rep_group_cnt);

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

    void SetLeaderTerm(NodeGroupId ng_id, int64_t term)
    {
        leader_term_cache_[ng_id].store(term, std::memory_order_release);
    }

    void SetCandidateTerm(NodeGroupId ng_id, int64_t term)
    {
        candidate_leader_term_cache_[ng_id].store(term,
                                                  std::memory_order_release);
    }

    /**
     * @brief The term of the leader of the input cc node group.
     *
     * @param ng_id The cc node group ID.
     * @return int64_t The leader's term.
     */
    int64_t LeaderTerm(uint32_t ng_id) const;

    int64_t CandidateLeaderTerm(uint32_t ng_id) const;

    int64_t InvalidLeaderTerm(uint32_t ng_id) const
    {
        if (!cc_nodes_init_.load(std::memory_order_acquire))
        {
            return -1;
        }
        return invalid_leader_term_cache_[ng_id].load(
            std::memory_order_acquire);
    }

    void SetInvalidLeaderTerm(NodeGroupId ng_id, int64_t term)
    {
        int64_t cur_term = -1;
        while (!invalid_leader_term_cache_[ng_id].compare_exchange_strong(
            cur_term, term, std::memory_order_acq_rel))
        {
            if (cur_term >= term)
            {
                return;
            }
        }
    }

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
     * to each node. Update the leader cache without referring to hm service.
     *
     * @param ng_id The cc node group ID.
     * @param node_id The node_id of leader.
     */
    void UpdateLeader(uint32_t ng_id, uint32_t node_id);

    void OnLeaderStart(uint32_t ng_id, int64_t term);

    void OnLeaderStop(uint32_t ng_id);

    /**
     * @brief Update the log group's leader node id when the log group leader
     * changed.
     *
     * @param lg_id The log group ID.
     * @param node_id The node_id of leader in log group.
     */
    void UpdateLogGroupLeader(uint32_t lg_id, uint32_t node_id)
    {
        log_agent_->UpdateLeaderCache(lg_id, node_id);
    }

    /**
     * Whether this node is preferred node group's leader. If not, should
     * request leader transfer.
     * @return
     */
    bool IsPreferredGroupLeader()
    {
        return CandidateLeaderTerm(node_id_) > 0 || LeaderTerm(node_id_) > 0;
    }

    TxLog *GetLogAgent() const
    {
        return log_agent_.get();
    }

    /**
     * @brief Updates the specified cc node's status, when it is recovering as
     * the leader and has received all log records from the specifid log group.
     * The cc node starts serving as the leader, after receiving committed log
     * records from all log groups.
     *
     * @param cc_ng_id The cc node group ID.
     * @param cc_ng_term The cc node's term.
     * @param log_group_id The ID of the log group from which committed log
     * records have been shipped.
     * @param latest_txn_no The latest txn number committed from cc node group
     * cc_ng_id, valid only when cc node group cc_ng_id is bound to log group
     * log_group_id, otherwise it should be 0.
     * @param last_ckpt_ts The last checkpoint timestamp of node group cc_ng_id,
     * used to update each ccshard's ts_base_.
     */
    void FinishLogReplay(uint32_t cc_ng_id,
                         int64_t cc_ng_term,
                         uint32_t log_group_id,
                         uint32_t latest_txn_no,
                         uint64_t last_ckpt_ts);

    bool CheckLogGroupReplayFinished(uint32_t cc_ng_id,
                                     uint32_t log_group_id,
                                     int64_t cc_ng_term);

    /**
     * @brief Wait for all the tx_service nodes to finish the log recovery
     * process and setup the cc_stream_sender.
     *
     */
    void WaitClusterReady();

    /**
     * @brief Recovers the input orphan lock held for an extended period of
     * time.
     *
     * @param lock_tx_number The number of the tx who holds the lock.
     * @param lock_tx_coord_term The term of the tx coordinator node when
     * the lock was acquired.
     * @param lock_cc_ng_id The cc node group in which the lock resides.
     * @param lock_cc_ng_term The term of the cc node group of the lock.
     */
    void RecoverTx(uint64_t lock_tx_number,
                   int64_t lock_tx_coord_term,
                   uint64_t write_lock_ts,
                   uint32_t lock_cc_ng_id,
                   int64_t lock_cc_ng_term);
    /**
     * @brief Transfer the leader for the log group. This function is developed
     * for test
     *
     * @param log_group_id log group id that need to transfer leader
     * @param leader_idx the node index from 0 in log group for new leader
     */
    void LogTransferLeader(uint32_t log_group_id, uint32_t leader_idx);

    remote::CcStreamSender *GetCcStreamSender()
    {
        return cc_stream_sender_ != nullptr ? cc_stream_sender_.get() : nullptr;
    }

    uint32_t GetNodeCount()
    {
        std::shared_lock<std::shared_mutex> lk(cluster_cnf_mux_);
        return cluster_config_.ng_configs_.size();
    }

    uint32_t NodeId() const
    {
        return node_id_;
    }

    void NodeGroupFinishRecovery(uint32_t ng_id)
    {
        std::lock_guard<std::mutex> lk(recovery_state_mux_);

        recovered_leader_set_.emplace(ng_id);
        recovery_state_cv_.notify_one();
    }

    LocalCcShards *GetLocalCcShards()
    {
        return local_shards_;
    }

    void CleanCcTable(const TableName &tabname);

    void NotifyCheckPointer();

    std::vector<uint32_t> LocalNodeGroups();

    /**
     * Try to pin data of cc_ng_id if it is group leader
     * @param cc_ng_id
     * @return leader term of cc_ng_id, -1 if not found
     */
    int64_t TryPinNodeGroupData(uint32_t cc_ng_id);

    /**
     * Unpin data of cc_ng_id, clear ccmaps and catalogs if this node is no
     * longer group leader and pinning threads number decreases to 0.
     * Must be called in pair with TryPinNodeGroupData if TryPinNodeGroupData
     * returns success, otherwise OnLeaderStop will be blocked
     * forever.
     * @param cc_ng_id
     */
    void UnpinNodeGroupData(uint32_t cc_ng_id);

    uint64_t GetNodeGroupCkptTs(uint32_t cc_ng_id);

    bool UpdateNodeGroupCkptTs(uint32_t cc_ng_id, uint64_t ckpt_ts);

    TxWorkerPool *GetTxWorkerPool()
    {
        return tx_worker_pool_.get();
    }

    size_t GetLocalCcShardsCount();

    /**
     * @brief Calculate new node group config after adding new nodes to current
     * cluster.
     * @return New cluster node group configs.
     */
    std::unordered_map<uint32_t, std::vector<NodeConfig>> AddNodeToCluster(
        std::vector<std::pair<std::string, uint16_t>> &new_nodes);

    /**
     * @brief Calculate new node group config after removing nodes from current
     * cluster.
     * @return New cluster node group configs.
     */
    std::unordered_map<uint32_t, std::vector<NodeConfig>> RemoveNodeFromCluster(
        uint16_t removed_node_count);

    /**
     * @brief Update current cluster config to the new_ng_configs. The config
     * will only be updated if current config version is older than given
     * version.
     */
    void UpdateClusterConfig(
        const std::unordered_map<NodeGroupId, std::vector<NodeConfig>>
            &new_ng_configs,
        uint64_t version,
        CcRequestBase *cc_req,
        CcShard *cc_shard);

    uint64_t ClusterConfigVersion()
    {
        std::shared_lock<std::shared_mutex> lk(cluster_cnf_mux_);
        return cluster_config_.version_;
    }

    /**
     * @brief Should accept the cc requests from remote node after the
     * TxProcessor thread start, so, should start cc stream server after the
     * txservice::Start().
     */
    void StartCcStreamReceiver();

    std::shared_ptr<brpc::Channel> GetCcNodeServiceChannel(uint32_t node_id);

    /**
     * Update cc node service channel if the current cached channel equals
     * old_channel. If current cached channel != old_channel, that means it's
     * already updated by someone else.
     */
    std::shared_ptr<brpc::Channel> UpdateCcNodeServiceChannel(
        uint32_t node_id, std::shared_ptr<brpc::Channel> old_channel);

private:
    Sharder();

    ~Sharder();

    void SetCommandLineOptions();

private:
    uint32_t node_id_;

    std::shared_mutex cluster_cnf_mux_;
    // Stores the current cluster config. It contains the mapping relation
    // between node group id and node group members, current node group leader
    // etc.
    ClusterConfig cluster_config_;
    // The replicate number of node group.
    uint16_t rep_group_cnt_;

    // Ng leader cache. We preallocate it to the max cluster size so that we
    // don't need to modify the size of it.
    std::atomic<uint32_t> ng_leader_cache_[1000];
    std::atomic<int64_t> leader_term_cache_[1000];
    std::atomic<int64_t> candidate_leader_term_cache_[1000];
    // cache of the largest invalid term of each ng. Requests from nodes with
    // invalid term will be rejected.
    std::atomic<int64_t> invalid_leader_term_cache_[1000];
    std::vector<std::string> txlog_ips_;
    std::vector<uint16_t> txlog_ports_;

    // Used to protect recovered_leader_set
    std::mutex recovery_state_mux_;
    std::condition_variable recovery_state_cv_;

    // Used at node start stage to check whether all the involed tx_nodes finish
    // the log recovery. If some nodes stepdown during cluster startup, the
    // normal retry logic for each operation will handle it.
    std::unordered_set<uint32_t> recovered_leader_set_;

    /**
     * @brief Acts as a memory barrier such that initialized cc nodes are synced
     * with following reads of cc nodes at all cores.
     *
     */
    std::atomic<bool> cc_nodes_init_{false};

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

    // Worker pool for doing various aync works
    std::unique_ptr<TxWorkerPool> tx_worker_pool_;

    // Worker thread that communicates with host manager process.
    std::unique_ptr<TxWorkerPool> sharder_worker_;

    LocalCcShards *local_shards_;
    std::unique_ptr<TxLog> log_agent_;

    // Channel to cc node service of other nodes.
    std::unordered_map<uint32_t, std::shared_ptr<brpc::Channel>>
        cc_node_service_channels_;

    std::shared_mutex node_channel_mux_;
    // Host manager ip and port
    std::string hm_ip_{""};
    uint16_t hm_port_{0};
    brpc::Channel hm_channel_;
};
}  // namespace txservice
