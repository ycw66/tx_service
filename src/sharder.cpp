#include "sharder.h"

#include <atomic>

#include "cc_req_base.h"
#include "cc_shard.h"
#include "fault/cc_node.h"
#include "fault/log_replay_service.h"
#include "remote/cc_node_service.h"
#include "remote/cc_stream_receiver.h"
#include "remote/cc_stream_sender.h"
#include "tx_service.h"
#include "tx_worker_pool.h"

// gflags 2.1.1 missing GFLAGS_NAMESPACE. This is a workaround to handle gflags
// ABI issue.
#ifdef OVERRIDE_GFLAGS_NAMESPACE
namespace GFLAGS_NAMESPACE = gflags;
#endif

namespace txservice
{
Sharder::Sharder() = default;
Sharder::~Sharder() = default;

void Sharder::Shutdown()
{
    LOG(INFO) << "Shutting down the sharder at node #" << node_id_;

    cc_stream_receiver_->Shutdown();
    cc_stream_server_.Stop(0);
    cc_stream_server_.Join();
    cc_stream_receiver_ = nullptr;

    log_replay_service_->Shutdown();
    log_replay_server_.Stop(0);
    log_replay_server_.Join();

    // shutdown braft node.
    auto cluster_config = cluster_config_;
    for (auto &cc_node : cluster_config->cc_nodes_)
    {
        cc_node.second->Shutdown();
    }
    cc_node_server_.Stop(0);

    // join braft node.
    for (auto &cc_node : cluster_config->cc_nodes_)
    {
        cc_node.second->Join();
    }
    cc_node_server_.Join();
    cc_node_service_ = nullptr;

    // Delete all braft nodes with COW.
    std::shared_ptr<ClusterConfig> dirty_cluster_config =
        std::make_shared<ClusterConfig>();
    dirty_cluster_config->version_ = cluster_config->version_;
    dirty_cluster_config->ng_configs_ = cluster_config->ng_configs_;
    cluster_config_ = dirty_cluster_config;

    // CcNode will access log_replay_service_ to replay log when becoming node
    // group leader, so log_replay_service_ should be destructed after all
    // CcNodes are stopped.
    log_replay_service_ = nullptr;

    tx_worker_pool_->Shutdown();
    tx_worker_pool_ = nullptr;
    sharder_worker_->Shutdown();
    sharder_worker_ = nullptr;

    LOG(INFO) << "The sharder at node #" << node_id_ << " shut down.";
}

void Sharder::CloseStreamSender()
{
    LOG(INFO) << "Close Stream sender at node #" << node_id_;
    cc_stream_sender_ = nullptr;
}

void Sharder::CloseBraft()
{
    LOG(INFO) << "Close braft at node #" << node_id_;

    // shutdown braft node.
    auto cluster_config = cluster_config_;
    for (auto &cc_node : cluster_config->cc_nodes_)
    {
        cc_node.second->Shutdown();
    }
    cc_node_server_.Stop(0);

    LOG(INFO) << "The braft at node #" << node_id_ << " shut down.";
}

void Sharder::GetNodeAddress(uint32_t node_id, std::string &ip, uint16_t &port)
{
    auto cluster_config = cluster_config_;
    if (node_id >= cluster_config->ng_configs_.size())
    {
        // Node is already removed from cluster
        ip = "";
        port = 0;
        return;
    }

    ip = cluster_config->ng_configs_.at(node_id).front().host_name_;
    port = cluster_config->ng_configs_.at(node_id).front().port_;
}

int Sharder::Init(
    uint32_t node_id,
    const std::unordered_map<uint32_t, std::vector<NodeConfig>> *ng_configs,
    uint64_t config_version,
    const std::vector<std::string> *txlog_ips,
    const std::vector<uint16_t> *txlog_ports,
    LocalCcShards *local_shards,
    std::unique_ptr<TxLog> log_agent,
    const std::string &local_path)
{
    node_id_ = node_id;
    local_shards_ = local_shards;
    log_agent_ = std::move(log_agent);
    raft_local_path_ = local_path;

    cluster_config_ = std::make_shared<ClusterConfig>();
    for (uint32_t nid = 0; nid < 1000; nid++)
    {
        ng_leader_cache_[nid].store(nid);
        leader_term_cache_[nid].store(-1);
        candidate_leader_term_cache_[nid].store(-1);
    }
    if (ng_configs != nullptr)
    {
        for (const auto &pair : *ng_configs)
        {
            std::vector<NodeConfig> group_config;
            for (const auto &config : pair.second)
            {
                group_config.emplace_back(config);
            }
            cluster_config_->ng_configs_.try_emplace(pair.first,
                                                     std::move(group_config));
        }
        cluster_config_->version_ = config_version;
    }
    else
    {
        cluster_config_->ng_configs_.try_emplace(0);
        cluster_config_->version_ = config_version;
    }

    if (txlog_ips != nullptr)
    {
        txlog_ips_ = *txlog_ips;
        txlog_ports_ = *txlog_ports;
    }

    if (log_agent_ != nullptr)
    {
        log_agent_->Init(txlog_ips_, txlog_ports_, 0);
    }

#ifdef EXT_TX_PROC_ENABLED
    tx_worker_pool_ = std::make_unique<TxWorkerPool>(
        local_shards_->Count() >= 2 ? local_shards_->Count() / 2 : 1);
#else
    tx_worker_pool_ = std::make_unique<TxWorkerPool>(local_shards_->Count());
#endif
    sharder_worker_ = std::make_unique<TxWorkerPool>(1);
    // there shouldn't be any concurrent visit before Init retruns so we
    // can directly modify ng_configs_ without doing copy on write.
    // construct log_replay_service_ before cc_nodes_
    log_replay_service_ = std::make_unique<fault::ReplayService>(
        *local_shards_,
        GetLogAgent(),
        cluster_config_->ng_configs_.at(node_id_).front().host_name_,
        GET_LOG_REPLAY_RPC_PORT(
            cluster_config_->ng_configs_.at(node_id_).front().port_));
    if (log_replay_server_.AddService(log_replay_service_.get(),
                                      brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
    {
        LOG(FATAL) << "Fail to start add the log replay service to the log "
                      "replay server.";
        return -1;
    }

    for (uint32_t ng_id = 0; ng_id < cluster_config_->ng_configs_.size();
         ++ng_id)
    {
        for (size_t idx = 0;
             idx < cluster_config_->ng_configs_.at(ng_id).size();
             ++idx)
        {
            if (cluster_config_->ng_configs_.at(ng_id).at(idx).node_id_ ==
                node_id_)
            {
                std::string store_path(raft_local_path_);
                store_path.append("/cc_ng/");
                store_path.append(std::to_string(ng_id));

                // Use cc node port + 1 for cc node raft port
                std::vector<uint16_t> group_ports;
                std::vector<std::string> group_ips;
                for (const auto &config :
                     cluster_config_->ng_configs_.at(ng_id))
                {
                    group_ports.emplace_back(config.port_ + 1);
                    group_ips.emplace_back(config.host_name_);
                }
                cluster_config_->cc_nodes_.try_emplace(
                    ng_id,
                    std::make_shared<fault::CcNode>(
                        ng_id,
                        node_id_,
                        cluster_config_->ng_configs_.at(node_id_)
                            .front()
                            .host_name_,
                        cluster_config_->ng_configs_.at(node_id_)
                                .front()
                                .port_ +
                            1,
                        group_ips,
                        group_ports,
                        store_path,
                        *local_shards_,
                        log_replay_service_.get(),
                        log_agent_->LogGroupCount()));
            }
        }
    }

    cc_nodes_init_.store(true, std::memory_order_release);

    // Initialize cc stream related structs. Even we do not need it if cluster
    // is running in single node mode, the cluster might scale into multi-node
    // state later.
    cc_stream_receiver_ =
        std::make_unique<remote::CcStreamReceiver>(*local_shards_, msg_pool_);
    if (cc_stream_server_.AddService(cc_stream_receiver_.get(),
                                     brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
    {
        LOG(FATAL)
            << "Fail to add the cc stream service to the cc stream server.";
        return -1;
    }

    cc_stream_sender_ = std::make_unique<remote::CcStreamSender>(msg_pool_);
    cc_stream_sender_->UpdateRemoteNodes(cluster_config_->ng_configs_);

    // Initializes the Raft service that listens on the port of local_port + 1.
    if (braft::add_service(
            &cc_node_server_,
            GET_CCNODE_RPC_PORT(
                cluster_config_->ng_configs_.at(node_id_).front().port_)) != 0)
    {
        LOG(ERROR) << "Fail to add the Raft service for cc nodes.";
        return -1;
    }

    SetCommandLineOptions();

    cc_node_service_ = std::make_unique<remote::CcNodeService>(*local_shards_);
    if (cc_node_server_.AddService(cc_node_service_.get(),
                                   brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
    {
        LOG(FATAL) << "Fail to add the cc node service to the server.";
        return -1;
    }

    // Start the cc_stream_server_ after TxProcessor start using interface
    // StartCcStreamReceiver().

    if (cc_node_server_.Start(
            GET_CCNODE_RPC_PORT(
                cluster_config_->ng_configs_.at(node_id_).front().port_),
            NULL) != 0)
    {
        LOG(FATAL) << "Fail to start the cc node server.";
        return -1;
    }

    // start braft state machine by initialize braft node.
    for (auto &pair : cluster_config_->cc_nodes_)
    {
        pair.second->Start();
    }

    ConfigRouteTable(cluster_config_->ng_configs_);

    // The log replay server uses local_port+3 for receiving streams from log
    // groups.
    if (log_replay_server_.Start(
            GET_LOG_REPLAY_RPC_PORT(
                cluster_config_->ng_configs_.at(node_id_).front().port_),
            nullptr) != 0)
    {
        LOG(FATAL) << "Fail to start the log replay server.";
        return -1;
    }

    return 0;
}

bool Sharder::CheckLeaderTerm(uint32_t ng_id, int64_t term) const
{
    int64_t node_term = LeaderTerm(ng_id);

    if (node_term < 0)
    {
        // The cc node is not the leader. The cc maps in this node does not
        // own cc entries belonging to this shard/node group.
        return false;
    }

    return term == node_term;
}

int64_t Sharder::LeaderTerm(uint32_t ng_id) const
{
    if (!cc_nodes_init_.load(std::memory_order_acquire))
    {
        return -1;
    }
    return leader_term_cache_[ng_id].load(std::memory_order_acquire);
}

int64_t Sharder::CandidateLeaderTerm(uint32_t ng_id) const
{
    if (!cc_nodes_init_.load(std::memory_order_acquire))
    {
        return -1;
    }

    return candidate_leader_term_cache_[ng_id].load(std::memory_order_acquire);
}

void Sharder::UpdateLeaders()
{
    auto cluster_config = cluster_config_;
    for (const auto &ng_pair : cluster_config->ng_configs_)
    {
        UpdateLeader(ng_pair.first);
    }
}

void Sharder::UpdateLeader(uint32_t ng_id)
{
    // Hand it off to worker thread since braft refresh leader blocks the
    // thread.
    sharder_worker_->SubmitWork(
        [this, ng_id]
        {
            std::string node_group_id("ng");
            node_group_id.append(std::to_string(ng_id));

            // Blocking the thread until query_leader finishes
            butil::Status st = braft::rtb::refresh_leader(node_group_id, 1000);
            if (!st.ok())
            {
                std::cout << "Fail to refresh leader. " << st.error_str()
                          << std::endl;
                return;
            }

            braft::PeerId leader;
            // Get the cached leader of the target group from RouteTable
            if (braft::rtb::select_leader(node_group_id, &leader) != 0)
            {
                std::cout << "Fail to select the leader." << std::endl;
                return;
            }
            std::string leader_ip_port;
            if (leader.type_ == braft::PeerId::Type::EndPoint)
            {
                leader_ip_port.append(butil::endpoint2str(leader.addr).c_str());
            }
            else
            {
                leader_ip_port.append(leader.hostname_addr.to_string());
            }

            size_t comma_pos = leader_ip_port.find(':');
            assert(comma_pos != std::string::npos);
            std::string leader_ip_str = leader_ip_port.substr(0, comma_pos);
            uint16_t leader_port =
                std::stoi(leader_ip_port.substr(comma_pos + 1));

            for (auto &node : cluster_config_->ng_configs_.at(ng_id))
            {
                if (node.host_name_ == leader_ip_str &&
                    GET_CCNODE_RPC_PORT(node.port_) == leader_port)
                {
                    ng_leader_cache_[ng_id].store(node.node_id_,
                                                  std::memory_order_release);
                    break;
                }
            }
        });
}

void Sharder::UpdateLeader(uint32_t ng_id, uint32_t node_id)
{
    DLOG(INFO) << "ccnode group ng" << ng_id
               << " updates leader to node_id:" << node_id;

    // leader cache update is atomic by itself so we don't need to copy on
    // write.
    ng_leader_cache_[ng_id].store(node_id, std::memory_order_release);
}

void Sharder::FinishLogReplay(uint32_t cc_ng_id,
                              int64_t cc_ng_term,
                              uint32_t log_group_id,
                              uint32_t latest_txn_no,
                              uint64_t last_ckpt_ts)
{
    auto cluster_config = cluster_config_;

    auto find_it = cluster_config->cc_nodes_.find(cc_ng_id);
    if (find_it == cluster_config->cc_nodes_.end())
    {
        return;
    }

    find_it->second->FinishLogGroupReplay(
        log_group_id, cc_ng_term, latest_txn_no, last_ckpt_ts);
    local_shards_->UpdateTsBase(last_ckpt_ts);
}

void Sharder::WaitClusterReady()
{
    bool recovery_all_finished = false;
    do
    {
        std::unique_lock<std::mutex> lk(recovery_state_mux_);
        // cluster_config_ might be updated during replay. We need
        // to obtain the latest cluster_config_ before checking in every loop.
        auto cluster_config = cluster_config_;
        for (auto &pair : cluster_config->ng_configs_)
        {
            uint32_t ng_id = pair.first;
            if (recovered_leader_set_.find(ng_id) ==
                recovered_leader_set_.end())
            {
                if (ng_id == node_id_)
                {
                    // Wait FinishLogReplay emplace recovered_leader_set_.
                }
                else
                {
                    // Send message to remote node to check whether it finish
                    // the log recovery. we use the cc_stream_sender and
                    // cc_stream_receiver to test whether the stream is
                    // established or not.
                    remote::CcMessage send_msg;

                    send_msg.set_type(
                        remote::CcMessage::MessageType::
                            CcMessage_MessageType_RecoverStateCheckRequest);

                    remote::RecoverStateCheckRequest *recover_req =
                        send_msg.mutable_recover_state_check_req();
                    recover_req->set_src_node_id(node_id_);
                    recover_req->set_node_group_id(ng_id);

                    cc_stream_sender_->SendMessageToNg(ng_id, send_msg);
                }
            }
        }

        recovery_all_finished = recovery_state_cv_.wait_for(
            lk,
            1s,
            [this, cluster_config]() {
                return recovered_leader_set_.size() ==
                       cluster_config->ng_configs_.size();
            });
    } while (!recovery_all_finished);
}

void Sharder::RecoverTx(uint64_t lock_tx_number,
                        int64_t lock_tx_coord_term,
                        uint64_t write_lock_ts,
                        uint32_t lock_cc_ng_id,
                        int64_t lock_cc_ng_term)
{
    if (LeaderTerm(lock_cc_ng_id) > 0)
    {
        log_replay_service_->RecoverTx(lock_tx_number,
                                       lock_tx_coord_term,
                                       write_lock_ts,
                                       lock_cc_ng_id,
                                       lock_cc_ng_term);
    }
}

void Sharder::ConfigRouteTable(
    const std::unordered_map<NodeGroupId, std::vector<NodeConfig>> &ng_configs)
{
    for (auto &pair : ng_configs)
    {
        uint32_t ng_id = pair.first;
        std::string group_id("ng");
        group_id.append(std::to_string(ng_id));

        std::string group_conf;
        for (uint32_t idx = 0; idx < pair.second.size(); ++idx)
        {
            if (idx > 0)
            {
                group_conf.append(",");
            }

            group_conf.append(pair.second.at(idx).host_name_);
            group_conf.append(":");
            group_conf.append(std::to_string(pair.second.at(idx).port_ + 1));
            group_conf.append(":");
            group_conf.append(std::to_string(idx));
        }

        braft::rtb::update_configuration(group_id, group_conf);
    }
}

int Sharder::TransferLeader(uint32_t ng_id)
{
    auto cluster_config = cluster_config_;

    auto find_it = cluster_config->cc_nodes_.find(ng_id);
    if (find_it == cluster_config->cc_nodes_.end())
    {
        return 1;
    }

    return find_it->second->TransferLeader();
}

void Sharder::LogTransferLeader(uint32_t log_group_id, uint32_t leader_idx)
{
    log_agent_->TransferLeader(log_group_id, leader_idx);
}

void Sharder::CleanCcTable(const TableName &tabname)
{
    return local_shards_->CleanCcTable(tabname);
}

void Sharder::NotifyCheckPointer()
{
    return local_shards_->NotifyCheckPointer();
}

std::vector<uint32_t> Sharder::LocalNodeGroups()
{
    std::vector<uint32_t> ngs;
    auto cluster_config = cluster_config_;
    for (auto &pair : cluster_config->cc_nodes_)
    {
        ngs.push_back(pair.first);
    }
    return ngs;
}

int64_t Sharder::TryPinNodeGroupData(uint32_t cc_ng_id)
{
    auto cluster_config = cluster_config_;

    auto find_it = cluster_config->cc_nodes_.find(cc_ng_id);
    if (find_it != cluster_config->cc_nodes_.end())
    {
        return find_it->second->PinData();
    }
    return -1;
}

void Sharder::UnpinNodeGroupData(uint32_t cc_ng_id)
{
    auto cluster_config = cluster_config_;

    auto find_it = cluster_config->cc_nodes_.find(cc_ng_id);
    if (find_it != cluster_config->cc_nodes_.end())
    {
        find_it->second->UnpinData();
    }
}

uint64_t Sharder::GetNodeGroupCkptTs(uint32_t cc_ng_id)
{
    auto cluster_config = cluster_config_;

    auto find_it = cluster_config->cc_nodes_.find(cc_ng_id);
    if (find_it != cluster_config->cc_nodes_.end())
    {
        return find_it->second->GetCkptTs();
    }
    return 0;
}

bool Sharder::UpdateNodeGroupCkptTs(uint32_t cc_ng_id, uint64_t ckpt_ts)
{
    auto cluster_config = cluster_config_;

    auto find_it = cluster_config->cc_nodes_.find(cc_ng_id);
    if (find_it != cluster_config->cc_nodes_.end())
    {
        return find_it->second->UpdateCkptTs(ckpt_ts);
    }
    return false;
}

void Sharder::SetCommandLineOptions()
{
    // set brpc circuit_breaker max isolation duration smaller than election
    // timeout so that restarted node will join raft group before trying to
    // start a new vote
    GFLAGS_NAMESPACE::SetCommandLineOption(
        "circuit_breaker_max_isolation_duration_ms", "4500");
}

size_t Sharder::GetLocalCcShardsCount()
{
    return local_shards_->Count();
}

std::unordered_map<uint32_t, std::vector<NodeConfig>> Sharder::AddNodeToCluster(
    std::vector<std::pair<std::string, uint16_t>> &new_nodes)
{
    // Make a copy of the current ng configs.
    auto cluster_config = cluster_config_;
    std::unordered_map<uint32_t, std::vector<NodeConfig>> new_ng_configs(
        cluster_config->ng_configs_);

    uint32_t rep_group_cnt =
        fault::CcNode::rep_group_cnt <
                new_nodes.size() + cluster_config->ng_configs_.size()
            ? fault::CcNode::rep_group_cnt
            : new_nodes.size() + cluster_config->ng_configs_.size();
    // Add a new node group for each new added node, and assign the nodes
    // that are in least number of node groups as the member of new node
    // groups.
    for (auto &node : new_nodes)
    {
        NodeGroupId new_ng_id = new_ng_configs.size();
        // Add this node to the new node group as the preferred leader.
        std::vector<NodeConfig> members{
            NodeConfig(new_ng_id, node.first, node.second)};
        new_ng_configs.try_emplace(new_ng_id, std::move(members));
    }

    // Loop over current ng configs, and build a map from node id
    // to the number of node groups this node is in.
    std::unordered_map<uint32_t, int> node_ng_count;
    for (auto &pair : new_ng_configs)
    {
        for (auto &node : pair.second)
        {
            auto res_pair = node_ng_count.try_emplace(node.node_id_, 0);
            res_pair.first->second++;
        }
    }

    // Rebalance the members in each node groups.
    // Make sure each ng has at least rep_group_cnt members.
    for (auto &config_pair : new_ng_configs)
    {
        // Find members for this new node group.
        std::vector<NodeConfig> &members = config_pair.second;
        while (members.size() < rep_group_cnt)
        {
            int least_node_id = -1;
            int least_node_ng_count = INT32_MAX;
            // Find the node with the least number of node groups.
            for (auto &[node_id, ng_count] : node_ng_count)
            {
                if (ng_count < least_node_ng_count)
                {
                    // check if this node is already in this node group.
                    bool skip = false;
                    for (auto &node : members)
                    {
                        if (node.node_id_ == node_id)
                        {
                            skip = true;
                            break;
                        }
                    }
                    if (!skip)
                    {
                        least_node_id = node_id;
                        least_node_ng_count = ng_count;
                    }
                }
            }
            assert(least_node_id != -1);
            members.emplace_back(new_ng_configs[least_node_id].front());
            node_ng_count[least_node_id]++;
        }
    }

    return new_ng_configs;
}

std::unordered_map<uint32_t, std::vector<NodeConfig>>
Sharder::RemoveNodeFromCluster(uint16_t removed_node_count)
{
    // Make a copy of the current ng configs.
    auto cluster_config = cluster_config_;
    std::unordered_map<uint32_t, std::vector<NodeConfig>> new_ng_configs(
        cluster_config->ng_configs_);

    uint32_t rep_group_cnt =
        fault::CcNode::rep_group_cnt <
                cluster_config->ng_configs_.size() - removed_node_count
            ? fault::CcNode::rep_group_cnt
            : cluster_config->ng_configs_.size() - removed_node_count;
    assert(rep_group_cnt > 0);
    // Remove the nodes with greatest node id.
    NodeGroupId largest_node_id = new_ng_configs.size() - 1;
    for (int i = 0; i < removed_node_count; i++)
    {
        // Remove the node groups where these nodes are preferred leader.
        new_ng_configs.erase(largest_node_id);
        largest_node_id--;
    }
    for (auto &ng_config : new_ng_configs)
    {
        // Remove these nodes from other node groups where they are members.
        for (auto member_it = std::next(ng_config.second.begin());
             member_it != ng_config.second.end();)
        {
            if (member_it->node_id_ > largest_node_id)
            {
                member_it = ng_config.second.erase(member_it);
            }
            else
            {
                member_it++;
            }
        }
    }
    // Loop over current ng configs, and build a map from node id
    // to the number of node groups this node is in.
    std::unordered_map<uint32_t, int> node_ng_count;
    for (auto &pair : new_ng_configs)
    {
        for (auto &node : pair.second)
        {
            auto res_pair = node_ng_count.try_emplace(node.node_id_, 0);
            res_pair.first->second++;
        }
    }

    // Rebalance the members in each node groups.
    // Make sure each ng has at least rep_group_cnt members.
    for (auto &config_pair : new_ng_configs)
    {
        // Find members for this new node group.
        std::vector<NodeConfig> &members = config_pair.second;
        while (members.size() < rep_group_cnt)
        {
            int least_node_id = -1;
            int least_node_ng_count = INT32_MAX;
            // Find the node with the least number of node groups.
            for (auto &[node_id, ng_count] : node_ng_count)
            {
                if (ng_count < least_node_ng_count)
                {
                    // check if this node is already in this node group.
                    bool skip = false;
                    for (auto &node : members)
                    {
                        if (node.node_id_ == node_id)
                        {
                            skip = true;
                            break;
                        }
                    }
                    if (!skip)
                    {
                        least_node_id = node_id;
                        least_node_ng_count = ng_count;
                    }
                }
            }
            assert(least_node_id != -1);
            members.emplace_back(new_ng_configs[least_node_id].front());
            node_ng_count[least_node_id]++;
        }
    }

    return new_ng_configs;
}

void Sharder::UpdateClusterConfig(
    const std::unordered_map<NodeGroupId, std::vector<NodeConfig>>
        &new_ng_configs,
    uint64_t version,
    CcRequestBase *cc_req,
    CcShard *cc_shard)
{
    // Since sharder worker is a single thread worker, we don't need to worry
    // about cluster config being updated by multiple threads at the same time.
    sharder_worker_->SubmitWork(
        [this, new_ng_configs, cc_req, cc_shard, version]
        {
            auto cluster_config = cluster_config_;
            if (cluster_config->version_ >= version)
            {
                // If the given version is older than current version, do
                // nothing.
                cc_shard->Enqueue(cc_req);
                return;
            }
            // Shutdown the cc nodes for the deleted node groups.
            for (auto &pair : cluster_config->cc_nodes_)
            {
                auto ng_iter = new_ng_configs.find(pair.first);
                if (ng_iter == new_ng_configs.end())
                {
                    pair.second->Remove();
                }
                else
                {
                    bool is_member = false;
                    for (auto &node : ng_iter->second)
                    {
                        if (node.node_id_ == node_id_)
                        {
                            is_member = true;
                            break;
                        }
                    }

                    if (!is_member)
                    {
                        pair.second->Remove();
                    }
                }
            }
            std::mutex mux;
            std::condition_variable cv;
            bool finished = false;
            bool succ = true;

            std::shared_ptr<ClusterConfig> dirty_cluster_config =
                std::make_shared<ClusterConfig>();
            dirty_cluster_config->version_ = version;
            dirty_cluster_config->ng_configs_ = new_ng_configs;
            bool braft_group_updated = false;

            for (auto &ng_pair : new_ng_configs)
            {
                bool is_member = false;
                for (auto &node : ng_pair.second)
                {
                    if (node.node_id_ == node_id_)
                    {
                        is_member = true;
                        break;
                    }
                }

                // If this node group already exists in the current cluster
                // config
                auto find_it = cluster_config->ng_configs_.find(ng_pair.first);
                if (find_it != cluster_config->ng_configs_.end())
                {
                    if (is_member)
                    {
                        auto cc_node_it =
                            cluster_config->cc_nodes_.find(ng_pair.first);
                        if (cc_node_it != cluster_config->cc_nodes_.end())
                        {
                            // Reuse original cc_node_ object
                            auto ins_pair =
                                dirty_cluster_config->cc_nodes_.try_emplace(
                                    ng_pair.first, cc_node_it->second);

                            // Use cc node port + 1 for cc node raft port
                            std::vector<uint16_t> group_ports;
                            std::vector<std::string> group_ips;
                            for (auto &config :
                                 new_ng_configs.at(ng_pair.first))
                            {
                                group_ports.emplace_back(config.port_ + 1);
                                group_ips.emplace_back(config.host_name_);
                            }
                            // Update node group config in cc_node_, this will
                            // also update raft config if this node is preferred
                            // leader.
                            braft_group_updated =
                                ins_pair.first->second->UpdateNodeGroupConfig(
                                    group_ips,
                                    group_ports,
                                    mux,
                                    cv,
                                    finished,
                                    succ) ||
                                braft_group_updated;
                        }
                        else
                        {
                            std::string store_path(raft_local_path_);
                            store_path.append("/cc_ng/");
                            store_path.append(std::to_string(ng_pair.first));

                            // Use cc node port + 1 for cc node raft port
                            std::vector<uint16_t> group_ports;
                            std::vector<std::string> group_ips;
                            for (auto &config :
                                 new_ng_configs.at(ng_pair.first))
                            {
                                group_ports.emplace_back(config.port_ + 1);
                                group_ips.emplace_back(config.host_name_);
                            }
                            auto ins_pair =
                                dirty_cluster_config->cc_nodes_.try_emplace(
                                    ng_pair.first,
                                    std::make_shared<fault::CcNode>(
                                        ng_pair.first,
                                        node_id_,
                                        new_ng_configs.at(node_id_)
                                            .front()
                                            .host_name_,
                                        new_ng_configs.at(node_id_)
                                                .front()
                                                .port_ +
                                            1,
                                        group_ips,
                                        group_ports,
                                        store_path,
                                        *local_shards_,
                                        log_replay_service_.get(),
                                        log_agent_->LogGroupCount()));

                            if (ins_pair.first->second->Start() < 0)
                            {
                                cc_req->AbortCcRequest(
                                    CcErrorCode::REQUESTED_NODE_NOT_LEADER);
                                return;
                            }
                        }
                    }
                }
                else
                {
                    // Node does not exists in the old cluster config.
                    if (is_member)
                    {
                        std::string store_path(raft_local_path_);
                        store_path.append("/cc_ng/");
                        store_path.append(std::to_string(ng_pair.first));

                        // Use cc node port + 1 for cc node raft port
                        std::vector<uint16_t> group_ports;
                        std::vector<std::string> group_ips;
                        for (auto &config : new_ng_configs.at(ng_pair.first))
                        {
                            group_ports.emplace_back(config.port_ + 1);
                            group_ips.emplace_back(config.host_name_);
                        }
                        auto ins_pair =
                            dirty_cluster_config->cc_nodes_.try_emplace(
                                ng_pair.first,
                                std::make_shared<fault::CcNode>(
                                    ng_pair.first,
                                    node_id_,
                                    new_ng_configs.at(node_id_)
                                        .front()
                                        .host_name_,
                                    new_ng_configs.at(node_id_).front().port_ +
                                        1,
                                    group_ips,
                                    group_ports,
                                    store_path,
                                    *local_shards_,
                                    log_replay_service_.get(),
                                    log_agent_->LogGroupCount()));
                        if (ins_pair.first->second->Start() < 0)
                        {
                            cc_req->AbortCcRequest(
                                CcErrorCode::REQUESTED_NODE_NOT_LEADER);
                            return;
                        }
                    }
                }
            }

            cc_stream_sender_->UpdateRemoteNodes(
                dirty_cluster_config->ng_configs_);

            ConfigRouteTable(dirty_cluster_config->ng_configs_);

            // Wait until braft node group config update is done
            // before actually switching the cluster config.
            if (braft_group_updated)
            {
                std::unique_lock<std::mutex> lk(mux);
                cv.wait(lk, [&finished] { return finished; });
                if (!succ)
                {
                    // If update config has failed due to this node
                    // no longer being leader, abort the cc request
                    cc_req->AbortCcRequest(
                        CcErrorCode::REQUESTED_NODE_NOT_LEADER);
                    return;
                }
            }

            // Make the copy on write switch
            cluster_config_ = dirty_cluster_config;
            cc_shard->Enqueue(cc_req);
        });
}

void Sharder::StartCcStreamReceiver()
{
    // The cc_stream_receiver_ object has been add to this server during
    // Sharder::Init().
    if (cc_stream_server_.Start(
            cluster_config_->ng_configs_.at(node_id_).front().port_, NULL) != 0)
    {
        LOG(FATAL) << "Fail to start the cc stream server.";
    }
}
}  // namespace txservice
