#include "sharder.h"

#include "braft/route_table.h"
#include "brpc/server.h"
#include "fault/cc_node.h"
#include "fault/log_replay_service.h"
#include "proto/cc_request.pb.h"
#include "remote/cc_node_service.h"
#include "remote/cc_stream_receiver.h"
#include "remote/cc_stream_sender.h"
#include "tx_service.h"
#include "tx_worker_pool.h"
#include "txlog.h"

// gflags 2.1.1 missing GFLAGS_NAMESPACE. This is a workaround to handle gflags
// ABI issue.
#ifdef OVERRIDE_GFLAGS_NAMESPACE
namespace GFLAGS_NAMESPACE = gflags;
#endif

namespace txservice
{
Sharder::Sharder(uint32_t node_id,
                 const std::map<uint32_t, std::vector<NodeConfig>> *ng_configs,
                 const std::vector<std::string> *txlog_ips,
                 const std::vector<uint16_t> *txlog_ports,
                 LocalCcShards &local_shards,
                 std::unique_ptr<TxLog> log_agent)
    : node_id_(node_id),
      mux_(),
      recovery_state_mux_(),
      cc_stream_sender_(nullptr),
      cc_stream_receiver_(nullptr),
      cc_node_service_(nullptr),
      log_replay_service_(nullptr),
      tx_worker_pool_(nullptr),
      local_shards_(local_shards),
      log_agent_(std::move(log_agent))
{
    if (ng_configs != nullptr)
    {
        for (auto &pair : *ng_configs)
        {
            ng_leader_cache_.try_emplace(pair.first, pair.first);
            std::vector<NodeConfig> group_config;
            for (auto &config : pair.second)
            {
                group_config.emplace_back(config);
            }
            ng_configs_.try_emplace(pair.first, std::move(group_config));
        }
    }
    else
    {
        ng_leader_cache_.try_emplace(0, 0);
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
}

Sharder::~Sharder() = default;

void Sharder::Shutdown()
{
    LOG(INFO) << "Shutting down the sharder at node #" << node_id_;

    if (ng_configs_.size() > 1)
    {
        cc_stream_sender_ = nullptr;

        cc_stream_receiver_->Shutdown();
        cc_stream_server_.Stop(0);
        cc_stream_server_.Join();
        cc_stream_receiver_ = nullptr;
    }

    log_replay_service_->Shutdown();
    log_replay_server_.Stop(0);
    log_replay_server_.Join();

    // shutdown braft node.
    for (auto &cc_node : cc_nodes_)
    {
        cc_node.second->Shutdown();
    }
    cc_node_server_.Stop(0);

    // join braft node.
    for (auto &cc_node : cc_nodes_)
    {
        cc_node.second->Join();
    }
    cc_node_server_.Join();
    cc_node_service_ = nullptr;

    // CcNode will access log_replay_service_ to replay log when becoming node
    // group leader, so log_replay_service_ should be destructed after all
    // CcNodes are stopped.
    log_replay_service_ = nullptr;

    tx_worker_pool_->Shutdown();
    tx_worker_pool_ = nullptr;

    LOG(INFO) << "The sharder at node #" << node_id_ << " shut down.";
}

void Sharder::CloseBraft()
{
    LOG(INFO) << "Close braft at node #" << node_id_;

    // shutdown braft node.
    for (auto &cc_node : cc_nodes_)
    {
        cc_node.second->Shutdown();
    }
    cc_node_server_.Stop(0);

    LOG(INFO) << "The braft at node #" << node_id_ << " shut down.";
}

void Sharder::GetNodeAddress(uint32_t node_id, std::string &ip, uint16_t &port)
{
    assert(node_id < ng_configs_.size());

    ip = ng_configs_.at(node_id).front().host_name_;
    port = ng_configs_.at(node_id).front().port_;
}

int Sharder::Init(const std::string &path)
{
    // construct log_replay_service_ before cc_nodes_
    log_replay_service_ = std::make_unique<fault::ReplayService>(
        local_shards_,
        GetLogAgent(),
        ng_configs_.at(node_id_).front().host_name_,
        GET_LOG_REPLAY_RPC_PORT(ng_configs_.at(node_id_).front().port_));
    if (log_replay_server_.AddService(log_replay_service_.get(),
                                      brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
    {
        LOG(FATAL) << "Fail to start add the log replay service to the log "
                      "replay server.";
        return -1;
    }

    for (uint32_t ng_id = 0; ng_id < ng_configs_.size(); ++ng_id)
    {
        for (size_t idx = 0; idx < ng_configs_.at(ng_id).size(); ++idx)
        {
            if (ng_configs_.at(ng_id).at(idx).node_id_ == node_id_)
            {
                std::string store_path(path);
                store_path.append("/cc_ng/");
                store_path.append(std::to_string(ng_id));

                // Use cc node port + 1 for cc node raft port
                std::vector<uint16_t> group_ports;
                std::vector<std::string> group_ips;
                for (auto &config : ng_configs_.at(ng_id))
                {
                    group_ports.emplace_back(config.port_ + 1);
                    group_ips.emplace_back(config.host_name_);
                }
                cc_nodes_.try_emplace(
                    ng_id,
                    std::make_unique<fault::CcNode>(
                        ng_id,
                        node_id_,
                        ng_configs_.at(node_id_).front().host_name_,
                        ng_configs_.at(node_id_).front().port_ + 1,
                        group_ips,
                        group_ports,
                        store_path,
                        local_shards_,
                        log_replay_service_.get(),
                        log_agent_->LogGroupCount()));
            }
        }
    }

    cc_nodes_init_.store(true, std::memory_order_release);

    if (ng_configs_.size() > 1)
    {
        cc_stream_receiver_ = std::make_unique<remote::CcStreamReceiver>(
            local_shards_, msg_pool_);
        if (cc_stream_server_.AddService(cc_stream_receiver_.get(),
                                         brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
        {
            LOG(FATAL)
                << "Fail to add the cc stream service to the cc stream server.";
            return -1;
        }

        if (cc_stream_server_.Start(ng_configs_.at(node_id_).front().port_,
                                    NULL) != 0)
        {
            LOG(FATAL) << "Fail to start the cc stream server.";
            return -1;
        }

        cc_stream_sender_ = std::make_unique<remote::CcStreamSender>(msg_pool_);
        for (auto &pair : ng_configs_)
        {
            // Build a stream to every node even to ourselves because the
            // current node can become leader of multiple node groups during
            // failover. In that case we might need to handle remote requests
            // sent from the same node but from different node group.
            cc_stream_sender_->AddRemoteNode(pair.first,
                                             pair.second.front().host_name_,
                                             pair.second.front().port_);
        }
    }

    // Initializes the Raft service that listens on the port of local_port + 1.
    if (braft::add_service(
            &cc_node_server_,
            GET_CCNODE_RPC_PORT(ng_configs_.at(node_id_).front().port_)) != 0)
    {
        LOG(ERROR) << "Fail to add the Raft service for cc nodes.";
        return -1;
    }

    SetCommandLineOptions();

    cc_node_service_ = std::make_unique<remote::CcNodeService>(local_shards_);
    if (cc_node_server_.AddService(cc_node_service_.get(),
                                   brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
    {
        LOG(FATAL) << "Fail to add the cc node service to the server.";
        return -1;
    }

    if (cc_node_server_.Start(
            GET_CCNODE_RPC_PORT(ng_configs_.at(node_id_).front().port_),
            NULL) != 0)
    {
        LOG(FATAL) << "Fail to start the cc node server.";
        return -1;
    }

    // start braft state machine by initialize braft node.
    for (auto rit = cc_nodes_.begin(); rit != cc_nodes_.end(); ++rit)
    {
        rit->second->Start();
    }

    ConfigRouteTable();

    // The log replay server uses local_port+3 for receiving streams from log
    // groups.
    if (log_replay_server_.Start(
            GET_LOG_REPLAY_RPC_PORT(ng_configs_.at(node_id_).front().port_),
            nullptr) != 0)
    {
        LOG(FATAL) << "Fail to start the log replay server.";
        return -1;
    }

    tx_worker_pool_ = std::make_unique<TxWorkerPool>(local_shards_.Count());

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

    auto find_it = cc_nodes_.find(ng_id);
    if (find_it == cc_nodes_.end())
    {
        return -1;
    }

    const fault::CcNode &cc_node = *find_it->second;
    return cc_node.Term();
}

int64_t Sharder::CandidateLeaderTerm(uint32_t ng_id) const
{
    if (!cc_nodes_init_.load(std::memory_order_acquire))
    {
        return -1;
    }

    auto find_it = cc_nodes_.find(ng_id);
    if (find_it == cc_nodes_.end())
    {
        return -1;
    }

    const fault::CcNode &cc_node = *find_it->second;
    return cc_node.CandidateTerm();
}

void Sharder::UpdateLeaders()
{
    for (const auto &ng_pair : ng_leader_cache_)
    {
        UpdateLeader(ng_pair.first);
    }
}

void Sharder::UpdateLeader(uint32_t ng_id)
{
    std::string node_group_id("ng");
    node_group_id.append(std::to_string(ng_id));

    std::lock_guard<std::mutex> lk(mux_);
    // Blocking the thread until query_leader finishes
    butil::Status st = braft::rtb::refresh_leader(node_group_id, 1000);
    if (!st.ok())
    {
        std::cout << "Fail to refresh leader. " << st.error_str() << std::endl;
        return;
    }

    braft::PeerId leader;
    // Get the cached leader of the target group from RouteTable
    if (braft::rtb::select_leader(node_group_id, &leader) != 0)
    {
        std::cout << "Fail to select the leader." << std::endl;
        return;
    }

    std::string leader_ip_port(butil::endpoint2str(leader.addr).c_str());
    size_t comma_pos = leader_ip_port.find(':');
    assert(comma_pos != std::string::npos);
    std::string leader_ip_str = leader_ip_port.substr(0, comma_pos);
    uint16_t leader_port = leader.addr.port;

    // We only need shared_lock here to make sure ng_configs_ and
    // ng_leader_cache_ are not adding or removing entries since
    // ng_leader_cache_ is storing atomic values.
    for (auto &node : ng_configs_[ng_id])
    {
        if (node.host_name_ == leader_ip_str &&
            GET_CCNODE_RPC_PORT(node.port_) == leader_port)
        {
            ng_leader_cache_.at(ng_id).store(node.node_id_,
                                             std::memory_order_release);
            break;
        }
    }
}

void Sharder::UpdateLeader(uint32_t ng_id, uint32_t node_id)
{
    DLOG(INFO) << "ccnode group ng" << ng_id
               << " updates leader to node_id:" << node_id;
    ng_leader_cache_.at(ng_id).store(node_id, std::memory_order_release);
}

void Sharder::FinishLogReplay(uint32_t cc_ng_id,
                              int64_t cc_ng_term,
                              uint32_t log_group_id,
                              uint32_t latest_txn_no,
                              uint64_t last_ckpt_ts)
{
    auto ng_it = cc_nodes_.find(cc_ng_id);
    if (ng_it == cc_nodes_.end())
    {
        return;
    }

    ng_it->second->FinishLogGroupReplay(
        log_group_id, cc_ng_term, latest_txn_no, last_ckpt_ts);
    local_shards_.UpdateTsBase(last_ckpt_ts);
}

void Sharder::WaitClusterReady()
{
    std::unique_lock<std::mutex> lk(recovery_state_mux_);

    while (true)
    {
        bool recovery_all_finished = true;
        for (auto &pair : ng_configs_)
        {
            uint32_t ng_id = pair.first;
            if (recovered_leader_set.find(ng_id) == recovered_leader_set.end())
            {
                recovery_all_finished = false;

                if (ng_id == node_id_)
                {
                    if (Sharder::Instance().LeaderTerm(ng_id) > 0)
                    {
                        recovered_leader_set.emplace(ng_id);
                    }
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

        if (recovery_all_finished)
        {
            break;
        }
        else
        {
            using namespace std::chrono_literals;
            lk.unlock();
            std::this_thread::sleep_for(1s);
            lk.lock();
        }
    }
}

void Sharder::RecoverTx(uint64_t lock_tx_number,
                        int64_t lock_tx_coord_term,
                        uint32_t lock_cc_ng_id,
                        int64_t lock_cc_ng_term)
{
    if (LeaderTerm(lock_cc_ng_id) > 0)
    {
        log_replay_service_->RecoverTx(
            lock_tx_number, lock_tx_coord_term, lock_cc_ng_id, lock_cc_ng_term);
    }
}

void Sharder::ConfigRouteTable()
{
    for (auto &pair : ng_configs_)
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
    auto ng_it = cc_nodes_.find(ng_id);
    if (ng_it == cc_nodes_.end())
    {
        return 1;
    }

    return ng_it->second->TransferLeader();
}

void Sharder::LogTransferLeader(uint32_t log_group_id, uint32_t leader_idx)
{
    log_agent_->TransferLeader(log_group_id, leader_idx);
}

void Sharder::CleanCcTable(const TableName &tabname)
{
    return local_shards_.CleanCcTable(tabname);
}

void Sharder::NotifyCheckPointer()
{
    return local_shards_.NotifyCheckPointer();
}

std::vector<uint32_t> Sharder::LocalNodeGroups()
{
    std::vector<uint32_t> ngs;
    for (auto &pair : cc_nodes_)
    {
        ngs.push_back(pair.first);
    }
    return ngs;
}

int64_t Sharder::TryPinNodeGroupData(uint32_t cc_ng_id)
{
    auto it = cc_nodes_.find(cc_ng_id);
    if (it != cc_nodes_.end())
    {
        return it->second->PinData();
    }
    return -1;
}

void Sharder::UnpinNodeGroupData(uint32_t cc_ng_id)
{
    auto it = cc_nodes_.find(cc_ng_id);
    if (it != cc_nodes_.end())
    {
        it->second->UnpinData();
    }
}

uint64_t Sharder::GetNodeGroupCkptTs(uint32_t cc_ng_id)
{
    auto it = cc_nodes_.find(cc_ng_id);
    if (it != cc_nodes_.end())
    {
        return it->second->GetCkptTs();
    }
    return 0;
}

bool Sharder::UpdateNodeGroupCkptTs(uint32_t cc_ng_id, uint64_t ckpt_ts)
{
    auto it = cc_nodes_.find(cc_ng_id);
    if (it != cc_nodes_.end())
    {
        return it->second->UpdateCkptTs(ckpt_ts);
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
    return local_shards_.Count();
}

std::map<uint32_t, std::vector<NodeConfig>> Sharder::AddNodeToCluster(
    std::vector<std::pair<std::string, uint16_t>> &new_nodes)
{
    // Make a copy of the current ng configs.
    std::map<uint32_t, std::vector<NodeConfig>> new_ng_configs;
    for (auto &pair : ng_configs_)
    {
        std::vector<NodeConfig> members;
        for (auto &node : pair.second)
        {
            members.push_back(node);
        }
        new_ng_configs.try_emplace(pair.first, std::move(members));
    }

    uint32_t rep_group_cnt =
        fault::CcNode::rep_group_cnt < new_nodes.size() + ng_configs_.size()
            ? fault::CcNode::rep_group_cnt
            : new_nodes.size() + ng_configs_.size();
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
    std::map<uint32_t, int> node_ng_count;
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

std::map<uint32_t, std::vector<NodeConfig>> Sharder::RemoveNodeFromCluster(
    uint16_t removed_node_count,
    std::vector<std::pair<std::string, uint16_t>> &removed_nodes)
{
    // Make a copy of the current ng configs.
    std::map<uint32_t, std::vector<NodeConfig>> new_ng_configs;
    for (auto &pair : ng_configs_)
    {
        std::vector<NodeConfig> members;
        for (auto &node : pair.second)
        {
            members.push_back(node);
        }
        new_ng_configs.try_emplace(pair.first, std::move(members));
    }

    uint32_t rep_group_cnt =
        fault::CcNode::rep_group_cnt < ng_configs_.size() - removed_node_count
            ? fault::CcNode::rep_group_cnt
            : ng_configs_.size() - removed_node_count;
    assert(rep_group_cnt > 0);
    // Remove the nodes with greatest node id.
    NodeGroupId largest_node_id = new_ng_configs.size() - 1;
    for (int i = 0; i < removed_node_count; i++)
    {
        // Remove the node groups where these nodes are preferred leader.
        removed_nodes.emplace_back(
            new_ng_configs[largest_node_id].front().host_name_,
            new_ng_configs[largest_node_id].front().port_);
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
    std::map<uint32_t, int> node_ng_count;
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
}  // namespace txservice
