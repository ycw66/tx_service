#include "sharder.h"

#include <atomic>
#include <memory>
#include <mutex>
#include <shared_mutex>

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
Sharder::~Sharder()
{
    Shutdown();
}

void Sharder::Shutdown()
{
    LOG(INFO) << "Shutting down the sharder at node #" << node_id_;

    if (cc_stream_receiver_)
    {
        cc_stream_receiver_->Shutdown();
    }
    cc_stream_server_.Stop(0);
    cc_stream_server_.Join();
    cc_stream_receiver_ = nullptr;

    if (log_replay_service_)
    {
        log_replay_service_->Shutdown();
    }
    log_replay_server_.Stop(0);
    log_replay_server_.Join();

    cc_node_server_.Stop(0);
    cc_node_server_.Join();
    cc_node_service_ = nullptr;

    // CcNode will access log_replay_service_ to replay log when becoming node
    // group leader, so log_replay_service_ should be destructed after all
    // CcNodes are stopped.
    log_replay_service_ = nullptr;

    if (tx_worker_pool_)
    {
        tx_worker_pool_->Shutdown();
        tx_worker_pool_ = nullptr;
    }
    if (sharder_worker_)
    {
        sharder_worker_->Shutdown();
        sharder_worker_ = nullptr;
    }

    LOG(INFO) << "The sharder at node #" << node_id_ << " shut down.";
}

void Sharder::CloseStreamSender()
{
    LOG(INFO) << "Close Stream sender at node #" << node_id_;
    cc_stream_sender_ = nullptr;
}

void Sharder::GetNodeAddress(uint32_t node_id, std::string &ip, uint16_t &port)
{
    std::shared_lock<std::shared_mutex> cnf_lk(cluster_cnf_mux_);
    if (node_id >= cluster_config_.ng_configs_.size())
    {
        // Node is already removed from cluster
        ip = "";
        port = 0;
        return;
    }

    ip = cluster_config_.ng_configs_.at(node_id).front().host_name_;
    port = cluster_config_.ng_configs_.at(node_id).front().port_;
}

int Sharder::Init(
    uint32_t node_id,
    const std::unordered_map<uint32_t, std::vector<NodeConfig>> *ng_configs,
    uint64_t config_version,
    const std::vector<std::string> *txlog_ips,
    const std::vector<uint16_t> *txlog_ports,
    const std::string *hm_ip,
    const uint16_t *hm_port,
    const std::string *hm_bin_path,
    LocalCcShards *local_shards,
    std::unique_ptr<TxLog> log_agent,
    const std::string &local_path,
    const uint16_t rep_group_cnt)
{
    node_id_ = node_id;
    local_shards_ = local_shards;
    rep_group_cnt_ = rep_group_cnt;
    log_agent_ = std::move(log_agent);

    {
        std::lock_guard<std::shared_mutex> lk(cluster_cnf_mux_);
        for (uint32_t nid = 0; nid < 1000; nid++)
        {
            ng_leader_cache_[nid].store(nid);
            leader_term_cache_[nid].store(-1);
            candidate_leader_term_cache_[nid].store(-1);
            invalid_leader_term_cache_[nid].store(-1);
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
                cluster_config_.ng_configs_.try_emplace(
                    pair.first, std::move(group_config));
            }
            cluster_config_.version_ = config_version;
        }
        else
        {
            cluster_config_.ng_configs_.try_emplace(0);
            cluster_config_.version_ = config_version;
        }

        if (txlog_ips != nullptr)
        {
            txlog_ips_ = *txlog_ips;
            txlog_ports_ = *txlog_ports;

            for (uint16_t port : txlog_ports_)
            {
                LOG(INFO) << "txlog_port = " << port;
            }
        }

        if (log_agent_ != nullptr)
        {
            log_agent_->Init(txlog_ips_, txlog_ports_, 0);
        }

#ifdef EXT_TX_PROC_ENABLED
        tx_worker_pool_ = std::make_unique<TxWorkerPool>(
            local_shards_->Count() >= 2 ? local_shards_->Count() / 2 : 1);
#else
        tx_worker_pool_ =
            std::make_unique<TxWorkerPool>(local_shards_->Count());
#endif
        sharder_worker_ = std::make_unique<TxWorkerPool>(1);
        log_replay_service_ = std::make_unique<fault::ReplayService>(
            *local_shards_,
            GetLogAgent(),
            cluster_config_.ng_configs_.at(node_id_).front().host_name_,
            GET_LOG_REPLAY_RPC_PORT(
                cluster_config_.ng_configs_.at(node_id_).front().port_));
        if (log_replay_server_.AddService(log_replay_service_.get(),
                                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
        {
            LOG(FATAL)
                << "Failed to start add the log replay service to the log "
                   "replay server.";
            return -1;
        }

        for (uint32_t ng_id = 0; ng_id < cluster_config_.ng_configs_.size();
             ++ng_id)
        {
            for (size_t idx = 0;
                 idx < cluster_config_.ng_configs_.at(ng_id).size();
                 ++idx)
            {
                if (cluster_config_.ng_configs_.at(ng_id).at(idx).node_id_ ==
                    node_id_)
                {
                    cluster_config_.cc_nodes_.try_emplace(
                        ng_id,
                        std::make_shared<fault::CcNode>(
                            ng_id,
                            node_id_,
                            *local_shards_,
                            log_agent_ != nullptr ? log_agent_->LogGroupCount()
                                                  : 0));
                }
            }
        }
    }

    std::shared_lock<std::shared_mutex> lk(cluster_cnf_mux_);
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
            << "Failed to add the cc stream service to the cc stream server.";
        return -1;
    }

    cc_stream_sender_ = std::make_unique<remote::CcStreamSender>(msg_pool_);
    cc_stream_sender_->UpdateRemoteNodes(cluster_config_.ng_configs_);

    cc_node_service_ = std::make_unique<remote::CcNodeService>(*local_shards_);
    if (cc_node_server_.AddService(cc_node_service_.get(),
                                   brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
    {
        LOG(FATAL) << "Failed to add the cc node service to the server.";
        return -1;
    }

    // Start the cc_stream_server_ after TxProcessor start using interface
    // StartCcStreamReceiver().

    brpc::ServerOptions server_options;
    // server_options.num_threads 0 means use default bthread worker count.
    server_options.num_threads = 0;
    if (cc_node_server_.Start(
            GET_CCNODE_RPC_PORT(
                cluster_config_.ng_configs_.at(node_id_).front().port_),
            &server_options) != 0)
    {
        LOG(FATAL) << "Failed to start the cc node server.";
        return -1;
    }

    // The log replay server uses local_port+3 for receiving streams from log
    // groups.

    if (log_replay_server_.Start(
            GET_LOG_REPLAY_RPC_PORT(
                cluster_config_.ng_configs_.at(node_id_).front().port_),
            &server_options) != 0)
    {
        LOG(FATAL) << "Failed to start the log replay server.";
        return -1;
    }

    // Notify host manager that this node has been started
    if (hm_ip && !hm_ip->empty())
    {
        hm_ip_ = *hm_ip;
        hm_port_ = *hm_port;
#ifdef FORK_HM_PROCESS
        // Fork host manager process.

        hm_ip_ = "0.0.0.0";
        assert(hm_bin_path != nullptr);
        LOG(INFO) << "Forking host manager process with " << *hm_bin_path
                  << ", the hm will be listening on " << hm_ip_ << ":"
                  << hm_port_;
        int pid = fork();
        if (pid == -1)
        {
            LOG(FATAL) << "Failed to fork host manager process";
            return -1;
        }
        if (pid == 0)
        {
            std::string log_path = local_path + "/cc_ng";
            if (execl(hm_bin_path->c_str(),
                      "host_manager",
                      hm_ip_.c_str(),
                      std::to_string(hm_port_).c_str(),
                      log_path.c_str(),
                      (char *) 0) == -1)
            {
                LOG(ERROR) << "Failed to start host manager process, errno: "
                           << errno;
            }
            // Should not reach here if exec succeeds.
            std::exit(0);
        }
#endif
        int max_retries = 300;
        int retries = 0;
        int delay_ms = 200;
        bool connected = false;
        while (retries < max_retries && !connected)
        {
            if (0 == hm_channel_.Init(hm_ip_.c_str(), hm_port_, nullptr))
            {
                connected = true;
            }
            else
            {
                LOG(WARNING)
                    << "Failed to init channel to host manager. Retrying in "
                    << delay_ms << " ms... (Attempt " << (retries + 1) << " of "
                    << max_retries << ")";
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(delay_ms));
                ++retries;
            }
        }
        if (!connected)
        {
            LOG(FATAL) << "Failed to init channel to host manager after "
                       << max_retries << " attempts.";
            return -1;
        }

        remote::HostMangerService_Stub stub(&hm_channel_);
        brpc::Controller cntl;
        cntl.set_timeout_ms(1000);
        remote::StartNodeRequest req;
        remote::StartNodeResponse response;
        req.set_node_id(node_id_);
        req.set_config_version(config_version);
        if (log_agent_)
        {
            req.set_log_replica_num(log_agent_->LogGroupReplicaNum());
            assert(txlog_ips && txlog_ports);
            for (auto &ip : *txlog_ips)
            {
                req.add_log_ips(ip);
            }
            for (auto port : *txlog_ports)
            {
                req.add_log_ports(port);
            }
        }
        else
        {
            req.clear_log_replica_num();
            req.clear_log_ips();
            req.clear_log_ports();
        }
        for (const auto &ng_config : cluster_config_.ng_configs_)
        {
            auto node_buf = req.add_node_configs();
            auto &node_config = ng_config.second.front();
            node_buf->set_node_id(node_config.node_id_);
            node_buf->set_host_name(node_config.host_name_);
            node_buf->set_port(GET_CCNODE_RPC_PORT(node_config.port_));
            auto ng_buf = req.add_cluster_config();
            ng_buf->set_ng_id(ng_config.first);
            for (auto &member : ng_config.second)
            {
                ng_buf->add_member_nodes(member.node_id_);
            }
        }
        cntl.set_timeout_ms(500);
        stub.StartNode(&cntl, &req, &response, nullptr);
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        for (int retry = 120; retry > 0 && cntl.Failed(); --retry)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            cntl.Reset();
            cntl.set_timeout_ms(500);
            stub.StartNode(&cntl, &req, &response, nullptr);
        }
        if (cntl.Failed())
        {
            LOG(ERROR) << "Failed to notify host manager on node start"
                       << ". Error code: " << cntl.ErrorCode()
                       << ". Msg: " << cntl.ErrorText();
            return -1;
        }
        if (response.error())
        {
            LOG(ERROR) << "Failed to notify host manager on node start.";
            return -1;
        }
    }
    else
    {
        cluster_config_.cc_nodes_.at(node_id_)->OnLeaderStart(1);
    }

    return 0;
}

// Used on HashPartition
uint16_t Sharder::ShardBucketIdToCoreIdx(uint16_t bucket_id)
{
    return (bucket_id & 0x3FF) % local_shards_->Count();
}

std::shared_ptr<brpc::Channel> Sharder::GetCcNodeServiceChannel(
    uint32_t node_id)
{
    std::shared_lock<std::shared_mutex> lk(node_channel_mux_);
    auto channel_it = cc_node_service_channels_.find(node_id);
    if (channel_it == cc_node_service_channels_.end())
    {
        // If channel to this node is not initialized yet, try to construct the
        // channel to this node.
        lk.unlock();
        std::unique_lock<std::shared_mutex> unique_lk(node_channel_mux_);
        std::string ip;
        uint16_t port;
        GetNodeAddress(node_id, ip, port);
        if (ip.empty())
        {
            // Invalid node id
            return nullptr;
        }
        channel_it = cc_node_service_channels_.find(node_id);
        if (channel_it == cc_node_service_channels_.end() ||
            channel_it->second == nullptr)
        {
            auto channel = std::make_shared<brpc::Channel>();
            if (channel->Init(ip.c_str(), GET_CCNODE_RPC_PORT(port), NULL) != 0)
            {
                LOG(ERROR) << "Failed to init the cc node service channel.";
                return nullptr;
            }
            if (channel_it == cc_node_service_channels_.end())
            {
                cc_node_service_channels_.try_emplace(node_id, channel);
            }
            else
            {
                channel_it->second = channel;
            }
            return channel;
        }
        return channel_it->second;
    }

    return channel_it->second;
}

std::shared_ptr<brpc::Channel> Sharder::UpdateCcNodeServiceChannel(
    uint32_t node_id, std::shared_ptr<brpc::Channel> old_channel)
{
    std::string ip;
    uint16_t port;
    GetNodeAddress(node_id, ip, port);
    assert(!ip.empty());
    std::unique_lock<std::shared_mutex> lk(node_channel_mux_);
    auto channel_it = cc_node_service_channels_.find(node_id);
    if (channel_it == cc_node_service_channels_.end() ||
        channel_it->second == nullptr)
    {
        auto channel = std::make_shared<brpc::Channel>();
        if (channel->Init(ip.c_str(), GET_CCNODE_RPC_PORT(port), NULL) != 0)
        {
            LOG(ERROR) << "Failed to init the cc node service channel.";
            return nullptr;
        }
        if (channel_it == cc_node_service_channels_.end())
        {
            cc_node_service_channels_.try_emplace(node_id, channel);
        }
        else
        {
            channel_it->second = channel;
        }
        return channel;
    }

    if (channel_it->second == old_channel)
    {
        // No one has updated this channel since we read it, update it by
        // ourselves.
        auto channel = std::make_shared<brpc::Channel>();
        if (channel->Init(ip.c_str(), GET_CCNODE_RPC_PORT(port), NULL) != 0)
        {
            LOG(ERROR) << "Failed to update the cc node service channel.";
            return nullptr;
        }
        channel_it->second = channel;
        return channel_it->second;
    }
    else
    {
        // Someone has already updated this channel, return the updated channel
        // directly.
        return channel_it->second;
    }
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
    std::shared_lock<std::shared_mutex> cnf_lk(cluster_cnf_mux_);
    for (const auto &ng_pair : cluster_config_.ng_configs_)
    {
        UpdateLeader(ng_pair.first);
    }
}

void Sharder::UpdateLeader(uint32_t ng_id)
{
    if (!hm_ip_.empty())
    {
        Sharder::Instance().sharder_worker_->SubmitWork(
            [ng_id, this]
            {
                brpc::Controller cntl;
                remote::GetLeaderRequest req;
                req.set_ng_id(ng_id);
                remote::GetLeaderResponse resp;
                remote::HostMangerService_Stub stub(&hm_channel_);
                stub.GetLeader(&cntl, &req, &resp, nullptr);
                if (!cntl.Failed() && !resp.error())
                {
                    UpdateLeader(ng_id, resp.node_id());
                }
            });
    }
}

void Sharder::UpdateLeader(uint32_t ng_id, uint32_t node_id)
{
    DLOG(INFO) << "ccnode group ng" << ng_id
               << " updates leader to node_id:" << node_id;

    ng_leader_cache_[ng_id].store(node_id, std::memory_order_release);
}

void Sharder::FinishLogReplay(uint32_t cc_ng_id,
                              int64_t cc_ng_term,
                              uint32_t log_group_id,
                              uint32_t latest_txn_no,
                              uint64_t last_ckpt_ts)
{
    std::shared_lock<std::shared_mutex> cnf_lk(cluster_cnf_mux_);

    auto find_it = cluster_config_.cc_nodes_.find(cc_ng_id);
    if (find_it == cluster_config_.cc_nodes_.end())
    {
        return;
    }

    find_it->second->FinishLogGroupReplay(
        log_group_id, cc_ng_term, latest_txn_no, last_ckpt_ts);
    local_shards_->UpdateTsBase(last_ckpt_ts);
}

bool Sharder::CheckLogGroupReplayFinished(uint32_t cc_ng_id,
                                          uint32_t log_group_id,
                                          int64_t cc_ng_term)
{
    std::shared_lock<std::shared_mutex> cnf_lk(cluster_cnf_mux_);

    auto find_it = cluster_config_.cc_nodes_.find(cc_ng_id);
    if (find_it == cluster_config_.cc_nodes_.end())
    {
        return false;
    }

    return find_it->second->CheckLogGroupReplayFinished(log_group_id,
                                                        cc_ng_term);
}

void Sharder::WaitClusterReady()
{
    bool recovery_all_finished = false;
    do
    {
        std::unique_lock<std::mutex> lk(recovery_state_mux_);
        // cluster_config_ might be updated during replay. We need
        // to obtain the latest cluster_config_ before checking in every loop.
        std::shared_lock<std::shared_mutex> cnf_lk(cluster_cnf_mux_);
        for (auto &pair : cluster_config_.ng_configs_)
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
        size_t ng_cnt = cluster_config_.ng_configs_.size();
        cnf_lk.unlock();

        recovery_all_finished = recovery_state_cv_.wait_for(
            lk,
            1s,
            [this, ng_cnt]()
            { return recovered_leader_set_.size() == ng_cnt; });
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

void Sharder::OnLeaderStart(uint32_t ng_id, int64_t term)
{
    std::shared_ptr<fault::CcNode> node;
    {
        std::shared_lock<std::shared_mutex> lk(cluster_cnf_mux_);
        auto find_it = cluster_config_.cc_nodes_.find(ng_id);
        // TODO: is this always true when cluster config is changed?
        assert(find_it != cluster_config_.cc_nodes_.end());
        node = find_it->second;
    }

    return node->OnLeaderStart(term);
}

void Sharder::OnLeaderStop(uint32_t ng_id)
{
    std::shared_ptr<fault::CcNode> node;
    {
        std::shared_lock<std::shared_mutex> lk(cluster_cnf_mux_);
        auto find_it = cluster_config_.cc_nodes_.find(ng_id);
        // TODO: is this always true when cluster config is changed?
        assert(find_it != cluster_config_.cc_nodes_.end());
        node = find_it->second;
    }

    return node->OnLeaderStop();
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
    std::shared_lock<std::shared_mutex> lk(cluster_cnf_mux_);
    for (auto &pair : cluster_config_.cc_nodes_)
    {
        ngs.push_back(pair.first);
    }
    return ngs;
}

int64_t Sharder::TryPinNodeGroupData(uint32_t cc_ng_id)
{
    std::shared_lock<std::shared_mutex> lk(cluster_cnf_mux_);

    auto find_it = cluster_config_.cc_nodes_.find(cc_ng_id);
    if (find_it != cluster_config_.cc_nodes_.end())
    {
        return find_it->second->PinData();
    }
    return -1;
}

void Sharder::UnpinNodeGroupData(uint32_t cc_ng_id)
{
    std::shared_lock<std::shared_mutex> lk(cluster_cnf_mux_);

    auto find_it = cluster_config_.cc_nodes_.find(cc_ng_id);
    if (find_it != cluster_config_.cc_nodes_.end())
    {
        find_it->second->UnpinData();
    }
}

uint64_t Sharder::GetNodeGroupCkptTs(uint32_t cc_ng_id)
{
    std::shared_lock<std::shared_mutex> lk(cluster_cnf_mux_);

    auto find_it = cluster_config_.cc_nodes_.find(cc_ng_id);
    if (find_it != cluster_config_.cc_nodes_.end())
    {
        return find_it->second->GetCkptTs();
    }
    return 0;
}

bool Sharder::UpdateNodeGroupCkptTs(uint32_t cc_ng_id, uint64_t ckpt_ts)
{
    std::shared_lock<std::shared_mutex> lk(cluster_cnf_mux_);

    auto find_it = cluster_config_.cc_nodes_.find(cc_ng_id);
    if (find_it != cluster_config_.cc_nodes_.end())
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
    std::shared_lock<std::shared_mutex> lk(cluster_cnf_mux_);
    // Make a copy of the current ng configs.
    std::unordered_map<uint32_t, std::vector<NodeConfig>> new_ng_configs(
        cluster_config_.ng_configs_);

    uint32_t rep_group_cnt =
        rep_group_cnt_ < new_nodes.size() + cluster_config_.ng_configs_.size()
            ? rep_group_cnt_
            : new_nodes.size() + cluster_config_.ng_configs_.size();
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
    std::shared_lock<std::shared_mutex> lk(cluster_cnf_mux_);
    // Make a copy of the current ng configs.
    std::unordered_map<uint32_t, std::vector<NodeConfig>> new_ng_configs(
        cluster_config_.ng_configs_);

    uint32_t rep_group_cnt =
        rep_group_cnt_ < cluster_config_.ng_configs_.size() - removed_node_count
            ? rep_group_cnt_
            : cluster_config_.ng_configs_.size() - removed_node_count;
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
    if (ClusterConfigVersion() >= version)
    {
        // If the given version is older than current version, do
        // nothing.
        cc_shard->Enqueue(cc_req);
        return;
    }

    // Since sharder worker is a single thread worker, we don't need to worry
    // about cluster config being updated by multiple threads at the same time.
    sharder_worker_->SubmitWork(
        [this, &new_ng_configs, cc_req, cc_shard, version]
        {
            remote::HostMangerService_Stub stub(&hm_channel_);
            brpc::Controller cntl;
            remote::UpdateNodeGroupConfigRequest req;
            remote::UpdateNodeGroupConfigResponse resp;
            for (auto &ng_pair : new_ng_configs)
            {
                auto node_buf = req.add_new_node_configs();
                node_buf->set_node_id(ng_pair.first);
                node_buf->set_host_name(ng_pair.second.front().host_name_);
                node_buf->set_port(
                    GET_CCNODE_RPC_PORT(ng_pair.second.front().port_));

                auto ng_buf = req.add_new_cluster_config();
                ng_buf->set_ng_id(ng_pair.first);
                for (auto &node : ng_pair.second)
                {
                    ng_buf->add_member_nodes(node.node_id_);
                }
            }
            auto last_term = LeaderTerm(node_id_);
            req.set_ng_id(node_id_);
            req.set_config_version(version);
            cntl.set_timeout_ms(10000);
            stub.UpdateNodeGroupConfigs(&cntl, &req, &resp, nullptr);
            if (cntl.Failed())
            {
                LOG(ERROR) << "RPC to host manager to update node group "
                              "configs failed "
                           << cntl.ErrorText();
                cc_shard->Enqueue(cc_req);
                return;
            }
            if (resp.error())
            {
                LOG(ERROR)
                    << "Failed to update node group configs on host manager";
                cc_shard->Enqueue(cc_req);
                return;
            }

            {
                std::unique_lock<std::shared_mutex> lk(cluster_cnf_mux_);
                bool truncate_log = false;
                // First remove node groups that are removed from the cluster.
                size_t cur_ngs = cluster_config_.ng_configs_.size();
                for (size_t deleted_ng = new_ng_configs.size();
                     deleted_ng < cur_ngs;
                     deleted_ng++)
                {
                    if (deleted_ng == node_id_)
                    {
                        // Truncate previous log if this ng is also removed.
                        truncate_log = true;
                    }
                    cluster_config_.ng_configs_.erase(deleted_ng);
                    cluster_config_.ng_configs_.erase(deleted_ng);
                }

                if (truncate_log)
                {
                    auto [last_ckpt_ts, mem_usage] =
                        Sharder::Instance()
                            .GetLocalCcShards()
                            ->GetTxService()
                            ->ckpt_.GetNewCheckpointTs(node_id_, true);
                    log_agent_->UpdateCheckpointTs(
                        node_id_, last_term, last_ckpt_ts);
                }
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
                    if (is_member)
                    {
                        // If this node is a member of a new node group, create
                        // cc node for it.
                        auto cc_node_it =
                            cluster_config_.cc_nodes_.find(ng_pair.first);
                        if (cc_node_it == cluster_config_.cc_nodes_.end())
                        {
                            cluster_config_.cc_nodes_.try_emplace(
                                ng_pair.first,
                                std::make_shared<fault::CcNode>(
                                    ng_pair.first,
                                    node_id_,
                                    *local_shards_,
                                    log_agent_->LogGroupCount()));
                        }
                    }

                    // Update ng config in cluster config
                    auto ng_cnf_it =
                        cluster_config_.ng_configs_.try_emplace(ng_pair.first);
                    ng_cnf_it.first->second = ng_pair.second;
                }
                cluster_config_.version_ = version;
            }

            cc_stream_sender_->UpdateRemoteNodes(cluster_config_.ng_configs_);

            cc_shard->Enqueue(cc_req);
        });
}

void Sharder::StartCcStreamReceiver()
{
    // The cc_stream_receiver_ object has been add to this server during
    // Sharder::Init().
    brpc::ServerOptions server_options;
    // server_options.num_threads 0 means use default bthread worker count.
    server_options.num_threads = 0;
    if (cc_stream_server_.Start(
            cluster_config_.ng_configs_.at(node_id_).front().port_,
            &server_options) != 0)
    {
        LOG(FATAL) << "Failed to start the cc stream server.";
    }
}
}  // namespace txservice
