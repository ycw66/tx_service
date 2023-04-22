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
                 const std::vector<std::string> *ips,
                 const std::vector<uint16_t> *ports,
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
    if (ips != nullptr)
    {
        ips_.reserve(ips->size());
        ports_.reserve(ips->size());

        for (uint32_t ng_id = 0; ng_id < ips->size(); ++ng_id)
        {
            ips_.emplace_back(ips->at(ng_id));
            ports_.emplace_back(ports->at(ng_id));
            ng_leader_cache_.try_emplace(ng_id, ng_id);
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

    if (ips_.size() > 1)
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
    assert(node_id < ips_.size());

    ip = ips_.at(node_id);
    port = ports_.at(node_id);
}

int Sharder::Init(const std::string &path)
{
    // construct log_replay_service_ before cc_nodes_
    log_replay_service_ = std::make_unique<fault::ReplayService>(
        local_shards_,
        GetLogAgent(),
        ips_.at(node_id_),
        GET_LOG_REPLAY_RPC_PORT(ports_.at(node_id_)));
    if (log_replay_server_.AddService(log_replay_service_.get(),
                                      brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
    {
        LOG(FATAL) << "Fail to start add the log replay service to the log "
                      "replay server.";
        return -1;
    }

    uint32_t rep_group_cnt = fault::CcNode::rep_group_cnt < ips_.size()
                                 ? fault::CcNode::rep_group_cnt
                                 : ips_.size();

    cc_nodes_.reserve(rep_group_cnt);
    uint32_t cc_group_id = node_id_;

    // generate #rep_group_cnt number of cc_node
    for (size_t offset = 0; offset < rep_group_cnt; ++offset)
    {
        std::vector<std::string> group_ips;
        group_ips.reserve(rep_group_cnt);
        std::vector<uint16_t> group_ports;
        group_ports.reserve(rep_group_cnt);

        // determine the members in this ccnode's raft group.
        // for example, suppose 3 nodes, the raft configuration is as
        // follows: group1: 1(preferred leader),2,3 group2: 1,2(preferred
        // leader),3 group3: 1,2,3(preferred leader)
        for (uint32_t idx = 0; idx < rep_group_cnt; ++idx)
        {
            uint32_t gid = cc_group_id + idx;

            if (gid >= ips_.size())
            {
                gid -= ips_.size();
            }

            group_ips.emplace_back(ips_.at(gid));
            // TODO: use delta or a separate conf parameter
            // mapcc port plus 1 as ccnode raft port.
            group_ports.emplace_back(ports_.at(gid) + 1);
        }

        std::string store_path(path);
        store_path.append("/cc_ng/");
        store_path.append(std::to_string(cc_group_id));

        cc_nodes_.try_emplace(
            cc_group_id,
            std::make_unique<fault::CcNode>(cc_group_id,
                                            node_id_,
                                            ips_.at(node_id_),
                                            ports_.at(node_id_) + 1,
                                            group_ips,
                                            group_ports,
                                            store_path,
                                            local_shards_,
                                            log_replay_service_.get(),
                                            log_agent_->LogGroupCount()));

        // cc_nodes contains all the raft groups in which the current
        // node(node_id) exists. As a result, scan back to search the
        // group where the current node is a follower. Each node can
        // exist in #rep_group_cnt number of groups.
        if (cc_group_id == 0)
        {
            cc_group_id = ips_.size() - 1;
        }
        else
        {
            --cc_group_id;
        }
    }
    cc_nodes_init_.store(true, std::memory_order_release);

    if (ips_.size() > 1)
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

        if (cc_stream_server_.Start(ports_.at(node_id_), NULL) != 0)
        {
            LOG(FATAL) << "Fail to start the cc stream server.";
            return -1;
        }

        cc_stream_sender_ = std::make_unique<remote::CcStreamSender>(msg_pool_);
        for (uint32_t nid = 0; nid < ips_.size(); ++nid)
        {
            // Build a stream to every node even to ourselves because the
            // current node can become leader of multiple node groups during
            // failover. In that case we might need to handle remote requests
            // sent from the same node but from different node group.
            cc_stream_sender_->AddRemoteNode(nid, ips_.at(nid), ports_.at(nid));
        }
    }

    // Initializes the Raft service that listens on the port of local_port + 1.
    if (braft::add_service(&cc_node_server_,
                           GET_CCNODE_RPC_PORT(ports_.at(node_id_))) != 0)
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

    if (cc_node_server_.Start(GET_CCNODE_RPC_PORT(ports_.at(node_id_)), NULL) !=
        0)
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
    if (log_replay_server_.Start(GET_LOG_REPLAY_RPC_PORT(ports_.at(node_id_)),
                                 nullptr) != 0)
    {
        LOG(FATAL) << "Fail to start the log replay server.";
        return -1;
    }

    tx_worker_pool_ = std::make_unique<TxWorkerPool>();

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

    uint32_t rep_group_cnt = fault::CcNode::rep_group_cnt < ips_.size()
                                 ? fault::CcNode::rep_group_cnt
                                 : ips_.size();
    uint32_t nid = ng_id;
    for (size_t idx = 0; idx < rep_group_cnt; ++idx)
    {
        if (ips_.at(nid) == leader_ip_str &&
            GET_CCNODE_RPC_PORT(ports_.at(nid)) == leader_port)
        {
            ng_leader_cache_.at(ng_id).store(nid, std::memory_order_release);
            break;
        }

        // scan forward since the members are [ng_id, ng_id+rep_group_cnt-1]
        // for group ng_id.
        if (nid == ips_.size() - 1)
        {
            nid = 0;
        }
        else
        {
            ++nid;
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
        for (uint32_t ng_id = 0; ng_id < ips_.size(); ng_id++)
        {
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
    uint32_t rep_group_cnt = fault::CcNode::rep_group_cnt < ips_.size()
                                 ? fault::CcNode::rep_group_cnt
                                 : ips_.size();

    for (uint32_t ng_id = 0; ng_id < ips_.size(); ++ng_id)
    {
        std::string group_id("ng");
        group_id.append(std::to_string(ng_id));

        std::string group_conf;
        for (uint32_t idx = 0; idx < rep_group_cnt; ++idx)
        {
            if (idx > 0)
            {
                group_conf.append(",");
            }

            uint32_t nidx = ng_id + idx;
            if (nidx >= ips_.size())
            {
                nidx -= ips_.size();
            }

            group_conf.append(ips_.at(nidx));
            group_conf.append(":");
            group_conf.append(std::to_string(ports_.at(nidx) + 1));
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
}  // namespace txservice
