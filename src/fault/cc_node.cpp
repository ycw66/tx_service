#include "fault/cc_node.h"

#include "local_cc_shards.h"
#include "sharder.h"

namespace txservice::fault
{
CcNode::CcNode(const uint32_t ng_id,
               const uint32_t node_id,
               const std::string &ip,
               const uint16_t port,
               const std::vector<std::string> &ng_ips,
               const std::vector<uint16_t> &ng_ports,
               std::string storage_path,
               LocalCcShards &local_shards,
               fault::ReplayService *replay_service,
               uint32_t log_group_cnt)
    : ng_id_(ng_id),
      node_id_(node_id),
      ip_(ip),
      port_(port),
      ng_ips_(ng_ips),
      ng_ports_(ng_ports),
      storage_path_(storage_path),
      leader_term_(-1),
      candidate_leader_term_(-1),
      last_ckpt_ts_(0),
      pinning_threads_(0),
      local_cc_shards_(local_shards),
      replay_service_(replay_service),
      log_group_cnt_(log_group_cnt)
{
    // FIXME: in which case the node_idx_ is not zero? should be failover
    size_t nid = 0;
    for (; nid < ng_ips_.size(); ++nid)
    {
        if (ng_ips_.at(nid) == ip_ && ng_ports_.at(nid) == port_)
        {
            node_idx_ = nid;
            break;
        }
    }

    if (nid >= ng_ips_.size())
    {
        LOG(ERROR) << "Failed to initialize the CC node, whose IP is not "
                      "included in the node group.";
    }
}

int CcNode::Start()
{
    braft::NodeOptions node_options = BaseNodeOptions();
    butil::EndPoint addr;
    butil::str2endpoint(ip_.c_str(), port_, &addr);

    std::string raft_conf;
    for (size_t nid = 0; nid < ng_ips_.size(); ++nid)
    {
        if (nid > 0)
        {
            raft_conf.append(",");
        }

        raft_conf.append(ng_ips_.at(nid));
        raft_conf.append(":");
        raft_conf.append(std::to_string(ng_ports_.at(nid)));
        raft_conf.append(":");
        raft_conf.append(std::to_string(nid));
    }

    if (node_options.initial_conf.parse_from(raft_conf) != 0)
    {
        LOG(ERROR) << "Fail to parse configuration " << raft_conf;
        return -1;
    }
    node_options.fsm = this;
    node_options.log_uri = storage_path_ + "/log";
    node_options.raft_meta_uri = storage_path_ + "/meta";
    node_options.snapshot_uri = storage_path_ + "/snapshot";

    // preferred leader has lower election timeout.
    if (node_idx_ == 0)
    {
        node_options.election_timeout_ms = 1000;
    }
    else
    {
        node_options.election_timeout_ms = 5000;
    }

    std::string group_id("ng");
    group_id.append(std::to_string(ng_id_));

    braft::Node *node =
        new braft::Node(group_id, braft::PeerId(addr, node_idx_));

    if (node->init(node_options) != 0)
    {
        LOG(ERROR) << "Fail to init raft node";
        delete node;
        return -1;
    }
    node_ = node;

    /*if (node_idx_ == 0)
    {
        node_->vote(0);
    }*/

    return 0;
}

CcNode::~CcNode()
{
    delete node_;
}

// Shut this node down.
void CcNode::Shutdown()
{
    if (node_)
    {
        node_->shutdown(NULL);
    }
}

// Blocking this thread until the node is eventually down.
void CcNode::Join()
{
    if (node_)
    {
        node_->join();
    }
}

int CcNode::TransferLeader()
{
    // By default, the first node in a cc node group is expected to be the
    // leader, so that leaders of all cc node groups are evenly distributed
    // among physical nodes for maximal usage of resources. If the current
    // node is the leader and yet is not the first in the group (either
    // because the old leader fails over to this node or because on group
    // start this node happens to be elected as the leader), tries to
    // transfer the leadership to the first node for rebalance.

    if (node_idx_ > 0 && node_->is_leader())
    {
        butil::EndPoint addr;
        // ng_ips[0]/ng_ports[0] stores the addr of the preferred leader.
        butil::str2endpoint(ng_ips_.at(0).c_str(), ng_ports_.at(0), &addr);
        braft::PeerId first_peer(addr, 0);

        int err = node_->transfer_leadership_to(first_peer);

        size_t retry = 3;
        while (retry > 0 && err != 0)
        {
            using namespace std::chrono_literals;
            std::this_thread::sleep_for(1s);
            err = node_->transfer_leadership_to(first_peer);
            --retry;
        }

        return err;
    }

    return -1;
}

void CcNode::FinishLogGroupReplay(uint32_t log_group_id,
                                  int64_t ng_term,
                                  uint32_t latest_committed_txn_no,
                                  uint64_t last_ckpt_ts)
{
    // recovery_mux_ is used to protect recovered_log_groups_, since raft
    // service thread will also modify it concurrently.
    std::lock_guard<std::mutex> lk(recovery_mux_);

    // ignore the FinishReplayMsg whose ng_term is smaller than the current
    // candidate_leader_term_ or if this cc node is not recovering.
    int64_t candidate_term =
        candidate_leader_term_.load(std::memory_order_acquire);
    if (candidate_term < 0 || candidate_term > ng_term)
    {
        return;
    }

    // set CcNode's last_ckpt_ts to the greatest last_ckpt_ts received from all
    // log groups
    UpdateCkptTs(last_ckpt_ts);

    // native cc node finishes log replay from its bound log group, set
    // starting txn numbers of local cc shards. Since only native cc_node can
    // start transaction.
    // each ccshard reads tx_ident_ in NewTx(), which is concurrent with this
    // write, native cc node's leader_term_ synchronizes them.
    // Since cc_node is not bound to specific log node group, finally the
    // cc_shard will be setup with the MAX last_committed_txn_no from all log
    // groups
    if (node_idx_ == 0)
    {
        local_cc_shards_.SetTxIdent(latest_committed_txn_no);
    }

    auto lg_it = recovered_log_groups_.emplace(log_group_id);
    if (lg_it.second && recovered_log_groups_.size() == log_group_cnt_)
    {
        // reset the recovered_log_groups_ since we have finished the log replay
        // work for the current term.
        recovered_log_groups_.clear();

        leader_term_.store(
            candidate_leader_term_.load(std::memory_order_acquire),
            std::memory_order_release);
        LOG(INFO) << "The leader of cc node group ng#" << ng_id_
                  << " with the term " << candidate_leader_term_
                  << " has been recovered.";
        candidate_leader_term_.store(-1, std::memory_order_release);
    }
}

int64_t CcNode::PinData()
{
    std::unique_lock lk(pinning_threads_mux_);
    int64_t leader_term = leader_term_.load(std::memory_order_acquire);
    if (leader_term > 0)
    {
        pinning_threads_++;
    }
    else
    {
        leader_term = candidate_leader_term_.load(std::memory_order_acquire);
        if (leader_term > 0)
        {
            pinning_threads_++;
        }
    }
    return leader_term;
}

void CcNode::UnpinData()
{
    std::unique_lock lk(pinning_threads_mux_);
    if (pinning_threads_ > 0)
    {
        pinning_threads_--;
        // wake up braft thread in case it is waiting in on_leader_stop()
        pinning_threads_cv_.notify_one();
    }
}

bool CcNode::UpdateCkptTs(uint64_t new_ckpt_ts)
{
    uint64_t expected_old_value = last_ckpt_ts_.load(std::memory_order_relaxed);
    // update last_ckpt_ts_ only if new_ckpt_ts is bigger
    bool success = false;
    while (new_ckpt_ts > expected_old_value && !success)
    {
        // if CAS fails, expected_old_value will be set to the actual value of
        // last_ckpt_ts_
        success = last_ckpt_ts_.compare_exchange_weak(
            expected_old_value, new_ckpt_ts, std::memory_order_relaxed);
    }
    return success;
}

/**
 * @brief Notify all the nodes that the node_id of the new leader in node
 * group leader_ng_id, and request these nodes to update their leader cache.
 *
 * @param leader_ng_id
 * @param leader_node_id
 */
void CcNode::NotifyNewLeaderStart(uint32_t leader_ng_id,
                                  uint32_t leader_node_id)
{
    uint32_t node_id;
    std::string node_ip;
    uint16_t node_port;

    uint32_t node_count = Sharder::Instance().GetNodeCount();

    for (node_id = 0; node_id < node_count; node_id++)
    {
        if (node_id == leader_node_id)
        {
            Sharder::Instance().UpdateLeader(leader_ng_id, leader_node_id);
            continue;
        }

        Sharder::Instance().GetNodeAddress(node_id, node_ip, node_port);

        brpc::Channel channel;
        if (channel.Init(
                node_ip.c_str(), GET_CCNODE_RPC_PORT(node_port), nullptr) != 0)
        {
            // Fails to establish the channel to the tx node. Silently
            // returns. The tx will be recovered again by next
            // conflicting tx.
            LOG(ERROR) << "Fail to init the channel to the leader of ng#"
                       << leader_ng_id << " for tx lock recovery.";
            continue;
        }

        remote::CcRpcService_Stub stub(&channel);
        remote::NotifyNewLeaderStartRequest req;
        req.set_ng_id(leader_ng_id);
        req.set_node_id(leader_node_id);
        remote::NotifyNewLeaderStartResponse res;
        res.set_error(false);

        brpc::Controller cntl;
        cntl.set_timeout_ms(100);
        stub.NotifyNewLeaderStart(&cntl, &req, &res, nullptr);

        // Retry is not needed at here, the remote nodes will also refresh their
        // leader caches passively.
        if (cntl.Failed())
        {
            LOG(ERROR) << "Fail the NotifyNewLeaderStart RPC of ng#"
                       << leader_ng_id << ". Error code: " << cntl.ErrorCode()
                       << ". Msg: " << cntl.ErrorText();
        }
        else if (res.error())
        {
            LOG(ERROR) << "Fail to notify the new leader of ng#" << leader_ng_id
                       << " to remote node id:" << node_id;
        }
    }
}

void CcNode::on_leader_start(int64_t term)
{
    {
        // replay thread and leader election thread may update
        // candidate_leader_term_ and recovered_log_groups_ concurrently.
        std::lock_guard<std::mutex> lk(recovery_mux_);

        candidate_leader_term_.store(term, std::memory_order_release);

        // new leader will send ReplayLog request to logservice to replay logs.
        // It should reset the recovered_log_groups_ ahead.
        recovered_log_groups_.clear();
    }

    LOG(INFO) << "CC node " << ip_ << ":" << port_
              << " becomes the leader of ng#" << ng_id_ << ". Term: " << term;

    // TODO{liunyl}: need to update ng config in sharder and connect to peers
    // based on the new read cluster configs.
    if (!local_cc_shards_.IsRangeBucketsInitialized(ng_id_))
    {
        // We need to initialize range bucket info for new ng
        // before replaying.
        std::map<uint32_t, std::vector<NodeConfig>> ng_configs;
        uint64_t version;
        int32_t seed;
        bool uninitialized;
        // read ng config from kv store
        while (!local_cc_shards_.store_hd_->ReadClusterConfig(
            ng_configs, version, seed, uninitialized))
        {
            ng_configs.clear();
            assert(!uninitialized);
        }
        local_cc_shards_.InitRangeBuckets(ng_id_, ng_configs, version, seed);
    }
    replay_service_->ReplayLog(ng_id_, term);

    NotifyNewLeaderStart(ng_id_, node_id_);
}

void CcNode::on_leader_stop(const butil::Status &status)
{
    LOG(INFO) << "CC node " << ip_ << ":" << port_
              << " steps down as the leader of ng#" << ng_id_ << ".";

    leader_term_.store(-1, std::memory_order_release);
    candidate_leader_term_.store(-1, std::memory_order_release);

    // Wait for data unpin then clear all node_group data
    {
        std::unique_lock lk(pinning_threads_mux_);
        pinning_threads_cv_.wait(lk, [this] { return pinning_threads_ == 0; });
    }
    uint16_t core_cnt = local_cc_shards_.Count();
    ClearCcNodeGroup clear_ccm_req(ng_id_, core_cnt);
    for (uint16_t core_id = 0; core_id < core_cnt; ++core_id)
    {
        local_cc_shards_.EnqueueCcRequest(core_id, &clear_ccm_req);
    }
    clear_ccm_req.Wait();
}

void CcNode::on_start_following(const ::braft::LeaderChangeContext &ctx)
{
    LOG(INFO) << "CC node " << ip_ << ":" << port_ << " starts following in ng#"
              << ng_id_ << ", term: " << ctx.term();

    // notify replay service to request leader transfer immediately
    replay_service_->NotifyLeaderTransfer();
}
}  // namespace txservice::fault
