#include "fault/cc_node.h"

#include "sharder.h"

namespace txservice::fault
{
CcNode::CcNode(const uint32_t ng_id,
               const std::string &ip,
               const uint16_t port,
               const std::vector<std::string> &ng_ips,
               const std::vector<uint16_t> &ng_ports,
               std::string storage_path,
               LocalCcShards &local_shards,
               uint32_t log_group_cnt)
    : ng_id_(ng_id),
      ip_(ip),
      port_(port),
      ng_ips_(ng_ips),
      ng_ports_(ng_ports),
      storage_path_(storage_path),
      leader_term_(-1),
      candidate_leader_term_(-1),
      local_cc_shards_(local_shards),
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

        using namespace std::chrono_literals;
        std::this_thread::sleep_for(5s);
        int err = node_->transfer_leadership_to(first_peer);

        size_t retry = 3;
        while (retry > 0 && err != 0)
        {
            std::this_thread::sleep_for(5s);
            err = node_->transfer_leadership_to(first_peer);
            --retry;
        }

        return err;
    }

    return -1;
}

void CcNode::FinishLogGroupReplay(uint32_t log_group_id)
{
    auto lg_it = recovered_log_groups_.emplace(log_group_id);
    if (lg_it.second && recovered_log_groups_.size() == log_group_cnt_)
    {
        leader_term_.store(candidate_leader_term_, std::memory_order_release);
        LOG(INFO) << "The leader of cc node group #" << ng_id_
                  << " with the term " << candidate_leader_term_
                  << " has been recovered.";
    }
}

void CcNode::RecoverTx(uint64_t tx_number,
                       int64_t tx_term,
                       uint32_t cc_ng_id,
                       int64_t cc_ng_term)
{
    log_notify_hd_->RecoverTx(tx_number, tx_term, cc_ng_id, cc_ng_term);
}

void CcNode::on_leader_start(int64_t term)
{
    candidate_leader_term_ = term;

    LOG(INFO) << "CC node " << ip_ << ":" << port_
              << " becomes the leader of ng" << ng_id_ << ". Term: " << term;

    // A log notify handler starts a background thread that notifies all log
    // groups the new leader's term. The cc node becomes the real leader, only
    // after it receives log records from all log groups.
    log_notify_hd_ = std::make_unique<fault::LogNotifier>(
        ng_id_, term, ip_, port_ + 2, local_cc_shards_);
}

void CcNode::on_start_following(const ::braft::LeaderChangeContext &ctx)
{
    LOG(INFO) << "CC node " << ip_ << ":" << port_ << " starts following in ng"
              << ng_id_ << ", term: " << ctx.term();

    recovered_log_groups_.clear();

    leader_term_.store(-1, std::memory_order_release);

    // Interruptes and de-allocates the log notifier if the cc node just became
    // the leader and had not notified all log groups.
    log_notify_hd_ = nullptr;

    // when preferred leader is actually a follower, e.g. caused by a
    // failover, it will send the TransferRequest to the current leader
    // through ccmap service.
    if (node_idx_ == 0)
    {
        braft::PeerId leader_peer = ctx.leader_id();
        // port minus 1 direct to ccmap service.
        leader_peer.addr.port = leader_peer.addr.port - 1;

        brpc::Channel channel;
        if (channel.Init(leader_peer.addr, nullptr) != 0)
        {
            LOG(ERROR) << "Fail to init the channel to the leader of ng"
                       << ng_id_ << " for leadership transfer.";
            return;
        }

        remote::CcService_Stub stub(&channel);

        remote::TransferRequest req;
        req.set_ng_id(ng_id_);
        remote::TransferResponse res;
        res.set_error(false);

        brpc::Controller cntl;
        cntl.set_timeout_ms(-1);
        stub.Transfer(&cntl, &req, &res, nullptr);

        if (cntl.Failed())
        {
            LOG(ERROR) << "Fail the transfer RPC of ng" << ng_id_
                       << ". Error code: " << cntl.ErrorCode()
                       << ". Msg: " << cntl.ErrorText();
        }
        else if (res.error())
        {
            // TODO: consider retry logic.
            LOG(ERROR) << "Fail to transfer the leader of ng" << ng_id_;
        }
    }
}
}  // namespace txservice::fault