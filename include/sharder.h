#pragma once

#include <cstdint>
#include <unordered_map>

#include "braft/route_table.h"
#include "brpc/server.h"
#include "fault/cc_node.h"
#include "fault/log_replay_handler.h"
#include "txlog.h"

namespace txservice
{
class LocalCcShards;

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
            node_id, ips, ports, local_shards, std::move(log_agent));
        return instance_;
    }

    uint32_t NodeId(uint32_t shard_id) const
    {
        return ng_leader_cache_.at(shard_id).load(std::memory_order_acquire);
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

    // Initialize cc_node related to current node_id and start raft serveice.
    int Init(const std::string &path)
    {
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
                                                ips_.at(node_id_),
                                                ports_.at(node_id_) + 1,
                                                group_ips,
                                                group_ports,
                                                store_path,
                                                *local_cc_shards_,
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

        // raft can share the same RPC server. Notice the second parameter,
        // because adding services into a running server is not allowed and the
        // listen address of this server is impossible to get before the server
        // starts. You have to specify the address of the server.
        if (braft::add_service(&brpc_server_, ports_.at(node_id_) + 1) != 0)
        {
            LOG(ERROR) << "Fail to add the raft service for ccm nodes";
            return -1;
        }

        // It's recommended to start the server before Counter is started to
        // avoid the case that it becomes the leader while the service is
        // unreacheable by clients. Notice the default options of server is used
        // here. Check out details from the doc of brpc if you would like change
        // some options;
        if (brpc_server_.Start(ports_.at(node_id_) + 1, NULL) != 0)
        {
            LOG(ERROR) << "Fail to start the brpc service for ccm nodes";
            return -1;
        }

        for (auto rit = cc_nodes_.begin(); rit != cc_nodes_.end(); ++rit)
        {
            rit->second->Start();
        }

        ConfigRouteTable();

        // The cc stream is on local_port.
        // The cc node group is on local_port + 1.
        // The log group is on local_port + 2.
        // The log replay is on local_port + 3.
        // Starts the log replay server that accepts log replay messages when
        // one of the cc nodes in this node is recovering as the leader.
        log_replay_hd_ = std::make_unique<fault::LogReplayHandler>(
            *local_cc_shards_, ports_.at(node_id_) + 3);

        return 0;
    }

    bool CheckLeaderTerm(uint32_t ng_id, int64_t term) const
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

    int64_t LeaderTerm(uint32_t ng_id) const
    {
        if (!replicated_)
        {
            return 0;
        }

        auto find_it = cc_nodes_.find(ng_id);
        if (find_it == cc_nodes_.end())
        {
            return -1;
        }

        const fault::CcNode &cc_node = *find_it->second;
        return cc_node.Term();
    }

    void UpdateLeaders()
    {
        for (const auto &ng_pair : ng_leader_cache_)
        {
            UpdateLeader(ng_pair.first);
        }
    }

    // Update leader when request is directed to a wrong leader.
    void UpdateLeader(uint32_t ng_id)
    {
        std::string node_group_id("ng");
        node_group_id.append(std::to_string(ng_id));

        std::lock_guard<std::mutex> lk(mux_);
        butil::Status st = braft::rtb::refresh_leader(node_group_id, 1000);
        if (!st.ok())
        {
            std::cout << "Fail to refresh leader. " << st.error_str()
                      << std::endl;
            return;
        }

        braft::PeerId leader;
        // Selects the leader of the target group from RouteTable
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
                ports_.at(nid) + 1 == leader_port)
            {
                ng_leader_cache_.at(ng_id).store(nid,
                                                 std::memory_order_release);
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

    // handle leader transfer request
    int TransferLeader(uint32_t ng_id)
    {
        auto ng_it = cc_nodes_.find(ng_id);
        if (ng_it == cc_nodes_.end())
        {
            return 1;
        }

        return ng_it->second->TransferLeader();
    }

    std::unique_ptr<TxLog> GetLogAgent() const
    {
        return log_agent_->Clone();
    }

    // Update the specified cc node's status, when it is recovering as the
    // leader and has received all log records from the specifid log group.
    void FinishLogReplay(uint32_t cc_ng_id, uint32_t log_group_id)
    {
        auto ng_it = cc_nodes_.find(cc_ng_id);
        if (ng_it == cc_nodes_.end())
        {
            return;
        }

        ng_it->second->FinishLogGroupReplay(log_group_id);
    }

    void RecoverTx(uint64_t tx_number,
                   int64_t tx_term,
                   uint32_t cc_ng_id,
                   int64_t cc_ng_term)
    {
        auto cc_ng_it = cc_nodes_.find(cc_ng_id);
        if (cc_ng_it == cc_nodes_.end())
        {
            LOG(ERROR) << "RecoverTx(): the specified cc node group does not "
                          "exist in this node.";
            return;
        }

        cc_ng_it->second->RecoverTx(tx_number, tx_term, cc_ng_id, cc_ng_term);
    }

private:
    Sharder(uint32_t node_id,
            const std::vector<std::string> *ips,
            const std::vector<uint16_t> *ports,
            LocalCcShards *local_shards,
            std::unique_ptr<TxLog> log_agent)
        : node_id_(node_id),
          mux_(),
          local_cc_shards_(local_shards),
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

            replicated_ = true;
        }
        else
        {
            ng_leader_cache_.try_emplace(0, 0);
            replicated_ = false;
        }
    }

    ~Sharder()
    {
        LOG(INFO) << "Shutting down cc node groups at node #" << node_id_;

        cc_nodes_.clear();
        brpc_server_.Stop(0);
        brpc_server_.Join();

        log_replay_hd_ = nullptr;

        LOG(INFO) << "CC node groups at node #" << node_id_ << " shut down.";
    }

    void ConfigRouteTable()
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

    uint32_t node_id_;
    std::vector<std::string> ips_;
    std::vector<uint16_t> ports_;
    // we have one raft group for each logical shard(specified by ip & port)
    // each group's current leader is stored in ng_leader_cache_.
    std::unordered_map<uint32_t, std::atomic<uint32_t>> ng_leader_cache_;
    std::mutex mux_;
    std::unordered_map<uint32_t, std::unique_ptr<fault::CcNode>> cc_nodes_;
    brpc::Server brpc_server_;
    bool replicated_;
    std::unique_ptr<fault::LogReplayHandler> log_replay_hd_;

    LocalCcShards *local_cc_shards_;
    std::unique_ptr<TxLog> log_agent_;
};
}  // namespace txservice
