#include "tx_start_ts_collector.h"

#include <braft/util.h>  //braft::HostNameAddr2NSUrl
#include <brpc/channel.h>
#include <butil/logging.h>

#include <chrono>

#include "sharder.h"

namespace txservice
{
void TxStartTsCollector::Init(LocalCcShards *shards, uint32_t delay_seconds)
{
    active_ = false;
    min_start_ts_ = 1UL;
    local_shards_ = shards;
    delay_seconds_ = std::max(1U, delay_seconds);
    uint32_t ng_cnt = Sharder::Instance().NodeGroupCount();
    for (uint32_t ng_id = 0; ng_id < ng_cnt; ++ng_id)
    {
        min_start_ts_map_.emplace(ng_id, 1U);
    }

    DLOG(INFO) << "TxStartTsCollector init, interval seconds: "
               << delay_seconds;
}

void TxStartTsCollector::Start()
{
    DLOG(INFO) << "TxStartTsCollector start, interval seconds: "
               << delay_seconds_;
    active_ = true;
    thd_ = std::thread([this] { Run(); });
}

void TxStartTsCollector::Shutdown()
{
    {
        std::unique_lock<std::mutex> lk(active_mux_);
        active_ = false;
        active_cv_.notify_one();
    }
    thd_.join();
}

void TxStartTsCollector::Run()
{
    std::unique_lock<std::mutex> lk(active_mux_);

    while (active_)
    {
        active_cv_.wait_for(lk,
                            std::chrono::seconds(delay_seconds_),
                            [this]() { return active_ == false; });
        lk.unlock();
        min_start_ts_ = CollectMinTxStartTs();

        lk.lock();
    }
}

/**
 * @brief Collect the minimal start_ts of all active transactions in all
 * cc node groups. If leader node failed, use the previous minimal start_ts.
 *
 * @return The minimal start_ts
 */
uint64_t TxStartTsCollector::CollectMinTxStartTs()
{
    // collect recyle ts from all ccshards in all cc_node_group
    uint64_t min_start_ts = UINT64_MAX;

    uint32_t ng_cnt = Sharder::Instance().NodeGroupCount();

    std::string node_ip;
    uint16_t node_port;
    for (uint32_t ng_id = 0; ng_id < ng_cnt; ++ng_id)
    {
        uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(ng_id);

        if (dest_node_id != ng_id)
        {
            continue;  // ng leader changed
        }

        if (dest_node_id == local_shards_->NodeId())
        {
            min_start_ts_map_[ng_id] = local_shards_->StatsLocalActiveSiTxs();
            continue;
        }

        Sharder::Instance().GetNodeAddress(dest_node_id, node_ip, node_port);

        brpc::Channel channel;
        butil::ip_t ip_t;
        if (0 != butil::str2ip(node_ip.c_str(), &ip_t))
        {
            // for case `node_ip` is hostname format
            std::string naming_service_url;
            braft::HostNameAddr hostname_addr(node_ip,
                                              GET_CCNODE_RPC_PORT(node_port));
            braft::HostNameAddr2NSUrl(hostname_addr, naming_service_url);
            if (channel.Init(naming_service_url.c_str(),
                             braft::LOAD_BALANCER_NAME,
                             nullptr) != 0)
            {
                LOG(ERROR) << "Fail to init the channel to the node("
                           << dest_node_id << ") .";
                continue;
            }
        }
        else
        {
            if (channel.Init(node_ip.c_str(),
                             GET_CCNODE_RPC_PORT(node_port),
                             nullptr) != 0)
            {
                LOG(ERROR) << "Fail to init the channel to the node("
                           << dest_node_id << ") .";

                continue;
            }
        }
        remote::CcRpcService_Stub stub(&channel);
        remote::GetMinTxStartTsRequest req;
        req.set_ng_id(ng_id);
        remote::GetMinTxStartTsResponse res;

        brpc::Controller cntl;
        cntl.set_timeout_ms(100);
        cntl.set_max_retry(3);
        stub.GetMinTxStartTs(&cntl, &req, &res, nullptr);

        if (cntl.Failed())
        {
            LOG(ERROR) << "Fail to call the GetMinTxStartTs RPC of node("
                       << dest_node_id << "). Error code: " << cntl.ErrorCode()
                       << ". Msg: " << cntl.ErrorText();
        }
        else
        {
            if (!res.error() && res.term() > 0)
            {
                min_start_ts_map_[ng_id] = res.ts();
            }
        }
    }

    for (auto it : min_start_ts_map_)
    {
        min_start_ts = std::min(min_start_ts, it.second);
    }
    // LOG(INFO) << "collect min start ts of all active tx :" << min_start_ts;
    return min_start_ts;
}

}  // namespace txservice
