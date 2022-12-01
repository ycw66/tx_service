#include "tx_start_ts_collector.h"

#include <chrono>

#include "brpc/channel.h"
#include "butil/logging.h"
#include "sharder.h"

namespace txservice
{
TxStartTsCollector::TxStartTsCollector(LocalCcShards *shards,
                                       uint32_t delay_seconds)
    : active_(false), min_start_ts_(1UL), local_shards_(shards)
{
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
    active_.store(true);
    thd_ = std::thread([this] { Run(); });
}

void TxStartTsCollector::Shutdown()
{
    active_.store(false);
    thd_.join();
}

void TxStartTsCollector::Run()
{
    uint32_t period = 0U;
    while (active_.load())
    {
        if (period < delay_seconds_)
        {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            period++;
        }
        else
        {
            min_start_ts_ = CollectMinTxStartTs();
            period = 0U;
        }
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
        if (channel.Init(
                node_ip.c_str(), GET_CCNODE_RPC_PORT(node_port), nullptr) != 0)
        {
            LOG(ERROR) << "Fail to init the channel to the node("
                       << dest_node_id << ") .";

            continue;
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
