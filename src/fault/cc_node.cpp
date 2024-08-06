#include "fault/cc_node.h"

#include <atomic>

#include "local_cc_shards.h"
#include "sharder.h"
#include "tx_service.h"

namespace txservice::fault
{

CcNode::CcNode(const uint32_t ng_id,
               const uint32_t node_id,
               LocalCcShards &local_shards,
               uint32_t log_group_cnt)
    : ng_id_(ng_id),
      node_id_(node_id),
      last_ckpt_ts_(0),
      pinning_threads_(0),
      local_cc_shards_(local_shards),
      log_group_cnt_(log_group_cnt)
{
}

bool CcNode::CheckLogGroupReplayFinished(uint32_t log_group_id, int64_t ng_term)
{
    std::lock_guard<std::mutex> lk(recovery_mux_);
    int64_t term = Sharder::Instance().LeaderTerm(ng_id_);
    if (term >= ng_term)
    {
        return true;
    }
    int64_t candidate_term = Sharder::Instance().CandidateLeaderTerm(ng_id_);
    if (candidate_term > ng_term)
    {
        return true;
    }
    if (recovered_log_groups_.find(log_group_id) == recovered_log_groups_.end())
    {
        return false;
    }
    return true;
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
    int64_t candidate_term = Sharder::Instance().CandidateLeaderTerm(ng_id_);
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
    if (ng_id_ == node_id_)
    {
        local_cc_shards_.SetTxIdent(latest_committed_txn_no);
    }

    auto lg_it = recovered_log_groups_.emplace(log_group_id);
    if (lg_it.second && recovered_log_groups_.size() == log_group_cnt_)
    {
        // reset the recovered_log_groups_ since we have finished the log replay
        // work for the current term.
        recovered_log_groups_.clear();

        candidate_term = Sharder::Instance().CandidateLeaderTerm(ng_id_);
        Sharder::Instance().SetLeaderTerm(ng_id_, candidate_term);
        LOG(INFO) << "The leader of cc node group ng#" << ng_id_
                  << " with the term " << candidate_term
                  << " has been recovered.";
        Sharder::Instance().SetCandidateTerm(ng_id_, -1);

        Sharder::Instance().NodeGroupFinishRecovery(ng_id_);
    }
}

int64_t CcNode::PinData()
{
    std::unique_lock lk(pinning_threads_mux_);
    int64_t leader_term = Sharder::Instance().LeaderTerm(ng_id_);
    if (leader_term > 0)
    {
        pinning_threads_++;
    }
    else
    {
        leader_term = Sharder::Instance().CandidateLeaderTerm(ng_id_);
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
        if (node_ip.empty())
        {
            // node is already removed from cluster.
            continue;
        }

        std::shared_ptr<brpc::Channel> channel =
            Sharder::Instance().GetCcNodeServiceChannel(node_id);
        if (channel == nullptr)
        {
            // Fails to establish the channel to the tx node. Silently
            // returns. The tx will be recovered again by next
            // conflicting tx.
            LOG(ERROR) << "Fail to init the channel to the leader of ng#"
                       << leader_ng_id << " to notify leader start.";
            continue;
        }

        remote::CcRpcService_Stub stub(channel.get());
        remote::NotifyNewLeaderStartRequest req;
        req.set_ng_id(leader_ng_id);
        req.set_node_id(leader_node_id);
        remote::NotifyNewLeaderStartResponse res;
        res.set_error(false);

        brpc::Controller cntl;
        cntl.set_timeout_ms(100);
        stub.NotifyNewLeaderStart(&cntl, &req, &res, nullptr);

        // Retry is not needed at here, the remote nodes will also
        // refresh their leader caches passively.
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

bool CcNode::OnLeaderStart(int64_t term)
{
    bool expected = false;
    if (!is_processing_.compare_exchange_strong(
            expected, true, std::memory_order_acq_rel))
    {
        // Keep retring
        return false;
    }

    std::shared_ptr<void> defer_release(
        nullptr,
        [this](void *)
        { is_processing_.store(false, std::memory_order_release); });

    if (Sharder::Instance().InvalidLeaderTerm(ng_id_) >= term)
    {
        // outdate request. This ccnode is not leader anymore.
        return true;
    }

    if (Sharder::Instance().CandidateLeaderTerm(ng_id_) >= term ||
        Sharder::Instance().LeaderTerm(ng_id_) >= term)
    {
        // This ccnode has become a candidate leader or leader
        return true;
    }

    // Invalidate terms smaller than the new term on this ng.
    Sharder::Instance().SetInvalidLeaderTerm(ng_id_, term - 1);

    {
        // replay thread and leader election thread may update
        // candidate_leader_term_, leader_term_ and recovered_log_groups_
        // concurrently.
        std::lock_guard<std::mutex> lk(recovery_mux_);
        Sharder::Instance().SetCandidateTerm(ng_id_, term);
        // new leader will send ReplayLog request to logservice to replay logs.
        // It should reset the recovered_log_groups_ ahead.
        recovered_log_groups_.clear();
    }

    LOG(INFO) << "CC node " << node_id_ << " becomes the leader of ng#"
              << ng_id_ << ". Term: " << term;

    if (!local_cc_shards_.IsRangeBucketsInitialized(ng_id_))
    {
        if (txservice_skip_kv)
        {
            // TODO: HARDCORE SEED
            // If kv is not enabled, just copy bucket info from preferred ng.
            local_cc_shards_.InitRangeBuckets(
                ng_id_,
                Sharder::Instance().NodeGroupCount(),
                Sharder::Instance().ClusterConfigVersion(),
                9001);
        }
        else
        {
            // We need to initialize range bucket info for new ng
            // before replaying.
            std::unordered_map<uint32_t, std::vector<NodeConfig>> ng_configs;
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
            local_cc_shards_.InitRangeBuckets(
                ng_id_, ng_configs.size(), version, seed);
            if (Sharder::Instance().ClusterConfigVersion() < version)
            {
                // Use a dummy cc request that returns once it's put into cc
                // queue.
                WaitableCc cc;
                Sharder::Instance().UpdateClusterConfig(
                    ng_configs, version, &cc, local_cc_shards_.GetCcShard(0));
                cc.Wait();
            }
        }
    }

    local_cc_shards_.InitPrebuiltTables(ng_id_);

    if (txservice_skip_wal)
    {
        {
            // replay thread and leader election thread may update
            // candidate_leader_term_, leader_term_ and recovered_log_groups_
            // concurrently.
            std::lock_guard<std::mutex> lk(recovery_mux_);
            Sharder::Instance().SetLeaderTerm(ng_id_, term);
            LOG(INFO) << "Skipped log replay for cc node group #" << ng_id_
                      << " with the term " << term;
            Sharder::Instance().SetCandidateTerm(ng_id_, -1);
            Sharder::Instance().NodeGroupFinishRecovery(ng_id_);
        }
        NotifyNewLeaderStart(ng_id_, node_id_);
    }

    return true;
}

bool CcNode::OnLeaderStop(int64_t term)
{
    bool expected = false;
    if (!is_processing_.compare_exchange_strong(
            expected, true, std::memory_order_acq_rel))
    {
        // Keep retrying
        return false;
    }

    std::shared_ptr<void> defer_release(
        nullptr,
        [this](void *)
        { is_processing_.store(false, std::memory_order_release); });

    if (Sharder::Instance().InvalidLeaderTerm(ng_id_) >= term)
    {
        // This ccnode has already call `OnLeaderStop` at `term`
        return true;
    }

    Sharder::Instance().SetInvalidLeaderTerm(ng_id_, term);

    {
        // replay thread and leader election thread may update
        // candidate_leader_term_, leader_term_ and recovered_log_groups_
        // concurrently.
        std::lock_guard<std::mutex> lk(recovery_mux_);
        Sharder::Instance().SetLeaderTerm(ng_id_, -1);
        Sharder::Instance().SetCandidateTerm(ng_id_, -1);
    }

    LOG(INFO) << "CC node " << node_id_ << " steps down as the leader of ng#"
              << ng_id_ << ".";

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

    return true;
}

}  // namespace txservice::fault
