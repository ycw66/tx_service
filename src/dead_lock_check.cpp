#include "dead_lock_check.h"

#include <stack>

#include "cc_request.h"
#include "fault/fault_inject.h"
#include "local_cc_shards.h"
#include "sharder.h"

using namespace std::chrono_literals;

namespace txservice
{
DeadLockCheck *DeadLockCheck::inst_ = nullptr;
uint64_t DeadLockCheck::time_interval_ = 60 * 1000000;
CcRequestPool<AbortTransactionCc> abort_tran_pool;

DeadLockCheck::DeadLockCheck(LocalCcShards &local_shards)
    : reply_vct_(Sharder::Instance().GetNodeCount(), false),
      stop_(false),
      local_shards_(local_shards),
      dead_lock_cc_(new CheckDeadLockCc),
      check_node_id_(0)
{
    last_check_time_ = std::chrono::duration_cast<std::chrono::microseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count();
    thd_ = std::thread([this] { Run(); });
}

DeadLockCheck::~DeadLockCheck()
{
}

void DeadLockCheck::MergeRemoteWaitingLockInfo(const tr::DeadLockResponse *rsp)
{
    uint32_t node_id = rsp->node_id();
    std::unique_lock<std::mutex> lock(inst_->mutex_);

    for (int i = 0; i < rsp->block_entry_size(); i++)
    {
        const tr::BlockEntry &be = rsp->block_entry(i);

        LockNode le(node_id, be.core_id(), be.entry());

        auto setid = inst_->entry_locked_txid_map_.try_emplace(le);
        for (int j = 0; j < be.locked_tids_size(); j++)
        {
            LockNode tx(be.locked_tids(j));
            setid.first->second.lock_node_set.insert(tx);
        }

        for (int j = 0; j < be.waited_tids_size(); j++)
        {
            LockNode tx(be.waited_tids(j));
            auto it = inst_->txid_waited_entry_map_.try_emplace(tx);
            it.first->second.lock_node_set.insert(le);
        }
    }

    for (int i = 0; i < rsp->tx_etys_size(); i++)
    {
        const tr::TxEntrys &te = rsp->tx_etys(i);
        auto it = inst_->txid_ety_count_map_.try_emplace(te.txid(), 0);
        it.first->second += te.ety_count();
    }

    inst_->reply_vct_[node_id] = true;
    inst_->node_unfinished_--;

    if (inst_->node_unfinished_ == 0)
    {
        inst_->con_var_.notify_one();
    }
}

void DeadLockCheck::MergeLocalWaitingLockInfo(const CheckDeadLockResult &dlres)
{
    std::unique_lock<std::mutex> lock(inst_->mutex_);

    for (size_t i = 0; i < dlres.entry_lock_info_vec_.size(); i++)
    {
        auto &vcteti = dlres.entry_lock_info_vec_[i];
        for (auto &iter : vcteti)
        {
            LockNode le(
                inst_->local_shards_.NodeId(), (uint32_t) i, iter.first);
            auto setid = inst_->entry_locked_txid_map_.try_emplace(le);
            for (uint64_t txid : iter.second.lock_txids)
            {
                LockNode tx(txid);
                setid.first->second.lock_node_set.insert(tx);
            }
            for (uint64_t txid : iter.second.wait_txids)
            {
                LockNode tx(txid);
                auto it = inst_->txid_waited_entry_map_.try_emplace(tx);
                it.first->second.lock_node_set.insert(le);
            }
        }
    }

    for (auto &vcttx : dlres.txid_ety_lock_count_)
    {
        for (auto &iter : vcttx)
        {
            auto it = inst_->txid_ety_count_map_.try_emplace(iter.first, 0);
            it.first->second += iter.second;
        }
    }

    inst_->reply_vct_[inst_->local_shards_.NodeId()] = true;
    inst_->node_unfinished_--;

    if (inst_->node_unfinished_ == 0)
    {
        inst_->con_var_.notify_one();
    }
}

void DeadLockCheck::UpdateCheckNodeId(uint32_t node_id)
{
    if (inst_->check_node_id_ > node_id ||
        LocalCcShards::ClockTs() - inst_->last_check_time_ > time_interval_ * 5)
    {
        inst_->check_node_id_ = node_id;
    }

    if (inst_->check_node_id_ == node_id)
    {
        inst_->last_check_time_ = LocalCcShards::ClockTs();
    }
}

void DeadLockCheck::GatherLockDependancy()
{
    std::unique_lock<std::mutex> lk(mutex_);
    UpdateCheckNodeId(Sharder::Instance().NodeId());
    node_unfinished_ = reply_vct_.size();
    entry_locked_txid_map_.clear();
    txid_waited_entry_map_.clear();
    txid_ety_count_map_.clear();

    // Send dead lock request to local and remote nodes
    for (uint32_t i = 0; i < (uint32_t) reply_vct_.size(); i++)
    {
        uint32_t node_id = Sharder::Instance().LeaderNodeId(i);
        // If this node group has drifted to other node, it is not need to visit
        // this node again.
        if (node_id != i)
        {
            reply_vct_[i] = true;
            node_unfinished_--;
            continue;
        }

        reply_vct_[i] = false;
        if (node_id == Sharder::Instance().NodeId())
        {
            dead_lock_cc_->GetDeadLockResult().Reset();
            for (size_t i = 0; i < local_shards_.Count(); i++)
            {
                local_shards_.EnqueueCcRequest(i, dead_lock_cc_.get());
            }
        }
        else
        {
            tr::CcMessage send_msg;

            send_msg.set_type(tr::CcMessage::MessageType::
                                  CcMessage_MessageType_DeadLockRequest);
            send_msg.set_tx_number(0);
            send_msg.set_handler_addr(0);
            send_msg.set_tx_term(0);
            send_msg.set_command_id(0);

            tr::DeadLockRequest *dl = send_msg.mutable_dead_lock_request();
            dl->set_src_node_id(Sharder::Instance().NodeId());
            bool hr =
                Sharder::Instance().GetCcStreamSender()->SendMessageToNode(
                    node_id, send_msg);
            if (!hr)
            {
                reply_vct_[i] = true;
                node_unfinished_--;
            }
        }
    }

    // Wait all nodes to finish dead check and return data. If exceed the half
    // of interval time, this time for dead lock will be neglect
    con_var_.wait_for(lk,
                      std::chrono::microseconds(time_interval_ / 2),
                      [this]() {
                          return node_unfinished_ == 0 ||
                                 stop_.load(std::memory_order_acquire);
                      });

    if (stop_.load(std::memory_order_acquire))
    {
        return;
    }

    if (node_unfinished_ > 0)
    {
        LOG(INFO) << "[Global dead lock detector]: fails to receive lock "
                     "waiting information from node. Failed nodes: "
                  << node_unfinished_;
        return;
    }

    std::vector<std::vector<LockNode>> vct_dead;
    DetectDeadLock(vct_dead);
    RemoveDeadTransaction(vct_dead);
}

// The algorithm describer for dead lock detect.
// Txids and all its locked cc entrys saved in map_txid_locked_entry_, cc entrys
// and its all waited txids saved in map_entry_waited_txid_. Here will traverse
// map_txid_locked_entry_ and judge if the pair has been visit. If visited,
// neglect and go to next pair. If not, use depth-first traversal, first mark
// LockNodeSet.ivisit with "count", then according the cc entry to find waited
// txids, then according txids to find its locked cc entry. Here use v_set_pos
// to record the position in set of locked cc entrys or waited txids. if remeet
// the lock entry (identified by LockEntry.ivisit), it indicate to find dead
// lock, according to v_set_pos to find the related txid and cc entrys.
void DeadLockCheck::DetectDeadLock(std::vector<std::vector<LockNode>> &vct_dead)
{
    // Here will start from a ccentry to vist its locked txid, then from txid to
    // waited ccentry. To avoid repeate work, if a ccentry has visited, it is
    // not need to visit again. Here count to make sure the ccentry is not
    // visited again.
    int32_t count = 0;
    for (auto ccety_iter = entry_locked_txid_map_.begin();
         ccety_iter != entry_locked_txid_map_.end();
         ccety_iter++)
    {
        // If this ccentry has been visited, continue to avoid visit again.
        if (ccety_iter->second.ivisit != -1)
        {
            continue;
        }
        count++;

        // Here is depth-first traversal. Due to stack can not be traversed, so
        // here use vector to imitate the stack. Start from root ccentry, it
        // will first save the set address with locked txids from root ccentry
        // into v_set, and save the begin position of v_set into v_set_pos.
        std::vector<LockNodeSet *> v_set;
        std::vector<std::unordered_set<LockNode, NeHash, NeEqual>::iterator>
            v_set_pos;

        // To mark this ccentry has been visited
        ccety_iter->second.ivisit = count;
        // Push the set from the root ccentry into vector
        v_set.push_back(&ccety_iter->second);
        // push the begin iterator of the set from root ccentry into vector
        v_set_pos.push_back(ccety_iter->second.lock_node_set.begin());

        while (true)
        {
            // Get the last layer's set from stack
            LockNodeSet *lety = *v_set.rbegin();
            // Get the last layer's set's iterator from stack
            auto &itpos = *v_set_pos.rbegin();
            // If has visit the last element in top layer, it will pop this set
            // and set next layer as top layer.
            if (itpos == lety->lock_node_set.end())
            {
                v_set.pop_back();
                v_set_pos.pop_back();
                // If the stack is empty, break this search.
                if (v_set.size() == 0)
                {
                    break;
                }
                // After pop the last layer and move to next, here will make the
                // iterator of set move to next position.
                (*v_set_pos.rbegin())++;
                continue;
            }

            if (itpos->is_ccentry_addr)
            {
                // If the current node is ccentry type, it will search
                // entry_locked_txid_map_ to find the the txids that locked this
                // ccentry.
                auto itety = entry_locked_txid_map_.find(*itpos);
                if (itety == entry_locked_txid_map_.end() ||
                    (itety->second.ivisit > 0 && itety->second.ivisit != count))
                {
                    // If failed to find the txids or has visited this node,
                    // move to next element in set.
                    itpos++;
                }
                else if (itety->second.ivisit == count)
                {
                    // ivisit==count means here has find the circle of dead
                    // lock, save the entire path of circle into a vector for
                    // next step.
                    std::vector<LockNode> vct;
                    auto itp = v_set_pos.rbegin();
                    auto ite = v_set.rbegin();
                    for (; itp != v_set_pos.rend(); itp++, ite++)
                    {
                        vct.push_back(**itp);
                        if (*ite == &itety->second)
                        {
                            break;
                        }
                    }

                    vct_dead.push_back(vct);
                    itpos++;
                }
                else
                {
                    // Push the search result into stack
                    itety->second.ivisit = count;
                    v_set.push_back(&itety->second);
                    v_set_pos.push_back(itety->second.lock_node_set.begin());
                }
            }
            else
            {
                // If the current node is txid type, search
                // txid_waited_entry_map_ and find the waited ccentry.
                auto itety = txid_waited_entry_map_.find(*itpos);
                if (itety == txid_waited_entry_map_.end() ||
                    (itety->second.ivisit > 0 && itety->second.ivisit != count))
                {
                    // If failed to find the txids or has visited this node,
                    // move to next element in set.
                    itpos++;
                }
                else if (itety->second.ivisit == count)
                {
                    // If ivisit == count, means here find the circle of dead
                    // lock, save the entire path into a vector.
                    std::vector<LockNode> vct;
                    auto itp = v_set_pos.rbegin();
                    auto ite = v_set.rbegin();
                    for (; itp != v_set_pos.rend(); itp++, ite++)
                    {
                        vct.push_back(**itp);
                        if (*ite == &itety->second)
                        {
                            break;
                        }
                    }

                    vct_dead.push_back(vct);
                    itpos++;
                }
                else
                {
                    // Push the search result into stack.
                    itety->second.ivisit = count;
                    v_set.push_back(&itety->second);
                    v_set_pos.push_back(itety->second.lock_node_set.begin());
                }
            }
        }
    }
}

void DeadLockCheck::RemoveDeadTransaction(
    std::vector<std::vector<LockNode>> &vct_dead)
{
    for (std::vector<LockNode> &dead : vct_dead)
    {
        uint64_t tx_id = 0;
        uint32_t max_ety = 0;

        for (LockNode &le : dead)
        {
            if (le.is_ccentry_addr)
            {
                continue;
            }

            uint32_t cnt = txid_ety_count_map_.find(le.tx_id)->second;
            if (cnt > max_ety)
            {
                max_ety = cnt;
                tx_id = le.tx_id;
            }
            // This brance to ensure the small tx id to be abort and test case
            // will not fail due to uncertainty
            else if (cnt == max_ety && tx_id > le.tx_id)
            {
                tx_id = le.tx_id;
            }
        }

        LockNode le(tx_id);
        LockNodeSet &lety = txid_waited_entry_map_.find(le)->second;
        for (const LockNode &lent : lety.lock_node_set)
        {
            if (lent.node_id == Sharder::Instance().NodeId())
            {
                AbortTransactionCc *atcc = abort_tran_pool.NextRequest();
                atcc->Reset(lent.ety_addr,
                            entry_locked_txid_map_.find(lent)
                                ->second.lock_node_set.begin()
                                ->tx_id,
                            tx_id,
                            lent.node_id);
                local_shards_.EnqueueCcRequest(lent.core_id, atcc);
            }
            else
            {
                tr::CcMessage send_msg;

                send_msg.set_type(
                    tr::CcMessage::MessageType::
                        CcMessage_MessageType_AbortTransactionRequest);
                send_msg.set_tx_number(0);
                send_msg.set_handler_addr(0);
                send_msg.set_tx_term(0);
                send_msg.set_command_id(0);

                tr::AbortTransactionRequest *atreq =
                    send_msg.mutable_abort_tran_req();
                atreq->set_src_node_id(Sharder::Instance().NodeId());
                atreq->set_node_id(lent.node_id);
                atreq->set_core_id(lent.core_id);
                atreq->set_entry(lent.ety_addr);
                atreq->set_wait_txid(tx_id);
                atreq->set_lock_txid(entry_locked_txid_map_.find(lent)
                                         ->second.lock_node_set.begin()
                                         ->tx_id);

                Sharder::Instance().GetCcStreamSender()->SendMessageToNode(
                    lent.node_id, send_msg);
            }
        }
    }
}

void DeadLockCheck::Run()
{
    // Wait until LocalCcShards thread has started. Or it will maybe make crash.
    while (LocalCcShards::ClockTs() < last_check_time_)
    {
        std::unique_lock<std::mutex> lk(mutex_);
        con_var_.wait_for(lk,
                          10s,
                          [this]()
                          { return stop_.load(std::memory_order_acquire); });
    }

    while (!stop_.load(std::memory_order_acquire))
    {
        CODE_FAULT_INJECTOR("dead_lock_check", {
            FaultInject::Instance().InjectFault("dead_lock_check", "remove");
            GatherLockDependancy();
        });

        std::unique_lock<std::mutex> lk(mutex_);
        con_var_.wait_for(
            lk, 1s, [this]() { return stop_.load(std::memory_order_acquire); });
        // If the time is in interval time since previous check, it will sleep
        // again.
        uint64_t ival = LocalCcShards::ClockTs() - last_check_time_;
        if (stop_.load(std::memory_order_acquire) || ival < time_interval_)
        {
            continue;
        }

        // If the last check riser is this node, it will call dead lock check
        // again. Or if the time spend more than two times than interval time
        // due to last check riser crashed, this node will rise the check. To
        // avoid multi nodes rise the check at the same time, here add
        // local_shards_.NodeId() * MICRO_SECOND to make more waitting seconds
        // according the node id.
        if (stop_.load(std::memory_order_acquire) ||
            ival < time_interval_ * 2 + local_shards_.NodeId() * MICRO_SECOND &&
                check_node_id_ != local_shards_.NodeId())
        {
            continue;
        }

        lk.unlock();
        GatherLockDependancy();
    }
}
}  // namespace txservice
