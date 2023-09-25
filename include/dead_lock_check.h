#pragma once
#include <condition_variable>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "proto/cc_request.pb.h"

namespace txservice
{
const uint64_t MICRO_SECOND = 1000000;

class LocalCcShards;
struct CheckDeadLockResult;
struct CheckDeadLockCc;

struct LockNode
{
    LockNode(uint32_t nodeid, uint32_t coreid, uint64_t addr)
        : is_ccentry_addr(true),
          node_id(nodeid),
          core_id(coreid),
          ety_addr(addr)
    {
    }

    explicit LockNode(uint64_t txid)
        : is_ccentry_addr(false), node_id(0), tx_id(txid)
    {
    }

    bool operator==(const LockNode &other) const
    {
        assert(is_ccentry_addr == other.is_ccentry_addr);
        if (is_ccentry_addr)
        {
            return node_id == other.node_id && ety_addr == other.ety_addr;
        }
        else
        {
            return tx_id == other.tx_id;
        }
    }

    // false: txid; true: entry_address
    bool is_ccentry_addr;
    // Only valid when is_ccentry_addr=true, to save which node from for entry
    uint32_t node_id;
    // Only valid when is_ccentry_addr=true, to save which core from for entry
    uint32_t core_id;
    // To save txid or entry addr according is_ccentry_addr;
    union
    {
        uint64_t tx_id;
        uint64_t ety_addr;
    };
};

struct NeHash
{
    std::size_t operator()(const LockNode &ne) const
    {
        if (ne.is_ccentry_addr)
        {
            return std::hash<uint64_t>{}(ne.node_id ^ ne.ety_addr);
        }
        else
        {
            return std::hash<uint64_t>{}(ne.tx_id);
        }
    }
};

struct NeEqual
{
    bool operator()(const LockNode &lhs, const LockNode &rhs) const
    {
        return (lhs == rhs);
    }
};

struct LockNodeSet
{
    int32_t ivisit = -1;
    std::unordered_set<LockNode, NeHash, NeEqual> lock_node_set;
};

namespace tr = txservice::remote;
class DeadLockCheck
{
public:
    explicit DeadLockCheck(LocalCcShards &local_shards);
    ~DeadLockCheck();
    static void MergeRemoteWaitingLockInfo(const tr::DeadLockResponse *rsp);
    static void MergeLocalWaitingLockInfo(const CheckDeadLockResult &dlres);
    static void SetStop()
    {
        {
            std::unique_lock<std::mutex> lk(mutex_);
            stop_.store(true, std::memory_order_release);
            con_var_.notify_one();
        }

        thd_.join();
    }
    static void Free()
    {
        delete inst_;
        inst_ = nullptr;
    }
    static void Init(LocalCcShards &local_shards)
    {
        inst_ = new DeadLockCheck(local_shards);
    }
    static void SetTimeInterval(uint64_t itval)
    {
        time_interval_ = itval * MICRO_SECOND;
    }
    static void UpdateCheckNodeId(uint32_t node_id);

protected:
    void Run();
    void GatherLockDependancy();
    void DetectDeadLock(std::vector<std::vector<LockNode>> &vct_dead);
    void RemoveDeadTransaction(std::vector<std::vector<LockNode>> &vct_dead);

protected:
    static DeadLockCheck *inst_;
    // Time interval for dead lock check, microseconds
    static uint64_t time_interval_;
    // map for cc entry and its locked txids
    std::unordered_map<LockNode, LockNodeSet, NeHash, NeEqual>
        entry_locked_txid_map_;
    // the map for txid and its waited entrys
    std::unordered_map<LockNode, LockNodeSet, NeHash, NeEqual>
        txid_waited_entry_map_;
    // The map for txids and the number that locked entrys
    std::unordered_map<uint64_t, uint32_t> txid_ety_count_map_;
    // The related nodes have send back the result or not.
    std::vector<bool> reply_vct_;
    // The count of nodes without relay
    int32_t node_unfinished_ = 0;

    std::thread thd_;
    // If process has been closed and this thread need to stop;
    std::atomic<bool> stop_;

    // mutex_ and con_var_ are used to wait local node and remote nodes to
    // finish dead lock check and return the related cc entrys and tx ids.
    std::mutex mutex_;
    std::condition_variable con_var_;

    LocalCcShards &local_shards_;
    std::unique_ptr<CheckDeadLockCc> dead_lock_cc_;
    // The last time to receive check command
    uint64_t last_check_time_;
    // The node to rise dead lock check.
    uint32_t check_node_id_;
};
}  // namespace txservice
