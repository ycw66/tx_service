#pragma once

#include <algorithm>
#include <memory>
#include <set>
#include <utility>
#include <vector>

#include "butil/logging.h"
#include "tx_command.h"
#include "tx_record.h"

namespace txservice
{
struct TxObject : public TxRecord
{
public:
    TxObject() = default;

    TxObject(const TxObject &obj)
    {
    }

    TxObject &operator=(const TxObject &obj)
    {
        return *this;
    }

    /**
     * Commit all pending commands stored on this object.
     */
    void CommitCommands()
    {
        for (auto &cmd : cmd_list_)
        {
            cmd->CommitOn(*this);
        }

        cmd_list_.clear();
    }

    /**
     * The replayed commands must apply in order. Replayed commands are first
     * stored in replay_cmd_list_ and committed in order.
     * @param obj_ver
     * @param commit_ts
     * @param cmd_list
     * @param cur_ver
     */
    void EmplaceAndCommitReplayTxnCommand(
        uint64_t obj_ver,
        uint64_t commit_ts,
        std::vector<std::unique_ptr<TxCommand>> cmd_list,
        uint64_t &cur_ver)
    {
        if (replay_cmd_list_ == nullptr)
        {
            replay_cmd_list_ = std::make_unique<ReplayTxnCmdList>();
        }
        auto cmp = [](const TxnCmd &lhs, const TxnCmd &rhs) -> bool
        { return lhs.obj_version_ < rhs.obj_version_; };

        std::vector<TxnCmd> &txn_cmd_list = replay_cmd_list_->txn_cmd_list_;

        TxnCmd cmd(obj_ver, commit_ts, std::move(cmd_list));
        auto lb_it = std::lower_bound(
            txn_cmd_list.begin(), txn_cmd_list.end(), cmd, cmp);
        LOG(INFO) << "Try emplace txn obj_ver: " << obj_ver
                  << ", commit_ts: " << commit_ts
                  << ", lb: " << lb_it - txn_cmd_list.begin();

        txn_cmd_list.insert(lb_it, std::move(cmd));

        TryCommitReplayCommands(cur_ver);
    }

    /**
     * Commit replay txn commands in version order and update cur_ver.
     * @param cur_ver
     */
    void TryCommitReplayCommands(uint64_t &cur_ver)
    {
        assert(replay_cmd_list_ != nullptr);
        std::vector<TxnCmd> &txn_cmd_list = replay_cmd_list_->txn_cmd_list_;
        // iterate the list and apply the commands in version order
        for (auto it = txn_cmd_list.begin(); it != txn_cmd_list.end();)
        {
            // check version match
            if (it->obj_version_ != cur_ver)
            {
                break;
            }
            // apply the commands of this txn
            for (auto &cmd : it->cmd_list_)
            {
                cmd->CommitOn(*this);
            }
            cur_ver = it->new_version_;
            LOG(INFO) << "commit replay txn cmds, obj ver: " << it->obj_version_
                      << ", new ver: " << it->new_version_;
            it = txn_cmd_list.erase(it);
        }
        if (txn_cmd_list.empty())
        {
            LOG(INFO) << "destruct replay_cmd_list_ on object: " << this;
            replay_cmd_list_ = nullptr;
        }
    }

    /**
     * Execute cmd to get the result and put the cmd into cmd_list_ for later
     * commit.
     * @param cmd
     * @param cmd_res
     */
    virtual void Apply(TxCommand &cmd)
    {
    }

    // object owns the commands
    std::vector<std::unique_ptr<TxCommand>> cmd_list_;

    // commands and information of the same txn
    struct TxnCmd
    {
        TxnCmd(uint64_t obj_ver,
               uint64_t commit_ts,
               std::vector<std::unique_ptr<TxCommand>> cmd_list)
            : obj_version_(obj_ver),
              new_version_(commit_ts),
              cmd_list_(std::move(cmd_list))
        {
        }

        TxnCmd(TxnCmd &&cmd)
            : obj_version_(cmd.obj_version_),
              new_version_(cmd.new_version_),
              cmd_list_(std::move(cmd.cmd_list_))
        {
        }

        TxnCmd &operator=(TxnCmd &&cmd)
        {
            obj_version_ = cmd.obj_version_;
            new_version_ = cmd.new_version_;
            cmd_list_ = std::move(cmd.cmd_list_);
            return *this;
        }

        // the commit_ts of the object when commands of this txn applies to
        // it
        uint64_t obj_version_{};
        // commit_ts of the txn
        uint64_t new_version_{};
        // the commands the txn applies to this object
        std::vector<std::unique_ptr<TxCommand>> cmd_list_;
    };

    struct ReplayTxnCmdList
    {
        // TODO(zkl): set cur_version_ to the object's version when load object
        //  from kv
        // commit_ts of the last applied transaction, commands must be
        // applied in transactions' commit order
        uint64_t cur_version_{1};
        std::vector<TxnCmd> txn_cmd_list_;
    };

    std::unique_ptr<ReplayTxnCmdList> replay_cmd_list_;
};
}  // namespace txservice
