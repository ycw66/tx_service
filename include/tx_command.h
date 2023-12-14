#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "tx_record.h"

namespace txservice
{
struct TxObject;

struct TxCommandResult
{
public:
    virtual ~TxCommandResult() = default;
    virtual void Serialize(std::string &buf) const = 0;
    virtual void Deserialize(const char *buf, size_t &offset) = 0;
};

struct TxCommand
{
public:
    virtual ~TxCommand() = default;
    virtual std::unique_ptr<TxCommand> Clone() = 0;
    virtual bool IsReadOnly() const = 0;
    virtual bool IsDelete() const
    {
        return false;
    }
    virtual std::unique_ptr<TxRecord> CreateObject(
        const std::string *image) const = 0;
    virtual std::unique_ptr<TxCommandResult> CreateCommandResult() const = 0;
    virtual bool ProceedOnNonExistentObject() const = 0;
    virtual bool ProceedOnExistentObject() const = 0;
    /**
     * Execute cmd on object to get the result.
     * @param object
     * @return success
     */
    virtual bool ExecuteOn(TxObject &object) = 0;

    // Commit current command on obj_ptr, return the new object if the command
    // changes or deletes the object. Read only command need not commit.
    virtual TxObject *CommitOn(TxObject *obj_ptr)
    {
        assert(false);
        return obj_ptr;
    }

    // serialize command for remote request and writing log
    virtual void Serialize(std::string &str) const
    {
    }

    // deserialize a command from binary blob for processing remote request and
    // replaying log
    virtual void Deserialize(std::string_view cmd_img)
    {
    }
};

/**
 * Commands that operate on multiple keys, like MSET, DEL.
 */
struct MultiObjectTxCommand
{
    virtual ~MultiObjectTxCommand() = default;

    std::vector<const TxKey *> *KeyPointers()
    {
        return &key_ptrs_;
    }

    std::vector<TxCommand *> *CommandPointers()
    {
        return &cmd_ptrs_;
    }

    std::vector<const txservice::TxKey *> key_ptrs_;
    std::vector<txservice::TxCommand *> cmd_ptrs_;
};

// commands and information of the same txn
struct TxnCmd
{
    TxnCmd(uint64_t obj_ver,
           uint64_t commit_ts,
           bool has_del,
           std::vector<std::unique_ptr<TxCommand>> &&cmd_list)
        : obj_version_(obj_ver),
          new_version_(commit_ts),
          has_del_(has_del),
          cmd_list_(std::move(cmd_list))
    {
    }

    // the commit_ts of the object when commands of this txn applies to
    // it
    uint64_t obj_version_{};
    // commit_ts of the txn
    uint64_t new_version_{};
    // whether this txn has a del command on this object
    bool has_del_{};
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

    void EmplaceTxnCmd(TxnCmd &txn_cmd)
    {
        auto cmp = [](const TxnCmd &lhs, const TxnCmd &rhs) -> bool
        { return lhs.obj_version_ < rhs.obj_version_; };

        std::vector<TxnCmd> &txn_cmd_list = txn_cmd_list_;

        auto lb_it = std::lower_bound(
            txn_cmd_list.begin(), txn_cmd_list.end(), txn_cmd, cmp);

        if (txn_cmd.has_del_)
        {
            // For Del command, remove the txn commands before this txn since
            // the old object was deleted.
            if (txn_cmd.obj_version_ > cur_version_)
            {
                cur_version_ = txn_cmd.obj_version_;
            }

            lb_it = txn_cmd_list.erase(txn_cmd_list.begin(), lb_it);
        }
        txn_cmd_list.insert(lb_it, std::move(txn_cmd));
    }
};

/**
 * Commit replay txn commands in version order and update cur_ver.
 *
 * @tparam T
 * @param payload
 * @param replay_cmd_list
 * @param cur_ver
 */
template <class T>
void TryCommitReplayCommands(std::shared_ptr<T> &payload,
                             std::unique_ptr<ReplayTxnCmdList> &replay_cmd_list,
                             uint64_t &cur_ver)
{
    assert(replay_cmd_list != nullptr);
    std::vector<TxnCmd> &txn_cmd_list = replay_cmd_list->txn_cmd_list_;
    // iterate the list and apply the commands in version order
    for (auto it = txn_cmd_list.begin(); it != txn_cmd_list.end();)
    {
        if (it->has_del_ && it->obj_version_ >= cur_ver)
        {
            cur_ver = it->obj_version_;
        }

        // check version match
        if (it->obj_version_ != cur_ver)
        {
            break;
        }
        if (it->has_del_)
        {
            payload = nullptr;
        }
        // apply the commands of this txn
        for (auto &cmd : it->cmd_list_)
        {
            if (payload == nullptr)
            {
                std::shared_ptr<TxRecord> obj_ptr = cmd->CreateObject(nullptr);
                payload = std::dynamic_pointer_cast<T>(obj_ptr);
            }
            TxObject *obj_ptr = payload.get();
            TxObject *new_obj_ptr = cmd->CommitOn(obj_ptr);
            if (new_obj_ptr != obj_ptr)
            {
                payload = std::shared_ptr<T>(static_cast<T *>(new_obj_ptr));
            }
        }
        cur_ver = it->new_version_;
        LOG(INFO) << "commit replay txn cmds, obj ver: " << it->obj_version_
                  << ", new ver: " << it->new_version_;
        it = txn_cmd_list.erase(it);
    }

    if (txn_cmd_list.empty())
    {
        LOG(INFO) << "destruct replay_cmd_list_ on object: ";
        replay_cmd_list = nullptr;
    }
}

/**
 * The replayed commands must apply in order. Replayed commands are first
 * stored in replay_cmd_list_ and committed in order.
 * @param obj_ver
 * @param commit_ts
 * @param cmd_list
 * @param cur_ver
 */
template <class T>
void EmplaceAndCommitReplayTxnCommand(
    std::shared_ptr<T> &payload,
    std::unique_ptr<ReplayTxnCmdList> &replay_cmd_list,
    TxnCmd &txn_cmd,
    uint64_t &cur_ver)
{
    if (replay_cmd_list == nullptr)
    {
        replay_cmd_list = std::make_unique<ReplayTxnCmdList>();
        replay_cmd_list->cur_version_ = cur_ver;
    }

    if (txn_cmd.obj_version_ < cur_ver)
    {
        // discard the obsolete txn command
        LOG(INFO) << "discard TxnCmd with a version smaller than cur_ver";
        return;
    }

    replay_cmd_list->EmplaceTxnCmd(txn_cmd);

    TryCommitReplayCommands(payload, replay_cmd_list, cur_ver);
}

}  // namespace txservice
