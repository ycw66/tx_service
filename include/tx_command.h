#pragma once

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "tx_key.h"
#include "tx_record.h"

namespace txservice
{
struct TxObject;
struct LruEntry;
class CcShard;

struct TxCommandResult
{
public:
    virtual ~TxCommandResult() = default;
    virtual void Serialize(std::string &buf) const = 0;
    virtual void Deserialize(const char *buf, size_t &offset) = 0;
};

enum class ExecResult
{
    Fail,   // Failed to execute command
    Read,   // Success to execute readonly command
    Write,  // Succes to execute the command and modified object.
    Block,  // The command is blocked
    Unlock  // There has not expected result and release ccentry lock
};

enum class BlockOperation
{
    NoBlock,     // Not block operation type
    PopBlock,    // Pop an element if has or block until expired or insert an
                 // element
    PopNoBlock,  // Pop an element if has or return empty.
    BlockLock,   // BLock on the object until the object has at least one
                 // element, then lock the obj and return
    PopElement,  // Pop the element, only used after BlockLock.
    Discard      // To discard the blocked command
};

struct TxCommand
{
public:
    virtual ~TxCommand() = default;
    virtual std::unique_ptr<TxCommand> Clone() = 0;
    virtual bool IsReadOnly() const = 0;
    // If this value overwrites old value.
    virtual bool IsOverwrite() const
    {
        return false;
    }
    // If this commands does not need previous object value. Note that
    // this is different with IsOverwrite since some of the commands overwrites
    // old value but need to return the status / value of the old object.
    virtual bool IgnoreKvValue() const
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
     * @return return ExecResult, ObjectCcMap.Execute will has different
     * response with different return value.
     */
    virtual ExecResult ExecuteOn(const TxObject &object) = 0;

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
        assert(false);
    }

    // deserialize a command from binary blob for processing remote request and
    // replaying log
    virtual void Deserialize(std::string_view cmd_img)
    {
        assert(false);
    }

    virtual TxCommandResult *GetResult() = 0;

    // To Judge if this command passed to execute, or failed
    // The default result is true;
    // If a transaction need to execute more than one command, and one of them
    // failed, it need to set all command that has executed abort. So it need
    // RedisServiceImpl::SimpleCommand and other methods call this methods to
    // know if this command passed or failed, then decide if the transaction is
    // continue or abort.
    virtual bool IsPassed() const
    {
        // TODO(lzx): replace "ObjectCommandResult::cmd_success_" with this.
        return true;
    }
    // If this command will be existing until the transaction committed.
    // True: It will be destroy after execute and need to clone for commit.
    // False: It will always exist until commited.
    virtual bool IsVolatile() = 0;
    // The default value is not need to clone, for lua, it should call this
    // method to set Volatile to true
    virtual void SetVolatile() = 0;

    // Pop a blocked request from the queue if exist. The reason to add this
    // method is due to it maybe needs some conditions with the related object.
    // for example the object should has elements.
    virtual bool AblePopBlockRequest(TxObject *object) const
    {
        return false;
    }

    virtual BlockOperation GetBlockOperationType()
    {
        return BlockOperation::NoBlock;
    }
};

/**
 * Commands that operate on multiple keys, like MSET, DEL.
 */
struct MultiObjectTxCommand
{
    virtual ~MultiObjectTxCommand() = default;

    virtual std::vector<TxKey> *KeyPointers() = 0;

    virtual std::vector<TxCommand *> *CommandPointers() = 0;

    // For block commands, it need to rewrite below 4 methods to support
    // flexible steps.
    virtual bool IsFinished() = 0;
    virtual bool IsLastStep() = 0;
    virtual size_t CmdSteps() = 0;

    virtual void IncrSteps() = 0;
    // If it has two parts of commands and finished to run the first part, it
    // should call below method to collect the result and fill the second part
    // of commands, then run the second part.
    //@return true: Need to run the second part of command; false: Not need to
    // run
    virtual bool HandleMiddleResult()
    {
        assert(false);
        return false;
    }

    // To judge if all commands passed. If at least one command failed, return
    // false, or return true. If is_two_parts_=true, it will according to
    // is_second_time_ to judge the first part or the second part.
    // The default return is true
    virtual bool IsPassed() const
    {
        return true;
    }

    // To judge if this block command is expired or not.
    virtual bool IsExpired() const
    {
        return false;
    }
    // The number of finished block commands. For block commands, they are not
    // need to wait all block commands to finished, one or some of them are
    // finished, the results can be satisfied, and it need the surplus commands
    // to abort. If return 0. means it is not block command.
    // Not all steps have blocked commands, maybe only one step has, other steps
    // should return 0
    virtual uint32_t NumOfFinishBlockCommands() const
    {
        return 0;
    }
    // Only called when NumOfFinishBlockCommands()>0 and (expired or the related
    // commands have finished). In this method, it will decide which child
    // commands should be discard and which is the next step to run.
    // @return true: need to discard obsolete cc request and go to next step;
    //          false: Only go to the next step and wait all local cc request
    //          to finish
    virtual bool ForwardResult()
    {
        assert(false);
        return false;
    }
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

    bool IsNull() const
    {
        return txn_cmd_list_.empty();
    }

    void Clear()
    {
        txn_cmd_list_.shrink_to_fit();
    }

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
void TryCommitReplayCommands(std::unique_ptr<T> &payload,
                             ReplayTxnCmdList &replay_cmd_list,
                             uint64_t &cur_ver)
{
    std::vector<TxnCmd> &txn_cmd_list = replay_cmd_list.txn_cmd_list_;
    assert(!txn_cmd_list.empty());
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
        // apply the commands of this txn
        for (auto &cmd : it->cmd_list_)
        {
            if (payload == nullptr)
            {
                std::unique_ptr<TxRecord> obj_ptr = cmd->CreateObject(nullptr);
                payload.reset(static_cast<T *>(obj_ptr.release()));
            }
            TxObject *obj_ptr = payload.get();
            TxObject *new_obj_ptr = cmd->CommitOn(obj_ptr);
            if (new_obj_ptr != obj_ptr)
            {
                // FIXME(lzx): should we use "new_obj_ptr->Clone()" ?
                payload.reset(static_cast<T *>(new_obj_ptr));
            }
        }
        cur_ver = it->new_version_;
        DLOG(INFO) << "commit replay txn cmds, obj ver: " << it->obj_version_
                   << ", new ver: " << it->new_version_;
        it = txn_cmd_list.erase(it);
    }

    if (txn_cmd_list.empty())
    {
        DLOG(INFO) << "destruct replay_cmd_list_ on object";
        replay_cmd_list.Clear();
    }
    else
    {
        DLOG(INFO) << "replay not finished: ";
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
void EmplaceAndCommitReplayTxnCommand(std::unique_ptr<T> &payload,
                                      ReplayTxnCmdList &replay_cmd_list,
                                      TxnCmd &txn_cmd,
                                      uint64_t &cur_ver,
                                      RecordStatus &status)
{
    bool waiting_for_fetch =
        status == RecordStatus::Unknown && !replay_cmd_list.IsNull();
    if (replay_cmd_list.IsNull())
    {
        replay_cmd_list.cur_version_ = cur_ver;
    }

    replay_cmd_list.EmplaceTxnCmd(txn_cmd);

    if (!waiting_for_fetch || txn_cmd.has_del_)
    {
        TryCommitReplayCommands(payload, replay_cmd_list, cur_ver);
        status =
            payload == nullptr ? RecordStatus::Deleted : RecordStatus::Normal;
    }
}

}  // namespace txservice
