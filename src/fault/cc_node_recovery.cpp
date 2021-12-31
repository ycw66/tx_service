#include "fault/cc_node_recovery.h"

#include <brpc/channel.h>

#include "../log_service/include/log_agent.h"
#include "cc/local_cc_shards.h"
#include "cc_request.h"
#include "sharder.h"
#include "txlog.h"

namespace txservice::fault
{
CcNodeRecoveryAgent::CcNodeRecoveryAgent(NodeGroupId ng_id,
                                         int64_t term,
                                         const std::string &ip,
                                         uint16_t port,
                                         LocalCcShards &local_shards)
    : finish_(false),
      ng_id_(ng_id),
      term_(term),
      ip_(ip),
      port_(port),
      local_shards_(local_shards)
{
    notify_thd_ = std::thread(
        [this]
        {
            // send ReplayLog request to all the log groups of LogService, since
            // one phase commit shuffles the redo logs to every log groups and
            // thus requires full recovery.
            std::unique_ptr<TxLog> log_agent =
                Sharder::Instance().GetLogAgent();

            log_agent->ReplayLog(ng_id_, term_, ip_, port_, finish_);

            brpc::Channel channel;

            while (!finish_.load(std::memory_order_acquire))
            {
                RecoverTxInfo recover_tx_info;

                {
                    std::unique_lock<std::mutex> lk(queue_mux_);
                    queue_cv_.wait(
                        lk,
                        [this]()
                        {
                            return recover_tx_queue_.size() > 0 ||
                                   finish_.load(std::memory_order_acquire);
                        });

                    if (finish_.load(std::memory_order_acquire))
                    {
                        break;
                    }

                    recover_tx_info = recover_tx_queue_.front();
                    recover_tx_queue_.pop_front();
                }

                // Recovering a tx's lock consists of two parts: (1) inquires
                // the tx status in the cc node in which the tx resides, and (2)
                // if the tx's status is committed or the tx is not found,
                // checks the tx status in the log group.

                // The tx node ID is represented by the higher 4 bytes, in which
                // the lower 10 bits represents the local core ID.
                uint32_t tx_ng = (recover_tx_info.tx_number_ >> 32L) >> 10;
                uint32_t tx_leader = Sharder::Instance().LeaderNodeId(tx_ng);

                std::string tx_ip;
                uint16_t tx_port;
                Sharder::Instance().GetNodeAddress(tx_leader, tx_ip, tx_port);

                if (channel.Init(tx_ip.c_str(), tx_port + 1, nullptr) != 0)
                {
                    // Fails to establish the channel to the tx node. Silently
                    // returns. The tx will be recovered again by next
                    // conflicting tx.
                    LOG(ERROR)
                        << "Fail to init the channel to the leader of ng#"
                        << tx_ng << " for tx lock recovery.";
                    continue;
                }

                remote::CcRpcService_Stub stub(&channel);

                remote::CheckTxStatusRequest req;
                req.set_tx_number(recover_tx_info.tx_number_);
                req.set_tx_term(recover_tx_info.tx_term_);
                remote::CheckTxStatusResponse res;

                brpc::Controller cntl;
                stub.CheckTxStatus(&cntl, &req, &res, nullptr);

                if (cntl.Failed())
                {
                    LOG(ERROR) << "Fail to check the tx status in ng#" << tx_ng
                               << ". Error code: " << cntl.ErrorCode()
                               << ". Msg: " << cntl.ErrorText();
                    continue;
                }

                remote::CheckTxStatusResponse_TxStatus tx_status =
                    res.tx_status();

                if (tx_status == remote::CheckTxStatusResponse_TxStatus_ONGOING)
                {
                    LOG(INFO) << "The tx " << recover_tx_info.tx_number_
                              << " is ongoing. Does nothing for recovery.";
                    continue;
                }
                else if (tx_status ==
                         remote::CheckTxStatusResponse_TxStatus_ABORTED)
                {
                    LOG(INFO) << "The tx" << recover_tx_info.tx_number_
                              << " has aborted. Clears the tx's lock.";
                    ClearTx(recover_tx_info.tx_number_);
                }
                else
                {
                    // The tx is either committed or not found in the tx's cc
                    // node, either because the tx node fails or because the tx
                    // didn't finish post-processing but decided to move on.
                    // In either case, asks the log group: if the tx has
                    // committed, the log group ships the tx's log record to the
                    // cc node to recover the committed record. Or, the tx must
                    // have aborted.

                    RecoverTxStatus status =
                        log_agent->RecoverTx(recover_tx_info.tx_number_,
                                             recover_tx_info.tx_term_,
                                             recover_tx_info.cc_ng_id_,
                                             recover_tx_info.cc_ng_term_);

                    if (status == RecoverTxStatus::NotCommitted ||
                        status == RecoverTxStatus::Alive)
                    {
                        LOG(INFO) << "The tx is to be cleared, after asking "
                                     "the log group.";

                        // If the tx is not committed, sends a cc request to
                        // local cc shards to clear write intentions left by the
                        // tx. If the tx node is still alive according to the
                        // log group, and yet no log record is found, given that
                        // the prior inquiry of the tx status is inconclusive,
                        // the tx must have aborted proactively. Clears the tx's
                        // locks.
                        ClearTx(recover_tx_info.tx_number_);
                    }
                    else if (status == RecoverTxStatus::RecoverError)
                    {
                        LOG(INFO) << "There is a tx recovery error when asking "
                                     "the log group.";
                    }
                    else
                    {
                        LOG(INFO) << "The tx to be recovered has committed.";
                    }
                    // If the tx has committed, the log group will ship the tx's
                    // committed records to the cc node. If there is an error,
                    // does nothing. The next conflicting tx will try a new
                    // recovery.
                }
            }
        });
}

CcNodeRecoveryAgent ::~CcNodeRecoveryAgent()
{
    {
        std::unique_lock<std::mutex> lk(queue_mux_);
        finish_.store(true, std::memory_order_release);
    }
    queue_cv_.notify_one();

    notify_thd_.join();
}

void CcNodeRecoveryAgent::RecoverTx(uint64_t tx_number,
                                    int64_t tx_term,
                                    uint32_t cc_ng_id,
                                    int64_t cc_ng_term)
{
    std::unique_lock<std::mutex> lk(queue_mux_);
    recover_tx_queue_.push_back(
        RecoverTxInfo(tx_number, tx_term, cc_ng_id, cc_ng_term));
    queue_cv_.notify_one();
}

void CcNodeRecoveryAgent::ClearTx(TxNumber tx_number)
{
    ClearTxCc req(local_shards_.Count());
    req.Set(tx_number);

    for (uint32_t core_id = 0; core_id < local_shards_.Count(); ++core_id)
    {
        local_shards_.EnqueueCcRequest(core_id, &req);
    }

    req.Wait();
}
}  // namespace txservice::fault