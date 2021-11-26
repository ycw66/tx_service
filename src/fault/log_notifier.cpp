#include "fault/log_notifier.h"

#include "../log_service/include/log_agent.h"
#include "cc/local_cc_shards.h"
#include "cc_request.h"
#include "sharder.h"
#include "txlog.h"

namespace txservice::fault
{
LogNotifier::LogNotifier(NodeGroupId ng_id,
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
            std::unique_ptr<TxLog> log_agent =
                Sharder::Instance().GetLogAgent();

            // Notify the Log Sevice to send uncheckpointed redo log to the new
            // CC Node leader to replay redo log.
            log_agent->ReplayLog(ng_id_, term_, ip_, port_, finish_);

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

                // RecoverTx() is a synchronous call. If the call fails (because
                // the target log group is unavailable), we do not retry. Future
                // transactions will discover the conflicting orhpan lock and
                // re-start the recovery again.
                RecoverTxStatus status =
                    log_agent->RecoverTx(recover_tx_info.tx_number_,
                                         recover_tx_info.tx_term_,
                                         recover_tx_info.cc_ng_id_,
                                         recover_tx_info.cc_ng_term_);

                if (status == RecoverTxStatus::NotCommitted)
                {
                    // If the tx is not committed, sends a cc request to local
                    // cc shards to clear write intentions left by the tx. If
                    // the tx has committed, the log group will push the tx's
                    // log records to the cc node group separately. If the tx
                    // is still alive, does nothing. If the rpc of checking the
                    // tx's status returns with an error, leaves the tx as it
                    // is. Later conflicting tx's will retry recovery.
                    ClearTxCc req(local_shards_.Count());
                    req.Set(recover_tx_info.tx_number_);

                    for (uint32_t core_id = 0; core_id < local_shards_.Count();
                         ++core_id)
                    {
                        local_shards_.EnqueueCcRequest(core_id, &req);
                    }

                    req.Wait();
                }
            }
        });
}

LogNotifier ::~LogNotifier()
{
    finish_.store(true, std::memory_order_release);
    queue_cv_.notify_one();
    notify_thd_.join();
}

void LogNotifier::RecoverTx(uint64_t tx_number,
                            int64_t tx_term,
                            uint32_t cc_ng_id,
                            int64_t cc_ng_term)
{
    std::unique_lock<std::mutex> lk(queue_mux_);
    recover_tx_queue_.push_back(
        RecoverTxInfo(tx_number, tx_term, cc_ng_id, cc_ng_term));
    queue_cv_.notify_one();
}
}  // namespace txservice::fault