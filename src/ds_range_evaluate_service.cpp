#include "ds_range_evaluate_service.h"

#include "local_cc_shards.h"
#include "tx_request.h"
#include "tx_service.h"
#include "tx_util.h"
#include "util.h"

namespace txservice
{
DsRangeEvaluateOperationService::DsRangeEvaluateOperationService(
    const txservice::LocalCcShards &local_shards)
    : local_cc_shards_(local_shards), quit_indicator_(false)
{
    worker_thread_ = std::thread(
        [this]
        {
            while (!quit_indicator_.load(std::memory_order_acquire))
            {
                std::unique_lock<std::mutex> lk(queue_mutex_);
                queue_cv_.wait(
                    lk,
                    [this]
                    {
                        return !ds_evaluate_range_size_work_queue_.empty() ||
                               quit_indicator_.load(std::memory_order_acquire);
                    });

                if (quit_indicator_.load(std::memory_order_acquire))
                {
                    lk.unlock();
                    break;
                }
                else if (!ds_evaluate_range_size_work_queue_.empty())
                {
                    DsEvaluateRangeSizeWorkSettings ws =
                        std::move(ds_evaluate_range_size_work_queue_.front());
                    assert(!ws.table_name_.IsStringOwner() &&
                           ws.table_name_.Type() ==
                               txservice::TableType::RangePartition);
                    ds_evaluate_range_size_work_queue_.pop_front();
                    lk.unlock();

                    for (auto it = ws.ranges_.begin(); it != ws.ranges_.end();
                         it++)
                    {
                        int32_t partition_id = it->first;
                        const TxKey *range_key = it->second;
                        int64_t range_size = 0;

                        TxService *tx_service = local_cc_shards_.tx_service_;
                        TransactionExecution *txm = txservice::NewTxInit(
                            tx_service,
                            txservice::IsolationLevel::RepeatableRead,
                            txservice::CcProtocol::Locking);
                        if (txm == nullptr)
                        {
                            break;
                        }
                        // check whether this node is group leader, pin its
                        // data if it is
                        uint32_t node_group_id = txm->TxCcNodeId();
                        int64_t leader_term =
                            Sharder::Instance().TryPinNodeGroupData(
                                node_group_id);
                        if (leader_term < 0)
                        {
                            break;
                        }
                        CatalogKey table_key(ws.table_name_);
                        CatalogRecord catalog_rec;
                        txservice::ReadTxRequest read_catalog_tx_req;
                        read_catalog_tx_req.Reset();
                        read_catalog_tx_req.Set(&txservice::catalog_ccm_name,
                                                &table_key,
                                                &catalog_rec,
                                                LockType::ReadLock,
                                                true);
                        txm->Execute(&read_catalog_tx_req);
                        read_catalog_tx_req.Wait();
                        if (read_catalog_tx_req.IsError() ||
                            read_catalog_tx_req.Result() !=
                                RecordStatus::Normal)
                        {
                            txservice::AbortTx(txm);
                            // finish checkpoint on this node group, unpin
                            // its data and clear its ccmaps and catalogs if
                            // it is no longer leader
                            Sharder::Instance().UnpinNodeGroupData(
                                node_group_id);
                            continue;
                        }
                        local_cc_shards_.store_hd_->GetRangeSize(
                            catalog_rec.Schema(), partition_id, &range_size);
                        txservice::CommitTxRequest commit_tx_req;
                        commit_tx_req.Reset();
                        txm->Execute(&commit_tx_req);
                        commit_tx_req.Wait();
                        bool success = commit_tx_req.Result();
                        if (!success)
                        {
                            // finish checkpoint on this node group, unpin
                            // its data and clear its ccmaps and catalogs if
                            // it is no longer leader
                            Sharder::Instance().UnpinNodeGroupData(
                                node_group_id);
                            continue;
                        }
                        Sharder::Instance().UnpinNodeGroupData(node_group_id);

                        // TODO(XIAO JI): Here need a comprehensive trigger
                        // criteria
                        if (range_size > 10)
                        {
                            // Read table schema and start split range operation
                            txm = txservice::NewTxInit(
                                tx_service,
                                txservice::IsolationLevel::RepeatableRead,
                                txservice::CcProtocol::Locking);

                            node_group_id = txm->TxCcNodeId();
                            leader_term =
                                Sharder::Instance().TryPinNodeGroupData(
                                    node_group_id);
                            if (leader_term < 0)
                            {
                                break;
                            }
                            // Read table schema
                            CatalogRecord catalog_rec;
                            read_catalog_tx_req.Reset();
                            read_catalog_tx_req.Set(
                                &txservice::catalog_ccm_name,
                                &table_key,
                                &catalog_rec,
                                LockType::ReadLock,
                                true);
                            txm->Execute(&read_catalog_tx_req);
                            read_catalog_tx_req.Wait();
                            if (read_catalog_tx_req.IsError() ||
                                read_catalog_tx_req.Result() !=
                                    RecordStatus::Normal)
                            {
                                txservice::AbortTx(txm);
                                // finish checkpoint on this node group, unpin
                                // its data and clear its ccmaps and catalogs if
                                // it is no longer leader
                                Sharder::Instance().UnpinNodeGroupData(
                                    node_group_id);
                                continue;
                            }

                            // Read range record
                            txservice::RangeRecord range_record;
                            txservice::ReadTxRequest read_range_tx_req;
                            read_range_tx_req.Reset();
                            read_range_tx_req.Set(&ws.table_name_,
                                                  range_key,
                                                  &range_record,
                                                  txservice::LockType::ReadLock,
                                                  true);
                            txm->Execute(&read_range_tx_req);
                            read_range_tx_req.Wait();
                            if (read_range_tx_req.IsError() ||
                                read_range_tx_req.Result() !=
                                    RecordStatus::Normal)
                            {
                                txservice::AbortTx(txm);
                                // finish checkpoint on this node group, unpin
                                // its data and clear its ccmaps and catalogs if
                                // it is no longer leader
                                Sharder::Instance().UnpinNodeGroupData(
                                    node_group_id);
                                continue;
                            }

                            // Split range request
                            txservice::SplitRangeTxRequest range_split_req(
                                ws.table_name_,
                                catalog_rec.Schema(),
                                range_key,
                                &range_record);
                            txm->Execute(&range_split_req);
                            range_split_req.Wait();

                            if (range_split_req.IsError())
                            {
                                txservice::AbortTx(txm);
                                // Print error message
                                // TODO(Xiao Ji): How to handle split failure
                                break;
                            }

                            commit_tx_req.Reset();
                            txm->Execute(&commit_tx_req);
                            commit_tx_req.Wait();
                            success = commit_tx_req.Result();
                            if (!success)
                            {
                                // Print error message
                                // TODO(Xiao Ji): How to handle split failure
                            }
                            // finish checkpoint on this node group, unpin
                            // its data and clear its ccmaps and catalogs if
                            // it is no longer leader
                            Sharder::Instance().UnpinNodeGroupData(
                                node_group_id);
                        }
                    }
                }
            }
        });
}

void DsRangeEvaluateOperationService::SubmitEvaluateRangeSizeWork(
    const txservice::TableName &range_table_name,
    std::map<int32_t, const TxKey *> &&ranges)
{
    std::unique_lock lk(queue_mutex_);
    DsEvaluateRangeSizeWorkSettings ws(range_table_name, std::move(ranges));
    ds_evaluate_range_size_work_queue_.emplace_back(std::move(ws));
    queue_cv_.notify_one();
}

void DsRangeEvaluateOperationService::Shutdown()
{
    {
        std::unique_lock<std::mutex> lk(queue_mutex_);
        quit_indicator_.store(true, std::memory_order_release);
        queue_cv_.notify_one();
    }
    worker_thread_.join();
}
}  // namespace txservice
