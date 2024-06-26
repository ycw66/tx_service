#include "sk_generator.h"

#include <mutex>
#include <optional>

#include "error_messages.h"
#include "tx_request.h"
#include "tx_service.h"

namespace txservice
{
void SkGenerator::GenerateSkFromPk(TxKey start_key,
                                   TxKey end_key,
                                   uint64_t scan_ts,
                                   std::vector<TableName> &new_indexes_name,
                                   size_t &scanned_pk_count,
                                   CcErrorCode &res_code,
                                   GenerateSkStatus &task_status)
{
    int64_t ng_term = Sharder::Instance().TryPinNodeGroupData(node_group_id_);
    if (ng_term < 0)
    {
        LOG(WARNING) << "GenerateSkFromPk: Generate sk from pk on non-leader "
                     << "node for partition: " << partition_id_ << " of ng#"
                     << node_group_id_ << ", terminate directly.";
        res_code = CcErrorCode::REQUESTED_NODE_NOT_LEADER;
        return;
    }
    // guard to unpin node group on finish.
    std::shared_ptr<void> defer_unpin(
        nullptr,
        [this](void *)
        {
            Sharder::Instance().UnpinNodeGroupData(node_group_id_);
            upload_batch_worker_ctx_.Terminate();
        });

    LOG(INFO) << "GenerateSkFromPk: Generate sk from pk on range#"
              << partition_id_
              << " for base table: " << base_table_name_.Trace() << " of ng#"
              << node_group_id_;

    const TableName &range_table_name =
        TableName(base_table_name_.StringView(), TableType::RangePartition);
    LocalCcShards *cc_shards = Sharder::Instance().GetLocalCcShards();
    TransactionExecution *acq_range_lock_txm =
        cc_shards->GetTxService()->NewTx();
    InitTxRequest init_req;
    init_req.iso_level_ = IsolationLevel::RepeatableRead;
    init_req.protocol_ = CcProtocol::Locking;
    init_req.tx_ng_id_ = node_group_id_;
    // Init the txm until succeed or the node is not leader.
    do
    {
        init_req.Reset();
        acq_range_lock_txm->Execute(&init_req);
        init_req.Wait();
        if (init_req.IsError())
        {
            if (Sharder::Instance().LeaderTerm(node_group_id_) < 0)
            {
                LOG(ERROR) << "GenerateSkFromPk: Node not leader of ng#"
                           << node_group_id_;
                res_code = CcErrorCode::REQUESTED_NODE_NOT_LEADER;
                return;
            }
            LOG(ERROR)
                << "GenerateSkFromPk: Init acquire range txm failed for table: "
                << base_table_name_.Trace() << " of ng#" << node_group_id_
                << ", with error: " << init_req.ErrorMsg()
                << ". Retry after 3s.";
            std::this_thread::sleep_for(3s);
        }
    } while (init_req.IsError());

    // Acquire the range read lock
    ReadTxRequest read_range_req;
    RangeRecord range_rec;
    read_range_req.Set(
        &range_table_name, &start_key, &range_rec, false, false, true);
    read_range_req.Reset();
    acq_range_lock_txm->Execute(&read_range_req);
    read_range_req.Wait();
    if (read_range_req.IsError())
    {
        // This read operation might fail if it's blocked by a write
        // lock acquired by range split.
        LOG(ERROR) << "GenerateSkFromPk: Acquire range#" << partition_id_
                   << " read lock failed for table: "
                   << base_table_name_.Trace() << " of ng#" << node_group_id_
                   << ", with error: " << read_range_req.ErrorMsg();
        res_code = CcErrorCode::GET_RANGE_ID_ERR;
        return;
    }

    CommitTxRequest commit_req;
    // Check the range boundary.
    TxKey range_end_key = range_rec.GetRangeInfo()->EndTxKey();

    if (!(range_end_key == end_key))
    {
        // The range have changed, return error.
        LOG(ERROR) << "GenerateSkFromPk: The boundary of range#"
                   << partition_id_
                   << " has changed for table: " << base_table_name_.Trace()
                   << " of ng#" << node_group_id_
                   << ". Terminated this range task.";
        res_code = CcErrorCode::GET_RANGE_ID_ERR;
        // Release the range read locks.
        acq_range_lock_txm->CommitTx(commit_req);
        return;
    }

    res_code = CcErrorCode::NO_ERROR;
    scanned_pk_count = 0;
    uint32_t ng_cnt = Sharder::Instance().NodeGroupCount();
    leader_terms_.resize(ng_cnt, INIT_TERM);

    res_code = ScanPkAndGenerateSk(&start_key,
                                   &end_key,
                                   scan_ts,
                                   ng_term,
                                   acq_range_lock_txm->TxNumber(),
                                   new_indexes_name,
                                   scanned_pk_count,
                                   task_status);

    // Release the range read locks.
    acq_range_lock_txm->CommitTx(commit_req);

    {
        std::unique_lock<std::mutex> upload_sender_lk(upload_sender_mux_);
        // Wait until no ongoing task.
        upload_sender_cv_.wait(upload_sender_lk,
                               [this]()
                               { return ongoing_upload_task_size_ == 0; });
    }
    defer_unpin.reset();
    LOG(INFO) << "GenerateSkFromPk: Finished generate sk from range#"
              << partition_id_
              << " for base table: " << base_table_name_.Trace() << " of ng#"
              << node_group_id_ << " with result: " << CcErrorMessage(res_code);
}

void SkGenerator::RemoteGenerateSkFromPk(
    const std::string &start_key_str,
    const std::string &end_key_str,
    uint64_t scan_ts,
    std::vector<TableName> &new_indexes_name,
    size_t &scanned_pk_count,
    CcErrorCode &res_code,
    GenerateSkStatus &task_status)
{
    int32_t ng_term = Sharder::Instance().TryPinNodeGroupData(node_group_id_);
    if (ng_term < 0)
    {
        LOG(WARNING) << "RemoteGenerateSkFromPk: Generate sk from pk on "
                     << "non-leader node for partition id: " << partition_id_
                     << " of ng#" << node_group_id_ << ", terminate directly.";
        res_code = CcErrorCode::REQUESTED_NODE_NOT_LEADER;
        return;
    }
    // guard to unpin node group on finish.
    std::shared_ptr<void> defer_unpin(
        nullptr,
        [this](void *)
        {
            Sharder::Instance().UnpinNodeGroupData(node_group_id_);
            upload_batch_worker_ctx_.Terminate();
        });

    LOG(INFO) << "RemoteGenerateSkFromPk: Generate sk from pk on range#"
              << partition_id_
              << " for base table: " << base_table_name_.Trace() << " of ng#"
              << node_group_id_;

    CODE_FAULT_INJECTOR("term_AlterTableIndex_RangeBoundaryMismatch", {
        // The range have changed, return error.
        DLOG(ERROR) << "The boundary of range#" << partition_id_
                    << " has changed for table: " << base_table_name_.Trace()
                    << " of ng#" << node_group_id_ << ". Terminated this task.";
        res_code = CcErrorCode::GET_RANGE_ID_ERR;
        FaultInject::Instance().InjectFault(
            "term_AlterTableIndex_RangeBoundaryMismatch", "remove");
        return;
    });

    const TableName &range_table_name =
        TableName(base_table_name_.StringView(), TableType::RangePartition);
    LocalCcShards *cc_shards = Sharder::Instance().GetLocalCcShards();
    TransactionExecution *acq_range_lock_txm =
        cc_shards->GetTxService()->NewTx();
    InitTxRequest init_req;
    init_req.iso_level_ = IsolationLevel::RepeatableRead;
    init_req.protocol_ = CcProtocol::Locking;
    init_req.tx_ng_id_ = node_group_id_;
    // Init the txm until succeed or the node is not leader.
    do
    {
        init_req.Reset();
        acq_range_lock_txm->Execute(&init_req);
        init_req.Wait();
        if (init_req.IsError())
        {
            if (Sharder::Instance().LeaderTerm(node_group_id_) < 0)
            {
                LOG(ERROR) << "RemoteGenerateSkFromPk: Node not leader of ng#"
                           << node_group_id_;
                res_code = CcErrorCode::REQUESTED_NODE_NOT_LEADER;
                return;
            }
            LOG(ERROR) << "RemoteGenerateSkFromPk: Init acquire range txm "
                       << "failed for table: " << base_table_name_.Trace()
                       << " of ng#" << node_group_id_
                       << ", with error: " << init_req.ErrorMsg()
                       << ". Retry after 3s.";
            std::this_thread::sleep_for(3s);
        }
    } while (init_req.IsError());

    // Acquire the range read lock
    ReadTxRequest read_range_req;
    RangeRecord range_rec;
    TxKey range_start_key;
    if (start_key_str.size() > 0)
    {
        read_range_req.Set(
            &range_table_name, &start_key_str, &range_rec, false, false, true);
    }
    else
    {
        range_start_key = cc_shards->GetCatalogFactory()->NegativeInfKey();
        read_range_req.Set(&range_table_name,
                           &range_start_key,
                           &range_rec,
                           false,
                           false,
                           true);
    }
    read_range_req.Reset();
    acq_range_lock_txm->Execute(&read_range_req);
    read_range_req.Wait();
    if (read_range_req.IsError())
    {
        // This read operation might fail if it's blocked by a write
        // lock acquired by range split.
        LOG(ERROR) << "RemoteGenerateSkFromPk: Acquire range#" << partition_id_
                   << " read lock failed for table: "
                   << base_table_name_.Trace() << " of ng#" << node_group_id_
                   << ", with error: " << read_range_req.ErrorMsg();
        res_code = CcErrorCode::GET_RANGE_ID_ERR;
        return;
    }

    CommitTxRequest commit_req;
    // Check the range boundary.
    range_start_key = range_rec.GetRangeInfo()->StartTxKey();
    TxKey range_end_key = range_rec.GetRangeInfo()->EndTxKey();

    std::string serialized_end_key;
    if (range_end_key.Type() == KeyType::Normal)
    {
        range_end_key.Serialize(serialized_end_key);
    }
    if (serialized_end_key.length() != end_key_str.length() ||
        serialized_end_key.compare(end_key_str))
    {
        // The range have changed, return error.
        LOG(ERROR) << "RemoteGenerateSkFromPk: The boundary of range#"
                   << partition_id_
                   << " has changed for table: " << base_table_name_.Trace()
                   << " of ng#" << node_group_id_
                   << ". Terminated this range task.";
        res_code = CcErrorCode::GET_RANGE_ID_ERR;
        // Release the range read locks.
        acq_range_lock_txm->CommitTx(commit_req);
        return;
    }

    res_code = CcErrorCode::NO_ERROR;
    scanned_pk_count = 0;
    uint32_t ng_cnt = Sharder::Instance().NodeGroupCount();
    leader_terms_.resize(ng_cnt, INIT_TERM);

    res_code = ScanPkAndGenerateSk(&range_start_key,
                                   &range_end_key,
                                   scan_ts,
                                   ng_term,
                                   acq_range_lock_txm->TxNumber(),
                                   new_indexes_name,
                                   scanned_pk_count,
                                   task_status);

    // Release the range read locks.
    acq_range_lock_txm->CommitTx(commit_req);

    {
        std::unique_lock<std::mutex> upload_sender_lk(upload_sender_mux_);
        // Wait until no ongoing task.
        upload_sender_cv_.wait(upload_sender_lk,
                               [this]()
                               { return ongoing_upload_task_size_ == 0; });
    }
    defer_unpin.reset();
    LOG(INFO) << "RemoteGenerateSkFromPk: Finished generate sk from range#"
              << partition_id_
              << " for base table: " << base_table_name_.Trace() << " of ng#"
              << node_group_id_ << " with result: " << CcErrorMessage(res_code);
}

CcErrorCode SkGenerator::ScanPkAndGenerateSk(
    const TxKey *start_key,
    const TxKey *end_key,
    uint64_t scan_ts,
    int64_t ng_term,
    uint64_t tx_number,
    const std::vector<TableName> &new_indexes_name,
    size_t &scanned_pk_count,
    GenerateSkStatus &task_status)
{
    LocalCcShards *cc_shards = Sharder::Instance().GetLocalCcShards();
    auto catalog_entry =
        cc_shards->GetCatalog(base_table_name_, node_group_id_);
    TableSchema *table_schema =
        const_cast<TableSchema *>(catalog_entry->dirty_schema_.get());
    assert(table_schema != nullptr && new_indexes_name.size() > 0);
    size_t core_cnt = cc_shards->Count();

    DataSyncScanCc scan_req(base_table_name_,
                            0,
                            0,
                            scan_ts,
                            node_group_id_,
                            ng_term,
                            core_cnt,
                            scan_batch_size_,
                            tx_number,
                            start_key,
                            end_key,
                            false
#ifdef RANGE_PARTITION_ENABLED
                            ,
                            true,
                            true
#else
                            ,
                            false,
                            [](size_t hash_code) { return true; }
#endif
    );

    CcErrorCode scan_res = CcErrorCode::NO_ERROR;
    bool scan_data_drained = false;
    bool scan_pk_finished = false;
    std::vector<TxKey> last_finished_pos;
    last_finished_pos.reserve(core_cnt);
    for (size_t i = 0; i < core_cnt; ++i)
    {
        last_finished_pos.emplace_back(start_key->Clone());
    }

    std::vector<SkEncoder::uptr> sk_encoder_vec;
    sk_encoder_vec.reserve(new_indexes_name.size());
    TxKey target_key;
    const TxRecord *target_rec = nullptr;
    uint64_t version_ts = 0;

    size_t reserve_size = core_cnt * scan_batch_size_;
    size_t batch_tuples = 0;
    size_t key_position = 0;

    do
    {
        batch_tuples = 0;
        key_position = 0;
        for (size_t idx = 0; idx < core_cnt; ++idx)
        {
            cc_shards->EnqueueToCcShard(idx, &scan_req);
        }
        scan_req.Wait();

        if (scan_req.IsError())
        {
            scan_res = scan_req.ErrorCode();
            LOG(ERROR)
                << "ScanPkAndGenerateSk: Scan pk records failed on range#"
                << partition_id_
                << " for base table: " << base_table_name_.StringView()
                << " of ng#" << node_group_id_
                << " with error: " << CcErrorMessage(scan_res);
            if (scan_res == CcErrorCode::REQUESTED_NODE_NOT_LEADER ||
                scan_res == CcErrorCode::NG_TERM_CHANGED)
            {
                sk_encoder_vec.clear();
                break;
            }
            else if (scan_res == CcErrorCode::OUT_OF_MEMORY ||
                     scan_res == CcErrorCode::DATA_STORE_ERR)
            {
                bool is_waiting = false;
                do
                {
                    std::this_thread::sleep_for(std::chrono::seconds(30));
                    is_waiting = cc_shards->IsWaitingCkpt();
                    DLOG(INFO)
                        << "Can retry scan? "
                        << (!is_waiting ? "YES" : "NO! Continue sleep...");
                } while (is_waiting);
#ifndef ON_KEY_OBJECT
                // Reset the paused key.
                for (size_t i = 0; i < core_cnt; ++i)
                {
                    auto &paused_key = scan_req.PausePos(i).first;
                    if (!scan_req.IsDrained(i))
                    {
#ifdef RANGE_PARTITION_ENABLED
                        // Should use one copy of the key, instead of move the
                        // ownership of the key, because this round of scan may
                        // failed again.
                        assert(paused_key.IsOwner());
                        paused_key.Copy(last_finished_pos[i]);
#endif
                    }
                }
#endif
                scan_req.Reset();
                scan_pk_finished = false;
                scan_res = CcErrorCode::NO_ERROR;
                continue;
            }
            else
            {
                assert(false && "Unknown scan error.");
                sk_encoder_vec.clear();
                return scan_res;
            }
        }

        scan_data_drained = true;

        std::unique_lock<std::mutex> upload_sender_lk(upload_sender_mux_);
        if (ongoing_upload_task_size_ == SkGenerator::UploadBatchWorkerSize)
        {
            LOG(WARNING) << "ScanPkAndGenerateSk: Waitting the idle upload "
                         << "worker on range#" << partition_id_
                         << " for table: " << base_table_name_.Trace()
                         << " of ng#" << node_group_id_;
            // Wait until get free task slot.
            upload_sender_cv_.wait(
                upload_sender_lk,
                [this]() {
                    return ongoing_upload_task_size_ <
                           SkGenerator::UploadBatchWorkerSize;
                });
        }
        upload_sender_lk.unlock();

        std::unique_lock<std::mutex> task_lk(upload_batch_worker_ctx_.mux_);
        uint8_t free_task_slot =
            upload_task_head_ == UINT8_MAX
                ? 0
                : (upload_task_head_ + 1) % SkGenerator::UploadBatchWorkerSize;
        auto upload_task_status =
            upload_batch_queue_.at(free_task_slot).task_status_;
        while (upload_task_status != UploadTaskStatus::Free)
        {
            free_task_slot =
                (free_task_slot + 1) % SkGenerator::UploadBatchWorkerSize;
            upload_task_status =
                upload_batch_queue_.at(free_task_slot).task_status_;
        }
        auto &new_upload_task = upload_batch_queue_.at(free_task_slot);
        task_lk.unlock();
        for (auto index_it = new_indexes_name.cbegin();
             index_it != new_indexes_name.cend();
             ++index_it)
        {
            auto iter = new_upload_task.write_entry_set_.find(*index_it);
            if (iter == new_upload_task.write_entry_set_.end())
            {
                auto write_entry_it = new_upload_task.write_entry_set_.emplace(
                    std::piecewise_construct,
                    std::forward_as_tuple(index_it->StringView(),
                                          index_it->Type()),
                    std::forward_as_tuple(std::vector<WriteEntry>()));
                iter = write_entry_it.first;
                iter->second.reserve(reserve_size);
            }
            size_t vec_idx = index_it - new_indexes_name.cbegin();
            const SkEncoder *sk_encoder = nullptr;
            if (vec_idx >= sk_encoder_vec.size())
            {
                sk_encoder_vec.emplace_back(
                    std::move(table_schema->CreateSkEncoder(*index_it)));
            }
            sk_encoder = sk_encoder_vec[vec_idx].get();
            key_position = 0;

            for (size_t core_idx = 0; core_idx < core_cnt; ++core_idx)
            {
                for (size_t key_idx = 0;
                     key_idx < scan_req.accumulated_scan_cnt_.at(core_idx);
                     ++key_idx)
                {
                    auto &tuple = scan_req.DataSyncVec(core_idx).at(key_idx);
                    target_key = tuple.Key();
                    target_rec = tuple.Payload();
                    version_ts = tuple.commit_ts_;
                    if (tuple.payload_status_ == RecordStatus::Deleted)
                    {
                        // Skip the deleted record.
                        continue;
                    }
                    assert(target_key.KeyPtr() != nullptr &&
                           target_rec != nullptr);

                    auto packed_sk =
                        sk_encoder->GeneratePackedSk(&target_key, target_rec);

                    if (packed_sk.first.KeyPtr() == nullptr)
                    {
                        LOG(ERROR)
                            << "ScanPkAndGenerateSk: Failed to generate "
                            << "packed sk for index: " << index_it->StringView()
                            << "of ng#" << node_group_id_;
                        // Finish the pack sk operation
                        sk_encoder_vec.clear();
                        return CcErrorCode::PACK_SK_ERR;
                    }

                    if (key_position < iter->second.size())
                    {
                        iter->second.at(key_position).key_ =
                            std::move(packed_sk.first);
                        iter->second.at(key_position).rec_ =
                            std::move(packed_sk.second);
                        iter->second.at(key_position).commit_ts_ = version_ts;
                    }
                    else
                    {
                        iter->second.emplace_back(std::move(packed_sk.first),
                                                  std::move(packed_sk.second),
                                                  version_ts);
                    }
                    ++key_position;
                } /* End of each key */
                if (index_it == new_indexes_name.cbegin())
                {
                    batch_tuples += scan_req.accumulated_scan_cnt_.at(core_idx);
                    if (batch_tuples % 10240 == 0 &&
                        !task_status.CheckTxTermStatus())
                    {
                        LOG(WARNING)
                            << "ScanPkAndGenerateSk: Terminate this task cause "
                            << "the tx leader transferred of ng#"
                            << node_group_id_;
                        sk_encoder_vec.clear();
                        task_status.TerminateGenerateSk();
                        return CcErrorCode::TX_NODE_NOT_LEADER;
                    }
#ifndef ON_KEY_OBJECT
                    // Update the last finished key.
                    auto &paused_key = scan_req.PausePos(core_idx).first;
                    if (!scan_req.IsDrained(core_idx))
                    {
#ifdef RANGE_PARTITION_ENABLED
                        if (last_finished_pos[core_idx].IsOwner())
                        {
                            last_finished_pos[core_idx].Copy(paused_key);
                        }
                        else
                        {
                            last_finished_pos[core_idx] = paused_key.Clone();
                        }
#endif
                    }
#endif
                    // If the data is drained
                    scan_data_drained =
                        scan_req.IsDrained(core_idx) && scan_data_drained;
                }
            } /* End of each core */

            if (key_position < iter->second.size())
            {
                iter->second.erase(iter->second.begin() + key_position,
                                   iter->second.end());
            }
        } /* End of foreach new_indexes_name */

        scan_pk_finished = scan_data_drained;
        scan_req.Reset();
        scanned_pk_count += batch_tuples;
        if (batch_tuples > 0)
        {
            if (upload_batch_worker_ctx_.worker_thd_.size() == 0)
            {
                // Launch upload task worker.
                for (int idx = 0; idx < upload_batch_worker_ctx_.worker_num_;
                     ++idx)
                {
                    upload_batch_worker_ctx_.worker_thd_.push_back(
                        std::thread([this]() { UploadBatchWorker(); }));
                }
            }

            task_lk.lock();
            new_upload_task.task_status_ = UploadTaskStatus::Pending;
            upload_task_head_ = free_task_slot;
            ++pending_upload_task_size_;
            upload_batch_worker_ctx_.cv_.notify_one();
            task_lk.unlock();
        }

        upload_sender_lk.lock();
        scan_pk_finished =
            scan_pk_finished || upload_task_result_ != CcErrorCode::NO_ERROR;
        if (scan_pk_finished && !scan_data_drained)
        {
            LOG(ERROR) << "ScanPkAndGenerateSk: Terminate scan on range#"
                       << partition_id_
                       << " for table: " << base_table_name_.Trace() << "of ng#"
                       << node_group_id_ << " caused by upload task failed."
                       << static_cast<uint32_t>(upload_task_result_);
            scan_req.UnpinSlices();
            scan_res = upload_task_result_;
            assert(upload_task_result_ == CcErrorCode::TX_NODE_NOT_LEADER ||
                   upload_task_result_ ==
                       CcErrorCode::REQUESTED_NODE_NOT_LEADER);
        }
        upload_sender_lk.unlock();
    } while (!scan_pk_finished);

    sk_encoder_vec.clear();
    DLOG(INFO) << "ScanPkAndGenerateSk: Finish scan and generate sk on range#"
               << partition_id_ << " for table:" << base_table_name_.Trace()
               << " of ng#" << node_group_id_
               << " with pk tuples: " << scanned_pk_count;
    return scan_res;
}

CcErrorCode SkGenerator::UploadWithoutDataLog(
    UploadBatchTask &upload_task,
    std::vector<std::unique_ptr<UploadBatchCc>> &upload_req_pool)
{
    CcErrorCode res = CcErrorCode::NO_ERROR;
    // The relationship between one WriteEntry and another, this indicate that
    // for the specific node group, which TxKeys belong to it.
    std::unordered_map<TableName, NGWriteEntry> ng_write_set;
#ifdef RANGE_PARTITION_ENABLED
    LocalCcShards *cc_shards = Sharder::Instance().GetLocalCcShards();
    TransactionExecution *acq_range_lock_txm =
        cc_shards->GetTxService()->NewTx();

    InitTxRequest init_req;
    init_req.iso_level_ = IsolationLevel::RepeatableRead;
    init_req.protocol_ = CcProtocol::Locking;
    init_req.tx_ng_id_ = node_group_id_;

    acq_range_lock_txm->Execute(&init_req);
    init_req.Wait();

    if (init_req.IsError())
    {
        LOG(ERROR) << "UploadWithoutDataLog: Transaction node not leader of ng#"
                   << node_group_id_;
        return CcErrorCode::TX_NODE_NOT_LEADER;
    }

    res = AcquireRangeReadLocks(acq_range_lock_txm, upload_task, ng_write_set);
    if (res != CcErrorCode::NO_ERROR)
    {
        LOG(ERROR)
            << "UploadWithoutDataLog: Acquire range read locks failed of ng#"
            << node_group_id_
            << " with error code: " << static_cast<uint32_t>(res);
        return res;
    }
    DLOG(INFO) << "UploadWithoutDataLog: Upload batch sk generated from range# "
               << partition_id_
               << " for base table: " << base_table_name_.Trace() << " of ng#"
               << node_group_id_
               << " with txn: " << acq_range_lock_txm->TxNumber();
#else
    size_t hash = 0;
    uint32_t key_shard_code = 0;
    NodeGroupId dest_ng_id = 0;
    for (auto table_it = upload_task.write_entry_set_.begin();
         table_it != upload_task.write_entry_set_.end();
         ++table_it)
    {
        auto &table_write_entrys = table_it->second;
        auto ng_write_entry_it = ng_write_set.find(table_it->first);
        if (ng_write_entry_it == ng_write_set.end())
        {
            auto ins_it = ng_write_set.emplace(
                std::piecewise_construct,
                std::forward_as_tuple(table_it->first.StringView(),
                                      table_it->first.Type()),
                std::forward_as_tuple(NGWriteEntry()));
            ng_write_entry_it = ins_it.first;
        }
        auto &ng_table_write_entrys = ng_write_entry_it->second;
        for (auto item_it = table_write_entrys.begin();
             item_it != table_write_entrys.end();
             ++item_it)
        {
            hash = item_it->key_.Hash();
            key_shard_code = Sharder::Instance().ShardCode(hash);
            dest_ng_id = Sharder::Instance().ShardToCcNodeGroup(key_shard_code);
            auto ng_it = ng_table_write_entrys.try_emplace(dest_ng_id);
            ng_it.first->second.push_back(&(*item_it));
        }
    }
#endif

    res = UploadSkInternal(ng_write_set, upload_req_pool);

#ifdef RANGE_PARTITION_ENABLED
    ReleaseRangeReadLocks(acq_range_lock_txm, true);
#endif

    DLOG(INFO) << "UploadWithoutDataLog: Finished on range#" << partition_id_
               << " for base table: " << base_table_name_.Trace() << " of ng#"
               << node_group_id_ << " with result: " << CcErrorMessage(res);
    return res;
}

CcErrorCode SkGenerator::UploadSkInternal(
    std::unordered_map<TableName, NGWriteEntry> &ng_write_set,
    std::vector<std::unique_ptr<UploadBatchCc>> &upload_req_pool)
{
    size_t entry_vec_size = 0;
    size_t batch_req_cnt = 0;
    bthread::Mutex req_mux;
    bthread::ConditionVariable req_cv;
    size_t finished_upload_count = 0;
    CcErrorCode upload_res_code = CcErrorCode::NO_ERROR;
    size_t upload_req_count = 0;
    for (auto &[table_name, ng_entries] : ng_write_set)
    {
        for (auto &[ng_id, entry_vec] : ng_entries)
        {
            entry_vec_size = entry_vec.size();
            batch_req_cnt = (entry_vec_size / upload_batch_size_ +
                             (entry_vec_size % upload_batch_size_ ? 1 : 0));

            int64_t &expected_term = leader_terms_.at(ng_id);

            size_t start_idx = 0;
            size_t end_idx =
                (batch_req_cnt > 1 ? upload_batch_size_ : entry_vec_size);
            for (size_t idx = 0; idx < batch_req_cnt; ++idx)
            {
                UploadBatch(table_name,
                            ng_id,
                            expected_term,
                            entry_vec,
                            (end_idx - start_idx),
                            start_idx,
                            req_mux,
                            req_cv,
                            finished_upload_count,
                            upload_res_code,
                            upload_req_pool);
                ++upload_req_count;
                // Next batch
                start_idx = end_idx;
                end_idx = ((start_idx + upload_batch_size_) > entry_vec_size
                               ? entry_vec_size
                               : (start_idx + upload_batch_size_));
            }
        }
    }

    {
        std::unique_lock<bthread::Mutex> req_lk(req_mux);
        while (upload_req_count != finished_upload_count)
        {
            req_cv.wait(req_lk);
        }
    }

    return upload_res_code;
}

void SkGenerator::UploadBatch(
    const TableName &table_name,
    NodeGroupId dest_ng_id,
    int64_t &ng_term,
    const std::vector<WriteEntry *> &write_entry_vec,
    size_t batch_size,
    size_t start_key_idx,
    bthread::Mutex &req_mux,
    bthread::ConditionVariable &req_cv,
    size_t &finished_req_cnt,
    CcErrorCode &res_code,
    std::vector<std::unique_ptr<UploadBatchCc>> &upload_req_pool)
{
    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(dest_ng_id);
    LocalCcShards *cc_shards = Sharder::Instance().GetLocalCcShards();
    size_t core_cnt = cc_shards->Count();
    if (dest_node_id == cc_shards->NodeId())
    {
        size_t idx = 0;
        UploadBatchCc *req_ptr = nullptr;
        while (idx < upload_req_pool.size())
        {
            req_ptr = upload_req_pool[idx].get();
            if (!req_ptr->InUse())
            {
                req_ptr->Use();
                break;
            }
            ++idx;
        }
        if (idx == upload_req_pool.size())
        {
            upload_req_pool.emplace_back(std::make_unique<UploadBatchCc>());
            req_ptr = upload_req_pool.back().get();
            req_ptr->Use();
        }

        req_ptr->Reset(table_name,
                       dest_ng_id,
                       ng_term,
                       core_cnt,
                       batch_size,
                       start_key_idx,
                       write_entry_vec,
                       req_mux,
                       req_cv,
                       finished_req_cnt,
                       res_code,
                       false);

        for (size_t core = 0; core < core_cnt; ++core)
        {
            cc_shards->EnqueueToCcShard(core, req_ptr);
        }
    }
    else
    {
        // remote node
        std::shared_ptr<brpc::Channel> channel =
            Sharder::Instance().GetCcNodeServiceChannel(dest_node_id);
        if (channel == nullptr)
        {
            // Fail to establish the channel to the tx node. Do not update the
            // leader term of input node group.
            LOG(ERROR) << "UploadBatch: Failed to init the channel of ng#"
                       << dest_ng_id;
            std::unique_lock<bthread::Mutex> req_lk(req_mux);
            res_code = CcErrorCode::ESTABLISH_NODE_CHANNEL_FAILED;
            ++finished_req_cnt;
            req_cv.notify_one();
            return;
        }

        remote::CcRpcService_Stub stub(channel.get());

        UploadBatchClosure *upload_batch_closure = new UploadBatchClosure(
            [dest_ng_id,
             &res_code,
             &finished_req_cnt,
             &req_mux,
             &req_cv,
             &ng_term](CcErrorCode res, int32_t dest_term)
            {
                std::unique_lock<bthread::Mutex> req_lk(req_mux);
                res_code = res;
                if (res == CcErrorCode::NO_ERROR)
                {
                    if (ng_term == INIT_TERM)
                    {
                        ng_term = dest_term;
                    }
                    else if (ng_term != dest_term)
                    {
                        LOG(ERROR)
                            << "Response for upload batch failed of ng#"
                            << dest_ng_id
                            << " of term mismatch, with expected term: "
                            << ng_term << " and actual term: " << dest_term;
                        res_code = CcErrorCode::REQUESTED_NODE_NOT_LEADER;
                    }
                    else
                    {
                        assert(ng_term == dest_term);
                    }
                }
                else
                {
                    LOG(ERROR)
                        << "Response for upload batch failed of ng#"
                        << dest_ng_id << ", with error: " << (uint32_t) res;
                }
                ++finished_req_cnt;
                req_cv.notify_one();
            },
            SkGenerator::UploadTimeout,
            true);
        upload_batch_closure->SetChannel(dest_node_id, channel);

        remote::UploadBatchRequest *req_ptr =
            upload_batch_closure->UploadBatchRequest();
        req_ptr->set_node_group_id(dest_ng_id);
        req_ptr->set_node_group_term(ng_term);
        req_ptr->set_table_name_str(table_name.String());
        req_ptr->set_table_type(
            remote::ToRemoteType::ConvertTableType(table_name.Type()));
        size_t end_key_idx = start_key_idx + batch_size;
        req_ptr->set_is_persisted(false);
        req_ptr->set_batch_size(batch_size);
        // keys
        req_ptr->clear_keys();
        std::string *keys_str = req_ptr->mutable_keys();
        // records
        req_ptr->clear_records();
        std::string *recs_str = req_ptr->mutable_records();
        // commit_ts
        req_ptr->clear_commit_ts();
        std::string *commit_ts_str = req_ptr->mutable_commit_ts();
        size_t len_sizeof = sizeof(uint64_t);
        const char *val_ptr = nullptr;
        // rec_status
        req_ptr->clear_rec_status();
        std::string *rec_status_str = req_ptr->mutable_rec_status();
        // All generated sk should be normal status.
        const RecordStatus rec_status = RecordStatus::Normal;
        for (size_t idx = start_key_idx; idx < end_key_idx; ++idx)
        {
            write_entry_vec.at(idx)->key_.Serialize(*keys_str);
            write_entry_vec.at(idx)->rec_->Serialize(*recs_str);
            val_ptr = reinterpret_cast<const char *>(
                &(write_entry_vec.at(idx)->commit_ts_));
            commit_ts_str->append(val_ptr, len_sizeof);
            rec_status_str->append(reinterpret_cast<const char *>(&rec_status),
                                   sizeof(rec_status));
        }

        brpc::Controller *cntl_ptr = upload_batch_closure->Controller();
        cntl_ptr->set_timeout_ms(SkGenerator::UploadTimeout);
        remote::UploadBatchResponse *resp_ptr =
            upload_batch_closure->UploadBatchResponse();
        // Asynchronous mode
        stub.UploadBatch(cntl_ptr, req_ptr, resp_ptr, upload_batch_closure);
        DLOG(INFO) << "UploadBatch service of ng#" << dest_ng_id;
    }
}

CcErrorCode SkGenerator::AcquireRangeReadLocks(
    TransactionExecution *acq_lock_txm,
    UploadBatchTask &upload_task,
    std::unordered_map<TableName, NGWriteEntry> &ng_write_set)
{
    for (auto table_it = upload_task.write_entry_set_.begin();
         table_it != upload_task.write_entry_set_.end();
         ++table_it)
    {
        const TableName &range_table_name =
            TableName(table_it->first.StringView(), TableType::RangePartition);

        auto &table_write_entrys = table_it->second;
        auto [it, inserted] = ng_write_set.try_emplace(table_it->first);
        auto &ng_table_write_entrys = it->second;
        if (!inserted)
        {
            ng_table_write_entrys.clear();
        }

        const TxKey *write_key = nullptr;
        for (auto write_entry_it = table_write_entrys.begin();
             write_entry_it != table_write_entrys.end();)
        {
            write_key = &write_entry_it->key_;

            RangeRecord range_rec;
            ReadTxRequest read_range_req(
                &range_table_name, write_key, &range_rec, false, false, true);
            acq_lock_txm->Execute(&read_range_req);
            read_range_req.Wait();
            TxErrorCode tx_res = read_range_req.ErrorCode();
            if (tx_res != TxErrorCode::NO_ERROR)
            {
                ReleaseRangeReadLocks(acq_lock_txm, false);

                LOG(ERROR) << "!!!ERROR!!! Read range info failed finally with "
                           << "error message: " << read_range_req.ErrorMsg()
                           << ", for table: " << range_table_name.Trace();
                if (tx_res == TxErrorCode::CC_REQ_FOLLOWER)
                {
                    return CcErrorCode::REQUESTED_NODE_NOT_LEADER;
                }
                else
                {
                    return CcErrorCode::ACQUIRE_KEY_LOCK_FAILED_FOR_RW_CONFLICT;
                }
            }

            AdvanceWriteEntryForRangeInfo(range_rec,
                                          write_entry_it,
                                          table_write_entrys.end(),
                                          ng_table_write_entrys);

        } /* End of table write entrys */
    }     /* End of tables */
    return CcErrorCode::NO_ERROR;
}

/**
 * @brief
 * @param is_success false is acquire range read lock failed.
 */
void SkGenerator::ReleaseRangeReadLocks(TransactionExecution *acq_lock_txm,
                                        bool is_success)
{
    CommitTxRequest commit_req;
    commit_req.to_commit_ = is_success;
    acq_lock_txm->CommitTx(commit_req);
}

void SkGenerator::AdvanceWriteEntryForRangeInfo(
    const RangeRecord &range_record,
    std::vector<WriteEntry>::iterator &cur_write_entry_it,
    const std::vector<WriteEntry>::iterator &write_entry_end,
    NGWriteEntry &ng_write_entrys)
{
    // Advances the write entry iterator such that it points to the first key
    // belonging to the next range.
    TxKey range_end_key = range_record.GetRangeInfo()->EndTxKey();
    auto next_range_start = cur_write_entry_it;
    if (range_end_key.Type() == KeyType::PositiveInf)
    {
        next_range_start = write_entry_end;
    }
    else
    {
        next_range_start = std::lower_bound(
            cur_write_entry_it,
            write_entry_end,
            range_end_key,
            [](const WriteEntry &a, const TxKey &val) { return a.key_ < val; });
    }

    NodeGroupId range_ng = range_record.GetRangeOwnerNg()->BucketOwner();
    NodeGroupId new_bucket_ng =
        range_record.GetRangeOwnerNg()->DirtyBucketOwner();

    const std::vector<const BucketInfo *> *splitting_range_ngs =
        range_record.GetNewRangeOwnerNgs();

    // Updates the sharding codes of the write-entry keys belonging to this
    // range. The higher 22 bits represent the range ID.
    NodeGroupId new_range_ng = UINT32_MAX;
    NodeGroupId new_range_new_bucket_ng = UINT32_MAX;
    size_t new_range_idx = 0;

    auto *range_info = range_record.GetRangeInfo();
    while (cur_write_entry_it != next_range_start)
    {
        WriteEntry &write_entry = *cur_write_entry_it;
        auto ng_it = ng_write_entrys.try_emplace(range_ng);
        ng_it.first->second.push_back(&write_entry);

        // If current range is migrating, forward to new range owner.
        if (new_bucket_ng != UINT32_MAX)
        {
            ng_write_entrys.try_emplace(new_bucket_ng)
                .first->second.push_back(&write_entry);
        }

        // If range is splitting and the key will fall on a new range after
        // split is finished, register forward_addr_ to indicate
        // entry needs to be double written.
        while (range_info->IsDirty() &&
               new_range_idx < range_info->NewKey()->size() &&
               !(write_entry.key_ < range_info->NewKey()->at(new_range_idx)))
        {
            new_range_ng =
                splitting_range_ngs->at(new_range_idx)->BucketOwner();
            new_range_new_bucket_ng =
                splitting_range_ngs->at(new_range_idx++)->DirtyBucketOwner();
        }
        if (new_range_ng != UINT32_MAX)
        {
            if (new_range_ng != range_ng)
            {
                ng_write_entrys.try_emplace(new_range_ng)
                    .first->second.push_back(&write_entry);
            }
            // If the new range is migrating, forward to the new owner of new
            // range.
            if (new_range_new_bucket_ng != UINT32_MAX &&
                new_range_new_bucket_ng != range_ng)
            {
                ng_write_entrys.try_emplace(new_range_new_bucket_ng)
                    .first->second.push_back(&write_entry);
            }
        }

        ++cur_write_entry_it;
    }
}

void SkGenerator::UploadBatchWorker()
{
    // For each node group, and each index table
    std::vector<std::unique_ptr<UploadBatchCc>> upload_req_pool;
    upload_req_pool.reserve(1024);
    uint8_t sleep_duration_s = 30;
    std::unique_lock<std::mutex> worker_lk(upload_batch_worker_ctx_.mux_);
    while (upload_batch_worker_ctx_.status_ == WorkerStatus::Active)
    {
        upload_batch_worker_ctx_.cv_.wait(
            worker_lk,
            [this]()
            {
                return pending_upload_task_size_ > 0 ||
                       upload_batch_worker_ctx_.status_ != WorkerStatus::Active;
            });

        if (!pending_upload_task_size_)
        {
            continue;
        }

        auto &upload_task = upload_batch_queue_.at(upload_task_head_);
        if (upload_task.task_status_ == UploadTaskStatus::Pending)
        {
            upload_task.task_status_ = UploadTaskStatus::Ongoing;
            --pending_upload_task_size_;
            worker_lk.unlock();

            {
                std::unique_lock<std::mutex> upload_sender_lk(
                    upload_sender_mux_);
                if (upload_task_result_ != CcErrorCode::NO_ERROR)
                {
                    break;
                }
                ++ongoing_upload_task_size_;
            }
        }
        else
        {
            continue;
        }

#ifdef RANGE_PARTITION_ENABLED
        for (auto table_it = upload_task.write_entry_set_.begin();
             table_it != upload_task.write_entry_set_.end();
             ++table_it)
        {
            // Sort for each index table.
            std::sort(table_it->second.begin(),
                      table_it->second.end(),
                      [](const WriteEntry &e1, const WriteEntry &e2)
                      { return e1.key_ < e2.key_; });
        }
#endif

        DLOG(INFO) << "Upload sk generated from pk on range#" << partition_id_
                   << " for base table: " << base_table_name_.Trace()
                   << " of ng#" << node_group_id_;

        CcErrorCode res_code = CcErrorCode::NO_ERROR;
        LocalCcShards *cc_shards = Sharder::Instance().GetLocalCcShards();
        do
        {
            res_code = UploadWithoutDataLog(upload_task, upload_req_pool);
            if (res_code == CcErrorCode::TX_NODE_NOT_LEADER ||
                res_code == CcErrorCode::REQUESTED_NODE_NOT_LEADER ||
                res_code == CcErrorCode::NG_TERM_CHANGED)
            {
                LOG(ERROR) << "Upload this batch sk records failed of ng#"
                           << node_group_id_
                           << " for partition id: " << partition_id_
                           << " with error code: " << CcErrorMessage(res_code)
                           << ". Base table: " << base_table_name_.StringView()
                           << ". Terminate this range task.";
                break;
            }
            else if (res_code == CcErrorCode::ESTABLISH_NODE_CHANNEL_FAILED ||
                     res_code == CcErrorCode::REQUEST_LOST)
            {
                LOG(ERROR) << "Upload this batch sk records failed of ng#"
                           << node_group_id_
                           << " for partition id: " << partition_id_
                           << " with error code: " << CcErrorMessage(res_code)
                           << ". Base table: " << base_table_name_.StringView()
                           << ". Retry after 1s.";
                std::this_thread::sleep_for(1s);
                continue;
            }
            else if (res_code == CcErrorCode::OUT_OF_MEMORY ||
                     res_code == CcErrorCode::DATA_STORE_ERR ||
                     res_code == CcErrorCode::PIN_RANGE_SLICE_FAILED ||
                     res_code ==
                         CcErrorCode::ACQUIRE_KEY_LOCK_FAILED_FOR_RW_CONFLICT)
            {
                LOG(ERROR) << "Upload this batch sk records failed of ng#"
                           << node_group_id_
                           << " for partition id: " << partition_id_
                           << " with error code: " << CcErrorMessage(res_code)
                           << ". Base table: " << base_table_name_.StringView()
                           << ". Retry after "
                           << static_cast<uint32_t>(sleep_duration_s) << "s."
                           << " with upload batch size: " << upload_batch_size_;
                bool is_waiting = false;
                do
                {
                    std::this_thread::sleep_for(
                        std::chrono::seconds(sleep_duration_s));
                    is_waiting = cc_shards->IsWaitingCkpt();
                    DLOG(INFO)
                        << "Can retry upload? "
                        << (!is_waiting ? "YES" : "NO! Continue sleep...");
                } while (is_waiting);
                continue;
            }
            else
            {
                DLOG(INFO) << "Upload this batch sk records finished on range#"
                           << partition_id_
                           << " for base table: " << base_table_name_.Trace()
                           << "of ng#" << node_group_id_
                           << " with result: " << CcErrorMessage(res_code);
                assert(res_code == CcErrorCode::NO_ERROR);
            }
        } while (res_code != CcErrorCode::NO_ERROR);

        worker_lk.lock();
        upload_task.task_status_ = UploadTaskStatus::Free;

        {
            std::unique_lock<std::mutex> upload_sender_lk(upload_sender_mux_);
            --ongoing_upload_task_size_;
            upload_task_result_ = (upload_task_result_ == CcErrorCode::NO_ERROR
                                       ? res_code
                                       : upload_task_result_);
            upload_sender_cv_.notify_one();
        }
        if (res_code != CcErrorCode::NO_ERROR)
        {
            assert(res_code == CcErrorCode::TX_NODE_NOT_LEADER ||
                   res_code == CcErrorCode::REQUESTED_NODE_NOT_LEADER ||
                   res_code == CcErrorCode::NG_TERM_CHANGED);
            break;
        }
    }
    worker_lk.unlock();
    DLOG(INFO) << "Finish upload worker for range#" << partition_id_
               << " of ng#" << node_group_id_;
    for (size_t i = 0; i < upload_req_pool.size();)
    {
        if (upload_req_pool[i]->InUse())
        {
            std::this_thread::sleep_for(100ms);
            continue;
        }
        ++i;
    }
    upload_req_pool.clear();
}

}  // namespace txservice