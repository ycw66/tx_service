#include "sk_generator.h"

#include "tx_request.h"
#include "tx_service.h"

namespace txservice
{
void SkGenerator::GenerateSkFromPk(const TableName &table_name,
                                   int32_t partition_id,
                                   const TxKey *start_key,
                                   const TxKey *end_key,
                                   NodeGroupId range_owner,
                                   uint64_t scan_ts,
                                   std::vector<TableName> &new_indexes_name,
                                   uint32_t &scanned_pk_count,
                                   CcErrorCode &res_code,
                                   GenerateSkStatus &task_status)
{
    int64_t ng_term = Sharder::Instance().TryPinNodeGroupData(range_owner);
    if (ng_term < 0)
    {
        LOG(WARNING) << "Generate sk from pk on non-leader node for partition: "
                     << partition_id << " of ng#" << range_owner
                     << ", terminate directly.";
        res_code = CcErrorCode::REQUESTED_NODE_NOT_LEADER;
        return;
    }
    // guard to unpin node group on finish.
    std::shared_ptr<void> defer_unpin(
        nullptr,
        [range_owner](void *)
        { Sharder::Instance().UnpinNodeGroupData(range_owner); });

    LOG(INFO) << "Generate sk from pk on ng#" << range_owner
              << " for base table: " << table_name.Trace()
              << " of the partition id: " << partition_id;

    const TableName &range_table_name =
        TableName(table_name.StringView(), TableType::RangePartition);
    LocalCcShards *cc_shards = Sharder::Instance().GetLocalCcShards();
    TransactionExecution *acq_range_lock_txm =
        cc_shards->GetTxService()->NewTx();
    InitTxRequest init_req;
    init_req.iso_level_ = IsolationLevel::RepeatableRead;
    init_req.protocol_ = CcProtocol::Locking;
    init_req.tx_ng_id_ = range_owner;
    // Init the txm until succeed or the node is not leader.
    do
    {
        init_req.Reset();
        acq_range_lock_txm->Execute(&init_req);
        init_req.Wait();
        if (init_req.IsError())
        {
            if (Sharder::Instance().LeaderTerm(range_owner) < 0)
            {
                LOG(ERROR) << "GenerateSkFromPk: Node not leader of ng#"
                           << range_owner;
                res_code = CcErrorCode::REQUESTED_NODE_NOT_LEADER;
                return;
            }
            LOG(ERROR) << "Init acquire range txm failed for table: "
                       << table_name.Trace() << " of ng#" << range_owner
                       << ", with error: " << init_req.ErrorMsg()
                       << ". Retry after 3s.";
            std::this_thread::sleep_for(3s);
        }
    } while (init_req.IsError());

    // Acquire the range read lock
    ReadTxRequest read_range_req;
    RangeRecord range_rec;
    read_range_req.Set(
        &range_table_name, start_key, &range_rec, false, false, true);
    read_range_req.Reset();
    acq_range_lock_txm->Execute(&read_range_req);
    read_range_req.Wait();
    if (read_range_req.IsError())
    {
        // This read operation might fail if it's blocked by a write
        // lock acquired by range split.
        LOG(ERROR) << "Acquire range lock failed for table: "
                   << table_name.Trace() << " of ng#" << range_owner
                   << ", with error: " << read_range_req.ErrorMsg();
        res_code = CcErrorCode::GET_RANGE_ID_ERR;
        return;
    }

    CommitTxRequest commit_req;
    // Check the range boundary.
    const TxKey *range_end_key = range_rec.GetRangeInfo()->EndKey();
    range_end_key = !range_end_key
                        ? cc_shards->GetCatalogFactory()->PositiveInfKey()
                        : range_end_key;
    if (!(*range_end_key == *end_key))
    {
        // The range have changed, return error.
        LOG(ERROR) << "The boundary of range#" << partition_id
                   << " has changed for table: " << table_name.Trace()
                   << " of ng#" << range_owner << ". Terminated this task.";
        res_code = CcErrorCode::GET_RANGE_ID_ERR;
        // Release the range read locks.
        acq_range_lock_txm->CommitTx(commit_req);
        return;
    }

    res_code = CcErrorCode::NO_ERROR;
    scanned_pk_count = 0;
    uint32_t ng_cnt = Sharder::Instance().NodeGroupCount();
    leader_terms_.resize(ng_cnt, INIT_TERM);

    uint32_t core_cnt = Sharder::Instance().GetLocalCcShards()->Count();
    do
    {
        DataSyncScanCc scan_req(table_name,
                                0,
                                0,
                                scan_ts,
                                range_owner,
                                ng_term,
                                core_cnt,
                                LocalCcShards::DATA_SYNC_SCAN_BATCH_SIZE,
                                acq_range_lock_txm->TxNumber(),
                                start_key,
                                end_key
#ifdef RANGE_PARTITION_ENABLED
                                ,
                                true,
                                true
#endif
        );

        std::tie(scanned_pk_count, res_code) = ScanPkAndGenerateSk(
            table_name, range_owner, new_indexes_name, scan_req, task_status);
        if (res_code == CcErrorCode::REQUESTED_NODE_NOT_LEADER)
        {
            LOG(ERROR) << "Scan pk records failed of ng#" << range_owner
                       << " for partition id: " << partition_id
                       << " with error code: " << CcErrorMessage(res_code)
                       << ". Base table: " << table_name.StringView()
                       << ". Terminate this task.";
            // Release the range read locks.
            acq_range_lock_txm->CommitTx(commit_req);
            return;
        }
        else if (res_code == CcErrorCode::OUT_OF_MEMORY ||
                 res_code == CcErrorCode::DATA_STORE_ERR)
        {
            uint8_t sleep_dur = SleepDuration();
            LOG(ERROR) << "Scan pk records failed of ng#" << range_owner
                       << " for partition id: " << partition_id
                       << " with error code: " << CcErrorMessage(res_code)
                       << ". Base table: " << table_name.StringView()
                       << ". Retry after " << static_cast<uint32_t>(sleep_dur)
                       << "s.";
            bool is_waiting = false;
            do
            {
                std::this_thread::sleep_for(std::chrono::seconds(sleep_dur));
                is_waiting = cc_shards->IsWaitingCkpt();
                LOG(INFO) << "Can retry scan? "
                          << (!is_waiting ? "YES" : "NO! Continue sleep...");
            } while (is_waiting);
            for (auto it = write_entry_set_.begin();
                 it != write_entry_set_.end();
                 ++it)
            {
                it->second.clear();
            }
            continue;
        }
        else
        {
            LOG(INFO) << "Scan pk records finished of ng#" << range_owner
                      << " for partition id: " << partition_id
                      << " with result code: " << CcErrorMessage(res_code)
                      << ". Base table: " << table_name.StringView();
            assert(res_code == CcErrorCode::NO_ERROR);
        }
    } while (res_code != CcErrorCode::NO_ERROR);

    // Release the range read locks.
    acq_range_lock_txm->CommitTx(commit_req);

    if (scanned_pk_count > 0)
    {
#ifdef RANGE_PARTITION_ENABLED
        for (auto table_it = write_entry_set_.begin();
             table_it != write_entry_set_.end();
             ++table_it)
        {
            // Sort for each index table.
            std::sort(table_it->second.begin(),
                      table_it->second.end(),
                      [](const WriteEntry &e1, const WriteEntry &e2)
                      { return *(e1.key_) < *(e2.key_); });
        }
#endif

        LOG(INFO) << "Upload sk generated from pk of ng#" << range_owner
                  << " for base table: " << table_name.Trace()
                  << " of the partition id: " << partition_id;
        do
        {
            res_code = UploadWithoutDataLog(range_owner, task_status);
            if (res_code == CcErrorCode::TX_NODE_NOT_LEADER ||
                res_code == CcErrorCode::REQUESTED_NODE_NOT_LEADER)
            {
                LOG(ERROR) << "Upload this batch sk records failed of ng#"
                           << range_owner
                           << " for partition id: " << partition_id
                           << " with error code: " << CcErrorMessage(res_code)
                           << ". Base table: " << table_name.StringView()
                           << ". Terminate this task.";
                break;
            }
            else if (res_code == CcErrorCode::ESTABLISH_NODE_CHANNEL_FAILED ||
                     res_code == CcErrorCode::REQUEST_LOST)
            {
                LOG(ERROR) << "Upload this batch sk records failed of ng#"
                           << range_owner
                           << " for partition id: " << partition_id
                           << " with error code: " << CcErrorMessage(res_code)
                           << ". Base table: " << table_name.StringView()
                           << ". Retry after 1s.";
                std::this_thread::sleep_for(1s);
                continue;
            }
            else if (res_code == CcErrorCode::OUT_OF_MEMORY ||
                     res_code == CcErrorCode::DATA_STORE_ERR ||
                     res_code ==
                         CcErrorCode::ACQUIRE_KEY_LOCK_FAILED_FOR_RW_CONFLICT)
            {
                uint8_t sleep_dur = SleepDuration();
                UpdateUploadBatchSize(scanned_pk_count);
                LOG(ERROR) << "Upload this batch sk records failed of ng#"
                           << range_owner
                           << " for partition id: " << partition_id
                           << " with error code: " << CcErrorMessage(res_code)
                           << ". Base table: " << table_name.StringView()
                           << ". Retry after "
                           << static_cast<uint32_t>(sleep_dur) << "s."
                           << " with upload batch size: " << upload_batch_size_;
                bool is_waiting = false;
                do
                {
                    std::this_thread::sleep_for(
                        std::chrono::seconds(sleep_dur));
                    is_waiting = cc_shards->IsWaitingCkpt();
                    LOG(INFO)
                        << "Can retry upload? "
                        << (!is_waiting ? "YES" : "NO! Continue sleep...");
                } while (is_waiting);
                ng_cnt = Sharder::Instance().NodeGroupCount();
                leader_terms_.resize(ng_cnt, INIT_TERM);
                upload_results_.clear();
                continue;
            }
            else
            {
                LOG(INFO) << "Upload this batch sk records finished of ng#"
                          << range_owner
                          << " for partition id: " << partition_id
                          << " with result code: " << CcErrorMessage(res_code)
                          << ". Base table: " << table_name.StringView();
                assert(res_code == CcErrorCode::NO_ERROR);
            }
        } while (res_code != CcErrorCode::NO_ERROR);
    }
    else
    {
        // This range is empty.
    }

    defer_unpin.reset();
    for (size_t idx = 0; idx < upload_batch_req_vec_.size();)
    {
        if (upload_batch_req_vec_[idx]->InUse())
        {
            // wait the request finish.
            std::this_thread::sleep_for(1s);
            continue;
        }
        ++idx;
    }
    LOG(INFO) << "Finished generate sk of ng#" << range_owner
              << ", for partition id: " << partition_id
              << " with result code: " << CcErrorMessage(res_code);
}

void SkGenerator::RemoteGenerateSkFromPk(
    const TableName &table_name,
    int32_t partition_id,
    const std::string &start_key_str,
    const std::string &end_key_str,
    NodeGroupId ng_id,
    uint64_t scan_ts,
    std::vector<TableName> &new_indexes_name,
    uint32_t &scanned_pk_count,
    CcErrorCode &res_code,
    GenerateSkStatus &task_status)
{
    int32_t ng_term = Sharder::Instance().TryPinNodeGroupData(ng_id);
    if (ng_term < 0)
    {
        LOG(WARNING)
            << "Generate sk from pk on non-leader node for partition id: "
            << partition_id << " of ng#" << ng_id << ", terminate directly.";
        res_code = CcErrorCode::REQUESTED_NODE_NOT_LEADER;
        return;
    }
    // guard to unpin node group on finish.
    std::shared_ptr<void> defer_unpin(
        nullptr,
        [ng_id](void *) { Sharder::Instance().UnpinNodeGroupData(ng_id); });

    DLOG(INFO) << "Generate sk from pk on ng#" << ng_id
               << " for base table: " << table_name.Trace()
               << " of the partition id: " << partition_id;

    CODE_FAULT_INJECTOR("term_AlterTableIndex_RangeBoundaryMismatch", {
        // The range have changed, return error.
        DLOG(ERROR) << "The boundary of range#" << partition_id
                    << " has changed for table: " << table_name.Trace()
                    << " of ng#" << ng_id << ". Terminated this task.";
        res_code = CcErrorCode::GET_RANGE_ID_ERR;
        FaultInject::Instance().InjectFault(
            "term_AlterTableIndex_RangeBoundaryMismatch", "remove");
        return;
    });

    const TableName &range_table_name =
        TableName(table_name.StringView(), TableType::RangePartition);
    LocalCcShards *cc_shards = Sharder::Instance().GetLocalCcShards();
    TransactionExecution *acq_range_lock_txm =
        cc_shards->GetTxService()->NewTx();
    InitTxRequest init_req;
    init_req.iso_level_ = IsolationLevel::RepeatableRead;
    init_req.protocol_ = CcProtocol::Locking;
    init_req.tx_ng_id_ = ng_id;
    // Init the txm until succeed or the node is not leader.
    do
    {
        init_req.Reset();
        acq_range_lock_txm->Execute(&init_req);
        init_req.Wait();
        if (init_req.IsError())
        {
            if (Sharder::Instance().LeaderTerm(ng_id) < 0)
            {
                LOG(ERROR) << "RemoteGenerateSkFromPk: Node not leader of ng#"
                           << ng_id;
                res_code = CcErrorCode::REQUESTED_NODE_NOT_LEADER;
                return;
            }
            LOG(ERROR) << "Init acquire range txm failed for table: "
                       << table_name.Trace() << " of ng#" << ng_id
                       << ", with error: " << init_req.ErrorMsg()
                       << ". Retry after 3s.";
            std::this_thread::sleep_for(3s);
        }
    } while (init_req.IsError());

    // Acquire the range read lock
    ReadTxRequest read_range_req;
    RangeRecord range_rec;
    const TxKey *range_start_key = nullptr;
    if (start_key_str.size() > 0)
    {
        read_range_req.Set(
            &range_table_name, &start_key_str, &range_rec, false, false, true);
    }
    else
    {
        range_start_key = cc_shards->GetCatalogFactory()->NegativeInfKey();
        read_range_req.Set(
            &range_table_name, range_start_key, &range_rec, false, false, true);
    }
    read_range_req.Reset();
    acq_range_lock_txm->Execute(&read_range_req);
    read_range_req.Wait();
    if (read_range_req.IsError())
    {
        // This read operation might fail if it's blocked by a write
        // lock acquired by range split.
        LOG(ERROR) << "Acquire range lock failed for table: "
                   << table_name.Trace() << " of ng#" << ng_id
                   << ", with error: " << read_range_req.ErrorMsg();
        res_code = CcErrorCode::GET_RANGE_ID_ERR;
        return;
    }

    CommitTxRequest commit_req;
    // Check the range boundary.
    range_start_key = range_rec.GetRangeInfo()->StartKey();
    const TxKey *range_end_key = range_rec.GetRangeInfo()->EndKey();
    range_end_key = !range_end_key
                        ? cc_shards->GetCatalogFactory()->PositiveInfKey()
                        : range_end_key;
    std::string serialized_end_key;
    if (range_end_key->Type() == KeyType::Normal)
    {
        range_end_key->Serialize(serialized_end_key);
    }
    if (serialized_end_key.length() != end_key_str.length() ||
        serialized_end_key.compare(end_key_str))
    {
        // The range have changed, return error.
        LOG(ERROR) << "The boundary of range#" << partition_id
                   << " has changed for table: " << table_name.Trace()
                   << " of ng#" << ng_id << ". Terminated this task.";
        res_code = CcErrorCode::GET_RANGE_ID_ERR;
        // Release the range read locks.
        acq_range_lock_txm->CommitTx(commit_req);
        return;
    }

    res_code = CcErrorCode::NO_ERROR;
    scanned_pk_count = 0;
    uint32_t ng_cnt = Sharder::Instance().NodeGroupCount();
    leader_terms_.resize(ng_cnt, INIT_TERM);

    uint32_t core_cnt = Sharder::Instance().GetLocalCcShards()->Count();
    do
    {
        DataSyncScanCc scan_req(table_name,
                                0,
                                0,
                                scan_ts,
                                ng_id,
                                ng_term,
                                core_cnt,
                                LocalCcShards::DATA_SYNC_SCAN_BATCH_SIZE,
                                acq_range_lock_txm->TxNumber(),
                                range_start_key,
                                range_end_key
#ifdef RANGE_PARTITION_ENABLED
                                ,
                                true,
                                true
#endif
        );

        std::tie(scanned_pk_count, res_code) = ScanPkAndGenerateSk(
            table_name, ng_id, new_indexes_name, scan_req, task_status);
        if (res_code == CcErrorCode::REQUESTED_NODE_NOT_LEADER)
        {
            LOG(ERROR) << "Scan pk records failed of ng#" << ng_id
                       << " for partition id: " << partition_id
                       << " with error code: " << CcErrorMessage(res_code)
                       << ". Base table: " << table_name.StringView()
                       << ". Terminate this task.";
            // Release the range read locks.
            acq_range_lock_txm->CommitTx(commit_req);
            return;
        }
        else if (res_code == CcErrorCode::OUT_OF_MEMORY ||
                 res_code == CcErrorCode::DATA_STORE_ERR)
        {
            uint8_t sleep_dur = SleepDuration();
            LOG(ERROR) << "Scan pk records failed of ng#" << ng_id
                       << " for partition id: " << partition_id
                       << " with error code: " << CcErrorMessage(res_code)
                       << ". Base table: " << table_name.StringView()
                       << ". Retry after " << static_cast<uint32_t>(sleep_dur)
                       << "s.";
            bool is_waiting = false;
            do
            {
                std::this_thread::sleep_for(std::chrono::seconds(sleep_dur));
                is_waiting = cc_shards->IsWaitingCkpt();
                LOG(INFO) << "Can retry scan? "
                          << (!is_waiting ? "YES" : "NO! Continue sleep...");
            } while (is_waiting);
            for (auto it = write_entry_set_.begin();
                 it != write_entry_set_.end();
                 ++it)
            {
                it->second.clear();
            }
            continue;
        }
        else
        {
            LOG(INFO) << "Scan pk records finished of ng#" << ng_id
                      << " for partition id: " << partition_id
                      << " with result code: " << CcErrorMessage(res_code)
                      << ". Base table: " << table_name.StringView();
            assert(res_code == CcErrorCode::NO_ERROR);
        }
    } while (res_code != CcErrorCode::NO_ERROR);

    // Release the range read locks.
    acq_range_lock_txm->CommitTx(commit_req);

    if (scanned_pk_count > 0)
    {
#ifdef RANGE_PARTITION_ENABLED
        for (auto table_it = write_entry_set_.begin();
             table_it != write_entry_set_.end();
             ++table_it)
        {
            // Sort for each index table.
            std::sort(table_it->second.begin(),
                      table_it->second.end(),
                      [](const WriteEntry &e1, const WriteEntry &e2)
                      { return *(e1.key_) < *(e2.key_); });
        }
#endif

        LOG(INFO) << "Upload sk generated from pk of ng#" << ng_id
                  << " for base table: " << table_name.Trace()
                  << " of the partition id: " << partition_id;

        do
        {
            res_code = UploadWithoutDataLog(ng_id, task_status);
            if (res_code == CcErrorCode::TX_NODE_NOT_LEADER ||
                res_code == CcErrorCode::REQUESTED_NODE_NOT_LEADER)
            {
                LOG(ERROR) << "Upload this batch sk records failed of ng#"
                           << ng_id << " for partition id: " << partition_id
                           << " with error code: " << CcErrorMessage(res_code)
                           << ". Base table: " << table_name.StringView()
                           << ". Terminate this task.";
                break;
            }
            else if (res_code == CcErrorCode::ESTABLISH_NODE_CHANNEL_FAILED ||
                     res_code == CcErrorCode::REQUEST_LOST)
            {
                LOG(ERROR) << "Upload this batch sk records failed of ng#"
                           << ng_id << " for partition id: " << partition_id
                           << " with error code: " << CcErrorMessage(res_code)
                           << ". Base table: " << table_name.StringView()
                           << ". Retry after 1s.";
                std::this_thread::sleep_for(1s);
                continue;
            }
            else if (res_code == CcErrorCode::OUT_OF_MEMORY ||
                     res_code == CcErrorCode::DATA_STORE_ERR ||
                     res_code ==
                         CcErrorCode::ACQUIRE_KEY_LOCK_FAILED_FOR_RW_CONFLICT)
            {
                uint8_t sleep_dur = SleepDuration();
                UpdateUploadBatchSize(scanned_pk_count);
                LOG(ERROR) << "Upload this batch sk records failed of ng#"
                           << ng_id << " for partition id: " << partition_id
                           << " with error code: " << CcErrorMessage(res_code)
                           << ". Base table: " << table_name.StringView()
                           << ". Retry after "
                           << static_cast<uint32_t>(sleep_dur) << "s."
                           << " with upload batch size: " << upload_batch_size_;
                bool is_waiting = false;
                do
                {
                    std::this_thread::sleep_for(
                        std::chrono::seconds(sleep_dur));
                    is_waiting = cc_shards->IsWaitingCkpt();
                    LOG(INFO)
                        << "Can retry upload? "
                        << (!is_waiting ? "YES" : "NO! Continue sleep...");
                } while (is_waiting);
                ng_cnt = Sharder::Instance().NodeGroupCount();
                leader_terms_.resize(ng_cnt, INIT_TERM);
                upload_results_.clear();
                continue;
            }
            else
            {
                LOG(ERROR) << "Upload this batch sk records finished of ng#"
                           << ng_id << " for partition id: " << partition_id
                           << " with error code: " << CcErrorMessage(res_code)
                           << ", for base table: " << table_name.StringView();
                assert(res_code == CcErrorCode::NO_ERROR);
            }
        } while (res_code != CcErrorCode::NO_ERROR);
    }
    else
    {
        // This range is empty.
    }

    defer_unpin.reset();
    for (size_t idx = 0; idx < upload_batch_req_vec_.size();)
    {
        if (upload_batch_req_vec_[idx]->InUse())
        {
            // wait the request finish.
            std::this_thread::sleep_for(1s);
            continue;
        }
        ++idx;
    }
    DLOG(INFO) << "Finished generate sk of ng#" << ng_id
               << ", for partition id: " << partition_id
               << " with result code: " << CcErrorMessage(res_code);
}

std::pair<size_t, CcErrorCode> SkGenerator::ScanPkAndGenerateSk(
    const TableName &table_name,
    NodeGroupId range_owner,
    const std::vector<TableName> &new_indexes_name,
    DataSyncScanCc &scan_req,
    GenerateSkStatus &task_status)
{
    LocalCcShards *cc_shards = Sharder::Instance().GetLocalCcShards();
    auto catalog_entry = cc_shards->GetCatalog(table_name, range_owner);
    TableSchema *table_schema =
        const_cast<TableSchema *>(catalog_entry->dirty_schema_.get());
    assert(table_schema != nullptr && new_indexes_name.size() > 0);
    SkEncoder::uptr sk_encoder = nullptr;
    const TxKey *target_key = nullptr;
    const TxRecord *target_rec = nullptr;
    uint64_t version_ts = 0;
    size_t total_tuples = 0;

    size_t core_cnt = cc_shards->Count();
    bool scan_data_drained = false;
    while (!scan_data_drained)
    {
        // Dispatch this scan request to the first core if need to deserialize
        // the scan key. The request is then dispatched to remaining cores to
        // scan in parallel.
        for (size_t idx = 0; idx < core_cnt; ++idx)
        {
            cc_shards->EnqueueToCcShard(idx, &scan_req);
        }
        scan_req.Wait();

        if (scan_req.IsError())
        {
            LOG(ERROR) << "Scan pk records failed on table "
                       << table_name.StringView() << " of ng#" << range_owner;
            if (sk_encoder != nullptr)
            {
                sk_encoder.reset(nullptr);
            }
            return std::pair<size_t, CcErrorCode>(total_tuples,
                                                  scan_req.ErrorCode());
        }
        else
        {
            scan_data_drained = true;

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
                    assert(target_key != nullptr && target_rec != nullptr);

                    if (sk_encoder == nullptr)
                    {
                        sk_encoder = table_schema->CreateSkEncoder();
                    }

                    for (auto index_it = new_indexes_name.cbegin();
                         index_it != new_indexes_name.cend();
                         ++index_it)
                    {
                        auto packed_sk = sk_encoder->GeneratePackedSk(
                            target_key, target_rec, *index_it);

                        if (packed_sk.first.get() == nullptr)
                        {
                            LOG(ERROR)
                                << "Failed to generate packed sk for index: "
                                << index_it->StringView();
                            // Finish the pack sk operation
                            sk_encoder.reset(nullptr);
                            return std::pair<size_t, CcErrorCode>(
                                total_tuples, CcErrorCode::PACK_SK_ERR);
                        }

                        auto iter = write_entry_set_.find(*index_it);
                        if (iter == write_entry_set_.end())
                        {
                            auto write_entry_it = write_entry_set_.emplace(
                                std::piecewise_construct,
                                std::forward_as_tuple(index_it->StringView(),
                                                      index_it->Type()),
                                std::forward_as_tuple(
                                    std::vector<WriteEntry>()));
                            iter = write_entry_it.first;
                            iter->second.reserve(UPLOAD_BATCH_SIZE);
                        }

                        iter->second.emplace_back(std::move(packed_sk.first),
                                                  std::move(packed_sk.second),
                                                  version_ts);
                    } /* End of foreach new_indexes_name */

                    ++total_tuples;
                    if (total_tuples % 10000 == 0 &&
                        !task_status.CheckTxTermStatus())
                    {
                        LOG(WARNING)
                            << "Terminate this task cause the tx leader "
                               "transferred.";
                        sk_encoder.reset(nullptr);
                        task_status.TerminateGenerateSk();
                        return std::pair<size_t, CcErrorCode>(
                            total_tuples, CcErrorCode::TX_NODE_NOT_LEADER);
                    }

                } /* End of each key */

                // If the data is drained
                scan_data_drained =
                    scan_req.IsDrained(core_idx) && scan_data_drained;
            } /* End of each core */

            scan_req.Reset();
        } /* End of this round scan result */
    }     /* End of scan */
    DLOG(INFO) << "Finish scan and generate sk records of ng#" << range_owner
               << " with count: " << total_tuples;
    return std::pair<size_t, CcErrorCode>(total_tuples, CcErrorCode::NO_ERROR);
}

CcErrorCode SkGenerator::UploadWithoutDataLog(NodeGroupId ng_id,
                                              GenerateSkStatus &task_status)
{
    CcErrorCode res = CcErrorCode::NO_ERROR;
#ifdef RANGE_PARTITION_ENABLED
    LocalCcShards *cc_shards = Sharder::Instance().GetLocalCcShards();
    TransactionExecution *acq_range_lock_txm =
        cc_shards->GetTxService()->NewTx();

    InitTxRequest init_req;
    init_req.iso_level_ = IsolationLevel::RepeatableRead;
    init_req.protocol_ = CcProtocol::Locking;
    init_req.tx_ng_id_ = ng_id;

    acq_range_lock_txm->Execute(&init_req);
    init_req.Wait();

    if (init_req.IsError())
    {
        LOG(ERROR) << "UploadWithoutDataLog: Transaction node not leader of ng#"
                   << ng_id;
        return CcErrorCode::TX_NODE_NOT_LEADER;
    }

    res = AcquireRangeReadLocks(acq_range_lock_txm);
    if (res != CcErrorCode::NO_ERROR)
    {
        LOG(ERROR)
            << "UploadWithoutDataLog: Acquire range read locks failed of ng#"
            << ng_id << " with error code: " << static_cast<uint32_t>(res);
        return res;
    }
    LOG(INFO) << "Acquire sk range locks successfully of ng#" << ng_id
              << " with txn: " << acq_range_lock_txm->TxNumber();
#else
    size_t hash = 0;
    uint32_t key_shard_code = 0;
    NodeGroupId dest_ng_id = 0;
    for (auto table_it = write_entry_set_.begin();
         table_it != write_entry_set_.end();
         ++table_it)
    {
        auto &table_write_entrys = table_it->second;
        auto ng_write_entry_it = ng_write_entry_set_.find(table_it->first);
        if (ng_write_entry_it == ng_write_entry_set_.end())
        {
            auto ins_it = ng_write_entry_set_.emplace(
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
            hash = item_it->key_->Hash();
            key_shard_code = Sharder::Instance().ShardCode(hash);
            dest_ng_id = Sharder::Instance().ShardToCcNodeGroup(key_shard_code);
            auto ng_it = ng_table_write_entrys.try_emplace(dest_ng_id);
            ng_it.first->second.push_back(&(*item_it));
        }
    }
#endif

    if (!task_status.CheckTxTermStatus())
    {
        LOG(WARNING) << "Terminate this task cause the tx leader transferred.";
        task_status.TerminateGenerateSk();
        res = CcErrorCode::TX_NODE_NOT_LEADER;
    }
    else
    {
        res = UploadSkInternal();
    }

#ifdef RANGE_PARTITION_ENABLED
    ReleaseRangeReadLocks(acq_range_lock_txm, true);
#endif

    DLOG(INFO) << "UploadWithoutDataLog: Finished of ng#" << ng_id
               << " with result code: " << CcErrorMessage(res);
    return res;
}

CcErrorCode SkGenerator::UploadSkInternal()
{
    size_t entry_vec_size = 0;
    size_t batch_req_cnt = 0;
    size_t ng_request_cnt = 0;
    size_t finished_ng_request = 0;

    upload_results_.reserve(64);
    upload_batch_req_vec_.reserve(1024);
    std::mutex upload_mux;
    std::condition_variable upload_cv;
    for (auto &[table_name, ng_entries] : ng_write_entry_set_)
    {
        ng_request_cnt += ng_entries.size();
        for (auto &[ng_id, entry_vec] : ng_entries)
        {
            entry_vec_size = entry_vec.size();
            batch_req_cnt = (entry_vec_size / upload_batch_size_ +
                             (entry_vec_size % upload_batch_size_ ? 1 : 0));

            upload_results_.emplace_back(nullptr);
            auto &hd_result = upload_results_.back();
            hd_result.Reset();
            hd_result.SetRefCnt(batch_req_cnt);
            hd_result.post_lambda_ =
                [&upload_mux, &upload_cv, &finished_ng_request](
                    CcHandlerResult<UploadBatchResult> *hd_res)
            {
                std::unique_lock<std::mutex> lk(upload_mux);
                ++finished_ng_request;
                upload_cv.notify_one();
            };
            hd_result.Value().Reset();
            hd_result.Value().node_group_id_ = ng_id;

            int64_t expected_term = leader_terms_.at(ng_id);

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
                            hd_result);
                // Next batch
                start_idx = end_idx;
                end_idx = ((start_idx + upload_batch_size_) > entry_vec_size
                               ? entry_vec_size
                               : (start_idx + upload_batch_size_));
            }
        }
    }

    {
        std::unique_lock<std::mutex> lk(upload_mux);
        upload_cv.wait(lk,
                       [&ng_request_cnt, &finished_ng_request]()
                       { return ng_request_cnt == finished_ng_request; });
    }

    // Check leader.
    for (size_t idx = 0; idx < ng_request_cnt; ++idx)
    {
        auto &hd_res = upload_results_.at(idx);
        auto &res_val = hd_res.Value();
        if (hd_res.IsError())
        {
            LOG(ERROR) << "Upload batch sk record failed of ng#"
                       << static_cast<uint32_t>(res_val.node_group_id_)
                       << ", with error: " << hd_res.ErrorMsg();
            // For OOM, should release sk range read lock; For leader
            // transferred, should re-execute from the first batch
            // record.
            return hd_res.ErrorCode();
        }
        else
        {
            NodeGroupId ng_id = res_val.node_group_id_;
            auto &leader_term = leader_terms_.at(ng_id);
            int64_t cur_term = res_val.term_.load(std::memory_order_relaxed);
            if (leader_term < 0)
            {
                leader_term = cur_term;
            }
            else if (leader_term != cur_term)
            {
                LOG(ERROR) << "Upload batch sk record failed caused by "
                           << "leader transferred of ng#" << ng_id;
                return CcErrorCode::REQUESTED_NODE_NOT_LEADER;
            }
            else
            {
                assert(leader_term == cur_term);
            }
        }
    }

    return CcErrorCode::NO_ERROR;
}

void SkGenerator::UploadBatch(const TableName &table_name,
                              NodeGroupId dest_ng_id,
                              int64_t ng_term,
                              const std::vector<WriteEntry *> &write_entry_vec,
                              size_t batch_size,
                              size_t start_key_idx,
                              CcHandlerResult<UploadBatchResult> &hd_res)
{
    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(dest_ng_id);
    LocalCcShards *cc_shards = Sharder::Instance().GetLocalCcShards();
    size_t core_cnt = cc_shards->Count();
    if (dest_node_id == cc_shards->NodeId())
    {
        std::unique_ptr<UploadBatchCc> req = std::make_unique<UploadBatchCc>();
        req->Use();
        req->Reset(table_name,
                   dest_ng_id,
                   ng_term,
                   core_cnt,
                   batch_size,
                   start_key_idx,
                   write_entry_vec,
                   hd_res);

        for (size_t core = 0; core < core_cnt; ++core)
        {
            cc_shards->EnqueueToCcShard(core, req.get());
        }
        upload_batch_req_vec_.push_back(std::move(req));
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
            hd_res.SetError(CcErrorCode::ESTABLISH_NODE_CHANNEL_FAILED);
            return;
        }

        remote::CcRpcService_Stub stub(channel.get());

        UploadBatchClosure *upload_batch_closure =
            new UploadBatchClosure(&hd_res);
        upload_batch_closure->SetChannel(dest_node_id, channel);

        remote::UploadBatchRequest *req_ptr =
            upload_batch_closure->UploadBatchRequest();
        req_ptr->set_node_group_id(dest_ng_id);
        req_ptr->set_node_group_term(ng_term);
        req_ptr->set_table_name_str(table_name.String());
        req_ptr->set_table_type(
            remote::ToRemoteType::ConvertTableType(table_name.Type()));
        size_t end_key_idx = start_key_idx + batch_size;
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
        for (size_t idx = start_key_idx; idx < end_key_idx; ++idx)
        {
            write_entry_vec.at(idx)->key_->Serialize(*keys_str);
            write_entry_vec.at(idx)->rec_->Serialize(*recs_str);
            val_ptr = reinterpret_cast<const char *>(
                &(write_entry_vec.at(idx)->commit_ts_));
            commit_ts_str->append(val_ptr, len_sizeof);
        }

        brpc::Controller *cntl_ptr = upload_batch_closure->Controller();
        cntl_ptr->set_timeout_ms(-1);
        remote::UploadBatchResponse *resp_ptr =
            upload_batch_closure->UploadBatchResponse();
        // Asynchronous mode
        stub.UploadBatch(cntl_ptr, req_ptr, resp_ptr, upload_batch_closure);
        DLOG(INFO) << "UploadBatch service of ng#" << dest_ng_id;
    }
}

CcErrorCode SkGenerator::AcquireRangeReadLocks(
    TransactionExecution *acq_lock_txm)
{
    for (auto table_it = write_entry_set_.begin();
         table_it != write_entry_set_.end();
         ++table_it)
    {
        const TableName &range_table_name =
            TableName(table_it->first.StringView(), TableType::RangePartition);

        auto &table_write_entrys = table_it->second;
        auto [it, inserted] = ng_write_entry_set_.try_emplace(table_it->first);
        auto &ng_table_write_entrys = it->second;
        if (!inserted)
        {
            ng_table_write_entrys.clear();
        }

        const TxKey *write_key = nullptr;
        for (auto write_entry_it = table_write_entrys.begin();
             write_entry_it != table_write_entrys.end();)
        {
            write_key = write_entry_it->key_.get();

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
    const TxKey *range_end_key = range_record.GetRangeInfo()->EndKey();
    auto next_range_start = cur_write_entry_it;
    if (range_end_key == nullptr ||
        range_end_key->Type() == KeyType::PositiveInf)
    {
        next_range_start = write_entry_end;
    }
    else
    {
        next_range_start = std::lower_bound(cur_write_entry_it,
                                            write_entry_end,
                                            range_end_key,
                                            [](WriteEntry &a, const TxKey *val)
                                            { return *(a.key_) < *val; });
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
               !(*write_entry.key_ < *range_info->NewKey()->at(new_range_idx)))
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

}  // namespace txservice