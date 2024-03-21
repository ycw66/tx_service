#pragma once

#include "cc_req_pool.h"
#include "rpc_closure.h"
#include "store/data_store_scanner.h"
#include "tx_operation.h"

namespace txservice
{
struct KickoutDataAllOp : public TransactionOperation
{
    explicit KickoutDataAllOp(TransactionExecution *txm);
    void Reset(uint32_t ng_cnt, size_t table_cnt);
    void Clear();
    void ResetHandlerTxm(TransactionExecution *txm);
    void Forward(TransactionExecution *txm) override;

    // To handle multi tables.
    std::vector<const TableName *> table_names_;
    uint64_t commit_ts_{0};
    CcHandlerResult<Void> hd_result_;
};

struct UpsertTableIndexOp : public SchemaOp
{
    UpsertTableIndexOp() = delete;
    UpsertTableIndexOp(const std::string_view table_name_sv,
                       const std::string &current_image,
                       uint64_t curr_schema_ts,
                       const std::string &dirty_image,
                       const std::string &alter_table_image,
                       OperationType op_type,
                       TransactionExecution *txm);

    void Forward(TransactionExecution *txm) override;

    void Reset(const std::string_view table_name_str,
               const std::string &current_image,
               uint64_t curr_schema_ts,
               const std::string &dirty_image,
               const std::string &alter_table_image,
               OperationType op_type,
               TransactionExecution *txm);

    /**
     * @brief The current stage of this multi-stage schema operation.
     */
    TransactionOperation *op_{nullptr};
    /**
     * @brief Acquire read lock on local cluster config ccmap to block cluster
     * config update during upsert table op. We cannot allow config update
     * between acquire write all and post write all.
     */
    ReadLocalOperation lock_cluster_config_op_;
    /**
     * @brief Acquires write intents on the table's catalog in all nodes to
     * prevent concurrent schema modifications.
     */
    AcquireAllOp acquire_all_intent_op_;
    /**
     * @brief Upgrade write intent to write lock on all nodes. To prevent
     * concurrent DDL and DML on the table. Then get a boundary between new
     * and old tuples.
     */
    AcquireAllOp upgrade_all_intent_to_lock_op_;
    /**
     * @brief Flushes the prepare log to the log service. The schema operation
     * is guaranteed to succeed after this stage.
     */
    WriteToLogOp prepare_log_op_;
    /**
     * @brief Installs the dirty schema in the tx service and returns a local
     * view (pointer) of it, and downgrade all write lock to write intent.
     */
    PostWriteAllOp downgrade_all_lock_to_intent_op_;
    /**
     * @brief Release cluster config read lock after post write all.
     */
    PostReadOperation unlock_cluster_config_op_;
    /**
     * @brief Creates/deletes the data store table and persists/removes the
     * binary representation of the catalog in the data store.
     */
    DsUpsertTableOp upsert_kv_table_op_;
#ifndef RANGE_PARTITION_ENABLED
    /**
     * @brief Flush the old tuples whose commit timestamp less than the new
     * table schema's version of the base table ccmap on all nodes into data
     * store. Consist of scan, flush.
     */
    AsyncOp<Void> flush_all_old_tuples_pk_op_;
    /**
     * @brief Generate index data for old tuples, and upload them to sk ccmap.
     * Consist of fetching from data store, constructing packed index data, and
     * upload to the sk ccmap.
     *
     * NOTE: Store the expected node group term into result value, using it to
     * check whether has failover during upload sk and flush sk operation.
     */
    AsyncOp<Void> fetch_old_tuples_from_kv_gen_sk_data_upload_op_;
    /**
     * @brief Flush the index data of the old tuples into data store, and kick
     * out them from the memory. Consist of scan, flush.
     *
     * NOTE: table_name is the index table name.
     * NOTE: Currently, because of no data log during the operation of
     * @@fetch_old_tuples_from_kv_gen_sk_data_upload_op_, should re-execute from
     * the former stage if leader-transfer happened during this step.
     */
    AsyncOp<Void> flush_all_old_tuples_sk_op_;
    /**
     * @brief Kickout the old packed sk tuples from sk ccmap that new created.
     */
    KickoutDataAllOp kickout_data_all_op_;
    /**
     * @brief Flushes the log to the log service. This log confirms that the
     * index data operation of the old tuples succeeds. In term of recovery,
     * this log ensure that write intent is hold before this log, rather than
     * write lock which will block checkpointer(acquire read lock).
     */
    WriteToLogOp prepare_log_for_sk_op_;
#else
    /**
     * @brief Generate sk record from pk record parallelly. The parallel
     * granularity of the operation is range.
     */
    AsyncOp<Void> generate_sk_parallel_op_;
    /**
     * @brief Flush the index data of the old tuples into data store. Consist of
     * scan, flush.
     *
     * NOTE: table_name is the index table name.
     */
    AsyncOp<Void> flush_all_old_tuples_sk_op_;
    /**
     * @brief Flushes the log to the log service. This log confirms that the
     * index data operation of the old tuples succeeds. In term of recovery,
     * this log ensure that write intent is hold before this log, rather than
     * write lock which will block checkpointer(acquire read lock).
     */
    WriteToLogOp prepare_log_for_sk_op_;
#endif
    /**
     * @brief Upgrades acquired write intents to write locks in all nodes.
     */
    AcquireAllOp acquire_all_lock_op_;
    /**
     * @brief Flushes the commit log to the log service. The commit log confirms
     * that the data store operation succeeds and does not need redo upon
     * failures.
     */
    WriteToLogOp commit_log_op_;
    /**
     * @brief Removes write locks in all nodes. If the schema operation
     * succeeds, also installs the new schema in all nodes.
     */
    PostWriteAllOp post_all_lock_op_;
    /**
     * @brief The last log operation that removes the schema record from the log
     * state machine.
     */
    WriteToLogOp clean_log_op_;

    CcHandlerResult<ReadKeyResult> read_cluster_result_;
    ClusterConfigRecord cluster_conf_rec_;

    // The last finished end key.
    union
    {
        const TxKey *last_finished_end_key_;
        const std::string *last_finished_end_key_str_;
    };
    bool is_last_finished_key_str_;

private:
    void FillPrepareLogRequest(TransactionExecution *txm);
    void FillPrepareIndexTableLogRequest(TransactionExecution *txm);
    void FillCommitLogRequest(TransactionExecution *txm);
    void ForceToFinish(TransactionExecution *txm);

    // Flush pk or sk data from ccmap into data store.
    void FlushDataIntoDataStore(const TableName &table_name,
                                NodeGroupId ng_id,
                                uint64_t data_sync_ts,
                                bool is_dirty,
                                CcHandlerResult<Void> &hres,
                                int64_t ng_term = INIT_TERM);

    // Acquire and release range read lock.
    bool AcquireRangeReadLocks(TransactionExecution *acquire_lock_txm,
                               ReadWriteSet &rw_set);
    void ReleaseRangeReadLocks(TransactionExecution *acquire_lock_txm,
                               bool is_success);
    // Acquire and reset node group leader term
    void ResetLeaderTerms();
    CcErrorCode AcquireLeaderTermsIfNecessary(TransactionExecution *txm);
    void AcquireNodeGroupLeaderTerm(NodeGroupId ng_id,
                                    std::mutex &request_mux,
                                    std::condition_variable &request_cv,
                                    uint32_t &finished_req_cnt,
                                    CcErrorCode &request_res);

    // Upload sk record from local write set into sk ccmap
    void UploadRecord(TxNumber tx_number,
                      int64_t tx_term,
                      uint16_t command_id,
                      uint64_t commit_ts,
                      const TableName &table_name,
                      const TxKey *key,
                      const TxRecord *record,
                      OperationType operation_type,
                      uint32_t key_shard_code,
                      CcHandlerResult<PostProcessResult> &hres,
                      int64_t expected_term);
    void UploadSkData(TransactionExecution *txm, ReadWriteSet &rw_set);
    bool UploadWithoutDataLog(TransactionExecution *upload_txm);
    // Scan pk from data store
    std::unique_ptr<store::DataStoreScanner> PrepareScanFromDataStore(
        const TableName &table_name,
        const TableSchema *table_schema,
        NodeGroupId ng_id,
        uint64_t commit_ts);
    void ScanNextFromDataStore(store::DataStoreScanner *ds_scanner,
                               bool &is_first_scan,
                               const TxKey *&target_key,
                               const TxRecord *&target_rec);
    void FinishScanFromDataStore(
        std::unique_ptr<store::DataStoreScanner> &ds_scanner);

    void FetchTuplesAndUploadPackedKey(TransactionExecution *txm);

    bool NeedTriggerFlushSkOp()
    {
        return (scanned_pk_range_count_ % 60 == 0) ||
               (last_scanned_end_key_ == nullptr ||
                last_scanned_end_key_->Type() == KeyType::PositiveInf);
    }
    void DispatchRangeTask(TransactionExecution *upsert_index_txm,
                           CcHandlerResult<Void> &hd_res);
    void HandleRangeTask(
        const TableName &base_table_name,
        int32_t partition_id,
        const TxKey *range_start_key,
        const TxKey *range_end_key,
        NodeGroupId range_owner,
        uint64_t scan_ts,
        uint64_t tx_number,
        int64_t tx_term,
        std::mutex &task_mux,
        std::condition_variable &task_cv,
        uint32_t &unfinished_task_cnt,
        bool &all_task_started,
        uint32_t &total_pk_items_count,
        uint32_t &dispatched_task_count,
        CcErrorCode &task_res,
        std::function<void(const TxKey *batch_range_start_key,
                           const TxKey *batch_range_end_key,
                           const std::string *batch_range_start_key_str,
                           const std::string *batch_range_end_key_str,
                           const TxKey *&last_scanned_end_key,
                           bool &is_last_scanned_key_str,
                           size_t batch_range_cnt,
                           uint32_t &actual_task_cnt)> &dispatch_func);

    void UpdateBatchRangeSize()
    {
        uint8_t new_batch_size = scan_batch_range_size_ * 0.8;
        scan_batch_range_size_ = new_batch_size > 1 ? new_batch_size : 1;
    }

#if WITH_KV_STORAGE != KV_CASS
    // Scan pk from ccmap
    bool PrepareScanFromCcMap(const TableName &table_name,
                              size_t &scan_alias,
                              TransactionExecution *&scan_txm);
    bool ScanNextFromCcMap(TransactionExecution *scan_txm,
                           const TableName &table_name,
                           const size_t &scan_alias,
                           uint64_t commit_ts,
                           std::vector<ScanBatchTuple> &scan_batch,
                           size_t &scan_batch_idx,
                           bool &is_last_scan_batch,
                           const TxKey *&target_key,
                           const TxRecord *&target_rec);
    void FinishScanFromCcMap(TransactionExecution *scan_txm,
                             const TableName &table_name,
                             const size_t &scan_alias,
                             std::vector<ScanBatchTuple> &scan_batch,
                             bool is_success);

    uint8_t PrefetchSize()
    {
        std::array<uint32_t, 5> boundaries = {1, 4, 16, 64, 256};

        size_t idx = 0;
        for (; idx < boundaries.size(); ++idx)
        {
            if (scan_batch_cnt_ < boundaries[idx])
            {
                break;
            }
        }

        return idx < boundaries.size() ? boundaries[idx] - 1 : 255;
    }

    uint8_t scan_batch_cnt_;
#endif

    // This variable have two roles:
    // 1) deserialize as AlterTableInfo object. 2) save into log.
    std::string alter_table_info_image_str_{""};
    AlterTableInfo alter_table_info_;

    // Store the node group leader terms after acquired them.
    std::vector<int64_t> leader_terms_;
    CcHandlerResult<PostProcessResult> post_write_result_;

    // Due to term or other error, called ForceToFinish to terminate this
    // operation
    bool is_force_finished_{false};

    CcRequestPool<PostWriteCc> upload_pool_;

#ifdef NDEBUG
    uint8_t scan_batch_range_size_{10};
#else
    uint8_t scan_batch_range_size_{3};
#endif

    union
    {
        const TxKey *last_scanned_end_key_;
        const std::string *last_scanned_end_key_str_;
    };
    bool is_last_scanned_key_str_;
    std::vector<TableName> new_indexes_name_;
    std::vector<std::thread> local_task_workers_;
    size_t scanned_pk_range_count_{0};
    size_t finished_pk_range_count_{0};
    size_t total_scanned_pk_items_count_{0};
};

}  // namespace txservice
