#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "catalog_key_record.h"
#include "cc_entry.h"
#include "cluster_config_record.h"
#include "log_closure.h"
#include "metrics.h"
#include "range_record.h"
#include "read_write_set.h"
#include "tx_command.h"
#include "tx_key.h"
#include "tx_operation_result.h"
#include "tx_record.h"

namespace txservice
{
class TransactionExecution;
struct ReadTxRequest;
struct ReadOutsideTxRequest;
struct ScanOpenTxRequest;
struct ScanBatchTxRequest;
struct ScanBatchTuple;
struct AnalyzeTableTxRequest;
struct BatchReadTxRequest;
struct DataMigrationStatus;

#define RETRY_NUM 3

enum class TxLogType
{
    DATA,
    PREPARE,
    COMMIT,
    CLEAN
};

void AdvanceWriteKeyForRangeInfo(const RangeRecord &range_record,
                                 TableWriteSet &table_write_set,
                                 TableWriteSet::iterator &write_key_it,
                                 const TableWriteSet::iterator &write_key_end,
                                 ReadWriteSet &rw_set);

struct TransactionOperation
{
    TransactionOperation()
    {
    }
    virtual ~TransactionOperation() = default;
    /**
     * @brief Called by txm->Forward() for determining the how
     * this operation will be processed, e.g. calling txm->Process(this) if this
     * operation has not been processed, or calling txm->PostProcess(this) if
     * the result has been set finished, or rerun this operation if the result
     * is set error.
     */
    virtual void Forward(TransactionExecution *txm) = 0;

    /**
     * @brief If operation fails since remote node dies, auto-failover will
     * elect a new leader and recover the dead node group. Re-run the operator
     * automatically to avoid client to re-run the whole query.
     *
     */
    void ReRunOp(TransactionExecution *txm);

    int retry_num_{RETRY_NUM};
    bool is_running_{false};
    static const uint64_t tx_op_failed_ts_ = 0;
    metrics::TimePoint op_start_{metrics::TimePoint::max()};
};

struct CompositeTransactionOperation : TransactionOperation
{
    CompositeTransactionOperation();

    virtual ~CompositeTransactionOperation() = default;

    template <typename Op>
    void ForwardToSubOperation(TransactionExecution *txm, Op *next_op);

    template <typename Op>
    void RetrySubOperation(TransactionExecution *txm, Op *next_op);

    /**
     * @brief The current stage of this multi-stage schema operation
     */
    TransactionOperation *op_{nullptr};
};

struct ReadLocalOperation : TransactionOperation
{
public:
    void Reset();
    void Reset(TableName tbl_name,
               const TxKey *key,
               TxRecord *rec,
               CcHandlerResult<ReadKeyResult> *hd_res)
    {
        table_name_ = std::move(tbl_name);
        key_ = key;
        rec_ = rec;
        hd_result_ = hd_res;
    }

    void Forward(TransactionExecution *txm) override;

    // in-parameters
    const TxKey *key_{};
    TableName table_name_{empty_sv, TableType::RangePartition};
    TxRecord *rec_{};

    // out-parameters, to pass result to caller operation
    CcHandlerResult<ReadKeyResult> *hd_result_{};
};

struct ReadOperation : TransactionOperation
{
public:
    explicit ReadOperation(
        TransactionExecution *txm,
        CcHandlerResult<ReadKeyResult> *lock_range_result = nullptr);

    void Reset();
    void Forward(TransactionExecution *txm) override;

    ReadType read_type_{ReadType::Inside};
    CcProtocol protocol_{CcProtocol::OCC};
    IsolationLevel iso_level_{IsolationLevel::ReadCommitted};
    ReadTxRequest *read_tx_req_{nullptr};
    ReadOutsideTxRequest *read_outside_tx_req_{nullptr};
    CcHandlerResult<ReadKeyResult> hd_result_;
    bool local_cache_miss_{false};

#ifdef RANGE_PARTITION_ENABLED
    CcHandlerResult<ReadKeyResult> *lock_range_result_{nullptr};
#endif
};

struct PostReadOperation : TransactionOperation
{
public:
    explicit PostReadOperation(TransactionExecution *txm);
    void ResetHandlerTxm(TransactionExecution *txm);

    void Reset(const CcEntryAddr *cce_addr = nullptr,
               const ReadSetEntry *read_set_entry = nullptr);

    void Forward(TransactionExecution *txm) override;

    const CcEntryAddr *cce_addr_;
    const ReadSetEntry *read_set_entry_;
    CcHandlerResult<PostProcessResult> hd_result_;
};

struct SetCommitTsOperation : TransactionOperation
{
public:
    explicit SetCommitTsOperation(TransactionExecution *txm);
    void Reset();
    void Forward(TransactionExecution *txm) override;

    CcHandlerResult<uint64_t> hd_result_;
};

struct ValidateOperation : TransactionOperation
{
public:
    static const uint32_t default_read_set_capacity = 16;

    explicit ValidateOperation(TransactionExecution *txm);
    void Reset(size_t read_cnt);
    bool IsError();
    void Forward(TransactionExecution *txm) override;

    CcHandlerResult<PostProcessResult> hd_result_;
};

/**
 * @brief
 * Acquire write lock operation.
 * Write are cached in local rwset for each transaction, hence no write lock is
 * held at write operation. This operation is called right after Commit request.
 */
struct AcquireWriteOperation : TransactionOperation
{
public:
    explicit AcquireWriteOperation(TransactionExecution *txm);
    void Reset(size_t acquire_write_cnt, size_t wentry_cnt);
    void Reset();
    void AggregateAcquiredKeys(TransactionExecution *txm);
    void Forward(TransactionExecution *txm) override;

    CcHandlerResult<std::vector<AcquireKeyResult>> hd_result_;
    std::vector<WriteSetEntry *> acquire_write_entries_{16};

    // Number of remote keys on which the acquire write operation needs to
    // acquire write intentions/locks.
    std::atomic<int32_t> remote_ack_cnt_{0};
    // Identify whether any keys in rset are expired (may be updated by other
    // tx) under the RepeatableRead or Serializable isolation level.
    bool rset_has_expired_{false};
};

#ifdef RANGE_PARTITION_ENABLED
struct LockWriteRangesOp : public TransactionOperation
{
public:
    LockWriteRangesOp(CcHandlerResult<ReadKeyResult> *lock_range_result)
        : lock_range_result_(lock_range_result)
    {
    }

    void Forward(TransactionExecution *txm) override;

    void Reset()
    {
        init_ = false;
        is_running_ = false;
    }

    /**
     * @brief Advances the internal iterator to the next range to acquire a
     * write lock.
     *
     */
    void Advance(TransactionExecution *txm);

    TableName range_table_name_{empty_sv, TableType::RangePartition};
    // RangeRecord range_rec_;
    CcHandlerResult<ReadKeyResult> *lock_range_result_{nullptr};

    std::unordered_map<TableName, TableWriteSet>::iterator table_it_;
    std::unordered_map<TableName, TableWriteSet>::iterator table_end_;
    TableWriteSet::iterator write_key_it_;
    TableWriteSet::iterator write_key_end_;
    bool init_;
};
#endif

struct ReloadCacheOperation : TransactionOperation
{
    ReloadCacheOperation(TransactionExecution *txm);
    void Reset(uint32_t hres_ref_cnt);
    void Forward(TransactionExecution *txm) override;

    CcHandlerResult<Void> hd_result_;
};

struct FaultInjectOp : TransactionOperation
{
public:
    FaultInjectOp(TransactionExecution *txm);

    void Set(const std::string &fault_name,
             const std::string &fault_paras,
             std::vector<int> vct_node_id)
    {
        fault_name_ = fault_name;
        fault_paras_ = fault_paras;
        vct_node_id_ = vct_node_id;
        succeed_ = false;
    }

    void Reset();
    void Forward(TransactionExecution *txm) override;

    std::string fault_name_;
    std::string fault_paras_;
    std::vector<int> vct_node_id_;
    bool succeed_;
    CcHandlerResult<bool> hd_result_;
};

struct WriteToLogOp : TransactionOperation
{
    WriteToLogOp(TransactionExecution *txm);
    void Forward(TransactionExecution *txm) override;
    void Reset();
    void ResetHandlerTxm(TransactionExecution *txm);

    TxLogType log_type_{TxLogType::DATA};
    uint32_t log_group_id_{0};
    CcHandlerResult<Void> hd_result_;
    LogClosure log_closure_{&hd_result_};
};

struct UpdateTxnStatus : TransactionOperation
{
    UpdateTxnStatus(TransactionExecution *txm);
    void Reset();
    void Forward(TransactionExecution *txm) override;

    CcHandlerResult<Void> hd_result_;
};

struct PostProcessOp : TransactionOperation
{
    PostProcessOp(TransactionExecution *txm);
    void Reset(size_t write_cnt,
               size_t data_read_cnt,
               size_t catalog_range_read_cnt);
    void Forward(TransactionExecution *txm) override;

    CcHandlerResult<PostProcessResult> hd_result_;
    CcHandlerResult<PostProcessResult> catalog_range_hd_result_;
};

struct InitTxnOperation : TransactionOperation
{
    explicit InitTxnOperation(TransactionExecution *txm);
    void Reset();
    void Forward(TransactionExecution *txm) override;

    uint32_t log_group_id_{UINT32_MAX};
    uint32_t tx_ng_id_;
    CcHandlerResult<InitTxResult> hd_result_;
};

struct ScanOpenOperation : TransactionOperation
{
    explicit ScanOpenOperation(TransactionExecution *txm);
    void Forward(TransactionExecution *txm) override;

    void Set(const TableName *table_name,
             ScanIndexType index_type,
             const TxKey *key,
             bool inclusive,
             ScanDirection direction,
             bool is_ckpt_delta)
    {
        table_name_ = table_name;
        index_type_ = index_type;
        start_key_ = key;
        inclusive_ = inclusive;
        direction_ = direction;
        is_ckpt_delta_ = is_ckpt_delta;
    }

    void Reset();

    CcHandlerResult<ScanOpenResult> hd_result_;

    const TableName *table_name_{nullptr};
    ScanIndexType index_type_{ScanIndexType::Primary};
    const TxKey *start_key_{nullptr};
    bool inclusive_{true};
    ScanDirection direction_{ScanDirection::Forward};
    bool is_ckpt_delta_{false};
    ScanOpenTxRequest *tx_req_{nullptr};
};

struct ScanState
{
    ScanState() = delete;
    ScanState(std::unique_ptr<CcScanner> scanner,
              const TxKey *end_key,
              bool end_inclusive)
        : scanner_(std::move(scanner)),
          scan_end_key_(end_key),
          scan_end_inclusive_(end_inclusive)
    {
    }

    std::unique_ptr<CcScanner> scanner_;
    const TxKey *scan_end_key_;
    bool scan_end_inclusive_;

#ifdef RANGE_PARTITION_ENABLED
    ScanState(std::unique_ptr<CcScanner> scanner,
              const TxKey *end_key,
              bool end_inclusive,
              uint32_t range_id,
              NodeGroupId range_ng,
              const TxKey *last_key,
              bool inclusive,
              SlicePosition position)
        : scanner_(std::move(scanner)),
          scan_end_key_(end_key),
          scan_end_inclusive_(end_inclusive),
          range_id_(range_id),
          range_ng_(range_ng),
          slice_last_key_ptr_(last_key),
          is_key_owner_(false),
          inclusive_(inclusive),
          slice_position_(position)
    {
    }

    ScanState(std::unique_ptr<CcScanner> scanner,
              const TxKey *end_key,
              bool end_inclusive,
              uint32_t range_id,
              NodeGroupId range_ng,
              std::unique_ptr<TxKey> last_key,
              bool inclusive,
              SlicePosition position)
        : scanner_(std::move(scanner)),
          scan_end_key_(end_key),
          scan_end_inclusive_(end_inclusive),
          range_id_(range_id),
          range_ng_(range_ng),
          slice_last_key_uptr_(std::move(last_key)),
          is_key_owner_(true),
          inclusive_(inclusive),
          slice_position_(position)
    {
    }

    ~ScanState()
    {
        if (is_key_owner_)
        {
            slice_last_key_uptr_ = nullptr;
        }
    }

    void SetSliceLastKey(TxKey::Uptr slice_last_key)
    {
        if (!is_key_owner_)
        {
            slice_last_key_uptr_.release();
        }
        slice_last_key_uptr_ = std::move(slice_last_key);
        is_key_owner_ = true;
    }

    const TxKey *SliceLastKey() const
    {
        return is_key_owner_ ? slice_last_key_uptr_.get() : slice_last_key_ptr_;
    }

    uint32_t range_id_;
    NodeGroupId range_ng_;
    union
    {
        const TxKey *slice_last_key_ptr_;
        TxKey::Uptr slice_last_key_uptr_;
    };
    bool is_key_owner_{false};
    bool inclusive_;
    SlicePosition slice_position_;
    CcEntryAddr range_cce_addr_;
#endif
};

struct ScanNextOperation : TransactionOperation
{
    explicit ScanNextOperation(TransactionExecution *txm);
    void Forward(TransactionExecution *txm) override;
    void Reset();
    void ResetResult();

    ScanDirection Direction() const
    {
        return scan_state_->scanner_ != nullptr
                   ? scan_state_->scanner_->Direction()
                   : ScanDirection::Forward;
    }

    void UpdateScanState(ScanState *scan_state)
    {
        scan_state_ = scan_state;
#ifdef RANGE_PARTITION_ENABLED
        slice_hd_result_.Value().ccm_scanner_ = scan_state->scanner_.get();
#endif
    }

    ScanState *scan_state_;
    CcHandlerResult<ScanNextResult> hd_result_;

#ifdef RANGE_PARTITION_ENABLED
    int64_t RangeNgTerm() const
    {
        return slice_hd_result_.Value().ccm_scanner_->PartitionNgTerm();
    }

    SlicePosition LastScannedSlicePosition() const
    {
        return scan_state_->slice_position_;
    }

    CcHandlerResult<RangeScanSliceResult> slice_hd_result_;
    TableName range_table_name_{empty_sv, TableType::RangePartition};
    RangeRecord range_rec_;
    CcHandlerResult<ReadKeyResult> lock_range_result_;
    CcHandlerResult<PostProcessResult> unlock_range_result_;
#endif

    size_t alias_{0};
    ScanBatchTxRequest *tx_req_{nullptr};
};

struct AcquireAllOp : public TransactionOperation
{
    AcquireAllOp(TransactionExecution *txm);
    void Resize(size_t new_size);
    void Reset(size_t node_cnt);
    void ResetHandlerTxm(TransactionExecution *txm);
    void Forward(TransactionExecution *txm) override;
    /**
     * @brief Get the max commit/validate ts of the all result
     */
    uint64_t MaxTs();

    std::vector<CcHandlerResult<AcquireAllResult>> hd_results_;
    uint32_t upload_cnt_{0};
    std::atomic<uint32_t> finish_cnt_{0};
    std::atomic<uint32_t> fail_cnt_{0};
    // Number of remote keys on which the upload operation needs to acquire
    // write intentions/locks.
    std::atomic<int32_t> remote_ack_cnt_{0};

    const TableName *table_name_{nullptr};
    const TxKey *key_{nullptr};

    CcOperation cc_op_{CcOperation::ReadForWrite};
    CcProtocol protocol_{CcProtocol::OCC};
};

struct PostWriteAllOp : public TransactionOperation
{
    PostWriteAllOp(TransactionExecution *txm);
    void Reset(uint32_t ng_cnt);
    void ResetHandlerTxm(TransactionExecution *txm);
    void Forward(TransactionExecution *txm) override;
    bool IsFailed();

    CcHandlerResult<PostProcessResult> hd_result_;

    const TableName *table_name_{nullptr};
    const TxKey *key_{nullptr};
    TxRecord *rec_{nullptr};
    OperationType op_type_{OperationType::Upsert};
    PostWriteType write_type_{PostWriteType::PrepareCommit};
};

struct DsUpsertTableOp : public TransactionOperation
{
    DsUpsertTableOp() = delete;
    DsUpsertTableOp(const TableName *table_name,
                    OperationType op_type,
                    TransactionExecution *txm);

    void Reset();
    void ResetHandlerTxm(TransactionExecution *txm);
    void Forward(TransactionExecution *txm) override;

    const TableName *table_name_{nullptr};
    const TableSchema *table_schema_{nullptr};
    OperationType op_type_{OperationType::Upsert};
    CcHandlerResult<Void> hd_result_;
    txservice::AlterTableInfo *alter_table_info_{nullptr};
};

template <typename ResultType>
struct AsyncOp : public TransactionOperation
{
    AsyncOp() = delete;
    ~AsyncOp();
    explicit AsyncOp(TransactionExecution *txm);
    void ResetHandlerTxm(TransactionExecution *txm);

    void Forward(TransactionExecution *txm) override;
    void Reset();

    std::function<void()> op_func_;
    CcHandlerResult<ResultType> hd_result_;
    std::thread worker_thread_;
    bool handle_timeout_{false};
    uint32_t wait_secs_{10};
};

struct SchemaOp : public TransactionOperation
{
    SchemaOp() = delete;
    SchemaOp(const std::string_view table_name_sv,
             const std::string &current_image,
             const std::string &dirty_image,
             uint64_t schema_ts,
             OperationType op_type);

    void FillPrepareLogRequestCommon(TransactionExecution *txm,
                                     WriteToLogOp &prepare_log_op);
    void FillCommitLogRequestCommon(TransactionExecution *txm,
                                    WriteToLogOp &commit_log_op);
    void FillCleanLogRequestCommon(TransactionExecution *txm,
                                   WriteToLogOp &clean_log_op);

    CatalogKey table_key_;  // string owner
    CatalogRecord catalog_rec_;
    std::string image_str_{""};
    std::string dirty_image_str_{""};
    uint64_t curr_schema_ts_;
    OperationType op_type_{OperationType::Insert};
};

struct UpsertTableOp : public SchemaOp
{
    UpsertTableOp() = delete;
    UpsertTableOp(const std::string_view table_name_str,
                  const std::string &current_image,
                  uint64_t curr_schema_ts,
                  const std::string &dirty_image,
                  OperationType op_type,
                  TransactionExecution *txm);

    void Reset(const std::string_view table_name_str,
               const std::string &current_image,
               uint64_t curr_schema_ts,
               const std::string &dirty_image,
               OperationType op_type,
               TransactionExecution *txm);

    void Forward(TransactionExecution *txm) override;

    /**
     * @brief The current stage of this multi-stage schema operation.
     *
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
     *
     */
    AcquireAllOp acquire_all_intent_op_;
    /**
     * @brief Flushes the prepare log to the log service. The schema operation
     * is guaranteed to succeed after this stage.
     *
     */
    WriteToLogOp prepare_log_op_;
    /**
     * @brief Installs the dirty schema in the tx service and returns a local
     * view (pointer) of it.
     *
     */
    PostWriteAllOp post_all_intent_op_;
    /**
     * @brief Release cluster config read lock after post write all.
     */
    PostReadOperation unlock_cluster_config_op_;
    /**
     * @brief Creates/deletes the data store table and persists/removes the
     * binary representation of the catalog in the data store.
     *
     */
    DsUpsertTableOp upsert_kv_table_op_;
    /**
     * @brief Flush sequence data log to the log service. This data log ensures
     * that even in the case of failover, the values in the sequence ccmap are
     * up-to-date.
     */
    WriteToLogOp sequence_data_log_op_;
    /**
     * @brief Reset the sequence record in the sequence table if this table has
     * auto_increment column during create table.
     */
    AsyncOp<PostProcessResult> reset_sequence_record_op_;
    /**
     * @brief Upgrades acquired write intents to write locks in all nodes.
     *
     */
    AcquireAllOp acquire_all_lock_op_;
    /**
     * @brief Flushes the commit log to the log service. The commit log confirms
     * that the data store operation succeeds and does not need redo upon
     * failures.
     *
     */
    WriteToLogOp commit_log_op_;
    /**
     * @brief Removes write locks in all nodes. If the schema operation
     * succeeds, also installs the new schema in all nodes.
     *
     */
    PostWriteAllOp post_all_lock_op_;
    /**
     * @brief The last log operation that removes the schema record from the log
     * state machine.
     *
     */
    WriteToLogOp clean_log_op_;

    CcHandlerResult<ReadKeyResult> read_cluster_result_;
    ClusterConfigRecord cluster_conf_rec_;

private:
    void FillPrepareLogRequest(TransactionExecution *txm);
    void FillCommitLogRequest(TransactionExecution *txm);
    void ForceToFinish(TransactionExecution *txm);
    // Due to term or other error, called ForceToFinish to terminate this
    // operation
    bool is_force_finished;
};

struct SleepOperation : TransactionOperation
{
public:
    explicit SleepOperation(TransactionExecution *txm);

    void Forward(TransactionExecution *txm) override;

    int sleep_secs_{0};
};

struct CleanCcEntryForTestOp : TransactionOperation
{
public:
    explicit CleanCcEntryForTestOp(TransactionExecution *txm);

    void Set(const TableName *tn,
             const TxKey *key,
             bool only_archives,
             bool flush)
    {
        tab_name_ = tn;
        key_ = key;
        only_archives_ = only_archives;
        flush_ = flush;
        succeed_ = false;
    }

    void Reset()
    {
        succeed_ = false;
        hd_result_.Reset();
    }
    void Forward(TransactionExecution *txm) override;

    const TableName *tab_name_{nullptr};
    const TxKey *key_{nullptr};
    bool only_archives_{false};
    bool flush_{true};

    bool succeed_{false};
    CcHandlerResult<bool> hd_result_;
};

struct NoOp : public TransactionOperation
{
    NoOp(TransactionExecution *txm);
    void Forward(TransactionExecution *txm) override;
    CcHandlerResult<Void> hd_result_;
};

struct FlushDataOp : public TransactionOperation
{
    FlushDataOp(TransactionExecution *txm);
    void ResetHandlerTxm(TransactionExecution *txm);
    void Forward(TransactionExecution *txm) override;
    void Reset();

    const TableName *tab_name_{nullptr};
    uint64_t data_sync_ts_;
    NodeGroupId node_group_;
    int64_t tx_term_;
    const TableSchema *schema_{nullptr};
    std::vector<FlushRecord> *data_sync_vec_{nullptr};
    std::vector<FlushRecord> *archive_vec_{nullptr};
    std::vector<const TxKey *> *mv_vec_{nullptr};
    CcHandlerResult<Void> hd_result_;
    bool delay_update_ckpt_ts_{false};
};

struct KickoutDataOp : public TransactionOperation
{
    KickoutDataOp(TransactionExecution *txm);
    void Reset();
    void ResetHandlerTxm(TransactionExecution *txm);
    void Forward(TransactionExecution *txm) override;

    const TableName *table_name_{nullptr};
    NodeGroupId node_group_;
    uint64_t commit_ts_{0};
    const TxKey *start_key_{nullptr};
    const TxKey *end_key_{nullptr};
    CcHandlerResult<Void> hd_result_;
};

struct SplitFlushRangeOp : public CompositeTransactionOperation
{
    SplitFlushRangeOp() = delete;

    SplitFlushRangeOp(
        const TableName &table_name,
        const TableSchema *table_schema,
        const TxKey *old_start_key,
        const TxKey *old_end_key,
        const RangeInfo *old_range_info,
        std::vector<std::pair<TxKey::Uptr, int32_t>> &&new_range_info,
        uint64_t previous_scan_ts,
        std::vector<FlushRecord> &&previous_data_sync_vec,
        std::vector<FlushRecord> &&previous_archive_vec,
        std::vector<const TxKey *> &&previous_mv_base_vec,
        TransactionExecution *txm);

    void Reset(const TableName &table_name,
               const TableSchema *table_schema,
               const TxKey *old_start_key,
               const TxKey *old_end_key,
               const RangeInfo *old_range_info,
               std::vector<std::pair<TxKey::Uptr, int32_t>> &&new_range_info,
               uint64_t previous_scan_ts,
               std::vector<FlushRecord> &&previous_data_sync_vec,
               std::vector<FlushRecord> &&previous_archive_vec,
               std::vector<const TxKey *> &&previous_mv_base_vec,
               TransactionExecution *txm);

    void Forward(TransactionExecution *txm) override;

    const TableSchema *table_schema_{nullptr};
    TableName table_name_;        // TableName owner.
    TableName range_table_name_;  // References table_name_.
    CcHandlerResult<ReadKeyResult> read_cluster_result_;
    ClusterConfigRecord cluster_conf_rec_;

    RangeInfo range_info_;
    std::unique_ptr<RangeRecord> range_record_;
    // TODO{liunyl}: change these to Uptr after we update inf key instance.
    // Now we need to use raw pointers to accomadate with inf key instance.
    // Now we make them point to the Uptr in range_info_ if they are normal key,
    // or raw pointers to inf key instances otherwise.
    const TxKey *old_start_key_;
    const TxKey *old_end_key_;
    // vector< new start key, new partition id >
    std::vector<std::pair<TxKey::Uptr, int32_t>> new_range_info_;
    // Used during commit post write. We cannot rely on the range slice stored
    // in TableRangeEntry since that might become invalid during retry.
    std::vector<std::pair<TxKey::Uptr, size_t>> slice_info_;

    // vector buffer used during checkpoint scan
    std::vector<FlushRecord> data_sync_vec_;
    std::vector<FlushRecord> archive_vec_;
    std::vector<const TxKey *> mv_base_vec_;

    uint64_t previous_scan_ts_{0};
    std::vector<FlushRecord> previous_data_sync_vec_;
    std::vector<FlushRecord> previous_archive_vec_;
    std::vector<const TxKey *> previous_mv_base_vec_;
    bool scan_finished_{false};
    std::unordered_map<size_t, int32_t> old_delta_sizes_;

    std::vector<std::pair<TxKey::Uptr, int32_t>>::const_iterator
        kickout_data_it_;

    /**
     * @brief Acquire read lock on local cluster config ccmap to block cluster
     * config update during upsert table op. We cannot allow config update
     * between acquire write all and post write all.
     */
    ReadLocalOperation lock_cluster_config_op_;
    /**
     * @brief Acquire write lock on all node groups. Since split-flush op is
     * the only operation that would try to acquire write lock on range
     * table entry, and each range is only sharded to one node group, we
     * should be the only one trying to acquire write lock on this range
     * entry. So we can directly try to acquire write lock and not worrying
     * about dead locks. The last valid ts returned by this op will be used
     * to calculate commit ts of the split-flush tx.
     */
    AcquireAllOp prepare_acquire_all_write_op_;
    /**
     * @brief Write prepare log. Prepare log should have
     * 1. The old range id.
     * 2. The new range id.
     * 3. The start key of new range id.
     * 4. The commit ts of this split-flush tx.
     */
    WriteToLogOp prepare_log_op_;
    /**
     * @brief Add new partition id to range entry on each node group. Now
     * write operation will write to both new and old range partition.
     * Downgrade write lock to write intent lock.
     */
    PostWriteAllOp install_new_range_op_;
    /**
     * @brief Release cluster config lock after post write all.
     */
    PostReadOperation unlock_cluster_config_op_;
    /**
     * @brief Scan for data before commit_ts in the splitting range. We need to
     * make these data available to the new range before we commit the range
     * split.
     */
    AsyncOp<Void> data_sync_scan_op_;
    /**
     * @brief Flush data in memory before commit_ts to KV storage. These data
     * will be flushed into new partitions. We can't safely update ckpt_ts of
     * CcEntry for now.
     */
    FlushDataOp flush_op_;
    /**
     * @brief Acquire write lock on all node group on the old partition and
     * new partition.
     */
    AcquireAllOp commit_acquire_all_write_op_;

    /**
     * @brief We can safely update ckpt_ts of CcEntry after acquiring range
     * write lock
     */
    AsyncOp<Void> update_ckpt_ts_op_;

    /**
     * @brief Write commit log.
     */
    WriteToLogOp commit_log_op_;
    /**
     * @brief Upsert new ranges into range table in KV store.
     */
    AsyncOp<Void> ds_upsert_range_op_;
    /**
     * @brief Kickout old range data from cc map if the data now
     * falls on a new node group.
     */
    KickoutDataOp kickout_old_range_data_op_;
    /**
     * @brief 1. Insert new range into range tables on all nodes
     * 2. Remove new range info from old range entry.
     * 3. Release locks on all nodes.
     */
    PostWriteAllOp post_all_lock_op_;
    /**
     * @brief Remove obselete data from old range in KV store. We don't need
     * any lock here since these data will not be visible to anyone after the
     * new range info has been comitted.
     */
    AsyncOp<Void> ds_clean_old_range_op_;
    /**
     * @brief Remove split-flush log.
     */
    WriteToLogOp clean_log_op_;

private:
    /**
     * Merge multi sorted ascending vectors into a single one. Remove the data
     * from previous_datas. Note that We need to make sure the element of input
     * vector has not duplication.
     */
    void MergeFlushRecord(std::vector<FlushRecord> &&previous_datas,
                          std::vector<std::vector<FlushRecord>> &&datas,
                          std::vector<FlushRecord> &output,
                          std::vector<FlushRecord> &new_archive_records,
                          std::unordered_map<size_t, int32_t> &old_delta_sizes,
                          bool enable_mvcc);

    void FillPrepareLogRequest(TransactionExecution *txm);
    void FillCommitLogRequest(TransactionExecution *txm);
    void FillCleanLogRequest(TransactionExecution *txm);
    void ForceToFinish(TransactionExecution *txm);
    void ClearDataSyncVec();
    void ClearInfos();
};

// To remove remainder records' lock when scan close
struct ReleaseScanExtraLockOp : TransactionOperation
{
    explicit ReleaseScanExtraLockOp(TransactionExecution *txm);
    void Reset();
    void Forward(TransactionExecution *txm) override;

    CcHandlerResult<PostProcessResult> hd_result_;
};

struct AnalyzeTableAllOp : TransactionOperation
{
private:
    constexpr static uint32_t range_sample_pool_capacity_{128};

public:
    AnalyzeTableAllOp(TransactionExecution *txm);
    void Reset(uint32_t hres_ref_cnt);
    void Forward(TransactionExecution *txm) override;

    AnalyzeTableTxRequest *analyze_tx_req_{nullptr};
    CcHandlerResult<Void> hd_result_;
};

struct ObjectCommandOp : TransactionOperation
{
    explicit ObjectCommandOp(TransactionExecution *txm);
    void Reset(const TableName *table_name,
               const TxKey *key,
               const TxCommand *command,
               TxCommandResult *cmd_result,
               bool auto_commit = false);
    void Forward(TransactionExecution *txm) override;

    const TableName *table_name_{};
    const TxKey *key_{};
    const TxCommand *command_{};
    TxCommandResult *cmd_result_{};
    CcHandlerResult<ObjectCommandResult> hd_result_;

    bool auto_commit_{};

#ifdef RANGE_PARTITION_ENABLED
    RangeRecord range_rec_;
    CcHandlerResult<ReadKeyResult> lock_range_result_;
#endif
};

class NotifyMigrationClosure : public google::protobuf::Closure
{
public:
    NotifyMigrationClosure(
        std::atomic<size_t> *unfinished_cnt,
        std::unordered_map<NodeGroupId, BucketMigrateInfo> &migrate_plans)
        : cntl_(),
          unfinished_cnt_(unfinished_cnt),
          migrate_plans_(migrate_plans)
    {
    }

    ~NotifyMigrationClosure() = default;

    void Run() override
    {
        if (cntl_.Failed())
        {
            LOG(INFO)
                << "Cluster scale tx notify migration response, node group: "
                << request_.orig_owner() << ", success: false";
            migrate_plans_[request_.orig_owner()].has_migration_tx_ = false;
        }
        else
        {
            LOG(INFO)
                << "Cluster scale tx notify migration response, node group: "
                << request_.orig_owner()
                << ", success: " << response_.success();
            migrate_plans_[request_.orig_owner()].has_migration_tx_ =
                response_.success();
        }

        unfinished_cnt_->fetch_sub(1, std::memory_order_release);
    }

    remote::InitMigrationRequest &Request()
    {
        return request_;
    }

    remote::InitMigrationResponse &Response()
    {
        return response_;
    }

    brpc::Controller *Controller()
    {
        return &cntl_;
    }

    void Reset()
    {
        cntl_.Reset();
        response_.Clear();
    }

private:
    brpc::Controller cntl_;
    remote::InitMigrationRequest request_;
    remote::InitMigrationResponse response_;
    std::atomic<size_t> *unfinished_cnt_;
    std::unordered_map<NodeGroupId, BucketMigrateInfo> &migrate_plans_;
};

struct NotifyStartMigrateOp : public TransactionOperation
{
    explicit NotifyStartMigrateOp(TransactionExecution *txm);

    void Clear();
    void Reset(size_t node_group_count);
    void Forward(TransactionExecution *txm) override;

    void InitDataMigration(TxNumber tx_number, NodeGroupId old_owner);

    std::unordered_map<NodeGroupId, BucketMigrateInfo> migrate_plans_;

    std::atomic<size_t> unfinished_req_cnt_{0};
    std::vector<std::unique_ptr<NotifyMigrationClosure>> closures_;
};

struct CheckMigrationIsFinishedOp : public TransactionOperation
{
    explicit CheckMigrationIsFinishedOp(TransactionExecution *txm);

    void Reset();
    void Forward(TransactionExecution *txm) override;

    bool migration_is_finished_{false};
    std::atomic<bool> rpc_is_finished_{false};

    CheckMigrationIsFinishedClosure closure_;
};

/**
 * Cluster scale op consists of 2 parts, changing cluster config and migrating
 * data. Changing cluster config will only modify peers and cc node group
 * configs, but it does not rebalance data among node groups. Data migration
 * will rebalance data among node groups after the cluster config change.
 */
struct ClusterScaleOp : public CompositeTransactionOperation
{
public:
    ClusterScaleOp() = delete;
    ClusterScaleOp(ClusterScaleOpType event_type,
                   std::unordered_map<NodeGroupId, std::vector<NodeConfig>>
                       &&new_ng_config,
                   TransactionExecution *txm);
    void Reset(ClusterScaleOpType event_type,
               std::unordered_map<NodeGroupId, std::vector<NodeConfig>>
                   &&new_ng_config,
               TransactionExecution *txm);
    void Forward(TransactionExecution *txm) override;

    remote::ClusterScaleStatus GetStatus(TxNumber txn)
    {
        std::unique_lock<std::mutex> lk(mux_);
        if (txn != txn_)
        {
            return remote::ClusterScaleStatus::INVALID_TXN;
        }
        return status_;
    }

    bool SetStatus(TxNumber txn, remote::ClusterScaleStatus status)
    {
        std::unique_lock<std::mutex> lk(mux_);
        if (txn != txn_)
        {
            return false;
        }
        status_ = status;
        return true;
    }

    std::unordered_map<NodeGroupId, BucketMigrateInfo> bucket_migrate_infos_;
    /**
     * Cluster scale tx has different op processing order based on the event
     * type. For add node, the order is
     * 1. prepare_log_op_
     * 2. acquire_cluster_config_write_op_
     * 3. update_cluster_config_log_op_
     * 4. flush_new_cluster_config_op_
     * 5. install_cluster_config_op_
     * 6. notify_migration_op_
     * 7. check_migration_is_finished_op_
     * 8. clean_log_op_
     *
     * For remove node, the order is
     * 1. prepare_log_op_
     * 2. notify_migration_op_
     * 3. check_migration_is_finished_op_
     * 4. acquire_cluster_config_write_op_
     * 5. update_cluster_config_log_op_
     * 6. flush_new_cluster_config_op_
     * 7. install_cluster_config_op_
     * 8. clean_log_op_
     *
     * Basically for add node we're adding new nodes into the cluster first,
     * then migrate data to the new nodes. For remove node we're migrating data
     * from the to be removed nodes, then actually removing them from the
     * cluster.
     * When a new node is just added into the cluster, we can start tx
     * on it but it does not hold any data yet. We need to migrate data
     * ownership to the new ngs separately through data migration.
     * When a node is removed from the cluster, we need to make sure that all
     * data owned by that node group is migrated to other node groups.
     */

    /**
     * Write prepare log for the scale event. This log should contain the new
     * cluster config and data migration plan.
     */
    WriteToLogOp prepare_log_op_;

    /**
     * Upgrade the cluster config lock to write lock as we're now going to
     * update the cluster config.
     */
    AcquireAllOp acquire_cluster_config_write_op_;

    /**
     * Write log for cluster config update. The cluster config update is a 1
     * phase commit.
     */
    WriteToLogOp update_cluster_config_log_op_;

    /**
     * Flush the new cluster config to kv storage. When the new nodes are
     * started, they be starting with the new cluster config.
     */
    AsyncOp<Void> flush_new_cluster_config_op_;

    /**
     * Release the cluster config lock. Install the new cluster config on all
     * ngs. This will establish new cc streams and raft nodes. After this stage,
     * the CP will be able to start new nodes or remove old nodes.
     */
    PostWriteAllOp install_cluster_config_op_;

    /**
     * @brief Notify all node group to start bucekt migration.
     */
    NotifyStartMigrateOp notify_migration_op_;

    /**
     * @brief Check whether bucket migration is finished.
     */
    CheckMigrationIsFinishedOp check_migration_is_finished_op_;

    WriteToLogOp clean_log_op_;

private:
    void FillPrepareLogRequest(TransactionExecution *txm);
    void FillUpdateClusterConfigLogRequest(TransactionExecution *txm);
    void FillCleanLogRequest(TransactionExecution *txm);

    void ForceToFinish(TransactionExecution *txm);

    void ClearContainer();

    ClusterConfigRecord cluster_config_rec_;

    ClusterScaleOpType event_type_;
    std::unordered_map<NodeGroupId, std::vector<NodeConfig>> new_ng_config_;
    // mutex protects txn_ and finished_, which are used when control plane
    // queries for current cluster scale event status for a specific txn.
    std::mutex mux_;
    TxNumber txn_;
    remote::ClusterScaleStatus status_;
};

struct DataMigrationOp : public CompositeTransactionOperation
{
public:
    DataMigrationOp() = delete;

    DataMigrationOp(TransactionExecution *txm,
                    std::shared_ptr<DataMigrationStatus> status);

    void Reset(TransactionExecution *txm,
               std::shared_ptr<DataMigrationStatus> status);

    void Forward(TransactionExecution *txm) override;

    /**
     * @brief Write the first prepare log. This log request will check if the
     * ClusterScaleTx log is exsit. If not, log service will reject this write
     * log request. This migration transaction will be aborted. Since a finished
     * ClusterScaleTx notify request will arrive late due network delay. It will
     * break idempotent. This log request will also insert an empty log for
     * DataMigration Tx. Empty log just used by RecoverTx.
     */
    WriteToLogOp write_first_prepare_log_op_;
    /**
     * @brief Write a log to indicate that this data migrate tx is going to be
     * migrating this bucket. This is to make sure the write lock acquired by
     * prepare_bucket_lock_op_ can be correctly recovered.
     */
    WriteToLogOp write_before_locking_log_op_;
    /**
     * @brief Acquire bucket write lock on all node groups.
     */
    AcquireAllOp prepare_bucket_lock_op_;
    /**
     * @brief Write prepare log for bucket migration
     */
    WriteToLogOp prepare_log_op_;
    /**
     * @brief Install dirty bucket record on CcMap and downgrade write lock to
     * write intent lock.
     */
    PostWriteAllOp install_dirty_bucket_op_;
    /**
     * @brief Flush data in this bucket into data store so that after bucket
     * is migrated the new bucket owner can access latest data from data store.
     */
    AsyncOp<Void> data_sync_op_;
    /**
     * @brief Upgrade write intent lock to write lock on all node group.
     */
    AcquireAllOp acquire_bucket_lock_op_;
    /**
     * @brief Write commit log for bucket migration
     */
    WriteToLogOp commit_log_op_;
    /**
     * @brief Kick out data in this bucket from memory before switching bucket
     * owner.
     */
    KickoutDataOp kickout_data_op_;
    /**
     * @brief Commit dirty bucket record and release bucket write lock
     */
    PostWriteAllOp post_all_bucket_lock_op_;
    /**
     * @brief Write clean log for bucket migration
     */
    WriteToLogOp clean_log_op_;
    /**
     * @brief When all bucket migration are finished, we will write the last
     * clean log to log service. This op will also erase empty log of
     * DataMigration. once this operation is finished, we will set finished flag
     * to true on MigrateStatus.
     * Note: Whether write_last_clean_log_op_ is needed needs more
     * consideration.
     */
    WriteToLogOp write_last_clean_log_op_;

    size_t migrate_bucket_idx_{0};
    RangeBucketKey bucket_key_;
    RangeBucketRecord bucket_record_;
    BucketInfo bucket_info_;

private:
    void FillLogRequest(TransactionExecution *txm,
                        WriteToLogOp *log_op,
                        TxLogType log_type,
                        txlog::BucketMigrateMessage_Stage migrate_stage);

    void FillFirstLogRequest(TransactionExecution *txm,
                             std::vector<uint64_t> &migration_txns);
    void FillLastLogRequest(TransactionExecution *txm);

    void ForceToFinish(TransactionExecution *txm);
    void Clear();

    // the snapshot of the ranges that we need to migrate for current bucket.
    // Note that the table name here is of type range partition.
    std::unordered_map<TableName, std::unordered_set<int32_t>>
        ranges_in_bucket_snapshot_;
    std::unordered_map<TableName, std::unordered_set<int32_t>>::const_iterator
        kickout_tbl_it_;
    std::unordered_set<int32_t>::const_iterator kickout_range_it_;
    TableName kickout_table_{std::string(""), TableType::Primary};

    std::shared_ptr<DataMigrationStatus> status_;
};

struct BatchReadOperation : TransactionOperation
{
public:
    explicit BatchReadOperation(
        TransactionExecution *txm,
        CcHandlerResult<ReadKeyResult> *lock_range_result = nullptr);

    void Reset();
    void Forward(TransactionExecution *txm) override;

    bool IsFinished() const
    {
        return unfinished_cnt_.load(std::memory_order_relaxed) == 0;
    }

    BatchReadTxRequest *batch_read_tx_req_{nullptr};
    std::vector<CcHandlerResult<ReadKeyResult>> hd_result_vec_;
    bool local_cache_checked_;  // If checked local cache for this op
    std::atomic<uint32_t> unfinished_cnt_{0};

#ifdef RANGE_PARTITION_ENABLED
    CcHandlerResult<ReadKeyResult> *lock_range_result_{nullptr};
    std::vector<ScanBatchTuple>::iterator lock_it_;
#endif
};
}  // namespace txservice
