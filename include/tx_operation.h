#pragma once

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "catalog_key_record.h"
#include "cc_handler.h"
#include "log_closure.h"
#include "range_record.h"
#include "read_write_set.h"
#include "tx_key.h"
#include "tx_operation_result.h"
#include "tx_record.h"
#include "tx_req_result.h"

namespace txservice
{
class TransactionExecution;
struct ReadTxRequest;
struct ReadOutsideTxRequest;
struct ScanOpenTxRequest;
struct ScanBatchTxRequest;
struct ScanBatchTuple;

#define RETRY_NUM 5

enum class TxLogType
{
    DATA,
    PREPARE,
    COMMIT,
    CLEAN
};

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
};

struct CompositeTransactionOperation : TransactionOperation
{
    CompositeTransactionOperation();

    virtual ~CompositeTransactionOperation() = default;

    template <typename Op>
    void ForwardToSubOperation(TransactionExecution *txm, Op *next_op);

    template <typename Op>
    void RetrySubOperation(TransactionExecution *txm, Op *next_op);

    bool CheckLeaderTerm(uint32_t ng_id,
                         int64_t term,
                         TxnStatus txn_status) const;
    /**
     * @brief The current stage of this multi-stage schema operation
     */
    TransactionOperation *op_{nullptr};
};

struct ReadOperation : TransactionOperation
{
public:
    ReadOperation(TransactionExecution *txm);

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
    TableName range_table_name_{empty_sv, TableType::RangePartition};
    RangeRecord range_rec_;
    CcHandlerResult<ReadKeyResult> lock_range_result_;
    CcHandlerResult<PostProcessResult> unlock_range_result_;
#endif
};

struct PostReadOperation : TransactionOperation
{
public:
    explicit PostReadOperation(TransactionExecution *txm);

    void Reset(std::pair<CcEntryAddr *, ReadSetEntry *> cce_entry);

    void Forward(TransactionExecution *txm) override;

    std::pair<CcEntryAddr *, ReadSetEntry *> cce_entry_;
    CcHandlerResult<PostProcessResult> hd_result_;
};

struct SetCommitTsOperation : TransactionOperation
{
public:
    SetCommitTsOperation(TransactionExecution *txm);
    void Reset();
    void Forward(TransactionExecution *txm) override;

    CcHandlerResult<uint64_t> hd_result_;
};

struct ValidateOperation : TransactionOperation
{
public:
    static const uint32_t default_read_set_capacity = 16;

    ValidateOperation(TransactionExecution *txm);
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
    AcquireWriteOperation(TransactionExecution *txm);
    void Reset(size_t acquire_write_cnt);
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

struct LockWriteRangesOp : public TransactionOperation
{
public:
    LockWriteRangesOp(TransactionExecution *txm) : lock_range_result_(txm)
    {
    }

    void Forward(TransactionExecution *txm) override;

    void Reset()
    {
        init_ = false;
        is_running_ = false;
        lock_range_result_.Reset();
    }

    /**
     * @brief Advances the internal iterator to the next range to acquire a
     * write lock.
     *
     */
    void Advance();

    TableName range_table_name_{empty_sv, TableType::RangePartition};
    RangeRecord range_rec_;
    CcHandlerResult<ReadKeyResult> lock_range_result_;

    std::unordered_map<TableName, TableWriteSet>::iterator table_it_;
    std::unordered_map<TableName, TableWriteSet>::iterator table_end_;
    TableWriteSet::iterator write_key_it_;
    TableWriteSet::iterator write_key_end_;
    bool init_;
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
    void Reset(size_t write_cnt, size_t data_read_cnt, size_t catalog_read_cnt);
    void Forward(TransactionExecution *txm) override;

    CcHandlerResult<PostProcessResult> hd_result_;
    CcHandlerResult<PostProcessResult> catalog_hd_result_;
};

struct InitTxnOperation : TransactionOperation
{
    InitTxnOperation(TransactionExecution *txm);
    void Reset();
    void Forward(TransactionExecution *txm) override;

    CcHandlerResult<InitTxResult> hd_result_;
};

struct ScanOpenOperation : TransactionOperation
{
    ScanOpenOperation(TransactionExecution *txm);
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
    ScanState(std::unique_ptr<CcScanner> scanner) : scanner_(std::move(scanner))
    {
    }

    std::unique_ptr<CcScanner> scanner_;

#ifdef RANGE_PARTITION_ENABLED
    ScanState(std::unique_ptr<CcScanner> scanner,
              uint32_t range_id,
              const TxKey *last_key,
              bool inclusive,
              SlicePosition position)
        : scanner_(std::move(scanner)),
          range_id_(range_id),
          slice_last_key_ptr_(last_key),
          is_key_owner_(false),
          inclusive_(inclusive),
          slice_position_(position)
    {
    }

    ScanState(std::unique_ptr<CcScanner> scanner,
              uint32_t range_id,
              std::unique_ptr<TxKey> last_key,
              bool inclusive,
              SlicePosition position)
        : scanner_(std::move(scanner)),
          range_id_(range_id),
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
    ScanNextOperation(TransactionExecution *txm);
    void Forward(TransactionExecution *txm) override;
    void Reset();

    ScanDirection Direction() const
    {
        return scan_state_->scanner_ != nullptr
                   ? scan_state_->scanner_->Direction()
                   : ScanDirection::Forward;
    }

    ScanState *scan_state_;
    CcHandlerResult<ScanNextResult> hd_result_;

#ifdef RANGE_PARTITION_ENABLED
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

struct SchemaOp : public TransactionOperation
{
    SchemaOp() = delete;
    SchemaOp(const std::string_view table_name_sv,
             const std::string &current_image,
             const std::string &dirty_image,
             uint64_t schema_ts,
             const std::string *alter_table_info_image);

    CatalogKey table_key_;  // string owner
    CatalogRecord catalog_rec_;
    std::string image_str_{""};
    std::string dirty_image_str_{""};
    uint64_t curr_schema_ts_;
    std::string alter_table_info_image_str_{""};
};

struct UpsertTableOp : public SchemaOp
{
    UpsertTableOp() = delete;
    UpsertTableOp(const std::string_view table_name_str,
                  const std::string &current_image,
                  uint64_t curr_schema_ts,
                  const std::string &dirty_image,
                  OperationType op_type,
                  TransactionExecution *txm,
                  const std::string *alter_table_info_image);

    void Reset(const std::string_view table_name_str,
               const std::string &current_image,
               uint64_t curr_schema_ts,
               const std::string &dirty_image,
               OperationType op_type,
               TransactionExecution *txm,
               const std::string *alter_table_info_image);

    void Forward(TransactionExecution *txm) override;

    OperationType op_type_{OperationType::Insert};
    /**
     * @brief The current stage of this multi-stage schema operation.
     *
     */
    TransactionOperation *op_{nullptr};
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
     * @brief Creates/deletes the data store table and persists/removes the
     * binary representation of the catalog in the data store.
     *
     */
    DsUpsertTableOp upsert_kv_table_op_;
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

    txservice::AlterTableInfo alter_table_info_;

private:
    void FillPrepareLogRequest(TransactionExecution *txm);
    void FillCommitLogRequest(TransactionExecution *txm);
    void FillCleanLogRequest(TransactionExecution *txm);
    void ForceToFinish(TransactionExecution *txm);
};

struct SleepOperation : TransactionOperation
{
public:
    SleepOperation(TransactionExecution *txm);

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

template <typename ResultType>
struct DsOp : public TransactionOperation
{
    DsOp() = delete;
    DsOp(TransactionExecution *txm);

    void Forward(TransactionExecution *txm) override;
    void Reset();

    std::function<void()> op_func_;
    CcHandlerResult<ResultType> hd_result_;
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
    void Forward(TransactionExecution *txm) override;
    void Reset();

    const TableName *tab_name_{nullptr};
    uint64_t ckpt_ts_;
    NodeGroupId node_group_;
    int64_t tx_term_;
    const TableSchema *schema_{nullptr};
    std::vector<FlushRecord> *ckpt_vec_{nullptr};
    std::vector<FlushRecord> *archive_vec_{nullptr};
    std::vector<const TxKey *> *mv_vec_{nullptr};
    CcHandlerResult<Void> hd_result_;
};

struct CkptScanOp : public TransactionOperation
{
    CkptScanOp(TransactionExecution *txm);
    CkptScanOp(const TableName &table_name,
               uint64_t ckpt_ts,
               NodeGroupId node_group,
               std::vector<FlushRecord> *ckpt_vec,
               std::vector<FlushRecord> *archive_vec,
               std::vector<const TxKey *> *mv_vec,
               TransactionExecution *txm,
               const TxKey *start_key = nullptr,
               const TxKey *end_key = nullptr);
    void Forward(TransactionExecution *txm) override;
    void Reset();

    const TableName *tab_name_{nullptr};
    uint64_t ckpt_ts_;
    NodeGroupId node_group_;
    std::vector<FlushRecord> *ckpt_vec_{nullptr};
    std::vector<FlushRecord> *archive_vec_{nullptr};
    std::vector<const TxKey *> *mv_vec_{nullptr};
    // Start/end key of the target range of the ckpt scan.
    // nullptr if target is entire table.
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
        NodeGroupId node_group,
        const TxKey *old_start_key,
        const TxKey *old_end_key,
        const RangeInfo *old_range_info,
        std::vector<std::pair<TxKey::Uptr, int32_t>> &&new_range_info,
        TransactionExecution *txm);

    void Forward(TransactionExecution *txm) override;

    const TableSchema *table_schema_{nullptr};
    const TableName table_name_;
    const TableName range_table_name_;
    NodeGroupId node_group_;

    RangeInfo range_info_;
    RangeRecord range_record_;
    // TODO{liunyl}: change these to Uptr after we update inf key instance.
    // Now we need to use raw pointers to accomadate with inf key instance.
    // Now we make them point to the Uptr in range_info_ if they are normal key,
    // or raw pointers to inf key instances otherwise.
    const TxKey *old_start_key_;
    const TxKey *old_end_key_;
    // vector< new start key, new partition id >
    std::vector<std::pair<TxKey::Uptr, int32_t>> new_range_info_;

    // vector buffer used during checkpoint scan
    std::vector<FlushRecord> ckpt_vec_;
    std::vector<FlushRecord> archive_vec_;
    std::vector<const TxKey *> mv_base_vec_;

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
     * @brief Copy data from old partition to new partition in KV store.
     * Flush in-memory data that has smaller ts than commit ts to new KV
     * store.
     */
    DsOp<Void> ds_migrate_old_partition_op_;
    /**
     * @brief Scan for data before commit_ts in the splitting range. We need to
     * make these data available to the new range before we commit the range
     * split.
     */
    CkptScanOp ckpt_scan_op_;
    /**
     * @brief Flush data in memory before commit_ts to KV storage. These data
     * will be flushed into both old and new partitions.
     */
    FlushDataOp flush_op_;
    /**
     * @brief Acquire write lock on all node group on the old partition and
     * new partition.
     */
    AcquireAllOp commit_acquire_all_write_op_;
    /**
     * @brief Write commit log.
     */
    WriteToLogOp commit_log_op_;
    /**
     * @brief Upsert new ranges into range table in KV store.
     */
    DsOp<Void> ds_upsert_range_op_;
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
    DsOp<Void> ds_clean_old_range_op_;
    /**
     * @brief Remove split-flush log.
     */
    WriteToLogOp clean_log_op_;

private:
    void FillPrepareLogRequest(TransactionExecution *txm);
    void FillCommitLogRequest(TransactionExecution *txm);
    void FillCleanLogRequest(TransactionExecution *txm);
    void ForceToFinish(TransactionExecution *txm);
};

struct DsSplitRangeOp : public CompositeTransactionOperation
{
    DsSplitRangeOp() = delete;

    DsSplitRangeOp(const TableName &table_name,
                   const TableSchema *table_schema,
                   const TxKey *range_key,
                   std::unique_ptr<RangeRecord> splitting_range_record,
                   TransactionExecution *txm,
                   std::optional<std::pair<CcEntryAddr, ReadSetEntry>>
                       catalog_cc_entry = std::nullopt);

    void FillTxLog(TransactionExecution *txm,
                   WriteToLogOp &log_op,
                   TxLogType log_type,
                   ::txlog::SplitRangeOpMessage::Stage stage);
    void FillTxLogForCleanLog(TransactionExecution *txm);
    void ForceToFinish(TransactionExecution *txm);
    void Forward(TransactionExecution *txm) override;
    void PrepareUploadRangeRecord();

    TableName table_name_{empty_sv, TableType::Primary};
    TableName range_table_name_{empty_sv, TableType::RangePartition};

    const TableSchema *table_schema_{nullptr};
    int32_t partition_id_{-1};
    const TxKey *range_key_{nullptr};
    std::unique_ptr<RangeRecord> old_range_record_{nullptr};
    std::unique_ptr<TxKey> new_range_key_{nullptr};
    std::unique_ptr<RangeInfo> upload_range_entry_{nullptr};
    std::unique_ptr<RangeRecord> upload_range_record_{nullptr};
    int32_t new_partition_id_{-1};
    // Store the catalog read lock information, only effect for recovering
    std::optional<std::pair<CcEntryAddr, ReadSetEntry>> catalog_cc_entry_{
        std::nullopt};

    /**
     * @brief Acquire write intents on the range to split at all shards. This is
     * to prevent concurrent modifications on the same range.
     */
    AcquireAllOp acquire_all_intent_for_update_old_range_op_;
    /**
     * @brief Find the median key value of the old range
     */
    DsOp<RangeMedianKeyResult> ds_find_median_key_for_old_range_op_;
    /**
     * @brief Upgrades the write intents to write locks. This is to wait for
     * existing queries reading or writing the range to finish and to block new
     * reads and writes on the range to start.
     */
    AcquireAllOp acquire_all_lock_for_update_old_range_op_;
    /**
     * @brief Write log to mark the split range is started, with the information
     * of the old range and new range information, after this it is guaranted to
     * succeed after this
     */
    WriteToLogOp prepare_log_for_update_old_range_op_;
    /**
     * @brief
     * 1. Upload the new key, ne_partition_id to the
     * old range entry, it is visible to all nodes
     * 2. Downgrade lock to write intent
     */
    PostWriteAllOp post_all_lock_for_update_old_range_op_;
    /**
     * @brief
     * 1.Start a new thread to do work of copy data from the old range to the
     * new ranges 2.The working thread updates the running status to the
     * ds_copy_old_range_data_op_
     */
    DsOp<Void> ds_copy_old_range_data_op_;
    /**
     * @brief Write log to mark the range split is finished
     */
    WriteToLogOp ds_copy_old_range_data_finished_log_op_;
    /**
     * @brief
     * 1. Upgrade the write intent on old range entry to write lock on all nodes
     * 2. Add write lock on new range entries to write locks on all nodes
     */
    AcquireAllOp acquire_all_lock_for_dirty_old_range_op_;
    /**
     * @brief Write commit log to mark the split range transaction is succeed
     */
    WriteToLogOp commit_log_for_dirty_old_range_op_;
    /**
     * @brief
     * 1. Update the dirty old range, clean new key and new partition id
     * 2. Upload the new range
     * 3. Remove all write locks on all nodes
     */
    PostWriteAllOp post_write_all_for_dirty_old_range_op_;
    /**
     * @brief Flush all updated range entries into KV storage
     */
    DsOp<Void> ds_upsert_new_range_op_;
    /**
     * @brief Write log to mark removing out of data from old range
     */
    // WriteToLogOp delete_out_of_old_range_data_log_op_;
    WriteToLogOp delete_out_of_old_range_data_log_op_;
    /**
     * @brief Delete out of range data from the Cassandra partition
     */
    DsOp<Void> delete_out_of_old_range_data_op_;
    /**
     * @brief Remove split range log from the log state machine
     */
    // WriteToLogOp clean_log_op_;
    WriteToLogOp clean_log_op_;
    /**
     * @brief Post read if acquired catalog read lock in recovery scenario
     */
    PostReadOperation catalog_post_read_op_;
};

// To remove remainder records' lock when scan close
struct ReleaseScanExtraLockOp : TransactionOperation
{
    explicit ReleaseScanExtraLockOp(TransactionExecution *txm);
    void Reset(std::vector<ScanBatchTuple> *scan_batch,
               size_t scan_batch_idx,
               const TableName *table_name,
               CcScanner *scanner,
               TxResult<size_t> *scan_open_tx_result,
               TxResult<Void> *scan_close_tx_result);
    void Forward(TransactionExecution *txm) override;

    CcHandlerResult<PostProcessResult> hd_result_;
    TxResult<size_t> *scan_open_tx_result_;
    TxResult<Void> *scan_close_tx_result_;
    std::vector<ScanBatchTuple> *scan_batch_;
    size_t scan_batch_idx_;
    const TableName *table_name_;
    CcScanner *scanner_;
};

}  // namespace txservice
