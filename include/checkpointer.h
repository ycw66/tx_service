#pragma once

#include <chrono>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "cc/cc_entry.h"
#include "cc/cc_request.h"
#include "cc/local_cc_shards.h"
#include "txlog.h"
#include "util.h"

using namespace std::chrono;

namespace txservice
{

class Checkpointer
{
public:
    static const size_t CKPT_SCAN_BATCH_SIZE = 3 * 1024;

    Checkpointer(LocalCcShards &shards,
                 store::DataStoreHandler *write_hd,
                 const uint32_t &checkpoint_interval,
                 TxLog *log_agent,
                 uint32_t ckpt_delay_seconds);

    ~Checkpointer();

    void Ckpt(bool is_last_ckpt = false);

    /**
     * @brief Checkpoint one Entry to KvStore synchronously.
     * Now, only used for test.
     */
    bool CkptEntryForTest(LruEntry *entry, std::vector<FlushRecord> &ckpt_vec);
    bool FlushArchiveForTest(LruEntry *entry,
                             std::vector<FlushRecord> &archives);

    void Run();

    /**
     * @brief Called by TxProcessor thread to notify checkpointer thread
     * to do checkpoint if there is no freeable entries to be kicked out
     * from ccmap.
     */
    void Notify();

    bool IsTerminated();

    /**
     * @brief When TxService is stopping, this function will be called and
     * triggers checkpoint to flush data to KvStore.
     *
     */
    void Terminate();

    void Join()
    {
        thd_.join();
    }

    /**
     * @brief Put the passed into data into pending_flush_work_ and notify
     * workers.
     */
    void FlushData(const TableName &table_name,
                   const TableSchema *schema,
                   uint64_t node_group,
                   int64_t term,
                   uint64_t ckpt_ts,
                   std::vector<FlushRecord> *ckpt_vec,
                   std::vector<FlushRecord> *archive_vec,
                   std::vector<const TxKey *> *mv_vec,
                   CcHandlerResult<Void> *res);

private:
    /**
     * @brief Called after checkpoint is done. Update data store slice size
     * in memory and in data store. Reset post ckpt size in store slice.
     */
    bool UpdateStoreSlice(const TableName &tbl_name,
                          const KVCatalogInfo *kv_info,
                          uint64_t schema_ts,
                          NodeGroupId node_group_id,
                          std::vector<FlushRecord> &flush_batch,
                          uint64_t ckpt_ts,
                          bool flush_res);

    /**
     * @brief Called before checkpoint to calculate the storage slice size after
     * checkpoint and decide if the slice needs to be updated(merge/split).
     * Update slice info accordingly, but does not update the actual slice size
     * since the data is not flushed yet.
     * Also decide the range update plan based on the number of slices after the
     * slice update.
     */
    bool UpdateSliceAndCalculateRangeUpdate(
        const TableName &tbl_name,
        const KVCatalogInfo *kv_info,
        uint64_t schema_ts,
        NodeGroupId node_group_id,
        std::vector<FlushRecord> &flush_batch,
        uint64_t last_ckpt_ts,
        uint64_t ckpt_ts,
        size_t &batch_idx,
        std::pair<const StoreRange *, std::vector<const TxKey *>>
            &splitting_info);

    /**
     * @brief Given a vector of checkpoint records and splitting ranges, moves
     * the checkpoint records not in the splitting ranges into a new vector.
     *
     * @param flush_vec A vector of checkpoint records
     * @param non_split_vec The new vector for checkpoint records not falling
     * into splitting ranges
     * @param split_ranges Ranges to be split
     * @param lower_bound_cmp comapre func of type T and const TxKey *
     */
    template <typename T, class Compare>
    void MoveNonSplittingRecords(
        std::vector<T> &flush_vec,
        std::vector<T> &non_split_vec,
        const std::vector<std::pair<const TxKey *, const TxKey *>>
            &split_ranges,
        Compare lower_bound_cmp);

    /**
     * @brief Worker thread that split the target range and flush the data into
     * data store in their new partitions. This is called during checkpoint on a
     * table, after this function returns, we can assume the splitting ranges
     * are flushed too.
     */
    void SplitFlushRange(
        const TableName &table_name,
        NodeGroupId node_group,
        std::pair<const StoreRange *, std::vector<const TxKey *>> split_info);

    enum struct Status
    {
        Active,
        Terminating,
        Terminated
    };

    struct FlushDataWork
    {
    public:
        FlushDataWork(uint32_t node_group,
                      int64_t term,
                      uint64_t ckpt_ts,
                      const TableName &table_name,
                      const TableSchema *schema,
                      std::unique_ptr<std::vector<FlushRecord>> &&ckpt_vec,
                      std::unique_ptr<std::vector<FlushRecord>> &&archive_vec,
                      std::unique_ptr<std::vector<const TxKey *>> &&mv_base_vec,
                      TransactionExecution *txm,
                      std::mutex *sender_mux,
                      std::condition_variable *sender_cv,
                      uint16_t *finish_work_cnt,
                      std::atomic_bool *fail)
            : node_group_(node_group),
              term_(term),
              ckpt_ts_(ckpt_ts),
              table_name_(table_name),
              schema_(schema),
              ckpt_vec_(std::move(ckpt_vec)),
              archive_vec_(std::move(archive_vec)),
              mv_base_vec_(std::move(mv_base_vec)),
              vec_owner_(true),
              txm_(txm),
              sender_mux_(sender_mux),
              sender_cv_(sender_cv),
              finish_work_cnt_(finish_work_cnt),
              fail_(fail),
              hand_res_(nullptr)
        {
        }

        FlushDataWork(uint32_t node_group,
                      int64_t term,
                      uint64_t ckpt_ts,
                      const TableName &table_name,
                      const TableSchema *schema,
                      std::vector<FlushRecord> *ckpt_vec,
                      std::vector<FlushRecord> *archive_vec,
                      std::vector<const TxKey *> *mv_base_vec,
                      CcHandlerResult<Void> *res)
            : node_group_(node_group),
              term_(term),
              ckpt_ts_(ckpt_ts),
              table_name_(table_name),
              schema_(schema),
              ckpt_vec_ptr_(ckpt_vec),
              archive_vec_ptr_(archive_vec),
              mv_base_vec_ptr_(mv_base_vec),
              vec_owner_(false),
              txm_(nullptr),
              sender_mux_(nullptr),
              sender_cv_(nullptr),
              finish_work_cnt_(nullptr),
              fail_(nullptr),
              hand_res_(res)
        {
        }

        uint32_t node_group_;
        int64_t term_;
        uint64_t ckpt_ts_;
        TableName table_name_;
        const TableSchema *schema_;
        std::unique_ptr<std::vector<FlushRecord>> ckpt_vec_{nullptr};
        std::unique_ptr<std::vector<FlushRecord>> archive_vec_{nullptr};
        std::unique_ptr<std::vector<const TxKey *>> mv_base_vec_{nullptr};
        std::vector<FlushRecord> *ckpt_vec_ptr_{nullptr};
        std::vector<FlushRecord> *archive_vec_ptr_{nullptr};
        std::vector<const TxKey *> *mv_base_vec_ptr_{nullptr};
        bool vec_owner_{true};

        // Worker is now the owner of ckpt txm and should commit it
        // once the data flush is compelted.
        TransactionExecution *txm_{nullptr};
        // Increased by worker after finishing the retrieved work.
        std::mutex *sender_mux_{nullptr};
        std::condition_variable *sender_cv_{nullptr};
        uint16_t *finish_work_cnt_{nullptr};
        // Set by worker to indicate flush data result
        std::atomic_bool *fail_{nullptr};
        CcHandlerResult<Void> *hand_res_{nullptr};
    };

    struct UpdateSliceSpecWork
    {
    public:
        UpdateSliceSpecWork(uint32_t node_group,
                            uint64_t ckpt_ts,
                            const TableName &table_name,
                            const std::vector<FlushRecord> &flush_vec,
                            StoreRange *range,
                            StoreSlice *slice,
                            size_t start_idx,
                            size_t end_idx,
                            std::mutex &sender_mux,
                            std::condition_variable &sender_cv,
                            size_t &finish_work_cnt,
                            bool &fail)
            : node_group_(node_group),
              ckpt_ts_(ckpt_ts),
              table_name_(table_name),
              flush_vec_(flush_vec),
              range_(range),
              slice_(slice),
              start_idx_(start_idx),
              end_idx_(end_idx),
              sender_mux_(sender_mux),
              sender_cv_(sender_cv),
              finish_work_cnt_(finish_work_cnt),
              fail_(fail)
        {
        }

        uint32_t node_group_;
        uint64_t ckpt_ts_;
        TableName table_name_;
        const std::vector<FlushRecord> &flush_vec_;
        StoreRange *range_;
        StoreSlice *slice_;
        size_t start_idx_;
        size_t end_idx_;

        std::mutex &sender_mux_;
        std::condition_variable &sender_cv_;
        // Increased by worker after finishing the retrieved work.
        size_t &finish_work_cnt_;
        // Set by worker to indicate work result
        bool &fail_;
    };

    LocalCcShards &local_shards_;
    // protects request_ckpt_ and status_
    std::mutex ckpt_mux_;
    std::condition_variable ckpt_cv_;
    bool request_ckpt_;
    store::DataStoreHandler *store_hd_;
    std::thread thd_;
    Status ckpt_thd_status_;
    const uint32_t checkpoint_interval_;
    // ckpt_ts = {min_being_held_locks_ts} - {ckpt_delay_time_}
    uint32_t ckpt_delay_time_;  // unit: Microsecond
    TxService *tx_service_;
    TxLog *log_agent_;
    // Protects pending_flush_work_ and flush_worker_failed_.
    std::mutex flush_mux_;
    std::condition_variable flush_cv_;
    std::vector<FlushDataWork> pending_flush_work_;
    std::atomic_bool flush_worker_failed_{false};
    std::vector<std::thread> flush_worker_thds_;
    Status worker_thd_status_;

    // Workers for updating slice specs. Since update slice
    // spec would cause potential data store read, we launched
    // workers so we can have some degree of parallelism, but
    // not to the degree where it slows down regular read from data store.
    std::mutex slice_update_mux_;
    std::condition_variable slice_update_cv_;
    std::vector<UpdateSliceSpecWork> pending_slice_work_;
    std::vector<std::thread> update_slice_spec_thds_;
    Status slice_thd_status_;
    static const int checkpointer_worker_num_ = 5;

    void NotifyLogOfCkptTs(uint32_t node_group, int64_t term, uint64_t ckpt_ts);

    /**
     * @brief Flush statistics to storage and broadcast statistics to other
     * nodes.
     */
    static void SyncStatistics(Checkpointer *ckptr,
                               const TableName &table_name,
                               uint32_t table_shard_code,
                               const TableSchema *table_schema,
                               uint64_t table_schema_ts);
    void FlushDataWorker();
    void UpdateSliceSpecWorker();
};
}  // namespace txservice
