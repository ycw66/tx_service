#include "tx_index_operation.h"

#include <brpc/channel.h>

#include <algorithm>
#include <memory>

#include "error_messages.h"
#include "local_cc_shards.h"
#include "log_type.h"
#include "remote/remote_type.h"
#include "sk_generator.h"
#include "tx_execution.h"
#include "tx_request.h"
#include "tx_service.h"
#include "tx_trace.h"

namespace txservice
{
KickoutDataAllOp::KickoutDataAllOp(TransactionExecution *txm) : hd_result_(txm)
{
}

void KickoutDataAllOp::Reset(uint32_t ng_cnt, size_t table_cnt)
{
    hd_result_.Reset();
    hd_result_.SetRefCnt(ng_cnt * table_cnt);
}

void KickoutDataAllOp::Clear()
{
    table_names_.clear();
}

void KickoutDataAllOp::ResetHandlerTxm(TransactionExecution *txm)
{
    hd_result_.ResetTxm(txm);
}

void KickoutDataAllOp::Forward(TransactionExecution *txm)
{
    // Start the state machine if not running.
    if (!is_running_)
    {
        txm->Process(*this);
    }

    if (hd_result_.IsFinished())
    {
        // If leader-transferred, do not need to kickout data any more.
        if (hd_result_.IsError() &&
            hd_result_.ErrorCode() != CcErrorCode::REQUESTED_NODE_NOT_LEADER &&
            hd_result_.ErrorCode() != CcErrorCode::TX_NODE_NOT_LEADER)
        {
            if (retry_num_ > 0)
            {
                ReRunOp(txm);
                return;
            }
        }

        txm->PostProcess(*this);
    }
    else if (txm->IsTimeOut(30) && hd_result_.SetResultByTimeoutThread())
    {
        LOG(WARNING) << "Kickout data all operation timeout 30s";
        if (txm->CheckLeaderTerm() && retry_num_ > 0)
        {
            LOG(WARNING) << "ReRun this operation with the retry number: "
                         << retry_num_;
            ReRunOp(txm);
            return;
        }
        else
        {
            bool force_error = hd_result_.ForceError();
            if (force_error)
            {
                LOG(WARNING) << "Force error for this operation";
                txm->PostProcess(*this);
            }
        }
    }
}

UpsertTableIndexOp::UpsertTableIndexOp(
    const std::string_view table_name_sv,
    const std::string &current_image,
    uint64_t curr_schema_ts,
    const std::string &dirty_image,
    const std::string &alter_table_info_image,
    OperationType op_type,
    TransactionExecution *txm)
    : SchemaOp(
          table_name_sv, current_image, dirty_image, curr_schema_ts, op_type),
      lock_cluster_config_op_(),
      acquire_all_intent_op_(txm),
      upgrade_all_intent_to_lock_op_(txm),
      prepare_log_op_(txm),
      downgrade_all_lock_to_intent_op_(txm),
      unlock_cluster_config_op_(txm),
      upsert_kv_table_op_(&table_key_.Name(), op_type, txm),
#ifndef RANGE_PARTITION_ENABLED
      flush_all_old_tuples_pk_op_(txm),
      fetch_old_tuples_from_kv_gen_sk_data_upload_op_(txm),
      flush_all_old_tuples_sk_op_(txm),
      kickout_data_all_op_(txm),
      prepare_log_for_sk_op_(txm),
#else
      generate_sk_parallel_op_(txm),
      flush_all_old_tuples_sk_op_(txm),
      prepare_log_for_sk_op_(txm),
#endif
      acquire_all_lock_op_(txm),
      commit_log_op_(txm),
      post_all_lock_op_(txm),
      clean_log_op_(txm),
      read_cluster_result_(txm),
      alter_table_info_image_str_(alter_table_info_image),
      post_write_result_(txm)
{
    assert(op_type_ == OperationType::AddIndex ||
           op_type_ == OperationType::DropIndex);

    lock_cluster_config_op_.table_name_ =
        TableName(cluster_config_ccm_name_sv, TableType::ClusterConfig);
    lock_cluster_config_op_.key_ = NegativeInfinity<VoidKey>::Instance();
    lock_cluster_config_op_.rec_ = &cluster_conf_rec_;
    lock_cluster_config_op_.hd_result_ = &read_cluster_result_;

    acquire_all_intent_op_.table_name_ = &catalog_ccm_name;
    acquire_all_intent_op_.key_ = &table_key_;
    acquire_all_intent_op_.cc_op_ = CcOperation::ReadForWrite;
    acquire_all_intent_op_.protocol_ = CcProtocol::OCC;

    upgrade_all_intent_to_lock_op_.table_name_ = &catalog_ccm_name;
    upgrade_all_intent_to_lock_op_.key_ = &table_key_;
    upgrade_all_intent_to_lock_op_.cc_op_ = CcOperation::Write;
    upgrade_all_intent_to_lock_op_.protocol_ = CcProtocol::Locking;

    downgrade_all_lock_to_intent_op_.table_name_ = &catalog_ccm_name;
    downgrade_all_lock_to_intent_op_.key_ = &table_key_;
    downgrade_all_lock_to_intent_op_.rec_ = &catalog_rec_;
    downgrade_all_lock_to_intent_op_.op_type_ = op_type_;
    downgrade_all_lock_to_intent_op_.write_type_ = PostWriteType::PrepareCommit;

    acquire_all_lock_op_.table_name_ = &catalog_ccm_name;
    acquire_all_lock_op_.key_ = &table_key_;
    acquire_all_lock_op_.cc_op_ = CcOperation::Write;
    acquire_all_lock_op_.protocol_ = CcProtocol::Locking;

    post_all_lock_op_.table_name_ = &catalog_ccm_name;
    post_all_lock_op_.key_ = &table_key_;
    post_all_lock_op_.rec_ = &catalog_rec_;
    post_all_lock_op_.op_type_ = op_type_;
    post_all_lock_op_.write_type_ = PostWriteType::PostCommit;

    alter_table_info_.DeserializeAlteredTableInfo(alter_table_info_image_str_);

    is_force_finished_ = false;

    new_indexes_name_.reserve(alter_table_info_.index_add_count_);
    for (auto index_it = alter_table_info_.index_add_names_.cbegin();
         index_it != alter_table_info_.index_add_names_.cend();
         ++index_it)
    {
        new_indexes_name_.emplace_back(index_it->first.StringView(),
                                       index_it->first.Type());
    }

    const TxKey *neg_key = Sharder::Instance()
                               .GetLocalCcShards()
                               ->GetCatalogFactory()
                               ->NegativeInfKey();
    last_scanned_end_key_ = neg_key;
    is_last_scanned_key_str_ = false;
    scanned_pk_range_count_ = 0;
    last_finished_end_key_ = neg_key;
    is_last_finished_key_str_ = false;
    finished_pk_range_count_ = 0;
    total_scanned_pk_items_count_ = 0;
#ifdef NDEBUG
    scan_batch_range_size_ = 10;
#else
    scan_batch_range_size_ = 3;
#endif
    ResetLeaderTerms();

    TX_TRACE_ASSOCIATE(this, &acquire_all_intent_op_, "acquire_all_intent_op_");
    TX_TRACE_ASSOCIATE(this,
                       &upgrade_all_intent_to_lock_op_,
                       "upgrade_all_intent_to_lock_op_");
    TX_TRACE_ASSOCIATE(this, &prepare_log_op_, "prepare_log_op_");
    TX_TRACE_ASSOCIATE(this,
                       &downgrade_all_lock_to_intent_op_,
                       "downgrade_all_lock_to_intent_op_");
    TX_TRACE_ASSOCIATE(this, &upsert_kv_table_op_, "upsert_kv_table_op_");
#ifndef RANGE_PARTITION_ENABLED
    TX_TRACE_ASSOCIATE(
        this, &flush_all_old_tuples_pk_op_, "flush_all_old_tuples_pk_op_");
    TX_TRACE_ASSOCIATE(this,
                       &fetch_old_tuples_from_kv_gen_sk_data_upload_op_,
                       "fetch_old_tuples_from_kv_gen_sk_data_upload_op_");
    TX_TRACE_ASSOCIATE(
        this, &flush_all_old_tuples_sk_op_, "flush_all_old_tuples_sk_op_");
    TX_TRACE_ASSOCIATE(this, &kickout_data_all_op_, "kickout_data_all_op_");
    TX_TRACE_ASSOCIATE(this, &prepare_log_for_sk_op_, "prepare_log_for_sk_op_");
#else
    TX_TRACE_ASSOCIATE(
        this, &generate_sk_parallel_op_, "generate_sk_parallel_op_");
    TX_TRACE_ASSOCIATE(
        this, &flush_all_old_tuples_sk_op_, "flush_all_old_tuples_sk_op_");
    TX_TRACE_ASSOCIATE(this, &prepare_log_for_sk_op_, "prepare_log_for_sk_op_");
#endif
    TX_TRACE_ASSOCIATE(this, &acquire_all_lock_op_, "acquire_all_lock_op_");
    TX_TRACE_ASSOCIATE(this, &commit_log_op_, "commit_log_op_");
    TX_TRACE_ASSOCIATE(this, &post_all_lock_op_, "post_all_lock_op_");
    TX_TRACE_ASSOCIATE(this, &clean_log_op_, "clean_log_op_");
}

void UpsertTableIndexOp::Forward(TransactionExecution *txm)
{
    if (op_ == nullptr)
    {
        LOG(INFO) << "Alter Table Index transaction lock cluster config"
                  << " , txn: " << txm->TxNumber();
        op_ = &lock_cluster_config_op_;
        txm->PushOperation(&lock_cluster_config_op_);
        txm->Process(lock_cluster_config_op_);
    }
    else if (op_ == &lock_cluster_config_op_)
    {
        if (lock_cluster_config_op_.hd_result_->IsError())
        {
            LOG(ERROR) << "Alter Table Index read cluster config failed, txn:"
                       << txm->TxNumber();
            if (!prepare_log_op_.hd_result_.IsFinished() &&
                !prepare_log_for_sk_op_.hd_result_.IsFinished())
            {
                txm->commit_ts_ = tx_op_failed_ts_;
            }
            ForceToFinish(txm);
            return;
        }
        if (prepare_log_op_.hd_result_.IsFinished() ||
            prepare_log_for_sk_op_.hd_result_.IsFinished())
        {
            assert(op_type_ == OperationType::AddIndex);
            LOG(INFO) << "Alter Table Index transaction post acquire all"
                      << " write lock, txn: " << txm->TxNumber();
            op_ = &acquire_all_lock_op_;
            txm->PushOperation(&acquire_all_lock_op_);
            txm->Process(acquire_all_lock_op_);
        }
        else
        {
            // Acquire write intent, and then upgrade to write lock(Locking),
            // rather than acquire write lock(OCC) directly, aim to avoid two
            // situations: (1) concurrent DDL deadlock. (2) concurrent DML cause
            // to always abort this tx.
            LOG(INFO)
                << "Alter Table Index transaction prepare acquire all write"
                << " intent, txn: " << txm->TxNumber();
            op_ = &acquire_all_intent_op_;
            txm->PushOperation(&acquire_all_intent_op_);
            txm->Process(acquire_all_intent_op_);
        }
    }
    else if (op_ == &acquire_all_intent_op_)
    {
        if (acquire_all_intent_op_.fail_cnt_.load(std::memory_order_relaxed) >
            0)
        {
            LOG(ERROR) << "Alter Table Index for table: "
                       << table_key_.Name().Trace()
                       << ", acquire write intent failed, txn: "
                       << txm->TxNumber();
            txm->upsert_resp_->SetErrorCode(
                TxErrorCode::UPSERT_TABLE_ACQUIRE_WRITE_INTENT_FAIL);
            // Fails to acquire the write intent on the schema. Since write
            // intents only conflict with other writes, there must be
            // another tx trying to modify the same table's schema. Stops
            // the schema operation. Set the commit ts to 0 to signal that
            // the following post write operation releases all write
            // intents.
            txm->commit_ts_ = tx_op_failed_ts_;
            // Moves to the last operation that removes all write
            // intents/locks.
            op_ = &post_all_lock_op_;
            txm->PushOperation(&post_all_lock_op_);
            txm->Process(post_all_lock_op_);
            return;
        }

        // To avoid deadlock. If hold write lock directly, rather than get
        // write intent, and upgrade to write lock, concurrent DDL on the
        // same table may cause to deadlock.
        LOG(INFO) << "Alter Table Index transaction prepare acquire all write"
                  << " lock, txn: " << txm->TxNumber();
        op_ = &upgrade_all_intent_to_lock_op_;
        txm->PushOperation(&upgrade_all_intent_to_lock_op_);
        txm->Process(upgrade_all_intent_to_lock_op_);
    }
    else if (op_ == &upgrade_all_intent_to_lock_op_)
    {
        if (upgrade_all_intent_to_lock_op_.fail_cnt_.load(
                std::memory_order_relaxed) > 0)
        {
            LOG(ERROR) << "Alter Table Index for table: "
                       << table_key_.Name().Trace()
                       << ", upgrade write lock failed, txn: "
                       << txm->TxNumber();
            // Set the commit ts to 0 to signal that the following post write
            // operation releases all write intents.
            txm->commit_ts_ = tx_op_failed_ts_;
            // Moves to the last operation that removes all write
            // intents/locks.
            op_ = &post_all_lock_op_;
            txm->PushOperation(&post_all_lock_op_);
            txm->Process(post_all_lock_op_);
            return;
        }

        // Assigns a commit timestamp to the txm as the version of the new
        // schema. Rules to calculate the commit ts: the new schema's version
        // should be greater than (1) the current version, (2) the maximal
        // commit ts of all tx that have read the schema, (3) the local time
        // when the tx starts.
        txm->commit_ts_ = txm->commit_ts_bound_ + 1;

        for (size_t idx = 0; idx < upgrade_all_intent_to_lock_op_.upload_cnt_;
             ++idx)
        {
            const AcquireAllResult &upgrade_all_res =
                upgrade_all_intent_to_lock_op_.hd_results_[idx].Value();
            uint64_t ts = std::max(upgrade_all_res.commit_ts_ + 1,
                                   upgrade_all_res.last_vali_ts_ + 1);
            txm->commit_ts_ = std::max(txm->commit_ts_, ts);
        }

        LOG(INFO) << "Alter Table Index transaction write prepare log, txn: "
                  << txm->TxNumber()
                  << ". The schema version: " << txm->commit_ts_;
        op_ = &prepare_log_op_;
        FillPrepareLogRequest(txm);
        txm->PushOperation(&prepare_log_op_);
        txm->Process(prepare_log_op_);
    }
    else if (op_ == &prepare_log_op_)
    {
        if (prepare_log_op_.hd_result_.IsError())
        {
            if (prepare_log_op_.hd_result_.ErrorCode() ==
                CcErrorCode::LOG_CLOSURE_RESULT_UNKNOWN_ERR)
            {
                // prepare log result unknown, keep retrying until getting a
                // clear response, either success or failure, or the coordinator
                // itself is no longer leader
                if (txm->CheckLeaderTerm())
                {
                    LOG(WARNING) << "Alter Table Index for table: "
                                 << table_key_.Name().Trace()
                                 << ", write prepare log result unknown, txn: "
                                 << txm->TxNumber() << ", keep retrying";
                    // set retry flag and retry prepare log
                    ::txlog::WriteLogRequest *log_req =
                        prepare_log_op_.log_closure_.LogRequest()
                            .mutable_write_log_request();
                    log_req->set_retry(true);
                    txm->PushOperation(&prepare_log_op_);
                    txm->Process(prepare_log_op_);
                }
                else
                {
                    txm->commit_ts_ = tx_op_failed_ts_;
                    ForceToFinish(txm);
                }
            }
            else
            {
                LOG(ERROR) << "Alter Table Index for table: "
                           << table_key_.Name().Trace()
                           << ", write prepare log failed with error message: "
                           << prepare_log_op_.hd_result_.ErrorMsg()
                           << ", txn: " << txm->TxNumber();
                // Fails to flush the prepare log. The schema operation is
                // considered failed if the prepare log is not flushed. The
                // commit ts is set to 0 to signal that the following post write
                // operation releases all write intents.
                txm->commit_ts_ = tx_op_failed_ts_;
                // Moves to the last operation that removes all write
                // intents/locks.
                op_ = &post_all_lock_op_;

                txm->upsert_resp_->SetErrorCode(
                    TxErrorCode::UPSERT_TABLE_PREPARE_FAIL);

                txm->PushOperation(&post_all_lock_op_);
                txm->Process(post_all_lock_op_);
            }
        }
        else
        {
            ACTION_FAULT_INJECTOR("term_AlterTableIndex_PrepareCommitAllWLOp");
            LOG(INFO) << "Alter Table Index transaction install dirty table"
                      << " schema, txn: " << txm->TxNumber();
            op_ = &downgrade_all_lock_to_intent_op_;

            txm->PushOperation(&downgrade_all_lock_to_intent_op_);
            txm->Process(downgrade_all_lock_to_intent_op_);
        }
    }
    else if (op_ == &downgrade_all_lock_to_intent_op_)
    {
        if (downgrade_all_lock_to_intent_op_.hd_result_.IsError())
        {
            // After the prepare log is flushed, the schema op is guaranteed
            // to succeed and can only roll forward. Retry this step to
            // install the dirty schema in the tx service, if the tx node is
            // still the leader. The tx is also allowed to proceed if the tx
            // is in the recovery mode and the tx node is a leader
            // candidate.
            if (txm->CheckLeaderTerm())
            {
                // set catalog_rec_'s binary_value_ to image_str since it
                // could be set to TableSchemaView pointer in localshard.
                catalog_rec_.SetSchemaImage(image_str_);
                catalog_rec_.SetDirtySchemaImage(dirty_image_str_);

                txm->PushOperation(&downgrade_all_lock_to_intent_op_);
                txm->Process(downgrade_all_lock_to_intent_op_);
            }
            else
            {
                ForceToFinish(txm);
            }
        }
        // TODO(ysw): For DropIndex, since we already got all WriteLock, so it
        // OK as the order below:
        // WriteLock->Preparelog->InstallDirtySchema(but donot downgrade to
        // WriteIntent)->Commitlog.
        else if (op_type_ == OperationType::DropIndex)
        {
            // For DROP INDEX opertaion, the data store operation of deleting
            // the k-v table happens after the commit log is flushed.
            LOG(INFO) << "Alter Table Index transaction post acquire all"
                      << " write lock, txn: " << txm->TxNumber();
            op_ = &acquire_all_lock_op_;
            txm->PushOperation(&acquire_all_lock_op_);
            txm->Process(acquire_all_lock_op_);
        }
        else
        {
            // Release cluster config op before doing data store op.
            op_ = &unlock_cluster_config_op_;
            // Get cce addr of cluster config read lock from rset.
            auto &rset = txm->rw_set_.ReadSet();
            auto &cluster_config_rset = rset.at(cluster_config_ccm_name);
            assert(cluster_config_rset.size() == 1);
            for (const auto &[cce_addr, rset_entry] : cluster_config_rset)
            {
                unlock_cluster_config_op_.Reset(&cce_addr, &rset_entry);
            }
            LOG(INFO) << "Alter Table Index transaction release cluster config "
                         "lock, txn: "
                      << txm->TxNumber();
            txm->PushOperation(&unlock_cluster_config_op_);
            txm->Process(unlock_cluster_config_op_);
        }
    }
    else if (op_ == &unlock_cluster_config_op_)
    {
        if (unlock_cluster_config_op_.hd_result_.IsError())
        {
            if (txm->CheckLeaderTerm())
            {
                // Releasing a local read lock should never fail
                assert(false);
            }
            else
            {
                ForceToFinish(txm);
            }
        }
        assert(op_type_ == OperationType::AddIndex);
        LOG(INFO) << "Alter Table Index transaction upsert data store"
                  << " info for base table: " << table_key_.Name().Trace()
                  << ", txn: " << txm->TxNumber();
        uint16_t retry_times = 100;
        CODE_FAULT_INJECTOR("trigger_flush_kv_error", { retry_times = 3; });
        op_ = &upsert_kv_table_op_;
        // The post write request right after flushing the prepare log
        // installs the dirty schema in the tx service and returns a
        // local view (pointer) of the committed and dirty schema.
        upsert_kv_table_op_.table_schema_ = catalog_rec_.DirtySchema();
        upsert_kv_table_op_.alter_table_info_ = &alter_table_info_;
        txm->PushOperation(&upsert_kv_table_op_, retry_times);
        txm->Process(upsert_kv_table_op_);
    }
    else if (op_ == &upsert_kv_table_op_)
    {
        if (upsert_kv_table_op_.hd_result_.IsError())
        {
            // The data store operation failed. Retries the operation if the
            // tx node is the leader or the tx is in the recovery mode and
            // the cc node is a leader candidate.
            if (txm->CheckLeaderTerm())
            {
                // NOTE: The logic of this part is consistent with the logic in
                // UpsertTableOp::Forward.
                // Keep retrying if it is DropIndex.
                if (op_type_ == OperationType::DropIndex)
                {
                    txm->PushOperation(&upsert_kv_table_op_);
                    txm->Process(upsert_kv_table_op_);
                }
                else
                {
                    LOG(ERROR) << "Alter Table Index for table: "
                               << table_key_.Name().Trace()
                               << ", Failed to create tables in kv store"
                               << ". Txn: " << txm->TxNumber();

                    /*
                    After upsert kv fails, we need to flush a commit log to
                    indicate this error.
                    If we skip this commit log and jump to post_all_lock_op_
                    directly, once the participant crashes at the point between
                    it releases write intent and the coordinator flushes
                    clean_log, then during recovery, the participant sees a
                    prepare_log(whose commit_ts is not 0) and recovers write
                    lock and dirty_catalog.
                    Since the coordinator has finished its job, the write
                    lock recovered by participant becomes orphan lock, and the
                    dirty catalog can not be rejected either.
                    Also, in the current design, post_all_intent_op_ does not
                    release the write intent, which means the write intent is
                    still being held after upsert_kv_table_op_(during
                    CreateTable or AddIndex). If create table or add index in kv
                    fails, the only thing we should do after writing commit_log
                    is to reject dirty schema. So there is no need to upgrade
                    write intent to write lock, and it is safe to skip
                    acquire_all_lock_op_ and jump directly to commit_log_op_.
                    */
                    op_ = &commit_log_op_;
                    FillCommitLogRequest(txm);
                    txm->PushOperation(&commit_log_op_);
                    txm->Process(commit_log_op_);
                }
            }
            else
            {
                ForceToFinish(txm);
            }
        }
        else if (op_type_ == OperationType::DropIndex)
        {
            // For DROP INDEX, the data store operation happens after all
            // write locks are acquired and commit log is flushed.
            LOG(INFO) << "Alter Table Index transaction commit dirty table"
                      << " schema, txn: " << txm->TxNumber();
            op_ = &post_all_lock_op_;
            txm->PushOperation(&post_all_lock_op_);
            txm->Process(post_all_lock_op_);
        }
#ifndef RANGE_PARTITION_ENABLED
        else
        {
            if (txm->TxStatus() == TxnStatus::Recovering &&
                Sharder::Instance().CandidateLeaderTerm(txm->TxCcNodeId()) > 0)
            {
                // If this txm is in the recovering state, should wait until the
                // data log replay finished to avoid data lost.
                return;
            }

            ACTION_FAULT_INJECTOR("term_AlterTableIndex_FlushPkDataOp");
            LOG(INFO) << "Alter Table Index transaction flush all old base"
                      << " table data into data store, txn: " << txm->TxNumber()
                      << ", commit ts: " << txm->commit_ts_
                      << ", and tx term: " << txm->TxTerm();
            assert(op_type_ == OperationType::AddIndex);

            flush_all_old_tuples_pk_op_.op_func_ =
                [this, txm, &hd_res = flush_all_old_tuples_pk_op_.hd_result_]
            {
                uint32_t ng_cnt = Sharder::Instance().NodeGroupCount();
                hd_res.Reset();
                hd_res.SetRefCnt(ng_cnt);
#ifdef EXT_TX_PROC_ENABLED
                hd_res.SetToBlock();
#endif
                for (uint32_t nid = 0; nid < ng_cnt; ++nid)
                {
                    FlushDataIntoDataStore(
                        table_key_.Name(), nid, txm->commit_ts_, false, hd_res);
                }
            };

            op_ = &flush_all_old_tuples_pk_op_;
            txm->PushOperation(&flush_all_old_tuples_pk_op_);
            txm->Process(flush_all_old_tuples_pk_op_);
        }
    }
    else if (op_ == &flush_all_old_tuples_pk_op_)
    {
        if (flush_all_old_tuples_pk_op_.hd_result_.IsError())
        {
            if (txm->CheckLeaderTerm())
            {
                LOG(ERROR)
                    << "Alter Table Index for table: "
                    << table_key_.Name().Trace()
                    << ", flush all old pk tuples failed with error message: "
                    << flush_all_old_tuples_pk_op_.hd_result_.ErrorMsg()
                    << ", txn: " << txm->TxNumber()
                    << ". Retry flush all old pk data.";
                txm->PushOperation(&flush_all_old_tuples_pk_op_, 1);
                flush_all_old_tuples_pk_op_.ReRunOp(txm);
            }
            else
            {
                LOG(WARNING) << "Alter Table Index flush all old pk tuples on "
                             << "non-leader node, terminate directly for txn: "
                             << txm->TxNumber();
                ForceToFinish(txm);
            }
        }
        else
        {
            LOG(INFO) << "Alter Table Index transaction fetch all old pk data"
                      << " from data store and generate new sk data for new"
                      << " added index and upload new sk data into ccmap,"
                      << " txn: " << txm->TxNumber();

            // Reset the node group leader terms.
            ResetLeaderTerms();

            fetch_old_tuples_from_kv_gen_sk_data_upload_op_.op_func_ =
                [this, txm]
            {
#ifdef EXT_TX_PROC_ENABLED
                fetch_old_tuples_from_kv_gen_sk_data_upload_op_.hd_result_
                    .SetToBlock();
                std::atomic_thread_fence(std::memory_order_release);
#endif
                // Launch a new thread instead of sending it to tx workpool to
                // avoid blocking range split workers.
                fetch_old_tuples_from_kv_gen_sk_data_upload_op_.worker_thread_ =
                    std::thread([this, txm]
                                { FetchTuplesAndUploadPackedKey(txm); });
            };

            op_ = &fetch_old_tuples_from_kv_gen_sk_data_upload_op_;
            txm->PushOperation(
                &fetch_old_tuples_from_kv_gen_sk_data_upload_op_);
            txm->Process(fetch_old_tuples_from_kv_gen_sk_data_upload_op_);
        }
    }
    else if (op_ == &fetch_old_tuples_from_kv_gen_sk_data_upload_op_)
    {
        if (fetch_old_tuples_from_kv_gen_sk_data_upload_op_.hd_result_
                .IsError())
        {
            if (txm->CheckLeaderTerm())
            {
                LOG(WARNING)
                    << "Alter Table Index for table: "
                    << table_key_.Name().Trace()
                    << ", retry fetch old pk tuples from kv and upload packed "
                    << "sk failed, txn: " << txm->TxNumber();
                if (fetch_old_tuples_from_kv_gen_sk_data_upload_op_.hd_result_
                        .ErrorCode() == CcErrorCode::REQUESTED_NODE_NOT_LEADER)
                {
                    ResetLeaderTerms();
                }
                txm->PushOperation(
                    &fetch_old_tuples_from_kv_gen_sk_data_upload_op_);
                txm->Process(fetch_old_tuples_from_kv_gen_sk_data_upload_op_);
            }
            else
            {
                LOG(WARNING) << "Alter Table Index generate packed sk data on "
                             << "non-leader node, terminate directly for txn: "
                             << txm->TxNumber();
                ForceToFinish(txm);
            }
            return;
        }

        CODE_FAULT_INJECTOR(
            "term_AlterTableIndex_GeneratePackedSkOp_Continue", {
                static uint64_t count = 0;
                size_t step = txm->bind_to_ext_proc_ ? 100000 : 2;
                if (count++ % step == 0)
                {
                    DLOG(INFO) << "FaultInject term_AlterTableIndex_Generate"
                                  "PackedSkOp_Continue";
                }
                return;
            });
        ACTION_FAULT_INJECTOR("term_AlterTableIndex_FlushNewPackedSKOp");

        assert(op_type_ == OperationType::AddIndex);
        assert(alter_table_info_.index_add_count_ ==
               alter_table_info_.index_add_names_.size());

        auto add_index_it = alter_table_info_.index_add_names_.cbegin();
        assert(add_index_it != alter_table_info_.index_add_names_.cend());
        // This silences the -Wunused-but-set-variable warning without any
        // runtime overhead.
        (void) add_index_it;

        // Be sure that the no failover happen between this operation and the
        // former operation.

        LOG(INFO) << "Alter Table Index transaction flush all old sk data"
                  << " from new added index ccmap into data store, txn: "
                  << txm->TxNumber() << ", and commit ts: " << txm->commit_ts_;

        flush_all_old_tuples_sk_op_.op_func_ =
            [this, txm, &hd_res = flush_all_old_tuples_sk_op_.hd_result_]
        {
            // Send the flush data request to the node groups to which
            // the new packed sk data sharding, so obtain the node group
            // count from the @@leader_terms_.
            uint32_t ng_cnt = this->leader_terms_.size();

            auto &new_index_names = this->alter_table_info_.index_add_names_;
            size_t table_cnt = new_index_names.size();
            auto add_index_it = new_index_names.cbegin();
            assert(add_index_it != new_index_names.cend());

            hd_res.Reset();
            hd_res.SetRefCnt(ng_cnt * table_cnt);
#ifdef EXT_TX_PROC_ENABLED
            hd_res.SetToBlock();
#endif

            for (uint32_t nid = 0; nid < ng_cnt; ++nid)
            {
                int64_t expected_term = this->leader_terms_.at(nid);
                for (add_index_it = new_index_names.cbegin();
                     add_index_it != new_index_names.cend();
                     ++add_index_it)
                {
                    FlushDataIntoDataStore(add_index_it->first,
                                           nid,
                                           txm->commit_ts_,
                                           true,
                                           hd_res,
                                           expected_term);
                }
            }
        };

        op_ = &flush_all_old_tuples_sk_op_;
        txm->PushOperation(&flush_all_old_tuples_sk_op_);
        txm->Process(flush_all_old_tuples_sk_op_);
    }
    else if (op_ == &flush_all_old_tuples_sk_op_)
    {
        if (flush_all_old_tuples_sk_op_.hd_result_.IsError())
        {
            if (txm->CheckLeaderTerm())
            {
                if (flush_all_old_tuples_sk_op_.hd_result_.ErrorCode() ==
                        CcErrorCode::REQUESTED_NODE_NOT_LEADER ||
                    flush_all_old_tuples_sk_op_.hd_result_.ErrorCode() ==
                        CcErrorCode::REQUEST_LOST)
                {
                    LOG(WARNING)
                        << "Alter Table Index flush all old sk tuples failed "
                        << "because of leader transferred. Retry generate "
                        << "packed sk data, txn: " << txm->TxNumber();
                    // For this stage, should re-execute from the previous stage
                    // if leader transferred.
                    ResetLeaderTerms();
                    op_ = &fetch_old_tuples_from_kv_gen_sk_data_upload_op_;
                    txm->PushOperation(
                        &fetch_old_tuples_from_kv_gen_sk_data_upload_op_, 1);
                    // To sleep serval seconds.
                    fetch_old_tuples_from_kv_gen_sk_data_upload_op_.ReRunOp(
                        txm);
                }
                else
                {
                    LOG(WARNING)
                        << "Alter Table Index flush all old sk tuples failed "
                        << "with error message: "
                        << flush_all_old_tuples_sk_op_.hd_result_.ErrorMsg()
                        << ". Retry flush old sk operation, txn: "
                        << txm->TxNumber();

                    op_ = &flush_all_old_tuples_sk_op_;
                    txm->PushOperation(&flush_all_old_tuples_sk_op_);
                    txm->Process(flush_all_old_tuples_sk_op_);
                }
            }
            else
            {
                LOG(INFO) << "Alter Table Index flush all old sk tuples on "
                          << "non-leader node, termiate directly for txn: "
                          << txm->TxNumber();
                ForceToFinish(txm);
            }
        }
        else
        {
            ACTION_FAULT_INJECTOR("term_AlterTableIndex_KickoutAllOp");
            LOG(INFO) << "Alter Table Index transaction kickout all sk data"
                      << " from new added index ccmap, txn: "
                      << txm->TxNumber();
            assert(op_type_ == OperationType::AddIndex);
            assert(alter_table_info_.index_add_count_ ==
                   alter_table_info_.index_add_names_.size());

            auto add_index_it = alter_table_info_.index_add_names_.cbegin();
            assert(add_index_it != alter_table_info_.index_add_names_.cend());

            kickout_data_all_op_.Clear();
            for (; add_index_it != alter_table_info_.index_add_names_.cend();
                 ++add_index_it)
            {
                kickout_data_all_op_.table_names_.push_back(
                    &(add_index_it->first));
            }

            kickout_data_all_op_.commit_ts_ = txm->commit_ts_;

            op_ = &kickout_data_all_op_;
            txm->PushOperation(&kickout_data_all_op_);
            txm->Process(kickout_data_all_op_);
        }
    }
    else if (op_ == &kickout_data_all_op_)
    {
        if (kickout_data_all_op_.hd_result_.IsError())
        {
            if (txm->CheckLeaderTerm())
            {
                LOG(WARNING)
                    << "Alter Table Index kickout old tuples sk failed, with "
                    << "error message: "
                    << kickout_data_all_op_.hd_result_.ErrorMsg()
                    << ". Retry kickout data, txn: " << txm->TxNumber();

                txm->PushOperation(&kickout_data_all_op_);
                txm->Process(kickout_data_all_op_);
            }
            else
            {
                LOG(ERROR) << "Alter Table Index kickout sk data on non-leader "
                           << "node, terminate directly for txn: "
                           << txm->TxNumber();
                ForceToFinish(txm);
            }
        }
        else
        {
            ACTION_FAULT_INJECTOR(
                "term_AlterTableIndex_FlushPrepareIndexTableLogOp");
            LOG(INFO) << "Alter Table Index transaction write prepare index"
                      << " log, txn: " << txm->TxNumber();
            op_ = &prepare_log_for_sk_op_;
            FillPrepareIndexTableLogRequest(txm);
            txm->PushOperation(&prepare_log_for_sk_op_);
            txm->Process(prepare_log_for_sk_op_);
        }
    }
    else if (op_ == &prepare_log_for_sk_op_)
    {
        if (prepare_log_for_sk_op_.hd_result_.IsError())
        {
            // Fails to flush the prepare flush log. Retries the operation if
            // the tx node is still the leader or the tx is in the recovery
            // mode and the cc node is a leader candidate.
            if (txm->CheckLeaderTerm())
            {
                // set retry flag and retry commit log
                ::txlog::WriteLogRequest *log_req =
                    prepare_log_for_sk_op_.log_closure_.LogRequest()
                        .mutable_write_log_request();
                log_req->set_retry(true);
                txm->PushOperation(&prepare_log_for_sk_op_);
                txm->Process(prepare_log_for_sk_op_);
            }
            else
            {
                ForceToFinish(txm);
            }
        }
        else
        {
            LOG(INFO) << "Alter Table Index transaction lock cluster config"
                      << " , txn: " << txm->TxNumber();
            op_ = &lock_cluster_config_op_;
            txm->PushOperation(&lock_cluster_config_op_);
            txm->Process(lock_cluster_config_op_);
        }
    }
#else
        else
        {
            if (txm->TxStatus() == TxnStatus::Recovering &&
                Sharder::Instance().CandidateLeaderTerm(txm->TxCcNodeId()) > 0)
            {
                // If this txm is in the recovering state, should wait until the
                // data log replay finished to avoid data lost.
                return;
            }

            LOG(INFO) << "Alter Table Index transaction start generate sk "
                      << "with the parallelism: "
                      << static_cast<uint32_t>(scan_batch_range_size_)
                      << " of each node group for table: "
                      << table_key_.Name().Trace() << " with the start key: "
                      << (is_last_finished_key_str_
                              ? *last_finished_end_key_str_
                              : last_finished_end_key_->ToString())
                      << ", txn: " << txm->TxNumber();

            if (is_last_finished_key_str_)
            {
                last_scanned_end_key_str_ = last_finished_end_key_str_;
            }
            else
            {
                last_scanned_end_key_ = last_finished_end_key_;
            }
            is_last_scanned_key_str_ = is_last_finished_key_str_;
            ResetLeaderTerms();
            generate_sk_parallel_op_.Reset();
            generate_sk_parallel_op_.op_func_ = [this, txm]()
            {
                generate_sk_parallel_op_.worker_thread_ = std::thread(
                    [this, txm]()
                    {
                        auto &hd_res = generate_sk_parallel_op_.hd_result_;
                        hd_res.Reset();
#ifdef EXT_TX_PROC_ENABLED
                        hd_res.SetToBlock();
#endif
                        this->DispatchRangeTask(txm, hd_res);
                    });
            };
            op_ = &generate_sk_parallel_op_;
            txm->PushOperation(&generate_sk_parallel_op_);
            txm->Process(generate_sk_parallel_op_);
        }
    }
    else if (op_ == &generate_sk_parallel_op_)
    {
        if (generate_sk_parallel_op_.hd_result_.IsError())
        {
            LOG(ERROR)
                << "Alter Table Index generate sk parallel failed for table: "
                << table_key_.Name().Trace() << " with error: "
                << generate_sk_parallel_op_.hd_result_.ErrorMsg()
                << ", txn:" << txm->TxNumber();
            if (txm->CheckLeaderTerm())
            {
                // Retry from the last finished end key.
                assert(generate_sk_parallel_op_.hd_result_.ErrorCode() ==
                           CcErrorCode::REQUESTED_NODE_NOT_LEADER ||
                       generate_sk_parallel_op_.hd_result_.ErrorCode() ==
                           CcErrorCode::PIN_RANGE_SLICE_FAILED ||
                       generate_sk_parallel_op_.hd_result_.ErrorCode() ==
                           CcErrorCode::ACQUIRE_LOCK_BLOCKED ||
                       generate_sk_parallel_op_.hd_result_.ErrorCode() ==
                           CcErrorCode::DATA_STORE_ERR ||
                       generate_sk_parallel_op_.hd_result_.ErrorCode() ==
                           CcErrorCode::OUT_OF_MEMORY ||
                       generate_sk_parallel_op_.hd_result_.ErrorCode() ==
                           CcErrorCode::REQUEST_LOST);
                // Reset last end key.
                if (is_last_finished_key_str_)
                {
                    last_scanned_end_key_str_ = last_finished_end_key_str_;
                }
                else
                {
                    last_scanned_end_key_ = last_finished_end_key_;
                }
                is_last_scanned_key_str_ = is_last_finished_key_str_;
                // Reset scanned pk range count.
                scanned_pk_range_count_ = finished_pk_range_count_;
                ResetLeaderTerms();
                generate_sk_parallel_op_.Reset();
                txm->PushOperation(&generate_sk_parallel_op_, 1);
                // To sleep serval seconds.
                generate_sk_parallel_op_.ReRunOp(txm);
            }
            else
            {
                ForceToFinish(txm);
            }
            return;
        }

        if (total_scanned_pk_items_count_ == 0)
        {
            // There is no pk items in batch range task, and no sk items
            // generated, therefore there is no need to flush sk.
            op_ = &prepare_log_for_sk_op_;
            prepare_log_for_sk_op_.hd_result_.SetFinished();
            // Update the last finished end key.
            last_finished_end_key_ = last_scanned_end_key_;
            is_last_finished_key_str_ = false;
            finished_pk_range_count_ = scanned_pk_range_count_;
            DLOG(INFO) << "Alter Table Indedx no need to perform flush sk "
                       << "operation for this batch range task for base table: "
                       << table_key_.Name().Trace()
                       << ". Txn: " << txm->TxNumber();
            Forward(txm);
            return;
        }

        CODE_FAULT_INJECTOR(
            "term_AlterTableIndex_GeneratePackedSkOp_Continue", {
                static uint64_t count = 0;
                if (count++ % 1000000 == 0)
                {
                    DLOG(INFO) << "FaultInject term_AlterTableIndex_Generate"
                                  "PackedSkOp_Continue";
                }
                return;
            });
        ACTION_FAULT_INJECTOR("term_AlterTableIndex_FlushNewPackedSKOp");
        assert(op_type_ == OperationType::AddIndex);
        assert(alter_table_info_.index_add_count_ ==
               alter_table_info_.index_add_names_.size());

        LOG(INFO) << "Alter Table Index transaction flush old sk record "
                  << "for base table: " << table_key_.Name().Trace()
                  << ". Already scanned " << scanned_pk_range_count_
                  << " pk ranges. Txn: " << txm->TxNumber()
                  << ", and commit ts: " << txm->commit_ts_;

        flush_all_old_tuples_sk_op_.op_func_ =
            [this, txm, &hd_res = flush_all_old_tuples_sk_op_.hd_result_]
        {
            // Send the flush data request to the node groups to which
            // the new packed sk data sharding, so obtain the node group
            // count from the @@expected_ng_terms.
            auto &new_index_names = this->alter_table_info_.index_add_names_;
            size_t table_cnt = new_index_names.size();
            auto add_index_it = new_index_names.cbegin();
            assert(add_index_it != new_index_names.cend());

            uint32_t ng_cnt = leader_terms_.size();
            hd_res.Reset();
            hd_res.SetRefCnt(ng_cnt * table_cnt);

            for (uint32_t nid = 0; nid < ng_cnt; ++nid)
            {
                int64_t expected_term = leader_terms_.at(nid);
                for (add_index_it = new_index_names.cbegin();
                     add_index_it != new_index_names.cend();
                     ++add_index_it)
                {
                    this->FlushDataIntoDataStore(add_index_it->first,
                                                 nid,
                                                 txm->commit_ts_,
                                                 true,
                                                 hd_res,
                                                 expected_term);
                }
            }
        };

        op_ = &flush_all_old_tuples_sk_op_;
        txm->PushOperation(&flush_all_old_tuples_sk_op_);
        txm->Process(flush_all_old_tuples_sk_op_);
    }
    else if (op_ == &flush_all_old_tuples_sk_op_)
    {
        if (flush_all_old_tuples_sk_op_.hd_result_.IsError())
        {
            if (txm->CheckLeaderTerm())
            {
                if (flush_all_old_tuples_sk_op_.hd_result_.ErrorCode() ==
                        CcErrorCode::REQUESTED_NODE_NOT_LEADER ||
                    flush_all_old_tuples_sk_op_.hd_result_.ErrorCode() ==
                        CcErrorCode::REQUEST_LOST)
                {
                    LOG(WARNING)
                        << "Alter table index flush all old sk tuples failed "
                        << "for table: " << table_key_.Name().Trace()
                        << " because of leader transferred. Retry generate "
                        << "packed sk data, txn: " << txm->TxNumber();
                    // For this stage, should re-execute from the previous stage
                    // if leader transferred.
                    // Reset last end key.
                    if (is_last_finished_key_str_)
                    {
                        last_scanned_end_key_str_ = last_finished_end_key_str_;
                    }
                    else
                    {
                        last_scanned_end_key_ = last_finished_end_key_;
                    }
                    is_last_scanned_key_str_ = is_last_finished_key_str_;
                    // Reset scanned pk count.
                    scanned_pk_range_count_ = finished_pk_range_count_;
                    ResetLeaderTerms();
                    generate_sk_parallel_op_.Reset();
                    op_ = &generate_sk_parallel_op_;
                    txm->PushOperation(&generate_sk_parallel_op_, 1);
                    // To sleep serval seconds.
                    generate_sk_parallel_op_.ReRunOp(txm);
                }
                else
                {
                    LOG(WARNING)
                        << "Alter table index flush all old sk tuples failed "
                        << "for table: " << table_key_.Name().Trace()
                        << " with error message: "
                        << flush_all_old_tuples_sk_op_.hd_result_.ErrorMsg()
                        << ". Retry flush old sk operation, txn: "
                        << txm->TxNumber();

                    op_ = &flush_all_old_tuples_sk_op_;
                    txm->PushOperation(&flush_all_old_tuples_sk_op_);
                    txm->Process(flush_all_old_tuples_sk_op_);
                }
            }
            else
            {
                LOG(INFO) << "Alter table index flush all old sk tuples on "
                          << "non-leader node, terminate directly for txn: "
                          << txm->TxNumber();
                ForceToFinish(txm);
            }
        }
        else
        {
            ACTION_FAULT_INJECTOR(
                "term_AlterTableIndex_FlushPrepareIndexTableLogOp");
            // Update the last finished end key.
            last_finished_end_key_ = last_scanned_end_key_;
            is_last_finished_key_str_ = false;
            finished_pk_range_count_ = scanned_pk_range_count_;
            LOG(INFO) << "Alter Table Index transaction write prepare index"
                      << " log with last finished end key: "
                      << (last_finished_end_key_ != nullptr
                              ? last_finished_end_key_->ToString()
                              : "PositiveInf")
                      << ". Base table: " << table_key_.Name().Trace()
                      << ". Txn: " << txm->TxNumber();
            op_ = &prepare_log_for_sk_op_;
            FillPrepareIndexTableLogRequest(txm);
            txm->PushOperation(&prepare_log_for_sk_op_);
            txm->Process(prepare_log_for_sk_op_);
        }
    }
    else if (op_ == &prepare_log_for_sk_op_)
    {
        if (prepare_log_for_sk_op_.hd_result_.IsError())
        {
            // Fails to flush the prepare flush log. Retries the operation if
            // the tx node is still the leader or the tx is in the recovery
            // mode and the cc node is a leader candidate.
            if (txm->CheckLeaderTerm())
            {
                // set retry flag and retry commit log
                ::txlog::WriteLogRequest *log_req =
                    prepare_log_for_sk_op_.log_closure_.LogRequest()
                        .mutable_write_log_request();
                log_req->set_retry(true);
                txm->PushOperation(&prepare_log_for_sk_op_);
                txm->Process(prepare_log_for_sk_op_);
            }
            else
            {
                ForceToFinish(txm);
            }

            return;
        }

        if (txm->TxStatus() == TxnStatus::Recovering &&
            Sharder::Instance().CandidateLeaderTerm(txm->TxCcNodeId()) > 0)
        {
            // If this txm is in the recovering state, should wait until the
            // data log replay finished to avoid data lost.
            return;
        }

        if (!is_last_finished_key_str_ &&
            (last_finished_end_key_ == nullptr ||
             last_finished_end_key_->Type() == KeyType::PositiveInf))
        {
            // Reach the end.
            LOG(INFO) << "Alter Table Index transaction lock cluster config"
                      << ", txn: " << txm->TxNumber();
            op_ = &lock_cluster_config_op_;
            txm->PushOperation(&lock_cluster_config_op_);
            txm->Process(lock_cluster_config_op_);
        }
        else
        {
            // Next batch range task
            LOG(INFO) << "Alter Table Index transaction continue to generate "
                      << "sk parallel for table: " << table_key_.Name().Trace()
                      << " with the start key: "
                      << (is_last_finished_key_str_
                              ? *last_finished_end_key_str_
                              : last_finished_end_key_->ToString())
                      << ". Txn: " << txm->TxNumber();
            // Reset last end key.
            if (is_last_finished_key_str_)
            {
                last_scanned_end_key_str_ = last_finished_end_key_str_;
            }
            else
            {
                last_scanned_end_key_ = last_finished_end_key_;
            }
            is_last_scanned_key_str_ = is_last_finished_key_str_;
            ResetLeaderTerms();
            generate_sk_parallel_op_.Reset();
            generate_sk_parallel_op_.op_func_ = [this, txm]()
            {
                generate_sk_parallel_op_.worker_thread_ = std::thread(
                    [this, txm]()
                    {
                        auto &hd_res = generate_sk_parallel_op_.hd_result_;
                        hd_res.Reset();
#ifdef EXT_TX_PROC_ENABLED
                        hd_res.SetToBlock();
#endif
                        this->DispatchRangeTask(txm, hd_res);
                    });
            };
            op_ = &generate_sk_parallel_op_;
            txm->PushOperation(&generate_sk_parallel_op_);
            txm->Process(generate_sk_parallel_op_);
        }
    }
#endif
    else if (op_ == &acquire_all_lock_op_)
    {
        if (acquire_all_lock_op_.fail_cnt_.load(std::memory_order_relaxed) > 0)
        {
            // Fails to acquire the write lock. The schema operation can
            // only roll forward after flushing the prepare log. Retries the
            // request if the tx node is still the leader or the tx is in
            // the recovery mode and the cc node is a leader candidate.
            if (txm->CheckLeaderTerm())
            {
                // We downgrade catalog write lock to avoid blocking other
                // transaction which acquiring catalog read lock. And then retry
                // to acquire catalog write lock.
                if (acquire_all_lock_op_.IsDeadlock())
                {
                    LOG(INFO) << "Alter Table Index transaction deadlocks with "
                              << "other transaction, downgrade write lock"
                              << ", txn: " << txm->TxNumber();
                    post_all_lock_op_.write_type_ =
                        PostWriteType::DowngradeLock;
                    op_ = &post_all_lock_op_;
                    txm->PushOperation(&post_all_lock_op_);
                    txm->Process(post_all_lock_op_);
                }
                else
                {
                    txm->PushOperation(&acquire_all_lock_op_);
                    txm->Process(acquire_all_lock_op_);
                }
            }
            else
            {
                ForceToFinish(txm);
            }
        }
        else
        {
            LOG(INFO) << "Alter Table Index transaction write commit log"
                      << ", txn: " << txm->TxNumber();
            op_ = &commit_log_op_;
            FillCommitLogRequest(txm);
            txm->PushOperation(&commit_log_op_);
            txm->Process(commit_log_op_);
        }
    }
    else if (op_ == &commit_log_op_)
    {
        if (commit_log_op_.hd_result_.IsError())
        {
            // Fails to flush the commit log. Retries the operation if the
            // tx node is still the leader or the tx is in the  recovery
            // mode and the cc node is a leader candidate.
            if (txm->CheckLeaderTerm())
            {
                // set retry flag and retry commit log
                ::txlog::WriteLogRequest *log_req =
                    commit_log_op_.log_closure_.LogRequest()
                        .mutable_write_log_request();
                log_req->set_retry(true);
                txm->PushOperation(&commit_log_op_);
                txm->Process(commit_log_op_);
            }
            else
            {
                ForceToFinish(txm);
            }
        }
        else if (op_type_ == OperationType::DropIndex)
        {
            LOG(INFO) << "Alter Table Index transaction upsert data store"
                      << " info, txn: " << txm->TxNumber();
            op_ = &upsert_kv_table_op_;
            // Read table schema from local cc shard. This is because we
            // could be recovering from commit stage, in which case we have
            // skipped post_all_intent_op_ and the schema in catalog_rec_
            // would be empty.
            LocalCcShards *shards = Sharder::Instance().GetLocalCcShards();
            auto catalog_entry =
                shards->GetCatalog(table_key_.Name(), txm->TxCcNodeId());
            upsert_kv_table_op_.table_schema_ =
                catalog_entry->dirty_schema_.get();
            upsert_kv_table_op_.alter_table_info_ = &alter_table_info_;
            txm->PushOperation(&upsert_kv_table_op_);
            txm->Process(upsert_kv_table_op_);
        }
        else
        {
            ACTION_FAULT_INJECTOR("term_AlterTableIndex_PostCommitAllWLOp");
            LOG(INFO) << "Alter Table Index transaction commit dirty table"
                      << " schema, txn: " << txm->TxNumber();

            assert(post_all_lock_op_.write_type_ == PostWriteType::PostCommit);
            op_ = &post_all_lock_op_;
            txm->PushOperation(&post_all_lock_op_);
            txm->Process(post_all_lock_op_);
        }
    }
    else if (op_ == &post_all_lock_op_)
    {
        if (!txm->CheckLeaderTerm())
        {
            ForceToFinish(txm);
        }
        else if (acquire_all_intent_op_.fail_cnt_.load(
                     std::memory_order_relaxed) > 0)
        {
            // The schema operation failed without flushing the prepare log.
            // Do not retry post-processing (release write intents) even if
            // it fails. Remaining write intents on the schema, if there are
            // any, will be recovered by individual cc nodes separately.
            txm->upsert_resp_->Finish(UpsertResult::Failed);

            txm->state_stack_.pop_back();
            assert(txm->state_stack_.empty());
            LocalCcShards *local_cc_shards =
                Sharder::Instance().GetLocalCcShards();
            std::unique_lock<std::mutex> lk(
                local_cc_shards->table_index_op_pool_mux_);
            local_cc_shards->table_index_op_pool_.emplace_back(
                std::move(txm->index_op_));
        }
        else if (post_all_lock_op_.hd_result_.IsError())
        {
            // post_all_lock_op_ returns an error:
            // 1. if flush kv succeeds, the schema op is guaranteed to succeed
            // and can only roll forward. Retry this step to install the
            // committed schema and remove write locks;
            // 2. if flush kx fails, the schema op has to roll backward. Retry
            // this step to reject dirty schema and remove write locks.
            txm->PushOperation(&post_all_lock_op_);
            txm->Process(post_all_lock_op_);
        }
        else
        {
            // post_all_lock_op_ has finished without an error.
            assert(!post_all_lock_op_.IsFailed());

            if (post_all_lock_op_.write_type_ == PostWriteType::DowngradeLock)
            {
                post_all_lock_op_.write_type_ = PostWriteType::PostCommit;
                op_ = &acquire_all_lock_op_;
                txm->PushOperation(&acquire_all_lock_op_);
                txm->Process(acquire_all_lock_op_);
                return;
            }
            else if (txm->commit_ts_ != tx_op_failed_ts_)
            {
                assert(post_all_lock_op_.write_type_ !=
                       PostWriteType::DowngradeLock);
                // The tx's modification of the schema has succeeded. If the tx
                // has previously read the same schema and keeps a pointer in
                // the read set to the cc entry of the schema, removes it from
                // the read set. As a result, the tx will not try to release the
                // read lock of the schema when committing.
                const CcEntryAddr &schema_entry_addr =
                    acquire_all_lock_op_.hd_results_[txm->TxCcNodeId()]
                        .Value()
                        .local_cce_addr_;
                txm->rw_set_.DedupRead(schema_entry_addr);
            }
            else
            {
                assert(post_all_lock_op_.write_type_ !=
                       PostWriteType::DowngradeLock);
                // Flush kv failed, or it is recovering from a flush kv failure.
                // This schema op has already been rolled back by now, only need
                // to flush clean log here.
                // Also, flush kv failure does not require lock upgrade(write
                // intent to write lock). So the CcEntryAddr needs to be kept in
                // rset in order to release the read lock when committing.
            }

            LOG(INFO) << "Alter Table Index transaction write clean log"
                      << ", txn: " << txm->TxNumber();
            op_ = &clean_log_op_;
            FillCleanLogRequestCommon(txm, clean_log_op_);
            txm->PushOperation(&clean_log_op_);
            txm->Process(clean_log_op_);
        }
    }
    else if (op_ == &clean_log_op_)
    {
        // When a cc node leader begins recovery, the candidate term is set
        // to the Raft term. When recovery finishes, the candidate term is
        // set to -1 after the leader term. So, obtains the candidate term
        // before the leader term.

        if (clean_log_op_.hd_result_.IsError() && txm->CheckLeaderTerm())
        {
            // set retry flag and retry clean log
            ::txlog::WriteLogRequest *log_req =
                clean_log_op_.log_closure_.LogRequest()
                    .mutable_write_log_request();
            log_req->set_retry(true);
            txm->PushOperation(&clean_log_op_);
            txm->Process(clean_log_op_);
        }
        else
        {
            CODE_FAULT_INJECTOR("alter_schema_term_changed", {
                LOG(INFO) << "FaultInject  alter_schema_term_changed";
                is_force_finished_ = true;
            });
            if (txm->commit_ts_ == tx_op_failed_ts_)
            {
                // Flush kv error or fail to flush prepare_log.
                txm->upsert_resp_->Finish(UpsertResult::Failed);
            }
            else
            {
                assert(txm->commit_ts_ > 0);
                if (is_force_finished_)
                {
                    txm->upsert_resp_->Finish(UpsertResult::Unverified);
                }
                else
                {
                    txm->upsert_resp_->Finish(UpsertResult::Succeeded);
                }
            }

            txm->state_stack_.pop_back();
            assert(txm->state_stack_.empty());

            txm->index_op_->catalog_rec_.Reset();
            LocalCcShards *local_cc_shards =
                Sharder::Instance().GetLocalCcShards();
            std::unique_lock<std::mutex> lk(
                local_cc_shards->table_index_op_pool_mux_);
            local_cc_shards->table_index_op_pool_.emplace_back(
                std::move(txm->index_op_));
        }
    }
    else
    {
        assert(false);
    }
}

void UpsertTableIndexOp::Reset(const std::string_view table_name_str,
                               const std::string &current_image,
                               uint64_t curr_schema_ts,
                               const std::string &dirty_image,
                               const std::string &alter_table_image,
                               OperationType op_type,
                               TransactionExecution *txm)
{
    assert(op_type_ == OperationType::AddIndex ||
           op_type_ == OperationType::DropIndex);

    // 1. Reset TransactionOperation
    retry_num_ = RETRY_NUM;
    is_running_ = false;

    // 2. Reset SchemaOp
    table_key_.Name() = TableName(
        table_name_str.data(), table_name_str.size(), TableType::Primary);
    catalog_rec_.Reset();
    catalog_rec_.SetSchemaImage(current_image);
    catalog_rec_.SetDirtySchemaImage(dirty_image);

    image_str_ = current_image;
    dirty_image_str_ = dirty_image;
    curr_schema_ts_ = curr_schema_ts;
    op_type_ = op_type;

    // 3. Reset UpsertTableIndexOp
    op_ = nullptr;

    read_cluster_result_.Reset();
    read_cluster_result_.ResetTxm(txm);
    cluster_conf_rec_.Reset();
    lock_cluster_config_op_.Reset();
    lock_cluster_config_op_.key_ = NegativeInfinity<VoidKey>::Instance();
    lock_cluster_config_op_.table_name_ =
        TableName(cluster_config_ccm_name_sv, TableType::ClusterConfig);
    lock_cluster_config_op_.rec_ = &cluster_conf_rec_;
    lock_cluster_config_op_.hd_result_ = &read_cluster_result_;

    alter_table_info_image_str_ = alter_table_image;
    alter_table_info_.Reset();
    alter_table_info_.DeserializeAlteredTableInfo(alter_table_info_image_str_);

    uint32_t node_group_cnt = Sharder::Instance().NodeGroupCount();
    acquire_all_intent_op_.Reset(node_group_cnt);
    upgrade_all_intent_to_lock_op_.Reset(node_group_cnt);
    prepare_log_op_.Reset();
    downgrade_all_lock_to_intent_op_.Reset(node_group_cnt);
    unlock_cluster_config_op_.Reset();
    upsert_kv_table_op_.Reset();
#ifndef RANGE_PARTITION_ENABLED
    flush_all_old_tuples_pk_op_.Reset();
    fetch_old_tuples_from_kv_gen_sk_data_upload_op_.Reset();
    flush_all_old_tuples_sk_op_.Reset();
    kickout_data_all_op_.Reset(node_group_cnt, 1);
    kickout_data_all_op_.Clear();
    prepare_log_for_sk_op_.Reset();
#else
    generate_sk_parallel_op_.Reset();
    flush_all_old_tuples_sk_op_.Reset();
    prepare_log_for_sk_op_.Reset();
#endif
    acquire_all_lock_op_.Reset(node_group_cnt);
    commit_log_op_.Reset();
    post_all_lock_op_.Reset(node_group_cnt);
    clean_log_op_.Reset();

    acquire_all_intent_op_.table_name_ = &catalog_ccm_name;
    acquire_all_intent_op_.key_ = &table_key_;
    acquire_all_intent_op_.cc_op_ = CcOperation::ReadForWrite;
    acquire_all_intent_op_.protocol_ = CcProtocol::OCC;

    upgrade_all_intent_to_lock_op_.table_name_ = &catalog_ccm_name;
    upgrade_all_intent_to_lock_op_.key_ = &table_key_;
    upgrade_all_intent_to_lock_op_.cc_op_ = CcOperation::Write;
    upgrade_all_intent_to_lock_op_.protocol_ = CcProtocol::Locking;

    downgrade_all_lock_to_intent_op_.table_name_ = &catalog_ccm_name;
    downgrade_all_lock_to_intent_op_.key_ = &table_key_;
    downgrade_all_lock_to_intent_op_.rec_ = &catalog_rec_;
    downgrade_all_lock_to_intent_op_.op_type_ = op_type_;
    downgrade_all_lock_to_intent_op_.write_type_ = PostWriteType::PrepareCommit;

    upsert_kv_table_op_.alter_table_info_ = &alter_table_info_;
    upsert_kv_table_op_.op_type_ = op_type_;

    acquire_all_lock_op_.table_name_ = &catalog_ccm_name;
    acquire_all_lock_op_.key_ = &table_key_;
    acquire_all_lock_op_.cc_op_ = CcOperation::Write;
    acquire_all_lock_op_.protocol_ = CcProtocol::Locking;

    post_all_lock_op_.table_name_ = &catalog_ccm_name;
    post_all_lock_op_.key_ = &table_key_;
    post_all_lock_op_.rec_ = &catalog_rec_;
    post_all_lock_op_.op_type_ = op_type_;
    post_all_lock_op_.write_type_ = PostWriteType::PostCommit;

    // Reset cc_handler_res txm
    acquire_all_intent_op_.ResetHandlerTxm(txm);
    upgrade_all_intent_to_lock_op_.ResetHandlerTxm(txm);
    prepare_log_op_.ResetHandlerTxm(txm);
    downgrade_all_lock_to_intent_op_.ResetHandlerTxm(txm);
    unlock_cluster_config_op_.ResetHandlerTxm(txm);
    upsert_kv_table_op_.ResetHandlerTxm(txm);
#ifndef RANGE_PARTITION_ENABLED
    flush_all_old_tuples_pk_op_.ResetHandlerTxm(txm);
    fetch_old_tuples_from_kv_gen_sk_data_upload_op_.ResetHandlerTxm(txm);
    flush_all_old_tuples_sk_op_.ResetHandlerTxm(txm);
    kickout_data_all_op_.ResetHandlerTxm(txm);
    prepare_log_for_sk_op_.ResetHandlerTxm(txm);
#else
    generate_sk_parallel_op_.ResetHandlerTxm(txm);
    flush_all_old_tuples_sk_op_.ResetHandlerTxm(txm);
    prepare_log_for_sk_op_.ResetHandlerTxm(txm);
#endif
    acquire_all_lock_op_.ResetHandlerTxm(txm);
    commit_log_op_.ResetHandlerTxm(txm);
    post_all_lock_op_.ResetHandlerTxm(txm);
    clean_log_op_.ResetHandlerTxm(txm);
    post_write_result_.ResetTxm(txm);
    is_force_finished_ = false;

    new_indexes_name_.clear();
    new_indexes_name_.reserve(alter_table_info_.index_add_count_);
    for (auto index_it = alter_table_info_.index_add_names_.cbegin();
         index_it != alter_table_info_.index_add_names_.cend();
         ++index_it)
    {
        new_indexes_name_.emplace_back(index_it->first.StringView(),
                                       index_it->first.Type());
    }

    ResetLeaderTerms();
    const TxKey *neg_inf = Sharder::Instance()
                               .GetLocalCcShards()
                               ->GetCatalogFactory()
                               ->NegativeInfKey();
    last_scanned_end_key_ = neg_inf;
    is_last_scanned_key_str_ = false;
    scanned_pk_range_count_ = 0;
    last_finished_end_key_ = neg_inf;
    is_last_finished_key_str_ = false;
    finished_pk_range_count_ = 0;
    total_scanned_pk_items_count_ = 0;
#ifdef NDEBUG
    scan_batch_range_size_ = 10;
#else
    scan_batch_range_size_ = 3;
#endif
}

void UpsertTableIndexOp::FillPrepareLogRequest(TransactionExecution *txm)
{
    FillPrepareLogRequestCommon(txm, prepare_log_op_);

    ::txlog::WriteLogRequest *prepare_log_rec =
        prepare_log_op_.log_closure_.LogRequest().mutable_write_log_request();

    ::txlog::SchemaOpMessage *prepare_schema_msg =
        prepare_log_rec->mutable_log_content()->mutable_schema_log();
    prepare_schema_msg->set_alter_table_info_blob(alter_table_info_image_str_);

    auto &node_terms = *prepare_log_rec->mutable_node_terms();
    node_terms.clear();
    for (uint32_t nid = 0; nid < upgrade_all_intent_to_lock_op_.upload_cnt_;
         ++nid)
    {
        node_terms[nid] =
            upgrade_all_intent_to_lock_op_.hd_results_[nid].Value().node_term_;
    }
}

void UpsertTableIndexOp::FillPrepareIndexTableLogRequest(
    TransactionExecution *txm)
{
    prepare_log_for_sk_op_.log_type_ = TxLogType::PREPARE;

    prepare_log_for_sk_op_.log_closure_.LogRequest().Clear();

    ::txlog::WriteLogRequest *prepare_log_for_sk_rec =
        prepare_log_for_sk_op_.log_closure_.LogRequest()
            .mutable_write_log_request();

    prepare_log_for_sk_rec->set_tx_term(txm->tx_term_);
    prepare_log_for_sk_rec->set_txn_number(txm->tx_number_);
    prepare_log_for_sk_rec->set_commit_timestamp(txm->commit_ts_);

    ::txlog::SchemaOpMessage *prepare_schema_for_sk_msg =
        prepare_log_for_sk_rec->mutable_log_content()->mutable_schema_log();
    prepare_schema_for_sk_msg->set_stage(
        ::txlog::SchemaOpMessage_Stage_PrepareIndexTable);

    if (last_finished_end_key_ == nullptr ||
        last_finished_end_key_->Type() == KeyType::PositiveInf)
    {
        // reach the last range
        prepare_schema_for_sk_msg->set_last_key_type(
            txlog::SchemaOpMessage::LastKeyType::
                SchemaOpMessage_LastKeyType_PosInfKey);
        prepare_schema_for_sk_msg->mutable_last_key_value()->append("00");
    }
    else
    {
        prepare_schema_for_sk_msg->set_last_key_type(
            txlog::SchemaOpMessage::LastKeyType::
                SchemaOpMessage_LastKeyType_NormalKey);
        last_finished_end_key_->Serialize(
            *prepare_schema_for_sk_msg->mutable_last_key_value());
    }

    prepare_log_for_sk_rec->mutable_node_terms()->clear();
}

void UpsertTableIndexOp::FillCommitLogRequest(TransactionExecution *txm)
{
    FillCommitLogRequestCommon(txm, commit_log_op_);

    ::txlog::WriteLogRequest *commit_log_rec =
        commit_log_op_.log_closure_.LogRequest().mutable_write_log_request();

    if (this->upsert_kv_table_op_.hd_result_.IsError())
    {
        // Serve as new catalog_ts. Set to 0 if flush kv fails.
        commit_log_rec->set_commit_timestamp(tx_op_failed_ts_);
    }
    else
    {
        assert(txm->commit_ts_ != tx_op_failed_ts_);
        commit_log_rec->set_commit_timestamp(txm->commit_ts_);
    }
}

void UpsertTableIndexOp::ForceToFinish(TransactionExecution *txm)
{
    clean_log_op_.hd_result_.SetFinished();
    op_ = &clean_log_op_;
    is_force_finished_ = true;
    Forward(txm);
}

/**
 * @param is_dirty If true, should use the dirty table schema.
 */
void UpsertTableIndexOp::FlushDataIntoDataStore(const TableName &table_name,
                                                NodeGroupId ng_id,
                                                uint64_t data_sync_ts,
                                                bool is_dirty,
                                                CcHandlerResult<Void> &hres,
                                                int64_t ng_term)
{
    LocalCcShards *local_cc_shards = Sharder::Instance().GetLocalCcShards();
    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(ng_id);

#ifdef EXT_TX_PROC_ENABLED
    hres.SetToBlock();
#endif

    if (dest_node_id == local_cc_shards->NodeId())
    {
        if (ng_term < 0 &&
            (ng_term = Sharder::Instance().LeaderTerm(ng_id)) < 0)
        {
            LOG(ERROR)
                << "FlushData operation: request node not the leader of ng#"
                << ng_id;
            hres.SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            return;
        }
        if (ng_term < 0)
        {
            Sharder::Instance().UpdateLeader(ng_id);
            hres.SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            return;
        }
        assert(ng_term > 0);
        uint64_t table_last_synced_ts = 0;
        auto status = std::make_shared<DataSyncStatus>(false);
        local_cc_shards->EnqueueDataSyncTaskForTable(table_name,
                                                     ng_id,
                                                     ng_term,
                                                     data_sync_ts,
                                                     table_last_synced_ts,
                                                     is_dirty,
                                                     false,
                                                     status,
                                                     &hres);
    }
    else
    {
        // For remote node, use RPC service
        std::shared_ptr<brpc::Channel> channel =
            Sharder::Instance().GetCcNodeServiceChannel(dest_node_id);
        if (channel == nullptr)
        {
            LOG(ERROR) << "FlushDataIntoDataStore: Failed to init the channel"
                          " to the leader of ng#"
                       << ng_id;
            hres.SetError(CcErrorCode::ESTABLISH_NODE_CHANNEL_FAILED);
            return;
        }

        remote::CcRpcService_Stub stub(channel.get());

        FlushDataAllClosure *flush_data_closure =
            new FlushDataAllClosure(&hres);
        flush_data_closure->SetChannel(dest_node_id, channel);

        remote::FlushDataAllRequest *req_ptr =
            flush_data_closure->FlushDataAllRequest();
        req_ptr->set_table_name_str(table_name.String());
        req_ptr->set_table_type(
            remote::ToRemoteType::ConvertTableType(table_name.Type()));
        req_ptr->set_node_group_id(ng_id);
        req_ptr->set_node_group_term(ng_term);
        req_ptr->set_data_sync_ts(data_sync_ts);
        req_ptr->set_is_dirty(is_dirty);

        remote::FlushDataAllResponse *resp_ptr =
            flush_data_closure->FlushDataAllResponse();
        brpc::Controller *cntl = flush_data_closure->Controller();
        cntl->set_timeout_ms(-1);
        // Asynchronous mode
        stub.FlushDataAll(cntl, req_ptr, resp_ptr, flush_data_closure);
        DLOG(INFO) << "Acquire FlushDataAll service of ng#" << ng_id << ".";
    }
}

void UpsertTableIndexOp::ResetLeaderTerms()
{
    uint32_t ng_cnt = Sharder::Instance().NodeGroupCount();
    leader_terms_.resize(ng_cnt);
    size_t cnt = leader_terms_.size();
    for (size_t idx = 0; idx < cnt; ++idx)
    {
        leader_terms_.at(idx) = INIT_TERM;
    }
}

void UpsertTableIndexOp::AcquireNodeGroupLeaderTerm(
    NodeGroupId ng_id,
    std::mutex &request_mux,
    std::condition_variable &request_cv,
    uint32_t &finished_req_cnt,
    CcErrorCode &request_res)
{
    int64_t term = INIT_TERM;
    uint32_t leader_node_id = Sharder::Instance().LeaderNodeId(ng_id);
    LocalCcShards *local_cc_shards = Sharder::Instance().GetLocalCcShards();
    if (leader_node_id == local_cc_shards->NodeId())
    {
        // This node is the leader of the input node group.
        term = Sharder::Instance().LeaderTerm(ng_id);
        if (term > 0)
        {
            leader_terms_[ng_id] = term;
        }
        else
        {
            LOG(WARNING) << "Node[" << leader_node_id
                         << "] is not the leader for ng#" << ng_id;
        }
        std::unique_lock<std::mutex> lk(request_mux);
        ++finished_req_cnt;
        request_res = request_res == CcErrorCode::NO_ERROR
                          ? CcErrorCode::REQUESTED_NODE_NOT_LEADER
                          : request_res;
        request_cv.notify_one();
    }
    else
    {
        std::shared_ptr<brpc::Channel> channel =
            Sharder::Instance().GetCcNodeServiceChannel(leader_node_id);
        if (channel == nullptr)
        {
            // Fail to establish the channel to the tx node. Do not update the
            // leader term of input node group.
            LOG(ERROR) << "Acquire leader term: Fail to init the channel to the"
                       << " leader of ng#" << ng_id;
            std::unique_lock<std::mutex> lk(request_mux);
            ++finished_req_cnt;
            request_res = request_res == CcErrorCode::NO_ERROR
                              ? CcErrorCode::ESTABLISH_NODE_CHANNEL_FAILED
                              : request_res;
            request_cv.notify_one();
            return;
        }

        remote::CcRpcService_Stub stub(channel.get());
        AcquireTermClosure *closure = new AcquireTermClosure(ng_id,
                                                             request_mux,
                                                             request_cv,
                                                             finished_req_cnt,
                                                             request_res,
                                                             leader_terms_);
        closure->SetChannel(leader_node_id, channel);
        remote::AcquireNodeGroupTermRequest *request_ptr =
            closure->AcquireTermRequest();
        request_ptr->set_node_group_id(ng_id);
        // This will be deleted after the response been handled.
        remote::AcquireNodeGroupTermResponse *response_ptr =
            closure->AcquireTermResponse();
        brpc::Controller *cntl_ptr = closure->Controller();
        cntl_ptr->set_timeout_ms(1000);
        // Asynchronous mode
        stub.AcquireNodeGroupLeaderTerm(
            cntl_ptr, request_ptr, response_ptr, closure);
        DLOG(INFO) << "Acquire AcquireNodeGroupLeaderTerm service of ng#"
                   << ng_id << ".";
    }
}

bool UpsertTableIndexOp::AcquireRangeReadLocks(
    TransactionExecution *acquire_lock_txm, ReadWriteSet &rw_set)
{
    auto &wset = rw_set.WriteSet();
    for (auto table_it = wset.begin(); table_it != wset.end(); ++table_it)
    {
        const TableName &range_table_name =
            TableName(table_it->first.StringView(), TableType::RangePartition);

        auto &table_write_keys = table_it->second;
        const TxKey *write_key = nullptr;
        for (auto write_key_it = table_write_keys.begin();
             write_key_it != table_write_keys.end();)
        {
            write_key = write_key_it->first;

            RangeRecord range_rec;
            ReadTxRequest read_range_req(
                &range_table_name, write_key, &range_rec, false, false, true);
            acquire_lock_txm->Execute(&read_range_req);
            read_range_req.Wait();

            uint8_t retry_times = RETRY_NUM;
            while (read_range_req.ErrorCode() != TxErrorCode::NO_ERROR &&
                   retry_times > 0)
            {
                LOG(WARNING) << "!!!WARNING!!! Read range info failed with "
                                "error message: "
                             << read_range_req.ErrorMsg()
                             << ", for table: " << range_table_name.Trace()
                             << ". Re-try acquire range read lock for: "
                             << (RETRY_NUM - retry_times-- + 1) << " times.";

                read_range_req.Reset();
                acquire_lock_txm->Execute(&read_range_req);
                read_range_req.Wait();
            }

            if (read_range_req.ErrorCode() != TxErrorCode::NO_ERROR)
            {
                ReleaseRangeReadLocks(acquire_lock_txm, false);

                LOG(ERROR) << "!!!ERROR!!! Read range info failed finally with "
                              "error message: "
                           << read_range_req.ErrorMsg()
                           << ", for table: " << range_table_name.Trace();

                return false;
            }

            AdvanceWriteKeyForRangeInfo(range_rec,
                                        table_write_keys,
                                        write_key_it,
                                        table_write_keys.end(),
                                        acquire_lock_txm->rw_set_);

        } /* End of table write keys */
    }     /* End of tables */
    return true;
}

void UpsertTableIndexOp::ReleaseRangeReadLocks(
    TransactionExecution *acquire_lock_txm, bool is_success)
{
    CommitTxRequest commit_req;
    commit_req.to_commit_ = is_success;
    acquire_lock_txm->CommitTx(commit_req);
}

CcErrorCode UpsertTableIndexOp::AcquireLeaderTermsIfNecessary(
    TransactionExecution *txm)
{
    uint32_t ng_cnt = Sharder::Instance().NodeGroupCount();
    uint32_t old_ng_cnt = leader_terms_.size();
    if (ng_cnt > old_ng_cnt)
    {
        // During the index addition transaction, cluster expansion
        // occurred.
        leader_terms_.insert(
            leader_terms_.end(), (ng_cnt - old_ng_cnt), INIT_TERM);
    }
    // Find the node group id that need to acquire the leader term.
    uint32_t request_count = 0;
    std::vector<NodeGroupId> target_ng_ids;
    for (size_t idx = 0; idx < leader_terms_.size(); ++idx)
    {
        if (leader_terms_.at(idx) == INIT_TERM)
        {
            target_ng_ids.push_back(idx);
            ++request_count;
        }
    }

    if (request_count > 0)
    {
        std::mutex acquire_terms_mutex;
        std::condition_variable acquire_terms_cv;
        uint32_t finished_request_count = 0;
        CcErrorCode request_res = CcErrorCode::NO_ERROR;

        uint8_t retry_times = RETRY_NUM;
        do
        {
            assert(request_count == target_ng_ids.size());
            finished_request_count = 0;
            request_res = CcErrorCode::NO_ERROR;
            for (auto ng_id : target_ng_ids)
            {
                AcquireNodeGroupLeaderTerm(ng_id,
                                           acquire_terms_mutex,
                                           acquire_terms_cv,
                                           finished_request_count,
                                           request_res);
            }

            {
                std::unique_lock<std::mutex> acq_terms_lk(acquire_terms_mutex);
                acquire_terms_cv.wait_for(
                    acq_terms_lk,
                    std::chrono::seconds(2),
                    [&request_count, &finished_request_count]
                    { return request_count == finished_request_count; });
            }

            if (finished_request_count != request_count)
            {
                // Handle the timeout.
                LOG(ERROR) << "Acquire node group leader terms timeout for 2s.";
            }

            // Check txm leader and abort if leader has been transferred.
            if (!txm->CheckLeaderTerm())
            {
                LOG(ERROR)
                    << "Acquire node group leader terms on non-leader node.";
                return CcErrorCode::TX_NODE_NOT_LEADER;
            }

            if (request_res != CcErrorCode::NO_ERROR)
            {
                // Handle the error.
                if (retry_times > 0)
                {
                    LOG(ERROR) << "Acquire node group leader terms failed with "
                               << "error code: " << (uint32_t) request_res;
                    if (request_res == CcErrorCode::REQUEST_LOST ||
                        request_res == CcErrorCode::REQUESTED_NODE_NOT_LEADER)
                    {
                        // Wait a moment to retry this request.
                        std::this_thread::sleep_for(8s);
                    }

                    auto new_it = target_ng_ids.begin();
                    auto old_it = target_ng_ids.begin();
                    for (; old_it != target_ng_ids.end(); ++old_it)
                    {
                        if (leader_terms_.at(*old_it) > 0)
                        {
                            // Have already get the term of this node group
                            // leader.
                            --request_count;
                        }
                        else
                        {
                            *new_it = *old_it;
                            ++new_it;
                        }
                    }
                    target_ng_ids.erase(new_it, old_it);
                    --retry_times;
                }
                else
                {
                    LOG(ERROR) << "Acquire node group leader terms failed "
                               << "finally with error code: "
                               << (uint32_t) request_res;
                    return request_res;
                }
            }
        } while (request_res != CcErrorCode::NO_ERROR);
    }
    return CcErrorCode::NO_ERROR;
}

void UpsertTableIndexOp::UploadRecord(TxNumber tx_number,
                                      int64_t tx_term,
                                      uint16_t command_id,
                                      uint64_t commit_ts,
                                      const TableName &table_name,
                                      const TxKey *key,
                                      const TxRecord *record,
                                      OperationType operation_type,
                                      uint32_t key_shard_code,
                                      CcHandlerResult<PostProcessResult> &hres,
                                      int64_t expected_term)
{
    uint32_t ng_id = Sharder::Instance().ShardToCcNodeGroup(key_shard_code);
    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(ng_id);
    uint32_t local_node_id = Sharder::Instance().NodeId();

    if (dest_node_id == local_node_id)
    {
        PostWriteCc *req = upload_pool_.NextRequest();
        req->Reset(key,
                   table_name,
                   ng_id,
                   tx_number,
                   commit_ts,
                   record,
                   operation_type,
                   key_shard_code,
                   &hres,
                   (operation_type == OperationType::Insert),
                   expected_term);

        TX_TRACE_ACTION(this, req);
        TX_TRACE_DUMP(req);
        Sharder::Instance().GetLocalCcShards()->EnqueueCcRequest(key_shard_code,
                                                                 req);
    }
    else
    {
        hres.Value().is_local_ = false;
        hres.IncrementRemoteRef();

        remote::CcMessage send_msg;

        send_msg.set_type(remote::CcMessage::MessageType::
                              CcMessage_MessageType_ForwardPostCommitRequest);
        send_msg.set_tx_number(tx_number);
        send_msg.set_handler_addr(reinterpret_cast<uint64_t>(&hres));
        send_msg.set_tx_term(tx_term);
        send_msg.set_command_id(command_id);

        remote::ForwardPostCommitRequest *post_commit =
            send_msg.mutable_forward_post_commit_req();
        post_commit->set_src_node_id(local_node_id);
        uint32_t ng_id = Sharder::Instance().ShardToCcNodeGroup(key_shard_code);
        post_commit->set_node_group_id(ng_id);
        post_commit->set_node_group_term(expected_term);
        key->Serialize(*post_commit->mutable_key());
        post_commit->set_table_name_str(table_name.String());
        post_commit->set_table_type(
            remote::ToRemoteType::ConvertTableType(table_name.Type()));
        post_commit->clear_record();
        if (commit_ts > 0 && operation_type != OperationType::Delete)
        {
            // The commit ts is 0, if the post-write request is used to clear
            // the write lock when the tx aborts.
            if (record != nullptr)
            {
                record->Serialize(*post_commit->mutable_record());
            }
        }

        post_commit->set_commit_ts(commit_ts);
        post_commit->set_operation_type(static_cast<uint32_t>(operation_type));
        post_commit->set_key_shard_code(key_shard_code);

        auto *stream_sender = Sharder::Instance().GetCcStreamSender();
        stream_sender->SendMessageToNg(ng_id, send_msg, &hres);
    }
}

void UpsertTableIndexOp::UploadSkData(TransactionExecution *txm,
                                      ReadWriteSet &rw_set)
{
    std::mutex post_write_mutex;
    std::condition_variable post_write_cv;
    bool post_write_finished = false;

    post_write_result_.post_lambda_ =
        [&post_write_mutex, &post_write_cv, &post_write_finished](
            CcHandlerResult<PostProcessResult> *hd_res)
    {
        std::unique_lock<std::mutex> lk(post_write_mutex);
        post_write_finished = true;
        post_write_cv.notify_one();
    };

    do
    {
        // Handle the write set
        post_write_result_.Reset();
        post_write_result_.SetRefCnt(rw_set.WriteSetSize() +
                                     rw_set.ForwardWriteCnt());
        post_write_finished = false;
        ++(txm->command_id_);

        auto &wset = rw_set.WriteSet();
        for (auto &[table_name, entries] : wset)
        {
            for (auto &[key, write_entry] : entries)
            {
#ifndef RANGE_PARTITION_ENABLED
                write_entry.key_shard_code_ =
                    Sharder::Instance().ShardCode(key->Hash());
#endif

                uint32_t ng_id = Sharder::Instance().ShardToCcNodeGroup(
                    write_entry.key_shard_code_);
                int64_t expected_term = leader_terms_.at(ng_id);
                assert(expected_term > 0);

                UploadRecord(txm->tx_number_.load(std::memory_order_relaxed),
                             txm->tx_term_,
                             txm->command_id_.load(std::memory_order_relaxed),
                             txm->commit_ts_,
                             table_name,
                             key,
                             write_entry.rec_.get(),
                             write_entry.op_,
                             write_entry.key_shard_code_,
                             post_write_result_,
                             expected_term);

#ifdef RANGE_PARTITION_ENABLED
                // Double write if the target range is splitting.
                for (const auto &[forward_shard_code, cce_addr] :
                     write_entry.forward_addr_)
                {
                    uint32_t forward_ng_id =
                        Sharder::Instance().ShardToCcNodeGroup(
                            forward_shard_code);
                    int64_t forward_expected_term =
                        leader_terms_.at(forward_ng_id);
                    assert(forward_expected_term > 0);

                    UploadRecord(
                        txm->tx_number_.load(std::memory_order_relaxed),
                        txm->tx_term_,
                        txm->command_id_.load(std::memory_order_relaxed),
                        txm->commit_ts_,
                        table_name,
                        key,
                        write_entry.rec_.get(),
                        write_entry.op_,
                        forward_shard_code,
                        post_write_result_,
                        forward_expected_term);
                }
#endif
            }
        }

        do
        {
            std::unique_lock<std::mutex> post_write_lk(post_write_mutex);
            post_write_cv.wait_for(post_write_lk,
                                   std::chrono::seconds(3),
                                   [&post_write_finished]
                                   { return post_write_finished; });
        } while (post_write_result_.LocalRefCnt() > 0);

        if (!post_write_finished &&
            post_write_result_.SetResultByTimeoutThread())
        {
            // Handle the timeout.
            LOG(ERROR) << "Write the packed sk into sk ccmap timeout for 3s. "
                          "With remote ref count: "
                       << post_write_result_.RemoteRefCnt();
            post_write_result_.ForceError();
        }

        // Check txm leader and abort if leader has been transferred.
        if (!txm->CheckLeaderTerm())
        {
            LOG(ERROR)
                << "Write the packed sk into sk ccmap on the non-leader node.";
            post_write_result_.Reset();
            post_write_result_.SetError(CcErrorCode::TX_NODE_NOT_LEADER);
            return;
        }

        if (post_write_result_.IsError())
        {
            LOG(ERROR) << "Write the packed sk into sk ccmap failed with error "
                          "message: "
                       << post_write_result_.ErrorMsg();
            // Handle the error.
            CcErrorCode error_code = post_write_result_.ErrorCode();
            if (error_code == CcErrorCode::OUT_OF_MEMORY ||
                error_code == CcErrorCode::REQUESTED_NODE_NOT_LEADER)
            {
                // For OOM, should release sk range read lock; For leader
                // transferred, should re-execute from the first batch
                // record.
                return;
            }
        }
    } while (post_write_result_.IsError());
}

bool UpsertTableIndexOp::UploadWithoutDataLog(TransactionExecution *upload_txm)
{
#ifdef RANGE_PARTITION_ENABLED
    LocalCcShards *local_cc_shards = Sharder::Instance().GetLocalCcShards();
    TransactionExecution *acquire_range_lock_txm =
        local_cc_shards->GetTxService()->NewTx();

    InitTxRequest init_req;
    init_req.iso_level_ = IsolationLevel::RepeatableRead;
    init_req.protocol_ = CcProtocol::Locking;
    init_req.tx_ng_id_ = upload_txm->TxCcNodeId();
    init_req.Reset();
    acquire_range_lock_txm->Execute(&init_req);
    init_req.Wait();

    if (init_req.IsError())
    {
        LOG(ERROR) << "UploadWithoutDataLog: Transaction node not leader.";
        return false;
    }

    // 1. Acquire the range read locks.
    if (!AcquireRangeReadLocks(acquire_range_lock_txm, upload_txm->rw_set_))
    {
        LOG(ERROR) << "UploadWithoutDataLog: Acquire range read locks failed.";
        return false;
    }
#endif

    // Check txm leader and abort if leader has been transferred.
    if (!upload_txm->CheckLeaderTerm())
    {
        LOG(ERROR)
            << "UploadWithoutDataLog: Upload data on the non-leader node.";
        // todo: release range lock.
        post_write_result_.SetError(CcErrorCode::TX_NODE_NOT_LEADER);
        return false;
    }

    // 2. Acquire node group term
    CcErrorCode res_code = AcquireLeaderTermsIfNecessary(upload_txm);
    if (res_code != CcErrorCode::NO_ERROR)
    {
        LOG(ERROR) << "UploadWithoutDataLog: Acquire leader terms failed with "
                   << "error code: " << (uint32_t) res_code;
#ifdef RANGE_PARTITION_ENABLED
        ReleaseRangeReadLocks(acquire_range_lock_txm, true);
#endif
        post_write_result_.SetError(res_code);
        return false;
    }

    // 3. post write packed sk
    UploadSkData(upload_txm, upload_txm->rw_set_);
    // If OOM, will re-run this batch records, so can not clear the write
    // set here.
    if (post_write_result_.ErrorCode() != CcErrorCode::OUT_OF_MEMORY)
    {
        // Clear the write set.
        upload_txm->rw_set_.ClearWriteSet();
    }

#ifdef RANGE_PARTITION_ENABLED
    // 4. release the range locks.
    bool successed = post_write_result_.ErrorCode() == CcErrorCode::NO_ERROR;
    ReleaseRangeReadLocks(acquire_range_lock_txm, successed);
#endif

    DLOG(INFO) << "UploadWithoutDataLog: Finished with result code: "
               << (uint32_t) post_write_result_.ErrorCode();
    return !post_write_result_.IsError();
}

#if WITH_KV_STORAGE != KV_CASS
bool UpsertTableIndexOp::PrepareScanFromCcMap(const TableName &table_name,
                                              size_t &scan_alias,
                                              TransactionExecution *&scan_txm)
{
    LocalCcShards *local_cc_shards = Sharder::Instance().GetLocalCcShards();
    scan_txm = local_cc_shards->GetTxService()->NewTx();
    // Set isolation level as snapshot so that can release range lock after
    // scan over one range.
    InitTxRequest init_req;
    init_req.iso_level_ = IsolationLevel::Snapshot;
    init_req.protocol_ = CcProtocol::OccRead;
    init_req.Reset();
    scan_txm->Execute(&init_req);
    init_req.Wait();

    if (init_req.IsError())
    {
        LOG(ERROR) << "PrepareScanFromCcMap: Init scan transaction failed.";
        return false;
    }

    LOG(INFO) << "PrepareScanFromCcMap: ScanBatch txn: "
              << scan_txm->TxNumber();

    const TxKey *start_key =
        local_cc_shards->GetCatalogFactory()->NegativeInfKey();
    ScanOpenTxRequest scan_open(&table_name, ScanIndexType::Primary, start_key);
    scan_txm->Execute(&scan_open);
    scan_open.Wait();

    if (scan_open.IsError())
    {
        LOG(ERROR)
            << "PrepareScanFromCcMap: Scan open failed with error message: "
            << scan_open.ErrorMsg();
        // Abort the scan txm
        AbortTxRequest abort_req;
        scan_txm->Execute(&abort_req);
        abort_req.Wait();
        return false;
    }

    scan_alias = scan_open.Result();
    return true;
}

bool UpsertTableIndexOp::ScanNextFromCcMap(
    TransactionExecution *scan_txm,
    const TableName &table_name,
    const size_t &scan_alias,
    uint64_t commit_ts,
    std::vector<ScanBatchTuple> &scan_batch,
    size_t &scan_batch_idx,
    bool &is_last_scan_batch,
    const TxKey *&target_key,
    const TxRecord *&target_rec)
{
    assert(target_key == nullptr && target_rec == nullptr);
    RecordStatus ccm_scan_rec_status = RecordStatus::Normal;
    uint64_t target_version_ts = UINT64_MAX;

    do
    {
        if (scan_batch_idx < scan_batch.size())
        {
            ScanBatchTuple &scan_tuple = scan_batch[scan_batch_idx];
            target_key = scan_tuple.key_;
            target_rec = scan_tuple.record_;
            ccm_scan_rec_status = scan_tuple.status_;
            target_version_ts = scan_tuple.version_ts_;

            ++scan_batch_idx;
        }
        else if (scan_batch_idx == UINT64_MAX ||
                 !scan_batch.empty() && !is_last_scan_batch)
        {
            // Fetches the next batch.
            scan_batch_idx = 0;
            scan_batch.clear();

            ScanBatchTxRequest scan_batch_req(
                scan_alias, table_name, &scan_batch);
#ifdef RANGE_PARTITION_ENABLED
            scan_batch_req.prefetch_slice_cnt_ = PrefetchSize();
#endif
            scan_txm->Execute(&scan_batch_req);
            scan_batch_req.Wait();

            ++scan_batch_cnt_;

            if (scan_batch_req.IsError())
            {
                LOG(ERROR) << "ScanNextFromCcMap: Scan next batch failed: "
                           << scan_batch_req.ErrorMsg()
                           << ", with result status: "
                           << (uint32_t) scan_batch_req.tx_result_.Status();
                return false;
            }

            is_last_scan_batch = scan_batch_req.Result();
            if (!scan_batch.empty())
            {
                ScanBatchTuple &scan_tuple = scan_batch[scan_batch_idx];
                target_key = scan_tuple.key_;
                target_rec = scan_tuple.record_;
                ccm_scan_rec_status = scan_tuple.status_;
                target_version_ts = scan_tuple.version_ts_;
                ++scan_batch_idx;
            }
            else
            {
                // No more tuples
                break;
            }
        }
        else if (is_last_scan_batch)
        {
            break;
        }

        // Skip the non-Normal records, and get the next tuple.
    } while (!(ccm_scan_rec_status == RecordStatus::Normal &&
               target_version_ts <= commit_ts));

    assert(target_version_ts != 1);
    return true;
}

void UpsertTableIndexOp::FinishScanFromCcMap(
    TransactionExecution *scan_txm,
    const TableName &table_name,
    const size_t &scan_alias,
    std::vector<ScanBatchTuple> &scan_batch,
    bool is_success)
{
    ScanCloseTxRequest close_req(scan_batch, 0, scan_alias, &table_name);
    scan_txm->Execute(&close_req);
    close_req.Wait();

    if (is_success)
    {
        // Commit the scan txm
        CommitTxRequest commit_req;
        scan_txm->Execute(&commit_req);
        commit_req.Wait();
    }
    else
    {
        // Abort the scan txm
        AbortTxRequest abort_req;
        scan_txm->Execute(&abort_req);
        abort_req.Wait();
    }
}
#endif

std::unique_ptr<store::DataStoreScanner>
UpsertTableIndexOp::PrepareScanFromDataStore(const TableName &table_name,
                                             const TableSchema *table_schema,
                                             NodeGroupId ng_id,
                                             uint64_t commit_ts)
{
    // Construct the search condition.
    std::vector<store::DataStoreSearchCond> search_conds;
    search_conds.push_back({"___version___",
                            "<=",
                            std::to_string(commit_ts),
                            store::DataStoreDataType::Numeric});

    store::DataStoreHandler *const store_hd =
        Sharder::Instance().GetLocalCcShards()->store_hd_;
    std::vector<TableName> new_indexes_name;
    for (auto index_it = alter_table_info_.index_add_names_.cbegin();
         index_it != alter_table_info_.index_add_names_.cend();
         ++index_it)
    {
        new_indexes_name.emplace_back(index_it->first);
    }

    return store_hd->ScanPkAndNewSkColumns(
        table_name, table_schema, ng_id, search_conds, new_indexes_name);
}

void UpsertTableIndexOp::ScanNextFromDataStore(
    store::DataStoreScanner *ds_scanner,
    bool &is_first_scan,
    const TxKey *&target_key,
    const TxRecord *&target_rec)
{
    assert(target_key == nullptr && target_rec == nullptr);
    bool is_ds_key_deleted = true;
    uint64_t target_version_ts = UINT64_MAX;

    if (is_first_scan)
    {
        ds_scanner->Current(
            target_key, target_rec, target_version_ts, is_ds_key_deleted);
        if (target_key == nullptr)
        {
            // No more rows.
            return;
        }
        is_first_scan = false;
    }

    while (is_ds_key_deleted)
    {
        // Skip the deleted record and Move to the next tuple
        ds_scanner->MoveNext();

        ds_scanner->Current(
            target_key, target_rec, target_version_ts, is_ds_key_deleted);
        if (target_key == nullptr)
        {
            // No more rows.
            break;
        }
    }
}

void UpsertTableIndexOp::FinishScanFromDataStore(
    std::unique_ptr<store::DataStoreScanner> &ds_scanner)
{
    ds_scanner->End();
    ds_scanner = nullptr;
}

#ifndef RANGE_PARTITION_ENABLED
void UpsertTableIndexOp::FetchTuplesAndUploadPackedKey(
    TransactionExecution *txm)
{
    assert(alter_table_info_.index_add_count_ > 0 &&
           alter_table_info_.index_add_count_ ==
               alter_table_info_.index_add_names_.size());
    assert(txm != nullptr);
    LOG(INFO) << "Generate packed sk and write into sk ccmap, txn: "
              << txm->TxNumber();

    NodeGroupId ng_id = txm->TxCcNodeId();
    int32_t term = Sharder::Instance().TryPinNodeGroupData(ng_id);
    if (term < 0)
    {
        LOG(WARNING) << "Txm node not leader, terminate directly.";
        fetch_old_tuples_from_kv_gen_sk_data_upload_op_.hd_result_.SetError(
            CcErrorCode::TX_NODE_NOT_LEADER);
        return;
    }
    // guard to unpin node group on finish.
    std::shared_ptr<void> defer_unpin(
        nullptr,
        [ng_id](void *) { Sharder::Instance().UnpinNodeGroupData(ng_id); });
    uint64_t commit_ts = txm->commit_ts_;
    const TableName &base_table_name = table_key_.Name();
    // Read table schema from local cc shard. This is because we could be
    // recovering from prepare flush pk stage, in which case we have skipped
    // post_all_intent_op_ and the schema in catalog_rec_ would be empty.
    LocalCcShards *local_cc_shards = Sharder::Instance().GetLocalCcShards();
    auto catalog_entry = local_cc_shards->GetCatalog(base_table_name, ng_id);
    TableSchema *table_schema =
        const_cast<TableSchema *>(catalog_entry->dirty_schema_.get());
    assert(table_schema != nullptr);

    // 1. Prepare scan
#if WITH_KV_STORAGE != KV_CASS
    size_t scan_alias = 0;
    TransactionExecution *scan_txm = nullptr;
    if (!PrepareScanFromCcMap(base_table_name, scan_alias, scan_txm))
    {
        LOG(ERROR) << "[Generate packed sk] Prepare scan from ccmap failed.";
        fetch_old_tuples_from_kv_gen_sk_data_upload_op_.hd_result_.SetError(
            CcErrorCode::UPLOAD_RECORD_TO_CCMAP_ERR);
        return;
    }

    std::vector<ScanBatchTuple> scan_batch;
    scan_batch.clear();
    size_t scan_batch_idx = UINT64_MAX;
    bool is_last_scan_batch = false;
    scan_batch_cnt_ = 0;
#else
    std::unique_ptr<store::DataStoreScanner> ds_scanner =
        PrepareScanFromDataStore(
            base_table_name, table_schema, ng_id, commit_ts);
    assert(ds_scanner.get() != nullptr);
    bool is_first_scan = true;
#endif

    uint32_t upload_batch_cnt = 0;
    uint32_t total_upload_cnt = 0;
    bool has_initialized = false;
    bool need_move_next = true;
    const TxKey *target_key = nullptr;
    const TxRecord *target_rec = nullptr;
    SkEncoder::uptr sk_encoder = nullptr;

    // 2. Handle tuples one by one
    do
    {
        if (need_move_next)
        {
            target_key = nullptr;
            target_rec = nullptr;
#if WITH_KV_STORAGE != KV_CASS
            if (!ScanNextFromCcMap(scan_txm,
                                   base_table_name,
                                   scan_alias,
                                   commit_ts,
                                   scan_batch,
                                   scan_batch_idx,
                                   is_last_scan_batch,
                                   target_key,
                                   target_rec))
            {
                FinishScanFromCcMap(
                    scan_txm, base_table_name, scan_alias, scan_batch, false);
                // set result handler
                fetch_old_tuples_from_kv_gen_sk_data_upload_op_.hd_result_
                    .SetError(CcErrorCode::UPLOAD_RECORD_TO_CCMAP_ERR);
                return;
            }
#else
            ScanNextFromDataStore(
                ds_scanner.get(), is_first_scan, target_key, target_rec);
#endif

            if (target_key == nullptr)
            {
                // No more tuples
                break;
            }
        } /* End of need move next */

        if (!has_initialized)
        {
            // 2.1 Prepare generate pack sk operation.
            sk_encoder = table_schema->CreateSkEncoder();
            has_initialized = true;
        }

        TxErrorCode err = TxErrorCode::NO_ERROR;
        // Pack record for secondary index
        for (auto index_it = alter_table_info_.index_add_names_.cbegin();
             index_it != alter_table_info_.index_add_names_.cend();
             ++index_it)
        {
            // 2.2 Generate packed sk
            auto packed_sk = sk_encoder->GeneratePackedSk(
                target_key, target_rec, index_it->first);

            if (packed_sk.first.get() == nullptr)
            {
                LOG(ERROR) << "Failed to generate packed sk for table: ["
                           << index_it->first.StringView() << "].";

#if WITH_KV_STORAGE != KV_CASS
                FinishScanFromCcMap(
                    scan_txm, base_table_name, scan_alias, scan_batch, false);
#else
                FinishScanFromDataStore(ds_scanner);
#endif

                // Finish the pack sk operation
                sk_encoder.reset(nullptr);
                defer_unpin.reset();
                fetch_old_tuples_from_kv_gen_sk_data_upload_op_.hd_result_
                    .SetError(CcErrorCode::PACK_SK_ERR);
                return;
            }

            // 2.3 Put the record into local write set
            err = txm->TxUpsert(index_it->first,
                                std::move(packed_sk.first),
                                std::move(packed_sk.second),
                                OperationType::Upsert);

            // TxUpsert only failed caused by exceed the
            // @@ReadWriteSet::MaxWriteSetBytesCnt of the local write set.
            // Then, should upload this batch write entry, rather than stop
            // packed sk operation with error code.
            if (err != TxErrorCode::NO_ERROR)
            {
                assert(err == TxErrorCode::WRITE_SET_BYTES_COUNT_EXCEED_ERR);
                need_move_next = false;
                break;
            }
            need_move_next = true;
        } /* end of foreache add_index_names_ */

        if (upload_batch_cnt % 10000 == 0)
        {
            // Check for leader term periodically and abort if leader
            // has been transferred.
            if (!Sharder::Instance().CheckLeaderTerm(ng_id, term))
            {
#if WITH_KV_STORAGE != KV_CASS
                FinishScanFromCcMap(
                    scan_txm, base_table_name, scan_alias, scan_batch, false);
#else
                FinishScanFromDataStore(ds_scanner);
#endif
                // Finish the pack sk operation
                sk_encoder.reset(nullptr);
                defer_unpin.reset();
                LOG(WARNING) << "Generate packed sk and write into sk ccmap on "
                                "non-leader node for ng#"
                             << ng_id << ", and node group term " << term
                             << ". Terminate directly.";
                fetch_old_tuples_from_kv_gen_sk_data_upload_op_.hd_result_
                    .SetError(CcErrorCode::TX_NODE_NOT_LEADER);
                return;
            }
        }

        // Upload this batch write entry depending on the record count or the
        // record bytes.
        if (++upload_batch_cnt >= UPLOAD_BATCH_SIZE ||
            err == TxErrorCode::WRITE_SET_BYTES_COUNT_EXCEED_ERR)
        {
            CcErrorCode error_code = CcErrorCode::NO_ERROR;
            while (!UploadWithoutDataLog(txm))
            {
                error_code = post_write_result_.ErrorCode();
                if (error_code == CcErrorCode::OUT_OF_MEMORY)
                {
                    LOG(WARNING)
                        << "!!!WARNING!!! Write new packed sk into sk ccmap "
                           "failed caused by OOM. Retry after sleep 150s.";
                    std::this_thread::sleep_for(150s);
                }
                else
                {
                    LOG(ERROR)
                        << "Upload this batch new packed sk data failed "
                           "caused by leader transferred for base table: "
                        << base_table_name.StringView();
#if WITH_KV_STORAGE != KV_CASS
                    FinishScanFromCcMap(scan_txm,
                                        base_table_name,
                                        scan_alias,
                                        scan_batch,
                                        false);
#else
                    FinishScanFromDataStore(ds_scanner);
#endif
                    // Finish the pack sk operation
                    sk_encoder.reset(nullptr);
                    defer_unpin.reset();
                    if (error_code == CcErrorCode::TX_NODE_NOT_LEADER)
                    {
                        fetch_old_tuples_from_kv_gen_sk_data_upload_op_
                            .hd_result_.SetError(
                                CcErrorCode::TX_NODE_NOT_LEADER);
                    }
                    else
                    {
                        fetch_old_tuples_from_kv_gen_sk_data_upload_op_
                            .hd_result_.SetError(
                                CcErrorCode::REQUESTED_NODE_NOT_LEADER);
                    }
                    return;
                }
            }

            // Upload this batch sk records successfully.
            total_upload_cnt += upload_batch_cnt;
            if (total_upload_cnt % 1024000 == 0)
            {
                LOG(INFO) << "Alter Table Index transaction upload sk data"
                          << " into added sk ccmap, has upload batch count: "
                          << total_upload_cnt;
            }

            // Reset
            upload_batch_cnt = 0;
        }
    } while (true);

    // Upload the remaining records.
    if (upload_batch_cnt > 0)
    {
        CcErrorCode error_code = CcErrorCode::NO_ERROR;
        while (!UploadWithoutDataLog(txm))
        {
            error_code = post_write_result_.ErrorCode();
            if (error_code == CcErrorCode::OUT_OF_MEMORY)
            {
                LOG(WARNING) << "!!!WARNING!!! Write the last batch new packed "
                                "sk into sk ccmap failed caused by OOM. Retry "
                                "after sleep 150s.";
                std::this_thread::sleep_for(150s);
            }
            else
            {
                LOG(ERROR) << "Upload the last batch new packed sk data failed "
                              "caused by leader transferred for base table: "
                           << base_table_name.StringView();
#if WITH_KV_STORAGE != KV_CASS
                FinishScanFromCcMap(
                    scan_txm, base_table_name, scan_alias, scan_batch, false);
#else
                FinishScanFromDataStore(ds_scanner);
#endif
                // Finish the pack sk operation
                sk_encoder.reset(nullptr);
                defer_unpin.reset();
                if (error_code == CcErrorCode::TX_NODE_NOT_LEADER)
                {
                    fetch_old_tuples_from_kv_gen_sk_data_upload_op_.hd_result_
                        .SetError(CcErrorCode::TX_NODE_NOT_LEADER);
                }
                else
                {
                    fetch_old_tuples_from_kv_gen_sk_data_upload_op_.hd_result_
                        .SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
                }
                return;
            }
        }
        LOG(INFO) << "Alter Table Index transaction upload sk data into"
                  << " added sk ccmap for the last batch";
    }

#if WITH_KV_STORAGE != KV_CASS
    FinishScanFromCcMap(
        scan_txm, base_table_name, scan_alias, scan_batch, true);
#else
    FinishScanFromDataStore(ds_scanner);
#endif

    if (has_initialized)
    {
        // Finish the packed sk operation
        sk_encoder.reset(nullptr);
    }

    defer_unpin.reset();
    LOG(INFO)
        << "Generate packed sk and write into sk ccmap successfully. Txn: "
        << txm->TxNumber();
    fetch_old_tuples_from_kv_gen_sk_data_upload_op_.hd_result_.SetFinished();
}
#endif

void UpsertTableIndexOp::DispatchRangeTask(
    TransactionExecution *upsert_index_txm, CcHandlerResult<Void> &hd_res)
{
    LocalCcShards *cc_shards = Sharder::Instance().GetLocalCcShards();
    const TableName &base_table_name = table_key_.Name();
    const TableName &range_table_name =
        TableName(base_table_name.StringView(), TableType::RangePartition);
    uint32_t local_ng_id = Sharder::Instance().NodeId();
    uint64_t tx_number = upsert_index_txm->TxNumber();
    int64_t tx_term = upsert_index_txm->TxTerm();
    const TxKey *target_range_end_key =
        cc_shards->GetCatalogFactory()->PositiveInfKey();
    const TxKey *target_range_start_key = last_scanned_end_key_;

    uint32_t node_group_cnt = 0;
    size_t batch_range_cnt = 0;
    // Protect the task result.
    std::mutex task_mux;
    std::condition_variable task_cv;
    bool all_task_started = false;
    uint32_t unfinished_task_cnt = 1;
    CcErrorCode task_res = CcErrorCode::NO_ERROR;
    uint32_t pk_items_count = 0;
    uint32_t dispatched_task_count = 0;

    std::function<void(const TxKey *batch_range_start_key,
                       const TxKey *batch_range_end_key,
                       const std::string *batch_range_start_key_str,
                       const std::string *batch_range_end_key_str,
                       const TxKey *&last_scanned_end_key,
                       bool &is_last_scanned_key_str,
                       size_t batch_range_cnt,
                       uint32_t &actual_task_cnt)>
        dispatch_batch_tasks;

    dispatch_batch_tasks =
        [this,
         &base_table_name,
         &range_table_name,
         &local_ng_id,
         cc_shards,
         &tx_number,
         &tx_term,
         scan_ts = upsert_index_txm->commit_ts_,
         &task_mux,
         &task_cv,
         &all_task_started,
         &unfinished_task_cnt,
         &task_res,
         &pk_items_count,
         &dispatched_task_count,
         &dispatch_batch_tasks](const TxKey *batch_range_start_key,
                                const TxKey *batch_range_end_key,
                                const std::string *batch_range_start_key_str,
                                const std::string *batch_range_end_key_str,
                                const TxKey *&last_scanned_end_key,
                                bool &is_last_scanned_key_str,
                                size_t batch_range_cnt,
                                uint32_t &actual_task_cnt)
    {
        TransactionExecution *acq_range_lock_txm = nullptr;
        InitTxRequest init_req;
        acq_range_lock_txm = cc_shards->GetTxService()->NewTx();
        init_req.iso_level_ = IsolationLevel::RepeatableRead;
        init_req.protocol_ = CcProtocol::Locking;
        init_req.tx_ng_id_ = local_ng_id;
        // Init the txm until succeed or the txm node is not leader.
        do
        {
            init_req.Reset();
            acq_range_lock_txm->Execute(&init_req);
            init_req.Wait();
            if (init_req.IsError())
            {
                if (!Sharder::Instance().CheckLeaderTerm(local_ng_id, tx_term))
                {
                    LOG(ERROR)
                        << "DispatchRangeTask: Transaction node not leader.";
                    std::unique_lock<std::mutex> lk(task_mux);
                    task_res = CcErrorCode::TX_NODE_NOT_LEADER;
                    return;
                }
                LOG(WARNING)
                    << "Init acquire range txm failed for table: "
                    << range_table_name.Trace() << " of ng#" << local_ng_id
                    << ", with error: " << init_req.ErrorMsg()
                    << ". Retry after 3s.";
                std::this_thread::sleep_for(3s);
            }
        } while (init_req.IsError());

        ReadTxRequest read_range_req;
        RangeRecord range_rec;
        const TxKey *curr_range_start_key = batch_range_start_key;
        const TxKey *curr_range_end_key = nullptr;
        int32_t partition_id = 0;
        NodeGroupId range_owner = 0;
        size_t idx = 0;
        bool acquire_next_range = false;
        std::string log_info;

        do
        {
            if (!is_last_scanned_key_str)
            {
                read_range_req.Set(&range_table_name,
                                   curr_range_start_key,
                                   &range_rec,
                                   false,
                                   false,
                                   true);
            }
            else
            {
                read_range_req.Set(&range_table_name,
                                   batch_range_start_key_str,
                                   &range_rec,
                                   false,
                                   false,
                                   true);
            }
            // Acquire range read lock.
            read_range_req.Reset();
            acq_range_lock_txm->Execute(&read_range_req);
            read_range_req.Wait();
            if (read_range_req.IsError())
            {
                // This read operation might fail if it's blocked by a write
                // lock acquired by range split.
                LOG(ERROR) << "Acquire range read lock failed for table: "
                           << range_table_name.Trace() << " of ng#"
                           << local_ng_id
                           << ", with error: " << read_range_req.ErrorMsg();
                break;
            }

            curr_range_start_key = range_rec.GetRangeInfo()->StartKey();
            curr_range_end_key = range_rec.GetRangeInfo()->EndKey();
            partition_id = range_rec.GetRangeInfo()->PartitionId();
            range_owner = range_rec.GetRangeOwnerNg()->BucketOwner();
            if (curr_range_start_key == nullptr)
            {
                curr_range_start_key =
                    cc_shards->GetCatalogFactory()->NegativeInfKey();
            }
            if (curr_range_end_key == nullptr)
            {
                curr_range_end_key =
                    cc_shards->GetCatalogFactory()->PositiveInfKey();
            }

            HandleRangeTask(base_table_name,
                            partition_id,
                            curr_range_start_key,
                            curr_range_end_key,
                            range_owner,
                            scan_ts,
                            tx_number,
                            tx_term,
                            task_mux,
                            task_cv,
                            unfinished_task_cnt,
                            all_task_started,
                            pk_items_count,
                            dispatched_task_count,
                            task_res,
                            dispatch_batch_tasks);
            ++actual_task_cnt;

            if (batch_range_cnt > 0)
            {
                acquire_next_range =
                    ++idx < batch_range_cnt &&
                    curr_range_end_key->Type() == KeyType::Normal;
            }
            else
            {
                if (batch_range_end_key)
                {
                    acquire_next_range =
                        *curr_range_end_key < *batch_range_end_key;
                }
                else
                {
                    assert(batch_range_end_key_str);
                    std::string serialized_end_key;
                    if (curr_range_end_key->Type() == KeyType::Normal)
                    {
                        curr_range_end_key->Serialize(serialized_end_key);
                    }
                    acquire_next_range =
                        serialized_end_key.length() !=
                            batch_range_end_key_str->length() ||
                        serialized_end_key.compare(*batch_range_end_key_str);
                }
            }
            // Update the last range end key
            is_last_scanned_key_str = false;
            last_scanned_end_key = curr_range_end_key;
            // Read the next range.
            curr_range_start_key = curr_range_end_key;
            log_info.append(std::to_string(partition_id)).append(",");
        } while (acquire_next_range);

        LOG(INFO) << "Process this batch task for table: "
                  << base_table_name.Trace() << " of range ids: " << log_info
                  << ". Acquire range lock txn: "
                  << acq_range_lock_txm->TxNumber();
        // Relase the range locks.
        CommitTxRequest commit_req;
        acq_range_lock_txm->CommitTx(commit_req);
    };

    // Begin to dispatch pk range task until flush sk is needed.
    do
    {
        node_group_cnt = Sharder::Instance().NodeGroupCount();
        batch_range_cnt = node_group_cnt * scan_batch_range_size_;
        target_range_start_key = last_scanned_end_key_;
        all_task_started = false;
        unfinished_task_cnt = 1;
        task_res = CcErrorCode::NO_ERROR;
        pk_items_count = 0;
        dispatched_task_count = 0;
        uint32_t actual_task_cnt = 0;

        dispatch_batch_tasks(
            target_range_start_key,
            target_range_end_key,
            (is_last_scanned_key_str_ ? last_scanned_end_key_str_ : nullptr),
            nullptr,
            last_scanned_end_key_,
            is_last_scanned_key_str_,
            batch_range_cnt,
            actual_task_cnt);

        // Wait the result
        std::unique_lock<std::mutex> lk(task_mux);
        dispatched_task_count += actual_task_cnt;
        all_task_started = true;
        --unfinished_task_cnt;
        task_cv.wait(lk,
                     [&all_task_started, &unfinished_task_cnt]()
                     { return all_task_started && unfinished_task_cnt == 0; });

        for (auto &workers : local_task_workers_)
        {
            workers.join();
        }
        local_task_workers_.clear();

        if (task_res != CcErrorCode::NO_ERROR)
        {
            LOG(ERROR) << "Generate sk task failed for table: "
                       << base_table_name.Trace()
                       << ", with error: " << CcErrorMessage(task_res);
            hd_res.SetError(task_res);
            return;
        }
        else
        {
            // Update the scanned pk range count.
            scanned_pk_range_count_ += dispatched_task_count;
            total_scanned_pk_items_count_ += pk_items_count;
            DLOG(INFO) << "Generate sk task successfully for this batch ranges."
                       << " Base table: " << base_table_name.Trace();
        }
    } while (!NeedTriggerFlushSkOp());
    DLOG(INFO) << "Generate sk batch task finished."
               << " Base table: " << base_table_name.Trace();
    hd_res.SetFinished();
}

void UpsertTableIndexOp::HandleRangeTask(
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
                       uint32_t &actual_task_cnt)> &dispatch_func)
{
    uint32_t local_node_id = Sharder::Instance().NodeId();
    uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(range_owner);
    if (dest_node_id == local_node_id)
    {
        local_task_workers_.push_back(std::thread(
            [this,
             &base_table_name,
             partition_id,
             range_start_key,
             range_end_key,
             range_owner,
             scan_ts,
             tx_number,
             tx_term,
             &ng_terms = leader_terms_,
             &sk_names = new_indexes_name_,
             &task_mux,
             &task_cv,
             &unfinished_task_cnt,
             &all_task_started,
             &total_pk_items_count,
             &dispatched_task_count,
             &task_res,
             &dispatch_func]()
            {
                while (Sharder::Instance().LeaderTerm(range_owner) < 0 &&
                       Sharder::Instance().CandidateLeaderTerm(range_owner) > 0)
                {
                    // Waiting until log replay finished on this node group.
                    // Including data(.pk) log and and catalog(.table range
                    // info) log.
                    LOG(WARNING) << "GenerateSkFromPk of ng#" << range_owner
                                 << " for partition id: " << partition_id
                                 << " waiting log replay finished.";
                    std::this_thread::sleep_for(3s);
                }

                LocalCcShards *cc_shards =
                    Sharder::Instance().GetLocalCcShards();

                // Check the task status
                auto task_status = cc_shards->GetGenerateSkStatus(
                    range_owner, tx_number, partition_id, tx_term);
                if (!task_status->StartGenerateSk(tx_term))
                {
                    // Terminate itself
                    LOG(WARNING)
                        << "Terminate this generate sk task of ng#"
                        << range_owner << " for partition id: " << partition_id
                        << " with end key: " << range_end_key->ToString()
                        << " caused by the tx term is expired.";
                    std::unique_lock<std::mutex> lk(task_mux);
                    --unfinished_task_cnt;
                    task_res = CcErrorCode::TX_NODE_NOT_LEADER;
                    task_cv.notify_one();
                    return;
                }

                SkGenerator sk_generator;
                uint32_t scanned_pk_items_count = 0;
                CcErrorCode res_code = CcErrorCode::NO_ERROR;
                sk_generator.GenerateSkFromPk(base_table_name,
                                              partition_id,
                                              range_start_key,
                                              range_end_key,
                                              range_owner,
                                              scan_ts,
                                              sk_names,
                                              scanned_pk_items_count,
                                              res_code,
                                              *task_status);

                auto result = task_status->TaskStatus();
                task_status->FinishGenerateSk();
                if (result == GenerateSkStatus::Status::Terminating)
                {
                    LOG(ERROR)
                        << "Terminate this generate sk task of ng#"
                        << range_owner << " for partition id: " << partition_id
                        << "  caused by TX_NODE_NOT_LEADER";
                    std::unique_lock<std::mutex> lk(task_mux);
                    --unfinished_task_cnt;
                    task_res = CcErrorCode::TX_NODE_NOT_LEADER;
                    task_cv.notify_one();
                    return;
                }
                if (res_code == CcErrorCode::GET_RANGE_ID_ERR)
                {
                    LOG(WARNING)
                        << "Terminate this generate sk task of ng#"
                        << range_owner << " for partition id: " << partition_id
                        << " for table: " << base_table_name.Trace()
                        << " caused by the boundary of partition mismatch.";

                    // Update the task status
                    {
                        std::lock_guard<std::mutex> task_lk(task_mux);
                        all_task_started = false;
                        --unfinished_task_cnt;
                    }

                    // Re-dispatch this range task.
                    const TxKey *last_scanned_end_key = range_start_key;
                    bool is_last_scanned_key_str = false;
                    uint32_t actual_task_cnt = 0;
                    do
                    {
                        dispatch_func(range_start_key,
                                      range_end_key,
                                      nullptr,
                                      nullptr,
                                      last_scanned_end_key,
                                      is_last_scanned_key_str,
                                      0,
                                      actual_task_cnt);
                        {
                            std::lock_guard<std::mutex> task_lk(task_mux);
                            if (task_res == CcErrorCode::TX_NODE_NOT_LEADER)
                            {
                                all_task_started = true;
                                task_cv.notify_one();
                                return;
                            }
                        }
                    } while (*last_scanned_end_key < *range_end_key);

                    // Update the task status
                    {
                        std::lock_guard<std::mutex> task_lk(task_mux);
                        dispatched_task_count += (actual_task_cnt - 1);
                        all_task_started = true;
                        task_cv.notify_one();
                    }
                    return;
                }

                if (res_code != CcErrorCode::NO_ERROR)
                {
                    LOG(ERROR)
                        << "Finish this generate sk task of ng#" << range_owner
                        << " for partition id: " << partition_id
                        << " caused by error: " << CcErrorMessage(res_code);
                    std::unique_lock<std::mutex> lk(task_mux);
                    --unfinished_task_cnt;
                    task_res =
                        task_res == CcErrorCode::NO_ERROR ? res_code : task_res;
                    task_cv.notify_one();
                    return;
                }

                // check the terms
                auto &terms = sk_generator.NodeGroupTerms();
                std::unique_lock<std::mutex> lk(task_mux);
                for (size_t idx = 0; idx < terms.size(); ++idx)
                {
                    auto &term = terms.at(idx);
                    if (term < 0)
                    {
                        continue;
                    }

                    auto &ng_term = ng_terms.at(idx);
                    if (ng_term < 0)
                    {
                        ng_term = term;
                    }
                    else if (ng_term > 0 && ng_term != term)
                    {
                        LOG(ERROR) << "Generate sk from pk failed of ng#"
                                   << range_owner
                                   << " for partition id: " << partition_id
                                   << " caused by leader transferred.";
                        --unfinished_task_cnt;
                        task_res = task_res == CcErrorCode::NO_ERROR
                                       ? CcErrorCode::REQUESTED_NODE_NOT_LEADER
                                       : task_res;
                        task_cv.notify_one();
                        return;
                    }
                    else
                    {
                        assert(ng_term == term);
                    }
                }

                --unfinished_task_cnt;
                total_pk_items_count += scanned_pk_items_count;
                task_cv.notify_one();
            }));
    }
    else
    {
        // remote node
        std::shared_ptr<brpc::Channel> channel =
            Sharder::Instance().GetCcNodeServiceChannel(dest_node_id);
        if (channel == nullptr)
        {
            // Fail to establish the channel to the tx node.
            LOG(ERROR) << "Acquire GenerateSkFromPk failed to init the "
                          "channel of ng#"
                       << range_owner;
            std::unique_lock<std::mutex> lk(task_mux);
            task_res = task_res == CcErrorCode::NO_ERROR
                           ? CcErrorCode::ESTABLISH_NODE_CHANNEL_FAILED
                           : task_res;
            task_cv.notify_one();
            return;
        }

        remote::CcRpcService_Stub stub(channel.get());

        GenerateSkFromPkClosure *closure =
            new GenerateSkFromPkClosure(task_mux,
                                        task_cv,
                                        leader_terms_,
                                        unfinished_task_cnt,
                                        all_task_started,
                                        total_pk_items_count,
                                        dispatched_task_count,
                                        task_res,
                                        dispatch_func);
        closure->SetChannel(dest_node_id, channel);

        brpc::Controller *cntl_ptr = closure->Controller();
        cntl_ptr->set_timeout_ms(-1);
        auto req_ptr = closure->GenerateSkFromPkRequest();
        req_ptr->set_table_name_str(base_table_name.String());
        req_ptr->set_node_group_id(range_owner);
        req_ptr->set_tx_number(tx_number);
        req_ptr->set_tx_term(tx_term);
        req_ptr->set_scan_ts(scan_ts);
        req_ptr->set_partition_id(partition_id);
        assert(range_start_key != nullptr && range_end_key != nullptr);
        if (range_start_key->Type() == KeyType::Normal)
        {
            range_start_key->Serialize(*req_ptr->mutable_start_key());
        }
        if (range_end_key->Type() == KeyType::Normal)
        {
            range_end_key->Serialize(*req_ptr->mutable_end_key());
        }

        for (size_t idx = 0; idx < new_indexes_name_.size(); ++idx)
        {
            req_ptr->add_new_sk_name_str(new_indexes_name_.at(idx).String());
            req_ptr->add_new_sk_type(remote::ToRemoteType::ConvertTableType(
                new_indexes_name_.at(idx).Type()));
        }

        auto resp_ptr = closure->GenerateSkFromPkResponse();
        // Asynchronous mode
        stub.GenerateSkFromPk(cntl_ptr, req_ptr, resp_ptr, closure);
        DLOG(INFO) << "Acquire GenerateSkFromPk service for partition id: "
                   << partition_id
                   << " with start key: " << range_start_key->ToString()
                   << " and end key: " << range_end_key->ToString() << " of ng#"
                   << range_owner;
    }

    {
        std::lock_guard<std::mutex> task_lk(task_mux);
        ++unfinished_task_cnt;
    }
}

}  // namespace txservice
