#pragma once

#include <algorithm>
#include <map>
#include <memory>  // make_shared
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "../log_service/include/log_type.h"
#include "../log_service/proto/raft_log.pb.h"
#include "catalog_factory.h"
#include "catalog_key_record.h"
#include "cc_request.h"
#include "error_messages.h"  //CcErrorCode
#include "fault_inject.h"
#include "local_cc_shards.h"
#include "non_blocking_lock.h"
#include "sharder.h"
#include "template_cc_map.h"

namespace txservice
{
class CatalogCcMap : public TemplateCcMap<CatalogKey, CatalogRecord>
{
public:
    CatalogCcMap(const CatalogCcMap &rhs) = delete;
    ~CatalogCcMap() = default;

    /**
     * @brief Constructs a new catalog cc map object. The catalog cc map has no
     * schema, so the schema's timestamp is set to 1 (the beginning of history).
     *
     * @param shard
     */
    CatalogCcMap(CcShard *shard,
                 NodeGroupId cc_ng_id,
                 const TableName &table_name)
        : TemplateCcMap<CatalogKey, CatalogRecord>(
              shard, cc_ng_id, table_name, 1, nullptr, false)
    {
    }

    using TemplateCcMap::Execute;

    bool Execute(PostWriteAllCc &req) override
    {
        TX_TRACE_ACTION_WITH_CONTEXT(
            (txservice::CcMap *) this,
            &req,
            [&req]() -> std::string
            {
                return std::string("\"cc_map_type\":\"template_cc_map\"")
                    .append(",\"tx_number\":")
                    .append(std::to_string(req.Txn()))
                    .append(",\"term\":")
                    .append("0");
            });
        TX_TRACE_DUMP(&req);

        CODE_FAULT_INJECTOR("during_post_write_all", {
            std::string &action = FaultInject::Instance()
                                      .Entry("during_post_write_all")
                                      ->vctAction_.front();
            if ((req.CommitType() == PostWriteType::PrepareCommit &&
                 action == "prepare_commit_panic") ||
                (req.CommitType() == PostWriteType::PostCommit &&
                 req.CommitTs() == TransactionOperation::tx_op_failed_ts_ &&
                 action == "post_commit_panic"))
            {
                int retval;
                sigset_t new_mask;
                sigfillset(&new_mask);

                retval = kill(getpid(), SIGKILL);
                assert(retval == 0);
                retval = sigsuspend(&new_mask);
                fprintf(stderr,
                        "sigsuspend returned %d errno %d \n",
                        retval,
                        errno);
                assert(false); /* With full signal mask, we should never
                                  return here. */
            }
            else if ((req.CommitType() == PostWriteType::PrepareCommit &&
                      action == "prepare_commit_sleep") ||
                     (req.CommitType() == PostWriteType::PostCommit &&
                      req.CommitTs() ==
                          TransactionOperation::tx_op_failed_ts_ &&
                      action == "post_commit_sleep"))
            {
                LOG(INFO) << "sleep 10 seconds: ";
                sleep(10);
                // remove fault injection after sleep
                FaultInject::Instance().InjectFault("during_post_write_all",
                                                    "remove");
            }
        });

        int64_t ng_term = Sharder::Instance().LeaderTerm(req.NodeGroupId());
        CODE_FAULT_INJECTOR("term_CatalogCcMap_Execute_PostWriteAllCc", {
            LOG(INFO)
                << "FaultInject  term_CatalogCcMap_Execute_PostWriteAllCc";
            ng_term = -1;
            FaultInject::Instance().InjectFault(
                "term_CatalogCcMap_Execute_PostWriteAllCc", "remove");
        });
        if (ng_term < 0)
        {
            req.Result()->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            return true;
        }

        const CatalogKey *table_key = nullptr;
        if (req.Key() != nullptr)
        {
            table_key = static_cast<const CatalogKey *>(req.Key());
        }
        else
        {
            switch (*req.KeyStrType())
            {
            case KeyType::NegativeInf:
            case KeyType::PositiveInf:
                // For catalog, key type can't be Inf
                assert(false);
                break;
            case KeyType::Normal:
                const std::string *key_str = req.KeyStr();
                assert(key_str != nullptr);
                std::unique_ptr<CatalogKey> decoded_key =
                    std::make_unique<CatalogKey>();
                size_t offset = 0;
                decoded_key->Deserialize(key_str->data(), offset, KeySchema());
                table_key = decoded_key.get();
                req.SetDecodedKey(std::move(decoded_key));
                break;
            }
        }

        CcEntry<CatalogKey, CatalogRecord> *cce_ptr =
            TemplateCcMap<CatalogKey, CatalogRecord>::Find(*table_key).second;

        // Check whether cce key lock holder is the given tx of the
        // PostWriteAllCc before apply change.
        if (cce_ptr == nullptr || CheckCceKeyLock(cce_ptr, req) == false)
        {
            // When the catalog entry is null in the post-write-all
            // phase, it means that (1) the cc req is a resend request and
            // previous has successed, (2) the cc node group must have failed
            // over once, and (3) there is no catalog op in the log (so that the
            // recovered cc node group has no catalog entry). No catalog op in
            // the log means that this schema op fails before the prepare log
            // and this post-write-all request is to release the write
            // lock/intent. Given that the cc node group has failed once, the
            // previously acquired intent/lock has gone. There is no need to
            // proceed to release the intent/lock. The request is set to
            // finished.
            if (shard_->core_id_ == shard_->core_cnt_ - 1)
            {
                req.Result()->SetFinished();
                req.SetDecodedPayload(nullptr);
                return true;
            }
            else
            {
                req.ResetCcm();
                MoveRequest(&req, shard_->core_id_ + 1);
                return false;
            }
        }

        CatalogRecord *schema_rec = nullptr;
        CatalogEntry *catalog_entry = nullptr;

        // First setup the schema_rec that will replace the current catalog rec.
        switch (req.CommitType())
        {
        case PostWriteType::PrepareCommit:
        {
            if (req.CommitTs() == TransactionOperation::tx_op_failed_ts_)
            {
                // When the commit ts is 0, the request commits nothing and only
                // removes the write intents/locks acquired earlier.
                return TemplateCcMap::Execute(req);
            }

            if (shard_->core_id_ == 0)
            {
                // For prepare commit or commit, instantiates the dirty schema
                // instance in LocalCcShards when processing the request at the
                // first shard/core.

                if (req.Payload() != nullptr)
                {
                    // When the request comes from a tx in the same node, the
                    // request references a schema record in the tx's space.
                    schema_rec = static_cast<CatalogRecord *>(req.Payload());
                }
                else
                {
                    // When the request comes from a remote tx, the request
                    // contains a serialized representation of the catalog, but
                    // not a reference of a schema record. Allocates a schema
                    // record.
                    assert(req.PayloadStr() != nullptr);
                    std::unique_ptr<CatalogRecord> decoded_rec =
                        std::make_unique<CatalogRecord>();
                    if (req.OpType() != OperationType::DropTable)
                    {
                        size_t offset = 0;
                        decoded_rec->Deserialize(req.PayloadStr()->data(),
                                                 offset);
                    }
                    schema_rec = decoded_rec.get();
                    req.SetDecodedPayload(std::move(decoded_rec));
                }

                catalog_entry =
                    shard_->CreateDirtyCatalog(table_key->Name(),
                                               req.NodeGroupId(),
                                               schema_rec->DirtySchemaImage(),
                                               req.CommitTs());

                // For alter index, we need to initialize meta data for the
                // new indexes at prepare stage since the new indexes may
                // receive cc requests during the 2 phase commit DDL tx..
                if ((req.OpType() == OperationType::AddIndex ||
                     req.OpType() == OperationType::DropIndex))
                {
                    if (catalog_entry->schema_ == nullptr)
                    {
                        // For alter table, in some case, the current schema may
                        // not exists yet, so should create the current schema.
                        // For example, this node is the participant node of the
                        // alter table transaction, and does not execute any
                        // transaction about this table before this alter table
                        // tx since server start.
                        shard_->CreateCatalog(table_key->Name(),
                                              req.NodeGroupId(),
                                              schema_rec->SchemaImage(),
                                              schema_rec->SchemaTs());

#ifdef RANGE_PARTITION_ENABLED
                        // Initialize table ranges.
                        TableName base_range_table_name{
                            table_key->Name().StringView(),
                            TableType::RangePartition};
                        auto ranges = shard_->GetTableRangesForATable(
                            base_range_table_name, req.NodeGroupId());
                        if (ranges == nullptr)
                        {
                            shard_->FetchTableRanges(
                                base_range_table_name,
                                catalog_entry->schema_->GetKVCatalogInfo(),
                                &req,
                                req.NodeGroupId(),
                                ng_term);
                            return false;
                        }
#endif
                    }
                    // Bind statistics for the dirty schema.
                    catalog_entry->dirty_schema_->BindStatistics(
                        catalog_entry->schema_->StatisticsObject());
#ifdef RANGE_PARTITION_ENABLED
                    // Load ranges for the new added indexes. We cannot
                    // simply initialize it with empty range table since we
                    // might have pre-defined range table based on the data
                    // distribution offered by caller.
                    if (req.OpType() == OperationType::AddIndex)
                    {
                        std::vector<TableName> new_index_names =
                            catalog_entry->dirty_schema_->IndexNames();
                        std::vector<TableName> old_index_names =
                            catalog_entry->schema_->IndexNames();
                        bool found = false;
                        for (const TableName &new_index_name : new_index_names)
                        {
                            found = false;
                            for (const auto &old_index_name : old_index_names)
                            {
                                if (!new_index_name.String().compare(
                                        old_index_name.String()))
                                {
                                    found = true;
                                    break;
                                }
                            }
                            if (!found)
                            {
                                TableName index_range_name{
                                    new_index_name.StringView(),
                                    TableType::RangePartition};
                                auto ranges = shard_->GetTableRangesForATable(
                                    index_range_name, req.NodeGroupId());
                                if (ranges == nullptr)
                                {
                                    shard_->FetchTableRanges(
                                        index_range_name,
                                        catalog_entry->dirty_schema_
                                            ->GetKVCatalogInfo(),
                                        &req,
                                        req.NodeGroupId(),
                                        ng_term);
                                    return false;
                                }
                            }
                        }
                    }
#endif
                }

                schema_rec->Set(catalog_entry->schema_,
                                catalog_entry->dirty_schema_,
                                catalog_entry->Version());
            }
            else
            {
                assert(req.Payload() != nullptr);
                schema_rec = static_cast<CatalogRecord *>(req.Payload());
                catalog_entry =
                    shard_->GetCatalog(table_key->Name(), req.NodeGroupId());
            }

            break;
        }
        case PostWriteType::PostCommit:
        {
            catalog_entry =
                shard_->GetCatalog(table_key->Name(), req.NodeGroupId());
            if (catalog_entry == nullptr)
            {
                req.Result()->SetFinished();
                req.SetDecodedPayload(nullptr);
                return true;
            }

            if (req.CommitTs() == TransactionOperation::tx_op_failed_ts_)
            {
                // For add index op, we create new sk ccmap, table ranges and
                // table statistics for the new sk during prepare phase. If
                // flush kv failed, should clean these up. But, if the dirty
                // schema is nullptr, that is mean, this is the recovering
                // transaction, and there is no need to drop the new sk ccmap.
                if (req.OpType() == OperationType::AddIndex &&
                    catalog_entry->dirty_schema_ != nullptr)
                {
                    std::vector<TableName> new_index_names =
                        catalog_entry->dirty_schema_->IndexNames();
                    std::vector<TableName> old_index_names =
                        catalog_entry->schema_->IndexNames();
                    for (const TableName &new_index_name : new_index_names)
                    {
                        if (std::find(old_index_names.begin(),
                                      old_index_names.end(),
                                      new_index_name) == old_index_names.end())
                        {
                            shard_->DropCcm(new_index_name, req.NodeGroupId());
#ifdef RANGE_PARTITION_ENABLED
                            // Clean up table ranges for new sk.
                            const TableName index_range_name{
                                new_index_name.StringView(),
                                TableType::RangePartition};
                            shard_->DropCcm(index_range_name,
                                            req.NodeGroupId());
#endif
                            if (shard_->core_id_ == shard_->core_cnt_ - 1)
                            {
#ifdef RANGE_PARTITION_ENABLED
                                shard_->CleanTableRange(index_range_name,
                                                        req.NodeGroupId());
#endif
                                Statistics *statistics =
                                    catalog_entry->dirty_schema_
                                        ->StatisticsObject()
                                        .get();
                                statistics->DropIndex(new_index_name);
                            }
                        }
                    }
                }

                // Flush kv fails, need to clear dirty CatalogEntry
                catalog_entry->RejectDirtySchema();

                if (cce_ptr->payload_)
                {
                    cce_ptr->payload_->ClearDirtySchema();
                    cce_ptr->payload_->SetDirtySchemaImage("");
                }
                return TemplateCcMap::Execute(req);
            }

            if (shard_->core_id_ == 0)
            {
                // For post commit, retrieves the current and dirty schema pair
                // from the current shard.
                if (req.Payload() != nullptr)
                {
                    // When the request comes from a tx in the same node, the
                    // request references a schema record in the tx's space.
                    schema_rec = static_cast<CatalogRecord *>(req.Payload());
                }
                else
                {
                    // When the request comes from a remote tx, allocates a
                    // schema record, which acts as a container referencing the
                    // current and dirty schema pair.
                    std::unique_ptr<CatalogRecord> empty_rec =
                        std::make_unique<CatalogRecord>();
                    schema_rec = empty_rec.get();
                    req.SetDecodedPayload(std::move(empty_rec));
                }

                if (req.OpType() == OperationType::CreateTable)
                {
                    std::vector<InitRangeEntry> range_init_vec;
                    std::string_view table_name_view =
                        table_key->Name().StringView();
                    int init_partition_id = 0;
                    if (table_name_view != "./mysql/sequences")
                    {
                        size_t tbl_name_hash =
                            std::hash<std::string_view>()(table_name_view);
                        init_partition_id = tbl_name_hash & 0xFFF;
                    }

                    // Use nullptr to represent negative infinity key here.
                    range_init_vec.emplace_back(
                        nullptr, init_partition_id, req.CommitTs());

                    TableName range_table_name(table_name_view,
                                               TableType::RangePartition);
                    shard_->InitTableRanges(range_table_name,
                                            range_init_vec,
                                            req.NodeGroupId(),
                                            true);

                    std::vector<TableName> index_names =
                        catalog_entry->dirty_schema_->IndexNames();
                    for (const TableName &index_name : index_names)
                    {
                        // Create range table for each sk index
                        TableName index_range_table_name{
                            index_name.StringView(), TableType::RangePartition};

                        size_t tbl_name_hash = std::hash<std::string_view>()(
                            index_name.StringView());
                        init_partition_id = tbl_name_hash & 0xFFF;
                        range_init_vec.clear();
                        range_init_vec.emplace_back(
                            nullptr, init_partition_id, req.CommitTs());
                        shard_->InitTableRanges(index_range_table_name,
                                                range_init_vec,
                                                req.NodeGroupId(),
                                                true);
                    }
                    shard_->InitTableStatistics(
                        catalog_entry->dirty_schema_.get(), cc_ng_id_);
                }

                schema_rec->Set(catalog_entry->dirty_schema_,
                                nullptr,
                                catalog_entry->DirtyVersion());
            }
            else
            {
                assert(req.Payload() != nullptr);
                schema_rec = static_cast<CatalogRecord *>(req.Payload());
            }
            break;
        }
        case PostWriteType::Commit:
            // Schema operations always employ multi-stage commits.
            assert(true);
            break;
        default:
            break;
        }

        assert(catalog_entry != nullptr);

        // When the request commits the schema operation, modifies the cc
        // map(s) at this shard.
        const TableSchema *old_schema = catalog_entry->schema_.get();
        const TableSchema *new_schema = catalog_entry->dirty_schema_.get();
        if (req.CommitType() == PostWriteType::PostCommit &&
            catalog_entry->DirtyVersion() > 0)
        {
            if (req.OpType() == OperationType::DropTable)
            {
                // A remote tx is allowed to acquire write intents/locks and
                // drop a table, even if the table's schema has not been
                // initialized at this node. The earlier acquiring-write-intent
                // request creates a schema cc entry in the catalog cc map and a
                // node-level schema view. The version timestamp of the schema
                // is 0, if the schema is uninitialized (null). Or, the current
                // schema must not be null.
                assert(catalog_entry->Version() == 0 || old_schema != nullptr);

                // This is a DROP TABLE statement. Drops the cc maps
                // associated with the table in the final commit step.
                shard_->DropCcm(table_key->Name(), req.NodeGroupId());

#ifdef RANGE_PARTITION_ENABLED
                // Drop range table if exist
                TableName range_table_name{table_key->Name().StringView(),
                                           TableType::RangePartition};
                shard_->DropCcm(range_table_name, req.NodeGroupId());
#endif
                if (old_schema != nullptr)
                {
                    std::vector<TableName> index_names =
                        old_schema->IndexNames();
                    for (const TableName &index_name : index_names)
                    {
                        shard_->DropCcm(index_name, req.NodeGroupId());
#ifdef RANGE_PARTITION_ENABLED
                        // Drop range table if exist
                        TableName index_range_table_name{
                            index_name.StringView(), TableType::RangePartition};
                        shard_->DropCcm(index_range_table_name,
                                        req.NodeGroupId());
#endif
                    }
                }
            }
            else if (req.OpType() == OperationType::CreateTable)
            {
                assert(catalog_entry->DirtyVersion() > 0 &&
                       new_schema != nullptr);

                // This is a CREATE TABLE statement. Creates the cc maps
                // associated with the table in the final commit step.
                shard_->CreateOrUpdatePkCcMap(table_key->Name(),
                                              new_schema,
                                              req.NodeGroupId(),
                                              catalog_entry->DirtyVersion(),
                                              true,
                                              true);

                std::vector<TableName> index_names = new_schema->IndexNames();
                for (const TableName &index_name : index_names)
                {
                    shard_->CreateOrUpdateSkCcMap(
                        index_name,
                        new_schema,
                        req.NodeGroupId(),
                        catalog_entry->DirtyVersion());
                }
            }
            // Alter Table
            else
            {
                assert(old_schema != nullptr && new_schema != nullptr);
                // Update pk cc map using new schema.
                shard_->CreateOrUpdatePkCcMap(table_key->Name(),
                                              new_schema,
                                              req.NodeGroupId(),
                                              catalog_entry->DirtyVersion(),
                                              false);

#ifdef RANGE_PARTITION_ENABLED
                // Update pk range table if exist.
                TableName base_range_table_name{table_key->Name().StringView(),
                                                TableType::RangePartition};
                auto ranges = shard_->GetTableRangesForATable(
                    base_range_table_name, req.NodeGroupId());
                if (ranges != nullptr)
                {
                    shard_->CreateOrUpdateRangeCcMap(
                        base_range_table_name,
                        new_schema,
                        req.NodeGroupId(),
                        catalog_entry->DirtyVersion(),
                        false);
                }
#endif

                if (req.OpType() == OperationType::AddIndex ||
                    req.OpType() == OperationType::DropIndex)
                {
                    std::vector<TableName> new_index_names =
                        new_schema->IndexNames();
                    std::vector<TableName> old_index_names =
                        old_schema->IndexNames();

                    for (const TableName &old_index_name : old_index_names)
                    {
                        if (std::find(new_index_names.begin(),
                                      new_index_names.end(),
                                      old_index_name) != new_index_names.end())
                        {
                            // Update current sk cc map using new schema.
                            shard_->CreateOrUpdateSkCcMap(
                                old_index_name,
                                new_schema,
                                req.NodeGroupId(),
                                catalog_entry->DirtyVersion(),
                                false);
#ifdef RANGE_PARTITION_ENABLED
                            // Update current sk range table if exist.
                            TableName index_range_table_name{
                                old_index_name.StringView(),
                                TableType::RangePartition};
                            auto ranges = shard_->GetTableRangesForATable(
                                index_range_table_name, req.NodeGroupId());
                            if (ranges != nullptr)
                            {
                                shard_->CreateOrUpdateRangeCcMap(
                                    index_range_table_name,
                                    new_schema,
                                    req.NodeGroupId(),
                                    catalog_entry->DirtyVersion(),
                                    false);
                            }
#endif
                        }
                        else
                        {
                            assert(req.OpType() == OperationType::DropIndex);
                            // Remove sk cc map for dropped index.
                            shard_->DropCcm(old_index_name, req.NodeGroupId());
// range table operation.
#ifdef RANGE_PARTITION_ENABLED
                            // Drop range table if exist
                            TableName old_index_range_table_name{
                                old_index_name.StringView(),
                                TableType::RangePartition};
                            shard_->DropCcm(old_index_range_table_name,
                                            req.NodeGroupId());
#endif
                        }
                    }
                }  // End of alter table index
            }
        }
        else if (req.CommitType() == PostWriteType::PrepareCommit &&
                 catalog_entry->DirtyVersion() > 0)
        {
            // Prepare commit. For certain schema operations, e.g., create
            // secondary index, the cc map is modified in the prepare commit
            // step.
            // ALTER TABLE statement (include CREATE/DROP INDEX)
            if (req.OpType() == OperationType::AddIndex)
            {
                std::vector<TableName> new_index_names =
                    new_schema->IndexNames();
                std::vector<TableName> old_index_names =
                    old_schema->IndexNames();
                bool found = false;
                for (const TableName &new_index_name : new_index_names)
                {
                    found = false;
                    for (const auto &old_index_name : old_index_names)
                    {
                        if (!new_index_name.String().compare(
                                old_index_name.String()))
                        {
                            found = true;
                            break;
                        }
                    }
                    if (!found)
                    {
                        // In this step, just create cc map for new sk.
                        // We will update current sk ccmap in PostCommit.
                        shard_->CreateOrUpdateSkCcMap(
                            new_index_name,
                            new_schema,
                            req.NodeGroupId(),
                            catalog_entry->DirtyVersion());

                        // New sk range cc map should use the dirty schema
                        const TableName new_index_range_name{
                            new_index_name.StringView(),
                            TableType::RangePartition};
                        shard_->CreateOrUpdateRangeCcMap(
                            new_index_range_name,
                            new_schema,
                            req.NodeGroupId(),
                            catalog_entry->DirtyVersion());
                    }
                }
            }
        }

        if (req.CommitType() == PostWriteType::PostCommit &&
            shard_->core_id_ == shard_->core_cnt_ - 1 &&
            catalog_entry->DirtyVersion() > 0)
        {
            // If this is a drop table req, drop the range table also
            // Drop table range before drop catalog
            if (req.OpType() == OperationType::DropTable)
            {
                shard_->CleanTableStatistics(table_key->Name());
#ifdef RANGE_PARTITION_ENABLED
                TableName range_table_name{table_key->Name().StringView(),
                                           TableType::RangePartition};
                shard_->CleanTableRange(range_table_name, req.NodeGroupId());
                if (old_schema != nullptr)
                {
                    std::vector<TableName> index_names =
                        old_schema->IndexNames();
                    for (const TableName &index_name : index_names)
                    {
                        // Drop range table if exist
                        TableName index_range_table_name{
                            index_name.StringView(), TableType::RangePartition};
                        shard_->CleanTableRange(index_range_table_name,
                                                req.NodeGroupId());
                    }
                }
#endif
            }

            else if (req.OpType() == OperationType::AddIndex ||
                     req.OpType() == OperationType::DropIndex)
            {
                std::vector<TableName> new_index_names =
                    new_schema->IndexNames();
                std::vector<TableName> old_index_names =
                    old_schema->IndexNames();
                for (const TableName &old_index_name : old_index_names)
                {
                    // Drop old index range table if not exist any more
                    if (std::find(new_index_names.begin(),
                                  new_index_names.end(),
                                  old_index_name) == new_index_names.end())
                    {
#ifdef RANGE_PARTITION_ENABLED
                        TableName old_index_range_table_name{
                            old_index_name.StringView(),
                            TableType::RangePartition};
                        shard_->CleanTableRange(old_index_range_table_name,
                                                req.NodeGroupId());
#endif
                        Statistics *statistics =
                            old_schema->StatisticsObject().get();
                        statistics->DropIndex(old_index_name);
                    }
                }
            }
            shard_->CommitDirtyCatalog(table_key->Name(), req.NodeGroupId());
        }

        return TemplateCcMap::Execute(req);
    }

    bool Execute(ReadCc &req) override
    {
        TX_TRACE_ACTION_WITH_CONTEXT(
            (txservice::CcMap *) this,
            &req,
            [&req]() -> std::string
            {
                return std::string("\"cc_map_type\":\"template_cc_map\"")
                    .append(",\"tx_number\":")
                    .append(std::to_string(req.Txn()))
                    .append(",\"term\":")
                    .append(std::to_string(req.TxTerm()));
            });
        TX_TRACE_DUMP(&req);

        assert(req.IsLocal());

        uint32_t ng_id = req.NodeGroupId();
        int64_t ng_term = Sharder::Instance().LeaderTerm(ng_id);
        if (req.IsInRecovering())
        {
            ng_term = ng_term > 0
                          ? ng_term
                          : Sharder::Instance().CandidateLeaderTerm(ng_id);
        }

        CODE_FAULT_INJECTOR("term_CatalogCcMap_Execute_ReadCc", {
            LOG(INFO) << "FaultInject  term_CatalogCcMap_Execute_ReadCc";
            ng_term = -1;
            FaultInject::Instance().InjectFault(
                "term_CatalogCcMap_Execute_ReadCc", "remove");
        });

        if (ng_term < 0)
        {
            req.Result()->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            return true;
        }

        const CatalogKey *table_key =
            static_cast<const CatalogKey *>(req.Key());
        Iterator it = FindEmplace(*table_key);
        CcEntry<CatalogKey, CatalogRecord> *cce = it->second;
        if (cce->payload_status_ == RecordStatus::Unknown)
        {
            const CatalogEntry *catalog_entry =
                shard_->GetCatalog(table_key->Name(), req.NodeGroupId());

            // If the read toward the catalog cc entry acquires the read
            // lock but the cc entry does not contain the value, checks if
            // the catalog has been constructed at this node. If so, turns
            // this request into a read-outside request that installs the
            // value in the cc entry.
            if (catalog_entry != nullptr)
            {
                if (catalog_entry->schema_ != nullptr)
                {
                    {
#ifndef ON_KEY_OBJECT
                        // Initialize table statistics before create ccmap.
                        if (!shard_->LoadRangesAndStatisticsNx(
                                catalog_entry->schema_.get(),
                                req.NodeGroupId(),
                                ng_term,
                                &req))
                        {
                            return false;
                        }
#endif
                    }

                    // upload catalog record
                    cce->payload_ = std::make_unique<CatalogRecord>();
                    cce->payload_->Set(catalog_entry->schema_,
                                       catalog_entry->dirty_schema_,
                                       catalog_entry->Version());
                    cce->payload_status_ = RecordStatus::Normal;
                    cce->commit_ts_ = catalog_entry->Version();
                }
                else
                {
                    cce->payload_status_ = RecordStatus::Deleted;
                    cce->commit_ts_ = catalog_entry->Version();
                }
            }
            else
            {
                shard_->FetchCatalog(
                    table_key->Name(), req.NodeGroupId(), ng_term, &req);
                return false;
            }
        }

        return TemplateCcMap::Execute(req);
    }

    bool Execute(ReplayLogCc &req) override
    {
        TX_TRACE_ACTION_WITH_CONTEXT(
            (txservice::CcMap *) this,
            &req,
            [&req]() -> std::string
            {
                return std::string("\"cc_map_type\":\"template_cc_map\"")
                    .append(",\"tx_number\":")
                    .append(std::to_string(req.Txn()))
                    .append(",\"term\":")
                    .append("0");
            });
        TX_TRACE_DUMP(&req);
        int64_t ng_term =
            Sharder::Instance().CandidateLeaderTerm(req.NodeGroupId());
        if (ng_term < 0)
        {
            LOG(INFO) << "ReplayLogCc, node_group(#" << req.NodeGroupId()
                      << ") term < 0, tx:" << req.Txn();
            req.Result()->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            return false;
        }

        ::txlog::SchemaOpMessage schema_op_msg;
        const std::string_view &content = req.LogContentView();
        schema_op_msg.ParseFromArray(content.data(), content.length());

        const CatalogEntry *catalog_entry = nullptr;
        bool is_coordinator = false;
        uint32_t tx_node_id = (req.Txn() >> 32L) >> 10;

        if (tx_node_id == req.NodeGroupId())
        {
            if (Sharder::Instance().CandidateLeaderTerm(tx_node_id) >= 0)
            {
                is_coordinator = true;
            }
        }

        // Need to parse the string if not include table type in protobuf
        TableType table_type = ::txlog::ToLocalType::ConvertCcTableType(
            schema_op_msg.table_type());
        std::string_view table_name_sv{schema_op_msg.table_name_str()};
        TableName table_name{table_name_sv, table_type};

        // 1. Replay the table catalog and table schema.
        // The first shard is in charge of creating catalog_entry.
        if (shard_->core_id_ == 0)
        {
            if (schema_op_msg.stage() ==
                    ::txlog::SchemaOpMessage_Stage::
                        SchemaOpMessage_Stage_PrepareSchema ||
                (schema_op_msg.stage() ==
                     ::txlog::SchemaOpMessage_Stage::
                         SchemaOpMessage_Stage_CommitSchema &&
                 is_coordinator) ||
                schema_op_msg.stage() ==
                    ::txlog::SchemaOpMessage_Stage::
                        SchemaOpMessage_Stage_PrepareIndexTable)
            {
                // If we are coordinator, we need to recover to the state
                // right after commit log is flushed since the
                // upsert_kv_table_op_ might need both old table schema and
                // new table schema.
                // If we are recovering from prepare log, we need to restore to
                // the state right before commit log is flushed, so both current
                // and dirty schema are needed.
                auto [success, new_catalog_entry] = shard_->CreateReplayCatalog(
                    table_name,
                    req.NodeGroupId(),
                    schema_op_msg.old_catalog_blob(),
                    schema_op_msg.new_catalog_blob(),
                    schema_op_msg.catalog_ts(),
                    req.CommitTs());
                if (!success)
                {
                    // create fail, the catalog to be created is out of date
                    LOG(INFO)
                        << "create catalog fails, table name: " << table_name_sv
                        << ", catalog entry of the same or higher "
                           "version exists, stop replaying this schema op";
                    req.SetFinish();
                    return false;
                }
                catalog_entry = new_catalog_entry;
            }
            else
            {
                /*
                If we are recovering as participant from commit stage,
                we don't need to do kv_upsert_table_op_ so we can directly
                recover to the state before commit log is cleaned, that is:

                1. if dirty_schema_commit_ts>0, dirty schema is commited as
                current schema and write lock has been released.

                2. if dirty_schema_commit_ts=0, old schema is restored as
                current schema and write lock has been released.

                P.S. If it is create table statement and
                dirty_schema_commit_ts=0, the restored CatalogEntry has schema_
                set to nullptr and schema_version_ set to 1.
                */

                uint64_t dirty_schema_commit_ts = req.CommitTs();
                uint64_t old_schema_commit_ts = schema_op_msg.catalog_ts();

                auto [success, new_catalog_entry] = shard_->CreateCatalog(
                    table_name,
                    req.NodeGroupId(),
                    dirty_schema_commit_ts > 0
                        ? schema_op_msg.new_catalog_blob()
                        : schema_op_msg.old_catalog_blob(),
                    dirty_schema_commit_ts > 0 ? dirty_schema_commit_ts
                                               : old_schema_commit_ts);

                assert(new_catalog_entry != nullptr);
                if (!success)
                {
                    // create fail, the catalog to be created is out of date
                    LOG(INFO)
                        << "create catalog entry fails, table name: "
                        << table_name_sv
                        << ", catalog entry of the same or higher version "
                           "exists, stop replaying this schema op";
                    req.SetFinish();
                    return false;
                }
                catalog_entry = new_catalog_entry;
            }

            if (catalog_entry->schema_)
            {
                if (!shard_->LoadRangesAndStatisticsNx(
                        catalog_entry->schema_.get(),
                        req.NodeGroupId(),
                        ng_term,
                        &req))
                {
                    return false;
                }

                // Load range and stats for the new added indexes.
                if (catalog_entry->dirty_schema_)
                {
                    std::vector<TableName> new_index_names =
                        catalog_entry->dirty_schema_->IndexNames();
                    std::vector<TableName> old_index_names =
                        catalog_entry->schema_->IndexNames();
                    bool found = false;
                    for (const TableName &new_index_name : new_index_names)
                    {
                        found = false;
                        for (const auto &old_index_name : old_index_names)
                        {
                            if (!new_index_name.String().compare(
                                    old_index_name.String()))
                            {
                                found = true;
                                break;
                            }
                        }
                        if (!found)
                        {
                            TableName index_range_name{
                                new_index_name.StringView(),
                                TableType::RangePartition};
                            auto ranges = shard_->GetTableRangesForATable(
                                index_range_name, req.NodeGroupId());
                            if (ranges == nullptr)
                            {
                                shard_->FetchTableRanges(
                                    index_range_name,
                                    catalog_entry->dirty_schema_
                                        ->GetKVCatalogInfo(),
                                    &req,
                                    req.NodeGroupId(),
                                    ng_term);
                                return false;
                            }
                        }
                    }
                }
            }
        }
        else
        {
            // other cores
            catalog_entry = shard_->GetCatalog(table_name, req.NodeGroupId());
            assert(catalog_entry != nullptr);
        }

        // 2. Replay the table ccmap.
        if (schema_op_msg.stage() == ::txlog::SchemaOpMessage_Stage::
                                         SchemaOpMessage_Stage_PrepareSchema ||
            schema_op_msg.stage() ==
                ::txlog::SchemaOpMessage_Stage::
                    SchemaOpMessage_Stage_PrepareIndexTable ||
            (schema_op_msg.stage() == ::txlog::SchemaOpMessage_Stage::
                                          SchemaOpMessage_Stage_CommitSchema &&
             is_coordinator))
        {
            const TableSchema *old_schema = catalog_entry->schema_.get();
            const TableSchema *new_schema = catalog_entry->dirty_schema_.get();
            if (old_schema != nullptr && new_schema != nullptr)
            {
                // Alter table index operation.
                // Pk table ccmap using old schema.
                shard_->CreateOrUpdatePkCcMap(table_name,
                                              old_schema,
                                              req.NodeGroupId(),
                                              catalog_entry->Version());

                // Old sk table ccmap using old schema.
                std::vector<TableName> old_index_names =
                    old_schema->IndexNames();
                for (const auto &old_index_name : old_index_names)
                {
                    shard_->CreateOrUpdateSkCcMap(old_index_name,
                                                  old_schema,
                                                  req.NodeGroupId(),
                                                  catalog_entry->Version());
                }

                // New sk table ccmap using new schema
                std::vector<TableName> new_index_names =
                    new_schema->IndexNames();
                for (const auto &new_index_name : new_index_names)
                {
                    if (std::find(old_index_names.cbegin(),
                                  old_index_names.cend(),
                                  new_index_name) == old_index_names.cend())
                    {
                        shard_->CreateOrUpdateSkCcMap(
                            new_index_name,
                            new_schema,
                            req.NodeGroupId(),
                            catalog_entry->DirtyVersion());

                        // New sk range cc maps should use the dirty schema
                        const TableName new_index_range_name{
                            new_index_name.StringView(),
                            TableType::RangePartition};
                        shard_->CreateOrUpdateRangeCcMap(
                            new_index_range_name,
                            new_schema,
                            req.NodeGroupId(),
                            catalog_entry->DirtyVersion());
                    }
                }
            }
        }

        CatalogKey table_key(table_name);
        Iterator it = FindEmplace(table_key);
        CcEntry<CatalogKey, CatalogRecord> *cce = it->second;

        if (cce == nullptr)
        {
            shard_->Enqueue(shard_->LocalCoreId(), &req);
            return false;
        }

        // 3. Replay the table write intent/lock.
        LockType lock_type = LockType::NoLock;
        TableName base_table_name(table_name.GetBaseTableNameSV(),
                                  TableType::Primary);
        OperationType op_type =
            static_cast<OperationType>(schema_op_msg.table_op().op_type());
        switch (schema_op_msg.stage())
        {
        case ::txlog::SchemaOpMessage_Stage::
            SchemaOpMessage_Stage_PrepareSchema:
        {
            // If the prepare log has been flushed, the recovered cc ng leader
            // replays all steps between the prepare log and the commit log,
            // including the write lock on the schema. The recovered write lock
            // is special in that the holding tx is not associated with the tx's
            // term (always set to 0). This is because after the prepare log,
            // the write lock on the schema and the coordinating tx are
            // guaranteed to be recovered upon failures. The tx's term is not
            // necessary here to mark whether or not if the coordinating tx has
            // failed or not.
            if (req.RangeSplitting(base_table_name) ||
                op_type == OperationType::AddIndex)
            {
                // If range splitting is also happening on this table, which
                // must have acquired a read lock on the catalog entry, that
                // means we only need to recover a write intent.
                // For add index operation, between the prepare log and the
                // prepare index table log, the recovered cc ng leader should
                // recover the write intent on the schema.
                lock_type = LockType::WriteIntent;
            }
            else
            {
                lock_type = LockType::WriteLock;
            }
            break;
        }
        case ::txlog::SchemaOpMessage_Stage::
            SchemaOpMessage_Stage_PrepareIndexTable:
        {
            lock_type = LockType::WriteLock;
            break;
        }
        case ::txlog::SchemaOpMessage_Stage::SchemaOpMessage_Stage_CommitSchema:
        {
            if (is_coordinator)
            {
                // When coordinator is recovering from commit log, we need to
                // restore the state right after commit log is flushed, so we
                // need to acquire write lock as well.
                lock_type = req.RangeSplitting(base_table_name)
                                ? LockType::WriteIntent
                                : LockType::WriteLock;
            }
            break;
        }
        default:
            break;
        }

        if (lock_type == LockType::WriteIntent)
        {
            auto lock_pair = AcquireCceKeyLock(cce,
                                               cce->payload_status_,
                                               &req,
                                               req.NodeGroupId(),
                                               ng_term,
                                               0,
                                               CcOperation::ReadForWrite,
                                               IsolationLevel::RepeatableRead,
                                               CcProtocol::OCC,
                                               0,
                                               false);
            assert(lock_pair.first == LockType::WriteIntent &&
                   lock_pair.second == CcErrorCode::NO_ERROR);
        }
        else if (lock_type == LockType::WriteLock)
        {
            auto lock_pair = AcquireCceKeyLock(cce,
                                               cce->payload_status_,
                                               &req,
                                               req.NodeGroupId(),
                                               ng_term,
                                               0,
                                               CcOperation::Write,
                                               IsolationLevel::RepeatableRead,
                                               CcProtocol::Locking,
                                               0,
                                               false);
            // When a cc node recovers, no one should be holding read locks.
            // So, the acquire operation should always succeed.
            assert(lock_pair.first == LockType::WriteLock &&
                   lock_pair.second == CcErrorCode::NO_ERROR);
        }

        if (cce->payload_ == nullptr)
        {
            cce->payload_ = std::make_unique<CatalogRecord>();
        }
        cce->payload_->Set(catalog_entry->schema_,
                           catalog_entry->dirty_schema_,
                           catalog_entry->Version());

        if (shard_->core_id_ < shard_->core_cnt_ - 1)
        {
            req.ResetCcm();
            MoveRequest(&req, shard_->core_id_ + 1);
        }
        else
        {
            uint32_t tx_node_id = (req.Txn() >> 32L) >> 10;
            int64_t tx_candidate_term =
                Sharder::Instance().CandidateLeaderTerm(tx_node_id);

            if (tx_node_id == req.NodeGroupId() && tx_candidate_term >= 0)
            {
                // If the coordinating tx is bound to the recoverying cc
                // node, resumes the tx. This will spawn
                // a worker thread that will restore the transaction. It will
                // call SetFinish() after acqruing all needed locks.
                shard_->local_shards_.CreateSchemaRecoveryTx(
                    req, schema_op_msg, tx_candidate_term);
            }
            else
            {
                req.SetFinish();
            }
        }

        return false;
    }

    TableType Type() const override
    {
        return TableType::Catalog;
    }
};
}  // namespace txservice
