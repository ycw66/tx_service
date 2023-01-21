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
                sleep(10);
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

        CatalogRecord *schema_rec = nullptr;
        CatalogEntry *catalog_entry = nullptr;

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
                                               schema_rec->StatisticsBinary(),
                                               req.CommitTs());

                schema_rec->Set(catalog_entry->schema_,
                                catalog_entry->dirty_schema_.get(),
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
            assert(catalog_entry != nullptr);

            if (req.CommitTs() == TransactionOperation::tx_op_failed_ts_)
            {
                // Flush kv fails, need to clear dirty CatalogEntry and dirty
                // CatalogRecord.
                catalog_entry->RejectDirtySchema();

                CcEntry<CatalogKey, CatalogRecord> *cce =
                    TemplateCcMap<CatalogKey, CatalogRecord>::Find(*table_key)
                        .second;
                assert(cce != nullptr);
                cce->payload_->ClearDirtySchema();
                cce->payload_->SetDirtySchemaImage("");

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
            if (new_schema == nullptr)
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
            else if (old_schema == nullptr)
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
                            auto ranges = shard_->GetTableRangesForATable(
                                old_index_name, req.NodeGroupId());
                            if (ranges != nullptr)
                            {
                                shard_->CreateOrUpdateRangeCcMap(
                                    old_index_name,
                                    new_schema,
                                    req.NodeGroupId(),
                                    catalog_entry->DirtyVersion());
                            }
#endif
                        }
                        else
                        {
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
                    // for range table, upate tableschema.
                }
            }
        }
        else if (req.CommitType() == PostWriteType::PrepareCommit &&
                 catalog_entry->DirtyVersion() > 0)
        {
            // Prepare commit. For certain schema operations, e.g., create
            // secondary index, the cc map is modified in the prepare commit
            // step.
            // ALTER TABLE statement (include CREATE/DROP INDEX)
            if (new_schema != nullptr && old_schema != nullptr &&
                req.OpType() == OperationType::AddIndex)
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
#ifdef RANGE_PARTITION_ENABLED
            if (new_schema == nullptr)
            {
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
            }
            else if (old_schema == nullptr && new_schema != nullptr)
            {
                std::vector<InitRangeEntry> range_init_vec;
                std::string_view table_name_view =
                    table_key->Name().StringView();
                int init_partition_id = 0;
                if (table_name_view != "./mysql/sequences")
                {
                    size_t tbl_name_hash =
                        std::hash<std::string_view>()(table_name_view);
                    init_partition_id = tbl_name_hash & 0x3FF;
                }

                // Use nullptr to represent negative infinity key here.
                range_init_vec.emplace_back(
                    nullptr, init_partition_id, req.CommitTs());

                TableName range_table_name(table_name_view,
                                           TableType::RangePartition);
                shard_->InitTableRanges(
                    range_table_name, range_init_vec, req.NodeGroupId());

                std::vector<TableName> index_names = new_schema->IndexNames();
                for (const TableName &index_name : index_names)
                {
                    // Drop range table if exist
                    TableName index_range_table_name{index_name.StringView(),
                                                     TableType::RangePartition};

                    size_t tbl_name_hash =
                        std::hash<std::string_view>()(index_name.StringView());
                    init_partition_id = tbl_name_hash & 0x3FF;
                    range_init_vec.clear();
                    range_init_vec.emplace_back(
                        nullptr, init_partition_id, req.CommitTs());
                    shard_->InitTableRanges(index_range_table_name,
                                            range_init_vec,
                                            req.NodeGroupId());
                }
            }
            else if (old_schema != nullptr && new_schema != nullptr)
            {
                if (req.OpType() == OperationType::AddIndex ||
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
                            TableName old_index_range_table_name{
                                old_index_name.StringView(),
                                TableType::RangePartition};
                            shard_->CleanTableRange(old_index_range_table_name,
                                                    req.NodeGroupId());
                        }
                    }
                }
            }
#endif
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
                    shard_->CreateOrUpdatePkCcMap(table_key->Name(),
                                                  catalog_entry->schema_.get(),
                                                  req.NodeGroupId(),
                                                  catalog_entry->Version());

                    std::vector<TableName> index_names =
                        catalog_entry->schema_->IndexNames();
                    for (const TableName &index_name : index_names)
                    {
                        shard_->CreateOrUpdateSkCcMap(
                            index_name,
                            catalog_entry->schema_.get(),
                            req.NodeGroupId(),
                            catalog_entry->Version());
                    }

                    // upload catalog record
                    cce->payload_ = std::make_unique<CatalogRecord>();
                    cce->payload_->Set(catalog_entry->schema_,
                                       catalog_entry->dirty_schema_.get(),
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
                    table_key->Name(), req.NodeGroupId(), &req);
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

        // The first shard is in charge of creating catalog_entry.
        if (shard_->core_id_ == 0)
        {
            if (schema_op_msg.stage() ==
                    ::txlog::SchemaOpMessage_Stage::
                        SchemaOpMessage_Stage_PrepareSchema ||
                (schema_op_msg.stage() ==
                     ::txlog::SchemaOpMessage_Stage::
                         SchemaOpMessage_Stage_CommitSchema &&
                 is_coordinator))
            {
                // If we are coordinator, we need to recover to the state
                // right after commit log is flushed since the
                // upsert_kv_table_op_ might need both old table schema and
                // new table schema.
                // If we are recovering from prepare log, we also need to
                // restore to the state right before commit log is flushed, so
                // both current and dirty schema are needed.
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
                // If we are recovering as participant from commit stage,
                // we don't  need to do kv_upsert_table_op_ so we can directly
                // recover to the state before commit log is cleaned, that is
                // after dirty schema is commited as current schema and write
                // lock has been released.
                uint64_t commit_ts = req.CommitTs();
                auto [success, new_catalog_entry] = shard_->CreateCatalog(
                    table_name,
                    req.NodeGroupId(),
                    commit_ts > 0 ? schema_op_msg.new_catalog_blob()
                                  : schema_op_msg.old_catalog_blob(),
                    Statistics::EMPTY_STATISTICS_BINARY,
                    commit_ts > 0 ? commit_ts : schema_op_msg.catalog_ts());

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

                const TableSchema *committed_schema =
                    catalog_entry->schema_.get();
                if (committed_schema != nullptr)
                {
                    shard_->CreateOrUpdatePkCcMap(table_name,
                                                  committed_schema,
                                                  req.NodeGroupId(),
                                                  catalog_entry->Version());

                    std::vector<TableName> index_names =
                        committed_schema->IndexNames();
                    for (const TableName &index_name : index_names)
                    {
                        shard_->CreateOrUpdateSkCcMap(index_name,
                                                      committed_schema,
                                                      req.NodeGroupId(),
                                                      catalog_entry->Version());
                    }
                }
            }
        }
        else
        {
            // other shards
            catalog_entry = shard_->GetCatalog(table_name, req.NodeGroupId());
            assert(catalog_entry != nullptr);
        }

        CatalogKey table_key(table_name);
        Iterator it = FindEmplace(table_key);
        CcEntry<CatalogKey, CatalogRecord> *cce = it->second;

        if (cce == nullptr)
        {
            shard_->Enqueue(shard_->LocalCoreId(), &req);
            return false;
        }

        if (schema_op_msg.stage() == ::txlog::SchemaOpMessage_Stage::
                                         SchemaOpMessage_Stage_PrepareSchema ||
            (schema_op_msg.stage() == ::txlog::SchemaOpMessage_Stage::
                                          SchemaOpMessage_Stage_CommitSchema &&
             is_coordinator))
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
            // When coordinator is recovering from commit log, we need to
            // restore the state right after commit log is flushed, so we need
            // to acquire write lock as well.
            auto lock_pair = AcquireCceKeyLock(cce,
                                               cce->payload_status_,
                                               &req,
                                               req.NodeGroupId(),
                                               ng_term,
                                               0,
                                               CcOperation::Write,
                                               IsolationLevel::RepeatableRead,
                                               CcProtocol::Locking,
                                               0);

            // When a cc node recovers, no one should be holding read locks. So,
            // the acquire operation should always succeed.
            assert(lock_pair.first == LockType::WriteLock &&
                   lock_pair.second == CcErrorCode::NO_ERROR);
        }

        if (cce->payload_ == nullptr)
        {
            cce->payload_ = std::make_unique<CatalogRecord>();
        }
        cce->payload_->Set(catalog_entry->schema_,
                           catalog_entry->dirty_schema_.get(),
                           catalog_entry->Version());

        if (shard_->core_id_ < shard_->core_cnt_ - 1)
        {
            req.ResetCcm();
            MoveRequest(&req, shard_->core_id_ + 1);
        }
        else
        {
            uint32_t tx_node_id = (req.Txn() >> 32L) >> 10;

            if (tx_node_id == req.NodeGroupId())
            {
                int64_t tx_candidate_term =
                    Sharder::Instance().CandidateLeaderTerm(tx_node_id);

                if (tx_candidate_term >= 0)
                {
                    // If the coordinating tx is bound to the recoverying cc
                    // node, resumes the tx.
                    shard_->local_shards_.CreateSchemaRecoveryTx(
                        schema_op_msg,
                        req.Txn(),
                        tx_candidate_term,
                        req.CommitTs());
                }
            }

            req.SetFinish();
        }

        return false;
    }

    TableType Type() const override
    {
        return TableType::Catalog;
    }
};
}  // namespace txservice
