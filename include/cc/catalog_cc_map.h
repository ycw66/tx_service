#pragma once

#include <string>
#include <unordered_map>

#include "../log_service/proto/raft_log.pb.h"
#include "catalog_factory.h"
#include "catalog_key_record.h"
#include "cc_request.h"
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
    CatalogCcMap(CcShard *shard)
        : TemplateCcMap<CatalogKey, CatalogRecord>(shard, 1)
    {
    }

    using TemplateCcMap::Execute;

    std::unique_ptr<CcMap> Clone() const override
    {
        return std::make_unique<CatalogCcMap>(shard_);
    }

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
        int64_t ng_term = Sharder::Instance().LeaderTerm(req.NodeGroupId());
        if (ng_term < 0)
        {
            req.Result()->SetError(-1);
            return true;
        }

        const CatalogKey *table_key = nullptr;
        if (req.Key() != nullptr)
        {
            table_key = static_cast<const CatalogKey *>(req.Key());
        }
        else
        {
            assert(req.KeyStr() != nullptr);

            std::unique_ptr<CatalogKey> decoded_key =
                std::make_unique<CatalogKey>();
            size_t offset = 0;
            decoded_key->Deserialize(req.KeyStr()->data(), offset, nullptr);
            table_key = decoded_key.get();
            req.SetDecodedKey(std::move(decoded_key));
        }

        if (req.CommitTs() == 0)
        {
            // When the commit ts is 0, the request commits nothing and only
            // removes the write intents/locks acquired earlier.
            return TemplateCcMap::Execute(req);
        }

        CatalogRecord *schema_rec = nullptr;
        switch (req.CommitType())
        {
        case PostWriteType::PrepareCommit:
        {
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
                    if (req.DmlOp() != DmlOperation::Delete)
                    {
                        size_t offset = 0;
                        decoded_rec->Deserialize(req.PayloadStr()->data(),
                                                 offset);
                    }
                    schema_rec = decoded_rec.get();
                    req.SetDecodedPayload(std::move(decoded_rec));
                }

                const TableSchemaView *schema_view =
                    shard_->CreateDirtyCatalog(table_key->Name(),
                                               req.NodeGroupId(),
                                               schema_rec->SchemaImage(),
                                               req.CommitTs());

                schema_rec->SetSchemaView(schema_view);
            }
            else
            {
                assert(req.Payload() != nullptr);
                schema_rec = static_cast<CatalogRecord *>(req.Payload());
            }

            break;
        }
        case PostWriteType::PostCommit:
        {
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

                schema_rec->SetSchemaView(
                    shard_->GetCatalog(table_key->Name(), req.NodeGroupId()));
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

        assert(schema_rec->SchemaView() != nullptr);
        const TableSchemaView *schema_view = schema_rec->SchemaView();

        // When the request commits the schema operation, modifies the cc
        // map(s) at this shard.
        if (req.CommitType() == PostWriteType::PostCommit &&
            schema_view->dirty_version_ts_ > 0)
        {
            const TableSchema *old_schema = schema_view->schema_;
            const TableSchema *new_schema = schema_view->dirty_schema_;

            if (new_schema == nullptr)
            {
                // A remote tx is allowed to acquire write intents/locks and
                // drop a table, even if the table's schema has not been
                // initialized at this node. The earlier acquiring-write-intent
                // request creates a schema cc entry in the catalog cc map and a
                // node-level schema view. The version timestamp of the schema
                // is 0, if the schema is uninitialized (null). Or, the current
                // schema must not be null.
                assert(schema_view->version_ts_ == 0 || old_schema != nullptr);

                // This is a DROP TABLE statement. Drops the cc maps
                // associated with the table in the final commit step.
                shard_->DropCcm(table_key->Name(), req.NodeGroupId());

                if (old_schema != nullptr)
                {
                    std::vector<TableName> index_names =
                        old_schema->IndexNames();
                    for (const TableName &index_name : index_names)
                    {
                        shard_->DropCcm(index_name, req.NodeGroupId());
                    }
                }
            }
            else if (old_schema == nullptr)
            {
                assert(schema_view->dirty_version_ts_ > 0 &&
                       new_schema != nullptr);

                // This is a CREATE TABLE statement. Creates the cc maps
                // associated with the table in the final commit step.
                shard_->CreatePkCcMap(table_key->Name(),
                                      new_schema,
                                      req.NodeGroupId(),
                                      schema_view->dirty_version_ts_,
                                      true);

                std::vector<TableName> index_names = new_schema->IndexNames();
                for (const TableName &index_name : index_names)
                {
                    shard_->CreateSkCcMap(index_name,
                                          new_schema,
                                          req.NodeGroupId(),
                                          schema_view->dirty_version_ts_);
                }
            }
        }
        else if (req.CommitType() == PostWriteType::PrepareCommit)
        {
            // Prepare commit. For certain schema operations, e.g., create
            // secondary index, the cc map is modified in the prepare commit
            // step.
        }

        if (req.CommitType() == PostWriteType::PostCommit &&
            shard_->core_id_ == 0 && schema_view->dirty_version_ts_ > 0)
        {
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
        int64_t ng_term = Sharder::Instance().LeaderTerm(req.NodeGroupId());
        if (ng_term < 0)
        {
            req.Result()->SetError(-1);
            return true;
        }

        const CcEntryAddr &cce_addr = req.Result()->Value().cce_addr_;
        const CatalogKey *table_key = nullptr;
        CatalogRecord *schema_rec = static_cast<CatalogRecord *>(req.Record());

        if (cce_addr.CcePtr() != 0)
        {
            const CcEntry<CatalogKey, CatalogRecord> *cce =
                reinterpret_cast<const CcEntry<CatalogKey, CatalogRecord> *>(
                    cce_addr.CcePtr());
            table_key = cce->key_;
        }
        else
        {
            // A read request toward a table's catalog is always dispatched to
            // the local shard to which the sending tx is bound.
            assert(req.Key() != nullptr);
            table_key = static_cast<const CatalogKey *>(req.Key());
        }

        if (req.Type() == ReadType::OutsideNormal)
        {
            const TableSchemaView *schema_view =
                shard_->GetCatalog(table_key->Name(), req.NodeGroupId());

            if (schema_view == nullptr)
            {
                assert(schema_rec->SchemaImage().size() > 0);

                schema_view = shard_->CreateCatalog(table_key->Name(),
                                                    req.NodeGroupId(),
                                                    schema_rec->SchemaImage(),
                                                    req.ReadTimestamp());
            }
            schema_rec->SetSchemaView(schema_view);

            const TableSchema *curr_schema = schema_view->schema_;
            if (curr_schema != nullptr)
            {
                shard_->CreatePkCcMap(table_key->Name(),
                                      curr_schema,
                                      req.NodeGroupId(),
                                      schema_view->version_ts_);

                std::vector<TableName> index_names = curr_schema->IndexNames();
                for (const TableName &index_name : index_names)
                {
                    shard_->CreateSkCcMap(index_name,
                                          curr_schema,
                                          req.NodeGroupId(),
                                          schema_view->version_ts_);
                }
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
            req.Result()->SetError(-1);
            return false;
        }

        ::txlog::SchemaOpMessage schema_op_msg;
        const std::string_view &content = req.LogContentView();
        schema_op_msg.ParseFromArray(content.data(), content.length());

        const TableSchemaView *schema_view = nullptr;
        if (shard_->core_id_ == 0)
        {
            if (schema_op_msg.stage() == ::txlog::SchemaOpMessage_Stage::
                                             SchemaOpMessage_Stage_CommitSchema)
            {
                uint64_t commit_ts = req.CommitTs();
                assert(commit_ts > 0);

                schema_view =
                    shard_->CreateCatalog(schema_op_msg.table_name(),
                                          req.NodeGroupId(),
                                          schema_op_msg.catalog_blob(),
                                          commit_ts);

                const TableSchema *committed_schema = schema_view->schema_;
                if (committed_schema != nullptr)
                {
                    shard_->CreatePkCcMap(schema_op_msg.table_name(),
                                          committed_schema,
                                          req.NodeGroupId(),
                                          schema_view->version_ts_);

                    std::vector<TableName> index_names =
                        committed_schema->IndexNames();
                    for (const TableName &index_name : index_names)
                    {
                        shard_->CreateSkCcMap(index_name,
                                              committed_schema,
                                              req.NodeGroupId(),
                                              schema_view->version_ts_);
                    }
                }
            }
            else
            {
                schema_view =
                    shard_->CreateDirtyCatalog(schema_op_msg.table_name(),
                                               req.NodeGroupId(),
                                               schema_op_msg.catalog_blob(),
                                               req.CommitTs());
            }
        }
        else
        {
            schema_view = shard_->GetCatalog(schema_op_msg.table_name(),
                                             req.NodeGroupId());
        }

        CatalogKey table_key(schema_op_msg.table_name());
        CcEntry<CatalogKey, CatalogRecord> *cce =
            FindEmplace(table_key, req.CommitTs());

        if (cce == nullptr)
        {
            shard_->Enqueue(shard_->LocalCoreId(), &req);
            return false;
        }

        if (schema_op_msg.stage() !=
            ::txlog::SchemaOpMessage_Stage::SchemaOpMessage_Stage_CommitSchema)
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
            bool success =
                cce->key_lock_.AcquireWriteLock(&req, 0, CcProtocol::Locking);

            // When a cc node recovers, no one should be holding read locks. So,
            // the acquire operation should always succeed.
            // TODO: when a cc node steps down as the leader, should clear the
            // node group's cc maps.
            assert(success);
        }
        cce->payload_.SetSchemaView(schema_view);

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
                    // node, re-resumes the tx.
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
};
}  // namespace txservice
