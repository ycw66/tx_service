#pragma once

#include <unordered_map>

#include "catalog_factory.h"
#include "catalog_key_record.h"
#include "cc/cc_request.h"
#include "cc/non_blocking_lock.h"
#include "cc/template_cc_map.h"
#include "sharder.h"

namespace txservice
{
class CatalogCcMap : public TemplateCcMap<CatalogKey, CatalogRecord>
{
public:
    CatalogCcMap(const CatalogCcMap &rhs) = delete;
    ~CatalogCcMap() = default;

    CatalogCcMap(CcShard *shard)
        : TemplateCcMap<CatalogKey, CatalogRecord>(shard)
    {
    }

    using TemplateCcMap::Execute;

    bool Execute(PostWriteAllCc &req) override
    {
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
                                               schema_rec->SchemaBlob(),
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
                    shard_->GetCatalog(table_key->Name()));
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

        // When the request commits the schema operation, modifies the cc
        // map(s) at this shard.
        if (req.CommitType() == PostWriteType::PostCommit)
        {
            const TableSchemaView *schema_view = schema_rec->SchemaView();
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
                shard_->CreatePkCcMap(
                    table_key->Name(), new_schema, req.NodeGroupId());

                std::vector<TableName> index_names = new_schema->IndexNames();
                for (const TableName &index_name : index_names)
                {
                    shard_->CreateSkCcMap(
                        index_name, new_schema, req.NodeGroupId());
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
            shard_->core_id_ == shard_->core_cnt_ - 1)
        {
            shard_->CommitDirtyCatalog(table_key->Name());
        }

        return TemplateCcMap::Execute(req);
    }

    bool Execute(ReadCc &req) override
    {
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
                shard_->GetCatalog(table_key->Name());

            if (schema_view == nullptr)
            {
                assert(schema_rec->SchemaBlob().size() > 0);

                schema_view = shard_->CreateCatalog(
                    table_key->Name(), schema_rec->SchemaBlob(), 1);
            }
            schema_rec->SetSchemaView(schema_view);

            const TableSchema *curr_schema = schema_view->schema_;
            if (curr_schema != nullptr)
            {
                shard_->CreatePkCcMap(
                    table_key->Name(), curr_schema, req.NodeGroupId());

                std::vector<TableName> index_names = curr_schema->IndexNames();
                for (const TableName &index_name : index_names)
                {
                    shard_->CreateSkCcMap(
                        index_name, curr_schema, req.NodeGroupId());
                }
            }
        }

        return TemplateCcMap::Execute(req);
    }
};
}  // namespace txservice