#pragma once

#include "cc_map.h"
#include "cluster_config_record.h"
#include "template_cc_map.h"
#include "tx_key.h"
#include "tx_record.h"

namespace txservice
{
class ClusterConfigCcMap : public TemplateCcMap<VoidKey, ClusterConfigRecord>
{
public:
    ClusterConfigCcMap(const ClusterConfigCcMap &rhs) = delete;

    using TemplateCcMap<VoidKey, ClusterConfigRecord>::Execute;
    using TemplateCcMap<VoidKey, ClusterConfigRecord>::AcquireCceKeyLock;
    using TemplateCcMap<VoidKey,
                        ClusterConfigRecord>::LockHandleForResumedRequest;
    using TemplateCcMap<VoidKey, ClusterConfigRecord>::neg_inf_;

    ClusterConfigCcMap(CcShard *shard,
                       NodeGroupId cc_ng_id,
                       const TableName &table_name)
        : TemplateCcMap<VoidKey, ClusterConfigRecord>(
              shard, cc_ng_id, table_name, 1, nullptr, true)
    {
        // We only store one record in ClusterConfigCcMap as neg_inf_ key. It is
        // is only used for concurrency control purpose.
        neg_inf_.payload_ = std::make_shared<ClusterConfigRecord>();
        neg_inf_.commit_ts_ = 0;
        neg_inf_.payload_status_ = RecordStatus::Normal;
    }

    bool Execute(AcquireAllCc &req) override
    {
        CcHandlerResult<AcquireAllResult> *hd_res = req.Result();
        AcquireAllResult &acquire_all_result = hd_res->Value();
        uint32_t ng_id = req.NodeGroupId();
        int64_t ng_term = Sharder::Instance().LeaderTerm(ng_id);
        if (ng_term < 0)
        {
            hd_res->SetError(CcErrorCode::REQUESTED_NODE_NOT_LEADER);
            return true;
        }

        LockType acquired_lock = LockType::NoLock;
        CcErrorCode err_code = CcErrorCode::NO_ERROR;
        CcEntry<VoidKey, ClusterConfigRecord> *cce_ptr = nullptr;
        bool resume = false;
        if (req.CcePtr() != nullptr)
        {
            // The request was blocked before and is now unblocked.
            resume = true;
            cce_ptr = static_cast<CcEntry<VoidKey, ClusterConfigRecord> *>(
                req.CcePtr());
            std::tie(acquired_lock, err_code) =
                LockHandleForResumedRequest(cce_ptr,
                                            neg_inf_.payload_status_,
                                            &req,
                                            ng_id,
                                            ng_term,
                                            req.TxTerm(),
                                            req.CcOp(),
                                            req.Isolation(),
                                            req.Protocol(),
                                            0,
                                            false);
        }
        else
        {
            // First time the request is processed in this shard. Or the request
            // is blocked previously because the cc map is full.
            // There's only one record in ClusterConfigCcMap, so we don't need
            // to worry about the key.
            req.SetCcePtr(&neg_inf_);
        }

        // On execution resumption, the write lock has been acquired when
        // being unblocked.
        CcEntry<VoidKey, ClusterConfigRecord> &cc_entry = neg_inf_;
        if (!resume)
        {
            int64_t tx_term = req.TxTerm();
            IsolationLevel iso_lvl = req.Isolation();
            CcProtocol cc_proto = req.Protocol();
            CcOperation cc_op = req.CcOp();
            std::tie(acquired_lock, err_code) =
                AcquireCceKeyLock(&cc_entry,
                                  cc_entry.payload_status_,
                                  &req,
                                  req.NodeGroupId(),
                                  ng_term,
                                  tx_term,
                                  cc_op,
                                  iso_lvl,
                                  cc_proto,
                                  0,
                                  false);
        }

        switch (err_code)
        {
        case CcErrorCode::NO_ERROR:
        {
            // Updates last_vali_ts such that it is no smaller than (1) all
            // read transactions that have read the item in all shards, and
            // (2) the local time.
            acquire_all_result.last_vali_ts_ = cc_entry.last_read_ts_;
            acquire_all_result.local_cce_addr_.SetCce(
                reinterpret_cast<uint64_t>(cce_ptr),
                ng_term,
                req.NodeGroupId(),
                shard_->LocalCoreId());
            acquire_all_result.commit_ts_ = cc_entry.commit_ts_;
            acquire_all_result.node_term_ = ng_term;

            // Cluster config map is only stored on the first core, so we don't
            // need to pass this request to other cores.
            hd_res->SetFinished();

            return true;
        }
        case CcErrorCode::ACQUIRE_LOCK_BLOCKED:
        {
            // If the request comes from a remote node, sends
            // acknowledgement to the sender when the request is
            // blocked.
            if (!req.IsLocal())
            {
                req.Result()->Value().node_term_ = ng_term;

                remote::RemoteAcquireAll &remote_req =
                    static_cast<remote::RemoteAcquireAll &>(req);
                remote_req.Acknowledge();
            }

            return false;
        }
        default:
        {
            // lock confilct: back off and retry.
            req.Result()->SetError(err_code);
            return true;
        }
        }
    }

    bool Execute(PostWriteAllCc &req) override
    {
        if (req.CommitTs() == TransactionOperation::tx_op_failed_ts_)
        {
            // transaction failed before prepare log. Release lock and return.
            ReleaseCceKeyLock(&neg_inf_, req.Txn(), req.NodeGroupId());
            req.Result()->SetFinished();
            return true;
        }

        ClusterConfigRecord *config_rec = nullptr;
        if (req.Key() != nullptr)
        {
            // request comes from same node group.
            assert(req.Key() == NegativeInfinity<VoidKey>::Instance());
            config_rec = static_cast<ClusterConfigRecord *>(req.Payload());
        }
        else
        {
            assert(*req.KeyStrType() == KeyType::NegativeInf);
            req.SetTxKey(NegativeInfinity<VoidKey>::Instance());
            assert(req.PayloadStr() != nullptr);
            std::unique_ptr<ClusterConfigRecord> decoded_rec =
                std::make_unique<ClusterConfigRecord>();
            size_t offset = 0;
            decoded_rec->Deserialize(req.PayloadStr()->c_str(), offset);
            config_rec = decoded_rec.get();
            req.SetDecodedPayload(std::move(decoded_rec));
        }
        // First we need to update cluster configs in Sharder.
        if (Sharder::Instance().UpdateClusterConfig(
                config_rec->GetNodeGroupConfigs(),
                req.CommitTs(),
                &req,
                this->shard_))
        {
            // async braft change_peers call is made. cc req will be put
            // back in queue once it's done.
            return false;
        }

        // Lastly release/downgrade lock. We do not need to update the record in
        // cluster config cc map since it is always empty.
        LockType lk_type = LockType::NoLock;
        TxNumber txn = req.Txn();
        if (neg_inf_.key_lock_ptr_ != nullptr)
        {
            // AcquireAllCc only acquire WriteIntent or WriteLock
            if (neg_inf_.key_lock_ptr_->HasWriteLock() &&
                neg_inf_.key_lock_ptr_->WriteLockTx() == txn)
            {
                lk_type = LockType::WriteLock;
            }
            else if (neg_inf_.key_lock_ptr_->HasWriteIntent() &&
                     neg_inf_.key_lock_ptr_->WriteIntentTx() == txn)
            {
                lk_type = LockType::WriteIntent;
            }
        }

        if (lk_type != LockType::NoLock)
        {
            neg_inf_.commit_ts_ = req.CommitTs();
            ReleaseCceKeyLock(&neg_inf_, txn, req.NodeGroupId());
        }

        // No need to move the request to next core since this map is only
        // stored on core 0.
        req.Result()->SetFinished();
        req.SetDecodedPayload(nullptr);
        return true;
    }
};
}  // namespace txservice