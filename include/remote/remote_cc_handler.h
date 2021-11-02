#pragma once

#include <memory>

#include "cc/cc_req_pool.h"
#include "cc/local_cc_shards.h"
#include "moodycamelqueue.h"
#include "proto/cc_request.pb.h"
#include "remote_cc_request.h"

namespace txservice
{
namespace remote
{
class RemoteCcHandler
{
public:
    RemoteCcHandler(LocalCcShards *shards) : msg_pool_(), local_shards_(shards)
    {
        local_shards_->remote_hd_ = this;
        for (auto &local_hd : local_shards_->cc_handlers_)
        {
            local_hd->remote_hd_ = this;
        }
    }

    virtual ~RemoteCcHandler() = default;

    virtual bool SendRequest(uint32_t node_group_id, const CcMessage &msg) = 0;
    virtual bool SendResponse(uint32_t node_id, const CcMessage &msg) = 0;

    void RecycleCcMsg(std::unique_ptr<CcMessage> msg)
    {
        msg_pool_.enqueue(std::move(msg));
    }

    std::unique_ptr<CcMessage> GetCcMsg()
    {
        std::unique_ptr<CcMessage> msg;
        if (msg_pool_.try_dequeue(msg))
        {
            return msg;
        }
        else
        {
            return std::make_unique<CcMessage>();
        }
    }

    void OnReceiveCcMsg(std::unique_ptr<CcMessage> msg);

    void AcquireWrite(const TableName &table_name,
                      const TxKey &key,
                      uint32_t key_shard_code,
                      const TxId &txid,
                      int64_t tx_term,
                      uint64_t ts,
                      bool is_insert,
                      CcHandlerResult<std::pair<uint64_t, CcEntryAddr>> &hres,
                      const CcProtocol proto = CcProtocol::OCC);

    void AcquireTableWriteLock(
        const TableName &table_name,
        const TxId &txid,
        int64_t tx_term,
        uint64_t tx_number,
        uint32_t node_group_id,
        CcHandlerResult<std::unordered_map<uint32_t, int64_t>> &hres);

    void ReleaseTableWriteLock(const TableName &table_name,
                               const TxId &txid,
                               int64_t tx_term,
                               uint64_t tx_number,
                               uint32_t node_group_id,
                               CcHandlerResult<Void> &hres);

    void ReleaseWrite(uint64_t tx_number,
                      int64_t tx_term,
                      const CcEntryAddr &cce_addr,
                      CcHandlerResult<Void> &hd_res);

    void CommitWrite(uint64_t tx_number,
                     int64_t tx_term,
                     uint64_t commit_ts,
                     const CcEntryAddr &cce_addr,
                     const TxRecord &record,
                     bool is_deleted,
                     CcHandlerResult<Void> &hres);

    void ValidateRead(uint64_t tx_number,
                      int64_t tx_term,
                      uint64_t key_ts,
                      uint64_t gap_ts,
                      uint64_t commit_ts,
                      const CcEntryAddr &cce_addr,
                      CcHandlerResult<std::vector<TxId>> &hres);

    void PostprocessRead(uint64_t tx_number,
                         int64_t tx_term,
                         const CcEntryAddr &cce_addr,
                         CcHandlerResult<Void> &hres,
                         CcProtocol proto = CcProtocol::OCC);

    void CommitCreateTable(const TableName &table_name,
                           std::string catalog_str,
                           const TxId &txid,
                           uint64_t ts,
                           uint32_t node_group_id,
                           CcHandlerResult<Void> &hresult);

    void CommitDropTable(const TableName &table_name,
                         const TxId &txid,
                         uint64_t ts,
                         uint32_t node_group_id,
                         CcHandlerResult<Void> &hresult);

    void Read(
        const TableName &table_name,
        const TxKey &key,
        uint32_t key_shard_code,
        const TxRecord &record,
        ReadType read_type,
        uint64_t tx_number,
        int64_t tx_term,
        const uint64_t ts,
        CcHandlerResult<
            std::tuple<TxRecord *, uint64_t, CcEntryAddr, RecordStatus>> &hres,
        CcProtocol proto = CcProtocol::OCC);

    void ReadOutside(const TxRecord &record,
                     bool is_deleted,
                     const CcEntryAddr &cce_addr);

    void ScanOpen(
        const TableName &table_name,
        ScanIndexType index_type,
        uint32_t node_group_id,
        const TxKey &start_key,
        bool inclusive,
        uint64_t tx_number,
        int64_t tx_term,
        uint64_t ts,
        CcHandlerResult<std::pair<size_t, std::unique_ptr<CcScanner>>> &hd_res,
        ScanDirection direction = ScanDirection::Forward,
        CcProtocol proto = CcProtocol::OCC,
        bool is_ckpt = false);

    void ScanNext(uint32_t ng_id,
                  uint64_t tx_number,
                  int64_t tx_term,
                  uint64_t start_ts,
                  ScanCache *scan_cache,
                  CcHandlerResult<uint32_t> &hd_res,
                  CcProtocol proto = CcProtocol::OCC,
                  bool is_ckpt = false);

    void ScanClose(const TableName &table_name,
                   size_t alias,
                   const TxKey &end_key,
                   bool inclusive,
                   CcProtocol proto = CcProtocol::OCC)
    {
    }

    void UploadRecord(const TableName &table_name,
                      const TxKey &key,
                      TxRecord *record,
                      const CcEntryAddr &cce_addr,
                      CcHandlerResult<Void> &)
    {
    }

    void UploadSecondaryKey(const TableName &table_name,
                            const TxKey &sk,
                            const TxKey &pk,
                            CcHandlerResult<Void> &)
    {
    }

    void CommitSecondaryKey(const TableName &table_name,
                            const TxKey &sk,
                            const TxKey &pk,
                            uint32_t key_shard_code,
                            bool is_delete,
                            uint64_t ts,
                            CcHandlerResult<Void> &hd_res);

    void UpdateCommitLowerBound(const TxId &txid,
                                uint64_t commit_ts_lower_bound,
                                CcHandlerResult<uint64_t> &)
    {
    }

    void FaultInject(const std::string &fault_name,
                     const std::string &fault_type,
                     int node_id,
                     CcHandlerResult<bool> &hres);

protected:
    moodycamel::ConcurrentQueue<std::unique_ptr<CcMessage>> msg_pool_;
    LocalCcShards *const local_shards_;

    // In each node there is one port accepting cc requests from all nodes via a
    // stream. CC requests in the stream are processed by a single thread
    // sequentially. So, even though this handler is shared by all threads in
    // the node, data structures related to processing incoming requests (such
    // as the following resource pools) are inherently thread-safe.
    CcRequestPool<RemoteAcquire> acquire_pool_;
    CcRequestPool<RemotePostDelete> postdel_pool_;
    CcRequestPool<RemotePostCommit> postcommit_pool_;
    CcRequestPool<RemoteValidate> vali_pool_;
    CcRequestPool<RemotePostRead> postread_pool_;
    CcRequestPool<RemoteRead> read_pool_;
    CcRequestPool<RemoteReadOutside> read_outside_pool_;
    CcRequestPool<RemoteScanOpen> scan_open_pool_;
    CcRequestPool<RemoteScanNextBatch> scan_next_pool_;
    CcRequestPool<RemoteCommitSk> commit_sk_pool_;
    CcRequestPool<RemoteAcquireTableWriteLockCC> acquire_table_write_lock_pool;
    CcRequestPool<RemoteCommitCreateTable> commit_create_table_pool;
    CcRequestPool<RemoteReleaseTableWriteLock> release_table_write_lock_pool;
    CcRequestPool<RemoteCommitDropTable> commit_drop_table_pool;
    CcRequestPool<RemoteFaultInjectCC> fault_inject_pool_;
    // CcRequestPool<NegotiateCc> negoti_pool;
};
}  // namespace remote

}  // namespace txservice
