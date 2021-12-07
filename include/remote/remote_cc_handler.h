#pragma once

#include <memory>

#include "cc/cc_req_pool.h"
#include "moodycamelqueue.h"
#include "proto/cc_request.pb.h"
#include "remote/cc_stream_sender.h"
#include "remote_cc_request.h"
#include "tx_operation_result.h"

namespace txservice
{
class LocalCcShards;

namespace remote
{
class RemoteCcHandler
{
public:
    RemoteCcHandler(CcStreamSender &stream_sender);
    ~RemoteCcHandler() = default;

    void AcquireWrite(uint32_t src_node_id,
                      const TableName &table_name,
                      const TxKey &key,
                      uint32_t key_shard_code,
                      const TxId &txid,
                      int64_t tx_term,
                      uint64_t ts,
                      bool is_insert,
                      CcHandlerResult<AcquireKeyResult> &hres,
                      const CcProtocol proto = CcProtocol::OCC);

    void AcquireTableWriteLock(
        uint32_t src_node_id,
        const TableName &table_name,
        const TxId &txid,
        int64_t tx_term,
        uint64_t tx_number,
        uint32_t node_group_id,
        CcHandlerResult<std::unordered_map<uint32_t, int64_t>> &hres);

    void ReleaseTableWriteLock(uint32_t src_node_id,
                               const TableName &table_name,
                               const TxId &txid,
                               int64_t tx_term,
                               uint64_t tx_number,
                               uint32_t node_group_id,
                               CcHandlerResult<Void> &hres);

    void ReleaseWrite(uint32_t src_node_id,
                      uint64_t tx_number,
                      int64_t tx_term,
                      const CcEntryAddr &cce_addr,
                      CcHandlerResult<Void> &hd_res);

    void CommitWrite(uint32_t src_node_id,
                     uint64_t tx_number,
                     int64_t tx_term,
                     uint64_t commit_ts,
                     const CcEntryAddr &cce_addr,
                     const TxRecord &record,
                     bool is_deleted,
                     CcHandlerResult<Void> &hres);

    void ValidateRead(uint32_t src_node_id,
                      uint64_t tx_number,
                      int64_t tx_term,
                      uint64_t key_ts,
                      uint64_t gap_ts,
                      uint64_t commit_ts,
                      const CcEntryAddr &cce_addr,
                      CcHandlerResult<std::vector<TxId>> &hres);

    void PostprocessRead(uint32_t src_node_id,
                         uint64_t tx_number,
                         int64_t tx_term,
                         const CcEntryAddr &cce_addr,
                         CcHandlerResult<Void> &hres,
                         CcProtocol proto = CcProtocol::OCC);

    void CommitCreateTable(uint32_t src_node_id,
                           const TableName &table_name,
                           std::string catalog_str,
                           const TxId &txid,
                           uint64_t ts,
                           uint32_t node_group_id,
                           CcHandlerResult<Void> &hresult);

    void CommitDropTable(uint32_t src_node_id,
                         const TableName &table_name,
                         const TxId &txid,
                         uint64_t ts,
                         uint32_t node_group_id,
                         CcHandlerResult<Void> &hresult);

    void Read(uint32_t src_node_id,
              const TableName &table_name,
              const TxKey &key,
              uint32_t key_shard_code,
              const TxRecord &record,
              ReadType read_type,
              uint64_t tx_number,
              int64_t tx_term,
              const uint64_t ts,
              CcHandlerResult<ReadKeyResult> &hres,
              CcProtocol proto = CcProtocol::OCC);

    void ReadOutside(const TxRecord &record,
                     bool is_deleted,
                     const CcEntryAddr &cce_addr);

    void ScanOpen(uint32_t src_node_id,
                  const TableName &table_name,
                  ScanIndexType index_type,
                  uint32_t node_group_id,
                  const TxKey &start_key,
                  bool inclusive,
                  uint64_t tx_number,
                  int64_t tx_term,
                  uint64_t ts,
                  CcHandlerResult<ScanOpenResult> &hd_res,
                  ScanDirection direction = ScanDirection::Forward,
                  CcProtocol proto = CcProtocol::OCC,
                  bool is_ckpt = false);

    void ScanNext(uint32_t src_node_id,
                  uint32_t ng_id,
                  uint64_t tx_number,
                  int64_t tx_term,
                  uint64_t start_ts,
                  ScanCache *scan_cache,
                  CcHandlerResult<ScanNextResult> &hd_res,
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

    void CommitSecondaryKey(uint32_t src_node_id,
                            const TableName &table_name,
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

    void FaultInject(uint32_t src_node_id,
                     const std::string &fault_name,
                     const std::string &fault_type,
                     int node_id,
                     CcHandlerResult<bool> &hres);

private:
    CcStreamSender &stream_sender_;
};
}  // namespace remote

}  // namespace txservice
