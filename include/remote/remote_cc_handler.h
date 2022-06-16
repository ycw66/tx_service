#pragma once

#include <memory>
#include <vector>

#include "cc/cc_req_pool.h"
#include "moodycamelqueue.h"
#include "proto/cc_request.pb.h"
#include "remote/cc_stream_sender.h"
#include "remote_cc_request.h"
#include "tx_operation_result.h"
#include "tx_record.h"  // RecordStatus;VersionedRecord

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

    void AcquireWriteAll(uint32_t src_node_id,
                         const TableName &table_name,
                         const TxKey &key,
                         uint32_t node_group_id,
                         TxNumber tx_number,
                         int64_t tx_term,
                         bool is_insert,
                         CcHandlerResult<AcquireAllResult> &hres,
                         CcProtocol proto,
                         LockType lk_type);

    void PostWrite(uint32_t src_node_id,
                   uint64_t tx_number,
                   int64_t tx_term,
                   uint64_t commit_ts,
                   const CcEntryAddr &cce_addr,
                   const TxRecord *record,
                   bool is_deleted,
                   CcHandlerResult<Void> &hres,
                   CcProtocol protocol);

    void PostWriteAll(uint32_t src_node_id,
                      const TableName &table_name,
                      const TxKey &key,
                      TxRecord &rec,
                      NodeGroupId ng_id,
                      uint64_t tx_number,
                      int64_t tx_term,
                      uint64_t commit_ts,
                      CcHandlerResult<Void> &hres,
                      DmlOperation dml_op,
                      PostWriteType post_write_type);

    void PostRead(uint32_t src_node_id,
                  uint64_t tx_number,
                  int64_t tx_term,
                  uint64_t key_ts,
                  uint64_t gap_ts,
                  uint64_t commit_ts,
                  const CcEntryAddr &cce_addr,
                  CcHandlerResult<std::vector<TxId>> &hres,
                  CcProtocol protocol,
                  LockType lock_type);

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
              IsolationLevel iso_level = IsolationLevel::ReadCommitted,
              CcProtocol proto = CcProtocol::OCC,
              LockType lock_type = LockType::ReadLock);

    void ReadOutside(int64_t tx_term,
                     const TxRecord &record,
                     bool is_deleted,
                     uint64_t commit_ts,
                     const CcEntryAddr &cce_addr,
                     const std::vector<VersionedRecord> *archives = nullptr);

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
                  IsolationLevel iso_level = IsolationLevel::ReadCommitted,
                  CcProtocol proto = CcProtocol::OCC,
                  LockType lock_type = LockType::ReadLock,
                  bool is_ckpt = false);

    void ScanNext(uint32_t src_node_id,
                  uint32_t ng_id,
                  uint64_t tx_number,
                  int64_t tx_term,
                  uint64_t start_ts,
                  ScanCache *scan_cache,
                  CcHandlerResult<ScanNextResult> &hd_res,
                  IsolationLevel iso_level = IsolationLevel::ReadCommitted,
                  CcProtocol proto = CcProtocol::OCC,
                  LockType lock_type = LockType::ReadLock,
                  bool is_ckpt = false);

    void ScanClose(const TableName &table_name,
                   size_t alias,
                   const TxKey &end_key,
                   bool inclusive,
                   CcProtocol proto = CcProtocol::OCC,
                   LockType lock_type = LockType::ReadLock)
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
                            TxNumber txn,
                            int64_t tx_term,
                            const TableName &table_name,
                            const TxKey &secondary_key,
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
                     const std::string &fault_paras,
                     int64_t tx_term,
                     const TxId &txid,
                     int node_id,
                     CcHandlerResult<bool> &hres);

    void CleanArchives(uint32_t src_node_id,
                       const TableName &table_name,
                       const TxKey &key,
                       uint32_t key_shard_code,
                       uint64_t tx_number,
                       int64_t tx_term,
                       CcHandlerResult<bool> &hres);

private:
    static IsolationType ConvertIsolation(IsolationLevel iso_level);
    static CcProtocolType ConvertProtocol(CcProtocol proto);
    static CcLockType ConvertLockType(LockType lock_type);
    static CommitType ConvertPostWriteType(PostWriteType write_type);
    static RecordStatusType ConvertRecordStatus(RecordStatus rec_status);

    CcStreamSender &stream_sender_;
};
}  // namespace remote

}  // namespace txservice
