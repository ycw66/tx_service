#pragma once

#include "../log_service/proto/raft_log.pb.h"

namespace txservice
{
enum struct RecoverTxStatus
{
    // The cc node in which the tx resides has not failed over to a new term.
    // The tx is considered to be still alive. No recovery is needed. Wait for
    // the tx to make futher actions.
    Alive = 0,
    // The cc node in which the tx resides has failed. There is a committed log
    // record from the tx in the log state machine.
    Committed,
    // The cc node in which the tx resides has failed. No log record from the tx
    // is found in the log state machine. The tx is deemed not committed and it
    // cannot commit in future (because there will be term mismatches for the
    // intentions/locks the tx holds).
    NotCommitted,
    // An error occurred when checking the tx status during recovery.
    RecoverError
};

class TxLog
{
public:
    virtual ~TxLog() = default;

    // Persists and replicates a log record in the specified log group.
    virtual void WriteLog(uint32_t log_group_id,
                          brpc::Controller *cntl,
                          const ::txlog::LogRequest &log_record,
                          ::txlog::LogResponse &log_response,
                          google::protobuf::Closure &done) = 0;

    // Invoked by a cc node group's leader to notify all log groups its
    // checkpoint timestamp (so that the transaction log can be truncated).
    virtual void TruncateLog(brpc::Controller *controller,
                             const ::txlog::LogRequest *request,
                             ::txlog::LogResponse *response,
                             ::google::protobuf::Closure *done) = 0;

    // Invoked by a failing over cc node group to replay log and notify all log
    // groups the raft term of the group's new leader.
    virtual void ReplayLog(uint32_t cc_node_group_id,
                           int64_t term,
                           const std::string &source_ip,
                           uint16_t source_port,
                           std::atomic<bool> &interrupt) = 0;

    virtual RecoverTxStatus RecoverTx(uint64_t tx_number,
                                      int64_t tx_term,
                                      uint32_t cc_ng_id,
                                      int64_t cc_ng_term) = 0;

    virtual std::unique_ptr<TxLog> Clone() const = 0;

    virtual uint32_t LogGroupCount() const = 0;

    virtual uint32_t LogGroupReplicaNum() const = 0;

    virtual uint32_t GetLogGroupId(uint32_t cc_node_id) const = 0;
};
}  // namespace txservice