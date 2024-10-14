#pragma once

#include <queue>

#include "glog/logging.h"
#include "proto/cc_request.pb.h"
#include "tx_id.h"
#include "type.h"

namespace txservice
{
struct ApplyCc;
struct StandbyForwardEntry
{
    StandbyForwardEntry()
    {
        msg.set_type(remote::CcMessage::MessageType::
                         CcMessage_MessageType_KeyObjectStandbyForwardRequest);
    }

    void AddTxCommand(ApplyCc &cc_req);

    void Free()
    {
        in_use_ = false;
    }

    bool IsFree() const
    {
        return !in_use_;
    }

    void Reset()
    {
        assert(!in_use_);
        in_use_ = true;
        msg.clear_key_obj_standby_forward_req();
        sequence_id_ = UINT64_MAX;
    }

    remote::KeyObjectStandbyForwardRequest &Request()
    {
        return *msg.mutable_key_obj_standby_forward_req();
    }

    uint64_t SequenceId() const
    {
        return sequence_id_;
    }

    void SetSequenceId(uint64_t seq_id)
    {
        sequence_id_ = seq_id;
    }

    const remote::CcMessage &Message() const
    {
        return msg;
    }

private:
    bool in_use_{false};
    uint64_t sequence_id_{UINT64_MAX};
    remote::CcMessage msg;
};

struct StandbySequenceGroup
{
    void Subscribe(uint64_t initial_seq_id)
    {
        next_expecting_standby_sequence_id_ = initial_seq_id;
        last_consistent_standby_sequence_id_ = initial_seq_id - 1;
        last_requested_resend_sequence_id_ = 0;
        missing_standby_seqeunce_ids_.clear();
        while (!pending_standby_consistent_ts_.empty())
        {
            pending_standby_consistent_ts_.pop();
        }

        last_standby_consistent_ts_ = 0;
        subscribed_ = true;
    }

    void Unsubscribe()
    {
        subscribed_ = false;
    }

    // the largest sequencec received + 1
    uint64_t next_expecting_standby_sequence_id_{0};
    // the largest sequence number that we've received all msgs before this
    // sequence number.
    uint64_t last_consistent_standby_sequence_id_{0};
    // the largest missing sequence number that we've requested primary to
    // resend
    uint64_t last_requested_resend_sequence_id_{0};
    // Set of missing sequence msgs
    std::set<uint64_t> missing_standby_seqeunce_ids_;
    // The largest ts that is consistent in this seq group.
    uint64_t last_standby_consistent_ts_{0};
    // The pending ts that are not consistent yet.
    std::queue<std::pair<uint64_t, uint64_t>> pending_standby_consistent_ts_;
    bool subscribed_{false};
};

void BrocastPrimaryCkptTs(NodeGroupId node_group_id,
                          int64_t node_group_term,
                          uint64_t primary_ckpt_ts);

};  // namespace txservice