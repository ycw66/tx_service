#pragma once

#include <unordered_map>

#include "remote_cc_handler.h"

namespace txservice
{
class TxService;

namespace remote
{
class RemoteCcHandler_Win : public RemoteCcHandler
{
public:
    RemoteCcHandler_Win(LocalCcShards *shards = nullptr)
        : RemoteCcHandler(shards)
    {
    }

    bool SendRequest(uint32_t shard_id, const CcMessage &msg) override;
    bool SendResponse(uint32_t node_id, const CcMessage &msg) override;

    void AddTxService(uint32_t node_id, TxService *node_service)
    {
        global_services_.try_emplace(node_id, node_service);
    }

private:
    std::unordered_map<uint32_t, TxService *> global_services_;
};
}  // namespace remote
}  // namespace txservice