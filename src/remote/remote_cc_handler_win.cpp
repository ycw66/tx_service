#include "remote/remote_cc_handler_win.h"

#include "tx_service.h"

bool txservice::remote::RemoteCcHandler_Win::SendRequest(uint32_t shard_id,
                                                         const CcMessage &msg)
{
    uint32_t dest_node_id = Sharder::Instance().NodeId(shard_id);
    assert(global_services_.find(dest_node_id) != global_services_.end());
    TxService *tx_serv = global_services_.at(dest_node_id);

    RemoteCcHandler_Win *dest_hd_ =
        static_cast<RemoteCcHandler_Win *>(tx_serv->remote_hd_.get());

    std::unique_ptr<CcMessage> dest_msg = dest_hd_->GetCcMsg();
    *dest_msg = msg;
    dest_hd_->OnReceiveCcMsg(std::move(dest_msg));

    return true;
}

bool txservice::remote::RemoteCcHandler_Win::SendResponse(uint32_t node_id,
                                                          const CcMessage &msg)
{
    assert(global_services_.find(node_id) != global_services_.end());

    TxService *tx_serv = global_services_.at(node_id);
    RemoteCcHandler_Win *dest_hd_ =
        static_cast<RemoteCcHandler_Win *>(tx_serv->remote_hd_.get());

    std::unique_ptr<CcMessage> dest_msg = dest_hd_->GetCcMsg();
    *dest_msg = msg;
    dest_hd_->OnReceiveCcMsg(std::move(dest_msg));

    return false;
}
