#include "standby.h"

#include "cc_request.h"

namespace txservice
{

void StandbyForwardEntry::AddTxCommand(ApplyCc &cc_req)
{
    auto &req = Request();
    if (cc_req.IsLocal())
    {
        TxCommand *cmd = cc_req.CommandPtr();

        std::string cmd_str;
        cmd->Serialize(cmd_str);
        req.add_cmd_list(std::move(cmd_str));
    }
    else
    {
        req.add_cmd_list(*cc_req.CommandImage());
    }

    assert(cc_req.GetCommand());
    if (cc_req.GetCommand()->IsOverwrite())
    {
        req.set_has_overwrite(true);
    }
}
};  // namespace txservice