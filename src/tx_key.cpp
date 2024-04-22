#include "tx_key.h"

namespace txservice
{
TxKey TxKeyInterface::Clone(const void *this_obj) const
{
    return clone_func_(this_obj);
}
}  // namespace txservice