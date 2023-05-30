#include "tx_service.h"

namespace txservice
{
moodycamel::ConcurrentQueue<TransactionExecution::uptr> TxProcessor::free_txs{};
}
