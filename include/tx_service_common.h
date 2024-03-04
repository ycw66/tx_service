#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>

namespace txservice
{
enum struct TxShardStatus
{
    Free = 0,
    Occupied,
    Deconstructed
};

struct TxProcCoordinator
{
    std::mutex sleep_mux_;
    std::condition_variable sleep_cv_;
    std::atomic<TxShardStatus> shard_status_{TxShardStatus::Free};
#ifdef EXT_TX_PROC_ENABLED
    std::atomic<int16_t> ext_processor_cnt_{0};
#endif
};
}  // namespace txservice