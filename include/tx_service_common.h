#pragma once

#include <mimalloc-2.1/mimalloc.h>

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
    // Original thread default heap for external tx processor.
    // This is only set when external tx processor occupies the shard,
    // and only should be access after occupying shard_status_.
    mi_heap_t *ext_tx_proc_heap_{nullptr};
#endif
};
}  // namespace txservice