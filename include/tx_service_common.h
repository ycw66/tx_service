#pragma once

#include <mimalloc-2.1/mimalloc.h>

#include <atomic>
#include <condition_variable>
#include <mutex>

namespace txservice
{

// whether skip write redo log to log_service.
inline bool txservice_skip_wal = false;
// whether skip accessing KV when cc map cache misses.
inline bool txservice_skip_kv = false;

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
#ifdef ON_KEY_OBJECT
    // The external txm count. If it's not zero, the external processor
    // shouldn't sleep.
    std::atomic<int16_t> external_txm_cnt_{0};
#endif
    std::atomic<int16_t> ext_processor_cnt_{0};
    // Original thread default heap for external tx processor.
    // This is only set when external tx processor occupies the shard,
    // and only should be access after occupying shard_status_.
    mi_heap_t *ext_tx_proc_heap_{nullptr};
#endif
};
}  // namespace txservice
