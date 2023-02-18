#pragma once

#include <atomic>
#include <string>
#include <thread>
#include <unordered_map>

#include "cc/local_cc_shards.h"  // LocalCcShards
#include "fault/fault_inject.h"  // CODE_FAULT_INJECTOR

namespace txservice
{
class TxStartTsCollector
{
public:
    static TxStartTsCollector &Instance(LocalCcShards *shards = nullptr,
                                        uint32_t delay_seconds = 60)
    {
        static TxStartTsCollector instance_(shards, delay_seconds);
        return instance_;
    }

    ~TxStartTsCollector() = default;

    void Start();
    void Shutdown();

    uint64_t GlobalMinSiTxStartTs()
    {
        CODE_FAULT_INJECTOR("stop_safely_clean_archives", {
            std::cout << "stop_safely_clean_archives" << std::endl;
            return 1U;
        });
        return min_start_ts_.load(std::memory_order_relaxed);
    }

    void SetDelaySeconds(uint32_t secs)
    {
        delay_seconds_ = secs;
    }
    uint32_t GetDelaySeconds()
    {
        return delay_seconds_;
    }

private:
    TxStartTsCollector(LocalCcShards *shards, uint32_t delay_seconds = 10);

    void Run();
    uint64_t CollectMinTxStartTs();

    std::atomic<bool> active_;

    // {ng_id -> min_tx_start_ts}
    std::unordered_map<uint32_t, uint64_t> min_start_ts_map_;
    std::atomic<uint64_t> min_start_ts_;
    // Run time period of scheduled recycling task
    uint32_t delay_seconds_;

    LocalCcShards *local_shards_;

    std::thread thd_;
};

}  // namespace txservice
