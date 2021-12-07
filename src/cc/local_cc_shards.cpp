#include "cc/local_cc_shards.h"

namespace txservice
{
std::atomic<uint64_t> LocalCcShards::local_clock(0);

LocalCcShards::LocalCcShards(uint32_t node_id,
                             uint16_t core_cnt,
                             Catalog *catalog)
    : node_id_(node_id), timer_terminate_(false)
{
    using namespace std::chrono_literals;
    uint64_t ts_base = std::chrono::duration_cast<std::chrono::microseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count();

    for (uint16_t thd_idx = 0; thd_idx < core_cnt; ++thd_idx)
    {
        cc_shards_.emplace_back(std::make_unique<CcShard>(
            thd_idx, core_cnt, ts_base, node_id, catalog));
    }

    timer_thd_ = std::thread([this] { TimerRun(); });
}

uint64_t LocalCcShards::ClockTs()
{
    return LocalCcShards::local_clock.load(std::memory_order_acquire);
}

void LocalCcShards::TimerRun()
{
    while (!timer_terminate_.load(std::memory_order_acquire))
    {
        using namespace std::chrono_literals;

        uint64_t clock_ts =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count();
        LocalCcShards::local_clock.store(clock_ts, std::memory_order_relaxed);

        for (std::unique_ptr<CcShard> &ccs : cc_shards_)
        {
            uint64_t tsb = ccs->ts_base_.load(std::memory_order_acquire);
            // If the CAS fails, since timestamps always roll forward, the
            // ts base must be greater than the current time or the old ts
            // base.
            ccs->ts_base_.compare_exchange_strong(tsb, std::max(tsb, clock_ts));
        }

        std::this_thread::sleep_for(2s);
    }
}
}  // namespace txservice