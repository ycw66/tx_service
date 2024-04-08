#pragma once
#include <cstddef>
#include <memory>

#include "meter.h"
#include "metrics.h"

namespace metrics
{
inline std::unique_ptr<metrics::Meter> kv_meter{nullptr};
inline const metrics::Name NAME_KV_FLUSH_ROWS_TOTAL{"kv_flush_rows_total"};
inline const metrics::Name NAME_KV_LOAD_SLICE_TOTAL{"kv_load_slice_total"};
inline const metrics::Name NAME_KV_LOAD_SLICE_DURATION{
    "kv_load_slice_duration"};
inline const metrics::Name NAME_KV_READ_TOTAL{"kv_read_total"};
inline const metrics::Name NAME_KV_READ_DURATION{"kv_read_duration"};

inline const metrics::Name NAME_MEMORY_LIMIT{"memory_limit"};
inline const metrics::Name NAME_CACHE_HIT_OR_MISS_TOTAL{
    "cache_hit_or_miss_total"};
inline const metrics::Name NAME_MEMORY_USAGE{"memory_usage"};

inline const metrics::Name NAME_BUSY_ROUND_DURATION{"busy_round_duration"};
inline const metrics::Name NAME_BUSY_ROUND_ACTIVE_TX_COUNT{
    "busy_round_active_tx_count"};
inline const metrics::Name NAME_BUSY_ROUND_PROCESSED_CC_REQUEST_COUNT{
    "busy_round_processed_cc_request_count"};
inline const metrics::Name NAME_EMPTY_ROUND_RATIO{"empty_round_ratio"};

inline const metrics::Name NAME_TX_DURATION{"tx_duration"};
inline const metrics::Name NAME_TX_PROCESSED_TOTAL{"tx_processed_total"};
inline const metrics::Name NAME_REMOTE_REQUEST_DURATION{
    "remote_request_duration"};
inline const metrics::Name NAME_IN_FLIGHT_REMOTE_REQUEST_COUNT{
    "remote_request_in_flight_count"};

inline bool enable_memory_usage{false};
inline bool enable_cache_hit_rate{false};
inline bool enable_tx_metrics{false};
inline bool enable_remote_request_metrics{false};
inline bool enable_busy_round_metrics{false};
inline bool enable_kv_metrics{false};

inline size_t collect_memory_usage_round{0};
inline size_t collect_tx_duration_round{0};
inline size_t busy_round_threshold{0};
}  // namespace metrics
