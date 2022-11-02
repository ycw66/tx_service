//
// Created by pangzhenzhou on 2022/10/28.
//
#pragma once
#include <memory>
#include <optional>
#include <unordered_map>
#include <vector>

#include "metrics.h"

namespace txservice::metrics
{
struct MetricsNamingHash
{
    std::size_t operator()(const MetricsNaming &metric_naming) const;
};

class TxMeter
{
public:
    TxMeter(const TxMeter &) = delete;

    TxMeter &operator=(const TxMeter &) = delete;

    TxMeter() = delete;

    ~TxMeter() = default;

    TxMeter(metrics::MetricsRegistry *metrics_registry_,
            size_t node_id,
            size_t thd_id);

    std::unique_ptr<metrics::MeterV2> GetMeter(
        metrics::MetricsNaming &&naming,
        std::optional<metrics::MetricsLabels> custom_label);

    metrics::MetricsLabels CommonLabel();

private:
    const metrics::MetricsLabels common_label;
    metrics::MetricsRegistry *metrics_registry;
};

}  // namespace txservice::metrics
