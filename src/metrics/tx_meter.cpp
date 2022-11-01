//
// Created by pangzhenzhou on 2022/10/28.
//
#include "metrics/tx_meter.h"

#include <cassert>

namespace txservice::metrics
{
std::size_t MetricsNamingHash::operator()(
    const txservice::metrics::MetricsNaming &metric_naming) const
{
    std::size_t seed = 0xff;
    std::size_t name_hash_code = std::hash<std::string>()(metric_naming.name);
    auto type_hash_code = static_cast<std::size_t>(metric_naming.type);

    seed ^= name_hash_code + 0x9e3779b9 + type_hash_code + (seed << 6) +
            (seed >> 2);
    return seed;
}

TxMeter::TxMeter(metrics::MetricsRegistry *metrics_registry_,
                 size_t node_id,
                 size_t thd_id)
    : common_label{{"node_id", std::to_string(node_id)},
                   {"cord_id", std::to_string(thd_id)}},
      metrics_registry{metrics_registry_}
{
}

metrics::MetricsLabels TxMeter::CommonLabel()
{
    return common_label;
}

std::unique_ptr<metrics::MeterV2> TxMeter::GetMeter(
    metrics::MetricsNaming &&naming,
    std::optional<metrics::MetricsLabels> custom_label)
{
    metrics::MetricsLabels final_labels{common_label};
    if (custom_label.has_value())
    {
        custom_label.value();
        final_labels.insert(final_labels.end(),
                            custom_label.value().begin(),
                            custom_label.value().end());
    }
    return metrics_registry->RegisterV2(std::move(naming),
                                        std::move(final_labels));
}
};  // namespace txservice::metrics
