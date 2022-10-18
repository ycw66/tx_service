//
// Created by pangzhenzhou on 2022/10/8.
//
#pragma once
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace txservice::metrics
{
using Meter = std::function<void(double)>;
using MetricsLabels = std::vector<std::pair<std::string, std::string>>;

struct MetricsNaming
{
    enum class Type
    {
        Gauge = 0x00,
        Counter = 0x11,
        Histograms = 0x12,
    };
    std::string name;
    Type type;
};

enum class MetricsErrors
{
    Success = 1000,
    OpenErr = -1001,
};
/// <summary>
/// The entry class for metrics collection is MetricsRegistry. The purpose of
/// this class is to help initialize the context of the metrics collector and to
/// provide different implementations of the metrics collector. Generally only
/// need one MetricsRegistry instance per application. The metrics collection is
/// related to the runtime and deployment environment. We currently offer a
/// Prometheus-based implementation.
/// </summary>
class MetricsRegistry
{
public:
    MetricsRegistry() = default;

    virtual MetricsErrors Open() = 0;

    virtual std::unique_ptr<Meter> Register(MetricsNaming &&,
                                            MetricsLabels &&) = 0;

    virtual ~MetricsRegistry() = default;
};
}  // namespace txservice::metrics
