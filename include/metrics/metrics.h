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
// FIXME: The code duplicates the value type in mono_metrics
struct Value
{
    enum IncDecValue
    {
        Increment,
        Decrement,
        None,
    };
    IncDecValue inc_dec;
    double value;

    Value() = delete;

    explicit Value(IncDecValue inc_dec_) : inc_dec{inc_dec_}, value{0}
    {
    }

    explicit Value(double value_) : inc_dec{None}, value{value_}
    {
    }
};

using Meter = std::function<void(double)>;
using MeterV2 = std::function<void(Value)>;
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

    bool operator==(const MetricsNaming &naming) const
    {
        return name == naming.name && type == naming.type;
    }
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

    virtual std::unique_ptr<MeterV2> RegisterV2(MetricsNaming &&,
                                                MetricsLabels &&) = 0;
    virtual ~MetricsRegistry() = default;
};
}  // namespace txservice::metrics
