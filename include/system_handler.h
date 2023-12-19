#pragma once

#include <functional>

namespace txservice
{
// This is used to callback functions in app server, such as MariaDB, on every
// node.
class SystemHandler
{
public:
    virtual ~SystemHandler() = default;

    // C++ std::future doesn't contain a then() method. Pass the continuation as
    // the done callback.
    virtual void ReloadCache(std::function<void(bool)> done)
    {
        done(true);
    }
};
}  // namespace txservice
