#pragma once

#include "tx_key.h"

namespace txservice
{
enum struct SliceStatus
{
    /**
     * @brief The slice's data are not fully cached in memory.
     *
     */
    PartiallyCached = 0,
    /**
     * @brief The slice's data are fully cached in memory.
     *
     */
    FullyCached,
    /**
     * @brief The slice's data are being loaded into memory. The flag prevents
     * cache replacement from kicking out records from the slice while a cc
     * request is loading the slice's records into cc maps.
     *
     */
    BeingLoaded
};

struct SliceInitInfo
{
    SliceInitInfo() = delete;
    explicit SliceInitInfo(TxKey key, uint32_t size, SliceStatus status)
        : key_(std::move(key)), size_(size), status_(status)
    {
    }

    SliceInitInfo(SliceInitInfo &&rhs)
        : key_(std::move(rhs.key_)), size_(rhs.size_), status_(rhs.status_)
    {
    }

    SliceInitInfo(const SliceInitInfo &rhs) = delete;

    TxKey key_;
    uint32_t size_;
    SliceStatus status_;
};
}  // namespace txservice