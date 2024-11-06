#pragma once

#include <memory>
#include <string>

#include "tx_key.h"
#include "tx_record.h"

namespace txservice
{
struct RawSliceDataItem
{
    RawSliceDataItem() = delete;

    RawSliceDataItem(std::string &&key_str,
                     std::string &&rec_str,
                     uint64_t version_ts,
                     bool is_deleted)
        : key_str_(std::move(key_str)),
          rec_str_(std::move(rec_str)),
          version_ts_(version_ts),
          is_deleted_(is_deleted)
    {
    }

    std::string key_str_;
    std::string rec_str_;
    uint64_t version_ts_;
    bool is_deleted_;
};
struct SliceDataItem
{
    SliceDataItem() = delete;

    SliceDataItem(txservice::TxKey key,
                  std::unique_ptr<txservice::TxRecord> &&rec,
                  uint64_t version_ts,
                  bool is_deleted)
        : key_(std::move(key)),
          record_(std::move(rec)),
          version_ts_(version_ts),
          is_deleted_(is_deleted)
    {
    }

    txservice::TxKey key_;
    std::unique_ptr<txservice::TxRecord> record_;
    uint64_t version_ts_;
    bool is_deleted_;
};
}  // namespace txservice
