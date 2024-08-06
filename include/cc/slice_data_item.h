#pragma once

#include <memory>

#include "tx_key.h"
#include "tx_record.h"

namespace txservice
{
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
