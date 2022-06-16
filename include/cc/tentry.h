#pragma once

#include <memory>

namespace txservice
{
/*
 * TEntry describes a transaction.
 */
struct TEntry
{
public:
    using Uptr = std::unique_ptr<TEntry>;

    TEntry() = delete;
    TEntry(const TEntry &rhs) = delete;

    TEntry(uint32_t vec_idx)
        : commit_ts_(0),
          lower_bound_(0),
          status_(TxnStatus::Finished),
          ident_(UINT32_MAX),
          vec_idx_(vec_idx)
    {
    }

    TEntry(TEntry &&rhs) noexcept
        : commit_ts_(rhs.commit_ts_),
          lower_bound_(rhs.lower_bound_),
          status_(rhs.status_),
          ident_(rhs.ident_),
          vec_idx_(rhs.vec_idx_)
    {
    }

    void Reset(uint64_t start_ts, uint32_t tx_ident, int64_t term)
    {
        ident_ = tx_ident;
        commit_ts_ = 0;
        lower_bound_ = start_ts;
        status_ = TxnStatus::Ongoing;
        term_ = term;
    }

    TxId GetTxId(uint32_t core_id) const
    {
        return TxId(core_id, ident_, vec_idx_);
    }

    uint64_t commit_ts_;
    uint64_t lower_bound_;  // start_ts
    TxnStatus status_;
    uint32_t ident_;
    const uint32_t vec_idx_;
    int64_t term_;
};
}  // namespace txservice
