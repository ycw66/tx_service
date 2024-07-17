#pragma once

#include <algorithm>
#include <memory>
#include <utility>

#include "cc_entry.h"
#include "cc_req_misc.h"
#include "cc_request.h"
#include "cc_shard.h"
#include "tx_record.h"
#include "type.h"

namespace txservice
{
/**
 * Procedure of cleaning one page is divided into two subprocedure: Mark and
 * Compact. Mark marks those to-be-cleaned entries by assigning them to nullptr.
 * Compact erase those to-be-cleaned key/entries. Note that when a page's first
 * key can be cleanned, the ccmp_ has to erase/update its node key.
 *
 * Mark is done by LocalCcShards::KickoutPage.
 */
template <typename KeyT, typename ValueT>
struct CcPageCleanGuard
{
public:
    explicit CcPageCleanGuard(CcShard *cc_shard,
                              NodeGroupId cc_ng_id,
                              const TableName &table_name,
                              CcPage<KeyT, ValueT> *page)
        : cc_shard_(cc_shard),
          cc_ng_id_(cc_ng_id),
          table_name_(table_name),
          page_(page)
    {
    }

    virtual ~CcPageCleanGuard() = default;

    virtual bool CleanSuccess() const = 0;

    virtual void Compact()
    {
        // Those to-be-cleaned entries have been assigned to nullptr. Erase
        // those keys/entries now.
        auto key_insert_it = page_->keys_.begin();
        auto entry_insert_it = page_->entries_.begin();
        for (size_t idx = 0; idx < page_->Size(); ++idx)
        {
            if (page_->entries_[idx])
            {
                *key_insert_it++ = std::move(page_->keys_[idx]);
                *entry_insert_it++ = std::move(page_->entries_[idx]);
            }
        }
        page_->keys_.erase(key_insert_it, page_->keys_.end());
        page_->entries_.erase(entry_insert_it, page_->entries_.end());
    }

    size_t CleanCount() const
    {
        return clean_cnt_;
    }

protected:
    virtual bool CanBeCleaned(const CcEntry<KeyT, ValueT> *cce) const = 0;

    virtual bool IsCleanTarget(const KeyT &key,
                               const CcEntry<KeyT, ValueT> *cce) const = 0;

    virtual void Reserve(const CcEntry<KeyT, ValueT> *cce,
                         bool is_clean_target) = 0;

    virtual bool NeedInvalidateLockTerm() const = 0;

    virtual bool RemoveFromKeyCache() const = 0;

#ifdef RANGE_PARTITION_ENABLED
    size_t MarkCleanInRange(TemplateStoreRange<KeyT> *store_range,
                            size_t idx_in_page,
                            bool &kickout_any)
    {
        kickout_any = false;
        bool remove_from_key_cache = RemoveFromKeyCache();

        size_t range_end_idx;
        const KeyT *range_end_key = store_range->RangeEndKey();
        if (range_end_key)
        {
            assert(page_->keys_[0] <= *range_end_key);
            range_end_idx = page_->LowerBound(*range_end_key);
        }
        else
        {
            range_end_idx = page_->Size();  // nullptr indicates +00.
        }

        while (idx_in_page < range_end_idx)
        {
            const KeyT &start_key = page_->keys_[idx_in_page];

            // Clean next slice.
            TemplateStoreSlice<KeyT> *store_slice =
                store_range->FindSlice(start_key);
            assert(store_slice);

            size_t slice_end_idx;
            const KeyT *slice_end_key = store_slice->EndKey();
            if (slice_end_key)
            {
                assert(page_->keys_[0] <= *slice_end_key);
                slice_end_idx = page_->LowerBound(*slice_end_key);
            }
            else
            {
                slice_end_idx = page_->Size();  // nullptr indicates +00.
            }

            assert(slice_end_idx <= range_end_idx);

            bool slice_kicked = false;
            bool tried_slice_kick = false;

            for (size_t idx = idx_in_page; idx < slice_end_idx; ++idx)
            {
                KeyT &key = page_->keys_[idx];
                auto &cce = page_->entries_[idx];

                bool is_clean_target = IsCleanTarget(key, cce.get());
                bool can_be_cleaned = CanBeCleaned(cce.get());

                if (is_clean_target && can_be_cleaned)
                {
                    if (!tried_slice_kick)
                    {
                        slice_kicked = store_slice->Kickout();
                        tried_slice_kick = true;
                    }

                    if (slice_kicked)
                    {
                        // The key cache contains all keys in this range, but
                        // when we delete a key from the range, the update is
                        // delayed until the cce is removed from ccmap. This is
                        // because we always search for key in ccmap first
                        // before trying to query the key cache. Remove the key
                        // from key cache if the key is in deleted status.
                        // In certain special cases like cleaning up cces that
                        // no longer belong to this range, we remove keys from
                        // key cache as long as it is not in unknown status.
                        // This is because cces in unknown status is not added
                        // to the key cache yet.
                        if (txservice_enable_key_cache &&
                            table_name_.IsBase() &&
                            ((remove_from_key_cache &&
                              cce->PayloadStatus() != RecordStatus::Unknown) ||
                             cce->PayloadStatus() == RecordStatus::Deleted))
                        {
                            store_range->DeleteKey(
                                key, cc_shard_->core_id_, store_slice);
                        }

                        MarkClean(cc_ng_id_, std::move(cce));
                        continue;
                    }
                }

                Reserve(cce.get(), is_clean_target);
            }

            idx_in_page = slice_end_idx;
            kickout_any = kickout_any || slice_kicked;
        }

        return idx_in_page;
    }
#endif

    /**
     * @brief Mark a key if it is can be cleanned.
     *
     * Under range partition, OrphanKey is the key whose StoreRange metadata has
     * been kicked out.
     * Under hash partition, regards every key as OrphanKey.
     */
    void MarkCleanForOrphanKey(const KeyT &key,
                               std::unique_ptr<CcEntry<KeyT, ValueT>> &cce)
    {
        bool is_clean_target = IsCleanTarget(key, cce.get());
        bool can_be_cleaned = CanBeCleaned(cce.get());

        if (is_clean_target && can_be_cleaned)
        {
            MarkClean(cc_ng_id_, std::move(cce));
        }
        else
        {
            Reserve(cce.get(), is_clean_target);
        }
    }

    void MarkClean(NodeGroupId cc_ng_id,
                   std::unique_ptr<CcEntry<KeyT, ValueT>> cce)
    {
        // Check if the cce has any locks on it. If so recycle
        // the lock entry before deleting cce.
        bool delay_free = false;
        if (cce->GetKeyLock() && !cce->GetKeyLock()->IsEmpty())
        {
            // Do not free this cce directly since it might be visited
            // by an expired cc req. Put it into the invalid cce pool
            // and recycle it later.
            DLOG(WARNING) << "Cleanning up cce that still being referenced, "
                             "adding it to invalid cce list. cce: "
                          << cce.get();
            delay_free = true;
        }
        cce->ClearLocks(*cc_shard_, cc_ng_id);
        if (delay_free)
        {
            cc_shard_->AddInvalidCce(std::move(cce));
        }
        else
        {
            cce.reset(nullptr);  // Set cce to nullptr to indicate deleting.
        }
        ++clean_cnt_;
    }

protected:
    CcShard *cc_shard_{nullptr};
    NodeGroupId cc_ng_id_{0};
    const TableName &table_name_;
    CcPage<KeyT, ValueT> *page_{nullptr};
    uint64_t last_commit_ts_{0};
    uint64_t clean_cnt_{0};

    friend class LocalCcShards;
};

template <typename KeyT, typename ValueT>
struct CcPageCleanGuardWithoutKickoutCc : public CcPageCleanGuard<KeyT, ValueT>
{
public:
    explicit CcPageCleanGuardWithoutKickoutCc(CcShard *cc_shard,
                                              NodeGroupId cc_ng_id,
                                              const TableName &table_name_,
                                              CcPage<KeyT, ValueT> *page)
        : CcPageCleanGuard<KeyT, ValueT>(cc_shard, cc_ng_id, table_name_, page)
    {
    }

    bool CleanSuccess() const
    {
        // If we're just doing regular page clean, clean_succecss is always
        // true.
        return true;
    }

private:
    bool CanBeCleaned(const CcEntry<KeyT, ValueT> *cce) const override
    {
        return cce->IsFree();
    }

    bool IsCleanTarget(const KeyT &key,
                       const CcEntry<KeyT, ValueT> *cce) const override
    {
        // If we're just doing regular page clean, all cce is specific clean
        // target.
        return true;
    }

    void Reserve(const CcEntry<KeyT, ValueT> *cce,
                 bool is_clean_target) override
    {
        assert(is_clean_target);
    }

    bool NeedInvalidateLockTerm() const override
    {
        return false;
    }

    // Returns true if the key should be removed from key cache regardless
    // of rec status.
    bool RemoveFromKeyCache() const override
    {
        return false;
    }
};

template <typename KeyT, typename ValueT>
struct CcPageCleanGuardWithKickoutCc : public CcPageCleanGuard<KeyT, ValueT>
{
public:
    CcPageCleanGuardWithKickoutCc(CcShard *cc_shard,
                                  NodeGroupId cc_ng_id,
                                  const TableName &table_name,
                                  CcPage<KeyT, ValueT> *page,
                                  const KickoutCcEntryCc *kickout_cc)
        : CcPageCleanGuard<KeyT, ValueT>(cc_shard, cc_ng_id, table_name, page),
          kickout_cc_(kickout_cc),
          need_invalidate_lock_term_(
              DeduceNeedInvalidateLockTerm(kickout_cc->GetCleanType())),
          clean_success_(true)
    {
        assert(kickout_cc);
    }

    bool CleanSuccess() const override
    {
        return clean_success_;
    }

    void Compact() override
    {
        CcPageCleanGuard<KeyT, ValueT>::Compact();

        UpdatePageDirtyCommitTs();
    }

private:
    static bool DeduceNeedInvalidateLockTerm(CleanType type)
    {
        if (type == CleanType::CleanRangeData ||
            type == CleanType::CleanRangeDataForMigration ||
            type == CleanType::CleanBucketData)
        {
            // If the ccentry that expect to clean still has lock on it,
            // it must be that the owner of this lock has failed. The
            // reason is that the lock owner must have acquired
            // range/bucket read lock before accessing data in
            // range/bucket. And if we're doing clean data on the
            // range/bucket, that means the DDL has acquired write lock
            // on this range/bucket on all ngs. So it must be that the
            // data lock owner ng has failed and the read lock has
            // expired. In this case invalidate the lock term so that if
            // the failed node tries to access data with the deleted cce
            // addr, we can reject the request.
            return true;
        }
        else
        {
            return false;
        }
    }

private:
    bool CanBeCleaned(const CcEntry<KeyT, ValueT> *cce) const override
    {
        return kickout_cc_->CanBeCleaned(cce);
    }

    bool IsCleanTarget(const KeyT &key,
                       const CcEntry<KeyT, ValueT> *cce) const override
    {
        return kickout_cc_->IsCleanTarget(key, cce);
    }

    void Reserve(const CcEntry<KeyT, ValueT> *cce,
                 bool is_clean_target) override
    {
        if (is_clean_target)
        {
            clean_success_ = false;
        }
    }

    bool NeedInvalidateLockTerm() const override
    {
        return need_invalidate_lock_term_;
    }

    bool RemoveFromKeyCache() const override
    {
        // When kicking data that no longer belongs to this range,
        // we should remove the key regardless of its rec status.
        return kickout_cc_->GetCleanType() == CleanType::CleanRangeData;
    }

    void UpdatePageDirtyCommitTs()
    {
        CcPage<KeyT, ValueT> *page = CcPageCleanGuard<KeyT, ValueT>::page_;

        if (!page->Empty())
        {
            // During range split kickout, we might clean cc entries that
            // are still dirty from page. So the max dirty ts might
            // decrease.
            auto commit_ts_less =
                [](const std::unique_ptr<CcEntry<KeyT, ValueT>> &left,
                   const std::unique_ptr<CcEntry<KeyT, ValueT>> &right)
            { return left->CommitTs() < right->CommitTs(); };

            uint64_t page_max_commit_ts =
                (*std::max_element(page->entries_.begin(),
                                   page->entries_.end(),
                                   commit_ts_less))
                    ->CommitTs();
            page->last_dirty_commit_ts_ =
                std::min(page_max_commit_ts, page->last_dirty_commit_ts_);
        }
        else
        {
            page->last_dirty_commit_ts_ = 0;
        }
    }

private:
    const KickoutCcEntryCc *kickout_cc_;

    bool need_invalidate_lock_term_;

    bool clean_success_;
};

}  // namespace txservice
