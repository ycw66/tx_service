#pragma once

#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>
#include <vector>

#include "cc_handler_result.h"
#include "scan.h"
#include "tx_key.h"
#include "tx_record.h"
#include "type.h"

namespace txservice
{
struct ScanOpenCc;
struct ScanBatchCc;
struct ScanCloseCc;

class CcMapScanner;

enum class ScannerStatus
{
    Open,
    Closed,
    Blocked
};

class CcScanner;

struct ScanCache
{
public:
    static constexpr size_t ScanBatchSize = 128;

    ScanCache(CcScanner *scanner) : idx_(0), size_(0), scanner_(scanner)
    {
    }

    ScanCache(ScanCache &&rhs) noexcept
        : idx_(rhs.idx_), size_(rhs.size_), scanner_(rhs.scanner_)
    {
    }

    virtual ~ScanCache() = default;

    ScannerStatus Status() const
    {
        if (idx_ < size_)
        {
            return ScannerStatus::Open;
        }
        else if (size_ == ScanCache::ScanBatchSize)
        {
            return ScannerStatus::Blocked;
        }
        else
        {
            return ScannerStatus::Closed;
        }
    }

    void MoveNext()
    {
        ++idx_;
    }

    void Reset()
    {
        idx_ = 0;
        size_ = 0;
    }

    void Rewind()
    {
        idx_ = 0;
    }

    size_t Size() const
    {
        return size_;
    }

    bool Full() const
    {
        return size_ == ScanCache::ScanBatchSize;
    }

    CcScanner *Scanner() const
    {
        return scanner_;
    }

    virtual ScanTuple *AddScanTuple(const std::string &key_str,
                                    uint64_t key_ts,
                                    const std::string &record_str,
                                    RecordStatus rec_status,
                                    uint64_t gap_ts,
                                    uint64_t cce_ptr,
                                    int64_t term,
                                    uint32_t shard_id,
                                    bool is_ckpt_delta = false) = 0;

    virtual const ScanTuple *LastTuple() const = 0;

protected:
    size_t idx_;
    size_t size_;
    CcScanner *const scanner_;
};

template <typename KeyT, typename ValueT>
struct TemplateScanCache : public ScanCache
{
public:
    TemplateScanCache() = delete;

    TemplateScanCache(CcScanner *scanner, const Schema *key_schema)
        : ScanCache(scanner), cache_(), key_schema_(key_schema)
    {
    }

    TemplateScanCache(TemplateScanCache &&rhs)
        : cache_(rhs.cache_),
          key_schema_(rhs.key_schema_),
          ScanCache(std::move(rhs))
    {
    }

    TemplateScanCache(const TemplateScanCache &rhs) = delete;

    ~TemplateScanCache() = default;

    TemplateScanTuple<KeyT, ValueT> *AddScanTuple()
    {
        assert(size_ < cache_.size());

        TemplateScanTuple<KeyT, ValueT> *scan_t = &cache_.at(size_);
        ++size_;

        return scan_t;
    }

    ScanTuple *AddScanTuple(const std::string &key_str,
                            uint64_t key_ts,
                            const std::string &record_str,
                            RecordStatus rec_status,
                            uint64_t gap_ts,
                            uint64_t cce_ptr,
                            int64_t term,
                            uint32_t ng_id,
                            bool is_ckpt_delta = false) override
    {
        assert(size_ < cache_.size());

        TemplateScanTuple<KeyT, ValueT> &scan_tuple = cache_.at(size_);

        scan_tuple.key_ts_ = key_ts;
        if (key_ts > 0)
        {
            // When the key's timestamp is 0, the tuple's key is not included in
            // this scan. Only deserializes the key when the key is included.
            size_t offset = 0;
            scan_tuple.Key().Deserialize(key_str.data(), offset, key_schema_);
        }

        scan_tuple.rec_status_ = rec_status;
        if (rec_status == RecordStatus::Normal ||
            (rec_status == RecordStatus::Deleted && is_ckpt_delta))
        {
            size_t offset = 0;
            scan_tuple.Record().Deserialize(record_str.data(), offset);
        }

        scan_tuple.gap_ts_ = gap_ts;
        scan_tuple.cce_addr_.SetCce(cce_ptr, term, ng_id);

        ++size_;
        return &scan_tuple;
    }

    const TemplateScanTuple<KeyT, ValueT> *Current() const
    {
        return idx_ < size_ ? &cache_.at(idx_) : nullptr;
    }

    const TemplateScanTuple<KeyT, ValueT> *Last() const
    {
        return &cache_.at(size_ - 1);
    }

    const ScanTuple *LastTuple() const override
    {
        return Last();
    }

private:
    std::array<TemplateScanTuple<KeyT, ValueT>, ScanCache::ScanBatchSize>
        cache_;
    const Schema *const key_schema_;
};

class CcScanner
{
public:
    using Uptr = std::unique_ptr<CcScanner>;

    CcScanner(ScanDirection direction, ScanIndexType index_type)
        : direct_(direction),
          index_type_(index_type),
          drain_cache_mode_(false),
          is_ckpt_delta_(false)
    {
    }

    virtual ~CcScanner() = default;

    virtual CcScanner::Uptr Clone() const = 0;

    // virtual ScannerStatus MoveNext(const ScanTuple *&tuple) = 0;
    virtual uint32_t BlockedShard() const = 0;
    virtual ScanCache *Cache(uint32_t shard_code) = 0;
    virtual ScanCache *AddShard(uint32_t shard_code) = 0;
    virtual void ShardCacheSizes(
        std::vector<std::pair<uint32_t, size_t>> *shard_code_and_sizes) = 0;

    virtual const ScanTuple *Current() = 0;
    virtual ScannerStatus Status() const = 0;
    virtual void MoveNext() = 0;

    virtual void SetDrainCacheMode(bool drain_cache_mode) = 0;
    virtual bool GetDrainCacheMode() = 0;

    ScanDirection Direction() const
    {
        return direct_;
    }

    ScanIndexType IndexType() const
    {
        return index_type_;
    }

    LockType DeduceScanTupleLockType(const ScanTuple *scan_tuple)
    {
        if (scan_tuple == nullptr ||
            scan_tuple->rec_status_ == RecordStatus::Deleted)
        {
            return LockType::NoLock;
        }
        CcOperation cc_op = CcOperation::Read;
        if (index_type_ == ScanIndexType::Secondary)
        {
            cc_op = CcOperation::ReadSkIndex;
        }
        else if (is_for_write_)
        {
            cc_op = CcOperation::ReadForWrite;
        }
        return LockTypeUtil::DeduceLockType(cc_op, iso_level_, protocol_);
    }

protected:
    ScanDirection direct_;
    ScanIndexType index_type_;
    // In drain cache mode, Movenext/Current will drain out the cached the
    // tuples in each buckets
    bool drain_cache_mode_{false};

public:
    bool read_local_{false};
    bool is_ckpt_delta_{false};
    bool is_for_write_{false};
    IsolationLevel iso_level_{IsolationLevel::ReadCommitted};
    CcProtocol protocol_{CcProtocol::OCC};
};

template <typename KeyT, typename ValueT>
class TemplateCcScanner : public CcScanner
{
public:
    TemplateCcScanner(ScanDirection direct,
                      ScanIndexType index_type,
                      const Schema *schema)
        : CcScanner(direct, index_type),
          scans_(),
          curr_shard_code_(0),
          curr_tuple_(nullptr),
          status_(ScannerStatus::Open),
          key_schema_(schema)
    {
    }

    ~TemplateCcScanner() = default;

    CcScanner::Uptr Clone() const override
    {
        return std::make_unique<TemplateCcScanner<KeyT, ValueT>>(
            direct_, index_type_, key_schema_);
    }

    ScanCache *AddShard(uint32_t shard_code) override
    {
        std::unique_lock<std::mutex> lock(mutex_);
        auto em_it = scans_.try_emplace(shard_code, this, key_schema_);
        assert(em_it.second == true);
        return &em_it.first->second;
    }

    uint32_t BlockedShard() const override
    {
        return curr_shard_code_;
    }

    ScanCache *Cache(uint32_t shard_code) override
    {
        return &scans_.at(shard_code);
    }

    void ShardCacheSizes(
        std::vector<std::pair<uint32_t, size_t>> *shard_code_and_sizes) override
    {
        std::unique_lock<std::mutex> lock(mutex_);
        for (const auto &[shard_code, cache] : scans_)
        {
            shard_code_and_sizes->emplace_back(shard_code, cache.Size());
        }
    }

    const ScanTuple *Current() override
    {
        if (curr_tuple_ != nullptr)
        {
            return curr_tuple_;
        }
        else if (status_ == ScannerStatus::Closed)
        {
            return nullptr;
        }

        const KeyT *min_key = nullptr;

        for (const auto &[shard_code, cache] : scans_)
        {
            const TemplateScanTuple<KeyT, ValueT> *tuple = cache.Current();

            if (tuple != nullptr)
            {
                if (min_key == nullptr || tuple->key_ts_ == 0 ||
                    (direct_ == ScanDirection::Forward &&
                     tuple->key_obj_ < *min_key) ||
                    (direct_ == ScanDirection::Backward &&
                     !(tuple->key_obj_ < *min_key)))
                {
                    min_key = &tuple->key_obj_;
                    curr_shard_code_ = shard_code;

                    if (tuple->key_ts_ == 0)
                    {
                        // When the key's timestamp is 0, the key is not
                        // included in the scan results. The scan tuple is
                        // only returned for later validation. Since the
                        // key is not included in the results, the key's
                        // relative order w.r.t. other keys is irrelevant.
                        // Hence, we terminate merging early and returns the
                        // tuple immediately. The upper-layer tx will bookkeep
                        // the gap in the scan set and skips the key.
                        min_key = &tuple->key_obj_;
                        curr_shard_code_ = shard_code;
                        break;
                    }
                }
            }
            else
            {
                if (cache.Status() == ScannerStatus::Blocked)
                {
                    if (!drain_cache_mode_)
                    {
                        status_ = ScannerStatus::Blocked;
                        curr_shard_code_ = shard_code;
                        curr_tuple_ = nullptr;

                        return nullptr;
                    }
                    {
                        // In drain_cache_mode_, we just it iterate all cache
                    }
                }
            }
        }

        if (min_key == nullptr)
        {
            curr_tuple_ = nullptr;
            status_ = ScannerStatus::Closed;
            return nullptr;
        }
        else
        {
            curr_tuple_ = scans_.at(curr_shard_code_).Current();
            status_ = ScannerStatus::Open;
            return curr_tuple_;
        }
    }

    ScannerStatus Status() const override
    {
        return status_;
    }

    void MoveNext() override
    {
        if (curr_tuple_ != nullptr)
        {
            scans_.at(curr_shard_code_).MoveNext();
            curr_tuple_ = nullptr;
        }
        else if (status_ != ScannerStatus::Closed)
        {
            curr_tuple_ =
                static_cast<const TemplateScanTuple<KeyT, ValueT> *>(Current());
            if (curr_tuple_ != nullptr)
            {
                // The scanner is not blocked. Advances the cache that produces
                // the min/max key.
                scans_.at(curr_shard_code_).MoveNext();
                curr_tuple_ = nullptr;
            }
        }
    }

    void SetDrainCacheMode(bool drain_cache_mode) override
    {
        std::unique_lock<std::mutex> lock(mutex_);
        drain_cache_mode_ = drain_cache_mode;
    }

    bool GetDrainCacheMode() override
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return drain_cache_mode_;
    }

private:
    /// <summary>
    /// A collection of local and remote scan caches, one per core.
    /// </summary>
    std::unordered_map<uint32_t, TemplateScanCache<KeyT, ValueT>> scans_;

    uint32_t curr_shard_code_;
    const TemplateScanTuple<KeyT, ValueT> *curr_tuple_;
    ScannerStatus status_;

    const Schema *key_schema_;
    std::mutex mutex_;
};
}  // namespace txservice
