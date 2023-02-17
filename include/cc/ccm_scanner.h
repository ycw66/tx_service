#pragma once

#include <chrono>
#include <memory>
#include <mutex>
#include <queue>
#include <unordered_map>
#include <utility>
#include <vector>

#include "butil/logging.h"
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
    Open = 0,
    Closed,
    Blocked
};

class CcScanner;

struct ScanCache
{
public:
    static constexpr size_t ScanBatchSize = 128;

    ScanCache(CcScanner *scanner)
        : idx_(0), size_(0), scanner_(scanner), mem_size_(0)
    {
    }

    ScanCache(size_t idx, size_t size, CcScanner *scanner)
        : idx_(idx), size_(size), scanner_(scanner), mem_size_(0)
    {
    }

    ScanCache(ScanCache &&rhs) noexcept
        : idx_(rhs.idx_),
          size_(rhs.size_),
          scanner_(rhs.scanner_),
          mem_size_(rhs.mem_size_)
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
        mem_size_ = 0;
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
                                    uint32_t core_id,
                                    uint32_t shard_id,
                                    bool is_ckpt_delta = false) = 0;

    virtual const ScanTuple *LastTuple() const = 0;

protected:
    size_t idx_;
    size_t size_;
    CcScanner *const scanner_;
    uint32_t mem_size_{0};
};

template <typename KeyT, typename ValueT>
struct TemplateScanCache : public ScanCache
{
public:
    TemplateScanCache() = delete;

    TemplateScanCache(CcScanner *scanner, const Schema *key_schema)
        : ScanCache(scanner),
          cache_(ScanCache::ScanBatchSize),
          key_schema_(key_schema)
    {
        assert(cache_.size() == ScanCache::ScanBatchSize);
    }

    TemplateScanCache(TemplateScanCache &&rhs)
        : ScanCache(rhs.idx_, rhs.size_, rhs.scanner_),
          cache_(std::move(rhs.cache_)),
          key_schema_(rhs.key_schema_)
    {
    }

    TemplateScanCache(const TemplateScanCache &rhs) = delete;

    ~TemplateScanCache() = default;

    bool IsFull() const
    {
        return mem_size_ >= 1024;
    }

    TemplateScanTuple<KeyT, ValueT> *AddScanTuple()
    {
        TemplateScanTuple<KeyT, ValueT> *scan_t = nullptr;

        if (size_ < cache_.size())
        {
            scan_t = &cache_[size_];
        }
        else
        {
            TemplateScanTuple<KeyT, ValueT> &new_tuple = cache_.emplace_back();
            scan_t = &new_tuple;
        }
        ++size_;

        return scan_t;
    }

    void AddScanTupleSize(uint32_t tuple_size)
    {
        mem_size_ += tuple_size;
    }

    ScanTuple *AddScanTuple(const std::string &key_str,
                            uint64_t key_ts,
                            const std::string &record_str,
                            RecordStatus rec_status,
                            uint64_t gap_ts,
                            uint64_t cce_ptr,
                            int64_t term,
                            uint32_t core_id,
                            uint32_t ng_id,
                            bool is_ckpt_delta = false) override
    {
        assert(size_ <= cache_.size());

        TemplateScanTuple<KeyT, ValueT> *scan_tuple = nullptr;
        if (size_ < cache_.size())
        {
            scan_tuple = &cache_[size_];
        }
        else
        {
            TemplateScanTuple<KeyT, ValueT> &new_tuple = cache_.emplace_back();
            scan_tuple = &new_tuple;
        }

        scan_tuple->key_ts_ = key_ts;
        if (key_ts > 0)
        {
            // When the key's timestamp is 0, the tuple's key is not included in
            // this scan. Only deserializes the key when the key is included.
            size_t offset = 0;
            scan_tuple->KeyObj().Deserialize(
                key_str.data(), offset, key_schema_);
        }

        scan_tuple->rec_status_ = rec_status;
        if (rec_status == RecordStatus::Normal ||
            (rec_status == RecordStatus::Deleted && is_ckpt_delta))
        {
            size_t offset = 0;
            scan_tuple->RecordObj().Deserialize(record_str.data(), offset);
        }

        scan_tuple->gap_ts_ = gap_ts;
        scan_tuple->cce_addr_.SetCce(cce_ptr, term, ng_id, core_id);

        ++size_;
        return scan_tuple;
    }

    const TemplateScanTuple<KeyT, ValueT> *Current() const
    {
        return idx_ < size_ ? &cache_.at(idx_) : nullptr;
    }

    const TemplateScanTuple<KeyT, ValueT> *Last() const
    {
        return size_ > 0 ? &cache_[size_ - 1] : nullptr;
    }

    const ScanTuple *LastTuple() const override
    {
        return Last();
    }

private:
    std::vector<TemplateScanTuple<KeyT, ValueT>> cache_;
    const Schema *const key_schema_;
};

enum struct CcmScannerType
{
    HashPartition = 0,
    RangePartition
};

class CcScanner
{
public:
    using Uptr = std::unique_ptr<CcScanner>;

    CcScanner(ScanDirection direction, ScanIndexType index_type)
        : direct_(direction),
          index_type_(index_type),
          status_(ScannerStatus::Open),
          drain_cache_mode_(false),
          is_ckpt_delta_(false)
    {
    }

    virtual ~CcScanner() = default;

    // virtual ScannerStatus MoveNext(const ScanTuple *&tuple) = 0;
    virtual uint32_t BlockedShard() const = 0;
    virtual ScanCache *Cache(uint32_t shard_code) = 0;
    virtual ScanCache *AddShard(uint32_t shard_code) = 0;
    virtual void ResetShards(size_t shard_cnt) = 0;
    virtual void ShardCacheSizes(
        std::vector<std::pair<uint32_t, size_t>> *shard_code_and_sizes) = 0;

    virtual const ScanTuple *Current() = 0;
    virtual CcmScannerType Type() const = 0;

    ScannerStatus Status() const
    {
        return status_;
    }

    void SetStatus(ScannerStatus status)
    {
        status_ = status;
    }

    virtual void MoveNext() = 0;

    virtual void SetDrainCacheMode(bool drain_cache_mode) = 0;
    virtual bool GetDrainCacheMode() = 0;
    virtual std::unique_ptr<TxKey> DecodeKey(const std::string &blob) const
    {
        return nullptr;
    }

    virtual int64_t PartitionNgTerm() const
    {
        return -1;
    }
    virtual void SetPartitionNgTerm(int64_t partition_ng_term)
    {
    }

    virtual uint32_t CacheCount() const = 0;

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
            (scan_tuple->rec_status_ == RecordStatus::Deleted &&
             !is_for_write_))
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
    ScannerStatus status_;
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
          key_schema_(schema)
    {
    }

    ScanCache *AddShard(uint32_t shard_code) override
    {
        std::unique_lock<std::mutex> lock(mutex_);
        auto em_it = scans_.try_emplace(shard_code, this, key_schema_);
        assert(em_it.second == true);
        return &em_it.first->second;
    }

    void ResetShards(size_t shard_cnt) override
    {
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

    CcmScannerType Type() const override
    {
        return CcmScannerType::HashPartition;
    }

    uint32_t CacheCount() const override
    {
        return scans_.size();
    }

private:
    /// <summary>
    /// A collection of local and remote scan caches, one per core.
    /// </summary>
    std::unordered_map<uint32_t, TemplateScanCache<KeyT, ValueT>> scans_;

    uint32_t curr_shard_code_;
    const TemplateScanTuple<KeyT, ValueT> *curr_tuple_;

    const Schema *key_schema_;
    std::mutex mutex_;
};

template <typename KeyT, typename ValueT, bool IsForward>
class RangePartitionedCcmScanner : public CcScanner
{
public:
    RangePartitionedCcmScanner(ScanDirection direct,
                               ScanIndexType index_type,
                               const Schema *schema)
        : CcScanner(direct, index_type), scans_(), key_schema_(schema)
    {
    }

    ScanCache *AddShard(uint32_t shard_code) override
    {
        size_t curr_size = scans_.size();
        if (shard_code >= curr_size)
        {
            scans_.reserve(shard_code + 1);
            for (size_t idx = curr_size; idx < shard_code + 1; ++idx)
            {
                scans_.emplace_back(this, key_schema_);
            }
        }

        for (size_t idx = 0; idx < curr_size; ++idx)
        {
            scans_[idx].Reset();
        }

        assert(shard_code < scans_.size());

        return &scans_[shard_code];
    }

    void ResetShards(size_t shard_cnt) override
    {
        size_t old_size = scans_.size();
        if (shard_cnt > old_size)
        {
            scans_.reserve(shard_cnt);
            for (size_t idx = old_size; idx < shard_cnt; ++idx)
            {
                scans_.emplace_back(this, key_schema_);
            }
        }
        else if (shard_cnt < old_size)
        {
            for (size_t idx = shard_cnt; idx < old_size; ++idx)
            {
                scans_.pop_back();
            }
        }

        assert(scans_.size() == shard_cnt);

        for (size_t idx = 0; idx < old_size; ++idx)
        {
            scans_[idx].Reset();
        }
    }

    uint32_t BlockedShard() const override
    {
        return UINT32_MAX;
    }

    ScanCache *Cache(uint32_t shard_code) override
    {
        return &scans_[shard_code];
    }

    void ShardCacheSizes(
        std::vector<std::pair<uint32_t, size_t>> *shard_code_and_sizes) override
    {
        for (size_t core_id = 0; core_id < scans_.size(); ++core_id)
        {
            shard_code_and_sizes->emplace_back(core_id, scans_[core_id].Size());
        }
    }

    const ScanTuple *Current() override
    {
        if (status_ != ScannerStatus::Open)
        {
            return nullptr;
        }

        if (heap_.empty())
        {
            for (size_t core_id = 0; core_id < scans_.size(); ++core_id)
            {
                if (scans_[core_id].Status() == ScannerStatus::Open)
                {
                    const TemplateScanTuple<KeyT, ValueT> *scan_tuple =
                        scans_[core_id].Current();
                    assert(scan_tuple != nullptr);
                    heap_.emplace(scan_tuple, core_id);
                }
            }
        }

        if (heap_.empty())
        {
            status_ = ScannerStatus::Blocked;
            return nullptr;
        }

        const std::pair<const TemplateScanTuple<KeyT, ValueT> *, size_t> &top =
            heap_.top();
        return top.first;
    }

    void MoveNext() override
    {
        if (heap_.empty())
        {
            return;
        }

        size_t core_id = heap_.top().second;
        heap_.pop();
        scans_[core_id].MoveNext();

        if (scans_[core_id].Status() == ScannerStatus::Open)
        {
            const TemplateScanTuple<KeyT, ValueT> *scan_tuple =
                scans_[core_id].Current();
            assert(scan_tuple != nullptr);
            heap_.emplace(scan_tuple, core_id);
        }

        if (heap_.empty())
        {
            status_ = ScannerStatus::Blocked;
        }
    }

    void SetDrainCacheMode(bool drain_cache_mode) override
    {
        drain_cache_mode_ = drain_cache_mode;
    }

    bool GetDrainCacheMode() override
    {
        return drain_cache_mode_;
    }

    CcmScannerType Type() const override
    {
        return CcmScannerType::RangePartition;
    }

    int64_t PartitionNgTerm() const override
    {
        return partition_ng_term_;
    }

    void SetPartitionNgTerm(int64_t partition_ng_term) override
    {
        partition_ng_term_ = partition_ng_term;
    }

    std::unique_ptr<TxKey> DecodeKey(const std::string &blob) const override
    {
        std::unique_ptr<KeyT> key = std::make_unique<KeyT>();
        size_t offset = 0;
        key->Deserialize(blob.data(), offset, key_schema_);

        return key;
    }

    uint32_t CacheCount() const override
    {
        return scans_.size();
    }

private:
    struct ForwardCompare
    {
        bool operator()(const std::pair<const TemplateScanTuple<KeyT, ValueT> *,
                                        size_t> &lhs,
                        const std::pair<const TemplateScanTuple<KeyT, ValueT> *,
                                        size_t> &rhs)
        {
            return !(*lhs.first < *rhs.first);
        }
    };

    struct BackwardCompare
    {
        bool operator()(const std::pair<const TemplateScanTuple<KeyT, ValueT> *,
                                        size_t> &lhs,
                        const std::pair<const TemplateScanTuple<KeyT, ValueT> *,
                                        size_t> &rhs)
        {
            return *lhs.first < *rhs.first;
        }
    };

    using CompareFunc =
        std::conditional_t<IsForward, ForwardCompare, BackwardCompare>;

    std::priority_queue<
        std::pair<const TemplateScanTuple<KeyT, ValueT> *, size_t>,
        std::vector<std::pair<const TemplateScanTuple<KeyT, ValueT> *, size_t>>,
        CompareFunc>
        heap_;

    std::vector<TemplateScanCache<KeyT, ValueT>> scans_;

    const Schema *key_schema_;
    /**
     * @brief The term of the cc node group where the range partition resides.
     * When the first slice from the range is scanned, the term is set. The
     * following scans of the same range partition expects to see the same term.
     *
     */
    int64_t partition_ng_term_{-1};
};
}  // namespace txservice
