#pragma once

// Implement random pairing algorithm(reservoir sample)
// *A Dip in the Reservoir: Maintaining Sample Synopses of Evolving Datasets*
// https://www.vldb.org/conf/2006/p595-gemulla.pdf

#include <assert.h>

#include <algorithm>
#include <functional>
#include <random>
#include <unordered_map>
#include <utility>
#include <vector>

namespace txservice
{
template <typename KeyT>
class RandomPairing
{
public:
    struct Hash
    {
        std::size_t operator()(const KeyT &key) const
        {
            return key.Hash();
        }
    };

public:
    RandomPairing(int32_t capacity) : capacity_(capacity)
    {
        assert(capacity_ > 0);
    }

    template <typename Iterator>
    RandomPairing(
        int32_t capacity, int32_t c1, int32_t c2, Iterator begin, Iterator end)
        : capacity_(capacity), c1_(c1), c2_(c2)
    {
        for (Iterator iter = begin; iter != end; ++iter)
        {
            Insert(*iter);
        }
    }

    void Insert(const KeyT &key, int64_t dataset)
    {
        if (c1_ + c2_ <= 0)
        {
            if (static_cast<int32_t>(sample_pool_vec_.size()) < capacity_)
            {
                Insert(key);
            }
            else
            {
                std::uniform_int_distribution<int64_t> random_dis(0,
                                                                  dataset - 1);
                int64_t random = random_dis(random_dev_);
                if (random < capacity_)
                {
                    sample_pool_map_.erase(sample_pool_vec_[random]);
                    auto [iter, insert] = sample_pool_map_.emplace(key, random);
                    sample_pool_vec_[random] = iter;
                }
            }
        }
        else
        {
            std::uniform_int_distribution<int64_t> random_dis(0, c1_ + c2_ - 1);
            int64_t random = random_dis(random_dev_);

            if (random < c1_)
            {
                assert(c1_ > 0);
                c1_ -= 1;

                Insert(key);
            }
            else
            {
                assert(c2_ > 0);
                c2_ -= 1;
            }
        }
    }

    void Delete(const KeyT &key)
    {
        auto iter = sample_pool_map_.find(key);
        if (iter != sample_pool_map_.end())
        {
            c1_ += 1;

            size_t index = iter->second;

            std::swap(sample_pool_vec_[index], sample_pool_vec_.back());
            sample_pool_vec_.resize(sample_pool_vec_.size() - 1);

            sample_pool_map_.erase(iter);
        }
        else
        {
            c2_ += 1;
        }
    }

    std::vector<const KeyT *> SamplePool() const
    {
        std::vector<const KeyT *> vec;
        for (const auto &[key, index] : sample_pool_map_)
        {
            vec.push_back(&key);
        }
        std::sort(vec.begin(), vec.end(), PtrLessThan<KeyT>());
        return vec;
    }

    size_t Size() const
    {
        return sample_pool_vec_.size();
    }

    void Clear()
    {
        capacity_ = 0;
        c1_ = 0;
        c2_ = 0;

        sample_pool_vec_.clear();
        sample_pool_vec_.reserve(0);
        sample_pool_map_.clear();
    }

    int32_t Capacity() const
    {
        return capacity_;
    }

    int32_t C1() const
    {
        return c1_;
    }

    int32_t C2() const
    {
        return c2_;
    }

private:
    void Insert(const KeyT &key)
    {
        auto [iter, insert] =
            sample_pool_map_.emplace(key, sample_pool_vec_.size());

        // Here should have a assert.
        //
        // assert(insert);
        //
        // But system table in Mariadb store in Aria engine in past. Aria don't
        // distinguish between insert and update. So for system table, key may
        // be inserted multiple times.

        sample_pool_vec_.push_back(iter);
    }

private:
    // upper bound on sample size
    int32_t capacity_{0};

    // no. of deletions which have been in the sample
    int32_t c1_{0};

    // no. of deletions which have not been in the sample
    int32_t c2_{0};

    // for erase a given key
    std::unordered_map<KeyT, size_t, Hash> sample_pool_map_;

    // for random discard one key
    std::vector<typename std::unordered_map<KeyT, size_t>::iterator>
        sample_pool_vec_;

    std::mt19937 random_dev_;
};
}  // namespace txservice
