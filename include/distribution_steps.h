#pragma once

// Implement equal-depth histogram
// *ACCURATE ESTIMATION OF THE NUMBER OF TUPLES SATISFYING A CONDITION*
// https://dl.acm.org/doi/pdf/10.1145/971697.602294

#include <assert.h>

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

namespace txservice
{
template <typename KeyT>
class DistributionSteps
{
public:
public:
    DistributionSteps() = default;

    DistributionSteps(const std::vector<KeyT> &sort_vector, int32_t hint_steps)
    {
        int64_t sz = sort_vector.size();
        if (sz > hint_steps)
        {
            steps_ = hint_steps;
            step_ = (sz - 1) / steps_;
            for (int32_t i = 0; i <= steps_; ++i)
            {
                uint64_t k = i * step_;
                assert(k < sort_vector.size());

                step_values_.push_back(sort_vector[k]);
            }
        }
        else
        {
            steps_ = sort_vector.size();
            step_ = 1;
            for (const KeyT &key : sort_vector)
            {
                step_values_.push_back(key);
            }
        }
    }

    DistributionSteps(const std::vector<const KeyT *> &sort_vector,
                      int32_t hint_steps)
    {
        int64_t sz = sort_vector.size();
        if (sz > hint_steps)
        {
            steps_ = hint_steps;
            step_ = (sz - 1) / steps_;
            for (int32_t i = 0; i <= steps_; ++i)
            {
                uint64_t k = i * step_;
                assert(k < sort_vector.size());

                step_values_.push_back(*sort_vector[k]);
            }
        }
        else
        {
            steps_ = sort_vector.size();
            step_ = 1;
            for (const KeyT *key : sort_vector)
            {
                step_values_.push_back(*key);
            }
        }
    }

    bool Available() const
    {
        return !step_values_.empty();
    }

    // Return percentage of [min_key, max_key)
    double Selectivity(const Schema *key_schema,
                       const MaybeInfinityKey<KeyT> &min_key,
                       const MaybeInfinityKey<KeyT> &max_key) const
    {
        double sel = 0;

        if (*(max_key.Key()) < *(min_key.Key()) ||
            *(max_key.Key()) == *(min_key.Key()))
        {
            return sel;
        }

        sel =
            Selectivity(key_schema, max_key) - Selectivity(key_schema, min_key);
        assert(sel >= 0 && sel <= 1);

        return sel;
    }

private:
    // Percentage of Selectivity(<key)
    double Selectivity(const Schema *key_schema,
                       const MaybeInfinityKey<KeyT> &key) const
    {
        typename std::vector<KeyT>::const_iterator it;

        it = std::lower_bound(
            step_values_.begin(), step_values_.end(), *(key.Key()));

        if (it == step_values_.end())
        {
            return 1;
        }
        else if (it == step_values_.begin())
        {
            return 0;
        }
        else
        {
            assert(key.Key()->Type() == KeyType::Normal);

            size_t steps = std::distance(step_values_.begin(), it);
            double pos =
                key.Key()->PosInInterval(key_schema, *(std::prev(it)), *it);

            return (static_cast<double>(steps) - (1.0 - pos)) / steps_;
        }
    }

private:
    std::vector<KeyT> step_values_;
    int32_t steps_{0};
    int32_t step_{0};
};
}  // namespace txservice
