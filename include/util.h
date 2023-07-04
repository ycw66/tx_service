#pragma once

#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include "constants.h"
#include "type.h"

namespace txservice
{
/** @brief
  Split a string_view by a delimiter
 */
static inline std::vector<std::string_view> SplitStringView(
    std::string_view str, std::string_view delimter)
{
    assert(delimter.size() > 0);

    std::vector<std::string_view> output;
    size_t first = 0, last = 0;
    std::string_view token;

    while ((last = str.find(delimter, first)) != std::string::npos)
    {
        if (last > first)
        {
            token = str.substr(first, last - first);
            output.emplace_back(token);
        }

        first = last + delimter.size();

        if (first >= str.size())
        {
            break;
        }
    }

    if (first != 0 && first < str.size())
    {
        token = str.substr(first);
        output.emplace_back(token);
    }

    return output;
}

/** @brief
  Replace the 1st occurrence of to_replace with replace in str

  Output: is the to_replace found in str
 */
static inline bool ReplaceInString(std::string &str,
                                   const std::string to_replace,
                                   const std::string replace)
{
    size_t pos = str.find(to_replace);
    bool found = pos != std::string::npos;

    // Replace this occurrence of Sub String
    if (found)
    {
        str.replace(pos, to_replace.size(), replace);
    }

    return found;
}

/** @brief
  Replace all occurrences of to_replace with replace in str

  Output: is the to_replace found in str
 */
static inline bool ReplaceAllInString(std::string &str,
                                      const std::string to_replace,
                                      const std::string replace)
{
    // Get the first occurrence
    size_t pos = str.find(to_replace);
    bool found = pos != std::string::npos;
    // Repeat till end is reached
    while (pos != std::string::npos)
    {
        // Replace this occurrence of Sub String
        str.replace(pos, to_replace.size(), replace);
        // Get the next occurrence from the current position
        pos = str.find(to_replace, pos + replace.size());
    }

    return found;
}

/**
 * Merge multiple sorted ascending vectors into a single one.
 * Note that the passed in compare func need to be greater than.
 */
template <typename T, class Compare>
static inline void MergeSortedVectors(std::vector<std::vector<T>> &&vecs,
                                      std::vector<T> &output,
                                      Compare greater,
                                      bool dedup = false)
{
    // We need to build a priority queue with pair elements. Each element
    // will contain which subvec the element comes from and the actual value T.
    // Build a new cmp function for the pair object with the passed in cmp.
    auto greater_pair = [greater](std::pair<T, size_t> &p1,
                                  std::pair<T, size_t> &p2) -> bool
    { return greater(p1.first, p2.first); };
    std::priority_queue<std::pair<T, size_t>,
                        std::vector<std::pair<T, size_t>>,
                        decltype(greater_pair)>
        pq(greater_pair);
    size_t total_size = 0;
    // Record pos in each sub vec.
    std::vector<size_t> idxs;
    for (size_t i = 0; i < vecs.size(); ++i)
    {
        total_size += vecs.at(i).size();
        idxs.push_back(1);
        if (!vecs.at(i).empty())
        {
            pq.emplace(std::move(vecs.at(i).front()), i);
        }
    }
    output.reserve(total_size);
    while (pq.size())
    {
        // Move the top object to output vec before popping it.
        const auto &top = pq.top();
        if (!dedup || output.empty() || greater(output.back(), top.first) ||
            greater(top.first, output.back()))
        {
            output.push_back(std::move(const_cast<T &>(top.first)));
        }
        size_t grp = top.second;
        pq.pop();
        // Add the next object from the same sub vec if it has not
        // reached the end.
        if (idxs.at(grp) < vecs.at(grp).size())
        {
            T &next = vecs.at(grp).at(idxs.at(grp));
            pq.emplace(std::move(next), grp);
            idxs.at(grp)++;
        }
    }
    assert(dedup || output.size() == total_size);
}
}  // namespace txservice
