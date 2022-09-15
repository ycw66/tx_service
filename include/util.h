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

/** @brief
  Replace all prefix of index, ranges in table name with a given replace
  string

  Output: is any prefix found in str
 */
static inline bool ReplaceAllPrefixInTablename(std::string &table_name_str,
                                               std::string replace)
{
    bool index_name_prefix_found =
        ReplaceAllInString(table_name_str, INDEX_NAME_PREFIX, replace);
    bool range_table_name_prefix_found =
        ReplaceAllInString(table_name_str, RANGE_TABLE_NAME_PREFIX, replace);

    return index_name_prefix_found || range_table_name_prefix_found;
    return true;
}
}  // namespace txservice
