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
  Get the range table name in mysql style

  Input table name format: ./dbname/tablename
  Range table name format: ./dbname/tablename/ranges

  Range table don't actaully visible in Mysql side.
 */
static inline txservice::TableName GetRangeTablenameFromTablename(
    const txservice::TableName &table_name)
{
    txservice::TableName range_table_name(table_name);
    return range_table_name.append(RANGE_TABLE_NAME_PREFIX)
        .append(RANGE_TABLE_NAME_SUFFIX);
}

/** @brief
  If the table_name is a range table name

  Input table name format: ./dbname/tablename*~~ranges
 */
static inline bool IsRangeTablename(
    const txservice::TableName &range_table_name)
{
    bool is_range_table = false;

    std::string search_str =
        std::string(RANGE_TABLE_NAME_PREFIX).append(RANGE_TABLE_NAME_SUFFIX);
    size_t pos = range_table_name.find(search_str);
    if (pos != std::string::npos)
    {
        is_range_table =
            range_table_name.size() ==
            pos + search_str.size();  // search_str is the last part of the
                                      // range_table_name
    }

    return is_range_table;
}

/** @brief
  If the table_name is a index name
*/
static inline bool IsIndexTableName(const txservice::TableName &index_name)
{
    std::string::size_type pos = index_name.find(INDEX_NAME_PREFIX);
    if (pos == std::string::npos)
    {
        return false;
    }
    return true;
}

/** @brief
  Derive the Mysql table name from the range table name

  Input range table name format: ./dbname/tablename*~~ranges
  Origin table name: ./dbname/tablename
 */
static inline txservice::TableName GetBaseTableNameFromRangeTableName(
    const txservice::TableName &range_table_name)
{
    if (!IsRangeTablename(range_table_name))
    {
        return std::string();
    }

    txservice::TableName table_name(range_table_name);
    std::string search_str =
        std::string(RANGE_TABLE_NAME_PREFIX).append(RANGE_TABLE_NAME_SUFFIX);
    return table_name.substr(0, table_name.rfind(search_str));
}

static inline txservice::TableName GetBaseTablenameFromIndexTableName(
    const txservice::TableName &index_table_name)
{
    if (!IsIndexTableName(index_table_name))
    {
        return std::string();
    }

    txservice::TableName table_name(index_table_name);
    std::string::size_type pos = table_name.find(INDEX_NAME_PREFIX);
    TableName sk_base_table_name = table_name.substr(0, pos);
    return sk_base_table_name;
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
  Replace all prefix of index, ranges in table name with a given replace string

  Output: is any prefix found in str
 */
static inline bool ReplaceAllPrefixInTablename(txservice::TableName &table_name,
                                               std::string replace)
{
    bool index_name_prefix_found =
        ReplaceAllInString(table_name, INDEX_NAME_PREFIX, replace);
    bool range_table_name_prefix_found =
        ReplaceAllInString(table_name, RANGE_TABLE_NAME_PREFIX, replace);

    return index_name_prefix_found || range_table_name_prefix_found;
}
}  // namespace txservice
