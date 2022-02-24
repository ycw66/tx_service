#pragma once

#include <string>
#include <string_view>
#include <vector>
#include <sstream>

#include "type.h"
#include "constants.h"

namespace txservice
{

/** @brief
  Split a string_view by a delimiter
 */
static inline std::vector<std::string_view> SplitStringView(std::string_view str, std::string_view delimter)
{
  std::vector<std::string_view> output;
  size_t first = 0, last = 0;
  std::string_view token;

  while((last = str.find(delimter, first)) != std::string::npos){
    if(last > first){
      token  = str.substr(first, last - first);
      output.emplace_back(token);
      first = last + delimter.size();
    }
  }

  if(first != 0){
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
static inline txservice::TableName GetRangeTablenameFromTablename(const txservice::TableName &table_name)
{
  txservice::TableName range_table_name(table_name);
  return range_table_name
      .append(RANGE_TABLE_NAME_PREFIX)
      .append(RANGE_TABLE_NAME_SUFFIX);
}

/** @brief
  If the table_name is a range table name

  Input table name format: ./dbname/tablename*~~ranges
 */
static inline bool IsRangeTablename(const txservice::TableName &range_table_name)
{
  bool is_range_table = false;

  std::string search_str = std::string(RANGE_TABLE_NAME_PREFIX).append(RANGE_TABLE_NAME_SUFFIX);
  size_t pos = range_table_name.find(search_str);
  if(pos != std::string::npos){
   is_range_table = range_table_name.size() == pos + search_str.size() + 1; //search_str is the last part of the range_table_name
  }

  return is_range_table;
}


/** @brief
  Derive the Mysql table name from the range table name

  Input range table name format: ./dbname/tablename*~~ranges
  Origin table name: ./dbname/tablename
 */
static inline txservice::TableName GetTablenameFromRangeTablename(const txservice::TableName &range_table_name)
{
  if(!IsRangeTablename(range_table_name)) {
    return std::string();
  }

  txservice::TableName table_name(range_table_name);
  std::string search_str = std::string(RANGE_TABLE_NAME_PREFIX).append(RANGE_TABLE_NAME_SUFFIX);
  return table_name.substr(0, table_name.rfind(search_str));
}

/** @brief
  Replace all occurrences of to_replace with replace in str
 */
static inline std::string ReplaceAll(const std::string &str, std::string to_replace, std::string replace)
{

  std::string data(str);
  // Get the first occurrence
  size_t pos = data.find(to_replace);
  // Repeat till end is reached
  while( pos != std::string::npos){
      // Replace this occurrence of Sub String
      data.replace(pos, to_replace.size(), replace);
      // Get the next occurrence from the current position
      pos =data.find(to_replace, pos + replace.size());
  }

  return data;
}

/** @brief
  Replace all prefix of index, ranges in table name with a given replace string
 */
static inline txservice::TableName ReplaceAllPrefixInTablename(txservice::TableName &table_name, std::string replace)
{
  std::string str(table_name);
  str = ReplaceAll(str, INDEX_NAME_PREFIX, replace);
  str = ReplaceAll(str, RANGE_TABLE_NAME_PREFIX, replace);
  return str;
}
}
