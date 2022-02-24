#pragma once
#include <string>

/** @brief
 Table / Index / Range naming convention for txservice
 Table name: ./DB/Table
 Index name: ./DB/Table*$$Index
 Range name: ./DB/Table*~~ranges
 */
namespace txservice
{
inline const std::string INDEX_NAME_PREFIX = "*$$";
inline const std::string RANGE_TABLE_NAME_PREFIX = "*~~";
inline const std::string RANGE_TABLE_NAME_SUFFIX = "ranges";
}
