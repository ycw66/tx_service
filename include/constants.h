#pragma once
#include <string>

/** @brief
 Table / Index / Unique_Index naming convention for txservice
 Table name: ./DB/Table
 Index name: ./DB/Table*$$Index
 Unique_Index name: ./DB/Table*~~Unique_Index
 */
namespace txservice
{
inline const std::string INDEX_NAME_PREFIX = "*$$";
inline const std::string UNIQUE_INDEX_NAME_PREFIX = "*~~";
}  // namespace txservice
