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

static constexpr int64_t UNKNOWN_TERM = -3;
static constexpr int64_t SKIP_CHECK_TERM = -2;
static constexpr int64_t INIT_TERM = -1;
}  // namespace txservice
