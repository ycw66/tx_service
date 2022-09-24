#pragma once

#include <string>

struct TxServiceOptions
{
    std::string local_path;
    std::uint32_t core_num;
    std::uint32_t checkpointer_interval;
    bool enable_mvcc;
};
