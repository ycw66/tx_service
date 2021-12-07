#pragma once

#include <cstdint>
#include <unordered_map>

#include "braft/route_table.h"
#include "brpc/server.h"
#include "fault/cc_node.h"
#include "txlog.h"

namespace txservice
{
static std::vector<std::string> type_name_to_enum_vec{"SLEEP",
                                                      "ERROR",
                                                      "FATAL",
                                                      "PANIC",
                                                      "INFI_LOOP",
                                                      "SUSPEND",
                                                      "RESUME",
                                                      "SKIP",
                                                      "RESET",
                                                      "STATUS",
                                                      "WAIT_UNTIL_TRIGGER"};
enum struct FaultType
{
    SLEEP = 0,
    ERROR,
    FATAL,
    PANIC,
    INFI_LOOP,
    SUSPEND,
    RESUME,
    SKIP,
    RESET,
    STATUS,
    WAIT_UNTIL_TRIGGER
};

class FaultEntry
{
public:
    FaultEntry()
    {
    }

    ~FaultEntry()
    {
    }

    void Set(std::string fault_name,
             FaultType fault_type,
             std::string database_name,
             std::string table_name,
             int start_occurrence,
             int end_occurrence)
    {
        fault_name_ = fault_name;
        fault_type_ = fault_type;
        database_name_ = database_name;
        table_name_ = table_name;
        start_occurrence_ = start_occurrence;
        end_occurrence_ = end_occurrence;
        num_times_triggered_ = 0;
    }

    std::string fault_name_;
    FaultType fault_type_;
    // advanced field, not used yet.
    std::string database_name_;
    std::string table_name_;
    int start_occurrence_;
    int end_occurrence_;
    int num_times_triggered_;
};

class FaultInject
{
public:
    static FaultInject &Instance()
    {
        static FaultInject instance_;
        return instance_;
    }

    void TriggerFaultIfSet(std::string fault_name,
                           std::string database_name,
                           std::string table_name)
    {
        FaultEntry local_fentry;
        {
            std::lock_guard<std::mutex> lk(mux_);
            auto iter = injected_fault_map_.find(fault_name);
            if (iter != injected_fault_map_.end())
            {
                local_fentry = iter->second;
            }
            else
            {
                return;
            }
        }

        switch (local_fentry.fault_type_)
        {
        case FaultType::PANIC:
            kill(getpid(), SIGKILL);
            break;
        default:
            break;
        }
        return;
    }

    void InjectFault(std::string fault_name,
                     std::string fault_type,
                     std::string database_name,
                     std::string table_name,
                     int start_occurrence,
                     int end_occurrence)
    {
        FaultEntry fentry;
        FaultType fault_enum_type;
        uint32_t i;

        for (i = 0; i < type_name_to_enum_vec.size(); i++)
        {
            if (fault_type == type_name_to_enum_vec[i])
                fault_enum_type = (FaultType) i;
        }

        fentry.Set(fault_name,
                   fault_enum_type,
                   database_name,
                   table_name,
                   start_occurrence,
                   end_occurrence);

        {
            std::lock_guard<std::mutex> lk(mux_);
            injected_fault_map_.try_emplace(fault_name, fentry);
        }
    }

private:
    FaultInject()
    {
    }

    ~FaultInject()
    {
    }

    uint32_t node_id_;
    std::unordered_map<std::string, FaultEntry> injected_fault_map_;
    std::mutex mux_;
};

#ifdef FAULT_INJECTOR
#define SIMPLE_FAULT_INJECTOR(FaultName) \
    FaultInject::Instance().TriggerFaultIfSet(FaultName, "", "")
#else
#define SIMPLE_FAULT_INJECTOR(FaultName)
#endif

}  // namespace txservice
