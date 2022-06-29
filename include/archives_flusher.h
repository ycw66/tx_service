#pragma once

#include <atomic>
#include <condition_variable>
#include <map>
#include <mutex>
#include <thread>

#include "cc/cc_entry.h"
#include "type.h"  // TableName

namespace txservice
{

namespace store
{
class DataStoreHandler;
}

struct ArchiveFlushTask
{
    TableName tbl_;
    TxKey::Uptr key_{nullptr};
    std::vector<VersionedRecord> archives_;
    uint32_t node_group_id_{0};
    int64_t term_{-1};
};

class ArchivesFlusher
{
public:
    static ArchivesFlusher &Instance(
        store::DataStoreHandler *store_hd = nullptr)
    {
        static ArchivesFlusher instance_(store_hd);
        return instance_;
    }

    ~ArchivesFlusher();
    void AddTask(LruEntry *entry);

    // Flush archives synchronously. Now, only used for test.
    bool Flush(LruEntry *entry);

    void Start();
    void Shutdown();

private:
    ArchivesFlusher(store::DataStoreHandler *store_hd);
    void HandleTask();
    void Run();

    store::DataStoreHandler *store_hd_;
    std::map<LruEntry *, ArchiveFlushTask> flush_map_;

    std::atomic<bool> active_;
    std::mutex mux_;
    std::condition_variable cv_;
    std::thread thd_;
};
}  // namespace txservice