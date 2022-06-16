#include "archives_flusher.h"

#include "cc/cc_request.h"
#include "sharder.h"

namespace txservice
{

ArchivesFlusher::ArchivesFlusher(store::DataStoreWriteHandler *store_hd)
    : store_hd_(store_hd), flush_map_(), mux_(), cv_()
{
}

ArchivesFlusher::~ArchivesFlusher()
{
    thd_.join();
}

void ArchivesFlusher::Start()
{
    active_.store(true);
    thd_ = std::thread([this] { Run(); });
}

void ArchivesFlusher::Shutdown()
{
    active_.store(false);
    cv_.notify_all();
}

void ArchivesFlusher::Run()
{
    using namespace std::chrono_literals;

    while (active_.load())
    {
        std::unique_lock<std::mutex> lk(mux_);
        cv_.wait(lk,
                 [this] { return !active_.load() || flush_map_.size() > 0; });

        while (flush_map_.size() > 0)
        {
            lk.unlock();
            HandleTask();
            lk.lock();
        }
    }
}

void ArchivesFlusher::AddTask(LruEntry *entry)
{
    if (store_hd_ == nullptr)
    {
        return;
    }
    if (entry->ArchiveRecordsCount() == 0)
    {
        return;
    }

    if (!mux_.try_lock())
    {
        return;
    }

    auto it = flush_map_.find(entry);
    if (it != flush_map_.end())
    {
        mux_.unlock();
        return;
    }

    ArchiveFlushTask &task = flush_map_[entry];
    task.tbl_ = entry->parent_map_->table_name_;
    task.key_ = entry->ExportKey();
    uint32_t shard_code = Sharder::Instance().ShardCode(task.key_->Hash());
    task.node_group_id_ = shard_code >> 10;
    task.term_ = Sharder::Instance().LeaderTerm(shard_code);

    if (task.term_ < 0 ||
        Sharder::Instance().LeaderNodeId(task.node_group_id_) !=
            Sharder::Instance().NodeId())
    {
        flush_map_.erase(entry);
        mux_.unlock();
        return;
    }

    entry->ExportArchives(task.archives_);

    mux_.unlock();
    cv_.notify_one();
}

void ArchivesFlusher::HandleTask()
{
    LruEntry *entry = nullptr;
    const ArchiveFlushTask *task;
    {
        std::unique_lock<std::mutex> lk(mux_);
        if (flush_map_.cbegin() == flush_map_.cend())
        {
            return;
        }
        else
        {
            entry = flush_map_.cbegin()->first;
            task = &flush_map_.cbegin()->second;
        }
    }

    bool res =
        store_hd_->PutArchives(task->tbl_, *(task->key_), task->archives_);
    if (res)
    {
        // send clean archives cc request
        if (Sharder::Instance().CheckLeaderTerm(task->node_group_id_,
                                                task->term_))
        {
            // req will be deleted by itself in Execute()
            KickoutArchivesCc *req = new KickoutArchivesCc();
            req->Set(entry,
                     task->node_group_id_,
                     task->term_,
                     task->archives_[0].commit_ts_);

            entry->parent_map_->shard_->Enqueue(req);
        }

        std::unique_lock<std::mutex> lk(mux_);
        flush_map_.erase(entry);
    }
}

bool ArchivesFlusher::Flush(LruEntry *entry)
{
    if (store_hd_ == nullptr)
    {
        return false;
    }

    TableName tbl = entry->parent_map_->table_name_;
    TxKey::Uptr key = entry->ExportKey();
    std::vector<VersionedRecord> archives;
    entry->ExportArchives(archives);

    return store_hd_->PutArchives(tbl, *key, archives);
}

}  // namespace txservice