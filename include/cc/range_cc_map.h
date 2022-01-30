#pragma once

#include <map>

#include "cc_entry.h"
#include "cc_map.h"
#include "cc_request.h"
#include "non_blocking_lock.h"
#include "partition_id_record.h"

namespace txservice
{
struct RangePartitionEntry
{
    uint32_t partition_id_;
    NonBlockingLock lock_;
};

template <typename KeyT>
class RangePartition : public TemplateCcMap<KeyT, PartitionIdRecord>
{
public:
    RangePartition(const RangePartition &rhs) = delete;
    ~RangePartition() = default;

    RangePartition(CcShard *shard)
        : TemplateCcMap<KeyT, PartitionIdRecord>(shard)
    {
    }

    bool Execute(AcquireCc &req) override
    {
        return false;
    }

    bool Resume(AcquireCc &req) override
    {
        return false;
    }

    bool Execute(PostWriteCc &req) override
    {
        return false;
    }

    bool Execute(PostReadCc &req) override
    {
        return false;
    }

    bool Execute(ReadCc &req) override
    {
        CcEntry<KeyT, PartitionIdRecord> *floor_cce = nullptr;
        bool resume = false;

        if (req.cce_ptr_ == nullptr)
        {
            const KeyT *look_key = static_cast<const KeyT *>(req.key_);
            floor_cce = Floor(*look_key, ScanDirection::Forward, true);
            req.cce_ptr_ = floor_cce;

            bool success = floor_cce->key_lock_.AcquireRead(&req);
            if (!success)
            {
                // The request is put into the cc entry's blocking queue. Does
                // not free the request.
                return false;
            }
        }
        else
        {
            resume = true;
            floor_cce =
                static_cast<CcEntry<KeyT, PartitionIdRecord> *>(req.cce_ptr_);
        }

        PartitionIdRecord *partition_rec =
            static_cast<PartitionIdRecord *>(req.rec_);
        *partition_rec = floor_cce->payload_;
        ReadKeyResult &read_res = req.res_->Value();
        read_res.ts_ = floor_cce->commit_ts_;
        read_res.rec_status_ = RecordStatus::Normal;
        read_res.cce_addr_.SetCce(
            reinterpret_cast<uint64_t>(floor_cce), 0, CcMap::shard_->node_id_);

        req.res_->SetFinished();
        if (resume)
        {
            req.Free();
            return false;
        }
        else
        {
            return true;
        }
    }

    bool Resume(ReadCc &req) override
    {
        return false;
    }

    bool Execute(ScanCloseCc &req) override
    {
        return false;
    }

    bool Execute(ScanOpenBatchCc &req) override
    {
        return false;
    }

    bool Execute(ScanNextBatchCc &req) override
    {
        return false;
    }

    bool Execute(remote::RemoteScanOpen &req) override
    {
        return false;
    }

    bool Execute(remote::RemoteScanNextBatch &req) override
    {
        return false;
    }

    bool Execute(CommitSkCc &req) override
    {
        return false;
    }

    bool Execute(CkptScanCc &req) override
    {
        return false;
    }

    bool Execute(remote::RemoteReadOutside &req) override
    {
        return false;
    }

    bool Execute(ReplayLogCc &req) override
    {
        return false;
    }

    bool Execute(FaultInjectCC &req) override
    {
        return false;
    }

    std::unique_ptr<CcScanner> CreateScanner(
        ScanDirection direction) const override
    {
        return nullptr;
    }

private:
    // Partition ID 0 is reserved for the first range from negative infinity to
    // the next key.
    uint32_t partition_counter_{1};
};
}  // namespace txservice