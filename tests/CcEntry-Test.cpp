// Let Catch provide main():
#define CATCH_CONFIG_MAIN

#include <catch2/catch.hpp>

#include "cc/cc_entry.h"
#include "cc/cc_request.h"
#include "tx_key.h"     // CompositeKey
#include "tx_record.h"  // CompositeRecord

namespace txservice
{

TEST_CASE("CcEntry Init", "[cc-entry]")
{
    CcEntry<CompositeKey<int>, CompositeRecord<int>> entry(nullptr);

    REQUIRE(entry.commit_ts_ == 1);
    REQUIRE(entry.payload_status_ == RecordStatus::Unknown);
    REQUIRE(entry.archives_.size() == 0);
}

TEST_CASE("CcEntry ArchiveBeforeUpdate", "[cc-entry]")
{
    CcEntry<CompositeKey<int>, CompositeRecord<int>> entry(nullptr);

    entry.commit_ts_ = 1U;
    entry.payload_ = std::make_unique<CompositeRecord<int>>(1);
    entry.payload_status_ = RecordStatus::Unknown;

    entry.ArchiveBeforeUpdate();
    REQUIRE(entry.ArchiveRecordsCount() == 0);

    entry.commit_ts_ = 2U;
    entry.payload_ = std::make_unique<CompositeRecord<int>>(2);
    entry.payload_status_ = RecordStatus::Normal;
    entry.ArchiveBeforeUpdate();

    entry.commit_ts_ = 3U;
    entry.payload_ = std::make_unique<CompositeRecord<int>>(3);
    entry.payload_status_ = RecordStatus::Normal;
    entry.ArchiveBeforeUpdate();
    REQUIRE(entry.ArchiveRecordsCount() == 2);

    REQUIRE(entry.archives_[0].commit_ts_ == 3);
    REQUIRE(std::get<0>(entry.archives_[0].payload_->Tuple()) == 3);
    REQUIRE(entry.archives_[0].payload_status_ == RecordStatus::Normal);

    REQUIRE(entry.archives_[0].commit_ts_ == 3);
    REQUIRE(std::get<0>(entry.archives_[0].payload_->Tuple()) == 3);
    REQUIRE(entry.archives_[0].payload_status_ == RecordStatus::Normal);
}

TEST_CASE("CcEntry AddArchiveRecords", "[cc-entry]")
{
    CcEntry<CompositeKey<int>, CompositeRecord<int>> entry(nullptr);

    // [6,5,3]->...=>[6,5,3]
    std::vector<VersionTxRecord> records;  // desc order

    std::vector<int> nums{6, 5, 3};
    for (auto n : nums)
    {
        auto &ref = records.emplace_back();
        ref.commit_ts_ = n;
        ref.record_ = std::make_unique<CompositeRecord<int>>(n);
        ref.record_status_ = RecordStatus::Normal;
    }

    entry.AddArchiveRecords(records);
    REQUIRE(entry.ArchiveRecordsCount() == nums.size());

    for (size_t i = 0; i < nums.size(); i++)
    {
        REQUIRE(entry.archives_[i].commit_ts_ ==
                static_cast<uint64_t>(nums[i]));
        REQUIRE(std::get<0>(entry.archives_[i].payload_->Tuple()) == nums[i]);
        REQUIRE(entry.archives_[i].payload_status_ == RecordStatus::Normal);
    }

    // [8]->...=>[8,6,5,3]
    {
        records.clear();
        auto &ref = records.emplace_back();
        ref.commit_ts_ = 8U;
        ref.record_ = std::make_unique<CompositeRecord<int>>(8);
        ref.record_status_ = RecordStatus::Normal;

        nums = {8, 6, 5, 3};
        entry.AddArchiveRecords(records);
        REQUIRE(entry.ArchiveRecordsCount() == nums.size());

        for (size_t i = 0; i < nums.size(); i++)
        {
            REQUIRE(entry.archives_[i].commit_ts_ ==
                    static_cast<uint64_t>(nums[i]));
            REQUIRE(std::get<0>(entry.archives_[i].payload_->Tuple()) ==
                    nums[i]);
            REQUIRE(entry.archives_[i].payload_status_ == RecordStatus::Normal);
        }
    }

    // [2]->...=>[8,6,5,3,2]
    {
        records.clear();
        auto &ref = records.emplace_back();
        ref.commit_ts_ = 2U;
        ref.record_ = std::make_unique<CompositeRecord<int>>(2);
        ref.record_status_ = RecordStatus::Normal;

        nums = {8, 6, 5, 3, 2};
        entry.AddArchiveRecords(records);
        REQUIRE(entry.ArchiveRecordsCount() == nums.size());

        for (size_t i = 0; i < nums.size(); i++)
        {
            REQUIRE(entry.archives_[i].commit_ts_ ==
                    static_cast<uint64_t>(nums[i]));
            REQUIRE(std::get<0>(entry.archives_[i].payload_->Tuple()) ==
                    nums[i]);
            REQUIRE(entry.archives_[i].payload_status_ == RecordStatus::Normal);
        }
    }

    // [3,2,1]->...=>[8,6,5,3,2,1]
    {
        records.clear();
        std::vector<int> add_nums{3, 2, 1};
        for (auto n : add_nums)
        {
            auto &ref = records.emplace_back();
            ref.commit_ts_ = n;
            ref.record_ = std::make_unique<CompositeRecord<int>>(n);
            ref.record_status_ = RecordStatus::Normal;
        }

        nums = {8, 6, 5, 3, 2, 1};
        entry.AddArchiveRecords(records);
        REQUIRE(entry.ArchiveRecordsCount() == nums.size());

        for (size_t i = 0; i < nums.size(); i++)
        {
            REQUIRE(entry.archives_[i].commit_ts_ ==
                    static_cast<uint64_t>(nums[i]));
            REQUIRE(std::get<0>(entry.archives_[i].payload_->Tuple()) ==
                    nums[i]);
            REQUIRE(entry.archives_[i].payload_status_ == RecordStatus::Normal);
        }
    }

    // Abnormal test case: [5,3,1,0]->...=>[8,6,5,3,1,0,2,1]
    {
        records.clear();
        std::vector<int> add_nums{5, 3, 1, 0};
        for (auto n : add_nums)
        {
            auto &ref = records.emplace_back();
            ref.commit_ts_ = n;
            ref.record_ = std::make_unique<CompositeRecord<int>>(n);
            ref.record_status_ = RecordStatus::Normal;
        }

        nums = {8, 6, 5, 3, 1, 0, 2, 1};
        entry.AddArchiveRecords(records);
        REQUIRE(entry.ArchiveRecordsCount() == nums.size());

        for (size_t i = 0; i < nums.size(); i++)
        {
            REQUIRE(entry.archives_[i].commit_ts_ ==
                    static_cast<uint64_t>(nums[i]));
            REQUIRE(std::get<0>(entry.archives_[i].payload_->Tuple()) ==
                    nums[i]);
            REQUIRE(entry.archives_[i].payload_status_ == RecordStatus::Normal);
        }
    }
}

TEST_CASE("CcEntry KickOutArchiveRecords", "[cc-entry]")
{
    CcEntry<CompositeKey<int>, CompositeRecord<int>> entry(nullptr);
    entry.commit_ts_ = 12U;
    entry.payload_status_ = RecordStatus::Deleted;

    // [10,9,8,6,3,2]
    std::vector<VersionTxRecord> records;  // desc order
    std::vector<int> nums{10, 9, 8, 6, 3, 2};
    for (auto n : nums)
    {
        auto &ref = records.emplace_back();
        ref.commit_ts_ = n;
        ref.record_ = std::make_unique<CompositeRecord<int>>(n);
        ref.record_status_ = RecordStatus::Normal;
    }
    entry.AddArchiveRecords(records);
    REQUIRE(entry.ArchiveRecordsCount() == nums.size());

    // (oldest_active_tx_ts: 1)->... => [10,9,8,6,3,2]
    {
        uint64_t oldest_active_tx_ts = 1;
        nums = {10, 9, 8, 6, 3, 2};

        entry.KickOutArchiveRecords(oldest_active_tx_ts);
        REQUIRE(entry.ArchiveRecordsCount() == nums.size());

        for (size_t i = 0; i < nums.size(); i++)
        {
            REQUIRE(entry.archives_[i].commit_ts_ ==
                    static_cast<uint64_t>(nums[i]));
            REQUIRE(std::get<0>(entry.archives_[i].payload_->Tuple()) ==
                    nums[i]);
            REQUIRE(entry.archives_[i].payload_status_ == RecordStatus::Normal);
        }
    }

    // (oldest_active_tx_ts: 2)->... => [10,9,8,6,3,2]
    {
        uint64_t oldest_active_tx_ts = 2;
        nums = {10, 9, 8, 6, 3, 2};

        entry.KickOutArchiveRecords(oldest_active_tx_ts);
        REQUIRE(entry.ArchiveRecordsCount() == nums.size());

        for (size_t i = 0; i < nums.size(); i++)
        {
            REQUIRE(entry.archives_[i].commit_ts_ ==
                    static_cast<uint64_t>(nums[i]));
            REQUIRE(std::get<0>(entry.archives_[i].payload_->Tuple()) ==
                    nums[i]);
            REQUIRE(entry.archives_[i].payload_status_ == RecordStatus::Normal);
        }
    }

    // (oldest_active_tx_ts: 7)->... => [10,9,8,6]
    {
        uint64_t oldest_active_tx_ts = 7;
        nums = {10, 9, 8, 6};

        entry.KickOutArchiveRecords(oldest_active_tx_ts);
        REQUIRE(entry.ArchiveRecordsCount() == nums.size());

        for (size_t i = 0; i < nums.size(); i++)
        {
            REQUIRE(entry.archives_[i].commit_ts_ ==
                    static_cast<uint64_t>(nums[i]));
            REQUIRE(std::get<0>(entry.archives_[i].payload_->Tuple()) ==
                    nums[i]);
            REQUIRE(entry.archives_[i].payload_status_ == RecordStatus::Normal);
        }
    }

    // (oldest_active_tx_ts: 8)->... => [10,9,8]
    {
        uint64_t oldest_active_tx_ts = 8;
        nums = {10, 9, 8};

        entry.KickOutArchiveRecords(oldest_active_tx_ts);
        REQUIRE(entry.ArchiveRecordsCount() == nums.size());

        for (size_t i = 0; i < nums.size(); i++)
        {
            REQUIRE(entry.archives_[i].commit_ts_ ==
                    static_cast<uint64_t>(nums[i]));
            REQUIRE(std::get<0>(entry.archives_[i].payload_->Tuple()) ==
                    nums[i]);
            REQUIRE(entry.archives_[i].payload_status_ == RecordStatus::Normal);
        }
    }

    // (oldest_active_tx_ts: 5)->... => [10,9,8]
    {
        uint64_t oldest_active_tx_ts = 5;
        nums = {10, 9, 8};

        entry.KickOutArchiveRecords(oldest_active_tx_ts);
        REQUIRE(entry.ArchiveRecordsCount() == nums.size());

        for (size_t i = 0; i < nums.size(); i++)
        {
            REQUIRE(entry.archives_[i].commit_ts_ ==
                    static_cast<uint64_t>(nums[i]));
            REQUIRE(std::get<0>(entry.archives_[i].payload_->Tuple()) ==
                    nums[i]);
            REQUIRE(entry.archives_[i].payload_status_ == RecordStatus::Normal);
        }
    }

    // (oldest_active_tx_ts: 15)->... => []
    {
        uint64_t oldest_active_tx_ts = 15;
        nums = {};

        entry.KickOutArchiveRecords(oldest_active_tx_ts);
        REQUIRE(entry.ArchiveRecordsCount() == nums.size());

        for (size_t i = 0; i < nums.size(); i++)
        {
            REQUIRE(entry.archives_[i].commit_ts_ ==
                    static_cast<uint64_t>(nums[i]));
            REQUIRE(std::get<0>(entry.archives_[i].payload_->Tuple()) ==
                    nums[i]);
            REQUIRE(entry.archives_[i].payload_status_ == RecordStatus::Normal);
        }
    }
}

TEST_CASE("CcEntry MvccGet", "[cc-entry]")
{
    CcEntry<CompositeKey<int>, CompositeRecord<int>> entry(nullptr);
    //== CcEntry has not been filled

    // (read_ts: 5)->... => Unknown
    {
        uint64_t ts = 5;
        VersionResultRecord<CompositeRecord<int>> rec;

        bool res = entry.MvccGet(ts, rec);
        REQUIRE(res);
        REQUIRE(rec.payload_status_ == RecordStatus::Unknown);
    }

    entry.commit_ts_ = 12U;
    entry.payload_status_ = RecordStatus::Deleted;
    entry.payload_ = std::make_unique<CompositeRecord<int>>(12);
    //== CcEntry has been filled, but has no historical version.

    // (read_ts: 5)->... => VersionUnknown
    {
        uint64_t ts = 5;
        VersionResultRecord<CompositeRecord<int>> rec;

        bool res = entry.MvccGet(ts, rec);
        REQUIRE(res);
        REQUIRE(rec.payload_status_ == RecordStatus::VersionUnknown);
    }

    // (read_ts: 15)->... => 12 (latest version)
    {
        uint64_t ts = 15;
        uint64_t target = 12;
        VersionResultRecord<CompositeRecord<int>> rec;

        bool res = entry.MvccGet(ts, rec);
        REQUIRE(res);
        REQUIRE(rec.commit_ts_ == static_cast<uint64_t>(target));
        REQUIRE(rec.payload_ptr_ == nullptr);
        REQUIRE(rec.payload_status_ == RecordStatus::Deleted);
    }

    //== Filled historical versions: [10,9,8,6,3,2]
    std::vector<VersionTxRecord> records;  // desc order
    std::vector<int> nums{10, 9, 8, 6, 3, 2};
    for (auto n : nums)
    {
        auto &ref = records.emplace_back();
        ref.commit_ts_ = n;
        ref.record_ = std::make_unique<CompositeRecord<int>>(n);
        ref.record_status_ = RecordStatus::Normal;
    }
    entry.AddArchiveRecords(records);
    REQUIRE(entry.ArchiveRecordsCount() == nums.size());

    // (read_ts: 1)->... => VersionUnknown
    {
        uint64_t ts = 1;
        uint64_t target = 1;
        VersionResultRecord<CompositeRecord<int>> rec;

        bool res = entry.MvccGet(ts, rec);
        REQUIRE(res);
        REQUIRE(rec.commit_ts_ == static_cast<uint64_t>(target));
        REQUIRE(rec.payload_status_ == RecordStatus::VersionUnknown);
    }

    // (read_ts: 2)->... => 2
    {
        uint64_t ts = 2;
        uint64_t target = 2;
        VersionResultRecord<CompositeRecord<int>> rec;

        bool res = entry.MvccGet(ts, rec);
        REQUIRE(res);
        REQUIRE(rec.commit_ts_ == static_cast<uint64_t>(target));
        REQUIRE(std::get<0>(rec.payload_ptr_->Tuple()) ==
                static_cast<int>(target));
        REQUIRE(rec.payload_status_ == RecordStatus::Normal);
    }

    // (read_ts: 7)->... => 6
    {
        uint64_t ts = 7;
        uint64_t target = 6;
        VersionResultRecord<CompositeRecord<int>> rec;

        bool res = entry.MvccGet(ts, rec);
        REQUIRE(res);
        REQUIRE(rec.commit_ts_ == static_cast<uint64_t>(target));
        REQUIRE(std::get<0>(rec.payload_ptr_->Tuple()) ==
                static_cast<int>(target));
        REQUIRE(rec.payload_status_ == RecordStatus::Normal);
    }

    // (read_ts: 15)->... => 12 (latest version)
    {
        uint64_t ts = 15;
        uint64_t target = 12;
        VersionResultRecord<CompositeRecord<int>> rec;

        bool res = entry.MvccGet(ts, rec);
        REQUIRE(res);
        REQUIRE(rec.commit_ts_ == static_cast<uint64_t>(target));
        REQUIRE(rec.payload_ptr_ == nullptr);
        REQUIRE(rec.payload_status_ == RecordStatus::Deleted);
    }
}

TEST_CASE("CcEntry MvccGet hasWriteLock", "[cc-entry]")
{
    CcEntry<CompositeKey<int>, CompositeRecord<int>> entry(nullptr);
    entry.commit_ts_ = 12U;
    entry.payload_status_ = RecordStatus::Deleted;
    entry.payload_ = std::make_unique<CompositeRecord<int>>(12);

    // [10,9,8,6,3,2]
    std::vector<VersionTxRecord> records;  // desc order
    std::vector<int> nums{10, 9, 8, 6, 3, 2};
    for (auto n : nums)
    {
        auto &ref = records.emplace_back();
        ref.commit_ts_ = n;
        ref.record_ = std::make_unique<CompositeRecord<int>>(n);
        ref.record_status_ = RecordStatus::Normal;
    }
    entry.AddArchiveRecords(records);
    REQUIRE(entry.ArchiveRecordsCount() == nums.size());

    // required write lock, lock_ts = 13
    AcquireCc req;
    TxId txid;
    txid.Reset(1, 1, 1);
    TableName tbl = "tbl";
    string key_str = "1";
    req.Reset(&tbl,
              &key_str,
              0,
              txid.TxNumber(),
              1,
              1,
              false,
              nullptr,
              0,
              CcProtocol::Locking);
    entry.key_lock_.AcquireWriteLock(&req, 1, CcProtocol::Locking);
    entry.wlock_ts_ = 13;

    // (read_ts: 9)->... => 9
    {
        uint64_t ts = 9;
        uint64_t target = 9;
        VersionResultRecord<CompositeRecord<int>> rec;

        bool res = entry.MvccGet(ts, rec);
        REQUIRE(res);
        REQUIRE(rec.commit_ts_ == static_cast<uint64_t>(target));
        REQUIRE(std::get<0>(rec.payload_ptr_->Tuple()) ==
                static_cast<int>(target));
        REQUIRE(rec.payload_status_ == RecordStatus::Normal);
    }

    // (read_ts: 13)->... => 12 (latest version)
    {
        uint64_t ts = 13;
        uint64_t target = 12;
        VersionResultRecord<CompositeRecord<int>> rec;

        bool res = entry.MvccGet(ts, rec);
        REQUIRE(res);
        REQUIRE(rec.commit_ts_ == static_cast<uint64_t>(target));
        REQUIRE(rec.payload_ptr_ == nullptr);
        REQUIRE(rec.payload_status_ == RecordStatus::Deleted);
    }

    // (read_ts: 15)->... => false
    {
        uint64_t ts = 15;
        VersionResultRecord<CompositeRecord<int>> rec;

        bool res = entry.MvccGet(ts, rec);
        REQUIRE_FALSE(res);
    }

    entry.key_lock_.ReleaseWriteLock(req.Txn(), nullptr);
}

}  // namespace txservice
