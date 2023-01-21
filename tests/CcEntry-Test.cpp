// Let Catch provide main():
#define CATCH_CONFIG_MAIN

#include <catch2/catch.hpp>

#include "cc/cc_entry.h"
#include "cc/cc_request.h"
#include "cc/template_cc_map.h"
#include "tx_key.h"     // CompositeKey
#include "tx_record.h"  // CompositeRecord

namespace txservice
{

TEST_CASE("CcEntry Init", "[cc-entry]")
{
    CcEntry<CompositeKey<int>, CompositeRecord<int>> entry(nullptr);

    REQUIRE(entry.commit_ts_ == 1);
    REQUIRE(entry.payload_status_ == RecordStatus::Unknown);
    REQUIRE(entry.ArchiveRecordsCount() == 0);
}

TEST_CASE("CcEntry ArchiveBeforeUpdate", "[cc-entry]")
{
    CcEntry<CompositeKey<int>, CompositeRecord<int>> entry(nullptr);

    entry.commit_ts_ = 1U;
    entry.payload_ = std::make_unique<CompositeRecord<int>>(1);
    entry.payload_status_ = RecordStatus::Unknown;

    entry.ArchiveBeforeUpdate(TableType::Primary);
    REQUIRE(entry.ArchiveRecordsCount() == 0);

    entry.commit_ts_ = 2U;
    entry.payload_ = std::make_unique<CompositeRecord<int>>(2);
    entry.payload_status_ = RecordStatus::Normal;
    entry.ArchiveBeforeUpdate(TableType::Primary);

    entry.commit_ts_ = 3U;
    entry.payload_ = std::make_unique<CompositeRecord<int>>(3);
    entry.payload_status_ = RecordStatus::Normal;
    entry.ArchiveBeforeUpdate(TableType::Primary);
    REQUIRE(entry.ArchiveRecordsCount() == 2);

    REQUIRE(entry.archives_->front().commit_ts_ == 3);
    REQUIRE(std::get<0>(entry.archives_->front().payload_->Tuple()) == 3);
    REQUIRE(entry.archives_->front().payload_status_ == RecordStatus::Normal);

    REQUIRE(entry.archives_->front().commit_ts_ == 3);
    REQUIRE(std::get<0>(entry.archives_->front().payload_->Tuple()) == 3);
    REQUIRE(entry.archives_->front().payload_status_ == RecordStatus::Normal);
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

    auto entry_it = entry.archives_->cbegin();
    for (size_t i = 0; i < nums.size(); i++)
    {
        assert(entry_it != entry.archives_->cend());
        REQUIRE(entry_it->commit_ts_ == static_cast<uint64_t>(nums[i]));
        REQUIRE(std::get<0>(entry_it->payload_->Tuple()) == nums[i]);
        REQUIRE(entry_it->payload_status_ == RecordStatus::Normal);
        entry_it++;
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

        auto entry_it = entry.archives_->cbegin();
        for (size_t i = 0; i < nums.size(); i++)
        {
            assert(entry_it != entry.archives_->cend());
            REQUIRE(entry_it->commit_ts_ == static_cast<uint64_t>(nums[i]));
            REQUIRE(std::get<0>(entry_it->payload_->Tuple()) == nums[i]);
            REQUIRE(entry_it->payload_status_ == RecordStatus::Normal);
            entry_it++;
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

        auto entry_it = entry.archives_->cbegin();
        for (size_t i = 0; i < nums.size(); i++)
        {
            assert(entry_it != entry.archives_->cend());
            REQUIRE(entry_it->commit_ts_ == static_cast<uint64_t>(nums[i]));
            REQUIRE(std::get<0>(entry_it->payload_->Tuple()) == nums[i]);
            REQUIRE(entry_it->payload_status_ == RecordStatus::Normal);
            entry_it++;
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

        auto entry_it = entry.archives_->cbegin();
        for (size_t i = 0; i < nums.size(); i++)
        {
            assert(entry_it != entry.archives_->cend());
            REQUIRE(entry_it->commit_ts_ == static_cast<uint64_t>(nums[i]));
            REQUIRE(std::get<0>(entry_it->payload_->Tuple()) == nums[i]);
            REQUIRE(entry_it->payload_status_ == RecordStatus::Normal);
            entry_it++;
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

        auto entry_it = entry.archives_->cbegin();
        for (size_t i = 0; i < nums.size(); i++)
        {
            assert(entry_it != entry.archives_->cend());
            REQUIRE(entry_it->commit_ts_ == static_cast<uint64_t>(nums[i]));
            REQUIRE(std::get<0>(entry_it->payload_->Tuple()) == nums[i]);
            REQUIRE(entry_it->payload_status_ == RecordStatus::Normal);
            entry_it++;
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

        auto entry_it = entry.archives_->cbegin();
        for (size_t i = 0; i < nums.size(); i++)
        {
            assert(entry_it != entry.archives_->cend());
            REQUIRE(entry_it->commit_ts_ == static_cast<uint64_t>(nums[i]));
            REQUIRE(std::get<0>(entry_it->payload_->Tuple()) == nums[i]);
            REQUIRE(entry_it->payload_status_ == RecordStatus::Normal);
            entry_it++;
        }
    }

    // (oldest_active_tx_ts: 2)->... => [10,9,8,6,3,2]
    {
        uint64_t oldest_active_tx_ts = 2;
        nums = {10, 9, 8, 6, 3, 2};

        entry.KickOutArchiveRecords(oldest_active_tx_ts);
        REQUIRE(entry.ArchiveRecordsCount() == nums.size());

        auto entry_it = entry.archives_->cbegin();
        for (size_t i = 0; i < nums.size(); i++)
        {
            assert(entry_it != entry.archives_->cend());
            REQUIRE(entry_it->commit_ts_ == static_cast<uint64_t>(nums[i]));
            REQUIRE(std::get<0>(entry_it->payload_->Tuple()) == nums[i]);
            REQUIRE(entry_it->payload_status_ == RecordStatus::Normal);
            entry_it++;
        }
    }

    // (oldest_active_tx_ts: 7)->... => [10,9,8,6]
    {
        uint64_t oldest_active_tx_ts = 7;
        nums = {10, 9, 8, 6};

        entry.KickOutArchiveRecords(oldest_active_tx_ts);
        REQUIRE(entry.ArchiveRecordsCount() == nums.size());

        auto entry_it = entry.archives_->cbegin();
        for (size_t i = 0; i < nums.size(); i++)
        {
            assert(entry_it != entry.archives_->cend());
            REQUIRE(entry_it->commit_ts_ == static_cast<uint64_t>(nums[i]));
            REQUIRE(std::get<0>(entry_it->payload_->Tuple()) == nums[i]);
            REQUIRE(entry_it->payload_status_ == RecordStatus::Normal);
            entry_it++;
        }
    }

    // (oldest_active_tx_ts: 8)->... => [10,9,8]
    {
        uint64_t oldest_active_tx_ts = 8;
        nums = {10, 9, 8};

        entry.KickOutArchiveRecords(oldest_active_tx_ts);
        REQUIRE(entry.ArchiveRecordsCount() == nums.size());

        auto entry_it = entry.archives_->cbegin();
        for (size_t i = 0; i < nums.size(); i++)
        {
            assert(entry_it != entry.archives_->cend());
            REQUIRE(entry_it->commit_ts_ == static_cast<uint64_t>(nums[i]));
            REQUIRE(std::get<0>(entry_it->payload_->Tuple()) == nums[i]);
            REQUIRE(entry_it->payload_status_ == RecordStatus::Normal);
            entry_it++;
        }
    }

    // (oldest_active_tx_ts: 5)->... => [10,9,8]
    {
        uint64_t oldest_active_tx_ts = 5;
        nums = {10, 9, 8};

        entry.KickOutArchiveRecords(oldest_active_tx_ts);
        REQUIRE(entry.ArchiveRecordsCount() == nums.size());

        auto entry_it = entry.archives_->cbegin();
        for (size_t i = 0; i < nums.size(); i++)
        {
            assert(entry_it != entry.archives_->cend());
            REQUIRE(entry_it->commit_ts_ == static_cast<uint64_t>(nums[i]));
            REQUIRE(std::get<0>(entry_it->payload_->Tuple()) == nums[i]);
            REQUIRE(entry_it->payload_status_ == RecordStatus::Normal);
            entry_it++;
        }
    }

    // (oldest_active_tx_ts: 15)->... => []
    {
        uint64_t oldest_active_tx_ts = 15;
        nums = {};

        entry.KickOutArchiveRecords(oldest_active_tx_ts);
        REQUIRE(entry.ArchiveRecordsCount() == nums.size());
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

        entry.MvccGet(ts, TableType::Primary, rec);
        REQUIRE(rec.payload_status_ == RecordStatus::Unknown);
    }

    entry.commit_ts_ = 12U;
    entry.payload_status_ = RecordStatus::Deleted;
    entry.payload_ = std::make_unique<CompositeRecord<int>>(12);
    //== CcEntry has been filled, but has no historical version.

    // (read_ts: 5, ckpt_ts:0)->... => Unknown
    {
        uint64_t ts = 5;
        VersionResultRecord<CompositeRecord<int>> rec;

        entry.MvccGet(ts, TableType::Primary, rec);
        REQUIRE(rec.payload_status_ == RecordStatus::Unknown);
    }

    // (read_ts: 5, ckpt_ts=3, ckpt_ts<=read_ts)->... => Unknown
    entry.ckpt_ts_ = 3;
    {
        uint64_t ts = 5;
        VersionResultRecord<CompositeRecord<int>> rec;

        entry.MvccGet(ts, TableType::Primary, rec);
        REQUIRE(rec.payload_status_ == RecordStatus::Unknown);
    }

    // (read_ts: 5, ckpt_ts=8, ckpt_ts>read_ts)->... =>VesionUnknown
    entry.ckpt_ts_ = 8;
    {
        uint64_t ts = 5;
        VersionResultRecord<CompositeRecord<int>> rec;

        entry.MvccGet(ts, TableType::Primary, rec);
        REQUIRE(rec.payload_status_ == RecordStatus::VersionUnknown);
    }

    // (read_ts: 15)->... => 12 (latest version)
    {
        uint64_t ts = 15;
        uint64_t target = 12;
        VersionResultRecord<CompositeRecord<int>> rec;

        entry.MvccGet(ts, TableType::Primary, rec);
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

        entry.MvccGet(ts, TableType::Primary, rec);
        REQUIRE(rec.commit_ts_ == static_cast<uint64_t>(target));
        REQUIRE(rec.payload_status_ == RecordStatus::VersionUnknown);
    }

    // (read_ts: 2)->... => 2
    {
        uint64_t ts = 2;
        uint64_t target = 2;
        VersionResultRecord<CompositeRecord<int>> rec;

        entry.MvccGet(ts, TableType::Primary, rec);
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

        entry.MvccGet(ts, TableType::Primary, rec);
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

        entry.MvccGet(ts, TableType::Primary, rec);
        REQUIRE(rec.commit_ts_ == static_cast<uint64_t>(target));
        REQUIRE(rec.payload_ptr_ == nullptr);
        REQUIRE(rec.payload_status_ == RecordStatus::Deleted);
    }
}

}  // namespace txservice
