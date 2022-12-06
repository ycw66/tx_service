#pragma once

#include <math.h>

#include <algorithm>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog_factory.h"
#include "distribution_steps.h"
#include "proto/statistics.pb.h"
#include "random_pairing.h"
#include "schema.h"
#include "statistics.h"
#include "type.h"

namespace txservice
{
template <typename KeyT>
class TypedShardProfile : public ShardProfile
{
public:
    explicit TypedShardProfile(const TableName &table_name,
                               const Schema *key_schema)
        : table_name_(table_name), key_schema_(key_schema)
    {
        assert(table_name.Type() == TableType::Primary ||
               table_name.Type() == TableType::Secondary);
        assert(key_schema);
    }

    TypedShardProfile(const store::IndexStatistics &store_index_statistics,
                      const Schema *key_schema)
        : TypedShardProfile(TableName(store_index_statistics.table_name(),
                                      static_cast<TableType>(
                                          store_index_statistics.table_type())),
                            key_schema)
    {
        records_ = store_index_statistics.records();

        const store::SamplePool &sample_pool =
            store_index_statistics.sample_pool();
        int samples_size = sample_pool.samples_size();

        std::vector<KeyT> keys(samples_size);
        for (int i = 0; i < samples_size; ++i)
        {
            const std::string &sample = sample_pool.samples(i);
            size_t offset = 0;
            keys[i].Deserialize(sample.data(), offset, key_schema);
        }
        assert(std::is_sorted(keys.begin(), keys.end()));

        sample_pool_ = RandomPairing<KeyT>(sample_pool.capacity(),
                                           sample_pool.c1(),
                                           sample_pool.c2(),
                                           keys.begin(),
                                           keys.end());

        distribution_steps_ =
            DistributionSteps<KeyT>(keys, distribution_steps_hint_);

        records_at_ckpt_ = records_;
    }

    void OnInsert(const KeyT &key)
    {
        records_ += 1;
        sample_pool_.Insert(key, records_);
        if (records_at_ckpt_ == 0 ||
            abs(records_ - records_at_ckpt_) /
                    static_cast<double>(records_at_ckpt_) >
                0.1)
        {
            Snapshot();
        }
    }

    void OnDelete(const KeyT &key)
    {
        if (records_ > 0)
        {
            records_ -= 1;
            sample_pool_.Delete(key);
            if (records_at_ckpt_ == 0 ||
                abs(records_ - records_at_ckpt_) /
                        static_cast<double>(records_at_ckpt_) >
                    0.1)
            {
                Snapshot();
            }
        }
    }

    void ToSerializableObj(
        store::IndexStatistics *store_index_statistics) const override
    {
        store_index_statistics->set_table_type((int32_t) table_name_.Type());
        store_index_statistics->set_table_name(table_name_.String());
        store_index_statistics->set_records(records_);
        store::SamplePool *store_sample_pool =
            store_index_statistics->mutable_sample_pool();
        store_sample_pool->set_capacity(sample_pool_.Capacity());
        store_sample_pool->set_c1(sample_pool_.C1());
        store_sample_pool->set_c2(sample_pool_.C2());

        for (const KeyT *key : sample_pool_.SamplePool())
        {
            std::string sample;
            key->Serialize(sample);
            store_sample_pool->add_samples(std::move(sample));
        }
    }

    int64_t Records() const
    {
        std::shared_lock<std::shared_mutex> slk(shared_mutex_);
        return records_at_ckpt_;
    }

    // Return records between [min_key, max_key)
    int64_t Records(const MaybeInfinityKey<KeyT> &min_key,
                    const MaybeInfinityKey<KeyT> &max_key) const
    {
        std::shared_lock<std::shared_mutex> slk(shared_mutex_);

        assert(min_key.Key() && max_key.Key());

        int64_t records = 0;

        if (distribution_steps_.Available())
        {
            records = records_at_ckpt_ * distribution_steps_.Selectivity(
                                             key_schema_, min_key, max_key);
        }

        return records;
    }

    void ClearSamplePool()
    {
        sample_pool_.Clear();
    }

private:
    void Snapshot()
    {
        std::unique_lock<std::shared_mutex> ulk(shared_mutex_);

        distribution_steps_ = DistributionSteps<KeyT>(sample_pool_.SamplePool(),
                                                      distribution_steps_hint_);
        records_at_ckpt_ = records_;
    }

private:
    static const int32_t sample_pool_capacity_{4096};
    static const int32_t distribution_steps_hint_{128};

private:
    TableName table_name_;

    const Schema *key_schema_;

    int64_t records_{0};

    RandomPairing<KeyT> sample_pool_{sample_pool_capacity_};

    // Mariadb handler threads read records_at_ckpt_/distribution_steps_,
    // however tx_processor thread may do snapshot and update
    // records_at_ckpt_/distribution_steps_.
    //
    // records_/sample_pool_ is not need to be protected, because only
    // tx_processor could change them.
    mutable std::shared_mutex shared_mutex_;
    DistributionSteps<KeyT> distribution_steps_;
    int64_t records_at_ckpt_{0};
};

template <typename KeyT>
class TypedStatistics : public Statistics
{
public:
    explicit TypedStatistics(const TableSchema *table_schema,
                             const std::string &statistics_binary)
        : table_schema_(table_schema)
    {
        if (statistics_binary.empty())
        {
            const TableName &table_name = table_schema->GetBaseTableName();
            primary_shard_profile_ = std::make_shared<TypedShardProfile<KeyT>>(
                table_name, table_schema->KeySchema());
            for (const TableName &index_name : table_schema->IndexNames())
            {
                secondary_shard_profile_map_.try_emplace(
                    index_name,
                    std::make_shared<TypedShardProfile<KeyT>>(
                        index_name, table_schema->IndexKeySchema(index_name)));
            }
        }
        else
        {
            Reset(statistics_binary);
        }
    }

    std::shared_ptr<ShardProfile> GetShardProfile(
        const TableName &table_name) const override
    {
        std::shared_lock<std::shared_mutex> slk(shared_mutex_);

        if (table_name.Type() == TableType::Primary)
        {
            return primary_shard_profile_;
        }
        else
        {
            return secondary_shard_profile_map_.at(table_name);
        }
    }

    void Reset(const std::string &statistics_binary,
               bool discard_sample_pool = false) override
    {
        assert(!statistics_binary.empty());
        std::unique_lock<std::shared_mutex> ulk(shared_mutex_);
        primary_shard_profile_.reset();
        secondary_shard_profile_map_.clear();

        store::Statistics store_statistics;
        bool ok = store_statistics.ParseFromString(statistics_binary);
        assert(ok);

        int size = store_statistics.index_statistics_list_size();
        for (int i = 0; i < size; ++i)
        {
            const store::IndexStatistics &store_index_statistics =
                store_statistics.index_statistics_list(i);

            TableName table_name(
                store_index_statistics.table_name(),
                static_cast<TableType>(store_index_statistics.table_type()));

            if (table_name.Type() == TableType::Primary)
            {
                primary_shard_profile_ =
                    std::make_shared<TypedShardProfile<KeyT>>(
                        store_index_statistics, table_schema_->KeySchema());
                if (discard_sample_pool)
                {
                    primary_shard_profile_->ClearSamplePool();
                }
            }
            else
            {
                auto secondary_shard_profile =
                    std::make_shared<TypedShardProfile<KeyT>>(
                        store_index_statistics,
                        table_schema_->IndexKeySchema(table_name));
                if (discard_sample_pool)
                {
                    secondary_shard_profile->ClearSamplePool();
                }

                secondary_shard_profile_map_.insert_or_assign(
                    table_name, std::move(secondary_shard_profile));
            }
        }
    }

    void ToSerializableObj(store::Statistics *store_statistics) const override
    {
        primary_shard_profile_->ToSerializableObj(
            store_statistics->add_index_statistics_list());

        for (const auto &[index_name, secondary_shard_profile] :
             secondary_shard_profile_map_)
        {
            secondary_shard_profile->ToSerializableObj(
                store_statistics->add_index_statistics_list());
        }
    }

private:
    const TableSchema *table_schema_{nullptr};

    // Mariadb handler threads read statistics, however cc_stream_receiver
    // thread may update statistics when receive broadcast_statistics message.
    mutable std::shared_mutex shared_mutex_;
    typename std::shared_ptr<TypedShardProfile<KeyT>> primary_shard_profile_{
        nullptr};
    std::unordered_map<TableName,
                       typename std::shared_ptr<TypedShardProfile<KeyT>>>
        secondary_shard_profile_map_;
};

}  // namespace txservice
