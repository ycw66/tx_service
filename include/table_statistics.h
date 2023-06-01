#pragma once

#include <assert.h>
#include <math.h>

#include <algorithm>
#include <atomic>
#include <functional>
#include <map>
#include <memory>
#include <set>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "catalog_factory.h"
#include "cc_request.h"
#include "distribution_steps.h"
#include "local_cc_shards.h"
#include "proto/cc_request.pb.h"
#include "random_pairing.h"
#include "remote_type.h"
#include "schema.h"
#include "sharder.h"
#include "statistics.h"
#include "store/data_store_handler.h"
#include "tx_key.h"
#include "tx_worker_pool.h"
#include "type.h"

namespace txservice
{
template <typename KeyT>
struct SamplePoolParam
{
    std::vector<KeyT> sample_keys_{};
    uint64_t records_{0};
};

template <typename KeyT>
class TemplateCcMapSamplePool : public CcMapSamplePool
{
public:
    struct CopyKey
    {
        constexpr void operator()(KeyT &lhs, const KeyT &rhs) const
        {
            lhs.Copy(rhs);
        }
    };

public:
    using SamplePool = RandomPairing<1024, KeyT, CopyKey>;
    using OnMassChange = std::function<void(
        const TemplateCcMapSamplePool<KeyT> &ccmap_sample_pool,
        uint64_t schema_version)>;

public:
    TemplateCcMapSamplePool(const TableName &table_or_index_name,
                            NodeGroupId ng_id)
        : table_or_index_name_(table_or_index_name),
          ng_id_(ng_id),
          is_local_(IsLocal(ng_id))
    {
    }

    TemplateCcMapSamplePool(const TableName &table_or_index_name,
                            NodeGroupId ng_id,
                            const SamplePoolParam<KeyT> &param)
        : table_or_index_name_(table_or_index_name),
          ng_id_(ng_id),
          is_local_(IsLocal(ng_id)),
          units_(Units(param.records_)),
          sample_pool_(param.sample_keys_)
    {
    }

    void Reset(SamplePool &&sample_pool,
               uint64_t records,
               uint64_t schema_version)
    {
        units_ = Units(records);

        assert(sample_pool.Capacity() == sample_pool_.Capacity());
        sample_pool_ = std::move(sample_pool);

        on_mass_change_(*this, schema_version);
    }

    void SetOnMassChange(OnMassChange on_mass_change)
    {
        on_mass_change_ = on_mass_change;
    }

    void OnInsert(const KeyT &key, uint64_t schema_version)
    {
        assert(is_local_);

        units_ += 1;
        insert_delete_counter_ += 1;
        sample_pool_.Insert(key, units_);

        if (insert_delete_counter_ > units_ / 10)
        {
            on_mass_change_(*this, schema_version);
            insert_delete_counter_ = 0;
        }
    }

    void OnDelete(const KeyT &key, uint64_t schema_version)
    {
        assert(is_local_);

        if (units_ > 0)
        {
            units_ -= 1;
            insert_delete_counter_ += 1;
            sample_pool_.Delete(key);

            if (insert_delete_counter_ > units_ / 10)
            {
                on_mass_change_(*this, schema_version);
                insert_delete_counter_ = 0;
            }
        }
    }

    const TableName &GetTableOrIndexName() const
    {
        return table_or_index_name_;
    }

    int64_t Records() const
    {
        return units_ * Unit();
    }

    const std::vector<KeyT> &SampleKeys() const
    {
        return sample_pool_.SampleKeys();
    }

    void Merge(const SamplePoolParam<KeyT> &param)
    {
        for (const KeyT &sample_key : param.sample_keys_)
        {
            sample_pool_.Insert(
                sample_key,
                std::min(static_cast<int64_t>(sample_pool_.Size() * 2),
                         units_));
        }
        units_ += Units(param.records_);
        sample_pool_.ClearCounter();
    }

    void Prune(const SamplePoolParam<KeyT> &param)
    {
        for (const KeyT &sample_key : param.sample_keys_)
        {
            sample_pool_.Delete(sample_key);
        }
        units_ -= std::min(units_, static_cast<int64_t>(Units(param.records_)));
        sample_pool_.ClearCounter();
    }

    void To(remote::NodeGroupSamplePool *remote_ccmap_sample_pool) const
    {
        remote_ccmap_sample_pool->set_ng_id(ng_id_);
        remote_ccmap_sample_pool->set_records(Records());
        for (const KeyT &key : sample_pool_.SampleKeys())
        {
            std::string sample;
            key.Serialize(sample);
            remote_ccmap_sample_pool->add_samples(std::move(sample));
        }
    }

    void BindCcShard(CcShard *cc_shard)
    {
        assert(cc_shard_ == nullptr || cc_shard_ == cc_shard);
        assert(cc_shard && cc_shard->core_id_ ==
                               Statistics::CoreDoSample(table_or_index_name_));
        assert(IsLocal(ng_id_));

        cc_shard_ = cc_shard;

        if (!is_local_)
        {
            // Failover ccmap
            int64_t records = Records();
            is_local_ = true;
            if (records > 0)
            {
                units_ = Units(records);
            }
        }
    }

private:
    static bool IsLocal(NodeGroupId ng_id)
    {
        int64_t leader_term = Sharder::Instance().LeaderTerm(ng_id);
        int64_t candidate_term = Sharder::Instance().CandidateLeaderTerm(ng_id);
        return leader_term >= 0 || candidate_term >= 0;
    }

    int32_t Unit() const
    {
        if (is_local_)
        {
            // For local sample pool, we know core count.
            // We bookkeep the records as it's original value divides core
            // count.
            return Sharder::Instance().GetLocalCcShardsCount();
        }
        else
        {
            // For remote sample pool, we don't known core count.
            // We bookkeep the records with it's original value.
            return 1;
        }
    }

    int64_t Units(uint64_t records) const
    {
        return (records + Unit() - 1) / Unit();
    }

private:
    TableName table_or_index_name_;

    NodeGroupId ng_id_{0};

    bool is_local_{false};

    int64_t units_{0};
    int64_t units_last_{0};

    // How many keys are inserted/deleted since last stats recalc.
    int64_t insert_delete_counter_{0};

    SamplePool sample_pool_;

    // Is this ccmap sample pool local or remote. If it is local, cc_shard_
    // points to the CcShard it is binding.
    //
    // Available after BindCcShard is called.
    CcShard *cc_shard_{nullptr};

    OnMassChange on_mass_change_;
};

template <typename KeyT>
class IndexDistribution : public Distribution
{
public:
    IndexDistribution() : records_(0), distribution_steps_()
    {
    }

    IndexDistribution(uint64_t records,
                      const std::set<const KeyT *, PtrLessThan<KeyT>> &keys)
        : records_(records), distribution_steps_(keys)
    {
    }

    uint64_t Records() const
    {
        return records_;
    }

    uint64_t Records(const Schema *key_schema,
                     const KeyT &min_key,
                     const KeyT &max_key) const
    {
        uint64_t records = 0;

        if (distribution_steps_.Available())
        {
            records = records_ * distribution_steps_.Selectivity(
                                     key_schema, min_key, max_key);
        }
        else
        {
            records = records_ / 100;
        }

        return records;
    }

private:
    uint64_t records_{0};
    DistributionSteps<KeyT> distribution_steps_;
};

template <typename KeyT>
class TableStatistics : public Statistics
{
public:
    explicit TableStatistics(const TableName &base_table_name)
        : base_table_name_(base_table_name)
    {
    }

    TableStatistics(
        const TableName &base_table_name,
        std::unordered_map<TableName,
                           std::pair<uint64_t, std::vector<TxKey::Uptr>>>
            &&sample_pool_map,
        const std::unordered_map<TableName, std::vector<uint64_t>>
            &ng_weights_map,
        CcShard *ccs,
        NodeGroupId cc_ng_id)
        : base_table_name_(base_table_name)
    {
        if (!sample_pool_map.empty())
        {
            BuildSamplePoolMap(
                *ccs, cc_ng_id, std::move(sample_pool_map), ng_weights_map);

            for (const auto &[table_or_index_name, ng_sample_pool_map] :
                 index_sample_pool_map_)
            {
                BuildDistribution(table_or_index_name);
            }
        }
    }

    std::shared_ptr<Distribution> GetDistribution(
        const TableName &table_or_index_name) const override
    {
        std::shared_lock<std::shared_mutex> slk(index_distribution_map_mutex_);
        auto iter = index_distribution_map_.find(table_or_index_name);
        if (iter != index_distribution_map_.end())
        {
            return iter->second;
        }
        else
        {
            return std::shared_ptr<Distribution>(nullptr);
        }
    }

    TemplateCcMapSamplePool<KeyT> *GetOrInitSamplePool(
        const TableName &table_or_index_name,
        NodeGroupId ng_id,
        CcShard *cc_shard)
    {
        typename IndexSamplePoolMap::iterator it =
            index_sample_pool_map_.find(table_or_index_name);
        if (it == index_sample_pool_map_.end())
        {
            index_sample_pool_map_[table_or_index_name].emplace(
                std::piecewise_construct,
                std::forward_as_tuple(ng_id),
                std::forward_as_tuple(table_or_index_name, ng_id));
        }
        else
        {
            NodeGroupSamplePoolMap &ng_sample_pool_map = it->second;

            typename NodeGroupSamplePoolMap::iterator iter =
                ng_sample_pool_map.find(ng_id);
            if (iter == ng_sample_pool_map.end())
            {
                ng_sample_pool_map.emplace(
                    std::piecewise_construct,
                    std::forward_as_tuple(ng_id),
                    std::forward_as_tuple(table_or_index_name, ng_id));
            }
        }

        TemplateCcMapSamplePool<KeyT> &ccmap_sample_pool =
            index_sample_pool_map_[table_or_index_name].at(ng_id);

        ccmap_sample_pool.BindCcShard(cc_shard);

        ccmap_sample_pool.SetOnMassChange(
            std::bind(&TableStatistics<KeyT>::OnLocalStatisticsMessage,
                      this,
                      std::placeholders::_1,
                      std::placeholders::_2));

        return &ccmap_sample_pool;
    }

    void DropIndex(const TableName &index_name) override
    {
        // No lock is required. Because Transaction already acquire write lock
        // on table catalog
        index_distribution_map_.erase(index_name);
        index_sample_pool_map_.erase(index_name);
    }

    // This method is called in one of Sharder::tx_worker_pool_ thread.
    void OnRemoteStatisticsMessage(
        const TableName &table_or_index_name,
        const TableSchema *table_schema,
        const remote::NodeGroupSamplePool &remote_sample_pool) override
    {
        Task task =
            [this, &table_or_index_name, table_schema, &remote_sample_pool](
                CcShard &ccs)
        {
            NodeGroupId ng_id =
                static_cast<NodeGroupId>(remote_sample_pool.ng_id());

            SamplePoolParam<KeyT> param;
            param.records_ = remote_sample_pool.records();

            const Schema *key_schema =
                table_or_index_name.IsBase()
                    ? table_schema->KeySchema()
                    : table_schema->IndexKeySchema(table_or_index_name);

            for (const std::string &sample : remote_sample_pool.samples())
            {
                KeyT key;
                size_t offset = 0;
                key.Deserialize(sample.data(), offset, key_schema);
                param.sample_keys_.push_back(std::move(key));
            }

            index_sample_pool_map_[table_or_index_name].insert_or_assign(
                ng_id,
                TemplateCcMapSamplePool<KeyT>(
                    table_or_index_name, ng_id, param));

            BuildDistribution(table_or_index_name);
        };
        RunOnBindingCcShard(task);

        need_save_counter_.fetch_add(1, std::memory_order_release);
    }

    // This method is called in checkpointer range split thread.
    void PriorSplitRange(const TableName &table_or_index_name,
                         NodeGroupId ng_id,
                         uint64_t schema_version) const override
    {
        Task task =
            [this, &table_or_index_name, ng_id, schema_version](CcShard &ccs)
        {
            const TemplateCcMapSamplePool<KeyT> &ccmap_sample_pool =
                index_sample_pool_map_.at(table_or_index_name).at(ng_id);
            Broadcast(ccmap_sample_pool, schema_version);
        };
        RunOnBindingCcShard(task);
    }

    // This method is called in checkpointer thread.
    bool PostCheckpoint(store::DataStoreHandler *store_hd,
                        const TableName &table_or_index_name,
                        NodeGroupId ng_id,
                        uint64_t schema_version,
                        uint64_t ckpt_ts,
                        bool ckpt_empty) const override
    {
        bool ok = true;

        int32_t need_save_counter =
            need_save_counter_.load(std::memory_order_acquire);
        if (!ckpt_empty || need_save_counter > 0)
        {
            std::unordered_map<TableName,
                               std::pair<uint64_t, std::vector<TxKey::Uptr>>>
                sample_pool_map;
            Task task = [this,
                         &table_or_index_name,
                         ng_id,
                         schema_version,
                         ckpt_empty,
                         &sample_pool_map](CcShard &ccs)
            {
                To(sample_pool_map);

                const auto iter =
                    index_sample_pool_map_.at(table_or_index_name).find(ng_id);
                if (iter !=
                    index_sample_pool_map_.at(table_or_index_name).end())
                {
                    const TemplateCcMapSamplePool<KeyT> &ccmap_sample_pool =
                        iter->second;
                    if (!ckpt_empty)
                    {
                        Broadcast(ccmap_sample_pool, schema_version);
                    }
                }
            };
            RunOnBindingCcShard(task);

            if (DoStore(ng_id))
            {
                ok = Store(store_hd, sample_pool_map, ckpt_ts);
            }

            need_save_counter_.fetch_sub(need_save_counter,
                                         std::memory_order_release);
        }
        return ok;
    }

    // This method is called in tx_processor thread.
    //
    // Every node group execute this method.
    //
    // Sample keys and records of the old range should be subtracted from old
    // node group. Sample keys and records of every new range should be added to
    // new node group.
    void OnSplitSamplePool(CcShard *ccs,
                           NodeGroupId cc_ng_id,
                           const TableName &table_or_index_name,
                           const RangeInfo *old_info)
    {
        uint32_t ng_cnt = Sharder::Instance().NodeGroupCount();
        if (ng_cnt <= 1)
        {
            return;
        }

        NodeGroupId old_ng_id = old_info->PartitionId() % ng_cnt;

        NodeGroupSamplePoolMap &ng_sample_pool_map =
            index_sample_pool_map_.at(table_or_index_name);

        auto iter = ng_sample_pool_map.find(old_ng_id);
        if (iter == ng_sample_pool_map.end())
        {
            return;
        }

        TemplateCcMapSamplePool<KeyT> &old_sample_pool = iter->second;

        std::unordered_map<NodeGroupId, SamplePoolParam<KeyT>> param_map =
            SamplePoolParamsForSplit(ccs, cc_ng_id, old_sample_pool, old_info);
        for (const auto &[new_ng_id, param] : param_map)
        {
            assert(new_ng_id != old_ng_id);
            auto iter = ng_sample_pool_map.find(new_ng_id);
            if (iter != ng_sample_pool_map.end())
            {
                TemplateCcMapSamplePool<KeyT> &new_sample_pool = iter->second;
                new_sample_pool.Merge(param);
                old_sample_pool.Prune(param);
            }
            else
            {
                ng_sample_pool_map.emplace(
                    std::piecewise_construct,
                    std::forward_as_tuple(new_ng_id),
                    std::forward_as_tuple(
                        table_or_index_name, new_ng_id, param));
                old_sample_pool.Prune(param);
            }
        }

        need_save_counter_.fetch_add(1, std::memory_order_release);
    }

private:
    // This method is called in tx_processor thread.
    void OnLocalStatisticsMessage(
        const TemplateCcMapSamplePool<KeyT> &ccmap_sample_pool,
        uint64_t schema_version)
    {
        BuildDistribution(ccmap_sample_pool.GetTableOrIndexName());
        CODE_FAULT_INJECTOR("broadcast_statistics_early",
                            { Broadcast(ccmap_sample_pool, schema_version); });

        need_save_counter_.fetch_add(1, std::memory_order_release);
    }

    void BuildSamplePoolMap(
        CcShard &ccs,
        NodeGroupId cc_ng_id,
        std::unordered_map<TableName,
                           std::pair<uint64_t, std::vector<TxKey::Uptr>>>
            &&sample_pool_map,
        const std::unordered_map<TableName, std::vector<uint64_t>>
            &ng_weights_map)
    {
        uint32_t ng_cnt = Sharder::Instance().NodeGroupCount();
        for (auto &[table_or_index_name, index_sample_pool] : sample_pool_map)
        {
            uint64_t records = index_sample_pool.first;
            std::vector<TxKey::Uptr> &samplekeys = index_sample_pool.second;

            NodeGroupSamplePoolMap &ng_sample_pool_map =
                index_sample_pool_map_[table_or_index_name];
            std::vector<SamplePoolParam<KeyT>> ng_param_vec(ng_cnt);

            std::vector<uint64_t> records_vec = DivideRecordsByNodeGroupWeight(
                records, ng_weights_map.at(table_or_index_name));

            for (NodeGroupId ng_id = 0; ng_id < ng_cnt; ++ng_id)
            {
                ng_param_vec[ng_id].records_ = records_vec.at(ng_id);
            }

            for (TxKey::Uptr &samplekey : samplekeys)
            {
                KeyT &key = static_cast<KeyT &>(*samplekey);
#ifdef RANGE_PARTITION_ENABLED
                NodeGroupId key_ng_id =
                    RouteKey(ccs, table_or_index_name, cc_ng_id, key);
#else
                NodeGroupId key_ng_id = RouteKey(key);
#endif
                ng_param_vec[key_ng_id].sample_keys_.emplace_back(
                    std::move(key));
            }

            for (NodeGroupId ng_id = 0; ng_id < ng_cnt; ++ng_id)
            {
                ng_sample_pool_map.emplace(
                    std::piecewise_construct,
                    std::forward_as_tuple(ng_id),
                    std::forward_as_tuple(
                        table_or_index_name, ng_id, ng_param_vec.at(ng_id)));
            }
        }
    }

    // This method is called in tx_processor thread.
    //
    // It is safe to access index_sample_pool_map_ directly, because all sample
    // pool of a table are sampled on same one core.
    void BuildDistribution(const TableName &table_or_index_name)
    {
        std::unique_lock<std::shared_mutex> ulk_distribution(
            index_distribution_map_mutex_);

        std::set<const KeyT *, PtrLessThan<KeyT>> index_sample_keys;
        uint64_t index_total_keys = 0;

        for (const auto &[ng_id, ccmap_sample_pool] :
             index_sample_pool_map_.at(table_or_index_name))
        {
            const std::vector<KeyT> &sample_keys =
                ccmap_sample_pool.SampleKeys();
            for (const KeyT &key : sample_keys)
            {
                index_sample_keys.insert(&key);
            }
            index_total_keys += ccmap_sample_pool.Records();
        }

        index_distribution_map_.insert_or_assign(
            table_or_index_name,
            std::make_shared<IndexDistribution<KeyT>>(index_total_keys,
                                                      index_sample_keys));
    }

    // This method must called in tx_processor thread.
    void Broadcast(const TemplateCcMapSamplePool<KeyT> &ccmap_sample_pool,
                   uint64_t schema_version) const
    {
        remote::CcStreamSender *stream_sender =
            Sharder::Instance().GetCcStreamSender();

        uint32_t src_node_id = Sharder::Instance().NodeId();

        remote::CcMessage send_msg;
        send_msg.set_type(remote::CcMessage::MessageType::
                              CcMessage_MessageType_BroadcastStatisticsRequest);
        send_msg.set_tx_term(Sharder::Instance().LeaderTerm(src_node_id));

        remote::BroadcastStatisticsRequest *broadcast_stat_req =
            send_msg.mutable_broadcast_statistics_req();
        broadcast_stat_req->set_src_node_id(src_node_id);
        broadcast_stat_req->set_node_group_id(UINT32_MAX);
        broadcast_stat_req->set_table_type(
            remote::ToRemoteType::ConvertTableType(
                ccmap_sample_pool.GetTableOrIndexName().Type()));
        broadcast_stat_req->set_table_name_str(
            ccmap_sample_pool.GetTableOrIndexName().String());
        broadcast_stat_req->set_schema_version(schema_version);
        remote::NodeGroupSamplePool *remote_sample_pool =
            broadcast_stat_req->mutable_node_group_sample_pool();
        ccmap_sample_pool.To(remote_sample_pool);

        NodeGroupId from_ng_id = remote_sample_pool->ng_id();

        uint32_t ng_cnt = Sharder::Instance().NodeGroupCount();
        for (uint32_t ng_id = 0; ng_id < ng_cnt; ng_id++)
        {
            if (ng_id != from_ng_id)
            {
                uint32_t dest_node_id = Sharder::Instance().LeaderNodeId(ng_id);
                if (dest_node_id != src_node_id)
                {
                    broadcast_stat_req->set_node_group_id(ng_id);
                    stream_sender->SendMessageToNode(dest_node_id, send_msg);
                }
                else
                {
                    Sharder::Instance().GetTxWorkerPool()->SubmitWork(
                        [this,
                         table_name = ccmap_sample_pool.GetTableOrIndexName(),
                         schema_version,
                         remote_sample_pool = *remote_sample_pool]() mutable
                        {
                            Sharder::Instance()
                                .GetLocalCcShards()
                                ->CreateRemoteStatisticsTx(
                                    std::move(table_name),
                                    schema_version,
                                    std::move(remote_sample_pool));
                        });
                }
            }
        }
    }

    // To prevent statistics in storage broken, only one node is allowed to
    // write.
    bool DoStore(NodeGroupId ng_id) const
    {
        return NodeGroupDoStore(base_table_name_) == ng_id;
    }

    bool Store(
        store::DataStoreHandler *store_hd,
        const std::unordered_map<TableName,
                                 std::pair<uint64_t, std::vector<TxKey::Uptr>>>
            &sample_pool_map,
        uint64_t ckpt_ts) const
    {
        bool ok = false;
        int retry = 10;
        while (retry-- > 0 && !ok)
        {
            ok = store_hd->UpsertTableStatistics(
                base_table_name_, sample_pool_map, ckpt_ts);
            if (!ok)
            {
                std::this_thread::sleep_for(100ms);
            }
        }

        return ok;
    }

private:
    static std::vector<uint64_t> DivideRecordsByNodeGroupWeight(
        uint64_t records, const std::vector<uint64_t> &ng_weights_map)
    {
        uint32_t ng_cnt = Sharder::Instance().NodeGroupCount();
        assert(ng_cnt == ng_weights_map.size());

        std::vector<uint64_t> records_vec(ng_cnt, 0);

        uint64_t total_weight =
            std::accumulate(ng_weights_map.begin(), ng_weights_map.end(), 0UL);

        uint64_t c = 0;
        for (NodeGroupId ng_id = 0; ng_id < ng_cnt - 1; ++ng_id)
        {
            records_vec[ng_id] =
                records * (static_cast<double>(ng_weights_map[ng_id]) /
                           static_cast<double>(total_weight));
            c += records_vec[ng_id];
        }
        records_vec.back() = records - c;

        return records_vec;
    }

private:
    using Task = std::function<void(CcShard &ccs)>;

    // Deliver task to tx_processor to avoid lock contention.
    void RunOnBindingCcShard(Task task) const
    {
        uint32_t shard_code = ShardCode(base_table_name_.StringView());

        RunOnTxProcessorCc cc_req(task);
        Sharder::Instance().GetLocalCcShards()->EnqueueCcRequest(shard_code,
                                                                 &cc_req);
        cc_req.Wait();
    }
    // This method is called in tx_processor thread.
    void To(std::unordered_map<TableName,
                               std::pair<uint64_t, std::vector<TxKey::Uptr>>>
                &sample_pool_map) const
    {
        for (const auto &[table_or_index_name, index_sample_pool] :
             index_sample_pool_map_)
        {
            sample_pool_map.try_emplace(
                table_or_index_name, 0, std::vector<TxKey::Uptr>());

            uint64_t &records = sample_pool_map.at(table_or_index_name).first;
            std::vector<TxKey::Uptr> &sample_keys =
                sample_pool_map.at(table_or_index_name).second;

            for (const auto &[ng_id, ccmap_sample_pool] : index_sample_pool)
            {
                records += ccmap_sample_pool.Records();

                for (const KeyT &sample_key : ccmap_sample_pool.SampleKeys())
                {
                    sample_keys.emplace_back(sample_key.Clone());
                }
            }
        }
    }

    NodeGroupId RouteKey(const KeyT &key) const
    {
        uint32_t key_shard_code = Sharder::Instance().ShardCode(key.Hash());
        NodeGroupId key_ng_id =
            Sharder::Instance().ShardToCcNodeGroup(key_shard_code);
        return key_ng_id;
    }

    NodeGroupId RouteKey(CcShard &ccs,
                         const TableName &table_or_index_name,
                         NodeGroupId cc_ng_id,
                         const KeyT &key) const
    {
        // Safe to use TableRangeEntry *.
        const TableRangeEntry *range_entry = ccs.GetTableRangeEntryNonLocking(
            table_or_index_name, cc_ng_id, &key);
        assert(range_entry != nullptr);

        uint32_t ng_cnt = Sharder::Instance().NodeGroupCount();
        NodeGroupId key_ng_id =
            range_entry->GetRangeInfo()->PartitionId() % ng_cnt;
        return key_ng_id;
    }

    std::unordered_map<NodeGroupId, SamplePoolParam<KeyT>>
    SamplePoolParamsForSplit(
        CcShard *ccs,
        NodeGroupId cc_ng_id,
        const TemplateCcMapSamplePool<KeyT> &old_sample_pool,
        const RangeInfo *old_info) const
    {
        std::unordered_map<NodeGroupId, SamplePoolParam<KeyT>> param_map;

        uint32_t ng_cnt = Sharder::Instance().NodeGroupCount();
        NodeGroupId old_ng_id = old_info->PartitionId() % ng_cnt;

        for (const KeyT &key : old_sample_pool.SampleKeys())
        {
            TableRangeEntry *range_entry = ccs->GetTableRangeEntry(
                old_sample_pool.GetTableOrIndexName(), cc_ng_id, &key);
            int32_t new_partition_id =
                range_entry->GetRangeInfo()->PartitionId();
            assert(new_partition_id >= 0);
            NodeGroupId new_ng_id = new_partition_id % ng_cnt;
            if (new_ng_id != old_ng_id)
            {
                param_map[new_ng_id].sample_keys_.push_back(key);
            }
        }

        uint64_t avg_range_key_count = AvgRangeKeyCountBeforeRangeSplit(
            ccs, old_sample_pool.GetTableOrIndexName(), cc_ng_id, old_info);
        for (int32_t new_partition_id : old_info->NewPartitionIdUncheckDirty())
        {
            NodeGroupId new_ng_id = new_partition_id % ng_cnt;
            if (new_ng_id != old_ng_id)
            {
                param_map[new_ng_id].records_ += avg_range_key_count;
            }
        }

        return param_map;
    }

    uint64_t AvgRangeKeyCountBeforeRangeSplit(
        CcShard *ccs,
        const TableName &table_or_index_name,
        NodeGroupId cc_ng_id,
        const RangeInfo *old_info) const
    {
        uint32_t ng_cnt = Sharder::Instance().NodeGroupCount();
        NodeGroupId old_ng_id = old_info->PartitionId() % ng_cnt;

        int32_t split_out = 0;
        for (int32_t new_partition_id : old_info->NewPartitionIdUncheckDirty())
        {
            NodeGroupId new_ng_id = new_partition_id % ng_cnt;
            if (new_ng_id != old_ng_id)
            {
                split_out += 1;
            }
        }

        uint64_t ng_range_count =
            ccs->CountRanges(table_or_index_name, cc_ng_id, old_ng_id) +
            split_out;
        uint64_t ng_key_count = index_sample_pool_map_.at(table_or_index_name)
                                    .at(old_ng_id)
                                    .Records();
        return ng_key_count / ng_range_count;
    }

private:
    TableName base_table_name_;

    // tx_processor thread may modify index_distribution_map_, and SQL thread
    // may read index_distribution_map_. Mutex protection is required.
    mutable std::shared_mutex index_distribution_map_mutex_;
    std::unordered_map<TableName, std::shared_ptr<IndexDistribution<KeyT>>>
        index_distribution_map_;

    using NodeGroupSamplePoolMap =
        std::unordered_map<NodeGroupId, TemplateCcMapSamplePool<KeyT>>;
    using IndexSamplePoolMap =
        std::unordered_map<TableName, NodeGroupSamplePoolMap>;

    // Access on index_sample_pool_map_ and sample pool is through tx_processor
    // thread always. Mutex protection is not required.
    IndexSamplePoolMap index_sample_pool_map_;

    mutable std::atomic<int32_t> need_save_counter_{0};
};

}  // namespace txservice
