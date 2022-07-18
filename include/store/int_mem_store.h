#pragma once

#include <map>
#include <memory>  //unique_ptr
#include <string>
#include <utility>  //pair
#include <vector>

#include "store/data_store_handler.h"
#include "tx_key.h"     //CompositeKey
#include "tx_record.h"  //CompositeRecord,VersionedRecord

namespace txservice::store
{
class IntMemoryStore : public DataStoreHandler
{
public:
    IntMemoryStore()
    {
    }

    bool Connect() override
    {
        return true;
    }

    bool PutAll(const TableName &table_name,
                std::vector<LruEntry *> &batch,
                const Schema *key_schema,
                const Schema *rec_schema,
                uint64_t schema_ts) override
    {
        for (const auto &entry : batch)
        {
            CcEntry<CompositeKey<int>, CompositeRecord<int>> *cce =
                static_cast<CcEntry<CompositeKey<int>, CompositeRecord<int>> *>(
                    entry);

            const CompositeKey<int> &key = *cce->key_;
            const CompositeRecord<int> &rec = cce->payload_ckpt_.first;
            bool is_deleted = cce->payload_ckpt_.second;

            int key_val = std::get<0>(key.Tuple());
            if (is_deleted)
            {
                int_store_.erase(key_val);
            }
            else
            {
                int rec_val = std::get<0>(rec.Tuple());
                int_store_.insert_or_assign(key_val, rec_val);
            }

            if (int_store_.size() > 1000)
            {
                int_store_.erase(int_store_.begin());
            }
        }

        return true;
    }

    bool PutSkAll(const TableName &table_name,
                  std::vector<LruEntry *> &batch,
                  const SecondaryKeySchema *sk_schema,
                  uint64_t schema_ts) override
    {
        for (const auto &entry : batch)
        {
            using KeyPair = std::pair<CompositeKey<int>, CompositeKey<int>>;
            using KeyPtrPair =
                std::pair<const CompositeKey<int> *, const CompositeKey<int> *>;

            CcEntry<KeyPair, KeyPtrPair> *cce =
                static_cast<CcEntry<KeyPair, KeyPtrPair> *>(entry);

            const CompositeKey<int> &sk = *cce->payload_ckpt_.first.first;
            const CompositeKey<int> &pk = *cce->payload_ckpt_.first.second;
            bool is_del = cce->payload_ckpt_.second;

            int sk_val = std::get<0>(sk.Tuple());
            int pk_val = std::get<0>(pk.Tuple());

            std::pair<int, int> key(sk_val, pk_val);
            if (is_del)
            {
                int_index_.erase(key);
            }
            else
            {
                int_index_.try_emplace(key);
            }

            if (int_index_.size() > 1000)
            {
                int_index_.erase(int_index_.begin());
            }
        }

        return true;
    }

    void UpsertTable(
        const txservice::TableName &ccm_table_name,
        const txservice::TableSchema *table_schema,
        const std::vector<txservice::TableName> *indexes,
        bool is_deleted,
        uint64_t commit_ts,
        txservice::CcHandlerResult<txservice::Void> *hd_res) override
    {
    }

    void FetchTableCatalog(const TableName &ccm_table_name, void *fetch_req)
    {
    }

    void FetchTableRanges(const TableName &range_table_name, void *fetch_req)
    {
    }

    bool Read(const txservice::TableName &table_name,
              const txservice::TxKey &key,
              txservice::TxRecord &rec,
              bool &found,
              uint64_t &version_ts,
              const txservice::Schema *key_schema,
              const txservice::Schema *rec_schema,
              uint64_t table_schema_ts) override
    {
        assert(false);
        return false;
    }

    bool FetchTable(const txservice::TableName &table_name,
                    std::string &schema_image,
                    bool &found,
                    uint64_t &version_ts) const override
    {
        assert(false);
        return false;
    }

    bool FetchTable(const txservice::TableName &table_name,
                    std::string &schema_image,
                    bool &found) const override
    {
        assert(false);
        return false;
    }

    bool DiscoverAllTableNames(
        std::vector<std::string> &norm_name_vec) const override
    {
        assert(false);
        return false;
    }

    //-- database
    bool UpsertDatabase(std::string_view db,
                        std::string_view definition) const override
    {
        assert(false);
        return false;
    }
    bool DropDatabase(std::string_view db) const override
    {
        assert(false);
        return false;
    }
    bool FetchDatabase(std::string_view db,
                       std::string &definition,
                       bool &found) const override
    {
        assert(false);
        return false;
    }
    bool FetchAllDatabase(std::vector<std::string> &dbnames) const override
    {
        assert(false);
        return false;
    }

    //-- view
    bool UpsertView(std::string_view view,
                    std::string_view definition) const override
    {
        assert(false);
        return false;
    }
    bool DropView(std::string_view view) const override
    {
        assert(false);
        return false;
    }
    bool FetchView(std::string_view view,
                   std::string &definition,
                   bool &found) const override
    {
        assert(false);
        return false;
    }
    bool DiscoverAllViewNames(
        std::vector<std::string> &view_names) const override
    {
        assert(false);
        return false;
    }

    std::unique_ptr<DataStoreScanner> ScanForward(
        const txservice::TableName &table_name,
        const txservice::TxKey &start_key,
        bool inclusive,
        uint8_t key_parts,
        const txservice::Schema *key_schema,
        const txservice::Schema *rec_schema,
        bool scan_foward) override
    {
        assert(false);
        return nullptr;
    }

    std::unique_ptr<DataStoreScanner> ScanForward(
        const txservice::TableName &table_name,
        const std::string &search_cond,
        const txservice::Schema *key_schema,
        const txservice::Schema *rec_schema,
        bool scan_foward) override
    {
        assert(false);
        return nullptr;
    }

    /**
     * @brief Write historical versions into DataStore.
     *
     */
    bool PutArchives(const txservice::TableName &table_name,
                     const txservice::TxKey &key,
                     const std::vector<txservice::VersionedRecord> &archives)
    {
        auto &typed_key = dynamic_cast<const CompositeKey<int> &>(key);
        int int_key = std::get<0>(typed_key.Tuple());
        int_archives_[std::pair<TableName, int>(table_name, int_key)] =
            archives;

        return true;
    }

    /**
     * @brief  Get the latest visible(commit_ts <= upper_bound_ts) historical
     * version.
     */
    bool FetchVisibleArchive(const txservice::TableName &table_name,
                             const txservice::TxKey &key,
                             const uint64_t upper_bound_ts,
                             txservice::TxRecord &rec,
                             txservice::RecordStatus &rec_status,
                             uint64_t &commit_ts)
    {
        auto &typed_key = dynamic_cast<const CompositeKey<int> &>(key);
        int int_key = std::get<0>(typed_key.Tuple());
        auto &ref =
            int_archives_[std::pair<TableName, int>(table_name, int_key)];
        for (size_t i = 0; i < ref.size(); i++)
        {
            if (ref[i].commit_ts_ <= upper_bound_ts)
            {
                rec = *ref[i].record_;
                rec_status = ref[i].record_status_;
                commit_ts = ref[i].commit_ts_;
                return true;
            }
        }

        return false;
    }

    /**
     * @brief  Fetch all archives whose commit_ts >= from_ts.
     */
    bool FetchArchives(const txservice::TableName &table_name,
                       const txservice::TxKey &key,
                       std::vector<txservice::VersionedRecord> &archives,
                       uint64_t from_ts)
    {
        auto &typed_key = dynamic_cast<const CompositeKey<int> &>(key);
        int int_key = std::get<0>(typed_key.Tuple());
        auto &ref =
            int_archives_[std::pair<TableName, int>(table_name, int_key)];
        for (size_t i = 0; i < ref.size(); i++)
        {
            if (ref[i].commit_ts_ >= from_ts)
            {
                archives.emplace_back(ref[i]);
            }
        }

        return true;
    }

    size_t Size() const
    {
        return int_store_.size();
    }

    int MinKey() const
    {
        return int_store_.begin()->first;
    }

    int MaxKey() const
    {
        return int_store_.rbegin()->first;
    }

private:
    std::map<int, int> int_store_;
    std::map<std::pair<int, int>, Void> int_index_;
    std::map<std::pair<TableName, int>, std::vector<VersionedRecord>>
        int_archives_;
};
}  // namespace txservice::store
