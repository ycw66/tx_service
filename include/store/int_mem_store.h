#pragma once

#include "data_store_handler.h"

namespace txservice::store
{
class IntMemoryStore : public DataStoreWriteHandler
{
public:
    IntMemoryStore()
    {
    }

    bool PutAll(const TableName &table_name,
                std::vector<LruEntry *> &batch,
                const Schema *key_schema,
                const Schema *rec_schema) override
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
                  const SecondaryKeySchema *sk_schema) override
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
};
}  // namespace txservice::store
