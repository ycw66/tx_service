#pragma once

#include <butil/logging.h>

#include <map>
#include <string>
#include <unordered_map>
#include <vector>

#include "read_write_entry.h"

namespace txservice
{
using TableWriteSet =
    std::map<const TxKey *, WriteSetEntry, PtrLessThan<TxKey>>;

enum class ReadEntryResult : uint8_t
{
    NO_INSERT = 0,
    INSERT_ONE,
    INSERT_REPEAT
};

class ReadWriteSet
{
    static const uint32_t MaxWriteSetBytesCnt = 62 * 1024 * 1024;

public:
    ReadWriteSet()
        : rset_(),
          wset_(),
          wset_cnt_(0),
          data_rset_cnt_(0),
          wset_bytes_cnt_(0),
          forward_write_cnt_(0)
    {
    }

    void Reset()
    {
        wset_cnt_ = 0;
        wset_.clear();
        rset_.clear();
        read_cache_.clear();
        wset_bytes_cnt_ = 0;
        data_rset_cnt_ = 0;
        forward_write_cnt_ = 0;

#ifdef ON_KEY_OBJECT
        cmd_set_.clear();
        cmd_cnt_ = 0;
#endif
    }

    /**
     * @brief Returns the number of read data items, excluding catalog entries.
     *
     * @return size_t
     */
    size_t ReadSetSize() const
    {
        return data_rset_cnt_;
    }

    size_t CatalogRangeSetSize() const
    {
        size_t set_size = 0;
        for (auto &[tbl_name, rset] : rset_)
        {
            if (tbl_name.Type() == TableType::Catalog ||
                tbl_name.Type() == TableType::RangePartition)
            {
                set_size += rset.size();
            }
        }

        return set_size;
    }

    size_t WriteSetSize() const
    {
        return wset_cnt_;
    }

    size_t ForwardWriteCnt() const
    {
        return forward_write_cnt_;
    }

    void IncreaseFowardWriteCnt()
    {
        forward_write_cnt_++;
    }

    const std::unordered_map<TableName,
                             std::unordered_map<CcEntryAddr, ReadSetEntry>>
        &ReadSet() const
    {
        return rset_;
    }

    /**
     * @brief When adding a read key, checks if there is already one in the
     * readset and matches the timestamp. Does not add the read key if
     * there is a timestamp mismatch.
     *
     * @return true - add sucess; false - the version is different with previous
     * read, that is, break RepeatableRead isolation level.
     */
    bool AddRead(const CcEntryAddr &cce_addr,
                 uint64_t read_ts,
                 const TableName *table_name)
    {
        auto iter = rset_.find(*table_name);
        if (iter == rset_.end())
        {
            auto insert_it = rset_.emplace(
                std::piecewise_construct,
                std::forward_as_tuple(table_name->StringView(),
                                      table_name->Type()),
                std::forward_as_tuple(
                    std::unordered_map<CcEntryAddr, ReadSetEntry>()));
            iter = insert_it.first;
        }

        assert(!iter->first.IsStringOwner());

        auto [it, inserted] = iter->second.try_emplace(cce_addr, read_ts);
        if (!inserted)
        {
            // (read_ts == 0) means it is a boundary key added gap lock or its
            // payload status is Unkonwn.
            if (it->second.version_ts_ < read_ts && it->second.version_ts_ != 0)
            {
                // breaks repeatable read isolation level under
                // Occ/OccRead protocol, return error.
                return false;
            }
            else if (read_ts > 0)
            {
                it->second.version_ts_ = read_ts;
            }

            it->second.read_cnt_++;
        }
        else if (!(*table_name == catalog_ccm_name) &&
                 (table_name->Type() != TableType::RangePartition))
        {
            ++data_rset_cnt_;
        }
        return true;
    }

    /**
     * @brief Updates a data item's timestamp in the read set. The method is
     * called after a read-outside request. A read-outside request immediately
     * follows a read-inside request and is only issued if the initial
     * read-inside returns a record whose value is unknown. The read-outside
     * request retrieves the value and its commit timestamp from the data store
     * and updates the timestamp in the read set.
     *
     * @param cce_addr Cc entry address
     * @param version_ts Commit timestamp of the value retrieved from the data
     * store.
     */
    void UpdateRead(const CcEntryAddr &cce_addr, uint64_t version_ts)
    {
        for (auto &table_key_it : rset_)
        {
            auto read_it = table_key_it.second.find(cce_addr);
            if (read_it != table_key_it.second.end())
            {
                ReadSetEntry &rs_entry = read_it->second;
                rs_entry.version_ts_ = version_ts;
                break;
            }
        }
    }

    /**
     * @brief Removes the read-set key given the cc entry's address.
     *
     * @param cce_addr The cc entry's address.
     * @return uint64_t Commit timestamp of the cc entry, if the specified
     * cc entry exists and is removed. 0, if the specified cc entry does not
     * exist.
     */
    uint64_t DedupRead(const CcEntryAddr &cce_addr)
    {
        uint64_t read_ts = 0;
        for (auto &[table_name, tbl_read_set] : rset_)
        {
            auto cce_it = tbl_read_set.find(cce_addr);
            if (cce_it != tbl_read_set.end())
            {
                if (!(table_name == catalog_ccm_name) &&
                    (table_name.Type() != TableType::RangePartition))
                {
                    --data_rset_cnt_;
                }

                read_ts = cce_it->second.version_ts_;
                tbl_read_set.erase(cce_it);

                break;
            }
        }

        return read_ts;
    }

    uint64_t DedupRead(const TableName &tbl_name, const CcEntryAddr &cce_addr)
    {
        auto tbl_it = rset_.find(tbl_name);
        if (tbl_it == rset_.end())
        {
            return 0;
        }

        std::unordered_map<CcEntryAddr, ReadSetEntry> &tbl_read_set =
            tbl_it->second;
        auto cce_it = tbl_read_set.find(cce_addr);
        if (cce_it != tbl_read_set.end())
        {
            if (!(tbl_name == catalog_ccm_name) &&
                (tbl_name.Type() != TableType::RangePartition))
            {
                --data_rset_cnt_;
            }

            uint64_t read_ts = cce_it->second.version_ts_;
            tbl_read_set.erase(cce_it);

            if (tbl_it->second.empty())
            {
                rset_.erase(tbl_it);
            }

            return read_ts;
        }
        else
        {
            return 0;
        }
    }

    bool AddWrite(const TableName &table_name,
                  TxKey::Uptr key,
                  TxRecord::Uptr rec,
                  OperationType op_type)
    {
        // Check write set bytes count.
        wset_bytes_cnt_ += ((key.get() ? key.get()->SerializedLength() : 0) +
                            (rec.get() ? rec.get()->SerializedLength() : 0));
        if (wset_bytes_cnt_ > ReadWriteSet::MaxWriteSetBytesCnt)
        {
            return false;
        }

        auto iter = wset_.find(table_name);
        if (iter == wset_.end())
        {
            auto insert_it =
                wset_.emplace(std::piecewise_construct,
                              std::forward_as_tuple(table_name.StringView(),
                                                    table_name.Type()),
                              std::forward_as_tuple(TableWriteSet()));
            iter = insert_it.first;
        }

        assert(!iter->first.IsStringOwner());

        TableWriteSet &tws = iter->second;

        WriteSetEntry wset_entry;
        wset_entry.key_ = std::move(key);
        wset_entry.rec_ = std::move(rec);
        wset_entry.op_ = op_type;

        auto [it, inserted] =
            tws.try_emplace(wset_entry.key_.get(), std::move(wset_entry));
        if (inserted)
        {
            ++wset_cnt_;
        }
        else
        {
            // Modify old WriteSetEntry.
            it->second.rec_ = std::move(wset_entry.rec_);
            it->second.op_ = wset_entry.op_;
        }
        return true;
    }

    const WriteSetEntry *FindWrite(const TableName &table_name,
                                   const TxKey &key) const
    {
        auto tab_it = wset_.find(table_name);
        if (tab_it != wset_.end())
        {
            auto key_it = tab_it->second.find(&key);
            if (key_it != tab_it->second.end())
            {
                return &key_it->second;
            }
        }

        return nullptr;
    }

    std::pair<TableWriteSet::const_iterator, TableWriteSet::const_iterator>
    InitIter(const TableWriteSet &table_wset,
             const TxKey *start_key,
             bool inclusive)
    {
        auto it = table_wset.lower_bound(start_key);
        if (it != table_wset.end())
        {
            if (*it->first == *start_key && !inclusive)
            {
                ++it;
            }
        }
        return std::make_pair(it, table_wset.end());
    }

    std::pair<TableWriteSet::const_reverse_iterator,
              TableWriteSet::const_reverse_iterator>
    InitReverseIter(const TableWriteSet &table_wset,
                    const TxKey *start_key,
                    bool inclusive)
    {
        auto rit = std::make_reverse_iterator(table_wset.upper_bound(
            start_key));  // return key not large than start_key
        if (rit != table_wset.rend())
        {
            if (*rit->first == *start_key && !inclusive)
            {
                ++rit;
            }
        }
        return std::make_pair(rit, table_wset.rend());
    }

    /**
     * @brief Removes all read-set entries of data items, keeping catalog and
     * range read entries.
     *
     */
    void ClearReadSet()
    {
        for (auto tbl_it = rset_.begin(); tbl_it != rset_.end();)
        {
            if (tbl_it->first == catalog_ccm_name ||
                tbl_it->first.Type() == TableType::RangePartition)
            {
                ++tbl_it;
            }
            else
            {
                data_rset_cnt_ -= tbl_it->second.size();
                tbl_it = rset_.erase(tbl_it);
            }
        }

        assert(data_rset_cnt_ == 0);
    }

    void ClearScanSet()
    {
        /*sset_cnt_ = 0;
        sset_.clear();*/
    }

    void ClearWriteSet()
    {
        wset_.clear();
        wset_cnt_ = 0;
        wset_bytes_cnt_ = 0;
        forward_write_cnt_ = 0;
    }

    void ClearTable(const TableName &table_name)
    {
        auto tab_it = wset_.find(table_name);
        if (tab_it != wset_.end())
        {
            const TableWriteSet &tab_wset = tab_it->second;
            assert(wset_cnt_ >= tab_wset.size());
            wset_cnt_ -= tab_wset.size();
            for (auto &key_it : tab_wset)
            {
                assert(key_it.second.key_ != nullptr);

                wset_bytes_cnt_ -= key_it.second.key_->SerializedLength();
                wset_bytes_cnt_ -= key_it.second.rec_ != nullptr
                                       ? key_it.second.rec_->SerializedLength()
                                       : 0;

                if (key_it.second.forward_key_shard_code_ != UINT32_MAX)
                {
                    forward_write_cnt_--;
                }
            }
            wset_.erase(tab_it);
        }
    }

    std::unordered_map<TableName, TableWriteSet> &WriteSet()
    {
        return wset_;
    }

    void AddCacheRead(const TableName &table_name,
                      const TxKey &key,
                      const TxRecord &record)
    {
        auto key_rec_it = read_cache_.find(table_name);
        if (key_rec_it != read_cache_.end())
        {
            key_rec_it->second.first->Copy(key);
            key_rec_it->second.second->Copy(record);
        }
        else
        {
            read_cache_.emplace(
                std::piecewise_construct,
                std::forward_as_tuple(table_name.StringView(),
                                      table_name.Type()),
                std::forward_as_tuple(key.Clone(), record.Clone()));
        }
    }

    const TxRecord *FindCacheRead(const TableName &table_name, const TxKey &key)
    {
        auto key_rec_it = read_cache_.find(table_name);
        if (key_rec_it != read_cache_.end() && *key_rec_it->second.first == key)
        {
            return key_rec_it->second.second.get();
        }
        else
        {
            return nullptr;
        }
    }

    void ClearReadSet(const TableName &table_name)
    {
        auto tbl_it = rset_.find(table_name);
        if (tbl_it != rset_.end())
        {
            if (!(table_name == catalog_ccm_name))
            {
                data_rset_cnt_ -= tbl_it->second.size();
            }

            rset_.erase(tbl_it);
        }

#ifdef RANGE_PARTITION_ENABLED
        TableName range_tbl_name(table_name.StringView(),
                                 TableType::RangePartition);
        tbl_it = rset_.find(range_tbl_name);
        if (tbl_it != rset_.end())
        {
            rset_.erase(tbl_it);
        }
#endif
    }

    uint16_t RemoveReadEntry(const TableName &table_name,
                             const CcEntryAddr &addr)
    {
        assert(table_name.Type() != TableType::Catalog ||
               table_name.Type() != TableType::RangePartition);

        auto iter = rset_.find(table_name);
        if (iter == rset_.end())
        {
            return 0;
        }

        auto it_addr = iter->second.find(addr);
        if (it_addr == iter->second.end())
        {
            return 0;
        }

        assert(it_addr->second.read_cnt_ > 0);
        it_addr->second.read_cnt_--;
        uint16_t read_cnt = it_addr->second.read_cnt_;
        if (read_cnt == 0)
        {
            --data_rset_cnt_;
            iter->second.erase(it_addr);
        }

        return read_cnt;
    }

    void AddObjectCommand(const TableName &table_name,
                          const CcEntryAddr &cce_addr,
                          uint64_t cce_version,
                          const TxKey *key,
                          const TxCommand *cmd)
    {
#ifdef ON_KEY_OBJECT
        auto [table_it, success] = cmd_set_.try_emplace(table_name);
        auto &table_cmd_set = table_it->second;

        std::string key_str;
        key->Serialize(key_str);
        std::string cmd_str;
        cmd->Serialize(cmd_str);
        auto [cce_it, inserted] = table_cmd_set.try_emplace(
            cce_addr, cce_version, std::move(key_str), std::move(cmd_str));
        if (!inserted)
        {
            CmdSetEntry &entry = cce_it->second;
            entry.cmd_str_list_.emplace_back(std::move(cmd_str));
        }

        cmd_cnt_++;
#endif
    }

    const std::unordered_map<TableName,
                             std::unordered_map<CcEntryAddr, CmdSetEntry>>
        *ObjectCommandCce() const
    {
#ifdef ON_KEY_OBJECT
        return &cmd_set_;
#else
        return nullptr;
#endif
    }

    uint32_t ObjectCommandSize() const
    {
#ifdef ON_KEY_OBJECT
        return cmd_cnt_;
#else
        return 0;
#endif
    }

    void ResetForwardWriteCount()
    {
        forward_write_cnt_ = 0;
    }

private:
    // rset_, wset_cnt_, read_cache_ are not string owner.
    std::unordered_map<TableName, std::unordered_map<CcEntryAddr, ReadSetEntry>>
        rset_;
    std::unordered_map<TableName, TableWriteSet> wset_;
    size_t wset_cnt_;
    size_t data_rset_cnt_;
    std::unordered_map<TableName, std::pair<TxKey::Uptr, TxRecord::Uptr>>
        read_cache_;
    size_t wset_bytes_cnt_;
    size_t forward_write_cnt_;

#ifdef ON_KEY_OBJECT
    /**
     * Collection of object keys and commands.
     */
    std::unordered_map<TableName, std::unordered_map<CcEntryAddr, CmdSetEntry>>
        cmd_set_;

    uint32_t cmd_cnt_{};
#endif
};
}  // namespace txservice
