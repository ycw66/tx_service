#pragma once

#include <butil/logging.h>

#include <map>
#include <unordered_map>
#include <vector>

#include "read_write_entry.h"

namespace txservice
{
using TableWriteSet =
    std::map<const TxKey *, WriteSetEntry, PtrLessThan<TxKey>>;

class ReadWriteSet
{
    static const uint32_t MaxWriteSetBytesCnt = 62 * 1024 * 1024;

public:
    ReadWriteSet() : rset_(), wset_(), wset_cnt_(0), wset_bytes_cnt_(0)
    //, sset_(), sset_cnt_(0)
    {
    }

    void Reset()
    {
        wset_cnt_ = 0;
        wset_.clear();
        rset_.clear();
        read_cache_.clear();
        wset_bytes_cnt_ = 0;

        // sset_cnt_ = 0;
        // sset_.clear();
    }

    size_t ReadSetSize() const
    {
        size_t rset_size = 0;
        for (auto &table_key_it : rset_)
        {
            rset_size += table_key_it.second.size();
        }
        return rset_size;
    }

    size_t WriteSetSize() const
    {
        return wset_cnt_;
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
                 CcProtocol proto,
                 LockType lock_type,
                 const TableName *table_name)
    {
        auto iter = rset_.find(*table_name);
        if (iter == rset_.end())
        {
            rset_.emplace(std::piecewise_construct,
                          std::forward_as_tuple(table_name->StringView(),
                                                table_name->Type()),
                          std::forward_as_tuple(
                              std::unordered_map<CcEntryAddr, ReadSetEntry>()));
        }

        // find again to locate iter
        iter = rset_.find(*table_name);
        assert(!iter->first.IsStringOwner());

        auto [it, inserted] =
            iter->second.try_emplace(cce_addr, read_ts, proto, lock_type);
        if (!inserted)
        {
            // Under Occ/OccRead protocol and RepeatableRead/Serializable
            // isolation level, the read operation adds ReadIntent locktype,
            // not read lock. So, we must verify whether the record has been
            // changed between current read and previous.
            // (read_ts == 0) means it is a boundary key added gap lock.
            // (read_ts == 1) means it's payload status is Unkonwn.
            if (it->second.version_ts_ != read_ts)
            {
                if (it->second.version_ts_ > 1)
                {
                    if (it->second.lock_type_ == LockType::ReadIntent)
                    {
                        // breaks repeatable read isolation level under
                        // Occ/OccRead protocol, return error.
                        it->second.lock_type_ = lock_type;
                        return false;
                    }
                    else
                    {
                        // ReadLock and WriteIntent always block update.
                        // Case enter this branch, must be a bug.
                        assert(false);
                    }
                }
                else
                {
                    // The entry maybe has been backfilled.
                    it->second.version_ts_ = read_ts;
                }
            }
            else if (lock_type >= it->second.lock_type_)
            {
                it->second.lock_type_ = lock_type;
                it->second.protocol_ = proto;
            }
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
        for (auto &table_key_it : rset_)
        {
            auto cce_it = table_key_it.second.find(cce_addr);
            if (cce_it != table_key_it.second.end())
            {
                read_ts = cce_it->second.version_ts_;
                table_key_it.second.erase(cce_it);
                break;
            }
        }

        return read_ts;
    }

    bool AddWrite(const TableName &table_name,
                  TxKey::Uptr key,
                  TxRecord::Uptr rec,
                  OperationType op_type)
    {
        // Check write set bytes count.
        wset_bytes_cnt_ += ((key.get() ? key.get()->MemUsage() : 0) +
                            (rec.get() ? rec.get()->MemUsage() : 0));
        if (wset_bytes_cnt_ > ReadWriteSet::MaxWriteSetBytesCnt)
        {
            return false;
        }

        auto iter = wset_.find(table_name);
        if (iter == wset_.end())
        {
            wset_.emplace(std::piecewise_construct,
                          std::forward_as_tuple(table_name.StringView(),
                                                table_name.Type()),
                          std::forward_as_tuple(TableWriteSet()));
        }

        // find again to locate iter
        iter = wset_.find(table_name);
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

    const ReadSetEntry *FindRead(const TableName &tablename,
                                 const CcEntryAddr &cce_addr) const
    {
        auto table_key_it = rset_.find(tablename);
        if (table_key_it != rset_.end())
        {
            auto key_it = table_key_it->second.find(cce_addr);
            if (key_it != table_key_it->second.end())
            {
                return &key_it->second;
            }
        }

        return nullptr;
    }

    /*ScanSetEntry &NewScanEntry(const TableName &tabname, TxKey *key)
    {
        auto find_iter = sset_.find(tabname);
        if (find_iter == sset_.end())
        {
            auto iter = sset_.emplace(
                tabname,
                std::map<const TxKey *, ScanSetEntry, PtrLessThan<TxKey>>());
            find_iter = iter.first;
        }

        TxKeyContainer keycon(key->Clone());
        auto iter = find_iter->second.emplace(keycon.get(), ScanSetEntry());
        ScanSetEntry &scan_entry = iter.first->second;
        ++sset_cnt_;

        return scan_entry;
    }*/

    // const std::unordered_map<
    //    TableName,
    //    std::map<const TxKey *, ScanSetEntry, PtrLessThan<TxKey>>>
    //    &ScanSet()
    //{
    //    return sset_;
    //}

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

    void ClearReadSet()
    {
        rset_.clear();
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
                wset_bytes_cnt_ -= (key_it.second.key_.get()->MemUsage() +
                                    key_it.second.rec_.get()->MemUsage());
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
        rset_.erase(table_name);
    }

private:
    // rset_, wset_cnt_, read_cache_ are not string owner.
    std::unordered_map<TableName, std::unordered_map<CcEntryAddr, ReadSetEntry>>
        rset_;
    std::unordered_map<TableName, TableWriteSet> wset_;
    size_t wset_cnt_;
    std::unordered_map<TableName, std::pair<TxKey::Uptr, TxRecord::Uptr>>
        read_cache_;
    size_t wset_bytes_cnt_;
    /*std::unordered_map<TableName,
        std::map<const TxKey *, ScanSetEntry, PtrLessThan<TxKey>>>
        sset_;
    size_t sset_cnt_;*/
};
}  // namespace txservice
