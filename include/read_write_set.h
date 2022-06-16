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
public:
    ReadWriteSet() : rset_(), wset_(), wset_cnt_(0)  //, sset_(), sset_cnt_(0)
    {
    }

    void Reset()
    {
        wset_cnt_ = 0;
        wset_.clear();
        rset_.clear();
        read_cache_.clear();

        // sset_cnt_ = 0;
        // sset_.clear();
    }

    size_t ReadSetSize() const
    {
        return rset_.size();
    }

    size_t WriteSetSize() const
    {
        return wset_cnt_;
    }

    const std::unordered_map<CcEntryAddr, ReadSetEntry> &ReadSet() const
    {
        return rset_;
    }

    void AddRead(const CcEntryAddr &cce_addr,
                 uint64_t read_ts,
                 CcProtocol proto,
                 LockType lock_type)
    {
        auto [it, inserted] =
            rset_.try_emplace(cce_addr, read_ts, proto, lock_type);
        if (!inserted)
        {
            it->second.version_ts_ = read_ts;
            it->second.protocol_ = proto;
            it->second.lock_type_ = lock_type;
        }
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
        auto read_it = rset_.find(cce_addr);
        if (read_it != rset_.end())
        {
            ReadSetEntry &rs_entry = read_it->second;
            rs_entry.version_ts_ = version_ts;
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

        auto cce_it = rset_.find(cce_addr);
        if (cce_it != rset_.end())
        {
            read_ts = cce_it->second.version_ts_;
            rset_.erase(cce_it);
        }

        return read_ts;
    }

    void AddWrite(const TableName &tabname,
                  TxKey::Uptr key,
                  TxRecord::Uptr rec,
                  DmlOperation op_type)
    {
        auto table_iter = wset_.find(tabname);
        if (table_iter == wset_.end())
        {
            auto iter = wset_.try_emplace(tabname);
            table_iter = iter.first;
        }
        TableWriteSet &tws = table_iter->second;

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

    const ReadSetEntry *FindRead(const CcEntryAddr &cce_addr) const
    {
        auto key_it = rset_.find(cce_addr);
        if (key_it != rset_.end())
        {
            return &key_it->second;
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
            read_cache_.try_emplace(table_name, key.Clone(), record.Clone());
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

private:
    std::unordered_map<CcEntryAddr, ReadSetEntry> rset_;
    std::unordered_map<TableName, TableWriteSet> wset_;
    size_t wset_cnt_;
    std::unordered_map<TableName, std::pair<TxKey::Uptr, TxRecord::Uptr>>
        read_cache_;
    /*std::unordered_map<TableName,
        std::map<const TxKey *, ScanSetEntry, PtrLessThan<TxKey>>>
        sset_;
    size_t sset_cnt_;*/
};
}  // namespace txservice
