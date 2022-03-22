#pragma once

#include <map>
#include <unordered_map>
#include <vector>

#include "raft_log.pb.h"
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
        cache_table_.clear();

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
                 ReadType read_type,
                 LockType lock_type)
    {
        if (read_type == ReadType::Inside)
        {
            rset_.try_emplace(cce_addr, read_ts, proto, lock_type);
        }
        else
        {
            // A read-outside request immediately follows a read-inside request
            // and is only issued if the initial read-inside returns a record
            // whose value is unknown. If the read-outside request returns a
            // version newer than the value in the data store, uses the new ts
            // for validation.
            rset_.insert_or_assign(cce_addr,
                                   ReadSetEntry(read_ts, proto, lock_type));
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
                  TxKeyContainer &key_c,
                  TxRecordContainer &rec_c,
                  DmlOperation op_type,
                  SecondaryKeys *skeys = nullptr)
    {
        auto table_iter = wset_.find(tabname);
        if (table_iter == wset_.end())
        {
            auto iter = wset_.try_emplace(tabname);
            table_iter = iter.first;
        }

        TableWriteSet &tws = table_iter->second;
        auto key_iter = tws.find(key_c.get());

        if (key_iter != tws.end())
        {
            WriteSetEntry &write_entry = key_iter->second;
            write_entry.rec_ = rec_c;
            write_entry.op_ = op_type;
            if (skeys != nullptr)
            {
                write_entry.sindx_ = std::move(*skeys);
            }
        }
        else
        {
            WriteSetEntry write_entry;
            write_entry.key_ = key_c;
            write_entry.rec_ = rec_c;
            write_entry.op_ = op_type;
            if (skeys != nullptr && skeys->size() > 0)
            {
                write_entry.sindx_ = std::move(*skeys);
            }

            table_iter->second.emplace(write_entry.key_.get(),
                                       std::move(write_entry));
            ++wset_cnt_;
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

    std::string cache_table_;
    TxKey::Uptr cache_key_;
    TxRecord::Uptr cache_rec_;

private:
    std::unordered_map<CcEntryAddr, ReadSetEntry> rset_;
    std::unordered_map<TableName, TableWriteSet> wset_;
    size_t wset_cnt_;
    /*std::unordered_map<TableName,
        std::map<const TxKey *, ScanSetEntry, PtrLessThan<TxKey>>>
        sset_;
    size_t sset_cnt_;*/
};
}  // namespace txservice
