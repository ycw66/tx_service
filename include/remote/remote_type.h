#pragma once

#include "cc_protocol.h"
#include "proto/cc_request.pb.h"
#include "tx_record.h"  // RecordStatus;
#include "type.h"

namespace txservice
{

namespace remote
{

class ToRemoteType
{
public:
    static txservice::remote::IsolationType ConvertIsolation(
        txservice::IsolationLevel iso_level)
    {
        switch (iso_level)
        {
        case IsolationLevel::ReadCommitted:
            return IsolationType::ReadCommitted;
        case IsolationLevel::Snapshot:
            return IsolationType::SnapshotIsolation;
        case IsolationLevel::RepeatableRead:
            return IsolationType::RepeatableRead;
        case IsolationLevel::Serializable:
            return IsolationType::Serializable;
        default:
            assert(false);
            return IsolationType::ReadCommitted;
        }
    }

    static txservice::remote::CcProtocolType ConvertProtocol(
        txservice::CcProtocol proto)
    {
        switch (proto)
        {
        case CcProtocol::OCC:
            return CcProtocolType::Occ;
        case CcProtocol::Locking:
            return CcProtocolType::Locking;
        case CcProtocol::MVCC:
            return CcProtocolType::Mvcc;
        default:
            assert(false);
            return CcProtocolType::Occ;
        }
    }

    static txservice::remote::CcLockType ConvertLockType(
        txservice::LockType lock_type)
    {
        switch (lock_type)
        {
        case LockType::NoLock:
            return CcLockType::NoLock;
        case LockType::ReadIntent:
            return CcLockType::ReadIntent;
        case LockType::ReadLock:
            return CcLockType::ReadLock;
        case LockType::WriteIntent:
            return CcLockType::WriteIntent;
        case LockType::WriteLock:
            return CcLockType::WriteLock;
        default:
            assert(false);
            return CcLockType::NoLock;
        }
    }

    static txservice::remote::CommitType ConvertPostWriteType(
        txservice::PostWriteType write_type)
    {
        switch (write_type)
        {
        case PostWriteType::PrepareCommit:
            return CommitType::PrepareCommit;
        case PostWriteType::PostCommit:
            return CommitType::PostCommit;
        default:
            assert(false);
            return CommitType::PostCommit;
        }
    }

    static txservice::remote::RecordStatusType ConvertRecordStatus(
        txservice::RecordStatus rec_status)
    {
        switch (rec_status)
        {
        case RecordStatus::Normal:
            return RecordStatusType::NORMAL;
        case RecordStatus::Deleted:
            return RecordStatusType::DELETED;
        case RecordStatus::Unknown:
            return RecordStatusType::UNDEFINED;
        case RecordStatus::RemoteUnknown:
            return RecordStatusType::UNDEFINED;
        case RecordStatus::VersionUnknown:
            return RecordStatusType::VERSIONUNDEFIND;
        default:
            assert(false);
            return RecordStatusType::UNDEFINED;
        }
    }
};

class ToLocalType
{
public:
    static txservice::IsolationLevel ConvertIsolation(
        txservice::remote::IsolationType iso_type)
    {
        switch (iso_type)
        {
        case IsolationType::ReadCommitted:
            return IsolationLevel::ReadCommitted;
        case IsolationType::SnapshotIsolation:
            return IsolationLevel::Snapshot;
        case IsolationType::RepeatableRead:
            return IsolationLevel::RepeatableRead;
        case IsolationType::Serializable:
            return IsolationLevel::Serializable;
        default:
            return IsolationLevel::ReadCommitted;
        }
    }

    static txservice::CcProtocol ConvertProtocol(
        txservice::remote::CcProtocolType proto)
    {
        if (proto == CcProtocolType::Locking)
        {
            return CcProtocol::Locking;
        }
        else if (proto == CcProtocolType::Mvcc)
        {
            return CcProtocol::MVCC;
        }
        else
        {
            return CcProtocol::OCC;
        }
    }

    static txservice::LockType ConvertLockType(
        txservice::remote::CcLockType lock_type)
    {
        if (lock_type == CcLockType::NoLock)
        {
            return LockType::NoLock;
        }
        else if (lock_type == CcLockType::ReadIntent)
        {
            return LockType::ReadIntent;
        }
        else if (lock_type == CcLockType::ReadLock)
        {
            return LockType::ReadLock;
        }
        else if (lock_type == CcLockType::WriteIntent)
        {
            return LockType::WriteIntent;
        }
        else
        {
            return LockType::WriteLock;
        }
    }

    static txservice::PostWriteType ConvertCommitType(
        txservice::remote::CommitType commit_type)
    {
        if (commit_type == CommitType::PrepareCommit)
        {
            return PostWriteType::PrepareCommit;
        }
        else
        {
            return PostWriteType::PostCommit;
        }
    }

    static txservice::RecordStatus ConvertRecordStatusType(
        txservice::remote::RecordStatusType status_type)
    {
        switch (status_type)
        {
        case RecordStatusType::NORMAL:
            return RecordStatus::Normal;
        case RecordStatusType::DELETED:
            return RecordStatus::Deleted;
        case RecordStatusType::UNDEFINED:
            return RecordStatus::Unknown;
        default:
            return RecordStatus::Unknown;
        }
    }
};

}  // namespace remote
}  // namespace txservice
