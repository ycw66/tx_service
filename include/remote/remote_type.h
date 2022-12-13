#pragma once

#include <cassert>  //assert

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
        case CcProtocol::OccRead:
            return CcProtocolType::OccRead;
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

    static txservice::remote::CcOperationType ConvertCcOperation(
        txservice::CcOperation cc_op)
    {
        switch (cc_op)
        {
        case CcOperation::Read:
            return CcOperationType::Read;
        case CcOperation::ReadForWrite:
            return CcOperationType::ReadForWrite;
        case CcOperation::ReadSkIndex:
            return CcOperationType::ReadSkIndex;
        case CcOperation::Write:
            return CcOperationType::Write;
        default:
            assert(false);
            return CcOperationType::Read;
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

    static txservice::remote::CcTableType ConvertTableType(
        txservice::TableType table_type)
    {
        switch (table_type)
        {
        case TableType::Primary:
            return CcTableType::Primary;
        case TableType::Secondary:
            return CcTableType::Secondary;
        case TableType::Catalog:
            return CcTableType::Catalog;
        case TableType::RangePartition:
            return CcTableType::RangePartition;
        default:
            assert(false);
            return CcTableType::Primary;
        }
    }

    static int ConvertCcErrorCode(txservice::CcErrorCode error_code)
    {
        return static_cast<int>(error_code);
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
        else if (proto == CcProtocolType::OccRead)
        {
            return CcProtocol::OccRead;
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

    static txservice::CcOperation ConvertCcOperation(
        txservice::remote::CcOperationType cc_op_type)
    {
        switch (cc_op_type)
        {
        case CcOperationType::Read:
            return CcOperation::Read;
        case CcOperationType::ReadForWrite:
            return CcOperation::ReadForWrite;
        case CcOperationType::ReadSkIndex:
            return CcOperation::ReadSkIndex;
        case CcOperationType::Write:
            return CcOperation::Write;
        default:
            assert(false);
            return CcOperation::Read;
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

    static txservice::TableType ConvertCcTableType(
        txservice::remote::CcTableType table_type)
    {
        switch (table_type)
        {
        case CcTableType::Primary:
            return TableType::Primary;
        case CcTableType::Secondary:
            return TableType::Secondary;
        case CcTableType::Catalog:
            return TableType::Catalog;
        default:
            return TableType::RangePartition;
        }
    }

    static txservice::CcErrorCode ConvertCcErrorCode(int error_code)
    {
        assert(error_code >= 0 &&
               error_code < static_cast<int>(CcErrorCode::LAST_ERROR_CODE));
        return txservice::CcErrorCode(error_code);
    }
};

}  // namespace remote
}  // namespace txservice
