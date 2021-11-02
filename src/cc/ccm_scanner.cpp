#include "cc/ccm_scanner.h"

#include "cc/cc_request.h"

using namespace txservice;

SingleShardScanner::SingleShardScanner()
    : cache_(ScanBatchSize),
      idx_(0),
      size_(0),
      last_batch_(false),
      direct_(ScanDirection::Forward),
      tab_name_(nullptr),
      indx_type_(ScanIndexType::Primary),
      open_(std::make_unique<ScanOpenCc>()),
      fetch_(std::make_unique<ScanBatchCc>())
{
}

ScanBatchCc *SingleShardScanner::NextBatch(CcMapScanner *scanners,
                                           CcHandlerResult<ScanTuple *> *hres)
{
    if (idx_ >= size_ && !last_batch_)
    {
        TxKey *start_key = cache_[size_ - 1].key_.get();
        fetch_->Set(tab_name_, start_key, false, direct_, this, scanners, hres);

        return fetch_.get();
    }
    else
    {
        return nullptr;
    }
}

ScanOpenCc *SingleShardScanner::Initi(const TableName *tab_name,
                                      ScanIndexType index_type,
                                      const TxKey *start_key,
                                      bool inclusive,
                                      ScanDirection direction,
                                      CcHandlerResult<size_t> *hres)
{
    last_batch_ = false;
    idx_ = 0;
    size_ = 0;
    direct_ = direction;
    tab_name_ = tab_name;
    indx_type_ = index_type;

    open_->Set(
        tab_name, index_type, start_key, inclusive, direction, this, hres);
    return open_.get();
}

ScanCloseCc *SingleShardScanner::Close(TxKey *end_key,
                                       bool inclusive,
                                       bool reverse)
{
    last_batch_ = true;
    idx_ = 0;
    close_->key_ = end_key;
    close_->inclusive_ = inclusive;
    close_->direct_ = reverse;

    return close_.get();
}