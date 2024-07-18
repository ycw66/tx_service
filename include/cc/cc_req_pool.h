#pragma once

#include <memory>
#include <vector>

#include "cc_req_base.h"
#include "circular_queue.h"

namespace txservice
{
template <typename T>
class CcRequestPool
{
public:
    CcRequestPool() : head_(0)
    {
        pool_.reserve(8);
        for (size_t idx = 0; idx < 8; ++idx)
        {
            pool_.emplace_back(std::make_unique<T>());
        }
    }

    T *NextRequest()
    {
        size_t count = 0;

        CcRequestBase *req_ptr = nullptr;
        while (count < pool_.size())
        {
            req_ptr = static_cast<CcRequestBase *>(pool_[head_].get());

            if (!req_ptr->InUse())
            {
                break;
            }

            ++head_;
            head_ = head_ == pool_.size() ? 0 : head_;
            ++count;
        }

        if (count == pool_.size())
        {
            size_t old_size = pool_.size();
            pool_.resize((size_t) (old_size * 1.5));
            for (size_t idx = old_size; idx < pool_.size(); ++idx)
            {
                pool_[idx] = std::make_unique<T>();
            }
            req_ptr = static_cast<CcRequestBase *>(pool_[old_size].get());
            req_ptr->Use();
            head_ = old_size + 1;
            return pool_[old_size].get();
        }
        else
        {
            req_ptr->Use();
            T *ptr = pool_[head_].get();
            ++head_;
            head_ = head_ == pool_.size() ? 0 : head_;
            return ptr;
        }
    }

private:
    std::vector<std::unique_ptr<T>> pool_;
    size_t head_;
};
}  // namespace txservice
