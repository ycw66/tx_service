#pragma once

#include <memory>
#include <vector>

#include "cc_req_base.h"

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

template <typename T>
class CircularQueue
{
public:
    CircularQueue(size_t capacity = 8) : head_(0), cnt_(0), capacity_(capacity)
    {
        vec_ = std::make_unique<T[]>(capacity);
    }

    ~CircularQueue() = default;

    void Enqueue(const T &item)
    {
        if (cnt_ == 0)
        {
            vec_[0] = item;
            head_ = 0;
            cnt_ = 1;
        }
        else if (cnt_ == capacity_)
        {
            size_t new_capacity = (size_t) (capacity_ * 1.5);
            std::unique_ptr<T[]> new_vec = std::make_unique<T[]>(new_capacity);

            // Before: 0-------Tail-Head---------N-1
            // After:  0----------------------------Tail------------M-1
            // Copy Head --> N-1
            std::copy(
                vec_.get() + head_, vec_.get() + capacity_, new_vec.get());

            size_t half_cnt = capacity_ - head_;
            // Copy 0 --> Tail
            std::copy(vec_.get(),
                      vec_.get() + cnt_ - half_cnt,
                      new_vec.get() + half_cnt);

            new_vec[capacity_] = item;
            cnt_ = capacity_ + 1;
            capacity_ = new_capacity;
            head_ = 0;
            vec_ = std::move(new_vec);
        }
        else
        {
            size_t tail = (head_ + cnt_) % capacity_;
            vec_[tail] = item;
            ++cnt_;
        }
    }

    void Dequeue()
    {
        if (cnt_ > 0)
        {
            ++head_;
            if (head_ == capacity_)
            {
                head_ = 0;
            }
            --cnt_;
        }
    }

    const T &Peek()
    {
        return vec_[head_];
    }

    size_t Size() const
    {
        return cnt_;
    }

private:
    std::unique_ptr<T[]> vec_;
    size_t head_;
    size_t cnt_;
    size_t capacity_;
};
}  // namespace txservice