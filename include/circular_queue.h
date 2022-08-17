#pragma once

#include <memory>
#include <utility>

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
            std::copy(vec_.get(), vec_.get() + head_, new_vec.get() + half_cnt);

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

    void EnqueueAsFirst(const T &item)
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
            std::copy(vec_.get(), vec_.get() + head_, new_vec.get() + half_cnt);

            new_vec[new_capacity - 1] = item;
            head_ = new_capacity - 1;
            cnt_ = capacity_ + 1;
            capacity_ = new_capacity;
            vec_ = std::move(new_vec);
        }
        else
        {
            if (head_ != 0)
            {
                vec_[head_ - 1] = item;
                --head_;
            }
            else
            {
                vec_[capacity_ - 1] = item;
                head_ = capacity_ - 1;
            }
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

    size_t Capacity() const
    {
        return capacity_;
    }

    size_t MemUsage() const
    {
        return (sizeof(CircularQueue) + capacity_ * sizeof(uint64_t));
    }

private:
    std::unique_ptr<T[]> vec_;
    size_t head_;
    size_t cnt_;
    size_t capacity_;
};