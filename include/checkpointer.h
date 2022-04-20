#pragma once

#include <chrono>
#include <iostream>
#include <thread>
#include <vector>

#include "cc/cc_entry.h"
#include "cc/cc_request.h"
#include "cc/local_cc_shards.h"
#include "store/data_store_handler.h"
#include "util.h"

using namespace std::chrono;

namespace txservice
{
class Checkpointer
{
public:
    Checkpointer(LocalCcShards &shards, store::DataStoreWriteHandler *write_hd);
    ~Checkpointer();

    void Ckpt();

    void Run();

    /**
     * @brief Called by TxProcessor thread to notify checkpointer thread
     * to do checkpoint if there is no freeable entries to be kicked out
     * from ccmap.
     */
    void Notify();

    bool IsTerminated();

    void Terminate();

private:
    enum struct Status
    {
        Active,
        Terminating,
        Terminated
    };

    LocalCcShards &local_shards_;
    uint64_t last_ckpt_ts_;
    std::mutex mux_;
    std::condition_variable cv_;
    bool request_ckpt_;
    store::DataStoreWriteHandler *store_hd_;
    std::thread thd_;
    Status status_;

    TxService *tx_service_;
};
}  // namespace txservice
