#pragma once

#include <chrono>
#include <iostream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "cc/cc_entry.h"
#include "cc/cc_request.h"
#include "cc/local_cc_shards.h"
#include "txlog.h"
#include "util.h"

using namespace std::chrono;

namespace txservice
{

class Checkpointer
{
public:
    Checkpointer(LocalCcShards &shards,
                 store::DataStoreHandler *write_hd,
                 const uint32_t &checkpoint_interval,
                 TxLog *log_agent);

    ~Checkpointer();

    void Ckpt();

    /**
     * @brief Checkpoint one Entry to KvStore synchronously.
     * Now, only used for test.
     */
    bool CkptEntry(LruEntry *entry);

    void Run();

    /**
     * @brief Called by TxProcessor thread to notify checkpointer thread
     * to do checkpoint if there is no freeable entries to be kicked out
     * from ccmap.
     */
    void Notify();

    bool IsTerminated();

    void Terminate();

    void Join()
    {
        thd_.join();
    }

private:
    enum struct Status
    {
        Active,
        Terminating,
        Terminated
    };

    LocalCcShards &local_shards_;
    // last checkpoint timestamp of each cc node
    std::unordered_map<uint32_t, uint64_t> last_ckpt_ts_;
    std::mutex mux_;
    std::condition_variable cv_;
    bool request_ckpt_;
    store::DataStoreHandler *store_hd_;
    std::thread thd_;
    Status status_;
    const uint32_t checkpoint_interval_;

    TxService *tx_service_;
    TxLog *log_agent_;

    void TruncateLog(uint32_t node_group, int64_t term, uint64_t ckpt_ts);
};
}  // namespace txservice
