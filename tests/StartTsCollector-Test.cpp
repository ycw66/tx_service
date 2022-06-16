// Let Catch provide main():
#define CATCH_CONFIG_MAIN

#include <catch2/catch.hpp>
#include <filesystem>
#include <iostream>

#include "log_service/include/log_server.h"
#include "mock/mock_catalog_factory.h"
#include "mock/mock_log_agent.h"
#include "store/int_mem_store.h"
#include "tx_execution.h"
#include "tx_service.h"
#include "tx_start_ts_collector.h"

using namespace txservice;

static std::unique_ptr<store::IntMemoryStore> store_hd =
    std::make_unique<store::IntMemoryStore>();
static MockCatalogFactory mock_catalog_factory{};
static std::vector<std::string> ips{"127.0.0.1"};
static std::vector<uint16_t> ports{8600};

TEST_CASE("TxStartTsCollector GlobalMinTxStartTs", "[start-ts-collector]")
{
    //== Create and start TxService
    std::filesystem::path output_dir = std::filesystem::path("/tmp/");
    output_dir.append("test_output");
    std::filesystem::create_directory(output_dir);
    // braft need: must add protocol
    std::string local_path = "local://" + output_dir.string();
    std::cout << "output_dir: " << local_path << std::endl;
    uint32_t node_id = 0;
    uint32_t core_num = 5;

    uint16_t log_server_port = 8602;
    std::vector<uint16_t> log_instance_ports = ports;
    for (auto it = log_instance_ports.begin(); it != log_instance_ports.end();
         it++)
    {
        *it += 2;
    }
    output_dir.append("tx_log");
    std::filesystem::create_directory(output_dir);
    // braft need: must add protocol
    std::string txlog_path = "local://" + output_dir.string();
    std::cout << "txlog_path: " << txlog_path << std::endl;

    std::unique_ptr<::txlog::LogServer> txlog_server =
        std::make_unique<::txlog::LogServer>(
            node_id, log_server_port, ips, log_instance_ports, txlog_path);
    int err = txlog_server->Start();
    if (err != 0)
    {
        std::cout << "==Failed to start the tx log service in this node."
                  << std::endl;
    }
    REQUIRE(err == 0);

    std::unique_ptr<TxService> tx_service_ = std::make_unique<TxService>(
        local_path,
        &mock_catalog_factory,
        node_id,
        core_num,
        &ips,
        &ports,
        store_hd.get(),
        std::make_unique<MockLogAgent>(txlog_server->LogGroupCount(),
                                       txlog_server->LogGroupReplicaNum()));

    tx_service_->Start();
    TxStartTsCollector::Instance().SetDelaySeconds(5);

    sleep(5);

    //== Init serveral transactions
    size_t tx_count = 5;
    std::vector<TransactionExecution *> txs;
    txs.resize(tx_count);
    std::vector<InitTxRequest *> init_tx_reqs;
    init_tx_reqs.resize(tx_count);

    for (size_t i = 0; i < tx_count; i++)
    {
        init_tx_reqs[i] = new InitTxRequest();
        init_tx_reqs[i]->Reset();
        txs[i] = tx_service_->NewTx();
        // REQUIRE(txs[i]->GetStartTs() == 0U);
        txs[i]->Execute(init_tx_reqs[i]);
        sleep(3);  // to use diffrent value of ts_base_
    }

    uint64_t min_tx_ts = UINT64_MAX;
    size_t min_tx_index = 0;
    for (size_t i = 0; i < tx_count; i++)
    {
        init_tx_reqs[i]->Wait();
        REQUIRE_FALSE(init_tx_reqs[i]->IsError());
        REQUIRE(txs[i]->GetStartTs() > 0U);

        std::cout << "tx#" << i << " start_ts: " << txs[i]->GetStartTs()
                  << std::endl;

        if (min_tx_ts > txs[i]->GetStartTs())
        {
            min_tx_ts = txs[i]->GetStartTs();
            min_tx_index = i;
        }
    }

    sleep(10);  // Assure "TxStartTsCollector" has collected the min start ts.

    uint64_t collected_min_ts =
        TxStartTsCollector::Instance().GlobalMinTxStartTs();
    REQUIRE(min_tx_ts == collected_min_ts);

    // commit tx1
    CommitTxRequest commit_req1;
    txs[min_tx_index]->Execute(&commit_req1);
    commit_req1.Wait();
    size_t committed_tx_index = min_tx_index;

    sleep(10);

    min_tx_ts = UINT64_MAX;
    for (size_t i = 0; i < tx_count; i++)
    {
        if (min_tx_ts > txs[i]->GetStartTs() && i != committed_tx_index)
        {
            min_tx_ts = txs[i]->GetStartTs();
            min_tx_index = i;
        }
    }

    collected_min_ts = TxStartTsCollector::Instance().GlobalMinTxStartTs();
    REQUIRE(min_tx_ts == collected_min_ts);

    for (auto *req : init_tx_reqs)
    {
        delete req;
    }
}