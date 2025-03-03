/**
 *    Copyright (C) 2025 EloqData Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under either of the following two licenses:
 *    1. GNU Affero General Public License, version 3, as published by the Free
 *    Software Foundation.
 *    2. GNU General Public License as published by the Free Software
 *    Foundation; version 2 of the License.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License or GNU General Public License for more
 *    details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    and GNU General Public License V2 along with this program.  If not, see
 *    <http://www.gnu.org/licenses/>.
 *
 */
#include <iostream>

#include "cc/cc_map.h"
#include "cc/cc_request.h"
#include "cc/local_cc_shards.h"
#include "cc/scan.h"
#include "cc/sk_cc_map.h"
#include "cc/template_cc_map.h"
#include "checkpointer.h"
#include "read_write_set.h"
#include "sharder.h"
#include "store/data_store_handler.h"
#include "store/int_mem_store.h"
#include "tx_execution.h"
#include "tx_key.h"
#include "tx_record.h"
#include "tx_service.h"

#ifdef _MSC_VER
#include "remote/remote_cc_handler_win.h"
#endif

using namespace txservice;

void AdvanceCc(LocalCcShards &shards)
{
    for (size_t tid = 0; tid < shards.Count(); ++tid)
    {
        shards.ProcessRequests(tid);
    }
}

void ScanTest(int thd_cnt = 1)
{
    LocalCcShards cc_shards(0, thd_cnt);
    LocalCcHandler *hd = cc_shards.GetCcHandler(0);

    std::string tabname("tab");
    cc_shards.CreateCcTable<CompositeKey<int>, CompositeRecord<int>>(tabname);

    TransactionExecution::uptr txe =
        std::make_unique<TransactionExecution>(hd, nullptr);

    CompositeKey<int> key(0);
    TxKeyContainer key_c(&key);
    CompositeRecord<int> rec(0);
    TxRecordContainer rec_c(&rec);

    for (int v = 0; v < 100; ++v)
    {
        key.Reset(v);
        rec.Reset(v + 1000);

        txe->Reset();
        txe->Begin();
        txe->Upsert(tabname, key_c, rec_c);
        TxResult<bool> *res = txe->Commit();
        while (res->Status() == TxResultStatus::Unknown)
        {
            AdvanceCc(cc_shards);
            txe->Forward();
        }
    }
    cc_shards.PrintCcMap();

    CompositeKey<int> start_key(3);
    txe->Reset();
    txe->Begin();

    key.Reset(5);
    rec.Reset(5);
    txe->Upsert(tabname, key_c, rec_c);
    key.Reset(10);
    rec.Reset(10);
    txe->Upsert(tabname, key_c, rec_c);

    TxResult<size_t> *open_res = txe->ScanOpen(tabname,
                                               ScanIndexType::Primary,
                                               start_key,
                                               false,
                                               ScanDirection::Forward);

    while (open_res->Status() == TxResultStatus::Unknown)
    {
        AdvanceCc(cc_shards);
        txe->Forward();
    }

    size_t alias = open_res->Value();
    for (size_t idx = 0; idx < 10; ++idx)
    {
        TxResult<std::pair<const TxKey *, const TxRecord *>> *next_res =
            txe->ScanNext(alias);

        while (next_res->Status() == TxResultStatus::Unknown)
        {
            AdvanceCc(cc_shards);
            txe->Forward();
        }

        if (next_res->Value().first == nullptr)
        {
            break;
        }

        std::cout << "[" << next_res->Value().first->ToString() << ", "
                  << next_res->Value().second->ToString() << "]" << std::endl;
    }

    TxResult<bool> *res = txe->Commit();
    while (res->Status() == TxResultStatus::Unknown)
    {
        AdvanceCc(cc_shards);
        txe->Forward();
    }
    assert(res->Value() == true);
}

void ReadWriteTest(int thd_cnt = 1, CcProtocol protocol = CcProtocol::OCC)
{
    LocalCcShards cc_shards(0, thd_cnt);
    LocalCcHandler *hd = cc_shards.GetCcHandler(0);

    std::string tabname("tab");
    cc_shards.CreateCcTable<CompositeKey<int>, CompositeRecord<int>>(tabname);

    TransactionExecution::uptr txe =
        std::make_unique<TransactionExecution>(hd, nullptr);

    CompositeKey<int> key(0);
    TxKeyContainer key_c(&key);
    CompositeRecord<int> rec(0);
    TxRecordContainer rec_c(&rec);

    for (int v = 0; v < 100; ++v)
    {
        key.Reset(v);
        rec.Reset(v + 1000);

        txe->Reset();
        txe->Begin();
        txe->Upsert(tabname, key_c, rec_c);
        TxResult<bool> *res = txe->Commit();
        while (res->Status() == TxResultStatus::Unknown)
        {
            AdvanceCc(cc_shards);
            txe->Forward();
        }
    }

    cc_shards.PrintCcMap();

    // transaction execution
    TransactionExecution::uptr tx1 =
        std::make_unique<TransactionExecution>(hd, nullptr, protocol);

    TransactionExecution::uptr tx2 =
        std::make_unique<TransactionExecution>(hd, nullptr, protocol);

    CompositeKey<int> k38(38);
    CompositeRecord<int> r38(0);
    tx1->Reset(protocol);
    tx1->Begin();
    TxResult<RecordStatus> *read_res = tx1->Read(tabname, k38, r38);
    while (read_res->Status() == TxResultStatus::Unknown)
    {
        AdvanceCc(cc_shards);
        tx1->Forward();
    }

    // Tx1 reads key 38, whose value should be 1038.
    assert(std::get<0>(r38.Tuple()) == 1038);
    std::cout << "key: 38, rec: " << r38.ToString() << std::endl;

    TxKeyContainer k38_c(&k38);
    r38.Reset(999);
    TxRecordContainer r38_c(&r38);
    tx2->Reset(protocol);
    tx2->Begin();
    tx2->Upsert(tabname, k38_c, r38_c);
    TxResult<bool> *commit_res2 = tx2->Commit();

    // Tx2 runs toward the end.
    size_t round = 0;
    while (commit_res2->Status() == TxResultStatus::Unknown && round < 10)
    {
        AdvanceCc(cc_shards);
        tx2->Forward();
        ++round;
    }

    if (protocol == CcProtocol::OCC)
    {
        // Tx2 updates k38 and commits.
        assert(commit_res2->Status() == TxResultStatus::Unknown &&
               commit_res2->Value() == true);
    }
    else
    {
        // Tx2 cannot finish and is blocked because of Tx1 holding a read lock
        // on k38.
        assert(commit_res2->Status() == TxResultStatus::Unknown);
    }

    TxResult<bool> *commit_res = tx1->Commit();
    // Tx1 runs toward the end.
    round = 0;
    while (commit_res->Status() == TxResultStatus::Unknown && round < 10)
    {
        AdvanceCc(cc_shards);
        tx1->Forward();
        ++round;
    }

    if (protocol == CcProtocol::OCC)
    {
        // Tx1 should abort because Tx2 has committed.
        assert(commit_res->Value() == false);
    }
    else
    {
        // Tx2 should commit because it blocks T2.
        assert(commit_res->Value() == true);
        // Tx2 can now run toward the end and finish.
        while (commit_res2->Status() == TxResultStatus::Unknown)
        {
            AdvanceCc(cc_shards);
            tx2->Forward();
        }
        assert(commit_res2->Value() == true);
    }

    r38.Reset(10000);

    // Write-write conflict.
    // Tx1 first updates k38.
    tx1->Reset(protocol);
    tx1->Begin();
    tx1->Upsert(tabname, k38_c, r38_c);
    commit_res = tx1->Commit();
    // Tx1 acquires the write intention first.
    AdvanceCc(cc_shards);

    // Tx2 tries to update the same key.
    tx2->Reset(protocol);
    tx2->Begin();
    tx2->Upsert(tabname, k38_c, r38_c);
    commit_res2 = tx2->Commit();

    // Tx2 runs toward the end.
    round = 0;
    while (commit_res2->Status() == TxResultStatus::Unknown && round < 10)
    {
        AdvanceCc(cc_shards);
        tx2->Forward();
        ++round;
    }

    if (protocol == CcProtocol::OCC)
    {
        // Tx2 should abort because of the write-write conflict.
        assert(commit_res2->Value() == false);
    }
    else
    {
        // Tx2 cannot finish because it's blocked by Tx1.
        assert(commit_res2->Status() == TxResultStatus::Unknown);
    }

    // Tx1 now aborts.
    // commit_res = tx1->Abort();
    while (commit_res->Status() == TxResultStatus::Unknown)
    {
        AdvanceCc(cc_shards);
        tx1->Forward();
    }
    assert(commit_res->Value() == true);

    if (protocol == CcProtocol::Locking)
    {
        // Tx2 can now finish and commit.
        round = 0;
        while (commit_res2->Status() == TxResultStatus::Unknown && round < 10)
        {
            AdvanceCc(cc_shards);
            tx2->Forward();
            ++round;
        }

        assert(commit_res2->Value());
    }
}

void TestTxService(uint32_t thd_cnt = 1)
{
    txservice::TxService service(0, thd_cnt);
    service.Start();

    std::string tabname("tab");

    service.CreateCcTable<CompositeKey<int>, CompositeRecord<int>>(tabname);

    std::cout << "Start inserting ..." << std::endl;

    std::thread thd1(
        [&service, &tabname]
        {
            for (int i = 0; i < 10000; ++i)
            {
                TransactionExecution *ins_tx = service.NewTx();
                BeginRequest begin_req;
                ins_tx->Execute(&begin_req);
                begin_req.Wait();

                CompositeKey<int> key(i);
                CompositeRecord<int> record(i + 1000);

                UpsertRequest ups_req(&tabname, &key, &record);
                ins_tx->Execute(&ups_req);
                ups_req.Wait();

                CommitRequest commit_req;
                ins_tx->Execute(&commit_req);
                commit_req.Wait();

                // std::cout << i << std::endl;
            }
        });

    std::thread thd2(
        [&service, &tabname]
        {
            for (int i = 10000; i < 20000; ++i)
            {
                TransactionExecution *ins_tx = service.NewTx();
                BeginRequest begin_req;
                ins_tx->Execute(&begin_req);
                begin_req.Wait();

                CompositeKey<int> key(i);
                CompositeRecord<int> record(i + 1000);

                UpsertRequest ups_req(&tabname, &key, &record);
                ins_tx->Execute(&ups_req);
                ups_req.Wait();

                CommitRequest commit_req;
                ins_tx->Execute(&commit_req);
                commit_req.Wait();

                // std::cout << i << std::endl;
            }
        });

    thd1.join();
    thd2.join();

    /*for (int i = 0; i < 10000; ++i)
    {
        TransactionExecution *ins_tx = service.NewTx();
        BeginRequest begin_req(true);
        ins_tx->Execute(&begin_req);
        begin_req.Wait();

        CompositeKey<int> key(i);
        CompositeRecord<int> record(i + 1000);

        UpsertRequest ups_req(&tabname, &key, &record, true);
        ins_tx->Execute(&ups_req);
        ups_req.Wait();

        CommitRequest commit_req(true);
        ins_tx->Execute(&commit_req);
        commit_req.Wait();

        std::cout << i << std::endl;
    }*/

    LocalCcShards &cc_shards = service.CcShards();
    cc_shards.PrintCcMap();

    TransactionExecution *txm = service.NewTx();
    BeginRequest begin_req;
    txm->Execute(&begin_req);
    begin_req.Wait();

    CompositeKey<int>::Uptr k19 = std::make_unique<CompositeKey<int>>(19);
    CompositeRecord<int>::Uptr r9999 =
        std::make_unique<CompositeRecord<int>>(9999);

    UpsertRequest ups_req(&tabname, std::move(k19), std::move(r9999));
    txm->Execute(&ups_req);
    ups_req.Wait();
    assert(ups_req.Finish());

    CompositeKey<int>::Uptr k20 = std::make_unique<CompositeKey<int>>(20);
    CompositeRecord<int>::Uptr r9998 =
        std::make_unique<CompositeRecord<int>>(9998);
    UpsertRequest ups_req2(&tabname, std::move(k20), std::move(r9998));
    txm->Execute(&ups_req2);
    ups_req2.Wait();
    assert(ups_req2.Finish());

    CompositeKey<int>::Uptr k19a = std::make_unique<CompositeKey<int>>(19);
    CompositeRecord<int>::Uptr r10010 =
        std::make_unique<CompositeRecord<int>>(10010);
    UpsertRequest ups_req3(&tabname, std::move(k19a), std::move(r10010));
    txm->Execute(&ups_req3);
    ups_req3.Wait();
    assert(ups_req3.Finish());

    CompositeKey<int> k19b(19);
    CompositeRecord<int> r19(0);
    ReadRequest read_req(&tabname, k19b, r19, ReadType::Inside);
    txm->Execute(&read_req);
    read_req.Wait();
    assert(std::get<0>(r19.Tuple()) == 10010);

    CommitRequest commit_req;
    txm->Execute(&commit_req);
    commit_req.Wait();
    assert(commit_req.Result() == true);

    txm = service.NewTx();
    BeginRequest begin_req2;
    txm->Execute(&begin_req2);
    begin_req2.Wait();

    ReadRequest read_req2(&tabname, k19b, r19, ReadType::Inside);
    txm->Execute(&read_req2);
    read_req2.Wait();
    assert(std::get<0>(r19.Tuple()) == 10010);

    CompositeKey<int> kint_max(INT32_MAX);
    CompositeRecord<int> rint_max(0);
    ReadRequest read_req3(&tabname, kint_max, rint_max, ReadType::Inside);
    txm->Execute(&read_req3);
    read_req3.Wait();
    assert(read_req3.Finish());

    assert(read_req3.Result() == RecordStatus::Unknown);

    commit_req.Reset();
    txm->Execute(&commit_req);
    commit_req.Wait();
    assert(commit_req.Finish());

    assert(commit_req.Result() == true);

    /*CompositeKey<int> start_key(6);
    txm = service.NewTx();

    ScanOpenRequest scan_open(
        &tabname, ScanIndexType::Primary, &start_key, false, true);
    txm->Execute(&scan_open);
    scan_open.Wait();
    size_t alias = scan_open.Result();

    ScanNextRequest scan_next(&tabname, alias, true);

    size_t cnt = 0;
    do
    {
        scan_next.Reset();
        txm->Execute(&scan_next);
        scan_next.Wait();
        assert(scan_next.Finish());
        std::pair<TxKey *, TxRecord *> &tuple = scan_next.Result();

        if (tuple.first == nullptr)
        {
            break;
        }

        std::cout << "key: " << tuple.first->ToString()
                  << ", val: " << tuple.second->ToString() << std::endl;

        ++cnt;
    } while (cnt < 10);*/
}

void TxServiceThroughput(uint32_t shard_cnt, uint32_t reader_cnt)
{
    txservice::TxService service(0, shard_cnt);
    service.Start();

    std::string tabname("tab");
    std::cout << "Start inserting ..." << std::endl;

    std::thread thd1(
        [&service, &tabname]
        {
            for (int i = 0; i < 100000; ++i)
            {
                TransactionExecution *ins_tx = service.NewTx();
                BeginRequest begin_req;
                ins_tx->Execute(&begin_req);
                begin_req.Wait();

                CompositeKey<int> key(i);
                CompositeRecord<int> record(i + 1000);

                UpsertRequest ups_req(&tabname, &key, &record);
                ins_tx->Execute(&ups_req);
                ups_req.Wait();

                CommitRequest commit_req;
                ins_tx->Execute(&commit_req);
                commit_req.Wait();

                // std::cout << i << std::endl;
            }
        });

    std::thread thd2(
        [&service, &tabname]
        {
            for (int i = 100000; i < 200000; ++i)
            {
                TransactionExecution *ins_tx = service.NewTx();
                BeginRequest begin_req;
                ins_tx->Execute(&begin_req);
                begin_req.Wait();

                CompositeKey<int> key(i);
                CompositeRecord<int> record(i + 1000);

                UpsertRequest ups_req(&tabname, &key, &record);
                ins_tx->Execute(&ups_req);
                ups_req.Wait();

                CommitRequest commit_req;
                ins_tx->Execute(&commit_req);
                commit_req.Wait();

                // std::cout << i << std::endl;
            }
        });

    thd1.join();
    thd2.join();

    LocalCcShards &cc_shards = service.CcShards();
    cc_shards.PrintCcMap();

    std::condition_variable cv;
    std::mutex mx;
    size_t finish_cnt = 0;
    bool start = false;
    std::vector<std::thread> readers;
    for (int rid = 0; rid < (int) reader_cnt; ++rid)
    {
        readers.emplace_back(std::thread(
            [&service, &tabname, &cv, &mx, &start, rid, &finish_cnt]
            {
                {
                    std::unique_lock<std::mutex> lk(mx);
                    cv.wait(lk, [&start] { return start; });
                }

                for (int i = rid * 1000; i < 100000 + rid * 1000; ++i)
                {
                    TransactionExecution *read_tx = service.NewTx();
                    BeginRequest begin_req;
                    read_tx->Execute(&begin_req);
                    begin_req.Wait();

                    CompositeKey<int> key(i);
                    CompositeRecord<int> record(0);

                    ReadRequest read_req(
                        &tabname, key, record, ReadType::Inside);
                    read_tx->Execute(&read_req);
                    read_req.Wait();

                    assert(std::get<0>(record.Tuple()) == i + 1000);

                    CommitRequest commit_req;
                    read_tx->Execute(&commit_req);
                    commit_req.Wait();

                    assert(commit_req.Result() == true);
                }

                {
                    std::unique_lock<std::mutex> lk(mx);
                    ++finish_cnt;
                }
                cv.notify_all();
            }));
    }

    auto t1 = std::chrono::high_resolution_clock::now();

    {
        std::unique_lock<std::mutex> lk(mx);
        start = true;
    }
    cv.notify_all();

    {
        std::unique_lock<std::mutex> lk(mx);
        cv.wait(lk,
                [&finish_cnt, &reader_cnt]
                { return finish_cnt == reader_cnt; });
    }

    auto t2 = std::chrono::high_resolution_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count();

    int64_t throughput = (int64_t) 100000 * reader_cnt * 1000 / duration;
    std::cout << "Time: " << duration << "ms" << std::endl;
    std::cout << "Throughput: " << throughput << std::endl;

    for (size_t rid = 0; rid < reader_cnt; ++rid)
    {
        readers.at(rid).join();
    }
}

void MultiTxServices(uint32_t core_cnt = 1)
{
    Sharder::Instance(2);
    std::string tabname("tab");

    txservice::TxService serv1(0, core_cnt, 8000);
    serv1.CreateCcTable<CompositeKey<int>, CompositeRecord<int>>(tabname);
    serv1.Start();
    txservice::TxService serv2(1, core_cnt, 8001);
    serv2.CreateCcTable<CompositeKey<int>, CompositeRecord<int>>(tabname);
    serv2.Start();

#ifdef _MSC_VER
    remote::RemoteCcHandler_Win *whd =
        static_cast<remote::RemoteCcHandler_Win *>(serv1.remote_hd_.get());
    whd->AddTxService(0, &serv1);
    whd->AddTxService(1, &serv2);

    whd = static_cast<remote::RemoteCcHandler_Win *>(serv2.remote_hd_.get());
    whd->AddTxService(0, &serv1);
    whd->AddTxService(1, &serv2);
#else
    std::string ip1("127.0.0.1:8000");
    std::string ip2("127.0.0.1:8001");

    remote::RemoteCcHandler_Brpc *rpc_hd =
        static_cast<remote::RemoteCcHandler_Brpc *>(serv1.remote_hd_.get());
    rpc_hd->AddNode(0, ip1);
    rpc_hd->AddNode(1, ip2);

    rpc_hd =
        static_cast<remote::RemoteCcHandler_Brpc *>(serv2.remote_hd_.get());
    rpc_hd->AddNode(0, ip1);
    rpc_hd->AddNode(1, ip2);
#endif

    std::cout << "Start inserting ..." << std::endl;

    for (int i = 924; i < 1024; ++i)
    {
        TransactionExecution *ins_tx = serv2.NewTx();
        BeginRequest begin_req;
        ins_tx->Execute(&begin_req);
        begin_req.Wait();

        CompositeKey<int> key(i);
        CompositeRecord<int> record(i + 1000);

        UpsertRequest ups_req(&tabname, &key, &record);
        ins_tx->Execute(&ups_req);
        ups_req.Wait();

        CommitRequest commit_req;
        ins_tx->Execute(&commit_req);
        commit_req.Wait();
    }

    for (int i = 1024; i < 1124; ++i)
    {
        TransactionExecution *ins_tx = serv1.NewTx();
        BeginRequest begin_req;
        ins_tx->Execute(&begin_req);
        begin_req.Wait();

        CompositeKey<int> key(i);
        CompositeRecord<int> record(i + 1000);

        UpsertRequest ups_req(&tabname, &key, &record);
        ins_tx->Execute(&ups_req);
        ups_req.Wait();

        CommitRequest commit_req;
        ins_tx->Execute(&commit_req);
        commit_req.Wait();

        assert(commit_req.Result());
    }

    LocalCcShards &cc_shards_1 = serv1.CcShards();
    cc_shards_1.PrintCcMap();

    LocalCcShards &cc_shards_2 = serv2.CcShards();
    cc_shards_2.PrintCcMap();

    for (int i = 924; i < 1124; ++i)
    {
        TransactionExecution *read_tx = serv1.NewTx();
        BeginRequest begin_req;
        read_tx->Execute(&begin_req);
        begin_req.Wait();

        CompositeKey<int> key(i);
        CompositeRecord<int> record(0);

        ReadRequest read_req(&tabname, key, record, ReadType::Inside);
        read_tx->Execute(&read_req);
        read_req.Wait();

        assert(read_req.Result() == RecordStatus::Normal);

        if (std::get<0>(record.Tuple()) != i + 1000)
        {
            std::cout << "key: " << key.ToString()
                      << ", val: " << std::get<0>(record.Tuple()) << std::endl;
        }
        assert(std::get<0>(record.Tuple()) == i + 1000);

        CommitRequest commit_req;
        read_tx->Execute(&commit_req);
        commit_req.Wait();

        assert(commit_req.Result() == true);
    }

    CompositeKey<int> start_key(930);
    TransactionExecution *scan_tx = serv2.NewTx();
    ScanOpenRequest scan_open_req(
        &tabname,
        ScanIndexType::Primary,
        NegativeInfinity<CompositeKey<int>>::Instance(),
        //&start_key,
        false,
        ScanDirection::Forward);
    scan_tx->Execute(&scan_open_req);
    scan_open_req.Wait();

    size_t alias = scan_open_req.Result();
    for (size_t idx = 0; idx < 20; ++idx)
    {
        ScanNextRequest scan_next_req(alias);
        scan_tx->Execute(&scan_next_req);
        scan_next_req.Wait();

        const std::pair<const TxKey *, const TxRecord *> &tuple =
            scan_next_req.Result();

        if (tuple.first == nullptr)
        {
            break;
        }

        std::cout << "[" << tuple.first->ToString() << ", "
                  << tuple.second->ToString() << "]" << std::endl;
    }
}

void SecondaryIndexTest(int thd_cnt = 1)
{
    LocalCcShards cc_shards(0, thd_cnt);
    LocalCcHandler *hd = cc_shards.GetCcHandler(0);

    std::string tabname("tab");
    cc_shards.CreateCcTable<CompositeKey<int>, CompositeRecord<std::string>>(
        tabname);
    std::string indexname("tab_index");
    cc_shards.CreateSkCcTable<CompositeKey<std::string>, CompositeKey<int>>(
        indexname);

    TransactionExecution::uptr txe =
        std::make_unique<TransactionExecution>(hd, nullptr);

    CompositeKey<int> key(0);
    TxKeyContainer key_c(&key);
    CompositeRecord<std::string> rec;
    TxRecordContainer rec_c(&rec);

    CompositeKey<std::string> sk;
    SecondaryKeys sk_vec;

    for (int v = 0; v < 100; ++v)
    {
        key.Reset(v);

        char prefix = 'a' + (v % 26);
        std::string str_val;
        str_val.append(&prefix, 1);
        // str_val.append(std::to_string(v + 1000));

        rec.Reset(str_val);
        sk.Reset(str_val);

        sk_vec.clear();
        sk_vec.emplace_back(&indexname, &sk, false);

        txe->Reset();
        txe->Begin();
        txe->Upsert(tabname, key_c, rec_c, &sk_vec);
        TxResult<bool> *res = txe->Commit();
        while (res->Status() == TxResultStatus::Unknown)
        {
            AdvanceCc(cc_shards);
            txe->Forward();
        }
    }
    cc_shards.PrintCcMap();

    CompositeKey<std::string> start_key(std::string("c0000"));
    txe->Reset();
    txe->Begin();

    TxResult<size_t> *open_res = txe->ScanOpen(indexname,
                                               ScanIndexType::Secondary,
                                               start_key,
                                               false,
                                               ScanDirection::Backward);

    while (open_res->Status() == TxResultStatus::Unknown)
    {
        AdvanceCc(cc_shards);
        txe->Forward();
    }

    size_t alias = open_res->Value();
    for (size_t idx = 0; idx < 10; ++idx)
    {
        TxResult<std::pair<const TxKey *, const TxRecord *>> *next_res =
            txe->ScanNext(alias);

        while (next_res->Status() == TxResultStatus::Unknown)
        {
            AdvanceCc(cc_shards);
            txe->Forward();
        }

        if (next_res->Value().first == nullptr)
        {
            break;
        }

        std::cout << "[" << next_res->Value().first->ToString() << "]"
                  << std::endl;
    }
    std::cout << std::endl;

    TxResult<bool> *res = txe->Commit();
    while (res->Status() == TxResultStatus::Unknown)
    {
        AdvanceCc(cc_shards);
        txe->Forward();
    }

    txe->Reset();
    txe->Begin();

    char prefix = 'a';
    std::string old_val;
    old_val.append(&prefix, 1);
    // old_val.append(std::to_string(1000));
    CompositeKey<std::string> old_sk(old_val);
    TxKeyContainer old_sk_c(&old_sk, ContainerType::immutable_ref);

    prefix = 'b';
    std::string new_val;
    new_val.append(&prefix, 1);
    new_val.append(std::to_string(1000));
    CompositeKey<std::string> new_sk(new_val);
    TxKeyContainer new_sk_c(&new_sk, ContainerType::immutable_ref);

    sk_vec.clear();
    sk_vec.emplace_back(&indexname, &old_sk, true);
    sk_vec.emplace_back(&indexname, &new_sk, false);

    key.Reset(0);
    rec.Reset(new_val);
    txe->Upsert(tabname, key_c, rec_c, &sk_vec);

    res = txe->Commit();
    while (res->Status() == TxResultStatus::Unknown)
    {
        AdvanceCc(cc_shards);
        txe->Forward();
    }

    txe->Reset();
    txe->Begin();

    open_res = txe->ScanOpen(indexname,
                             ScanIndexType::Secondary,
                             *NegativeInfinity<CompositeKey<int>>::Instance(),
                             false,
                             ScanDirection::Forward);

    while (open_res->Status() == TxResultStatus::Unknown)
    {
        AdvanceCc(cc_shards);
        txe->Forward();
    }

    alias = open_res->Value();
    for (size_t idx = 0; idx < 10;)
    {
        TxResult<std::pair<const TxKey *, const TxRecord *>> *next_res =
            txe->ScanNext(alias);

        while (next_res->Status() == TxResultStatus::Unknown)
        {
            AdvanceCc(cc_shards);
            txe->Forward();
        }

        if (next_res->Value().first == nullptr)
        {
            break;
        }

        if (next_res->Value().second == nullptr)
        {
            // The index key has been deleted.
            continue;
        }

        std::cout << "[" << next_res->Value().first->ToString() << "]"
                  << std::endl;

        ++idx;
    }
}

void SecondaryIndexTestTxService(int thd_cnt = 1)
{
    Sharder::Instance(2);
    std::string tabname("tab");
    std::string indexname("tab_index");

    txservice::TxService serv1(0, thd_cnt, 8000);
    serv1.CreateCcTable<CompositeKey<int>, CompositeRecord<std::string>>(
        tabname);
    serv1.CreateIndexCcTable<CompositeKey<std::string>, CompositeKey<int>>(
        indexname);
    serv1.Start();

    txservice::TxService serv2(1, thd_cnt, 8001);
    serv2.CreateCcTable<CompositeKey<int>, CompositeRecord<std::string>>(
        tabname);
    serv2.CreateIndexCcTable<CompositeKey<std::string>, CompositeKey<int>>(
        indexname);
    serv2.Start();

#ifdef _MSC_VER
    remote::RemoteCcHandler_Win *whd =
        static_cast<remote::RemoteCcHandler_Win *>(serv1.remote_hd_.get());
    whd->AddTxService(0, &serv1);
    whd->AddTxService(1, &serv2);

    whd = static_cast<remote::RemoteCcHandler_Win *>(serv2.remote_hd_.get());
    whd->AddTxService(0, &serv1);
    whd->AddTxService(1, &serv2);
#else
    std::string ip1("127.0.0.1:8000");
    std::string ip2("127.0.0.1:8001");

    remote::RemoteCcHandler_Brpc *rpc_hd =
        static_cast<remote::RemoteCcHandler_Brpc *>(serv1.remote_hd_.get());
    rpc_hd->AddNode(0, ip1);
    rpc_hd->AddNode(1, ip2);

    rpc_hd =
        static_cast<remote::RemoteCcHandler_Brpc *>(serv2.remote_hd_.get());
    rpc_hd->AddNode(0, ip1);
    rpc_hd->AddNode(1, ip2);
#endif

    std::cout << "Start inserting ..." << std::endl;

    CompositeKey<int> key(0);
    CompositeRecord<std::string> rec;

    CompositeKey<std::string> sk;
    SecondaryKeys sk_vec;

    for (int i = 0; i < 100; ++i)
    {
        TransactionExecution *ins_tx = serv2.NewTx();
        BeginRequest begin_req;
        ins_tx->Execute(&begin_req);
        begin_req.Wait();

        key.Reset(i);

        char prefix = 'a' + (i % 26);
        std::string str_val;
        str_val.append(&prefix, 1);
        // str_val.append(std::to_string(v + 1000));

        rec.Reset(str_val);
        sk.Reset(str_val);

        sk_vec.clear();
        sk_vec.emplace_back(&indexname, &sk, false);

        UpsertRequest ups_req(&tabname, &key, &rec, &sk_vec);
        ins_tx->Execute(&ups_req);
        ups_req.Wait();

        CommitRequest commit_req;
        ins_tx->Execute(&commit_req);
        commit_req.Wait();
    }

    LocalCcShards &cc_shards_1 = serv1.CcShards();
    cc_shards_1.PrintCcMap();

    LocalCcShards &cc_shards_2 = serv2.CcShards();
    cc_shards_2.PrintCcMap();

    CompositeKey<std::string> start_key(std::string("a0"));

    TransactionExecution *scan_tx = serv2.NewTx();
    ScanOpenRequest scan_open_req(
        &indexname,
        ScanIndexType::Secondary,
        // NegativeInfinity<CompositeKey<std::string>>::Instance(),
        &start_key,
        true,
        ScanDirection::Backward);
    scan_tx->Execute(&scan_open_req);
    scan_open_req.Wait();

    size_t alias = scan_open_req.Result();
    for (size_t idx = 0; idx < 10; ++idx)
    {
        ScanNextRequest scan_next_req(alias);
        scan_tx->Execute(&scan_next_req);
        scan_next_req.Wait();

        const std::pair<const TxKey *, const TxRecord *> &tuple =
            scan_next_req.Result();

        if (tuple.first == nullptr)
        {
            break;
        }

        std::cout << "[" << tuple.first->ToString() << "]" << std::endl;
    }
    std::cout << std::endl;
}

void ReadOutsideTest(int thd_cnt = 1)
{
    LocalCcShards cc_shards(0, thd_cnt);
    LocalCcHandler *hd = cc_shards.GetCcHandler(0);

    std::string tabname("tab");
    cc_shards.CreateCcTable<CompositeKey<int>, CompositeRecord<int>>(tabname);

    TransactionExecution txe(hd, nullptr);

    CompositeKey<int> key(0);
    CompositeRecord<int> rec(0);

    txe.Reset();
    txe.Begin();

    TxResult<RecordStatus> *read_res = txe.Read(tabname, key, rec);
    while (read_res->Status() == TxResultStatus::Unknown)
    {
        AdvanceCc(cc_shards);
        txe.Forward();
    }

    assert(read_res->Value() == RecordStatus::Unknown);

    rec.Reset(1000);

    key.Reset(100);
    read_res = txe.Read(tabname, key, rec, ReadType::Inside);
    while (read_res->Status() == TxResultStatus::Unknown)
    {
        AdvanceCc(cc_shards);
        txe.Forward();
    }
    assert(read_res->Value() == RecordStatus::Unknown);

    TransactionExecution txe2(hd, nullptr);
    txe2.Begin();

    rec.Reset(1100);
    read_res = txe2.Read(tabname, key, rec, ReadType::OutsideNormal);
    while (read_res->Status() == TxResultStatus::Unknown)
    {
        AdvanceCc(cc_shards);
        txe2.Forward();
    }
    assert(read_res->Value() == RecordStatus::Normal);

    key.Reset(0);
    read_res = txe2.Read(tabname, key, rec, ReadType::OutsideDeleted);
    while (read_res->Status() == TxResultStatus::Unknown)
    {
        AdvanceCc(cc_shards);
        txe2.Forward();
    }
    assert(read_res->Value() == RecordStatus::Deleted);

    read_res = txe.Read(tabname, key, rec, ReadType::Inside);
    while (read_res->Status() == TxResultStatus::Unknown)
    {
        AdvanceCc(cc_shards);
        txe.Forward();
    }
    assert(read_res->Value() == RecordStatus::Deleted);

    key.Reset(100);
    read_res = txe.Read(tabname, key, rec, ReadType::Inside);
    while (read_res->Status() == TxResultStatus::Unknown)
    {
        AdvanceCc(cc_shards);
        txe.Forward();
    }
    assert(read_res->Value() == RecordStatus::Normal);
    assert(std::get<0>(rec.Tuple()) == 1100);
}

void ReadOutsideTestTxService(int thd_cnt = 1)
{
    Sharder::Instance(2);
    std::string tabname("tab");

    txservice::TxService serv1(0, thd_cnt, 8000);
    serv1.CreateCcTable<CompositeKey<int>, CompositeRecord<int>>(tabname);
    serv1.Start();

    txservice::TxService serv2(1, thd_cnt, 8001);
    serv2.CreateCcTable<CompositeKey<int>, CompositeRecord<int>>(tabname);
    serv2.Start();

#ifdef _MSC_VER
    remote::RemoteCcHandler_Win *whd =
        static_cast<remote::RemoteCcHandler_Win *>(serv1.remote_hd_.get());
    whd->AddTxService(0, &serv1);
    whd->AddTxService(1, &serv2);

    whd = static_cast<remote::RemoteCcHandler_Win *>(serv2.remote_hd_.get());
    whd->AddTxService(0, &serv1);
    whd->AddTxService(1, &serv2);
#else
    std::string ip1("127.0.0.1:8000");
    std::string ip2("127.0.0.1:8001");

    remote::RemoteCcHandler_Brpc *rpc_hd =
        static_cast<remote::RemoteCcHandler_Brpc *>(serv1.remote_hd_.get());
    rpc_hd->AddNode(0, ip1);
    rpc_hd->AddNode(1, ip2);

    rpc_hd =
        static_cast<remote::RemoteCcHandler_Brpc *>(serv2.remote_hd_.get());
    rpc_hd->AddNode(0, ip1);
    rpc_hd->AddNode(1, ip2);
#endif

    TransactionExecution *tx1 = serv2.NewTx();
    BeginRequest begin_req;
    tx1->Execute(&begin_req);
    begin_req.Wait();

    const CompositeKey<int> k1(0);
    const CompositeKey<int> k2(0);
    CompositeRecord<int> rec1(0);
    CompositeRecord<int> rec2(0);

    ReadRequest read1(&tabname, k1, rec1, ReadType::Inside);
    tx1->Execute(&read1);
    read1.Wait();
    assert(read1.Result() == RecordStatus::Unknown);

    TransactionExecution *tx2 = serv2.NewTx();
    BeginRequest begin2;
    tx2->Execute(&begin2);
    begin2.Wait();

    ReadRequest read2(&tabname, k2, rec2, ReadType::Inside);
    tx2->Execute(&read2);
    read2.Wait();
    assert(read2.Result() == RecordStatus::Unknown);

    rec2.Reset(1001);
    ReadOutsideRequest read_outside2(rec2, false);
    tx2->Execute(&read_outside2);
    read_outside2.Wait();

    ReadOutsideRequest read_outside1(rec1, true);
    tx1->Execute(&read_outside1);
    read_outside1.Wait();

    using namespace std::chrono_literals;

    auto t10min = std::chrono::minutes(10);
    std::this_thread::sleep_for(t10min);
}

void CkptTest(uint16_t thd_cnt = 1)
{
    LocalCcShards cc_shards(0, thd_cnt);
    LocalCcHandler *hd = cc_shards.GetCcHandler(0);

    std::string tabname("tab");
    cc_shards.CreateCcTable<CompositeKey<int>, CompositeRecord<int>>(tabname);

    store::IntMemoryStore int_store;
    Checkpointer ckpter(cc_shards, &int_store);

    TransactionExecution::uptr txe =
        std::make_unique<TransactionExecution>(hd, nullptr);

    CompositeKey<int> key(0);
    TxKeyContainer key_c(&key);
    CompositeRecord<int> rec;
    TxRecordContainer rec_c(&rec);

    for (int v = 0; v < 10000; ++v)
    {
        key.Reset(v);
        rec.Reset(v + 1000);

        txe->Reset();
        txe->Begin();
        txe->Upsert(tabname, key_c, rec_c);
        TxResult<bool> *res = txe->Commit();
        while (res->Status() == TxResultStatus::Unknown)
        {
            AdvanceCc(cc_shards);
            txe->Forward();
        }

        assert(res->Value() == true);
        // std::cout << "Committed key: " << v << std::endl;
    }

    ckpter.Exit();

    while (!ckpter.IsTerminated())
    {
        AdvanceCc(cc_shards);
    }

    std::cout << "Int memory store size: " << int_store.Size()
              << ", min key: " << int_store.MinKey()
              << ", max key: " << int_store.MaxKey() << std::endl;

    cc_shards.PrintCcMap();
}

int main(int argc, char *argv[])
{
    // ReadWriteTest(2);
    // ScanTest(2);
    // TestTxService(2);
    // TxServiceThroughput(2, 4);
    // MultiTxServices(2);

    // SecondaryIndexTest(2);
    // SecondaryIndexTestTxService();

    // ReadOutsideTest();
    ReadOutsideTestTxService();

    // CkptTest(2);

    return 0;
}