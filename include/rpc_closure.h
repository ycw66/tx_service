#pragma once

#include <brpc/controller.h>

#include <condition_variable>

#include "cc/cc_handler_result.h"
#include "error_messages.h"
#include "local_cc_shards.h"
#include "proto/cc_request.pb.h"
#include "remote/remote_type.h"
#include "tx_operation_result.h"
#include "type.h"

namespace txservice
{
class AcquireTermClosure : public ::google::protobuf::Closure
{
public:
    AcquireTermClosure(NodeGroupId ng_id,
                       std::mutex &mux,
                       std::condition_variable &cv,
                       uint32_t &finished_cnt,
                       CcErrorCode &request_res,
                       std::vector<int64_t> &leader_terms)
        : node_group_id_(ng_id),
          mux_(mux),
          cv_(cv),
          finished_cnt_(finished_cnt),
          request_res_(request_res),
          leader_terms_(leader_terms)
    {
    }
    ~AcquireTermClosure() = default;

    AcquireTermClosure(const AcquireTermClosure &rhs) = delete;
    AcquireTermClosure(AcquireTermClosure &&rhs) = delete;

    // Run() will be called when rpc request is processed by cc node service.
    void Run() override
    {
        // Free closure on exit
        std::unique_ptr<AcquireTermClosure> self_guard(this);
        CcErrorCode res_code = CcErrorCode::NO_ERROR;
        if (cntl_.Failed())
        {
            // RPC failed.
            LOG(ERROR) << "Failed for AcquireLeaderTerm RPC request of ng#"
                       << node_group_id_
                       << " with Error code: " << cntl_.ErrorCode()
                       << ". Error Msg: " << cntl_.ErrorText();
            res_code = CcErrorCode::REQUEST_LOST;
            Sharder::Instance().UpdateCcNodeServiceChannel(node_id_, channel_);
        }
        else
        {
            uint32_t ng_id = response_.node_group_id();
            int64_t term = response_.node_group_term();
            if (term < 0)
            {
                LOG(ERROR)
                    << "Handle acquire node group leader term response of ng#"
                    << ng_id << ", request node not leader.";
                res_code = CcErrorCode::REQUESTED_NODE_NOT_LEADER;
            }
            else
            {
                DLOG(INFO)
                    << "Handle acquire node group leader term response of ng#"
                    << ng_id << " with term: " << term;
                leader_terms_.at(ng_id) = term;
            }
        }
        channel_ = nullptr;

        std::unique_lock<std::mutex> lk(mux_);
        request_res_ =
            request_res_ == CcErrorCode::NO_ERROR ? res_code : request_res_;
        ++finished_cnt_;
        cv_.notify_one();
    }

    brpc::Controller *Controller()
    {
        return &cntl_;
    }

    remote::AcquireNodeGroupTermResponse *AcquireTermResponse()
    {
        return &response_;
    }

    remote::AcquireNodeGroupTermRequest *AcquireTermRequest()
    {
        return &request_;
    }

    void SetChannel(uint32_t node_id, std::shared_ptr<brpc::Channel> channel)
    {
        node_id_ = node_id;
        channel_ = channel;
    }

private:
    brpc::Controller cntl_;
    remote::AcquireNodeGroupTermRequest request_;
    remote::AcquireNodeGroupTermResponse response_;
    std::shared_ptr<brpc::Channel> channel_;
    uint32_t node_id_;

    NodeGroupId node_group_id_{0};
    std::mutex &mux_;
    std::condition_variable &cv_;
    uint32_t &finished_cnt_;
    CcErrorCode &request_res_;
    std::vector<int64_t> &leader_terms_;
};

class FlushDataAllClosure : public ::google::protobuf::Closure
{
public:
    explicit FlushDataAllClosure(CcHandlerResult<Void> *hd_res)
        : hd_result_(hd_res)
    {
    }
    ~FlushDataAllClosure() = default;

    FlushDataAllClosure(const FlushDataAllClosure &rhs) = delete;
    FlushDataAllClosure(FlushDataAllClosure &&rhs) = delete;

    // Run() will be called when rpc request is processed by cc node service.
    void Run() override
    {
        // Free closure on exit
        std::unique_ptr<FlushDataAllClosure> self_guard(this);

        if (cntl_.Failed())
        {
            // RPC failed.
            LOG(ERROR)
                << "Failed to process the FlushDataAll RPC request of ng#"
                << request_.node_group_id()
                << " for table: " << request_.table_name_str()
                << " with Error code: " << cntl_.ErrorCode()
                << ". Error Msg: " << cntl_.ErrorText();
            Sharder::Instance().UpdateCcNodeServiceChannel(node_id_, channel_);
            channel_ = nullptr;
            hd_result_->SetError(CcErrorCode::REQUEST_LOST);
            return;
        }
        channel_ = nullptr;

        if (response_.error_code())
        {
            CcErrorCode error_code =
                static_cast<CcErrorCode>(response_.error_code());
            LOG(ERROR) << "Handle flush data all response of ng#"
                       << request_.node_group_id()
                       << " for table: " << request_.table_name_str()
                       << ". Failed with error: " << CcErrorMessage(error_code);
            hd_result_->SetError(error_code);
        }
        else
        {
            DLOG(INFO) << "Handle flush data all response successfully of ng#"
                       << request_.node_group_id()
                       << " for table: " << request_.table_name_str();
            hd_result_->SetFinished();
        }
    }

    brpc::Controller *Controller()
    {
        return &cntl_;
    }

    remote::FlushDataAllRequest *FlushDataAllRequest()
    {
        return &request_;
    }

    remote::FlushDataAllResponse *FlushDataAllResponse()
    {
        return &response_;
    }

    void SetChannel(uint32_t node_id, std::shared_ptr<brpc::Channel> channel)
    {
        node_id_ = node_id;
        channel_ = channel;
    }

private:
    brpc::Controller cntl_;
    remote::FlushDataAllRequest request_;
    remote::FlushDataAllResponse response_;
    CcHandlerResult<Void> *hd_result_{nullptr};
    std::shared_ptr<brpc::Channel> channel_;
    uint32_t node_id_;
};

class UploadBatchClosure : public ::google::protobuf::Closure
{
public:
    UploadBatchClosure(bthread::Mutex &req_mux,
                       bthread::ConditionVariable &req_cv,
                       size_t &finished_req_cnt,
                       CcErrorCode &res_code,
                       int64_t &ng_term,
                       uint16_t upload_timeout)
        : req_mux_(req_mux),
          req_cv_(req_cv),
          finished_req_cnt_(finished_req_cnt),
          res_code_(res_code),
          ng_term_(ng_term),
          upload_timeout_(upload_timeout)
    {
    }
    ~UploadBatchClosure() = default;

    UploadBatchClosure(const UploadBatchClosure &rhs) = delete;
    UploadBatchClosure(UploadBatchClosure &&rhs) = delete;

    // Run() will be called when rpc request is processed by cc node service.
    void Run() override
    {
        // Free closure on exit
        std::unique_ptr<UploadBatchClosure> self_guard(this);
        if (cntl_.Failed())
        {
            // RPC failed.
            LOG(ERROR) << "Failed for UploadBatch RPC request of ng#"
                       << request_.node_group_id()
                       << ", with Error code: " << cntl_.ErrorCode()
                       << ". Error Msg: " << cntl_.ErrorText();
            if (cntl_.ErrorCode() == brpc::ERPCTIMEDOUT)
            {
                self_guard.release();
                // Retry if timeout.
                cntl_.Reset();
                response_.Clear();
                remote::CcRpcService_Stub stub(channel_.get());
                cntl_.set_timeout_ms(upload_timeout_);
                stub.UploadBatch(&cntl_, &request_, &response_, this);
                DLOG(INFO) << "Retry UploadBatch service of ng#"
                           << request_.node_group_id();
                return;
            }
            Sharder::Instance().UpdateCcNodeServiceChannel(node_id_, channel_);
            std::unique_lock<bthread::Mutex> req_lk(req_mux_);
            res_code_ = CcErrorCode::REQUEST_LOST;
            ++finished_req_cnt_;
            req_cv_.notify_one();
        }
        else
        {
            CcErrorCode err_code =
                remote::ToLocalType::ConvertCcErrorCode(response_.error_code());
            std::unique_lock<bthread::Mutex> req_lk(req_mux_);
            ++finished_req_cnt_;
            if (err_code != CcErrorCode::NO_ERROR)
            {
                LOG(ERROR) << "Response for upload batch failed of ng#"
                           << request_.node_group_id()
                           << ", with error: " << (uint32_t) err_code;
                res_code_ =
                    res_code_ == CcErrorCode::NO_ERROR ? err_code : res_code_;
            }
            else
            {
                DLOG(INFO) << "Response for upload batch succeed of ng#"
                           << request_.node_group_id();
                int64_t dest_term = response_.ng_term();
                if (ng_term_ == INIT_TERM)
                {
                    ng_term_ = dest_term;
                }
                else if (ng_term_ != dest_term)
                {
                    LOG(ERROR)
                        << "Response for upload batch failed of ng#"
                        << request_.node_group_id()
                        << " of term mismatch, with expected term: " << ng_term_
                        << " and actual term: " << dest_term;
                    res_code_ = CcErrorCode::REQUESTED_NODE_NOT_LEADER;
                }
                else
                {
                    assert(ng_term_ == dest_term);
                }
            }
            req_cv_.notify_one();
        }
        channel_ = nullptr;
    }

    brpc::Controller *Controller()
    {
        return &cntl_;
    }

    remote::UploadBatchResponse *UploadBatchResponse()
    {
        return &response_;
    }

    remote::UploadBatchRequest *UploadBatchRequest()
    {
        return &request_;
    }

    void SetChannel(uint32_t node_id, std::shared_ptr<brpc::Channel> channel)
    {
        node_id_ = node_id;
        channel_ = channel;
    }

private:
    brpc::Controller cntl_;
    remote::UploadBatchRequest request_;
    remote::UploadBatchResponse response_;
    bthread::Mutex &req_mux_;
    bthread::ConditionVariable &req_cv_;
    size_t &finished_req_cnt_;
    CcErrorCode &res_code_;
    int64_t &ng_term_;
    uint16_t upload_timeout_{0};
    std::shared_ptr<brpc::Channel> channel_;
    uint32_t node_id_;
};

class GenerateSkFromPkClosure : public ::google::protobuf::Closure
{
public:
    GenerateSkFromPkClosure(
        std::mutex &mux,
        std::condition_variable &cv,
        std::vector<int64_t> &leader_terms,
        uint32_t &unfinished_task_cnt,
        bool &all_task_started,
        uint32_t &total_pk_items_count,
        uint32_t &dispatched_task_count,
        CcErrorCode &task_res,
        std::function<void(const TxKey *batch_range_start_key,
                           const TxKey *batch_range_end_key,
                           const std::string *batch_range_start_key_str,
                           const std::string *batch_range_end_key_str,
                           const TxKey *&last_scanned_end_key,
                           bool &is_last_scanned_key_str,
                           size_t batch_range_cnt,
                           uint32_t &actual_task_cnt)> &dispatch_func)
        : mux_(mux),
          cv_(cv),
          leader_terms_(leader_terms),
          unfinished_task_cnt_(unfinished_task_cnt),
          all_task_started_(all_task_started),
          total_pk_items_count_(total_pk_items_count),
          dispatched_task_count_(dispatched_task_count),
          task_res_(task_res),
          dispatch_func_(dispatch_func)
    {
    }
    ~GenerateSkFromPkClosure() = default;

    GenerateSkFromPkClosure(const GenerateSkFromPkClosure &rhs) = delete;
    GenerateSkFromPkClosure(GenerateSkFromPkClosure &&rhs) = delete;

    // Run() will be called when rpc request is processed by cc node service.
    void Run() override
    {
        // Free closure on exit
        std::unique_ptr<GenerateSkFromPkClosure> self_guard(this);

        if (cntl_.Failed())
        {
            // RPC failed.
            LOG(ERROR) << "Failed for GenerateSkFromPk RPC request of ng#"
                       << request_.node_group_id()
                       << " for partition id: " << request_.partition_id()
                       << ". Error code: " << cntl_.ErrorCode()
                       << ". Error Msg: " << cntl_.ErrorText();
            Sharder::Instance().UpdateCcNodeServiceChannel(node_id_, channel_);
            channel_ = nullptr;
            std::unique_lock<std::mutex> lk(mux_);
            --unfinished_task_cnt_;
            task_res_ = task_res_ == CcErrorCode::NO_ERROR
                            ? CcErrorCode::REQUEST_LOST
                            : task_res_;
            cv_.notify_one();
            return;
        }
        channel_ = nullptr;

        CcErrorCode res_code =
            remote::ToLocalType::ConvertCcErrorCode(response_.error_code());
        total_pk_items_count_ = response_.pk_items_count();
        if (res_code == CcErrorCode::GET_RANGE_ID_ERR)
        {
            LOG(WARNING) << "Terminate this generate sk task of ng#"
                         << request_.node_group_id()
                         << " for partition id: " << request_.node_group_id()
                         << " for table: " << request_.table_name_str()
                         << " caused by the boundary of partition mismatch.";

            bthread::Mutex bthd_mux;
            bthread::ConditionVariable bthd_cv;
            bool is_finished = false;
            std::thread worker_thd = std::thread(
                [this, &bthd_mux, &bthd_cv, &is_finished]()
                {
                    // Update the task status
                    {
                        std::lock_guard<std::mutex> task_lk(mux_);
                        all_task_started_ = false;
                        --unfinished_task_cnt_;
                    }

                    const TxKey *range_start_key = nullptr;
                    const TxKey *range_end_key = nullptr;
                    const std::string *range_start_key_str =
                        &(request_.start_key());
                    const std::string *range_end_key_str =
                        &(request_.end_key());
                    bool is_last_scanned_key_str = true;

                    // Re-dispatch this range task.
                    if (request_.start_key().size() == 0)
                    {
                        range_start_key = Sharder::Instance()
                                              .GetLocalCcShards()
                                              ->GetCatalogFactory()
                                              ->NegativeInfKey();
                        is_last_scanned_key_str = false;
                        range_start_key_str = nullptr;
                    }
                    if (request_.end_key().size() == 0)
                    {
                        range_end_key = Sharder::Instance()
                                            .GetLocalCcShards()
                                            ->GetCatalogFactory()
                                            ->PositiveInfKey();
                        range_end_key_str = nullptr;
                    }
                    const TxKey *last_scanned_end_key = range_start_key;
                    bool dispatch_next_range = true;
                    uint32_t actual_task_cnt = 0;
                    do
                    {
                        dispatch_func_(range_start_key,
                                       range_end_key,
                                       range_start_key_str,
                                       range_end_key_str,
                                       last_scanned_end_key,
                                       is_last_scanned_key_str,
                                       0,
                                       actual_task_cnt);
                        {
                            std::lock_guard<std::mutex> task_lk(mux_);
                            if (task_res_ == CcErrorCode::TX_NODE_NOT_LEADER)
                            {
                                all_task_started_ = true;
                                cv_.notify_one();
                                return;
                            }
                        }
                        // dispatch next range.
                        if (request_.end_key().size() == 0)
                        {
                            dispatch_next_range =
                                *last_scanned_end_key < *range_end_key;
                        }
                        else
                        {
                            assert(last_scanned_end_key->Type() ==
                                   KeyType::Normal);
                            std::string serialized_key;
                            last_scanned_end_key->Serialize(serialized_key);
                            dispatch_next_range =
                                serialized_key.length() !=
                                    request_.end_key().length() ||
                                serialized_key.compare(request_.end_key());
                        }

                    } while (dispatch_next_range);

                    // Update the task status
                    {
                        std::lock_guard<std::mutex> task_lk(mux_);
                        dispatched_task_count_ += (actual_task_cnt - 1);
                        all_task_started_ = true;
                        cv_.notify_one();
                    }

                    std::unique_lock<bthread::Mutex> lk(bthd_mux);
                    is_finished = true;
                    bthd_cv.notify_all();
                });

            std::unique_lock<bthread::Mutex> lk(bthd_mux);
            while (!is_finished)
            {
                bthd_cv.wait(lk);
            }

            worker_thd.join();
            return;
        }

        if (res_code != CcErrorCode::NO_ERROR)
        {
            LOG(ERROR) << "Response for GenerateSkFromPk failed of ng#"
                       << request_.node_group_id()
                       << " for partition id: " << request_.partition_id()
                       << " with error: " << CcErrorMessage(res_code);
            std::unique_lock<std::mutex> lk(mux_);
            --unfinished_task_cnt_;
            task_res_ =
                task_res_ == CcErrorCode::NO_ERROR ? res_code : task_res_;
            cv_.notify_one();
            return;
        }

        DLOG(INFO) << "Response for GenerateSkFromPk succeed of ng#"
                   << request_.node_group_id()
                   << " for partition id: " << request_.partition_id();
        // Check the node group terms.
        int64_t term = -1;
        size_t curr_ng_terms_cnt = response_.ng_terms_size();

        std::unique_lock<std::mutex> lk(mux_);
        size_t ng_terms_cnt = leader_terms_.size();
        if (curr_ng_terms_cnt > ng_terms_cnt)
        {
            leader_terms_.resize(curr_ng_terms_cnt, INIT_TERM);
        }
        for (int idx = 0; idx < response_.ng_terms_size(); ++idx)
        {
            term = response_.ng_terms(idx);
            if (term < 0)
            {
                continue;
            }

            auto &leader_term = leader_terms_.at(idx);
            if (leader_term < 0)
            {
                leader_term = term;
            }
            else if (leader_term != term)
            {
                LOG(ERROR) << "Response for GenerateSkFromPk succeed of ng#"
                           << request_.node_group_id()
                           << ", but leader transferred of ng#" << idx;
                --unfinished_task_cnt_;
                task_res_ = task_res_ == CcErrorCode::NO_ERROR
                                ? CcErrorCode::REQUESTED_NODE_NOT_LEADER
                                : task_res_;
                cv_.notify_one();
                return;
            }
            else
            {
                assert(term == leader_term);
            }
        }

        // Finished
        --unfinished_task_cnt_;
        total_pk_items_count_ += response_.pk_items_count();
        cv_.notify_one();
    }

    brpc::Controller *Controller()
    {
        return &cntl_;
    }

    remote::GenerateSkFromPkRequest *GenerateSkFromPkRequest()
    {
        return &request_;
    }

    remote::GenerateSkFromPkResponse *GenerateSkFromPkResponse()
    {
        return &response_;
    }

    void SetChannel(uint32_t node_id, std::shared_ptr<brpc::Channel> channel)
    {
        node_id_ = node_id;
        channel_ = channel;
    }

private:
    brpc::Controller cntl_;
    remote::GenerateSkFromPkRequest request_;
    remote::GenerateSkFromPkResponse response_;
    std::shared_ptr<brpc::Channel> channel_;
    uint32_t node_id_;

    std::mutex &mux_;
    std::condition_variable &cv_;
    std::vector<int64_t> &leader_terms_;
    uint32_t &unfinished_task_cnt_;
    bool &all_task_started_;
    uint32_t &total_pk_items_count_;
    uint32_t &dispatched_task_count_;
    CcErrorCode &task_res_;
    std::function<void(const TxKey *batch_range_start_key,
                       const TxKey *batch_range_end_key,
                       const std::string *batch_range_start_key_str,
                       const std::string *batch_range_end_key_str,
                       const TxKey *&last_scanned_end_key,
                       bool &is_last_scanned_key_str,
                       size_t batch_range_cnt,
                       uint32_t &actual_task_cnt)> &dispatch_func_;
};

}  // namespace txservice