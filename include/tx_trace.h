#pragma once

#include <cxxabi.h>

#include <cassert>
#include <cstddef>
#include <cstdio>
#include <memory>
#include <sstream>
#include <string>
#include <typeinfo>
#include <vector>

#include "butil/logging.h"
#include "tx_id.h"
#define TX_TRACE_DISABLED

#define FMT_POINTER_TO_UINT64T(pointer) \
    fmt_hex(reinterpret_cast<uint64_t>(pointer))

#define GET_TABLE_NAME(req) \
    (req->GetTableName() == nullptr ? "nullptr" : *req->GetTableName())

#define GET_MACRO3(_1, _2, _3, NAME, ...) NAME
#define GET_MACRO4(_1, _2, _3, _4, NAME, ...) NAME

#ifdef TX_TRACE_DISABLED
#define TX_TRACE_ACTION(...) \
    GET_MACRO3(__VA_ARGS__, TX_TRACE_ACTION3, TX_TRACE_ACTION2)(__VA_ARGS__)
#define TX_TRACE_ACTION2(subject_addr, object_addr)
#define TX_TRACE_ACTION3(subject_addr, action, object_addr)
#define TX_TRACE_ACTION_WITH_CONTEXT(...)     \
    GET_MACRO4(__VA_ARGS__,                   \
               TX_TRACE_ACTION_WITH_CONTEXT4, \
               TX_TRACE_ACTION_WITH_CONTEXT3) \
    (__VA_ARGS__)
#define TX_TRACE_ACTION_WITH_CONTEXT3(subject_addr, object_addr, context_func)
#define TX_TRACE_ACTION_WITH_CONTEXT4( \
    subject_addr, action, object_addr, context_func)
#define TX_TRACE_ASSOCIATE(...)                                       \
    GET_MACRO3(__VA_ARGS__, TX_TRACE_ASSOCIATE3, TX_TRACE_ASSOCIATE2) \
    (__VA_ARGS__)
#define TX_TRACE_ASSOCIATE2(obj1_addr, obj2_addr)
#define TX_TRACE_ASSOCIATE3(obj1_addr, obj2_addr, id)
#define TX_TRACE_ASSOCIATE_WITH_CONTEXT(obj1_addr, obj2_addr, context_func)
#define TX_TRACE_DUMP(obj)
#define TX_TRACE_DUMP_WITH_CONTEXT(obj, context_func)
#else
#define TX_TRACE_ACTION(...) \
    GET_MACRO3(__VA_ARGS__, TX_TRACE_ACTION3, TX_TRACE_ACTION2)(__VA_ARGS__)
#define TX_TRACE_ACTION2(subject_addr, object_addr) \
    DLOG(INFO) << "tx_trace:"                       \
               << txservice::tx_trace_action(       \
                      subject_addr, __FUNCTION__, object_addr, nullptr);
#define TX_TRACE_ACTION3(subject_addr, action, object_addr) \
    DLOG(INFO) << "tx_trace:"                               \
               << txservice::tx_trace_action(               \
                      subject_addr, action, object_addr, nullptr);
#define TX_TRACE_ACTION_WITH_CONTEXT(...)     \
    GET_MACRO4(__VA_ARGS__,                   \
               TX_TRACE_ACTION_WITH_CONTEXT4, \
               TX_TRACE_ACTION_WITH_CONTEXT3) \
    (__VA_ARGS__)
#define TX_TRACE_ACTION_WITH_CONTEXT3(subject_addr, object_addr, context_func) \
    DLOG(INFO) << "tx_trace:"                                                  \
               << txservice::tx_trace_action(                                  \
                      subject_addr, __FUNCTION__, object_addr, context_func);
#define TX_TRACE_ACTION_WITH_CONTEXT4(               \
    subject_addr, action, object_addr, context_func) \
    DLOG(INFO) << "tx_trace:"                        \
               << txservice::tx_trace_action(        \
                      subject_addr, action, object_addr, context_func);
#define TX_TRACE_ASSOCIATE(...)                                       \
    GET_MACRO3(__VA_ARGS__, TX_TRACE_ASSOCIATE3, TX_TRACE_ASSOCIATE2) \
    (__VA_ARGS__)
#define TX_TRACE_ASSOCIATE2(obj1_addr, obj2_addr) \
    DLOG(INFO) << "tx_trace:"                     \
               << txservice::tx_trace_associate(  \
                      obj1_addr, obj2_addr, "", nullptr);
#define TX_TRACE_ASSOCIATE3(obj1_addr, obj2_addr, id) \
    DLOG(INFO) << "tx_trace:"                         \
               << txservice::tx_trace_associate(      \
                      obj1_addr, obj2_addr, id, nullptr);
#define TX_TRACE_ASSOCIATE_WITH_CONTEXT(obj1_addr, obj2_addr, context_func) \
    DLOG(INFO) << "tx_trace:"                                               \
               << txservice::tx_trace_associate(                            \
                      obj1_addr, obj2_addr, "", context_func);
#define TX_TRACE_DUMP(obj_addr) \
    DLOG(INFO) << "tx_trace:" << tx_trace_dump(obj_addr, nullptr);
#define TX_TRACE_DUMP_WITH_CONTEXT(obj_addr, context_func) \
    DLOG(INFO) << "tx_trace:" << tx_trace_dump(obj_addr, context_func);
#endif

namespace txservice
{
struct Void;

template <typename... Args>
std::string fmt(const char *fmt, Args... args);

std::string fmt_hex(uint64_t i);

template <typename T>
std::string demangled_type_name(T t);

static const char *associate_fmt =
    "{\"associate\":{\"%s\":\"%x\",\"%s\":\"%x\",\"id\":\"%s\"}}";
static const char *associate_fmt_context =
    "{\"associate\":{\"%s\":\"%x\",\"%s\":\"%x\",\"id\":\"%s\", %s}}";
template <typename T, typename K>
std::string tx_trace_associate(T *t,
                               K *k,
                               std::string id,
                               std::function<std::string()> context_func);

static const char *action_fmt =
    "{\"action\":{\"%s\":\"%x\",\"action\":\"%s\",\"%s\":\"%x\"}}";
static const char *action_fmt_context =
    "{\"action\":{\"%s\":\"%x\",\"action\":\"%s\",\"%s\":\"%x\",%s}}";
template <typename T, typename K>
std::string tx_trace_action(T *t,
                            std::string action,
                            K k,
                            std::function<std::string()> context_func);
template <typename T, typename K>
std::string tx_trace_action(T *t,
                            std::string action,
                            K *k,
                            std::function<std::string()> context_func);

static const char *dump_fmt = "{\"dump\":{\"%s\":\"%x\",\"dump\":\"%s\"}}";
static const char *dump_fmt_tx_number =
    "{\"dump\":{\"%s\":\"%d\",\"dump\":\"%s\"}}";
static const char *dump_fmt_context =
    "{\"dump\":{\"%s\":\"%x\",\"dump\":\"%s\",%s}}";
static const char *dump_fmt_context_tx_number =
    "{\"dump\":{\"%s\":\"%d\",\"dump\":\"%s\",%s}}";
template <typename T>
std::string tx_trace_dump(T *t, std::function<std::string()> context_func);
std::string tx_trace_dump(txservice::Void *result,
                          std::function<std::string()> context_func);
std::string tx_trace_dump(txservice::TxNumber tx_number,
                          std::function<std::string()> context_func);
}  // namespace txservice
