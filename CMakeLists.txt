cmake_minimum_required(VERSION 3.8)
project (monograph)

find_package(Threads)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -Wno-parentheses -Wno-error -W -fPIC")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DFAULT_INJECTOR")

if ( CMAKE_COMPILER_IS_GNUCC )
    set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wno-reorder -Wno-unused-parameter -Wno-parentheses")
endif()
if ( MSVC )
    set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} /W3")
endif()

option(BRPC_WITH_GLOG "With glog" ON)
option(RUN_TX_SERVICE_TESTS "Run unit-tests" ON)
option(RANGE_PARTITION_ENABLED "Whether enable range partition" ON)
message(NOTICE "RANGE_PARTITION_ENABLED : ${RANGE_PARTITION_ENABLED}")
if (RANGE_PARTITION_ENABLED)
    add_definitions(-DRANGE_PARTITION_ENABLED)
endif()

option(TXRESULT_USE_BTHREAD_MUTEX "TxRequest use bthread::Mutex instead of std::mutex" OFF)
message(STATUS "TXRESULT_USE_BTHREAD_MUTEX : ${RANGE_PARTITION_ENABLED}")
if (TXRESULT_USE_BTHREAD_MUTEX)
    add_definitions(-DTXRESULT_USE_BTHREAD_MUTEX)
endif()

option(ON_KEY_OBJECT "Enable ON_KEY_OBJECT" OFF)
message(STATUS "ON_KEY_OBJECT : ${RANGE_PARTITION_ENABLED}")
if (ON_KEY_OBJECT)
    add_definitions(-DON_KEY_OBJECT)
endif()

find_package(Protobuf REQUIRED)
find_package(GFLAGS REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(MIMALLOC REQUIRED)

message(STATUS "OpenSSL include dir: ${OPENSSL_INCLUDE_DIR}")

message(STATUS "protobuf dir: ${PROTOBUF_LIBRARY}")
message(STATUS "protobuf include dir: ${Protobuf_INCLUDE_DIR}")
message(STATUS "gflags libs dir: ${GFLAGS_LIBRARY}")
message(STATUS "gflags include dir: ${GFLAGS_INCLUDE_DIR}")

if ( CMAKE_COMPILER_IS_GNUCC )
    find_path(BRPC_INCLUDE_PATH NAMES brpc/stream.h)
    find_library(BRPC_LIB NAMES brpc)
    if ((NOT BRPC_INCLUDE_PATH) OR (NOT BRPC_LIB))
        message(FATAL_ERROR "Fail to find brpc")
    endif()

    find_path(BRAFT_INCLUDE_PATH NAMES braft/raft.h)
    find_library(BRAFT_LIB NAMES braft)
    if ((NOT BRAFT_INCLUDE_PATH) OR (NOT BRAFT_LIB))
        message (FATAL_ERROR "Fail to find braft")
    endif()

    if(BRPC_WITH_GLOG)
        find_path(GLOG_INCLUDE_PATH NAMES glog/logging.h)
        find_library(GLOG_LIB NAMES glog VERSION ">=0.6.0" REQUIRED)
        if((NOT GLOG_INCLUDE_PATH) OR (NOT GLOG_LIB))
            message(FATAL_ERROR "Fail to find glog")
        endif()
        include_directories(${GLOG_INCLUDE_PATH})
        set(LINK_LIB ${LINK_LIB} ${GLOG_LIB})
        message(STATUS "glog lib dir: ${GLOG_LIB}")
    endif()

    find_path(LEVELDB_INCLUDE_PATH NAMES leveldb/db.h)
    find_library(LEVELDB_LIB NAMES leveldb)
    if ((NOT LEVELDB_INCLUDE_PATH) OR (NOT LEVELDB_LIB))
        message(FATAL_ERROR "Fail to find leveldb")
    endif()

    # find_path(OPENSSL_INCLUDE_PATH NAMES openssl/ssl.h)
    # find_library(OPENSSL_LIB NAMES ssl)
    # if ((NOT OPENSSL_INCLUDE_PATH) OR (NOT OPENSSL_LIB))
    #     message(FATAL_ERROR "Fail to find openssl")
    # endif()
endif()


# Compile all protobuf files under proto directory.
SET(PROTO_SRC ${CMAKE_SOURCE_DIR}/include/proto)
FILE(GLOB PROTO_FILES RELATIVE ${PROTO_SRC} ${PROTO_SRC}/*.proto)
FOREACH(PROTO_FILE ${PROTO_FILES})
	STRING(REGEX REPLACE "[^/]proto" "" PROTO_NAME ${PROTO_FILE})
	LIST(APPEND PROTO_CC_FILES ${PROTO_SRC}/${PROTO_NAME}.pb.cc)
	ADD_CUSTOM_COMMAND(
		OUTPUT "${PROTO_SRC}/${PROTO_NAME}.pb.cc" "${PROTO_SRC}/${PROTO_NAME}.pb.h"
		COMMAND protoc ${PROTO_FILE} --proto_path=./ --cpp_out=./
		WORKING_DIRECTORY ${PROTO_SRC}
	)
ENDFOREACH(PROTO_FILE)

add_subdirectory(log_service)

set (TX_SERVICE_SOURCE_DIR ${CMAKE_SOURCE_DIR})
set(INCLUDE_DIR
   ${TX_SERVICE_SOURCE_DIR}/include
   ${TX_SERVICE_SOURCE_DIR}/include/cc
   ${TX_SERVICE_SOURCE_DIR}/include/remote
   ${TX_SERVICE_SOURCE_DIR}/include/fault
   ${TX_SERVICE_SOURCE_DIR}/include/store
   ${TX_SERVICE_SOURCE_DIR}/log_service/proto
   ${TX_SERVICE_SOURCE_DIR}/log_service/include
   ${TX_SERVICE_SOURCE_DIR}/log_service/mono_metrics/include
   ${Protobuf_INCLUDE_DIR})

if ( CMAKE_COMPILER_IS_GNUCC )
    set(INCLUDE_DIR ${INCLUDE_DIR}
        ${MIMALLOC_INCLUDE_PATH}
        ${BRPC_INCLUDE_PATH}
        ${BRAFT_INCLUDE_PATH}
        ${GLOG_INCLUDE_PATH}
        ${GFLAGS_INCLUDE_PATH})
endif()

set(LINK_LIB ${LINK_LIB} ${PROTOBUF_LIBRARY})

if ( CMAKE_COMPILER_IS_GNUCC )
   set(LINK_LIB ${LINK_LIB}
       ${MIMALLOC_LIB}
       ${GFLAGS_LIBRARY}
       ${LEVELDB_LIB}
       ${BRAFT_LIB}
       ${BRPC_LIB}
       ${OPENSSL_LIB}
    )
endif()

SET(TxService_SOURCES
    ${TX_SERVICE_SOURCE_DIR}/src/tx_execution.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/tx_operation.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/checkpointer.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/tx_trace.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/tx_start_ts_collector.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/sharder.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/catalog_key_record.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/range_record.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/range_bucket_key_record.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/cc/cc_entry.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/cc/cc_map.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/cc/cc_shard.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/cc/cc_handler_result.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/cc/local_cc_handler.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/cc/local_cc_shards.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/cc/non_blocking_lock.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/cc/cc_req_misc.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/cc/range_slice.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/remote/remote_cc_handler.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/remote/remote_cc_request.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/remote/cc_node_service.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/remote/cc_stream_receiver.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/remote/cc_stream_sender.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/fault/log_replay_service.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/fault/cc_node.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/fault/fault_inject.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/tx_worker_pool.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/dead_lock_check.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/tx_index_operation.cpp
    ${TX_SERVICE_SOURCE_DIR}/log_service/mono_metrics/src/metrics.cc
    )

set(INCLUDE_DIR ${INCLUDE_DIR} ${PROTO_SRC} ${OPENSSL_INCLUDE_DIR})
set(TxService_SOURCES ${TxService_SOURCES} ${PROTO_CC_FILES})

add_library(txservice STATIC ${TxService_SOURCES} )

target_include_directories(txservice PUBLIC ${INCLUDE_DIR})

target_link_libraries(txservice PRIVATE ${LINK_LIB} logservice)

#add_executable(hello ${CMAKE_SOURCE_DIR}/hello.cpp)
#target_link_libraries(hello PRIVATE txservice ${CMAKE_THREAD_LIBS_INIT})


message(NOTICE "with tests: ${RUN_TX_SERVICE_TESTS}")
if (RUN_TX_SERVICE_TESTS)
    find_package(Catch2 CONFIG REQUIRED)
    add_executable(CcEntry-Test ${TX_SERVICE_SOURCE_DIR}/tests/CcEntry-Test.cpp)
    target_include_directories(CcEntry-Test PUBLIC ${TX_SERVICE_SOURCE_DIR}/tests/include ${INCLUDE_DIR})
    target_link_libraries(CcEntry-Test PUBLIC txservice Catch2::Catch2)
    add_executable(CcPage-Test ${TX_SERVICE_SOURCE_DIR}/tests/CcPage-Test.cpp)
    target_include_directories(CcPage-Test PUBLIC ${TX_SERVICE_SOURCE_DIR}/tests/include ${INCLUDE_DIR})
    target_link_libraries(CcPage-Test PUBLIC txservice Catch2::Catch2)
    add_executable(StartTsCollector-Test ${TX_SERVICE_SOURCE_DIR}/tests/StartTsCollector-Test.cpp)
    target_include_directories(StartTsCollector-Test PUBLIC ${TX_SERVICE_SOURCE_DIR}/tests/include ${INCLUDE_DIR})
    target_link_libraries(StartTsCollector-Test PUBLIC txservice Catch2::Catch2)
    add_test(NAME CcEntry-Test COMMAND CcEntry-Test)
    add_test(NAME CcPage-Test COMMAND CcPage-Test)
    add_test(NAME StartTsCollector-Test COMMAND StartTsCollector-Test)
    enable_testing()
endif ()
