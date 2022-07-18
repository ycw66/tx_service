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

find_package(Protobuf REQUIRED)
find_package(GFLAGS REQUIRED)
#find_package(OpenSSL REQUIRED)

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
        find_library(GLOG_LIB NAMES glog)
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



set(PROTO_SRC ${CMAKE_SOURCE_DIR}/include/proto)
set(PROTO_NAME cc_request)
execute_process(
    COMMAND protoc ./${PROTO_NAME}.proto --cpp_out=./ --proto_path=./
    WORKING_DIRECTORY ${PROTO_SRC}
)

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
   ${Protobuf_INCLUDE_DIR})

if ( CMAKE_COMPILER_IS_GNUCC )
    set(INCLUDE_DIR ${INCLUDE_DIR}
        ${BRPC_INCLUDE_PATH}
        ${BRAFT_INCLUDE_PATH}
        ${GLOG_INCLUDE_PATH}
        ${GFLAGS_INCLUDE_PATH})
endif()

set(LINK_LIB ${LINK_LIB} ${PROTOBUF_LIBRARY})

if ( CMAKE_COMPILER_IS_GNUCC )
   set(LINK_LIB ${LINK_LIB}
       ${GFLAGS_LIBRARY}
       ${LEVELDB_LIB}
       ${BRAFT_LIB}
       ${BRPC_LIB}
    )
endif()

if ( CMAKE_COMPILER_IS_GNUCC )
   set(LINK_LIB ${LINK_LIB} ${OPENSSL_LIB} ${GFLAGS_LIBRARY} ${BRAFT_LIB} ${BRPC_LIB})
endif()

SET (TxService_SOURCES
    ${TX_SERVICE_SOURCE_DIR}/src/tx_execution.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/tx_operation.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/checkpointer.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/tx_trace.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/tx_start_ts_collector.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/archives_flusher.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/sharder.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/catalog_key_record.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/cc/cc_entry.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/cc/cc_map.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/cc/cc_shard.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/cc/cc_handler_result.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/cc/local_cc_handler.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/cc/local_cc_shards.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/cc/non_blocking_lock.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/cc/cc_req_misc.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/remote/remote_cc_handler.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/remote/remote_cc_request.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/remote/cc_node_service.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/remote/cc_stream_receiver.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/remote/cc_stream_sender.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/fault/log_replay_service.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/fault/cc_node.cpp
    ${TX_SERVICE_SOURCE_DIR}/src/fault/fault_inject.cpp
    )

set(INCLUDE_DIR ${INCLUDE_DIR} ${PROTO_SRC})
set(TxService_SOURCES ${TxService_SOURCES} ${PROTO_SRC}/${PROTO_NAME}.pb.cc)

add_library(txservice STATIC ${TxService_SOURCES} )

target_include_directories(txservice PUBLIC ${INCLUDE_DIR})

target_link_libraries(txservice PRIVATE ${LINK_LIB} logservice)

#add_executable(hello ${CMAKE_SOURCE_DIR}/hello.cpp)
#target_link_libraries(hello PRIVATE txservice ${CMAKE_THREAD_LIBS_INIT})


message(NOTICE "with tests: ${RUN_TX_SERVICE_TESTS}")
if (RUN_TX_SERVICE_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif ()