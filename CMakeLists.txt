cmake_minimum_required(VERSION 3.8)
project (monograph)

find_package(Threads)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

if ( CMAKE_COMPILER_IS_GNUCC )
    set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wno-reorder -Wno-unused-parameter -Wno-parentheses")
endif()
if ( MSVC )
    set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} /W3")
endif()

find_package(Protobuf REQUIRED)
find_package(GFLAGS REQUIRED)
#find_package(OpenSSL REQUIRED)

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

    find_path(OPENSSL_INCLUDE_PATH NAMES openssl/ssl.h)
    find_library(OPENSSL_LIB NAMES ssl)
    if ((NOT OPENSSL_INCLUDE_PATH) OR (NOT OPENSSL_LIB))
        message(FATAL_ERROR "Fail to find openssl")
    endif()
endif()

#if ( MSVC )
#    find_package(protobuf CONFIG REQUIRED)
#    set (PROTOBUF_LIBRARIES "protobuf::libprotobuf")
#endif()

#find_library(PROTOBUF_LIBRARY NAMES libprotobuf)
#if ((NOT PROTOBUF_LIBRARY))
#    message(FATAL_ERROR "Fail to find protobuf")
#endif()

message(STATUS "protobuf dir: ${PROTOBUF_LIBRARY}")
message(STATUS "protobuf dir: ${Protobuf_INCLUDE_DIR}")
message(STATUS "protobuf dir: ${OPENSSL_LIB}")
message(STATUS "protobuf dir: ${OPENSSL_INCLUDE_PATH}")

set(INCLUDE_DIR 
   ${CMAKE_SOURCE_DIR}/include
   ${CMAKE_SOURCE_DIR}/include/cc
   ${CMAKE_SOURCE_DIR}/include/remote
   ${CMAKE_SOURCE_DIR}/log_service/proto
   ${Protobuf_INCLUDE_DIR})

if ( CMAKE_COMPILER_IS_GNUCC )
   set(INCLUDE_DIR ${INCLUDE_DIR} ${OPENSSL_INCLUDE_PATH} ${BRPC_INCLUDE_PATH} ${BRAFT_INCLUDE_PATH})
endif()

set(LINK_LIB ${PROTOBUF_LIBRARY})

if ( CMAKE_COMPILER_IS_GNUCC )
   set(LINK_LIB ${LINK_LIB} ${OPENSSL_LIB} ${BRPC_LIB} ${BRAFT_LIB} ${GFLAGS_LIBRARY})
endif()

add_library(txservice STATIC 
    ${CMAKE_SOURCE_DIR}/src/tx_execution.cpp
    ${CMAKE_SOURCE_DIR}/src/tx_operation.cpp
    ${CMAKE_SOURCE_DIR}/src/cc/cc_shard.cpp
    ${CMAKE_SOURCE_DIR}/src/cc/local_cc_handler.cpp 
    ${CMAKE_SOURCE_DIR}/include/proto/cc_request.pb.cc
    ${CMAKE_SOURCE_DIR}/src/remote/remote_cc_handler.cpp
    ${CMAKE_SOURCE_DIR}/src/remote/remote_cc_request.cpp
    ${CMAKE_SOURCE_DIR}/src/remote/remote_cc_handler_win.cpp
    ${CMAKE_SOURCE_DIR}/log_service/proto/raft_log.pb.cc)

target_include_directories(txservice PUBLIC ${INCLUDE_DIR})

target_link_libraries(txservice PRIVATE ${LINK_LIB})

add_executable(hello ${CMAKE_SOURCE_DIR}/hello.cpp)
target_link_libraries(hello PRIVATE txservice ${CMAKE_THREAD_LIBS_INIT})
