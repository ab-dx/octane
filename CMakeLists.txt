cmake_minimum_required(VERSION 3.14)
project(Octane VERSION 1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wpedantic")

find_package(Threads REQUIRED)
find_package(protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)

set(PROTO_DIR "${CMAKE_CURRENT_SOURCE_DIR}/proto")
set(PROTO_FILE "${PROTO_DIR}/raft.proto")

set(PROTO_SRCS "${CMAKE_CURRENT_BINARY_DIR}/raft.pb.cc")
set(PROTO_HDRS "${CMAKE_CURRENT_BINARY_DIR}/raft.pb.h")
set(GRPC_SRCS "${CMAKE_CURRENT_BINARY_DIR}/raft.grpc.pb.cc")
set(GRPC_HDRS "${CMAKE_CURRENT_BINARY_DIR}/raft.grpc.pb.h")

add_custom_command(
    OUTPUT ${PROTO_SRCS} ${PROTO_HDRS} ${GRPC_SRCS} ${GRPC_HDRS}
    COMMAND protobuf::protoc
    ARGS --grpc_out=${CMAKE_CURRENT_BINARY_DIR}
         --cpp_out=${CMAKE_CURRENT_BINARY_DIR}
         --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
         -I ${PROTO_DIR}
         ${PROTO_FILE}
    DEPENDS ${PROTO_FILE}
)

add_library(raft_proto ${PROTO_SRCS} ${GRPC_SRCS})
target_include_directories(raft_proto PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(raft_proto PUBLIC gRPC::grpc++ protobuf::libprotobuf)

add_library(kv_core 
    src/core/store.cpp
    src/core/wal.cpp
    src/core/raft_node.cpp
)
target_include_directories(kv_core PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)
target_link_libraries(kv_core PUBLIC Threads::Threads raft_proto)

add_executable(kv_server src/server/main.cpp)
target_link_libraries(kv_server PRIVATE kv_core)
