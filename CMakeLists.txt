cmake_minimum_required(VERSION 3.14)
project(Octane VERSION 1.0 LANGUAGES CXX)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wpedantic")

add_library(kv_core 
    src/core/store.cpp
)

target_include_directories(kv_core PUBLIC 
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
)

find_package(Threads REQUIRED)
target_link_libraries(kv_core PUBLIC Threads::Threads)

add_executable(kv_server src/server/main.cpp)

target_link_libraries(kv_server PRIVATE kv_core)

