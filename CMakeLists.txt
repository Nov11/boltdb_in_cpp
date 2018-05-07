cmake_minimum_required(VERSION 3.9)
project(boltDB_in_cpp)

set(CMAKE_CXX_STANDARD 11)

# ---[ CTest
include(CTest)

# ---[ Dependencies
find_package(Threads REQUIRED)

include_directories(${CMAKE_SOURCE_DIR}/include)
include_directories(${CMAKE_SOURCE_DIR}/third_party)
include_directories(BEFORE src) # This is needed for gtest.

set(SRC
        src/util.cpp
        include/util.h
        src/bucket.cpp
        include/bucket.h
        include/types.h
        src/txn.cpp
        include/txn.h
        include/db.h
        include/rwlock.h
        src/db.cpp
        src/cursor.cpp
        include/cursor.h
        src/node.cpp
        include/node.h
        include/memory_pool.h
        include/bucket_header.h
        src/meta.cpp
        include/meta.h)
add_library(boltDB_in_cpp ${SRC})

add_subdirectory(test)