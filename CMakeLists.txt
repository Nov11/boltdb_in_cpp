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
        src/Util.cpp
        include/Util.h
        src/Bucket.cpp
        include/Bucket.h
        include/Types.h
        src/Txn.cpp
        include/Txn.h
        include/Database.h
        include/RWLock.h
        src/Database.cpp
        src/Cursor.cpp
        include/Cursor.h
        src/Node.cpp
        include/Node.h
        include/MemoryPool.h
        include/Bucketheader.h
        src/Meta.cpp
        include/Meta.h)
add_library(boltDB_in_cpp ${SRC})

add_subdirectory(test)