cmake_minimum_required(VERSION 3.10)
project(boltDB_in_cpp)

set(CMAKE_CXX_STANDARD 11)

include_directories(${CMAKE_SOURCE_DIR}/include)
set(SRC
        src/utility.cpp
        include/utility.h
        src/bucket.cpp
        include/bucket.h
        include/boltDB_types.h
        src/Transaction.cpp
        include/Transaction.h
        include/Database.h
        include/rwlock.h
        src/Database.cpp
        src/Cursor.cpp
        include/Cursor.h
        src/Node.cpp
        include/Node.h
        include/MemoryPool.h include/bucketheader.h)
add_library(boltDB_in_cpp ${SRC})