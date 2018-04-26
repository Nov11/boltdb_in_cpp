cmake_minimum_required(VERSION 3.10)
project(boltDB_in_cpp)

set(CMAKE_CXX_STANDARD 11)

include_directories(${CMAKE_SOURCE_DIR}/include)

add_executable(boltDB_in_cpp src/utility.cpp include/utility.h src/bucket.cpp include/bucket.h include/boltDB_types.h src/Transaction.cpp include/Transaction.h include/Database.h include/rwlock.h)