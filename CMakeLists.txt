cmake_minimum_required(VERSION 3.9)
project(boltDB_in_cpp)

set(CMAKE_CXX_STANDARD 11)

# ---[ CTest
include(CTest)

# ---[ Dependencies
find_package(Threads REQUIRED)

include_directories(${CMAKE_SOURCE_DIR}/include)
include_directories(${CMAKE_SOURCE_DIR}/test/include)
include_directories(${CMAKE_SOURCE_DIR}/third_party)
include_directories(BEFORE src) # This is needed for gtest.
file(GLOB SRC_BOLT ${PROJECT_SOURCE_DIR}/src/*.cpp)
file(GLOB INC_BOLT ${PROJECT_SOURCE_DIR}/include/*.h)
set(SRC
        ${SRC_BOLT}
        ${INC_BOLT})
add_library(boltDB_in_cpp ${SRC})

add_subdirectory(test)