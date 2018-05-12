cmake_minimum_required(VERSION 3.9)
project(boltDB_in_cpp)

set(CMAKE_CXX_STANDARD 11)

# ---[ CTest
include(CTest)

# ---[ Dependencies
find_package(Threads REQUIRED)

# ---[ Using cmake scripts and modules
list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake/Modules)

include(Coveralls)
include(Utils)
coveralls_turn_on_coverage()

# Create the coveralls target.
file(GLOB_RECURSE srcs ${PROJECT_SOURCE_DIR}/src/*.cpp)

peloton_convert_absolute_paths(srcs)
peloton_convert_absolute_paths(parser_srcs)

#message(STATUS "Coverage srcs : ${srcs}" )
set(COVERAGE_SRCS ${srcs})

coveralls_setup("${COVERAGE_SRCS}" ON)


include_directories(${CMAKE_SOURCE_DIR}/include)
include_directories(${CMAKE_SOURCE_DIR}/test/include)
include_directories(${CMAKE_SOURCE_DIR}/third_party)
include_directories(BEFORE src) # This is needed for gtest.
file(GLOB SRC_BOLT ${PROJECT_SOURCE_DIR}/src/*.cpp)
file(GLOB INC_BOLT ${PROJECT_SOURCE_DIR}/include/*.h)
set(FNV ${CMAKE_SOURCE_DIR}/src/fnv/hash_64a.c ${CMAKE_SOURCE_DIR}/include/fnv/fnv.h)
set(SRC
        ${SRC_BOLT}
        ${INC_BOLT}
        )
add_library(boltDB_in_cpp ${SRC} ${FNV})
add_subdirectory(test)

