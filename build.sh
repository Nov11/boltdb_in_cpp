#!/bin/sh
if [ ! -d "build" ] ; then
    mkdir build
fi

cd build
cmake .. -DCMAKE_BUILD_TYPE=Debug  -DCOVERALLS=ON
make -j4
make check
make coveralls