//
// Created by c6s on 18-5-9.
//

#ifndef BOLTDB_IN_CPP_TEST_UTIL_H
#define BOLTDB_IN_CPP_TEST_UTIL_H
#include "gtest/gtest.h"
#include "db.h"
namespace boltDB_CPP {
std::string newFileName() {
  auto ret = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now().time_since_epoch());
  int64_t currentTime = ret.count();
  return std::to_string(currentTime);
}

struct TmpFile : testing::Test {
  std::unique_ptr<DB> db;
  void SetUp() override {
    //create a tmp db file
    db.reset(new DB);
    auto ret = db->openDB(newFileName(), 0666);
    assert(ret);
  }
  void TearDown() override {
    //close the tmp db file
    db->closeDB();
  }

};
}
#endif //BOLTDB_IN_CPP_TEST_UTIL_H
