//
// Created by c6s on 18-5-5.
//
#include <memory>
#include <chrono>
#include <test_util.h>
#include "db.h"
#include "gtest/gtest.h"

namespace boltDB_CPP {
class TmpFile : testing::Test {
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
TEST(TypeTests, helloworldtest) {
  std::cout << "hello world" << std::endl;

}
}
