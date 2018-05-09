//
// Created by c6s on 18-5-5.
//
#include <memory>
#include <chrono>
#include "db.h"
#include "gtest/gtest.h"

std::string newFileName() {
  auto ret = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now().time_since_epoch());
  int64_t currentTime = ret.count();
  return std::to_string(currentTime);
}

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
