//
// Created by c6s on 18-5-7.
//

#include <gmock/gtest/gtest.h>
#include <util.h>
#include <thread>
#include <fcntl.h>
#include <test_util.h>
namespace boltDB_CPP {
TEST(dbtest, opentest) {
  std::unique_ptr<DB> ptr(new DB);
  auto ret = ptr->openDB(newFileName(), S_IRWXU);
  EXPECT_EQ(ret, ptr.get());
  ptr->closeDB();
}

}