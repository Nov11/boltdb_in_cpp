//
// Created by c6s on 18-5-7.
//

#include <gmock/gtest/gtest.h>
#include <util.h>
#include <thread>
#include <fcntl.h>
#include <test_util.h>
namespace boltDB_CPP {
//open an empty file as db
TEST(dbtest, opentest) {
  std::unique_ptr<DB> ptr(new DB);
  auto ret = ptr->openDB(newFileName(), S_IRWXU);
  EXPECT_EQ(ret, ptr.get());
  ptr->closeDB();
}
//open a file that is not a valid db file
TEST(dbtest, open_invalid_db_file) {
  auto name = newFileName();
  auto fd = ::open(name.c_str(), O_CREAT | O_RDWR, S_IRWXU);
  EXPECT_NE(fd, -1);
  auto wcnt = ::write(fd, "abc", 4);
  EXPECT_EQ(wcnt, 4);
  ::close(fd);
  std::unique_ptr<DB> ptr(new DB);
  auto ret = ptr->openDB(name, S_IRWXU);
  EXPECT_EQ(ret, nullptr);
  ptr->closeDB();
}
}