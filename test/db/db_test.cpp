//
// Created by c6s on 18-5-7.
//

#include <gmock/gtest/gtest.h>
#include <util.h>
#include <thread>
#include <fcntl.h>
#include <test_util.h>
#include <txn.h>
#include <node.h>
#include <sys/mman.h>
#include "db.h"
namespace boltDB_CPP {
static const int SLEEP_WAIT_TIME = 1;
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
//re-open a db
TEST(dbtest, dbtest_reopendb_Test) {
  auto name = newFileName();
  std::unique_ptr<DB> db1(new DB);
  db1->openDB(name, S_IRWXU);
  int ret = 0;
  db1->view([&ret](Txn *txn) {
    ret = txn->isFreelistCheckOK();
    return ret;
  });
  EXPECT_EQ(ret, 0);
  db1->closeDB();
  db1.reset();

  std::unique_ptr<DB> db2(new DB);
  db2->openDB(name, S_IRWXU);
  db2->view([&ret](Txn *txn) {
    ret = txn->isFreelistCheckOK();
    return ret;
  });
  EXPECT_EQ(ret, 0);
  db2->closeDB();
}

//open a truncated db
TEST(dbtest, dbtest_open_truncatefile_Test) {
  auto name = newFileName();
  std::unique_ptr<DB> db1(new DB);
  db1->openDB(name, S_IRWXU);
  db1->closeDB();
  int fd = open(name.c_str(), O_RDWR);
  EXPECT_NE(fd, -1);
  auto ret = ftruncate(fd, 0x1000);
  EXPECT_EQ(ret, 0);
  std::unique_ptr<DB> db2(new DB);
  EXPECT_EQ(db2->openDB(name, S_IRWXU), nullptr);
  db2->closeDB();
}

TEST_F(TmpFile, dbtest_start_rw_txn_Test) {
  auto txn = db->beginRWTx();
  EXPECT_NE(txn, nullptr);
  EXPECT_EQ(db->commitTxn(txn), 0);
  db->closeDB();
}

//any not closed txn should stop the db from closing
TEST(dbtest, dbtest_closedb_while_rw_txn_is_active_Test) {
  std::cout << std::endl;
  auto name = newFileName();
  std::unique_ptr<DB> db1(new DB);
  EXPECT_NE(db1->openDB(name, S_IRWXU), nullptr);
  //1.rw txn
  auto rw = db1->beginRWTx();
  EXPECT_NE(rw, nullptr);
  std::thread th([&]() {
    sleep(SLEEP_WAIT_TIME);
    std::cout << "committing txn" << std::endl;
    EXPECT_EQ(db1->commitTxn(rw), 0);
  });
  std::cout << "close in main thread" << std::endl;
  db1->closeDB();
  th.join();
}
TEST(dbtest, dbtest_closedb_while_ro_txn_is_active_Test) {
  std::cout << std::endl;
  auto name = newFileName();
  std::unique_ptr<DB> db1(new DB);
  EXPECT_NE(db1->openDB(name, S_IRWXU), nullptr);
  //1.rw txn
  auto ro = db1->beginTx();
  EXPECT_NE(ro, nullptr);
  std::thread th([&]() {
    sleep(SLEEP_WAIT_TIME);
    std::cout << "rollback ro txn" << std::endl;
    db1->rollbackTxn(ro);
  });
  std::cout << "close in main thread" << std::endl;
  db1->closeDB();
  th.join();
}
TEST_F(TmpFile, dbtest_udpate_and_read_Test) {
  std::string s = "penapplepen";
  Item bname(s.c_str(), s.size());
  auto updateFunc = [&bname](Txn *txn) -> int {
    auto b = txn->createBucket(bname);
    EXPECT_NE(b, nullptr);
    EXPECT_EQ(b->put(Item::make_item("foo"), Item::make_item("bar")), 0);
    EXPECT_EQ(b->put(Item::make_item("foo1"), Item::make_item("bar1")), 0);
    EXPECT_EQ(b->put(Item::make_item("foo2"), Item::make_item("bar2")), 0);
    EXPECT_EQ(b->remove(Item::make_item("foo2")), 0);
    return 0;
  };
  auto updateRet = db->update(updateFunc);
  EXPECT_EQ(updateRet, 0);
  auto viewFunc = [&bname](Txn *txn) -> int {
    auto b = txn->getBucket(bname);
    EXPECT_NE(b, nullptr);
    auto item1 = b->get(Item::make_item("foo2"));
    EXPECT_EQ(item1.length, 0);
    auto item2 = b->get(Item::make_item("foo"));
    EXPECT_EQ(item2, Item::make_item("bar"));
    return 0;
  };
  EXPECT_EQ(db->view(viewFunc), 0);
}

TEST(dbtest, cmp_test) {
  std::vector<Inode> inodeList;
  auto in1 = Inode{};
  in1.key = Item::make_item("foo");
  inodeList.push_back(in1);
  auto in2 = Inode{};
  in2.key = Item::make_item("foo1");
  inodeList.push_back(in2);

  auto in3 = Inode{};
  auto t = Item::make_item("foo2");
  in3.key = t;
  bool found = false;
  auto ret = binary_search(inodeList, t, cmp_wrapper<Inode>,
                           inodeList.size(), found);
  EXPECT_EQ(ret, 2);
}

TEST(dbtest, update_underlying_file) {
  auto name = newFileName();
  std::unique_ptr<DB> db1(new DB);
  EXPECT_NE(db1->openDB(name, S_IRWXU), nullptr);
  int fd = db1->getFd();
  lseek(fd, SEEK_SET, 0);
  auto ret = write(fd, "1234567890", 10);
  EXPECT_NE(ret, -1);
  db1->closeDB();
}

TEST(dbtest, mmap_update_underlying_file) {
  auto name = newFileName();
  auto fd = ::open(name.c_str(), O_CREAT | O_RDWR, S_IRWXU);
  EXPECT_NE(fd, -1);
  auto r1 = ::write(fd, "123456789", 10);
  EXPECT_EQ(r1, 10);
  auto mmapRet = ::mmap(nullptr, 10, PROT_READ, MAP_SHARED, fd, 0);
  EXPECT_NE(mmapRet, MAP_FAILED);
  for (size_t i = 0; i < 10; i++) {
    std::cout << ((char *) mmapRet)[i] << " ";
  }
  std::cout << std::endl;

//  lseek(fd, SEEK_SET, 0);
  auto ret = pwrite(fd, "ABCDEFG", 8, 0);
  EXPECT_NE(ret, -1);

  for (size_t i = 0; i < 10; i++) {
    std::cout << ((char *) mmapRet)[i] << " " << std::endl;
  }
  close(fd);
}

TEST(dbtest, update_closed) {
  auto name = newFileName();
  std::unique_ptr<DB> db1(new DB);
  auto updateFunc = [](Txn *txn) -> int {
    auto b = txn->createBucket(Item::make_item("anewbucket"));
    return b != nullptr;
  };
  auto ret = db1->update(updateFunc);
  EXPECT_EQ(ret, -1);
}

TEST(dbtest, commit_managed_txn) {
  auto name = newFileName();
  std::unique_ptr<DB> db1(new DB);
  auto updateFunc = [](Txn *txn) -> int {
    return txn->commit();
  };
  auto ret = db1->update(updateFunc);
  EXPECT_EQ(ret, -1);
}

TEST(dbtest, rollback_managed_txn) {
  auto name = newFileName();
  std::unique_ptr<DB> db1(new DB);
  auto updateFunc = [](Txn *txn) -> int {
    return txn->rollback();
  };
  auto ret = db1->update(updateFunc);
  EXPECT_EQ(ret, -1);
}

TEST(dbtest, view_commit_managed_txn) {
  auto name = newFileName();
  std::unique_ptr<DB> db1(new DB);
  auto viewFunc = [](Txn *txn) -> int {
    return txn->commit();
  };
  auto ret = db1->view(viewFunc);
  EXPECT_EQ(ret, -1);
}

TEST(dbtest, view_rollback_managed_txn) {
  auto name = newFileName();
  std::unique_ptr<DB> db1(new DB);
  auto viewFunc = [](Txn *txn) -> int {
    return txn->rollback();
  };
  auto ret = db1->view(viewFunc);
  EXPECT_EQ(ret, -1);
}

TEST_F(TmpFile, dbtest_consistency_test) {
  auto create = [](Txn *txn) {
    auto ret = txn->createBucket(Item::make_item("widgets"));
    return !(ret != nullptr);
  };
  EXPECT_EQ(db->update(create), 0);

  auto updateFunc = [](Txn *txn) {
    return txn->getBucket(Item::make_item("widgets"))->put(Item::make_item("foo"), Item::make_item("bar"));
  };
  for (int i = 0; i < 10; i++) {
    db->update(updateFunc);
  }

  auto updateRet = db->update([](Txn *txn) -> int {
    auto p0 = txn->getPage(0);
    EXPECT_NE(p0, nullptr);
    EXPECT_EQ(isSet(p0->flag, PageFlag::metaPageFlag), true);

    auto p1 = txn->getPage(1);
    EXPECT_NE(p1, nullptr);
    EXPECT_EQ(isSet(p1->flag, PageFlag::metaPageFlag), true);

    auto p2 = txn->getPage(2);
    EXPECT_NE(p2, nullptr);
    EXPECT_EQ(isSet(p2->flag, PageFlag::freelistPageFlag), true);

    auto p3 = txn->getPage(3);
    EXPECT_NE(p3, nullptr);
    EXPECT_EQ(isSet(p3->flag, PageFlag::freelistPageFlag), true);

    auto p4 = txn->getPage(4);
    EXPECT_NE(p4, nullptr);
    EXPECT_EQ(isSet(p4->flag, PageFlag::leafPageFlag), true);

    auto p5 = txn->getPage(5);
    EXPECT_NE(p5, nullptr);
    EXPECT_EQ(isSet(p5->flag, PageFlag::freelistPageFlag), true);

    auto p6 = txn->getPage(6);
    EXPECT_EQ(p6, nullptr);

    return 0;
  });

  EXPECT_EQ(updateRet, 0);
}
}