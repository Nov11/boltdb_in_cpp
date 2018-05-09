//
// Created by c6s on 18-5-7.
//
#include <iostream>
#include <gmock/gtest/gtest.h>
#include <util.h>
#include <fcntl.h>
#include <thread>
#include <atomic>
namespace boltDB_CPP {
TEST(flockteset, flockWLockOn2RLocksGranted) {
  std::cout << std::endl;
/**
 * flock works on open file descriptions
 * see what happens if acquiring a write lock when there're multiple descriptions and two read locks are granted.
 *
 * write lock can not be granted until the read locks are released
 */
  auto fd1 = ::open("dbtest", O_CREAT | O_RDWR, 0666);
  EXPECT_NE(fd1, -1);
  auto fd2 = ::open("dbtest", O_CREAT | O_RDWR, 0666);
  EXPECT_NE(fd2, -1);
  auto fd3 = ::open("dbtest", O_CREAT | O_RDWR, 0666);
  EXPECT_NE(fd3, -1);

  std::atomic<bool> granted{false};
  //2 read locks
  EXPECT_EQ(file_Rlock(fd1), 0);
  EXPECT_EQ(file_Rlock(fd2), 0);
  std::cout << "grabbed 2 read locks" << std::endl;
  std::thread th([fd3, &granted]() {
    EXPECT_EQ(file_WlockBlocking(fd3), 0);
    std::cout << "grabbed write lock" << std::endl;
    granted.store(true);
  });
  //release 2 read locks
  sleep(1);
  file_Unlock(fd1);
  EXPECT_EQ(granted.load(), false);
  file_Unlock(fd2);
  std::cout << "2 read locks released" << std::endl;
  th.join();
  EXPECT_EQ(granted.load(), true);
  file_Unlock(fd3);
  close(fd1);
  close(fd2);
  close(fd3);
  std::cout << "test ends" << std::endl;
}

TEST(flockteset, flockWLockOn2RLocksGranted_closeButNotFunlock) {
  std::cout << std::endl;
/**
 * flock works on open file descriptions
 * see what happens if acquiring a write lock when there're multiple descriptions and two read locks are granted.
 *
 * write lock can not be granted until the read locks are released
 *
 * close will release the lock implicitly
 *
 * this is a proof that read only db does not need to funlock when closing it
 */
  auto fd1 = ::open("dbtest", O_CREAT | O_RDWR, 0666);
  EXPECT_NE(fd1, -1);
  auto fd2 = ::open("dbtest", O_CREAT | O_RDWR, 0666);
  EXPECT_NE(fd2, -1);
  auto fd3 = ::open("dbtest", O_CREAT | O_RDWR, 0666);
  EXPECT_NE(fd3, -1);

  //2 read locks
  EXPECT_EQ(file_Rlock(fd1), 0);
  EXPECT_EQ(file_Rlock(fd2), 0);
  std::cout << "grabbed 2 read locks" << std::endl;
  std::thread th([fd3]() {
    EXPECT_EQ(file_WlockBlocking(fd3), 0);
    std::cout << "grabbed write lock" << std::endl;
  });
  //release 2 read locks
  sleep(1);
  close(fd1);
  close(fd2);
  std::cout << "2 read locks closed" << std::endl;
  th.join();
  file_Unlock(fd3);
  close(fd3);
  std::cout << "test ends" << std::endl;
}

TEST(flockteset, flockLockTypeChange) {
  std::cout << std::endl;
/**
 * flock calls on an already locked file will do lock type conversion. share <-> exclusive
 */
  auto fd1 = ::open("dbtest", O_CREAT | O_RDWR, 0666);
  EXPECT_NE(fd1, -1);

  EXPECT_EQ(file_Rlock(fd1), 0);
  std::cout << "grabbed 1 read lock" << std::endl;
  EXPECT_EQ(file_WlockBlocking(fd1), 0);
  std::cout << "grabbed 1 write lock" << std::endl;
  EXPECT_EQ(file_Rlock(fd1), 0);
  std::cout << "grabbed 1 read lock" << std::endl;
  file_Unlock(fd1);
  close(fd1);
  std::cout << "test ends" << std::endl;
}

//this will block
//TEST(dbteset, flockAcquireWLockOn2RLocksBeingGranted) {
//  std::cout << std::endl;
//  /**
//   * flock works on open file descriptions
//   * see what happens if acquiring a write lock when there're multiple descriptions of the same underlying file and two read locks are granted.
//   *
//   * write lock can not be granted
//   */
//  auto fd1 = ::open("dbtest", O_CREAT | O_RDWR, 0666);
//  EXPECT_NE(fd1, -1);
//  auto fd2 = ::open("dbtest", O_CREAT | O_RDWR, 0666);
//  EXPECT_NE(fd2, -1);
//  auto fd3 = ::open("dbtest", O_CREAT | O_RDWR, 0666);
//  EXPECT_NE(fd3, -1);
//
//  //2 read lock
//  EXPECT_EQ(file_Rlock(fd1), 0);
//  EXPECT_EQ(file_Rlock(fd2), 0);
//  std::cout << "grabbed 2 read locks" << std::endl;
//  EXPECT_EQ(file_WlockBlocking(fd3), 0);
//  std::cout << "grabbed write lock" << std::endl;
//  std::cout << "test ends" << std::endl;
//}

//this will block
TEST(flockteset, flockAcquireWLockOn1WLockBeingGranted) {
  std::cout << std::endl;
  /**
   * flock works on open file descriptions
   * see what happens if acquiring a write lock when there is a write lock granted on a different file description
   * of the same underlying file.
   *
   * write lock can not be granted
   */
  auto fd1 = ::open("dbtest", O_CREAT | O_RDWR, 0666);
  assert(fd1 != -1);
  auto fd2 = ::open("dbtest", O_CREAT | O_RDWR, 0666);
  assert(fd1 != -1);

  //2 write lock
  EXPECT_EQ(file_WlockBlocking(fd1), 0);
  std::cout << "grabbed 1 write lock" << std::endl;
  std::thread th([fd1](){
    sleep(1);
    close(fd1);
  });
  EXPECT_EQ(file_WlockBlocking(fd2), 0);
  std::cout << "grabbed 1 write lock" << std::endl;
  th.join();
  close(fd2);
  close(fd1);
  std::cout << "test ends" << std::endl;
}

TEST(flockteset, flockTwoWLocks) {
  std::cout << std::endl;
/**
 * open one fd
 * apply two wlock on it
 * all write locks can be granted
 */
  auto fd1 = ::open("dbtest", O_CREAT | O_RDWR, 0666);
  EXPECT_NE((fd1), -1);

  //2 write locks
  EXPECT_EQ(file_WlockBlocking(fd1), 0);
  std::cout << "grabbed 1 write lock" << std::endl;
  EXPECT_EQ(file_WlockBlocking(fd1), 0);
  std::cout << "grabbed 1 write lock" << std::endl;
  file_Unlock(fd1);
  close(fd1);
  std::cout << "test ends" << std::endl;
}
}
