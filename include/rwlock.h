//
// Created by c6s on 18-4-26.
//

#ifndef BOLTDB_IN_CPP_RWLOCK_H
#define BOLTDB_IN_CPP_RWLOCK_H
#include <condition_variable>
#include <climits>
namespace boltDB_CPP {
class RWLock {
  bool write_entered;
  size_t reader_count;
  std::mutex mtx;
  std::condition_variable reader;
  std::condition_variable writer;
 public:
  RWLock() : write_entered(false), reader_count(0) {}
  RWLock(const RWLock &) = delete;
  RWLock &operator=(const RWLock &) = delete;

  void readLock() {
    std::unique_lock<std::mutex> lock(mtx);
    while (write_entered) {
      reader.wait(lock);
    }
    if (reader_count == INT_MAX) {
      throw std::runtime_error("possibly request too many read locks");
    }
    reader_count++;
  }
  void readUnlock() {
    std::lock_guard<std::mutex> guard(mtx);
    if (reader_count == 0) {
      throw std::runtime_error("try release read lock without any reader holding the lock");
    }
    reader_count--;
    if (write_entered && reader_count == 0) {
      writer.notify_one();
    }
  }
  void writeLock() {
    std::unique_lock<std::mutex> lock(mtx);
    while (write_entered) {
      reader.wait(lock);
    }
    write_entered = true;
    while (reader_count) {
      writer.wait(lock);
    }
  }
  void writeUnlock() {
    std::lock_guard<std::mutex> lock(mtx);
    write_entered = false;
    reader.notify_all();
  }
};
}
#endif //BOLTDB_IN_CPP_RWLOCK_H
