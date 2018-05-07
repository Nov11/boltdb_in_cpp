//
// Created by c6s on 18-4-26.
//

#include "util.h"
#include <sys/file.h>
#include <sys/mman.h>
#include <zconf.h>
#include <cerrno>
#include <cstdio>
#include "db.h"

namespace boltDB_CPP {

static int file_lock_nonblocking(int fd, int operation) {
  int flockRet = flock(fd, operation);
  if (flockRet == -1) {
    perror("flock file");
  }
  return flockRet;
}

int file_Wlock(int fd) { return file_lock_nonblocking(fd, LOCK_EX | LOCK_NB); }

int file_WlockBlocking(int fd) { return file_lock_nonblocking(fd, LOCK_EX); }

int file_Rlock(int fd) { return file_lock_nonblocking(fd, LOCK_SH | LOCK_NB); }

int file_Unlock(int fd) { return file_lock_nonblocking(fd, LOCK_UN | LOCK_NB); }

int mmap_db_file(DB *database, size_t sz) {
  void *ret = ::mmap(nullptr, sz, PROT_READ, MAP_SHARED, database->getFd(), 0);
  if (ret == MAP_FAILED) {
    perror("mmap");
    return -1;
  }

  int retAdvise = ::madvise(ret, sz, MADV_RANDOM);
  if (retAdvise == -1) {
    perror("madvise");
    return -1;
  }

//  database->data = reinterpret_cast<decltype(database->data)>(ret);
//  database->dataref = (ret);
//  database->dataSize = (sz);
  database->resetData(ret, ret, sz);
}

//// un-map database file from memory
//int munmap_db_file(DB *database) {
//
//}

int file_data_sync(int fd) {
  int ret = fdatasync(fd);
  if (ret != 0) {
    perror("fdatasync");
  }
  return ret;
}

void mergePageIds(std::vector<boltDB_CPP::page_id> &dest,
                  const std::vector<boltDB_CPP::page_id> &a,
                  const std::vector<boltDB_CPP::page_id> &b) {
  if (a.empty()) {
    dest = b;
    return;
  }
  if (b.empty()) {
    dest = a;
    return;
  }

  dest.clear();
  size_t ia = 0;
  size_t ib = 0;
  while (ia != a.size() || ib != b.size()) {
    if (ia == a.size()) {
      dest.push_back(b[ib++]);
    } else if (ib == b.size()) {
      dest.push_back(a[ia++]);
    } else {
      if (b[ib] < a[ia]) {
        dest.push_back(b[ib++]);
      } else {
        dest.push_back(a[ia++]);
      }
    }
  }
}

std::vector<boltDB_CPP::page_id> merge(
    const std::vector<boltDB_CPP::page_id> &a,
    const std::vector<boltDB_CPP::page_id> &b) {
  std::vector<boltDB_CPP::page_id> result;
  mergePageIds(result, a, b);
  return result;
}
}  // namespace boltDB_CPP
