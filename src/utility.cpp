//
// Created by c6s on 18-4-26.
//

#include <sys/mman.h>
#include <sys/file.h>
#include <cstdio>
#include <zconf.h>
#include <cerrno>
#include "utility.h"
#include "Database.h"

namespace boltDB_CPP {

static int file_lock_nonblocking(int fd, int operation) {
  int flockRet = flock(fd, operation | LOCK_NB);
  if (flockRet == -1 && errno != EWOULDBLOCK) {
    perror("flock file");
  }
  return flockRet;
}

int file_Wlock(int fd) {
  return file_lock_nonblocking(fd, LOCK_EX);
}

int file_Rlock(int fd) {
  return file_lock_nonblocking(fd, LOCK_SH);
}

int file_Unlock(int fd) {
  return file_lock_nonblocking(fd, LOCK_UN);
}

int mmap_db_file(Database *database, size_t sz) {
  void *ret = ::mmap(nullptr, sz, PROT_READ, MAP_SHARED, database->FD(), 0);
  if (ret == MAP_FAILED) {
    perror("mmap");
    return -1;
  }

  int retAdvise = ::madvise(ret, sz, MADV_RANDOM);
  if (retAdvise == -1) {
    perror("madvise");
    return -1;
  }

  database->setData(ret);
  database->setDataRef(ret);
  database->setDataSize(sz);
}

//un-map database file from memory
int munmap_db_file(Database *database) {
  //return if it has no mapping data
  if (database->getDataRef() == nullptr) {
    return 0;
  }
  int ret = ::munmap(database->getDataRef(), database->getDataSize());
  if (ret == -1) {
    perror("munmap");
    return ret;
  }

  database->resetData();
  return 0;
}

int file_data_sync(int fd) {
  int ret = fdatasync(fd);
  if (ret != 0) {
    perror("fdatasync");
  }
  return ret;
}
}

