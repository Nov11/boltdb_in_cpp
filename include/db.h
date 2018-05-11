//
// Created by c6s on 18-4-26.
//

#ifndef BOLTDB_IN_CPP_DATABASE_H
#define BOLTDB_IN_CPP_DATABASE_H

#include <unistd.h>
#include <cassert>
#include <iostream>
#include <map>
#include <mutex>
#include <stack>
#include <string>
#include <vector>
#include <memory>
#include "bucket_header.h"
#include "rwlock.h"
#include "types.h"

namespace boltDB_CPP {

/**
 * constant definations
 */

// for ease of debug, use constants of x86 on x86_64 machine

const uint64_t MAXMAPSIZE = 0x7FFFFFFF;  // 2GB on x86
// const uint64_t MAXMAPSIZE = 0xFFFFFFFFFFFF; // 256TB on x86_64
const uint64_t MAXALLOCSIZE = 0xFFFFFFF;  // used on x86
// const uint64_t MAXALLOCSIZE = 0x7FFFFFFF; // on x86_64 used when creating
// array pointers x86/x86_64 will not break on unaligned load/store

const uint64_t MAXMMAPSTEP = 1 << 30;  // 1GB used when remapping the mmap
const uint64_t VERSION = 2;            // v1.3.1
const uint32_t MAGIC = 0xED0CDAED;

// default configuration values

const int DEFAULTMAXBATCHSIZE = 100;
const int DEFAULTMAXBATCHDELAYMILLIIONSEC = 10;
const int DEFAULTALLOCATIONSIZE = 16 * 1024 * 1024;
const int DEFAULTPAGESIZE = 4096;  // this value is returned by `getconf
// PAGE_SIZE` on ubuntu 17.10 x86_64

/**
 * forward declaration
 */
struct Page;
struct DB;
struct TxStat;
struct Txn;
struct Meta;

struct FreeList {
  std::vector<page_id> pageIds;                    // free & available
  std::map<txn_id, std::vector<page_id>> pending;  // soon to be free
  std::map<page_id, bool> cache;  // all free & pending page_ids

  void free(txn_id tid, Page *page);
  size_t size() const;  // size in bytes after serialization
  size_t count() const;
  size_t free_count() const;
  size_t pending_count() const;
  void copyall(std::vector<page_id> &dest);
  page_id allocate(size_t sz);
  void release(txn_id tid);
  void rollback(txn_id tid);
  bool freed(page_id pageId);
  void read(Page *page);
  int write(Page *page);
  void reindex();
  void reload(Page *page);
  void reset();
};

struct Stat {
  // about free list
  uint64_t freePageNumber = 0;
  uint64_t pendingPageNumber = 0;
  // in byte
  uint64_t freeAlloc = 0;
  // in byte
  uint64_t freeListInUse = 0;

  // txn stat
  uint64_t txnNumber = 0;
  uint64_t opened_txnNumber = 0;
  TxStat *txStat;
};

struct Options {
  uint32_t timeOut = 0;  // currently not supporting this parameter
  bool noGrowSync = false;
  bool readOnly = false;
  uint32_t mmapFlag = 0;
  size_t initalMmapSize = 0;
};
const Options DEFAULTOPTION{};

class Batch {
  DB *database;
  // timer
  // call once
  // a function list
};

class DB {
  static uint32_t pageSize;
  bool strictMode = false;
  bool noSync = false;
  bool noGrowSync = false;
  uint32_t mmapFlags = 0;
  uint32_t maxBatchSize = 0;
  uint32_t maxBatchDelayMillionSeconds = 0;
  uint32_t allocSize = 0;
  off_t fileSize = 0;
  std::string path;
  int fd = -1;
  void *dataref = nullptr;   // readonly . this is mmap data
  char *data = nullptr;  // data is a pointer to block of memory if sizeof
  // MAXMAPSIZE
  uint64_t dataSize = 0;
  Meta *meta0 = nullptr;
  Meta *meta1 = nullptr;
  bool opened = false;
  std::unique_ptr<Txn> rwtx;
  std::vector<std::unique_ptr<Txn>> txs;
  FreeList freeList;
  Stat stat;

  std::mutex batchMtx;
  Batch *batch = nullptr;

  std::mutex readWriteAccessMutex;  // this is writer's mutex. writers must
  // acquire this lock before proceeding.
  std::mutex metaLock;  // this protects the database object.
  RWLock mmapLock;
  RWLock statLock;

  bool readOnly = false;

  int munmap_db_file();
 public:
  const std::function<int(char *, size_t, off_t)> writeAt =
      [this](char *buf, size_t len, off_t offset) {
        auto ret = ::pwrite(fd, buf, len, offset);
        if (ret == -1) {
          perror("pwrite");
        }
        return ret;
      };
  bool isNoSync() const { return noSync; }
  void writerEnter() { readWriteAccessMutex.lock(); }
  void writerLeave() { readWriteAccessMutex.unlock(); }
  void resetRWTX();
  int getFd() const { return this->fd; }
  size_t freeListSerialSize() const { return freeList.size(); }
  void resetData();
  void resetData(void *data_p, void *dataref_p, size_t datasz_p);
  bool hasMappingData() const { return dataref != nullptr; }
  Page *pagePointer(page_id pageId);
  FreeList &getFreeLIst();
  static uint64_t getPageSize();
  Page *allocate(size_t count, Txn *txn);
  Meta *meta();
  void removeTxn(Txn *txn);
  int grow(size_t sz);
  int init();
  Page *pageInBuffer(char *ptr, size_t length, page_id pageId);
  DB *openDB(const std::string &path, uint16_t mode,
             const Options &options = DEFAULTOPTION);
  void closeDB();
  void do_closeDB();
  int initMeta(off_t minMmapSize);
  int mmapSize(off_t &targetSize);  // targetSize is a hint. calculate the mmap
  // size based on input param
  int update(std::function<int(Txn *tx)> fn);
  int view(std::function<int(Txn *tx)> fn);
  Txn *beginRWTx();
  Txn *beginTx();
  void closeTx(Txn* txn);
};

struct BranchPageElement {
  size_t pos = 0;
  size_t ksize = 0;
  page_id pageId = 0;

  Item Key() const {
    auto ptr = reinterpret_cast<const char *>(this);
    //    return std::string(&ptr[pos], &ptr[pos + ksize]);
    return {&ptr[pos], ksize};
  }
};

struct LeafPageElement {
  uint32_t flag = 0;  // is this element a bucket? yes:1 no:0
  size_t pos = 0;
  size_t ksize = 0;
  size_t vsize = 0;

  Item read(uint32_t p, uint32_t s) const {
    const auto *ptr = reinterpret_cast<const char *>(this);
    //    return std::string(&ptr[p], &ptr[p + s]);
    return {&ptr[p], s};
  }

  Item Key() const { return read(pos, ksize); }

  Item Value() const { return read(pos + ksize, vsize); }
};

struct Page {
  friend class ElementRef;
  page_id pageId = 0;
  uint16_t flag = 0;
  uint16_t count = 0;
  uint32_t overflow = 0;
  char *ptr = nullptr;

  LeafPageElement *getLeafPageElement(uint64_t index) const {
    assert(ptr);
    const auto *list = reinterpret_cast<const LeafPageElement *>(ptr);
    return const_cast<LeafPageElement *>(&list[index]);
  }

  BranchPageElement *getBranchPageElement(uint64_t index) const {
    assert(ptr);
    const auto *list = reinterpret_cast<const BranchPageElement *>(ptr);
    return const_cast<BranchPageElement *>(&list[index]);
  }

  uint16_t getFlag() const { return flag; }
  void setFlag(uint16_t flags) { Page::flag = flags; }
  uint16_t getCount() const { return count; }

  Meta *metaPointer() { return reinterpret_cast<Meta *>(&ptr); }
};

// transverse all kv pairs in a bucket in sorted order
// valid only if related txn is valid
enum class PageFlag : uint16_t {
  branchPageFlag = 0x01,
  leafPageFlag = 0x02,
  bucketLeafFlag = 0x02,
  metaPageFlag = 0x04,
  freelistPageFlag = 0x10,
};
static inline uint16_t pageFlagValue(PageFlag pf) {
  return static_cast<uint16_t >(pf);
}

static inline bool isSet(uint32_t flag, PageFlag flag1) {
  return static_cast<bool>(flag & static_cast<uint32_t>(flag1));
}

static inline bool isBucketLeaf(uint32_t flag) {
  return isSet(flag, PageFlag::bucketLeafFlag);
}

const size_t PAGEHEADERSIZE = offsetof(Page, ptr);
const size_t MINKEYSPERPAGE = 2;
const size_t BRANCHPAGEELEMENTSIZE = sizeof(BranchPageElement);
const size_t LEAFPAGEELEMENTSIZE = sizeof(LeafPageElement);

}  // namespace boltDB_CPP
#endif  // BOLTDB_IN_CPP_DATABASE_H
