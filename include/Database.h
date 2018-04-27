//
// Created by c6s on 18-4-26.
//

#ifndef BOLTDB_IN_CPP_DATABASE_H
#define BOLTDB_IN_CPP_DATABASE_H

#include <mutex>
#include <vector>
#include <stack>
#include <cassert>
#include <iostream>
#include <string>
#include "bucket.h"
#include "rwlock.h"
#include "Transaction.h"

namespace boltDB_CPP {

/**
 * constant definations
 */

//for ease of debug, use constants of x86 on x86_64 machine

const uint64_t MAXMAPSIZE = 0x7FFFFFFF; // 2GB on x86
//const uint64_t MAXMAPSIZE = 0xFFFFFFFFFFFF; // 256TB on x86_64
const uint64_t MAXALLOCSIZE = 0xFFFFFFF; // used on x86
//const uint64_t MAXALLOCSIZE = 0x7FFFFFFF; // on x86_64 used when creating array pointers
//x86/x86_64 will not break on unaligned load/store

const uint64_t MAXMMAPSTEP = 1 << 30; //1GB used when remapping the mmap
const uint64_t VERSION = 2; //v1.3.1
const uint32_t MAGIC = 0xED0CDAED;

//default configuration values

const int DEFAULTMAXBATCHSIZE = 100;
const int DEFAULTMAXBATCHDELAYMILLIIONSEC = 10;
const int DEFAULTALLOCATIONSIZE = 16 * 1024 * 1024;
const int DEFAULTPAGESIZE = 4096;// this value is returned by `getconf PAGE_SIZE` on ubuntu 17.10 x86_64

struct MetaData {
  uint32_t magic;
  uint32_t version;
  uint32_t pageSize;
  uint32_t flags;
  Bucket *root;
  page_id freeList;
  page_id pageId;
  txn_id txnId;
  uint64_t checkSum;
};

struct FreeList {
  std::vector<page_id> pageIds;//free & available
  std::map<txn_id, std::vector<page_id>> pending;//soon to be free
  std::map<page_id, bool> cache;//all free & pending page_ids
};

struct Stat {
  //about free list
  uint64_t freePageNumber = 0;
  uint64_t pendingPageNumber = 0;
  //in byte
  uint64_t freeAlloc = 0;
  //in byte
  uint64_t freeListInUse = 0;

  //txn stat
  uint64_t txnNumber = 0;
  uint64_t opened_txnNumber = 0;
  TxStat *txStat;
};

class Database;

class Batch {
  Database *database;
  //timer
  //call once
  //a function list
};

class Database {
  bool strictMode = false;
  bool noSync = false;
  bool noGrowSync = false;
  uint32_t mmapFlags = 0;
  uint32_t maxBatchSize = 0;
  uint32_t maxBatchDelayMillionSeconds = 0;
  uint32_t allocSize = 0;

  std::string path;
  int fd = -1;
  void *dataref;//readonly
  char(*data)[MAXMAPSIZE];//data is a pointer to block of memory if sizeof MAXMAPSIZE
  uint64_t dataSize;
  MetaData *meta0;
  MetaData *meta1;
  uint64_t pageSize = 0;
  bool opened;
  Transaction *rwtx;
  std::vector<Transaction *> txs;
  FreeList *freeList;
  Stat stat;

  std::mutex batchMtx;
  Batch *batch;

  std::mutex rwlock;
  std::mutex metaLock;
  RWLock mmapLock;
  RWLock statLock;

  bool readOnly;

 public:
  int FD() const {
    return fd;
  }

  void *getDataRef() const {
    return dataref;
  }
  void setDataRef(void *dataref_p) {
    dataref = dataref_p;
  }

  void setData(void *data_p) {
    data = reinterpret_cast<char (*)[MAXMAPSIZE]> (data_p);
  }

  void *getData() const {
    return data;
  }

  void setDataSize(uint64_t sz_p) {
    dataSize = sz_p;
  }

  uint64_t getDataSize() const {
    return dataSize;
  }

  void resetData() {
    dataSize = 0;
    data = nullptr;
    dataref = nullptr;
  }

  Page *getPage(page_id pageId);
};

struct BranchPageElement {
  uint32_t pos = 0;
  uint32_t ksize = 0;
  page_id pageId = 0;

  std::string Key() const {
    auto ptr = reinterpret_cast<const char *>(this);
    return std::string(&ptr[pos], &ptr[pos + ksize]);
  }
};

struct LeafPageElement {
  uint32_t flag = 0;
  uint32_t pos = 0;
  uint32_t ksize = 0;
  uint32_t vsize = 0;

  std::string read(uint32_t p, uint32_t s) const {
    const char *ptr = reinterpret_cast<const char *>(this);
    return std::string(&ptr[p], &ptr[p + s]);
  }

  std::string Key() const {
    return read(pos, ksize);
  }

  std::string Value() const {
    return read(pos + ksize, vsize);
  }
};

struct Page {
  page_id pageId;
  uint16_t flag;
  uint16_t count;
  uint32_t overflow;
  uintptr_t ptr;

  LeafPageElement *getLeafPageElement(uint64_t index) const {
    assert(ptr);
    const auto *list = reinterpret_cast<const LeafPageElement *>(ptr);
    return const_cast<LeafPageElement *> (&list[index]);
  }

  BranchPageElement *getBranchPageElement(uint64_t index) const {
    assert(ptr);
    const auto *list = reinterpret_cast<const BranchPageElement *>(ptr);
    return const_cast<BranchPageElement *> (&list[index]);
  }

  page_id getPageId() const {
    return pageId;
  }
  void setPageId(page_id pageId) {
    Page::pageId = pageId;
  }
  uint16_t getFlag() const {
    return flag;
  }
  void setFlag(uint16_t flags) {
    Page::flag = flags;
  }
  uint16_t getCount() const {
    return count;
  }
  void setCount(uint16_t count) {
    Page::count = count;
  }
  uint32_t getOverflow() const {
    return overflow;
  }
  void setOverflow(uint32_t overflow) {
    Page::overflow = overflow;
  }
  uintptr_t getPtr() const {
    return ptr;
  }
  void setPtr(uintptr_t ptr) {
    Page::ptr = ptr;
  }
};

//this is a pointer to element. The element can be in a page or not added to a page yet.
//1.points to an element in a page
//2.points to an element not yet in a page
struct Inode {
  uint32_t flag = 0;
  page_id pageId = 0;
  std::string key;
  std::string value;
  std::string Key() const {
    return key;
  }
  std::string Value() const {
    return value;
  }
};

typedef std::vector<Inode *> InodeList;
//struct InodeList {
//  std::vector<Inode *> list;
//};

class Node;

typedef std::vector<Node *> NodeList;
//struct NodeList {
//  std::vector<Node *> list;
//};

//this is a in-memory deserialized page
struct Node {
  Bucket *bucket = nullptr;
  bool isLeaf = false;
  bool unbalanced = false;
  bool spilled = false;
  std::string key;
  page_id pageId = 0;
  Node *parentNode = nullptr;
  NodeList children;
  InodeList inodeList;
};

// transverse all kv pairs in a bucket in sorted order
//valid only if related txn is valid
enum class PageFlag : uint16_t {
  branchPageFlag = 0x01,
  leafPageFlag = 0x02,
  metaPageFlag = 0x04,
  freelistPageFlag = 0x10,
};

}
#endif //BOLTDB_IN_CPP_DATABASE_H
