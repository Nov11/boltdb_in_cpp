//
// Created by c6s on 18-4-26.
//

#ifndef BOLTDB_IN_CPP_DATABASE_H
#define BOLTDB_IN_CPP_DATABASE_H

#include <mutex>
#include "bucket.h"
#include "rwlock.h"

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

class TransactionStats;
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
  TransactionStats *txStat;
};

class Database;

class Batch {
  Database *database;
  //timer
  //call once
  //a function list
};

class Transaction;
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
  void* dataref;//readonly
  char(*data)[MAXMAPSIZE];//data is a pointer to block of memory if sizeof MAXMAPSIZE
  uint64_t fileSize;
  MetaData *meta0;
  MetaData *meta1;
  uint64_t pageSize;
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
  void setDataRef(void* dataref_p){
    dataref = dataref_p;
  }
};

class Page {

};

struct TxStat {
  uint64_t pageCount = 0;
  uint64_t pageAlloc = 0;//in bytes

  uint64_t cursorCount = 0;

  uint64_t nodeCount = 0;
  uint64_t nodeDereferenceCount = 0;

  uint64_t rebalanceCount = 0;
  uint64_t rebalanceTime = 0;

  uint64_t splitCount = 0;
  uint64_t spillCount = 0;
  uint64_t spillTime = 0;

  uint64_t writeCount = 0;
  uint64_t writeTime = 0;
};

class Inode {
  uint32_t flag = 0;
  page_id pageId = 0;
  std::string key;
  std::string value;
};

struct InodeList {
  std::vector<Inode *> list;
};

class Node;

struct NodeList {
  std::vector<Node *> list;
};

struct Node {
  Bucket *bucket = nullptr;
  bool isLeaf = false;
  bool unbalanced = false;
  bool spilled = false;
  std::string key;
  page_id pageId;
  Node *parentNode;
  NodeList children;
  InodeList inodeList;
};

}
#endif //BOLTDB_IN_CPP_DATABASE_H
