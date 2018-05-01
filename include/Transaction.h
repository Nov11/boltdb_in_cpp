//
// Created by c6s on 18-4-26.
//

#ifndef BOLTDB_IN_CPP_TRANSACTION_H
#define BOLTDB_IN_CPP_TRANSACTION_H
#include <functional>
#include <algorithm>
#include <map>
#include <vector>
#include "boltDB_types.h"
#include "MemoryPool.h"

namespace boltDB_CPP {

class Database;
class MetaData;
class Bucket;
class Page;
class Node;

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

struct Transaction {
  bool writable = false;
  bool managed = false;
  Database *db = nullptr;
  MetaData *metaData = nullptr;
  std::shared_ptr<Bucket> root;
  std::map<page_id, Page *> pageTable;
  std::vector<std::function<void()>> commitHandlers;
  bool writeFlag = false;
  TxStat stats;
 public:
  MemoryPool pool;

  bool isWritable() const {
    return writable;
  }

  void increaseCurserCount() {
    stats.cursorCount++;
  }
  void increaseNodeCount() {
    stats.nodeCount++;
  }

  Page *getPage(page_id pageId);

  Page *allocate(size_t count);

  void for_each_page(page_id pageId, int depth, std::function<void(Page *, int)>);

  void init(Database *db);
  std::shared_ptr<Bucket> getBucket(const Item &name);
  std::shared_ptr<Bucket> createBucket(const Item &name);
  std::shared_ptr<Bucket> createBucketIfNotExists(const Item &name);
  int deleteBucket(const Item &name);
  int for_each(std::function<int(const Item &name, Bucket *b)>);
  void OnCommit(std::function<void()> fn);
  int commit();
  void rollback();
  void closeTxn();
  int writeMeta();
  int write();
};

}

#endif //BOLTDB_IN_CPP_TRANSACTION_H
