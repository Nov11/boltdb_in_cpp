//
// Created by c6s on 18-4-26.
//

#ifndef BOLTDB_IN_CPP_TRANSACTION_H
#define BOLTDB_IN_CPP_TRANSACTION_H
#include <algorithm>
#include <functional>
#include <map>
#include <vector>
#include "bucket.h"
#include "memory_pool.h"
#include "types.h"
namespace boltDB_CPP {

class db;
class meta;
class bucket;
class Page;
class node;

struct TxStat {
  uint64_t pageCount = 0;
  uint64_t pageAlloc = 0;  // in bytes

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

struct txn {
  bool writable = false;
  bool managed = false;
  db *db = nullptr;
  meta *metaData = nullptr;
  bucket rootBucket;
  std::map<page_id, Page *> dirtyPageTable;  // this is dirty page table
  std::vector<std::function<void()>> commitHandlers;
  bool writeFlag = false;
  TxStat stats;

 public:
  memory_pool pool;

  bool isWritable() const { return writable; }

  void increaseCurserCount() { stats.cursorCount++; }
  void increaseNodeCount() { stats.nodeCount++; }

  Page *getPage(page_id pageId);

  Page *allocate(size_t count);

  void for_each_page(page_id pageId, int depth,
                     std::function<void(Page *, int)>);

  void init(db *db);
  bucket *getBucket(const Item &name);
  bucket *createBucket(const Item &name);
  bucket *createBucketIfNotExists(const Item &name);
  int deleteBucket(const Item &name);
  int for_each(std::function<int(const Item &name, bucket *b)>);
  void OnCommit(std::function<void()> fn);
  int commit();
  void rollback();
  void closeTxn();
  int writeMeta();
  int write();
  bool freelistcheck();
  bool checkBucket(bucket &bucket, std::map<page_id, Page *> &reachable,
                   std::map<page_id, bool> &freed);
};

}  // namespace boltDB_CPP

#endif  // BOLTDB_IN_CPP_TRANSACTION_H
