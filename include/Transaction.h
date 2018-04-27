//
// Created by c6s on 18-4-26.
//

#ifndef BOLTDB_IN_CPP_TRANSACTION_H
#define BOLTDB_IN_CPP_TRANSACTION_H
#include <functional>
#include <map>
#include <vector>
#include "boltDB_types.h"

namespace boltDB_CPP {

class Database;
class MetaData;
class Bucket;
class Page;

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

class Transaction {
  bool writable = false;
  bool managed = false;
  Database *db = nullptr;
  MetaData *metaData = nullptr;
  Bucket *root = nullptr;
  std::map<page_id, Page *> pageTable;
  std::vector<std::function<void()>> commitHandlers;
  bool writeFlag = false;
  TxStat stats;
 public:
  bool isWritable() const {
    return writable;
  }

  void increaseCurserCount() {
    stats.cursorCount++;
  }

  Page* getPage(page_id pageId);
};

}

#endif //BOLTDB_IN_CPP_TRANSACTION_H
