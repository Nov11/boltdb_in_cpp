//
// Created by c6s on 18-4-26.
//

#ifndef BOLTDB_IN_CPP_BUCKET_H
#define BOLTDB_IN_CPP_BUCKET_H

#include <memory>
#include <unordered_map>
#include "Database.h"
#include "boltDB_types.h"
#include "Transaction.h"

namespace boltDB_CPP {
const uint32_t MAXKEYSIZE = 32768;
const uint32_t MAXVALUESIZE = (1L << 31) - 2;
struct bucketInFile {
  page_id root = 0;
  uint64_t sequence = 0;
};

class Node;
class Cursor;
class Bucket {
  Transaction *tx = nullptr;
  Page *page = nullptr;
  std::shared_ptr<Node> rootNode = nullptr;
  std::unique_ptr<bucketInFile> bucketPointer;
  std::unordered_map<Item, std::shared_ptr<Bucket>> buckets;//subbucket cache. used if txn is writable
  std::unordered_map<page_id, std::shared_ptr<Node>> nodes;//node cache. used if txn is writable
  double fillpercent = 0.5;

  std::shared_ptr<Bucket> newBucket(Transaction *tx);
 public:
  Transaction *getTransaction() const {
    return tx;
  }
  page_id getRoot() const {
    assert(bucketPointer);
    return bucketPointer->root;
  }

  bool isWritable() const {
    return tx->isWritable();
  }

  Cursor *createCursor();
  std::shared_ptr<Bucket> getBucketByName(const Item &searchKey);
  std::shared_ptr<Bucket> openBucket(const Item &value);
  std::shared_ptr<Bucket> createBucket(const Item &key);

  void getPageNode(page_id pageId, std::shared_ptr<Node> &node, Page *&page);
  std::shared_ptr<Node> getNode(page_id pageId, std::shared_ptr<Node> parent);
};
const uint32_t BUCKETHEADERSIZE = sizeof(boltDB_CPP::bucketInFile);
}
#endif //BOLTDB_IN_CPP_BUCKET_H
