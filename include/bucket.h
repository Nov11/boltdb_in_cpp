//
// Created by c6s on 18-4-26.
//

#ifndef BOLTDB_IN_CPP_BUCKET_H
#define BOLTDB_IN_CPP_BUCKET_H

#include <unordered_map>
#include "Database.h"
#include "boltDB_types.h"
#include "Transaction.h"

namespace boltDB_CPP {
const uint32_t MAXKEYSIZE = 32768;
const uint32_t MAXVALUESIZE = (1L << 31) - 2;
struct bucketInFile {
  page_id root;
  uint64_t sequence;
};

class Node;
class Cursor;
class Bucket {
  bucketInFile bucketInFile1;
  Transaction *tx = nullptr;
  std::unordered_map<Item, Bucket *> buckets;//subbucket cache. used if txn is writable
  Page *page = nullptr;
  Node *rootNode = nullptr;
  std::unordered_map<page_id, Node *> nodes;//node cache. used if txn is writable
  double fillpercent = 0.5;
  Bucket *newBucket(Transaction *tx);
 public:
  Transaction *getTransaction() const {
    return tx;
  }
  page_id getRoot() const {
    return bucketInFile1.root;
  }

  bool isWritable() const {
    return tx->isWritable();
  }

  Cursor *createCursor();
  Bucket *getBucketByName(const Item &searchKey);
  Bucket *openBucket(const Item& value);
  Bucket *createBucket(const Item &key);

  void getPageNode(page_id pageId, Node *&node, Page *&page);
  Node *getNode(page_id pageId, Node *parent);
};
const uint32_t BUCKETHEADERSIZE = sizeof(boltDB_CPP::bucketInFile);
}
#endif //BOLTDB_IN_CPP_BUCKET_H
