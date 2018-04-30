//
// Created by c6s on 18-4-26.
//

#ifndef BOLTDB_IN_CPP_BUCKET_H
#define BOLTDB_IN_CPP_BUCKET_H

#include <memory>
#include <unordered_map>
#include <utility>
#include "Database.h"
#include "boltDB_types.h"
#include "Transaction.h"

namespace boltDB_CPP {
const uint32_t MAXKEYSIZE = 32768;
const uint32_t MAXVALUESIZE = (1L << 31) - 2;
const double MINFILLPERCENT = 0.1;
const double MAXFILLPERCENT = 1.0;
const double DEFAULTFILLPERCENT = 0.5;
struct bucketInFile {
  page_id root = 0;
  uint64_t sequence = 0;
};

class Page;
class Node;
class Cursor;
struct Bucket {
  Transaction *tx = nullptr;
  Page *page = nullptr;
  std::shared_ptr<Node> rootNode = nullptr;
  std::unique_ptr<bucketInFile> bucketPointer;
  std::unordered_map<Item, std::shared_ptr<Bucket>> buckets;//subbucket cache. used if txn is writable. k:bucket name
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

  void setBucketPointer(std::unique_ptr<bucketInFile> ptr) {
    bucketPointer = std::move(ptr);
  }

  void setRootNode(std::shared_ptr<Node> node) {
    rootNode = std::move(node);
  }

  double getFillPercent() const {
    return fillpercent;
  }

  Cursor *createCursor();
  std::shared_ptr<Bucket> getBucketByName(const Item &searchKey);
  std::shared_ptr<Bucket> openBucket(const Item &value);
  std::shared_ptr<Bucket> createBucket(const Item &key);
  std::shared_ptr<Bucket> createBucketIfNotExists(const Item &key);
  int deleteBucket(const Item &key);

  void getPageNode(page_id pageId, std::shared_ptr<Node> &node, Page *&page);
  std::shared_ptr<Node> getNode(page_id pageId, std::shared_ptr<Node> parent);

  std::unique_ptr<char[]> write(size_t *retSz = nullptr);

  int for_each(std::function<int(const Item &, const Item &)>);
  void for_each_page(std::function<void(Page *, int)>);
  void for_each_page_node(std::function<void(Page *, Node *, int)>);
  void for_each_page_node_impl(page_id page, int depth, std::function<void(Page *, Node *, int)>);
  void free();
  void dereference();
  void rebalance();
  char* cloneBytes(const Item &key, size_t *retSz = nullptr);
  Item get(const Item &key);
  int put(const Item &key, const Item &value);
  int remove(const Item &key);
  uint64_t sequence();
  int setSequence(uint64_t v);
  int nextSequence(uint64_t &v);
  int maxInlineBucketSize();
  bool inlinable();
  int spill();//write dirty pages
};
const uint32_t BUCKETHEADERSIZE = sizeof(boltDB_CPP::bucketInFile);
}
#endif //BOLTDB_IN_CPP_BUCKET_H
