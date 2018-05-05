//
// Created by c6s on 18-4-26.
//

#ifndef BOLTDB_IN_CPP_BUCKET_H
#define BOLTDB_IN_CPP_BUCKET_H

#include <unordered_map>
#include "Bucketheader.h"
#include "Database.h"
#include "Types.h"

namespace boltDB_CPP {
const uint32_t MAXKEYSIZE = 32768;
const uint32_t MAXVALUESIZE = (1L << 31) - 2;
const double MINFILLPERCENT = 0.1;
const double MAXFILLPERCENT = 1.0;
const double DEFAULTFILLPERCENT = 0.5;


class Page;
class Node;
class Cursor;
class Txn;
struct Bucket {
  Txn *tx = nullptr;
  Page *page = nullptr;//useful for inline buckets, page points to beginning of the serialized value i.e. a page' header
  Node* rootNode = nullptr;
  BucketHeader bucketHeader;
  std::unordered_map<Item, Bucket*> buckets;//subbucket cache. used if txn is writable. k:bucket name
  std::unordered_map<page_id, Node*> nodes;//node cache. used if txn is writable
  double fillpercent = 0.5;
 public:
  Txn *getTransaction() const {
    return tx;
  }
  page_id getRootPage() const {
    return bucketHeader.root;
  }

  bool isWritable() const;

  double getFillPercent() const {
    return fillpercent;
  }

  Cursor *createCursor();
  Bucket* getBucketByName(const Item &searchKey);
  Bucket* openBucket(const Item &value);
  Bucket* createBucket(const Item &key);
  Bucket* createBucketIfNotExists(const Item &key);
  int deleteBucket(const Item &key);

  void getPageNode(page_id pageId, Node *&node, Page *&page);
  Node* getNode(page_id pageId, Node *parent);

  Item write();//serialize bucket into a byte array

  int for_each(std::function<int(const Item &, const Item &)>);
  void for_each_page(std::function<void(Page *, int)>);
  void for_each_page_node(std::function<void(Page *, Node *, int)>);
  void for_each_page_node_impl(page_id page, int depth, std::function<void(Page *, Node *, int)>);
  void free();
  void dereference();
  void rebalance();
  char *cloneBytes(const Item &key, size_t *retSz = nullptr);
  Item get(const Item &key);
  int put(const Item &key, const Item &value);
  int remove(const Item &key);
  uint64_t sequence();
  int setSequence(uint64_t v);
  int nextSequence(uint64_t &v);
  int maxInlineBucketSize();
  bool inlineable();
  int spill();//write dirty pages
  void reset();
};
Bucket* newBucket(Txn *tx);
const uint32_t BUCKETHEADERSIZE = sizeof(boltDB_CPP::BucketHeader);
}
#endif //BOLTDB_IN_CPP_BUCKET_H
