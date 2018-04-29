//
// Created by c6s on 18-4-26.
//
#include <cstring>
#include "Database.h"
#include "bucket.h"
#include "Cursor.h"
#include "Node.h"

namespace boltDB_CPP {

Bucket *Bucket::newBucket(Transaction *tx_p) {
  auto *bucket = new Bucket;
  bucket->tx = tx_p;
  return nullptr;
}

Cursor *Bucket::createCursor() {
  tx->increaseCurserCount();
  return new Cursor(this);
}
void Bucket::getPageNode(page_id pageId_p, Node *&node_p, Page *&page_p) {
  node_p = nullptr;
  page_p = nullptr;
  if (bucketInFile1.root == 0) {
    if (pageId_p) {
      assert(false);
    }
    if (rootNode) {
      node_p = rootNode;
      return;
    }
    page_p = page;
    return;
  }
  if (!nodes.empty()) {
    auto iter = nodes.find(pageId_p);
    if (iter != nodes.end()) {
      node_p = iter->second;
      return;
    }
  }

  page_p = tx->getPage(pageId_p);
  return;
}

//create a node from a page and associate it with a given parent
Node *Bucket::getNode(page_id pageId, Node *parent) {
  auto iter = nodes.find(pageId);
  if (iter != nodes.end()) {
    return iter->second;
  }

  Node *node = new Node;

  node->bucket = this;
  node->parentNode = parent;

  if (parent == nullptr) {
    rootNode = node;
  } else {
    parent->children.push_back(node);
  }

  Page *p = page;
  if (p == nullptr) {
    p = tx->getPage(pageId);
  }

  node->read(p);
  nodes[pageId] = node;

  tx->increaseNodeCount();

  return node;
}
Bucket *Bucket::getBucketByName(const Item &searchKey) {
  auto iter = buckets.find(searchKey);
  if (iter != buckets.end()) {
    return iter->second;
  }

  auto cursor = createCursor();
  Item key;
  Item value;
  uint32_t flag = 0;
  cursor->seek(searchKey, key, value, flag);
  if (searchKey != key || (flag & static_cast<uint32_t >(PageFlag::bucketLeafFlag)) == 0) {
    return nullptr;
  }

  openBucket(value);
}
Bucket *Bucket::createBucket(const Item &key) {

}
Bucket *Bucket::openBucket(const Item &value) {
  auto child = newBucket(tx);

  //this may result in un-equivalent to the original purpose
  //in boltDB, it saves the pointer to mmapped file.
  //it's reinterpreting value on read-only txn, make a copy in writable one
  //here I don't make 'value' a pointer.
  //reimplementation is needed if value is shared among read txns
  // and update in mmap file is reflected through 'bucket' field
//  std::memcpy(&child->bucketInFile1, value.c_str(), sizeof(child->bucketInFile1));
//  if (child->bucketInFile1.root == 0) {
//    child->page =
//  }
  return nullptr;
}

}