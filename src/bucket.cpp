//
// Created by c6s on 18-4-26.
//
#include <memory>
#include <cstring>
#include "Database.h"
#include "bucket.h"
#include "Cursor.h"
#include "Node.h"

namespace boltDB_CPP {

std::shared_ptr<Bucket> Bucket::newBucket(Transaction *tx_p) {
  auto bucket = std::make_shared<Bucket>();
  bucket->tx = tx_p;
  return bucket;
}

Cursor *Bucket::createCursor() {
  tx->increaseCurserCount();
  return new Cursor(this);
}
void Bucket::getPageNode(page_id pageId_p, std::shared_ptr<Node> &node_p, Page *&page_p) {
  assert(bucketPointer);
  node_p = nullptr;
  page_p = nullptr;

  if (bucketPointer->root == 0) {
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
std::shared_ptr<Node> Bucket::getNode(page_id pageId, std::shared_ptr<Node> parent) {
  auto iter = nodes.find(pageId);
  if (iter != nodes.end()) {
    return iter->second;
  }

  auto node = std::make_shared<Node>();

  node->bucket = this;
  node->parentNode = parent.get();

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
std::shared_ptr<Bucket> Bucket::getBucketByName(const Item &searchKey) {
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
std::shared_ptr<Bucket> Bucket::createBucket(const Item &key) {
  return nullptr;
}
std::shared_ptr<Bucket> Bucket::openBucket(const Item &value) {
  auto child = newBucket(tx);
  //<del>
  //this may result in un-equivalent to the original purpose
  //in boltDB, it saves the pointer to mmapped file.
  //it's reinterpreting value on read-only txn, make a copy in writable one
  //here I don't make 'value' a pointer.
  //reimplementation is needed if value is shared among read txns
  // and update in mmap file is reflected through 'bucket' field
  //</del>

  if (child->tx->isWritable()) {
    child->bucketPointer = make_unique<bucketInFile>();
    std::memcpy(child->bucketPointer.get(), value.pointer, sizeof(bucketInFile));
  } else {
    child->bucketPointer.reset(reinterpret_cast<bucketInFile *>(const_cast<char *>(value.pointer)));
  }

  //is this a inline bucket?
  if (child->bucketPointer->root == 0) {
    child->page = reinterpret_cast<Page *>(const_cast<char *>(&value.pointer[BUCKETHEADERSIZE]));
  }
  return child;
}

}