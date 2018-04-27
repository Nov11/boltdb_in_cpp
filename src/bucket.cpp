//
// Created by c6s on 18-4-26.
//

#include "bucket.h"
#include "Cursor.h"

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

}