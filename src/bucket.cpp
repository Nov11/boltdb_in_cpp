//
// Created by c6s on 18-4-26.
//
#include "bucket.h"
#include <cstring>
#include <memory>
#include "cursor.h"
#include "db.h"
#include "meta.h"
#include "node.h"
#include "txn.h"

namespace boltDB_CPP {

Bucket *Bucket::newBucket(Txn *tx_p) {
  auto bucket = tx_p->pool.allocate<Bucket>(tx_p);
  return bucket;
}

Cursor *Bucket::createCursor() {
  tx->increaseCurserCount();
  auto ret = tx->pool.allocate<Cursor>(this);
//  std::cout << std::showbase << std::hex << (void *) ret << std::endl;
  return ret;
}

void Bucket::getPageNode(page_id pageId_p, Node *&node_p, Page *&page_p) {
  node_p = nullptr;
  page_p = nullptr;

  if (bucketHeader.rootPageId == 0) {
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

// create a node from a page and associate it with a given parent
Node *Bucket::getNode(page_id pageId, Node *parent) {
  //if the page is in the cache, return it
  auto iter = nodes.find(pageId);
  if (iter != nodes.end()) {
    return iter->second;
  }

  //make a new node
  auto node = tx->pool.allocate<Node>(this, parent);

  if (parent == nullptr) {
    assert(rootNode == nullptr);
    rootNode = node;
  } else {
    parent->addChild(node);
  }

  Page *p = page;
  if (p == nullptr) {
    p = tx->getPage(pageId);
  }

  node->read(p);
  //add this new created node to node cache
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
  if (searchKey != key ||
      (flag & static_cast<uint32_t>(PageFlag::bucketLeafFlag)) == 0) {
    return nullptr;
  }

  auto result = openBucket(value);
  buckets[searchKey] = result;
  return result;
}

Bucket *Bucket::createBucket(const Item &key) {
  if (tx->db == nullptr || !tx->writable || key.length == 0) {
    std::cerr << "invalid param " << std::endl;
    return nullptr;
  }
  auto c = Cursor(this);
  Item k;
  Item v;
  uint32_t flag;
  c.seek(key, k, v, flag);

  if (k == key) {
    if (flag & static_cast<uint16_t>(PageFlag::bucketLeafFlag)) {
      std::cerr << "key already exists" << std::endl;
      return getBucketByName(key);
    }
    return nullptr;
  }

  // create an empty inline bucket
  Bucket bucket(this->tx);
  bucket.rootNode = tx->pool.allocate<Node>(&bucket, nullptr);
  bucket.rootNode->markLeaf();

  // todo:use memory pool to fix memory leak
  Item putValue = bucket.write();

  c.getNode()->put(key, key, putValue, 0,
                   static_cast<uint32_t>(PageFlag::bucketLeafFlag));

  // this is not inline bucket any more
  page = nullptr;
  return getBucketByName(key);
}

Bucket *Bucket::openBucket(const Item &value) {
  auto child = newBucket(tx);
  //<del>
  // this may result in un-equivalent to the original purpose
  // in boltDB, it saves the pointer to mmapped file.
  // it's reinterpreting value on read-only txn, make a copy in writable one
  // here I don't make 'value' a pointer.
  // reimplementation is needed if value is shared among read txns
  // and update in mmap file is reflected through 'bucket' field
  //</del>

  if (child->tx->isWritable()) {
    std::memcpy((char *) &child->bucketHeader, value.pointer,
                sizeof(bucketHeader));
  } else {
    child->bucketHeader =
        *(reinterpret_cast<BucketHeader *>(const_cast<char *>(value.pointer)));
  }

  // is this a inline bucket?
  if (child->bucketHeader.rootPageId == 0) {
    child->page = reinterpret_cast<Page *>(
        const_cast<char *>(&value.pointer[BUCKETHEADERSIZE]));
  }
  return child;
}

// serialize bucket header & rootNode
Item Bucket::write() {
  size_t length = BUCKETHEADERSIZE + rootNode->size();
  assert(tx);
  char *result = tx->pool.allocateByteArray(length);

  // write bucketHeader in the front
  *(reinterpret_cast<BucketHeader *>(result)) = bucketHeader;

  // serialize node after bucketHeader
  auto pageInBuffer = (Page *) &result[BUCKETHEADERSIZE];
  rootNode->write(pageInBuffer);

  return Item{result, length};
}

Bucket *Bucket::createBucketIfNotExists(const Item &key) {
  auto child = createBucket(key);
  return child;
}

int Bucket::deleteBucket(const Item &key) {
  if (tx->db == nullptr || !isWritable()) {
    return -1;
  }
  auto c = createCursor();
  Item k;
  Item v;
  uint32_t flag;
  c->seek(key, k, v, flag);
  if (k != key || flag & static_cast<uint32_t>(PageFlag::bucketLeafFlag)) {
    return -1;
  }

  auto child = getBucketByName(key);
  auto ret = for_each([&child](const Item &k, const Item &v) {
    if (v.length == 0) {
      auto ret = child->deleteBucket(k);
      if (ret != 0) {
        return ret;
      }
    }
    return 0;
  });
  if (ret != 0) {
    return ret;
  }

  // remove cache
  buckets.erase(key);

  child->nodes.clear();
  child->rootNode = nullptr;
  child->free();

  c->getNode()->del(key);

  return 0;
}

int Bucket::for_each(std::function<int(const Item &, const Item &)> fn) {
  if (tx->db == nullptr) {
    return -1;
  }
  auto c = createCursor();
  Item k;
  Item v;
  c->first(k, v);
  while (k.length != 0) {
    auto ret = fn(k, v);
    if (ret != 0) {
      return ret;
    }
    c->next(k, v);
  }
  return 0;
}

void Bucket::free() {
  if (bucketHeader.rootPageId == 0) {
    return;
  }

  for_each_page_node([this](Page *p, Node *n, int) {
    if (p) {
      tx->free(tx->metaData->txnId, p);
    } else {
      assert(n);
      n->free();
    }
  });

  bucketHeader.rootPageId = 0;
}

void Bucket::for_each_page_node(std::function<void(Page *, Node *, int)> fn) {
  if (page) {
    fn(page, nullptr, 0);
    return;
  }
  for_each_page_node_impl(getRootPage(), 0, fn);
}

void Bucket::for_each_page_node_impl(
    page_id pid, int depth, std::function<void(Page *, Node *, int)> fn) {
  Node *node;
  Page *page;
  getPageNode(pid, node, page);

  fn(page, node, depth);
  if (page) {
    if (isSet(page->flag, PageFlag::branchPageFlag)) {
      for (size_t i = 0; i < page->getCount(); i++) {
        auto element = page->getBranchPageElement(i);
        for_each_page_node_impl(element->pageId, depth + 1, fn);
      }
    }
  } else {
    if (!node->isLeafNode()) {
      for (auto pid : node->branchPageIds()) {
        for_each_page_node_impl(pid, depth + 1, fn);
      }
    }
  }
}

void Bucket::dereference() {
  if (rootNode) {
    rootNode->root()->dereference();
  }

  for (auto item : buckets) {
    item.second->dereference();
  }
}

/**
 * this is merging node which has elements below threshold
 */
void Bucket::rebalance() {
  for (auto &item : nodes) {
    item.second->rebalance();
  }

  for (auto &item : buckets) {
    item.second->rebalance();
  }
}

char *Bucket::cloneBytes(const Item &key, size_t *retSz) {
  if (retSz) {
    *retSz = key.length;
  }
  auto result(new char[key.length]);
  std::memcpy(result, key.pointer, key.length);
  return result;
}

Item Bucket::get(const Item &key) {
  Item k;
  Item v;
  uint32_t flag = 0;
  createCursor()->seek(key, k, v, flag);
  if (isSet(flag, PageFlag::bucketLeafFlag) || k != key) {
    return Item();
  }
  return v;
}

int Bucket::put(const Item &key, const Item &value) {
  if (tx->db == nullptr || !isWritable() || key.length == 0 ||
      key.length > MAXKEYSIZE || value.length > MAXVALUESIZE) {
    return -1;
  }

  auto c = createCursor();
  Item k;
  Item v;
  uint32_t flag = 0;

  c->seek(key, k, v, flag);

  if (k == key && isSet(flag, PageFlag::bucketLeafFlag)) {
    return -1;
  }

  c->getNode()->put(key, key, value, 0, 0);

  return 0;
}

int Bucket::remove(const Item &key) {
  if (tx->db == nullptr || !isWritable()) {
    return -1;
  }

  auto c = createCursor();
  Item k;
  Item v;
  uint32_t flag = 0;

  c->seek(key, k, v, flag);

  if (isBucketLeaf(flag)) {
    return -1;
  }

  c->getNode()->del(key);
  return 0;
}

uint64_t Bucket::sequence() { return bucketHeader.sequence; }

int Bucket::setSequence(uint64_t v) {
  if (tx->db == nullptr || !isWritable()) {
    return -1;
  }
  if (rootNode == nullptr) {
    getNode(getRootPage(), nullptr);
  }

  bucketHeader.sequence = v;
  return 0;
}

int Bucket::nextSequence(uint64_t &v) {
  if (tx->db == nullptr || !isWritable()) {
    return -1;
  }
  if (rootNode == nullptr) {
    getNode(getRootPage(), nullptr);
  }
  bucketHeader.sequence++;
  v = bucketHeader.sequence;
  return 0;
}

void Bucket::for_each_page(std::function<void(Page *, int)> fn) {
  if (page) {
    fn(page, 0);
    return;
  }

  tx->for_each_page(getRootPage(), 0, fn);
}

size_t Bucket::maxInlineBucketSize() {
  return boltDB_CPP::DB::getPageSize() / 4;
}

bool Bucket::isInlineable() {
  auto r = rootNode;
  if (r == nullptr || !r->isLeafNode()) {
    return false;
  }

  return r->isinlineable(maxInlineBucketSize());
}

int Bucket::spill() {
  for (auto item : buckets) {
    auto name = item.first;
    auto child = item.second;

    Item newValue;

    if (child->isInlineable()) {
      child->free();
      newValue = child->write();
    } else {
      if (child->spill()) {
        return -1;
      }

      newValue.length = sizeof(bucketHeader);
      auto ptr = tx->pool.allocateByteArray(newValue.length);
      *(reinterpret_cast<BucketHeader *>(ptr)) = child->bucketHeader;
      newValue.pointer = ptr;
    }

    if (child->rootNode == nullptr) {
      continue;
    }

    auto c = createCursor();
    Item k;
    Item v;
    uint32_t flag = 0;

    c->seek(name, k, v, flag);

    if (k != name) {
      assert(false);
    }

    if (!isBucketLeaf(flag)) {
      assert(false);
    }

    c->getNode()->put(name, name, newValue, 0,
                      static_cast<uint32_t>(PageFlag::bucketLeafFlag));
  }

  if (rootNode == nullptr) {
    return 0;
  }

  auto ret = rootNode->spill();
  if (ret) {
    return ret;
  }

  rootNode = rootNode->root();

  if (rootNode->getPageId() >= tx->metaData->totalPageNumber) {
    assert(false);
  }

  bucketHeader.rootPageId = rootNode->getPageId();
  return 0;
}

bool Bucket::isWritable() const { return tx->isWritable(); }

void Bucket::reset() {
  tx = nullptr;
  page = nullptr;  // useful for inline buckets, page points to beginning of the
  // serialized value i.e. a page' header
  rootNode = nullptr;
  bucketHeader.reset();
  buckets.clear();  // subbucket cache. used if txn is writable. k:bucket name
  nodes.clear();    // node cache. used if txn is writable
}
size_t Bucket::getTotalPageNumber() const {
  return tx->metaData->totalPageNumber;
}
MemoryPool &Bucket::getPool() { return tx->pool; }
}  // namespace boltDB_CPP