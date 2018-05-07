//
// Created by c6s on 18-4-26.
//
#include "bucket.h"
#include <txn.h>
#include <cstring>
#include <memory>
#include "cursor.h"
#include "db.h"
#include "meta.h"
#include "node.h"

namespace boltDB_CPP {

bucket *newBucket(txn *tx_p) {
  auto bucket = tx_p->pool.allocate<bucket>();
  bucket->tx = tx_p;
  return bucket;
}

cursor *bucket::createCursor() {
  tx->increaseCurserCount();
  return tx->pool.allocate<cursor>(this);
}

void bucket::getPageNode(page_id pageId_p, node *&node_p, Page *&page_p) {
  node_p = nullptr;
  page_p = nullptr;

  if (bucketHeader.root == 0) {
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
node *bucket::getNode(page_id pageId, node *parent) {
  auto iter = nodes.find(pageId);
  if (iter != nodes.end()) {
    return iter->second;
  }

  auto node = tx->pool.allocate<node>();

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

bucket *bucket::getBucketByName(const Item &searchKey) {
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

  openBucket(value);
}

bucket *bucket::createBucket(const Item &key) {
  if (tx->db == nullptr || !tx->writable || key.length == 0) {
    std::cerr << "invalid param " << std::endl;
    return nullptr;
  }
  auto c = cursor();
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
  auto bucket = bucket();
  bucket.rootNode = tx->pool.allocate<node>();
  rootNode->isLeaf = true;

  // todo:use memory pool to fix memory leak
  Item putValue = bucket.write();

  c.getNode()->put(key, key, putValue, 0,
                   static_cast<uint32_t>(PageFlag::bucketLeafFlag));

  // this is not inline bucket any more
  page = nullptr;
  return getBucketByName(key);
}

bucket *bucket::openBucket(const Item &value) {
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
    std::memcpy((char *)&child->bucketHeader, value.pointer,
                sizeof(bucketHeader));
  } else {
    child->bucketHeader =
        *(reinterpret_cast<bucketHeader *>(const_cast<char *>(value.pointer)));
  }

  // is this a inline bucket?
  if (child->bucketHeader.root == 0) {
    child->page = reinterpret_cast<Page *>(
        const_cast<char *>(&value.pointer[BUCKETHEADERSIZE]));
  }
  return child;
}

// serialize bucket header & rootNode
Item bucket::write() {
  size_t length = BUCKETHEADERSIZE + rootNode->size();

  char *result = tx->pool.allocateByteArray(length);

  // write bucketHeader in the front
  *(reinterpret_cast<bucketHeader *>(result)) = bucketHeader;

  // serialize node after bucketHeader
  auto pageInBuffer = (Page *)&result[BUCKETHEADERSIZE];
  rootNode->write(pageInBuffer);

  return Item{result, length};
}

bucket *bucket::createBucketIfNotExists(const Item &key) {
  auto child = createBucket(key);
  return child;
}

int bucket::deleteBucket(const Item &key) {
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

int bucket::for_each(std::function<int(const Item &, const Item &)> fn) {
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

void bucket::free() {
  if (bucketHeader.root == 0) {
    return;
  }

  for_each_page_node([this](Page *p, node *n, int) {
    if (p) {
      tx->db->freeList.free(tx->metaData->txnId, p);
    } else {
      assert(n);
      n->free();
    }
  });

  bucketHeader.root = 0;
}

void bucket::for_each_page_node(std::function<void(Page *, node *, int)> fn) {
  if (page) {
    fn(page, nullptr, 0);
    return;
  }
  for_each_page_node_impl(getRootPage(), 0, fn);
}

void bucket::for_each_page_node_impl(
    page_id pid, int depth, std::function<void(Page *, node *, int)> fn) {
  node *node;
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
    if (!node->isLeaf) {
      for (auto item : node->inodeList) {
        for_each_page_node_impl(item.pageId, depth + 1, fn);
      }
    }
  }
}

void bucket::dereference() {
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
void bucket::rebalance() {
  for (auto &item : nodes) {
    item.second->rebalance();
  }

  for (auto &item : buckets) {
    item.second->rebalance();
  }
}

char *bucket::cloneBytes(const Item &key, size_t *retSz) {
  if (retSz) {
    *retSz = key.length;
  }
  auto result(new char[key.length]);
  std::memcpy(result, key.pointer, key.length);
  return result;
}

Item bucket::get(const Item &key) {
  Item k;
  Item v;
  uint32_t flag = 0;
  createCursor()->seek(key, k, v, flag);
  if (isSet(flag, PageFlag::bucketLeafFlag) || k != key) {
    return Item();
  }
  return v;
}

int bucket::put(const Item &key, const Item &value) {
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

  auto tmp = cloneBytes(key);
  Item newKey(tmp, key.length);
  c->getNode()->put(newKey, newKey, value, 0,
                    static_cast<uint32_t>(PageFlag::bucketLeafFlag));

  return 0;
}

int bucket::remove(const Item &key) {
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

uint64_t bucket::sequence() { return bucketHeader.sequence; }

int bucket::setSequence(uint64_t v) {
  if (tx->db == nullptr || !isWritable()) {
    return -1;
  }
  if (rootNode == nullptr) {
    getNode(getRootPage(), nullptr);
  }

  bucketHeader.sequence = v;
  return 0;
}

int bucket::nextSequence(uint64_t &v) {
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

void bucket::for_each_page(std::function<void(Page *, int)> fn) {
  if (page) {
    fn(page, 0);
    return;
  }

  tx->for_each_page(getRootPage(), 0, fn);
}

int bucket::maxInlineBucketSize() {
  return static_cast<int>(tx->db->getPageSize() / 4);
}

bool bucket::inlineable() {
  auto r = rootNode;
  if (r == nullptr || !r->isLeaf) {
    return false;
  }

  size_t s = PAGEHEADERSIZE;
  for (auto item : r->inodeList) {
    s += LEAFPAGEELEMENTSIZE + item.key.length + item.value.length;

    if (isBucketLeaf(item.flag)) {
      return false;
    }
    if (s > maxInlineBucketSize()) {
      return false;
    }
  }
  return true;
}

int bucket::spill() {
  for (auto item : buckets) {
    auto name = item.first;
    auto child = item.second;

    Item newValue;
    size_t len = 0;
    if (child->inlineable()) {
      child->free();
      newValue = child->write();
    } else {
      if (child->spill()) {
        return -1;
      }

      newValue.length = sizeof(bucketHeader);
      auto ptr = tx->pool.allocateByteArray(newValue.length);
      *(reinterpret_cast<bucketHeader *>(ptr)) = child->bucketHeader;
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

  if (rootNode->pageId >= tx->metaData->totalPageNumber) {
    assert(false);
  }

  bucketHeader.root = rootNode->pageId;
  return 0;
}

bool bucket::isWritable() const { return tx->isWritable(); }

void bucket::reset() {
  tx = nullptr;
  page = nullptr;  // useful for inline buckets, page points to beginning of the
                   // serialized value i.e. a page' header
  rootNode = nullptr;
  bucketHeader.reset();
  buckets.clear();  // subbucket cache. used if txn is writable. k:bucket name
  nodes.clear();    // node cache. used if txn is writable
}

}  // namespace boltDB_CPP