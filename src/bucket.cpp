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
  if (tx->db == nullptr || !tx->writable || key.length == 0) {
    std::cerr << "invalid param " << std::endl;
    return nullptr;
  }
  auto c = Cursor();
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

  //create an empty inline bucket
  auto bucket = Bucket();
  auto rootNode = std::make_shared<Node>();
  rootNode->isLeaf = true;
  setRootNode(rootNode);
  auto bptr = make_unique<bucketInFile>();
  setBucketPointer(std::move(bptr));

  auto value = bucket.write();
  size_t k_len = 0;
  auto ck = cloneBytes(key, &k_len);
  Item newKey(ck, k_len);
  Item putValue(value.get(), BUCKETHEADERSIZE + rootNode->size());
  c.getNode()->put(newKey, newKey, putValue, 0, static_cast<uint32_t >(PageFlag::bucketLeafFlag));
  page = nullptr;
  return getBucketByName(key);
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
std::unique_ptr<char[]> Bucket::write(size_t *retSz) {
  size_t s = rootNode->size();
  size_t length = BUCKETHEADERSIZE + s;
  std::unique_ptr<char[]> result(new char[length]);
  char *ptr = result.get();
  *(reinterpret_cast<bucketInFile *>(ptr)) = *bucketPointer;
  auto ret = (Page *) &result[BUCKETHEADERSIZE];
  rootNode->write(ret);
  if (retSz) {
    *retSz = length;
  }
  return result;
}
std::shared_ptr<Bucket> Bucket::createBucketIfNotExists(const Item &key) {
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
  if (k != key || flag & static_cast<uint32_t >(PageFlag::bucketLeafFlag)) {
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

  //remove cache
  buckets.erase(key);

  child->nodes.clear();
  child->rootNode = nullptr;
  child->free();

  c->getNode()->del(key);

  return 0;
}
int Bucket::for_each(std::function<int(const Item &, const Item &)> fn) {
  if (tx->db == nullptr) { return -1; }
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
  if (bucketPointer->root == 0) {
    return;
  }

  for_each_page_node([this](Page *p, Node *n, int) {
    if (p) {
      tx->db->getFreeLIst()->free(tx->metaData->txnId, p);
    } else {
      assert(n);
      n->free();
    }
  });

  bucketPointer->root = 0;

}
void Bucket::for_each_page_node(std::function<void(Page *, Node *, int)> fn) {
  if (page) {
    fn(page, nullptr, 0);
    return;
  }
  for_each_page_node_impl(getRoot(), 0, fn);
}
void Bucket::for_each_page_node_impl(page_id pid, int depth, std::function<void(Page *, Node *, int)> fn) {
  std::shared_ptr<Node> node;
  Page *page;
  getPageNode(pid, node, page);

  fn(page, node.get(), depth);
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
void Bucket::dereference() {
  if (rootNode) {
    rootNode->root()->dereference();
  }

  for (auto item : buckets) {
    item.second->dereference();
  }
}
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
  if (tx->db == nullptr || !isWritable() || key.length == 0 || key.length > MAXKEYSIZE
      || value.length > MAXVALUESIZE) {
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
  c->getNode()->put(newKey, newKey, value, 0, static_cast<uint32_t >(PageFlag::bucketLeafFlag));

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
uint64_t Bucket::sequence() {
  return bucketPointer->sequence;
}
int Bucket::setSequence(uint64_t v) {
  if (tx->db == nullptr || !isWritable()) {
    return -1;
  }
  if (rootNode == nullptr) {
    getNode(getRoot(), nullptr);
  }

  bucketPointer->sequence = v;
  return 0;
}
int Bucket::nextSequence(uint64_t &v) {
  if (tx->db == nullptr || !isWritable()) {
    return -1;
  }
  if (rootNode == nullptr) {
    getNode(getRoot(), nullptr);
  }
  bucketPointer->sequence++;
  v = bucketPointer->sequence;
  return 0;
}
void Bucket::for_each_page(std::function<void(Page *, int)> fn) {
  if (page) {
    fn(page, 0);
    return;
  }

  tx->for_each_page(getRoot(), 0, fn);
}
int Bucket::maxInlineBucketSize() {
  return static_cast<int>(tx->db->getPageSize() / 4);
}
bool Bucket::inlinable() {
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
int Bucket::spill() {
  for (auto item : buckets) {
    auto name = item.first;
    auto child = item.second;

    std::unique_ptr<char[]> value;
    size_t len = 0;
    if (child->inlinable()) {
      child->free();
      value = child->write(&len);
    } else {
      if (child->spill()) {
        return -1;
      }

      len = sizeof(bucketInFile);
      value.reset(new char[len]);
      *(reinterpret_cast<bucketInFile *>(value.get())) = *child->bucketPointer;
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
    Item newValue(value.get(), len);
    c->getNode()->put(name, name, newValue, 0, static_cast<uint32_t >(PageFlag::bucketLeafFlag));
  }

  if (rootNode == nullptr) {
    return 0;
  }

  auto ret = rootNode->spill();
  if (ret) {
    return ret;
  }

  rootNode = rootNode->root();

  if (rootNode->pageId >= tx->metaData->pageId) {
    assert(false);
  }

  bucketPointer->root = rootNode->pageId;
  return 0;
}

}