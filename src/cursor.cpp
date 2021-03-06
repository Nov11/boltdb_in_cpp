//
// Created by c6s on 18-4-27.
//

#include "cursor.h"
#include <algorithm>
#include <cassert>
#include <iostream>
#include "bucket.h"
#include "db.h"
#include "node.h"
#include "txn.h"
#include "util.h"

namespace boltDB_CPP {

bool ElementRef::isLeaf() const {
  if (node != nullptr) {
    return node->isLeaf;
  }
  assert(page);
  return (page->flag & static_cast<uint16_t>(PageFlag::leafPageFlag)) != 0;
}

size_t ElementRef::count() const {
  if (node != nullptr) {
    return node->inodeList.size();
  }
  assert(page);
  return page->count;
}

void Cursor::keyValue(Item &key, Item &value, uint32_t &flag) {
  if (dq.empty()) {
    key.reset();
    value.reset();
    flag = 0;
    return;
  }

  auto ref = dq.back();
  if (ref.count() == 0 || ref.index >= ref.count()) {
    std::cerr << "get Key/value from empty bucket / index out of range"
              << std::endl;
    return;
  }

  // are those values sitting a node?
  if (ref.node) {
    auto inode = ref.node->getInode(ref.index);
    key = inode.Key();
    value = inode.Value();
    flag = inode.flag;
    return;
  }

  // let's get them from page
  auto ret = ref.page->getLeafPageElement(ref.index);
  key = ret->Key();
  value = ret->Value();
  flag = ret->flag;
  return;
}

void Cursor::search(const Item &key, page_id pageId) {
  Node *node = nullptr;
  Page *page = nullptr;
  bucket->getPageNode(pageId, node, page);
  if (page &&
      (page->getFlag() & (static_cast<uint16_t>(PageFlag::branchPageFlag) |
          static_cast<uint16_t>(PageFlag::leafPageFlag))) == 0) {
    assert(false);
  }
  ElementRef ref{page, node};
  dq.push_back(ref);
  if (ref.isLeaf()) {
    searchLeaf(key);
    return;
  }

  if (node) {
    searchBranchNode(key, node);
    return;
  }
  searchBranchPage(key, page);
}

void Cursor::searchLeaf(const Item &key) {
  assert(!dq.empty());
  ElementRef &ref = dq.back();

  bool found = false;
  if (ref.node) {
    // search through inodeList for a matching Key
    // inodelist should be sorted in ascending order
    ref.index = static_cast<uint32_t>(ref.node->search(key, found));
    return;
  }

  auto ptr = ref.page->getLeafPageElement(0);
  ref.index = static_cast<uint32_t>(binary_search(
      ptr, key, cmp_wrapper<LeafPageElement>, ref.page->count, found));
}

void Cursor::searchBranchNode(const Item &key, Node *node) {
  bool found = false;
  auto index = node->search(key, found);
  if (!found && index > 0) {
    index--;
  }
  assert(!dq.empty());
  dq.back().index = index;
  search(key, node->getInode(index).pageId);
}

void Cursor::searchBranchPage(const Item &key, Page *page) {
  auto branchElements = page->getBranchPageElement(0);
  bool found = false;
  auto index = binary_search(
      branchElements, key, cmp_wrapper<BranchPageElement>, page->count, found);
  if (!found && index > 0) {
    index--;
  }
  assert(!dq.empty());
  dq.back().index = index;
  search(key, branchElements[index].pageId);
}

void Cursor::do_seek(Item searchKey, Item &key, Item &value, uint32_t &flag) {
  dq.clear();
  search(searchKey, bucket->getRootPage());

  auto &ref = dq.back();
  if (ref.index >= ref.count()) {
    key.reset();
    value.reset();
    flag = 0;
    return;
  }
  keyValue(key, value, flag);
}

/**
 * refactory this after main components are implemented
 * @return
 */
Node *Cursor::getNode() const {
  if (!dq.empty() && dq.back().node && dq.back().isLeaf()) {
    return dq.back().node;
  }

  std::vector<ElementRef> v(dq.begin(), dq.end());

  assert(!v.empty());
  Node *node = v[0].node;
  if (node == nullptr) {
    node = bucket->getNode(v[0].page->pageId, nullptr);
  }

  //the last one should be a leaf node
  //transverse every branch node
  for (size_t i = 0; i + 1 < v.size(); i++) {
    assert(!node->isLeafNode());
    node = node->childAt(v[i].index);
  }

  assert(node->isLeafNode());
  return node;
}

void Cursor::do_next(Item &key, Item &value, uint32_t &flag) {
  while (true) {
    while (!dq.empty()) {
      auto &ref = dq.back();
      // not the last element
      if (ref.index + 1 < ref.count()) {
        ref.index++;
        break;
      }
      dq.pop_back();
    }

    if (dq.empty()) {
      key.reset();
      value.reset();
      flag = 0;
      return;
    }

    do_first();
    // not sure what this intends to do
    if (dq.back().count() == 0) {
      continue;
    }

    keyValue(key, value, flag);
    return;
  }
}

// get to first leaf element under the last page in the stack
void Cursor::do_first() {
  while (true) {
    assert(!dq.empty());
    if (dq.back().isLeaf()) {
      break;
    }

    auto &ref = dq.back();
    page_id pageId = 0;
    if (ref.node != nullptr) {
      pageId = ref.node->getInode(ref.index).pageId;
    } else {
      pageId = ref.page->getBranchPageElement(ref.index)->pageId;
    }

    Page *page = nullptr;
    Node *node = nullptr;
    bucket->getPageNode(pageId, node, page);
    ElementRef element(page, node);
    dq.push_back(element);
  }
}

void Cursor::do_last() {
  while (true) {
    assert(!dq.empty());
    auto &ref = dq.back();
    if (ref.isLeaf()) {
      break;
    }

    page_id pageId = 0;
    if (ref.node != nullptr) {
      pageId = ref.node->getInode(ref.index).pageId;
    } else {
      pageId = ref.page->getBranchPageElement(ref.index)->pageId;
    }

    Page *page = nullptr;
    Node *node = nullptr;
    bucket->getPageNode(pageId, node, page);
    ElementRef element(page, node);
    element.index = element.count() - 1;
    dq.push_back(element);
  }
}

int Cursor::remove() {
  if (bucket->getTxn()->db == nullptr) {
    std::cerr << "db closed" << std::endl;
    return -1;
  }

  if (!bucket->isWritable()) {
    std::cerr << "txn not writable" << std::endl;
    return -1;
  }

  Item key;
  Item value;
  uint32_t flag;
  keyValue(key, value, flag);

  if (flag & static_cast<uint32_t>(PageFlag::bucketLeafFlag)) {
    std::cerr << "current value is a bucket| try removing a branch bucket "
                 "other than kv in leaf node"
              << std::endl;
    return -1;
  }

  getNode()->do_remove(key);
  return 0;
}

void Cursor::seek(const Item &searchKey, Item &key, Item &value,
                  uint32_t &flag) {
  key.reset();
  value.reset();
  flag = 0;
  do_seek(searchKey, key, value, flag);
  auto &ref = dq.back();
  if (ref.index >= ref.count()) {
    flag = 0;
    key.reset();
    value.reset();
    return;
  }
  keyValue(key, value, flag);
}

void Cursor::prev(Item &key, Item &value) {
  key.reset();
  value.reset();
  while (!dq.empty()) {
    auto &ref = dq.back();
    if (ref.index > 0) {
      ref.index--;
      break;
    }
    dq.pop_back();
  }

  if (dq.empty()) {
    return;
  }

  do_last();
  uint32_t flag = 0;
  keyValue(key, value, flag);
  // I think there's no need to clear value if current node is a branch node
}

void Cursor::next(Item &key, Item &value) {
  key.reset();
  value.reset();
  uint32_t flag = 0;
  do_next(key, value, flag);
}

void Cursor::last(Item &key, Item &value) {
  key.reset();
  value.reset();
  dq.clear();
  Page *page = nullptr;
  Node *node = nullptr;
  bucket->getPageNode(bucket->getRootPage(), node, page);
  ElementRef element{page, node};
  element.index = element.count() - 1;
  dq.push_back(element);
  do_last();
  uint32_t flag = 0;
  keyValue(key, value, flag);
}

void Cursor::first(Item &key, Item &value) {
  key.reset();
  value.reset();
  dq.clear();
  Page *page = nullptr;
  Node *node = nullptr;
  bucket->getPageNode(bucket->getRootPage(), node, page);
  ElementRef element{page, node};

  dq.push_back(element);
  do_first();

  uint32_t flag = 0;
  // what does this do?
  if (dq.back().count() == 0) {
    do_next(key, value, flag);
  }

  keyValue(key, value, flag);
}
}  // namespace boltDB_CPP
