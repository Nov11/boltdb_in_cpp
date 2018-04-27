//
// Created by c6s on 18-4-27.
//

#include <cassert>
#include <iostream>
#include "utility.h"
#include "Cursor.h"
#include "Database.h"

namespace boltDB_CPP {

bool ElementRef::isLeaf() const {
  if (node != nullptr) {
    return node->isLeaf;
  }
  assert(page);
  return (page->flag & static_cast<uint16_t >( PageFlag::leafPageFlag)) != 0;
}

size_t ElementRef::count() const {
  if (node != nullptr) {
    return node->inodeList.size();
  }
  assert(page);
  return page->count;
}

void Cursor::keyValue(std::string &key, std::string &value, uint32_t &flag) {
  assert(!stk.empty());
  auto ref = stk.top();
  if (ref.count() == 0 || ref.index >= ref.count()) {
    std::cerr << "get Key/value from empty bucket / index out of range" << std::endl;
    return;
  }

  //are those values sitting a node?
  if (ref.node) {
    auto inode = ref.node->inodeList[ref.index];
    key = inode->key;
    value = inode->value;
    flag = inode->flag;
    return;
  }

  //let's get them from page
  auto ret = ref.page->getLeafPageElement(ref.index);
  key = ret->Key();
  value = ret->Value();
  flag = ret->flag;
  return;
}

void Cursor::search(const std::string &key, page_id pageId) {
  Node *node = nullptr;
  Page *page = nullptr;
  bucket->getPageNode(pageId, node, page);
  if (page && (page->getFlag()
      & (static_cast<uint16_t >(PageFlag::branchPageFlag) | static_cast<uint16_t >(PageFlag::leafPageFlag)))) {
    assert(false);
  }
  ElementRef ref{page, node};
  stk.push(ref);
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

template<class T>
int cmp_wrapper(T &t, const std::string &p) {
  if (t.Key() < p) {
    return -1;
  }
  if (t.Key() == p) {
    return 0;
  }
  return -1;
}

template<>
int cmp_wrapper<Inode*>(Inode* &t, const std::string &p) {
  if (t->Key() < p) {
    return -1;
  }
  if (t->Key() == p) {
    return 0;
  }
  return -1;
}


void Cursor::searchLeaf(const std::string &key) {
  assert(!stk.empty());
  ElementRef &ref = stk.top();

  bool found = false;
  if (ref.node) {
    //search through inodeList for a matching Key
    //inodelist should be sorted in ascending order
    ref.index =
        static_cast<uint32_t >(binary_search(ref.node->inodeList,
                                             key,
                                             cmp_wrapper<Inode *>,
                                             ref.node->inodeList.size(),
                                             found
        ));
    return;
  }

  auto ptr = ref.page->getLeafPageElement(0);
  ref.index = static_cast<uint32_t >(binary_search(ptr,
                                                   key,
                                                   cmp_wrapper<LeafPageElement>,
                                                   ref.page->count,
                                                   found
  ));
}
void Cursor::searchBranchNode(const std::string &key, Node *node) {
  bool found = false;
  auto index = binary_search(node->inodeList, key, cmp_wrapper<Inode *>, node->inodeList.size(), found);
  if (!found && index > 0) {
    index--;
  }
  assert(!stk.empty());
  stk.top().index = index;
  search(key, node->inodeList[index]->pageId);
}
void Cursor::searchBranchPage(const std::string &key, Page *page) {
  auto branchElements = page->getBranchPageElement(0);
  bool found = false;
  auto index = binary_search(branchElements, key, cmp_wrapper<BranchPageElement >, page->count, found);
  if (!found && index > 0) {
    index--;
  }
  assert(!stk.empty());
  stk.top().index = index;
  search(key, branchElements[index].pageId);
}
void Cursor::seek(std::string searchKey, std::string &key, std::string &value, uint32_t &flag) {
  {
    decltype(stk) tmp;
    swap(stk, tmp);
  }
  search(searchKey, bucket->getRoot());

  auto &ref = stk.top();
  if (ref.index >= ref.count()) {
    key.clear();
    value.clear();
    flag = 0;
    return;
  }
  keyValue(key, value, flag);
  return;
}
Node *Cursor::getNode() const {
  if (!stk.empty() && stk.top().node && stk.top().isLeaf()) {
    stk.top().node;
  }

  return nullptr;
}
}

