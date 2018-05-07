//
// Created by c6s on 18-4-27.
//

#ifndef BOLTDB_IN_CPP_CURSOR_H
#define BOLTDB_IN_CPP_CURSOR_H

#include <cstdint>
#include <stack>
#include <vector>
#include "types.h"
namespace boltDB_CPP {
class Page;
class node;
class bucket;
// reference to an element on a given page/node
struct ElementRef {
  Page *page = nullptr;
  node *node = nullptr;
  uint64_t index = 0;  // DO NOT change this default ctor build up a ref to the
                       // first element in a page
  // is this a leaf page/node
  bool isLeaf() const;

  // return the number of inodes or page elements
  size_t count() const;

  ElementRef(Page *page_p, node *node_p) : page(page_p), node(node_p) {}
};

struct cursor {
  bucket *bucket = nullptr;
  std::stack<ElementRef> stk;

  cursor() = default;
  explicit cursor(bucket *bucket1) : bucket(bucket1) {}

  bucket *getBucket() const { return bucket; }
  void search(const Item &key, page_id pageId);
  // search leaf node (which is on the top of the stack) for a Key
  void searchLeaf(const Item &key);
  void searchBranchNode(const Item &key, node *node);
  void searchBranchPage(const Item &key, Page *page);
  void keyValue(Item &key, Item &value, uint32_t &flag);

  // weird function signature
  // return kv of the search Key if searchkey exists
  // or return the next Key
  void do_seek(Item searchKey, Item &key, Item &value, uint32_t &flag);
  void seek(const Item &searchKey, Item &key, Item &value, uint32_t &flag);

  // return the node the cursor is currently on
  node *getNode() const;

  void do_next(Item &key, Item &value, uint32_t &flag);

  void do_first();
  void do_last();
  int remove();
  void prev(Item &key, Item &value);
  void next(Item &key, Item &value);
  void last(Item &key, Item &value);
  void first(Item &key, Item &value);
};

}  // namespace boltDB_CPP

#endif  // BOLTDB_IN_CPP_CURSOR_H
