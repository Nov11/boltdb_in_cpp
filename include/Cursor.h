//
// Created by c6s on 18-4-27.
//

#ifndef BOLTDB_IN_CPP_CURSOR_H
#define BOLTDB_IN_CPP_CURSOR_H

#include <cstdint>
#include <vector>
#include <stack>
#include "boltDB_types.h"
namespace boltDB_CPP {
class Page;
class Node;
class Bucket;
//reference to an element on a given page/node
struct ElementRef {
  Page *page = nullptr;
  Node *node = nullptr;
  uint64_t index = 0;//what does this indicate? no known yet
  //is this a leaf page/node
  bool isLeaf() const;

  //return the number of inodes or page elements
  size_t count() const;

  ElementRef(Page *page_p, Node *node_p) : page(page_p), node(node_p) {}
};

struct Cursor {
  Bucket *bucket = nullptr;
  std::stack<ElementRef> stk;

  Cursor() = default;
  explicit Cursor(Bucket *bucket1) : bucket(bucket1) {}

  Bucket *getBucket() const {
    return bucket;
  }
  void search(const std::string &key, page_id pageId);
  //search leaf node (which is on the top of the stack) for a Key
  void searchLeaf(const std::string &key);
  void searchBranchNode(const std::string &key, Node *node);
  void searchBranchPage(const std::string &key, Page *page);
  void keyValue(std::string &key, std::string &value, uint32_t &flag);

  //weird function signature
  //return kv of the search Key if searchkey exists
  //or return the next Key
  void seek(std::string searchKey, std::string& key, std::string& value, uint32_t &flag);

  //return the node the cursor is currently on
  Node* getNode()const;
};

}

#endif //BOLTDB_IN_CPP_CURSOR_H
