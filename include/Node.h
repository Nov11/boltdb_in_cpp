//
// Created by c6s on 18-4-27.
//

#ifndef BOLTDB_IN_CPP_NODE_H
#define BOLTDB_IN_CPP_NODE_H
#include <string>
#include <vector>
#include "boltDB_types.h"
#include "utility.h"
namespace boltDB_CPP {
class Node;
typedef std::vector<Node *> NodeList;

//this is a pointer to element. The element can be in a page or not added to a page yet.
//1.points to an element in a page
//2.points to an element not yet in a page
struct Inode {
  uint32_t flag = 0;
  page_id pageId = 0;
  std::string key;
  std::string value;
  std::string Key() const {
    return key;
  }
  std::string Value() const {
    return value;
  }
};

typedef std::vector<Inode *> InodeList;

class Page;
class Bucket;
//this is a in-memory deserialized page
struct Node {
  Bucket *bucket = nullptr;
  bool isLeaf = false;
  bool unbalanced = false;
  bool spilled = false;
  std::string key;
  page_id pageId = 0;
  Node *parentNode = nullptr;
  NodeList children;
  InodeList inodeList;

  void read(Page *page);
  Node *childAt(uint64_t index);
  void do_remove(const std::string &key);
};

template<>
int cmp_wrapper<Inode *>(Inode *&t, const std::string &p) {
  if (t->Key() < p) {
    return -1;
  }
  if (t->Key() == p) {
    return 0;
  }
  return -1;
}
}

#endif //BOLTDB_IN_CPP_NODE_H
