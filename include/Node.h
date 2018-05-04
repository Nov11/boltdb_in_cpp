//
// Created by c6s on 18-4-27.
//

#ifndef BOLTDB_IN_CPP_NODE_H
#define BOLTDB_IN_CPP_NODE_H
#include <string>
#include <vector>
#include "Types.h"
#include "Util.h"
namespace boltDB_CPP {
class Node;
typedef std::vector<Node *> NodeList;

//this is a pointer to element. The element can be in a page or not added to a page yet.
//1.points to an element in a page
//2.points to an element not yet in a page
//this can be pointing to kv pair. in this case, pageId is meaningless.
//if the inode is comprised in a branch node, then pageId is the page starts with key value equals to 'key' member
//and the value is meaningless.
//may use an union to wrap up pageId and value
struct Inode {
  uint32_t flag = 0;
  page_id pageId = 0;
  Item key;
  Item value;
  Item Key() const {
    return key;
  }
  Item Value() const {
    return value;
  }
};

typedef std::vector<Inode> InodeList;

class Page;
class Bucket;
//this is a in-memory deserialized page
struct Node {
  Bucket *bucket = nullptr;
  bool isLeaf = false;
  bool unbalanced = false;
  bool spilled = false;
  Item key;
  page_id pageId = 0;
  Node *parentNode = nullptr;
  NodeList children;
  InodeList inodeList;

  void read(Page *page);
  Node *childAt(uint64_t index);
  void do_remove(const Item &key);
  //return size of deserialized node
  size_t size() const;
  size_t pageElementSize() const;
  Node *root();
  size_t minKeys() const;
  bool sizeLessThan(size_t s) const;
  size_t childIndex(Node* child) const;
  size_t numChildren() const;
  Node *nextSibling();
  Node *prevSibling();
  void put(const Item &oldKey, const Item &newKey, const Item &value, page_id pageId, uint32_t flag);
  void del(const Item &key);
  void write(Page *page);
  std::vector<Node*> split(size_t pageSize);
  void splitTwo(size_t pageSize, Node *&a, Node *&b);
  size_t splitIndex(size_t threshold, size_t &sz);//sz is return value. it's the size of the first page.
  void free();
  void removeChild(Node* target);
  void dereference();
  int spill();
  void rebalance();
};

template<>
int cmp_wrapper<Inode>(const Inode &t, const Item &p) {
  if (t.key < p) {
    return -1;
  }
  if (t.key == p) {
    return 0;
  }
  return -1;
}
}

#endif //BOLTDB_IN_CPP_NODE_H
