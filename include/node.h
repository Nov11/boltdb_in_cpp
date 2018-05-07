//
// Created by c6s on 18-4-27.
//

#ifndef BOLTDB_IN_CPP_NODE_H
#define BOLTDB_IN_CPP_NODE_H
#include <string>
#include <vector>
#include "types.h"
#include "util.h"
namespace boltDB_CPP {
class node;
typedef std::vector<node *> NodeList;

// this is a pointer to element. The element can be in a page or not added to a
// page yet. 1.points to an element in a page 2.points to an element not yet in a
// page this can be pointing to kv pair. in this case, pageId is meaningless. if
// the inode is comprised in a branch node, then pageId is the page starts with
// key value equals to 'key' member and the value is meaningless. may use an
// union to wrap up pageId and value
struct Inode {
  uint32_t flag = 0;
  page_id pageId = 0;
  Item key;
  Item value;
  Item Key() const { return key; }
  Item Value() const { return value; }
};

typedef std::vector<Inode> InodeList;

class Page;
class bucket;
// this is a in-memory deserialized page
struct node {
  bucket *bucket = nullptr;
  bool isLeaf = false;
  bool unbalanced = false;
  bool spilled = false;
  Item key;
  page_id pageId = 0;
  node *parentNode = nullptr;
  NodeList children;
  InodeList inodeList;

  void read(Page *page);
  node *childAt(uint64_t index);
  void do_remove(const Item &key);
  // return size of deserialized node
  size_t size() const;
  size_t pageElementSize() const;
  node *root();
  size_t minKeys() const;
  bool sizeLessThan(size_t s) const;
  size_t childIndex(node *child) const;
  size_t numChildren() const;
  node *nextSibling();
  node *prevSibling();
  void put(const Item &oldKey, const Item &newKey, const Item &value,
           page_id pageId, uint32_t flag);
  void del(const Item &key);
  void write(Page *page);
  std::vector<node *> split(size_t pageSize);
  void splitTwo(size_t pageSize, node *&a, node *&b);
  size_t splitIndex(
      size_t threshold,
      size_t &sz);  // sz is return value. it's the size of the first page.
  void free();
  void removeChild(node *target);
  void dereference();
  int spill();
  void rebalance();
};

}  // namespace boltDB_CPP

#endif  // BOLTDB_IN_CPP_NODE_H
