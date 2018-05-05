//
// Created by c6s on 18-4-27.
//

#include <cstring>
#include <algorithm>
#include "Node.h"
#include "Database.h"
#include "Util.h"
#include "Bucket.h"
#include "Txn.h"
#include "Meta.h"
namespace boltDB_CPP {
void Node::read(boltDB_CPP::Page *page) {
  //this is called inside a function, should not receive nullptr
  assert(page);
  this->pageId = page->pageId;
  this->isLeaf = static_cast<bool>(page->flag & static_cast<int>(PageFlag::leafPageFlag));
  this->inodeList.resize(page->count);

  for (size_t i = 0; i < page->count; i++) {
    auto item = this->inodeList[i];
    if (this->isLeaf) {
      auto element = page->getLeafPageElement(i);
      item.flag = element->flag;
      item.key = element->Key();
      item.value = element->Value();
    } else {
      auto element = page->getBranchPageElement(i);
      item.pageId = element->pageId;
      item.key = element->Key();
    }
    assert(item.Key().length != 0);
  }

  if (!inodeList.empty()) {
    key = inodeList.front().Key();
    assert(!key.empty());
  } else {
    key.reset();
  }
}
Node *Node::childAt(uint64_t index) {
  if (isLeaf) {
    assert(false);
  }
  return bucket->getNode(inodeList[index].pageId, this);
}
void Node::do_remove(const Item &key) {
  bool found = false;
  size_t index = binary_search(inodeList, key, cmp_wrapper<Inode>, inodeList.size(), found);
  if (!found) {
    return;
  }

  auto b = inodeList.begin();
  std::advance(b, index);
  inodeList.erase(b);

  unbalanced = true;//need re-balance
}

/**
 * \start                                                                  |end
 * |page header| leafPageElement or branchPageElement ... | key & value ...|
 * @return
 */
size_t Node::size() const {
  size_t result = PAGEHEADERSIZE;
  for (size_t i = 0; i < inodeList.size(); i++) {
    result += pageElementSize() + inodeList[i].key.length + inodeList[i].value.length;
  }
  return result;
}
size_t Node::pageElementSize() const {
  if (isLeaf) {
    return LEAFPAGEELEMENTSIZE;
  }
  return BUCKETHEADERSIZE;
}
Node *Node::root() {
  if (parentNode == nullptr) {
    return this;
  }
  return parentNode->root();
}
size_t Node::minKeys() const {
  if (isLeaf) { return 1; }
  return 2;
}
bool Node::sizeLessThan(size_t s) const {
  size_t sz = PAGEHEADERSIZE;
  for (size_t i = 0; i < inodeList.size(); i++) {
    sz += pageElementSize() + inodeList[i].key.length + inodeList[i].value.length;
    if (sz >= s) {
      return false;
    }
  }
  return true;
}
size_t Node::childIndex(Node *child) const {
  bool found = false;
  auto ret = binary_search(inodeList, child->key, cmp_wrapper<Inode>, inodeList.size(), found);
  return ret;
}
size_t Node::numChildren() const {
  return inodeList.size();
}
Node *Node::nextSibling() {
  if (parentNode == nullptr) {
    return nullptr;
  }
  auto idx = parentNode->childIndex(this);
  if (idx == 0) {
    return nullptr;
  }
  return parentNode->childAt(idx + 1);
}

void Node::put(const Item &oldKey, const Item &newKey, const Item &value, page_id pageId, uint32_t flag) {
  if (pageId >= bucket->getTransaction()->metaData->totalPageNumber) {
    assert(false);
  }
  if (oldKey.length <= 0 || newKey.length <= 0) {
    assert(false);
  }

  bool found = false;
  auto ret = binary_search(inodeList, oldKey, cmp_wrapper<Inode>, inodeList.size(), found);
  if (!found) {
    inodeList.insert(inodeList.begin() + ret, Inode());
  }
  auto &ref = inodeList[ret];
  ref.flag = flag;
  ref.key = newKey;
  ref.value = value;
  ref.pageId = pageId;
}

void Node::del(const Item &key) {
  bool found = false;
  auto ret = binary_search(inodeList, key, cmp_wrapper<Inode>, inodeList.size(), found);

  if (!found) { return; }
  for (size_t i = ret; i + 1 < inodeList.size(); i++) {
    inodeList[i] = inodeList[i + 1];
  }

  //need re-balance
  unbalanced = true;
}
//serialize itself into a given page
void Node::write(Page *page) {
  if (isLeaf) {
    page->flag |= static_cast<uint32_t >(PageFlag::leafPageFlag);
  } else {
    page->flag |= static_cast<uint32_t >(PageFlag::branchPageFlag);
  }

  //why it exceed 0xffff ?
  if (inodeList.size() > 0xffff) {
    assert(false);
  }

  page->count = inodeList.size();
  if (page->count == 0) {
    return;
  }

  //|page header | leaf/branch element .... | kv pair ...  |
  //|<-page start| &page->ptr               |<-contentPtr  |<-page end
  auto contentPtr = (&page->ptr)[inodeList.size() * pageElementSize()];
  for (size_t i = 0; i < inodeList.size(); i++) {
    if (isLeaf) {
      auto item = page->getLeafPageElement(i);
      item->pos = contentPtr - (char *) &item;
      item->flag = inodeList[i].flag;
      item->ksize = inodeList[i].key.length;
      item->vsize = inodeList[i].value.length;
    } else {
      auto item = page->getBranchPageElement(i);
      item->pos = contentPtr - (char *) &item;
      item->ksize = inodeList[i].key.length;
      item->pageId = inodeList[i].pageId;
    }

    std::memcpy(contentPtr, inodeList[i].key.pointer, inodeList[i].key.length);
    contentPtr += inodeList[i].key.length;
    std::memcpy(contentPtr, inodeList[i].value.pointer, inodeList[i].value.length);
    contentPtr += inodeList[i].value.length;
  }

}
std::vector<Node *> Node::split(size_t pageSize) {
  std::vector<Node *> result;

  auto cur = this;
  while (true) {
    Node *a;
    Node *b;
    splitTwo(pageSize, a, b);
    result.push_back(a);
    if (b == nullptr) {
      break;
    }
    cur = b;
  }
  return result;
}

//used only inside split
void Node::splitTwo(size_t pageSize, Node *&a, Node *&b) {
  if (inodeList.size() <= MINKEYSPERPAGE * 2 || sizeLessThan(pageSize)) {
    a = nullptr;
    b = nullptr;
    return;
  }

  double fill = bucket->getFillPercent();
  if (fill < MINFILLPERCENT) {
    fill = MINFILLPERCENT;
  }
  if (fill > MAXFILLPERCENT) {
    fill = MAXFILLPERCENT;
  }

  auto threshold = static_cast<size_t >(pageSize * fill);

  size_t sz;
  auto index = splitIndex(threshold, sz);

  if (parentNode == nullptr) {
    //using share pointer to deal with this
    parentNode = bucket->tx->pool.allocate<Node>();
    parentNode->bucket = bucket;
    parentNode->children.push_back(this);
  }

  auto newNode = bucket->tx->pool.allocate<Node>();
  newNode->bucket = bucket;
  newNode->isLeaf = isLeaf;
  newNode->parentNode = parentNode;
  parentNode->children.push_back(newNode);

  for (size_t i = index; i < inodeList.size(); i++) {
    newNode->inodeList.push_back(inodeList[i]);
  }
  inodeList.erase(inodeList.begin() + index, inodeList.end());
  bucket->getTransaction()->stats.splitCount++;

}
size_t Node::splitIndex(size_t threshold, size_t &sz) {
  size_t index = 0;
  sz = PAGEHEADERSIZE;
  for (size_t i = 0; i < inodeList.size() - MINKEYSPERPAGE; i++) {
    index = i;
    auto &ref = inodeList[i];
    auto elementSize = pageElementSize() + ref.key.length + ref.value.length;
    if (i >= MINKEYSPERPAGE && sz + elementSize > threshold) {
      break;
    }
    sz += elementSize;
  }
  return index;
}
void Node::free() {
  if (pageId) {
    auto txn = bucket->getTransaction();
    txn->db->getFreeLIst().free(txn->metaData->txnId, txn->getPage(pageId));
    pageId = 0;
  }
}
void Node::removeChild(Node *target) {
  for (auto iter = children.begin(); iter != children.end(); iter++) {
    if (*iter == target) {
      children.erase(iter);
      return;
    }
  }
}
void Node::dereference() {
  //<del>
  //node lives in heap
  //nothing to be done here
  //</del>
  //2 kinds of nodes lives in inodeslist
  //1.value pointers to mmap address
  //2.value pointers to heap/memory pool allocated object
  //when remapping is needed, the first kind needs to be saved to somewhere.
  //or it will pointing to undefined values after a new mmap
  //the second case will not need to be saved
  //may provide a method in memorypool to distinguish with pointer should be saved
  //for now, just copy every value to memory pool
  //duplicate values exist. they will be freed when memorypool clears itself

  //clone current node's key
  if (!key.empty()) {
    key = key.clone(&bucket->tx->pool);
  }

  //clone current node's kv pairs
  for (auto &item : inodeList) {
    item.key = item.key.clone(&bucket->tx->pool);
    item.value = item.value.clone(&bucket->tx->pool);
  }

  //do copy recursively
  for (auto &child : children) {
    child->dereference();
  }

}
int Node::spill() {
  if (spilled) {
    return 0;
  }
  auto tx = bucket->getTransaction();

  //by pointer value for now
  std::sort(children.begin(), children.end());
  //spill will modify children's size, no range loop here
  for (size_t i = 0; i < children.size(); i++) {
    if (children[i]->spill()) {
      return -1;
    }
  }

  children.clear();
  auto nodes = split(tx->db->getPageSize());

  for (auto &item : nodes) {
    if (item->pageId > 0) {
      tx->db->getFreeLIst().free(tx->metaData->txnId, tx->getPage(item->pageId));
    }

    auto page = tx->allocate((size() / tx->db->getPageSize()) + 1);
    if (page == nullptr) { return -1; }

    if (page->pageId >= tx->metaData->totalPageNumber) {
      assert(false);
    }
    item->pageId = page->pageId;
    item->write(page);
    item->spilled = true;

    if (item->parentNode) {
      auto k = item->key;
      if (k.length == 0) {
        k = inodeList.front().key;
      }
      Item emptyValue;
      item->parentNode->put(k, item->inodeList.front().key, emptyValue, item->pageId, 0);
      item->key = item->inodeList[0].key;
    }

    tx->stats.spillCount++;
  }

  if (parentNode && parentNode->pageId == 0) {
    children.clear();
    return parentNode->spill();
  }
  return 0;
}

/**
 * this should be named by 'merge sibling'
 */
void Node::rebalance() {
  if (!unbalanced) {
    return;
  }
  unbalanced = false;
  bucket->getTransaction()->stats.rebalanceCount++;

  auto threshold = bucket->getTransaction()->db->getPageSize() / 4;
  if (size() > threshold && inodeList.size() > minKeys()) {
    return;
  }

  if (parentNode == nullptr) {
    //root node has only one branch, need to collapse it
    //assign current node to child, and remove chlid node
    if (!isLeaf && inodeList.size() == 1) {
      auto child = bucket->getNode(inodeList[0].pageId, this);
      isLeaf = child->isLeaf;
      inodeList = child->inodeList;
      children = child->children;

      for (auto &item : inodeList) {
        //branch node will have a meaningful value in pageId field
        auto iter = bucket->nodes.find(item.pageId);
        //what about those nodes that are not cached?
        //only in memory nodes will be write out to db file
        //that means if a branch node is not found/not accessed before
        //there should be something unexpected happening
        if (iter != bucket->nodes.end()) {
          iter->second->parentNode = this;
        } else {
          assert(false);
        }
      }

      child->parentNode = nullptr;
      bucket->nodes.erase(child->pageId);
      child->free();
    }

    return;
  }

  if (numChildren() == 0) {
    parentNode->del(key);
    parentNode->removeChild(this);
    bucket->nodes.erase(pageId);
    free();
    parentNode->rebalance();
    return;
  }

  assert(parentNode->numChildren() > 1);

  if (parentNode->childIndex(this) == 0) {
    auto target = nextSibling();

    //this should move inodes of target into current node
    //and re set up between node's parent&child link

    //set sibling node's children's parent to current node
    for (auto &item : target->inodeList) {
      auto iter = bucket->nodes.find(item.pageId);
      if (iter != bucket->nodes.end()) {
        auto &childNode = iter->second;
        childNode->parentNode->removeChild(childNode);
        childNode->parentNode = this;
        childNode->parentNode->children.push_back(childNode);
      }
    }

    //copy sibling node's children to current node
    std::copy(target->inodeList.begin(), target->inodeList.end(), std::back_inserter(inodeList));
    //remove sibling node
    parentNode->del(target->key);
    parentNode->removeChild(target);
    bucket->nodes.erase(target->pageId);
    target->free();
  } else {
    auto target = prevSibling();

    for (auto &item : target->inodeList) {
      auto iter = bucket->nodes.find(item.pageId);
      if (iter != bucket->nodes.end()) {
        auto &childNode = iter->second;
        childNode->parentNode->removeChild(childNode);
        childNode->parentNode = this;
        childNode->parentNode->children.push_back(childNode);
      }
    }

    std::copy(target->inodeList.begin(), target->inodeList.end(), std::back_inserter(inodeList));
    parentNode->del(this->key);
    parentNode->removeChild(this);
    bucket->nodes.erase(this->pageId);
    this->free();
  }

  //as parent node has one element removed, re-balance it
  parentNode->rebalance();
}
Node *Node::prevSibling() {
  if (parentNode == nullptr) {
    return nullptr;
  }
  auto idx = parentNode->childIndex(this);
  if (idx == 0) {
    return nullptr;
  }
  return parentNode->childAt(idx - 1);
}

}

