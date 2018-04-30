//
// Created by c6s on 18-4-27.
//

#include <Database.h>
#include <utility.h>
#include <cstring>
#include <algorithm>
#include "Node.h"
namespace boltDB_CPP {
void Node::read(boltDB_CPP::Page *page) {
  //this is called inside a function, should not receive nullptr
  assert(page);
  this->pageId = page->pageId;
  this->isLeaf = static_cast<bool>(page->flag & static_cast<int>(PageFlag::leafPageFlag));

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
std::shared_ptr<Node> Node::childAt(uint64_t index) {
  if (isLeaf) {
    assert(false);
  }
  return bucket->getNode(inodeList[index].pageId, shared_from_this());
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
std::shared_ptr<Node> Node::root() {
  if (parentNode == nullptr) {
    return shared_from_this();
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
size_t Node::childIndex(std::shared_ptr<Node> child) const {
  bool found = false;
  auto ret = binary_search(inodeList, child->key, cmp_wrapper<Inode>, inodeList.size(), found);
  return ret;
}
size_t Node::numChildren() const {
  return inodeList.size();
}
std::shared_ptr<Node> Node::nextSibling() {
  if (parentNode == nullptr) {
    return nullptr;
  }
  auto idx = parentNode->childIndex(shared_from_this());
  if (idx == 0) {
    return nullptr;
  }
  return parentNode->childAt(idx + 1);
}
void Node::put(Item &oldKey, Item &newKey, Item &value, page_id pageId, uint32_t flag) {
  if (pageId >= bucket->getTransaction()->metaData->pageId) {
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
void Node::del(Item &key) {
  bool found = false;
  auto ret = binary_search(inodeList, key, cmp_wrapper<Inode>, inodeList.size(), found);

  if (!found) { return; }
  for (size_t i = ret; i + 1 < inodeList.size(); i++) {
    inodeList[i] = inodeList[i + 1];
  }

  //need re-balance
  unbalanced = true;
}
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

  auto cur = (char *) &page->ptr;
  auto length = pageElementSize() * inodeList.size();
  for (size_t i = 0; i < inodeList.size(); i++) {
    if (isLeaf) {
      auto item = page->getLeafPageElement(i);
      item->pos = cur - (char *) &item;
      item->flag = inodeList[i].flag;
      item->ksize = inodeList[i].key.length;
      item->vsize = inodeList[i].value.length;
    } else {
      auto item = page->getBranchPageElement(i);
      item->pos = cur - (char *) &item;
      item->ksize = inodeList[i].key.length;
      item->pageId = inodeList[i].pageId;
    }

    assert(length >= inodeList[i].key.length + inodeList[i].value.length);
    std::memcpy(cur, inodeList[i].key.pointer, inodeList[i].key.length);
    cur += inodeList[i].key.length;
    std::memcpy(cur, inodeList[i].value.pointer, inodeList[i].value.length);
    cur += inodeList[i].value.length;
  }

}
std::vector<std::shared_ptr<Node>> Node::split(size_t pageSize) {
  std::vector<std::shared_ptr<Node>> result;

  auto cur = shared_from_this();
  while (true) {
    std::shared_ptr<Node> a, b;
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
void Node::splitTwo(size_t pageSize, std::shared_ptr<Node> &a, std::shared_ptr<Node> &b) {
  if (inodeList.size() <= MINKEYSPERPAGE * 2 || sizeLessThan(pageSize)) {
    a.reset();
    b.reset();
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
    parentNode = new Node;
    parentNode->bucket = bucket;
    parentNode->children.push_back(shared_from_this());
  }

  auto newNode = std::make_shared<Node>();
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
    txn->db->getFreeLIst()->free(txn->metaData->txnId, txn->getPage(pageId));
    pageId = 0;
  }
}
void Node::removeChild(std::shared_ptr<Node> target) {
  for (auto iter = children.begin(); iter != children.end(); iter++) {
    if (*iter == target) {
      children.erase(iter);
      return;
    }
  }
}
void Node::dereference() {
  //node lives in heap
  //nothing to be done here
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
      tx->db->getFreeLIst()->free(tx->metaData->txnId, tx->getPage(item->pageId));
    }

    auto page = tx->allocate((size() / tx->db->getPageSize()) + 1);
    if (page == nullptr) { return -1; }

    if (page->pageId >= tx->metaData->pageId) {
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
    if (!isLeaf && inodeList.size() == 1) {
      auto child = bucket->getNode(inodeList[0].pageId, shared_from_this());
      isLeaf = child->isLeaf;
      inodeList = child->inodeList;
      children = child->children;

      for (auto &item : inodeList) {
        auto iter = bucket->nodes.find(item.pageId);
        if (iter != bucket->nodes.end()) {
          iter->second->parentNode = this;
        }
      }

      child->parentNode = nullptr;
      bucket->nodes.erase(child->pageId);
      child->free();
    }
  }

  if (numChildren() == 0) {
    parentNode->del(key);
    parentNode->removeChild(shared_from_this());
    bucket->nodes.erase(pageId);
    free();
    parentNode->rebalance();
    return;
  }

  assert(parentNode->numChildren() > 1);

  if (parentNode->childIndex(shared_from_this()) == 0) {
    auto target = nextSibling();

    //this should move inodes of target into current node
    //and re set up between node's parent&child link
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
    parentNode->removeChild(shared_from_this());
    bucket->nodes.erase(this->pageId);
    this->free();
  }

  parentNode->rebalance();
}
std::shared_ptr<Node> Node::prevSibling() {
  if (parentNode == nullptr) {
    return nullptr;
  }
  auto idx = parentNode->childIndex(shared_from_this());
  if (idx == 0) {
    return nullptr;
  }
  return parentNode->childAt(idx - 1);
}

}

