//
// Created by c6s on 18-4-27.
//

#include <Database.h>
#include <utility.h>
#include <cstring>
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
  return parentNode->childAt(idx - 1);
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

}

