//
// Created by c6s on 18-4-27.
//

#include <Database.h>
#include <utility.h>
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
      item->flag = element->flag;
      item->key = element->Key();
      item->value = element->Value();
    } else {
      auto element = page->getBranchPageElement(i);
      item->pageId = element->pageId;
      item->key = element->Key();
    }
    assert(item->Key().length != 0);
  }

  if (!inodeList.empty()) {
    key = inodeList.front()->Key();
    assert(!key.empty());
  } else {
    key.reset();
  }
}
std::shared_ptr<Node> Node::childAt(uint64_t index) {
  if (isLeaf) {
    assert(false);
  }
  return bucket->getNode(inodeList[index]->pageId, shared_from_this());
}
void Node::do_remove(const Item &key) {
  bool found = false;
  size_t index = binary_search(inodeList, key, cmp_wrapper<Inode *>, inodeList.size(), found);
  if (!found) {
    return;
  }

  auto b = inodeList.begin();
  std::advance(b, index);
  inodeList.erase(b);

  unbalanced = true;//need re-balance
}
}

