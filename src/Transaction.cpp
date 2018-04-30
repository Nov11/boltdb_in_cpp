//
// Created by c6s on 18-4-26.
//
#include <Database.h>
namespace boltDB_CPP {

Page *boltDB_CPP::Transaction::getPage(page_id pageId) {
  if (!pageTable.empty()) {
    auto iter = pageTable.find(pageId);
    if (iter != pageTable.end()) {
      return iter->second;
    }
  }
  return db->getPage(pageId);
}
Page *Transaction::allocate(size_t count) {
  auto ret = db->allocate(count);
  if (ret == nullptr) {
    return ret;
  }
  pageTable[ret->pageId] = ret;

  stats.pageCount++;
  stats.pageAlloc += count * db->getPageSize();

  return ret;
}
void Transaction::for_each_page(page_id pageId, int depth, std::function<void(Page *, int)> fn) {
  auto p = getPage(pageId);
  fn(p, depth);

  if (p->flag & static_cast<uint32_t >(PageFlag::branchPageFlag)) {
    for (int i = 0; i < p->getCount(); i++) {
      auto element = p->getBranchPageElement(i);
      for_each_page(element->pageId, depth, fn);
    }
  }
}
}
