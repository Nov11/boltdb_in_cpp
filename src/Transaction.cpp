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
}
