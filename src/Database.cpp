//
// Created by c6s on 18-4-27.
//

#include <Database.h>
#include <algorithm>
namespace boltDB_CPP {

Page *boltDB_CPP::Database::getPage(page_id pageId) {
  assert(pageSize != 0);
  uint64_t pos = pageId * pageSize;
  return reinterpret_cast<Page *>(&data[pos]);
}
FreeList *Database::getFreeLIst() {
  return freeList;
}
uint64_t Database::getPageSize() const {
  return pageSize;
}
Page *Database::allocate(size_t count) {
  return nullptr;
}

void FreeList::free(txn_id tid, Page *page) {
  if (page->pageId <= 1) {
    assert(false);
  }
  auto &idx = pending[tid];
  for (auto iter = page->pageId; iter <= page->pageId + page->overflow; iter++) {
    if (cache[iter]) {

      assert(false);
    }

    idx.push_back(iter);
    cache[iter] = true;
  }
}
size_t FreeList::size() {
  auto ret = count();

  if (ret >= 0xffff) {
    ret++;
  }

  return PAGEHEADERSIZE + ret * sizeof(page_id);
}
size_t FreeList::count() {
  return free_count() + pending_count();
}
size_t FreeList::free_count() {
  return pageIds.size();
}
size_t FreeList::pending_count() {
  size_t result = 0;
  for (auto &item : pending) {
    result += item.second.size();
  }
  return result;
}
void FreeList::copyall(std::vector<page_id> &dest) {
  std::vector<page_id> tmp;
  for (auto item : pending) {
    std::copy(item.second.begin(), item.second.end(), std::back_inserter(tmp));
  }
  std::sort(tmp.begin(), tmp.end());

}
}