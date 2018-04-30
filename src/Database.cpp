//
// Created by c6s on 18-4-27.
//

#include <Database.h>
#include <algorithm>
#include <utility.h>
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
page_id FreeList::allocate(size_t sz) {
  if (pageIds.empty()) {
    return 0;
  }

  page_id init = 0;
  page_id prev = 0;

  for (size_t i = 0; i < pageIds.size(); i++) {
    page_id id = pageIds[i];
    if (id <= 1) {
      assert(false);
    }

    if (prev == 0 || id - prev != 1) {
      init = id;
    }

    if (id - prev == sz + 1) {
      for (size_t j = 0; j < sz; j++) {
        pageIds[i + 1 - sz] = pageIds[i + 1 + j];
      }

      for (size_t j = 0; j < sz; j++) {
        cache.erase(init + j);
      }

      return init;
    }

    prev = id;
  }

  return 0;
}
void FreeList::release(txn_id tid) {
  std::vector<page_id> list;
  for (auto iter = pending.begin(); iter != pending.end();) {
    if (iter->first <= tid) {
      std::copy(iter->second.begin(), iter->second.end(), std::back_inserter(list));
      iter = pending.erase(iter);
    }
  }

  std::sort(list.begin(), list.end());
  pageIds = merge(pageIds, list);
}
void FreeList::rollback(txn_id tid) {
  for (auto item : pending[tid]) {
    cache.erase(item);
  }

  pending.erase(tid);
}
bool FreeList::freed(page_id pageId) {
  return cache[pageId];
}
void FreeList::read(Page *page) {
  size_t idx = 0;
  size_t count = page->count;
  if (count == 0xffff) {
    idx = 1;
    count = *reinterpret_cast<page_id *>(&page->ptr);
  }

  if (count == 0) {
    pageIds.clear();
  } else {
    pageIds.clear();
    page_id *ptr = reinterpret_cast<page_id *>(&page->ptr) + idx;
    for (size_t i = idx; i < count; i++) {
      pageIds.push_back(*ptr);
      ptr++;
    }

    std::sort(pageIds.begin(), pageIds.end());
  }

  reindex();
}
void FreeList::reindex() {
  cache.clear();
  for (auto item : pageIds) {
    cache[item] = true;
  }

  for (auto &item : pending) {
    for (auto inner : item.second) {
      cache[inner] = true;
    }
  }
}
int FreeList::write(Page *page) {
  page->flag |= static_cast<uint16_t >(PageFlag::freelistPageFlag);
  auto count = this->count();
  if (count == 0) {
    page->count = count;
  } else if (count < 0xffff) {
    page->count = count;
    //re-implement it to avoid copying later
    std::vector<page_id> dest;
    copyall(dest);
    auto ptr = reinterpret_cast<page_id *>(&page->ptr);
    for (auto item : dest) {
      *ptr = item;
      ptr++;
    }
  } else {
    page->count = 0xffff;
    auto ptr = reinterpret_cast<page_id *>(&page->ptr);
    *ptr = count;
    std::vector<page_id> dest;
    copyall(dest);
    ptr++;
    for (auto item : dest) {
      *ptr = item;
      ptr++;
    }
  }
  return 0;
}
void FreeList::reload(Page *page) {
  read(page);

  //filter out current pending pages from those just read from page file
  std::map<page_id, bool> curPending;
  for (auto item : pending) {
    for (auto inner : item.second) {
      curPending[inner] = true;
    }
  }

  std::vector<page_id> newIds;
  for (auto item : pageIds) {
    if (curPending.find(item) == curPending.end()) {
      newIds.push_back(item);
    }
  }

  pageIds = newIds;
  reindex();
}

}