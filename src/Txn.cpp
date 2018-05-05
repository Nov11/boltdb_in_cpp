//
// Created by c6s on 18-4-26.
//
#include <algorithm>
#include <cstring>
#include "Database.h"
#include "Util.h"
#include "Bucket.h"
#include "Txn.h"
#include "Meta.h"
namespace boltDB_CPP {

Page *boltDB_CPP::Txn::getPage(page_id pageId) {
  if (!dirtyPageTable.empty()) {
    auto iter = dirtyPageTable.find(pageId);
    if (iter != dirtyPageTable.end()) {
      return iter->second;
    }
  }
  return db->getPage(pageId);
}

Page *Txn::allocate(size_t count) {
  auto ret = db->allocate(count,this);
  if (ret == nullptr) {
    return ret;
  }
  dirtyPageTable[ret->pageId] = ret;

  stats.pageCount++;
  stats.pageAlloc += count * db->getPageSize();

  return ret;
}

void Txn::for_each_page(page_id pageId, int depth, std::function<void(Page *, int)> fn) {
  auto p = getPage(pageId);
  fn(p, depth);

  if (p->flag & static_cast<uint32_t >(PageFlag::branchPageFlag)) {
    for (int i = 0; i < p->getCount(); i++) {
      auto element = p->getBranchPageElement(i);
      for_each_page(element->pageId, depth, fn);
    }
  }
}

void Txn::init(Database *db) {
  this->db = db;
  metaData = Meta::copyCreateFrom(db->meta());
  //todo:reset bucket using member function
  rootBucket = *newBucket(this);
  rootBucket.bucketHeader = metaData->rootBucketHeader;
  if (writable) {
    metaData->txnId += 1;
  }
}

Bucket *Txn::getBucket(const Item &name) {
  return rootBucket.getBucketByName(name);
}

Bucket *Txn::createBucket(const Item &name) {
  return rootBucket.createBucket(name);
}

Bucket *Txn::createBucketIfNotExists(const Item &name) {
  return rootBucket.createBucketIfNotExists(name);
}

int Txn::deleteBucket(const Item &name) {
  rootBucket.deleteBucket(name);
}

int Txn::for_each(std::function<int(const Item &name, Bucket *b)> fn) {
  return rootBucket.for_each([&fn, this](const Item &k, const Item &v) -> int {
    auto ret = fn(k, rootBucket.getBucketByName(k));
    return ret;
  });
}

void Txn::OnCommit(std::function<void()> fn) {
  commitHandlers.push_back(fn);
}

/**
 *
 * @return disk write error / called on read only txn (what? I think read only txn should succeed on doing commit)
 */
int Txn::commit() {
  if (db == nullptr || !isWritable()) {
    return -1;
  }

  //recursively merge nodes of which numbers of elements are below threshold
  rootBucket.rebalance();
  if (rootBucket.spill()) {
    rollback();
    return -1;
  }

  metaData->rootBucketHeader.root = rootBucket.getRootPage();
  auto pgid = metaData->totalPageNumber;

  db->freeList.free(metaData->txnId, db->getPage(metaData->freeListPageNumber));
  auto page = allocate((db->freeList.size() / db->getPageSize()) + 1);
  if (page == nullptr) {
    rollback();
    return -1;
  }
  if (db->freeList.write(page)) {
    rollback();
    return -1;
  }

  metaData->freeListPageNumber = page->pageId;

  if (metaData->totalPageNumber > pgid) {
    if (db->grow((metaData->totalPageNumber + 1) * db->getPageSize())) {
      rollback();
      return -1;
    }
  }

  if (!write()) {
    rollback();
    return -1;
  }

  if (!writeMeta()) {
    rollback();
    return -1;
  }

  closeTxn();

  for (auto &item :commitHandlers) {
    item();
  }
  return 0;
}

void Txn::rollback() {
  if (db == nullptr) {
    return;
  }
  if (isWritable()) {
    db->freeList.rollback(metaData->txnId);
    db->freeList.reload(db->getPage(metaData->freeListPageNumber));
  }
  closeTxn();
}

void Txn::closeTxn() {
  if (db == nullptr) {
    return;
  }

  if (writable) {
    db->rwtx = nullptr;
    db->rwlock.unlock();
  } else {
    db->removeTxn(this);
  }

  db = nullptr;
  metaData = nullptr;
  dirtyPageTable.clear();
  //need a new root bucket
  rootBucket.reset();
  rootBucket.tx = this;
}

int Txn::writeMeta() {
  std::vector<char> tmp(db->getPageSize());
  Page *page = reinterpret_cast<Page *>(tmp.data());
  metaData->write(page);

  if (db->writeAt(tmp.data(), tmp.size(), page->pageId * db->getPageSize())) {
    return -1;
  }

  if (!db->noSync) {
    if (file_data_sync(db->fd)) {
      return -1;
    }
  }
  return 0;
}

int Txn::write() {
  std::vector<Page *> pages;
  for (auto item : dirtyPageTable) {
    pages.push_back(item.second);
  }
  dirtyPageTable.clear();
  std::sort(pages.begin(), pages.end());

  for (auto p : pages) {
    auto length = (p->overflow + 1) * db->getPageSize();
    auto offset = p->pageId * db->getPageSize();
    if (db->writeAt(reinterpret_cast<char *>(p), length, offset)) {
      return -1;
    }
  }

  if (!db->noSync) {
    if (file_data_sync(db->fd)) {
      return -1;
    }
  }

  return 0;
}

bool Txn::freelistcheck() {
  std::map<page_id, bool> hash;
  for (auto item : db->freeList.pageIds) {
    if (hash.find(item) != hash.end()) {
      return false;
    }
    hash[item] = true;
  }

  for (auto &p : db->freeList.pending) {
    for (auto item : p.second) {
      if (hash.find(item) != hash.end()) {
        return false;
      }
      hash[item] = true;
    }
  }

  std::map<page_id, Page *> pmap;
  pmap[0] = getPage(0);
  pmap[1] = getPage(1);
  for (size_t i = 0; i <= getPage(metaData->freeListPageNumber)->overflow; i++) {
    pmap[metaData->freeListPageNumber + i] = getPage(metaData->freeListPageNumber);
  }

  if (checkBucket(rootBucket, pmap, hash)) {
    return false;
  }

  for (size_t i = 0; i < metaData->totalPageNumber; i++) {
    if ((pmap.find(i) == pmap.end()) && (hash.find(i) == hash.end())) {
      return false;
    }
  }
  return false;
}

bool Txn::checkBucket(Bucket &bucket, std::map<page_id, Page *> &reachable, std::map<page_id, bool> &freed) {
  if (bucket.bucketHeader.root == 0) {
    return true;
  }
  bool ret = true;
  bucket.tx->for_each_page(bucket.bucketHeader.root, 0, [&, this](Page *page, int i) {
    if (page->pageId > metaData->totalPageNumber) {
      ret = false;
      return;
    }

    for (size_t i = 0; i <= page->overflow; i++) {
      auto id = page->pageId + i;
      if (reachable.find(id) != reachable.end()) {
        //multiple reference
        ret = false;
        return;
      }
      reachable[id] = page;
    }

    if (freed.find(page->pageId) != freed.end()) {
      ret = false;
      return;
    }

    PageFlag pf = static_cast<PageFlag >(page->flag);
    if (pf != PageFlag::branchPageFlag && pf != PageFlag::leafPageFlag) {
      return;
    }
  });

  if (!ret) {
    return ret;
  }

  bucket.for_each([&, this](const Item &k, const Item &v) {
    auto child = bucket.getBucketByName(k);
    if (child) {
      this->checkBucket(*child, reachable, freed);
    }
    return 0;
  });
  return true;
}
}
