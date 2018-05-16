//
// Created by c6s on 18-4-26.
//
#include "txn.h"
#include <algorithm>
#include <cstring>
#include "bucket.h"
#include "db.h"
#include "meta.h"
#include "util.h"
namespace boltDB_CPP {

Page *boltDB_CPP::Txn::getPage(page_id pageId) {
  if (!dirtyPageTable.empty()) {
    auto iter = dirtyPageTable.find(pageId);
    if (iter != dirtyPageTable.end()) {
      return iter->second;
    }
  }
  return db->pagePointer(pageId);
}

Page *Txn::allocate(size_t count) {
  auto ret = db->allocate(count, this);
  if (ret == nullptr) {
    return ret;
  }
  dirtyPageTable[ret->pageId] = ret;

  stats.pageCount++;
  stats.pageAlloc += count * db->getPageSize();

  return ret;
}

void Txn::for_each_page(page_id pageId, int depth,
                        std::function<void(Page *, int)> fn) {
  auto p = getPage(pageId);
  fn(p, depth);

  if (p->flag & static_cast<uint32_t>(PageFlag::branchPageFlag)) {
    for (int i = 0; i < p->getCount(); i++) {
      auto element = p->getBranchPageElement(i);
      for_each_page(element->pageId, depth + 1, fn);
    }
  }
}

void Txn::init(DB *db) {
  this->db = db;
  metaData = Meta::copyCreateFrom(db->meta(), pool);
  rootBucket.setBucketHeader(metaData->rootBucketHeader);
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

int Txn::deleteBucket(const Item &name) { rootBucket.deleteBucket(name); }

int Txn::for_each(std::function<int(const Item &name, Bucket *b)> fn) {
  return rootBucket.for_each([&fn, this](const Item &k, const Item &v) -> int {
    auto ret = fn(k, rootBucket.getBucketByName(k));
    return ret;
  });
}

void Txn::OnCommit(std::function<void()> fn) { commitHandlers.push_back(fn); }

/**
 * deal with txn level work. db will release this txn' resources in upper level
 * @return disk write error / called on read only txn (what? I think read only
 * txn should succeed on doing commit)
 */
int Txn::commit() {
  // txn should not call commit in Update/View
  if (managed) {
    return -1;
  }
  if (db == nullptr || !isWritable()) {
    return -1;
  }

  // recursively merge nodes of which numbers of elements are below threshold
  rootBucket.rebalance();
  if (rootBucket.spill()) {
    rollback();
    return -1;
  }

  metaData->rootBucketHeader.rootPageId = rootBucket.getRootPage();
  auto pgid = metaData->totalPageNumber;

  free(metaData->txnId, db->pagePointer(metaData->freeListPageNumber));
  auto page = allocate((db->freeListSerialSize() / boltDB_CPP::DB::getPageSize()) + 1);
  if (page == nullptr) {
    rollback();
    return -1;
  }
  if (db->getFreeLIst().write(page)) {
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

  if (write() != 0) {
    rollback();
    return -1;
  }

  if (writeMeta() != 0) {
    rollback();
    return -1;
  }

  for (auto &item : commitHandlers) {
    item();
  }

  return 0;
}

/**
 * this should be move to db class other than placed here
 */
void Txn::rollback() {
  if (db == nullptr) {
    return;
  }
  if (isWritable()) {
    db->getFreeLIst().rollback(metaData->txnId);
    db->getFreeLIst().reload(db->pagePointer(metaData->freeListPageNumber));
  }
}

int Txn::writeMeta() {
  std::vector<char> tmp(boltDB_CPP::DB::getPageSize());
  Page *page = reinterpret_cast<Page *>(tmp.data());
  metaData->write(page);

  if (db->writeAt(tmp.data(), tmp.size(), page->pageId * boltDB_CPP::DB::getPageSize()) != tmp.size()) {
    return -1;
  }

  if (!db->isNoSync()) {
    if (file_data_sync(db->getFd())) {
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
    if (db->writeAt(reinterpret_cast<char *>(p), length, offset) != length) {
      return -1;
    }
  }

  if (!db->isNoSync()) {
    if (file_data_sync(db->getFd())) {
      return -1;
    }
  }

  return 0;
}

int Txn::isFreelistCheckOK() {
  std::map<page_id, bool> freePageIds;
  for (auto item : db->getFreeLIst().pageIds) {
    if (freePageIds.find(item) != freePageIds.end()) {
      return -1;
    }
    freePageIds[item] = true;
  }

  for (auto &p : db->getFreeLIst().pending) {
    for (auto item : p.second) {
      if (freePageIds.find(item) != freePageIds.end()) {
        return -1;
      }
      freePageIds[item] = true;
    }
  }

  std::map<page_id, Page *> occupiedPageIds;
  occupiedPageIds[0] = getPage(0);
  occupiedPageIds[1] = getPage(1);
  //add all pages included in free list page, it could be many consecutive pages there
  for (size_t i = 0; i <= getPage(metaData->freeListPageNumber)->overflow;
       i++) {
    occupiedPageIds[metaData->freeListPageNumber + i] =
        getPage(metaData->freeListPageNumber);
  }

  if (!isBucketsRemainConsistent(rootBucket, occupiedPageIds, freePageIds)) {
    return -1;
  }

  for (size_t i = 0; i < metaData->totalPageNumber; i++) {
    if ((occupiedPageIds.find(i) == occupiedPageIds.end()) && (freePageIds.find(i) == freePageIds.end())) {
      return -1;
    }
  }
  return 0;
}

bool Txn::isBucketsRemainConsistent(Bucket &bucket, std::map<page_id, Page *> &reachable,
                                    std::map<page_id, bool> &freed) {
  if (bucket.isInlineBucket()) {
    return true;
  }
  bool ret = true;
  bucket.tx->for_each_page(
      bucket.bucketHeader.rootPageId, 0, [&, this](Page *page, int i) {
        if (page->pageId >= metaData->totalPageNumber) {
          ret = false;
          return;
        }

        for (size_t i = 0; i <= page->overflow; i++) {
          auto id = page->pageId + i;
          if (reachable.find(id) != reachable.end()) {
            // multiple reference
            ret = false;
            return;
          }
          reachable[id] = page;
        }

        if (freed.find(page->pageId) != freed.end()) {
          // this page appeared in free list
          ret = false;
          return;
        }

        PageFlag pf = static_cast<PageFlag>(page->flag);
        //one page of a bucket must be either branch page or leaf page
        if (pf != PageFlag::branchPageFlag && pf != PageFlag::leafPageFlag) {
          ret = false;
          return;
        }
      });

  if (!ret) {
    return ret;
  }

  //check every sub buckets of current bucket
  auto bucketCheck = bucket.for_each([&, this](const Item &k, const Item &v) {
    auto child = bucket.getBucketByName(k);
    if (child) {
      if (!this->isBucketsRemainConsistent(*child, reachable, freed)) {
        return 1;
      }
    }
    return 0;
  });
  return bucketCheck == 0;
}

txn_id Txn::txnId() const { return metaData->txnId; }
void Txn::free(txn_id tid, Page *page) { db->getFreeLIst().free(tid, page); }
}  // namespace boltDB_CPP
