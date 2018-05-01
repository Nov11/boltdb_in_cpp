//
// Created by c6s on 18-4-26.
//
#include <Database.h>
#include <utility.h>
#include <algorithm>
#include <cstring>
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
void Transaction::init(Database *db) {
  this->db = db;
  metaData = MetaData::copyCreateFrom(db->meta());
  root = newBucket(this);
  root->bucketPointer.reset(new bucketInFile);
  *root->bucketPointer = metaData->root;
  if (writable) {
    metaData->txnId += 1;
  }
}
std::shared_ptr<Bucket> Transaction::getBucket(const Item &name) {
  return root->getBucketByName(name);
}
std::shared_ptr<Bucket> Transaction::createBucket(const Item &name) {
  return root->createBucket(name);
}
std::shared_ptr<Bucket> Transaction::createBucketIfNotExists(const Item &name) {
  return root->createBucketIfNotExists(name);
}
int Transaction::deleteBucket(const Item &name) {
  root->deleteBucket(name);
}
int Transaction::for_each(std::function<int(const Item &name, Bucket *b)> fn) {
  return root->for_each([&fn, this](const Item &k, const Item &v) -> int {
    auto ret = fn(k, root->getBucketByName(k).get());
    return ret;
  });
}
void Transaction::OnCommit(std::function<void()> fn) {
  commitHandlers.push_back(fn);
}
int Transaction::commit() {
  if (db == nullptr || !isWritable()) {
    return -1;
  }

  root->rebalance();
  if (root->spill()) {
    rollback();
    return -1;
  }

  metaData->root.root = root->getRoot();
  auto pgid = metaData->pageId;

  db->getFreeLIst()->free(metaData->txnId, db->getPage(metaData->freeList));
  auto page = allocate((db->getFreeLIst()->size() / db->getPageSize()) + 1);
  if (page == nullptr) {
    rollback();
    return -1;
  }
  if (db->freeList->write(page)) {
    rollback();
    return -1;
  }

  metaData->freeList = page->pageId;

  if (metaData->pageId > pgid) {
    if (db->grow((metaData->pageId + 1) * db->getPageSize())) {
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
void Transaction::rollback() {
  if (db == nullptr) {
    return;
  }
  if (isWritable()) {
    db->freeList->rollback(metaData->txnId);
    db->freeList->reload(db->getPage(metaData->freeList));
  }
  closeTxn();
}
void Transaction::closeTxn() {
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
  pageTable.clear();
  root = std::make_shared<Bucket>();
  root->tx = this;
}
int Transaction::writeMeta() {
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
int Transaction::write() {
  std::vector<Page *> pages;
  for (auto item : pageTable) {
    pages.push_back(item.second);
  }
  pageTable.clear();
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

  for (auto p : pages) {
    if (p->overflow != 0) {
      continue;
    }
//    std::memset(0, (void*)p, db->getPageSize());
    //todo:free page & need a memory pool
  }
  return 0;
}
}
