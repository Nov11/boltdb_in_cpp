//
// Created by c6s on 18-5-4.
//

#include <Database.h>
#include "Meta.h"
namespace boltDB_CPP {
bool Meta::validate() {
  if (magic != MAGIC) {
    return false;
  }

  if (version != VERSION) {
    return false;
  }

//  if (checkSum != 0 && checkSum != sum64()) {
//    return false;
//  }
  return true;
}
void Meta::write(Page *page) {
  if (rootBucketHeader.root >= totalPageNumber) {
    assert(false);
  }
  if (freeListPageNumber >= totalPageNumber) {
    assert(false);
  }

  page->pageId = txnId % 2;
  page->flag |= static_cast<uint32_t >(PageFlag::metaPageFlag);

  checkSum = 0;

  std::memcpy(page->getMeta(), this, sizeof(Meta));
}
}