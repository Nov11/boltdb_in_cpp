//
// Created by c6s on 18-5-4.
//

#include "meta.h"
#include <db.h>
namespace boltDB_CPP {
bool meta::validate() {
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
void meta::write(Page *page) {
  if (rootBucketHeader.root >= totalPageNumber) {
    assert(false);
  }
  if (freeListPageNumber >= totalPageNumber) {
    assert(false);
  }

  page->pageId = txnId % 2;
  page->flag |= static_cast<uint32_t>(PageFlag::metaPageFlag);

  checkSum = 0;

  std::memcpy(page->getMeta(), this, sizeof(meta));
}
}  // namespace boltDB_CPP