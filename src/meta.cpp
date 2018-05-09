//
// Created by c6s on 18-5-4.
//

#include "meta.h"
#include "db.h"
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
  if (rootBucketHeader.rootPageId >= totalPageNumber) {
    assert(false);
  }
  if (freeListPageNumber >= totalPageNumber) {
    assert(false);
  }

  page->pageId = txnId % 2;
  page->flag |= static_cast<uint32_t>(PageFlag::metaPageFlag);

  checkSum = 0;

  std::memcpy(page->metaPointer(), this, sizeof(Meta));
}
}  // namespace boltDB_CPP