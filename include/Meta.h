//
// Created by c6s on 18-5-4.
//

#ifndef BOLTDB_IN_CPP_METADATA_H
#define BOLTDB_IN_CPP_METADATA_H
namespace boltDB_CPP{
struct Meta {
  uint32_t magic = 0;
  uint32_t version = 0;
  uint32_t pageSize = 0;
  uint32_t reservedFlag = 0;
  BucketHeader rootBucketHeader;
  page_id freeListPageNumber = 0;
  page_id totalPageNumber = 0;
  txn_id txnId = 0;
  uint64_t checkSum = 0;
  static Meta *copyCreateFrom(Meta *other) {
    if (other == nullptr) {
      return other;
    }
    auto ret = new Meta;
    //member wise copy
    *ret = *other;
    return ret;
  }
  bool validate();
//  uint64_t sum64();

  void write(Page *page);
};
}


#endif //BOLTDB_IN_CPP_METADATA_H
