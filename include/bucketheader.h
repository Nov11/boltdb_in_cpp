//
// Created by c6s on 18-5-3.
//

#ifndef BOLTDB_IN_CPP_BUCKETHEADER_H
#define BOLTDB_IN_CPP_BUCKETHEADER_H
#include "boltDB_types.h"
namespace boltDB_CPP {
struct BucketHeader {
  page_id root = 0;
  uint64_t sequence = 0;
  void reset() {
    root = 0;
    sequence = 0;
  }
};
}
#endif //BOLTDB_IN_CPP_BUCKETHEADER_H
