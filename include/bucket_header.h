//
// Created by c6s on 18-5-3.
//

#ifndef BOLTDB_IN_CPP_BUCKETHEADER_H
#define BOLTDB_IN_CPP_BUCKETHEADER_H
#include "types.h"
namespace boltDB_CPP {
struct bucket_header {
  page_id root = 0;
  uint64_t sequence = 0;
  void reset() {
    root = 0;
    sequence = 0;
  }
};
}  // namespace boltDB_CPP
#endif  // BOLTDB_IN_CPP_BUCKETHEADER_H
