//
// Created by c6s on 18-4-27.
//

#include <Database.h>
namespace boltDB_CPP {

Page *boltDB_CPP::Database::getPage(page_id pageId) {
  assert(pageSize != 0);
  uint64_t pos = pageId * pageSize;
  return reinterpret_cast<Page *>(&data[pos]);
}
}