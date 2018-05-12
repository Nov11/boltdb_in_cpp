//
// Created by c6s on 18-4-26.
//

#ifndef BOLTDB_IN_CPP_BOLTDB_TYPES_H
#define BOLTDB_IN_CPP_BOLTDB_TYPES_H
#include <cstdint>
#include <functional>
#include <cstring>
namespace boltDB_CPP {

typedef uint64_t txn_id;
typedef uint64_t page_id;

class MemoryPool;
// this is used to hold values from page element
// it doesn't own any memory resource
// value copy is exactly what I want
struct Item {
  const char *pointer = nullptr;
  size_t length = 0;
  Item() = default;
  Item(const char *p, size_t sz) : pointer(p), length(sz) {}
  bool operator==(const Item &other) const;
  bool operator!=(const Item &other) const;
  bool operator<(const Item &other) const;

  void reset();
  bool empty() const;
  Item clone(MemoryPool *pool);
  static Item make_item(const char *p) {
    if (p == nullptr || *p == 0) {
      return {};
    }
    return {p, strlen(p)};
  }
};
}  // namespace boltDB_CPP

namespace std {
template<>
struct hash<boltDB_CPP::Item> {
  std::size_t operator()(const boltDB_CPP::Item &k) const {
    return hash<size_t>()(reinterpret_cast<size_t>(k.pointer)) ^
        hash<size_t>()(k.length);
  }
};
}  // namespace std
#endif  // BOLTDB_IN_CPP_BOLTDB_TYPES_H
