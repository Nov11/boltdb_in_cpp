//
// Created by c6s on 18-4-26.
//

#ifndef BOLTDB_IN_CPP_BOLTDB_TYPES_H
#define BOLTDB_IN_CPP_BOLTDB_TYPES_H
#include <cstdint>
#include <functional>
#include "memory_pool.h"
namespace boltDB_CPP {

typedef uint64_t txn_id;
typedef uint64_t page_id;

// this is used to hold values from page element
// it doesn't own any memory resource
// value copy is exactly what I want
struct Item {
  const char *pointer = nullptr;
  size_t length = 0;
  Item() = default;
  Item(const char *p, size_t sz) : pointer(p), length(sz) {}
  bool operator==(const Item &other) const {
    if (this == &other) {
      return true;
    }
    return pointer == other.pointer && length == other.length;
  }
  bool operator!=(const Item &other) const {
    return !(this->operator==(other));
  }
  bool operator<(const Item &other) const {
    size_t i = 0;
    size_t j = 0;
    while (i < length && j < other.length) {
      if (pointer[i] == pointer[j]) {
        i++;
        j++;
        continue;
      }
      return pointer[i] < pointer[j];
    }
    return false;
  }

  void reset() {
    pointer = nullptr;
    length = 0;
  }

  bool empty() const { return length == 0; }

  Item clone(MemoryPool *pool) {
    char *ptr = pool->arrayCopy(pointer, length);
    return Item{ptr, length};
  }
};
}  // namespace boltDB_CPP

namespace std {
template <>
struct hash<boltDB_CPP::Item> {
  std::size_t operator()(const boltDB_CPP::Item &k) const {
    return hash<size_t>()(reinterpret_cast<size_t>(k.pointer)) ^
           hash<size_t>()(k.length);
  }
};
}  // namespace std
#endif  // BOLTDB_IN_CPP_BOLTDB_TYPES_H
