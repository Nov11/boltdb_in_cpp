//
// Created by c6s on 18-5-11.
//

#include "types.h"
#include "memory_pool.h"
namespace boltDB_CPP{

bool Item::operator==(const Item &other) const {
  if (this == &other) {
    return true;
  }
  return pointer == other.pointer && length == other.length;
}
bool Item::operator!=(const Item &other) const {
  return !(this->operator==(other));
}
bool Item::operator<(const Item &other) const {
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
void Item::reset() {
  pointer = nullptr;
  length = 0;
}
bool Item::empty() const { return length == 0; }
Item Item::clone(MemoryPool *pool) {
  char *ptr = pool->arrayCopy(pointer, length);
  return Item{ptr, length};
}
}