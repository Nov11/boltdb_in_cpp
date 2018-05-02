//
// Created by c6s on 18-5-1.
//

#ifndef BOLTDB_IN_CPP_MEMORYPOOL_H
#define BOLTDB_IN_CPP_MEMORYPOOL_H

#include <cstddef>
#include <utility>
#include <algorithm>
#include <vector>
#include <cassert>
#include <cstring>
namespace boltDB_CPP {

/**
 * this is needed by zero copy.
 * it allocate/deallocate memory, but DOES NOT INVOKE DESTRUCTOR.
 * destructors are ignored when an object is created in memory pool.
 * nor will this works with hierarchy.
 * it CANNOT be used with new / shared / unique pointer.
 *
 * MemoryPool may not be a descriptive name since it doesn't have a 'pool' of memory spaces at all.
 * But it fits my need for now.
 */
class MemoryPool {
  std::vector<char *> arrays;
 public:
  ~MemoryPool() {
    for (auto item : arrays) {
      delete[] item;
    }
  }
  char *allocateByteArray(size_t sz) {
    auto ret = new char[sz];
    for (size_t i = 0; i < sz; i++) {
      ret[i] = 0;
    }
    arrays.push_back(ret);
    return ret;
  }
  template<class T, class ... Args>
  T *allocate(Args &&... args) {
    auto ret = allocateByteArray(sizeof(T));
    new(ret) T(std::forward<Args>(args)...);
    return reinterpret_cast<T *>(ret);
  }
  void deallocateByteArray(char *ptr) {
    auto iter = std::find(arrays.begin(), arrays.end(), ptr);
    assert(iter != arrays.end());
    delete[] ptr;
    arrays.erase(iter);
  }
  template<class T>
  void deallocate(T *ptr) {
    char *cptr = reinterpret_cast<char *>(ptr);
    deallocateByteArray(cptr);
  }
  char *arrayCopy(const char *src, size_t len) {
    auto ret = allocateByteArray(len);
    std::memcpy(ret, src, len);
    return ret;
  }
  MemoryPool operator=(const MemoryPool &) = delete;
  MemoryPool(const MemoryPool &) = delete;
};
}
#endif //BOLTDB_IN_CPP_MEMORYPOOL_H
