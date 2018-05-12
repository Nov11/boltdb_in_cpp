//
// Created by c6s on 18-5-1.
//

#ifndef BOLTDB_IN_CPP_MEMORYPOOL_H
#define BOLTDB_IN_CPP_MEMORYPOOL_H

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstring>
#include <utility>
#include <vector>
#include <iostream>
#include <map>
namespace boltDB_CPP {

/**
 * this is needed by zero copy.
 * it allocate/deallocate memory, but DOES NOT INVOKE DESTRUCTOR.
 * destructors are ignored when an object is created in memory pool.
 * nor will this works with hierarchy.
 * it CANNOT be used with new / shared / unique pointer.
 *
 * MemoryPool may not be a descriptive name since it doesn't have a 'pool' of
 * memory spaces at all. But it fits my need for now.
 */
class MemoryPool {
  //this is for char arrays
  std::vector<char *> arrays;
  //this is for typed objects
  //I need to separate this from plain arrays
  std::map<void *, std::function<void(void *)> > objs;
 public:
  ~MemoryPool() {
    for (auto item : arrays) {
//      std::cout << "delete " << std::showbase << std::hex << (void *) item << std::endl;
      //call destructor first if it has one
      auto iter = objs.find(item);
      if (iter != objs.end()) {
        iter->second(item);
      }
      //release the memory
      delete[] item;
    }
  }
  char *allocateByteArray(size_t sz) {
    auto ret = new char[sz];
    for (size_t i = 0; i < sz; i++) {
      ret[i] = 0;
    }
    arrays.push_back(ret);
//    std::cout << "allocate " << std::showbase << std::hex << (void *) ret << std::endl;
    return ret;
  }
  template<class T, class... Args>
  T *allocate(Args &&... args) {
    auto ret = allocateByteArray(sizeof(T));
    new(ret) T(std::forward<Args>(args)...);
    //register destructors into objs

    auto fn = [](void *pointer) {
      reinterpret_cast<T *>(pointer)->~T();
    };
    objs[ret] = fn;

    return reinterpret_cast<T *>(ret);
  }
  void deallocateByteArray(char *ptr) {
    assert(objs.find(ptr) == objs.end());
    auto iter = std::find(arrays.begin(), arrays.end(), ptr);
    assert(iter != arrays.end());
    delete[] ptr;
    arrays.erase(iter);
  }
  template<class T>
  void deallocate(T *ptr) {
    //there should be a destructor
    auto dtor = objs.find(ptr);
    assert(dtor != objs.end());
    //ptr should be in 'arrays'
    auto iter = std::find(arrays.begin(), arrays.end(), ptr);
    assert(iter != arrays.end());
    //call destructor first
    dtor->second(ptr);
    //release memory
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
  MemoryPool() = default;
};
}  // namespace boltDB_CPP
#endif  // BOLTDB_IN_CPP_MEMORYPOOL_H
