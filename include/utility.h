//
// Created by c6s on 18-4-26.
//

#ifndef BOLTDB_IN_CPP_UTILITY_H
#define BOLTDB_IN_CPP_UTILITY_H
#include "boltDB_types.h"
#include "Database.h"
namespace boltDB_CPP {
int file_Wlock(int fd);
int file_Rlock(int fd);
int file_Unlock(int fd);
int mmap_db_file(Database *database, size_t sz);
int munmap_db_file(Database *database);
int file_data_sync(int fd);
template<class T, class V, class CMP>
size_t binary_search(T &target, V &key, CMP cmp, size_t e_p, bool &found) {
  found = false;
  size_t b = 0;
  size_t e = e_p;
  while (b < e) {
    size_t mid = b + (e - b) / 2;
    int ret = cmp(target[mid], key);

    if (ret == 0) {
      found = true;
      return mid;
    } else if (ret < 0) {
      b = mid + 1;
    } else {
      e = mid;
    }
  }
  return b;
};

template<class T>
int cmp_wrapper(const T &t, const Item &p) {
  if (t.Key() < p) {
    return -1;
  }
  if (t.Key() == p) {
    return 0;
  }
  return -1;
}

template<typename T, typename... Args>
std::unique_ptr<T> make_unique(Args &&... args) {
  return std::unique_ptr<T>(new T(std::forward<Args>(args)...));
}
}

void mergePageIds(std::vector<boltDB_CPP::page_id> &dest,
                  const std::vector<boltDB_CPP::page_id> &a,
                  const std::vector<boltDB_CPP::page_id> &b) {
  if (a.empty()) {
    dest = b;
    return;
  }
  if (b.empty()) {
    dest = a;
    return;
  }

  dest.clear();
  size_t ia = 0;
  size_t ib = 0;
  while (ia != a.size() || ib != b.size()) {
    if (ia == a.size()) {
      dest.push_back(b[ib++]);
    } else if (ib == b.size()) {
      dest.push_back(a[ia++]);
    } else {
      if (b[ib] < a[ia]) {
        dest.push_back(b[ib++]);
      } else {
        dest.push_back(a[ia++]);
      }
    }
  }
}

std::vector<boltDB_CPP::page_id> merge(const std::vector<boltDB_CPP::page_id> &a,
                                       const std::vector<boltDB_CPP::page_id> &b) {
  std::vector<boltDB_CPP::page_id> result;
  mergePageIds(result, a, b);
  return result;
}

#endif //BOLTDB_IN_CPP_UTILITY_H
