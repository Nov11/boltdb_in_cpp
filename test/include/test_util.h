//
// Created by c6s on 18-5-9.
//

#ifndef BOLTDB_IN_CPP_TEST_UTIL_H
#define BOLTDB_IN_CPP_TEST_UTIL_H
namespace boltDB_CPP {
std::string newFileName() {
  auto ret = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now().time_since_epoch());
  int64_t currentTime = ret.count();
  return std::to_string(currentTime);
}
}
#endif //BOLTDB_IN_CPP_TEST_UTIL_H
