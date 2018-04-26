//
// Created by c6s on 18-4-26.
//

#ifndef BOLTDB_IN_CPP_UTILITY_H
#define BOLTDB_IN_CPP_UTILITY_H

#include "Database.h"
namespace boltDB_CPP {
int file_Wlock(int fd);
int file_Rlock(int fd);
int file_Unlock(int fd);
int mmap_db_file(Database* database, size_t sz);

}

#endif //BOLTDB_IN_CPP_UTILITY_H
