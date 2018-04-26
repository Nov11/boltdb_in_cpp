//
// Created by c6s on 18-4-26.
//

#ifndef BOLTDB_IN_CPP_TRANSACTION_H
#define BOLTDB_IN_CPP_TRANSACTION_H
#include <map>
#include <functional>
#include <vector>
#include "boltDB_types.h"
#include "Database.h"
#include "bucket.h"

namespace boltDB_CPP {

class TransactionStats {

};

class Transaction {
  bool writable = false;
  bool managed = false;
  Database *db = nullptr;
  MetaData *metaData = nullptr;
  Bucket *root = nullptr;
  std::map<page_id, Page *> pageTable;
  TransactionStats transactionStats;
  std::vector<std::function<void()>> commitHandlers;
  bool writeFlag = false;
 public:
  txn_id id() const {
    return metaData->txnId;
  }
};

}

#endif //BOLTDB_IN_CPP_TRANSACTION_H
