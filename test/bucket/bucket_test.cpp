//
// Created by c6s on 18-6-2.
//

#include "test_util.h"
namespace boltDB_CPP {
TEST_F(TmpFile, get_nonexist_bucket) {
  auto updateFunc = [](Txn *txn) -> int {
    auto b = txn->createBucket(Item::make_item("widgets"));
    EXPECT_NE(b, nullptr);
    auto v = b->get(Item::make_item("foo"));
    EXPECT_EQ(v.empty(), true);
    return 0;
  };
  db->update(updateFunc);
}

TEST_F(TmpFile, get_value_from_bucket) {
  auto updateFunc = [](Txn *txn) -> int {
    auto b = txn->createBucket(Item::make_item("widgets"));
    EXPECT_NE(b, nullptr);
    auto r = b->put(Item::make_item("foo"), Item::make_item("bar"));
    EXPECT_EQ(0, r);
    auto v = b->get(Item::make_item("foo"));
    EXPECT_EQ(v, Item::make_item("bar"));
    return 0;
  };
  db->update(updateFunc);
}

TEST_F(TmpFile, subbucket_should_be_null) {
  auto updateFunc = [](Txn *txn) -> int {
    auto b1 = txn->createBucket(Item::make_item("widgets"));
    EXPECT_NE(b1, nullptr);
    auto b2 = txn->getBucket(Item::make_item("widgets"))->createBucket(Item::make_item("foo"));
    EXPECT_NE(b2, nullptr);
    auto ret = txn->getBucket(Item::make_item("widgets"))->get(Item::make_item("foo"));
    EXPECT_EQ(ret.empty(), true);
    return 0;
  };
  db->update(updateFunc);
}

TEST_F(TmpFile, bucket_put) {
  auto updateFunc = [](Txn *txn) -> int {
    auto b1 = txn->createBucket(Item::make_item("widgets"));
    EXPECT_NE(b1, nullptr);
    auto ret = b1->put(Item::make_item("foo"), Item::make_item("bar"));
    EXPECT_EQ(0, ret);
    auto val = b1->get(Item::make_item("foo"));
    EXPECT_EQ(val == Item::make_item("bar"), true);
    return 0;
  };
  db->update(updateFunc);
}

TEST_F(TmpFile, bucket_repeat_put) {
  auto updateFunc = [](Txn *txn) -> int {
    auto b1 = txn->createBucket(Item::make_item("widgets"));
    EXPECT_NE(b1, nullptr);
    auto ret = b1->put(Item::make_item("foo"), Item::make_item("bar"));
    EXPECT_EQ(0, ret);
    auto ret2 = b1->put(Item::make_item("foo"), Item::make_item("bar"));
    EXPECT_EQ(0, ret2);
    auto val = b1->get(Item::make_item("foo"));
    EXPECT_EQ(val == Item::make_item("bar"), true);
    return 0;
  };
  db->update(updateFunc);
}

}