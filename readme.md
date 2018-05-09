[![Build Status](https://travis-ci.org/Nov11/boltdb_in_cpp.svg?branch=master)](https://travis-ci.org/Nov11/boltdb_in_cpp)
### A C++ implementation of [BoltDB](https://github.com/boltdb/bolt)
* under-construction
* based on version tagged 1.3.1
* supports x86_64 linux only
* compiles against C++ 11
* will be tested on ubuntu 17.10


### todo
- [ ] uniform pointer/shared pointer usage(this will not be needed providing memory pool)
- [ ] create a per txn memory pool and replace raw 'new' to calls of it (in progress)
- [ ] re-encapsulate : avoid annoying getters and provide a proper user interface
### progress
- [x] node
- [x] page
- [x] cursor
- [x] bucket
- [x] tx
- [x] freelist
- [x] db
- [ ] test cases

### later
- [ ] batch support
- [ ] status
- [ ] better error message

### on disk file layouts
#### file
* |page 0|page 1|page 2  |page 3   |...| <- general  
* |meta 0|meta 1|freelist|leaf page|...| <- typical
#### page
* page layout : |page header | page content|  
* page header : |page id | flag | count | overflow|  
* page type : meta / freelist / leaf node / branch node

#### node
* leaf node serialized:  
|page header | leaf element ....   | kv pair ...  |
* branch node serialized:  
|page header | branch element .... | kv pair ...  |

### mvcc
1. isolation level(close to) : read committed
2. exclusive remapping 
3. no exclusion on writing meta page

### great resource:
* [a series of source code reading blogs(by oceanken)](https://www.jianshu.com/p/b86a69892990)
* [bucket data structure explanation (2016)](http://www.d-kai.me/boltdb%E4%B9%8Bbucket%E4%B8%80/)

### lock lifetime
* file lock on db file
    * (acquire)when opening db in read only way, it grabs a share lock. An exclusive lock if in read write mode.
    * (release)when 'closeDB' is invoked, it releases the lock if grabbed an exclusive lock. Do nothing if grabbed a shared lock. 
    As shared lock will be released implicitly when closing the related file descriptor, a invocation of flock with LOCK_UN will be
    redundant.
* readWriteAccessMutex
    * (acquire)when starting a read/write txn
    * (release)when committing a txn(it must be a read write txn)
* mmaplock
    * (acquire)1.remap db file(full lock) 2. start a ro txn(read lock)
    * (release)1.remapped db file 2. started the ro txn 3. remove a txn
* metalock(this is actually db data structure mutex)
    * (acquire)1.begin a new rw/ro txn 2.remove a txn
    * (release)1.txn being created 2. removed a txn
* rwmtx, mmaplock, metalock are acquired and released during database shutdown