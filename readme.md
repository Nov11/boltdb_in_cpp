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

### layouts
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
[a series of source code reading blogs(by oceanken)](https://www.jianshu.com/p/b86a69892990)
[bucket data structure explanation (2016)](http://www.d-kai.me/boltdb%E4%B9%8Bbucket%E4%B8%80/)