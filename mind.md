记录下作业的过程，个人感觉在书写的过程中比较麻烦的主要有，首先是理解给出的数据结构，然后对应上代码相应的变量，也就是数据结构的一一对应，还有就是理解相应的函数需要完成的功能(我下面的主要是进行回顾方法的作用，具体的细节边界的问题查看对应的源文件)。前面的就是这样了，这一部分可能有点进展缓慢，之后理解完数据结构还有函数的功能就开始写代码了，然后就是根据测试用例一行一行的找bug。



# p0

## Trie

### 数据结构

![image-20221122220732527](/home/shuai/.config/Typora/typora-user-images/image-20221122220732527.png)

理解这个数据结构之前最好去看一下前缀树这一个算法leetcode，当看完之后这个之后就比较清除了，之后就是需要将代码中的变量对应上面的数据结构。

![image-20221122221934214](/home/shuai/.config/Typora/typora-user-images/image-20221122221934214.png)

```cpp
class TrieNode {
 protected:
  /** 当前节点存储的char字符 */
  char key_char_;
  /** 当前节点是否是终端节点，false代表不是，true代表是终端节点 */
  bool is_end_{false};
  /** 其中map的key记录了当前的节点的孩子的char字符是什么，
     value是智能指针指向了下一个TrieNode节点,因为是指针由于多态性，所以当前的value可以指向TrieNode的孩子节点TrieNodeWithValue */
  std::unordered_map<char, std::unique_ptr<TrieNode>> children_;
};
//其中map还有key的char的原因主要是为了快速判断当前的节点是否还有想要的孩子节点，然后进行对应的Node节点进行下一阶段的查找。

class TrieNodeWithValue : public TrieNode {
 private:
  /* 终端节点所具有的value的值 */
  T value_;
};
//类似与上面的情况插入对应的情况，其中value_一个是1另一个是"val",TrieNodeWithValue仅仅是终端节点需要is_end_=true

class Trie {
 private:
  /* root根节点本身的key_char_不存储数据 */
  std::unique_ptr<TrieNode> root_;
  /* Read-write lock for the trie */
  ReaderWriterLatch latch_;
};
//根据上面的情况根据root节点,根据插入方法不断的构建新的分支节点，并在终端节点创建与之对应的value
//根据GetValue获取当前的对应的string的终端节点的value
//根据remove方法删除对应的string
```



### 方法功能

```cpp
class TrieNode {  
  //设置当前Node的key_char_的值，设置默认不是终端节点，map初始化为空
  explicit TrieNode(char key_char) : key_char_(key_char) { is_end_ = false; }
  //查看是否有key_char孩子节点
  bool HasChild(char key_char) const { return children_.find(key_char) != children_.end(); }
  //查看当前的节点是否还有孩子节点，主要是为了在删除的时候是不是应该释放当前的节点
  bool HasChildren() const { return !children_.empty(); }
  //获取当前的节点是不是终端节点
  bool IsEndNode() const { return is_end_; }
  //获取当前节点的存储的key_char_
  char GetKeyChar();
  //将当前Node节点插入另一个Node注意key_char应该与child的key_char_一致，为了下一次插入方便，应该返回当前插入Node节点的指针
  std::unique_ptr<TrieNode> *InsertChildNode(char key_char, std::unique_ptr<TrieNode> &&child);
  //根据当前的节点查找map对应的key_char是否有子结点，返回子结点
  std::unique_ptr<TrieNode> *GetChildNode(char key_char)；
  //删除当前节点的key_char孩子节点
  void RemoveChildNode(char key_char);
  //设置当前是否是终端节点
  void SetEndNode(bool is_end) { is_end_ = is_end; }
};

template <typename T>
class TrieNodeWithValue : public TrieNode {
  //根据当前的TrieNode节点和value生成对应的本类节点
  TrieNodeWithValue(TrieNode &&trieNode, T value) : TrieNode(std::move(trieNode)), value_(value) { SetEndNode(true); }
  //根据key_char,value生成对应的终端节点，is_end终端节点默认是true
  TrieNodeWithValue(char key_char, T value) : TrieNode(key_char), value_(value) { SetEndNode(true); }
  //获取本类的T类型的value
  T GetValue() const { return value_; }
};

class Trie {
  //默认的根节点的情况
  Trie() : root_(std::make_unique<TrieNode>('\0')) {}
  //根据当前的string不断的获取char调用TrieNode的InsertChildNode方法获取插入的子结点不断的进行插入
  //最终插入一个含有value的TrieNodeWithValue的终端节点
  template <typename T>
  bool Insert(const std::string &key, T value);
  //从string的末端删除如果当前节点没有孩子节点那么就删除
  bool Remove(const std::string &key);
  //不断的遍历string到终端节点获取T的value
  template <typename T>
  T GetValue(const std::string &key, bool *success);
};
```



# p1

## ExtendibleHashTable

### 数据结构

理解相关的可扩展hash：https://www.geeksforgeeks.org/extendible-hashing-dynamic-approach-to-dbms/ 

```cpp
  class Bucket {
   private:
    //当前每一个桶中最大可以容纳的数据
    size_t size_;
    //主要是比较当前的depth和全局的depth的情况
    int depth_;
    //记录当前的链表的pair队有哪些
    std::list<std::pair<K, V>> list_;
  };
//重点需要理解的是local_depth和global_depth有什么作用
```

```cpp
template <typename K, typename V> 
class ExtendibleHashTable : public HashTable<K, V> {
  int global_depth_;    // The global depth of the directory
  size_t bucket_size_;  // 记录每一个桶最大能够容量的数据
  int num_buckets_;     // 当前有多少个桶
  std::vector<std::shared_ptr<Bucket>> dir_;  //使用该数据结构表示map
};
```

我们根据上面的数据结构结合上面的链接进行分析，也就是说其中bucket_size_ = 3对应当前的情况，初始的情况是global_depth_ = 0 , num_buckets = 1，其中num_buckets_最大值为2^global_depth，我们不断的向数据结构中进行添加数据，会导致桶溢出的情况，这个时候我们就需要进行判断，如果当前的桶的depth小于全局的depth，那么当前的桶就可以进行拆分，以下面为例如果向第三个桶中进行数据的添加，那么我们就会发现其中01和11共享同一个桶，我们可以将扩展一个桶，并不需要2倍扩展，对于第一个桶溢出需要进行2倍扩展然后进行拆分。

重点思考：

- 当桶的数据溢出的时候需要进行拆分，但是拆分我们需要修改什么指针，当local_depth小，我们需要找到对应的那一个组进行增加桶
- 如果local_depth相等的时候，扩展2倍的指针，除了该扩展的那一个之外，其余的指针应该与上面的未扩展的指向一致的地方
- 还有就是扩展完毕之后需要将当前的桶的数据重新计算hash值，比如下面的情况扩展第一个，100递增一个桶，其余的指向分别与上面的一致，但是我们对于第一个桶的数据重新计算hash并不是说不是桶000,就是桶100,扩展之后的数据对于扩展的桶可能是所有的地方。
- 注意我仅仅是对于溢出的桶进行了重新hash计算插入，对于没有溢出的并没有进行重拍，按照上面的逻辑好像不对。

![image-20221128202820020](/home/shuai/.config/Typora/typora-user-images/image-20221128202820020.png)

### 方法功能

```cpp
  class Bucket {
   public:
    //传入的是每一个桶的最大容量
    explicit Bucket(size_t size, int depth = 0);
	//判断当前的桶是否是满的，对于扩容的需要
    inline auto IsFull() const -> bool { return list_.size() == size_; }
	//获取当前的桶的depth需要对比global_depth，不同的桶depth可能在扩展上面会有所不同
    inline auto GetDepth() const -> int { return depth_; }
	//扩容后进行修改当前的depth
    inline void IncrementDepth() { depth_++; }
  	//获取当前的桶中的数据链表
    inline auto GetItems() -> std::list<std::pair<K, V>> & { return list_; }
    //对于当前的链表进行查找key，如果找到对应的key，设置当前的value指针的值返回true
    //如果没有找到返回false
    auto Find(const K &key, V &value) -> bool;
	//移除当前桶中的数据key，有key返回true，其余返回false
    auto Remove(const K &key) -> bool;
	//插入key和value。如果key存在，更新value，如果桶满了返回false
    auto Insert(const K &key, const V &value) -> bool;
  };
```



```cpp
template <typename K, typename V>
class ExtendibleHashTable : public HashTable<K, V> {
 public:
  //初始化仅仅需要，设置桶的大小
  explicit ExtendibleHashTable(size_t bucket_size);
  //获取当前全局的桶的depth
  auto GetGlobalDepth() const -> int;
  //获取某个局部的桶的depth
  auto GetLocalDepth(int dir_index) const -> int;
  //获取桶的数量
  auto GetNumBuckets() const -> int;
  //根据key获取value，进行设置value，返回true如果key找到，否则返回false
  auto Find(const K &key, V &value) -> bool override;
  //对当前的可扩展hash进行插入key,value根据溢出和depth不同进行扩展操作
  void Insert(const K &key, const V &value) override;
  //溢出可扩展hash中的key，key存在返回true，否则返回false
  auto Remove(const K &key) -> bool override;

 private:
  //根据不同类型的key，可以调用当前的函数来获取vector中的下标值，进行接下来的操作
  auto IndexOf(const K &key) -> size_t;
  auto GetGlobalDepthInternal() const -> int;
  auto GetLocalDepthInternal(int dir_index) const -> int;
  auto GetNumBucketsInternal() const -> int;
};
```



## LRUKReplacer

### 数据结构+方法功能

https://segmentfault.com/a/1190000022558044，然后在结合以下leetcode上面的LRU算法。

首先我设计的数据结构和上面的链接对应，主要是一个map，key是frame_id_t类型的数据，value是一个node节点

```cpp
class Node {
 public:
  frame_id_t frame_id_;
  // 用于替换算法
  size_t cnt_;
  // 标记是否可以被替换
  bool is_replace_;  // false 可以被取代
  std::shared_ptr<Node> next_, pre_;
  Node(frame_id_t frame_id, size_t cnt, bool is_replace) : frame_id_(frame_id), cnt_(cnt), is_replace_(is_replace) {}
};
```

```cpp
class DoubleList {
 public:
  std::shared_ptr<Node> head_, tail_;  // head_存放下一个删除的节点,tail_存放更新的节点的位置
  DoubleList() {
    head_ = std::make_shared<Node>(0, 0, false);
    tail_ = std::make_shared<Node>(0, 0, false);
    head_->next_ = tail_;
    tail_->pre_ = head_;
  }
  ~DoubleList() {
    head_->next_ = nullptr;
    tail_->pre_ = nullptr;
  }
  // 给定的节点进行删除操作	
  void RemoveNode(std::shared_ptr<Node> &node) ;
  void AddNodeToTail(std::shared_ptr<Node> &node) ;
  // 删除一个节点，如果没有返回空
  auto DeleteFirstEvictableNode() -> std::shared_ptr<Node> ;
  // 返回一个可以最开始的删除的节点
  auto GetFirstEvictableNode() -> std::shared_ptr<Node> ;
};

```



```cpp
class LRUKReplacer {
 public:
  //初始化相关的内容和两个双链表，map内置类型不用显示初始化
  explicit LRUKReplacer(size_t num_frames, size_t k);
  //不允许拷贝和移动
  DISALLOW_COPY_AND_MOVE(LRUKReplacer);
  //析构函数主要是释放双链表，在双链表的析构函数已经实现了
  ~LRUKReplacer() = default;
  //删除并返回相关的hist的头节点如果没有的话返回cach的头节点，还没有代表map中没有数据返回false(返回的数据放到frame_id中)
  auto Evict(frame_id_t *frame_id) -> bool;
  //将当前的frame_id的size_t+1，如果存在的话，不存在的话进行创建
  //如果当前的frame_id超过了传入的replacer_size_，抛出异常
  void RecordAccess(frame_id_t frame_id);
  //如果map中存在frame_id并且set_evictable=false，删除对应的map和链表
  //如果map中不存在frame_id并且set_evictable=true，添加对应的map和链表
  //如果当前的frame_id超过了传入的replacer_size_，抛出异常
  void SetEvictable(frame_id_t frame_id, bool set_evictable);
  //如果没有发现frame_id直接返回，发现了进行移除map和双链表，超出范围抛出异常
  void Remove(frame_id_t frame_id);
  //返回当前map中的数据大小
  auto Size() -> size_t;

 private:
  //代表着map的数据容量
  size_t replacer_size_;
  //代表着如果访问次数大于当前的k_，那么需要移动到双链表cach中
  size_t k_;
  //下面是使用map和两个双链表进行表示lru-k算法
  std::unordered_map<frame_id_t, std::shared_ptr<Node>> cache_;
  // 历史队列双链表存储没有到达K的cache_
  // 当前的cache_队列存储到达K的cach
  std::shared_ptr<DoubleList> hist_, cach_;
};
```



## BufferPoolManagerInstance



这一个的数据结构困扰了我好久，主要是没有明白磁盘里的页，缓冲区里面的页和帧的区别，下面我们规范下我们的说法，首先对于磁盘来说，磁盘会分为第一页、第二页这样的说法，我们的意思是磁盘是一个大的空间我们获取下标是1,2的页，然后我们对于缓冲区的页也就是下面的数据结构，也就是说我们需要将磁盘中的第几页中的数据移到下面的data_数组中，帧对应着什么，根据给出的数据结构我们可以发现，所有的帧其实和内存中的页的数组大小是一样的，帧代表着内存页数组的下标，换句话说 free_list_ 剩下什么数据，那么就代表着当前内存页面中某一个页是没有使用的，也就是说对于硬盘巨大的空间我们使用页来代表下标，对于内存中(还句话是缓冲池中)我们使用帧来表示当前页数组的下标。

我们分析下面的Page类中的数据、方法到底有什么作用。

### page数据结构+功能

对于data_数组来说我们需要将硬盘中的数据放到此处，

```cpp
class Page {
  // There is book-keeping information inside the page that should only be relevant to the buffer pool manager.
  friend class BufferPoolManagerInstance;

 private:
  /** Zeroes out the data that is held within the page. */
  inline void ResetMemory() { memset(data_, OFFSET_PAGE_START, BUSTUB_PAGE_SIZE); }

  //需要将硬盘中的实际的数据存到此处
  char data_[BUSTUB_PAGE_SIZE]{};
  //需要根据map传入的page_id获取内存中的页数组下标(帧)，但是我们需要记录原来代表硬盘页，才可以进行将该页进行写回
  page_id_t page_id_ = INVALID_PAGE_ID;
  //有多少个东东进行访问当前的页
  int pin_count_ = 0;
  //true代表当前的页已经被修改过了，否则false
  bool is_dirty_ = false;
};
```

功能比较简单，主要是获取相应的变量值，不再进行分析了。

### 分析变量

```cpp
class BufferPoolManagerInstance : public BufferPoolManager {
 protected:
  const size_t pool_size_;                      //缓冲池的大小记录着当前缓冲区，页的最大数目
  std::atomic<page_id_t> next_page_id_ = 0;     //原子操作记录着下一个分配的页的下标编号，第一个分配的是从0开始
  const size_t bucket_size_ = 4;                //可扩展hashmap的桶的最大数目
  Page *pages_;                                 //page类型代表着页面，指针指向一个pool_size_容量的Page数组
  //根据需求的页表进行向查找对应记录的frame_id_t的值，查看当前是否已经在缓冲池中，存在的话直接用，不存在的话进行添加进去
  ExtendibleHashTable<page_id_t, frame_id_t> *page_table_;
  LRUKReplacer *replacer_;                      //找到一个unpinned页表(最近很少访问的)，进行替换
  std::list<frame_id_t> free_list_;             //列出空闲的frames，初始化的时候所有的frames都是空闲的
  //我们让帧的下标指向的pages代表这当前的页是否存在。
    
  /** Pointer to the disk manager. */
  DiskManager *disk_manager_ __attribute__((__unused__));
  /** Pointer to the log manager. Please ignore this for P1. */
  LogManager *log_manager_ __attribute__((__unused__));
};
```

之后我们分析一下DiskManager::ReadPage和WritePage方法

```cpp
// 大致感觉主要是将page_id的硬盘里的数据，放到内存page_data位置
void DiskManager::ReadPage(page_id_t page_id, char *page_data);

// 大致感觉主要是将page_data中的数据放到硬盘中page_id的位置
void DiskManager::WritePage(page_id_t page_id, const char *page_data); 
```





### 方法功能

```cpp
class BufferPoolManagerInstance : public BufferPoolManager {
 public:
  //初始化pool_size,replacer_k,还有相应的replacer_和page_table_，当前所有的free_list_帧id都含有，初始化pool_size大小的数组pages_
  BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k = LRUK_REPLACER_K,
                            LogManager *log_manager = nullptr);

  ~BufferPoolManagerInstance() override;

  //返回缓冲池的大小
  auto GetPoolSize() -> size_t override { return pool_size_; }
  //返回页面数组的第一个地址
  auto GetPages() -> Page * { return pages_; }

 protected:
  //如果当前的帧都在被使用，并且pinned都不是0,那么不可以进行替换，返回null
  //对于帧来说你应该从free_list_的头部获取(存在帧)，如果从replacer_中进行替换相应的帧(如果满的话)
  //调用AllocatePage()方法进行分配一个新的页id
  //如果被取代的帧有一个脏页需要向磁盘中进行写入，需要清除数据对于新的页，记住需要设置lru-k中的SetEvictable(frame_id, false)方法
  //如果没有新的页可以被创建返回null，否则返回一个新的页  
  //主要分析：首先pinned>0代表着不可以被释放的帧，创建的时候应该设置为1,并且被使用也就是pin>0的时候lru应该设置为false，代表着不可以被舍弃
  auto NewPgImp(page_id_t *page_id) -> Page * override;

  //从缓冲池中获取相关的页面，返回null(所有的帧都被使用不可被驱逐,也就是页的pin>0)
  //否则进行添加或者替换策略，首先在缓冲区中进行查找页面，如果没有发现，从相关的free_list_的开始位置获取剩余的帧，在分配相关的页面，
  //调用disk_manager_->ReadPage()读取相关的页的数据内容，如果是一个脏页面，需要进行写回操作
  //对于当前的帧进行一次lruk的RecordAccess方法
  auto FetchPgImp(page_id_t page_id) -> Page * override;

  //对于缓冲池中的page_id页面进行Unpin操作，如果page_id不再缓冲池中,或者它的pin的值是0,返回false，其余返回true
  //对于页面减少pin的次数，如果当前的pin到达0,那么对于当前的帧应该执行驱逐操作
  //根据is_dirty设置当前的页的脏位
  auto UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool override;

  //主要是根据当前page_id获取帧，然后帧作为下标访问缓冲池页数组，查看页里面存储的page_id进行写回到相关的硬盘页中
  //使用DiskManager::WritePage()方法刷新一个页面到硬盘，刷新完毕之后取消脏位的标记,
  auto FlushPgImp(page_id_t page_id) -> bool override;

  //将所有的在缓冲池中的页面进行刷新操作
  void FlushAllPgsImp() override;

  //删除缓冲池中的page_id页，如果当前的页不再缓冲池中直接返回true，如果页被pinned不可以被删除直接返回false
  //对于删除一个page_id页，我们需要删除page_table_里的东西，并且将相关的帧重新添加到相应的free_list_中
  //重置page内存，最后调用DeallocatePage()方法释放页面
  auto DeletePgImp(page_id_t page_id) -> bool override;
  
  //分配一个page_id_t，调用该函数需要上锁
  auto AllocatePage() -> page_id_t;

  //删除一个页面，调用需要上锁
  void DeallocatePage(__attribute__((unused)) page_id_t page_id) {
    // This is a no-nop right now without a more complex data structure to track deallocated pages
  }
};
```

注意pin的值==0，并不意味着需要将当前的页进行立即删除，进行将数据写回，首先我们需要进行分析的是，pin代表着当前访问page的有多少人，那么pin==0的时候代表没有人进行访问，但是我们接下来可能会发生继续有人进行访问，我们并不需要急着将pin==0的进行写回，只有缓冲池没有空间了，然后又有新的页面请求我们需要进行lru算法获取将要被遗弃的页。







# p2

## B+树介绍

b+树由根节点、内部节点、叶子结点组成。其中每一个节点可以有多个孩子节点，根节点可以是叶子结点或者是内部节点。B+树是一个面向块设备的一种高效存储的数据结构。在一个节点中有多个指针指向孩子节点(扇入大)，在搜索的过程中执行IO次数比较少。

B+树性质

- 个别节点有关键字或者是孩子，但是不会同时含有。
- 顺序或分支因子b代表内部节点的容量，最大值代表该节点的最大孩子节点的数目
- 内部节点没有关键字，但是有非0个孩子节点，孩子节点m的取值范围如下，每一个孩子使用p来表示其中，[p(1), p(m-1)] 

$$
\lceil b/2 \rceil\leq m \leq  b
$$

- 叶子节点没有孩子取代的是B+树的元素代表着关键字，关键字的个树n和m的范围一样
- 对于一个根节点，如果是一个内部节点他的孩子节点m的范围是[ 2 , b ]
- 只有一个节点根节点，其关键字n的范围是[ 0 , b-1 ]

内部节点的区间表示，对于每一个节点的关键字的个数来说，关键字的个数n = 孩子节点的个数m - 1，当前节点会从第二个孩子节点开始选择每一个孩子节点的第一个关键字，作为父节点关键子。

<img src="/home/shuai/bustub/mind/image-20221216115710029.png" alt="image-20221216115710029" style="zoom:150%;" />

按照这个图来进行构建相关的B+树。

构建插入函数需要的功能、从初始的情况开始考虑。

- 找到想要插入的叶子节点，然后放进去，如果超出了范围进行拆分叶子节点，如果没有超出范围不用管

1. 需要超出范围的叶子节点进行差分、但是注意最大大小是奇数和偶数的情况、最好是先直接添加进去`可能会溢出`，然后size/2找到应该提取到根节点的值，最后将两边的数组进行拆分，如果提取到上面依旧是超出范围的话，那么继续拆分，直到根节点的拆分需要多生成一个页面有点复杂。





![image-20221216162100284](/home/shuai/bustub/mind/image-20221216162100284.png)

- 初始根节点添加10、20







- 添加15

![image-20221219203218754](/home/shuai/bustub/mind/image-20221219203218754.png)

size大于最大值、叶子节点需要进行拆分(根的size忘了修改)

![image-20221219203557247](/home/shuai/bustub/mind/image-20221219203557247.png)

继续插入25(根的size忘了修改)

![image-20221219204120096](/home/shuai/bustub/mind/image-20221219204120096.png)

在插入26内部节点root需要进行拆分

![image-20221219205710683](/home/shuai/bustub/mind/image-20221219205710683.png)



![image-20221219210057287](/home/shuai/bustub/mind/image-20221219210057287.png)

![image-20221219210620822](/home/shuai/bustub/mind/image-20221219210620822.png)

