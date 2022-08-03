/*
    跳表的实现
*/
#ifndef DB_SKIPLIST_H_
#define DB_SKIPLIST_H_

#include <assert.h>
#include <stdint.h>

#include <atomic>
#include <new>

#include "logger/log.h"
#include "logger/log_level.h"
#include "utils/random_util.h"
namespace z_kv {
struct SkipListOption {
  static constexpr int32_t kMaxHeight = 20;
  //有多少概率被选中, 空间和时间的折中
  static constexpr unsigned int kBranching = 4;
};

template <typename _KeyType, typename _KeyComparator, typename _Allocator>
class SkipList final {
  struct Node;

 public:
  SkipList(_KeyComparator comparator);

  SkipList(const SkipList&) = delete;

  SkipList& operator=(const SkipList&) = delete;

  //将key插入跳表（插入前key还没有被节点封装）
  void Insert(const _KeyType& key) {
    // 记录插入位置在各层的前驱
    Node* prev[SkipListOption::kMaxHeight] = {nullptr};
    // 获取插入位置的后继
    Node* node = FindGreaterOrEqual(key, prev);
    //在key的构造过程中，有一个持续递增的序号，因此理论上不会有重复的key
    if (nullptr != node) {
      if (Equal(key, node->key)) {
        LOG(WARN, "key:%s has existed", key);
        return;
      }
    }
    //要插入的层数
    int32_t new_level = RandomHeight();
    int32_t cur_max_level = GetMaxHeight();
    if (new_level > cur_max_level) {
      //因为skiplist存在多层，而刚开始的时候只是分配kMaxHeight个空间，每一层的next并没有真正使用
      for (int32_t index = cur_max_level; index < new_level; ++index) {
        prev[index] = head_;
      }
      // 更新当前的层数
      cur_height_.store(new_level, std::memory_order_relaxed);
    }
    //上链
    Node* new_node = NewNode(key, new_level);
    for (int32_t index = 0; index < new_level; ++index) {
      new_node->NoBarrier_SetNext(index, prev[index]->NoBarrier_Next(index));
      prev[index]->NoBarrier_SetNext(index, new_node);
    }
  }
 
  //查询跳表中是否存在key
  bool Contains(const _KeyType& key) {
    Node* node = FindGreaterOrEqual(key, nullptr);
    return nullptr != node && Equal(key, node->key);
  }

  //比较key的大小
  bool Equal(const _KeyType& a, const _KeyType& b) {
    return comparator_.Compare(a, b) == 0;
  }

 private:
  Node* NewNode(const _KeyType& key, int32_t height); //新建跳表节点
  int32_t RandomHeight();                             //满足概率分布的返回一个层数
  int32_t GetMaxHeight() { return cur_height_.load(std::memory_order_relaxed); }//返回跳表的层

  //判断key是否在Node之后
  bool KeyIsAfterNode(const _KeyType& key, Node* n) {
    return (nullptr != n && comparator_.Compare( n->key, key) < 0);
  }
 
  //找到第一个大于等于key的node（升序跳表） 并将计划插入位置的所有层前驱放到prev中
  Node* FindGreaterOrEqual(const _KeyType& key, Node** prev) {
    Node* cur = head_;
    //当前有效的最高层
    int32_t level = GetMaxHeight() - 1;
    Node* near_bigger_node = nullptr;
    while (true) {
      // 根据跳表原理，他是从最上层开始，向左或者向下遍历
      Node* next = cur->Next(level);
      if (KeyIsAfterNode(key, next)) {
        // 说明key还在next后面，直接往后next即可
        cur = next;
      } else {
        //说明key在cur和next之间
        if (prev != NULL) {
          //记下每层的前驱，为之后插入做准备
          prev[level] = cur;
        }
        if (level == 0) {
          //已经到底层则返回插入位置的下一个
          return next;
        }
        //进入下一层
        level--;
      }
    }
  }

  // 找到key插入位置的前一个node
  Node* FindLessThan(const _KeyType& key) {
    Node* cur = head_;
    int32_t level = GetMaxHeight() - 1;
    while (true) {
      Node* next = cur->Next(level);
      int32_t cmp = (next == nullptr) ? 1 : comparator_.Compare(next->key, key);
      //刚好next大于等于0
      if (cmp >= 0) {
        // 因为高度是随机生成的，在这里只有level=0才能确定到底是哪个node
        if (level == 0) {
          return cur;
        } else {
          //向下找
          level--;
        }
      } else {
        //在本层向后找
        cur = next;
      }
    }
  }

  //查找最后一个节点的数据（在0层向后遍历）
  Node* FindLast() {
    Node* cur = head_;
    static constexpr uint32_t kBaseLevel = 0;
    while (true) {
      Node* next = cur->Next(kBaseLevel);
      if (nullptr == next) {
        return cur;
      }
      cur = next;
    }
  }

 private:
  _KeyComparator comparator_;  //比较器
  _Allocator arena_;           //内存管理对象
  Node* head_ = nullptr;        //头节点
  std::atomic<int32_t> cur_height_;  //当前有效的层数
  RandomUtil rnd_;  //概率函数
};

// Implementation details follow
//跳表节点定义
template <typename _KeyType, class _KeyComparator, typename _Allocator>
struct SkipList<_KeyType, _KeyComparator, _Allocator>::Node {
  // explicit关键字的作用就是防止类构造函数的隐式自动转换.
  explicit Node(const _KeyType& k) : key(k) {}
  //节点的key值
  const _KeyType key;

  // Accessors/mutators for links.  Wrapped in methods so we can
  // add the appropriate barriers as necessary.
  //返回节点再第n层的后继
  Node* Next(int32_t n) {
    // Use an 'acquire load' so that we observe a fully initialized
    // version of the returned Node.
    return next_[n].load(std::memory_order_acquire);
  }
  //设置节点在第n层的后继为x
  void SetNext(int n, Node* x) {
    assert(n >= 0);
    // Use a 'release store' so that anybody who reads through this
    // pointer observes a fully initialized version of the inserted node.
    next_[n].store(x, std::memory_order_release);
  }

  // No-barrier variants that can be safely used in a few locations.
  Node* NoBarrier_Next(int n) {
    return next_[n].load(std::memory_order_relaxed);
  }
  void NoBarrier_SetNext(int n, Node* x) {
    next_[n].store(x, std::memory_order_relaxed);
  }

 private:
  // Array of length equal to the node height.  next_[0] is lowest level link.
  //节点的后继列表
  std::atomic<Node*> next_[1];
};

//跳表的构造函数，跳表默认为0层，出现高层节点插入的情况时才会进行扩充
template <typename _KeyType, class _Comparator, typename _Allocator>
SkipList<_KeyType, _Comparator, _Allocator>::SkipList(_Comparator cmp)
    : comparator_(cmp),//自定义排序规则
      cur_height_(1),//当前层数
      head_(NewNode(0, SkipListOption::kMaxHeight)) {//头节点
  for (int i = 0; i < SkipListOption::kMaxHeight; i++) {
    head_->SetNext(i, nullptr);
  }
}

//根据key和所在层数申请内存并构建新建节点，返回节点指针，（节点天然包含层属性）
template <typename _KeyType, typename _Comparator, typename _Allocator>
typename SkipList<_KeyType, _Comparator, _Allocator>::Node*
SkipList<_KeyType, _Comparator, _Allocator>::NewNode(const _KeyType& key,
                                                     int32_t height) {
  char* node_memory = (char*)arena_.Allocate(
      sizeof(Node) + sizeof(std::atomic<Node*>) * (height - 1));
  //定位new写法
  return new (node_memory) Node(key);
}

//返回本次节点要插入第几层，0.5插入0层 0.25插入1层 0.125插入2层...
template <typename _KeyType, typename _Comparator, typename _Allocator>
int32_t SkipList<_KeyType, _Comparator, _Allocator>::RandomHeight() {
  int32_t height = 1;
  while (height < SkipListOption::kMaxHeight &&
         ((rnd_.GetSimpleRandomNum() % SkipListOption::kBranching) == 0)) {
    height++;
  }
  return height;
}

}  // namespace corekv
#endif
