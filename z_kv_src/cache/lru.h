#pragma once
#include <functional>
#include <list>
#include <string>
#include <type_traits>
#include <unordered_map>

#include "../utils/hash_util.h"
#include "../utils/mutex.h"
#include "../utils/util.h"
#include "cache_node.h"
#include "cache_policy.h"

//具体的缓存策略，对抽象策略进行继承实现

namespace z_kv {
template <typename KeyType, typename ValueType, typename LockType = NullLock>
class LruCachePolicy final : public CachePolicy<KeyType, ValueType> {
  using ListIter = typename std::list<CacheNode<KeyType, ValueType>*>::iterator;

 public:
  LruCachePolicy(uint32_t capacity) : capacity_(capacity) {}
  // 其实走到这里，说明整个程序都结束了,系统负责释放就行
  ~LruCachePolicy() {
    for (auto it = nodes_.begin(); it != nodes_.end(); it++) {
      // Unref(it);
    }
  }
  
  //在缓存中插入数据
  void Insert(const KeyType& key, ValueType* value, uint32_t ttl = 0) {
    //加锁
    LockType lock_type;
    ScopedLockImpl<LockType> lock_guard(lock_type);
    //定义新节点
    CacheNode<KeyType, ValueType>* new_node =
        new CacheNode<KeyType, ValueType>();
    //生成hash
    new_node->hash = std::hash<KeyType>{}(key);
    new_node->key = key;
    new_node->value = value;
    new_node->in_cache = true;
    new_node->refs = 1;
    new_node->ttl = ttl;
    //lfu(备用)
    if (ttl > 0) {
      new_node->last_access_time = util::GetCurrentTime();
    }
    //定义一个迭代器，用于访问map缓存容器
    typename std::unordered_map<
        KeyType,
        typename std::list<CacheNode<KeyType, ValueType>*>::iterator>::iterator
        iter = index_.find(key);
    //首先判断是否有相同的key在缓存中
    if (iter == index_.end()) {
      //如果缓存满了，淘汰最后一个
      if (nodes_.size() == capacity_) {
        CacheNode<KeyType, ValueType>* node = nodes_.back();
        //两个缓存容器中都要删除
        index_.erase(node->key);
        nodes_.pop_back();
        FinishErase(node);
      }
      //然后将其加到第一个位置
      nodes_.push_front(new_node);
      index_[key] = nodes_.begin();
    } else {
      //说明已经存在有新的值
      //更新节点的值，并将其加到第一个位置
      FinishErase(*(iter->second));
      //将nodes_中迭代器index_[key]位置上的元素拼接到,nodes_中nodes_.begin()之前
      nodes_.splice(nodes_.begin(), nodes_, index_[key]);
      index_[key] = nodes_.begin();
    }
  }

  //从缓存中获取元素，不存在就返回nullptr ，存在就返回，并1.更新其在缓存中位置 2.引用计数+1
  CacheNode<KeyType, ValueType>* Get(const KeyType& key) {
    LockType lock_type;
    ScopedLockImpl<LockType> lock_guard(lock_type);
    typename std::unordered_map<
        KeyType,
        typename std::list<CacheNode<KeyType, ValueType>*>::iterator>::iterator
        iter = index_.find(key);
    //  不存在就返回nullptr  
    if (iter == index_.end()) {
      return nullptr;
    }

    CacheNode<KeyType, ValueType>* node = *(iter->second);
    nodes_.erase(iter->second);
    //存在就把所查node移动到头部，并更新map中的迭代器
    nodes_.push_front(node);
    index_[node->key] = nodes_.begin();
    Ref(node);
    return node;
  }

  // 注册用于释放key 和value的函数
  void RegistCleanHandle(
      std::function<void(const KeyType& key, ValueType* value)> destructor) {
    destructor_ = destructor;
  }

  //引用计数-1，持有者不再不使用node时调用
  void Release(CacheNode<KeyType, ValueType>* node) {
    LockType lock_type;
    ScopedLockImpl<LockType> lock_guard(lock_type);
    Unref(node);
  }

  // 定期的来进行回收（针对待释放容器中的node）
  void Prune() {
    LockType lock_type;
    ScopedLockImpl<LockType> lock_guard(lock_type);
    for (auto it = wait_erase_.begin(); it != wait_erase_.end(); ++it) {
      Unref((it->second));
    }
  }

  // 从缓存中删除某个key对应的节点
  void Erase(const KeyType& key) {
    LockType lock_type;
    ScopedLockImpl<LockType> lock_guard(lock_type);
    typename std::unordered_map<
        KeyType,
        typename std::list<CacheNode<KeyType, ValueType>*>::iterator>::iterator
        iter = index_.find(key);
    if (iter == index_.end()) {
      return;
    }
    CacheNode<KeyType, ValueType>* node = *(iter->second);
    //从user列表中删除该对象
    nodes_.erase(iter->second);
    index_.erase(node->key);
    FinishErase(node);
  }

 private:
  //增加一个引用计数
  void Ref(CacheNode<KeyType, ValueType>* node) {
    if (node) {
      ++node->refs;
    }
  }
  //减少一个引用计数，如果引用计数减成0，则1.调用注册的释放函数，释放节点的key和value。2.把node从待删除容器内移除并释放node，
  void Unref(CacheNode<KeyType, ValueType>* node) {
    if (!node) {
      --node->refs;
      if (node->refs == 0) {
        destructor_(node->key, node->value);
        if (wait_erase_.count(node->key) > 0) {
          wait_erase_.erase(node->key);
        }
        delete node;
        node = nullptr;
      }
    }
  }
  
  //将node移入待释放容器
  void MoveToEraseContainer(CacheNode<KeyType, ValueType>* node) {
    if (wait_erase_.count(node->key) == 0) {
      wait_erase_[node->key] = node;
    }
  }
  
  //将node标记为不在缓存中,放入待删容器,并引用计数减一，这个是不带锁的版本
  void FinishErase(CacheNode<KeyType, ValueType>* node) {
    if (node) {
      node->in_cache = false;
      MoveToEraseContainer(node);
      Unref(node);
    }
  }

 private:
  const uint32_t capacity_;//缓存的容量
  uint32_t cur_size_ = 0;//当前大小
  std::list<CacheNode<KeyType, ValueType>*> nodes_;//双向链表，保存缓存中的node指针，用于表明缓存顺序
  typename std::unordered_map<
      KeyType, typename std::list<CacheNode<KeyType, ValueType>*>::iterator>
      index_;//哈希map，保存key和缓存链表中对应节点迭代器，用于高效查找
  std::unordered_map<KeyType, CacheNode<KeyType, ValueType>*> wait_erase_;//待释放容器,(不在缓存中且任然被其他使用者持有)
  std::function<void(const KeyType& key, ValueType* value)> destructor_;//自定义的 key value释放函数
};
}  // namespace corekv
