#pragma once
#include <functional>

//缓存节点的结构定义
namespace z_kv {
template <typename KeyType, typename ValueType>
struct CacheNode {
  // key的话我们保证他是深度复制的
  KeyType key;
  ValueType* value;
  // 引用计数，
  uint32_t refs = 0;
  uint32_t hash = 0;
  // 是否在缓存中
  bool in_cache = false;
  // 最近一次更新的时间
  uint64_t last_access_time = 0;
  // 有效周期
  uint64_t ttl = 0;
};

}  // namespace corekv
