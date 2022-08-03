#include "alloc.h"

#include <stdlib.h>
#include <iostream>
#include <cassert>
namespace z_kv {
//释放二级内存，最后统一释放
SimpleFreeListAlloc::~SimpleFreeListAlloc() {
  if(!free_list_start_pos_) {
    FreeList* p = (FreeList*)free_list_start_pos_;
    while(p) {
      FreeList* next = p->next;
      free(p);
      p = next;
    }
  }
}
//根据要分匹配的大小确定要从哪个槽分配
int32_t SimpleFreeListAlloc::M_FreelistIndex(int32_t bytes) {
  // first fit策略,
  //方法巧妙
  return (bytes + kAlignBytes - 1) / kAlignBytes - 1;
}
//按八的倍数向上取整
int32_t SimpleFreeListAlloc::M_Roundup(int32_t bytes) {
  
  return (bytes + kAlignBytes - 1) & ~(kAlignBytes - 1);
}
//从二级内存中分配nobjs个bytes大小的空间，如果空间不够就对其进行扩充（扩充内存池）
char* SimpleFreeListAlloc::M_ChunkAlloc(int32_t bytes, int32_t& nobjs) {
  char* result = nullptr;
  //总共需要扩充的字节大小
  uint32_t total_bytes = bytes * nobjs;
  //当前可用的内存(这里需要注意一下，两个内存之间相减是有意义的)
  //free_list_end_pos_-->可用空间的终止位置， free_list_start_pos_-->可用空间的起始位置
  uint32_t bytes_left = free_list_end_pos_ - free_list_start_pos_;

  if (bytes_left >= total_bytes) {
    // 剩余空间满足，那么此时直接使用剩余的空间
    result = free_list_start_pos_;
    //这里需要进行更新，因为部分有一部分给了链表内存管理
    free_list_start_pos_ += total_bytes;
    memory_usage_.fetch_add(total_bytes, std::memory_order_relaxed);
    return result;
  } else if (bytes_left >= bytes) {
    // 内存池剩余空间不能完全满足需求量，但足够供应一个以上的块
    //这里需要更新实际分配的个数nobjs
    nobjs = bytes_left / bytes;
    total_bytes = bytes * nobjs;
    result = free_list_start_pos_;
    free_list_start_pos_ += total_bytes;
    memory_usage_.fetch_add(total_bytes, std::memory_order_relaxed);
    return result;
  } else {
    //内存池一个区块也无法分配时，就对其进行扩充，扩充大小为两倍的申请大小
    //如果分配失败，尝试把已经挂到空闲列表中的比需求块大的块再摘下来重新放入内存池
    //在这里又分配了2倍，见uint32_t total_bytes = bytes * nobjs;
    int32_t bytes_to_get = 2 * total_bytes + M_Roundup(heap_size_ >> 4);
    //头插法将边界剩余的零碎内存挂接到对应空闲链表
    if (bytes_left > 0) {
      // 内存池中还有剩余，先配给适当的freelist，否则这部分会浪费掉
      FreeList* volatile* cur_free_list =
          freelist_ + M_FreelistIndex(bytes_left);
      // 调整freelist，将内存池剩余空间编入
      ((FreeList*)free_list_start_pos_)->next = *cur_free_list;
      *cur_free_list = ((FreeList*)free_list_start_pos_);
    }

    // 分配新的空间
    free_list_start_pos_ = (char*)malloc(bytes_to_get);
    //如果分配失败，尝试把已经挂到空闲列表中的比需求块大的块再摘下来重新放入内存池
    if (!free_list_start_pos_) {
      FreeList *volatile *my_free_list = nullptr, *p = nullptr;
      // 尝试从freelist中查找是不是有足够大的没有用过的区块
      for (int32_t index = bytes; index <= kSmallObjectBytes;
           index += kAlignBytes) {
        my_free_list = freelist_ + M_FreelistIndex(index);
        p = *my_free_list;
        if (p) {
          //说明找到了，此时在进行下一轮循环时候，我们就能直接返回
          *my_free_list = p->next;
          free_list_start_pos_ = (char*)p;
          free_list_end_pos_ = free_list_start_pos_ + index;
          return M_ChunkAlloc(bytes, nobjs);
        }
      }
      //两个方法均失败时，我们再重新尝试分配一次，如果分配还失败，此时将终止程序
      free_list_end_pos_ = nullptr;
      free_list_start_pos_ = (char*)malloc(bytes_to_get);
      if (!free_list_start_pos_) {
        exit(1);
      }
    }
    heap_size_ += bytes_to_get;
    free_list_end_pos_ = free_list_start_pos_ + bytes_to_get;
    memory_usage_.fetch_add(bytes_to_get, std::memory_order_relaxed);
    return M_ChunkAlloc(bytes, nobjs);
  }
}
//把二级内存中获取大小10*bytes的空间挂接到对应的空闲链表（扩充空闲链表）
void* SimpleFreeListAlloc::M_Refill(int32_t bytes) {
  //默认一次先分配10个block，分配太多，导致浪费，太少可能不够用
  static const int32_t kInitBlockCount = 10;  //一次先分配10个，STL默认是20个
  int32_t real_block_count = kInitBlockCount;  //初始化，先按理想值来分配
  char* address = M_ChunkAlloc(bytes, real_block_count);
  do {
    //当前内存池刚好只够分配一个block块时
    if (real_block_count == 1) {
      break;
    }
    FreeList* next = nullptr;
    FreeList* cur = nullptr;
    FreeList* new_free_list = nullptr;
    //我们将第一个给申请者，剩下的9个放到对应的链表上，但是我们在M_ChunkAlloc
    //函数中，分配的是2倍，因此剩下的10个放在内存池中，供我们使用
    new_free_list = next = reinterpret_cast<FreeList*>(address + bytes);
    freelist_[M_FreelistIndex(bytes)] = new_free_list;
    //挂接过程
    for (uint32_t index = 1;; ++index) {
      cur = next;
      next = (FreeList*)((char*)next + bytes);  //下一个块的首地址
      if (index != real_block_count - 1) {
        cur->next = next;
      } else {
        cur->next = nullptr;
        break;
      }
    }
  } while (0);
  FreeList* result = reinterpret_cast<FreeList*>(address);
  return result;
}
//分配n个字节的连续内存，先从空闲列表中取，空闲列表中没有就使用二级内存扩充空闲列表（申请空间）
void* SimpleFreeListAlloc::Allocate(int32_t n) {
  assert(n > 0);
  //如果超过4kb，此时直接使用glibc自带的malloc，因为我们假设大多数时候都是小对象
  //针对大对象，可能会存在大key，在上层我们就应该尽可能的规避
  if (n > kSmallObjectBytes) {
    memory_usage_.fetch_add(n, std::memory_order_relaxed);
    return (char*)malloc(n);
  }
  FreeList** select_free_list = nullptr;
  //根据对象大小，定位需要从空闲列表哪个slot进行分配，对应内存分配策略其实最佳适配原则
  select_free_list = freelist_ + M_FreelistIndex(n);
  FreeList* result = *select_free_list;
  //默认情况下，我们的slot不能提前分配内存，因为他为空
  if (!result) {
    //如果为空，此时我们需要分配内存来进行填充
    void* ret = (char*)M_Refill(M_Roundup(n));
    return ret;
  }
  //更新下一个可用
  *select_free_list = result->next;
  return result;
}
//释放空间-->即把变量占的内存头插到空闲列表
void SimpleFreeListAlloc::Deallocate(void* address, int32_t n) {
  if (address) {
    FreeList* p = (FreeList*)address;
    FreeList* volatile* cur_free_list = nullptr;
    memory_usage_.fetch_sub(n, std::memory_order_relaxed);
    if (n > kSmallObjectBytes) {
      free(address);

      address = nullptr;
      return;
    }
    //可用内存挂在最前端
    cur_free_list = freelist_ + M_FreelistIndex(n);
    p->next = *cur_free_list;
    *cur_free_list = p;
  }
}
//更换持有的内存大小
void* SimpleFreeListAlloc::Reallocate(void* address, int32_t old_size,
                                      int32_t new_size) {
  Deallocate(address, old_size);
  address = Allocate(new_size);
  return address;
}

}  // namespace corekv