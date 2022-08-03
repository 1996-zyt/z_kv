#pragma once
#include <string>
#include <string_view>
#include <vector>

#include "../db/options.h"
#include "../filter/filter_policy.h"
namespace z_kv {
class DataBlockBuilder final {
 public:
  DataBlockBuilder(const Options* options);
  void Add(const std::string_view& key, const std::string_view& value);
  void Finish();
  
  // 思考一下这里为什么不是直接buffer.size呢？
  //当前datablock的大小（加上restart数据）
  const uint64_t CurrentSize() {
    return buffer_.size() + restarts_.size() * sizeof(uint32_t) +
           sizeof(uint32_t);
  }
  //返回当前数据string
  const std::string& Data() { return buffer_; }
  //重置data和restar，将datablock落盘后调用
  void Reset() {
      restarts_.clear();
      restarts_.emplace_back(0);
      is_finished_ = false;
      buffer_.clear();
      pre_key_ = "";
      restart_pointer_counter_ = 0;
  }
  private:
  //添加restar
  void AddRestartPointers();
 private:
  
  bool is_finished_ = false;              // 判断本block是否结束了
  const Options* options_;                // sst属性
  std::string buffer_;                    // 目标buffer（数据块）
  std::vector<uint32_t> restarts_;        // 记录片的偏移
  uint32_t restart_pointer_counter_ = 0;  // 记录当前片数据条数，满16条就分片
  std::string pre_key_;                   // 记录前一个key(需要进行深度复制，不能使用string_view)
};
// filter_block_builder的话
class FilterBlockBuilder final {
 public:
  FilterBlockBuilder(const Options& options);
  bool Availabe() { return policy_filter_ != nullptr; }
  void Add(const std::string_view& key);
  void CreateFilter();
  bool MayMatch(const std::string_view& key);
  bool MayMatch(const std::string_view& key,
                const std::string_view& bf_datas);
  const std::string& Data();
  void Finish(); 
 private:
 std::string buffer_;
  std::vector<std::string> datas_;
  FilterPolicy* policy_filter_ = nullptr;
};
}  // namespace corekv
