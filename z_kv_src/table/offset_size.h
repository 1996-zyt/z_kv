#pragma once
#include <stdint.h>

#include <string>
#include <string_view>

#include "../db/status.h"
namespace z_kv {

//block的位置属性
struct OffSetSize {
  // 记录数据的起点
  uint64_t offset = 0;
  // 记录数据的长度
  uint64_t length = 0;
};
//对block的位置属性进行编解码（位置和偏移）
class OffsetBuilder final {
 public:
  // 对OffSetSize中的起点和长度进行64位定长编码，并追加到output
  void Encode(const OffSetSize& offset_size, std::string& output);
  // 对OffSetSize中的起点和长度进行64位定长解码，并追加到input
  DBStatus Decode(const char* input, OffSetSize& offset_size);
  // 验证编解码的正确性
  std::string DebugString(const OffSetSize& offset_size);
};

}  // namespace corekv
