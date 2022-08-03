#pragma once
#include <string_view>
#include <string>
namespace z_kv {
class DataDebugIterator final {
 public:
  std::string ParseData(const std::string_view&st);
};
}  // namespace  corekv
