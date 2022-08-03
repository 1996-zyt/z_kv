#ifndef LOGGER_LOG_H_
#define LOGGER_LOG_H_
#include <memory>

#include "log_appender.h"

namespace z_kv {
class Log final {
 public:
  static Log *GetInstance() {
    static Log g_log;
    return &g_log;
  }
  void InitLog(const LogConfig &log_config);

  void LogV(LogLevel log_level, const char *fmt, ...);
 private:
  LogConfig log_config_;
  bool inited_ = false;
  std::unique_ptr<LogAppender> log_appender_ = nullptr;
};

}  // namespace corekv
#define LOG(level, format, args...)                                        \
  z_kv::Log::GetInstance()->LogV(level, "%s(%d): " format, __FILE__, \
                                   __LINE__, ##args)
#endif