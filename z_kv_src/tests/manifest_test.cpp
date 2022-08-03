#include "manifest/manifest.h"

#include <gtest/gtest.h>

#include <iostream>
#include <string>
#include <vector>

#include "logger/log.h"
#include "manifest/manifest_change_edit.h"

using namespace std;
using namespace z_kv;
TEST(manifestTest, Insert) {
  z_kv::LogConfig log_config;
  log_config.log_type = z_kv::LogType::CONSOLE;
  log_config.rotate_size = 100;
  z_kv::Log::GetInstance()->InitLog(log_config);
  ManifestChangeEdit manifest_change_edit;
  ManifestHandler manifest_handler("./");
  manifest_handler.AddTableMeta(1, 1111);
}