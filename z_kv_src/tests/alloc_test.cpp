#include "memory/alloc.h"

#include <gtest/gtest.h>

#include <iostream>

#include "logger/log.h"

using namespace std;

TEST(allocTest, Allocate) {
  int nCount = 4;
  z_kv::SimpleFreeListAlloc simple_freelist_alloc;
  int *a = (int *)simple_freelist_alloc.Allocate(sizeof(int) * nCount);
  for (int i = 0; i < nCount; ++i) {
    a[i] = i;
  }

  std::cout << "a[](" << a << "): ";
  for (int i = 0; i < nCount; ++i) {
    std::cout << a[i] << " ";
  }

  simple_freelist_alloc.Deallocate(a, sizeof(int) * nCount);

  std::cout << std::endl;
  std::cout << "a[](" << a << "): ";
  for (int i = 0; i < nCount; ++i) {
    std::cout << a[i] << " ";
  }
}


TEST(logTest, Log) {
  z_kv::LogConfig log_config;
  log_config.log_path="./tests";
  log_config.log_type = z_kv::LogType::FILE;
  log_config.rotate_size = 100;
  z_kv::Log::GetInstance()->InitLog(log_config);
  LOG(z_kv::LogLevel::ERROR, "name:%s,owner:%s,course:%s", "hardcore","logic,chen","corekv");
}