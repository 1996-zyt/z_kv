#pragma once
#include <stdint.h>
namespace z_kv {
namespace hash_util {
uint32_t SimMurMurHash(const char *data, uint32_t len);
}  // namespace hash_utl

}  // namespace corekv
