#include "status.h"

namespace z_kv {

bool operator==(const DBStatus &x, const DBStatus &y) {
  return x.code == y.code;
}
bool operator!=(const DBStatus &x, const DBStatus &y) {
  return x.code != y.code;
}

}  // namespace corekv
