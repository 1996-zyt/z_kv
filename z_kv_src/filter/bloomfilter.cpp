#include "bloomfilter.h"

#include <cmath>

#include "utils/codec.h"
#include "utils/hash_util.h"
//布隆过滤器的长度m
//数据块存放的数据个数n
//哈希函数个数k
//要求哈希碰撞概率p
namespace z_kv {
//构造过程确定了最优k值
//构造（1）：bits_per_key --> m/n的值
BloomFilter::BloomFilter(int32_t bits_per_key) : bits_per_key_(bits_per_key) {
  CalcHashNum();
}
//构造（2）：entries_num --> n  positive --> p
BloomFilter::BloomFilter(int32_t entries_num, float positive) {
  if (entries_num > 0) {
    CalcBloomBitsPerKey(entries_num, positive);
  }
  CalcHashNum();
}
//由m/n确定最优哈希函数个数,并限制在1-30内
void BloomFilter::CalcHashNum() {
  if (bits_per_key_ < 0) {
    bits_per_key_ = 0;
  }
  filter_policy_meta_.hash_num = static_cast<int32_t>(bits_per_key_ *
                            0.69314718056);  // 0.69314718056 =~ ln(2)
  filter_policy_meta_.hash_num = filter_policy_meta_.hash_num < 1 ? 1 : filter_policy_meta_.hash_num;
  filter_policy_meta_.hash_num = filter_policy_meta_.hash_num > 30 ? 30 : filter_policy_meta_.hash_num;
}
//由p和n确定最优布隆过滤器的长度m,并设置m/n的值（bits_per_key_ ）
void BloomFilter::CalcBloomBitsPerKey(int32_t entries_num, float positive) {
  float size = -1 * entries_num * logf(positive) / powf(0.69314718056, 2.0);
  bits_per_key_ = static_cast<int32_t>(ceilf(size / entries_num));
}
//返回布隆过滤器名称
const char* BloomFilter::Name() { return "general_bloomfilter"; }
//为n条数据创建布隆过滤器，
void BloomFilter::CreateFilter(const std::string* keys, int32_t n) {
  if (n <= 0 || !keys) {
    return;
  }
  //计算过滤器大小（字节数bytes和位数bits），bits限制为大于64并且是8的倍数
  int32_t bits = n * bits_per_key_;
  bits = bits < 64 ? 64 : bits;
  const int32_t bytes = (bits + 7) / 8;
  bits = bytes * 8;
  //这里主要是在corekv场景下，可能多个bf共用一个底层bloomfilter_data_对象
  //在这里对bloomfilter_data_进行扩充，并在扩充的位置创建
  const int32_t init_size = bloomfilter_data_.size();
  bloomfilter_data_.resize(init_size + bytes, 0);
  // 转成数组使用起来更方便
  char* array = &(bloomfilter_data_)[init_size];

  //在布隆过滤器上写一
  for (int i = 0; i < n; i++) {
    // Use double-hashing to generate a sequence of hash values.
    // See analysis in [Kirsch,Mitzenmacher 2006].
    //根据key计算一次hash值
    uint32_t hash_val =
        hash_util::SimMurMurHash(keys[i].data(), keys[i].size());
    //计算一个基数，将hash值累加k-1次这个数，进而模拟k个哈希函数产生的hash值
    const uint32_t delta =
        (hash_val >> 17) | (hash_val << 15);  // Rotate right 17 bits
    for (size_t j = 0; j < filter_policy_meta_.hash_num; j++) {
      const uint32_t bitpos = hash_val % bits;
      //位操作赋1
      array[bitpos / 8] |= (1 << (bitpos % 8));
      //累加基数
      hash_val += delta;
    }
  }
}
//查询是否存在（在sst中许多不同块的布隆过滤器连续存放，需要使用start_pos和len指定使用那一块）
bool BloomFilter::MayMatch(const std::string_view& key, int32_t start_pos,
                           int32_t len) {
  if (key.empty() || bloomfilter_data_.empty()) {
    return false;
  }

  // 获取bloomfilter_data_中的目标段并转成char*
  const char* array = bloomfilter_data_.data();
  const size_t total_len = bloomfilter_data_.size();
  if (start_pos >= total_len) {
    return false;
  }
  if (len == 0) {
    len = total_len - start_pos;
  }
  std::string_view  bloom_filter(array + start_pos, len);
  const char* cur_array = bloom_filter.data();
  const int32_t bits = len * 8;
  if (filter_policy_meta_.hash_num > 30) {
    return true;
  }

  //进行hash匹配
  uint32_t hash_val = hash_util::SimMurMurHash(key.data(), key.size());
  const uint32_t delta =
      (hash_val >> 17) | (hash_val << 15);  // Rotate right 17 bits
  for (int32_t j = 0; j < filter_policy_meta_.hash_num; j++) {
    const uint32_t bitpos = hash_val % bits;
    if ((cur_array[bitpos / 8] & (1 << (bitpos % 8))) == 0) {
      return false;
    }
    hash_val += delta;
  }
  return true;
}
//直接传入key和布隆过滤器进行匹配，bf_datas是sst中恢复的数据段，中存放了布隆过滤器和哈希函数个数
bool BloomFilter::MayMatch(const std::string_view& key,
                           const std::string_view& bf_datas) {
  static constexpr uint32_t kFixedSize = 4;
  // 先恢复k_
  const auto& size = bf_datas.size();
  if (size < kFixedSize || key.empty()) {
    return false;
  }
  uint32_t k = util::DecodeFixed32(bf_datas.data() + size - kFixedSize);
  if (k > 30) {
    return true;
  }
  
  const int32_t bits = (size - kFixedSize) * 8;
  std::string_view bloom_filter(bf_datas.data(), size - kFixedSize);
  const char* cur_array = bloom_filter.data();
  uint32_t hash_val = hash_util::SimMurMurHash(key.data(), key.size());
  const uint32_t delta =
      (hash_val >> 17) | (hash_val << 15);  // Rotate right 17 bits
  for (int32_t j = 0; j < k; j++) {
    const uint32_t bitpos = hash_val % bits;
    if ((cur_array[bitpos / 8] & (1 << (bitpos % 8))) == 0) {
      return false;
    }
    hash_val += delta;
  }
  return true;
}
}  // namespace corekv
