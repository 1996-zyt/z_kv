#pragma once
//锁工具
#include <pthread.h>

namespace z_kv {

template <class T>
//锁的智能代理 构建时加锁，析构时解锁（防止忘解锁）
struct ScopedLockImpl {
 public:
  ScopedLockImpl(T& mutex) : mutex_(mutex) {
    mutex_.Lock();
    locked_ = true;
  }
  ~ScopedLockImpl() { UnLock(); }
  void Lock() {
    if (!locked_) {
      mutex_.Lock();
      locked_ = true;
    }
  }
  void UnLock() {
    if (locked_) {
      mutex_.UnLock();
      locked_ = false;
    }
  }

 private:
  T& mutex_;
  bool locked_ = false;
};


//无锁
class NullLock final {
 public:
  NullLock() = default;
  ~NullLock() = default;
  void Lock() {}
  void UnLock() {}
};
// 互斥锁
class MutexLock final {
 public:
  MutexLock() { pthread_mutex_init(&mutex_, NULL); }
  ~MutexLock() { pthread_mutex_destroy(&mutex_); }
  void Lock() { pthread_mutex_lock(&mutex_); }
  void UnLock() { pthread_mutex_unlock(&mutex_); }

 private:
  pthread_mutex_t mutex_;
};
//自旋锁
class SpinLock final {
#ifndef __APPLE__
  SpinLock() { pthread_spin_init(&spin_lock_, NULL); }
  ~SpinLock() { pthread_spin_destroy(&spin_lock_); }
  void Lock() { pthread_spin_lock(&spin_lock_); }
  void UnLock() { pthread_spin_unlock(&spin_lock_); }

 private:
  pthread_spinlock_t spin_lock_;
#endif
};

}  // namespace corekv
