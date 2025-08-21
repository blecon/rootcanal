/*
 * Copyright 2025 The Android Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef ROOTCANAL_SHARED_THREAD_POOL_H_
#define ROOTCANAL_SHARED_THREAD_POOL_H_

#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace rootcanal {

// Shared thread pool used by all AsyncManager instances in RootCanal.
// This prevents thread explosion when many HCI devices are connected.
// The thread pool size is bounded to CPU core count for optimal performance.
class SharedAsyncThreadPool {
 public:
  static SharedAsyncThreadPool& Instance();
  static void Initialize();
  static void Shutdown();

  void Submit(std::function<void()> task);
  
  ~SharedAsyncThreadPool();

 private:
  SharedAsyncThreadPool();
  SharedAsyncThreadPool(const SharedAsyncThreadPool&) = delete;
  SharedAsyncThreadPool& operator=(const SharedAsyncThreadPool&) = delete;

  void WorkerThread();
  
  static std::unique_ptr<SharedAsyncThreadPool> instance_;
  static std::mutex instance_mutex_;
  
  std::vector<std::thread> worker_threads_;
  std::queue<std::function<void()>> task_queue_;
  std::mutex queue_mutex_;
  std::condition_variable queue_condition_;
  std::atomic<bool> stop_flag_;
};

}  // namespace rootcanal

#endif  // ROOTCANAL_SHARED_THREAD_POOL_H_
