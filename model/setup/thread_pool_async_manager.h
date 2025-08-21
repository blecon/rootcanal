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

#ifndef ROOTCANAL_THREAD_POOL_ASYNC_MANAGER_H_
#define ROOTCANAL_THREAD_POOL_ASYNC_MANAGER_H_

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>

#include "async_manager.h"  // Use definitions from original AsyncManager

namespace rootcanal {

// AsyncManager that uses a shared thread pool for task execution
// while maintaining per-instance file descriptor watching.
// This provides the same interface as AsyncManager but with better
// resource utilization when many instances are created.
class ThreadPoolAsyncManager {
public:
  // File descriptor watching (still per-instance with dedicated thread)
  int WatchFdForNonBlockingReads(int file_descriptor,
                                 const ReadCallback& on_read_fd_ready_callback);
  void StopWatchingFileDescriptor(int file_descriptor);

  // Task scheduling (uses shared thread pool for execution)
  AsyncUserId GetNextUserId();
  AsyncTaskId ExecAsync(AsyncUserId user_id, std::chrono::milliseconds delay,
                        const TaskCallback& callback);
  AsyncTaskId ExecAsyncPeriodically(AsyncUserId user_id, std::chrono::milliseconds delay,
                                    std::chrono::milliseconds period, const TaskCallback& callback);

  // Task cancellation
  bool CancelAsyncTask(AsyncTaskId async_task_id);
  bool CancelAsyncTasksFromUser(AsyncUserId user_id);

  // Synchronized execution
  void Synchronize(const CriticalCallback& critical_callback);

  ThreadPoolAsyncManager();
  ThreadPoolAsyncManager(const ThreadPoolAsyncManager&) = delete;
  ThreadPoolAsyncManager& operator=(const ThreadPoolAsyncManager&) = delete;
  ~ThreadPoolAsyncManager();

private:
  // Forward declaration of internal implementation classes
  class AsyncFdWatcher;
  class ThreadPoolTaskManager;

  // FD watching still uses per-instance thread (since select() is blocking/stateful)
  std::unique_ptr<AsyncFdWatcher> fd_watcher_;
  
  // Task management uses shared thread pool for execution
  std::unique_ptr<ThreadPoolTaskManager> task_manager_;
};

}  // namespace rootcanal

#endif  // ROOTCANAL_THREAD_POOL_ASYNC_MANAGER_H_
