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

#include "thread_pool_async_manager.h"

#include <fcntl.h>
#include <sys/select.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <map>
#include <mutex>
#include <set>
#include <thread>
#include <vector>

#include "log.h"
#include "shared_thread_pool.h"

#ifndef TEMP_FAILURE_RETRY
/* Used to retry syscalls that can return EINTR. */
#define TEMP_FAILURE_RETRY(exp)            \
  ({                                       \
    __typeof__(exp) _rc;                   \
    do {                                   \
      _rc = (exp);                         \
    } while (_rc == -1 && errno == EINTR); \
    _rc;                                   \
  })
#endif  // TEMP_FAILURE_RETRY

namespace rootcanal {

static const uint16_t kMaxTaskId = -1; /* 2^16 - 1, permisible ids are {1..2^16-1}*/
static inline AsyncTaskId NextAsyncTaskId(const AsyncTaskId id) {
  return (id == kMaxTaskId) ? 1 : id + 1;
}

static const int kNotificationBufferSize = 10;

// Async File Descriptor Watcher Implementation (unchanged from AsyncManager):
class ThreadPoolAsyncManager::AsyncFdWatcher {
public:
  int WatchFdForNonBlockingReads(int file_descriptor,
                                 const ReadCallback& on_read_fd_ready_callback) {
    // add file descriptor and callback
    {
      std::unique_lock<std::recursive_mutex> guard(internal_mutex_);
      watched_shared_fds_[file_descriptor] = on_read_fd_ready_callback;
    }

    // start the thread if not started yet
    int started = tryStartThread();
    if (started != 0) {
      ERROR("{}: Unable to start thread", __func__);
      return started;
    }

    // notify the thread so that it knows of the new FD
    notifyThread();

    return 0;
  }

  void StopWatchingFileDescriptor(int file_descriptor) {
    std::unique_lock<std::recursive_mutex> guard(internal_mutex_);
    watched_shared_fds_.erase(file_descriptor);
  }

  AsyncFdWatcher() = default;
  AsyncFdWatcher(const AsyncFdWatcher&) = delete;
  AsyncFdWatcher& operator=(const AsyncFdWatcher&) = delete;

  ~AsyncFdWatcher() = default;

  int stopThread() {
    if (!std::atomic_exchange(&running_, false)) {
      return 0;  // if not running already
    }

    notifyThread();

    if (std::this_thread::get_id() != thread_.get_id()) {
      thread_.join();
    } else {
      WARNING("{}: Starting thread stop from inside the reading thread itself", __func__);
    }

    {
      std::unique_lock<std::recursive_mutex> guard(internal_mutex_);
      watched_shared_fds_.clear();
    }

    return 0;
  }

private:
  int tryStartThread() {
    if (std::atomic_exchange(&running_, true)) {
      return 0;  // if already running
    }
    // set up the communication channel
    int pipe_fds[2];
    if (pipe(pipe_fds)) {
      ERROR("{}: Unable to establish a communication channel to the reading "
            "thread",
            __func__);
      return -1;
    }
    // configure the fds as non blocking.
    if (fcntl(pipe_fds[0], F_SETFL, O_NONBLOCK) || fcntl(pipe_fds[1], F_SETFL, O_NONBLOCK)) {
      ERROR("{}: Unable to configure the communication channel to the reading "
            "thread",
            __func__);
      return -1;
    }

    notification_listen_fd_ = pipe_fds[0];
    notification_write_fd_ = pipe_fds[1];

    thread_ = std::thread([this]() { ThreadRoutine(); });
    if (!thread_.joinable()) {
      ERROR("{}: Unable to start reading thread", __func__);
      return -1;
    }
    return 0;
  }

  int notifyThread() const {
    char buffer = '0';
    if (TEMP_FAILURE_RETRY(write(notification_write_fd_, &buffer, 1)) < 0) {
      ERROR("{}: Unable to send message to reading thread", __func__);
      return -1;
    }
    return 0;
  }

  int setUpFileDescriptorSet(fd_set& read_fds) {
    // add comm channel to the set
    FD_SET(notification_listen_fd_, &read_fds);
    int nfds = notification_listen_fd_;

    // add watched FDs to the set
    {
      std::unique_lock<std::recursive_mutex> guard(internal_mutex_);
      for (auto& fdp : watched_shared_fds_) {
        FD_SET(fdp.first, &read_fds);
        nfds = std::max(fdp.first, nfds);
      }
    }
    return nfds;
  }

  bool consumeThreadNotifications(fd_set& read_fds) const {
    if (FD_ISSET(notification_listen_fd_, &read_fds)) {
      char buffer[kNotificationBufferSize];
      while (TEMP_FAILURE_RETRY(read(notification_listen_fd_, buffer, kNotificationBufferSize)) ==
             kNotificationBufferSize) {
      }
      return true;
    }
    return false;
  }

  void runAppropriateCallbacks(fd_set& read_fds) {
    std::vector<decltype(watched_shared_fds_)::value_type> fds;
    std::unique_lock<std::recursive_mutex> guard(internal_mutex_);
    for (auto& fdc : watched_shared_fds_) {
      if (FD_ISSET(fdc.first, &read_fds)) {
        fds.push_back(fdc);
      }
    }
    for (auto& p : fds) {
      p.second(p.first);
    }
  }

  void ThreadRoutine() {
    while (running_) {
      fd_set read_fds;
      FD_ZERO(&read_fds);
      int nfds = setUpFileDescriptorSet(read_fds);

      // wait until there is data available to read on some FD
      int retval = select(nfds + 1, &read_fds, NULL, NULL, NULL);
      if (retval <= 0) {  // there was some error or a timeout
        ERROR("{}: There was an error while waiting for data on the file "
              "descriptors: {}",
              __func__, strerror(errno));
        continue;
      }

      consumeThreadNotifications(read_fds);

      // Do not read if there was a call to stop running
      if (!running_) {
        break;
      }

      runAppropriateCallbacks(read_fds);
    }
  }

  std::atomic_bool running_{false};
  std::thread thread_;
  std::recursive_mutex internal_mutex_;

  std::map<int, ReadCallback> watched_shared_fds_;

  // A pair of FD to send information to the reading thread
  int notification_listen_fd_{};
  int notification_write_fd_{};
};

// Thread Pool Task Manager Implementation:
class ThreadPoolAsyncManager::ThreadPoolTaskManager {
public:
  AsyncUserId GetNextUserId() { return lastUserId_++; }

  AsyncTaskId ExecAsync(AsyncUserId user_id, std::chrono::milliseconds delay,
                        const TaskCallback& callback) {
    return scheduleTask(
            std::make_shared<Task>(std::chrono::steady_clock::now() + delay, callback, user_id));
  }

  AsyncTaskId ExecAsyncPeriodically(AsyncUserId user_id, std::chrono::milliseconds delay,
                                    std::chrono::milliseconds period,
                                    const TaskCallback& callback) {
    return scheduleTask(std::make_shared<Task>(std::chrono::steady_clock::now() + delay, period,
                                               callback, user_id));
  }

  bool CancelAsyncTask(AsyncTaskId async_task_id) {
    std::unique_lock<std::mutex> guard(internal_mutex_);
    return cancel_task_with_lock_held(async_task_id);
  }

  bool CancelAsyncTasksFromUser(AsyncUserId user_id) {
    std::unique_lock<std::mutex> guard(internal_mutex_);
    if (tasks_by_user_id_.count(user_id) == 0) {
      return false;
    }
    for (auto task : tasks_by_user_id_[user_id]) {
      cancel_task_with_lock_held(task);
    }
    tasks_by_user_id_.erase(user_id);
    return true;
  }

  void Synchronize(const CriticalCallback& critical) {
    std::unique_lock<std::mutex> guard(synchronization_mutex_);
    critical();
  }

  ThreadPoolTaskManager() = default;
  ThreadPoolTaskManager(const ThreadPoolTaskManager&) = delete;
  ThreadPoolTaskManager& operator=(const ThreadPoolTaskManager&) = delete;

  ~ThreadPoolTaskManager() = default;

  int stopThread() {
    {
      std::unique_lock<std::mutex> guard(internal_mutex_);
      tasks_by_id_.clear();
      task_queue_.clear();
      if (!running_) {
        return 0;
      }
      running_ = false;
      internal_cond_var_.notify_one();
    }
    if (std::this_thread::get_id() != thread_.get_id()) {
      thread_.join();
    } else {
      WARNING("{}: Starting thread stop from inside the task thread itself", __func__);
    }
    return 0;
  }

private:
  class Task {
  public:
    Task(std::chrono::steady_clock::time_point time, std::chrono::milliseconds period,
         const TaskCallback& callback, AsyncUserId user)
        : time(time),
          periodic(true),
          period(period),
          callback(callback),
          task_id(kInvalidTaskId),
          user_id(user) {}
    Task(std::chrono::steady_clock::time_point time, const TaskCallback& callback, AsyncUserId user)
        : time(time), periodic(false), callback(callback), task_id(kInvalidTaskId), user_id(user) {}

    bool operator<(const Task& another) const {
      return std::make_pair(time, task_id) < std::make_pair(another.time, another.task_id);
    }

    bool isPeriodic() const { return periodic; }

    std::chrono::steady_clock::time_point time;
    bool periodic;
    std::chrono::milliseconds period{};
    std::mutex in_callback;
    TaskCallback callback;
    AsyncTaskId task_id;
    AsyncUserId user_id;
  };

  struct task_p_comparator {
    bool operator()(const std::shared_ptr<Task>& t1, const std::shared_ptr<Task>& t2) const {
      return *t1 < *t2;
    }
  };

  bool cancel_task_with_lock_held(AsyncTaskId async_task_id) {
    if (tasks_by_id_.count(async_task_id) == 0) {
      return false;
    }

    if (thread_.get_id() != std::this_thread::get_id()) {
      auto task = tasks_by_id_[async_task_id];
      const std::lock_guard<std::mutex> lock(task->in_callback);
      task_queue_.erase(task);
      tasks_by_id_.erase(async_task_id);
    } else {
      task_queue_.erase(tasks_by_id_[async_task_id]);
      tasks_by_id_.erase(async_task_id);
    }

    return true;
  }

  AsyncTaskId scheduleTask(const std::shared_ptr<Task>& task) {
    {
      std::unique_lock<std::mutex> guard(internal_mutex_);
      if (tasks_by_id_.size() == kMaxTaskId) {
        return kInvalidTaskId;
      }
      do {
        lastTaskId_ = NextAsyncTaskId(lastTaskId_);
      } while (isTaskIdInUse(lastTaskId_));
      task->task_id = lastTaskId_;
      tasks_by_id_[lastTaskId_] = task;
      tasks_by_user_id_[task->user_id].insert(task->task_id);
      task_queue_.insert(task);
    }
    int started = tryStartThread();
    if (started != 0) {
      ERROR("{}: Unable to start thread", __func__);
      return kInvalidTaskId;
    }
    internal_cond_var_.notify_one();
    return task->task_id;
  }

  bool isTaskIdInUse(const AsyncTaskId& task_id) const { return tasks_by_id_.count(task_id) != 0; }

  int tryStartThread() {
    std::unique_lock<std::mutex> guard(internal_mutex_);
    if (running_) {
      return 0;
    }
    running_ = true;
    thread_ = std::thread([this]() { ThreadRoutine(); });
    if (!thread_.joinable()) {
      ERROR("{}: Unable to start task thread", __func__);
      return -1;
    }
    return 0;
  }

  void ThreadRoutine() {
    auto& thread_pool = SharedAsyncThreadPool::Instance();
    
    while (running_) {
      TaskCallback callback;
      std::shared_ptr<Task> task_p;
      bool run_it = false;
      {
        std::unique_lock<std::mutex> guard(internal_mutex_);
        if (!task_queue_.empty()) {
          task_p = *(task_queue_.begin());
          if (task_p->time < std::chrono::steady_clock::now()) {
            run_it = true;
            callback = task_p->callback;
            task_queue_.erase(task_p);
            if (task_p->isPeriodic()) {
              task_p->time += task_p->period;
              task_queue_.insert(task_p);
            } else {
              tasks_by_user_id_[task_p->user_id].erase(task_p->task_id);
              tasks_by_id_.erase(task_p->task_id);
            }
          }
        }
      }
      
      if (run_it) {
        // Execute callback on shared thread pool instead of current thread
        const std::lock_guard<std::mutex> lock(task_p->in_callback);
        try {
          thread_pool.Submit([this, callback]() {
            Synchronize(callback);
          });
        } catch (const std::exception& e) {
          ERROR("Failed to submit task to thread pool: {}", e.what());
        }
      }
      
      {
        std::unique_lock<std::mutex> guard(internal_mutex_);
        if (!running_) {
          break;
        }
        if (!task_queue_.empty()) {
          std::chrono::steady_clock::time_point time = (*task_queue_.begin())->time;
          internal_cond_var_.wait_until(guard, time);
        } else {
          internal_cond_var_.wait(guard);
        }
      }
    }
  }

  bool running_ = false;
  std::thread thread_;
  std::mutex internal_mutex_;
  std::mutex synchronization_mutex_;
  std::condition_variable internal_cond_var_;

  AsyncTaskId lastTaskId_ = kInvalidTaskId;
  AsyncUserId lastUserId_{1};
  std::map<AsyncTaskId, std::shared_ptr<Task>> tasks_by_id_;
  std::map<AsyncUserId, std::set<AsyncTaskId>> tasks_by_user_id_;
  std::set<std::shared_ptr<Task>, task_p_comparator> task_queue_;
};

// ThreadPoolAsyncManager Implementation:
ThreadPoolAsyncManager::ThreadPoolAsyncManager()
    : fd_watcher_(std::make_unique<AsyncFdWatcher>()),
      task_manager_(std::make_unique<ThreadPoolTaskManager>()) {}

ThreadPoolAsyncManager::~ThreadPoolAsyncManager() {
  // Make sure threads are stopped before destroying the object
  fd_watcher_->stopThread();
  task_manager_->stopThread();
}

int ThreadPoolAsyncManager::WatchFdForNonBlockingReads(int file_descriptor,
                                                      const ReadCallback& on_read_fd_ready_callback) {
  return fd_watcher_->WatchFdForNonBlockingReads(file_descriptor, on_read_fd_ready_callback);
}

void ThreadPoolAsyncManager::StopWatchingFileDescriptor(int file_descriptor) {
  fd_watcher_->StopWatchingFileDescriptor(file_descriptor);
}

AsyncUserId ThreadPoolAsyncManager::GetNextUserId() { 
  return task_manager_->GetNextUserId(); 
}

AsyncTaskId ThreadPoolAsyncManager::ExecAsync(AsyncUserId user_id, std::chrono::milliseconds delay,
                                             const TaskCallback& callback) {
  return task_manager_->ExecAsync(user_id, delay, callback);
}

AsyncTaskId ThreadPoolAsyncManager::ExecAsyncPeriodically(AsyncUserId user_id,
                                                         std::chrono::milliseconds delay,
                                                         std::chrono::milliseconds period,
                                                         const TaskCallback& callback) {
  return task_manager_->ExecAsyncPeriodically(user_id, delay, period, callback);
}

bool ThreadPoolAsyncManager::CancelAsyncTask(AsyncTaskId async_task_id) {
  return task_manager_->CancelAsyncTask(async_task_id);
}

bool ThreadPoolAsyncManager::CancelAsyncTasksFromUser(AsyncUserId user_id) {
  return task_manager_->CancelAsyncTasksFromUser(user_id);
}

void ThreadPoolAsyncManager::Synchronize(const CriticalCallback& critical) {
  task_manager_->Synchronize(critical);
}

}  // namespace rootcanal
