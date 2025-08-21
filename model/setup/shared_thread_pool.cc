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

#include "shared_thread_pool.h"

#include <thread>

#include "log.h"

namespace rootcanal {

std::unique_ptr<SharedAsyncThreadPool> SharedAsyncThreadPool::instance_;
std::mutex SharedAsyncThreadPool::instance_mutex_;

SharedAsyncThreadPool& SharedAsyncThreadPool::Instance() {
  std::lock_guard<std::mutex> lock(instance_mutex_);
  if (!instance_) {
    Initialize();
  }
  return *instance_;
}

void SharedAsyncThreadPool::Initialize() {
  // Should be called with instance_mutex_ already held
  size_t num_threads = std::thread::hardware_concurrency();
  if (num_threads == 0) num_threads = 4;  // Fallback
  
  instance_.reset(new SharedAsyncThreadPool());
  
  // Start worker threads
  for (size_t i = 0; i < num_threads; ++i) {
    instance_->worker_threads_.emplace_back([instance = instance_.get()] { 
      instance->WorkerThread(); 
    });
  }
}

void SharedAsyncThreadPool::Shutdown() {
  std::lock_guard<std::mutex> lock(instance_mutex_);
  if (instance_) {
    instance_->stop_flag_ = true;
    instance_->queue_condition_.notify_all();
    
    for (std::thread& worker : instance_->worker_threads_) {
      if (worker.joinable()) {
        worker.join();
      }
    }
    
    instance_.reset();
  }
}

SharedAsyncThreadPool::SharedAsyncThreadPool() : stop_flag_(false) {}

SharedAsyncThreadPool::~SharedAsyncThreadPool() {
  stop_flag_ = true;
  queue_condition_.notify_all();
  
  for (std::thread& worker : worker_threads_) {
    if (worker.joinable()) {
      worker.join();
    }
  }
}

void SharedAsyncThreadPool::Submit(std::function<void()> task) {
  {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    if (stop_flag_) {
      return;  // Don't accept new tasks if shutting down
    }
    task_queue_.push(std::move(task));
  }
  queue_condition_.notify_one();
}

void SharedAsyncThreadPool::WorkerThread() {
  while (true) {
    std::function<void()> task;
    
    {
      std::unique_lock<std::mutex> lock(queue_mutex_);
      queue_condition_.wait(lock, [this] { 
        return stop_flag_ || !task_queue_.empty(); 
      });
      
      if (stop_flag_ && task_queue_.empty()) {
        break;
      }
      
      if (!task_queue_.empty()) {
        task = std::move(task_queue_.front());
        task_queue_.pop();
      }
    }
    
    if (task) {
      try {
        task();
      } catch (...) {
        // Log error but continue running - use simple logging for now
      }
    }
  }
}

}  // namespace rootcanal
