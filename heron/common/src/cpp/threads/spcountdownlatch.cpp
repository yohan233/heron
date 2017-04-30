/*
 * Copyright 2015 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "threads/spcountdownlatch.h"

CountDownLatch::CountDownLatch(sp_uint32 count) : count_(count) {}

CountDownLatch::~CountDownLatch() {}

void CountDownLatch::wait() {
  std::unique_lock<std::mutex> m(mutex_);
  // If count is greater than 0, then wait until it is 0.
  // Else return immediately.
  while (count_ > 0) {
    cond_.wait(m);
  }
}

// if you do not use the default count=0, no notify_all() will be triggered,
// thus you have to wait one more second (hard coded polling interval)
bool CountDownLatch::waitFor(sp_uint32 seconds, sp_uint32 count) {
  std::unique_lock<std::mutex> m(mutex_);
  if (count_ == count) {
    return true;
  }
  // wait until count reached or timeout.
  while (count_ > count && seconds > 0) {
    cond_.wait_for(m, std::chrono::seconds(1));
    seconds--;
  }
  return count_ == count;
}

void CountDownLatch::countDown() {
  std::unique_lock<std::mutex> m(mutex_);

  // Nothing to do if count is already 0
  if (count_ == 0) return;

  // Decrement count. If it is 0 after that, notify all blocked threads.
  if (--count_ == 0) {
    cond_.notify_all();
  }
}

sp_uint32 CountDownLatch::getCount() {
  std::unique_lock<std::mutex> m(mutex_);
  return count_;
}
