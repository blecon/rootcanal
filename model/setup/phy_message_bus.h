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

#ifndef ROOTCANAL_PHY_MESSAGE_BUS_H_
#define ROOTCANAL_PHY_MESSAGE_BUS_H_

#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "phy.h"
#include "phy_device.h"

namespace rootcanal {

// Packet being sent from a device to the Phy layer for routing
struct OutgoingPacket {
  PhyDevice::Identifier sender_id;
  Phy::Type phy_type;
  int8_t tx_power;
  std::shared_ptr<std::vector<uint8_t>> packet_data;
};

// Packet being delivered from Phy layer to a device
struct IncomingPacket {
  Phy::Type phy_type;
  int8_t rssi;
  std::shared_ptr<std::vector<uint8_t>> data;
};

// Simple lock-free single-producer single-consumer queue
// Safe for one producer and one consumer without locks
template<typename T>
class SPSCQueue {
public:
  explicit SPSCQueue(size_t capacity) 
    : capacity_(capacity), buffer_(capacity + 1) {}
  
  bool enqueue(const T& item) {
    size_t next_write = (write_pos_ + 1) % buffer_.size();
    if (next_write == read_pos_.load(std::memory_order_acquire)) {
      return false; // Queue is full
    }
    buffer_[write_pos_] = item;
    write_pos_.store(next_write, std::memory_order_release);
    return true;
  }
  
  bool try_dequeue(T& item) {
    size_t current_read = read_pos_.load(std::memory_order_relaxed);
    if (current_read == write_pos_.load(std::memory_order_acquire)) {
      return false; // Queue is empty
    }
    item = buffer_[current_read];
    read_pos_.store((current_read + 1) % buffer_.size(), std::memory_order_release);
    return true;
  }
  
private:
  size_t capacity_;
  std::vector<T> buffer_;
  std::atomic<size_t> write_pos_{0};
  std::atomic<size_t> read_pos_{0};
};

// Message bus for routing packets between HCI devices and Phy layers
// Uses lock-free queues for high-performance inter-thread communication
class PhyMessageBus {
public:
  PhyMessageBus();
  ~PhyMessageBus() = default;
  
  // Create queues for a new device
  std::shared_ptr<SPSCQueue<OutgoingPacket>> CreateOutgoingQueue(PhyDevice::Identifier device_id);
  std::shared_ptr<SPSCQueue<IncomingPacket>> CreateIncomingQueue(PhyDevice::Identifier device_id);
  
  // Register a device on a specific Phy type
  void RegisterDeviceOnPhy(PhyDevice::Identifier device_id, Phy::Type phy_type);
  void UnregisterDeviceFromPhy(PhyDevice::Identifier device_id, Phy::Type phy_type);
  
  // Route packets from outgoing queues to incoming queues
  void RoutePackets();
  
private:
  // Check if a device is registered on a specific Phy
  bool IsDeviceOnPhy(PhyDevice::Identifier device_id, Phy::Type phy_type);
  
  // Compute RSSI between two devices
  int8_t ComputeRssi(PhyDevice::Identifier sender_id, PhyDevice::Identifier receiver_id, int8_t tx_power);
  
  // Route a single packet to all appropriate receivers
  void RoutePacketToReceivers(const OutgoingPacket& packet);
  
  std::unordered_map<PhyDevice::Identifier, 
                     std::shared_ptr<SPSCQueue<OutgoingPacket>>> outgoing_queues_;
  std::unordered_map<PhyDevice::Identifier, 
                     std::shared_ptr<SPSCQueue<IncomingPacket>>> incoming_queues_;
  
  // Track which devices are on which Phy types
  std::unordered_map<Phy::Type, std::unordered_set<PhyDevice::Identifier>> phy_device_map_;
};

}  // namespace rootcanal

#endif  // ROOTCANAL_PHY_MESSAGE_BUS_H_
