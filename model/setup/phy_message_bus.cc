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

#include "phy_message_bus.h"

#include "log.h"

namespace rootcanal {

PhyMessageBus::PhyMessageBus() = default;

std::shared_ptr<SPSCQueue<OutgoingPacket>> PhyMessageBus::CreateOutgoingQueue(PhyDevice::Identifier device_id) {
  auto queue = std::make_shared<SPSCQueue<OutgoingPacket>>(1024); // 1KB queue depth
  outgoing_queues_[device_id] = queue;
  return queue;
}

std::shared_ptr<SPSCQueue<IncomingPacket>> PhyMessageBus::CreateIncomingQueue(PhyDevice::Identifier device_id) {
  auto queue = std::make_shared<SPSCQueue<IncomingPacket>>(1024); // 1KB queue depth
  incoming_queues_[device_id] = queue;
  return queue;
}

void PhyMessageBus::RegisterDeviceOnPhy(PhyDevice::Identifier device_id, Phy::Type phy_type) {
  phy_device_map_[phy_type].insert(device_id);
}

void PhyMessageBus::UnregisterDeviceFromPhy(PhyDevice::Identifier device_id, Phy::Type phy_type) {
  auto it = phy_device_map_.find(phy_type);
  if (it != phy_device_map_.end()) {
    it->second.erase(device_id);
    if (it->second.empty()) {
      phy_device_map_.erase(it);
    }
  }
}

bool PhyMessageBus::IsDeviceOnPhy(PhyDevice::Identifier device_id, Phy::Type phy_type) {
  auto it = phy_device_map_.find(phy_type);
  return it != phy_device_map_.end() && 
         it->second.find(device_id) != it->second.end();
}

int8_t PhyMessageBus::ComputeRssi(PhyDevice::Identifier /*sender_id*/, 
                                  PhyDevice::Identifier /*receiver_id*/, 
                                  int8_t /*tx_power*/) {
  // Simple RSSI computation - can be enhanced with distance modeling
  static uint8_t rssi_counter = 0;
  rssi_counter = (rssi_counter + 5) % 128;
  return static_cast<int8_t>(-rssi_counter);
}

void PhyMessageBus::RoutePackets() {
  // Collect all outgoing packets from all devices
  std::vector<OutgoingPacket> packets_to_route;
  
  for (auto& [device_id, queue] : outgoing_queues_) {
    OutgoingPacket packet;
    while (queue->try_dequeue(packet)) {
      packets_to_route.push_back(packet);
    }
  }
  
  // Route each packet to appropriate receiving devices
  for (const auto& packet : packets_to_route) {
    RoutePacketToReceivers(packet);
  }
}

void PhyMessageBus::RoutePacketToReceivers(const OutgoingPacket& packet) {
  // Find all devices on the same Phy that should receive this packet
  auto phy_devices_it = phy_device_map_.find(packet.phy_type);
  if (phy_devices_it == phy_device_map_.end()) {
    return; // No devices on this Phy type
  }
  
  for (PhyDevice::Identifier receiver_id : phy_devices_it->second) {
    // Don't send packet back to sender
    if (receiver_id == packet.sender_id) {
      continue;
    }
    
    // Find the incoming queue for this receiver
    auto incoming_queue_it = incoming_queues_.find(receiver_id);
    if (incoming_queue_it == incoming_queues_.end()) {
      continue; // No incoming queue for this device
    }
    
    IncomingPacket incoming{
      .phy_type = packet.phy_type,
      .rssi = ComputeRssi(packet.sender_id, receiver_id, packet.tx_power),
      .data = packet.packet_data
    };
    
    if (!incoming_queue_it->second->enqueue(incoming)) {
      WARNING("Failed to enqueue packet for device {}, queue full", receiver_id);
    }
  }
}

}  // namespace rootcanal
