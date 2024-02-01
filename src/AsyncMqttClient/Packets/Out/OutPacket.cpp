#include "OutPacket.hpp"

using AsyncMqttClientInternals::OutPacket;

OutPacket::OutPacket()
  : next(nullptr)
  , timeout(0)
  , noTries(0)
  , _released(true)
  , _packetId(0) {
}

OutPacket::~OutPacket() {
}

bool OutPacket::released() const {
  return _released;
}

bool OutPacket::timedOut(const size_t now_ms) const {
  return (timeout > 0) && (now_ms > timeout);
}

uint8_t OutPacket::packetType() const {
  return data(0)[0] >> 4;
}

uint16_t OutPacket::packetId() const {
  return _packetId;
}

uint8_t OutPacket::qos() const {
  if (packetType() == AsyncMqttClientInternals::PacketType.PUBLISH) {
    return (data()[1] & 0x06) >> 1;
  }
  return 0;
}

void OutPacket::release() {
  _released = true;
}

void OutPacket::setTimeout(const size_t timeout_ms, const size_t now_ms) {
  timeout = timeout_ms + now_ms;
}

uint16_t OutPacket::_nextPacketId = 0;

uint16_t OutPacket::_getNextPacketId() {
  if (++_nextPacketId == 0) {
    ++_nextPacketId;
  }
  return _nextPacketId;
}
