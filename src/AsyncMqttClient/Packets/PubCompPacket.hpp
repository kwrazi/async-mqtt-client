#pragma once

#include "../Callbacks.hpp"
#include "../ParsingInformation.hpp"
#include "Arduino.h"
#include "Packet.hpp"

namespace AsyncMqttClientInternals {
  class PubCompPacket : public Packet {
  public:
    explicit PubCompPacket(ParsingInformation* parsingInformation, OnPubCompInternalCallback callback);
    ~PubCompPacket();

    void parseVariableHeader(char* data, size_t len, size_t* currentBytePosition);
    void parsePayload(char* data, size_t len, size_t* currentBytePosition);

  private:
    ParsingInformation* _parsingInformation;
    OnPubCompInternalCallback _callback;

    uint8_t _bytePosition;
    char _packetIdMsb;
    uint16_t _packetId;
  };
}  // namespace AsyncMqttClientInternals
