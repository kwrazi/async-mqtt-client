#pragma once

#include "../Callbacks.hpp"
#include "../Flags.hpp"
#include "../ParsingInformation.hpp"
#include "Arduino.h"
#include "Packet.hpp"

namespace AsyncMqttClientInternals {
  class PublishPacket : public Packet {
  public:
    explicit PublishPacket(ParsingInformation* parsingInformation, OnMessageInternalCallback dataCallback, OnPublishInternalCallback completeCallback);
    ~PublishPacket();

    void parseVariableHeader(char* data, size_t len, size_t* currentBytePosition);
    void parsePayload(char* data, size_t len, size_t* currentBytePosition);

  private:
    ParsingInformation* _parsingInformation;
    OnMessageInternalCallback _dataCallback;
    OnPublishInternalCallback _completeCallback;

    void _preparePayloadHandling(uint32_t payloadLength);

    bool _dup;
    uint8_t _qos;
    bool _retain;

    uint8_t _bytePosition;
    char _topicLengthMsb;
    uint16_t _topicLength;
    bool _ignore;
    char _packetIdMsb;
    uint16_t _packetId;
    uint32_t _payloadLength;
    uint32_t _payloadBytesRead;
  };
}  // namespace AsyncMqttClientInternals
