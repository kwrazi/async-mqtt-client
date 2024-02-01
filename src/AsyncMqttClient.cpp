#include "AsyncMqttClient.hpp"

#include <string_view>

constexpr static std::string_view TAG = "aMQTTc";  // requires C++17

namespace AsyncMqttSettings {
  constexpr size_t MAX_QUEUE_SIZE = 5;
  constexpr esp_log_level_t DEFAULT_LOG_LEVEL = ESP_LOG_INFO;
  constexpr esp_log_level_t LOCAL_LOG_LEVEL = ESP_LOG_WARN;
}  // namespace AsyncMqttSettings

AsyncMqttClient::AsyncMqttClient()
  : _client()
  , _head(nullptr)
  , _tail(nullptr)
  , _sent(0)
  , _state(DISCONNECTED)
  , _disconnectReason(AsyncMqttClientDisconnectReason::TCP_DISCONNECTED)
  , _lastClientActivity(0)
  , _lastServerActivity(0)
  , _lastPingRequestTime(0)
  , _generatedClientId{0}
  , _ip()
  , _host(nullptr)
  , _useIp(false)
#if ASYNC_TCP_SSL_ENABLED
  , _secure(false)
#endif
  , _port(0)
  , _keepAlive(15)
  , _cleanSession(true)
  , _clientId(nullptr)
  , _username(nullptr)
  , _password(nullptr)
  , _willTopic(nullptr)
  , _willPayload(nullptr)
  , _willPayloadLength(0)
  , _willQos(0)
  , _willRetain(false)
#if ASYNC_TCP_SSL_ENABLED
  , _secureServerFingerprints()
#endif
  , _onConnectUserCallbacks()
  , _onDisconnectUserCallbacks()
  , _onSubscribeUserCallbacks()
  , _onUnsubscribeUserCallbacks()
  , _onMessageUserCallbacks()
  , _onPublishUserCallbacks()
  , _parsingInformation{.bufferState = AsyncMqttClientInternals::BufferState::NONE}
  , _currentParsedPacket(nullptr)
  , _remainingLengthBufferPosition(0)
  , _remainingLengthBuffer{0}
  , _pendingPubRels() {
  _client.onConnect([](void* obj, AsyncClient* c) { (static_cast<AsyncMqttClient*>(obj))->_onConnect(); }, this);
  _client.onDisconnect([](void* obj, AsyncClient* c) { (static_cast<AsyncMqttClient*>(obj))->_onDisconnect(); }, this);
  // _client.onError([](void* obj, AsyncClient* c, int8_t error) { (static_cast<AsyncMqttClient*>(obj))->_onError(error); }, this);
  // _client.onTimeout([](void* obj, AsyncClient* c, uint32_t time) { (static_cast<AsyncMqttClient*>(obj))->_onTimeout(); }, this);
  _client.onAck([](void* obj, AsyncClient* c, size_t len, uint32_t time) { (static_cast<AsyncMqttClient*>(obj))->_onAck(len); }, this);
  _client.onData([](void* obj, AsyncClient* c, void* data, size_t len) { (static_cast<AsyncMqttClient*>(obj))->_onData(static_cast<char*>(data), len); }, this);
  _client.onPoll([](void* obj, AsyncClient* c) { (static_cast<AsyncMqttClient*>(obj))->_onPoll(); }, this);
  _client.setNoDelay(true);  // send small packets immediately (PINGREQ/DISCONN are only 2 bytes)
#ifdef ESP32
  sprintf(_generatedClientId, "esp32-%06llx", ESP.getEfuseMac());
  _xSemaphore = xSemaphoreCreateMutex();
#elif defined(ESP8266)
  sprintf(_generatedClientId, "esp8266-%06x", ESP.getChipId());
#endif
  _clientId = _generatedClientId;

  setMaxTopicLength(128);

  // esp_log_level_set("*", AsyncMqttSettings::DEFAULT_LOG_LEVEL);
  esp_log_level_set(TAG.data(), AsyncMqttSettings::LOCAL_LOG_LEVEL);
}

AsyncMqttClient::~AsyncMqttClient() {
  delete _currentParsedPacket;
  delete[] _parsingInformation.topicBuffer;
  _clear();
  _pendingPubRels.clear();
  _pendingPubRels.shrink_to_fit();
  _clearQueue(false);  // _clear() doesn't clear session data
#ifdef ESP32
  vSemaphoreDelete(_xSemaphore);
#endif
}

AsyncMqttClient& AsyncMqttClient::setKeepAlive(uint16_t keepAlive) {
  _keepAlive = keepAlive;
  return *this;
}

AsyncMqttClient& AsyncMqttClient::setClientId(const char* clientId) {
  _clientId = clientId;
  return *this;
}

AsyncMqttClient& AsyncMqttClient::setCleanSession(bool cleanSession) {
  _cleanSession = cleanSession;
  return *this;
}

AsyncMqttClient& AsyncMqttClient::setMaxTopicLength(uint16_t maxTopicLength) {
  _parsingInformation.maxTopicLength = maxTopicLength;
  delete[] _parsingInformation.topicBuffer;
  _parsingInformation.topicBuffer = new char[maxTopicLength + 1];
  return *this;
}

AsyncMqttClient& AsyncMqttClient::setCredentials(const char* username, const char* password) {
  _username = username;
  _password = password;
  return *this;
}

AsyncMqttClient& AsyncMqttClient::setWill(const char* topic, uint8_t qos, bool retain, const char* payload, size_t length) {
  _willTopic = topic;
  _willQos = qos;
  _willRetain = retain;
  _willPayload = payload;
  _willPayloadLength = length;
  return *this;
}

AsyncMqttClient& AsyncMqttClient::setServer(IPAddress ip, uint16_t port) {
  _useIp = true;
  _ip = ip;
  _port = port;
  return *this;
}

AsyncMqttClient& AsyncMqttClient::setServer(const char* host, uint16_t port) {
  _useIp = false;
  _host = host;
  _port = port;
  return *this;
}

#if ASYNC_TCP_SSL_ENABLED
AsyncMqttClient& AsyncMqttClient::setSecure(bool secure) {
  _secure = secure;
  return *this;
}

AsyncMqttClient& AsyncMqttClient::addServerFingerprint(const uint8_t* fingerprint) {
  std::array<uint8_t, SHA1_SIZE> newFingerprint;
  memcpy(newFingerprint.data(), fingerprint, SHA1_SIZE);
  _secureServerFingerprints.push_back(newFingerprint);
  return *this;
}
#endif

AsyncMqttClient& AsyncMqttClient::onConnect(AsyncMqttClientInternals::OnConnectUserCallback callback) {
  _onConnectUserCallbacks.push_back(callback);
  return *this;
}

AsyncMqttClient& AsyncMqttClient::onDisconnect(AsyncMqttClientInternals::OnDisconnectUserCallback callback) {
  _onDisconnectUserCallbacks.push_back(callback);
  return *this;
}

AsyncMqttClient& AsyncMqttClient::onSubscribe(AsyncMqttClientInternals::OnSubscribeUserCallback callback) {
  _onSubscribeUserCallbacks.push_back(callback);
  return *this;
}

AsyncMqttClient& AsyncMqttClient::onUnsubscribe(AsyncMqttClientInternals::OnUnsubscribeUserCallback callback) {
  _onUnsubscribeUserCallbacks.push_back(callback);
  return *this;
}

AsyncMqttClient& AsyncMqttClient::onMessage(AsyncMqttClientInternals::OnMessageUserCallback callback) {
  _onMessageUserCallbacks.push_back(callback);
  return *this;
}

AsyncMqttClient& AsyncMqttClient::onPublish(AsyncMqttClientInternals::OnPublishUserCallback callback) {
  _onPublishUserCallbacks.push_back(callback);
  return *this;
}

void AsyncMqttClient::_freeCurrentParsedPacket() {
  delete _currentParsedPacket;
  _currentParsedPacket = nullptr;
}

void AsyncMqttClient::_clear() {
  _lastPingRequestTime = 0;
  _freeCurrentParsedPacket();
  _clearQueue(true);  // keep session data for now

  _parsingInformation.bufferState = AsyncMqttClientInternals::BufferState::NONE;

  _client.setRxTimeout(0);
}

/* TCP */
void AsyncMqttClient::_onConnect() {
  ESP_LOGI(TAG.data(), "TCP conn, MQTT CONNECT");
#if ASYNC_TCP_SSL_ENABLED
  if (_secure && _secureServerFingerprints.size() > 0) {
    SSL* clientSsl = _client.getSSL();

    bool sslFoundFingerprint = false;
    for (std::array<uint8_t, SHA1_SIZE> fingerprint : _secureServerFingerprints) {
      if (ssl_match_fingerprint(clientSsl, fingerprint.data()) == SSL_OK) {
        sslFoundFingerprint = true;
        break;
      }
    }

    if (!sslFoundFingerprint) {
      _disconnectReason = AsyncMqttClientDisconnectReason::TLS_BAD_FINGERPRINT;
      _client.close(true);
      return;
    }
  }
#endif
  AsyncMqttClientInternals::OutPacket* msg = new AsyncMqttClientInternals::ConnectOutPacket(_cleanSession, _username, _password, _willTopic, _willRetain,
                                                                                            _willQos, _willPayload, _willPayloadLength, _keepAlive, _clientId);
  _addFront(msg);
  _handleQueue();
}

void AsyncMqttClient::_onDisconnect() {
  ESP_LOGI(TAG.data(), "TCP disconn");
  _state = DISCONNECTED;

  _clear();

  for (auto callback : _onDisconnectUserCallbacks) callback(_disconnectReason);
}

/*
void AsyncMqttClient::_onError(int8_t error) {
  (void)error;
  // _onDisconnect called anyway
}

void AsyncMqttClient::_onTimeout() {
  // disconnection will be handled by ping/pong management
}
*/

// notify ack received and handle queue
void AsyncMqttClient::_onAck(size_t len) {
  ESP_LOGI(TAG.data(), "ack %u", len);
  _handleQueue();
}

void AsyncMqttClient::_onData(char* data, size_t len) {
  ESP_LOGI(TAG.data(), "data rcv (%u)", len);
  size_t currentBytePosition = 0;
  char currentByte;
  _lastServerActivity = millis();
  do {
    switch (_parsingInformation.bufferState) {
      case AsyncMqttClientInternals::BufferState::NONE:
        currentByte = data[currentBytePosition++];
        _parsingInformation.packetType = currentByte >> 4;
        _parsingInformation.packetFlags = (currentByte << 4) >> 4;
        _parsingInformation.bufferState = AsyncMqttClientInternals::BufferState::REMAINING_LENGTH;
        switch (_parsingInformation.packetType) {
          case AsyncMqttClientInternals::PacketType.CONNACK:
            ESP_LOGI(TAG.data(), "rcv CONNACK");
            _currentParsedPacket = new AsyncMqttClientInternals::ConnAckPacket(
                &_parsingInformation, std::bind(&AsyncMqttClient::_onConnAck, this, std::placeholders::_1, std::placeholders::_2));
            _client.setRxTimeout(0);
            break;
          case AsyncMqttClientInternals::PacketType.PINGRESP:
            ESP_LOGI(TAG.data(), "rcv PINGRESP");
            _currentParsedPacket = new AsyncMqttClientInternals::PingRespPacket(&_parsingInformation, std::bind(&AsyncMqttClient::_onPingResp, this));
            break;
          case AsyncMqttClientInternals::PacketType.SUBACK:
            ESP_LOGI(TAG.data(), "rcv SUBACK");
            _currentParsedPacket = new AsyncMqttClientInternals::SubAckPacket(
                &_parsingInformation, std::bind(&AsyncMqttClient::_onSubAck, this, std::placeholders::_1, std::placeholders::_2));
            break;
          case AsyncMqttClientInternals::PacketType.UNSUBACK:
            ESP_LOGI(TAG.data(), "rcv UNSUBACK");
            _currentParsedPacket = new AsyncMqttClientInternals::UnsubAckPacket(&_parsingInformation,
                                                                                std::bind(&AsyncMqttClient::_onUnsubAck, this, std::placeholders::_1));
            break;
          case AsyncMqttClientInternals::PacketType.PUBLISH:
            ESP_LOGI(TAG.data(), "rcv PUBLISH");
            _currentParsedPacket = new AsyncMqttClientInternals::PublishPacket(
                &_parsingInformation,
                std::bind(&AsyncMqttClient::_onMessage, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4,
                          std::placeholders::_5, std::placeholders::_6, std::placeholders::_7, std::placeholders::_8, std::placeholders::_9),
                std::bind(&AsyncMqttClient::_onPublish, this, std::placeholders::_1, std::placeholders::_2));
            break;
          case AsyncMqttClientInternals::PacketType.PUBREL:
            ESP_LOGI(TAG.data(), "rcv PUBREL");
            _currentParsedPacket = new AsyncMqttClientInternals::PubRelPacket(&_parsingInformation,
                                                                              std::bind(&AsyncMqttClient::_onPubRel, this, std::placeholders::_1));
            break;
          case AsyncMqttClientInternals::PacketType.PUBACK:
            ESP_LOGI(TAG.data(), "rcv PUBACK");
            _currentParsedPacket = new AsyncMqttClientInternals::PubAckPacket(&_parsingInformation,
                                                                              std::bind(&AsyncMqttClient::_onPubAck, this, std::placeholders::_1));
            break;
          case AsyncMqttClientInternals::PacketType.PUBREC:
            ESP_LOGI(TAG.data(), "rcv PUBREC");
            _currentParsedPacket = new AsyncMqttClientInternals::PubRecPacket(&_parsingInformation,
                                                                              std::bind(&AsyncMqttClient::_onPubRec, this, std::placeholders::_1));
            break;
          case AsyncMqttClientInternals::PacketType.PUBCOMP:
            ESP_LOGI(TAG.data(), "rcv PUBCOMP");
            _currentParsedPacket = new AsyncMqttClientInternals::PubCompPacket(&_parsingInformation,
                                                                               std::bind(&AsyncMqttClient::_onPubComp, this, std::placeholders::_1));
            break;
          default:
            ESP_LOGI(TAG.data(), "rcv PROTOCOL VIOLATION");
            disconnect(true);
            break;
        }
        break;
      case AsyncMqttClientInternals::BufferState::REMAINING_LENGTH:
        currentByte = data[currentBytePosition++];
        _remainingLengthBuffer[_remainingLengthBufferPosition++] = currentByte;
        if (currentByte >> 7 == 0) {
          _parsingInformation.remainingLength = AsyncMqttClientInternals::Helpers::decodeRemainingLength(_remainingLengthBuffer);
          _remainingLengthBufferPosition = 0;
          if (_parsingInformation.remainingLength > 0) {
            _parsingInformation.bufferState = AsyncMqttClientInternals::BufferState::VARIABLE_HEADER;
          } else {
            // PINGRESP is a special case where it has no variable header, so the packet ends right here
            _parsingInformation.bufferState = AsyncMqttClientInternals::BufferState::NONE;
            _onPingResp();
          }
        }
        break;
      case AsyncMqttClientInternals::BufferState::VARIABLE_HEADER:
        _currentParsedPacket->parseVariableHeader(data, len, &currentBytePosition);
        break;
      case AsyncMqttClientInternals::BufferState::PAYLOAD:
        _currentParsedPacket->parsePayload(data, len, &currentBytePosition);
        break;
      default:
        currentBytePosition = len;
    }
  } while (currentBytePosition != len);
}

void AsyncMqttClient::_onPoll() {
  // if there is too much time the client has sent a ping request without a response, disconnect client to avoid half open connections
  if (_lastPingRequestTime != 0 && (millis() - _lastPingRequestTime) >= (_keepAlive * 1000 * 2)) {
    ESP_LOGW(TAG.data(), "PING t/o, disconnecting");
    disconnect(true);
    return;
  }
  // send ping to ensure the server will receive at least one message inside keepalive window
  if (_state == CONNECTED && _lastPingRequestTime == 0 && (millis() - _lastClientActivity) >= (_keepAlive * 1000 * 0.7)) {
    _sendPing();
    // send ping to verify if the server is still there (ensure this is not a half connection)
  } else if (_state == CONNECTED && _lastPingRequestTime == 0 && (millis() - _lastServerActivity) >= (_keepAlive * 1000 * 0.7)) {
    _sendPing();
  } else {
    _handleQueue();
  }
}

/* QUEUE */

size_t AsyncMqttClient::_queueLength() const {
  auto it = _head;
  size_t count = 0u;
  while (it != nullptr) {
    ++count;
    ets_printf("@%u,", it->packetType());  // show packet type on queue
    it = it->next;
  }
  if (count > 0) {
    ets_printf("\n");
  }
  return count;
}

void AsyncMqttClient::_removeBack() {
  auto count = 0u;
  if (_head == nullptr) return;
  if (_head->next == nullptr) {
    ESP_LOGI(TAG.data(), "del back pos=%u, #%u", count, _head->packetType());
    delete _head;
    _head = nullptr;
    _tail = nullptr;
    return;
  }
  auto new_tail = _head;
  while (new_tail->next->next != nullptr) {
    new_tail = new_tail->next;
    ++count;
  }
  ESP_LOGI(TAG.data(), "del back pos=%u, #%u", count, _head->packetType());
  delete new_tail->next;
  _tail = new_tail;
}

// insert message in second position from front and handle queue
void AsyncMqttClient::_insert(AsyncMqttClientInternals::OutPacket* packet) {
  // We only use this for QoS2 PUBREL so there must be a PUBLISH packet present.
  // The queue therefore cannot be empty and _head points to this PUBLISH packet.
  SEMAPHORE_TAKE();
  if (_queueLength() >= AsyncMqttSettings::MAX_QUEUE_SIZE) {
    ESP_LOGE(TAG.data(), "queue full.");
    _removeBack();
  }
  ESP_LOGI(TAG.data(), "new insert #%u", packet->packetType());
  packet->next = _head->next;
  _head->next = packet;
  if (_head == _tail) {  // PUB packet is the only one in the queue
    _tail = packet;
  }
  SEMAPHORE_GIVE();
  _handleQueue();
}

// add message to front and handle queue
void AsyncMqttClient::_addFront(AsyncMqttClientInternals::OutPacket* packet) {
  // This is only used for the CONNECT packet, to be able to establish a connection
  // before anything else. The queue can be empty or has packets from the continued session.
  // In both cases, _head should always point to the CONNECT packet afterwards.
  SEMAPHORE_TAKE();
  if (_queueLength() >= AsyncMqttSettings::MAX_QUEUE_SIZE) {
    ESP_LOGE(TAG.data(), "queue full.");
    _removeBack();
  }
  ESP_LOGI(TAG.data(), "new front #%u", packet->packetType());
  if (_head == nullptr) {
    _tail = packet;
  } else {
    packet->next = _head;
  }
  _head = packet;
  SEMAPHORE_GIVE();
  _handleQueue();
}

// add message to back and handle queue, else drop and return false
bool AsyncMqttClient::_addBack(AsyncMqttClientInternals::OutPacket* packet) {
  auto queued = false;
  SEMAPHORE_TAKE();
  if (_queueLength() >= AsyncMqttSettings::MAX_QUEUE_SIZE) {
    ESP_LOGE(TAG.data(), "queue full. drop msg t=#%u", packet->packetType());
    packet->release();
    // this is an unlinked packet that should be dropped/deleted externally
  } else {
    ESP_LOGI(TAG.data(), "new back #%u", packet->packetType());
    if (!_tail) {
      _head = packet;
    } else {
      _tail->next = packet;
    }
    _tail = packet;
    _tail->next = nullptr;
    queued = true;
  }
  SEMAPHORE_GIVE();
  _handleQueue();
  return queued;
}

void AsyncMqttClient::_handleQueue() {
  // On ESP32, onDisconnect is called within the close()-call. So we need to make sure we don't lock
  bool disconnect = false;

  ESP_LOGI(TAG.data(), "hq< %u,%u", _sent, _client.space());
  SEMAPHORE_TAKE();

  // uint8_t apiflags = ASYNC_WRITE_FLAG_MORE;
  while (_head) {
    // 1. try to send
    if (_head->size() > _sent) {
      // On SSL the TCP library returns the total amount of bytes, not just the unencrypted payload length.
      // So we calculate the amount to be written ourselves.
      size_t willSend = std::min(_head->size() - _sent, _client.space());
      size_t realSend = _client.add(reinterpret_cast<const char*>(_head->data(_sent)), willSend);  // flag is set by LWIP anyway, added for clarity
      if (realSend == 0) {
        ESP_LOGE(TAG.data(), "tcp.queue failed, %u", _client.space());
        break;
      }
      if (_client.send() == false) {
        ESP_LOGE(TAG.data(), "tcp.send failed, %u", _client.space());
        break;
      }
      _sent += realSend;
      _lastClientActivity = millis();
      _lastPingRequestTime = 0;
      taskYIELD();
#if ASYNC_TCP_SSL_ENABLED
      ESP_LOGI(TAG.data(), "snd #%u: (tls: %u) %u/%u", _head->packetType(), realSent, _sent, _head->size());
#else
      ESP_LOGI(TAG.data(), "snd #%u: %u/%u", _head->packetType(), _sent, _head->size());
#endif
      if (_head->packetType() == AsyncMqttClientInternals::PacketType.DISCONNECT) {
        disconnect = true;
      }
    }

    // 2. stop processing when we have to wait for an MQTT acknowledgment
    if (_head->size() <= _sent) {
      auto freed_bytes = _head->size();
      if (_head->released()) {
        ESP_LOGI(TAG.data(), "p #%d rel", _head->packetType());
        AsyncMqttClientInternals::OutPacket* tmp = _head;
        _head = _head->next;
        if (!_head) _tail = nullptr;
        delete tmp;
        _sent -= freed_bytes;
      } else {
        break;  // sending is complete however send next only after mqtt confirmation
      }
    }
  }

  SEMAPHORE_GIVE();
  ESP_LOGI(TAG.data(), "hq>");

  if (disconnect) {
    ESP_LOGW(TAG.data(), "snd DISCONN, disconnecting");
    _client.close();
  }
}

void AsyncMqttClient::_clearQueue(bool keepSessionData) {
  SEMAPHORE_TAKE();
  AsyncMqttClientInternals::OutPacket* packet = _head;
  _head = nullptr;
  _tail = nullptr;

  while (packet) {
    /* MQTT spec 3.1.2.4 Clean Session:
     *  - QoS 1 and QoS 2 messages which have been sent to the Server, but have not been completely acknowledged.
     *  - QoS 2 messages which have been received from the Server, but have not been completely acknowledged.
     * + (unsent PUB messages with QoS > 0)
     *
     * To be kept:
     * - possibly first message (sent to server but not acked)
     * - PUBREC messages (QoS 2 PUB received but not acked)
     * - PUBCOMP messages (QoS 2 PUBREL received but not acked)
     */
    if (keepSessionData) {
      if (packet->qos() > 0 && packet->size() <= _sent) {  // check for qos includes check for PUB-packet type
        reinterpret_cast<AsyncMqttClientInternals::PublishOutPacket*>(packet)->setDup();
        AsyncMqttClientInternals::OutPacket* next = packet->next;
        ESP_LOGI(TAG.data(), "keep #%u", packet->packetType());
        SEMAPHORE_GIVE();
        _addBack(packet);
        SEMAPHORE_TAKE();
        packet = next;
      } else if (packet->qos() > 0 || packet->packetType() == AsyncMqttClientInternals::PacketType.PUBREC ||
                 packet->packetType() == AsyncMqttClientInternals::PacketType.PUBCOMP) {
        AsyncMqttClientInternals::OutPacket* next = packet->next;
        ESP_LOGI(TAG.data(), "keep #%u", packet->packetType());
        SEMAPHORE_GIVE();
        _addBack(packet);
        SEMAPHORE_TAKE();
        packet = next;
      } else {
        AsyncMqttClientInternals::OutPacket* next = packet->next;
        delete packet;
        packet = next;
      }
      /* Delete everything when not keeping session data
       */
    } else {
      AsyncMqttClientInternals::OutPacket* next = packet->next;
      delete packet;
      packet = next;
    }
  }
  _sent = 0;
  SEMAPHORE_GIVE();
}

/* MQTT */
void AsyncMqttClient::_onPingResp() {
  ESP_LOGI(TAG.data(), "PINGRESP");
  _freeCurrentParsedPacket();
  _lastPingRequestTime = 0;
}

void AsyncMqttClient::_onConnAck(bool sessionPresent, uint8_t connectReturnCode) {
  // sessionPresent is true if the connection is persistent
  ESP_LOGI(TAG.data(), "CONNACK");
  _freeCurrentParsedPacket();

  if (!sessionPresent) {
    _pendingPubRels.clear();
    _pendingPubRels.shrink_to_fit();
    _clearQueue(false);  // remove session data
  }

  if (connectReturnCode == 0) {
    _state = CONNECTED;
    for (auto callback : _onConnectUserCallbacks) callback(sessionPresent);
  } else {
    // Callbacks are handled by the onDisconnect function which is called from the AsyncTcp lib
    _disconnectReason = static_cast<AsyncMqttClientDisconnectReason>(connectReturnCode);
    return;
  }
  _handleQueue();  // send any remaining data from continued session
}

void AsyncMqttClient::_onSubAck(uint16_t packetId, char status) {
  ESP_LOGI(TAG.data(), "SUBACK");
  _freeCurrentParsedPacket();
  SEMAPHORE_TAKE();
  if (_head && _head->packetId() == packetId) {
    _head->release();
    ESP_LOGI(TAG.data(), "SUB released");
  }
  SEMAPHORE_GIVE();

  for (auto callback : _onSubscribeUserCallbacks) callback(packetId, status);

  _handleQueue();  // subscribe confirmed, ready to send next queued item
}

void AsyncMqttClient::_onUnsubAck(uint16_t packetId) {
  ESP_LOGI(TAG.data(), "UNSUBACK");
  _freeCurrentParsedPacket();
  SEMAPHORE_TAKE();
  if (_head && _head->packetId() == packetId) {
    _head->release();
    ESP_LOGI(TAG.data(), "UNSUB released");
  }
  SEMAPHORE_GIVE();

  for (auto callback : _onUnsubscribeUserCallbacks) callback(packetId);

  _handleQueue();  // unsubscribe confirmed, ready to send next queued item
}

void AsyncMqttClient::_onMessage(char* topic, char* payload, uint8_t qos, bool dup, bool retain, size_t len, size_t index, size_t total, uint16_t packetId) {
  bool notifyPublish = true;

  if (qos == 2) {
    for (AsyncMqttClientInternals::PendingPubRel pendingPubRel : _pendingPubRels) {
      if (pendingPubRel.packetId == packetId) {
        notifyPublish = false;
        break;
      }
    }
  }

  if (notifyPublish) {
    AsyncMqttClientMessageProperties properties;
    properties.qos = qos;
    properties.dup = dup;
    properties.retain = retain;

    for (auto callback : _onMessageUserCallbacks) callback(topic, payload, properties, len, index, total);
  }
}

void AsyncMqttClient::_onPublish(uint16_t packetId, uint8_t qos) {
  AsyncMqttClientInternals::PendingAck pendingAck;

  if (qos == 1) {
    pendingAck.packetType = AsyncMqttClientInternals::PacketType.PUBACK;
    pendingAck.headerFlag = AsyncMqttClientInternals::HeaderFlag.PUBACK_RESERVED;
    pendingAck.packetId = packetId;
    AsyncMqttClientInternals::OutPacket* msg = new AsyncMqttClientInternals::PubAckOutPacket(pendingAck);
    if (_addBack(msg) == false) {
      // could potentially be dropped if queue is full
      ESP_LOGW(TAG.data(), "PUBACK dropped");
      delete msg;
    }
  } else if (qos == 2) {
    pendingAck.packetType = AsyncMqttClientInternals::PacketType.PUBREC;
    pendingAck.headerFlag = AsyncMqttClientInternals::HeaderFlag.PUBREC_RESERVED;
    pendingAck.packetId = packetId;
    AsyncMqttClientInternals::OutPacket* msg = new AsyncMqttClientInternals::PubAckOutPacket(pendingAck);
    if (_addBack(msg) == false) {
      // could potentially be dropped if queue is full
      ESP_LOGW(TAG.data(), "PUBACK dropped");
      delete msg;
    } else {
      bool pubRelAwaiting = false;
      for (AsyncMqttClientInternals::PendingPubRel pendingPubRel : _pendingPubRels) {
        if (pendingPubRel.packetId == packetId) {
          pubRelAwaiting = true;
          break;
        }
      }

      if (!pubRelAwaiting) {
        AsyncMqttClientInternals::PendingPubRel pendingPubRel;
        pendingPubRel.packetId = packetId;
        _pendingPubRels.push_back(pendingPubRel);
      }
    }
  }

  _freeCurrentParsedPacket();
}

void AsyncMqttClient::_onPubRel(uint16_t packetId) {
  _freeCurrentParsedPacket();

  AsyncMqttClientInternals::PendingAck pendingAck;
  pendingAck.packetType = AsyncMqttClientInternals::PacketType.PUBCOMP;
  pendingAck.headerFlag = AsyncMqttClientInternals::HeaderFlag.PUBCOMP_RESERVED;
  pendingAck.packetId = packetId;
  if (_head && _head->packetId() == packetId) {
    AsyncMqttClientInternals::OutPacket* msg = new AsyncMqttClientInternals::PubAckOutPacket(pendingAck);
    _head->release();
    _insert(msg);
    ESP_LOGI(TAG.data(), "PUBREC released");
  }

  for (size_t i = 0; i < _pendingPubRels.size(); i++) {
    if (_pendingPubRels[i].packetId == packetId) {
      _pendingPubRels.erase(_pendingPubRels.begin() + i);
      _pendingPubRels.shrink_to_fit();
    }
  }
}

void AsyncMqttClient::_onPubAck(uint16_t packetId) {
  _freeCurrentParsedPacket();
  if (_head && _head->packetId() == packetId) {
    _head->release();
    ESP_LOGI(TAG.data(), "PUB released");
  }

  for (auto callback : _onPublishUserCallbacks) callback(packetId);
}

void AsyncMqttClient::_onPubRec(uint16_t packetId) {
  _freeCurrentParsedPacket();

  // We will only be sending 1 QoS>0 PUB message at a time (to honor message
  // ordering). So no need to store ACKS in a separate container as it will
  // be stored in the outgoing queue until a PUBCOMP comes in.
  AsyncMqttClientInternals::PendingAck pendingAck;
  pendingAck.packetType = AsyncMqttClientInternals::PacketType.PUBREL;
  pendingAck.headerFlag = AsyncMqttClientInternals::HeaderFlag.PUBREL_RESERVED;
  pendingAck.packetId = packetId;
  ESP_LOGI(TAG.data(), "snd PUBREL");

  AsyncMqttClientInternals::OutPacket* msg = new AsyncMqttClientInternals::PubAckOutPacket(pendingAck);
  if (_head && _head->packetId() == packetId) {
    _head->release();
    ESP_LOGI(TAG.data(), "PUB released");
  }
  _insert(msg);
}

void AsyncMqttClient::_onPubComp(uint16_t packetId) {
  _freeCurrentParsedPacket();

  // _head points to the PUBREL package
  if (_head && _head->packetId() == packetId) {
    _head->release();
    ESP_LOGI(TAG.data(), "PUBREL released");
  }

  for (auto callback : _onPublishUserCallbacks) callback(packetId);
}

// add ping message to queue and handle queue
void AsyncMqttClient::_sendPing() {
  ESP_LOGI(TAG.data(), "PING");
  _lastPingRequestTime = millis();
  AsyncMqttClientInternals::OutPacket* msg = new AsyncMqttClientInternals::PingReqOutPacket;
  if (_addBack(msg) == false) {
    delete msg;
  }
}

bool AsyncMqttClient::connected() const {
  return _state == CONNECTED;
}

void AsyncMqttClient::connect() {
  // prevent reconnection spamming - wait for reconnection
  static decltype(millis()) last_connection_attempt_ms = 0u;
  if (_state == CONNECTING) {
    auto duration_ms = millis() - last_connection_attempt_ms;
    if (last_connection_attempt_ms == 0 || duration_ms > 5000) {
      ESP_LOGW(TAG.data(), "connection timed out - disconnecting");
      disconnect();
      last_connection_attempt_ms = 0;
    }
    return;
  }
  if (_state != DISCONNECTED) return;
  ESP_LOGI(TAG.data(), "CONNECTING");
  _state = CONNECTING;
  _disconnectReason = AsyncMqttClientDisconnectReason::TCP_DISCONNECTED;  // reset any previous
  last_connection_attempt_ms = millis();

  _client.setRxTimeout(_keepAlive);

#if ASYNC_TCP_SSL_ENABLED
  if (_useIp) {
    _client.connect(_ip, _port, _secure);
  } else {
    _client.connect(_host, _port, _secure);
  }
#else
  if (_useIp) {
    _client.connect(_ip, _port);
  } else {
    _client.connect(_host, _port);
  }
#endif
}

void AsyncMqttClient::disconnect(bool force) {
  if (_state == DISCONNECTED) return;
  ESP_LOGI(TAG.data(), "DISCONNECT (f:%d)", force);
  if (force) {
    _state = DISCONNECTED;
    _client.close(true);
  } else if (_state != DISCONNECTING) {
    _state = DISCONNECTING;
    AsyncMqttClientInternals::OutPacket* msg = new AsyncMqttClientInternals::DisconnOutPacket;
    if (_addBack(msg) == false) {
      delete msg;
    }
  }
}

uint16_t AsyncMqttClient::subscribe(const char* topic, uint8_t qos) {
  if (_state != CONNECTED) return 0;
  ESP_LOGI(TAG.data(), "SUBSCRIBE");

  AsyncMqttClientInternals::OutPacket* msg = new AsyncMqttClientInternals::SubscribeOutPacket(topic, qos);
  if (_addBack(msg) == false) {
    delete msg;
    return 0;
  }
  return msg->packetId();
}

uint16_t AsyncMqttClient::unsubscribe(const char* topic) {
  if (_state != CONNECTED) return 0;
  ESP_LOGI(TAG.data(), "UNSUBSCRIBE");

  AsyncMqttClientInternals::OutPacket* msg = new AsyncMqttClientInternals::UnsubscribeOutPacket(topic);
  if (_addBack(msg) == false) {
    delete msg;
    return 0;
  }
  return msg->packetId();
}

uint16_t AsyncMqttClient::publish(const char* topic, uint8_t qos, bool retain, const char* payload, size_t length, bool dup, uint16_t message_id) {
  if (_state != CONNECTED || GET_FREE_MEMORY() < MQTT_MIN_FREE_MEMORY) return 0;
  ESP_LOGI(TAG.data(), "PUBLISH");

  AsyncMqttClientInternals::OutPacket* msg = new AsyncMqttClientInternals::PublishOutPacket(topic, qos, retain, payload, length);
  if (_addBack(msg) == false) {
    delete msg;
    return 0;
  }
  return msg->packetId();
}

bool AsyncMqttClient::clearQueue() {
  if (_state != DISCONNECTED) return false;
  _clearQueue(false);
  return true;
}

const char* AsyncMqttClient::getClientId() const {
  return _clientId;
}

const char* AsyncMqttClient::state_string() const {
  switch (_state) {
    case CONNECTED:
      return "CONNECTED";
    case DISCONNECTED:
      return "DISCONNECTED";
    case CONNECTING:
      return "CONNECTING";
    case DISCONNECTING:
      return "DISCONNECTING";
    default:
      return "UNKNOWN";
  }
}
