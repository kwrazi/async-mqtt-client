#pragma once

extern "C" {
  typedef int esp_err_t;
  #include "esp_err.h"
  #include "esp_heap_caps.h"
  #include "soc/soc.h"
}

namespace AsyncMqttClientInternals {

    template <class T>
    struct PsramAllocator {
        using value_type      = T;
        using size_type       = size_t;
        using pointer         = T *;
        using const_pointer   = const value_type *;
        using reference       = value_type &;
        using const_reference = const value_type &;
        using difference_type = ptrdiff_t;

        PsramAllocator() noexcept {};
        template <class U>
        PsramAllocator(const PsramAllocator<U> &) noexcept {};
        ~PsramAllocator() noexcept {};

        inline pointer allocate(size_type n) {
            auto ptr = static_cast<pointer>(heap_caps_malloc(n * sizeof(value_type), MALLOC_CAP_SPIRAM));
            if (ptr) return ptr;
            return static_cast<pointer>(malloc(n * sizeof(value_type)));
        }
        void deallocate(pointer p, size_type n) { heap_caps_free(p); }

        bool operator==(const PsramAllocator &a) { return this == &a; };
        bool operator!=(const PsramAllocator &a) { return !operator==(a); };
    };

inline bool is_in_psram(const uint8_t *ptr) {
  auto addr = (size_t) ptr;
  return (addr >= SOC_EXTRAM_DATA_LOW) && (addr < SOC_EXTRAM_DATA_HIGH);
}

class Helpers {
 public:
  static uint32_t decodeRemainingLength(char* bytes) {
    uint32_t multiplier = 1;
    uint32_t value = 0;
    uint8_t currentByte = 0;
    uint8_t encodedByte;
    do {
      encodedByte = bytes[currentByte++];
      value += (encodedByte & 127) * multiplier;
      multiplier *= 128;
    } while ((encodedByte & 128) != 0);

    return value;
  }

  static uint8_t encodeRemainingLength(uint32_t remainingLength, char* destination) {
    uint8_t currentByte = 0;
    uint8_t bytesNeeded = 0;

    do {
      uint8_t encodedByte = remainingLength % 128;
      remainingLength /= 128;
      if (remainingLength > 0) {
        encodedByte = encodedByte | 128;
      }

      destination[currentByte++] = encodedByte;
      bytesNeeded++;
    } while (remainingLength > 0);

    return bytesNeeded;
  }
};

#if defined(ARDUINO_ARCH_ESP32)
  #define SEMAPHORE_TAKE() xSemaphoreTake(_xSemaphore, portMAX_DELAY)
  #define SEMAPHORE_GIVE() xSemaphoreGive(_xSemaphore)
  #define GET_FREE_MEMORY() ESP.getMaxAllocHeap()
  #include <esp32-hal-log.h>
#elif defined(ARDUINO_ARCH_ESP8266)
  #define SEMAPHORE_TAKE(X) while (_xSemaphore) { /*ESP.wdtFeed();*/ } _xSemaphore = true
  #define SEMAPHORE_GIVE() _xSemaphore = false
  #define GET_FREE_MEMORY() ESP.getMaxFreeBlockSize()
  #if defined(DEBUG_ESP_PORT) && defined(DEBUG_ASYNC_MQTT_CLIENT)
    #define log_i(...) DEBUG_ESP_PORT.printf(__VA_ARGS__); DEBUG_ESP_PORT.print("\n")
    #define log_e(...) DEBUG_ESP_PORT.printf(__VA_ARGS__); DEBUG_ESP_PORT.print("\n")
    #define log_w(...) DEBUG_ESP_PORT.printf(__VA_ARGS__); DEBUG_ESP_PORT.print("\n")
  #else
    #define log_i(...)
    #define log_e(...)
    #define log_w(...)
  #endif
#else
  #pragma error "No valid architecture"
#endif

}  // namespace AsyncMqttClientInternals
