#ifndef HARDWARE_CONTROL_HPP
#define HARDWARE_CONTROL_HPP

#include <Arduino.h>
#include <ArduinoJson.h>
#include <Wire.h>
#include <Preferences.h> 
#include <vector> 
#include "Adafruit_CCS811.h"
#include "ClosedCube_HDC1080.h"
#include "Config.hpp"
#include "NetworkManager.hpp"

// ================= ĐỐI TƯỢNG PHẦN CỨNG =================
Adafruit_CCS811 ccs;
ClosedCube_HDC1080 hdc1080;
Preferences preferences; 

// Biến chia sẻ giữa các luồng 
volatile bool fanState = false;
volatile bool lightState = false;

// Cờ báo hiệu 
volatile bool fanChanged = false;
volatile bool lightChanged = false;

float valTemp = 0; float valHum = 0;
uint16_t valCO2 = 0; uint16_t valTVOC = 0;
bool hasCCS = false;

// Bộ đệm RAM lưu dữ liệu khi mất mạng
// Lưu tối đa 50 bản tin 
std::vector<String> sensorBuffer; 
const size_t MAX_BUFFER_SIZE = 50; 

// ================= HÀM HELPER =================
void sendDeviceStatus(String docId, bool isOn) {
    if (client.connected()) {
        StaticJsonDocument<200> doc;
        doc["deviceId"] = docId;
        doc["isOn"] = isOn;
        String out; serializeJson(doc, out);
        sendMQTT(TOPIC_STATUS, out);
    }
}

void saveState() {
    preferences.begin("bedroom_state", false); 
    preferences.putBool("fan", fanState);
    preferences.putBool("light", lightState);
    preferences.end();
}

void flushSensorBuffer() {
    if (sensorBuffer.empty()) return;

    Serial.printf("[SYNC] Đang đồng bộ %d bản tin cũ...\n", sensorBuffer.size());
    
    for (const String& payload : sensorBuffer) {
        sendMQTT(TOPIC_SENSORS, payload);
        delay(50); 
    }
    
    sensorBuffer.clear();
    Serial.println("[SYNC] Đã đồng bộ xong!");
}

// ================= TASK XỬ LÝ NÚT NHẤN =================
void taskButtonMonitor(void * parameter) {
    pinMode(PIN_BTN_FAN, INPUT_PULLUP);
    pinMode(PIN_BTN_LIGHT, INPUT_PULLUP);

    int lastFanState = HIGH;
    int lastLightState = HIGH;

    for (;;) { 
        int currentFan = digitalRead(PIN_BTN_FAN);
        if (lastFanState == HIGH && currentFan == LOW) { 
            fanState = !fanState; 
            digitalWrite(PIN_RELAY_FAN, fanState ? HIGH : LOW); 
            fanChanged = true; 
            Serial.println("[TASK] Fan Button Pressed!");
        }
        lastFanState = currentFan;

        int currentLight = digitalRead(PIN_BTN_LIGHT);
        if (lastLightState == HIGH && currentLight == LOW) { 
            lightState = !lightState;
            digitalWrite(PIN_LIGHT, lightState ? HIGH : LOW); 
            lightChanged = true;
            Serial.println("[TASK] Light Button Pressed!");
        }
        lastLightState = currentLight;

        vTaskDelay(50 / portTICK_PERIOD_MS); 
    }
}

// ================= SETUP =================
inline void setupHardware() {
    Serial.println("--- BEDROOM HARDWARE SETUP ---");
    
    preferences.begin("bedroom_state", true); 
    fanState = preferences.getBool("fan", false);   
    lightState = preferences.getBool("light", false);
    preferences.end();
    
    pinMode(PIN_RELAY_FAN, OUTPUT);
    pinMode(PIN_LIGHT, OUTPUT);
    digitalWrite(PIN_RELAY_FAN, fanState ? HIGH : LOW); 
    digitalWrite(PIN_LIGHT, lightState ? HIGH : LOW); 
    
    xTaskCreate(taskButtonMonitor, "ButtonTask", 2048, NULL, 1, NULL);

    Wire.begin(PIN_I2C_SDA, PIN_I2C_SCL);
    hdc1080.begin(0x40);
    if(ccs.begin()){ hasCCS = true; while(!ccs.available()); } 
    else { hasCCS = false; Serial.println("CCS811 skipped"); }
}

// ================= LOGIC LOOP  =================
inline void loopHardware() {
    if (fanChanged) {
        fanChanged = false; saveState(); sendDeviceStatus(ID_FAN_BEDROOM, fanState);
    }
    if (lightChanged) {
        lightChanged = false; saveState(); sendDeviceStatus(ID_LIGHT_BEDROOM, lightState);
    }

    static unsigned long lastSensorSend = 0;
    if (millis() - lastSensorSend > 5000) {
        lastSensorSend = millis();

        float t = hdc1080.readTemperature();
        float h = hdc1080.readHumidity();
        if (hasCCS && ccs.available() && !ccs.readData()) {
            valCO2 = ccs.geteCO2(); valTVOC = ccs.getTVOC(); ccs.setEnvironmentalData(h, t);
        }
        
        StaticJsonDocument<256> doc;
        doc["temperature"] = isnan(t)?0:t; 
        doc["humidity"] = isnan(h)?0:h;
        doc["co2"] = valCO2; 
        doc["tvoc"] = valTVOC;
        String out; serializeJson(doc, out);

        if (client.connected()) {
            if (!sensorBuffer.empty()) {
                flushSensorBuffer();
            }
            sendMQTT(TOPIC_SENSORS, out);
        } 
        else {
            if (sensorBuffer.size() < MAX_BUFFER_SIZE) {
                sensorBuffer.push_back(out);
                Serial.printf("[OFFLINE] Buffered data. Size: %d/%d\n", sensorBuffer.size(), MAX_BUFFER_SIZE);
            } else {
                sensorBuffer.erase(sensorBuffer.begin());
                sensorBuffer.push_back(out);
                Serial.println("[OFFLINE] Buffer full, rotating...");
            }
        }
    }

    static unsigned long lastStatusSync = 0;
    if (millis() - lastStatusSync > 10000) { 
        lastStatusSync = millis();
        if (client.connected()) {
             sendDeviceStatus(ID_FAN_BEDROOM, fanState);
             sendDeviceStatus(ID_LIGHT_BEDROOM, lightState);
        }
    }
}

// ================= XỬ LÝ LỆNH TỪ GATEWAY =================
inline void processCommand(String topic, String payload) {
    StaticJsonDocument<512> doc;
    if (deserializeJson(doc, payload)) return;

    const char* device = doc["device"];
    const char* action = doc["action"]; 
    bool state = (strcmp(action, "turn_on") == 0);

    Serial.printf("CMD: %s -> %s\n", device, action);

    if (strcmp(device, ID_FAN_BEDROOM) == 0) {
        fanState = state;
        digitalWrite(PIN_RELAY_FAN, fanState ? HIGH : LOW);
        saveState();
        sendDeviceStatus(ID_FAN_BEDROOM, fanState);
    }
    else if (strcmp(device, ID_LIGHT_BEDROOM) == 0) {
        lightState = state;
        digitalWrite(PIN_LIGHT, lightState ? HIGH : LOW);
        saveState();
        sendDeviceStatus(ID_LIGHT_BEDROOM, lightState);
    }
}

#endif