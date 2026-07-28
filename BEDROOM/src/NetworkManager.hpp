#ifndef NETWORK_MANAGER_HPP
#define NETWORK_MANAGER_HPP

#include <WiFi.h>
#include <PubSubClient.h>
#include "Config.hpp" 

WiFiClient espClient;
PubSubClient client(espClient);

typedef void (*MsgCallback)(char*, uint8_t*, unsigned int);

unsigned long lastReconnectAttempt = 0;

void setupNetwork(MsgCallback callback) {
    Serial.println("\n--- BAT DAU KET NOI (DIRECT HOTSPOT MODE) ---");
    
    WiFi.disconnect(true);
    delay(100);
    WiFi.mode(WIFI_STA);
    
    WiFi.setSleep(false);

    IPAddress local_IP(10, 42, 0, 51);
    IPAddress gateway(10, 42, 0, 1);
    IPAddress subnet(255, 255, 255, 0);

    if (!WiFi.config(local_IP, gateway, subnet)) {
        Serial.println("Loi cau hinh IP Tinh!");
    }

    Serial.print("Dang ket noi vao Hotspot: ");
    Serial.println(WIFI_SSID);

    if (String(WIFI_PASS) == "") {
        WiFi.begin(WIFI_SSID, NULL);
    } else {
        WiFi.begin(WIFI_SSID, WIFI_PASS);
    }

    int retries = 0;
    while (WiFi.status() != WL_CONNECTED && retries < 40) {
        delay(500);
        Serial.print(".");
        retries++;
    }

    if (WiFi.status() == WL_CONNECTED) {
        Serial.println("\nKET NOI WIFI THANH CONG!");
        Serial.print("IP ESP32: ");
        Serial.println(WiFi.localIP());
        Serial.print("Gateway:  ");
        Serial.println(WiFi.gatewayIP());
        Serial.print("Signal:   ");
        Serial.println(WiFi.RSSI());
    } else {
        Serial.print("\nTHAT BAI. Status: ");
        Serial.println(WiFi.status());
    }

    client.setServer(MQTT_SERVER, MQTT_PORT);
    client.setCallback(callback);
    client.setSocketTimeout(15);
    client.setKeepAlive(15);
}

inline void maintainConnection() {
    if (!client.connected()) {
        long now = millis();
        if (now - lastReconnectAttempt > 5000) {
            lastReconnectAttempt = now;
            
            if (WiFi.status() != WL_CONNECTED) {
                Serial.println("Mat Wifi. Dang ket noi lai...");
                if (String(WIFI_PASS) == "") WiFi.begin(WIFI_SSID, NULL);
                else WiFi.begin(WIFI_SSID, WIFI_PASS);
                return;
            }

            String clientId = String(DEVICE_NODE_ID) + "_" + String(random(0xffff), HEX);
            Serial.print("Connecting to MQTT Broker (" + String(MQTT_SERVER) + ")... ");
            
            if (client.connect(clientId.c_str())) {
                Serial.println("Broker Connected!");
                client.subscribe(TOPIC_CMD);
                Serial.println("Subscribed: " + String(TOPIC_CMD));
            } else {
                Serial.print("Failed, rc=");
                Serial.println(client.state());
            }
        }
    } else {
        client.loop(); 
    }
}

inline void sendMQTT(const char* topic, String jsonString) {
    if (client.connected()) {
        client.publish(topic, jsonString.c_str());
    }
}

#endif