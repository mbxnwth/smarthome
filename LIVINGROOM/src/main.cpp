#include <Arduino.h>
#include "Config.hpp"           
#include "NetworkManager.hpp"   
#include "HardwareControl.hpp"  

int lastBtnFanState = HIGH;
int lastBtnLightState = HIGH;
unsigned long lastDebounceTime = 0;
const unsigned long debounceDelay = 200; 

void mqttCallback(char* topic, byte* payload, unsigned int length) {
    String message;
    for (unsigned int i = 0; i < length; i++) {
        message += (char)payload[i];
    }
    processCommand(String(topic), message);
}

void setup() {
    Serial.begin(115200);
    delay(1000); 
    
    setupHardware(); 
    setupNetwork(mqttCallback); 
    
    Serial.println(">>> LIVINGROOM NODE READY <<<");
}

void loop() {
    maintainConnection();

    loopHardware();
}