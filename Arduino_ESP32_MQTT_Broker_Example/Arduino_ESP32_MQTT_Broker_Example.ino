// Arduino_ESP32_MQTT_Broker_Example.ino
// created by Josef Bernhardt 2025-07-31 josef@bernhardt.de

#include <WiFi.h>
#include <vector>
#include <map>

const char* ssid = "DEIN_SSID";
const char* password = "DEIN_PASSWORT";

const int mqttPort = 1883;
WiFiServer server(mqttPort);

struct Subscriber {
  WiFiClient client;
  String topic;
};

std::vector<Subscriber> subscribers;
std::map<String, String> retainedMessages;

void setup() {
  Serial.begin(115200);
  WiFi.begin(ssid, password);

  Serial.print("Verbinde mit WLAN...");
  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
    Serial.print(".");
  }
  Serial.println("\nVerbunden! IP: " + WiFi.localIP().toString());

  server.begin();
  Serial.println("MQTT-Broker läuft auf Port 1883");
}

void loop() {
  WiFiClient client = server.available();

  if (client) {
    Serial.println("Client verbunden");

    // CONNECT lesen
    while (client.connected() && !client.available()) delay(10);

    int header = client.read();
    if (header != 0x10) {
      Serial.println("Kein CONNECT-Paket.");
      client.stop();
      return;
    }

    int remLength = client.read();
    byte buffer[remLength];
    client.read(buffer, remLength);

    // CONNACK senden
    byte connAck[] = {0x20, 0x02, 0x00, 0x00};
    client.write(connAck, 4);
    Serial.println("CONNACK gesendet");

    while (client.connected()) {
      if (!client.available()) {
        delay(10);
        continue;
      }

      int byte1 = client.read();
      if (byte1 == -1) break;

      int packetType = byte1 >> 4;

      // Remaining Length
      int remLen = client.read();
      if (remLen <= 0 || remLen > 127) break;

      byte packet[remLen];
      client.read(packet, remLen);

      int pos = 0;

      if (packetType == 3) {  // PUBLISH
        bool retain = byte1 & 0x01;
        int qos = (byte1 >> 1) & 0x03;

        int topicLen = (packet[pos] << 8) | packet[pos + 1];
        pos += 2;

        String topic = "";
        for (int i = 0; i < topicLen; i++) {
          topic += (char)packet[pos++];
        }

        int packetId = 0;
        if (qos > 0) {
          packetId = (packet[pos] << 8) | packet[pos + 1];
          pos += 2;
        }

        String payload = "";
        while (pos < remLen) {
          payload += (char)packet[pos++];
        }

        Serial.print("[PUBLISH] Topic: ");
        Serial.print(topic);
        Serial.print(", Payload: ");
        Serial.println(payload);

        if (retain) {
          retainedMessages[topic] = payload;
          Serial.println("Retained gespeichert für: " + topic);
        }

        if (qos == 1) {
          byte puback[] = {0x40, 0x02, (byte)(packetId >> 8), (byte)(packetId & 0xFF)};
          client.write(puback, 4);
          Serial.println("PUBACK gesendet");
        }

        for (auto& sub : subscribers) {
          if (sub.client.connected() && topicMatches(sub.topic, topic)) {
            String out = "";
            out += (char)0x30; // PUBLISH Header, QoS 0
            int len = 2 + topic.length() + payload.length();
            out += (char)len;
            out += (char)(topic.length() >> 8);
            out += (char)(topic.length() & 0xFF);
            out += topic;
            out += payload;

            sub.client.write((const uint8_t*)out.c_str(), out.length());
            Serial.println("➜ Nachricht an Subscriber gesendet: " + sub.topic);
          }
        }
      }
      else if (packetType == 8) {  // SUBSCRIBE
        int packetId = (packet[pos] << 8) | packet[pos + 1];
        pos += 2;

        int topicLen = (packet[pos] << 8) | packet[pos + 1];
        pos += 2;

        String topic = "";
        for (int i = 0; i < topicLen; i++) {
          topic += (char)packet[pos++];
        }

        byte qos = packet[pos]; // angeforderter QoS

        // Subscriber merken
        subscribers.push_back({client, topic});
        Serial.println("Client abonnierte: " + topic);

        // SUBACK senden
        byte suback[] = {0x90, 0x03, (byte)(packetId >> 8), (byte)(packetId & 0xFF), qos};
        client.write(suback, 5);
        Serial.println("SUBACK gesendet");

        // Retained Message sofort senden, falls vorhanden
        for (auto& [retTopic, message] : retainedMessages) {
          if (topicMatches(topic, retTopic)) {
            String out = "";
            out += (char)0x31; // PUBLISH mit RETAIN=1, QoS 0
            int len = 2 + retTopic.length() + message.length();
            out += (char)len;
            out += (char)(retTopic.length() >> 8);
            out += (char)(retTopic.length() & 0xFF);
            out += retTopic;
            out += message;

            client.write((const uint8_t*)out.c_str(), out.length());
            Serial.println("➜ Retained an neuen Subscriber gesendet: " + retTopic);
          }
        }
      }
      else {
        Serial.println("Unbekannter Pakettyp: " + String(packetType));
      }
    }

    client.stop();
    Serial.println("Client getrennt");
  }
}

// 🔁 Wildcard-Matching (z. B. sensor/#, foo/+)
bool topicMatches(const String& sub, const String& pub) {
  std::vector<String> s = splitTopic(sub);
  std::vector<String> p = splitTopic(pub);

  for (size_t i = 0; i < s.size(); i++) {
    if (i >= p.size()) return false;
    if (s[i] == "#") return true;
    if (s[i] == "+") continue;
    if (s[i] != p[i]) return false;
  }

  return s.size() == p.size();
}

std::vector<String> splitTopic(const String& topic) {
  std::vector<String> parts;
  String part = "";
  for (size_t i = 0; i < topic.length(); i++) {
    if (topic[i] == '/') {
      parts.push_back(part);
      part = "";
    } else {
      part += topic[i];
    }
  }
  parts.push_back(part);
  return parts;
}
