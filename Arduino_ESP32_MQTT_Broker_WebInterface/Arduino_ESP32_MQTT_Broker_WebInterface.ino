// =================================================================================================
// ESP32 MQTT Broker mit integriertem Webinterface und AJAX-Datenaktualisierung
// =================================================================================================
// Dieses Programm verwandelt einen ESP32 in einen einfachen MQTT-Broker, der es IoT-Geräten
// ermöglicht, über das MQTT-Protokoll zu kommunizieren. Zusätzlich bietet es ein Webinterface,
// das den Status des Brokers, empfangene Nachrichten und verbundene Clients in Echtzeit anzeigt.
// Die Datenaktualisierung im Webinterface erfolgt dynamisch über AJAX.
//
// Bibliotheken:
// - WiFi.h: Für die WLAN-Konnektivität des ESP32.
// - WebServer.h: Zum Erstellen eines HTTP-Webservers auf dem ESP32.
// - vector: Für dynamische Arrays (z.B. Client-Listen, Topic-Teile).
// - map: Für Key-Value-Paare (z.B. Retained Messages).
// - algorithm: Für Algorithmen wie std::remove_if zum Entfernen von Elementen aus Vektoren.
// - ArduinoJson.h: Zum Serialisieren von Daten in das JSON-Format für das Webinterface.
//
// Hinweis: Die ArduinoJson-Bibliothek muss über den Bibliotheksmanager der Arduino IDE
//          installiert werden (Suchen Sie nach "ArduinoJson").
// =================================================================================================
#include <WiFi.h>
#include <WebServer.h>
#include <vector>
#include <map>
#include <algorithm>
#include <ArduinoJson.h>

// WLAN Parameter
const char* ssid = "FRITZ!Box 7590 RI";
const char* password = "52261156805385776967";

const int mqttPort = 1883;
WiFiServer mqttServer(mqttPort);
WebServer web(80);

struct Message {
    String topic;
    String payload;
    unsigned long timestamp;
};

struct MQTTClientState {
    WiFiClient client;
    String clientId;
    unsigned long lastSeen;
    String willTopic;
    String willMessage;
    bool hasLWT;
    bool connectedACKSent = false;
    std::vector<String> subscribedTopics;
};

// Die globale Liste aller verbundenen MQTT-Clients
std::vector<MQTTClientState> connectedMQTTClients;

// Die Struktur für Abonnements, die jetzt auf MQTTClientState zeigt
struct ActiveSubscription {
    MQTTClientState* clientState;
    String topicFilter;
};
std::vector<ActiveSubscription> activeSubscriptions;

std::map<String, String> retainedMessages;
std::vector<Message> messageLog;

// Hilfsfunktion: Warte auf bestimmte Anzahl Bytes im Client
bool waitForBytes(WiFiClient& client, int length, unsigned long timeout = 1000) {
    unsigned long start = millis();
    while (client.available() < length) {
        if (millis() - start > timeout) return false;
        delay(1);
    }
    return true;
}

// Hilfsfunktion: Zerlegt ein Topic in seine Segmente
std::vector<String> splitTopic(const String& topic) {
    std::vector<String> parts;
    int start = 0;
    int idx = topic.indexOf('/', start);
    while (idx != -1) {
        parts.push_back(topic.substring(start, idx));
        start = idx + 1;
        idx = topic.indexOf('/', start);
    }
    parts.push_back(topic.substring(start));
    return parts;
}

// Hilfsfunktion: Überprüft, ob ein Topic mit einem Filter übereinstimmt (für Wildcards)
bool topicMatches(const String& topic, const String& filter) {
    if (filter == "#") return true;

    std::vector<String> topicParts = splitTopic(topic);
    std::vector<String> filterParts = splitTopic(filter);

    if (!filterParts.empty() && filterParts.back() == "#") {
        if (topicParts.size() >= filterParts.size() - 1) {
            for (size_t i = 0; i < filterParts.size() - 1; ++i) {
                if (filterParts[i] == "+") continue;
                if (topicParts[i] != filterParts[i]) return false;
            }
            return true;
        } else {
            return false;
        }
    }

    if (topicParts.size() != filterParts.size()) return false;

    for (size_t i = 0; i < topicParts.size(); ++i) {
        if (filterParts[i] == "+") continue;
        if (topicParts[i] != filterParts[i]) return false;
    }
    return true;
}

// Handler für AJAX-Anfragen an /mqtt_data
void handleMqttData() {
    DynamicJsonDocument doc(2048);

    JsonArray messagesArray = doc.createNestedArray("messageLog");
    for (auto &msg : messageLog) {
        JsonObject msgObj = messagesArray.createNestedObject();
        msgObj["topic"] = msg.topic;
        msgObj["payload"] = msg.payload;
        msgObj["timestamp"] = msg.timestamp;
    }

    JsonObject retainedObj = doc.createNestedObject("retainedMessages");
    for (auto const& [topic, payload] : retainedMessages) {
        retainedObj[topic] = payload;
    }

    JsonArray clientsArray = doc.createNestedArray("connectedClients");
    for (auto &clientState : connectedMQTTClients) {
        JsonObject clientObj = clientsArray.createNestedObject();
        clientObj["id"] = clientState.clientId;
        clientObj["lastSeen"] = millis() - clientState.lastSeen;
        JsonArray subTopicsArray = clientObj.createNestedArray("subscribedTopics");
        for (const String& topic : clientState.subscribedTopics) {
            subTopicsArray.add(topic);
        }
    }

    doc["wifi_ssid"] = String(ssid);
    doc["wifi_ip"] = WiFi.localIP().toString();

    String jsonResponse;
    serializeJson(doc, jsonResponse);

    web.send(200, "application/json", jsonResponse);
}

// Webinterface HTML Seite
void handleRoot() {
    String html = "<!DOCTYPE html><html><head><meta charset='utf-8'><title>ESP32 MQTT Broker</title>";
    html += "<style>body{font-family:Arial;margin:20px;}table{border-collapse:collapse;width:100%;}";
    html += "th,td{border:1px solid #ccc;padding:8px;text-align:left;}</style>";
    html += "</head><body>";
    html += "<h1>MQTT Nachrichten Log</h1>";
    html += "<table id='messageLogTable'><thead><tr><th>Topic</th><th>Payload</th><th>Zuletzt empfangen</th></tr></thead><tbody></tbody></table>";

    html += "<h2>Retained Messages</h2><table id='retainedMessagesTable'><thead><tr><th>Topic</th><th>Payload</th></tr></thead><tbody></tbody></table>";

    html += "<h2>Connected MQTT Clients</h2><table id='connectedClientsTable'><thead><tr><th>Client ID</th><th>Last Seen (ms)</th><th>Subscribed Topics</th></tr></thead><tbody></tbody></table>";

    html += "<p>Verbunden mit: <span id='wifiSsid'></span><br>IP-Adresse: <span id='wifiIp'></span></p>";

    html += "<script>";
    html += "function fetchData() {";
    html += "    var xhr = new XMLHttpRequest();";
    html += "    xhr.onreadystatechange = function() {";
    html += "        if (xhr.readyState == 4 && xhr.status == 200) {";
    html += "            var data = JSON.parse(xhr.responseText);";

    html += "            var messageLogTableBody = document.getElementById('messageLogTable').getElementsByTagName('tbody')[0];";
    html += "            messageLogTableBody.innerHTML = '';";
    html += "            data.messageLog.forEach(function(item) {";
    html += "                var row = messageLogTableBody.insertRow();";
    html += "                row.insertCell(0).innerText = item.topic;";
    html += "                row.insertCell(1).innerText = item.payload;";
    html += "                row.insertCell(2).innerText = new Date(item.timestamp).toLocaleTimeString();";
    html += "            });";

    html += "            var retainedMessagesTableBody = document.getElementById('retainedMessagesTable').getElementsByTagName('tbody')[0];";
    html += "            retainedMessagesTableBody.innerHTML = '';";
    html += "            for (var topic in data.retainedMessages) {";
    html += "                if (data.retainedMessages.hasOwnProperty(topic)) {";
    html += "                    var row = retainedMessagesTableBody.insertRow();";
    html += "                    row.insertCell(0).innerText = topic;";
    html += "                    row.insertCell(1).innerText = data.retainedMessages[topic];";
    html += "                }";
    html += "            }";

    html += "            var connectedClientsTableBody = document.getElementById('connectedClientsTable').getElementsByTagName('tbody')[0];";
    html += "            connectedClientsTableBody.innerHTML = '';";
    html += "            data.connectedClients.forEach(function(item) {";
    html += "                var row = connectedClientsTableBody.insertRow();";
    html += "                row.insertCell(0).innerText = item.id;";
    html += "                row.insertCell(1).innerText = item.lastSeen + ' ms ago';";
    html += "                var topicsCell = row.insertCell(2);";
    html += "                item.subscribedTopics.forEach(function(topic) {";
    html += "                    topicsCell.innerHTML += topic + '<br>';";
    html += "                });";
    html += "            });";

    html += "            document.getElementById('wifiSsid').innerText = data.wifi_ssid;";
    html += "            document.getElementById('wifiIp').innerText = data.wifi_ip;";

    html += "        }";
    html += "    };";
    html += "    xhr.open('GET', '/mqtt_data', true);";
    html += "    xhr.send();";
    html += "}";
    html += "setInterval(fetchData, 2000);";
    html += "window.onload = fetchData;";
    html += "</script>";

    html += "</body></html>";

    web.send(200, "text/html", html);
}

void setupWebInterface() {
    web.on("/", handleRoot);
    web.on("/mqtt_data", handleMqttData);
    web.begin();
    Serial.println("HTTP server gestartet");
}

void setup() {
    Serial.begin(115200);
    WiFi.mode(WIFI_STA);
    WiFi.begin(ssid, password);
    Serial.print("Verbinde mit WLAN");
    while (WiFi.status() != WL_CONNECTED) {
        delay(500);
        Serial.print(".");
    }
    Serial.println();
    Serial.println("Verbunden mit WLAN!");
    Serial.print("IP-Adresse: ");
    Serial.println(WiFi.localIP());

    Serial.println("Scanne nach WLANs in der Nähe...");
    int n = WiFi.scanNetworks();
    if (n == 0) {
        Serial.println("Keine Netzwerke gefunden.");
    } else {
        for (int i = 0; i < n; ++i) {
            Serial.print(i + 1);
            Serial.print(": ");
            Serial.print(WiFi.SSID(i));
            Serial.print(" (RSSI: ");
            Serial.print(WiFi.RSSI(i));
            Serial.println(" dBm)");
        }
    }

    mqttServer.begin();
    Serial.printf("MQTT Broker läuft auf Port %d\n", mqttPort);

    setupWebInterface();
}

// Funktion zum Senden von MQTT Publish Paketen an Client
void sendPublish(WiFiClient &client, const String &topic, const String &payload, uint8_t qos = 0, bool retain = false, uint16_t packetId = 0) {
    if (!client.connected()) {
        Serial.println("Error: Tried to send PUBLISH to disconnected client.");
        return;
    }

    int len = topic.length();
    int payloadLen = payload.length();

    uint8_t fixedHeader = 0x30;
    fixedHeader |= (qos << 1);
    if (retain) fixedHeader |= 0x01;

    int remLen = 2 + len + payloadLen;
    if (qos > 0) {
        remLen += 2;
    }

    uint8_t encodedRemLen[4];
    int i = 0;
    do {
        uint8_t digit = remLen % 128;
        remLen /= 128;
        if (remLen > 0) {
            digit |= 0x80;
        }
        encodedRemLen[i++] = digit;
    } while (remLen > 0);

    client.write(fixedHeader);
    for (int j = 0; j < i; ++j) {
        client.write(encodedRemLen[j]);
    }

    client.write((len >> 8) & 0xFF);
    client.write(len & 0xFF);

    client.print(topic);

    if (qos > 0) {
        client.write((packetId >> 8) & 0xFF);
        client.write(packetId & 0xFF);
    }

    client.print(payload);
}

// Funktion zum Verarbeiten einer eingehenden PUBLISH-Nachricht
void handlePublish(MQTTClientState& clientState, byte header, byte remainingLength) {
    WiFiClient& client = clientState.client;
    uint8_t qos = (header >> 1) & 0x03;
    bool retain = (header & 0x01);

    if (!waitForBytes(client, remainingLength)) {
        Serial.println("Timeout bei PUBLISH Payload");
        return;
    }

    byte topicLenMsb = client.read();
    byte topicLenLsb = client.read();
    int topicLen = (topicLenMsb << 8) | topicLenLsb;

    char topicChar[topicLen + 1];
    for (int i = 0; i < topicLen; i++) {
        topicChar[i] = client.read();
    }
    topicChar[topicLen] = 0;
    String topic = String(topicChar);

    uint16_t packetId = 0;
    if (qos > 0) {
        byte packetIdMsb = client.read();
        byte packetIdLsb = client.read();
        packetId = (packetIdMsb << 8) | packetIdLsb;
    }

    int payloadLen = remainingLength - (2 + topicLen);
    if (qos > 0) payloadLen -= 2;

    String payload = "";
    if (payloadLen > 0) {
        char payloadChar[payloadLen + 1];
        for (int i = 0; i < payloadLen; i++) {
            payloadChar[i] = client.read();
        }
        payloadChar[payloadLen] = 0;
        payload = String(payloadChar);
    }

    Serial.printf("PUBLISH empfangen: Topic='%s', Payload='%s', QoS=%d, Retain=%d\n", topic.c_str(), payload.c_str(), qos, retain);

    Message msg = {topic, payload, millis()};
    messageLog.push_back(msg);
    if (messageLog.size() > 50) {
        messageLog.erase(messageLog.begin());
    }

    if (retain) {
        if (payload.length() == 0) {
            retainedMessages.erase(topic);
            Serial.printf("Retained message for topic '%s' cleared.\n", topic.c_str());
        } else {
            retainedMessages[topic] = payload;
            Serial.printf("Retained message for topic '%s' updated.\n", topic.c_str());
        }
    }

    for (auto &sub : activeSubscriptions) {
        if (sub.clientState->client.connected() && topicMatches(topic, sub.topicFilter)) {
            Serial.printf("Weiterleiten an Subscriber '%s' für Topic: %s\n", sub.clientState->clientId.c_str(), sub.topicFilter.c_str());
            sendPublish(sub.clientState->client, topic, payload, 0, retain);
        }
    }

    if (qos == 1) {
        byte puback[] = {0x40, 0x02, (byte)(packetId >> 8), (byte)(packetId & 0xFF)};
        client.write(puback, sizeof(puback));
        Serial.printf("PUBACK gesendet für Packet ID %d\n", packetId);
    }
}

void handleSubscribe(MQTTClientState& clientState, byte header, byte remainingLength) {
    WiFiClient& client = clientState.client;

    if (!waitForBytes(client, remainingLength)) {
        Serial.println("Timeout bei SUBSCRIBE Payload");
        return;
    }

    byte packetIdMsb = client.read();
    byte packetIdLsb = client.read();
    int packetId = (packetIdMsb << 8) | packetIdLsb;

    byte topicLenMsb = client.read();
    byte topicLenLsb = client.read();
    int topicLen = (topicLenMsb << 8) | topicLenLsb;

    char topicChar[topicLen + 1];
    for (int i = 0; i < topicLen; i++) {
        topicChar[i] = client.read();
    }
    topicChar[topicLen] = 0;
    String topic = String(topicChar);

    byte qos = client.read();

    Serial.printf("Client '%s' SUBSCRIBEd to Topic: %s with QoS %d\n", clientState.clientId.c_str(), topic.c_str(), qos);

    byte suback[] = {0x90, 0x03, packetIdMsb, packetIdLsb, qos};
    client.write(suback, sizeof(suback));
    Serial.println("SUBACK gesendet");

    ActiveSubscription newSub;
    newSub.clientState = &clientState;
    newSub.topicFilter = topic;
    activeSubscriptions.push_back(newSub);
    clientState.subscribedTopics.push_back(topic);

    for (auto const& [retainedTopic, retainedPayload] : retainedMessages) {
        if (topicMatches(retainedTopic, topic)) {
            Serial.printf("Sende Retained Message für Topic '%s' an neuen Subscriber '%s'.\n", retainedTopic.c_str(), clientState.clientId.c_str());
            sendPublish(client, retainedTopic, retainedPayload, 0, true);
        }
    }
}

void handlePingReq(MQTTClientState& clientState) {
    WiFiClient& client = clientState.client;
    byte pingresp[] = {0xD0, 0x00};
    client.write(pingresp, sizeof(pingresp));
    Serial.printf("PINGRESP gesendet an Client '%s'\n", clientState.clientId.c_str());
    clientState.lastSeen = millis();
}

void processMQTTClient(MQTTClientState& clientState) {
    WiFiClient& client = clientState.client;

    if (!client.connected()) {
        return;
    }

    clientState.lastSeen = millis();

    if (!clientState.connectedACKSent) {
        if (!waitForBytes(client, 2)) {
            return;
        }

        byte header = client.read();
        byte remainingLength = client.read();

        if (header != 0x10) {
            Serial.printf("Client '%s' sent non-CONNECT first: 0x%X. Disconnecting.\n", clientState.clientId.c_str(), header);
            client.stop();
            return;
        }

        if (!waitForBytes(client, remainingLength)) {
            Serial.printf("Timeout processing CONNECT payload for client '%s'. Disconnecting.\n", clientState.clientId.c_str());
            client.stop();
            return;
        }

        for (int i = 0; i < 8; ++i) client.read();

        byte clientIdLenHi = client.read();
        byte clientIdLenLo = client.read();
        int clientIdLen = (clientIdLenHi << 8) | clientIdLenLo;

        char clientId[clientIdLen + 1];
        for (int i = 0; i < clientIdLen; i++) {
            clientId[i] = client.read();
        }
        clientId[clientIdLen] = 0;
        clientState.clientId = String(clientId);

        // --- NEUE DEBUG-AUSGABE START ---
        if (clientState.clientId.length() == 0) {
            Serial.printf("DEBUG: Client verbunden OHNE ID (Länge 0). Remote IP: %s\n", client.remoteIP().toString().c_str());
            // Optional: Generieren Sie hier eine ID, wenn gewünscht
            // clientState.clientId = "AutoGen_" + String(client.remoteIP()[3]) + "_" + String(millis() % 1000);
        } else {
            Serial.printf("DEBUG: Client '%s' verbunden MIT ID. Remote IP: %s\n", clientState.clientId.c_str(), client.remoteIP().toString().c_str());
        }
        // --- NEUE DEBUG-AUSGABE ENDE ---

        Serial.printf("Client '%s' CONNECTED. Sending CONNACK.\n", clientState.clientId.c_str());

        byte connack[] = {0x20, 0x02, 0x00, 0x00};
        client.write(connack, sizeof(connack));
        clientState.connectedACKSent = true;
    }

    if (client.available() >= 2) {
      byte h = client.read();
      byte len = client.read();

      int packetType = h >> 4;

      switch (packetType) {
        case 3: // PUBLISH
          handlePublish(clientState, h, len);
          break;
        case 8: // SUBSCRIBE
          handleSubscribe(clientState, h, len);
          break;
        case 12: // PINGREQ
            handlePingReq(clientState);
            break;
        case 14: // DISCONNECT
            Serial.printf("Client '%s' DISCONNECT requested. Closing connection.\n", clientState.clientId.c_str());
            client.stop();
            break;
        default:
          Serial.printf("Client '%s': Unknown packet type 0x%X, length %d. Disconnecting.\n", clientState.clientId.c_str(), packetType, len);
          for (int i = 0; i < len && client.available(); ++i) client.read();
          client.stop();
          break;
      }
    }
}

void loop() {
    web.handleClient();

    WiFiClient newClient = mqttServer.available();
    if (newClient) {
        Serial.println("Neue TCP-Verbindung erkannt.");
        MQTTClientState newState;
        newState.client = newClient;
        newState.lastSeen = millis();
        connectedMQTTClients.push_back(newState);
        Serial.printf("Anzahl verbundener MQTT-Clients: %d\n", connectedMQTTClients.size());
    }

    for (int i = connectedMQTTClients.size() - 1; i >= 0; --i) {
        if (connectedMQTTClients[i].client.connected()) {
            processMQTTClient(connectedMQTTClients[i]);
        } else {
            Serial.printf("Client '%s' (%s) hat die Verbindung getrennt. Entferne ihn.\n",
                            connectedMQTTClients[i].clientId.c_str(),
                            connectedMQTTClients[i].client.remoteIP().toString().c_str());

            activeSubscriptions.erase(std::remove_if(activeSubscriptions.begin(), activeSubscriptions.end(),
                                                     [&](const ActiveSubscription& sub) {
                                                         return sub.clientState == &connectedMQTTClients[i];
                                                     }),
                                     activeSubscriptions.end());

            connectedMQTTClients.erase(connectedMQTTClients.begin() + i);
        }
    }
}