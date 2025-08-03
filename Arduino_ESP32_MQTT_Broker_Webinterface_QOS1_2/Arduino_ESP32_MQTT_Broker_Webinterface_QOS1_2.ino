// =================================================================================================
// ESP32 MQTT Broker mit integriertem Webinterface und AJAX-Datenaktualisierung
// =================================================================================================
// Dieses Programm verwandelt einen ESP32 in einen einfachen MQTT-Broker, der es IoT-Geraeten
// ermöglicht, ueber das MQTT-Protokoll zu kommunizieren. Zusaetzlich bietet es ein Webinterface,
// das den Status des Brokers, empfangene Nachrichten und verbundene Clients in Echtzeit anzeigt.
// Die Datenaktualisierung im Webinterface erfolgt dynamisch ueber AJAX.
//
// Bibliotheken:
// - WiFi.h: fuer die WLAN-Konnektivitaet des ESP32.
// - WebServer.h: Zum Erstellen eines HTTP-Webservers auf dem ESP32.
// - vector: fuer dynamische Arrays (z.B. Client-Listen, Topic-Teile).
// - map: fuer Key-Value-Paare (z.B. Retained Messages).
// - algorithm: fuer Algorithmen wie std::remove_if zum Entfernen von Elementen aus Vektoren.
// - ArduinoJson.h: Zum Serialisieren von Daten in das JSON-Format fuer das Webinterface.
//
// Hinweis: Die ArduinoJson-Bibliothek muss ueber den Bibliotheksmanager der Arduino IDE
//          installiert werden (Suchen Sie nach "ArduinoJson").
//          Die ArduinoSTL-Bibliothek muss ebenfalls über den Bibliotheksmanager der Arduino IDE
//          installiert werden (Standard C++ for Arduino” von Michael C. Morris oder 
//          eine ähnliche STL-Portierung) wird für die Includes vector, map und algorithm benötigt.
//
// Autor:  Josef Bernhardt www.bernhardt.de
//
// Hardware: ESP32 DEV Modul
//
// Letzte Änderung: 01.08.2025
// =================================================================================================
#include <WiFi.h>
#include <WebServer.h>
#include <vector>        // V.1.3.3
#include <map>
#include <algorithm>
#include <ArduinoJson.h> // V.7.4.2

// WLAN Parameter
const char* ssid = "Sunrise_2.4GHz_19B4C2";
const char* password = "u2uxxxxxxx1Ds";

const int mqttPort = 1883;
WiFiServer mqttServer(mqttPort);
WebServer web(80);

// Struktur für geloggte Nachrichten
struct Message {
    String topic;
    String payload;
    unsigned long timestamp;
};

// Struktur für den Zustand eines verbundenen MQTT-Clients
struct MQTTClientState {
    WiFiClient client;
    String clientId;
    unsigned long lastSeen;
    String willTopic;
    String willMessage;
    bool hasLWT;
    std::vector<String> subscribedTopics;
};

// Die globale Liste aller verbundenen MQTT-Clients
std::vector<MQTTClientState> connectedMQTTClients;

// Die Struktur für Abonnements
struct ActiveSubscription {
    MQTTClientState* clientState;
    String topicFilter;
};
std::vector<ActiveSubscription> activeSubscriptions;

// Speicherung von Retained Messages
std::map<String, String> retainedMessages;
// Log der letzten empfangenen Nachrichten für das Webinterface
std::vector<Message> messageLog;

// Struktur zur Speicherung von QoS 2 Nachrichten
struct QoS2Message {
    String topic;
    String payload;
};
std::map<String, QoS2Message> pendingQoS2;


// Hilfsfunktion: Warte auf bestimmte Anzahl Bytes im Client
bool waitForBytes(WiFiClient& client, int length, unsigned long timeout = 1000) {
    unsigned long start = millis();
    while (client.available() < length) {
        if (millis() - start > timeout) {
            Serial.printf("Timeout: Erwartete %d Bytes, aber nur %d verfuegbar.\n", length, client.available());
            return false;
        }
        delay(1);
    }
    return true;
}

// Hilfsfunktion: Variable Remaining Length decodieren
int decodeRemainingLength(WiFiClient& client) {
    int multiplier = 1;
    int value = 0;
    uint8_t encodedByte;
    do {
        if (!waitForBytes(client, 1)) return -1;
        encodedByte = client.read();
        value += (encodedByte & 127) * multiplier;
        multiplier *= 128;
        if (multiplier > 128 * 128 * 128) {
            return -1; // Fehler: Ungültige Remaining Length
        }
    } while ((encodedByte & 128) != 0);
    return value;
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

// Hilfsfunktion: überprüft, ob ein Topic mit einem Filter übereinstimmt
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
    DynamicJsonDocument doc(4096);

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
    html += "<style>";
    html += "body{font-family:Arial;margin:20px;background-color:#f4f7f6;color:#333;}";
    html += "h1,h2{color:#0056b3;}";
    html += "table{border-collapse:collapse;width:100%;margin-bottom:20px;box-shadow:0 2px 5px rgba(0,0,0,0.1);border-radius:8px;overflow:hidden;}";
    html += "th,td{border:1px solid #e0e0e0;padding:12px;text-align:left;}";
    html += "th{background-color:#e9ecef;font-weight:bold;color:#555;}";
    html += "tr:nth-child(even){background-color:#f9f9f9;}";
    html += "tr:hover{background-color:#e2f0ff;}";
    html += "p{margin-top:20px;font-size:0.9em;color:#666;}";
    html += "</style>";
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

    mqttServer.begin();
    Serial.printf("MQTT Broker laeuft auf Port %d\n", mqttPort);

    setupWebInterface();
}

void sendPublish(WiFiClient &client, const String &topic, const String &payload, uint8_t qos = 0, bool retain = false, uint16_t packetId = 0) {
    if (!client.connected()) {
        Serial.println("Fehler: Versuch, PUBLISH an getrennten Client zu senden.");
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

void distributeMessage(const String& topic, const String& payload, bool retain) {
    Message msg = {topic, payload, millis()};
    messageLog.push_back(msg);
    if (messageLog.size() > 50) {
        messageLog.erase(messageLog.begin());
    }

    if (retain) {
        if (payload.length() == 0) {
            retainedMessages.erase(topic);
            Serial.printf("Retained message fuer Topic '%s' gelöscht.\n", topic.c_str());
        } else {
            retainedMessages[topic] = payload;
            Serial.printf("Retained message fuer Topic '%s' aktualisiert.\n", topic.c_str());
        }
    }

    for (auto &sub : activeSubscriptions) {
        if (sub.clientState->client.connected() && topicMatches(topic, sub.topicFilter)) {
            Serial.printf("Weiterleiten an Subscriber '%s' fuer Topic: %s\n", sub.clientState->clientId.c_str(), sub.topicFilter.c_str());
            sendPublish(sub.clientState->client, topic, payload, 0, retain);
        }
    }
}

void handlePublish(MQTTClientState& clientState, byte header, int remainingLength) {
    WiFiClient& client = clientState.client;
    uint8_t qos = (header >> 1) & 0x03;
    bool retain = (header & 0x01);
    bool dup = (header >> 3) & 0x01;

    int expectedBytes = remainingLength;
    if (!waitForBytes(client, expectedBytes)) {
        Serial.println("Timeout beim Lesen der PUBLISH Payload.");
        return;
    }

    int topicLen = (client.read() << 8) | client.read();
    if (topicLen < 0 || topicLen > (remainingLength - 2)) {
        Serial.printf("Fehler: Ungültige Topic-Länge (%d) im PUBLISH Paket. Trenne Client.\n", topicLen);
        client.stop();
        return;
    }

    char topicChar[topicLen + 1];
    for (int i = 0; i < topicLen; i++) {
        topicChar[i] = client.read();
    }
    topicChar[topicLen] = 0;
    String topic = String(topicChar);

    int payloadLen = remainingLength - (2 + topicLen);
    uint16_t packetId = 0;
    if (qos > 0) {
        if (payloadLen < 2) {
            Serial.println("Fehler: QoS-Paket ohne Packet ID. Trenne Client.");
            client.stop();
            return;
        }
        packetId = (client.read() << 8) | client.read();
        payloadLen -= 2;
    }

    String payload = "";
    if (payloadLen > 0) {
        char payloadChar[payloadLen + 1];
        for (int i = 0; i < payloadLen; i++) {
            payloadChar[i] = client.read();
        }
        payloadChar[payloadLen] = 0;
        payload = String(payloadChar);
    }

    Serial.printf("PUBLISH empfangen von '%s': Topic='%s', Payload='%s', QoS=%d, Retain=%d, DUP=%d, PacketID=%d\n",
                  clientState.clientId.c_str(), topic.c_str(), payload.c_str(), qos, retain, dup, packetId);

    if (qos == 2) {
        byte pubrec[] = {0x50, 0x02, (byte)(packetId >> 8), (byte)(packetId & 0xFF)};
        client.write(pubrec, sizeof(pubrec));
        Serial.printf("PUBREC gesendet fuer Packet ID %d an Client '%s'\n", packetId, clientState.clientId.c_str());

        String key = clientState.clientId + "-" + String(packetId);
        if (!dup || !pendingQoS2.count(key)) {
            pendingQoS2[key] = {topic, payload};
            Serial.printf("QoS 2 Nachricht (Packet ID %d) von '%s' zur Bearbeitung vorgemerkt.\n", packetId, clientState.clientId.c_str());
        } else {
            Serial.printf("QoS 2 Nachricht (Packet ID %d) von '%s' ist eine DUP-Nachricht, wurde bereits vorgemerkt.\n", packetId, clientState.clientId.c_str());
        }
        return;
    } else {
        distributeMessage(topic, payload, retain);
        if (qos == 1) {
            byte puback[] = {0x40, 0x02, (byte)(packetId >> 8), (byte)(packetId & 0xFF)};
            client.write(puback, sizeof(puback));
            Serial.printf("PUBACK gesendet fuer Packet ID %d an Client '%s'\n", packetId, clientState.clientId.c_str());
        }
    }
}

void handlePubRel(MQTTClientState& clientState, int remainingLength) {
    WiFiClient& client = clientState.client;
    if (!waitForBytes(client, 2)) return;
    uint16_t packetId = (client.read() << 8) | client.read();
    Serial.printf("PUBREL empfangen von '%s' fuer Packet ID %d.\n", clientState.clientId.c_str(), packetId);

    byte pubcomp[] = {0x70, 0x02, (byte)(packetId >> 8), (byte)(packetId & 0xFF)};
    client.write(pubcomp, sizeof(pubcomp));
    Serial.printf("PUBCOMP gesendet fuer Packet ID %d an Client '%s'.\n", packetId, clientState.clientId.c_str());

    String key = clientState.clientId + "-" + String(packetId);
    if (pendingQoS2.count(key)) {
        auto msg = pendingQoS2[key];
        distributeMessage(msg.topic, msg.payload, false);
        pendingQoS2.erase(key);
        Serial.printf("QoS 2 Nachricht (Packet ID %d) von '%s' erfolgreich verarbeitet und verteilt.\n", packetId, clientState.clientId.c_str());
    } else {
        Serial.printf("Fehler: PUBREL fuer unbekannte Packet ID %d von Client '%s' empfangen.\n", packetId, clientState.clientId.c_str());
    }
}

void handleSubscribe(MQTTClientState& clientState, byte header, int remainingLength) {
    WiFiClient& client = clientState.client;
    if (!waitForBytes(client, remainingLength)) return;

    uint16_t packetId = (client.read() << 8) | client.read();
    
    int bytesRead = 2;
    while(bytesRead < remainingLength) {
        int topicLen = (client.read() << 8) | client.read();
        if (!waitForBytes(client, topicLen)) return;
        char topicChar[topicLen + 1];
        for (int i = 0; i < topicLen; i++) {
            topicChar[i] = client.read();
        }
        topicChar[topicLen] = 0;
        String topic = String(topicChar);
        byte qos = client.read();

        Serial.printf("Client '%s' SUBSCRIBEd to Topic: %s with requested QoS %d\n", clientState.clientId.c_str(), topic.c_str(), qos);

        ActiveSubscription newSub;
        newSub.clientState = &clientState;
        newSub.topicFilter = topic;
        activeSubscriptions.push_back(newSub);
        clientState.subscribedTopics.push_back(topic);

        // Sende Retained Messages
        for (auto const& [retainedTopic, retainedPayload] : retainedMessages) {
            if (topicMatches(retainedTopic, topic)) {
                sendPublish(client, retainedTopic, retainedPayload, 0, true);
            }
        }
        bytesRead += 2 + topicLen + 1;
    }
    
    // Sende SUBACK
    byte suback[] = {0x90, 0x03, (byte)(packetId >> 8), (byte)(packetId & 0xFF), 0x00}; // Return Code 0x00 für Success QoS 0
    client.write(suback, sizeof(suback));
    Serial.printf("SUBACK gesendet fuer Packet ID %d an Client '%s'\n", packetId, clientState.clientId.c_str());
}

void handlePingReq(MQTTClientState& clientState) {
    byte pingresp[] = {0xD0, 0x00};
    clientState.client.write(pingresp, sizeof(pingresp));
    Serial.printf("PINGRESP gesendet an Client '%s'.\n", clientState.clientId.c_str());
}

void handleDisconnect(MQTTClientState& clientState) {
    Serial.printf("Client '%s' hat eine DISCONNECT-Nachricht gesendet. Trenne die Verbindung.\n", clientState.clientId.c_str());
    clientState.client.stop();
}

void handlePubAck(MQTTClientState& clientState, int remainingLength) {
    if (!waitForBytes(clientState.client, 2)) return;
    uint16_t packetId = (clientState.client.read() << 8) | clientState.client.read();
    Serial.printf("PUBACK empfangen von '%s' fuer Packet ID %d.\n", clientState.clientId.c_str(), packetId);
}

void handlePubComp(MQTTClientState& clientState, int remainingLength) {
    if (!waitForBytes(clientState.client, 2)) return;
    uint16_t packetId = (clientState.client.read() << 8) | clientState.client.read();
    Serial.printf("PUBCOMP empfangen von '%s' fuer Packet ID %d.\n", clientState.clientId.c_str(), packetId);
}

void handlePubRec(MQTTClientState& clientState, int remainingLength) {
    if (!waitForBytes(clientState.client, 2)) return;
    uint16_t packetId = (clientState.client.read() << 8) | clientState.client.read();
    Serial.printf("PUBREC empfangen von '%s' fuer Packet ID %d.\n", clientState.clientId.c_str(), packetId);
}

void removeClient(int index) {
    MQTTClientState& clientState = connectedMQTTClients[index];
    if (clientState.hasLWT) {
        Serial.printf("Client '%s' hat die Verbindung unerwartet getrennt. Sende LWT-Nachricht.\n", clientState.clientId.c_str());
        distributeMessage(clientState.willTopic, clientState.willMessage, false);
    }
    
    activeSubscriptions.erase(std::remove_if(activeSubscriptions.begin(), activeSubscriptions.end(),
                                             [&](const ActiveSubscription& sub) {
                                                 return sub.clientState->clientId == clientState.clientId;
                                             }),
                              activeSubscriptions.end());
    
    connectedMQTTClients.erase(connectedMQTTClients.begin() + index);
    Serial.printf("Client '%s' (IP: %s) hat die Verbindung getrennt. Anzahl verbundener MQTT-Clients: %d\n",
                  clientState.clientId.c_str(), clientState.client.remoteIP().toString().c_str(), connectedMQTTClients.size());
}


void handleConnect(WiFiClient& client, byte header, int remainingLength) {
    if (!waitForBytes(client, remainingLength)) {
        Serial.println("Timeout beim Lesen der CONNECT-Nachricht.");
        return;
    }

    int bytesRead = 0;
    // Protocol Name
    int protocolNameLen = (client.read() << 8) | client.read();
    bytesRead += 2;
    for (int i = 0; i < protocolNameLen; ++i) { client.read(); }
    bytesRead += protocolNameLen;
    // Protocol Version
    client.read(); // version
    bytesRead++;
    // Connect Flags
    byte connectFlags = client.read();
    bytesRead++;
    // Keep Alive
    client.read(); client.read(); // keep alive
    bytesRead += 2;

    // Client ID
    int clientIdLen = (client.read() << 8) | client.read();
    bytesRead += 2;
    if (!waitForBytes(client, clientIdLen)) { client.stop(); return; }
    char clientIdChar[clientIdLen + 1];
    for(int i = 0; i < clientIdLen; ++i) { clientIdChar[i] = client.read(); }
    clientIdChar[clientIdLen] = 0;
    String clientId = String(clientIdChar);
    bytesRead += clientIdLen;

    // LWT Topic und Message, falls vorhanden
    String willTopic = "";
    String willMessage = "";
    bool hasLWT = (connectFlags & 0x04) != 0;
    if (hasLWT) {
        int willTopicLen = (client.read() << 8) | client.read();
        bytesRead += 2;
        if (!waitForBytes(client, willTopicLen)) { client.stop(); return; }
        char willTopicChar[willTopicLen + 1];
        for(int i = 0; i < willTopicLen; ++i) { willTopicChar[i] = client.read(); }
        willTopicChar[willTopicLen] = 0;
        willTopic = String(willTopicChar);
        bytesRead += willTopicLen;

        int willMessageLen = (client.read() << 8) | client.read();
        bytesRead += 2;
        if (!waitForBytes(client, willMessageLen)) { client.stop(); return; }
        char willMessageChar[willMessageLen + 1];
        for(int i = 0; i < willMessageLen; ++i) { willMessageChar[i] = client.read(); }
        willMessageChar[willMessageLen] = 0;
        willMessage = String(willMessageChar);
        bytesRead += willMessageLen;
    }
    
    // Benutzernamen und Passwort, falls vorhanden (werden hier ignoriert)
    if (connectFlags & 0x80) { // Has Username
        int usernameLen = (client.read() << 8) | client.read();
        if (!waitForBytes(client, usernameLen)) { client.stop(); return; }
        for(int i = 0; i < usernameLen; ++i) { client.read(); }
        bytesRead += 2 + usernameLen;
    }
    if (connectFlags & 0x40) { // Has Password
        int passwordLen = (client.read() << 8) | client.read();
        if (!waitForBytes(client, passwordLen)) { client.stop(); return; }
        for(int i = 0; i < passwordLen; ++i) { client.read(); }
        bytesRead += 2 + passwordLen;
    }

    MQTTClientState newState;
    newState.client = client;
    newState.clientId = clientId;
    newState.lastSeen = millis();
    newState.hasLWT = hasLWT;
    newState.willTopic = willTopic;
    newState.willMessage = willMessage;
    connectedMQTTClients.push_back(newState);
    
    byte connack[] = {0x20, 0x02, 0x00, 0x00};
    client.write(connack, sizeof(connack));
    Serial.printf("Client '%s' CONNECTED. Sende CONNACK.\n", clientId.c_str());
}

void loop() {
    web.handleClient();

    WiFiClient newClient = mqttServer.available();
    if (newClient) {
        Serial.println("Neue TCP-Verbindung erkannt.");
        if (newClient.connected()) {
            if (!waitForBytes(newClient, 1)) {
                newClient.stop();
                return;
            }
            byte header = newClient.peek();
            byte packetType = (header >> 4) & 0x0F;
            if (packetType == 0x01) { // CONNECT
                newClient.read(); // Header lesen
                int remainingLength = decodeRemainingLength(newClient);
                if (remainingLength > 0) {
                    handleConnect(newClient, header, remainingLength);
                } else {
                    Serial.println("Fehler: Ungültige Remaining Length im CONNECT-Paket.");
                    newClient.stop();
                }
            } else {
                Serial.println("Erstes Paket ist kein CONNECT. Verbindung getrennt.");
                newClient.stop();
            }
        }
    }

    for (int i = 0; i < connectedMQTTClients.size(); ++i) {
        MQTTClientState& clientState = connectedMQTTClients[i];
        WiFiClient& client = clientState.client;

        if (client.connected()) {
            if (client.available()) {
                byte header = client.read();
                int remainingLength = decodeRemainingLength(client);
                if (remainingLength == -1) {
                    Serial.printf("Fehler: Ungültige Remaining Length von Client '%s'. Trenne Verbindung.\n", clientState.clientId.c_str());
                    client.stop();
                    continue;
                }

                clientState.lastSeen = millis();
                byte packetType = (header >> 4) & 0x0F;

                switch (packetType) {
                    case 0x03: handlePublish(clientState, header, remainingLength); break;
                    case 0x08: handleSubscribe(clientState, header, remainingLength); break;
                    case 0x0C: handlePingReq(clientState); break;
                    case 0x0E: handleDisconnect(clientState); break;
                    case 0x04: handlePubAck(clientState, remainingLength); break;
                    case 0x05: handlePubRec(clientState, remainingLength); break;
                    case 0x06: handlePubRel(clientState, remainingLength); break;
                    case 0x07: handlePubComp(clientState, remainingLength); break;
                    default:
                        Serial.printf("Unbekannter MQTT-Pakettyp: 0x%02X von Client '%s'. Trenne.\n", packetType, clientState.clientId.c_str());
                        client.stop();
                        break;
                }
            }
        } else {
            removeClient(i);
            i--;
        }
    }
}
