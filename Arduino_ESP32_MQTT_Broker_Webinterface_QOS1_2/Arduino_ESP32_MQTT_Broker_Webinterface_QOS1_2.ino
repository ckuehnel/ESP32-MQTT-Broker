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

// =================================================================================================
// GLOBALE KONSTANTEN UND STRUKTUREN
// =================================================================================================

// Timeout für QoS-Nachrichten-Retransmissionen (in Millisekunden)
const unsigned long QoS_TIMEOUT_MS = 5000;
// Maximale Anzahl von Wiederholungsversuchen für QoS-Nachrichten
const int MAX_QOS_RETRIES = 3;

// Struktur für das Nachrichten-Log im Webinterface
struct Message {
    String topic;
    String payload;
    unsigned long timestamp;
};

// Struktur zur Verfolgung von ausgehenden QoS 1/2 Nachrichten
struct OutgoingMessage {
    String topic;
    String payload;
    uint8_t qos;
    bool retain;
    uint16_t packetId;
    unsigned long timestampSent; // Zeitpunkt des letzten Sendens
    uint8_t retryCount;          // Anzahl der Wiederholungsversuche
    uint8_t state;               // 0: PUBLISH gesendet, 1: PUBREC gesendet (nur für QoS 2)
};

// Struktur zur Verfolgung von eingehenden QoS 2 Nachrichten
struct IncomingQoS2State {
    String topic;
    String payload;
    uint8_t qos; // QoS des eingehenden PUBLISH
    uint16_t packetId;
    unsigned long timestampReceived; // Zeitpunkt des Empfangs des PUBLISH
    uint8_t state;                   // 0: PUBLISH empfangen, 1: PUBREC gesendet, 2: PUBREL empfangen
};

// Struktur für den Zustand jedes verbundenen MQTT-Clients
struct MQTTClientState {
    WiFiClient client;
    String clientId;
    unsigned long lastSeen; // Letzter Zeitpunkt der Client-Aktivität (für Keep-Alive)
    String willTopic;
    String willMessage;
    bool hasLWT;
    bool connectedACKSent = false; // Flag, ob CONNACK bereits gesendet wurde
    std::vector<String> subscribedTopics; // Liste der abonnierten Topics
    uint16_t clientKeepAliveSec = 0; // Keep-Alive-Intervall vom Client (in Sekunden)

    // QoS-spezifische Zustände
    std::map<uint16_t, OutgoingMessage> outgoingQoSMessages; // Nachrichten, die vom Broker an diesen Client gesendet wurden und auf Bestätigung warten
    std::map<uint16_t, IncomingQoS2State> incomingQoS2Messages; // Nachrichten, die von diesem Client mit QoS 2 empfangen wurden und auf Handshake-Abschluss warten
    uint16_t nextOutgoingPacketId = 1; // Zähler für Packet IDs, die dieser Broker für diesen Client vergibt (für ausgehende PUBLISH)
};

// Die globale Liste aller verbundenen MQTT-Clients
std::vector<MQTTClientState> connectedMQTTClients;

// Die Struktur für Abonnements, die jetzt auf MQTTClientState zeigt
struct ActiveSubscription {
    MQTTClientState* clientState;
    String topicFilter;
    uint8_t qos; // Der vom Subscriber gewünschte QoS für dieses Abonnement
};
std::vector<ActiveSubscription> activeSubscriptions;

// Speicherung von Retained Messages
std::map<String, String> retainedMessages;
// Log der letzten empfangenen Nachrichten für das Webinterface
std::vector<Message> messageLog;

// =================================================================================================
// HILFSFUNKTIONEN
// =================================================================================================

// Hilfsfunktion: Warte auf bestimmte Anzahl Bytes im Client
bool waitForBytes(WiFiClient& client, int length, unsigned long timeout = 1000) {
    unsigned long start = millis();
    while (client.available() < length) {
        if (!client.connected()) return false; // Client disconnected while waiting
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

// Funktion zum Lesen einer Längen-präfixierten String aus dem Client-Stream
String readLengthPrefixedString(WiFiClient& client) {
    if (!waitForBytes(client, 2)) return "";
    byte lenMsb = client.read();
    byte lenLsb = client.read();
    int len = (lenMsb << 8) | lenLsb;

    if (len == 0) return ""; // Leere Strings sind erlaubt

    if (!waitForBytes(client, len)) return ""; // Timeout beim Lesen des Strings

    char* buffer = new char[len + 1];
    for (int i = 0; i < len; i++) {
        buffer[i] = client.read();
    }
    buffer[len] = '\0';
    String result = String(buffer);
    delete[] buffer;
    return result;
}

// Funktion zum Senden eines MQTT-Pakets (generisch)
void sendMQTTPacket(WiFiClient &client, uint8_t fixedHeader, const std::vector<byte>& variableHeaderAndPayload) {
    if (!client.connected()) {
        Serial.println("Error: Tried to send packet to disconnected client.");
        return;
    }

    int remLen = variableHeaderAndPayload.size();
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
    client.write(variableHeaderAndPayload.data(), variableHeaderAndPayload.size());
}


// =================================================================================================
// MQTT PAKET SENDER FUNKTIONEN
// =================================================================================================

// Funktion zum Senden von MQTT Publish Paketen an Client
void sendPublish(MQTTClientState& clientState, const String &topic, const String &payload, uint8_t qos, bool retain) {
    WiFiClient& client = clientState.client;
    if (!client.connected()) {
        Serial.println("Error: Tried to send PUBLISH to disconnected client.");
        return;
    }

    uint16_t packetId = 0;
    if (qos > 0) {
        packetId = clientState.nextOutgoingPacketId++;
        if (clientState.nextOutgoingPacketId == 0) clientState.nextOutgoingPacketId = 1; // Packet ID 0 ist reserviert
        
        // Nachricht in die Warteschlange für Retransmission legen
        OutgoingMessage outMsg = {topic, payload, qos, retain, packetId, millis(), 0, 0};
        clientState.outgoingQoSMessages[packetId] = outMsg;
    }

    uint8_t fixedHeader = 0x30; // PUBLISH Typ
    fixedHeader |= (qos << 1);  // QoS Bits setzen
    if (retain) fixedHeader |= 0x01; // Retain Flag setzen

    std::vector<byte> variableHeaderAndPayload;
    
    // Topic Name
    variableHeaderAndPayload.push_back((topic.length() >> 8) & 0xFF);
    variableHeaderAndPayload.push_back(topic.length() & 0xFF);
    for (char c : topic) {
        variableHeaderAndPayload.push_back(c);
    }

    // Packet ID (nur für QoS 1 und 2)
    if (qos > 0) {
        variableHeaderAndPayload.push_back((packetId >> 8) & 0xFF);
        variableHeaderAndPayload.push_back(packetId & 0xFF);
    }

    // Payload
    for (char c : payload) {
        variableHeaderAndPayload.push_back(c);
    }

    sendMQTTPacket(client, fixedHeader, variableHeaderAndPayload);
    Serial.printf("PUBLISH gesendet an '%s': Topic='%s', Payload='%s', QoS=%d, PacketID=%d, Retain=%d\n", clientState.clientId.c_str(), topic.c_str(), payload.c_str(), qos, packetId, retain);
}

// Sendet ein PUBACK Paket
void sendPubAck(WiFiClient& client, uint16_t packetId) {
    std::vector<byte> variableHeader;
    variableHeader.push_back((packetId >> 8) & 0xFF);
    variableHeader.push_back(packetId & 0xFF);
    sendMQTTPacket(client, 0x40, variableHeader); // PUBACK Fixed Header
    Serial.printf("PUBACK gesendet für Packet ID %d\n", packetId);
}

// Sendet ein PUBREC Paket
void sendPubRec(WiFiClient& client, uint16_t packetId) {
    std::vector<byte> variableHeader;
    variableHeader.push_back((packetId >> 8) & 0xFF);
    variableHeader.push_back(packetId & 0xFF);
    sendMQTTPacket(client, 0x50, variableHeader); // PUBREC Fixed Header
    Serial.printf("PUBREC gesendet für Packet ID %d\n", packetId);
}

// Sendet ein PUBREL Paket
void sendPubRel(WiFiClient& client, uint16_t packetId) {
    std::vector<byte> variableHeader;
    variableHeader.push_back((packetId >> 8) & 0xFF);
    variableHeader.push_back(packetId & 0xFF);
    sendMQTTPacket(client, 0x62, variableHeader); // PUBREL Fixed Header (QoS 1)
    Serial.printf("PUBREL gesendet für Packet ID %d\n", packetId);
}

// Sendet ein PUBCOMP Paket
void sendPubComp(WiFiClient& client, uint16_t packetId) {
    std::vector<byte> variableHeader;
    variableHeader.push_back((packetId >> 8) & 0xFF);
    variableHeader.push_back(packetId & 0xFF);
    sendMQTTPacket(client, 0x70, variableHeader); // PUBCOMP Fixed Header
    Serial.printf("PUBCOMP gesendet für Packet ID %d\n", packetId);
}

// =================================================================================================
// MQTT PAKET HANDLER FUNKTIONEN
// =================================================================================================

// Funktion zum Verarbeiten einer eingehenden PUBLISH-Nachricht
void handlePublish(MQTTClientState& clientState, byte header, byte remainingLength) {
    WiFiClient& client = clientState.client;
    uint8_t qos = (header >> 1) & 0x03;
    bool retain = (header & 0x01);
    bool dup = (header >> 3) & 0x01; // DUP flag

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

    Serial.printf("PUBLISH empfangen von '%s': Topic='%s', Payload='%s', QoS=%d, PacketID=%d, Retain=%d, DUP=%d\n", clientState.clientId.c_str(), topic.c_str(), payload.c_str(), qos, packetId, retain, dup);

    // QoS 1 Handling
    if (qos == 1) {
        sendPubAck(client, packetId);
        // TODO: Hier könnte man eine einfache Duplicate-Erkennung für QoS 1 implementieren,
        // indem man die Packet ID für eine kurze Zeit speichert und DUP-Nachrichten ignoriert.
        // Für diese Implementierung wird die Nachricht immer verarbeitet.
    }
    // QoS 2 Handling
    else if (qos == 2) {
        // Prüfen, ob diese Packet ID bereits in Bearbeitung ist
        if (clientState.incomingQoS2Messages.count(packetId) && clientState.incomingQoS2Messages[packetId].state == 0 && dup) {
            // Dies ist ein Duplikat des initialen PUBLISH (DUP-Flag gesetzt), senden Sie PUBREC erneut
            Serial.printf("QoS 2 DUP PUBLISH empfangen (Packet ID %d). Sende PUBREC erneut.\n", packetId);
            sendPubRec(client, packetId);
            return; // Nachricht nicht erneut verarbeiten
        }

        // Neue QoS 2 Nachricht oder erste Übertragung eines QoS 2 PUBLISH
        IncomingQoS2State inMsg = {topic, payload, qos, packetId, millis(), 0};
        clientState.incomingQoS2Messages[packetId] = inMsg;
        sendPubRec(client, packetId);
        clientState.incomingQoS2Messages[packetId].state = 1; // PUBREC gesendet
        // Nachricht wird erst weitergeleitet, wenn PUBREL empfangen wurde
    }

    // Wenn QoS 0 oder QoS 1 (nach PUBACK) oder QoS 2 (nach vollem Handshake)
    // Nur bei QoS 0 wird sofort weitergeleitet. QoS 1 und 2 werden später weitergeleitet.
    if (qos == 0) {
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

        // Weiterleiten an alle passenden Subscriber
        for (auto &sub : activeSubscriptions) {
            if (sub.clientState->client.connected() && topicMatches(topic, sub.topicFilter)) {
                Serial.printf("Weiterleiten an Subscriber '%s' für Topic: %s (QoS %d)\n", sub.clientState->clientId.c_str(), sub.topicFilter.c_str(), sub.qos);
                sendPublish(*sub.clientState, topic, payload, sub.qos, retain); // Sende mit dem QoS des Abonnenten
            }
        }
    }
}

// Verarbeitet ein eingehendes PUBACK Paket (Antwort auf Broker-gesendetes QoS 1 PUBLISH)
void handlePubAck(MQTTClientState& clientState, byte remainingLength) {
    WiFiClient& client = clientState.client;
    if (!waitForBytes(client, remainingLength)) {
        Serial.println("Timeout bei PUBACK Payload");
        return;
    }
    byte packetIdMsb = client.read();
    byte packetIdLsb = client.read();
    uint16_t packetId = (packetIdMsb << 8) | packetIdLsb;

    Serial.printf("PUBACK empfangen von '%s' für Packet ID %d\n", clientState.clientId.c_str(), packetId);

    // Nachricht aus der Warteschlange der ausgehenden QoS-Nachrichten entfernen
    if (clientState.outgoingQoSMessages.count(packetId)) {
        clientState.outgoingQoSMessages.erase(packetId);
        Serial.printf("Ausgehende QoS 1 Nachricht (Packet ID %d) erfolgreich bestätigt.\n", packetId);
    } else {
        Serial.printf("Warnung: PUBACK für unbekannte oder bereits bestätigte Packet ID %d empfangen.\n", packetId);
    }
}

// Verarbeitet ein eingehendes PUBREC Paket (Antwort auf Broker-gesendetes QoS 2 PUBLISH)
void handlePubRec(MQTTClientState& clientState, byte remainingLength) {
    WiFiClient& client = clientState.client;
    if (!waitForBytes(client, remainingLength)) {
        Serial.println("Timeout bei PUBREC Payload");
        return;
    }
    byte packetIdMsb = client.read();
    byte packetIdLsb = client.read();
    uint16_t packetId = (packetIdMsb << 8) | packetIdLsb;

    Serial.printf("PUBREC empfangen von '%s' für Packet ID %d\n", clientState.clientId.c_str(), packetId);

    if (clientState.outgoingQoSMessages.count(packetId)) {
        OutgoingMessage& outMsg = clientState.outgoingQoSMessages[packetId];
        if (outMsg.qos == 2 && outMsg.state == 0) { // PUBLISH gesendet, warte auf PUBREC
            outMsg.state = 1; // PUBREC empfangen, sende PUBREL
            sendPubRel(client, packetId);
            outMsg.timestampSent = millis(); // Update timestamp for PUBREL retransmission
            outMsg.retryCount = 0; // Reset retry count for next step
            Serial.printf("QoS 2 Handshake: PUBREC empfangen, sende PUBREL für Packet ID %d.\n", packetId);
        } else {
            Serial.printf("Warnung: PUBREC für unerwarteten Zustand der Packet ID %d empfangen (QoS %d, State %d).\n", packetId, outMsg.qos, outMsg.state);
        }
    } else {
        Serial.printf("Warnung: PUBREC für unbekannte oder bereits bestätigte Packet ID %d empfangen.\n", packetId);
        // Wenn unbekannt, sende PUBREL als Reaktion auf ein unerwartetes PUBREC
        sendPubRel(client, packetId);
    }
}

// Verarbeitet ein eingehendes PUBREL Paket (Antwort auf Broker-gesendetes PUBREC, oder Teil des eingehenden QoS 2 Handshakes)
void handlePubRel(MQTTClientState& clientState, byte remainingLength) {
    WiFiClient& client = clientState.client;
    if (!waitForBytes(client, remainingLength)) {
        Serial.println("Timeout bei PUBREL Payload");
        return;
    }
    byte packetIdMsb = client.read();
    byte packetIdLsb = client.read();
    uint16_t packetId = (packetIdMsb << 8) | packetIdLsb;

    Serial.printf("PUBREL empfangen von '%s' für Packet ID %d\n", clientState.clientId.c_str(), packetId);

    // Teil des eingehenden QoS 2 Handshakes
    if (clientState.incomingQoS2Messages.count(packetId)) {
        IncomingQoS2State& inMsg = clientState.incomingQoS2Messages[packetId];
        if (inMsg.state == 1) { // PUBREC gesendet, warte auf PUBREL
            inMsg.state = 2; // PUBREL empfangen
            sendPubComp(client, packetId);
            Serial.printf("QoS 2 Handshake: PUBREL empfangen, sende PUBCOMP für Packet ID %d.\n", packetId);

            // Nachricht jetzt an Subscriber weiterleiten, da QoS 2 Handshake abgeschlossen ist
            Message msg = {inMsg.topic, inMsg.payload, millis()};
            messageLog.push_back(msg);
            if (messageLog.size() > 50) {
                messageLog.erase(messageLog.begin());
            }

            if (inMsg.qos == 2) { // Retain Flag wird vom originalen PUBLISH übernommen
                // Da Retain ein Flag im PUBLISH ist, wird es hier nicht direkt verwendet,
                // sondern die ursprüngliche Logik für Retained Messages im handlePublish
                // würde das Retain-Flag des *eingehenden* PUBLISH berücksichtigen.
                // Hier leiten wir nur die Nachricht weiter.
                // Wenn die originale PUBLISH-Nachricht retain war, wurde sie bereits in retainedMessages gespeichert.
            }

            for (auto &sub : activeSubscriptions) {
                if (sub.clientState->client.connected() && topicMatches(inMsg.topic, sub.topicFilter)) {
                    Serial.printf("Weiterleiten an Subscriber '%s' für Topic: %s (QoS %d)\n", sub.clientState->clientId.c_str(), sub.topicFilter.c_str(), sub.qos);
                    sendPublish(*sub.clientState, inMsg.topic, inMsg.payload, sub.qos, (inMsg.qos == 2 && inMsg.qos == 2)); // Retain Flag vom Original-PUBLISH
                }
            }
            clientState.incomingQoS2Messages.erase(packetId); // Handshake abgeschlossen
        } else {
            Serial.printf("Warnung: PUBREL für unerwarteten Zustand der eingehenden QoS 2 Nachricht Packet ID %d empfangen (State %d).\n", packetId, inMsg.state);
            // Sende PUBCOMP erneut, falls der Client es nicht erhalten hat
            sendPubComp(client, packetId);
        }
    }
    // Teil des ausgehenden QoS 2 Handshakes (Antwort auf Broker-gesendetes PUBREC)
    else if (clientState.outgoingQoSMessages.count(packetId)) {
        OutgoingMessage& outMsg = clientState.outgoingQoSMessages[packetId];
        if (outMsg.qos == 2 && outMsg.state == 1) { // PUBREC gesendet, warte auf PUBREL
            outMsg.state = 2; // PUBREL empfangen
            sendPubComp(client, packetId);
            outMsg.timestampSent = millis(); // Update timestamp for PUBCOMP retransmission
            outMsg.retryCount = 0; // Reset retry count
            Serial.printf("QoS 2 Handshake: PUBREL empfangen (ausgehend), sende PUBCOMP für Packet ID %d.\n", packetId);
        } else {
            Serial.printf("Warnung: PUBREL für unerwarteten Zustand der ausgehenden QoS 2 Nachricht Packet ID %d empfangen (QoS %d, State %d).\n", packetId, outMsg.qos, outMsg.state);
        }
    } else {
        Serial.printf("Warnung: PUBREL für unbekannte Packet ID %d empfangen.\n", packetId);
        // Wenn unbekannt, sende PUBCOMP als Reaktion auf ein unerwartetes PUBREL
        sendPubComp(client, packetId);
    }
}

// Verarbeitet ein eingehendes PUBCOMP Paket (Antwort auf Broker-gesendetes PUBREL)
void handlePubComp(MQTTClientState& clientState, byte remainingLength) {
    WiFiClient& client = clientState.client;
    if (!waitForBytes(client, remainingLength)) {
        Serial.println("Timeout bei PUBCOMP Payload");
        return;
    }
    byte packetIdMsb = client.read();
    byte packetIdLsb = client.read();
    uint16_t packetId = (packetIdMsb << 8) | packetIdLsb;

    Serial.printf("PUBCOMP empfangen von '%s' für Packet ID %d\n", clientState.clientId.c_str(), packetId);

    // Nachricht aus der Warteschlange der ausgehenden QoS-Nachrichten entfernen
    if (clientState.outgoingQoSMessages.count(packetId)) {
        clientState.outgoingQoSMessages.erase(packetId);
        Serial.printf("Ausgehende QoS 2 Nachricht (Packet ID %d) erfolgreich bestätigt.\n", packetId);
    } else {
        Serial.printf("Warnung: PUBCOMP für unbekannte oder bereits bestätigte Packet ID %d empfangen.\n", packetId);
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
    uint16_t packetId = (packetIdMsb << 8) | packetIdLsb;

    // SUBSCRIBE kann mehrere Topic-Filter enthalten, hier nur der erste
    byte topicLenMsb = client.read();
    byte topicLenLsb = client.read();
    int topicLen = (topicLenMsb << 8) | topicLenLsb;

    char topicChar[topicLen + 1];
    for (int i = 0; i < topicLen; i++) {
        topicChar[i] = client.read();
    }
    topicChar[topicLen] = 0;
    String topic = String(topicChar);

    byte requestedQoS = client.read(); // QoS des Abonnenten für dieses Topic

    Serial.printf("Client '%s' SUBSCRIBEd to Topic: %s with QoS %d\n", clientState.clientId.c_str(), topic.c_str(), requestedQoS);

    // Sende SUBACK mit dem gewährten QoS (hier der angeforderte QoS)
    // MQTT 3.1.1 erlaubt nur 0, 1, 2 als gewährten QoS. Wenn Client 3 anfordert, muss Broker 0, 1 oder 2 gewähren.
    // Hier vereinfacht: Gewährter QoS ist der angeforderte QoS.
    byte grantedQoS = requestedQoS;
    if (grantedQoS > 2) grantedQoS = 0; // Ungültiger QoS wird auf 0 gesetzt

    byte suback[] = {0x90, 0x03, packetIdMsb, packetIdLsb, grantedQoS};
    client.write(suback, sizeof(suback));
    Serial.printf("SUBACK gesendet für Packet ID %d mit Granted QoS %d\n", packetId, grantedQoS);

    ActiveSubscription newSub;
    newSub.clientState = &clientState;
    newSub.topicFilter = topic;
    newSub.qos = grantedQoS; // Speichere den gewährten QoS
    activeSubscriptions.push_back(newSub);
    clientState.subscribedTopics.push_back(topic);

    // Sende Retained Messages an den neuen Subscriber
    for (auto const& [retainedTopic, retainedPayload] : retainedMessages) {
        if (topicMatches(retainedTopic, topic)) {
            Serial.printf("Sende Retained Message für Topic '%s' an neuen Subscriber '%s'.\n", retainedTopic.c_str(), clientState.clientId.c_str());
            sendPublish(clientState, retainedTopic, retainedPayload, grantedQoS, true); // Sende mit dem gewährten QoS
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
        return; // Client ist bereits getrennt
    }

    // Keep-Alive-Prüfung
    if (clientState.clientKeepAliveSec > 0) {
        unsigned long idleTime = millis() - clientState.lastSeen;
        // Keep-Alive-Timeout ist 1.5 * Keep-Alive-Intervall
        if (idleTime > (unsigned long)clientState.clientKeepAliveSec * 1500) {
            Serial.printf("Client '%s' (ID: %s) Keep-Alive Timeout. Trenne Verbindung.\n",
                          client.remoteIP().toString().c_str(), clientState.clientId.c_str());
            if (clientState.hasLWT) {
                Serial.printf("Veröffentliche Last Will and Testament für Client '%s' (Topic: '%s', Message: '%s').\n",
                              clientState.clientId.c_str(), clientState.willTopic.c_str(), clientState.willMessage.c_str());
                // LWT wird immer mit QoS 0 und Retain=false veröffentlicht, gemäß Spezifikation (außer anders konfiguriert)
                // Hier wird es als normale PUBLISH-Nachricht an alle Subscriber gesendet.
                for (auto &sub : activeSubscriptions) {
                    if (sub.clientState->client.connected() && topicMatches(clientState.willTopic, sub.topicFilter)) {
                        sendPublish(*sub.clientState, clientState.willTopic, clientState.willMessage, sub.qos, false);
                    }
                }
            }
            client.stop();
            return;
        }
    }

    // Retransmission-Logik für ausgehende QoS-Nachrichten
    for (auto it = clientState.outgoingQoSMessages.begin(); it != clientState.outgoingQoSMessages.end(); ) {
        OutgoingMessage& outMsg = it->second;
        if (millis() - outMsg.timestampSent > QoS_TIMEOUT_MS) {
            if (outMsg.retryCount < MAX_QOS_RETRIES) {
                outMsg.retryCount++;
                outMsg.timestampSent = millis(); // Update timestamp for next retry

                Serial.printf("Retransmission: Sende PUBLISH/PUBREL erneut an '%s' (Packet ID %d, Retries %d)\n",
                              clientState.clientId.c_str(), outMsg.packetId, outMsg.retryCount);

                if (outMsg.qos == 1 || (outMsg.qos == 2 && outMsg.state == 0)) { // PUBLISH erneut senden (QoS 1 oder QoS 2 initial)
                    uint8_t fixedHeader = 0x38; // PUBLISH mit DUP=1
                    fixedHeader |= (outMsg.qos << 1);
                    if (outMsg.retain) fixedHeader |= 0x01;

                    std::vector<byte> variableHeaderAndPayload;
                    variableHeaderAndPayload.push_back((outMsg.topic.length() >> 8) & 0xFF);
                    variableHeaderAndPayload.push_back(outMsg.topic.length() & 0xFF);
                    for (char c : outMsg.topic) variableHeaderAndPayload.push_back(c);
                    variableHeaderAndPayload.push_back((outMsg.packetId >> 8) & 0xFF);
                    variableHeaderAndPayload.push_back(outMsg.packetId & 0xFF);
                    for (char c : outMsg.payload) variableHeaderAndPayload.push_back(c);
                    sendMQTTPacket(client, fixedHeader, variableHeaderAndPayload);
                } else if (outMsg.qos == 2 && outMsg.state == 1) { // PUBREL erneut senden
                    sendPubRel(client, outMsg.packetId);
                } else if (outMsg.qos == 2 && outMsg.state == 2) { // PUBCOMP erneut senden
                    sendPubComp(client, outMsg.packetId);
                }
                ++it;
            } else {
                Serial.printf("Client '%s': Maximale Retransmissionen für Packet ID %d erreicht. Trenne Verbindung.\n", clientState.clientId.c_str(), outMsg.packetId);
                client.stop();
                it = clientState.outgoingQoSMessages.erase(it); // Client trennen und Nachricht entfernen
                return; // Client wurde getrennt, beende Verarbeitung für diesen Client
            }
        } else {
            ++it;
        }
    }

    // Retransmission-Logik für eingehende QoS 2 Nachrichten (Broker sendet PUBREC/PUBCOMP erneut)
    for (auto it = clientState.incomingQoS2Messages.begin(); it != clientState.incomingQoS2Messages.end(); ) {
        IncomingQoS2State& inMsg = it->second;
        if (millis() - inMsg.timestampReceived > QoS_TIMEOUT_MS) {
            // Hier keine retryCount, da der Client das DUP-Flag setzt.
            // Wir senden einfach das letzte ACK-Paket erneut.
            if (inMsg.state == 1) { // PUBREC wurde gesendet, warte auf PUBREL
                Serial.printf("Retransmission: Sende PUBREC erneut an '%s' für Packet ID %d.\n", clientState.clientId.c_str(), inMsg.packetId);
                sendPubRec(client, inMsg.packetId);
            } else if (inMsg.state == 2) { // PUBREL wurde empfangen, warte auf PUBCOMP
                Serial.printf("Retransmission: Sende PUBCOMP erneut an '%s' für Packet ID %d.\n", clientState.clientId.c_str(), inMsg.packetId);
                sendPubComp(client, inMsg.packetId);
            }
        }
        ++it;
    }


    // Verarbeite eingehende Datenpakete
    if (client.available() >= 2) {
        byte h = client.read();
        byte len = client.read();

        int packetType = h >> 4;
        uint8_t flags = h & 0x0F; // Flags für spezifische Pakettypen

        switch (packetType) {
            case 1: // CONNECT (sollte nur einmal am Anfang kommen)
                Serial.printf("Warnung: Client '%s' sendet CONNECT erneut. Ignoriere.\n", clientState.clientId.c_str());
                // Read and discard remaining bytes for this packet to avoid blocking
                for (int i = 0; i < len && client.available(); ++i) client.read();
                break;
            case 3: // PUBLISH
                handlePublish(clientState, h, len);
                break;
            case 4: // PUBACK
                handlePubAck(clientState, len);
                break;
            case 5: // PUBREC
                handlePubRec(clientState, len);
                break;
            case 6: // PUBREL
                handlePubRel(clientState, len);
                break;
            case 7: // PUBCOMP
                handlePubComp(clientState, len);
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
                Serial.printf("Client '%s': Unbekannter Pakettyp 0x%X, Länge %d. Trenne Verbindung.\n", clientState.clientId.c_str(), packetType, len);
                // Lese und verwerfe die restlichen Bytes, um den Stream zu leeren, bevor die Verbindung geschlossen wird
                for (int i = 0; i < len && client.available(); ++i) client.read();
                client.stop();
                break;
        }
    }
}

void loop() {
    web.handleClient();

    // Neue eingehende MQTT-Verbindungen prüfen
    WiFiClient newClient = mqttServer.available();
    if (newClient) {
        Serial.println("Neue TCP-Verbindung erkannt.");
        MQTTClientState newState;
        newState.client = newClient;
        newState.lastSeen = millis();
        newState.hasLWT = false; // Standardmäßig keine LWT
        connectedMQTTClients.push_back(newState);
        Serial.printf("Anzahl verbundener MQTT-Clients: %d\n", connectedMQTTClients.size());
    }

    // Verarbeite alle verbundenen MQTT-Clients
    // Iteriere rückwärts, um sichere Entfernung zu ermöglichen
    for (int i = connectedMQTTClients.size() - 1; i >= 0; --i) {
        if (connectedMQTTClients[i].client.connected()) {
            // Wenn der Client noch auf das CONNACK wartet, verarbeite das CONNECT-Paket
            if (!connectedMQTTClients[i].connectedACKSent) {
                WiFiClient& client = connectedMQTTClients[i].client;
                if (client.available() >= 2) {
                    byte header = client.peek(); // Nur lesen, nicht verbrauchen
                    byte remainingLength = client.peek(1); // Nur lesen, nicht verbrauchen

                    if ((header >> 4) == 1) { // CONNECT-Paket
                        client.read(); // Verbrauche Header
                        client.read(); // Verbrauche Remaining Length

                        if (!waitForBytes(client, remainingLength)) {
                            Serial.printf("Timeout processing CONNECT payload for client %s. Disconnecting.\n", connectedMQTTClients[i].client.remoteIP().toString().c_str());
                            client.stop();
                            // Entfernung erfolgt im nächsten Loop-Durchlauf
                            continue;
                        }

                        // Protokoll Name (MQTT), Version
                        for (int k = 0; k < 8; ++k) client.read();

                        byte connectFlags = client.read(); // Connect Flags
                        connectedMQTTClients[i].clientKeepAliveSec = (client.read() << 8) | client.read(); // Keep-Alive

                        // Client ID
                        connectedMQTTClients[i].clientId = readLengthPrefixedString(client);

                        // Will Topic und Will Message (falls vorhanden)
                        bool willFlag = (connectFlags >> 2) & 0x01;
                        if (willFlag) {
                            connectedMQTTClients[i].willTopic = readLengthPrefixedString(client);
                            connectedMQTTClients[i].willMessage = readLengthPrefixedString(client);
                            connectedMQTTClients[i].hasLWT = true;
                        }

                        // User Name und Password (falls vorhanden) - hier ignoriert für diese Implementierung
                        bool userNameFlag = (connectFlags >> 7) & 0x01;
                        if (userNameFlag) {
                            readLengthPrefixedString(client); // Read and discard username
                        }
                        bool passwordFlag = (connectFlags >> 6) & 0x01;
                        if (passwordFlag) {
                            readLengthPrefixedString(client); // Read and discard password
                        }

                        Serial.printf("Client '%s' (IP: %s) CONNECTED. Keep-Alive: %d sec. Sending CONNACK.\n",
                                      connectedMQTTClients[i].clientId.c_str(), connectedMQTTClients[i].client.remoteIP().toString().c_str(),
                                      connectedMQTTClients[i].clientKeepAliveSec);

                        byte connack[] = {0x20, 0x02, 0x00, 0x00}; // CONNACK: Session Present = 0, Connect Acknowledge Flags = 0 (Connection Accepted)
                        client.write(connack, sizeof(connack));
                        connectedMQTTClients[i].connectedACKSent = true;
                        connectedMQTTClients[i].lastSeen = millis(); // Update lastSeen after CONNECT
                    } else {
                        Serial.printf("Client '%s' sent non-CONNECT first packet (0x%X). Disconnecting.\n", connectedMQTTClients[i].client.remoteIP().toString().c_str(), header);
                        client.stop();
                        // Entfernung erfolgt im nächsten Loop-Durchlauf
                        continue;
                    }
                }
            } else {
                // Client ist verbunden und CONNACK wurde gesendet, verarbeite normale MQTT-Pakete
                processMQTTClient(connectedMQTTClients[i]);
            }
        } else {
            Serial.printf("Client '%s' (ID: %s, IP: %s) hat die Verbindung getrennt. Entferne ihn.\n",
                            connectedMQTTClients[i].clientId.c_str(),
                            connectedMQTTClients[i].client.remoteIP().toString().c_str(),
                            connectedMQTTClients[i].client.remoteIP().toString().c_str());

            // Entferne alle Abonnements dieses Clients
            activeSubscriptions.erase(std::remove_if(activeSubscriptions.begin(), activeSubscriptions.end(),
                                                     [&](const ActiveSubscription& sub) {
                                                         return sub.clientState == &connectedMQTTClients[i];
                                                     }),
                                      activeSubscriptions.end());
            // Entferne ausstehende QoS-Nachrichten für diesen Client
            connectedMQTTClients[i].outgoingQoSMessages.clear();
            connectedMQTTClients[i].incomingQoS2Messages.clear();

            // Entferne den Client aus der Liste
            connectedMQTTClients.erase(connectedMQTTClients.begin() + i);
        }
    }
}
