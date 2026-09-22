#include <WiFi.h>
#include <WebServer.h>
#include <Update.h>
#include <ESPmDNS.h>
#include <Preferences.h>
#include <LittleFS.h>
#include <sMQTTBroker.h>
#include <time.h>
#include <math.h>

// =====================================================
// ESP32 MQTT BROKER 4.4.0 — with LWT device monitoring
// =====================================================

char HOSTNAME[33] = "mqtt-broker";

// =====================================================
// DEFAULT WIFI CREDENTIALS
//
// Leave these empty to use the built-in configuration AP:
//
//   1. On first boot, ESP32 starts an AP:
//        SSID:     ESP32-MQTT-Setup
//        Password: mqttsetup
//        URL:      http://192.168.4.1
//
//   2. Connect to that AP, open the web UI,
//      go to /settings and enter your Wi-Fi credentials.
//
//   3. ESP32 reboots and connects to your network.
//
// You can also hardcode your credentials here, but then
// remember NOT to commit them to a public repository.
// =====================================================

const char* DEFAULT_WIFI_SSID     = "";
const char* DEFAULT_WIFI_PASSWORD = "";
const char* DEFAULT_HOSTNAME      = "mqtt-broker";

const char* WIFI_AP_SSID     = "ESP32-MQTT-Setup";
const char* WIFI_AP_PASSWORD = "mqttsetup";

#define MQTT_PORT 1883
#define WEB_PORT  80
#define FIRMWARE_VERSION "4.4.1"

#define MAX_CLIENTS 20
#define MAX_TOPICS  30
#define MAX_RULES   16
#define MAX_TIMERS  16
#define MAX_DEVICES 20

#define SYS_INTERVAL        30000UL
#define WIFI_CHECK_INTERVAL 10000UL

#define MQTT_AUTH_ENABLED false
const char* MQTT_USERNAME = "mqtt";
const char* MQTT_PASSWORD = "mqtt";

const char* TZ_INFO = "EET-2EEST,M3.5.0/3,M10.5.0/4";

// Prefix used for device status topics: devices/<id>/status
#define DEVICE_TOPIC_PREFIX "devices/"
#define DEVICE_STATUS_SUFFIX "/status"

WebServer server(WEB_PORT);
Preferences preferences;

// =====================================================
// DATA STRUCTURES
// =====================================================

struct MQTTClientInfo {
  char clientId[64];
  char ip[16];
  unsigned long connectedAt;
  unsigned long lastActivity;
  bool active;
};
MQTTClientInfo clients[MAX_CLIENTS];

struct MQTTTopicInfo {
  char topic[128];
  char lastMessage[512];
  unsigned long lastUpdate;
  int qos;
  bool retained;
};
MQTTTopicInfo topics[MAX_TOPICS];

// >>> LWT device tracker
struct MQTTDeviceInfo {
  char id[64];
  char status[16];          // "online" / "offline" / "unknown"
  char lastPayload[256];
  unsigned long firstSeen;
  unsigned long lastSeen;
  unsigned long lastOnline;
  unsigned long lastOffline;
  unsigned int messages;
  bool active;
};
MQTTDeviceInfo devices[MAX_DEVICES];
// <<< LWT device tracker

enum RuleCondition {
  RULE_GT = 0,
  RULE_LT,
  RULE_GE,
  RULE_LE,
  RULE_EQ,
  RULE_CONTAINS
};

struct MQTTRule {
  bool enabled;
  char name[48];
  char sourceTopic[128];
  uint8_t condition;
  char value[64];
  char targetTopic[128];
  char targetPayload[256];
  uint8_t qos;
  bool retain;
};
MQTTRule rules[MAX_RULES];

struct MQTTTimer {
  bool enabled;
  char name[48];
  uint8_t hour;
  uint8_t minute;
  uint8_t daysMask;
  char targetTopic[128];
  char targetPayload[256];
  uint8_t qos;
  bool retain;
};
MQTTTimer timers[MAX_TIMERS];

int timerLastRunYear[MAX_TIMERS];
int timerLastRunYDay[MAX_TIMERS];

// =====================================================
// GLOBALS
// =====================================================

unsigned long bootTime = 0;
unsigned long lastWiFiCheck = 0;
unsigned long lastSysPublish = 0;

bool otaRunning = false;
bool brokerStarted = false;
bool wifiWasConnected = false;
bool timeSynced = false;
bool mdnsStarted = false;
bool webStarted = false;
bool apMode = false;
unsigned long wifiReconnectDelay = 5000UL;
unsigned long nextWiFiReconnect = 0;

unsigned long messagesReceived = 0;
unsigned long bytesReceived = 0;
unsigned long messagesPublished = 0;
unsigned long authRejected = 0;
unsigned long subscribeCount = 0;
unsigned long unsubscribeCount = 0;

bool ruleExecution = false;

// =====================================================
// PROTOTYPES
// =====================================================

void addClient(const char* id, const char* ip);
void removeClient(const char* id);
void updateClientActivity(const char* id);
void storeMessage(const char* topic, const char* payload, int qos, bool retained);
void executeRules(const char* topic, const char* payload);
void checkTimers();
bool saveRules();
void loadRules();
void saveTimers();
void loadTimers();
void publishSys();
void startMQTTBroker();
void startWebServer();
void startMDNS();
void setupTime();
bool updateTimeStatus();
void checkWiFi();
void loadWiFiConfig(String& ssid, String& password);
bool saveWiFiConfigIfChanged(const String& ssid, const String& password, const String& hostname);
void startConfigAP();
void handleSettings();
void handleWiFiSave();
void resetWiFiConfigToDefaults();

// >>> LWT device helpers
int findDevice(const char* id);
int findFreeDeviceSlot();
void updateDeviceStatus(const char* topic, const char* payload);
void clearDevices();
int getOnlineDevicesCount();
int getKnownDevicesCount();
// <<< LWT device helpers

// =====================================================
// BROKER CLASS
// =====================================================

class MyBroker : public sMQTTBroker {
public:
  bool onEvent(sMQTTEvent* event) override {
    if (!event) return true;

    if (event->Type() == NewClient_sMQTTEventType) {
      sMQTTNewClientEvent* e = (sMQTTNewClientEvent*)event;
      sMQTTClient* c = e->Client();

      if (MQTT_AUTH_ENABLED) {
        if (e->Login() != MQTT_USERNAME || e->Password() != MQTT_PASSWORD) {
          authRejected++;
          Serial.println("MQTT AUTH REJECTED");
          return false;
        }
      }

      if (c) {
        String id = String(c->getClientId().c_str());
        Serial.printf("MQTT CONNECT: %s\n", id.c_str());
        addClient(id.c_str(), "unknown");
      }
      return true;
    }

    if (event->Type() == RemoveClient_sMQTTEventType) {
      sMQTTRemoveClientEvent* e = (sMQTTRemoveClientEvent*)event;
      sMQTTClient* c = e->Client();
      if (c) {
        String id = String(c->getClientId().c_str());
        Serial.printf("MQTT DISCONNECT: %s\n", id.c_str());
        removeClient(id.c_str());
      }
      return true;
    }

    if (event->Type() == LostConnect_sMQTTEventType) {
      Serial.println("MQTT broker: WiFi lost");
      return true;
    }

    if (event->Type() == Subscribe_sMQTTEventType) {
      sMQTTSubUnSubClientEvent* e = (sMQTTSubUnSubClientEvent*)event;
      sMQTTClient* c = e->Client();
      subscribeCount++;
      if (c) {
        String id = String(c->getClientId().c_str());
        Serial.printf("MQTT SUBSCRIBE: %s -> %s\n", id.c_str(), e->Topic().c_str());
        updateClientActivity(id.c_str());
      }
      return true;
    }

    if (event->Type() == UnSubscribe_sMQTTEventType) {
      sMQTTSubUnSubClientEvent* e = (sMQTTSubUnSubClientEvent*)event;
      sMQTTClient* c = e->Client();
      unsubscribeCount++;
      if (c) {
        String id = String(c->getClientId().c_str());
        Serial.printf("MQTT UNSUBSCRIBE: %s -> %s\n", id.c_str(), e->Topic().c_str());
        updateClientActivity(id.c_str());
      }
      return true;
    }

    if (event->Type() == Public_sMQTTEventType) {
      sMQTTPublicClientEvent* e = (sMQTTPublicClientEvent*)event;
      sMQTTClient* c = e->Client();

      String topic = String(e->Topic().c_str());
      String payload = String(e->Payload().c_str());

      messagesReceived++;
      bytesReceived += topic.length() + payload.length();

      if (c) {
        String id = String(c->getClientId().c_str());
        updateClientActivity(id.c_str());
        Serial.printf("MQTT PUBLISH [%s] ", id.c_str());
      } else {
        // >>> LWT: event with no client = internal publish (LWT, rule, timer)
        Serial.print("MQTT PUBLISH (internal) ");
        // <<< LWT
      }

      Serial.printf("%s = ", topic.c_str());
      if (payload.length() > 100)
        Serial.println(payload.substring(0, 100));
      else
        Serial.println(payload);

      // >>> LWT: update device tracker when devices/<id>/status arrives
      updateDeviceStatus(topic.c_str(), payload.c_str());
      // <<< LWT

      storeMessage(topic.c_str(), payload.c_str(), -1, false);
      executeRules(topic.c_str(), payload.c_str());
      return true;
    }

    return true;
  }
};

MyBroker broker;

// =====================================================
// LWT DEVICE TRACKER
// =====================================================

int findDevice(const char* id) {
  if (!id) return -1;
  for (int i = 0; i < MAX_DEVICES; i++) {
    if (devices[i].active && strcmp(devices[i].id, id) == 0) return i;
  }
  return -1;
}

int findFreeDeviceSlot() {
  for (int i = 0; i < MAX_DEVICES; i++) {
    if (!devices[i].active) return i;
  }
  // Replace the least recently seen device
  int oldest = 0;
  for (int i = 1; i < MAX_DEVICES; i++) {
    if (devices[i].lastSeen < devices[oldest].lastSeen) oldest = i;
  }
  return oldest;
}

int getOnlineDevicesCount() {
  int n = 0;
  for (int i = 0; i < MAX_DEVICES; i++) {
    if (devices[i].active && strcmp(devices[i].status, "online") == 0) n++;
  }
  return n;
}

int getKnownDevicesCount() {
  int n = 0;
  for (int i = 0; i < MAX_DEVICES; i++) if (devices[i].active) n++;
  return n;
}

void clearDevices() {
  memset(devices, 0, sizeof(devices));
}

// Parse devices/<id>/status and update tracker.
// Works both for incoming device PUBLISH (online) and for LWT (offline).
void updateDeviceStatus(const char* topic, const char* payload) {
  if (!topic || !payload) return;

  const char* prefix = DEVICE_TOPIC_PREFIX;
  size_t prefixLen = strlen(prefix);

  if (strncmp(topic, prefix, prefixLen) != 0) return;

  const char* idStart = topic + prefixLen;
  const char* suffix = strstr(idStart, DEVICE_STATUS_SUFFIX);

  // Must be exactly "devices/<id>/status", nothing more
  if (!suffix) return;
  if (strlen(suffix) != strlen(DEVICE_STATUS_SUFFIX)) return;

  size_t idLen = suffix - idStart;
  if (idLen == 0 || idLen >= sizeof(devices[0].id)) return;

  char id[64];
  memcpy(id, idStart, idLen);
  id[idLen] = '\0';

  int idx = findDevice(id);
  if (idx < 0) {
    idx = findFreeDeviceSlot();
    memset(&devices[idx], 0, sizeof(devices[idx]));
    strncpy(devices[idx].id, id, sizeof(devices[idx].id) - 1);
    devices[idx].firstSeen = millis();
    devices[idx].active = true;
    Serial.printf("LWT: new device registered: %s\n", id);
  }

  devices[idx].lastSeen = millis();
  devices[idx].messages++;

  strncpy(devices[idx].lastPayload, payload, sizeof(devices[idx].lastPayload) - 1);

  if (strcmp(payload, "online") == 0) {
    strncpy(devices[idx].status, "online", sizeof(devices[idx].status) - 1);
    devices[idx].lastOnline = millis();
    Serial.printf("LWT: %s -> ONLINE\n", id);
  } else if (strcmp(payload, "offline") == 0) {
    strncpy(devices[idx].status, "offline", sizeof(devices[idx].status) - 1);
    devices[idx].lastOffline = millis();
    Serial.printf("LWT: %s -> OFFLINE\n", id);
  } else {
    // Custom payload — keep status unknown but log payload
    if (devices[idx].status[0] == '\0')
      strncpy(devices[idx].status, "unknown", sizeof(devices[idx].status) - 1);
  }
}

// =====================================================
// HELPERS
// =====================================================

String htmlEscape(const String& text) {
  String out;
  out.reserve(text.length() + 16);
  for (unsigned int i = 0; i < text.length(); i++) {
    char c = text[i];
    if (c == '&') out += "&amp;";
    else if (c == '<') out += "&lt;";
    else if (c == '>') out += "&gt;";
    else if (c == '"') out += "&quot;";
    else if (c == '\'') out += "&#39;";
    else out += c;
  }
  return out;
}

String jsonEscape(const String& text) {
  String out;
  out.reserve(text.length() + 16);
  for (unsigned int i = 0; i < text.length(); i++) {
    char c = text[i];
    if (c == '"') out += "\\\"";
    else if (c == '\\') out += "\\\\";
    else if (c == '\n') out += "\\n";
    else if (c == '\r') out += "\\r";
    else if (c == '\t') out += "\\t";
    else out += c;
  }
  return out;
}

String ruleOperator(uint8_t c) {
  switch (c) {
    case RULE_GT: return ">";
    case RULE_LT: return "<";
    case RULE_GE: return ">=";
    case RULE_LE: return "<=";
    case RULE_EQ: return "=";
    case RULE_CONTAINS: return "містить";
  }
  return "=";
}

String conditionName(uint8_t c) {
  switch (c) {
    case RULE_GT: return "більше ніж (>)";
    case RULE_LT: return "менше ніж (<)";
    case RULE_GE: return "більше або дорівнює (>=)";
    case RULE_LE: return "менше або дорівнює (<=)";
    case RULE_EQ: return "дорівнює (=)";
    case RULE_CONTAINS: return "містить текст";
  }
  return "дорівнює (=)";
}

String dayName(int d) {
  const char* n[] = {"Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Нд"};
  return (d >= 0 && d < 7) ? String(n[d]) : "?";
}

String getUptime() {
  unsigned long s = (millis() - bootTime) / 1000UL;
  unsigned long d = s / 86400UL; s %= 86400UL;
  unsigned long h = s / 3600UL; s %= 3600UL;
  unsigned long m = s / 60UL; s %= 60UL;
  char b[48];
  snprintf(b, sizeof(b), "%lu d %02lu:%02lu:%02lu", d, h, m, s);
  return String(b);
}

String formatBytes(size_t bytes) {
  if (bytes < 1024) return String(bytes) + " B";
  if (bytes < 1024UL * 1024UL) return String(bytes / 1024.0, 1) + " KB";
  return String(bytes / 1024.0 / 1024.0, 1) + " MB";
}

bool parseFloatStrict(const char* s, float& value) {
  if (!s || !*s) return false;
  char* end = nullptr;
  value = strtof(s, &end);
  if (end == s) return false;
  while (*end == ' ' || *end == '\t' || *end == '\r' || *end == '\n') end++;
  return *end == '\0' && isfinite(value);
}

bool mqttFilterMatches(const char* filter, const char* topic) {
  if (!filter || !topic) return false;
  size_t fi = 0;
  size_t ti = 0;
  while (true) {
    if (topic[ti] == '\0') {
      if (filter[fi] == '\0') return true;
      if (filter[fi] == '/' && filter[fi + 1] == '#' && filter[fi + 2] == '\0') return true;
      return false;
    }
    if (filter[fi] == '#') return true;
    size_t fstart = fi;
    size_t tstart = ti;
    while (filter[fi] != '\0' && filter[fi] != '/') fi++;
    while (topic[ti] != '\0' && topic[ti] != '/') ti++;
    bool plus = (fi - fstart == 1 && filter[fstart] == '+');
    if (!plus) {
      if ((fi - fstart) != (ti - tstart)) return false;
      for (size_t k = 0; k < fi - fstart; k++) {
        if (filter[fstart + k] != topic[tstart + k]) return false;
      }
    }
    if (filter[fi] == '\0') return topic[ti] == '\0';
    if (filter[fi] != '/') return false;
    if (topic[ti] != '/') {
      return filter[fi + 1] == '#' && filter[fi + 2] == '\0';
    }
    fi++;
    ti++;
  }
}

int getActiveClientsCount() {
  int n = 0;
  for (int i = 0; i < MAX_CLIENTS; i++) if (clients[i].active) n++;
  return n;
}
int getActiveTopicsCount() {
  int n = 0;
  for (int i = 0; i < MAX_TOPICS; i++) if (topics[i].topic[0]) n++;
  return n;
}
int getConfiguredRulesCount() {
  int n = 0;
  for (int i = 0; i < MAX_RULES; i++) if (rules[i].sourceTopic[0]) n++;
  return n;
}
int getConfiguredTimersCount() {
  int n = 0;
  for (int i = 0; i < MAX_TIMERS; i++) if (timers[i].targetTopic[0]) n++;
  return n;
}
int findFreeRuleSlot() {
  for (int i = 0; i < MAX_RULES; i++) if (!rules[i].sourceTopic[0]) return i;
  return -1;
}
int findFreeTimerSlot() {
  for (int i = 0; i < MAX_TIMERS; i++) if (!timers[i].targetTopic[0]) return i;
  return -1;
}

String navItem(const char* path, const char* icon, const char* label) {
  String item = "<a href='" + String(path) + "' class='nav-item";
  if (server.uri() == path) item += " active";
  item += "'><span class='icon'>" + String(icon) + "</span><span class='text'>" + String(label) + "</span></a>";
  return item;
}

String pageStart(const String& title, const String& header) {
  String h;
  h.reserve(5200);
  h += "<!doctype html><html lang='uk'><head><meta charset='UTF-8'>";
  h += "<meta name='viewport' content='width=device-width,initial-scale=1'>";
  h += "<title>" + htmlEscape(title) + "</title>";
  h += R"rawliteral(<style>
:root{--primary:#4f46e5;--primary-dark:#3730a3;--success:#15803d;--danger:#b91c1c;--warning:#b45309;--ink:#172033;--muted:#64748b;--surface:#fff;--line:#e2e8f0;--page:#f1f5f9}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;background:var(--page);color:var(--ink);min-height:100vh;display:flex}
.sidebar{width:248px;background:#111827;color:#fff;padding:22px 14px;position:fixed;height:100vh;overflow-y:auto}.sidebar .logo{font-size:21px;font-weight:750;margin:4px 10px 25px;letter-spacing:.2px}.sidebar .logo small{display:block;margin-top:4px;color:#94a3b8;font-size:12px;font-weight:500}
.sidebar .nav-item{display:flex;align-items:center;padding:11px 12px;color:#cbd5e1;text-decoration:none;border-radius:8px;margin:3px 0}.sidebar .nav-item:hover{background:#263247;color:#fff}.sidebar .nav-item.active{background:var(--primary);color:#fff}.sidebar .nav-item .icon{margin-right:10px;font-size:18px;width:22px;text-align:center}
.main-content{flex:1;margin-left:248px;padding:28px}.container{max-width:1180px;margin:0 auto}.header{background:var(--surface);border:1px solid var(--line);border-radius:14px;padding:22px 24px;margin-bottom:20px}.header h1{font-size:27px;color:var(--ink);margin-bottom:5px}.header .subtitle{color:var(--muted);font-size:14px}
.card{background:var(--surface);border:1px solid var(--line);border-radius:12px;padding:20px;margin-bottom:18px}.card h2{color:var(--ink);margin-bottom:14px;font-size:19px}.card h3{color:var(--ink);margin-bottom:10px;font-size:17px}p{line-height:1.55;margin:7px 0}table{width:100%;border-collapse:collapse}th{background:#eef2ff;color:#312e81;padding:11px;text-align:left;font-size:13px}td{padding:11px;border-bottom:1px solid var(--line)}tr:hover td{background:#f8fafc}
input,select{width:100%;padding:10px 12px;border:1px solid #cbd5e1;border-radius:8px;margin:6px 0 14px;font-size:15px;background:#fff}input:focus,select:focus{outline:3px solid #c7d2fe;border-color:var(--primary)}button,.button{padding:10px 15px;border:0;border-radius:8px;cursor:pointer;font-size:14px;font-weight:650;text-decoration:none;display:inline-block}.btn{background:var(--primary);color:#fff}.btn:hover{background:var(--primary-dark)}.green{background:var(--success);color:#fff}.red{background:var(--danger);color:#fff}.gray{background:#475569;color:#fff}
.status-on{color:var(--success);font-weight:700}.status-off{color:var(--muted);font-weight:700}.status-bad{color:var(--danger);font-weight:700}.small{color:var(--muted);font-size:13px}.rule-card{border:1px solid var(--line);border-left:4px solid #cbd5e1;background:var(--surface);border-radius:10px;padding:16px;margin-bottom:12px}.rule-enabled,.device-online{border-left-color:var(--success)}.rule-disabled{opacity:.72}.device-offline{border-left-color:var(--danger)}.device-unknown{border-left-color:#94a3b8}.rule-line{font-size:14px;margin:8px 0}.topic,.value{font-family:ui-monospace,SFMono-Regular,Consolas,monospace;background:#f1f5f9;padding:3px 6px;border-radius:5px;font-size:13px}.value{font-weight:650;color:#3730a3}.form-section{background:#f8fafc;border:1px solid var(--line);padding:16px;border-radius:10px;margin-bottom:14px}.days{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:10px}.day{padding:8px 12px;background:#e2e8f0;border-radius:18px;cursor:pointer;font-size:14px}.day-on{background:var(--primary);color:#fff}.timer-time{font-size:27px;font-weight:750;color:var(--ink);margin:8px 0}.stat{font-size:30px;font-weight:750;color:var(--primary)}.arrow{font-size:20px;margin:0 8px;color:#94a3b8}.ok{color:var(--success)}.warn{color:var(--warning)}.error{color:var(--danger)}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(155px,1fr));gap:12px}.stat-card{background:var(--surface);border:1px solid var(--line);border-radius:12px;padding:17px}.stat-card .icon{font-size:25px;margin-bottom:8px}.stat-card .value{font-size:26px;font-weight:750;color:var(--ink)}.stat-card .label{color:var(--muted);font-size:13px;margin-top:3px}.dot{display:inline-block;width:10px;height:10px;border-radius:50%;margin-right:6px}.dot-on{background:var(--success)}.dot-off{background:var(--danger)}.dot-unk{background:#94a3b8}
@media(max-width:760px){body{display:block}.sidebar{position:sticky;top:0;width:100%;height:auto;z-index:2;padding:10px 12px;display:flex;gap:4px;overflow-x:auto}.sidebar .logo{display:none}.sidebar .nav-item{white-space:nowrap;margin:0;padding:8px 10px}.sidebar .nav-item .icon{margin-right:5px}.main-content{margin-left:0;padding:15px}.header{padding:18px}.header h1{font-size:23px}.grid{grid-template-columns:repeat(2,minmax(0,1fr))}th,td{padding:8px;font-size:13px}}
</style>)rawliteral";
  h += R"rawliteral(<script>document.addEventListener('click',function(e){var a=e.target.closest('a[data-post]');if(!a)return;e.preventDefault();if(a.dataset.confirm&&!confirm(a.dataset.confirm))return;var f=document.createElement('form');f.method='POST';f.action=a.href;document.body.appendChild(f);f.submit();});</script>)rawliteral";
  h += "</head><body><div class='sidebar'>";
  h += "<div class='logo'>MQTT Broker<small>ESP32 · v" + String(FIRMWARE_VERSION) + "</small></div>";
  h += navItem("/", "🏠", "Головна");
  h += navItem("/devices", "📡", "Пристрої");
  h += navItem("/clients", "👥", "Клієнти");
  h += navItem("/topics", "📝", "Топіки");
  h += navItem("/retained", "💾", "Retained");
  h += navItem("/rules", "⚡", "Правила");
  h += navItem("/timers", "⏰", "Таймери");
  h += navItem("/sys", "📊", "$SYS");
  h += navItem("/settings", "⚙️", "Налаштування");
  h += navItem("/ota", "🔄", "OTA");
  h += "<a href='/restart' data-post data-confirm='Перезапустити ESP32?' class='nav-item'><span class='icon'>♻️</span><span class='text'>Перезапуск</span></a>";
  h += "</div><div class='main-content'><div class='container'>";
  h += "<div class='header'><h1>" + htmlEscape(header) + "</h1>";
  h += "<div class='subtitle'>ESP32 MQTT Broker v" + String(FIRMWARE_VERSION) + "</div></div>";
  return h;
}

String pageEnd() { return "</div></div></body></html>"; }

void redirectTo(const char* path) {
  server.sendHeader("Location", path);
  server.send(303);
}

void resetTimerRuntime(int i) {
  if (i >= 0 && i < MAX_TIMERS) {
    timerLastRunYear[i] = -1;
    timerLastRunYDay[i] = -1;
  }
}

// =====================================================
// CLIENTS / TOPICS
// =====================================================

void addClient(const char* id, const char* ip) {
  if (!id) return;
  for (int i = 0; i < MAX_CLIENTS; i++) {
    if (clients[i].active && strcmp(clients[i].clientId, id) == 0) {
      clients[i].lastActivity = millis();
      return;
    }
  }
  for (int i = 0; i < MAX_CLIENTS; i++) {
    if (!clients[i].active) {
      memset(&clients[i], 0, sizeof(clients[i]));
      strncpy(clients[i].clientId, id, sizeof(clients[i].clientId) - 1);
      strncpy(clients[i].ip, ip ? ip : "unknown", sizeof(clients[i].ip) - 1);
      clients[i].connectedAt = millis();
      clients[i].lastActivity = millis();
      clients[i].active = true;
      return;
    }
  }
  Serial.println("WARNING: local client table is full");
}

void removeClient(const char* id) {
  if (!id) return;
  for (int i = 0; i < MAX_CLIENTS; i++) {
    if (clients[i].active && strcmp(clients[i].clientId, id) == 0) {
      clients[i].active = false;
      return;
    }
  }
}

void updateClientActivity(const char* id) {
  if (!id) return;
  for (int i = 0; i < MAX_CLIENTS; i++) {
    if (clients[i].active && strcmp(clients[i].clientId, id) == 0) {
      clients[i].lastActivity = millis();
      return;
    }
  }
}

void storeMessage(const char* topic, const char* payload, int qos, bool retained) {
  if (!topic) return;
  for (int i = 0; i < MAX_TOPICS; i++) {
    if (strcmp(topics[i].topic, topic) == 0) {
      strncpy(topics[i].lastMessage, payload ? payload : "", sizeof(topics[i].lastMessage) - 1);
      topics[i].lastMessage[sizeof(topics[i].lastMessage) - 1] = 0;
      topics[i].lastUpdate = millis();
      topics[i].qos = qos;
      topics[i].retained = retained;
      return;
    }
  }
  for (int i = 0; i < MAX_TOPICS; i++) {
    if (!topics[i].topic[0]) {
      memset(&topics[i], 0, sizeof(topics[i]));
      strncpy(topics[i].topic, topic, sizeof(topics[i].topic) - 1);
      strncpy(topics[i].lastMessage, payload ? payload : "", sizeof(topics[i].lastMessage) - 1);
      topics[i].lastMessage[sizeof(topics[i].lastMessage) - 1] = 0;
      topics[i].lastUpdate = millis();
      topics[i].qos = qos;
      topics[i].retained = retained;
      return;
    }
  }
  int oldest = 0;
  for (int i = 1; i < MAX_TOPICS; i++) {
    if (topics[i].lastUpdate < topics[oldest].lastUpdate) oldest = i;
  }
  memset(&topics[oldest], 0, sizeof(topics[oldest]));
  strncpy(topics[oldest].topic, topic, sizeof(topics[oldest].topic) - 1);
  strncpy(topics[oldest].lastMessage, payload ? payload : "", sizeof(topics[oldest].lastMessage) - 1);
  topics[oldest].lastUpdate = millis();
  topics[oldest].qos = qos;
  topics[oldest].retained = retained;
}

// =====================================================
// RULES
// =====================================================

bool checkRuleCondition(MQTTRule& r, const char* payload) {
  if (!payload) return false;
  String p(payload);
  String v(r.value);
  if (r.condition == RULE_CONTAINS) return p.indexOf(v) >= 0;
  float pv = 0.0f;
  float rv = 0.0f;
  if (r.condition == RULE_EQ) {
    if (parseFloatStrict(payload, pv) && parseFloatStrict(r.value, rv))
      return fabsf(pv - rv) < 0.0001f;
    return p == v;
  }
  if (!parseFloatStrict(payload, pv) || !parseFloatStrict(r.value, rv)) return false;
  switch (r.condition) {
    case RULE_GT: return pv > rv;
    case RULE_LT: return pv < rv;
    case RULE_GE: return pv >= rv;
    case RULE_LE: return pv <= rv;
  }
  return false;
}

void executeRules(const char* topic, const char* payload) {
  if (!topic || !payload || ruleExecution) return;
  ruleExecution = true;
  for (int i = 0; i < MAX_RULES; i++) {
    MQTTRule& r = rules[i];
    if (!r.enabled || !r.sourceTopic[0]) continue;
    if (!mqttFilterMatches(r.sourceTopic, topic)) continue;
    if (!checkRuleCondition(r, payload)) continue;
    Serial.printf("RULE %d: %s -> %s = %s\n", i + 1, topic, r.targetTopic, r.targetPayload);
    broker.publish(std::string(r.targetTopic), std::string(r.targetPayload), r.qos, r.retain);
    messagesPublished++;
  }
  ruleExecution = false;
}

// Rules are larger than configuration values, so keep them in LittleFS rather
// than NVS. This avoids NVS fragmentation and leaves it for Wi-Fi/timers.
constexpr uint32_t RULES_FILE_MAGIC = 0x52554C45UL; // "RULE"
constexpr uint16_t RULES_FILE_VERSION = 1;
constexpr char RULES_FILE[] = "/rules.bin";
constexpr char RULES_TEMP_FILE[] = "/rules.tmp";
constexpr char RULES_BACKUP_FILE[] = "/rules.bak";

struct RulesFileHeader {
  uint32_t magic;
  uint16_t version;
  uint16_t ruleSize;
  uint16_t ruleCount;
  uint16_t reserved;
};

bool readRulesFile(const char* path) {
  File file = LittleFS.open(path, "r");
  if (!file) return false;
  RulesFileHeader header;
  bool valid = file.readBytes(reinterpret_cast<char*>(&header), sizeof(header)) == sizeof(header) &&
               header.magic == RULES_FILE_MAGIC &&
               header.version == RULES_FILE_VERSION &&
               header.ruleSize == sizeof(MQTTRule) &&
               header.ruleCount == MAX_RULES &&
               file.readBytes(reinterpret_cast<char*>(rules), sizeof(rules)) == sizeof(rules);
  file.close();
  if (!valid) memset(rules, 0, sizeof(rules));
  return valid;
}

bool saveRules() {
  // On the first run the LittleFS partition has no filesystem yet. Passing
  // true formats it only when mounting fails, then mounts it for use.
  if (!LittleFS.begin(true)) {
    Serial.println("ERROR: LittleFS mount failed; rules were not saved");
    return false;
  }

  LittleFS.remove(RULES_TEMP_FILE);
  File file = LittleFS.open(RULES_TEMP_FILE, "w");
  if (!file) return false;

  const RulesFileHeader header = {RULES_FILE_MAGIC, RULES_FILE_VERSION,
                                  sizeof(MQTTRule), MAX_RULES, 0};
  bool saved = file.write(reinterpret_cast<const uint8_t*>(&header), sizeof(header)) == sizeof(header) &&
               file.write(reinterpret_cast<const uint8_t*>(rules), sizeof(rules)) == sizeof(rules);
  file.close();
  if (!saved) {
    LittleFS.remove(RULES_TEMP_FILE);
    Serial.println("ERROR: rules file write failed");
    return false;
  }

  // Keep a recoverable previous copy until the new file has been installed.
  LittleFS.remove(RULES_BACKUP_FILE);
  if (LittleFS.exists(RULES_FILE) && !LittleFS.rename(RULES_FILE, RULES_BACKUP_FILE)) return false;
  if (!LittleFS.rename(RULES_TEMP_FILE, RULES_FILE)) {
    if (LittleFS.exists(RULES_BACKUP_FILE)) LittleFS.rename(RULES_BACKUP_FILE, RULES_FILE);
    return false;
  }
  LittleFS.remove(RULES_BACKUP_FILE);
  Serial.println("Rules saved to LittleFS");
  return true;
}

void loadRules() {
  memset(rules, 0, sizeof(rules));
  if (!LittleFS.begin(true)) {
    Serial.println("WARNING: LittleFS mount failed; no rules loaded");
    return;
  }
  if (readRulesFile(RULES_FILE)) {
    Serial.println("Rules loaded from LittleFS");
    return;
  }
  if (readRulesFile(RULES_BACKUP_FILE)) {
    Serial.println("Rules restored from LittleFS backup");
    return;
  }
  Serial.println("No saved rules found");
}

String ruleSummary(int i) {
  String s = "<b>ЯКЩО</b> ";
  s += "<span class='topic'>" + htmlEscape(String(rules[i].sourceTopic)) + "</span> ";
  s += "<b>" + htmlEscape(ruleOperator(rules[i].condition)) + "</b> ";
  s += "<span class='value'>" + htmlEscape(String(rules[i].value)) + "</span> ";
  s += "<b>→</b> ";
  s += "<span class='topic'>" + htmlEscape(String(rules[i].targetTopic)) + "</span> ";
  s += "<span class='value'>" + htmlEscape(String(rules[i].targetPayload)) + "</span>";
  return s;
}

void handleRules() {
  int edit = -1;
  if (server.hasArg("edit")) edit = server.arg("edit").toInt();
  if (edit < 0 || edit >= MAX_RULES || !rules[edit].sourceTopic[0]) edit = -1;

  String h = pageStart("Rules", "⚡ MQTT Rules — автоматизація");
  h += "<div class='card'><h2>Модель правила</h2><p><b>ЯКЩО</b> приходить повідомлення в topic (можна використовувати <code>+</code> та <code>#</code>), <b>І</b> payload відповідає умові, <b>ТО</b> брокер публікує інше повідомлення.</p></div>";

  h += "<div class='card'><h2>";
  h += (edit >= 0 ? "Редагування правила" : "➕ Створити правило");
  h += "</h2><form method='POST' action='/rule_save'>";
  h += "<input type='hidden' name='id' value='" + String(edit) + "'>";
  h += "<label>Назва</label><input name='name' maxlength='47' ";
  if (edit >= 0) h += "value='" + htmlEscape(String(rules[edit].name)) + "'";
  else h += "placeholder='Наприклад: Вентиляція'";
  h += ">";

  h += "<div class='form-section'><h3>📥 ЯКЩО</h3><label>Topic / filter</label><input name='source' required maxlength='127' ";
  if (edit >= 0) h += "value='" + htmlEscape(String(rules[edit].sourceTopic)) + "'";
  h += "><div class='small'>Приклади: <code>home/temp</code>, <code>home/+/temp</code>, <code>home/#</code>.</div></div>";

  h += "<div class='form-section'><h3>⚡ І значення</h3><select name='condition'>";
  for (int c = 0; c < 6; c++) {
    h += "<option value='" + String(c) + "'";
    if (edit >= 0 && rules[edit].condition == c) h += " selected";
    h += ">" + conditionName(c) + "</option>";
  }
  h += "</select><label>Значення</label><input name='value' required maxlength='63' ";
  if (edit >= 0) h += "value='" + htmlEscape(String(rules[edit].value)) + "'";
  h += "><div class='small'>Для &gt;, &lt;, &gt;=, &lt;= потрібні числові значення. Для «містить» — текст.</div></div>";

  h += "<div class='form-section'><h3>📤 ТО</h3><label>Topic</label><input name='target' required maxlength='127' ";
  if (edit >= 0) h += "value='" + htmlEscape(String(rules[edit].targetTopic)) + "'";
  h += "><label>Payload</label><input name='payload' required maxlength='255' ";
  if (edit >= 0) h += "value='" + htmlEscape(String(rules[edit].targetPayload)) + "'";
  h += "></div>";

  h += "<div class='form-section'><label>QoS</label><select name='qos'>";
  for (int q = 0; q <= 1; q++) {
    h += "<option value='" + String(q) + "'";
    if (edit >= 0 && rules[edit].qos == q) h += " selected";
    h += ">QoS " + String(q) + "</option>";
  }
  h += "</select><label>Retain</label><select name='retain'><option value='0'";
  if (edit < 0 || !rules[edit].retain) h += " selected";
  h += ">Ні</option><option value='1'";
  if (edit >= 0 && rules[edit].retain) h += " selected";
  h += ">Так</option></select></div>";

  h += "<button class='btn' type='submit'>" + String(edit >= 0 ? "💾 Зберегти зміни" : "➕ Створити правило") + "</button>";
  if (edit >= 0) h += " <a href='/rules'><button class='gray' type='button'>Скасувати</button></a>";
  h += "</form></div><h2>Мої правила</h2>";

  bool any = false;
  for (int i = 0; i < MAX_RULES; i++) {
    if (!rules[i].sourceTopic[0]) continue;
    any = true;
    h += "<div class='rule-card ";
    h += (rules[i].enabled ? "rule-enabled" : "rule-disabled");
    h += "'>";
    h += "<h3>" + htmlEscape(String(rules[i].name[0] ? rules[i].name : "Правило")) + " — ";
    h += (rules[i].enabled ? "<span class='status-on'>✅ УВІМКНЕНО</span>" : "<span class='status-off'>⭕ ВИМКНЕНО</span>");
    h += "</h3><div class='rule-line'>" + ruleSummary(i) + "</div>";
    h += "<p class='small'>Правило №" + String(i + 1) + " | QoS: " + String(rules[i].qos) + " | Retain: " + String(rules[i].retain ? "так" : "ні") + "</p>";
    h += "<a href='/rules?edit=" + String(i) + "'><button class='btn'>✏️ Редагувати</button></a> ";
    h += "<a href='/rule_toggle?id=" + String(i) + "' data-post class='button gray'>" + String(rules[i].enabled ? "⭕ Вимкнути" : "✅ Увімкнути") + "</a> ";
    h += "<a href='/rule_delete?id=" + String(i) + "' data-post data-confirm='Видалити це правило?' class='button red'>🗑️ Видалити</a></div>";
  }
  if (!any) h += "<div class='card'><p>📭 Правил ще немає.</p></div>";

  h += pageEnd();
  server.send(200, "text/html", h);
}

void handleRuleSave() {
  if (!server.hasArg("id")) { server.send(400, "text/plain", "Missing id"); return; }
  int id = server.arg("id").toInt();
  bool editing = id >= 0;
  if (editing && id >= MAX_RULES) { server.send(400, "text/plain", "Invalid id"); return; }
  if (!editing) {
    id = findFreeRuleSlot();
    if (id < 0) { server.send(400, "text/plain", "Максимум 16 правил"); return; }
  }
  MQTTRule r;
  memset(&r, 0, sizeof(r));
  r.enabled = editing ? rules[id].enabled : true;
  String name = server.arg("name");
  if (name.length()) strncpy(r.name, name.c_str(), sizeof(r.name) - 1);
  else snprintf(r.name, sizeof(r.name), "Правило %d", id + 1);
  strncpy(r.sourceTopic, server.arg("source").c_str(), sizeof(r.sourceTopic) - 1);
  r.condition = constrain(server.arg("condition").toInt(), 0, 5);
  strncpy(r.value, server.arg("value").c_str(), sizeof(r.value) - 1);
  strncpy(r.targetTopic, server.arg("target").c_str(), sizeof(r.targetTopic) - 1);
  strncpy(r.targetPayload, server.arg("payload").c_str(), sizeof(r.targetPayload) - 1);
  r.qos = constrain(server.arg("qos").toInt(), 0, 1);
  r.retain = server.arg("retain") == "1";
  if (!r.sourceTopic[0] || !r.targetTopic[0]) { server.send(400, "text/plain", "Topic cannot be empty"); return; }
  rules[id] = r;
  if (!saveRules()) { server.send(500, "text/plain; charset=utf-8", "Не вдалося зберегти правила у LittleFS"); return; }
  redirectTo("/rules");
}

void handleRuleDelete() {
  if (!server.hasArg("id")) { server.send(400, "text/plain", "Missing id"); return; }
  int id = server.arg("id").toInt();
  if (id < 0 || id >= MAX_RULES) { server.send(400, "text/plain", "Invalid id"); return; }
  memset(&rules[id], 0, sizeof(rules[id]));
  if (!saveRules()) { server.send(500, "text/plain; charset=utf-8", "Не вдалося зберегти правила у LittleFS"); return; }
  redirectTo("/rules");
}

void handleRuleToggle() {
  if (!server.hasArg("id")) { server.send(400, "text/plain", "Missing id"); return; }
  int id = server.arg("id").toInt();
  if (id < 0 || id >= MAX_RULES) { server.send(400, "text/plain", "Invalid id"); return; }
  if (rules[id].sourceTopic[0]) {
    rules[id].enabled = !rules[id].enabled;
    if (!saveRules()) { server.send(500, "text/plain; charset=utf-8", "Не вдалося зберегти правила у LittleFS"); return; }
  }
  redirectTo("/rules");
}

// =====================================================
// TIMERS
// =====================================================

void saveTimers() {
  preferences.begin("mqtt-timers", false);
  preferences.putBytes("timers_v2", timers, sizeof(timers));
  preferences.end();
}

void loadTimers() {
  memset(timers, 0, sizeof(timers));
  for (int i = 0; i < MAX_TIMERS; i++) resetTimerRuntime(i);
  preferences.begin("mqtt-timers", true);
  size_t n = preferences.getBytesLength("timers_v2");
  if (n == sizeof(timers)) {
    preferences.getBytes("timers_v2", timers, sizeof(timers));
    preferences.end();
    return;
  }
  preferences.end();
}

void timerDays(String& h, int id) {
  for (int d = 0; d < 7; d++) {
    if (timers[id].daysMask & (1 << d))
      h += "<span class='day day-on'>" + dayName(d) + "</span> ";
  }
}

void handleTimers() {
  int edit = -1;
  if (server.hasArg("edit")) edit = server.arg("edit").toInt();
  if (edit < 0 || edit >= MAX_TIMERS || !timers[edit].targetTopic[0]) edit = -1;

  String h = pageStart("Timers", "⏰ MQTT Timers — розклад");
  h += "<div class='card'><h2>🕐 Час ESP32</h2>";
  if (timeSynced) {
    struct tm t;
    if (getLocalTime(&t, 10)) {
      char b[32];
      strftime(b, sizeof(b), "%d.%m.%Y %H:%M:%S", &t);
      h += "<p class='timer-time'>" + String(b) + "</p><p class='status-on'>✅ NTP синхронізовано</p>";
    }
  } else {
    h += "<p class='status-off'>⭕ NTP не синхронізовано</p>";
  }
  h += "</div>";

  h += "<div class='card'><h2>" + String(edit >= 0 ? "✏️ Редагування таймера" : "➕ Створити таймер") + "</h2><form method='POST' action='/timer_save'>";
  h += "<input type='hidden' name='id' value='" + String(edit) + "'>";
  h += "<label>Назва</label><input name='name' maxlength='47' ";
  if (edit >= 0) h += "value='" + htmlEscape(String(timers[edit].name)) + "'";
  else h += "placeholder='Наприклад: Освітлення'";
  h += "><div class='form-section'><h3>🕐 Коли?</h3><label>Година</label><select name='hour'>";
  for (int x = 0; x < 24; x++) {
    h += "<option value='" + String(x) + "'";
    if (edit >= 0 && timers[edit].hour == x) h += " selected";
    h += ">" + String(x < 10 ? "0" : "") + String(x) + "</option>";
  }
  h += "</select><label>Хвилина</label><select name='minute'>";
  for (int x = 0; x < 60; x++) {
    h += "<option value='" + String(x) + "'";
    if (edit >= 0 && timers[edit].minute == x) h += " selected";
    h += ">" + String(x < 10 ? "0" : "") + String(x) + "</option>";
  }
  h += "</select></div>";

  h += "<div class='form-section'><h3>📅 У які дні?</h3><div class='days'>";
  for (int d = 0; d < 7; d++) {
    bool on = edit >= 0 && (timers[edit].daysMask & (1 << d));
    h += "<label class='day ";
    if (on) h += "day-on";
    h += "'><input type='checkbox' name='day" + String(d) + "' value='1'";
    if (on) h += " checked";
    h += " style='display:none'> " + dayName(d) + "</label>";
  }
  h += "</div></div>";

  h += "<div class='form-section'><h3>📤 Що зробити?</h3><label>Topic</label><input name='target' required maxlength='127' ";
  if (edit >= 0) h += "value='" + htmlEscape(String(timers[edit].targetTopic)) + "'";
  h += "><label>Payload</label><input name='payload' required maxlength='255' ";
  if (edit >= 0) h += "value='" + htmlEscape(String(timers[edit].targetPayload)) + "'";
  h += "></div>";

  h += "<div class='form-section'><label>QoS</label><select name='qos'>";
  for (int q = 0; q <= 1; q++) {
    h += "<option value='" + String(q) + "'";
    if (edit >= 0 && timers[edit].qos == q) h += " selected";
    h += ">QoS " + String(q) + "</option>";
  }
  h += "</select><label>Retain</label><select name='retain'><option value='0'";
  if (edit < 0 || !timers[edit].retain) h += " selected";
  h += ">Ні</option><option value='1'";
  if (edit >= 0 && timers[edit].retain) h += " selected";
  h += ">Так</option></select></div>";

  h += "<button class='btn' type='submit'>" + String(edit >= 0 ? "💾 Зберегти зміни" : "➕ Створити таймер") + "</button>";
  if (edit >= 0) h += " <a href='/timers'><button class='gray' type='button'>Скасувати</button></a>";
  h += "</form></div>";

  h += "<h2>📅 Мій розклад</h2>";
  bool any = false;
  for (int i = 0; i < MAX_TIMERS; i++) {
    if (!timers[i].targetTopic[0]) continue;
    any = true;
    h += "<div class='rule-card ";
    h += (timers[i].enabled ? "rule-enabled" : "rule-disabled");
    h += "'>";
    h += "<h3>" + htmlEscape(String(timers[i].name[0] ? timers[i].name : "Таймер")) + " — ";
    h += (timers[i].enabled ? "<span class='status-on'>✅ УВІМКНЕНО</span>" : "<span class='status-off'>⭕ ВИМКНЕНО</span>");
    h += "</h3>";
    h += "<div class='timer-time'>" + String(timers[i].hour < 10 ? "0" : "") + String(timers[i].hour) + ":" + String(timers[i].minute < 10 ? "0" : "") + String(timers[i].minute) + "</div><p>";
    timerDays(h, i);
    h += "</p><div class='rule-line'><b>📤 ОПУБЛІКУВАТИ</b> <span class='topic'>" + htmlEscape(String(timers[i].targetTopic)) + "</span> → <span class='value'>" + htmlEscape(String(timers[i].targetPayload)) + "</span></div>";
    h += "<p class='small'>Таймер №" + String(i + 1) + " | QoS: " + String(timers[i].qos) + " | Retain: " + String(timers[i].retain ? "так" : "ні") + "</p>";
    h += "<a href='/timers?edit=" + String(i) + "'><button class='btn'>✏️ Редагувати</button></a> ";
    h += "<a href='/timer_toggle?id=" + String(i) + "' data-post class='button gray'>" + String(timers[i].enabled ? "⭕ Вимкнути" : "✅ Увімкнути") + "</a> ";
    h += "<a href='/timer_delete?id=" + String(i) + "' data-post data-confirm='Видалити цей таймер?' class='button red'>🗑️ Видалити</a></div>";
  }
  if (!any) h += "<div class='card'><p>📭 Таймерів ще немає.</p></div>";

  h += pageEnd();
  server.send(200, "text/html", h);
}

void handleTimerSave() {
  if (!server.hasArg("id")) { server.send(400, "text/plain", "Missing id"); return; }
  int id = server.arg("id").toInt();
  bool editing = id >= 0;
  if (editing && id >= MAX_TIMERS) { server.send(400, "text/plain", "Invalid id"); return; }
  if (!editing) {
    id = findFreeTimerSlot();
    if (id < 0) { server.send(400, "text/plain", "Максимум 16 таймерів"); return; }
  }
  MQTTTimer t;
  memset(&t, 0, sizeof(t));
  t.enabled = editing ? timers[id].enabled : true;
  String name = server.arg("name");
  if (name.length()) strncpy(t.name, name.c_str(), sizeof(t.name) - 1);
  else snprintf(t.name, sizeof(t.name), "Таймер %d", id + 1);
  t.hour = constrain(server.arg("hour").toInt(), 0, 23);
  t.minute = constrain(server.arg("minute").toInt(), 0, 59);
  t.daysMask = 0;
  for (int d = 0; d < 7; d++) {
    if (server.hasArg("day" + String(d))) t.daysMask |= (1 << d);
  }
  if (t.daysMask == 0) { server.send(400, "text/plain", "Виберіть хоча б один день"); return; }
  strncpy(t.targetTopic, server.arg("target").c_str(), sizeof(t.targetTopic) - 1);
  strncpy(t.targetPayload, server.arg("payload").c_str(), sizeof(t.targetPayload) - 1);
  t.qos = constrain(server.arg("qos").toInt(), 0, 1);
  t.retain = server.arg("retain") == "1";
  if (!t.targetTopic[0]) { server.send(400, "text/plain", "Topic cannot be empty"); return; }
  timers[id] = t;
  resetTimerRuntime(id);
  saveTimers();
  redirectTo("/timers");
}

void handleTimerDelete() {
  if (!server.hasArg("id")) { server.send(400, "text/plain", "Missing id"); return; }
  int id = server.arg("id").toInt();
  if (id < 0 || id >= MAX_TIMERS) { server.send(400, "text/plain", "Invalid id"); return; }
  memset(&timers[id], 0, sizeof(timers[id]));
  resetTimerRuntime(id);
  saveTimers();
  redirectTo("/timers");
}

void handleTimerToggle() {
  if (!server.hasArg("id")) { server.send(400, "text/plain", "Missing id"); return; }
  int id = server.arg("id").toInt();
  if (id < 0 || id >= MAX_TIMERS) { server.send(400, "text/plain", "Invalid id"); return; }
  if (timers[id].targetTopic[0]) {
    timers[id].enabled = !timers[id].enabled;
    resetTimerRuntime(id);
    saveTimers();
  }
  redirectTo("/timers");
}

void checkTimers() {
  if (!brokerStarted || !timeSynced) return;
  struct tm t;
  if (!getLocalTime(&t, 10)) return;
  int day = (t.tm_wday == 0) ? 6 : t.tm_wday - 1;
  int year = t.tm_year + 1900;
  for (int i = 0; i < MAX_TIMERS; i++) {
    MQTTTimer& x = timers[i];
    if (!x.enabled || !x.targetTopic[0] || !(x.daysMask & (1 << day))) continue;
    if (t.tm_hour != x.hour || t.tm_min != x.minute) continue;
    if (timerLastRunYear[i] == year && timerLastRunYDay[i] == t.tm_yday) continue;
    Serial.printf("TIMER %d: %s = %s\n", i + 1, x.targetTopic, x.targetPayload);
    broker.publish(std::string(x.targetTopic), std::string(x.targetPayload), x.qos, x.retain);
    messagesPublished++;
    timerLastRunYear[i] = year;
    timerLastRunYDay[i] = t.tm_yday;
  }
}

// =====================================================
// DEVICES PAGE  (LWT monitoring)
// =====================================================

void handleDevices() {
  String h = pageStart("Devices", "📡 MQTT Devices — LWT моніторинг");

  h += "<div class='grid'>";
  h += "<div class='stat-card'><div class='icon'>🟢</div><div class='value'>" + String(getOnlineDevicesCount()) + "</div><div class='label'>Онлайн</div></div>";
  h += "<div class='stat-card'><div class='icon'>📡</div><div class='value'>" + String(getKnownDevicesCount()) + "</div><div class='label'>Відомо всього</div></div>";
  h += "<div class='stat-card'><div class='icon'>🔴</div><div class='value'>" + String(getKnownDevicesCount() - getOnlineDevicesCount()) + "</div><div class='label'>Офлайн</div></div>";
  h += "</div>";

  h += "<div class='card'><h2>📋 Як це працює</h2>";
  h += "<p>Кожен пристрій публікує статус у топік <span class='topic'>devices/&lt;id&gt;/status</span>:</p>";
  h += "<p>• після CONNECT — <b>online</b> (retained)</p>";
  h += "<p>• при аварійному відключенні брокер публікує <b>offline</b> через LWT</p>";
  h += "<p>• при коректному DISCONNECT Will <b>не</b> публікується</p>";
  h += "<a href='/devices_clear' data-post data-confirm='Очистити весь список пристроїв?' class='button red'>🗑️ Очистити список</a>";
  h += "</div>";

  h += "<h2>📡 Список пристроїв</h2>";

  bool any = false;
  for (int i = 0; i < MAX_DEVICES; i++) {
    if (!devices[i].active) continue;
    any = true;

    bool online = strcmp(devices[i].status, "online") == 0;
    bool offline = strcmp(devices[i].status, "offline") == 0;

    h += "<div class='rule-card ";
    if (online) h += "device-online";
    else if (offline) h += "device-offline";
    else h += "device-unknown";
    h += "'>";

    h += "<h3>";
    if (online) h += "<span class='dot dot-on'></span> <span class='status-on'>ONLINE</span>";
    else if (offline) h += "<span class='dot dot-off'></span> <span class='status-bad'>OFFLINE</span>";
    else h += "<span class='dot dot-unk'></span> <span class='status-off'>UNKNOWN</span>";
    h += " &nbsp; <b>" + htmlEscape(String(devices[i].id)) + "</b></h3>";

    h += "<div class='rule-line'>Статус topic: <span class='topic'>devices/" + htmlEscape(String(devices[i].id)) + "/status</span></div>";
    h += "<div class='rule-line'>Останнє значення: <span class='value'>" + htmlEscape(String(devices[i].lastPayload)) + "</span></div>";

    unsigned long seenSec = (millis() - devices[i].lastSeen) / 1000UL;
    unsigned long upSec = (millis() - devices[i].firstSeen) / 1000UL;

    h += "<p class='small'>";
    h += "Повідомлень: <b>" + String(devices[i].messages) + "</b> | ";
    h += "Востаннє: <b>" + String(seenSec) + " сек тому</b> | ";
    h += "Відомо: <b>" + String(upSec) + " сек</b>";
    h += "</p>";

    h += "<a href='/device?id=" + String(i) + "'><button class='btn'>🔍 Деталі</button></a> ";
    h += "<a href='/device_clear?id=" + String(i) + "' data-post data-confirm='Прибрати цей пристрій зі списку?' class='button gray'>🗑️ Прибрати зі списку</a>";
    h += "</div>";
  }
  if (!any) h += "<div class='card'><p>📭 Ще жоден пристрій не надсилав статус у <code>devices/&lt;id&gt;/status</code>.</p></div>";

  h += pageEnd();
  server.send(200, "text/html", h);
}

void handleDeviceDetail() {
  if (!server.hasArg("id")) { redirectTo("/devices"); return; }
  int id = server.arg("id").toInt();
  if (id < 0 || id >= MAX_DEVICES || !devices[id].active) { redirectTo("/devices"); return; }

  String h = pageStart("Device", "🔍 Пристрій: " + String(devices[id].id));

  bool online = strcmp(devices[id].status, "online") == 0;
  bool offline = strcmp(devices[id].status, "offline") == 0;

  h += "<div class='card'><h2>";
  if (online) h += "<span class='dot dot-on'></span> ONLINE";
  else if (offline) h += "<span class='dot dot-off'></span> OFFLINE";
  else h += "<span class='dot dot-unk'></span> UNKNOWN";
  h += " — " + htmlEscape(String(devices[id].id)) + "</h2>";

  h += "<table>";
  h += "<tr><th>Поле</th><th>Значення</th></tr>";
  h += "<tr><td>ID</td><td><b>" + htmlEscape(String(devices[id].id)) + "</b></td></tr>";
  h += "<tr><td>Статус</td><td>" + htmlEscape(String(devices[id].status)) + "</td></tr>";
  h += "<tr><td>Status topic</td><td><span class='topic'>devices/" + htmlEscape(String(devices[id].id)) + "/status</span></td></tr>";
  h += "<tr><td>Останній payload</td><td><span class='value'>" + htmlEscape(String(devices[id].lastPayload)) + "</span></td></tr>";
  h += "<tr><td>Повідомлень усього</td><td>" + String(devices[id].messages) + "</td></tr>";
  h += "<tr><td>Відомо з</td><td>" + String((millis() - devices[id].firstSeen) / 1000UL) + " сек тому</td></tr>";
  h += "<tr><td>Останній контакт</td><td>" + String((millis() - devices[id].lastSeen) / 1000UL) + " сек тому</td></tr>";
  if (devices[id].lastOnline)
    h += "<tr><td>Останній ONLINE</td><td>" + String((millis() - devices[id].lastOnline) / 1000UL) + " сек тому</td></tr>";
  if (devices[id].lastOffline)
    h += "<tr><td>Останній OFFLINE</td><td>" + String((millis() - devices[id].lastOffline) / 1000UL) + " сек тому</td></tr>";
  h += "</table>";

  h += "<br><a href='/devices'><button class='gray'>← Назад до пристроїв</button></a>";
  h += "</div>";

  h += pageEnd();
  server.send(200, "text/html", h);
}

void handleDeviceClear() {
  if (!server.hasArg("id")) { redirectTo("/devices"); return; }
  int id = server.arg("id").toInt();
  if (id >= 0 && id < MAX_DEVICES) memset(&devices[id], 0, sizeof(devices[id]));
  redirectTo("/devices");
}

void handleDevicesClear() {
  clearDevices();
  redirectTo("/devices");
}

// =====================================================
// WIFI CONFIGURATION
// =====================================================

void loadWiFiConfig(String& ssid, String& password) {
  ssid = "";
  password = "";
  String hostname = DEFAULT_HOSTNAME;

  preferences.begin("wifi", true);
  ssid = preferences.getString("ssid", "");
  password = preferences.getString("password", "");
  hostname = preferences.getString("hostname", DEFAULT_HOSTNAME);
  preferences.end();

  if (!ssid.length()) ssid = DEFAULT_WIFI_SSID;
  if (!password.length()) password = DEFAULT_WIFI_PASSWORD;
  if (!hostname.length()) hostname = DEFAULT_HOSTNAME;
  hostname.trim();
  if (!hostname.length()) hostname = DEFAULT_HOSTNAME;

  strncpy(HOSTNAME, hostname.c_str(), sizeof(HOSTNAME) - 1);
  HOSTNAME[sizeof(HOSTNAME) - 1] = '\0';
}

bool saveWiFiConfigIfChanged(const String& ssid, const String& password, const String& hostname) {
  String newSSID = ssid;
  String newPassword = password;
  String newHostname = hostname;
  newSSID.trim();
  newHostname.trim();

  if (!newSSID.length()) return false;
  if (!newHostname.length()) newHostname = DEFAULT_HOSTNAME;

  if (newSSID.length() > 63 || newPassword.length() > 63 || newHostname.length() > 32)
    return false;

  preferences.begin("wifi", false);
  String oldSSID = preferences.getString("ssid", "");
  String oldPassword = preferences.getString("password", "");
  String oldHostname = preferences.getString("hostname", "");

  bool changed = (oldSSID != newSSID) || (oldPassword != newPassword) || (oldHostname != newHostname);

  if (changed) {
    if (oldSSID != newSSID) preferences.putString("ssid", newSSID);
    if (oldPassword != newPassword) preferences.putString("password", newPassword);
    if (oldHostname != newHostname) preferences.putString("hostname", newHostname);
  }
  preferences.end();

  strncpy(HOSTNAME, newHostname.c_str(), sizeof(HOSTNAME) - 1);
  HOSTNAME[sizeof(HOSTNAME) - 1] = '\0';
  return true;
}

void resetWiFiConfigToDefaults() {
  preferences.begin("wifi", false);
  preferences.clear();
  preferences.end();
  strncpy(HOSTNAME, DEFAULT_HOSTNAME, sizeof(HOSTNAME) - 1);
  HOSTNAME[sizeof(HOSTNAME) - 1] = '\0';
}

void startConfigAP() {
  if (apMode) return;
  apMode = true;
  brokerStarted = false;
  WiFi.disconnect(true, false);
  delay(100);
  WiFi.mode(WIFI_AP);
  WiFi.softAP(WIFI_AP_SSID, WIFI_AP_PASSWORD);
  Serial.println("WiFi configuration AP started");
  Serial.print("AP SSID: "); Serial.println(WIFI_AP_SSID);
  Serial.print("AP IP: "); Serial.println(WiFi.softAPIP());
}

void handleSettings() {
  String savedSSID;
  String savedPassword;
  loadWiFiConfig(savedSSID, savedPassword);

  String h = pageStart("Settings", "⚙️ ESP32 MQTT Broker — Налаштування");
  h += "<div class='card'><h2>📡 Wi-Fi</h2>";

  if (apMode) {
    h += "<p class='warn'><b>⚠️ Режим налаштування Wi-Fi</b></p>";
    h += "<p>Підключіться до точки доступу <b>" + htmlEscape(String(WIFI_AP_SSID)) + "</b>, потім відкрийте <b>http://192.168.4.1</b>.</p>";
  } else {
    h += "<p>Статус: ";
    h += (WiFi.status() == WL_CONNECTED ? "<span class='status-on'>✅ ПІДКЛЮЧЕНО</span>" : "<span class='status-off'>⭕ НЕ ПІДКЛЮЧЕНО</span>");
    h += "</p>";
    h += "<p>IP: <b>" + (apMode ? WiFi.softAPIP().toString() : WiFi.localIP().toString()) + "</b></p>";
    if (WiFi.status() == WL_CONNECTED) h += "<p>RSSI: <b>" + String(WiFi.RSSI()) + " dBm</b></p>";
  }

  h += "<form method='POST' action='/wifi_save'>";
  h += "<label>Wi-Fi SSID</label><input name='ssid' maxlength='63' required value='" + htmlEscape(savedSSID) + "'>";
  h += "<label>Wi-Fi пароль</label><input name='password' type='password' maxlength='63' placeholder='Залишити поточний пароль — залиште поле порожнім'>";
  h += "<label>Hostname</label><input name='hostname' maxlength='32' value='" + htmlEscape(String(HOSTNAME)) + "'>";
  h += "<div class='small'>Hostname використовується також для mDNS: http://" + htmlEscape(String(HOSTNAME)) + ".local</div><br>";
  h += "<button class='btn' type='submit'>💾 Зберегти Wi-Fi</button></form></div>";

  h += "<div class='card'><h2>💾 Пам'ять Flash / NVS</h2>";
  h += "<p>Конфігурація Wi-Fi записується у Flash <b>тільки при зміні</b>.</p>";
  h += "<p>Статистика, uptime, клієнти, пристрої та кеш topic у Flash не записуються.</p>";
  h += "</div>";

  h += "<div class='card'><h2>🔧 Додатково</h2>";
  h += "<p class='small'>Кнопка нижче видаляє лише збережені Wi-Fi налаштування.</p>";
  h += "<form method='POST' action='/wifi_reset' onsubmit='return confirm(\"Видалити Wi-Fi налаштування?\")'>";
  h += "<button class='red' type='submit'>🗑️ Скинути Wi-Fi налаштування</button></form>";
  h += "</div>";

  h += pageEnd();
  server.send(200, "text/html", h);
}

void handleWiFiSave() {
  if (!server.hasArg("ssid")) { server.send(400, "text/plain", "SSID is required"); return; }
  String ssid = server.arg("ssid");
  String password = server.arg("password");
  String hostname = server.hasArg("hostname") ? server.arg("hostname") : String(DEFAULT_HOSTNAME);
  ssid.trim();
  hostname.trim();
  if (!ssid.length() || ssid.length() > 63 || password.length() > 63 || hostname.length() > 32) {
    server.send(400, "text/plain", "Invalid WiFi settings");
    return;
  }
  if (!password.length()) {
    String oldSSID;
    String oldPassword;
    loadWiFiConfig(oldSSID, oldPassword);
    password = oldPassword;
  }
  if (!saveWiFiConfigIfChanged(ssid, password, hostname)) {
    server.send(400, "text/plain", "Failed to save WiFi settings");
    return;
  }
  server.send(200, "text/html",
    "<!doctype html><html><meta charset='UTF-8'>"
    "<meta name='viewport' content='width=device-width,initial-scale=1'>"
    "<body style='font-family:Arial;padding:30px;background:linear-gradient(135deg,#667eea,#764ba2);color:white'>"
    "<h2>✅ Wi-Fi налаштування збережено</h2>"
    "<p>ESP32 зараз перезапуститься та підключиться до нового Wi-Fi.</p>"
    "</body></html>");
  delay(700);
  ESP.restart();
}

void handleWiFiReset() {
  resetWiFiConfigToDefaults();
  server.send(200, "text/html",
    "<!doctype html><html><meta charset='UTF-8'>"
    "<meta name='viewport' content='width=device-width,initial-scale=1'>"
    "<body style='font-family:Arial;padding:30px;background:linear-gradient(135deg,#667eea,#764ba2);color:white'>"
    "<h2>🗑️ Wi-Fi налаштування скинуто</h2>"
    "<p>ESP32 перезапускається.</p>"
    "</body></html>");
  delay(700);
  ESP.restart();
}

void handleRoot() {
  String h = pageStart("MQTT Broker", "🏠 ESP32 MQTT Broker");

  h += "<div class='grid'>";
  h += "<div class='stat-card'><div class='icon'>🔄</div><div class='value'>" + String(brokerStarted ? "ON" : "OFF") + "</div><div class='label'>MQTT Broker</div></div>";
  h += "<div class='stat-card'><div class='icon'>📡</div><div class='value'>" + String(getOnlineDevicesCount()) + "</div><div class='label'>Пристрої онлайн</div></div>";
  h += "<div class='stat-card'><div class='icon'>👥</div><div class='value'>" + String(getActiveClientsCount()) + "</div><div class='label'>Клієнти</div></div>";
  h += "<div class='stat-card'><div class='icon'>📝</div><div class='value'>" + String(getActiveTopicsCount()) + "</div><div class='label'>Топіки</div></div>";
  h += "<div class='stat-card'><div class='icon'>⚡</div><div class='value'>" + String(getConfiguredRulesCount()) + "</div><div class='label'>Правила</div></div>";
  h += "<div class='stat-card'><div class='icon'>⏰</div><div class='value'>" + String(getConfiguredTimersCount()) + "</div><div class='label'>Таймери</div></div>";
  h += "<div class='stat-card'><div class='icon'>⏱️</div><div class='value' style='font-size:20px'>" + getUptime() + "</div><div class='label'>Uptime</div></div>";
  h += "</div>";

  h += "<div class='card'><h2>🌐 Мережа</h2>";
  h += "<p>IP: <b>" + WiFi.localIP().toString() + "</b></p>";
  h += "<p>MQTT: <b>" + String(MQTT_PORT) + "</b> | Web: <b>" + String(WEB_PORT) + "</b></p>";
  h += "<p>mDNS: <b>" + String(HOSTNAME) + ".local</b></p>";
  h += "<p>MQTT auth: <b>" + String(MQTT_AUTH_ENABLED ? "УВІМКНЕНО" : "ВИМКНЕНО") + "</b></p>";
  h += "<p>Час: <b>";
  if (timeSynced) {
    struct tm t;
    if (getLocalTime(&t, 10)) {
      char b[32];
      strftime(b, sizeof(b), "%d.%m.%Y %H:%M:%S", &t);
      h += b;
    } else h += "помилка";
  } else h += "NTP не синхронізовано";
  h += "</b></p></div>";

  h += "<div class='card'><h2>📊 Статистика</h2>";
  h += "<p>Отримано MQTT повідомлень: <b>" + String(messagesReceived) + "</b></p>";
  h += "<p>Опубліковано правилами/таймерами/LWT: <b>" + String(messagesPublished) + "</b></p>";
  h += "<p>Отримано даних: <b>" + formatBytes(bytesReceived) + "</b></p>";
  h += "<p>Retained topic: <b>" + String(broker.getRetainedTopicCount()) + "</b></p>";
  h += "<p>Auth rejected: <b>" + String(authRejected) + "</b></p></div>";

  h += "<div class='card'><h2>💻 ESP32</h2>";
  h += "<p>Chip: <b>" + String(ESP.getChipModel()) + "</b></p>";
  h += "<p>CPU: <b>" + String(ESP.getCpuFreqMHz()) + " MHz</b></p>";
  h += "<p>Free heap: <b>" + formatBytes(ESP.getFreeHeap()) + "</b></p>";
  h += "<p>WiFi RSSI: <b>" + String(WiFi.status() == WL_CONNECTED ? WiFi.RSSI() : 0) + " dBm</b></p>";
  h += "<p>MAC: <b>" + WiFi.macAddress() + "</b></p></div>";

  h += pageEnd();
  server.send(200, "text/html", h);
}

void handleClients() {
  String h = pageStart("Clients", "👥 MQTT Clients");
  h += "<div class='card'><table><tr><th>Client ID</th><th>Підключений</th><th>Активність</th></tr>";
  for (int i = 0; i < MAX_CLIENTS; i++) {
    if (!clients[i].active) continue;
    h += "<tr><td><b>" + htmlEscape(String(clients[i].clientId)) + "</b></td>";
    h += "<td>" + String((millis() - clients[i].connectedAt) / 1000UL) + " сек</td>";
    h += "<td>" + String((millis() - clients[i].lastActivity) / 1000UL) + " сек тому</td></tr>";
  }
  h += "</table></div>" + pageEnd();
  server.send(200, "text/html", h);
}

void handleTopics() {
  String h = pageStart("Topics", "📝 MQTT Topics");
  h += "<div class='card'><a href='/clear' data-post data-confirm='Очистити кеш топіків?' class='button red'>🗑️ Очистити кеш топіків</a>";
  h += "<p class='small'>Максимум " + String(MAX_TOPICS) + " останніх topic.</p></div>";
  h += "<div class='card'><table><tr><th>Topic</th><th>Останнє повідомлення</th><th>Вік</th></tr>";
  for (int i = 0; i < MAX_TOPICS; i++) {
    if (!topics[i].topic[0]) continue;
    h += "<tr><td><span class='topic'>" + htmlEscape(String(topics[i].topic)) + "</span></td>";
    h += "<td>" + htmlEscape(String(topics[i].lastMessage)) + "</td>";
    h += "<td>" + String((millis() - topics[i].lastUpdate) / 1000UL) + " сек</td></tr>";
  }
  h += "</table></div>" + pageEnd();
  server.send(200, "text/html", h);
}

void handleClearTopics() {
  memset(topics, 0, sizeof(topics));
  redirectTo("/topics");
}

void handleRetained() {
  String h = pageStart("Retained", "💾 MQTT Retained Topics");
  unsigned long count = broker.getRetainedTopicCount();
  h += "<div class='card'><h2>Retained topics: " + String(count) + "</h2>";
  if (count == 0) h += "<p>📭 Retained topic немає.</p>";
  else {
    h += "<table><tr><th>#</th><th>Topic</th></tr>";
    for (unsigned long i = 0; i < count; i++) {
      String name = String(broker.getRetaiedTopicName(i).c_str());
      h += "<tr><td>" + String(i + 1) + "</td><td><span class='topic'>" + htmlEscape(name) + "</span></td></tr>";
    }
    h += "</table>";
  }
  h += "</div>" + pageEnd();
  server.send(200, "text/html", h);
}

void handleSys() {
  String h = pageStart("$SYS", "📊 $SYS Statistics");
  h += "<div class='card'><table><tr><th>MQTT $SYS topic</th><th>Значення</th></tr>";
  h += "<tr><td>$SYS/broker/version</td><td>" + String(FIRMWARE_VERSION) + "</td></tr>";
  h += "<tr><td>$SYS/broker/uptime</td><td>" + getUptime() + "</td></tr>";
  h += "<tr><td>$SYS/broker/clients/connected</td><td>" + String(getActiveClientsCount()) + "</td></tr>";
  h += "<tr><td>$SYS/broker/devices/online</td><td>" + String(getOnlineDevicesCount()) + "</td></tr>";
  h += "<tr><td>$SYS/broker/devices/known</td><td>" + String(getKnownDevicesCount()) + "</td></tr>";
  h += "<tr><td>$SYS/broker/topics</td><td>" + String(getActiveTopicsCount()) + "</td></tr>";
  h += "<tr><td>$SYS/broker/retained/count</td><td>" + String(broker.getRetainedTopicCount()) + "</td></tr>";
  h += "<tr><td>$SYS/broker/messages/received</td><td>" + String(messagesReceived) + "</td></tr>";
  h += "<tr><td>$SYS/broker/messages/published</td><td>" + String(messagesPublished) + "</td></tr>";
  h += "<tr><td>$SYS/broker/bytes/received</td><td>" + formatBytes(bytesReceived) + "</td></tr>";
  h += "<tr><td>$SYS/broker/auth/rejected</td><td>" + String(authRejected) + "</td></tr>";
  h += "<tr><td>$SYS/esp32/free_heap</td><td>" + String(ESP.getFreeHeap()) + "</td></tr>";
  h += "<tr><td>$SYS/esp32/min_free_heap</td><td>" + String(ESP.getMinFreeHeap()) + "</td></tr>";
  h += "<tr><td>$SYS/esp32/wifi_rssi</td><td>" + String(WiFi.status() == WL_CONNECTED ? WiFi.RSSI() : 0) + " dBm</td></tr>";
  h += "</table></div>" + pageEnd();
  server.send(200, "text/html", h);
}

// =====================================================
// JSON API
// =====================================================

void handleApiStatus() {
  String json;
  json.reserve(1800);
  json += "{";
  json += "\"firmware\":\"" + jsonEscape(FIRMWARE_VERSION) + "\",";
  json += "\"hostname\":\"" + jsonEscape(HOSTNAME) + "\",";
  json += "\"mqtt_port\":" + String(MQTT_PORT) + ",";
  json += "\"web_port\":" + String(WEB_PORT) + ",";
  json += "\"broker_started\":" + String(brokerStarted ? "true" : "false") + ",";
  json += "\"wifi_connected\":" + String(WiFi.status() == WL_CONNECTED ? "true" : "false") + ",";
  json += "\"ap_mode\":" + String(apMode ? "true" : "false") + ",";
  json += "\"ip\":\"" + jsonEscape(apMode ? WiFi.softAPIP().toString() : WiFi.localIP().toString()) + "\",";
  json += "\"clients\":" + String(getActiveClientsCount()) + ",";
  json += "\"devices_online\":" + String(getOnlineDevicesCount()) + ",";
  json += "\"devices_known\":" + String(getKnownDevicesCount()) + ",";
  json += "\"topic_cache\":" + String(getActiveTopicsCount()) + ",";
  json += "\"retained_topics\":" + String(broker.getRetainedTopicCount()) + ",";
  json += "\"rules\":" + String(getConfiguredRulesCount()) + ",";
  json += "\"timers\":" + String(getConfiguredTimersCount()) + ",";
  json += "\"messages_received\":" + String(messagesReceived) + ",";
  json += "\"messages_published\":" + String(messagesPublished) + ",";
  json += "\"bytes_received\":" + String(bytesReceived) + ",";
  json += "\"auth_rejected\":" + String(authRejected) + ",";
  json += "\"free_heap\":" + String(ESP.getFreeHeap()) + ",";
  json += "\"rssi\":" + String(WiFi.status() == WL_CONNECTED ? WiFi.RSSI() : 0) + ",";
  json += "\"uptime\":\"" + jsonEscape(getUptime()) + "\"";
  json += "}";
  server.send(200, "application/json", json);
}

void handleApiDevices() {
  String json = "[";
  bool first = true;
  for (int i = 0; i < MAX_DEVICES; i++) {
    if (!devices[i].active) continue;
    if (!first) json += ",";
    first = false;
    json += "{";
    json += "\"id\":\"" + jsonEscape(String(devices[i].id)) + "\",";
    json += "\"status\":\"" + jsonEscape(String(devices[i].status)) + "\",";
    json += "\"last_payload\":\"" + jsonEscape(String(devices[i].lastPayload)) + "\",";
    json += "\"messages\":" + String(devices[i].messages) + ",";
    json += "\"last_seen_sec\":" + String((millis() - devices[i].lastSeen) / 1000UL) + ",";
    json += "\"first_seen_sec\":" + String((millis() - devices[i].firstSeen) / 1000UL) + ",";
    json += "\"last_online_sec\":" + String(devices[i].lastOnline ? (millis() - devices[i].lastOnline) / 1000UL : 0) + ",";
    json += "\"last_offline_sec\":" + String(devices[i].lastOffline ? (millis() - devices[i].lastOffline) / 1000UL : 0);
    json += "}";
  }
  json += "]";
  server.send(200, "application/json", json);
}

void handleApiRetained() {
  String json = "[";
  unsigned long count = broker.getRetainedTopicCount();
  for (unsigned long i = 0; i < count; i++) {
    if (i) json += ",";
    json += "\"" + jsonEscape(String(broker.getRetaiedTopicName(i).c_str())) + "\"";
  }
  json += "]";
  server.send(200, "application/json", json);
}

// =====================================================
// OTA
// =====================================================

void handleOTAPage() {
  String h = pageStart("OTA", "🔄 OTA Update");
  h += "<div class='card'><h2>Оновлення прошивки</h2>";
  h += "<p>Після успішного оновлення ESP32 автоматично перезапуститься.</p>";
  h += "<form method='POST' action='/update' enctype='multipart/form-data'>";
  h += "<input type='file' name='firmware' required><br><br>";
  h += "<button class='btn' type='submit'>📤 Завантажити прошивку</button></form></div>";
  h += pageEnd();
  server.send(200, "text/html", h);
}

void handleOTAUpload() {
  HTTPUpload& u = server.upload();
  if (u.status == UPLOAD_FILE_START) {
    otaRunning = true;
    Serial.printf("OTA START: %s\n", u.filename.c_str());
    if (!Update.begin(ESP.getFreeSketchSpace())) Update.printError(Serial);
  }
  else if (u.status == UPLOAD_FILE_WRITE) {
    if (Update.isRunning()) {
      size_t written = Update.write(u.buf, u.currentSize);
      if (written != u.currentSize) Update.printError(Serial);
    }
  }
  else if (u.status == UPLOAD_FILE_END) {
    if (Update.end(true)) Serial.printf("OTA SUCCESS: %u bytes\n", (unsigned)u.totalSize);
    else Update.printError(Serial);
    otaRunning = false;
  }
  else if (u.status == UPLOAD_FILE_ABORTED) {
    Update.abort();
    otaRunning = false;
    Serial.println("OTA ABORTED");
  }
}

void handleOTAResult() {
  if (Update.hasError()) {
    server.send(500, "text/plain", "OTA update failed");
    return;
  }
  server.send(200, "text/html",
    "<html><body style='font-family:Arial;padding:30px;background:linear-gradient(135deg,#667eea,#764ba2);color:white'>"
    "<h2>✅ OTA successful. Restarting...</h2></body></html>");
  delay(1000);
  ESP.restart();
}

void handleRestart() {
  server.send(200, "text/html",
    "<html><body style='font-family:Arial;padding:30px;background:linear-gradient(135deg,#667eea,#764ba2);color:white'>"
    "<h2>♻️ ESP32 restarting...</h2></body></html>");
  delay(1000);
  ESP.restart();
}

void handleNotFound() {
  server.send(404, "text/plain", "Not found");
}

// =====================================================
// NTP
// =====================================================

void setupTime() {
  Serial.println("Starting NTP...");
  configTime(0, 0, "pool.ntp.org", "time.nist.gov", "time.google.com");
  setenv("TZ", TZ_INFO, 1);
  tzset();

  struct tm t;
  for (int i = 0; i < 20; i++) {
    if (getLocalTime(&t, 500)) {
      timeSynced = true;
      char b[32];
      strftime(b, sizeof(b), "%d.%m.%Y %H:%M:%S", &t);
      Serial.printf("NTP synchronized: %s\n", b);
      return;
    }
    delay(250);
  }
  timeSynced = false;
  Serial.println("NTP synchronization failed");
}

bool updateTimeStatus() {
  struct tm t;
  if (getLocalTime(&t, 10)) {
    timeSynced = true;
    return true;
  }
  timeSynced = false;
  return false;
}

// =====================================================
// $SYS
// =====================================================

void publishSys() {
  if (!brokerStarted) return;
  broker.publish("$SYS/broker/version", FIRMWARE_VERSION, 0, true);
  broker.publish("$SYS/broker/uptime", getUptime().c_str(), 0, true);
  broker.publish("$SYS/broker/clients/connected", String(getActiveClientsCount()).c_str(), 0, true);
  broker.publish("$SYS/broker/devices/online", String(getOnlineDevicesCount()).c_str(), 0, true);
  broker.publish("$SYS/broker/devices/known", String(getKnownDevicesCount()).c_str(), 0, true);
  broker.publish("$SYS/broker/topics", String(getActiveTopicsCount()).c_str(), 0, true);
  broker.publish("$SYS/broker/retained/count", String(broker.getRetainedTopicCount()).c_str(), 0, true);
  broker.publish("$SYS/broker/messages/received", String(messagesReceived).c_str(), 0, true);
  broker.publish("$SYS/broker/messages/published", String(messagesPublished).c_str(), 0, true);
  broker.publish("$SYS/broker/bytes/received", String(bytesReceived).c_str(), 0, true);
  broker.publish("$SYS/broker/auth/rejected", String(authRejected).c_str(), 0, true);
  broker.publish("$SYS/esp32/free_heap", String(ESP.getFreeHeap()).c_str(), 0, true);
  broker.publish("$SYS/esp32/min_free_heap", String(ESP.getMinFreeHeap()).c_str(), 0, true);
  broker.publish("$SYS/esp32/wifi_rssi", String(WiFi.status() == WL_CONNECTED ? WiFi.RSSI() : 0).c_str(), 0, true);
}

// =====================================================
// WIFI / SERVICES
// =====================================================

void connectWiFi() {
  String ssid;
  String password;
  loadWiFiConfig(ssid, password);

  Serial.println("\nConnecting WiFi...");
  Serial.print("SSID: "); Serial.println(ssid);

  apMode = false;
  WiFi.mode(WIFI_STA);
  WiFi.setHostname(HOSTNAME);
  WiFi.begin(ssid.c_str(), password.c_str());

  unsigned long started = millis();
  while (WiFi.status() != WL_CONNECTED && millis() - started < 20000UL) {
    delay(250);
    Serial.print('.');
  }
  Serial.println();

  if (WiFi.status() == WL_CONNECTED) {
    wifiWasConnected = true;
    nextWiFiReconnect = 0;
    wifiReconnectDelay = 5000UL;
    Serial.println("WiFi connected");
    Serial.print("IP address: "); Serial.println(WiFi.localIP());
  } else {
    wifiWasConnected = false;
    Serial.println("WiFi connection failed");
    startConfigAP();
  }
}

void startMDNS() {
  if (mdnsStarted) return;
  if (MDNS.begin(HOSTNAME)) {
    MDNS.addService("http", "tcp", WEB_PORT);
    MDNS.addService("mqtt", "tcp", MQTT_PORT);
    mdnsStarted = true;
    Serial.printf("mDNS: http://%s.local\n", HOSTNAME);
  } else {
    Serial.println("mDNS failed");
  }
}

void startWebServer() {
  if (webStarted) return;

  server.on("/", HTTP_GET, handleRoot);
  server.on("/devices", HTTP_GET, handleDevices);
  server.on("/device", HTTP_GET, handleDeviceDetail);
  server.on("/device_clear", HTTP_POST, handleDeviceClear);
  server.on("/devices_clear", HTTP_POST, handleDevicesClear);
  server.on("/clients", HTTP_GET, handleClients);
  server.on("/topics", HTTP_GET, handleTopics);
  server.on("/clear", HTTP_POST, handleClearTopics);
  server.on("/retained", HTTP_GET, handleRetained);

  server.on("/rules", HTTP_GET, handleRules);
  server.on("/rule_save", HTTP_POST, handleRuleSave);
  server.on("/rule_delete", HTTP_POST, handleRuleDelete);
  server.on("/rule_toggle", HTTP_POST, handleRuleToggle);

  server.on("/timers", HTTP_GET, handleTimers);
  server.on("/timer_save", HTTP_POST, handleTimerSave);
  server.on("/timer_delete", HTTP_POST, handleTimerDelete);
  server.on("/timer_toggle", HTTP_POST, handleTimerToggle);

  server.on("/sys", HTTP_GET, handleSys);
  server.on("/api/status", HTTP_GET, handleApiStatus);
  server.on("/api/devices", HTTP_GET, handleApiDevices);
  server.on("/api/retained", HTTP_GET, handleApiRetained);
  server.on("/settings", HTTP_GET, handleSettings);
  server.on("/wifi_save", HTTP_POST, handleWiFiSave);
  server.on("/wifi_reset", HTTP_POST, handleWiFiReset);

  server.on("/ota", HTTP_GET, handleOTAPage);
  server.on("/update", HTTP_POST, handleOTAResult, handleOTAUpload);
  server.on("/restart", HTTP_POST, handleRestart);

  server.onNotFound(handleNotFound);
  server.begin();
  webStarted = true;
  Serial.println("Web server started");
}

void startMQTTBroker() {
  if (brokerStarted) return;
  Serial.printf("Starting MQTT broker on port %d\n", MQTT_PORT);
  bool ok = broker.init(MQTT_PORT, true);
  brokerStarted = ok;
  Serial.println(ok ? "MQTT broker started" : "MQTT broker FAILED");
}

void checkWiFi() {
  if (millis() - lastWiFiCheck < WIFI_CHECK_INTERVAL) return;
  lastWiFiCheck = millis();

  bool connected = WiFi.status() == WL_CONNECTED;

  if (!connected) {
    if (wifiWasConnected) {
      Serial.println("WiFi lost");
      wifiWasConnected = false;
      brokerStarted = false;
    }
    if (apMode) return;
    if (millis() < nextWiFiReconnect) return;

    String ssid;
    String password;
    loadWiFiConfig(ssid, password);
    Serial.println("WiFi reconnect attempt...");
    WiFi.mode(WIFI_STA);
    WiFi.setHostname(HOSTNAME);
    WiFi.begin(ssid.c_str(), password.c_str());
    nextWiFiReconnect = millis() + wifiReconnectDelay;
    if (wifiReconnectDelay < 60000UL) wifiReconnectDelay += 5000UL;
    return;
  }

  if (!wifiWasConnected) {
    Serial.println("WiFi restored");
    wifiWasConnected = true;
    apMode = false;
    wifiReconnectDelay = 5000UL;
    nextWiFiReconnect = 0;

    setupTime();
    startMDNS();
    startWebServer();

    if (brokerStarted) {
      broker.restart();
      Serial.println("MQTT broker restarted");
    } else {
      startMQTTBroker();
    }
  }
  updateTimeStatus();
}

// =====================================================
// SETUP / LOOP
// =====================================================

void setup() {
  Serial.begin(115200);
  delay(500);

  Serial.println();
  Serial.println("================================");
  Serial.println("ESP32 MQTT BROKER");
  Serial.printf("Firmware: %s (LWT enabled)\n", FIRMWARE_VERSION);
  Serial.println("================================");

  bootTime = millis();
  memset(clients, 0, sizeof(clients));
  memset(topics, 0, sizeof(topics));
  memset(rules, 0, sizeof(rules));
  memset(timers, 0, sizeof(timers));
  memset(devices, 0, sizeof(devices));

  for (int i = 0; i < MAX_TIMERS; i++) resetTimerRuntime(i);

  loadRules();
  loadTimers();

  connectWiFi();
  startWebServer();

  if (WiFi.status() == WL_CONNECTED) {
    setupTime();
    startMDNS();
    startMQTTBroker();
  } else {
    Serial.println("WiFi unavailable. Configuration AP is active.");
    Serial.print("Open: http://");
    Serial.println(WiFi.softAPIP());
  }
}

void loop() {
  if (brokerStarted) broker.update();
  if (webStarted) server.handleClient();

  checkWiFi();
  checkTimers();

  if (brokerStarted && millis() - lastSysPublish >= SYS_INTERVAL) {
    lastSysPublish = millis();
    publishSys();
  }

  delay(1);
}
