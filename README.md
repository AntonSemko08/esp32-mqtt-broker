# ESP32 MQTT Broker

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Platform: ESP32](https://img.shields.io/badge/platform-ESP32-blue.svg)](https://www.espressif.com/)
[![Framework: Arduino](https://img.shields.io/badge/framework-Arduino-green.svg)](https://www.arduino.cc/)

A self-contained MQTT 3.1.1 broker for **ESP32** with a web dashboard, automation rules, timers, **Last Will and Testament (LWT)** device monitoring, and OTA updates. Runs entirely on a single ESP32 — no Raspberry Pi, no server, no cloud.

---

## Features

- 📡 **MQTT 3.1.1 broker** on port 1883
- 🌐 **Web dashboard** — live device status, topics, retained messages
- 📱 **LWT device monitoring** — track online/offline state of every device automatically
- ⚡ **Automation rules** — IF/THEN with `>`, `<`, `>=`, `<=`, `=`, `contains`, `+`, `#`
- ⏰ **Scheduled timers** — daily/weekly jobs via NTP
- 💾 **Retained messages** — stored in RAM
- 📊 **$SYS topics** — broker statistics
- 📜 **Message log** — recent 40 MQTT messages in RAM
- 🔐 **Optional MQTT authentication**
- 🔄 **OTA updates** via web interface
- ♻️ **Automatic Wi-Fi reconnect** with exponential backoff
- 🌍 **mDNS** — access via `http://mqtt-broker.local`
- 🛡️ **Config AP fallback** — first-boot setup without hardcoded credentials
- 🔧 **NVS-backed configuration** — Wi-Fi, rules, timers survive reboots

---

## Why?

If you have a small home-lab or IoT project and don't want to run a full Mosquitto on a Raspberry Pi, this firmware gives you a functional MQTT broker with a GUI in a 5×5 cm package. Perfect for:

- ESPHome / Tasmota / Zigbee2MQTT devices that need a local broker
- Small home automation setups without a server
- Off-grid installations (solar, RV, cabin) where a Pi is overkill
- Learning MQTT and home automation on a single board

---

## Hardware requirements

| Item  | Minimum              | Recommended      |
|-------|----------------------|------------------|
| Chip  | ESP32 (any variant)  | ESP32-WROOM-32   |
| Flash | 4 MB                 | 4 MB             |
| RAM   | 320 KB free          | 500 KB free      |
| Power | 5 V / 500 mA         | 5 V / 1 A        |

Tested on: ESP32-WROOM-32, ESP32-C3, ESP32-S3.

---

## Quick start

### 1. Install dependencies

- [Arduino IDE](https://www.arduino.cc/en/software) 2.x
- [ESP32 board support](https://github.com/espressif/arduino-esp32) ≥ 3.0.0
- [sMQTTBroker library (LWT fork)](https://github.com/AntonSemko08/sMQTTBroker)

### 2. First boot

No configuration needed. On first boot, ESP32 starts a **configuration access point**:

| Field    | Value                |
|----------|----------------------|
| SSID     | `ESP32-MQTT-Setup`   |
| Password | `mqttsetup`          |
| URL      | `http://192.168.4.1` |

1. Connect your phone or laptop to `ESP32-MQTT-Setup`.
2. Open `http://192.168.4.1` in a browser.
3. Go to **Settings**, enter your Wi-Fi SSID, password, and hostname.
4. Click **Save**. ESP32 reboots and connects to your network.
5. Find the broker's IP (check your router or use `http://mqtt-broker.local`).

Wi-Fi credentials are stored in NVS and survive reboots and firmware updates.

> **Note for developers:** You can hardcode default credentials in the sketch by editing `DEFAULT_WIFI_SSID` and `DEFAULT_WIFI_PASSWORD` at the top of `esp32-mqtt-broker.ino`. Never commit real credentials to a public repository.

### 3. Flash

Open `esp32-mqtt-broker.ino` in Arduino IDE, select **ESP32 Dev Module**, and upload.

### 4. Open the web UI

```
http://mqtt-broker.local/
```

or

```
http://<broker-ip>/
```

---

## Web interface

| Page            | Description                              |
|-----------------|------------------------------------------|
| `/`             | Dashboard — stats, network, devices      |
| `/devices`      | Device list with online/offline status   |
| `/device?id=N`  | Single device details                    |
| `/clients`      | Connected MQTT clients                   |
| `/topics`       | Recent topic cache                       |
| `/retained`     | Retained topics                          |
| `/rules`        | Automation rules                         |
| `/timers`       | Scheduled timers                         |
| `/sys`          | $SYS statistics                          |
| `/logs`         | Recent MQTT message log                  |
| `/settings`     | Wi-Fi configuration                      |
| `/ota`          | Firmware update                          |
| `/restart`      | Reboot the broker                        |

---

## Device LWT monitoring

The broker can display the online/offline status of every MQTT device in real time. For this to work, each device must publish its status to a topic with this pattern:

```
devices/<id>/status
```

Publish `online` after CONNECT (retained), and let LWT publish `offline` on unexpected disconnect.

### ESPHome example

```yaml
mqtt:
  broker: 192.168.1.7
  port: 1883
  topic_prefix: my-device

  birth_message:
    topic: devices/my-device/status
    payload: online
    qos: 0
    retain: true

  will_message:
    topic: devices/my-device/status
    payload: offline
    qos: 0
    retain: true
```

### PubSubClient (Arduino) example

```cpp
client.connect(
    "my-device",
    nullptr, nullptr,
    "devices/my-device/status",  // will topic
    0,                           // will qos
    true,                        // will retain
    "offline"                    // will message
);
client.publish("devices/my-device/status", "online", true);
```

Now the broker will display the device on `/devices` and toggle its state automatically.

---

## Automation rules

Open `/rules` and create an IF/THEN rule:

| Field          | Example                          |
|----------------|----------------------------------|
| IF Topic       | `devices/generator/status`       |
| Condition      | `equals`                         |
| Value          | `online`                         |
| THEN Topic     | `ssonoff-s60tpf/relay/command`   |
| THEN Payload   | `OFF`                            |

Supported conditions: `>`, `<`, `>=`, `<=`, `=`, `contains`.
Topic wildcards: `+` (single level), `#` (multi level).

**Example use case:** when a generator comes online, turn off two sockets automatically.

### Rule model

```
IF   <topic> matches <filter>
AND  <payload> <condition> <value>
THEN publish <payload> to <target topic>
```

Rules are evaluated on every incoming MQTT PUBLISH, including internal ones (LWT, other rules, timers). Recursion is prevented by an internal flag.

---

## Timers

Open `/timers` and create a scheduled job:

- Hour + minute
- Days of the week (multi-select)
- Target topic + payload

Requires NTP synchronization (automatic on boot when Wi-Fi is available).

**Example use case:** turn on the garden lights every day at 18:30, Monday–Friday.

---

## JSON API

| Endpoint             | Description                       |
|----------------------|-----------------------------------|
| `GET /api/status`    | Broker status and statistics      |
| `GET /api/devices`   | List of known devices with state  |
| `GET /api/retained`  | List of retained topic names      |

Example:

```bash
curl http://mqtt-broker.local/api/devices
```

```json
[
  {
    "id": "generator",
    "status": "online",
    "last_payload": "online",
    "messages": 42,
    "last_seen_sec": 3,
    "first_seen_sec": 3600,
    "last_online_sec": 3,
    "last_offline_sec": 120
  }
]
```

---

## OTA updates

1. Open `/ota` in a browser.
2. Select a `.bin` file (from `firmware/` releases or your build).
3. Click **Upload**.
4. The broker will reboot automatically.

---

## Architecture

```
┌──────────────────────────────────────────────────────┐
│                       ESP32                          │
│                                                      │
│  ┌─────────────┐        ┌─────────────────────────┐  │
│  │ sMQTTBroker │◄──────►│ WebServer (port 80)     │  │
│  │ port 1883   │        │ /devices /rules /logs   │  │
│  └──────┬──────┘        └─────────────────────────┘  │
│         │                                            │
│  ┌──────▼──────┐        ┌─────────────────────────┐  │
│  │ LWT device  │        │ Rules engine            │  │
│  │ tracker     │◄──────►│ Timers (NTP)            │  │
│  │             │        │ $SYS publisher          │  │
│  └─────────────┘        └─────────────────────────┘  │
│                                                      │
│  ┌────────────────────────────────────────────────┐  │
│  │ Preferences (NVS)                              │  │
│  └────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────┘
```

See [`docs/architecture.txt`](docs/architecture.txt) for details.

---

## Building from source

### Arduino IDE

1. Install the ESP32 board package (≥ 3.0.0).
2. Install the `sMQTTBroker` library.
3. Open `esp32-mqtt-broker.ino`.
4. Select board **ESP32 Dev Module**.
5. Upload.

### PlatformIO

```ini
[env:esp32dev]
platform = espressif32
board = esp32dev
framework = arduino
monitor_speed = 115200

lib_deps =
    https://github.com/AntonSemko08/sMQTTBroker.git
```

---

## Configuration

### Wi-Fi

Credentials are stored in NVS. On first boot, empty defaults are used, and the configuration AP is started. Change them later via `/settings`.

### MQTT authentication

Enable in the sketch:

```cpp
#define MQTT_AUTH_ENABLED true
const char* MQTT_USERNAME = "your-user";
const char* MQTT_PASSWORD = "your-pass";
```

When enabled, the broker will reject clients that supply wrong credentials.

---

## FAQ

**Q: How many MQTT clients can connect?**
A: The firmware is limited to `MAX_CLIENTS = 20`. ESP32 has enough RAM for more, but performance drops.

**Q: Are retained messages persisted across reboots?**
A: No — retained messages are stored in RAM only. Persistent retained storage is on the roadmap.

**Q: Can I use this broker as a public MQTT server?**
A: No. It has no TLS, no rate limiting, and limited client slots. Use it on a trusted LAN only.

**Q: Does it support MQTT 5.0?**
A: No. Only MQTT 3.1.1.

**Q: Can I change the MQTT port?**
A: Yes — edit `#define MQTT_PORT 1883` in the sketch.

**Q: Why does the device list show "unknown" instead of online/offline?**
A: The device is publishing to a topic that is not `devices/<id>/status`. Either fix the device config or add a rule.

---

## Roadmap

- [ ] Persistent retained messages (LittleFS)
- [ ] TLS (secure MQTT)
- [ ] WebSocket MQTT
- [ ] Import/export rules and timers as JSON
- [ ] Multi-action rules
- [ ] Bridge to external MQTT broker

---

## License

MIT — see [LICENSE](LICENSE).

---

## Contributing

Pull requests and issues are welcome.

---

## Acknowledgements

- [sMQTTBroker library (LWT fork)](https://github.com/AntonSemko08/sMQTTBroker)
- The ESP32 Arduino core team
- Everyone who filed issues and tested the firmware
