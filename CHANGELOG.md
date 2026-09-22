# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [4.5.0] — 2025-XX-XX

### Added

- Full MQTT Last Will and Testament (LWT) device monitoring
- `/devices` page with online/offline status and per-device details
- `/logs` page — recent MQTT message ring buffer (40 entries)
- `/api/devices` JSON endpoint
- `$SYS/broker/devices/online` and `$SYS/broker/devices/known`
- Live web UI — auto-refresh every 5 s without page reload
- mDNS support (`http://mqtt-broker.local`)
- Configuration AP fallback when Wi-Fi is unavailable
- NVS-backed Wi-Fi credentials
- Per-device "Details" page (`/device?id=N`)
- Config AP: `ESP32-MQTT-Setup` / `mqttsetup`

### Changed

- Devices sorted: online first
- Active nav item highlighted in sidebar
- Replaced `<meta refresh>` with `fetch()`-based updates
- Empty default Wi-Fi credentials — no hardcoded secrets

### Fixed

- Rules are now persisted correctly after reboot (per-rule NVS keys)
- Timers are now persisted correctly after reboot (per-timer NVS keys)

## [4.3.1] — 2024-XX-XX

### Added

- Initial public release
- MQTT 3.1.1 broker on port 1883
- Web dashboard with statistics
- Automation rules (IF/THEN)
- Scheduled timers via NTP
- Retained messages
- $SYS topics
- OTA updates via web interface
- mDNS support
