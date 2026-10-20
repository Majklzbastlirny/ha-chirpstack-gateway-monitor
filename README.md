# ChirpStack Gateway Monitor for Home Assistant

A Home Assistant integration that watches the MQTT traffic between ChirpStack
gateways (Gateway Bridge / MQTT Forwarder) and ChirpStack, and turns it into
gateway and spectrum health sensors:

- gateway online state, RX/TX packet counters, bad CRC, CRC-OK ratio
- packets per **channel** and per **spreading factor**, received and transmitted
- **channel airtime** (how busy each frequency is), noise floor, average RSSI/SNR
- **non-LoRaWAN traffic detection** and a **top talker** with per-gateway RSSI
  (for hunting interferers)
- downlink results (TX acknowledgements, failures), TX duty cycle
- optional tracking of your own nodes by DevAddr: RSSI, SNR, packet loss,
  gateways that heard it

It uses Home Assistant's **existing MQTT integration** – no second broker
connection and no extra Python packages. The protobuf payloads of the gateway
bridge are decoded by a small built-in decoder; the JSON marshaler works too.

## Requirements

- Home Assistant 2025.2 or newer, with the MQTT integration connected to the
  broker ChirpStack uses.
- The Home Assistant MQTT user must be allowed to read
  `<prefix>/gateway/+/event/+`, `<prefix>/gateway/+/state/+` and
  `<prefix>/gateway/+/command/down`.
- ChirpStack v4 (topics like `eu868/gateway/<gateway id>/event/up`). ChirpStack
  v3 topics (`gateway/<gateway id>/…`) work with an empty prefix.

## Installation

### With HACS (recommended)

[![Open your Home Assistant instance and open this repository in HACS.](https://my.home-assistant.io/badges/hacs_repository.svg)](https://my.home-assistant.io/redirect/hacs_repository/?owner=Majklzbastlirny&repository=ha-chirpstack-gateway-monitor&category=integration)

The button adds the repository to HACS on your Home Assistant. Or by hand:

1. Install [HACS](https://hacs.xyz/docs/use/) if you do not have it yet.
2. In Home Assistant open **HACS**, the **⋮** menu (top right) and **Custom repositories**.
3. Repository: `https://github.com/Majklzbastlirny/ha-chirpstack-gateway-monitor`, type: **Integration**, then **Add**.
4. Search HACS for **ChirpStack Gateway Monitor**, open it and **Download**.
5. Restart Home Assistant (*Settings → System → ⏻ → Restart*).

HACS then offers updates like for any other integration.

### Manually

Copy `custom_components/chirpstack_gateway_monitor` from this repository to
`/config/custom_components/` (so that `/config/custom_components/chirpstack_gateway_monitor/manifest.json`
exists) and restart Home Assistant. Updates: copy the folder again and restart.

### Set it up

*Settings → Devices & services → Add integration → ChirpStack Gateway
Monitor*:

| Field | Meaning |
|---|---|
| Topic prefix | ChirpStack region prefix, `eu868` by default |
| Own device addresses | Optional DevAddrs of your nodes, e.g. `00a1b2c3` |

Gateways appear automatically as soon as they publish something (stats come
every 30 s). Rename the gateway devices (e.g. *SenseCAP*, *Home gateway*,
*Lab RAK2246*); Home Assistant offers to rename the entity IDs with them.

**Options** (*Configure* on the integration): own DevAddrs (update this when a
node re-joins with a new DevAddr), update interval (60 s), statistics window
(10 min) and device window (60 min).

## Entities

### Gateway (one device per gateway)

| Entity | Notes |
|---|---|
| Online | From the retained `state/conn` message and the stats heartbeat (offline after 3 missed stats, ≥ 90 s) |
| Last seen | |
| RX packets, RX packets bad CRC, TX packets emitted | Totals from the gateway stats. *RX packets CRC OK* and *TX packets requested* exist but are disabled by default |
| RX rate, CRC OK ratio | Over the statistics window |
| LoRaWAN packets, Non-LoRaWAN packets | Received packets classified by the integration |
| RX *freq* MHz, RX SF*n*, TX *freq* MHz | Per-channel / per-SF counters, created when first seen |
| Airtime *freq* MHz, Peak channel airtime | Share of time the channel carried packets this gateway decoded |
| Average RSSI, Average SNR | LoRaWAN packets only – interferers do not skew them |
| Noise floor | Estimated from weak packets, see below |
| Downlinks failed | Attribute `tx_status` holds all TX ack results (OK, TOO_LATE, …) |
| TX duty cycle | Time on air of emitted downlinks over the device window |
| Duty cycle budget used | Only if the gateway reports duty-cycle stats (ChirpStack Concentratord) |

### LoRaWAN network (one per topic prefix)

Gateways online, unique packets (de-duplicated across gateways), LoRaWAN /
non-LoRaWAN packets, join requests, packet rate, devices heard (attribute
`networks`: devices per NetID), **Top talker** and **Top talker share**. The
top talker's attributes list the ten busiest sources with a payload sample for
non-LoRaWAN ones, and the RSSI/SNR each gateway hears the top talker with.

### Own nodes (one per configured DevAddr)

RSSI and SNR (best over the gateways that heard the last uplink, attribute
`gateway` says which one), gateways, packets, packet loss (FCnt gaps over the
device window), frame counter, spreading factor, frequency, airtime.

Per gateway that hears the node, created automatically and named after the
gateway device (rename gateways first to get e.g. *RSSI SenseCAP*):

| Entity | Notes |
|---|---|
| RSSI *gateway*, SNR *gateway* | Last uplink that gateway received; unknown when it has not heard the node within the device window |
| Reception *gateway* | Share of the node's uplinks the gateway received over the device window. Uplinks nobody heard are counted from FCnt gaps, so the gateways' receptions and the packet loss use the same total |

Graph *Reception* per gateway to compare antennas, cables or locations.

## How the numbers are made

- **Bad CRC** = `rx_packets_received − rx_packets_received_ok` from the gateway
  stats. Gateways normally forward only CRC-OK packets, so everything
  per-channel or per-SF counts forwarded packets.
- **Non-LoRaWAN**: SF5/SF6 (not used by LoRaWAN), MHDR RFU bits or Major set,
  proprietary MType, or impossible lengths (a JoinRequest must be 23 bytes, a
  RejoinRequest 19/24, FOpts must fit). A frame starting with `0xC8` looks like a
  RejoinRequest to a naive parser, but its RFU bits are set.
- **Airtime** uses the Semtech time-on-air formula (SX126x variant for SF5/6).
  It only includes packets the gateway could decode, so it is a lower bound of
  the real channel occupancy.
- **Noise floor**: SX1302/SX1303 gateways report the packet (signal) RSSI, so
  noise = RSSI − SNR; SX1301/SX1308 gateways report channel RSSI, so
  noise = RSSI − 10·log10(1 + 10^(SNR/10)). The integration detects which one a
  gateway uses from weak packets (SNR ≤ 0 dB) and shows it as the `rssi_model`
  attribute. The value appears once enough weak packets have been heard.
- **Downlinks**: `command/down` messages are matched with the gateway's
  `event/ack` to know which transmission option (RX1/RX2) actually went out.
- **Packet loss** of own nodes comes from FCnt gaps. The frame counter goes over
  the air as 16 bits, so after 65535 comes 0 (a node sending every 30 s wraps
  about every 23 days): that is the next uplink, not a counter reset. Jumps of
  more than 1000 count as a reset or re-join, not as loss.
- Counters survive restarts; windowed values restart empty.
- Entity states are written once per update interval, however busy the
  channel is, to keep the recorder small.

## Dashboard examples

Packets per channel and hour (replace the gateway ID with yours):

```yaml
type: statistics-graph
title: SenseCAP – packets per channel
chart_type: bar
period: hour
stat_types: [change]
days_to_show: 2
entities:
  - sensor.gateway_0016c001f1000001_rx_867_1_mhz
  - sensor.gateway_0016c001f1000001_rx_867_3_mhz
  - sensor.gateway_0016c001f1000001_rx_867_5_mhz
  - sensor.gateway_0016c001f1000001_rx_867_7_mhz
  - sensor.gateway_0016c001f1000001_rx_867_9_mhz
  - sensor.gateway_0016c001f1000001_rx_868_1_mhz
  - sensor.gateway_0016c001f1000001_rx_868_3_mhz
  - sensor.gateway_0016c001f1000001_rx_868_5_mhz
```

Channel airtime:

```yaml
type: history-graph
title: SenseCAP – channel airtime
hours_to_show: 24
entities:
  - sensor.gateway_0016c001f1000001_airtime_867_9_mhz
  - sensor.gateway_0016c001f1000001_airtime_868_1_mhz
  - sensor.gateway_0016c001f1000001_peak_channel_airtime
```

Who is on air:

```yaml
type: markdown
title: Top talkers (last hour)
content: >
  {% for t in state_attr('sensor.lorawan_network_eu868_top_talker', 'top_talkers') or [] %}
  - **{{ t.source }}**: {{ t.packets }} packets ({{ t.share }} %){% if t.sample %} `{{ t.sample }}`{% endif %}
  {% endfor %}
```

## Automation examples

```yaml
alias: SenseCAP gateway offline
triggers:
  - trigger: state
    entity_id: binary_sensor.gateway_0016c001f1000001_online
    to: "off"
    for: "00:05:00"
actions:
  - action: notify.notify
    data:
      message: The SenseCAP LoRaWAN gateway is offline.
```

```yaml
alias: LoRa channel congestion
triggers:
  - trigger: numeric_state
    entity_id: sensor.gateway_0016c001f1000001_peak_channel_airtime
    above: 20
    for: "00:10:00"
actions:
  - action: notify.notify
    data:
      message: >
        {{ state_attr('sensor.gateway_0016c001f1000001_peak_channel_airtime', 'channel_mhz') }} MHz
        is busy {{ states('sensor.gateway_0016c001f1000001_peak_channel_airtime') }} % of the time.
        Top talker: {{ states('sensor.lorawan_network_eu868_top_talker') }}
```

## Troubleshooting

- **No gateways appear**: check the topic prefix and that Home Assistant's MQTT
  user can read the gateway topics (*Settings → MQTT → Configure → Listen to a
  topic*, `eu868/gateway/#`).
- **Diagnostics** (*⋮ → Download diagnostics* on the integration) show what the
  gateways really send: message counts, payload format, decode errors, stats
  metadata and the per-frequency/per-modulation maps of the last stats report.
- Debug logging:

  ```yaml
  logger:
    logs:
      custom_components.chirpstack_gateway_monitor: debug
  ```

## Development

`custom_components/chirpstack_gateway_monitor/core` holds the Home Assistant
independent part (protobuf decoder, LoRaWAN parsing, aggregation).

```bash
# core: decoder checked against the official chirpstack-api encoder, plus a
# replay of logged captures (samples/*.db, not part of the repository)
pip install pytest chirpstack-api
pytest tests/core

# Home Assistant level (Python >= 3.14)
pip install pytest-homeassistant-custom-component
cd tests/ha && pytest

# regenerate tests/ha/payloads.json
python tests/make_ha_payloads.py
```

## Related

[lorawan-capture](https://github.com/Majklzbastlirny/lorawan-capture) records the same
gateway traffic into one SQLite file per day, with a web UI for longer term statistics,
channel occupancy, a device explorer and gateway uptime.

## License

MIT, see [LICENSE](LICENSE).
