"""Aggregation of ChirpStack gateway traffic into counters and windowed metrics.

All methods take ``now`` (epoch seconds) explicitly so captures can be
replayed with their original timing.
"""

from __future__ import annotations

from collections import Counter, defaultdict, deque
from datetime import datetime, timezone
from typing import Any, Iterable

from . import lorawan
from .messages import (
    FSK,
    LORA,
    ConnState,
    Downlink,
    DownlinkItem,
    GatewayStats,
    Modulation,
    TxAck,
    Uplink,
    is_json,
    modulation_label,
    parse_conn_state,
    parse_downlink,
    parse_stats,
    parse_tx_ack,
    parse_uplink,
)

KIND_UP = "up"
KIND_STATS = "stats"
KIND_ACK = "ack"
KIND_CONN = "conn"
KIND_DOWN = "down"

BUCKET_SECONDS = 10.0
DEDUP_SECONDS = 5.0
PENDING_DOWNLINK_SECONDS = 60.0
DEFAULT_STATS_INTERVAL = 30.0
MIN_OFFLINE_TIMEOUT = 90.0
MAX_WEAK_SAMPLES = 1000
TOP_TALKERS = 10
STORAGE_FORMAT = 1

GATEWAY_TOTALS = (
    "rx_received",
    "rx_ok",
    "rx_bad_crc",
    "tx_received",
    "tx_emitted",
    "lorawan",
    "non_lorawan",
    "downlinks_failed",
)
NETWORK_TOTALS = ("unique", "lorawan", "non_lorawan", "join_requests")


def _utc(ts: float | None) -> datetime | None:
    return None if ts is None else datetime.fromtimestamp(ts, tz=timezone.utc)


def mhz(frequency: int) -> str:
    """867900000 -> '867.9'."""
    return f"{frequency / 1e6:.6f}".rstrip("0").rstrip(".")


def mhz_label(frequency: int) -> str:
    return f"{mhz(frequency)} MHz"


def by_mhz(counts: dict[int, int] | None) -> dict[str, int]:
    """Frequency-keyed counts with readable (JSON safe) keys."""
    return {mhz_label(f): n for f, n in sorted((counts or {}).items())}


def _airtime(modulation: Modulation, payload_len: int, crc: bool = True) -> float:
    if modulation.kind == LORA:
        return lorawan.lora_airtime(
            payload_len,
            modulation.spreading_factor,
            modulation.bandwidth,
            modulation.code_rate,
            modulation.preamble,
            crc=crc,
        )
    if modulation.kind == FSK:
        return lorawan.fsk_airtime(payload_len, modulation.datarate)
    return 0.0


def _int_keys(data: dict[str, Any] | None) -> Counter[int]:
    out: Counter[int] = Counter()
    for key, value in (data or {}).items():
        try:
            out[int(key)] = int(value)
        except (TypeError, ValueError):
            continue
    return out


class _GatewayBucket:
    __slots__ = (
        "start",
        "packets",
        "by_freq",
        "airtime",
        "rssi_sum",
        "snr_sum",
        "signal_n",
        "stats_rx",
        "stats_rx_ok",
        "stats_seen",
    )

    def __init__(self, start: float) -> None:
        self.start = start
        self.packets = 0
        self.by_freq: Counter[int] = Counter()
        self.airtime: defaultdict[int, float] = defaultdict(float)
        self.rssi_sum = 0.0
        self.snr_sum = 0.0
        self.signal_n = 0
        self.stats_rx = 0
        self.stats_rx_ok = 0
        self.stats_seen = False


class _NetworkBucket:
    __slots__ = ("start", "packets", "talkers", "talker_rx", "talker_freq", "devices")

    def __init__(self, start: float) -> None:
        self.start = start
        self.packets = 0
        self.talkers: Counter[str] = Counter()
        # talker -> gateway -> [receptions, rssi sum, snr sum]
        self.talker_rx: dict[str, dict[str, list[float]]] = {}
        self.talker_freq: dict[str, Counter[int]] = {}
        self.devices: dict[str, str] = {}  # device id -> network label


class GatewayState:
    """Everything known about one gateway."""

    def __init__(self, gateway_id: str) -> None:
        self.gateway_id = gateway_id
        # persisted
        self.totals: Counter[str] = Counter()
        self.rx_by_freq: Counter[int] = Counter()
        self.rx_by_mod: Counter[str] = Counter()
        self.tx_by_freq: Counter[int] = Counter()
        self.tx_status: Counter[str] = Counter()
        self.crc_status: Counter[str] = Counter()
        self.last_seen: float | None = None
        self.rssi_model: str | None = None
        self.has_duty_cycle = False
        self.metadata: dict[str, str] = {}
        self.config_version: str | None = None
        self.stats_interval = DEFAULT_STATS_INTERVAL
        # this session only
        self.conn_online: bool | None = None
        self.last_stats: float | None = None
        self.last_uplink: float | None = None
        self.location: dict[str, float] | None = None
        self.duty_cycle_bands: list[dict[str, Any]] | None = None
        self.duty_cycle_window: float | None = None
        self.reported: dict[str, Any] | None = None
        self.last_tx: dict[str, Any] | None = None
        self.buckets: deque[_GatewayBucket] = deque()
        self.tx_events: deque[tuple[float, float]] = deque()
        # (time, rssi, snr) of weak packets for the noise floor; kept for the
        # long window because some gateways hear few weak packets.
        self.weak: deque[tuple[float, float, float]] = deque(maxlen=MAX_WEAK_SAMPLES)
        # Weak samples of any age, only collected until the RSSI model is known.
        self.model_samples: deque[tuple[float, float]] = deque(maxlen=200)

    def bucket(self, now: float) -> _GatewayBucket:
        start = now - now % BUCKET_SECONDS
        if not self.buckets or start > self.buckets[-1].start:
            self.buckets.append(_GatewayBucket(start))
        return self.buckets[-1]

    def prune(self, now: float, window: float, long_window: float) -> None:
        while self.buckets and self.buckets[0].start + BUCKET_SECONDS <= now - window:
            self.buckets.popleft()
        while self.tx_events and self.tx_events[0][0] <= now - long_window:
            self.tx_events.popleft()
        while self.weak and self.weak[0][0] <= now - long_window:
            self.weak.popleft()

    def online(self, now: float, started: float) -> bool | None:
        """Online state; stats (every ~30 s) are the heartbeat once seen."""
        if self.conn_online is False:
            return False
        timeout = max(3 * self.stats_interval, MIN_OFFLINE_TIMEOUT)
        if self.last_stats is not None:
            return now - self.last_stats <= timeout
        if self.conn_online:
            return True
        if self.last_uplink is not None and now - self.last_uplink <= timeout:
            return True
        if now - started <= timeout:
            return None
        return False

    def to_storage(self) -> dict[str, Any]:
        return {
            "totals": dict(self.totals),
            "rx_by_freq": {str(k): v for k, v in self.rx_by_freq.items()},
            "rx_by_mod": dict(self.rx_by_mod),
            "tx_by_freq": {str(k): v for k, v in self.tx_by_freq.items()},
            "tx_status": dict(self.tx_status),
            "crc_status": dict(self.crc_status),
            "last_seen": self.last_seen,
            "rssi_model": self.rssi_model,
            "has_duty_cycle": self.has_duty_cycle,
            "metadata": self.metadata,
            "config_version": self.config_version,
            "stats_interval": self.stats_interval,
        }

    def load(self, data: dict[str, Any]) -> None:
        self.totals.update({k: int(v) for k, v in (data.get("totals") or {}).items()})
        self.rx_by_freq.update(_int_keys(data.get("rx_by_freq")))
        self.rx_by_mod.update({k: int(v) for k, v in (data.get("rx_by_mod") or {}).items()})
        self.tx_by_freq.update(_int_keys(data.get("tx_by_freq")))
        self.tx_status.update({k: int(v) for k, v in (data.get("tx_status") or {}).items()})
        self.crc_status.update({k: int(v) for k, v in (data.get("crc_status") or {}).items()})
        self.last_seen = data.get("last_seen")
        self.rssi_model = data.get("rssi_model")
        self.has_duty_cycle = bool(data.get("has_duty_cycle"))
        self.metadata = dict(data.get("metadata") or {})
        self.config_version = data.get("config_version")
        self.stats_interval = float(data.get("stats_interval") or DEFAULT_STATS_INTERVAL)


class NodeState:
    """One of the user's own devices, identified by DevAddr."""

    def __init__(self, dev_addr: str) -> None:
        self.dev_addr = dev_addr
        self.packets = 0
        self.last_seen: float | None = None
        self.fcnt: int | None = None
        self.frequency: int | None = None
        self.modulation_key: str | None = None
        self.spreading_factor: int | None = None
        self.airtime: float | None = None
        self.fport: int | None = None
        self.confirmed: bool | None = None
        # receptions of the last uplink: gateway -> (rssi, snr)
        self.receptions: dict[str, tuple[int | None, float | None]] = {}
        # last reception by each gateway: gateway -> (rssi, snr, time)
        self.per_gateway: dict[str, tuple[int | None, float | None, float]] = {}
        # uplinks in the window: (time, FCnt, gateways that heard it)
        self.history: deque[tuple[float, int, set[str]]] = deque()

    def link_stats(self, now: float, window: float) -> tuple[int, int, Counter[str]]:
        """(uplinks received, uplinks lost, uplinks heard per gateway) over the window.

        Lost uplinks are FCnt values missing between received ones: sent by
        the node but heard by no gateway.
        """
        while self.history and self.history[0][0] < now - window:
            self.history.popleft()
        received = lost = 0
        heard: Counter[str] = Counter()
        previous: int | None = None
        for _, fcnt, gateways in self.history:
            if previous is not None:
                gap = (fcnt - previous) & 0xFFFF
                if gap == 0:  # repetition of the same uplink
                    continue
                if gap <= 1000:  # larger jumps are counter resets / re-joins
                    lost += gap - 1
            received += 1
            heard.update(gateways)
            previous = fcnt
        return received, lost, heard

    def to_storage(self) -> dict[str, Any]:
        return {
            "packets": self.packets,
            "last_seen": self.last_seen,
            "fcnt": self.fcnt,
            "frequency": self.frequency,
            "modulation_key": self.modulation_key,
            "spreading_factor": self.spreading_factor,
            "per_gateway": {gw: list(values) for gw, values in self.per_gateway.items()},
        }

    def load(self, data: dict[str, Any]) -> None:
        self.packets = int(data.get("packets") or 0)
        self.last_seen = data.get("last_seen")
        self.fcnt = data.get("fcnt")
        self.frequency = data.get("frequency")
        self.modulation_key = data.get("modulation_key")
        self.spreading_factor = data.get("spreading_factor")
        for gw, values in (data.get("per_gateway") or {}).items():
            try:
                rssi, snr, heard_at = values
                self.per_gateway[gw] = (rssi, snr, float(heard_at))
            except (TypeError, ValueError):
                continue


class Aggregator:
    """Turns the stream of gateway MQTT messages into health metrics."""

    def __init__(
        self,
        own_dev_addrs: Iterable[str] = (),
        window: float = 600.0,
        device_window: float = 3600.0,
        now: float = 0.0,
    ) -> None:
        self.window = window
        self.device_window = device_window
        self.started = now
        self.gateways: dict[str, GatewayState] = {}
        self.nodes: dict[str, NodeState] = {addr.lower(): NodeState(addr.lower()) for addr in own_dev_addrs}
        self.network_totals: Counter[str] = Counter()
        self.message_counts: Counter[str] = Counter()
        self.formats: Counter[str] = Counter()
        self.errors: Counter[str] = Counter()
        self.last_errors: deque[str] = deque(maxlen=10)
        self._net_buckets: deque[_NetworkBucket] = deque()
        self._seen: dict[bytes, float] = {}
        self._talker_labels: dict[str, str] = {}
        self._talker_samples: dict[str, str] = {}  # non-LoRaWAN key -> last payload prefix (hex)
        self._pending: dict[int, tuple[float, str, list[DownlinkItem]]] = {}

    # ------------------------------------------------------------------ input

    def handle(self, kind: str, gateway_id: str, payload: bytes, now: float) -> None:
        """Decode and process one MQTT message. Raises on undecodable payloads."""
        self.message_counts[kind] += 1
        self.formats["json" if is_json(payload) else "protobuf"] += 1
        if kind == KIND_UP:
            self.on_uplink(parse_uplink(payload, gateway_id), now)
        elif kind == KIND_STATS:
            self.on_stats(parse_stats(payload, gateway_id), now)
        elif kind == KIND_ACK:
            self.on_tx_ack(parse_tx_ack(payload, gateway_id), now)
        elif kind == KIND_DOWN:
            self.on_downlink(parse_downlink(payload, gateway_id), now)
        elif kind == KIND_CONN:
            self.on_conn_state(parse_conn_state(payload, gateway_id), now)

    def record_error(self, kind: str, gateway_id: str, err: BaseException) -> None:
        self.errors[kind] += 1
        self.last_errors.append(f"{kind} from {gateway_id}: {err!r}")

    def gateway(self, gateway_id: str) -> GatewayState:
        gw = self.gateways.get(gateway_id)
        if gw is None:
            gw = self.gateways[gateway_id] = GatewayState(gateway_id)
        return gw

    def forget_gateway(self, gateway_id: str) -> None:
        self.gateways.pop(gateway_id, None)
        for node in self.nodes.values():
            node.per_gateway.pop(gateway_id, None)

    def _net_bucket(self, now: float) -> _NetworkBucket:
        start = now - now % BUCKET_SECONDS
        if not self._net_buckets or start > self._net_buckets[-1].start:
            self._net_buckets.append(_NetworkBucket(start))
            self._housekeeping(now)
        return self._net_buckets[-1]

    def _housekeeping(self, now: float) -> None:
        while self._net_buckets and self._net_buckets[0].start + BUCKET_SECONDS <= now - self.device_window:
            self._net_buckets.popleft()
        self._seen = {k: t for k, t in self._seen.items() if now - t <= DEDUP_SECONDS}
        for downlink_id in [d for d, p in self._pending.items() if now - p[0] > PENDING_DOWNLINK_SECONDS]:
            del self._pending[downlink_id]
        for gw in self.gateways.values():
            gw.prune(now, self.window, self.device_window)

    def on_uplink(self, up: Uplink, now: float) -> None:
        gw = self.gateway(up.gateway_id)
        gw.last_seen = gw.last_uplink = now
        mod = up.modulation
        phy = up.phy_payload
        airtime = _airtime(mod, len(phy))

        bucket = gw.bucket(now)
        bucket.packets += 1
        if up.frequency > 0:
            gw.rx_by_freq[up.frequency] += 1
            bucket.by_freq[up.frequency] += 1
            bucket.airtime[up.frequency] += airtime
        if mod.kind is not None:
            gw.rx_by_mod[mod.key] += 1
        gw.crc_status[up.crc_status] += 1
        has_signal = mod.kind == LORA and up.rssi is not None and up.snr is not None
        if has_signal and up.snr <= lorawan.WEAK_SNR_LIMIT:  # type: ignore[operator]
            gw.weak.append((now, float(up.rssi), up.snr))  # type: ignore[arg-type]
            if gw.rssi_model is None:
                gw.model_samples.append((float(up.rssi), up.snr))  # type: ignore[arg-type]
                if len(gw.model_samples) % 8 == 0:
                    gw.rssi_model = lorawan.detect_rssi_model(list(gw.model_samples))
                    if gw.rssi_model is not None:
                        gw.model_samples.clear()

        if up.crc_status == "BAD_CRC":
            return  # payload is garbage; it is still counted per channel above

        info = lorawan.parse_phy(phy)
        is_lorawan = info.valid and lorawan.is_lorawan_modulation(mod.kind, mod.spreading_factor)
        gw.totals["lorawan" if is_lorawan else "non_lorawan"] += 1
        if is_lorawan and has_signal:  # interferers have their own talker statistics
            bucket.rssi_sum += up.rssi  # type: ignore[operator]
            bucket.snr_sum += up.snr  # type: ignore[operator]
            bucket.signal_n += 1

        first_seen = self._seen.get(phy)
        new_transmission = first_seen is None or now - first_seen > DEDUP_SECONDS
        key, label, device = self._talker(info, is_lorawan, mod)
        net = self._net_bucket(now)
        if new_transmission:
            self._seen[phy] = now
            self.network_totals["unique"] += 1
            self.network_totals["lorawan" if is_lorawan else "non_lorawan"] += 1
            if is_lorawan and info.mtype == 0:
                self.network_totals["join_requests"] += 1
            net.packets += 1
            net.talkers[key] += 1
            self._talker_labels[key] = label
            if not is_lorawan:
                self._talker_samples[key] = phy[:12].hex()
            if up.frequency > 0:
                net.talker_freq.setdefault(key, Counter())[up.frequency] += 1
            if device is not None:
                net.devices[device] = (
                    lorawan.network_label(info.dev_addr) if info.dev_addr else "Joining (DevEUI)"
                )
        if up.rssi is not None:
            rx = net.talker_rx.setdefault(key, {}).setdefault(up.gateway_id, [0, 0.0, 0.0])
            rx[0] += 1
            rx[1] += up.rssi
            rx[2] += up.snr or 0.0

        if is_lorawan and info.uplink and info.dev_addr in self.nodes:
            node = self.nodes[info.dev_addr]
            if new_transmission:
                node.packets += 1
                node.last_seen = now
                node.fcnt = info.fcnt
                node.frequency = up.frequency
                node.modulation_key = mod.key
                node.spreading_factor = mod.spreading_factor
                node.airtime = airtime
                node.fport = info.fport
                node.confirmed = info.mtype == 4
                node.receptions = {}
                if info.fcnt is not None:
                    node.history.append((now, info.fcnt, set()))
            node.receptions[up.gateway_id] = (up.rssi, up.snr)
            node.per_gateway[up.gateway_id] = (up.rssi, up.snr, now)
            if node.history:
                node.history[-1][2].add(up.gateway_id)

    @staticmethod
    def _talker(info: lorawan.PhyInfo, is_lorawan: bool, mod: Modulation) -> tuple[str, str, str | None]:
        """(key, label, device id) identifying who transmitted a packet."""
        if is_lorawan:
            if info.dev_addr:
                suffix = "" if info.uplink else " (downlink)"
                return f"dev:{info.dev_addr}", f"DevAddr {info.dev_addr}{suffix}", (
                    info.dev_addr if info.uplink else None
                )
            if info.dev_eui:
                kind = "join" if info.mtype == 0 else "rejoin"
                return f"eui:{info.dev_eui}", f"DevEUI {info.dev_eui} ({kind})", f"eui:{info.dev_eui}"
            return f"mtype:{info.mtype}", info.mtype_name or "LoRaWAN", None
        mhdr = "empty" if info.mhdr is None else f"0x{info.mhdr:02X}"
        mod_label = modulation_label(mod.key)
        return f"raw:{mhdr}:{mod.key}", f"Non-LoRaWAN {mhdr} {mod_label}", None

    def on_stats(self, stats: GatewayStats, now: float) -> None:
        gw = self.gateway(stats.gateway_id)
        if gw.last_stats is not None:
            # Clamped so a single gap (e.g. an MQTT reconnect) does not stretch
            # the offline timeout; a real interval change is still followed.
            delta = min(now - gw.last_stats, 5 * gw.stats_interval)
            if delta >= 1.0:
                gw.stats_interval = 0.7 * gw.stats_interval + 0.3 * delta
        gw.last_stats = gw.last_seen = now

        gw.totals["rx_received"] += stats.rx_received
        gw.totals["rx_ok"] += stats.rx_received_ok
        gw.totals["rx_bad_crc"] += max(stats.rx_received - stats.rx_received_ok, 0)
        gw.totals["tx_received"] += stats.tx_received
        gw.totals["tx_emitted"] += stats.tx_emitted

        bucket = gw.bucket(now)
        bucket.stats_rx += stats.rx_received
        bucket.stats_rx_ok += stats.rx_received_ok
        bucket.stats_seen = True

        gw.metadata = stats.metadata
        if stats.location is not None:
            gw.location = stats.location
        if stats.config_version:
            gw.config_version = stats.config_version
        if stats.duty_cycle_bands is not None:
            gw.has_duty_cycle = True
            gw.duty_cycle_window = stats.duty_cycle_window
            gw.duty_cycle_bands = [
                {
                    "name": band.name,
                    "frequency_min": band.frequency_min,
                    "frequency_max": band.frequency_max,
                    "load_max_s": round(band.load_max, 3),
                    "load_tracked_s": round(band.load_tracked, 3),
                    "usage_percent": None if band.usage_percent is None else round(band.usage_percent, 2),
                }
                for band in stats.duty_cycle_bands
            ]
        gw.reported = {
            "rx_per_frequency": by_mhz(stats.rx_per_frequency),
            "tx_per_frequency": by_mhz(stats.tx_per_frequency),
            "rx_per_modulation": {modulation_label(k): n for k, n in stats.rx_per_modulation.items()},
            "tx_per_modulation": {modulation_label(k): n for k, n in stats.tx_per_modulation.items()},
            "tx_per_status": stats.tx_per_status,
            "gateway_time": _utc(stats.time),
        }

    def on_downlink(self, downlink: Downlink, now: float) -> None:
        if downlink.downlink_id:
            self._pending[downlink.downlink_id] = (now, downlink.gateway_id, downlink.items)

    def on_tx_ack(self, ack: TxAck, now: float) -> None:
        gw = self.gateway(ack.gateway_id)
        gw.last_seen = now
        pending = self._pending.pop(ack.downlink_id, None) if ack.downlink_id else None
        for status in ack.statuses:
            if status != "IGNORED":
                gw.tx_status[status] += 1
        ok = next((i for i, status in enumerate(ack.statuses) if status == "OK"), None)
        if ok is None:
            gw.totals["downlinks_failed"] += 1
            return
        if pending is None or ok >= len(pending[2]):
            return
        item = pending[2][ok]
        airtime = _airtime(item.modulation, item.phy_length, crc=False)  # downlinks carry no CRC
        if item.frequency > 0:
            gw.tx_by_freq[item.frequency] += 1
        gw.tx_events.append((now, airtime))
        gw.last_tx = {
            "frequency_mhz": item.frequency / 1e6,
            "power_dbm": item.power,
            "modulation": modulation_label(item.modulation.key),
            "airtime_ms": round(airtime * 1000, 1),
            "time": _utc(now),
        }

    def on_conn_state(self, state: ConnState, now: float) -> None:
        gw = self.gateway(state.gateway_id)
        gw.conn_online = state.online

    # ----------------------------------------------------------------- output

    def snapshot(self, now: float) -> dict[str, Any]:
        """Current values for all entities."""
        self._housekeeping(now)
        return {
            "gateways": {gid: self._gateway_snapshot(gw, now) for gid, gw in self.gateways.items()},
            "network": self._network_snapshot(now),
            "nodes": {addr: self._node_snapshot(node, now) for addr, node in self.nodes.items()},
        }

    def _covered(self, now: float, window: float) -> float:
        return max(min(window, now - self.started), 1.0)

    def _gateway_snapshot(self, gw: GatewayState, now: float) -> dict[str, Any]:
        covered = self._covered(now, self.window)
        stats_rx = stats_rx_ok = packets = signal_n = 0
        stats_seen = False
        rssi_sum = snr_sum = 0.0
        airtime: defaultdict[int, float] = defaultdict(float)
        for bucket in gw.buckets:
            stats_rx += bucket.stats_rx
            stats_rx_ok += bucket.stats_rx_ok
            stats_seen |= bucket.stats_seen
            packets += bucket.packets
            rssi_sum += bucket.rssi_sum
            snr_sum += bucket.snr_sum
            signal_n += bucket.signal_n
            for freq, seconds in bucket.airtime.items():
                airtime[freq] += seconds

        channels = {
            freq: {
                "packets": gw.rx_by_freq.get(freq, 0),
                "airtime": round(100.0 * airtime.get(freq, 0.0) / covered, 2),
            }
            for freq in sorted(set(gw.rx_by_freq) | set(airtime))
        }
        peak_freq = max(channels, key=lambda f: channels[f]["airtime"], default=None)

        weak = [(rssi, snr) for _, rssi, snr in gw.weak]
        noise = lorawan.noise_floor(weak, gw.rssi_model)

        tx_airtime = sum(seconds for _, seconds in gw.tx_events)
        duty_cycle = None
        if gw.duty_cycle_bands:
            usages = [b["usage_percent"] for b in gw.duty_cycle_bands if b["usage_percent"] is not None]
            duty_cycle = max(usages, default=None)

        return {
            "online": gw.online(now, self.started),
            "conn_state": {True: "ONLINE", False: "OFFLINE"}.get(gw.conn_online),  # type: ignore[arg-type]
            "last_seen": _utc(gw.last_seen),
            **{name: gw.totals.get(name, 0) for name in GATEWAY_TOTALS},
            "rx_rate": round(60.0 * stats_rx / covered, 2) if stats_seen else None,
            "crc_ok_ratio": round(100.0 * stats_rx_ok / stats_rx, 1) if stats_rx else None,
            "packet_rate": round(60.0 * packets / covered, 2),
            "average_rssi": round(rssi_sum / signal_n, 1) if signal_n else None,
            "average_snr": round(snr_sum / signal_n, 1) if signal_n else None,
            "noise_floor": None if noise is None else round(noise, 1),
            "rssi_model": gw.rssi_model,
            "noise_samples": len(weak),
            "channels": channels,
            "peak_airtime": channels[peak_freq]["airtime"] if peak_freq is not None else None,
            "peak_channel": peak_freq,
            "modulations": dict(gw.rx_by_mod),
            "tx_channels": dict(gw.tx_by_freq),
            "tx_duty_cycle": round(100.0 * tx_airtime / self._covered(now, self.device_window), 3),
            "tx_status": dict(gw.tx_status),
            "last_tx": gw.last_tx,
            "has_duty_cycle": gw.has_duty_cycle,
            "duty_cycle": duty_cycle,
            "duty_cycle_bands": gw.duty_cycle_bands,
            "duty_cycle_window_s": gw.duty_cycle_window,
            "crc_status": dict(gw.crc_status),
            "metadata": gw.metadata,
            "config_version": gw.config_version,
            "location": gw.location,
            "stats_interval": round(gw.stats_interval, 1),
            "reported": gw.reported,
        }

    def _network_snapshot(self, now: float) -> dict[str, Any]:
        short_start = now - self.window
        talkers: Counter[str] = Counter()
        devices: dict[str, str] = {}
        packets_short = 0
        for bucket in self._net_buckets:
            talkers.update(bucket.talkers)
            devices.update(bucket.devices)
            if bucket.start >= short_start:
                packets_short += bucket.packets
        self._talker_labels = {k: v for k, v in self._talker_labels.items() if k in talkers}
        self._talker_samples = {k: v for k, v in self._talker_samples.items() if k in talkers}
        total = sum(talkers.values())

        top_list = []
        for key, count in talkers.most_common(TOP_TALKERS):
            entry: dict[str, Any] = {
                "source": self._talker_labels.get(key, key),
                "packets": count,
                "share": round(100.0 * count / total, 1),
            }
            if key in self._talker_samples:
                entry["sample"] = self._talker_samples[key]
            top_list.append(entry)
        top_key, top_count = (talkers.most_common(1) or [(None, 0)])[0]
        top_details: dict[str, Any] | None = None
        if top_key is not None:
            per_gateway: dict[str, list[float]] = {}
            freqs: Counter[int] = Counter()
            for bucket in self._net_buckets:
                for gid, (n, rssi, snr) in bucket.talker_rx.get(top_key, {}).items():
                    acc = per_gateway.setdefault(gid, [0, 0.0, 0.0])
                    acc[0] += n
                    acc[1] += rssi
                    acc[2] += snr
                freqs.update(bucket.talker_freq.get(top_key, {}))
            top_details = {
                "sample": self._talker_samples.get(top_key),
                "packets": top_count,
                "packets_per_minute": round(60.0 * top_count / self._covered(now, self.device_window), 2),
                "channels": {mhz_label(f): n for f, n in freqs.most_common()},
                "gateways": {
                    gid: {
                        "receptions": int(n),
                        "average_rssi": round(rssi / n, 1),
                        "average_snr": round(snr / n, 1),
                    }
                    for gid, (n, rssi, snr) in sorted(per_gateway.items(), key=lambda kv: -kv[1][1] / kv[1][0])
                    if n
                },
            }

        online = {gid: gw.online(now, self.started) for gid, gw in self.gateways.items()}
        return {
            "gateways_online": sum(1 for state in online.values() if state),
            "gateways": online,
            "unique": self.network_totals.get("unique", 0),
            "lorawan": self.network_totals.get("lorawan", 0),
            "non_lorawan": self.network_totals.get("non_lorawan", 0),
            "join_requests": self.network_totals.get("join_requests", 0),
            "packet_rate": round(60.0 * packets_short / self._covered(now, self.window), 2),
            "devices_heard": len(devices),
            "networks": dict(Counter(devices.values()).most_common()),
            "top_talker": self._talker_labels.get(top_key) if top_key else None,
            "top_talker_share": top_list[0]["share"] if top_list else None,
            "top_talker_details": top_details,
            "top_talkers": top_list,
        }

    def _node_snapshot(self, node: NodeState, now: float) -> dict[str, Any]:
        rssi = {g: r for g, (r, _) in node.receptions.items() if r is not None}
        snr = {g: s for g, (_, s) in node.receptions.items() if s is not None}
        rssi_gw = max(rssi, key=rssi.__getitem__, default=None)
        snr_gw = max(snr, key=snr.__getitem__, default=None)

        received, lost, heard = node.link_stats(now, self.device_window)
        sent = received + lost  # uplinks the node transmitted in the window, as far as FCnt tells
        per_gateway = {}
        for gw, (gw_rssi, gw_snr, heard_at) in node.per_gateway.items():
            recent = now - heard_at <= self.device_window
            per_gateway[gw] = {
                "rssi": gw_rssi if recent else None,
                "snr": gw_snr if recent else None,
                "last_heard": _utc(heard_at),
                "reception": round(100.0 * heard[gw] / sent, 1) if sent else None,
                "heard": heard[gw],
                "uplinks": sent,
            }
        return {
            "last_seen": _utc(node.last_seen),
            "packets": node.packets,
            "fcnt": node.fcnt,
            "packet_loss": round(100.0 * lost / sent, 1) if received >= 2 else None,
            "per_gateway": per_gateway,
            "rssi": rssi.get(rssi_gw) if rssi_gw else None,
            "rssi_gateway": rssi_gw,
            "snr": snr.get(snr_gw) if snr_gw else None,
            "snr_gateway": snr_gw,
            "gateway_count": len(node.receptions) if node.last_seen else None,
            "receptions": {g: {"rssi": r, "snr": s} for g, (r, s) in node.receptions.items()},
            "spreading_factor": node.spreading_factor,
            "modulation": modulation_label(node.modulation_key) if node.modulation_key else None,
            "frequency_mhz": node.frequency / 1e6 if node.frequency else None,
            "airtime_ms": None if node.airtime is None else round(node.airtime * 1000, 1),
            "fport": node.fport,
            "confirmed": node.confirmed,
        }

    # ------------------------------------------------------------ persistence

    def to_storage(self) -> dict[str, Any]:
        return {
            "format": STORAGE_FORMAT,
            "gateways": {gid: gw.to_storage() for gid, gw in self.gateways.items()},
            "network": {"totals": dict(self.network_totals)},
            "nodes": {addr: node.to_storage() for addr, node in self.nodes.items()},
        }

    def load_storage(self, data: dict[str, Any]) -> None:
        if not isinstance(data, dict) or data.get("format") != STORAGE_FORMAT:
            return
        for gid, gdata in (data.get("gateways") or {}).items():
            self.gateway(gid).load(gdata)
        self.network_totals.update({k: int(v) for k, v in ((data.get("network") or {}).get("totals") or {}).items()})
        for addr, ndata in (data.get("nodes") or {}).items():
            if addr in self.nodes:
                self.nodes[addr].load(ndata)

    def diagnostics(self, now: float) -> dict[str, Any]:
        return {
            "uptime_s": round(now - self.started),
            "window_s": self.window,
            "device_window_s": self.device_window,
            "own_dev_addrs": sorted(self.nodes),
            "messages": dict(self.message_counts),
            "payload_formats": dict(self.formats),
            "decode_errors": dict(self.errors),
            "last_errors": list(self.last_errors),
            "pending_downlinks": len(self._pending),
            "gateways": {
                gid: {
                    "online": gw.online(now, self.started),
                    "conn_state_online": gw.conn_online,
                    "seconds_since_stats": None if gw.last_stats is None else round(now - gw.last_stats),
                    "stats_interval_s": round(gw.stats_interval, 1),
                    "rssi_model": gw.rssi_model,
                    "config_version": gw.config_version,
                    "metadata": gw.metadata,
                    "location": gw.location,
                    "duty_cycle_bands": gw.duty_cycle_bands,
                    "last_reported_stats": gw.reported,
                    **gw.to_storage(),
                }
                for gid, gw in self.gateways.items()
            },
        }
