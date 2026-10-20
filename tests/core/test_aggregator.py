"""Aggregator tests: synthetic message flows plus a replay of a logged capture.

The replay re-encodes rows logged by samples/lorawan_logger.py into the exact
protobuf messages the ChirpStack gateway bridge publishes, feeds them through
the aggregator with their original timing and compares the outcome with
independent SQL queries over the same capture.
"""

from __future__ import annotations

import json
import sqlite3
import time
from collections import Counter
from datetime import datetime, timezone

import pytest

from conftest import SAMPLES
from core.aggregator import Aggregator

gw = pytest.importorskip("chirpstack_api.gw")

OWN = "00a1b2c3"
SENSECAP = "0016c001f1000001"
HOME = "0016c001f1000002"
LAB = "b827ebfffe000003"


def _uplink(phy: bytes, freq: int, sf: int, rssi: int, snr: float, cr: int = 1, crc: int = 2) -> bytes:
    frame = gw.UplinkFrame(phy_payload=phy)
    frame.tx_info.frequency = freq
    frame.tx_info.modulation.lora.spreading_factor = sf
    frame.tx_info.modulation.lora.bandwidth = 125_000
    frame.tx_info.modulation.lora.code_rate = cr
    frame.rx_info.rssi = rssi
    frame.rx_info.snr = snr
    frame.rx_info.crc_status = crc
    return frame.SerializeToString()


def _stats(rx: int, rx_ok: int, tx: int = 0, tx_emitted: int = 0) -> bytes:
    return gw.GatewayStats(
        rx_packets_received=rx,
        rx_packets_received_ok=rx_ok,
        tx_packets_received=tx,
        tx_packets_emitted=tx_emitted,
    ).SerializeToString()


def _own_phy(fcnt: int) -> bytes:
    return bytes.fromhex("40" + "c3b2a100" + "80") + fcnt.to_bytes(2, "little") + bytes.fromhex("02aabbcc11223344")


# --------------------------------------------------------------- synthetic


def test_dedup_own_node_and_packet_loss() -> None:
    agg = Aggregator(own_dev_addrs=[OWN], now=0)
    t = 1000.0
    for fcnt in (10, 11, 12, 15, 16):  # 13 and 14 are lost
        agg.handle("up", SENSECAP, _uplink(_own_phy(fcnt), 868_100_000, 7, -95, 9.5), t)
        agg.handle("up", LAB, _uplink(_own_phy(fcnt), 868_100_000, 7, -80, 10.0), t + 0.2)
        t += 60
    snap = agg.snapshot(t)
    node = snap["nodes"][OWN]
    assert node["packets"] == 5
    assert node["fcnt"] == 16
    assert node["gateway_count"] == 2
    assert (node["rssi"], node["rssi_gateway"]) == (-80, LAB)
    assert (node["snr"], node["snr_gateway"]) == (10.0, LAB)
    assert node["packet_loss"] == pytest.approx(100 * 2 / 7, abs=0.1)
    assert node["spreading_factor"] == 7
    assert node["frequency_mhz"] == 868.1
    assert snap["network"]["unique"] == 5
    assert snap["network"]["devices_heard"] == 1
    assert snap["gateways"][SENSECAP]["lorawan"] == 5
    assert snap["gateways"][LAB]["lorawan"] == 5


def test_packet_loss_across_the_frame_counter_wrap() -> None:
    """FCnt goes over the air as 16 bits: 65535 -> 0 is the next uplink, not a counter reset."""
    agg = Aggregator(own_dev_addrs=[OWN], now=0)
    t = 1000.0
    for fcnt in (65533, 65534, 1, 2):  # 65535 and 0 are lost
        agg.handle("up", SENSECAP, _uplink(_own_phy(fcnt), 868_100_000, 7, -95, 9.5), t)
        t += 60
    node = agg.snapshot(t)["nodes"][OWN]
    assert node["fcnt"] == 2
    assert node["packet_loss"] == pytest.approx(100 * 2 / 6, abs=0.1)


def test_node_reception_per_gateway() -> None:
    agg = Aggregator(own_dev_addrs=[OWN], now=0)
    agg.handle("up", SENSECAP, _uplink(_own_phy(10), 868_100_000, 7, -95, 9.5), 100)
    agg.handle("up", LAB, _uplink(_own_phy(10), 868_100_000, 7, -80, 10.0), 100.2)
    # FCnt 11 was heard by nobody
    agg.handle("up", SENSECAP, _uplink(_own_phy(12), 868_300_000, 7, -97, 8.0), 220)

    node = agg.snapshot(230)["nodes"][OWN]
    assert node["packet_loss"] == pytest.approx(33.3, abs=0.1)
    heard = node["per_gateway"]
    assert (heard[SENSECAP]["heard"], heard[SENSECAP]["uplinks"]) == (2, 3)
    assert heard[SENSECAP]["reception"] == pytest.approx(66.7, abs=0.1)
    assert heard[LAB]["reception"] == pytest.approx(33.3, abs=0.1)
    assert (heard[SENSECAP]["rssi"], heard[SENSECAP]["snr"]) == (-97, 8.0)
    assert (heard[LAB]["rssi"], heard[LAB]["snr"]) == (-80, 10.0)  # its last reception, of FCnt 10

    # an hour later the lab gateway has not heard the node within the device window
    later = agg.snapshot(3750)["nodes"][OWN]["per_gateway"]
    assert later[LAB]["rssi"] is None
    assert later[SENSECAP]["rssi"] == -97

    agg.forget_gateway(LAB)
    assert LAB not in agg.snapshot(3760)["nodes"][OWN]["per_gateway"]


def test_downlink_ack_counts_emitted_item() -> None:
    agg = Aggregator(now=0)
    frame = gw.DownlinkFrame(downlink_id=77)
    for freq, sf in ((868_100_000, 7), (869_525_000, 9)):
        item = frame.items.add(phy_payload=b"\x60" + b"\x00" * 16)
        item.tx_info.frequency = freq
        item.tx_info.power = 14
        item.tx_info.modulation.lora.spreading_factor = sf
        item.tx_info.modulation.lora.bandwidth = 125_000
    agg.handle("down", SENSECAP, frame.SerializeToString(), 100.0)
    ack = gw.DownlinkTxAck(downlink_id=77)
    ack.items.add(status=gw.TxAckStatus.TOO_LATE)
    ack.items.add(status=gw.TxAckStatus.OK)
    agg.handle("ack", SENSECAP, ack.SerializeToString(), 100.5)

    failed = gw.DownlinkTxAck(downlink_id=78)
    failed.items.add(status=gw.TxAckStatus.COLLISION_PACKET)
    agg.handle("ack", SENSECAP, failed.SerializeToString(), 101.0)

    snap = agg.snapshot(200.0)["gateways"][SENSECAP]
    assert snap["tx_channels"] == {869_525_000: 1}  # RX1 was too late, RX2 went out
    assert snap["tx_status"] == {"TOO_LATE": 1, "OK": 1, "COLLISION_PACKET": 1}
    assert snap["downlinks_failed"] == 1
    assert snap["last_tx"]["frequency_mhz"] == 869.525
    assert snap["tx_duty_cycle"] > 0


def test_online_state() -> None:
    agg = Aggregator(now=0)
    agg.handle("conn", HOME, gw.ConnState(state=gw.ConnState.State.ONLINE).SerializeToString(), 1)
    agg.handle("stats", SENSECAP, _stats(5, 4), 1)
    snap = agg.snapshot(10)
    assert snap["gateways"][HOME]["online"] is True
    assert snap["gateways"][SENSECAP]["online"] is True
    assert snap["network"]["gateways_online"] == 2

    agg.handle("conn", HOME, gw.ConnState(state=gw.ConnState.State.OFFLINE).SerializeToString(), 20)
    snap = agg.snapshot(30)
    assert snap["gateways"][HOME]["online"] is False
    assert snap["network"]["gateways_online"] == 1

    snap = agg.snapshot(200)  # SenseCAP stats stopped more than 90 s ago
    assert snap["gateways"][SENSECAP]["online"] is False


def test_stats_interval_survives_a_reconnect_gap() -> None:
    agg = Aggregator(now=0)
    t = 0.0
    for _ in range(10):
        agg.handle("stats", SENSECAP, _stats(1, 1), t)
        t += 30
    t += 600  # ten minutes without stats, e.g. an MQTT reconnect
    agg.handle("stats", SENSECAP, _stats(1, 1), t)
    assert agg.gateways[SENSECAP].stats_interval < 70
    # still reported offline about three intervals after stats stop
    assert agg.snapshot(t + 200)["gateways"][SENSECAP]["online"] is False


def test_bad_crc_is_counted_but_not_classified() -> None:
    agg = Aggregator(now=0)
    agg.handle("up", SENSECAP, _uplink(b"\xde\xad", 868_300_000, 7, -120, -10.0, crc=1), 5)
    snap = agg.snapshot(10)
    gw_snap = snap["gateways"][SENSECAP]
    assert gw_snap["channels"][868_300_000]["packets"] == 1
    assert gw_snap["crc_status"] == {"BAD_CRC": 1}
    assert gw_snap["lorawan"] == gw_snap["non_lorawan"] == 0
    assert snap["network"]["unique"] == 0


def test_undecodable_payload_raises() -> None:
    agg = Aggregator(now=0)
    with pytest.raises(ValueError):
        agg.handle("up", SENSECAP, b"\x0f\xff", 1)


def test_storage_roundtrip() -> None:
    agg = Aggregator(own_dev_addrs=[OWN], now=0)
    agg.handle("up", SENSECAP, _uplink(_own_phy(1), 868_100_000, 7, -95, 9.5), 1)
    agg.handle("up", SENSECAP, _uplink(b"\xc8" + b"\x00" * 50, 867_900_000, 5, -92, 8.0, cr=2), 2)
    agg.handle("stats", SENSECAP, _stats(10, 8, 1, 1), 3)
    stored = json.loads(json.dumps(agg.to_storage()))

    restored = Aggregator(own_dev_addrs=[OWN], now=100)
    restored.load_storage(stored)
    before, after = agg.snapshot(4), restored.snapshot(101)
    for key in ("rx_received", "rx_ok", "rx_bad_crc", "tx_emitted", "lorawan", "non_lorawan", "modulations"):
        assert after["gateways"][SENSECAP][key] == before["gateways"][SENSECAP][key]
    assert after["gateways"][SENSECAP]["channels"].keys() == before["gateways"][SENSECAP]["channels"].keys()
    assert after["network"]["unique"] == before["network"]["unique"] == 2
    assert after["nodes"][OWN]["packets"] == 1
    assert after["nodes"][OWN]["per_gateway"][SENSECAP]["rssi"] == -95


# ------------------------------------------------------------------ replay


# The captures hold the real IDs of the gateways and of the own node; samples/replay_ids.json, next
# to them and not part of the repository either, maps those to the example IDs used here.
REPLAY_IDS = SAMPLES / "replay_ids.json"


def _replay_ids() -> dict[str, str]:
    if not REPLAY_IDS.exists():
        pytest.skip("samples/replay_ids.json not available")
    return json.loads(REPLAY_IDS.read_text(encoding="utf-8"))


def _real(example: str) -> str:
    """The capture's own ID for one of the example IDs."""
    return next(real for real, ex in _replay_ids().items() if ex == example)


def _ex(real: str) -> str:
    """The example ID for one of the capture's own IDs."""
    return _replay_ids().get(real, real)


def _ts(value: str) -> float:
    return datetime.fromisoformat(value).timestamp()


@pytest.fixture(scope="module")
def capture():
    path = SAMPLES / "frames_floodHuntContinue.db"
    if not path.exists():
        pytest.skip("capture not available")
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    yield db
    db.close()


@pytest.fixture(scope="module")
def replayed(capture):
    return replay(capture)


def replay(capture: sqlite3.Connection) -> tuple[Aggregator, dict, int, float]:
    """Feed a logger capture through the aggregator; returns (aggregator, snapshot, messages, seconds)."""
    frames = capture.execute(
        "SELECT ts_utc, gateway_id, phy_hex, freq_hz, sf, bw_hz, code_rate, rssi, snr, crc FROM frames"
    ).fetchall()
    stats = capture.execute("SELECT * FROM stats").fetchall()
    events = [(_ts(r["ts_utc"]), 0, r) for r in frames] + [(_ts(r["ts_utc"]), 1, r) for r in stats]
    events.sort(key=lambda e: e[0])

    ids = _replay_ids()
    payloads = []
    for ts, is_stats, row in events:
        gateway_id = ids.get(row["gateway_id"], row["gateway_id"])
        if is_stats:
            payload = _stats(row["rx_received"], row["rx_crc_ok"], row["tx_received"], row["tx_emitted"])
            payloads.append((ts, "stats", gateway_id, payload))
        else:
            phy = bytes.fromhex(row["phy_hex"])
            if len(phy) >= 5 and (addr := "%08x" % int.from_bytes(phy[1:5], "little")) in ids:
                phy = phy[:1] + bytes.fromhex(ids[addr])[::-1] + phy[5:]  # the own node, under its example DevAddr
            frame = gw.UplinkFrame(phy_payload=phy)
            frame.tx_info.frequency = row["freq_hz"]
            lora = frame.tx_info.modulation.lora
            lora.spreading_factor = row["sf"]
            lora.bandwidth = row["bw_hz"]
            lora.code_rate = int(row["code_rate"])
            frame.rx_info.gateway_id = gateway_id
            frame.rx_info.rssi = row["rssi"]
            frame.rx_info.snr = row["snr"]
            frame.rx_info.crc_status = gw.CRCStatus.Value(row["crc"])
            payloads.append((ts, "up", gateway_id, frame.SerializeToString()))

    agg = Aggregator(own_dev_addrs=[OWN], window=600, device_window=3600, now=payloads[0][0])
    started = time.perf_counter()
    next_snapshot = payloads[0][0] + 60
    for ts, kind, gateway_id, payload in payloads:
        if ts >= next_snapshot:  # like the Home Assistant coordinator, once a minute
            agg.snapshot(ts)
            next_snapshot = ts + 60
        agg.handle(kind, gateway_id, payload, ts)
    elapsed = time.perf_counter() - started
    end = payloads[-1][0]
    return agg, agg.snapshot(end), len(payloads), elapsed


def test_replay_is_fast(replayed) -> None:
    _, _, count, elapsed = replayed
    assert count / elapsed > 2000, f"{count / elapsed:.0f} msg/s"


def test_replay_counters_match_capture(capture, replayed) -> None:
    _, snap, _, _ = replayed
    for row in capture.execute(
        "SELECT gateway_id, sum(rx_received) rx, sum(rx_crc_ok) ok, sum(tx_emitted) tx FROM stats GROUP BY 1"
    ):
        gw_snap = snap["gateways"][_ex(row["gateway_id"])]
        assert gw_snap["rx_received"] == row["rx"]
        assert gw_snap["rx_ok"] == row["ok"]
        assert gw_snap["rx_bad_crc"] == row["rx"] - row["ok"]
        assert gw_snap["tx_emitted"] == row["tx"]

    per_channel = Counter()
    for row in capture.execute("SELECT gateway_id, freq_hz, count(*) n FROM frames GROUP BY 1, 2"):
        per_channel[(_ex(row["gateway_id"]), row["freq_hz"])] = row["n"]
    for (gateway_id, freq), count in per_channel.items():
        assert snap["gateways"][gateway_id]["channels"][freq]["packets"] == count

    for row in capture.execute("SELECT gateway_id, sf, count(*) n FROM frames GROUP BY 1, 2"):
        assert snap["gateways"][_ex(row["gateway_id"])]["modulations"][f"sf{row['sf']}"] == row["n"]


def test_replay_flags_the_flood(capture, replayed) -> None:
    _, snap, _, _ = replayed
    # Independent rule: SF5/SF6, or an MHDR with RFU bits / Major set or the
    # proprietary MType. Besides the SF5 flood this capture holds SF7 traffic
    # on 868.1 MHz between two devices (payloads d6a1b2.. <-> 0dc3d4..) that
    # is not LoRaWAN either.
    expected = Counter()
    for row in capture.execute("SELECT gateway_id, sf, substr(phy_hex, 1, 2) mhdr FROM frames"):
        mhdr = int(row["mhdr"], 16)
        if row["sf"] < 7 or mhdr & 0x1F or mhdr >> 5 == 7:
            expected[_ex(row["gateway_id"])] += 1
    for gateway_id, count in expected.items():
        assert snap["gateways"][gateway_id]["non_lorawan"] == count

    network = snap["network"]
    assert network["top_talker"] == "Non-LoRaWAN 0xC8 SF5"
    assert network["top_talker_share"] > 80
    details = network["top_talker_details"]
    assert list(details["channels"]) == ["867.9 MHz"]
    # The SenseCAP hears the interferer ~20 dB stronger than the home gateway,
    # and the SX1308 lab gateway cannot demodulate SF5 at all.
    assert list(details["gateways"])[0] == SENSECAP
    assert LAB not in details["gateways"]
    assert details["gateways"][SENSECAP]["average_rssi"] > details["gateways"][HOME]["average_rssi"] + 10


def test_replay_airtime(replayed) -> None:
    _, snap, _, _ = replayed
    sensecap = snap["gateways"][SENSECAP]
    assert sensecap["peak_channel"] == 867_900_000
    assert 10 < sensecap["channels"][867_900_000]["airtime"] < 80
    for freq, channel in sensecap["channels"].items():
        if freq != 867_900_000:
            assert channel["airtime"] < 5, freq


def test_replay_own_node(capture, replayed) -> None:
    _, snap, _, _ = replayed
    # Only uplinks: the capture also holds two downlinks to the node, overheard by a gateway.
    (distinct,) = capture.execute(
        "SELECT count(DISTINCT frame_key) FROM frames WHERE dev_addr = ? AND mtype LIKE '%DataUp'", (_real(OWN),)
    ).fetchone()
    node = snap["nodes"][OWN]
    assert node["packets"] == distinct
    assert node["spreading_factor"] == 7
    assert node["packet_loss"] is not None and node["packet_loss"] < 20
    assert node["gateway_count"] >= 1

    # Per gateway reception over the last hour of the capture, checked against SQL.
    per_gateway = node["per_gateway"]
    assert set(per_gateway) == {SENSECAP, HOME, LAB}
    (end,) = capture.execute("SELECT max(ts_utc) FROM frames").fetchone()
    since = datetime.fromtimestamp(_ts(end) - 3600, tz=timezone.utc).isoformat()
    for row in capture.execute(
        "SELECT gateway_id, count(DISTINCT frame_key) n FROM frames"
        " WHERE dev_addr = ? AND mtype LIKE '%DataUp' AND ts_utc >= ? GROUP BY 1",
        (_real(OWN), since),
    ):
        # the aggregator's window starts at the first reception of an uplink, the SQL one per row
        assert abs(per_gateway[_ex(row["gateway_id"])]["heard"] - row["n"]) <= 1
    assert all(0 <= g["reception"] <= 100 for g in per_gateway.values())
    assert per_gateway[SENSECAP]["reception"] > 80


def test_replay_noise_floor(replayed) -> None:
    _, snap, _, _ = replayed
    for gateway_id in (SENSECAP, HOME):  # SX1302 based: report the packet (signal) RSSI
        gw_snap = snap["gateways"][gateway_id]
        assert gw_snap["rssi_model"] == "signal"
        assert -125 < gw_snap["noise_floor"] < -110
    assert snap["network"]["devices_heard"] > 5


def test_replay_signal_averages_exclude_interference(replayed) -> None:
    _, snap, _, _ = replayed
    # The SF5 flood arrives at ~-92 dBm; LoRaWAN devices are far weaker on average.
    assert snap["gateways"][SENSECAP]["average_rssi"] < -100


def test_noise_floor_lab_gateway_dual_capture() -> None:
    """The SX1308 (RAK2246) reports channel RSSI; needs the longer, quieter capture."""
    path = SAMPLES / "frames_dual_8dBi.db"
    if not path.exists():
        pytest.skip("capture not available")
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    try:
        _, snap, _, _ = replay(db)
    finally:
        db.close()
    lab = snap["gateways"][LAB]
    assert lab["rssi_model"] == "channel"
    assert -115 < lab["noise_floor"] < -100
    assert snap["gateways"][SENSECAP]["rssi_model"] == "signal"
