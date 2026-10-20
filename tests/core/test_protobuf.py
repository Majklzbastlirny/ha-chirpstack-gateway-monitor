"""Decoder tests against payloads produced by the official chirpstack-api package."""

import random

import pytest

from core import protobuf as pb
from core.messages import is_json, parse_conn_state, parse_downlink, parse_stats, parse_tx_ack, parse_uplink

gw = pytest.importorskip("chirpstack_api.gw")
json_format = pytest.importorskip("google.protobuf.json_format")

FLOOD_PHY = bytes.fromhex("c8" + "a5" * 212)


def _uplink() -> "gw.UplinkFrame":
    frame = gw.UplinkFrame(phy_payload=FLOOD_PHY)
    frame.tx_info.frequency = 867_900_000
    lora = frame.tx_info.modulation.lora
    lora.bandwidth = 125_000
    lora.spreading_factor = 5
    lora.code_rate = gw.CodeRate.CR_4_6
    lora.preamble = 8
    rx = frame.rx_info
    rx.gateway_id = "0016c001f1000001"
    rx.uplink_id = 4_000_000_000
    rx.gw_time.seconds = 1_790_000_000
    rx.gw_time.nanos = 500_000_000
    rx.rssi = -118
    rx.snr = -7.25
    rx.channel = 3
    rx.rf_chain = 1
    rx.context = b"\x01\x02\x03\x04"
    rx.metadata["region_config_id"] = "eu868"
    rx.crc_status = gw.CRCStatus.CRC_OK
    return frame


def _stats() -> "gw.GatewayStats":
    stats = gw.GatewayStats(
        gateway_id="0016c001f1000001",
        config_version="1.2.3",
        rx_packets_received=134,
        rx_packets_received_ok=120,
        tx_packets_received=2,
        tx_packets_emitted=1,
    )
    stats.time.seconds = 1_790_000_030
    stats.location.latitude = 50.1
    stats.location.longitude = 14.4
    stats.location.altitude = 350.0
    stats.metadata["concentratord_version"] = "4.4.0"
    stats.tx_packets_per_frequency[868_100_000] = 1
    stats.rx_packets_per_frequency[868_100_000] = 10
    stats.rx_packets_per_frequency[867_900_000] = 110
    item = stats.rx_packets_per_modulation.add(count=100)
    item.modulation.lora.spreading_factor = 5
    item.modulation.lora.bandwidth = 125_000
    item = stats.rx_packets_per_modulation.add(count=20)
    item.modulation.lora.spreading_factor = 7
    item.modulation.lora.bandwidth = 125_000
    item = stats.tx_packets_per_modulation.add(count=1)
    item.modulation.lora.spreading_factor = 9
    item.modulation.lora.bandwidth = 125_000
    stats.tx_packets_per_status["OK"] = 1
    stats.tx_packets_per_status["TOO_LATE"] = 1
    stats.duty_cycle_stats.window.seconds = 3600
    band = stats.duty_cycle_stats.bands.add(name="g1", frequency_min=868_000_000, frequency_max=868_600_000)
    band.load_max.seconds = 36
    band.load_tracked.seconds = 9
    band.load_tracked.nanos = 500_000_000
    return stats


def _both_encodings(msg):
    yield msg.SerializeToString()
    yield json_format.MessageToJson(msg).encode()


@pytest.mark.parametrize("payload", list(_both_encodings(_uplink())), ids=["protobuf", "json"])
def test_uplink(payload: bytes) -> None:
    up = parse_uplink(payload, "0016c001f1000001")
    assert up.phy_payload == FLOOD_PHY
    assert up.frequency == 867_900_000
    assert up.modulation.kind == "LORA"
    assert up.modulation.spreading_factor == 5
    assert up.modulation.bandwidth == 125_000
    assert up.modulation.code_rate == 2
    assert up.modulation.preamble == 8
    assert up.modulation.key == "sf5"
    assert up.rssi == -118
    assert up.snr == -7.25
    assert up.crc_status == "CRC_OK"
    assert up.channel == 3
    assert up.rf_chain == 1
    assert up.gw_time == pytest.approx(1_790_000_000.5)


def test_protobuf_starting_like_json_is_not_json() -> None:
    frame = _uplink()
    frame.phy_payload = b"\xc8" + b"\xbd" * 122  # 123 bytes: payload starts with b"\n{"
    payload = frame.SerializeToString()
    assert payload[:2] == b"\n{"
    assert not is_json(payload)
    assert parse_uplink(payload, "gw").phy_payload == frame.phy_payload

    as_json = b"\n" + json_format.MessageToJson(frame).encode()
    assert is_json(as_json)
    assert parse_uplink(as_json, "gw").phy_payload == frame.phy_payload


def test_uplink_zero_values_are_defaults() -> None:
    frame = gw.UplinkFrame(phy_payload=b"\x40" + b"\x00" * 11)
    frame.tx_info.frequency = 868_100_000
    frame.tx_info.modulation.lora.spreading_factor = 7
    frame.rx_info.crc_status = gw.CRCStatus.NO_CRC  # 0: omitted on the wire
    up = parse_uplink(frame.SerializeToString(), "gw")
    assert up.snr == 0.0
    assert up.rssi == 0
    assert up.crc_status == "NO_CRC"


def test_legacy_uplink_layout() -> None:
    frame = gw.UplinkFrame(phy_payload=b"\x40" + b"\x00" * 11)
    frame.tx_info_legacy.frequency = 868_300_000
    frame.tx_info_legacy.lora_modulation_info.spreading_factor = 12
    frame.tx_info_legacy.lora_modulation_info.bandwidth = 125
    frame.tx_info_legacy.lora_modulation_info.code_rate_legacy = "4/5"
    frame.rx_info_legacy.rssi = -120
    frame.rx_info_legacy.lora_snr = -12.5
    frame.rx_info_legacy.crc_status = gw.CRCStatus.CRC_OK
    up = parse_uplink(frame.SerializeToString(), "gw")
    assert up.frequency == 868_300_000
    assert up.modulation.spreading_factor == 12
    assert up.modulation.code_rate == 1
    assert up.rssi == -120
    assert up.snr == -12.5
    assert up.crc_status == "CRC_OK"


@pytest.mark.parametrize("payload", list(_both_encodings(_stats())), ids=["protobuf", "json"])
def test_stats(payload: bytes) -> None:
    st = parse_stats(payload, "0016c001f1000001")
    assert (st.rx_received, st.rx_received_ok, st.tx_received, st.tx_emitted) == (134, 120, 2, 1)
    assert st.config_version == "1.2.3"
    assert st.metadata == {"concentratord_version": "4.4.0"}
    assert st.location == {"latitude": 50.1, "longitude": 14.4, "altitude": 350.0}
    assert st.time == pytest.approx(1_790_000_030)
    assert st.rx_per_frequency == {868_100_000: 10, 867_900_000: 110}
    assert st.tx_per_frequency == {868_100_000: 1}
    assert st.rx_per_modulation == {"sf5": 100, "sf7": 20}
    assert st.tx_per_modulation == {"sf9": 1}
    assert st.tx_per_status == {"OK": 1, "TOO_LATE": 1}
    assert st.duty_cycle_window == 3600
    (band,) = st.duty_cycle_bands
    assert band.name == "g1"
    assert (band.frequency_min, band.frequency_max) == (868_000_000, 868_600_000)
    assert band.load_max == 36
    assert band.load_tracked == pytest.approx(9.5)
    assert band.usage_percent == pytest.approx(9.5 / 36 * 100)


def test_empty_stats() -> None:
    st = parse_stats(gw.GatewayStats().SerializeToString(), "gw")
    assert (st.rx_received, st.rx_received_ok, st.tx_received, st.tx_emitted) == (0, 0, 0, 0)
    assert st.duty_cycle_bands is None
    assert st.location is None


def _downlink() -> "gw.DownlinkFrame":
    frame = gw.DownlinkFrame(downlink_id=123456, gateway_id="0016c001f1000001")
    rx1 = frame.items.add(phy_payload=b"\x60" + b"\x11" * 16)
    rx1.tx_info.frequency = 868_100_000
    rx1.tx_info.power = 14
    rx1.tx_info.modulation.lora.spreading_factor = 7
    rx1.tx_info.modulation.lora.bandwidth = 125_000
    rx1.tx_info.modulation.lora.code_rate = gw.CodeRate.CR_4_5
    rx2 = frame.items.add(phy_payload=b"\x60" + b"\x11" * 16)
    rx2.tx_info.frequency = 869_525_000
    rx2.tx_info.power = 27
    rx2.tx_info.modulation.lora.spreading_factor = 9
    rx2.tx_info.modulation.lora.bandwidth = 125_000
    return frame


@pytest.mark.parametrize("payload", list(_both_encodings(_downlink())), ids=["protobuf", "json"])
def test_downlink(payload: bytes) -> None:
    dl = parse_downlink(payload, "0016c001f1000001")
    assert dl.downlink_id == 123456
    assert [(i.frequency, i.power, i.modulation.spreading_factor, i.phy_length) for i in dl.items] == [
        (868_100_000, 14, 7, 17),
        (869_525_000, 27, 9, 17),
    ]


def _ack() -> "gw.DownlinkTxAck":
    ack = gw.DownlinkTxAck(downlink_id=123456, gateway_id="0016c001f1000001")
    ack.items.add(status=gw.TxAckStatus.TOO_LATE)
    ack.items.add(status=gw.TxAckStatus.OK)
    return ack


@pytest.mark.parametrize("payload", list(_both_encodings(_ack())), ids=["protobuf", "json"])
def test_tx_ack(payload: bytes) -> None:
    ack = parse_tx_ack(payload, "0016c001f1000001")
    assert ack.downlink_id == 123456
    assert ack.statuses == ["TOO_LATE", "OK"]


def test_tx_ack_first_item_ignored_status_is_default() -> None:
    ack = gw.DownlinkTxAck(downlink_id=1)
    ack.items.add(status=gw.TxAckStatus.OK)
    ack.items.add(status=gw.TxAckStatus.IGNORED)  # 0: empty submessage on the wire
    assert parse_tx_ack(ack.SerializeToString(), "gw").statuses == ["OK", "IGNORED"]


@pytest.mark.parametrize("state", [gw.ConnState.State.ONLINE, gw.ConnState.State.OFFLINE])
def test_conn_state(state) -> None:
    msg = gw.ConnState(gateway_id="b827ebfffe000003", state=state)
    msg.gateway_id_legacy = bytes.fromhex("b827ebfffe000003")
    for payload in _both_encodings(msg):
        assert parse_conn_state(payload, "b827ebfffe000003").online is (state == gw.ConnState.State.ONLINE)


def test_unknown_fields_are_skipped() -> None:
    # field 99 (varint), field 98 (fixed64), field 97 (bytes), field 96 (fixed32) + a real field
    payload = bytes([0x98, 0x06, 0x01]) + bytes([0x91, 0x06]) + b"\x00" * 8
    payload += bytes([0x8A, 0x06, 0x02, 0xAA, 0xBB]) + bytes([0x85, 0x06]) + b"\x00" * 4
    payload += gw.GatewayStats(rx_packets_received=5).SerializeToString()
    assert pb.decode(payload, pb.GATEWAY_STATS) == {"rx_packets_received": 5}


def test_malformed_payloads_raise_decode_error_only() -> None:
    good = _uplink().SerializeToString()
    rng = random.Random(1234)
    for cut in range(1, len(good)):
        try:
            pb.decode(good[:cut], pb.UPLINK_FRAME)
        except pb.DecodeError:
            pass
    for _ in range(3000):
        blob = bytes(rng.randrange(256) for _ in range(rng.randrange(1, 64)))
        try:
            pb.decode(blob, pb.UPLINK_FRAME)
            pb.decode(blob, pb.GATEWAY_STATS)
        except pb.DecodeError:
            pass
