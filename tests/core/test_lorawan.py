"""LoRaWAN header parsing, classification, airtime and noise estimation."""

import math
import random

import pytest

from core import lorawan


def test_airtime_reference_values() -> None:
    # 23 byte PHYPayload (10 byte application payload), CR 4/5, 8 symbol preamble.
    assert lorawan.lora_airtime(23, 7, 125_000) == pytest.approx(0.061696)
    assert lorawan.lora_airtime(23, 12, 125_000) == pytest.approx(1.482752)  # LDRO on


def test_airtime_sf5_uses_sx126x_formula() -> None:
    t_sym = 32 / 125_000
    preamble = (8 + 6.25) * t_sym
    payload_symbols = 8 + math.ceil((8 * 213 + 16 - 20 + 20) / 20) * (2 + 4)
    expected = preamble + payload_symbols * t_sym
    assert lorawan.lora_airtime(213, 5, 125_000, code_rate=2) == pytest.approx(expected)
    assert 0.13 < expected < 0.14


def test_join_request() -> None:
    phy = bytes.fromhex("00" + "0807060504030201" + "1817161514131211" + "abcd" + "01020304")
    info = lorawan.parse_phy(phy)
    assert info.valid
    assert info.mtype_name == "JoinRequest"
    assert info.join_eui == "0102030405060708"
    assert info.dev_eui == "1112131415161718"


def test_data_uplink_from_capture() -> None:
    # FCtrl 0x80 (ADR), FCnt 0x0102, FPort 2
    phy = bytes.fromhex("40" + "c3b2a100" + "80" + "0201" + "02" + "aabbcc" + "11223344")
    info = lorawan.parse_phy(phy)
    assert info.valid
    assert info.uplink
    assert info.dev_addr == "00a1b2c3"
    assert info.fcnt == 0x0102
    assert info.fport == 2


@pytest.mark.parametrize(
    ("phy_hex", "reason"),
    [
        ("c8" + "a5" * 212, "rfu_bits"),  # the SF5 flood: MType 6 with RFU bits 010
        ("e0" + "00" * 20, "proprietary"),
        ("41" + "00" * 20, "major"),
        ("00" + "00" * 10, "length"),  # join request must be 23 bytes
        ("c0" + "00" * 30, "length"),  # rejoin request must be 19 or 24 bytes
        ("40" + "00000000" + "0f" + "0000" + "00" * 5, "fopts"),  # FOptsLen 15 but frame too short
        ("40" + "00000000" + "03" + "0000" + "aabbcc" + "00" + "11223344", "fopts"),  # FPort 0 with FOpts
    ],
)
def test_not_lorawan(phy_hex: str, reason: str) -> None:
    info = lorawan.parse_phy(bytes.fromhex(phy_hex))
    assert not info.valid
    assert info.reason == reason


def test_rejoin_type0() -> None:
    phy = bytes.fromhex("c0" + "00" + "130000" + "0807060504030201" + "0100" + "11223344")
    info = lorawan.parse_phy(phy)
    assert info.valid
    assert info.dev_eui == "0102030405060708"


@pytest.mark.parametrize(
    ("kind", "sf", "expected"),
    [("LORA", 5, False), ("LORA", 6, False), ("LORA", 7, True), ("LORA", 12, True), ("FSK", None, True)],
)
def test_lorawan_modulation(kind: str, sf: int | None, expected: bool) -> None:
    assert lorawan.is_lorawan_modulation(kind, sf) is expected


@pytest.mark.parametrize(
    ("dev_addr", "expected"),
    [("260b1234", (0, 0x13)), ("00a1b2c3", (0, 0)), ("48000123", (0, 0x24)), ("e0040000", (3, 0x2)), ("fc00ac12", (6, 0x2B))],
)
def test_dev_addr_network(dev_addr: str, expected: tuple[int, int]) -> None:
    assert lorawan.dev_addr_network(dev_addr) == expected


def test_network_labels() -> None:
    assert lorawan.network_label("260b1234") == "NetID 000013 (The Things Network)"
    assert lorawan.network_label("00a1b2c3") == "NetID 000000 (private/experimental)"


def _samples(model: str, noise: float, rng: random.Random) -> list[tuple[float, float]]:
    out = []
    for _ in range(40):
        snr = rng.uniform(-18, 0)
        if model == "signal":
            rssi = noise + snr
        else:
            rssi = noise + 10 * math.log10(1 + 10 ** (snr / 10))
        out.append((round(rssi + rng.uniform(-0.5, 0.5)), snr))  # gateways report integer RSSI
    return out


@pytest.mark.parametrize(("model", "noise"), [("signal", -118.5), ("channel", -108.5)])
def test_noise_floor_model_detection(model: str, noise: float) -> None:
    samples = _samples(model, noise, random.Random(7))
    assert lorawan.detect_rssi_model(samples) == model
    assert lorawan.noise_floor(samples, model) == pytest.approx(noise, abs=1.0)


def test_noise_floor_needs_snr_spread() -> None:
    samples = [(-120.0, -2.0)] * 20
    assert lorawan.detect_rssi_model(samples) is None
    assert lorawan.noise_floor(samples, None) is None
