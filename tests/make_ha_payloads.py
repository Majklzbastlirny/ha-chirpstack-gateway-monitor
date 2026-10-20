"""Generate tests/ha/payloads.json with the official chirpstack-api encoder.

The Home Assistant tests use these fixtures so their environment does not
need chirpstack-api (and grpcio). Run: python tests/make_ha_payloads.py
"""

import json
from pathlib import Path

from chirpstack_api import gw
from google.protobuf import json_format

SENSECAP = "0016c001f1000001"
HOME = "0016c001f1000002"
LAB = "b827ebfffe000003"


def uplink(phy: bytes, freq: int, sf: int, rssi: int, snr: float, cr=gw.CodeRate.CR_4_5) -> gw.UplinkFrame:
    frame = gw.UplinkFrame(phy_payload=phy)
    frame.tx_info.frequency = freq
    lora = frame.tx_info.modulation.lora
    lora.spreading_factor = sf
    lora.bandwidth = 125_000
    lora.code_rate = cr
    frame.rx_info.rssi = rssi
    frame.rx_info.snr = snr
    frame.rx_info.crc_status = gw.CRCStatus.CRC_OK
    return frame


def own(fcnt: int) -> bytes:
    return bytes.fromhex("40" + "c3b2a100" + "80") + fcnt.to_bytes(2, "little") + bytes.fromhex("01aabbccdd11223344")


def stats(rx: int, ok: int, tx: int = 0, emitted: int = 0, duty: bool = False) -> gw.GatewayStats:
    msg = gw.GatewayStats(
        rx_packets_received=rx, rx_packets_received_ok=ok, tx_packets_received=tx, tx_packets_emitted=emitted
    )
    msg.metadata["concentratord_version"] = "4.4.0"
    msg.rx_packets_per_frequency[867_900_000] = ok
    if duty:
        msg.duty_cycle_stats.window.seconds = 3600
        band = msg.duty_cycle_stats.bands.add(name="g1", frequency_min=868_000_000, frequency_max=868_600_000)
        band.load_max.seconds = 36
        band.load_tracked.seconds = 9
    return msg


def main() -> None:
    out: dict[str, str] = {}

    def put(name: str, msg, as_json: bool = False) -> None:
        out[name] = (json_format.MessageToJson(msg).encode() if as_json else msg.SerializeToString()).hex()

    put("conn_online", gw.ConnState(gateway_id=SENSECAP, state=gw.ConnState.State.ONLINE))
    put("conn_offline", gw.ConnState(gateway_id=HOME, state=gw.ConnState.State.OFFLINE))
    put("stats_sensecap", stats(134, 120, tx=1, emitted=1, duty=True))
    put("stats_home", stats(90, 70))
    put("stats_lab_json", stats(5, 4), as_json=True)

    put("own_100_sensecap", uplink(own(100), 868_100_000, 7, -99, 12.5))
    put("own_100_home", uplink(own(100), 868_100_000, 7, -107, 10.0))
    put("own_100_lab", uplink(own(100), 868_100_000, 7, -83, 9.25))
    put("own_103_sensecap_json", uplink(own(103), 867_700_000, 7, -97, 11.0), as_json=True)  # 101, 102 lost

    for i in range(5):  # the SF5 flood: MHDR 0xC8, CR 4/6, 867.9 MHz, distinct payloads
        phy = bytes([0xC8]) + bytes((i * 7 + k) % 256 for k in range(212))
        put(f"flood_{i}_sensecap", uplink(phy, 867_900_000, 5, -92, 8.0, cr=gw.CodeRate.CR_4_6))
    put("flood_0_home", uplink(bytes([0xC8]) + bytes(k % 256 for k in range(212)), 867_900_000, 5, -116, 2.5, cr=gw.CodeRate.CR_4_6))

    put("p2p_sensecap", uplink(bytes.fromhex("d6a1b20dc3d402fe01"), 868_100_000, 7, -118, 0.75))
    join = bytes.fromhex("00" + "0807060504030201" + "1817161514131211" + "abcd" + "01020304")
    put("join_sensecap", uplink(join, 868_300_000, 12, -125, -12.0))

    down = gw.DownlinkFrame(downlink_id=42, gateway_id=SENSECAP)
    for freq, sf, power in ((868_100_000, 7, 14), (869_525_000, 9, 27)):
        item = down.items.add(phy_payload=b"\x60" + b"\x00" * 16)
        item.tx_info.frequency = freq
        item.tx_info.power = power
        item.tx_info.modulation.lora.spreading_factor = sf
        item.tx_info.modulation.lora.bandwidth = 125_000
        item.tx_info.modulation.lora.code_rate = gw.CodeRate.CR_4_5
    put("down_42", down)
    ack = gw.DownlinkTxAck(downlink_id=42, gateway_id=SENSECAP)
    ack.items.add(status=gw.TxAckStatus.TOO_LATE)
    ack.items.add(status=gw.TxAckStatus.OK)
    put("ack_42", ack)

    path = Path(__file__).parent / "ha" / "payloads.json"
    path.write_text(json.dumps(out, indent=1) + "\n")
    print(f"wrote {len(out)} payloads to {path}")


if __name__ == "__main__":
    main()
