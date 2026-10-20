"""LoRaWAN PHYPayload inspection, time-on-air and signal helpers."""

from __future__ import annotations

import math
import statistics
from dataclasses import dataclass

MTYPE_NAMES = {
    0: "JoinRequest",
    1: "JoinAccept",
    2: "UnconfirmedDataUp",
    3: "UnconfirmedDataDown",
    4: "ConfirmedDataUp",
    5: "ConfirmedDataDown",
    6: "RejoinRequest",
    7: "Proprietary",
}

# Bits of the NwkID for each DevAddr type prefix (LoRaWAN Backend Interfaces,
# as corrected by its errata; same table as ChirpStack's lorawan crate).
_NWKID_BITS = (6, 6, 9, 11, 12, 13, 15, 17)


@dataclass(slots=True)
class PhyInfo:
    """What can be learned from a PHYPayload without any session keys."""

    valid: bool
    reason: str | None = None  # why it is not LoRaWAN
    mhdr: int | None = None
    mtype: int | None = None
    uplink: bool | None = None
    dev_addr: str | None = None
    fcnt: int | None = None
    fport: int | None = None
    dev_eui: str | None = None
    join_eui: str | None = None

    @property
    def mtype_name(self) -> str | None:
        return None if self.mtype is None else MTYPE_NAMES[self.mtype]


def _eui(raw: bytes) -> str:
    return raw[::-1].hex()


def parse_phy(phy: bytes) -> PhyInfo:
    """Decode the unencrypted LoRaWAN header and check it is plausible.

    Anything that breaks the LoRaWAN R1 framing rules (RFU bits set, wrong
    Major, impossible lengths, proprietary MType) is flagged as not LoRaWAN.
    That is what identifies foreign non-LoRaWAN LoRa traffic, which the
    gateways happily demodulate and forward.
    """
    size = len(phy)
    if not size:
        return PhyInfo(valid=False, reason="empty")
    mhdr = phy[0]
    mtype = mhdr >> 5
    info = PhyInfo(valid=False, mhdr=mhdr, mtype=mtype)

    if mtype == 7:
        info.reason = "proprietary"
    elif (mhdr >> 2) & 0x07:
        info.reason = "rfu_bits"
    elif mhdr & 0x03:
        info.reason = "major"
    elif mtype == 0:  # JoinRequest: MHDR | JoinEUI | DevEUI | DevNonce | MIC
        info.uplink = True
        if size != 23:
            info.reason = "length"
        else:
            info.join_eui = _eui(phy[1:9])
            info.dev_eui = _eui(phy[9:17])
    elif mtype == 1:  # JoinAccept (encrypted)
        info.uplink = False
        if size not in (17, 33):
            info.reason = "length"
    elif mtype == 6:  # RejoinRequest
        info.uplink = True
        rejoin_type = phy[1] if size > 1 else None
        if rejoin_type in (0, 2) and size == 19:
            info.dev_eui = _eui(phy[5:13])
        elif rejoin_type == 1 and size == 24:
            info.join_eui = _eui(phy[2:10])
            info.dev_eui = _eui(phy[10:18])
        else:
            info.reason = "length"
    else:  # data frames: MHDR | DevAddr | FCtrl | FCnt | FOpts | [FPort | FRMPayload] | MIC
        info.uplink = mtype in (2, 4)
        if size < 12:
            info.reason = "length"
        else:
            fopts_len = phy[5] & 0x0F
            if 8 + fopts_len + 4 > size:
                info.reason = "fopts"
            else:
                info.dev_addr = "%08x" % int.from_bytes(phy[1:5], "little")
                info.fcnt = int.from_bytes(phy[6:8], "little")
                if size > 8 + fopts_len + 4:
                    info.fport = phy[8 + fopts_len]
                    if info.fport == 0 and fopts_len:
                        info.reason = "fopts"  # MAC commands in both FOpts and FRMPayload
    info.valid = info.reason is None
    return info


def is_lorawan_modulation(kind: str | None, spreading_factor: int | None) -> bool:
    """LoRaWAN only uses SF7-SF12 for LoRa (plus FSK / LR-FHSS)."""
    if kind == "LORA":
        return spreading_factor is not None and 7 <= spreading_factor <= 12
    return kind in ("FSK", "LR_FHSS")


def lora_airtime(
    payload_len: int,
    spreading_factor: int | None,
    bandwidth: int | None,
    code_rate: int | None = 1,
    preamble: int | None = 8,
    crc: bool = True,
    explicit_header: bool = True,
) -> float:
    """Time on air in seconds (Semtech SX127x/SX126x formulas)."""
    if not spreading_factor or not bandwidth:
        return 0.0
    sf = spreading_factor
    cr = code_rate or 1
    n_preamble = preamble or 8
    t_sym = (1 << sf) / bandwidth
    ldro = t_sym > 0.016  # low data rate optimisation: SF11/12 at 125 kHz
    if sf < 7:  # SF5 / SF6 (SX126x / SX1302 only)
        t_preamble = (n_preamble + 6.25) * t_sym
        bits = 8 * payload_len + 16 * crc - 4 * sf + (20 if explicit_header else 0)
        n_payload = 8 + math.ceil(max(bits, 0) / (4 * sf)) * (cr + 4)
    else:
        t_preamble = (n_preamble + 4.25) * t_sym
        bits = 8 * payload_len - 4 * sf + 28 + 16 * crc - (0 if explicit_header else 20)
        n_payload = 8 + max(math.ceil(bits / (4 * (sf - 2 * ldro))) * (cr + 4), 0)
    return t_preamble + n_payload * t_sym


def fsk_airtime(payload_len: int, datarate: int | None) -> float:
    """Time on air in seconds of a LoRaWAN FSK frame (preamble 5, sync 3, length, CRC)."""
    if not datarate:
        return 0.0
    return (5 + 3 + 1 + payload_len + 2) * 8 / datarate


def dev_addr_network(dev_addr: str) -> tuple[int, int]:
    """Return (NetID type, NwkID) encoded in a DevAddr."""
    addr = int(dev_addr, 16)
    addr_type = 0
    while addr_type < 7 and (addr >> (31 - addr_type)) & 1:
        addr_type += 1
    bits = _NWKID_BITS[addr_type]
    nwk_id = (addr >> (32 - (addr_type + 1) - bits)) & ((1 << bits) - 1)
    return addr_type, nwk_id


def network_label(dev_addr: str) -> str:
    """Describe the network a DevAddr belongs to."""
    addr_type, nwk_id = dev_addr_network(dev_addr)
    if addr_type == 0:
        if nwk_id in (0, 1):
            return f"NetID 00000{nwk_id} (private/experimental)"
        if nwk_id == 0x13:
            return "NetID 000013 (The Things Network)"
        return f"NetID {nwk_id:06X}"
    return f"NetID type {addr_type}, NwkID {nwk_id:X}"


# ---------------------------------------------------------------------------
# Noise floor
# ---------------------------------------------------------------------------
#
# Gateways report RSSI in one of two ways:
#   "signal":  RSSI of the packet itself (SX1302/SX1303 'rssis'), so
#              noise = RSSI - SNR
#   "channel": total power in the channel (SX1301/SX1308 'rssic'), so
#              noise = RSSI - 10*log10(1 + 10^(SNR/10))
# Only weak packets (SNR <= 0 dB) are used: at high SNR the reported value
# saturates. The right model is the one giving a consistent noise estimate
# across different SNRs.

WEAK_SNR_LIMIT = 0.0


def _noise(model: str, rssi: float, snr: float) -> float:
    if model == "signal":
        return rssi - snr
    return rssi - 10 * math.log10(1 + 10 ** (snr / 10))


def detect_rssi_model(samples: list[tuple[float, float]]) -> str | None:
    """Decide how a gateway reports RSSI, or None when there is not enough data."""
    if len(samples) < 8:
        return None
    snrs = [snr for _, snr in samples]
    if max(snrs) - min(snrs) < 6:
        return None
    spread = {
        model: statistics.pstdev(_noise(model, rssi, snr) for rssi, snr in samples)
        for model in ("signal", "channel")
    }
    return min(spread, key=spread.get)  # type: ignore[arg-type]


def noise_floor(samples: list[tuple[float, float]], model: str | None) -> float | None:
    """Median noise estimate (dBm) from weak-packet (rssi, snr) samples."""
    if model is None or len(samples) < 3:
        return None
    return statistics.median(_noise(model, rssi, snr) for rssi, snr in samples)
