"""Sensors for gateways, the network and own nodes."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorEntityDescription,
    SensorStateClass,
)
from homeassistant.const import (
    PERCENTAGE,
    SIGNAL_STRENGTH_DECIBELS,
    SIGNAL_STRENGTH_DECIBELS_MILLIWATT,
    EntityCategory,
    UnitOfFrequency,
    UnitOfTime,
)
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.typing import StateType

from .coordinator import ChirpStackConfigEntry, ChirpStackCoordinator
from .core.aggregator import mhz
from .core.messages import modulation_label
from .entity import (
    SCOPE_GATEWAY,
    SCOPE_NETWORK,
    SCOPE_NODE,
    AddConfigEntryEntitiesCallback,
    ChirpStackEntity,
    entity_key,
    gateway_label,
)

PACKETS = "packets"
PACKETS_PER_MINUTE = "packets/min"


@dataclass(frozen=True, kw_only=True)
class ChirpStackSensorDescription(SensorEntityDescription):
    """Describes a sensor reading a value from its scope of the snapshot."""

    value_fn: Callable[[dict[str, Any]], StateType | datetime]
    attrs_fn: Callable[[dict[str, Any]], dict[str, Any] | None] | None = None
    exists_fn: Callable[[dict[str, Any]], bool] | None = None


def _counter(key: str, field: str, **kwargs: Any) -> ChirpStackSensorDescription:
    return ChirpStackSensorDescription(
        key=key,
        translation_key=key,
        native_unit_of_measurement=PACKETS,
        state_class=SensorStateClass.TOTAL_INCREASING,
        value_fn=lambda d: d[field],
        **kwargs,
    )


def _percent(key: str, field: str, precision: int = 1, **kwargs: Any) -> ChirpStackSensorDescription:
    return ChirpStackSensorDescription(
        key=key,
        translation_key=key,
        native_unit_of_measurement=PERCENTAGE,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=precision,
        value_fn=lambda d: d[field],
        **kwargs,
    )


def _signal(key: str, field: str, unit: str, **kwargs: Any) -> ChirpStackSensorDescription:
    return ChirpStackSensorDescription(
        key=key,
        translation_key=key,
        device_class=SensorDeviceClass.SIGNAL_STRENGTH,
        native_unit_of_measurement=unit,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=1,
        value_fn=lambda d: d[field],
        **kwargs,
    )


def _last_seen() -> ChirpStackSensorDescription:
    return ChirpStackSensorDescription(
        key="last_seen",
        translation_key="last_seen",
        device_class=SensorDeviceClass.TIMESTAMP,
        value_fn=lambda d: d["last_seen"],
    )


GATEWAY_SENSORS: tuple[ChirpStackSensorDescription, ...] = (
    _last_seen(),
    _counter(
        "rx_packets",
        "rx_received",
        attrs_fn=lambda d: {"last_stats_report": d["reported"]} if d["reported"] else None,
    ),
    _counter("rx_packets_ok", "rx_ok", entity_registry_enabled_default=False),
    _counter("rx_packets_bad_crc", "rx_bad_crc"),
    _counter("tx_packets_requested", "tx_received", entity_registry_enabled_default=False),
    _counter("tx_packets_emitted", "tx_emitted", attrs_fn=lambda d: {"last_tx": d["last_tx"]}),
    _counter("lorawan_packets", "lorawan"),
    _counter("non_lorawan_packets", "non_lorawan"),
    _counter("downlinks_failed", "downlinks_failed", attrs_fn=lambda d: {"tx_status": d["tx_status"]}),
    ChirpStackSensorDescription(
        key="rx_rate",
        translation_key="rx_rate",
        native_unit_of_measurement=PACKETS_PER_MINUTE,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=1,
        value_fn=lambda d: d["rx_rate"],
    ),
    _percent("crc_ok_ratio", "crc_ok_ratio"),
    _signal("average_rssi", "average_rssi", SIGNAL_STRENGTH_DECIBELS_MILLIWATT),
    _signal("average_snr", "average_snr", SIGNAL_STRENGTH_DECIBELS),
    _signal(
        "noise_floor",
        "noise_floor",
        SIGNAL_STRENGTH_DECIBELS_MILLIWATT,
        attrs_fn=lambda d: {"rssi_model": d["rssi_model"], "samples": d["noise_samples"]},
    ),
    _percent(
        "peak_channel_airtime",
        "peak_airtime",
        precision=2,
        attrs_fn=lambda d: {"channel_mhz": float(mhz(d["peak_channel"])) if d["peak_channel"] else None},
    ),
    _percent("tx_duty_cycle", "tx_duty_cycle", precision=3),
    _percent(
        "duty_cycle_budget",
        "duty_cycle",
        attrs_fn=lambda d: {"window_s": d["duty_cycle_window_s"], "bands": d["duty_cycle_bands"]},
        exists_fn=lambda d: d["has_duty_cycle"],
    ),
)

NETWORK_SENSORS: tuple[ChirpStackSensorDescription, ...] = (
    ChirpStackSensorDescription(
        key="gateways_online",
        translation_key="gateways_online",
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d["gateways_online"],
        attrs_fn=lambda d: {"gateways": d["gateways"]},
    ),
    _counter("unique_packets", "unique"),
    _counter("lorawan_packets", "lorawan"),
    _counter("non_lorawan_packets", "non_lorawan"),
    _counter("join_requests", "join_requests"),
    ChirpStackSensorDescription(
        key="packet_rate",
        translation_key="packet_rate",
        native_unit_of_measurement=PACKETS_PER_MINUTE,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=1,
        value_fn=lambda d: d["packet_rate"],
    ),
    ChirpStackSensorDescription(
        key="devices_heard",
        translation_key="devices_heard",
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d["devices_heard"],
        attrs_fn=lambda d: {"networks": d["networks"]},
    ),
    ChirpStackSensorDescription(
        key="top_talker",
        translation_key="top_talker",
        value_fn=lambda d: d["top_talker"],
        attrs_fn=lambda d: {"details": d["top_talker_details"], "top_talkers": d["top_talkers"]},
    ),
    _percent("top_talker_share", "top_talker_share"),
)

NODE_SENSORS: tuple[ChirpStackSensorDescription, ...] = (
    _last_seen(),
    _signal(
        "rssi",
        "rssi",
        SIGNAL_STRENGTH_DECIBELS_MILLIWATT,
        attrs_fn=lambda d: {"gateway": d["rssi_gateway"]},
    ),
    _signal("snr", "snr", SIGNAL_STRENGTH_DECIBELS, attrs_fn=lambda d: {"gateway": d["snr_gateway"]}),
    ChirpStackSensorDescription(
        key="gateway_count",
        translation_key="gateway_count",
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d["gateway_count"],
        attrs_fn=lambda d: {"receptions": d["receptions"]},
    ),
    _counter("packets", "packets"),
    _percent("packet_loss", "packet_loss"),
    ChirpStackSensorDescription(
        key="frame_counter",
        translation_key="frame_counter",
        value_fn=lambda d: d["fcnt"],
        attrs_fn=lambda d: {"fport": d["fport"], "confirmed": d["confirmed"]},
    ),
    ChirpStackSensorDescription(
        key="spreading_factor",
        translation_key="spreading_factor",
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda d: d["spreading_factor"],
        attrs_fn=lambda d: {"modulation": d["modulation"]},
    ),
    ChirpStackSensorDescription(
        key="frequency",
        translation_key="frequency",
        device_class=SensorDeviceClass.FREQUENCY,
        native_unit_of_measurement=UnitOfFrequency.MEGAHERTZ,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=1,
        value_fn=lambda d: d["frequency_mhz"],
    ),
    ChirpStackSensorDescription(
        key="airtime",
        translation_key="airtime",
        device_class=SensorDeviceClass.DURATION,
        native_unit_of_measurement=UnitOfTime.MILLISECONDS,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=1,
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda d: d["airtime_ms"],
    ),
)


def _rx_channel(freq: int) -> ChirpStackSensorDescription:
    return ChirpStackSensorDescription(
        key=f"rx_channel_{freq}",
        translation_key="rx_channel",
        native_unit_of_measurement=PACKETS,
        state_class=SensorStateClass.TOTAL_INCREASING,
        value_fn=lambda d: d["channels"].get(freq, {}).get("packets"),
    )


def _channel_airtime(freq: int) -> ChirpStackSensorDescription:
    return ChirpStackSensorDescription(
        key=f"channel_airtime_{freq}",
        translation_key="channel_airtime",
        native_unit_of_measurement=PERCENTAGE,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=2,
        value_fn=lambda d: d["channels"].get(freq, {}).get("airtime"),
    )


def _rx_modulation(mod: str) -> ChirpStackSensorDescription:
    return ChirpStackSensorDescription(
        key=f"rx_modulation_{mod}",
        translation_key="rx_modulation",
        native_unit_of_measurement=PACKETS,
        state_class=SensorStateClass.TOTAL_INCREASING,
        value_fn=lambda d: d["modulations"].get(mod),
    )


def _node_via_gateway(gateway_id: str) -> tuple[ChirpStackSensorDescription, ...]:
    """How one gateway hears an own node."""

    def value(name: str) -> Callable[[dict[str, Any]], Any]:
        return lambda d: (d["per_gateway"].get(gateway_id) or {}).get(name)

    def attrs(*names: str) -> Callable[[dict[str, Any]], dict[str, Any]]:
        return lambda d: {"gateway_id": gateway_id, **{name: value(name)(d) for name in names}}

    return (
        ChirpStackSensorDescription(
            key=f"gateway_rssi_{gateway_id}",
            translation_key="gateway_rssi",
            device_class=SensorDeviceClass.SIGNAL_STRENGTH,
            native_unit_of_measurement=SIGNAL_STRENGTH_DECIBELS_MILLIWATT,
            state_class=SensorStateClass.MEASUREMENT,
            suggested_display_precision=0,
            value_fn=value("rssi"),
            attrs_fn=attrs("last_heard"),
        ),
        ChirpStackSensorDescription(
            key=f"gateway_snr_{gateway_id}",
            translation_key="gateway_snr",
            device_class=SensorDeviceClass.SIGNAL_STRENGTH,
            native_unit_of_measurement=SIGNAL_STRENGTH_DECIBELS,
            state_class=SensorStateClass.MEASUREMENT,
            suggested_display_precision=1,
            value_fn=value("snr"),
            attrs_fn=attrs("last_heard"),
        ),
        ChirpStackSensorDescription(
            key=f"gateway_reception_{gateway_id}",
            translation_key="gateway_reception",
            native_unit_of_measurement=PERCENTAGE,
            state_class=SensorStateClass.MEASUREMENT,
            suggested_display_precision=1,
            value_fn=value("reception"),
            attrs_fn=attrs("heard", "uplinks"),
        ),
    )


def _tx_channel(freq: int) -> ChirpStackSensorDescription:
    return ChirpStackSensorDescription(
        key=f"tx_channel_{freq}",
        translation_key="tx_channel",
        native_unit_of_measurement=PACKETS,
        state_class=SensorStateClass.TOTAL_INCREASING,
        value_fn=lambda d: d["tx_channels"].get(freq),
    )


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ChirpStackConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    coordinator = entry.runtime_data

    @callback
    def _add_new_entities() -> None:
        data = coordinator.data or {}
        new: list[ChirpStackSensor] = []

        def add(
            scope: str,
            scope_id: str | None,
            description: ChirpStackSensorDescription,
            scope_data: dict[str, Any],
            placeholders: dict[str, str] | None = None,
        ) -> None:
            key = entity_key("sensor", scope, scope_id, description.key)
            if key in coordinator.known_entities:
                return
            if description.exists_fn is not None and not description.exists_fn(scope_data):
                return
            coordinator.known_entities.add(key)
            new.append(ChirpStackSensor(coordinator, scope, scope_id, description, placeholders))

        if (network := data.get("network")) is not None:
            for description in NETWORK_SENSORS:
                add(SCOPE_NETWORK, None, description, network)
        for dev_addr, node in data.get("nodes", {}).items():
            for description in NODE_SENSORS:
                add(SCOPE_NODE, dev_addr, description, node)
            for gateway_id in node["per_gateway"]:
                descriptions = _node_via_gateway(gateway_id)
                if all(
                    entity_key("sensor", SCOPE_NODE, dev_addr, d.key) in coordinator.known_entities
                    for d in descriptions
                ):
                    continue
                label = {"gateway": gateway_label(hass, coordinator, gateway_id)}
                for description in descriptions:
                    add(SCOPE_NODE, dev_addr, description, node, label)
        for gateway_id, gateway in data.get("gateways", {}).items():
            for description in GATEWAY_SENSORS:
                add(SCOPE_GATEWAY, gateway_id, description, gateway)
            for freq in gateway["channels"]:
                add(SCOPE_GATEWAY, gateway_id, _rx_channel(freq), gateway, {"frequency": mhz(freq)})
                add(SCOPE_GATEWAY, gateway_id, _channel_airtime(freq), gateway, {"frequency": mhz(freq)})
            for mod in gateway["modulations"]:
                if mod != "unknown":
                    add(SCOPE_GATEWAY, gateway_id, _rx_modulation(mod), gateway, {"modulation": modulation_label(mod)})
            for freq in gateway["tx_channels"]:
                add(SCOPE_GATEWAY, gateway_id, _tx_channel(freq), gateway, {"frequency": mhz(freq)})

        if new:
            async_add_entities(new)

    _add_new_entities()
    entry.async_on_unload(coordinator.async_add_listener(_add_new_entities))


class ChirpStackSensor(ChirpStackEntity, SensorEntity):
    """A sensor backed by a value in the coordinator snapshot."""

    entity_description: ChirpStackSensorDescription
    _unrecorded_attributes = frozenset(
        {
            "last_stats_report",
            "last_tx",
            "tx_status",
            "bands",
            "gateways",
            "networks",
            "details",
            "top_talkers",
            "receptions",
            "last_heard",
            "heard",
            "uplinks",
        }
    )

    def __init__(
        self,
        coordinator: ChirpStackCoordinator,
        scope: str,
        scope_id: str | None,
        description: ChirpStackSensorDescription,
        placeholders: dict[str, str] | None = None,
    ) -> None:
        super().__init__(coordinator, scope, scope_id, description.key, placeholders)
        self.entity_description = description

    @property
    def native_value(self) -> StateType | datetime:
        data = self.scope_data
        return None if data is None else self.entity_description.value_fn(data)

    @property
    def extra_state_attributes(self) -> dict[str, Any] | None:
        data = self.scope_data
        if data is None or self.entity_description.attrs_fn is None:
            return None
        return self.entity_description.attrs_fn(data)
