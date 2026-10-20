"""Gateway connectivity."""

from __future__ import annotations

from typing import Any

from homeassistant.components.binary_sensor import BinarySensorDeviceClass, BinarySensorEntity
from homeassistant.core import HomeAssistant, callback

from .coordinator import ChirpStackConfigEntry, ChirpStackCoordinator
from .entity import SCOPE_GATEWAY, AddConfigEntryEntitiesCallback, ChirpStackEntity, entity_key


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ChirpStackConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    coordinator = entry.runtime_data

    @callback
    def _add_new_entities() -> None:
        new = []
        for gateway_id in (coordinator.data or {}).get("gateways", {}):
            key = entity_key("binary_sensor", SCOPE_GATEWAY, gateway_id, "online")
            if key not in coordinator.known_entities:
                coordinator.known_entities.add(key)
                new.append(GatewayOnlineSensor(coordinator, gateway_id))
        if new:
            async_add_entities(new)

    _add_new_entities()
    entry.async_on_unload(coordinator.async_add_listener(_add_new_entities))


class GatewayOnlineSensor(ChirpStackEntity, BinarySensorEntity):
    """On while the gateway is connected and sending statistics."""

    _attr_device_class = BinarySensorDeviceClass.CONNECTIVITY
    _attr_translation_key = "online"
    _unrecorded_attributes = frozenset({"metadata", "location", "config_version", "stats_interval_s"})

    def __init__(self, coordinator: ChirpStackCoordinator, gateway_id: str) -> None:
        super().__init__(coordinator, SCOPE_GATEWAY, gateway_id, "online")

    @property
    def is_on(self) -> bool | None:
        data = self.scope_data
        return None if data is None else data["online"]

    @property
    def extra_state_attributes(self) -> dict[str, Any] | None:
        data = self.scope_data
        if data is None:
            return None
        attrs: dict[str, Any] = {
            "connection_state": data["conn_state"],
            "stats_interval_s": data["stats_interval"],
            "config_version": data["config_version"],
            "metadata": data["metadata"],
        }
        if data["location"]:
            attrs["location"] = data["location"]
        return attrs
