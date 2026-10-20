"""Base entity and device helpers."""

from __future__ import annotations

from typing import Any

from homeassistant.core import HomeAssistant
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers.device_registry import DeviceEntryType, DeviceInfo
from homeassistant.helpers.update_coordinator import CoordinatorEntity

try:
    from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback
except ImportError:  # Home Assistant < 2025.3
    from homeassistant.helpers.entity_platform import (  # type: ignore[assignment]
        AddEntitiesCallback as AddConfigEntryEntitiesCallback,
    )

from .const import DOMAIN
from .coordinator import ChirpStackCoordinator

SCOPE_GATEWAY = "gateway"
SCOPE_NETWORK = "network"
SCOPE_NODE = "node"

# Home Assistant 2026.8 links devices by registry id (via_device_id); the
# identifier based via_device is removed in 2027.8.
_HAS_VIA_DEVICE_ID = "via_device_id" in DeviceInfo.__annotations__


def device_identifier(entry_id: str, scope: str, scope_id: str | None = None) -> tuple[str, str]:
    return DOMAIN, f"{entry_id}_{scope}" + (f"_{scope_id}" if scope_id else "")


def network_device_info(coordinator: ChirpStackCoordinator) -> DeviceInfo:
    return DeviceInfo(
        identifiers={device_identifier(coordinator.config_entry.entry_id, SCOPE_NETWORK)},
        name=f"LoRaWAN network {coordinator.topic_prefix}".strip(),
        manufacturer="ChirpStack",
        model="Gateway traffic (MQTT)",
        entry_type=DeviceEntryType.SERVICE,
    )


def _device_info(coordinator: ChirpStackCoordinator, scope: str, scope_id: str | None) -> DeviceInfo:
    if scope == SCOPE_NETWORK:
        return network_device_info(coordinator)
    entry_id = coordinator.config_entry.entry_id
    info = DeviceInfo(
        identifiers={device_identifier(entry_id, scope, scope_id)},
        name=f"Gateway {scope_id}" if scope == SCOPE_GATEWAY else f"LoRaWAN node {scope_id}",
        model="LoRaWAN gateway" if scope == SCOPE_GATEWAY else "LoRaWAN end device",
        serial_number=scope_id,
    )
    if _HAS_VIA_DEVICE_ID:
        if coordinator.network_device_id:
            info["via_device_id"] = coordinator.network_device_id
    else:
        info["via_device"] = device_identifier(entry_id, SCOPE_NETWORK)  # type: ignore[typeddict-unknown-key]
    return info


def gateway_label(hass: HomeAssistant, coordinator: ChirpStackCoordinator, gateway_id: str) -> str:
    """The name the user gave a gateway's device, else the gateway ID."""
    registry = dr.async_get(hass)
    entry_id = coordinator.config_entry.entry_id
    identifier = device_identifier(entry_id, SCOPE_GATEWAY, gateway_id)
    if hasattr(registry, "async_get_device_by_identifier"):  # Home Assistant 2026.8+
        device = registry.async_get_device_by_identifier(identifier, entry_id)
    else:
        device = registry.async_get_device(identifiers={identifier})
    return device.name_by_user if device and device.name_by_user else gateway_id


def entity_key(platform: str, scope: str, scope_id: str | None, key: str) -> str:
    """Key used to track which entities were already added."""
    return f"{platform}:{scope}:{scope_id or ''}:{key}"


class ChirpStackEntity(CoordinatorEntity[ChirpStackCoordinator]):
    """An entity reading one scope (gateway, network or node) of the snapshot."""

    _attr_has_entity_name = True

    def __init__(
        self,
        coordinator: ChirpStackCoordinator,
        scope: str,
        scope_id: str | None,
        key: str,
        placeholders: dict[str, str] | None = None,
    ) -> None:
        super().__init__(coordinator)
        self._scope = scope
        self._scope_id = scope_id
        scope_part = f"{scope}_{scope_id}" if scope_id else scope
        self._attr_unique_id = f"{coordinator.config_entry.entry_id}_{scope_part}_{key}"
        if placeholders:
            self._attr_translation_placeholders = placeholders
        self._attr_device_info = _device_info(coordinator, scope, scope_id)

    @property
    def scope_data(self) -> dict[str, Any] | None:
        data = self.coordinator.data or {}
        if self._scope == SCOPE_NETWORK:
            return data.get("network")
        return data.get(f"{self._scope}s", {}).get(self._scope_id)

    @property
    def available(self) -> bool:
        return super().available and self.scope_data is not None
