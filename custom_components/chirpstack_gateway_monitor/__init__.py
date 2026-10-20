"""ChirpStack Gateway Monitor: LoRaWAN gateway and spectrum health from gateway MQTT traffic."""

from __future__ import annotations

from homeassistant.components import mqtt
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryNotReady
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers.storage import Store

from .const import CONF_OWN_DEV_ADDRS, DOMAIN, STORAGE_VERSION
from .coordinator import ChirpStackConfigEntry, ChirpStackCoordinator, storage_key
from .entity import network_device_info

PLATFORMS = [Platform.BINARY_SENSOR, Platform.SENSOR]


async def async_setup_entry(hass: HomeAssistant, entry: ChirpStackConfigEntry) -> bool:
    if not await mqtt.async_wait_for_mqtt_client(hass):
        raise ConfigEntryNotReady("The MQTT integration is not available")

    coordinator = ChirpStackCoordinator(hass, entry)
    await coordinator.async_load()
    await coordinator.async_subscribe()
    await coordinator.async_config_entry_first_refresh()
    entry.runtime_data = coordinator

    # The network device must exist before gateways and nodes link to it.
    registry = dr.async_get(hass)
    network = registry.async_get_or_create(config_entry_id=entry.entry_id, **network_device_info(coordinator))
    coordinator.network_device_id = network.id
    _remove_stale_node_devices(hass, entry)
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    entry.async_on_unload(entry.add_update_listener(_async_options_updated))
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ChirpStackConfigEntry) -> bool:
    if unloaded := await hass.config_entries.async_unload_platforms(entry, PLATFORMS):
        await entry.runtime_data.async_save()
    return unloaded


async def async_remove_entry(hass: HomeAssistant, entry: ChirpStackConfigEntry) -> None:
    await Store(hass, STORAGE_VERSION, storage_key(entry.entry_id)).async_remove()


async def _async_options_updated(hass: HomeAssistant, entry: ChirpStackConfigEntry) -> None:
    await hass.config_entries.async_reload(entry.entry_id)


def _remove_stale_node_devices(hass: HomeAssistant, entry: ChirpStackConfigEntry) -> None:
    """Remove node devices whose DevAddr was taken out of the options."""
    own = set(entry.options.get(CONF_OWN_DEV_ADDRS, []))
    prefix = f"{entry.entry_id}_node_"
    registry = dr.async_get(hass)
    for device in dr.async_entries_for_config_entry(registry, entry.entry_id):
        for domain, identifier in device.identifiers:
            if domain == DOMAIN and identifier.startswith(prefix) and identifier.removeprefix(prefix) not in own:
                registry.async_remove_device(device.id)  # our devices belong to this entry only
                break


async def async_remove_config_entry_device(
    hass: HomeAssistant, entry: ChirpStackConfigEntry, device_entry: dr.DeviceEntry
) -> bool:
    """Allow deleting gateways that are gone; nodes are managed through the options."""
    gateway_prefix = f"{entry.entry_id}_gateway_"
    for domain, identifier in device_entry.identifiers:
        if domain == DOMAIN and identifier.startswith(gateway_prefix):
            entry.runtime_data.forget_gateway(identifier.removeprefix(gateway_prefix))
            return True
    return False
