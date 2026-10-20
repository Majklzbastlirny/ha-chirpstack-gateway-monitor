"""Receives gateway traffic through Home Assistant's MQTT integration."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from datetime import timedelta
from typing import Any

from homeassistant.components import mqtt
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.storage import Store
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator

from .const import (
    CONF_DEVICE_WINDOW,
    CONF_OWN_DEV_ADDRS,
    CONF_TOPIC_PREFIX,
    CONF_UPDATE_INTERVAL,
    CONF_WINDOW,
    DEFAULT_DEVICE_WINDOW,
    DEFAULT_UPDATE_INTERVAL,
    DEFAULT_WINDOW,
    DOMAIN,
    STORAGE_SAVE_INTERVAL,
    STORAGE_VERSION,
    TOPICS,
)
from .core.aggregator import Aggregator

_LOGGER = logging.getLogger(__name__)

type ChirpStackConfigEntry = ConfigEntry[ChirpStackCoordinator]


def storage_key(entry_id: str) -> str:
    return f"{DOMAIN}.{entry_id}"


class ChirpStackCoordinator(DataUpdateCoordinator[dict[str, Any]]):
    """Feeds MQTT messages into the aggregator and publishes periodic snapshots.

    Messages are only aggregated in memory as they arrive; entity states are
    written once per update interval so a flooded channel cannot flood the
    recorder.
    """

    config_entry: ChirpStackConfigEntry

    def __init__(self, hass: HomeAssistant, entry: ChirpStackConfigEntry) -> None:
        options = entry.options
        super().__init__(
            hass,
            _LOGGER,
            config_entry=entry,
            name=f"{DOMAIN} {entry.title}",
            update_interval=timedelta(seconds=options.get(CONF_UPDATE_INTERVAL, DEFAULT_UPDATE_INTERVAL)),
        )
        self.topic_prefix: str = entry.data[CONF_TOPIC_PREFIX]
        self.own_dev_addrs: list[str] = list(options.get(CONF_OWN_DEV_ADDRS, []))
        self.aggregator = Aggregator(
            own_dev_addrs=self.own_dev_addrs,
            window=60.0 * options.get(CONF_WINDOW, DEFAULT_WINDOW),
            device_window=60.0 * options.get(CONF_DEVICE_WINDOW, DEFAULT_DEVICE_WINDOW),
            now=time.time(),
        )
        # Entity keys already added, shared by the platforms (see forget_gateway).
        self.known_entities: set[str] = set()
        self.network_device_id: str | None = None
        self._store: Store[dict[str, Any]] = Store(hass, STORAGE_VERSION, storage_key(entry.entry_id))
        self._save_deadline: float | None = None
        self._warned: set[str] = set()

    @property
    def topic_base(self) -> str:
        return f"{self.topic_prefix}/gateway" if self.topic_prefix else "gateway"

    async def async_load(self) -> None:
        """Restore counters from the previous run."""
        if data := await self._store.async_load():
            self.aggregator.load_storage(data)

    async def async_save(self) -> None:
        await self._store.async_save(self.aggregator.to_storage())

    async def async_subscribe(self) -> None:
        """Subscribe with raw (bytes) payloads: the gateway bridge sends protobuf."""
        for suffix, kind in TOPICS.items():
            unsubscribe = await mqtt.async_subscribe(
                self.hass,
                f"{self.topic_base}/+/{suffix}",
                self._message_handler(kind),
                qos=0,
                encoding=None,
            )
            self.config_entry.async_on_unload(unsubscribe)

    def _message_handler(self, kind: str) -> Callable[[mqtt.ReceiveMessage], None]:
        @callback
        def _handle(msg: mqtt.ReceiveMessage) -> None:
            self._handle_message(kind, msg)

        return _handle

    @callback
    def _handle_message(self, kind: str, msg: mqtt.ReceiveMessage) -> None:
        # <prefix>/gateway/<gateway id>/<event|state|command>/<name>
        parts = msg.topic.split("/")
        payload = msg.payload
        if len(parts) < 4 or not payload:  # an empty payload clears a retained message
            return
        if isinstance(payload, str):
            payload = payload.encode()
        gateway_id = parts[-3].lower()
        try:
            self.aggregator.handle(kind, gateway_id, payload, time.time())
        except Exception as err:  # noqa: BLE001 - one bad packet must not break the subscription
            self.aggregator.record_error(kind, gateway_id, err)
            if kind not in self._warned:
                self._warned.add(kind)
                _LOGGER.warning(
                    "Could not process %s message on %s: %r (further errors are logged at debug level)",
                    kind,
                    msg.topic,
                    err,
                )
            else:
                _LOGGER.debug("Could not process %s message on %s", kind, msg.topic, exc_info=True)

    async def _async_update_data(self) -> dict[str, Any]:
        snapshot = self.aggregator.snapshot(time.time())
        # Save at a fixed pace. async_delay_save debounces, so re-arming it on
        # every update with a constant delay would postpone the save forever;
        # a pending save is also written when Home Assistant stops.
        now = time.monotonic()
        if self._save_deadline is None or now >= self._save_deadline:
            self._save_deadline = now + STORAGE_SAVE_INTERVAL
        self._store.async_delay_save(self.aggregator.to_storage, self._save_deadline - now)
        return snapshot

    @callback
    def forget_gateway(self, gateway_id: str) -> None:
        """Drop a gateway whose device was deleted; it is re-created if it shows up again."""
        self.aggregator.forget_gateway(gateway_id)
        marker = f":gateway:{gateway_id}:"
        self.known_entities = {key for key in self.known_entities if marker not in key}
        self.config_entry.async_create_task(self.hass, self.async_save())
