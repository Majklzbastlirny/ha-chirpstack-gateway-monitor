"""Diagnostics: shows exactly what the gateways report (e.g. which stats fields are filled)."""

from __future__ import annotations

import time
from typing import Any

from homeassistant.components.diagnostics import async_redact_data
from homeassistant.core import HomeAssistant

from .coordinator import ChirpStackConfigEntry

TO_REDACT = {"location", "latitude", "longitude", "altitude"}


async def async_get_config_entry_diagnostics(hass: HomeAssistant, entry: ChirpStackConfigEntry) -> dict[str, Any]:
    coordinator = entry.runtime_data
    return {
        "entry": {"title": entry.title, "data": dict(entry.data), "options": dict(entry.options)},
        "aggregator": async_redact_data(coordinator.aggregator.diagnostics(time.time()), TO_REDACT),
    }
