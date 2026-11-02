"""Config and options flow."""

from __future__ import annotations

import re
from typing import Any

import voluptuous as vol

from homeassistant.config_entries import ConfigFlow, ConfigFlowResult, OptionsFlow
from homeassistant.core import callback
from homeassistant.helpers.selector import (
    NumberSelector,
    NumberSelectorConfig,
    NumberSelectorMode,
    TextSelector,
)

from .const import (
    CONF_DEVICE_WINDOW,
    CONF_OWN_DEV_ADDRS,
    CONF_TOPIC_PREFIX,
    CONF_UPDATE_INTERVAL,
    CONF_WINDOW,
    DEFAULT_DEVICE_WINDOW,
    DEFAULT_TOPIC_PREFIX,
    DEFAULT_UPDATE_INTERVAL,
    DEFAULT_WINDOW,
    DOMAIN,
)

_DEVICE = re.compile(r"^(?:[0-9a-f]{8}|[0-9a-f]{16})$")  # a DevAddr or a DevEUI


def parse_dev_addrs(text: str | None) -> tuple[list[str], list[str]]:
    """Split user input into (valid DevAddrs and DevEUIs, invalid entries)."""
    items = [
        re.sub(r"[:\-]", "", item.lower()).removeprefix("0x") for item in re.split(r"[\s,;]+", text or "") if item
    ]
    valid = list(dict.fromkeys(item for item in items if _DEVICE.match(item)))
    invalid = [item for item in items if not _DEVICE.match(item)]
    return valid, invalid


def normalize_prefix(text: str) -> str | None:
    """Topic prefix without surrounding slashes, or None when invalid."""
    prefix = text.strip().strip("/")
    if any(char in prefix for char in "+#") or "//" in prefix:
        return None
    return prefix


def _number(minimum: int, maximum: int, step: int, unit: str) -> NumberSelector:
    return NumberSelector(
        NumberSelectorConfig(min=minimum, max=maximum, step=step, unit_of_measurement=unit, mode=NumberSelectorMode.BOX)
    )


OPTIONS_SCHEMA = vol.Schema(
    {
        vol.Optional(CONF_OWN_DEV_ADDRS): TextSelector(),
        vol.Required(CONF_UPDATE_INTERVAL): _number(10, 600, 5, "s"),
        vol.Required(CONF_WINDOW): _number(1, 120, 1, "min"),
        vol.Required(CONF_DEVICE_WINDOW): _number(5, 1440, 5, "min"),
    }
)


class ChirpStackGatewayMonitorConfigFlow(ConfigFlow, domain=DOMAIN):
    """Set up monitoring of one ChirpStack region topic prefix."""

    VERSION = 1

    async def async_step_user(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        if not self.hass.config_entries.async_entries("mqtt"):
            return self.async_abort(reason="mqtt_not_configured")

        errors: dict[str, str] = {}
        if user_input is not None:
            prefix = normalize_prefix(user_input[CONF_TOPIC_PREFIX])
            dev_addrs, invalid = parse_dev_addrs(user_input.get(CONF_OWN_DEV_ADDRS))
            if prefix is None:
                errors[CONF_TOPIC_PREFIX] = "invalid_prefix"
            elif invalid:
                errors[CONF_OWN_DEV_ADDRS] = "invalid_dev_addr"
            else:
                await self.async_set_unique_id(prefix or "/")
                self._abort_if_unique_id_configured()
                return self.async_create_entry(
                    title=f"ChirpStack {prefix}" if prefix else "ChirpStack",
                    data={CONF_TOPIC_PREFIX: prefix},
                    options={
                        CONF_OWN_DEV_ADDRS: dev_addrs,
                        CONF_UPDATE_INTERVAL: DEFAULT_UPDATE_INTERVAL,
                        CONF_WINDOW: DEFAULT_WINDOW,
                        CONF_DEVICE_WINDOW: DEFAULT_DEVICE_WINDOW,
                    },
                )

        schema = vol.Schema(
            {
                vol.Required(CONF_TOPIC_PREFIX, default=DEFAULT_TOPIC_PREFIX): str,
                vol.Optional(CONF_OWN_DEV_ADDRS): TextSelector(),
            }
        )
        return self.async_show_form(
            step_id="user",
            data_schema=self.add_suggested_values_to_schema(schema, user_input or {}),
            errors=errors,
        )

    @staticmethod
    @callback
    def async_get_options_flow(config_entry: Any) -> ChirpStackGatewayMonitorOptionsFlow:
        return ChirpStackGatewayMonitorOptionsFlow()


class ChirpStackGatewayMonitorOptionsFlow(OptionsFlow):
    """Own DevAddrs, update interval and statistics windows."""

    async def async_step_init(self, user_input: dict[str, Any] | None = None) -> ConfigFlowResult:
        errors: dict[str, str] = {}
        if user_input is not None:
            dev_addrs, invalid = parse_dev_addrs(user_input.get(CONF_OWN_DEV_ADDRS))
            if invalid:
                errors[CONF_OWN_DEV_ADDRS] = "invalid_dev_addr"
            else:
                return self.async_create_entry(
                    data={
                        CONF_OWN_DEV_ADDRS: dev_addrs,
                        CONF_UPDATE_INTERVAL: int(user_input[CONF_UPDATE_INTERVAL]),
                        CONF_WINDOW: int(user_input[CONF_WINDOW]),
                        CONF_DEVICE_WINDOW: int(user_input[CONF_DEVICE_WINDOW]),
                    }
                )

        options = self.config_entry.options
        suggested = user_input or {
            CONF_OWN_DEV_ADDRS: ", ".join(options.get(CONF_OWN_DEV_ADDRS, [])),
            CONF_UPDATE_INTERVAL: options.get(CONF_UPDATE_INTERVAL, DEFAULT_UPDATE_INTERVAL),
            CONF_WINDOW: options.get(CONF_WINDOW, DEFAULT_WINDOW),
            CONF_DEVICE_WINDOW: options.get(CONF_DEVICE_WINDOW, DEFAULT_DEVICE_WINDOW),
        }
        return self.async_show_form(
            step_id="init",
            data_schema=self.add_suggested_values_to_schema(OPTIONS_SCHEMA, suggested),
            errors=errors,
        )
