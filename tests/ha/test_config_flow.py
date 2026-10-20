"""Config and options flow."""

from __future__ import annotations

from homeassistant.config_entries import SOURCE_USER
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType
from pytest_homeassistant_custom_component.typing import MqttMockHAClient

from conftest import OWN, setup_entry
from custom_components.chirpstack_gateway_monitor.const import (
    CONF_DEVICE_WINDOW,
    CONF_OWN_DEV_ADDRS,
    CONF_TOPIC_PREFIX,
    CONF_UPDATE_INTERVAL,
    CONF_WINDOW,
    DOMAIN,
)


async def test_user_flow_creates_entry(hass: HomeAssistant, mqtt_mock: MqttMockHAClient) -> None:
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": SOURCE_USER})
    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "user"

    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {CONF_TOPIC_PREFIX: "/eu868/", CONF_OWN_DEV_ADDRS: "00a1b2c3, 0x01ABCDEF"}
    )
    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["title"] == "ChirpStack eu868"
    assert result["data"] == {CONF_TOPIC_PREFIX: "eu868"}
    assert result["options"][CONF_OWN_DEV_ADDRS] == [OWN, "01abcdef"]
    await hass.async_block_till_done()

    # the same prefix cannot be added twice
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": SOURCE_USER})
    result = await hass.config_entries.flow.async_configure(result["flow_id"], {CONF_TOPIC_PREFIX: "eu868"})
    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "already_configured"


async def test_user_flow_validation(hass: HomeAssistant, mqtt_mock: MqttMockHAClient) -> None:
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": SOURCE_USER})
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"], {CONF_TOPIC_PREFIX: "eu868", CONF_OWN_DEV_ADDRS: "0085675"}
    )
    assert result["type"] is FlowResultType.FORM
    assert result["errors"] == {CONF_OWN_DEV_ADDRS: "invalid_dev_addr"}

    result = await hass.config_entries.flow.async_configure(result["flow_id"], {CONF_TOPIC_PREFIX: "eu868/+"})
    assert result["errors"] == {CONF_TOPIC_PREFIX: "invalid_prefix"}

    result = await hass.config_entries.flow.async_configure(result["flow_id"], {CONF_TOPIC_PREFIX: "eu868"})
    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["options"][CONF_OWN_DEV_ADDRS] == []


async def test_user_flow_requires_mqtt(hass: HomeAssistant) -> None:
    result = await hass.config_entries.flow.async_init(DOMAIN, context={"source": SOURCE_USER})
    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "mqtt_not_configured"


async def test_options_flow(hass: HomeAssistant, mqtt_mock: MqttMockHAClient) -> None:
    entry = await setup_entry(hass)

    result = await hass.config_entries.options.async_init(entry.entry_id)
    assert result["type"] is FlowResultType.FORM
    result = await hass.config_entries.options.async_configure(
        result["flow_id"],
        {CONF_OWN_DEV_ADDRS: "zz", CONF_UPDATE_INTERVAL: 30, CONF_WINDOW: 5, CONF_DEVICE_WINDOW: 120},
    )
    assert result["errors"] == {CONF_OWN_DEV_ADDRS: "invalid_dev_addr"}

    result = await hass.config_entries.options.async_configure(
        result["flow_id"],
        {CONF_OWN_DEV_ADDRS: "00a1b2c3 260B1234", CONF_UPDATE_INTERVAL: 30.0, CONF_WINDOW: 5, CONF_DEVICE_WINDOW: 120},
    )
    assert result["type"] is FlowResultType.CREATE_ENTRY
    await hass.async_block_till_done()
    assert dict(entry.options) == {
        CONF_OWN_DEV_ADDRS: [OWN, "260b1234"],
        CONF_UPDATE_INTERVAL: 30,
        CONF_WINDOW: 5,
        CONF_DEVICE_WINDOW: 120,
    }
    # reloaded with the new options
    assert entry.runtime_data.aggregator.window == 300
    assert set(entry.runtime_data.aggregator.nodes) == {OWN, "260b1234"}
