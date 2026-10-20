"""MQTT messages in, Home Assistant entities out."""

from __future__ import annotations

import pytest

from homeassistant.config_entries import ConfigEntryState
from homeassistant.core import HomeAssistant
from homeassistant.helpers import device_registry as dr, entity_registry as er
from pytest_homeassistant_custom_component.common import MockConfigEntry, async_fire_mqtt_message
from pytest_homeassistant_custom_component.typing import MqttMockHAClient

from conftest import HOME, LAB, OWN, SENSECAP, make_entry, setup_entry
from custom_components.chirpstack_gateway_monitor import async_remove_config_entry_device
from custom_components.chirpstack_gateway_monitor.const import CONF_OWN_DEV_ADDRS, DOMAIN
from custom_components.chirpstack_gateway_monitor.diagnostics import async_get_config_entry_diagnostics

SC = f"sensor.gateway_{SENSECAP}"
NET = "sensor.lorawan_network_eu868"
NODE = f"sensor.lorawan_node_{OWN}"


def fire(hass: HomeAssistant, gateway: str, suffix: str, payload: bytes) -> None:
    async_fire_mqtt_message(hass, f"eu868/gateway/{gateway}/{suffix}", payload)


async def refresh(hass: HomeAssistant, entry: MockConfigEntry) -> None:
    await entry.runtime_data.async_refresh()
    await hass.async_block_till_done()


def state(hass: HomeAssistant, entity_id: str) -> str:
    current = hass.states.get(entity_id)
    assert current is not None, f"{entity_id} does not exist"
    return current.state


def device(hass: HomeAssistant, entry: MockConfigEntry, suffix: str) -> dr.DeviceEntry | None:
    registry = dr.async_get(hass)
    identifier = (DOMAIN, f"{entry.entry_id}_{suffix}")
    if hasattr(registry, "async_get_device_by_identifier"):  # Home Assistant 2026.8+
        return registry.async_get_device_by_identifier(identifier, entry.entry_id)
    return registry.async_get_device(identifiers={identifier})


async def feed_traffic(hass: HomeAssistant, p: dict[str, bytes]) -> None:
    fire(hass, SENSECAP, "state/conn", p["conn_online"])
    fire(hass, SENSECAP, "event/stats", p["stats_sensecap"])
    fire(hass, LAB, "event/stats", p["stats_lab_json"])
    for gateway, name in ((SENSECAP, "own_100_sensecap"), (HOME, "own_100_home"), (LAB, "own_100_lab")):
        fire(hass, gateway, "event/up", p[name])
    fire(hass, SENSECAP, "event/up", p["own_103_sensecap_json"])
    for i in range(5):
        fire(hass, SENSECAP, "event/up", p[f"flood_{i}_sensecap"])
    fire(hass, HOME, "event/up", p["flood_0_home"])
    fire(hass, SENSECAP, "event/up", p["p2p_sensecap"])
    fire(hass, SENSECAP, "event/up", p["join_sensecap"])
    fire(hass, SENSECAP, "command/down", p["down_42"])
    fire(hass, SENSECAP, "event/ack", p["ack_42"])
    fire(hass, SENSECAP, "event/up", b"\x0f\xff")  # garbage must not break anything
    fire(hass, HOME, "state/conn", p["conn_offline"])  # the home gateway goes away
    await hass.async_block_till_done()


async def test_traffic_becomes_entities(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient, payloads: dict[str, bytes]
) -> None:
    entry = await setup_entry(hass)
    assert entry.state is ConfigEntryState.LOADED
    await feed_traffic(hass, payloads)
    await refresh(hass, entry)

    # connectivity: retained ONLINE + stats, explicit OFFLINE, JSON stats
    assert state(hass, f"binary_sensor.gateway_{SENSECAP}_online") == "on"
    assert state(hass, f"binary_sensor.gateway_{HOME}_online") == "off"
    assert state(hass, f"binary_sensor.gateway_{LAB}_online") == "on"

    # gateway statistics
    assert state(hass, f"{SC}_rx_packets") == "134"
    assert state(hass, f"{SC}_rx_packets_bad_crc") == "14"
    assert state(hass, f"{SC}_crc_ok_ratio") == "89.6"
    assert state(hass, f"{SC}_tx_packets_emitted") == "1"
    assert state(hass, f"{SC}_duty_cycle_budget_used") == "25.0"
    assert state(hass, f"sensor.gateway_{LAB}_rx_packets") == "5"

    # per channel / per spreading factor, classification
    assert state(hass, f"{SC}_rx_867_9_mhz") == "5"
    assert state(hass, f"{SC}_rx_868_1_mhz") == "2"
    assert state(hass, f"{SC}_rx_867_7_mhz") == "1"
    assert state(hass, f"{SC}_rx_sf5") == "5"
    assert state(hass, f"{SC}_rx_sf7") == "3"
    assert state(hass, f"{SC}_rx_sf12") == "1"
    assert float(state(hass, f"{SC}_airtime_867_9_mhz")) > 0
    assert state(hass, f"{SC}_non_lorawan_packets") == "6"
    assert state(hass, f"{SC}_lorawan_packets") == "3"

    # downlink: RX1 too late, RX2 on 869.525 MHz went out
    assert state(hass, f"{SC}_tx_869_525_mhz") == "1"
    failed = hass.states.get(f"{SC}_downlinks_failed")
    assert failed.state == "0"
    assert failed.attributes["tx_status"] == {"TOO_LATE": 1, "OK": 1}

    # network
    assert state(hass, f"{NET}_top_talker") == "Non-LoRaWAN 0xC8 SF5"
    assert state(hass, f"{NET}_unique_packets") == "9"
    assert state(hass, f"{NET}_gateways_online") == "2"
    assert state(hass, f"{NET}_join_requests") == "1"
    assert state(hass, f"{NET}_devices_heard") == "2"
    details = hass.states.get(f"{NET}_top_talker").attributes["details"]
    assert list(details["gateways"]) == [SENSECAP, HOME]

    # own node: FCnt 100 heard by three gateways, 103 by one (101, 102 lost)
    assert state(hass, f"{NODE}_packets") == "2"
    assert state(hass, f"{NODE}_frame_counter") == "103"
    assert state(hass, f"{NODE}_packet_loss") == "50.0"
    assert state(hass, f"{NODE}_rssi") == "-97"
    assert state(hass, f"{NODE}_gateways") == "1"
    assert state(hass, f"{NODE}_frequency") == "867.7"
    assert state(hass, f"{NODE}_spreading_factor") == "7"

    # how each gateway hears the node: 4 uplinks sent (100..103), SenseCAP heard 2, the others 1
    assert state(hass, f"{NODE}_rssi_{SENSECAP}") == "-97"
    assert state(hass, f"{NODE}_snr_{SENSECAP}") == "11.0"
    assert state(hass, f"{NODE}_reception_{SENSECAP}") == "50.0"
    assert state(hass, f"{NODE}_rssi_{LAB}") == "-83"
    assert state(hass, f"{NODE}_snr_{LAB}") == "9.25"
    assert state(hass, f"{NODE}_reception_{LAB}") == "25.0"
    assert state(hass, f"{NODE}_rssi_{HOME}") == "-107"
    reception = hass.states.get(f"{NODE}_reception_{HOME}")
    assert (reception.attributes["heard"], reception.attributes["uplinks"]) == (1, 4)

    diagnostics = await async_get_config_entry_diagnostics(hass, entry)
    aggregator = diagnostics["aggregator"]
    assert aggregator["decode_errors"] == {"up": 1}
    assert aggregator["payload_formats"]["json"] == 2
    assert aggregator["gateways"][SENSECAP]["metadata"] == {"concentratord_version": "4.4.0"}


async def test_entities_have_translated_names_and_devices(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient, payloads: dict[str, bytes]
) -> None:
    entry = await setup_entry(hass)
    await feed_traffic(hass, payloads)
    await refresh(hass, entry)

    registry = er.async_get(hass)
    rx = registry.async_get(f"{SC}_rx_867_9_mhz")
    assert rx.translation_key == "rx_channel"
    assert hass.states.get(f"{SC}_rx_867_9_mhz").attributes["friendly_name"] == f"Gateway {SENSECAP} RX 867.9 MHz"
    assert registry.async_get(f"{SC}_rx_packets_crc_ok").disabled_by is er.RegistryEntryDisabler.INTEGRATION

    network = device(hass, entry, "network")
    gateway = device(hass, entry, f"gateway_{SENSECAP}")
    node = device(hass, entry, f"node_{OWN}")
    assert network and gateway and node
    assert network.entry_type is dr.DeviceEntryType.SERVICE
    assert gateway.via_device_id == network.id
    assert node.via_device_id == network.id


async def test_node_gateway_sensors_use_gateway_device_names(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient, payloads: dict[str, bytes]
) -> None:
    entry = await setup_entry(hass)
    fire(hass, SENSECAP, "event/stats", payloads["stats_sensecap"])
    await hass.async_block_till_done()
    await refresh(hass, entry)
    dr.async_get(hass).async_update_device(device(hass, entry, f"gateway_{SENSECAP}").id, name_by_user="SenseCAP")

    fire(hass, SENSECAP, "event/up", payloads["own_100_sensecap"])
    await hass.async_block_till_done()
    await refresh(hass, entry)
    rssi = hass.states.get(f"{NODE}_rssi_sensecap")
    assert rssi.state == "-99"
    assert rssi.attributes["friendly_name"] == f"LoRaWAN node {OWN} RSSI SenseCAP"
    assert rssi.attributes["gateway_id"] == SENSECAP
    assert state(hass, f"{NODE}_reception_sensecap") == "100.0"


async def test_counters_survive_restart(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient, payloads: dict[str, bytes], hass_storage: dict
) -> None:
    entry = await setup_entry(hass)
    fire(hass, SENSECAP, "event/stats", payloads["stats_sensecap"])
    fire(hass, SENSECAP, "event/up", payloads["flood_0_sensecap"])
    await hass.async_block_till_done()
    await refresh(hass, entry)

    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()
    stored = hass_storage[f"{DOMAIN}.{entry.entry_id}"]["data"]
    assert stored["gateways"][SENSECAP]["totals"]["rx_received"] == 134

    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    # entities come back with their totals before any new traffic arrives
    assert state(hass, f"{SC}_rx_packets") == "134"
    assert state(hass, f"{SC}_rx_867_9_mhz") == "1"
    fire(hass, SENSECAP, "event/stats", payloads["stats_sensecap"])
    await hass.async_block_till_done()
    await refresh(hass, entry)
    assert state(hass, f"{SC}_rx_packets") == "268"


async def test_removing_a_node_from_options_removes_its_device(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    entry = await setup_entry(hass)
    assert device(hass, entry, f"node_{OWN}")

    hass.config_entries.async_update_entry(entry, options={**entry.options, CONF_OWN_DEV_ADDRS: []})
    await hass.async_block_till_done()
    assert entry.state is ConfigEntryState.LOADED
    assert device(hass, entry, f"node_{OWN}") is None
    assert hass.states.get(f"{NODE}_rssi") is None


async def test_deleted_gateway_comes_back_when_heard_again(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient, payloads: dict[str, bytes]
) -> None:
    entry = await setup_entry(hass)
    fire(hass, LAB, "event/stats", payloads["stats_lab_json"])
    await hass.async_block_till_done()
    await refresh(hass, entry)
    lab = device(hass, entry, f"gateway_{LAB}")
    assert lab
    # the network (hub) device itself cannot be deleted
    assert not await async_remove_config_entry_device(hass, entry, device(hass, entry, "network"))

    assert await async_remove_config_entry_device(hass, entry, lab)
    dr.async_get(hass).async_remove_device(lab.id)  # what the UI does after the integration agreed
    await hass.async_block_till_done()
    await refresh(hass, entry)
    assert device(hass, entry, f"gateway_{LAB}") is None
    assert hass.states.get(f"sensor.gateway_{LAB}_rx_packets") is None

    fire(hass, LAB, "event/stats", payloads["stats_lab_json"])
    await hass.async_block_till_done()
    await refresh(hass, entry)
    assert device(hass, entry, f"gateway_{LAB}")
    assert state(hass, f"sensor.gateway_{LAB}_rx_packets") == "5"


async def test_setup_retries_without_mqtt(hass: HomeAssistant) -> None:
    entry = make_entry()
    entry.add_to_hass(hass)
    await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    assert entry.state is ConfigEntryState.SETUP_RETRY


@pytest.mark.parametrize("prefix", ["", "chirpstack/eu868"])
async def test_other_topic_prefixes(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient, payloads: dict[str, bytes], prefix: str
) -> None:
    entry = MockConfigEntry(
        domain=DOMAIN, unique_id=prefix or "/", data={"topic_prefix": prefix}, options=dict(make_entry().options)
    )
    await setup_entry(hass, entry)
    base = f"{prefix}/gateway" if prefix else "gateway"
    async_fire_mqtt_message(hass, f"{base}/{SENSECAP}/event/stats", payloads["stats_sensecap"])
    await hass.async_block_till_done()
    await refresh(hass, entry)
    assert entry.runtime_data.data["gateways"][SENSECAP]["rx_received"] == 134
