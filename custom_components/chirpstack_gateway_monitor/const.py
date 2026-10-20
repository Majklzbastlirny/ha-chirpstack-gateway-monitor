"""Constants for the ChirpStack Gateway Monitor integration."""

from typing import Final

DOMAIN: Final = "chirpstack_gateway_monitor"

CONF_TOPIC_PREFIX: Final = "topic_prefix"
CONF_OWN_DEV_ADDRS: Final = "own_dev_addrs"
CONF_UPDATE_INTERVAL: Final = "update_interval"
CONF_WINDOW: Final = "window_minutes"
CONF_DEVICE_WINDOW: Final = "device_window_minutes"

DEFAULT_TOPIC_PREFIX: Final = "eu868"
DEFAULT_UPDATE_INTERVAL: Final = 60  # seconds
DEFAULT_WINDOW: Final = 10  # minutes: rates, airtime, signal averages
DEFAULT_DEVICE_WINDOW: Final = 60  # minutes: devices heard, top talkers, packet loss, TX duty cycle

STORAGE_VERSION: Final = 1
STORAGE_SAVE_INTERVAL: Final = 300  # seconds

# MQTT topic suffix (after <prefix>/gateway/<gateway id>/) -> message kind
TOPICS: Final = {
    "event/up": "up",
    "event/stats": "stats",
    "event/ack": "ack",
    "state/conn": "conn",
    "command/down": "down",
}
