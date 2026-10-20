"""Make the Home Assistant independent ``core`` package importable without HA."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "custom_components" / "chirpstack_gateway_monitor"))

SAMPLES = ROOT / "samples"
