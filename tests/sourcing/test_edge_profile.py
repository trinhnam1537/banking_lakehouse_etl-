"""Profile edge: entry Kafka chay tren may nguon (systemd), khong qua Glue (10-31)."""

from __future__ import annotations

import pytest

from sourcing.config import ConfigError

from .conftest import make_cfg

_KAFKA = {
    "source_type": "kafka",
    "kafka": {"bootstrap_servers": "10.0.0.8:19092", "topic": "card.authorizations", "group_id": "g"},
    "cdc": {"mode": "cdc1"},
    "runtime": {"profile": "edge"},
}


def test_edge_kafka_hop_le():
    assert make_cfg(**_KAFKA).runtime.profile == "edge"


def test_edge_cam_lich_eventbridge():
    with pytest.raises(ConfigError, match="systemd timer"):
        make_cfg(**_KAFKA, schedule={"enabled": True, "expression": "rate(5 minutes)"})


def test_edge_chi_cho_kafka():
    with pytest.raises(ConfigError, match="chi cho source_type kafka"):
        make_cfg(source_type="httpfile", cdc={"mode": "cdc5"}, httpfile={"urls": {"k": "https://x"}},
                 runtime={"profile": "edge"})
