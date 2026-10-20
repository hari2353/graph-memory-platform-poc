"""Pulsar producer (optional extra): envelopes onto `gmp.signals.<tenant>`.

The platform's broker adapter routes tenants by the topic tail and preserves
per-user ordering; its kit-owned stamping rules (record key / ordering key /
`Nats-Msg-Id`) live in the platform, so this producer only needs to pick the
topic and publish envelopes verbatim. Requires the `pulsar` extra:
`pip install "graph-memory-platform-poc[pulsar]"`.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Iterator
from typing import Any

_TOPIC_FORMAT = "persistent://public/default/gmp.signals.{tenant}"


def _require_pulsar() -> Any:
    try:
        import pulsar  # type: ignore[import-not-found,import-untyped]
    except ModuleNotFoundError as exc:  # pragma: no cover - install edge
        raise SystemExit(
            "pulsar-client is not installed. Run: pip install "
            '"graph-memory-platform-poc[pulsar]" (or uv pip install -e ".[pulsar]")'
        ) from exc
    return pulsar


class PulsarSignalProducer:
    """Publishes envelope dicts to the tenant's topic, one JSON message each."""

    def __init__(self, service_url: str, tenant: str) -> None:
        pulsar = _require_pulsar()
        self._client = pulsar.Client(service_url)
        self._topic = _TOPIC_FORMAT.format(tenant=tenant)
        self._producer = self._client.create_producer(self._topic)
        self._tenant = tenant

    def publish(self, signals: Iterable[dict[str, Any]]) -> dict[str, int]:
        count = 0
        for signal in signals:
            self._producer.send(json.dumps(signal).encode("utf-8"))
            count += 1
        return {"published": count}

    def close(self) -> None:
        self._producer.close()
        self._client.close()
