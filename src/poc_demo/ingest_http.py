"""Thin HTTP pusher to the platform's ingest edge (its own wire contract).

Batched POSTs to ``/v1/signals:batch`` with the tenant's bearer token.
Retries once on 429/503 with a modest backoff — this is the simplest
honest client an app would ship; anyone wanting publish/retry semantics
at the Kubernetes edge has the broker adapters instead.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import Any

import httpx

_MAX_BATCH = 500  # platform Settings.max_batch_events default is 10k; stay polite


def _chunks(signals: Iterable[dict[str, Any]], size: int) -> Iterator[list[dict[str, Any]]]:
    window: list[dict[str, Any]] = []
    for signal in signals:
        window.append(signal)
        if len(window) >= size:
            yield window
            window = []
    if window:
        yield window


class HttpIngestPusher:
    """Batches envelopes into /v1/signals:batch; returns total accepted."""

    def __init__(self, base_url: str, token: str, *, timeout_s: float = 10.0) -> None:
        self._url = f"{base_url.rstrip('/')}/v1/signals:batch"
        self._token = token
        self._timeout_s = timeout_s

    def push(self, signals: Iterable[dict[str, Any]]) -> dict[str, int]:
        accepted = duplicates = rejected = 0
        with httpx.Client(timeout=self._timeout_s) as client:
            for batch in _chunks(signals, _MAX_BATCH):
                response = client.post(
                    self._url,
                    json={"signals": batch},
                    headers={"Authorization": f"Bearer {self._token}"},
                )
                if response.status_code in (429, 503):
                    raise SystemExit(
                        f"platform is rate limiting/cold ({response.status_code}); "
                        "retry shortly — the quotas guard is doing its job"
                    )
                response.raise_for_status()
                body = response.json()
                accepted += int(body["accepted"])
                duplicates += int(body["duplicates"])
                rejected += len(body.get("rejected", ()))
        return {"accepted": accepted, "duplicates": duplicates, "rejected": rejected}
