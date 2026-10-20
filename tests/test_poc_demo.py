"""The demo runs end-to-end against `platform serve`; this module covers the
generated stream's shape and the HTTP pusher's batch contract offline."""

from __future__ import annotations

from poc_demo.generator import AIGenerator
from poc_demo.ingest_http import _chunks


def test_generator_stream_is_deterministic_and_correlated() -> None:
    first = AIGenerator(seed=7).stream(3, events_per_user=4)
    second = AIGenerator(seed=7).stream(3, events_per_user=4)
    assert first == second  # same seed ⇒ byte-identical stream

    recos = [signal for signal in first if signal["type"] == "itf.reco.v1"]
    outcomes = [signal for signal in first if signal["type"] == "itf.outcome.v1"]
    assert len(recos) == len(outcomes) > 0
    refs = {signal["data"]["reco_ref"] for signal in recos}
    assert outcomes[0]["data"]["correlation_id"] in refs

    ids = [signal["id"] for signal in first]
    assert len(ids) == len(set(ids))  # dedup-safe
    assert len({signal["partitionkey"] for signal in first}) == 3


def test_chunks_never_drop_a_signal() -> None:
    signals = [{"id": f"e{i}"} for i in range(7)]
    groups = list(_chunks(signals, 3))
    assert [len(group) for group in groups] == [3, 3, 1]
    assert sum(len(group) for group in groups) == len(signals)
