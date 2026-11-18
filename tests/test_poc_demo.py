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


# ── market-steal: point-in-time parity, evidence artifact, golden pin ────────


def test_context_reader_threads_as_of_to_both_channels() -> None:
    """as_of must reach REST (query param) and MCP (tool argument) — parity
    compares like with like."""
    from unittest.mock import patch

    from poc_demo.context_reads import ContextReader

    reader = ContextReader("http://rest", "http://mcp", "tok")
    captured = {}

    def fake_get(url, headers=None, timeout=None, params=None):
        captured["rest_params"] = params
        captured["rest_url"] = url

        class R:
            def raise_for_status(self): pass
            def json(self): return {"payload": {"items": []}}
        return R()

    with patch("poc_demo.context_reads.httpx.get", fake_get):
        reader.rest_context("u", "skills", as_of="2026-06-03T00:00:00Z")
    assert captured["rest_params"] == {"as_of": "2026-06-03T00:00:00Z"}

    def fake_post(url, json=None, headers=None):
        captured.setdefault("mcp_calls", []).append(json)

        class R:
            def raise_for_status(self): pass
            def json(self): return {"result": {"structuredContent": {"payload": {"items": []}}}}
        return R()

    class FakeClient:
        def __init__(self, timeout=None): pass
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def post(self, url, json=None, headers=None): return fake_post(url, json=json, headers=headers)

    with patch("poc_demo.context_reads.httpx.Client", FakeClient):
        reader.mcp_context("u", "skills", as_of="2026-06-03T00:00:00Z")
    tool_call = captured["mcp_calls"][1]
    assert tool_call["params"]["arguments"]["as_of"] == "2026-06-03T00:00:00Z"


def test_parity_compares_the_same_instant() -> None:
    from unittest.mock import patch

    from poc_demo.context_reads import ContextReader

    reader = ContextReader("http://rest", "http://mcp", "tok")
    seen = {}

    def fake_get(url, headers=None, timeout=None, params=None):
        seen["rest"] = params

        class R:
            def raise_for_status(self): pass
            def json(self): return {"payload": {"items": []}}
        return R()

    def fake_post(url, json=None, headers=None):
        class R:
            def raise_for_status(self): pass
            def json(self):
                return {"result": {"structuredContent": {"payload": {"items": []}}}}
        return R()

    class FakeClient:
        def __init__(self, timeout=None): pass
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def post(self, url, json=None, headers=None): return fake_post(url, json=json, headers=headers)

    with patch("poc_demo.context_reads.httpx.get", fake_get),             patch("poc_demo.context_reads.httpx.Client", FakeClient):
        rest, mcp, identical = reader.parity("u", "skills", as_of="2026-06-03T00:00:00Z")
    assert seen["rest"] == {"as_of": "2026-06-03T00:00:00Z"}
    assert identical  # same instant, same answer


def test_report_writer_produces_bounded_markdown() -> None:
    from poc_demo.report import write_report

    markdown = write_report(
        signal_count=12, reco_count=3, outcome_count=3, acted_on=2,
        ingest_result="accepted 12/12", parity_ok=True,
        top_k={"REST": ["skill:rust", "skill:kafka"], "MCP": ["skill:rust", "skill:kafka"]},
        as_of_comparison=[
            ("2026-06-01T12:00:00Z", ["skill:rust"]),
            ("now", ["skill:kafka"]),
        ],
        signals=[{"id": f"e{i}", "type": "t"} for i in range(50)],
    )
    assert "# POC run report" in markdown
    assert "accepted 12/12" in markdown
    assert "parity: OK" in markdown
    assert "2026-06-01T12:00:00Z" in markdown
    assert "e49" not in markdown            # raw signals are NOT dumped
    assert len(markdown) < 5_000            # bounded


def test_golden_stream_pin_seed_7() -> None:
    """The first events of seed-7 are pinned: generator drift must fail CI
    loudly, not silently change every demo run."""
    stream = AIGenerator(seed=7).stream(3, events_per_user=4)
    # the pinned golden: 6 assessments then recos (seed-7, 3 users, 4/user)
    golden_types = [signal["type"] for signal in stream[:7]]
    assert golden_types == [
        "itf.skill_assessed.v1", "itf.skill_assessed.v1",
        "itf.skill_assessed.v1", "itf.skill_assessed.v1",
        "itf.skill_assessed.v1", "itf.skill_assessed.v1",
        "itf.reco.v1",
    ]
    ids = [signal["id"] for signal in stream]
    assert len(ids) == len(set(ids))
