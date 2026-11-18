"""Evidence artifact — the demo's run written down as bounded Markdown:
counts, ingest result, the parity verdict, per-channel top-k, and the
point-in-time comparison. Reproducible evidence, not vibes (the README's
own bar). The raw signal stream is never dumped."""
from __future__ import annotations

from typing import Any


def write_report(
    *,
    signal_count: int,
    reco_count: int,
    outcome_count: int,
    acted_on: int,
    ingest_result: str,
    parity_ok: bool,
    top_k: dict[str, list[str]],
    as_of_comparison: list[tuple[str, list[str]]],
    signals: list[dict[str, Any]] | None = None,
) -> str:
    lines = [
        "# POC run report",
        "",
        f"- signals: {signal_count}"
        f" ({signal_count - reco_count - outcome_count} assessments,"
        f" {reco_count} recos, {outcome_count} outcomes, {acted_on} acted_on)",
        f"- platform ingest: {ingest_result}",
        f"- parity: {'OK' if parity_ok else 'MISMATCH'}",
    ]
    for label, targets in top_k.items():
        top = ", ".join(targets[:5]) or "(no items)"
        lines.append(f"- {label} top-k: {top}")
    if as_of_comparison:
        lines += ["", "## Point-in-time comparison", ""]
        for instant, targets in as_of_comparison[:5]:
            top = ", ".join(targets[:5]) or "(no items)"
            lines.append(f"- {instant}: {top}")
    return "\n".join(lines) + "\n"
