"""The demo orchestrator: generate → push (HTTP or Pulsar) → read (REST+MCP)."""

from __future__ import annotations

import argparse
import sys
import time

from poc_demo.context_reads import ContextReader
from poc_demo.generator import AIGenerator


def main() -> int:
    args = _parse_args()
    generator = AIGenerator(
        tenant=args.tenant, cohort=f"cohort-{args.cohort}", seed=args.seed
    )
    signals = generator.stream(args.users, events_per_user=args.events_per_user)
    reco_events = sum(1 for signal in signals if signal["type"] == "itf.reco.v1")
    outcome_events = sum(1 for signal in signals if signal["type"] == "itf.outcome.v1")
    acted = sum(
        1
        for signal in signals
        if signal["type"] == "itf.outcome.v1" and signal["data"]["verdict"] == "acted_on"
    )
    print(f"generated {len(signals)} events "
          f"({len(signals) - reco_events - outcome_events} assessments, "
          f"{reco_events} recos, {outcome_events} outcomes [{acted} acted_on])")

    accepted = _push(args, generator, signals)
    print(f"platform ingest: {accepted}")

    reader = ContextReader(
        f"http://{args.platform}:{args.rest_port}",
        f"http://{args.platform}:{args.mcp_port}",
        args.token,
    )
    # Let the projection pipeline catch up (the platform serves staleness
    # windows; a fresh get is a compile, not a queue wait, but be polite).
    time.sleep(args.settle_seconds)
    user = "demo-user-0000"
    rest_payload, mcp_payload, identical = reader.parity(user, "skills")
    print(f"\ncontext for {user} via REST and MCP — identical payloads: {identical}")
    print(f"{args.platform} parity contract:", "OK" if identical else "MISMATCH")
    top_k = {}
    for label, payload in (("REST", rest_payload), ("MCP", mcp_payload)):
        items = payload["items"]
        top = ", ".join(str(item["target"]) for item in items[:3]) or "(no items)"
        top_k[label] = [str(item["target"]) for item in items[:5]]
        print(f"  {label} top skills: {top}")

    # Point-in-time slice (the platform's as_of read): the memory as it
    # looked at the mid-stream instant — later facts must be absent there.
    as_of_comparison: list[tuple[str, list[str]]] = []
    if signals:
        mid = signals[len(signals) // 2]["time"]
        rest_mid, mcp_mid, mid_identical = reader.parity(user, "skills", as_of=mid)
        mid_targets = [str(item["target"]) for item in rest_mid["items"][:5]]
        print(f"\nas_of {mid} (mid-stream) — identical: {mid_identical}")
        print(f"  skills at that instant: {', '.join(mid_targets) or '(no items)'}")
        as_of_comparison = [(f"mid-stream {mid}", mid_targets), ("now", top_k["REST"])]

    if args.report:
        from pathlib import Path

        from poc_demo.report import write_report

        Path(args.report).write_text(
            write_report(
                signal_count=len(signals),
                reco_count=reco_events,
                outcome_count=outcome_events,
                acted_on=acted,
                ingest_result=accepted,
                parity_ok=identical,
                top_k=top_k,
                as_of_comparison=as_of_comparison,
                signals=signals,
            ),
            encoding="utf-8",
        )
        print(f"\nreport written: {args.report}")
    if not identical:
        return 1
    return 0


def _push(args: argparse.Namespace, generator: AIGenerator, signals: list[dict[str, str]]) -> str:
    if args.transport == "pulsar":
        from poc_demo.producer_pulsar import PulsarSignalProducer

        producer = PulsarSignalProducer(args.pulsar_url, args.tenant)
        try:
            counts = producer.publish(signals)
        finally:
            producer.close()
        return f"published={counts['published']} to gmp.signals.{args.tenant}"
    from poc_demo.ingest_http import HttpIngestPusher

    counts = HttpIngestPusher(f"http://{args.platform}:{args.ingest_port}", args.token).push(signals)
    return (
        f"accepted={counts['accepted']} duplicates={counts['duplicates']} "
        f"rejected={counts['rejected']}"
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="poc-demo", description="Push AI-shaped signals into graph-memory-platform and read the personalized context back."
    )
    parser.add_argument("--platform", default="127.0.0.1", help="platform host (serve command)")
    parser.add_argument("--ingest-port", type=int, default=8001)
    parser.add_argument("--rest-port", type=int, default=8002)
    parser.add_argument("--mcp-port", type=int, default=8003)
    parser.add_argument("--users", type=int, default=4, help="demo cohort size")
    parser.add_argument("--events-per-user", type=int, default=6)
    parser.add_argument("--cohort", default="a", help="suffix of the cohort attribute (cohort-a)")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--tenant", default="acme")
    parser.add_argument("--token", default="dev-acme-token")
    parser.add_argument("--transport", choices=("http", "pulsar"), default="http")
    parser.add_argument("--pulsar-url", default="pulsar://127.0.0.1:6650")
    parser.add_argument("--settle-seconds", type=float, default=2.5)
    parser.add_argument(
        "--report", default=None,
        help="write a bounded Markdown evidence report for this run to PATH",
    )
    return parser.parse_args()


if __name__ == "__main__":
    sys.exit(main())
