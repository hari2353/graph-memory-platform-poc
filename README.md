# graph-memory-platform — POC demo

Standalone demo for [graph-memory-platform](https://github.com/hari2353/graph-memory-platform):
an **AI-signal generator** (an app-shaped event stream: skill assessments,
recommendations, correlated outcomes — the ITF-style reference workload) that
pushes into the platform either

1. **through Pulsar** (`gmp.signals.<tenant>` topics → the platform's broker
   adapter does tenant routing), or
2. **directly over HTTP** (`POST /v1/signals:batch`, the ingest edge),

and then exercises the read side: the personalized context served through
**REST /v1** and **MCP** — parity you can eyeball from one terminal.

```
POC app (this repo)                    graph-memory-platform
┌────────────────────┐   signals  ┌──────────────────────────────┐
│ generator.py       │ ─────────▶ │ ingest edge :8001 (HTTP)     │
│ producer_pulsar.py │─ or Pulsar─│   or broker adapter (Pulsar) │
└────────────────────┘            │      │ ClickHouse ledger     │
       ▲                          │      ▼ deterministic projection│
       │ context (REST :8002,     │ per-user graph (ClickGraph)  │
       │ MCP :8003 parity)        │      → effectiveness loop    │
       └──────────────────────────┤ context-service + channels   │
                                  └──────────────────────────────┘
```

## Run it

1. Start the platform (uses its dev static token `dev-acme-token` for tenant
   `acme`):

   ```sh
   cd path/to/graph-memory-platform
   uv run platform serve --mapping tests/fixtures/mapping_skill_loop.yaml
   ```

   Boot replays the ClickHouse ledger first (projection convergence as
   warmup), then serves ingest :8001 / REST :8002 / MCP :8003.

2. In this repo:

   ```sh
   pip install -e .            # or: uv pip install -e ".[pulsar]" for the broker path
   python -m poc_demo.demo --users 4
   ```

   Output: the generated signal counts, the ingested batch result, and the
   **same context body returned by REST and by MCP** (one provider, two
   renders — the platform's channel-parity contract), plus the ranking the
   effectiveness loop produced for the acted-on recommendation class.

3. Broker path instead of HTTP? Start a Pulsar standalone
   (`docker run -p 6650:6650 apachepulsar/pulsar:latest bin/pulsar standalone`)
   and run:

   ```sh
   python -m poc_demo.demo --transport pulsar --users 4
   ```

   The producer emits to `gmp.signals.acme`; the platform's Pulsar adapter
   routes tenants by topic tail and stamps Kafka/Pulsar/NATS ordering keys
   per user (the kit-owned stamping rules).

## What the generator emits

Every event is a CloudEvents-shaped envelope, exactly the grammar the
platform validates:

| signal type | meaning | fields |
|---|---|---|
| `itf.skill_assessed.v1` | learner assessed a skill | `data.skill`, `data.score` |
| `itf.reco.v1` | app recommended a skill (recommendation-class declared) | `data.skill`, `data.cohort`, `data.reco_class` |
| `itf.outcome.v1` | verdict attaching to a reco by `correlation_id` | `data.correlation_id`, `data.verdict` (`acted_on`/`dismissed`) |

Deterministic by seed (`--seed`): identical runs produce identical streams,
so the demo's before/after ranking is reproducible evidence, not vibes.

## Files

```
src/poc_demo/generator.py        AI-signal generator (seeded, ITF-shaped)
src/poc_demo/ingest_http.py      thin HTTP pusher (the platform's own contract)
src/poc_demo/producer_pulsar.py  Pulsar producer (optional extra)
src/poc_demo/context_reads.py    REST + MCP reads for parity + top-k printing
src/poc_demo/demo.py             the orchestrator
```

This repo is a **client**: everything interesting (validating, storing,
projecting, scoring, serving) happens in the platform. For architecture and
tests, see the main repository.
