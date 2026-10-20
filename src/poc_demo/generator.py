"""Seeded AI-signal generator: an app-shaped event stream (ITF cohort loop).

This is the POC's "AI": a deterministic generator producing the signal types
the platform's reference mapping understands — assessments with plausible
skill distributions, recommendations carrying app-declared cohort/reco-class
attributes, and outcome verdicts correlated to the reco that caused them.
Deterministic by seed AND by wall-clock-free timestamps: identical
invocations produce byte-identical streams, so the demo's rankings are
reproducible evidence, not vibes.
"""

from __future__ import annotations

import random
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

_ENVELOPE_VERSION = "1.0"
_SKILLS = ("python", "rust", "go", "sql", "kafka", "ml")
_VERDICTS_POSITIVE = ("acted_on",)
_VERDICTS_NEGATIVE = ("dismissed",)
_BASE_TIME = datetime(2026, 6, 1, 12, 0, tzinfo=UTC)


class AIGenerator:
    """Generates one cohort's event stream: assess → recommend → act."""

    def __init__(
        self,
        *,
        tenant: str = "acme",
        cohort: str = "cohort-a",
        seed: int = 42,
        source: str = "https://poc.demo.app/signals",
    ) -> None:
        self._tenant = tenant
        self._cohort = cohort
        self._random = random.Random(seed)
        self._source = source
        self._seq = 0

    def stream(self, users: int, *, events_per_user: int = 6) -> list[dict[str, Any]]:
        """The batch: interleaved across users, ordered per user."""
        return list(self.iter_stream(users, events_per_user=events_per_user))

    def iter_stream(self, users: int, *, events_per_user: int = 6) -> Iterator[dict[str, Any]]:
        """Envelope dicts; correlation ids bind outcomes to their recos."""
        sequences = [self._sequence_for(self._user(i), events_per_user) for i in range(users)]
        for step in range(events_per_user):
            for sequence in sequences:
                if step < len(sequence):
                    yield sequence[step]

    def _user(self, index: int) -> str:
        return f"demo-user-{index:04d}"

    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq

    def _sequence_for(self, user: str, count: int) -> list[dict[str, Any]]:
        skill_pool = list(_SKILLS)
        self._random.shuffle(skill_pool)
        existing = skill_pool[:2]
        target = skill_pool[2]
        reco_ref = f"demo-reco-{self._next_seq():06d}"
        acted = self._random.random() < 0.75  # the app's loop-ish outcome pattern

        sequence: list[dict[str, Any]] = []
        for skill in existing:  # the learner's existing skills
            sequence.append(
                self._envelope(
                    "itf.skill_assessed.v1",
                    user,
                    {"skill": skill, "score": round(0.6 + self._random.random() * 0.4, 3)},
                )
            )
        sequence.append(
            self._envelope(
                "itf.reco.v1",
                user,
                {
                    "skill": target,
                    "cohort": self._cohort,
                    "reco_class": "gap",
                    "reco_ref": reco_ref,
                },
            )
        )
        sequence.append(
            self._envelope(
                "itf.outcome.v1",
                user,
                {
                    "skill": target,
                    "correlation_id": reco_ref,
                    "verdict": self._random.choice(
                        _VERDICTS_POSITIVE if acted else _VERDICTS_NEGATIVE
                    ),
                },
            )
        )
        return sequence[:count]

    def _envelope(self, type_: str, user: str, data: dict[str, Any]) -> dict[str, Any]:
        seq = self._next_seq()
        occurred = _BASE_TIME + timedelta(milliseconds=seq)
        return {
            "specversion": _ENVELOPE_VERSION,
            "id": f"demo-{seq:06d}",
            "source": self._source,
            "type": type_,
            "time": occurred.isoformat(),
            "partitionkey": user,
            # The tenant rides infra routing (token/topic), never a wire
            # claim — the platform derives it and refuses mismatches.
            "data": dict(data),
        }
