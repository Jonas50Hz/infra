"""Durable local state and outbox for the LFR at-least-once processor."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
import json
from pathlib import Path
import sqlite3

from processor_lfr_frequency_provision.engine import ClosedSecond
from processor_lfr_frequency_provision.selection import PmuQuality


@dataclass(frozen=True)
class PendingPublication:
    """One deterministic preferred-frequency output awaiting Kafka delivery."""

    publication_id: str
    closed_at_ms: int
    frequency_hz: float
    output_mrid: str
    quality: PmuQuality
    second: int

    @classmethod
    def from_closed_second(
        cls,
        closed: ClosedSecond,
        output_mrid: str,
    ) -> PendingPublication | None:
        """Create an outbox item only when a closed second has a preferred value."""

        preferred = closed.preferred_frequency
        if preferred is None:
            return None
        return cls(
            publication_id=f"{output_mrid}:{closed.second}",
            closed_at_ms=closed.closed_at_ms,
            frequency_hz=preferred.frequency_hz,
            output_mrid=output_mrid,
            quality=preferred.quality,
            second=closed.second,
        )


class StateStore:
    """Persist open LFR state and pending output publication atomically."""

    def __init__(self, path: str | Path) -> None:
        state_path = Path(path)
        state_path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(state_path)
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS engine_state (
                name TEXT PRIMARY KEY,
                snapshot TEXT NOT NULL
            )
            """
        )
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS outbox (
                publication_id TEXT PRIMARY KEY,
                second INTEGER NOT NULL,
                closed_at_ms INTEGER NOT NULL,
                output_mrid TEXT NOT NULL,
                frequency_hz REAL NOT NULL,
                quality INTEGER NOT NULL,
                delivered INTEGER NOT NULL DEFAULT 0
            )
            """
        )

    def close(self) -> None:
        """Close the underlying SQLite connection."""

        self._connection.close()

    def load_snapshot(self) -> dict[str, object] | None:
        """Load the most recently durable open-second engine state."""

        row = self._connection.execute(
            "SELECT snapshot FROM engine_state WHERE name = 'lfr'"
        ).fetchone()
        if row is None:
            return None
        try:
            snapshot = json.loads(row[0])
        except json.JSONDecodeError as error:
            raise RuntimeError("LFR state store contains invalid engine JSON") from error
        if not isinstance(snapshot, dict):
            raise RuntimeError("LFR state store engine snapshot must be an object")
        return snapshot

    def persist(
        self,
        snapshot: Mapping[str, object],
        publications: Iterable[PendingPublication] = (),
    ) -> None:
        """Atomically update engine state and enqueue any newly closed outputs."""

        encoded_snapshot = json.dumps(snapshot, sort_keys=True, separators=(",", ":"))
        with self._connection:
            self._connection.execute(
                """
                INSERT INTO engine_state(name, snapshot) VALUES ('lfr', ?)
                ON CONFLICT(name) DO UPDATE SET snapshot = excluded.snapshot
                """,
                (encoded_snapshot,),
            )
            for publication in publications:
                self._connection.execute(
                    """
                    INSERT OR IGNORE INTO outbox(
                        publication_id,
                        second,
                        closed_at_ms,
                        output_mrid,
                        frequency_hz,
                        quality,
                        delivered
                    ) VALUES (?, ?, ?, ?, ?, ?, 0)
                    """,
                    (
                        publication.publication_id,
                        publication.second,
                        publication.closed_at_ms,
                        publication.output_mrid,
                        publication.frequency_hz,
                        int(publication.quality),
                    ),
                )

    def pending_publications(self) -> tuple[PendingPublication, ...]:
        """Return the oldest undelivered LFR outputs for retry-safe publication."""

        rows = self._connection.execute(
            """
            SELECT publication_id, closed_at_ms, frequency_hz, output_mrid, quality, second
            FROM outbox
            WHERE delivered = 0
            ORDER BY second ASC, publication_id ASC
            """
        ).fetchall()
        return tuple(
            PendingPublication(
                publication_id=row[0],
                closed_at_ms=row[1],
                frequency_hz=row[2],
                output_mrid=row[3],
                quality=PmuQuality(row[4]),
                second=row[5],
            )
            for row in rows
        )

    def mark_delivered(self, publication_id: str) -> None:
        """Record confirmed Kafka delivery for one outbox item."""

        with self._connection:
            cursor = self._connection.execute(
                "UPDATE outbox SET delivered = 1 WHERE publication_id = ?",
                (publication_id,),
            )
        if cursor.rowcount != 1:
            raise RuntimeError(f"LFR outbox publication is unavailable: {publication_id}")