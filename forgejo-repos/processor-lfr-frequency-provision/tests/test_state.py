"""Tests for durable LFR engine state and output outbox behavior."""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from processor_lfr_frequency_provision.engine import ClosedSecond
from processor_lfr_frequency_provision.selection import PmuQuality, PreferredFrequency
from processor_lfr_frequency_provision.state import PendingPublication, StateStore


class StateStoreTests(unittest.TestCase):
    """Require output intent to survive independently of Kafka delivery."""

    def test_persists_snapshot_and_pending_output_across_reopen(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "state.sqlite3"
            store = StateStore(path)
            publication = self._publication()
            store.persist({"closed_through": 100, "states": {}}, (publication,))
            store.close()

            restored = StateStore(path)
            self.assertEqual(restored.load_snapshot(), {"closed_through": 100, "states": {}})
            self.assertEqual(restored.pending_publications(), (publication,))
            restored.mark_delivered(publication.publication_id)
            self.assertEqual(restored.pending_publications(), ())
            restored.close()

    def test_deduplicates_a_replayed_closed_second_outbox_item(self) -> None:
        with TemporaryDirectory() as directory:
            store = StateStore(Path(directory) / "state.sqlite3")
            publication = self._publication()

            store.persist({"closed_through": 100, "states": {}}, (publication, publication))

            self.assertEqual(store.pending_publications(), (publication,))
            store.close()

    def test_creates_publication_only_when_a_second_has_a_preferred_value(self) -> None:
        preferred = ClosedSecond(
            second=100,
            closed_at_ms=101_600,
            pmus=(),
            preferred_frequency=PreferredFrequency(50.01, PmuQuality.GOOD, ("pmu-a",)),
            rejection_counts={},
        )
        no_candidate = ClosedSecond(
            second=101,
            closed_at_ms=102_600,
            pmus=(),
            preferred_frequency=None,
            rejection_counts={},
        )

        publication = PendingPublication.from_closed_second(
            preferred,
            "urn:wama:test:lfr:preferred-frequency",
        )

        self.assertIsNotNone(publication)
        assert publication is not None
        self.assertEqual(publication.publication_id, "urn:wama:test:lfr:preferred-frequency:100")
        self.assertIsNone(
            PendingPublication.from_closed_second(
                no_candidate,
                "urn:wama:test:lfr:preferred-frequency",
            )
        )

    def _publication(self) -> PendingPublication:
        return PendingPublication(
            publication_id="urn:wama:test:lfr:preferred-frequency:100",
            closed_at_ms=101_600,
            frequency_hz=50.01,
            output_mrid="urn:wama:test:lfr:preferred-frequency",
            quality=PmuQuality.GOOD,
            second=100,
        )


if __name__ == "__main__":
    unittest.main()